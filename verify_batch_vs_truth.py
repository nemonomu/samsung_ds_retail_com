"""두 배치(12:35 cprice 정책 vs 13:02 featCprice 정책)를 truth와 비교.
truth = 사용자 명시 17건 mismatch + 12:35 batch의 나머지 48건 정답 (사용자 명시).
"""
import sys
try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

import pandas as pd
from sqlalchemy import create_engine, text
from config import DB_CONFIG_V2 as DB_CONFIG

engine = create_engine(
    f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
    f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

# 사용자가 전수조사로 알려준 17건 페이지 가격 (12:35 batch에서 mismatch였던 SKU)
USER_MISMATCH = {
    '6618322': 184.59,
    '6618321': 299.99,
    '6618930': 599.95,
    '6601257': 499.99,
    '6523590': 499.99,
    '6576883': 879.00,
    '6472036': 749.00,
    '6636636': 494.44,
    '6636635': 749.99,
    '6559270': 929.00,
    '6394313': 174.99,
    '6447128': 833.09,
    '6447130': 1682.08,
    '6636637': 453.74,
    '6559271': 939.99,
    '6566099': 249.99,
    '6509713': 349.99,
}

df = pd.read_sql(
    text(
        "SELECT retailersku, retailprice, crawl_strdatetime "
        "FROM bestbuy_price_crawl_tbl_usa_v2 "
        "WHERE DATE(crawl_datetime) = CURDATE() "
        "ORDER BY crawl_strdatetime DESC"
    ),
    engine,
)
df['retailersku'] = df['retailersku'].astype(str)
df['crawl_second'] = df['crawl_strdatetime'].str[:14]

seconds = sorted(df['crawl_second'].unique())
if len(seconds) < 2:
    print(f"ERROR: 오늘 batch가 {len(seconds)}개만 있음. 두 batch 필요.")
    sys.exit(1)

old_sec = seconds[-2]  # 더 오래된 = 12:35 (cprice 정책)
new_sec = seconds[-1]  # 가장 최근 = 13:02 (featCprice 정책)

old_batch = df[df['crawl_second'] == old_sec].drop_duplicates(
    subset=['retailersku'], keep='first'
).set_index('retailersku')['retailprice']
new_batch = df[df['crawl_second'] == new_sec].drop_duplicates(
    subset=['retailersku'], keep='first'
).set_index('retailersku')['retailprice']

print(f"OLD batch ({old_sec}): {len(old_batch)} rows  [cprice 정책]")
print(f"NEW batch ({new_sec}): {len(new_batch)} rows  [featCprice 정책]")
print()

# Truth = OLD batch (cprice) 값 + 사용자 명시 17건 override
truth = old_batch.copy()
for sku, expected in USER_MISMATCH.items():
    truth[sku] = float(expected)

def compare(name, batch):
    matches = 0
    mismatches = []
    missing = []
    for sku, expected in truth.items():
        if sku not in batch:
            missing.append(sku)
            continue
        actual = float(batch[sku])
        if abs(float(expected) - actual) < 0.01:
            matches += 1
        else:
            mismatches.append((sku, float(expected), actual))
    print(f"=== {name} vs truth ===")
    print(f"  match: {matches}/{len(truth)}")
    print(f"  mismatch: {len(mismatches)}")
    print(f"  missing: {len(missing)}")
    if mismatches:
        print("  mismatched SKUs:")
        for sku, exp, act in mismatches:
            print(f"    {sku}: expected={exp}, actual={act}")
    if missing:
        print(f"  missing SKUs: {missing}")
    print()

compare("OLD batch (cprice)", old_batch)
compare("NEW batch (featCprice)", new_batch)

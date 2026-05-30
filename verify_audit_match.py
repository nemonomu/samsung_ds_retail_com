"""사용자가 전수조사로 확인한 17개 SKU의 페이지 가격과
방금 real run으로 저장된 retailprice 가 일치하는지 검증.
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

# 사용자가 전수조사로 알려준 페이지 가격
VERIFIED = {
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

# 오늘 저장된 row 중 가장 최근 crawl_strdatetime 의 65개만
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
# crawl_strdatetime 은 microsecond 까지 포함되어 row 마다 다름.
# 같은 batch는 같은 second 안에 들어가므로 YYYYMMDDHHMMSS (14자) prefix 로 묶어 최근 batch 추출.
df['crawl_second'] = df['crawl_strdatetime'].str[:14]
latest_second = df['crawl_second'].max()
latest = df[df['crawl_second'] == latest_second]

print(f"Latest run: crawl_second={latest_second}, rows={len(latest)}")
print()
print(f"{'SKU':<10} | {'Expected':>10} | {'DB retailprice':>15} | Match")
print("-" * 55)

mismatched = []
for sku, expected in VERIFIED.items():
    rows = latest[latest['retailersku'] == sku]
    if rows.empty:
        print(f"{sku:<10} | {expected:>10.2f} | {'MISSING':>15} | ?")
        mismatched.append(sku)
        continue
    actual = float(rows['retailprice'].iloc[0])
    match = abs(actual - expected) < 0.01
    sym = 'OK' if match else 'MISMATCH'
    print(f"{sku:<10} | {expected:>10.2f} | {actual:>15.2f} | {sym}")
    if not match:
        mismatched.append((sku, expected, actual))

print()
print(f"Verified {len(VERIFIED)} SKU. Mismatched: {len(mismatched)}.")
if mismatched:
    print("Mismatched details:")
    for m in mismatched:
        print(f"  {m}")

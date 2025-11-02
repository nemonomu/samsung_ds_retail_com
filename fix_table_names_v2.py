# -*- coding: utf-8 -*-
"""
모든 v2 파일의 테이블 이름을 _v2 형식으로 수정
"""
import re
import os

# 테이블 이름 매핑 (old -> new)
table_mappings = {
    'amazon_price_crawl_tbl_fr': 'amazon_price_crawl_tbl_fr_v2',
    'amazon_price_crawl_tbl_de': 'amazon_price_crawl_tbl_de_v2',
    'amazon_price_crawl_tbl_ind': 'amazon_price_crawl_tbl_ind_v2',
    'amazon_price_crawl_tbl_it': 'amazon_price_crawl_tbl_it_v2',
    'amazon_price_crawl_tbl_uk': 'amazon_price_crawl_tbl_uk_v2',
}

# f-string 패턴 수정 (동적 테이블 이름)
fstring_files = ['es_v2.py', 'au_v2.py', 'usa_v2.py', 'jp_v2.py']

# 고정 테이블 이름 수정
for old_name, new_name in table_mappings.items():
    # 파일명 추출 (예: fr_v2.py)
    country_code = old_name.replace('amazon_price_crawl_tbl_', '').replace('ind', 'in')
    filename = f'{country_code}_v2.py'

    if not os.path.exists(filename):
        print(f"[SKIP] {filename} not found")
        continue

    print(f"\n{'='*60}")
    print(f"Processing {filename}...")
    print(f"{'='*60}")

    with open(filename, 'r', encoding='utf-8') as f:
        content = f.read()

    original_content = content

    # 테이블 이름 교체
    content = content.replace(f"'{old_name}'", f"'{new_name}'")

    if content != original_content:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"[OK] {filename} updated: {old_name} -> {new_name}")
    else:
        print(f"[NO CHANGE] {filename}")

# f-string 패턴 파일 수정
print(f"\n{'='*60}")
print(f"Processing f-string table names...")
print(f"{'='*60}")

for filename in fstring_files:
    if not os.path.exists(filename):
        print(f"[SKIP] {filename} not found")
        continue

    print(f"\nProcessing {filename}...")

    with open(filename, 'r', encoding='utf-8') as f:
        content = f.read()

    original_content = content

    # f'amazon_price_crawl_tbl_{self.country_code}' -> f'amazon_price_crawl_tbl_{self.country_code}_v2'
    content = re.sub(
        r"f'amazon_price_crawl_tbl_\{self\.country_code\}'",
        r"f'amazon_price_crawl_tbl_{self.country_code}_v2'",
        content
    )

    if content != original_content:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"[OK] {filename} updated: added _v2 suffix to dynamic table name")
    else:
        print(f"[NO CHANGE] {filename}")

print("\n" + "="*60)
print("All files processed!")
print("="*60)

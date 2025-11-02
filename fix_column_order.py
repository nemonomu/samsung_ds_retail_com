# -*- coding: utf-8 -*-
"""
타임존 컬럼 순서 수정
현재: crawl_datetime, kr_crawl_datetime, kr_crawl_strdatetime, crawl_strdatetime
수정: crawl_datetime, crawl_strdatetime, kr_crawl_datetime, kr_crawl_strdatetime
"""
import re
import os

v2_files = [
    'fr_v2.py', 'de_v2.py', 'it_v2.py', 'au_v2.py', 'in_v2.py', 'es_v2.py',
    'uk_v2.py', 'jp_v2.py', 'usa_v2.py', 'bestbuy_v2.py', 'currys_v2.py',
    'xkom_v2.py', 'danawa_v2.py', 'mediamarkt_v2.py', 'coolblue_nl_v2.py'
]

for filename in v2_files:
    if not os.path.exists(filename):
        print(f"[SKIP] {filename} not found")
        continue

    print(f"\n{'='*60}")
    print(f"Processing {filename}...")
    print(f"{'='*60}")

    with open(filename, 'r', encoding='utf-8') as f:
        content = f.read()

    original_content = content

    # 패턴 1: 4줄로 된 타임존 컬럼 (잘못된 순서)
    # 'crawl_datetime': local_time...
    # 'kr_crawl_datetime': now_time...
    # 'kr_crawl_strdatetime': now_time...
    # 'crawl_strdatetime': local_time...

    pattern = r"('crawl_datetime': local_time\.strftime\('%Y-%m-%d %H:%M:%S'\),)\n(\s+)('kr_crawl_datetime': now_time\.strftime\('%Y-%m-%d %H:%M:%S'\),.*?\n)(\s+)('kr_crawl_strdatetime': now_time\.strftime\('%Y%m%d%H%M%S'\) \+ f\"\{now_time\.microsecond:06d\}\"\[:4\],.*?\n)(\s+)('crawl_strdatetime': local_time\.strftime\('%Y%m%d%H%M%S'\) \+ f\"\{local_time\.microsecond:06d\}\"\[:4\],)"

    replacement = r"\1\n\2\7\n\4\3\6\5"

    content = re.sub(pattern, replacement, content)

    if content != original_content:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"[OK] {filename} updated")
    else:
        print(f"[NO CHANGE] {filename}")

print("\n" + "="*60)
print("All files processed!")
print("="*60)

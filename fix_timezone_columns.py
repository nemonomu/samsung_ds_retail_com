# -*- coding: utf-8 -*-
"""
모든 v2 파일의 타임존 컬럼을 올바른 형식으로 수정
crawl_datetime: 현지시간
crawl_strdatetime: 현지시간 문자열
kr_crawl_datetime: 한국시간
kr_crawl_strdatetime: 한국시간 문자열
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

    # 패턴 1: now_time과 local_time 정의 부분을 찾아서 수정
    # "# V2: 타임존 분리\n            now_time = datetime.now(self.korea_tz)\n            local_time = datetime.now(self.local_tz)"
    # 이 부분은 그대로 두고

    # 패턴 2: result 딕셔너리에서 컬럼 수정
    # 기존: 'crawl_datetime': now_time.strftime('%Y-%m-%d %H:%M:%S'),
    #       'local_crawl_datetime': local_time.strftime('%Y-%m-%d %H:%M:%S'),
    #       'crawl_strdatetime': now_time.strftime('%Y%m%d%H%M%S') + ...

    # 새로운: 'crawl_datetime': local_time.strftime('%Y-%m-%d %H:%M:%S'),
    #         'crawl_strdatetime': local_time.strftime('%Y%m%d%H%M%S') + ...
    #         'kr_crawl_datetime': now_time.strftime('%Y-%m-%d %H:%M:%S'),
    #         'kr_crawl_strdatetime': now_time.strftime('%Y%m%d%H%M%S') + ...

    # crawl_datetime을 local_time으로 변경
    content = re.sub(
        r"'crawl_datetime':\s*now_time\.strftime\('%Y-%m-%d %H:%M:%S'\)",
        "'crawl_datetime': local_time.strftime('%Y-%m-%d %H:%M:%S')",
        content
    )

    # crawl_strdatetime을 local_time으로 변경
    content = re.sub(
        r"'crawl_strdatetime':\s*now_time\.strftime\('%Y%m%d%H%M%S'\)\s*\+\s*f\"\{now_time\.microsecond:06d\}\"\[:4\]",
        "'crawl_strdatetime': local_time.strftime('%Y%m%d%H%M%S') + f\"{local_time.microsecond:06d}\"[:4]",
        content
    )

    # local_crawl_datetime을 kr_crawl_datetime으로 변경하고 값도 수정
    content = re.sub(
        r"'local_crawl_datetime':\s*local_time\.strftime\('%Y-%m-%d %H:%M:%S'\),?\s*#\s*V2:\s*현지시간",
        "'kr_crawl_datetime': now_time.strftime('%Y-%m-%d %H:%M:%S'),  # V2: 한국시간",
        content
    )

    # kr_crawl_datetime 뒤에 kr_crawl_strdatetime 추가
    content = re.sub(
        r"('kr_crawl_datetime':\s*now_time\.strftime\('%Y-%m-%d %H:%M:%S'\),\s*#\s*V2:\s*한국시간)",
        r"\1\n                'kr_crawl_strdatetime': now_time.strftime('%Y%m%d%H%M%S') + f\"{now_time.microsecond:06d}\"[:4],  # V2: 한국시간 문자열",
        content
    )

    # danawa나 다른 파일에서 local_time이 정의되지 않은 경우 처리
    # V2: 타임존 분리가 없는 파일 확인
    if 'local_time = datetime.now(self.local_tz)' not in content and 'self.local_tz' in content:
        # now_time 정의 찾기
        content = re.sub(
            r'(\s+)(now_time = datetime\.now\(self\.korea_tz\))\n',
            r'\1# V2: 타임존 분리\n\1now_time = datetime.now(self.korea_tz)\n\1local_time = datetime.now(self.local_tz)\n',
            content
        )

    if content != original_content:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"[OK] {filename} updated")
    else:
        print(f"[NO CHANGE] {filename}")

print("\n" + "="*60)
print("All files processed!")
print("="*60)

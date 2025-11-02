# -*- coding: utf-8 -*-
"""
원본 크롤러 파일들을 V2 버전으로 변환하는 스크립트
DB/타임존/파일서버 설정만 V2로 변경
"""
import re
import os

# 각 국가별 설정
COUNTRY_CONFIGS = {
    'de': {
        'timezone': 'Europe/Berlin',
        'country_name': '독일',
        'class_name': 'AmazonDEScraper',
        'method_prefix': 'de'
    },
    'it': {
        'timezone': 'Europe/Rome',
        'country_name': '이탈리아',
        'class_name': 'AmazonITScraper',
        'method_prefix': 'it'
    },
    'au': {
        'timezone': 'Australia/Sydney',
        'country_name': '호주',
        'class_name': 'AmazonAUScraper',
        'method_prefix': 'au'
    },
    'in': {
        'timezone': 'Asia/Kolkata',
        'country_name': '인도',
        'class_name': 'AmazonINScraper',
        'method_prefix': 'in'
    },
    'es': {
        'timezone': 'Europe/Madrid',
        'country_name': '스페인',
        'class_name': 'AmazonESScraper',
        'method_prefix': 'es'
    },
    'uk': {
        'timezone': 'Europe/London',
        'country_name': '영국',
        'class_name': 'AmazonUKScraper',
        'method_prefix': 'uk'
    },
    'jp': {
        'timezone': 'Asia/Tokyo',
        'country_name': '일본',
        'class_name': 'AmazonJPScraper',
        'method_prefix': 'jp'
    },
    'usa': {
        'timezone': 'America/New_York',
        'country_name': '미국',
        'class_name': 'AmazonUSAScraper',
        'method_prefix': 'usa'
    },
    'bestbuy': {
        'timezone': 'America/Chicago',
        'country_name': 'BestBuy',
        'class_name': 'BestBuyScraper',
        'method_prefix': 'bestbuy'
    }
}

def convert_file_to_v2(filename, country_code):
    """파일을 V2로 변환"""
    print(f"\n{'='*60}")
    print(f"Converting {filename} to V2...")
    print(f"{'='*60}")

    config = COUNTRY_CONFIGS[country_code]
    filepath = f"{filename}.py"
    v2_filepath = f"{filename}_v2.py"

    # 파일 읽기
    with open(v2_filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    original_content = content

    # 1. 헤더 주석 수정
    # 첫 번째 docstring 찾기
    docstring_pattern = r'(# -\*- coding: utf-8 -\*-\n""")(.*?)(""")'
    match = re.search(docstring_pattern, content, re.DOTALL)
    if match:
        new_header = f'''# -*- coding: utf-8 -*-
"""
Amazon {config['country_name']} 가격 추출 시스템 V2 (타임존 분리 버전)
원본 {filename}.py 기반 - DB/타임존/파일서버 설정만 V2로 변경
- 현지시간({config['country_name']})과 한국시간 분리 저장
- 새 데이터베이스 사용 (DB_CONFIG_V2)
- 핵심 로직은 원본과 동일
"""'''
        content = content.replace(match.group(0), new_header, 1)
        print("[OK] Header modified")

    # 2. DB_CONFIG import 수정
    content = re.sub(
        r'# Import database configuration\nfrom config import DB_CONFIG',
        '# Import database configuration V2\nfrom config import DB_CONFIG_V2 as DB_CONFIG',
        content
    )
    print("[OK] DB_CONFIG import modified")

    # 3. local_tz 추가
    # korea_tz 다음에 local_tz 추가
    korea_tz_pattern = r"(self\.korea_tz = pytz\.timezone\('Asia/Seoul'\))"
    if re.search(korea_tz_pattern, content):
        replacement = f"""# V2: 타임존 분리 (현지시간 + 한국시간)
        self.korea_tz = pytz.timezone('Asia/Seoul')
        self.local_tz = pytz.timezone('{config['timezone']}')  # {config['country_name']} 현지 시간"""
        content = re.sub(korea_tz_pattern, replacement, content)
        print(f"[OK] local_tz added ({config['timezone']})")

    # 4. crawl_datetime 부분에 local_crawl_datetime 추가
    # now_time = datetime.now(self.korea_tz) 패턴 찾기
    now_time_pattern = r"(\s+)(now_time = datetime\.now\(self\.korea_tz\))"

    def add_local_time(match):
        indent = match.group(1)
        return f'''{indent}# V2: 타임존 분리
{indent}now_time = datetime.now(self.korea_tz)
{indent}local_time = datetime.now(self.local_tz)'''

    content = re.sub(now_time_pattern, add_local_time, content)
    print("[OK] now_time sections modified")

    # 5. result 딕셔너리에 local_crawl_datetime 추가
    # 'crawl_datetime': now_time.strftime('%Y-%m-%d %H:%M:%S'), 다음에 추가
    crawl_datetime_pattern = r"('crawl_datetime': now_time\.strftime\('%Y-%m-%d %H:%M:%S'\),)"

    def add_local_crawl_datetime(match):
        return match.group(1) + "\n                'local_crawl_datetime': local_time.strftime('%Y-%m-%d %H:%M:%S'),  # V2: 현지시간"

    content = re.sub(crawl_datetime_pattern, add_local_crawl_datetime, content)
    print("[OK] local_crawl_datetime added")

    # 변경사항이 있으면 파일 저장
    if content != original_content:
        with open(v2_filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"[SUCCESS] {v2_filepath} conversion completed!")
        return True
    else:
        print(f"[WARNING] {v2_filepath} - No changes made")
        return False

def main():
    """메인 실행"""
    print("=" * 60)
    print("V2 변환 스크립트 시작")
    print("=" * 60)

    # fr은 이미 수동으로 완료
    files_to_convert = ['de', 'it', 'au', 'in', 'es', 'uk', 'jp', 'usa', 'bestbuy']

    success_count = 0
    for country_code in files_to_convert:
        try:
            if convert_file_to_v2(country_code, country_code):
                success_count += 1
        except Exception as e:
            print(f"[ERROR] {country_code} conversion failed: {e}")

    print("\n" + "=" * 60)
    print(f"변환 완료: {success_count}/{len(files_to_convert)} 파일")
    print("=" * 60)

if __name__ == "__main__":
    main()

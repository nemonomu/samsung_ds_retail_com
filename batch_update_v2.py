"""
15개 크롤러 파일 일괄 수정 스크립트
- 파일 경로 구조: /국가코드/날짜/
- MD5 파일 형식: .txt → .md5, 내용 형식 변경
"""
import os
import re

# 수정할 파일 목록
FILES_TO_UPDATE = [
    'it_v2.py',
    'es_v2.py',
    'jp_v2.py',
    'au_v2.py',
    'in_v2.py',
    'bestbuy_v2.py',
    'mediamarkt_v2.py',
    'currys_v2.py',
    'coolblue_nl_v2.py',
    'danawa_v2.py',
    'xkom_v2.py'
]

def update_file(filepath):
    """파일 수정"""
    print(f"\n{'='*60}")
    print(f"처리 중: {os.path.basename(filepath)}")
    print(f"{'='*60}")

    try:
        # 파일 읽기
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()

        original_content = content
        modifications = []

        # 수정 1: upload_to_file_server 함수 - 경로 구조 변경
        pattern1_old = r'''(\s+)# 날짜별 디렉토리 경로\s+date_dir = f"\{FILE_SERVER_CONFIG\['upload_path'\]\}/\{date_folder\}"\s+# 디렉토리가 없으면 생성\s+try:\s+sftp\.stat\(date_dir\)\s+except FileNotFoundError:\s+logger\.info\(f"📁 날짜 디렉토리 생성: \{date_dir\}"\)\s+sftp\.mkdir\(date_dir\)'''

        pattern1_new = r'''\1# 국가별 디렉토리 경로
\1country_dir = f"{FILE_SERVER_CONFIG['upload_path']}/{self.country_code}"

\1# 국가 디렉토리가 없으면 생성
\1try:
\1    sftp.stat(country_dir)
\1except FileNotFoundError:
\1    logger.info(f"📁 국가 디렉토리 생성: {country_dir}")
\1    sftp.mkdir(country_dir)

\1# 날짜별 디렉토리 경로
\1date_dir = f"{country_dir}/{date_folder}"

\1# 날짜 디렉토리가 없으면 생성
\1try:
\1    sftp.stat(date_dir)
\1except FileNotFoundError:
\1    logger.info(f"📁 날짜 디렉토리 생성: {date_dir}")
\1    sftp.mkdir(date_dir)'''

        # 더 간단한 패턴으로 시도
        old_path_block = '''            # 날짜별 디렉토리 경로
            date_dir = f"{FILE_SERVER_CONFIG['upload_path']}/{date_folder}"

            # 디렉토리가 없으면 생성
            try:
                sftp.stat(date_dir)
            except FileNotFoundError:
                logger.info(f"📁 날짜 디렉토리 생성: {date_dir}")
                sftp.mkdir(date_dir)'''

        new_path_block = '''            # 국가별 디렉토리 경로
            country_dir = f"{FILE_SERVER_CONFIG['upload_path']}/{self.country_code}"

            # 국가 디렉토리가 없으면 생성
            try:
                sftp.stat(country_dir)
            except FileNotFoundError:
                logger.info(f"📁 국가 디렉토리 생성: {country_dir}")
                sftp.mkdir(country_dir)

            # 날짜별 디렉토리 경로
            date_dir = f"{country_dir}/{date_folder}"

            # 날짜 디렉토리가 없으면 생성
            try:
                sftp.stat(date_dir)
            except FileNotFoundError:
                logger.info(f"📁 날짜 디렉토리 생성: {date_dir}")
                sftp.mkdir(date_dir)'''

        if old_path_block in content:
            content = content.replace(old_path_block, new_path_block)
            modifications.append("[OK] 파일 경로 구조 변경 (국가별 디렉토리)")
        else:
            print("  [WARNING] 경로 구조 패턴을 찾지 못했습니다")

        # 수정 2: save_results 함수 - MD5 파일 형식 변경
        old_md5_block = '''                # 4. TXT 파일 생성 (MD5 저장)
                txt_filename = f'{base_filename}.txt'
                with open(txt_filename, 'w', encoding='utf-8') as f:
                    f.write(f"csv_md5: {csv_md5}\\n")
                    f.write(f"zip_md5: {zip_md5}\\n")

                # 5. ZIP과 TXT를 날짜 폴더에 업로드
                if self.upload_to_file_server(zip_filename, date_str):
                    if self.upload_to_file_server(txt_filename, date_str):
                        results['server_uploaded'] = True

                # 6. 로컬 임시 파일 삭제
                for temp_file in [csv_filename, zip_filename, txt_filename]:'''

        new_md5_block = '''                # 4. MD5 파일 생성 (정합성 확인)
                md5_filename = f'{base_filename}.md5'
                with open(md5_filename, 'w', encoding='utf-8') as f:
                    f.write(f"{os.path.basename(zip_filename)} {zip_md5}\\n")
                    f.write(f"{os.path.basename(csv_filename)} {csv_md5}\\n")

                # 5. ZIP과 MD5를 날짜 폴더에 업로드
                if self.upload_to_file_server(zip_filename, date_str):
                    if self.upload_to_file_server(md5_filename, date_str):
                        results['server_uploaded'] = True

                # 6. 로컬 임시 파일 삭제
                for temp_file in [csv_filename, zip_filename, md5_filename]:'''

        if old_md5_block in content:
            content = content.replace(old_md5_block, new_md5_block)
            modifications.append("[OK] MD5 파일 형식 변경 (.txt -> .md5)")
        else:
            print("  [WARNING] MD5 파일 패턴을 찾지 못했습니다")

        # 수정 사항이 있는 경우에만 파일 쓰기
        if content != original_content:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)

            print(f"  [SUCCESS] 수정 완료:")
            for mod in modifications:
                print(f"     {mod}")
            return True
        else:
            print(f"  [INFO] 수정할 내용이 없습니다")
            return False

    except Exception as e:
        print(f"  [ERROR] 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """메인 실행"""
    base_dir = r"C:\Users\gomguard\Documents\퀵오일\삼성전자\samsung_ds_retail_com"

    print("="*60)
    print("15개 크롤러 파일 일괄 수정 시작")
    print("="*60)
    print(f"대상 파일: {len(FILES_TO_UPDATE)}개")
    print()

    success_count = 0
    fail_count = 0

    for filename in FILES_TO_UPDATE:
        filepath = os.path.join(base_dir, filename)

        if not os.path.exists(filepath):
            print(f"[ERROR] 파일 없음: {filename}")
            fail_count += 1
            continue

        if update_file(filepath):
            success_count += 1
        else:
            fail_count += 1

    print("\n" + "="*60)
    print("수정 완료")
    print("="*60)
    print(f"성공: {success_count}개")
    print(f"실패: {fail_count}개")
    print(f"전체: {len(FILES_TO_UPDATE)}개")

    if fail_count > 0:
        print("\n[WARNING] 일부 파일 수정 실패. backup_20251108 폴더에서 복원 가능합니다.")
    else:
        print("\n[SUCCESS] 모든 파일 수정 성공!")

if __name__ == "__main__":
    main()

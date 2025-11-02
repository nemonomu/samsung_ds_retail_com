# -*- coding: utf-8 -*-
"""
모든 v2 파일의 파일서버 저장 로직 업데이트
- ZIP/TXT 생성
- 날짜별 폴더 업로드
"""
import re
import os

# 각 파일의 국가코드/업체명 매핑
file_mappings = {
    'de_v2.py': 'de_amazon',
    'it_v2.py': 'it_amazon',
    'au_v2.py': 'au_amazon',
    'in_v2.py': 'in_amazon',
    'es_v2.py': 'es_amazon',
    'uk_v2.py': 'uk_amazon',
    'jp_v2.py': 'jp_amazon',
    'usa_v2.py': 'usa_amazon',
    'bestbuy_v2.py': 'usa_bestbuy',
    'currys_v2.py': 'gb_currys',
    'xkom_v2.py': 'pl_xkom',
    'danawa_v2.py': 'kr_danawa',
    'mediamarkt_v2.py': 'de_mediamarkt',
    'coolblue_nl_v2.py': 'nl_coolblue'
}

for filename, suffix in file_mappings.items():
    if not os.path.exists(filename):
        print(f"[SKIP] {filename} not found")
        continue

    print(f"\n{'='*60}")
    print(f"Processing {filename}...")
    print(f"{'='*60}")

    with open(filename, 'r', encoding='utf-8') as f:
        content = f.read()

    original_content = content

    # 1. import 추가 (zipfile, hashlib)
    if 'import zipfile' not in content:
        content = re.sub(
            r'(import os\nfrom io import StringIO)',
            r'\1\nimport zipfile\nimport hashlib',
            content
        )
        print("  ✓ Added zipfile and hashlib imports")

    # 2. upload_to_file_server() 함수 교체
    # 함수 시작부터 끝까지 찾기
    upload_pattern = r'def upload_to_file_server\(self, local_file_path[^)]*\):.*?(?=\n    def |\nclass |\Z)'

    new_upload_func = '''def upload_to_file_server(self, local_file_path, date_folder):
        """파일서버에 업로드"""
        try:
            transport = paramiko.Transport((FILE_SERVER_CONFIG['host'], FILE_SERVER_CONFIG['port']))
            transport.connect(
                username=FILE_SERVER_CONFIG['username'],
                password=FILE_SERVER_CONFIG['password']
            )
            sftp = paramiko.SFTPClient.from_transport(transport)

            # 날짜별 디렉토리 경로
            date_dir = f"{FILE_SERVER_CONFIG['upload_path']}/{date_folder}"

            # 디렉토리가 없으면 생성
            try:
                sftp.stat(date_dir)
            except FileNotFoundError:
                logger.info(f"📁 날짜 디렉토리 생성: {date_dir}")
                sftp.mkdir(date_dir)

            # 업로드 경로
            remote_filename = os.path.basename(local_file_path)
            remote_path = f"{date_dir}/{remote_filename}"

            # 파일 업로드
            sftp.put(local_file_path, remote_path)
            logger.info(f"✅ 파일서버 업로드 완료: {remote_path}")

            sftp.close()
            transport.close()

            return True
        except Exception as e:
            logger.error(f"❌ 파일서버 업로드 실패: {e}")
            return False

    '''

    if re.search(upload_pattern, content, re.DOTALL):
        content = re.sub(upload_pattern, new_upload_func, content, flags=re.DOTALL)
        print("  ✓ Updated upload_to_file_server() function")

    # 3. save_results() 함수에서 파일명 패턴과 업로드 로직 수정
    # base_filename 패턴 찾기 및 수정
    old_pattern = fr"base_filename = f\"{{now\.strftime\('%Y%m%d%H%M%S'\)}}_.*?{suffix.split('_')[1]}\""
    new_pattern = f'''date_str = now.strftime('%Y%m%d')
        time_str = now.strftime('%H%M%S')
        base_filename = f"{{date_str}}_{{time_str}}_{suffix}"'''

    if re.search(old_pattern, content):
        content = re.sub(old_pattern, new_pattern, content)
        print(f"  ✓ Updated base_filename to {suffix} format")

    # 4. save_results() 함수의 upload_server 블록 교체
    # 기존 CSV 업로드 로직을 ZIP/TXT 로직으로 교체
    old_upload_block = r'''if upload_server:
            try:
                csv_filename = f'\{base_filename\}\.csv'
                df\.to_csv\(csv_filename, index=False, encoding='utf-8-sig'\)

                if self\.upload_to_file_server\(csv_filename.*?\):
                    results\['server_uploaded'\] = True

                if os\.path\.exists\(csv_filename\):
                    os\.remove\(csv_filename\)

                logger\.info\(.*?임시 파일.*?\)
            except Exception as e:
                logger\.error\(.*?파일 저장 실패.*?\)'''

    new_upload_block = '''if upload_server:
            try:
                # 1. CSV 파일 생성
                csv_filename = f'{base_filename}.csv'
                df.to_csv(csv_filename, index=False, encoding='utf-8-sig')

                # 2. CSV를 ZIP으로 압축
                zip_filename = f'{base_filename}.zip'
                with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
                    zipf.write(csv_filename, arcname=csv_filename)

                # 3. MD5 계산
                def calculate_md5(filename):
                    md5 = hashlib.md5()
                    with open(filename, 'rb') as f:
                        for chunk in iter(lambda: f.read(4096), b''):
                            md5.update(chunk)
                    return md5.hexdigest()

                csv_md5 = calculate_md5(csv_filename)
                zip_md5 = calculate_md5(zip_filename)

                # 4. TXT 파일 생성 (MD5 저장)
                txt_filename = f'{base_filename}.txt'
                with open(txt_filename, 'w', encoding='utf-8') as f:
                    f.write(f"csv_md5: {csv_md5}\\n")
                    f.write(f"zip_md5: {zip_md5}\\n")

                # 5. ZIP과 TXT를 날짜 폴더에 업로드
                if self.upload_to_file_server(zip_filename, date_str):
                    if self.upload_to_file_server(txt_filename, date_str):
                        results['server_uploaded'] = True

                # 6. 로컬 임시 파일 삭제
                for temp_file in [csv_filename, zip_filename, txt_filename]:
                    if os.path.exists(temp_file):
                        os.remove(temp_file)

                logger.info("임시 파일 삭제 완료")
            except Exception as e:
                logger.error(f"파일 저장 실패: {e}")'''

    if re.search(old_upload_block, content, re.DOTALL):
        content = re.sub(old_upload_block, new_upload_block, content, flags=re.DOTALL)
        print("  ✓ Updated upload_server block with ZIP/TXT logic")

    # 파일 저장
    if content != original_content:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"[OK] {filename} updated successfully")
    else:
        print(f"[NO CHANGE] {filename}")

print("\n" + "="*60)
print("All files processed!")
print("="*60)

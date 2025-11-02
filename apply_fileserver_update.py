# -*- coding: utf-8 -*-
"""
fr_v2.py를 기준으로 나머지 v2 파일들의 파일서버 로직 업데이트
"""
import re

# 파일명과 suffix 매핑
files_to_update = [
    ('de_v2.py', 'de_amazon'),
    ('it_v2.py', 'it_amazon'),
    ('au_v2.py', 'au_amazon'),
    ('in_v2.py', 'in_amazon'),
    ('es_v2.py', 'es_amazon'),
    ('uk_v2.py', 'uk_amazon'),
    ('jp_v2.py', 'jp_amazon'),
    ('usa_v2.py', 'usa_amazon'),
    ('bestbuy_v2.py', 'usa_bestbuy'),
    ('currys_v2.py', 'gb_currys'),
    ('xkom_v2.py', 'pl_xkom'),
    ('danawa_v2.py', 'kr_danawa'),
    ('mediamarkt_v2.py', 'de_mediamarkt'),
    ('coolblue_nl_v2.py', 'nl_coolblue')
]

# 새로운 upload_to_file_server 함수 (fr_v2.py에서 가져옴)
new_upload_function = '''    def upload_to_file_server(self, local_file_path, date_folder):
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

# 새로운 save_results 함수 템플릿
save_results_template = '''    def save_results(self, df, save_db=True, upload_server=True):
        """결과 저장"""
        now = datetime.now(self.korea_tz)
        date_str = now.strftime('%Y%m%d')
        time_str = now.strftime('%H%M%S')
        base_filename = f"{{date_str}}_{{time_str}}_{SUFFIX}"

        results = {{'db_saved': False, 'server_uploaded': False}}

        if save_db:
            results['db_saved'] = self.save_to_db(df)

        if upload_server:
            try:
                # 1. CSV 파일 생성
                csv_filename = f'{{base_filename}}.csv'
                df.to_csv(csv_filename, index=False, encoding='utf-8-sig')

                # 2. CSV를 ZIP으로 압축
                zip_filename = f'{{base_filename}}.zip'
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
                txt_filename = f'{{base_filename}}.txt'
                with open(txt_filename, 'w', encoding='utf-8') as f:
                    f.write(f"csv_md5: {{csv_md5}}\\n")
                    f.write(f"zip_md5: {{zip_md5}}\\n")

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
                logger.error(f"파일 저장 실패: {{e}}")

        return results
'''

for filename, suffix in files_to_update:
    print(f"\\n{'='*60}")
    print(f"Processing {filename}...")
    print(f"{'='*60}")

    try:
        with open(filename, 'r', encoding='utf-8') as f:
            content = f.read()

        original = content

        # 1. import 추가
        if 'import zipfile' not in content:
            content = content.replace(
                'import os\\nfrom io import StringIO\\nimport json',
                'import os\\nfrom io import StringIO\\nimport json\\nimport zipfile\\nimport hashlib'
            )
            print("  ✓ Added imports")

        # 2. upload_to_file_server 함수 교체
        upload_pattern = r'    def upload_to_file_server\(self[^:]*\):.*?(?=\n    def [a-z_]|\nclass |\Z)'
        match = re.search(upload_pattern, content, re.DOTALL)
        if match:
            content = content.replace(match.group(0), new_upload_function.rstrip())
            print("  ✓ Replaced upload_to_file_server()")

        # 3. save_results 함수 교체
        save_pattern = r'    def save_results\(self[^:]*\):.*?(?=\n    def [a-z_]|\nclass |\Z)'
        match = re.search(save_pattern, content, re.DOTALL)
        if match:
            new_save = save_results_template.replace('_SUFFIX', suffix)
            content = content.replace(match.group(0), new_save.rstrip())
            print("  ✓ Replaced save_results()")

        # 파일 저장
        if content != original:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"[OK] {filename} updated")
        else:
            print(f"[NO CHANGE] {filename}")

    except Exception as e:
        print(f"[ERROR] {filename}: {e}")

print("\\n" + "="*60)
print("Done!")
print("="*60)

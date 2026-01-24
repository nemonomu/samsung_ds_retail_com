"""
BestBuy NULL 값 보완 도구
- DB에서 NULL 항목 조회
- 브라우저에서 URL 열기
- 사용자 입력으로 NULL 값 보완
- DB 업데이트 후 파일서버 업로드
"""

import pandas as pd
import pymysql
from sqlalchemy import create_engine, text
import webbrowser
import os
import zipfile
import hashlib
import paramiko
from datetime import datetime
import pytz

from config import DB_CONFIG_V2 as DB_CONFIG
from config import FILE_SERVER_CONFIG


class BestBuyNullFixer:
    def __init__(self):
        self.db_engine = None
        self.country_code = 'usa'
        self.korea_tz = pytz.timezone('Asia/Seoul')
        self.local_tz = pytz.timezone('America/New_York')
        self.setup_db_connection()

    def setup_db_connection(self):
        """DB 연결 설정"""
        try:
            connection_string = (
                f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
                f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            )
            self.db_engine = create_engine(connection_string)
            print("✅ DB 연결 성공")
        except Exception as e:
            print(f"❌ DB 연결 실패: {e}")
            self.db_engine = None

    def get_crawl_sessions(self):
        """크롤링 세션 목록 조회 (한국시간 기준, 시간 단위)"""
        try:
            query = """
            SELECT
                LEFT(kr_crawl_strdatetime, 10) as session_id,
                MIN(kr_crawl_datetime) as start_time,
                COUNT(*) as total_count,
                SUM(CASE WHEN retailprice IS NULL OR title IS NULL OR imageurl IS NULL THEN 1 ELSE 0 END) as null_count
            FROM bestbuy_price_crawl_tbl_usa_v2
            GROUP BY LEFT(kr_crawl_strdatetime, 10)
            ORDER BY session_id DESC
            LIMIT 10
            """
            df = pd.read_sql(query, self.db_engine)
            return df
        except Exception as e:
            print(f"❌ 세션 조회 실패: {e}")
            return pd.DataFrame()

    def get_null_records(self, session_id):
        """NULL 값이 있는 레코드 조회 (세션별)"""
        query = f"""
        SELECT *
        FROM bestbuy_price_crawl_tbl_usa_v2
        WHERE LEFT(kr_crawl_strdatetime, 10) = '{session_id}'
          AND (title IS NULL OR imageurl IS NULL OR retailprice IS NULL)
        ORDER BY kr_crawl_datetime DESC
        """

        try:
            df = pd.read_sql(query, self.db_engine)
            return df
        except Exception as e:
            print(f"❌ 조회 실패: {e}")
            return pd.DataFrame()

    def get_all_records(self, session_id):
        """해당 세션 전체 레코드 조회"""
        query = f"""
        SELECT *
        FROM bestbuy_price_crawl_tbl_usa_v2
        WHERE LEFT(kr_crawl_strdatetime, 10) = '{session_id}'
        ORDER BY kr_crawl_datetime DESC
        """

        try:
            df = pd.read_sql(query, self.db_engine)
            return df
        except Exception as e:
            print(f"❌ 조회 실패: {e}")
            return pd.DataFrame()

    def update_record(self, producturl, title=None, imageurl=None, retailprice=None):
        """레코드 업데이트"""
        try:
            updates = []
            params = {'producturl': producturl}

            if title is not None:
                updates.append("title = :title")
                params['title'] = title
            if imageurl is not None:
                updates.append("imageurl = :imageurl")
                params['imageurl'] = imageurl
            if retailprice is not None:
                updates.append("retailprice = :retailprice")
                params['retailprice'] = retailprice

            if not updates:
                return False

            query = text(f"""
            UPDATE bestbuy_price_crawl_tbl_usa_v2
            SET {', '.join(updates)}
            WHERE producturl = :producturl
            """)

            with self.db_engine.connect() as conn:
                conn.execute(query, params)
                conn.commit()

            return True
        except Exception as e:
            print(f"❌ 업데이트 실패: {e}")
            return False

    def open_url(self, url):
        """브라우저에서 URL 열기"""
        try:
            webbrowser.open(url)
        except Exception as e:
            print(f"브라우저 열기 실패: {e}")

    def upload_to_file_server(self, local_file_path, date_folder):
        """파일서버에 업로드"""
        try:
            transport = paramiko.Transport((FILE_SERVER_CONFIG['host'], FILE_SERVER_CONFIG['port']))
            transport.connect(
                username=FILE_SERVER_CONFIG['username'],
                password=FILE_SERVER_CONFIG['password']
            )
            sftp = paramiko.SFTPClient.from_transport(transport)

            country_dir = f"{FILE_SERVER_CONFIG['upload_path']}/{self.country_code}"
            try:
                sftp.stat(country_dir)
            except FileNotFoundError:
                sftp.mkdir(country_dir)

            date_dir = f"{country_dir}/{date_folder}"
            try:
                sftp.stat(date_dir)
            except FileNotFoundError:
                sftp.mkdir(date_dir)

            remote_filename = os.path.basename(local_file_path)
            remote_path = f"{date_dir}/{remote_filename}"

            sftp.put(local_file_path, remote_path)
            print(f"✅ 업로드 완료: {remote_path}")

            sftp.close()
            transport.close()
            return True
        except Exception as e:
            print(f"❌ 업로드 실패: {e}")
            return False

    def export_and_upload(self, session_id):
        """전체 데이터 내보내기 및 파일서버 업로드"""
        df = self.get_all_records(session_id)

        if df.empty:
            print("❌ 내보낼 데이터가 없습니다.")
            return False

        # 파일명 생성 (세션 ID 기준: YYYYMMDDHH, 한국시간)
        date_folder = session_id[:8]  # YYYYMMDD
        time_str = session_id[8:10] + "0000"  # HH -> HH0000
        base_filename = f"{date_folder}_{time_str}_usa_bestbuy"

        try:
            # CSV 생성
            csv_filename = f'{base_filename}.csv'
            df_csv = df.copy()
            df_csv.columns = df_csv.columns.str.upper()
            df_csv.to_csv(csv_filename, index=False, encoding='utf-8', lineterminator='\r\n')

            # ZIP 압축
            zip_filename = f'{base_filename}.zip'
            with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.write(csv_filename, arcname=csv_filename)

            # MD5 계산
            def calculate_md5(filename):
                md5 = hashlib.md5()
                with open(filename, 'rb') as f:
                    for chunk in iter(lambda: f.read(4096), b''):
                        md5.update(chunk)
                return md5.hexdigest()

            csv_md5 = calculate_md5(csv_filename)
            zip_md5 = calculate_md5(zip_filename)

            # MD5 파일 생성
            md5_filename = f'{base_filename}.md5'
            with open(md5_filename, 'w', encoding='utf-8') as f:
                f.write(f"{os.path.basename(zip_filename)} {zip_md5}\n")
                f.write(f"{os.path.basename(csv_filename)} {csv_md5}\n")

            # 업로드
            success = True
            if not self.upload_to_file_server(zip_filename, date_folder):
                success = False
            if not self.upload_to_file_server(md5_filename, date_folder):
                success = False

            # 임시 파일 삭제
            for temp_file in [csv_filename, zip_filename, md5_filename]:
                if os.path.exists(temp_file):
                    os.remove(temp_file)

            return success

        except Exception as e:
            print(f"❌ 내보내기 실패: {e}")
            return False

    def run_interactive(self):
        """대화형 NULL 값 보완"""
        print("\n" + "="*60)
        print("🔧 BestBuy NULL 값 보완 도구")
        print("="*60)

        # 크롤링 세션 목록 조회
        print("\n📊 최근 크롤링 세션 조회 중...")
        sessions = self.get_crawl_sessions()

        if sessions.empty:
            print("❌ 크롤링 세션이 없습니다.")
            return

        # 세션 목록 표시
        print("\n┌─────┬──────────────────────┬───────┬──────────┐")
        print("│ No. │ 크롤링 시간 (KST)     │ 전체  │ NULL    │")
        print("├─────┼──────────────────────┼───────┼──────────┤")
        for idx, row in sessions.iterrows():
            session_id = row['session_id']
            # YYYYMMDDHH -> YYYY-MM-DD HH시 형식
            formatted_time = f"{session_id[:4]}-{session_id[4:6]}-{session_id[6:8]} {session_id[8:10]}시"
            null_display = f"{int(row['null_count'])}개" if row['null_count'] > 0 else "없음"
            print(f"│ {idx+1:3} │ {formatted_time:20} │ {int(row['total_count']):5} │ {null_display:8} │")
        print("└─────┴──────────────────────┴───────┴──────────┘")

        # 세션 선택
        session_input = input(f"\n📌 세션 선택 (1-{len(sessions)}, 엔터=1): ").strip()
        if not session_input:
            selected_idx = 0
        else:
            try:
                selected_idx = int(session_input) - 1
                if selected_idx < 0 or selected_idx >= len(sessions):
                    print("❌ 잘못된 번호")
                    return
            except:
                print("❌ 잘못된 입력")
                return

        session_id = sessions.iloc[selected_idx]['session_id']
        print(f"\n📊 세션 {session_id} 데이터 조회 중...")

        # NULL 레코드 조회
        null_df = self.get_null_records(session_id)

        if null_df.empty:
            print("✅ NULL 값이 있는 항목이 없습니다!")

            # 파일서버 업로드 여부 확인
            upload = input("\n📤 파일서버에 업로드하시겠습니까? (y/n): ").strip().lower()
            if upload == 'y':
                self.export_and_upload(session_id)
            return

        print(f"\n⚠️ NULL 항목 {len(null_df)}개 발견\n")

        # 각 NULL 항목 처리
        for idx, row in null_df.iterrows():
            print(f"\n{'='*60}")
            print(f"[{idx + 1}/{len(null_df)}] {row['producturl']}")
            print(f"{'='*60}")

            # 현재 값 표시
            title_status = "✅ OK" if pd.notna(row['title']) else "❌ NULL"
            img_status = "✅ OK" if pd.notna(row['imageurl']) else "❌ NULL"
            price_status = "✅ OK" if pd.notna(row['retailprice']) else "❌ NULL"

            print(f"  - title: {title_status}")
            if pd.notna(row['title']):
                print(f"    → {row['title'][:50]}...")

            print(f"  - imageurl: {img_status}")
            if pd.notna(row['imageurl']):
                print(f"    → {row['imageurl'][:50]}...")

            print(f"  - retailprice: {price_status}")
            if pd.notna(row['retailprice']):
                print(f"    → ${row['retailprice']}")

            # 브라우저 열기 여부
            open_browser = input("\n🌐 브라우저에서 열기? (y/n/s=스킵): ").strip().lower()

            if open_browser == 's':
                print("⏭️ 스킵")
                continue

            if open_browser == 'y':
                self.open_url(row['producturl'])

            # NULL 값 입력
            new_title = None
            new_imageurl = None
            new_price = None

            if pd.isna(row['title']):
                title_input = input("📝 title 입력 (엔터=스킵): ").strip()
                if title_input:
                    new_title = title_input

            if pd.isna(row['imageurl']):
                img_input = input("🖼️ imageurl 입력 (엔터=스킵): ").strip()
                if img_input:
                    new_imageurl = img_input

            if pd.isna(row['retailprice']):
                price_input = input("💰 price 입력 (숫자만, 엔터=스킵): ").strip()
                if price_input:
                    try:
                        new_price = float(price_input.replace(',', '').replace('$', ''))
                    except:
                        print("⚠️ 잘못된 가격 형식, 스킵")

            # 업데이트
            if new_title or new_imageurl or new_price:
                if self.update_record(row['producturl'], new_title, new_imageurl, new_price):
                    print("✅ DB 업데이트 완료")
                else:
                    print("❌ DB 업데이트 실패")
            else:
                print("ℹ️ 변경 없음")

        # 최종 확인
        print("\n" + "="*60)
        print("📊 보완 작업 완료")
        print("="*60)

        # 남은 NULL 확인
        remaining_null = self.get_null_records(session_id)
        if not remaining_null.empty:
            print(f"⚠️ 아직 NULL 항목 {len(remaining_null)}개 남음")
        else:
            print("✅ 모든 NULL 값 보완 완료!")

        # 파일서버 업로드
        upload = input("\n📤 파일서버에 업로드하시겠습니까? (y/n): ").strip().lower()
        if upload == 'y':
            self.export_and_upload(session_id)

        print("\n👋 종료")


def main():
    fixer = BestBuyNullFixer()
    if fixer.db_engine:
        fixer.run_interactive()


if __name__ == "__main__":
    main()

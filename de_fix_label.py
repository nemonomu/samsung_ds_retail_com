"""
Amazon DE 라벨 오류 수정 도구
- DB에서 ships_from/sold_by에 'Versender/Verkäufer' 라벨이 저장된 레코드 조회
- 브라우저에서 URL 열어 실제 판매자 확인
- DB 업데이트 후 파일서버 재업로드
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


class DeLabelFixer:
    def __init__(self):
        self.db_engine = None
        self.country_code = 'de'
        self.korea_tz = pytz.timezone('Asia/Seoul')
        self.setup_db_connection()

    def setup_db_connection(self):
        """DB 연결 설정"""
        try:
            connection_string = (
                f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
                f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            )
            self.db_engine = create_engine(connection_string)
            print("DB 연결 성공")
        except Exception as e:
            print(f"DB 연결 실패: {e}")
            self.db_engine = None

    def get_label_error_sessions(self):
        """라벨 오류가 있는 세션 조회 (23일, 24일)"""
        try:
            query = """
            SELECT
                LEFT(kr_crawl_strdatetime, 10) as session_id,
                MIN(kr_crawl_datetime) as start_time,
                COUNT(DISTINCT producturl) as total_count,
                SUM(CASE WHEN ships_from LIKE '%%Versender%%' OR ships_from LIKE '%%Verkäufer%%'
                         OR sold_by LIKE '%%Versender%%' OR sold_by LIKE '%%Verkäufer%%' THEN 1 ELSE 0 END) as label_error_count
            FROM amazon_price_crawl_tbl_de_v2
            WHERE LEFT(kr_crawl_strdatetime, 8) IN ('20260123', '20260124')
            GROUP BY LEFT(kr_crawl_strdatetime, 10)
            HAVING label_error_count > 0
            ORDER BY session_id DESC
            """
            df = pd.read_sql(query, self.db_engine)
            return df
        except Exception as e:
            print(f"세션 조회 실패: {e}")
            return pd.DataFrame()

    def get_label_error_records(self, session_id):
        """라벨 오류 레코드 조회 (세션별, 중복 제외)"""
        query = f"""
        SELECT t1.*
        FROM amazon_price_crawl_tbl_de_v2 t1
        INNER JOIN (
            SELECT producturl, MAX(kr_crawl_datetime) as max_dt
            FROM amazon_price_crawl_tbl_de_v2
            WHERE LEFT(kr_crawl_strdatetime, 10) = '{session_id}'
            GROUP BY producturl
        ) t2 ON t1.producturl = t2.producturl AND t1.kr_crawl_datetime = t2.max_dt
        WHERE LEFT(t1.kr_crawl_strdatetime, 10) = '{session_id}'
          AND (t1.ships_from LIKE '%%Versender%%' OR t1.ships_from LIKE '%%Verkäufer%%'
               OR t1.sold_by LIKE '%%Versender%%' OR t1.sold_by LIKE '%%Verkäufer%%')
        ORDER BY t1.kr_crawl_datetime DESC
        """
        try:
            df = pd.read_sql(query, self.db_engine)
            return df
        except Exception as e:
            print(f"조회 실패: {e}")
            return pd.DataFrame()

    def get_all_records(self, session_id):
        """해당 세션 전체 레코드 조회 (중복 제외, 최신 레코드만)"""
        query = f"""
        SELECT t1.*
        FROM amazon_price_crawl_tbl_de_v2 t1
        INNER JOIN (
            SELECT producturl, MAX(kr_crawl_datetime) as max_dt
            FROM amazon_price_crawl_tbl_de_v2
            WHERE LEFT(kr_crawl_strdatetime, 10) = '{session_id}'
            GROUP BY producturl
        ) t2 ON t1.producturl = t2.producturl AND t1.kr_crawl_datetime = t2.max_dt
        WHERE LEFT(t1.kr_crawl_strdatetime, 10) = '{session_id}'
        ORDER BY t1.kr_crawl_datetime DESC
        """
        try:
            df = pd.read_sql(query, self.db_engine)
            return df
        except Exception as e:
            print(f"조회 실패: {e}")
            return pd.DataFrame()

    def update_record(self, producturl, session_id, ships_from=None, sold_by=None):
        """레코드 업데이트 (해당 세션의 모든 중복 레코드 포함)"""
        try:
            updates = []
            params = {'producturl': producturl, 'session_id': session_id}

            if ships_from is not None:
                updates.append("ships_from = :ships_from")
                params['ships_from'] = ships_from
            if sold_by is not None:
                updates.append("sold_by = :sold_by")
                params['sold_by'] = sold_by

            if not updates:
                return False

            query = text(f"""
            UPDATE amazon_price_crawl_tbl_de_v2
            SET {', '.join(updates)}
            WHERE producturl = :producturl
              AND LEFT(kr_crawl_strdatetime, 10) = :session_id
            """)

            with self.db_engine.connect() as conn:
                result = conn.execute(query, params)
                conn.commit()
                print(f"  -> {result.rowcount}개 레코드 업데이트됨")

            return True
        except Exception as e:
            print(f"업데이트 실패: {e}")
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
            print(f"업로드 완료: {remote_path}")

            sftp.close()
            transport.close()
            return True
        except Exception as e:
            print(f"업로드 실패: {e}")
            return False

    def export_and_upload(self, session_id, custom_folder=None, custom_datetime=None):
        """전체 데이터 내보내기 및 파일서버 업로드"""
        df = self.get_all_records(session_id)

        if df.empty:
            print("내보낼 데이터가 없습니다.")
            return False

        # 파일명 생성
        if custom_datetime:
            parts = custom_datetime.split('_')
            date_folder = custom_folder if custom_folder else parts[0]
            base_filename = f"{custom_datetime}_de_amazon"
        else:
            date_folder = custom_folder if custom_folder else session_id[:8]
            time_str = session_id[8:10] + "0000"
            base_filename = f"{session_id[:8]}_{time_str}_de_amazon"

        try:
            # 컬럼 순서 정의
            column_order = [
                'retailerid', 'country_code', 'ships_from', 'channel_name', 'channel',
                'retailersku', 'brand', 'brand_eng', 'form_factor',
                'segment_lv1', 'segment_lv2', 'segment_lv3', 'capacity', 'item',
                'retailprice', 'sold_by', 'imageurl', 'producturl',
                'crawl_datetime', 'crawl_strdatetime',
                'kr_crawl_datetime', 'kr_crawl_strdatetime',
                'title', 'vat'
            ]

            csv_filename = f'{base_filename}.csv'
            df_csv = df.copy()

            existing_columns = [col for col in column_order if col in df_csv.columns]
            df_csv = df_csv[existing_columns]

            df_csv.columns = df_csv.columns.str.upper()
            df_csv.to_csv(csv_filename, index=False, encoding='utf-8', lineterminator='\r\n')

            zip_filename = f'{base_filename}.zip'
            with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.write(csv_filename, arcname=csv_filename)

            def calculate_md5(filename):
                md5 = hashlib.md5()
                with open(filename, 'rb') as f:
                    for chunk in iter(lambda: f.read(4096), b''):
                        md5.update(chunk)
                return md5.hexdigest()

            csv_md5 = calculate_md5(csv_filename)
            zip_md5 = calculate_md5(zip_filename)

            md5_filename = f'{base_filename}.md5'
            with open(md5_filename, 'w', encoding='utf-8') as f:
                f.write(f"{os.path.basename(zip_filename)} {zip_md5}\n")
                f.write(f"{os.path.basename(csv_filename)} {csv_md5}\n")

            success = True
            if not self.upload_to_file_server(zip_filename, date_folder):
                success = False
            if not self.upload_to_file_server(md5_filename, date_folder):
                success = False

            for temp_file in [csv_filename, zip_filename, md5_filename]:
                if os.path.exists(temp_file):
                    os.remove(temp_file)

            return success

        except Exception as e:
            print(f"내보내기 실패: {e}")
            return False

    def run_interactive(self):
        """대화형 라벨 오류 수정"""
        print("\n" + "="*60)
        print("Amazon DE 라벨 오류 수정 도구")
        print("(23일, 24일 Versender/Verkaufer 오류 수정)")
        print("="*60)

        print("\n라벨 오류 세션 조회 중...")
        sessions = self.get_label_error_sessions()

        if sessions.empty:
            print("라벨 오류가 있는 세션이 없습니다.")
            return

        print("\n+-----+----------------------+-------+----------+")
        print("| No. | 크롤링 시간 (KST)    | 전체  | 오류     |")
        print("+-----+----------------------+-------+----------+")
        for idx, row in sessions.iterrows():
            session_id = row['session_id']
            formatted_time = f"{session_id[:4]}-{session_id[4:6]}-{session_id[6:8]} {session_id[8:10]}시"
            error_display = f"{int(row['label_error_count'])}개"
            print(f"| {idx+1:3} | {formatted_time:20} | {int(row['total_count']):5} | {error_display:8} |")
        print("+-----+----------------------+-------+----------+")

        session_input = input(f"\n세션 선택 (1-{len(sessions)}, 엔터=1, a=전체): ").strip()

        if session_input.lower() == 'a':
            # 전체 세션 처리
            for idx, row in sessions.iterrows():
                session_id = row['session_id']
                print(f"\n{'='*60}")
                print(f"세션 {session_id} 처리 중...")
                print(f"{'='*60}")
                self.process_session(session_id)
        else:
            if not session_input:
                selected_idx = 0
            else:
                try:
                    selected_idx = int(session_input) - 1
                    if selected_idx < 0 or selected_idx >= len(sessions):
                        print("잘못된 번호")
                        return
                except:
                    print("잘못된 입력")
                    return

            session_id = sessions.iloc[selected_idx]['session_id']
            self.process_session(session_id)

        print("\n종료")

    def process_session(self, session_id):
        """세션 처리"""
        print(f"\n세션 {session_id} 데이터 조회 중...")

        error_df = self.get_label_error_records(session_id)

        if error_df.empty:
            print("라벨 오류 항목이 없습니다!")
            return

        print(f"\n라벨 오류 항목 {len(error_df)}개 발견\n")

        # 일괄 처리 옵션
        batch_mode = input("일괄 처리? (y=모두 동일값, n=개별입력): ").strip().lower()

        if batch_mode == 'y':
            print("\n통합 라벨(Versender/Verkaufer)인 경우 ships_from = sold_by 입니다.")
            common_value = input("공통 판매자 이름 입력 (예: Amazon.de): ").strip()

            if common_value:
                for idx, row in error_df.iterrows():
                    print(f"[{idx+1}/{len(error_df)}] {row['producturl'][:60]}...")
                    self.update_record(row['producturl'], session_id, common_value, common_value)
                print(f"\n{len(error_df)}개 레코드 일괄 업데이트 완료")
        else:
            # 개별 처리
            for idx, row in error_df.iterrows():
                print(f"\n{'='*60}")
                print(f"[{idx + 1}/{len(error_df)}] {row['producturl']}")
                print(f"{'='*60}")

                print(f"  - ships_from: {row['ships_from']}")
                print(f"  - sold_by: {row['sold_by']}")

                open_browser = input("\n브라우저에서 열기? (y/n/s=스킵): ").strip().lower()

                if open_browser == 's':
                    print("스킵")
                    continue

                if open_browser == 'y':
                    self.open_url(row['producturl'])

                new_ships_from = input("ships_from 입력 (엔터=변경없음): ").strip()
                new_sold_by = input("sold_by 입력 (엔터=변경없음): ").strip()

                if new_ships_from or new_sold_by:
                    ships_from_val = new_ships_from if new_ships_from else None
                    sold_by_val = new_sold_by if new_sold_by else None
                    if self.update_record(row['producturl'], session_id, ships_from_val, sold_by_val):
                        print("DB 업데이트 완료")
                    else:
                        print("DB 업데이트 실패")
                else:
                    print("변경 없음")

        # 남은 오류 확인
        remaining = self.get_label_error_records(session_id)
        if not remaining.empty:
            print(f"\n아직 라벨 오류 {len(remaining)}개 남음")
        else:
            print("\n모든 라벨 오류 수정 완료!")

        # 파일서버 업로드
        upload = input("\n파일서버에 업로드하시겠습니까? (y/n): ").strip().lower()
        if upload == 'y':
            self.prompt_and_upload(session_id)

    def prompt_and_upload(self, session_id):
        """업로드 경로와 파일명 입력받아 업로드"""
        default_folder = session_id[:8]
        default_datetime = f"{session_id[:8]}_{session_id[8:10]}0000"

        print(f"\n업로드 설정")
        print(f"   기본 폴더: {default_folder}")
        print(f"   기본 파일명: {default_datetime}_de_amazon.zip")

        folder_input = input(f"\n업로드 폴더 (엔터={default_folder}): ").strip()
        custom_folder = folder_input if folder_input else None

        datetime_input = input(f"파일명 날짜시간 (예: 20260123_215429, 엔터={default_datetime}): ").strip()
        custom_datetime = datetime_input if datetime_input else None

        final_folder = custom_folder if custom_folder else default_folder
        final_filename = f"{custom_datetime}_de_amazon" if custom_datetime else f"{default_datetime}_de_amazon"
        print(f"\n업로드 정보:")
        print(f"   경로: /home/ftpuser/uploads/de/{final_folder}/")
        print(f"   파일: {final_filename}.zip")

        confirm = input("\n진행하시겠습니까? (y/n): ").strip().lower()
        if confirm == 'y':
            self.export_and_upload(session_id, custom_folder, custom_datetime)


def main():
    fixer = DeLabelFixer()
    if fixer.db_engine:
        fixer.run_interactive()


if __name__ == "__main__":
    main()

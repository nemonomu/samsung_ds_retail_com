"""
Title/ImageURL NULL 복구 스크립트
- fr_v2, uk_v2, currys_v2 크롤링 결과 중 title 또는 imageurl이 NULL인 레코드 복구
- DB UPDATE + 파일서버 재업로드

사용법:
    python recovery.py
"""

import os
import sys
import logging
import hashlib
import zipfile
from datetime import datetime
import pytz
import pandas as pd
from sqlalchemy import create_engine, text
import paramiko

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# 설정 임포트
from config import DB_CONFIG_V2 as DB_CONFIG
from config import FILE_SERVER_CONFIG

# 대상별 설정
TARGET_CONFIG = {
    'fr': {
        'name': '프랑스 Amazon',
        'table': 'amazon_price_crawl_tbl_fr_v2',
        'country_code': 'fr',
        'file_prefix': 'fr_amazon',
        'local_tz': 'Europe/Paris',
        'scraper_module': 'fr_v2',
        'scraper_class': 'AmazonFRScraper'
    },
    'gb': {
        'name': '영국 Amazon',
        'table': 'amazon_price_crawl_tbl_uk_v2',
        'country_code': 'gb',
        'file_prefix': 'gb_amazon',
        'local_tz': 'Europe/London',
        'scraper_module': 'uk_v2',
        'scraper_class': 'AmazonUKScraper'
    },
    'currys': {
        'name': '영국 Currys',
        'table': 'currys_price_crawl_tbl_gb_v2',
        'country_code': 'gb',
        'file_prefix': 'gb_currys',
        'local_tz': 'Europe/London',
        'scraper_module': 'currys_v2',
        'scraper_class': 'CurrysScraper'
    }
}


class RecoveryManager:
    def __init__(self):
        self.db_engine = None
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
            logger.info("DB 연결 성공")
        except Exception as e:
            logger.error(f"DB 연결 실패: {e}")
            self.db_engine = None

    def get_sessions(self, target):
        """세션 목록 조회 (날짜+시작시간 기준)"""
        config = TARGET_CONFIG[target]
        table = config['table']

        # 세션 구분: 같은 날짜 내에서 30분 이상 간격이 있으면 다른 세션으로 간주
        # 먼저 각 날짜의 최소 시간(세션 시작)을 기준으로 그룹핑
        query = f"""
        SELECT
            DATE(kr_crawl_datetime) as crawl_date,
            DATE_FORMAT(MIN(kr_crawl_datetime), '%%Y-%%m-%%d %%H:%%i:%%s') as session_start,
            COUNT(*) as total_count,
            SUM(CASE WHEN title IS NULL THEN 1 ELSE 0 END) as title_null_count,
            SUM(CASE WHEN imageurl IS NULL OR imageurl = '' THEN 1 ELSE 0 END) as imageurl_null_count
        FROM {table}
        GROUP BY DATE(kr_crawl_datetime), HOUR(kr_crawl_datetime)
        ORDER BY crawl_date DESC, session_start DESC
        LIMIT 20
        """

        try:
            df = pd.read_sql(query, self.db_engine)
            return df
        except Exception as e:
            logger.error(f"세션 조회 실패: {e}")
            return None

    def get_null_records(self, target, session_start):
        """해당 세션의 NULL 레코드 조회"""
        config = TARGET_CONFIG[target]
        table = config['table']

        # 세션 시작 시간 기준으로 같은 세션의 레코드 조회 (같은 날짜 + 같은 시간대)
        query = f"""
        SELECT *
        FROM {table}
        WHERE DATE(kr_crawl_datetime) = DATE(:session_start)
          AND HOUR(kr_crawl_datetime) = HOUR(:session_start)
          AND (title IS NULL OR imageurl IS NULL OR imageurl = '')
        """

        try:
            df = pd.read_sql(query, self.db_engine, params={'session_start': session_start})
            return df
        except Exception as e:
            logger.error(f"NULL 레코드 조회 실패: {e}")
            return None

    def get_session_all_records(self, target, session_start):
        """해당 세션의 전체 레코드 조회 (파일 생성용)"""
        config = TARGET_CONFIG[target]
        table = config['table']

        query = f"""
        SELECT *
        FROM {table}
        WHERE DATE(kr_crawl_datetime) = DATE(:session_start)
          AND HOUR(kr_crawl_datetime) = HOUR(:session_start)
        """

        try:
            df = pd.read_sql(query, self.db_engine, params={'session_start': session_start})
            return df
        except Exception as e:
            logger.error(f"세션 레코드 조회 실패: {e}")
            return None

    def load_scraper(self, target):
        """해당 대상의 스크래퍼 로드 및 브라우저 초기화"""
        config = TARGET_CONFIG[target]
        module_name = config['scraper_module']
        class_name = config['scraper_class']

        try:
            module = __import__(module_name)
            scraper_class = getattr(module, class_name)
            scraper = scraper_class()
            logger.info(f"{class_name} 로드 완료")

            # 브라우저 초기화
            logger.info("브라우저 초기화 중...")
            scraper.setup_driver()
            logger.info("브라우저 초기화 완료")

            return scraper
        except Exception as e:
            logger.error(f"스크래퍼 로드 실패: {e}")
            import traceback
            traceback.print_exc()
            return None

    def recrawl_url(self, scraper, url, row_data):
        """단일 URL 재크롤링"""
        try:
            # row_data를 dict로 변환 (Series인 경우)
            if isinstance(row_data, pd.Series):
                row_dict = row_data.to_dict()
            else:
                row_dict = row_data

            # URL 키 이름 맞추기
            row_dict['url'] = url

            result = scraper.extract_product_info(url, row_dict, retry_count=0, max_retries=1)
            return result
        except Exception as e:
            logger.error(f"재크롤링 실패 ({url}): {e}")
            return None

    def update_db_record(self, target, original_kr_crawl_datetime, new_data):
        """DB 레코드 UPDATE (7개 컬럼)

        Args:
            target: 대상 (fr/gb/currys)
            original_kr_crawl_datetime: 원본 레코드의 kr_crawl_datetime 값 (정확한 매칭용)
            new_data: 새로 크롤링한 데이터
        """
        config = TARGET_CONFIG[target]
        table = config['table']
        local_tz = pytz.timezone(config['local_tz'])

        # 현재 시간 (복구 시점)
        now_korea = datetime.now(self.korea_tz)
        now_local = datetime.now(local_tz)

        # datetime 컬럼 값 생성
        crawl_dt = now_local.strftime("%Y-%m-%dT%H:%M:%S")
        tz_offset = now_local.strftime("%z")
        tz_formatted = f"{tz_offset[:3]}:{tz_offset[3:]}" if tz_offset else "+00:00"
        crawl_datetime_iso = f"{crawl_dt}{tz_formatted}"

        crawl_strdatetime = now_local.strftime('%Y%m%d%H%M%S') + f"{now_local.microsecond:06d}"[:4]
        kr_crawl_datetime = now_korea.strftime('%Y-%m-%d %H:%M:%S')
        kr_crawl_strdatetime = now_korea.strftime('%Y%m%d%H%M%S') + f"{now_korea.microsecond:06d}"[:4]

        # UPDATE 쿼리 (producturl + 원본 kr_crawl_datetime으로 정확히 1개 레코드 특정)
        update_query = f"""
        UPDATE {table}
        SET title = :title,
            imageurl = :imageurl,
            retailprice = :retailprice,
            crawl_datetime = :crawl_datetime,
            crawl_strdatetime = :crawl_strdatetime,
            kr_crawl_datetime = :kr_crawl_datetime,
            kr_crawl_strdatetime = :kr_crawl_strdatetime
        WHERE producturl = :producturl
          AND kr_crawl_datetime = :original_kr_crawl_datetime
        """

        try:
            with self.db_engine.connect() as conn:
                result = conn.execute(text(update_query), {
                    'title': new_data.get('title'),
                    'imageurl': new_data.get('imageurl'),
                    'retailprice': new_data.get('retailprice'),
                    'crawl_datetime': crawl_datetime_iso,
                    'crawl_strdatetime': crawl_strdatetime,
                    'kr_crawl_datetime': kr_crawl_datetime,
                    'kr_crawl_strdatetime': kr_crawl_strdatetime,
                    'producturl': new_data.get('producturl'),
                    'original_kr_crawl_datetime': original_kr_crawl_datetime
                })
                conn.commit()
                return result.rowcount > 0
        except Exception as e:
            logger.error(f"DB UPDATE 실패: {e}")
            return False

    def upload_to_file_server(self, local_file_path, country_code, date_folder):
        """파일서버 업로드"""
        try:
            transport = paramiko.Transport((FILE_SERVER_CONFIG['host'], FILE_SERVER_CONFIG['port']))
            transport.connect(
                username=FILE_SERVER_CONFIG['username'],
                password=FILE_SERVER_CONFIG['password']
            )
            sftp = paramiko.SFTPClient.from_transport(transport)

            # 국가별 디렉토리 경로
            country_dir = f"{FILE_SERVER_CONFIG['upload_path']}/{country_code}"

            # 국가 디렉토리가 없으면 생성
            try:
                sftp.stat(country_dir)
            except FileNotFoundError:
                logger.info(f"국가 디렉토리 생성: {country_dir}")
                sftp.mkdir(country_dir)

            # 날짜별 디렉토리 경로
            date_dir = f"{country_dir}/{date_folder}"

            # 날짜 디렉토리가 없으면 생성
            try:
                sftp.stat(date_dir)
            except FileNotFoundError:
                logger.info(f"날짜 디렉토리 생성: {date_dir}")
                sftp.mkdir(date_dir)

            # 업로드 경로
            remote_filename = os.path.basename(local_file_path)
            remote_path = f"{date_dir}/{remote_filename}"

            # 파일 업로드
            sftp.put(local_file_path, remote_path)
            logger.info(f"파일서버 업로드 완료: {remote_path}")

            sftp.close()
            transport.close()
            return True
        except Exception as e:
            logger.error(f"파일서버 업로드 실패: {e}")
            return False

    def generate_and_upload_file(self, target, df, session_start):
        """CSV/ZIP/MD5 생성 및 파일서버 업로드"""
        config = TARGET_CONFIG[target]

        # 복구 시점 시간 (파일명용)
        now = datetime.now(self.korea_tz)
        time_str = now.strftime('%H%M%S')

        # 원본 세션 날짜 (폴더 및 파일명용) - session_start에서 추출
        if isinstance(session_start, str):
            session_date = datetime.strptime(session_start[:10], '%Y-%m-%d')
        elif isinstance(session_start, pd.Timestamp):
            session_date = session_start.to_pydatetime()
        else:
            session_date = session_start
        date_str = session_date.strftime('%Y%m%d')

        base_filename = f"{date_str}_{time_str}_{config['file_prefix']}"

        try:
            # 1. CSV 파일 생성
            csv_filename = f'{base_filename}.csv'
            df_copy = df.copy()
            df_copy.columns = df_copy.columns.str.upper()
            df_copy.to_csv(csv_filename, index=False, encoding='utf-8', lineterminator='\r\n')
            logger.info(f"CSV 생성: {csv_filename}")

            # 2. ZIP 압축
            zip_filename = f'{base_filename}.zip'
            with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.write(csv_filename, arcname=csv_filename)
            logger.info(f"ZIP 생성: {zip_filename}")

            # 3. MD5 계산
            def calculate_md5(filename):
                md5 = hashlib.md5()
                with open(filename, 'rb') as f:
                    for chunk in iter(lambda: f.read(4096), b''):
                        md5.update(chunk)
                return md5.hexdigest()

            csv_md5 = calculate_md5(csv_filename)
            zip_md5 = calculate_md5(zip_filename)

            # 4. MD5 파일 생성
            md5_filename = f'{base_filename}.md5'
            with open(md5_filename, 'w', encoding='utf-8') as f:
                f.write(f"{os.path.basename(zip_filename)} {zip_md5}\n")
                f.write(f"{os.path.basename(csv_filename)} {csv_md5}\n")
            logger.info(f"MD5 생성: {md5_filename}")

            # 5. 파일서버 업로드
            upload_success = True
            if not self.upload_to_file_server(zip_filename, config['country_code'], date_str):
                upload_success = False
            if not self.upload_to_file_server(md5_filename, config['country_code'], date_str):
                upload_success = False

            # 6. 로컬 임시 파일 삭제
            for temp_file in [csv_filename, zip_filename, md5_filename]:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
            logger.info("임시 파일 삭제 완료")

            return upload_success
        except Exception as e:
            logger.error(f"파일 생성/업로드 실패: {e}")
            return False

    def run_recovery(self, target, session_start):
        """복구 실행"""
        config = TARGET_CONFIG[target]
        logger.info(f"\n{'='*60}")
        logger.info(f"복구 시작: {config['name']}")
        logger.info(f"세션: {session_start}")
        logger.info(f"{'='*60}")

        # 1. NULL 레코드 조회
        null_records = self.get_null_records(target, session_start)
        if null_records is None or null_records.empty:
            logger.info("복구 대상 레코드가 없습니다.")
            return True

        # currys 예외: title null <= 3개면 복구 불필요
        if target == 'currys':
            title_null_count = null_records['title'].isna().sum()
            if title_null_count <= 3:
                logger.info(f"currys: title null {title_null_count}개 (3개 이하) - 복구 불필요")
                return True

        logger.info(f"복구 대상: {len(null_records)}개")

        # 2. 스크래퍼 로드
        scraper = self.load_scraper(target)
        if scraper is None:
            logger.error("스크래퍼 로드 실패")
            return False

        # 3. 각 URL 재크롤링 및 DB UPDATE
        success_count = 0
        fail_count = 0

        try:
            for i, (idx, row) in enumerate(null_records.iterrows()):
                url = row['producturl']
                original_kr_crawl_datetime = row['kr_crawl_datetime']  # 원본 시간 저장
                logger.info(f"\n[{i+1}/{len(null_records)}] 재크롤링: {url[:60]}...")

                result = self.recrawl_url(scraper, url, row)

                if result and result.get('title') is not None:
                    # DB UPDATE (원본 kr_crawl_datetime으로 정확히 매칭)
                    result['producturl'] = url
                    if self.update_db_record(target, original_kr_crawl_datetime, result):
                        logger.info(f"  -> 성공: title={result.get('title', '')[:30]}")
                        success_count += 1
                    else:
                        logger.warning(f"  -> DB UPDATE 실패")
                        fail_count += 1
                else:
                    logger.warning(f"  -> 재크롤링 실패 (여전히 NULL)")
                    fail_count += 1

        except Exception as e:
            logger.error(f"복구 중 오류 발생: {e}")
            import traceback
            traceback.print_exc()

        finally:
            # 4. 브라우저 종료 (항상 실행)
            if scraper and scraper.driver:
                try:
                    scraper.driver.quit()
                    logger.info("브라우저 종료")
                except Exception:
                    pass

        # 5. 결과 요약
        logger.info(f"\n{'='*60}")
        logger.info(f"복구 완료: 성공 {success_count}개, 실패 {fail_count}개")
        logger.info(f"{'='*60}")

        # 6. 파일서버 업로드 (전체 세션 데이터)
        if success_count > 0:
            logger.info("\n파일서버 업로드 시작...")
            all_records = self.get_session_all_records(target, session_start)
            if all_records is not None and not all_records.empty:
                if self.generate_and_upload_file(target, all_records, session_start):
                    logger.info("파일서버 업로드 완료")
                else:
                    logger.error("파일서버 업로드 실패")

        return success_count > 0 or fail_count == 0


def select_target():
    """대상 선택"""
    print("\n===== 복구 대상 선택 =====")
    print("1. fr (프랑스 Amazon)")
    print("2. gb (영국 Amazon)")
    print("3. currys (영국 Currys)")
    print("0. 종료")

    while True:
        try:
            choice = input("\n선택: ").strip()
            if choice == '0':
                return None
            elif choice == '1':
                return 'fr'
            elif choice == '2':
                return 'gb'
            elif choice == '3':
                return 'currys'
            else:
                print("올바른 번호를 입력하세요.")
        except KeyboardInterrupt:
            return None


def select_session(manager, target):
    """세션 선택"""
    config = TARGET_CONFIG[target]
    sessions = manager.get_sessions(target)

    if sessions is None or sessions.empty:
        print("조회된 세션이 없습니다.")
        return None

    print(f"\n===== {config['name']} 세션 목록 =====")
    print(f"{'번호':^4} | {'시작시간 (KST)':<20} | {'총개수':>6} | {'title NULL':>10} | {'imageurl NULL':>13}")
    print("-" * 70)

    for idx, row in sessions.iterrows():
        session_start = row['session_start']
        total = row['total_count']
        title_null = row['title_null_count']
        img_null = row['imageurl_null_count']

        # currys: title null <= 3 표시
        note = ""
        if target == 'currys' and title_null <= 3:
            note = " (복구불필요)"

        print(f"{idx+1:^4} | {str(session_start):<20} | {total:>6} | {title_null:>10} | {img_null:>13}{note}")

    if target == 'currys':
        print("\n* currys: title NULL <= 3개 세션은 복구 대상 아님")

    print("\n0. 뒤로가기")

    while True:
        try:
            choice = input("\n복구할 세션 번호: ").strip()
            if choice == '0':
                return None

            idx = int(choice) - 1
            if 0 <= idx < len(sessions):
                return sessions.iloc[idx]['session_start']
            else:
                print("올바른 번호를 입력하세요.")
        except ValueError:
            print("숫자를 입력하세요.")
        except KeyboardInterrupt:
            return None


def main():
    print("\n" + "="*60)
    print("  Title/ImageURL NULL 복구 스크립트")
    print("="*60)

    manager = RecoveryManager()

    if manager.db_engine is None:
        print("DB 연결 실패. 프로그램을 종료합니다.")
        return

    while True:
        # 1. 대상 선택
        target = select_target()
        if target is None:
            print("\n프로그램을 종료합니다.")
            break

        # 2. 세션 선택
        session_start = select_session(manager, target)
        if session_start is None:
            continue

        # 3. 확인
        print(f"\n선택한 세션: {session_start}")
        confirm = input("복구를 진행하시겠습니까? (y/n): ").strip().lower()

        if confirm == 'y':
            # 4. 복구 실행
            manager.run_recovery(target, session_start)

            # 계속할지 확인
            cont = input("\n다른 세션을 복구하시겠습니까? (y/n): ").strip().lower()
            if cont != 'y':
                print("\n프로그램을 종료합니다.")
                break
        else:
            print("복구가 취소되었습니다.")


if __name__ == "__main__":
    main()

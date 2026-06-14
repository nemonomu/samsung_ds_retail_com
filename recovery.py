"""
Title/ImageURL/Price NULL 복구 스크립트
- fr_v2, uk_v2, currys_v2, it_v2, de_v2, bestbuy_v2, mediamarkt_v2, xkom_v2, usa_v2, nl_amazon, in_v2 크롤링 결과 중 title, imageurl, retailprice가 NULL인 레코드 복구
- DB UPDATE + 파일서버 재업로드

사용법:
    python recovery.py
"""

import os
import sys
import logging
import hashlib
import zipfile
import shutil
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
from alert_monitor import monitor_and_alert

# 대상별 설정
TARGET_CONFIG = {
    'fr': {
        'name': '프랑스 Amazon',
        'table': 'amazon_price_crawl_tbl_fr_v2',
        'country_code': 'fr',
        'file_prefix': 'fr_amazon',
        'local_tz': 'Europe/Paris',
        'scraper_module': 'fr_v2',
        'scraper_class': 'AmazonFRScraper',
        'tracking_country': 'fr',
        'tracking_mall_name': 'amazon',
        'alert_code': 'fr'
    },
    'gb': {
        'name': '영국 Amazon',
        'table': 'amazon_price_crawl_tbl_uk_v2',
        'country_code': 'gb',
        'file_prefix': 'gb_amazon',
        'local_tz': 'Europe/London',
        'scraper_module': 'uk_v2',
        'scraper_class': 'AmazonUKScraper',
        'tracking_country': 'gb',
        'tracking_mall_name': 'amazon',
        'alert_code': 'gb'
    },
    'currys': {
        'name': '영국 Currys',
        'table': 'currys_price_crawl_tbl_gb_v2',
        'country_code': 'gb',
        'file_prefix': 'gb_currys',
        'local_tz': 'Europe/London',
        'scraper_module': 'currys_v2',
        'scraper_class': 'CurrysScraper',
        'tracking_country': 'gb',
        'tracking_mall_name': 'currys',
        'alert_code': 'gb_currys'
    },
    'it': {
        'name': '이탈리아 Amazon',
        'table': 'amazon_price_crawl_tbl_it_v2',
        'country_code': 'it',
        'file_prefix': 'it_amazon',
        'local_tz': 'Europe/Rome',
        'scraper_module': 'it_v2',
        'scraper_class': 'AmazonITScraper',
        'tracking_country': 'it',
        'tracking_mall_name': 'amazon',
        'alert_code': 'it'
    },
    'de': {
        'name': '독일 Amazon',
        'table': 'amazon_price_crawl_tbl_de_v2',
        'country_code': 'de',
        'file_prefix': 'de_amazon',
        'local_tz': 'Europe/Berlin',
        'scraper_module': 'de_v2',
        'scraper_class': 'AmazonDEScraper',
        'tracking_country': 'de',
        'tracking_mall_name': 'amazon',
        'alert_code': 'de'
    },
    'bestbuy': {
        'name': '미국 BestBuy',
        'table': 'bestbuy_price_crawl_tbl_usa_v2',
        'country_code': 'usa',
        'file_prefix': 'usa_bestbuy',
        'local_tz': 'America/New_York',
        'scraper_module': 'bestbuy_v2',
        'scraper_class': 'BestBuyScraper',
        'tracking_country': 'usa',
        'tracking_mall_name': 'bestbuy',
        'alert_code': 'usa_bestbuy'
    },
    'es': {
        'name': '스페인 Amazon',
        'table': 'amazon_price_crawl_tbl_es_v2',
        'country_code': 'es',
        'file_prefix': 'es_amazon',
        'local_tz': 'Europe/Madrid',
        'scraper_module': 'es_v2',
        'scraper_class': 'AmazonScraper',
        'scraper_kwargs': {'country_code': 'es'},
        'tracking_country': 'es',
        'tracking_mall_name': 'amazon',
        'alert_code': 'es'
    },
    'mediamarkt': {
        'name': '독일 MediaMarkt',
        'table': 'mediamarkt_price_crawl_tbl_de_v2',
        'country_code': 'de',
        'file_prefix': 'de_mediamarkt',
        'local_tz': 'Europe/Berlin',
        'scraper_module': 'mediamarkt_v2',
        'scraper_class': 'MediaMarktInfiniteScraper',
        'tracking_country': 'de',
        'tracking_mall_name': 'mediamarkt',
        'alert_code': 'de_mediamarkt'
    },
    'xkom': {
        'name': '폴란드 X-Kom',
        'table': 'xkom_price_crawl_tbl_pl_v2',
        'country_code': 'pl',
        'file_prefix': 'pl_xkom',
        'local_tz': 'Europe/Warsaw',
        'scraper_module': 'xkom_v2',
        'scraper_class': 'XKomScraper',
        'needs_manual_check': True,
        'tracking_country': 'pl',
        'tracking_mall_name': 'x-kom',
        'alert_code': 'pl_xkom'
    },
    'usa': {
        'name': '미국 Amazon',
        'table': 'amazon_price_crawl_tbl_usa_v2',
        'country_code': 'usa',
        'file_prefix': 'usa_amazon',
        'local_tz': 'America/New_York',
        'scraper_module': 'usa_v2',
        'scraper_class': 'AmazonScraper',
        'scraper_kwargs': {'country_code': 'usa'},
        'tracking_country': 'usa',
        'tracking_mall_name': 'amazon',
        'alert_code': 'usa'
    },
    'nl': {
        'name': '네덜란드 Amazon',
        'table': 'amazon_price_crawl_tbl_nl',
        'country_code': 'nl',
        'file_prefix': 'nl_amazon',
        'local_tz': 'Europe/Amsterdam',
        'scraper_module': 'nl_amazon',
        'scraper_class': 'AmazonNLScraper',
        'tracking_country': 'nl',
        'tracking_mall_name': 'amazon',
        'alert_code': 'nl'
    },
    'danawa': {
        'name': '한국 다나와',
        'table': 'danawa_price_crawl_tbl_kr_v2',
        'country_code': 'kr',
        'file_prefix': 'kr_danawa',
        'local_tz': 'Asia/Seoul',
        'scraper_module': 'danawa_v2',
        'scraper_class': 'DanawaScraper',
        'tracking_country': 'kr',
        'tracking_mall_name': 'danawa',
        'alert_code': 'kr_danawa'
    },
    'in': {
        'name': '인도 Amazon',
        'table': 'amazon_price_crawl_tbl_ind_v2',
        'country_code': 'in',
        'file_prefix': 'in_amazon',
        'local_tz': 'Asia/Kolkata',
        'scraper_module': 'in_v2',
        'scraper_class': 'AmazonIndiaScraper',
        'tracking_country': 'in',
        'tracking_mall_name': 'amazon',
        'alert_code': 'in'
    },
    'jp': {
        'name': '일본 Amazon',
        'table': 'amazon_price_crawl_tbl_jp_v2',
        'country_code': 'jp',
        'file_prefix': 'jp_amazon',
        'local_tz': 'Asia/Tokyo',
        'scraper_module': 'jp_v2',
        'scraper_class': 'AmazonScraper',
        'scraper_kwargs': {'country_code': 'jp'},
        'tracking_country': 'jp',
        'tracking_mall_name': 'amazon',
        'alert_code': 'jp'
    },
    'au': {
        'name': '호주 Centrecom',
        'table': 'centrecom_price_crawl_tbl_au',
        'country_code': 'au',
        'file_prefix': 'au_centrecom',
        'local_tz': 'Australia/Sydney',
        'scraper_module': 'centrecom',
        'scraper_class': 'CentrecomScraper',
        'tracking_country': 'au',
        'tracking_mall_name': 'centrecom',
        'alert_code': 'au_centrecom'
    },
    'coolblue': {
        'name': '네덜란드 Coolblue',
        'table': 'coolblue_price_crawl_tbl_nl_v2',
        'country_code': 'nl',
        'file_prefix': 'nl_coolblue',
        'local_tz': 'Europe/Amsterdam',
        'scraper_module': 'coolblue_nl_v2',
        'scraper_class': 'CoolblueScraper',
        'tracking_country': 'nl',
        'tracking_mall_name': 'coolblue',
        'alert_code': 'nl_coolblue'
    },
    'fnac': {
        'name': '프랑스 Fnac',
        'table': 'fnac_price_crawl_tbl_fr',
        'country_code': 'fr',
        'file_prefix': 'fr_fnac',
        'local_tz': 'Europe/Paris',
        'scraper_module': 'fnac_v2',
        'scraper_class': 'FnacScraperV2',
        'tracking_country': 'fr',
        'tracking_mall_name': 'fnac',
        'alert_code': 'fr_fnac'
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

        # danawa: retailprice = 0도 복구 대상
        price_null_condition = "retailprice IS NULL OR retailprice = 0" if target == 'danawa' else "retailprice IS NULL"

        # 세션 구분: 날짜 + 시간대별로 그룹화
        query = f"""
        SELECT
            DATE(kr_crawl_datetime) as crawl_date,
            DATE_FORMAT(MIN(kr_crawl_datetime), '%%Y-%%m-%%d %%H:%%i:%%s') as session_start,
            COUNT(*) as total_count,
            SUM(CASE WHEN title IS NULL THEN 1 ELSE 0 END) as title_null_count,
            SUM(CASE WHEN imageurl IS NULL OR imageurl = '' THEN 1 ELSE 0 END) as imageurl_null_count,
            SUM(CASE WHEN {price_null_condition} THEN 1 ELSE 0 END) as price_null_count
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
        """해당 세션의 NULL 레코드 조회 (session_start가 리스트면 여러 세션 합침)"""
        config = TARGET_CONFIG[target]
        table = config['table']

        # danawa: retailprice = 0도 복구 대상
        if target == 'danawa':
            null_condition = "(title IS NULL OR imageurl IS NULL OR imageurl = '' OR retailprice IS NULL OR retailprice = 0)"
        else:
            null_condition = "(title IS NULL OR imageurl IS NULL OR imageurl = '' OR retailprice IS NULL)"

        # session_start가 리스트면 여러 세션 합쳐서 조회
        session_list = session_start if isinstance(session_start, list) else [session_start]

        try:
            dfs = []
            for ss in session_list:
                query = f"""
                SELECT *
                FROM {table}
                WHERE DATE(kr_crawl_datetime) = DATE(:session_start)
                  AND HOUR(kr_crawl_datetime) = HOUR(:session_start)
                  AND {null_condition}
                """
                df = pd.read_sql(text(query), self.db_engine, params={'session_start': ss})
                dfs.append(df)
            combined = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
            combined = combined.drop_duplicates(subset=['producturl'], keep='last')
            return combined
        except Exception as e:
            logger.error(f"NULL 레코드 조회 실패: {e}")
            return None

    def get_session_all_records(self, target, session_start):
        """선택한 세션의 전체 레코드 조회 (파일 생성용, producturl 중복 제거 - 최신 것만)
        session_start가 리스트면 여러 세션을 합쳐서 조회 (DATE+HOUR로 세션 특정)"""
        config = TARGET_CONFIG[target]
        table = config['table']

        session_list = session_start if isinstance(session_start, list) else [session_start]

        try:
            dfs = []
            for ss in session_list:
                query = f"""
                SELECT t1.*
                FROM {table} t1
                INNER JOIN (
                    SELECT producturl, MAX(kr_crawl_datetime) as max_dt
                    FROM {table}
                    WHERE DATE(kr_crawl_datetime) = DATE(:session_start)
                      AND HOUR(kr_crawl_datetime) = HOUR(:session_start)
                    GROUP BY producturl
                ) t2 ON t1.producturl = t2.producturl AND t1.kr_crawl_datetime = t2.max_dt
                WHERE DATE(t1.kr_crawl_datetime) = DATE(:session_start)
                  AND HOUR(t1.kr_crawl_datetime) = HOUR(:session_start)
                """
                df = pd.read_sql(text(query), self.db_engine, params={'session_start': ss})
                dfs.append(df)
            combined = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
            combined = combined.drop_duplicates(subset=['producturl'], keep='last')
            return combined
        except Exception as e:
            logger.error(f"세션 레코드 조회 실패: {e}")
            return None

    def get_missing_urls(self, target, session_start):
        """세션에서 누락된 URL 조회 (마스터 리스트 vs 크롤 결과 비교)"""
        config = TARGET_CONFIG[target]
        table = config['table']
        tracking_country = config.get('tracking_country')
        tracking_mall_name = config.get('tracking_mall_name')

        if not tracking_country or not tracking_mall_name:
            logger.warning(f"{target}: tracking 설정 없음, 누락 URL 조회 건너뜀")
            return pd.DataFrame()

        try:
            # 1. 마스터 리스트 (전체 URL)
            master_query = """
            SELECT *
            FROM samsung_price_tracking_list
            WHERE country = :country
              AND mall_name = :mall_name
              AND is_active = TRUE
            """
            master_df = pd.read_sql(text(master_query), self.db_engine,
                                    params={'country': tracking_country, 'mall_name': tracking_mall_name})

            if master_df.empty:
                return pd.DataFrame()

            # 2. 선택한 세션에서 크롤링된 URL 목록 (DATE + HOUR로 세션 특정)
            session_list = session_start if isinstance(session_start, list) else [session_start]
            crawled_urls = set()
            for ss in session_list:
                session_query = f"""
                SELECT producturl
                FROM {table}
                WHERE DATE(kr_crawl_datetime) = DATE(:session_start)
                  AND HOUR(kr_crawl_datetime) = HOUR(:session_start)
                """
                session_df = pd.read_sql(text(session_query), self.db_engine, params={'session_start': ss})
                if not session_df.empty:
                    crawled_urls.update(session_df['producturl'].tolist())

            # 3. 비교: 마스터에는 있지만 크롤 결과에 없는 URL
            missing = master_df[~master_df['url'].isin(crawled_urls)]

            if not missing.empty:
                logger.info(f"누락 URL 발견: {len(missing)}개 (마스터 {len(master_df)}개 - 크롤 {len(crawled_urls)}개)")

            return missing

        except Exception as e:
            logger.error(f"누락 URL 조회 실패: {e}")
            return pd.DataFrame()

    def load_scraper(self, target):
        """해당 대상의 스크래퍼 로드 및 브라우저 초기화"""
        config = TARGET_CONFIG[target]
        module_name = config['scraper_module']
        class_name = config['scraper_class']

        try:
            module = __import__(module_name)
            scraper_class = getattr(module, class_name)
            scraper_kwargs = config.get('scraper_kwargs', {})
            scraper = scraper_class(**scraper_kwargs)
            logger.info(f"{class_name} 로드 완료")

            # 브라우저 초기화
            logger.info("브라우저 초기화 중...")
            if hasattr(scraper, 'setup_driver'):
                scraper.setup_driver()
            elif hasattr(scraper, 'setup_browser'):
                scraper.setup_browser()
            else:
                raise AttributeError(f"{class_name}에 setup_driver/setup_browser 메서드 없음")
            logger.info("브라우저 초기화 완료")

            return scraper
        except Exception as e:
            logger.error(f"스크래퍼 로드 실패: {e}")
            import traceback
            traceback.print_exc()
            return None

    def recrawl_url(self, scraper, url, row_data, target=None):
        """단일 URL 재크롤링"""
        try:
            # row_data를 dict로 변환 (Series인 경우)
            if isinstance(row_data, pd.Series):
                row_dict = row_data.to_dict()
            else:
                row_dict = row_data

            # URL 키 이름 맞추기
            row_dict['url'] = url

            config = TARGET_CONFIG.get(target, {})
            if config.get('needs_manual_check'):
                result = scraper.extract_product_info(url, row_dict)
            else:
                result = scraper.extract_product_info(url, row_dict, retry_count=0, max_retries=1)
            return result
        except Exception as e:
            logger.error(f"재크롤링 실패 ({url}): {e}")
            return None

    def update_db_record(self, target, original_kr_crawl_datetime, new_data):
        """DB 레코드 UPDATE (9개 컬럼)

        Args:
            target: 대상 (fr/gb/currys/it)
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
            ships_from = :ships_from,
            sold_by = :sold_by,
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
                    'ships_from': new_data.get('ships_from'),
                    'sold_by': new_data.get('sold_by'),
                    'crawl_datetime': crawl_datetime_iso,
                    'crawl_strdatetime': crawl_strdatetime,
                    'kr_crawl_datetime': kr_crawl_datetime,
                    'kr_crawl_strdatetime': kr_crawl_strdatetime,
                    'producturl': new_data.get('producturl'),
                    'original_kr_crawl_datetime': original_kr_crawl_datetime
                })
                conn.commit()
                if result.rowcount > 0:
                    # DB에 저장한 시간값을 new_data에 반영 (파일 생성 시 동일한 값 사용)
                    new_data['crawl_datetime'] = crawl_datetime_iso
                    new_data['crawl_strdatetime'] = crawl_strdatetime
                    new_data['kr_crawl_datetime'] = kr_crawl_datetime
                    new_data['kr_crawl_strdatetime'] = kr_crawl_strdatetime
                    return True
                return False
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

    def generate_and_upload_file(self, target, df, session_start, custom_filename=None, local_copy_dir=None):
        """CSV/ZIP/MD5 생성 및 파일서버 업로드"""
        config = TARGET_CONFIG[target]
        self.last_generated_files = {}

        # 원본 세션 날짜 (폴더용) - session_start에서 추출 (리스트면 가장 이른 날짜 사용)
        if isinstance(session_start, list):
            first_ss = min(str(s) for s in session_start)
        else:
            first_ss = session_start
        if isinstance(first_ss, str):
            session_date = datetime.strptime(first_ss[:10], '%Y-%m-%d')
        elif isinstance(first_ss, pd.Timestamp):
            session_date = first_ss.to_pydatetime()
        else:
            session_date = first_ss
        date_str = session_date.strftime('%Y%m%d')

        # 파일명 설정
        if custom_filename:
            base_filename = custom_filename
            # custom_filename의 앞 8자리를 폴더 날짜로 사용 (예: 20260409_094700_usa_bestbuy → 20260409)
            if len(custom_filename) >= 8 and custom_filename[:8].isdigit():
                date_str = custom_filename[:8]
        else:
            now = datetime.now(self.korea_tz)
            time_str = now.strftime('%H%M%S')
            base_filename = f"{date_str}_{time_str}_{config['file_prefix']}"

        try:
            # 1. CSV 파일 생성 (원본 스크래퍼와 동일한 컬럼 순서 유지)
            csv_filename = f'{base_filename}.csv'
            column_order = [
                'retailerid', 'country_code', 'ships_from', 'channel_name', 'channel',
                'retailersku', 'brand', 'brand_eng', 'form_factor',
                'segment_lv1', 'segment_lv2', 'segment_lv3', 'capacity', 'item',
                'retailprice', 'sold_by', 'imageurl', 'producturl',
                'crawl_datetime', 'crawl_strdatetime', 'kr_crawl_datetime', 'kr_crawl_strdatetime',
                'title', 'vat'
            ]
            df_copy = df.copy()
            # DB 컬럼명이 대소문자 다를 수 있으므로 소문자로 통일 후 정렬
            df_copy.columns = df_copy.columns.str.lower()
            existing_cols = [c for c in column_order if c in df_copy.columns]
            df_copy = df_copy[existing_cols]
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
            if local_copy_dir:
                os.makedirs(local_copy_dir, exist_ok=True)
                local_copies = {}
                for key, temp_file in [('csv', csv_filename), ('zip', zip_filename), ('md5', md5_filename)]:
                    local_path = os.path.join(local_copy_dir, os.path.basename(temp_file))
                    shutil.copy2(temp_file, local_path)
                    local_copies[key] = local_path
                self.last_generated_files = local_copies
                logger.info(f"LOCAL_FILESERVER_CSV={local_copies.get('csv')}")
                logger.info(f"LOCAL_FILESERVER_ZIP={local_copies.get('zip')}")
                logger.info(f"LOCAL_FILESERVER_MD5={local_copies.get('md5')}")

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

    def insert_missing_record(self, target, result):
        """누락 URL 크롤링 결과를 DB에 INSERT"""
        config = TARGET_CONFIG[target]
        table = config['table']

        try:
            df = pd.DataFrame([result])
            # 컬럼 순서 맞추기
            column_order = [
                'retailerid', 'country_code', 'ships_from', 'channel_name', 'channel',
                'retailersku', 'brand', 'brand_eng', 'form_factor',
                'segment_lv1', 'segment_lv2', 'segment_lv3', 'capacity', 'item',
                'retailprice', 'sold_by', 'imageurl', 'producturl',
                'crawl_datetime', 'crawl_strdatetime', 'kr_crawl_datetime', 'kr_crawl_strdatetime',
                'title', 'vat'
            ]
            existing_cols = [c for c in column_order if c in df.columns]
            df = df[existing_cols]
            df.to_sql(table, self.db_engine, if_exists='append', index=False)
            return True
        except Exception as e:
            logger.error(f"DB INSERT 실패: {e}")
            return False

    def run_recovery(self, target, session_start):
        """복구 실행 (session_start는 단일 값 또는 리스트)"""
        config = TARGET_CONFIG[target]
        session_display = session_start if not isinstance(session_start, list) else ' + '.join(str(s) for s in session_start)
        logger.info(f"\n{'='*60}")
        logger.info(f"복구 시작: {config['name']}")
        logger.info(f"세션: {session_display}")
        logger.info(f"{'='*60}")

        # 1. NULL 레코드 조회
        null_records = self.get_null_records(target, session_start)
        has_null = null_records is not None and not null_records.empty

        # currys 예외: title null <= 3개면 복구 불필요
        if has_null and target == 'currys':
            title_null_count = null_records['title'].isna().sum()
            if title_null_count <= 3:
                logger.info(f"currys: title null {title_null_count}개 (3개 이하) - 복구 불필요")
                has_null = False

        # 2. 누락 URL 조회
        missing_urls = self.get_missing_urls(target, session_start)
        has_missing = not missing_urls.empty if missing_urls is not None else False

        if not has_null and not has_missing:
            logger.info("복구 대상 레코드가 없습니다.")
            return True

        if has_null:
            logger.info(f"NULL 복구 대상: {len(null_records)}개")
        if has_missing:
            logger.info(f"누락 URL 대상: {len(missing_urls)}개")

        # 3. 복구 전 전체 세션 레코드 미리 조회 (파일 생성용)
        all_records = self.get_session_all_records(target, session_start)
        if all_records is None or all_records.empty:
            logger.error("전체 세션 레코드 조회 실패")
            return False
        logger.info(f"전체 세션 레코드: {len(all_records)}개 (미리 조회 완료)")
        expected_total = len(all_records) + (len(missing_urls) if has_missing else 0)
        logger.info(f"파일서버 업로드 예상: {expected_total}개 (세션 {len(all_records)}개 + 누락 {len(missing_urls) if has_missing else 0}개)")

        # 4. 스크래퍼 로드
        scraper = self.load_scraper(target)
        if scraper is None:
            logger.error("스크래퍼 로드 실패")
            return False

        # 4-1. 봇감지 수동 체크 (xkom 등)
        # 첫 페이지에서 봇체크박스가 안 떠도 첫 상품 추출이 실패하는 경우가 있어
        # 동일 URL을 2회 접속해서 워밍업 (1차에서 Cloudflare 통과, 2차에서 정상 로드 확인)
        if config.get('needs_manual_check'):
            if has_null:
                first_url = null_records.iloc[0]['producturl']
            else:
                first_url = missing_urls.iloc[0]['url']

            # 1차 접속
            logger.info(f"봇감지 수동 체크 (1차 접속): {first_url}")
            scraper.driver.get(first_url)
            print(f"\n{'='*60}")
            print(f"  [1차 접속] 봇감지(Cloudflare) 수동 체크가 필요합니다.")
            print(f"  브라우저에서 봇감지를 통과한 후 Enter를 눌러주세요.")
            print(f"{'='*60}")
            input("\n  준비 완료 후 Enter를 누르세요... ")

            # 2차 접속 (워밍업)
            logger.info(f"봇감지 수동 체크 (2차 접속): {first_url}")
            scraper.driver.get(first_url)
            print(f"\n{'='*60}")
            print(f"  [2차 접속] 페이지가 정상 로드되었는지 확인 후 Enter를 눌러주세요.")
            print(f"  (필요시 봇감지를 다시 통과시켜주세요)")
            print(f"{'='*60}")
            input("\n  준비 완료 후 Enter를 누르세요... ")

            scraper.is_logged_in = True
            logger.info("봇감지 수동 체크 완료, 복구 시작")

        # 5. 각 URL 재크롤링 및 DB UPDATE
        success_count = 0
        fail_count = 0
        _retailer_name = None
        delete_screenshots_for_sku = None
        is_null_result = None
        if has_null:
            try:
                from null_screenshot import RETAILER_NAME_BY_TARGET_KEY, delete_screenshots_for_sku, is_null_result
                _retailer_name = RETAILER_NAME_BY_TARGET_KEY.get(target)
            except Exception:
                _retailer_name = None
                delete_screenshots_for_sku = None
                is_null_result = None
        recovered_results = {}  # producturl -> 복구된 result
        missing_results = []  # 누락 URL INSERT용

        try:
            # 5-1. NULL 복구
            if has_null:
                logger.info(f"\n--- NULL 복구 시작 ({len(null_records)}개) ---")
                for i, (idx, row) in enumerate(null_records.iterrows()):
                    url = row['producturl']
                    original_kr_crawl_datetime = row['kr_crawl_datetime']  # 원본 시간 저장
                    logger.info(f"\n[{i+1}/{len(null_records)}] 재크롤링: {url[:60]}...")

                    if _retailer_name and delete_screenshots_for_sku:
                        try:
                            sku = row.get('retailersku', '')
                            original_date = str(original_kr_crawl_datetime)[:10].replace('-', '')
                            if sku and len(original_date) == 8:
                                delete_screenshots_for_sku(_retailer_name, sku, original_date)
                        except Exception as e:
                            logger.debug(f"ignored screenshot delete error: {e}")

                    result = self.recrawl_url(scraper, url, row, target)

                    if result and (result.get('title') is not None or result.get('retailprice') is not None):
                        # DB UPDATE (원본 kr_crawl_datetime으로 정확히 매칭)
                        result['producturl'] = url
                        if self.update_db_record(target, original_kr_crawl_datetime, result):
                            logger.info(f"  -> 성공: title={str(result.get('title', ''))[:30]}, price={result.get('retailprice')}")
                            success_count += 1
                            recovered_results[url] = result
                            if _retailer_name and delete_screenshots_for_sku and is_null_result and not is_null_result(result):
                                try:
                                    sku = result.get('retailersku') or row.get('retailersku', '')
                                    screenshot_dates = set()
                                    original_date = str(original_kr_crawl_datetime)[:10].replace('-', '')
                                    result_date = str(result.get('kr_crawl_datetime', ''))[:10].replace('-', '')
                                    if len(original_date) == 8:
                                        screenshot_dates.add(original_date)
                                    if len(result_date) == 8:
                                        screenshot_dates.add(result_date)
                                    for screenshot_date in screenshot_dates:
                                        delete_screenshots_for_sku(_retailer_name, sku, screenshot_date)
                                except Exception as e:
                                    logger.debug(f"ignored recovered screenshot cleanup error: {e}")
                        else:
                            logger.warning(f"  -> DB UPDATE 실패")
                            fail_count += 1
                    else:
                        logger.warning(f"  -> 재크롤링 실패 (여전히 NULL 또는 price=0)")
                        fail_count += 1

            # 5-2. 누락 URL 크롤링
            if has_missing:
                missing_success = 0
                missing_fail = 0
                logger.info(f"\n--- 누락 URL 크롤링 시작 ({len(missing_urls)}개) ---")
                for i, (idx, row) in enumerate(missing_urls.iterrows()):
                    url = row['url']
                    logger.info(f"\n[누락 {i+1}/{len(missing_urls)}] 크롤링: {url[:60]}...")

                    result = self.recrawl_url(scraper, url, row, target)

                    if result and (result.get('title') is not None or result.get('retailprice') is not None):
                        result['producturl'] = url
                        if self.insert_missing_record(target, result):
                            logger.info(f"  -> INSERT 성공: title={str(result.get('title', ''))[:30]}, price={result.get('retailprice')}")
                            missing_success += 1
                            missing_results.append(result)
                        else:
                            logger.warning(f"  -> DB INSERT 실패")
                            missing_fail += 1
                    else:
                        logger.warning(f"  -> 크롤링 실패")
                        missing_fail += 1

                logger.info(f"누락 URL 결과: 성공 {missing_success}개, 실패 {missing_fail}개")
                success_count += missing_success
                fail_count += missing_fail

        except Exception as e:
            logger.error(f"복구 중 오류 발생: {e}")
            import traceback
            traceback.print_exc()

        finally:
            # 6. 브라우저 종료 (항상 실행)
            if scraper:
                try:
                    if hasattr(scraper, 'driver') and scraper.driver:
                        scraper.driver.quit()
                    elif hasattr(scraper, 'page') and scraper.page:
                        scraper.page.quit()
                    logger.info("브라우저 종료")
                except Exception:
                    pass

        # 7. 결과 요약
        logger.info(f"\n{'='*60}")
        logger.info(f"복구 완료: 성공 {success_count}개, 실패 {fail_count}개")
        logger.info(f"{'='*60}")

        # 8. 파일서버 업로드 (미리 조회한 전체 레코드 + 복구 결과 merge + 누락 결과 추가)
        if success_count > 0:
            logger.info("\n파일서버 업로드 시작...")
            # 복구된 레코드를 미리 조회한 전체 레코드에 merge
            for url, result in recovered_results.items():
                mask = all_records['producturl'] == url
                if mask.any():
                    for col in ['title', 'imageurl', 'retailprice', 'ships_from', 'sold_by',
                                'crawl_datetime', 'crawl_strdatetime', 'kr_crawl_datetime', 'kr_crawl_strdatetime']:
                        if col in result:
                            all_records.loc[mask, col] = result.get(col)
                    logger.info(f"  merge 완료: {url[:60]}")

            # 누락 URL 결과를 all_records에 추가
            if missing_results:
                missing_df = pd.DataFrame(missing_results)
                all_records = pd.concat([all_records, missing_df], ignore_index=True)
                logger.info(f"  누락 URL {len(missing_results)}개 추가")

            logger.info(f"파일서버 업로드 대상: {len(all_records)}개 레코드")

            # 파일명 일시 입력 받기
            print(f"\n파일명 일시를 입력하세요 (예: 20260206_120000)")
            print("Enter를 누르면 현재 시간으로 자동 생성됩니다.")
            datetime_input = input("일시: ").strip()
            if datetime_input:
                custom_filename = f"{datetime_input}_{config['file_prefix']}"
            else:
                custom_filename = None

            if self.generate_and_upload_file(target, all_records, session_start, custom_filename):
                logger.info("파일서버 업로드 완료")
            else:
                logger.error("파일서버 업로드 실패")

            # 9. 복구 후 가격 이상 감지 알림
            alert_code = config.get('alert_code')
            if alert_code:
                # 마스터 리스트 개수 조회 (target_count용)
                tracking_country = config.get('tracking_country')
                tracking_mall_name = config.get('tracking_mall_name')
                master_count = len(all_records)  # 기본값
                if tracking_country and tracking_mall_name:
                    try:
                        count_query = """
                        SELECT COUNT(*) as cnt
                        FROM samsung_price_tracking_list
                        WHERE country = :country AND mall_name = :mall_name AND is_active = TRUE
                        """
                        count_df = pd.read_sql(text(count_query), self.db_engine,
                                              params={'country': tracking_country, 'mall_name': tracking_mall_name})
                        master_count = int(count_df['cnt'][0])
                    except Exception:
                        pass

                # 세션 날짜 계산 (파일서버 업로드 폴더와 동일)
                if isinstance(session_start, list):
                    _first_ss = min(str(s) for s in session_start)
                else:
                    _first_ss = str(session_start)
                session_date_str = _first_ss[:10].replace('-', '')  # 'YYYY-MM-DD' → 'YYYYMMDD'

                monitor_and_alert(alert_code, master_count, all_records,
                                 fs_country_code=config['country_code'], file_prefix=config['file_prefix'],
                                 skip_date=session_date_str)

        return success_count > 0 or fail_count == 0

    def upload_only(self, target, session_start):
        """파일서버 업로드만 실행 (복구 없이, session_start는 단일 값 또는 리스트)"""
        config = TARGET_CONFIG[target]
        session_display = session_start if not isinstance(session_start, list) else ' + '.join(str(s) for s in session_start)
        logger.info(f"\n{'='*60}")
        logger.info(f"파일 업로드: {config['name']}")
        logger.info(f"세션: {session_display}")
        logger.info(f"{'='*60}")

        # 전체 레코드 조회 (중복 제거)
        all_records = self.get_session_all_records(target, session_start)
        if all_records is None or all_records.empty:
            logger.error("레코드 조회 실패")
            return False

        logger.info(f"파일 생성 대상: {len(all_records)}개 레코드")

        # 파일명 일시 입력 받기
        print(f"\n파일명 일시를 입력하세요 (예: 20260206_120000)")
        print("Enter를 누르면 현재 시간으로 자동 생성됩니다.")
        datetime_input = input("일시: ").strip()
        if datetime_input:
            custom_filename = f"{datetime_input}_{config['file_prefix']}"
        else:
            custom_filename = None

        if self.generate_and_upload_file(target, all_records, session_start, custom_filename):
            logger.info("파일서버 업로드 완료")
            return True
        else:
            logger.error("파일서버 업로드 실패")
            return False


def select_target():
    """대상 선택"""
    print("\n===== 복구 대상 선택 =====")
    print("1. fr (프랑스 Amazon)")
    print("2. gb (영국 Amazon)")
    print("3. currys (영국 Currys)")
    print("4. it (이탈리아 Amazon)")
    print("5. de (독일 Amazon)")
    print("6. bestbuy (미국 BestBuy)")
    print("7. es (스페인 Amazon)")
    print("8. mediamarkt (독일 MediaMarkt)")
    print("9. xkom (폴란드 X-Kom)")
    print("10. usa (미국 Amazon)")
    print("11. nl (네덜란드 Amazon)")
    print("12. danawa (한국 다나와)")
    print("13. in (인도 Amazon)")
    print("14. jp (일본 Amazon)")
    print("15. au (호주 Centrecom)")
    print("16. coolblue (네덜란드 Coolblue)")
    print("17. fnac (프랑스 Fnac)")
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
            elif choice == '4':
                return 'it'
            elif choice == '5':
                return 'de'
            elif choice == '6':
                return 'bestbuy'
            elif choice == '7':
                return 'es'
            elif choice == '8':
                return 'mediamarkt'
            elif choice == '9':
                return 'xkom'
            elif choice == '10':
                return 'usa'
            elif choice == '11':
                return 'nl'
            elif choice == '12':
                return 'danawa'
            elif choice == '13':
                return 'in'
            elif choice == '14':
                return 'jp'
            elif choice == '15':
                return 'au'
            elif choice == '16':
                return 'coolblue'
            elif choice == '17':
                return 'fnac'
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
    print(f"{'번호':^4} | {'시작시간 (KST)':<20} | {'총개수':>6} | {'title NULL':>10} | {'imageurl NULL':>13} | {'price NULL':>10}")
    print("-" * 85)

    for idx, row in sessions.iterrows():
        session_start = row['session_start']
        total = row['total_count']
        title_null = row['title_null_count']
        img_null = row['imageurl_null_count']
        price_null = row['price_null_count']

        # currys: title null <= 3 표시
        note = ""
        if target == 'currys' and title_null <= 3:
            note = " (복구불필요)"

        print(f"{idx+1:^4} | {str(session_start):<20} | {total:>6} | {title_null:>10} | {img_null:>13} | {price_null:>10}{note}")

    if target == 'currys':
        print("\n* currys: title NULL <= 3개 세션은 복구 대상 아님")

    print("\n0. 뒤로가기")
    print("* 여러 세션을 합치려면 쉼표로 구분 (예: 1,2)")

    while True:
        try:
            choice = input("\n복구할 세션 번호: ").strip()
            if choice == '0':
                return None

            # 쉼표로 복수 선택 지원
            if ',' in choice:
                indices = [int(c.strip()) - 1 for c in choice.split(',')]
                if all(0 <= idx < len(sessions) for idx in indices):
                    selected = [sessions.iloc[idx]['session_start'] for idx in indices]
                    total = sum(sessions.iloc[idx]['total_count'] for idx in indices)
                    print(f"  → {len(selected)}개 세션 합침 (총 {int(total)}개 레코드)")
                    return selected
                else:
                    print("올바른 번호를 입력하세요.")
            else:
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

        # 3. 작업 선택
        session_display = session_start if not isinstance(session_start, list) else ' + '.join(str(s) for s in session_start)
        print(f"\n선택한 세션: {session_display}")
        print("1. 복구 실행 (NULL 재크롤링 + 파일 업로드)")
        print("2. 파일 업로드만 (복구 없이)")
        print("0. 취소")
        action = input("선택: ").strip()

        if action == '1':
            # 복구 실행
            manager.run_recovery(target, session_start)
        elif action == '2':
            # 파일 업로드만
            manager.upload_only(target, session_start)
        else:
            print("취소되었습니다.")
            continue

        # 계속할지 확인
        cont = input("\n다른 세션을 처리하시겠습니까? (y/n): ").strip().lower()
        if cont != 'y':
            print("\n프로그램을 종료합니다.")
            break


if __name__ == "__main__":
    main()

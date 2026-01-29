"""
Centrecom 가격 추출 시스템
호주 Centrecom.com.au 전용 크롤러
"""
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import pandas as pd
import pymysql
from sqlalchemy import create_engine
import paramiko
import time
import random
import re
from datetime import datetime
import pytz
import logging
import os
from io import StringIO
import json
import zipfile
import hashlib

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Import database configuration V2
from config import DB_CONFIG_V2 as DB_CONFIG

from config import FILE_SERVER_CONFIG
from alert_monitor import monitor_and_alert

class CentrecomScraper:
    def __init__(self, country_code='au'):
        self.driver = None
        self.db_engine = None
        self.country_code = country_code.lower()
        self.wait = None
        # V2: 타임존 분리 (현지시간 + 한국시간)
        self.korea_tz = pytz.timezone('Asia/Seoul')
        self.local_tz = pytz.timezone('Australia/Sydney')  # 호주 현지 시간

        # 크롤링 대상 URL 목록 저장 (차단 페이지 복구용)
        self.urls_data = []

        # DB 연결 설정
        self.setup_db_connection()

        # DB에서 선택자 로드
        self.load_selectors_from_db()

    def setup_db_connection(self):
        """DB 연결 설정"""
        try:
            connection_string = (
                f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
                f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            )
            self.db_engine = create_engine(connection_string)
            logger.info("DB 연결 설정 완료")

        except Exception as e:
            logger.error(f"DB 연결 실패: {e}")
            self.db_engine = None

    def load_selectors_from_db(self):
        """DB에서 Centrecom용 선택자 로드"""
        try:
            query = """
            SELECT element_type, selector_value, priority
            FROM mall_selectors
            WHERE mall_name = 'centrecom'
              AND country_code = 'au'
              AND is_active = TRUE
            ORDER BY element_type, priority ASC
            """

            df = pd.read_sql(query, self.db_engine)

            # element_type별로 그룹화
            self.selectors = {self.country_code: {}}
            for element_type in df['element_type'].unique():
                type_selectors = df[df['element_type'] == element_type]['selector_value'].tolist()
                self.selectors[self.country_code][element_type] = type_selectors

            logger.info(f"DB에서 선택자 로드 완료: {len(df)}개")

            # 기본값 설정 (DB에 없는 경우) - price, title, imageurl은 DB에서 관리
            if not self.selectors[self.country_code]:
                logger.warning("DB에 선택자가 없습니다. DB에서 centrecom/au 선택자를 확인하세요.")
                self.selectors[self.country_code] = {
                    'price': [],
                    'title': [],
                    'imageurl': []
                }

        except Exception as e:
            logger.error(f"선택자 로드 실패: {e}")
            # 기본값 사용
            self.selectors = {
                self.country_code: {
                    'price': [],
                    'title': [],
                    'imageurl': []
                }
            }

    def setup_driver(self):
        """Chrome 드라이버 설정"""
        logger.info("Chrome 드라이버 설정 중...")

        try:
            options = uc.ChromeOptions()

            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-setuid-sandbox')
            # 메모리 최적화 옵션
            options.add_argument('--disable-gpu')
            options.add_argument('--disable-extensions')
            options.add_argument('--disable-infobars')
            options.add_argument('--disable-renderer-backgrounding')
            options.add_argument('--js-flags=--max-old-space-size=512')

            user_agents = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            ]
            options.add_argument(f'--user-agent={random.choice(user_agents)}')

            # 호주 영어 설정
            options.add_experimental_option('prefs', {'intl.accept_languages': 'en-AU,en'})

            self.driver = uc.Chrome(options=options, version_main=144)
            self.driver.maximize_window()

            self.wait = WebDriverWait(self.driver, 20)

            logger.info("드라이버 설정 완료")
            return True

        except Exception as e:
            logger.error(f"드라이버 설정 실패: {e}")
            return False

    def wait_for_page_load(self, timeout=10):
        """페이지 로드 대기"""
        try:
            self.wait.until(
                lambda driver: driver.execute_script("return document.readyState") == "complete"
            )
            time.sleep(2)  # 추가 안전 대기
            return True

        except Exception as e:
            logger.warning(f"페이지 로드 대기 중 오류: {e}")
            return False

    def extract_element_text(self, selectors, element_name="요소"):
        """선택자 목록에서 텍스트 추출"""
        logger.info(f"{element_name} 추출 시작 - 총 {len(selectors)}개 선택자")

        for idx, selector in enumerate(selectors, 1):
            try:
                logger.info(f"[{idx}/{len(selectors)}] 시도 중: {selector}")

                if selector.startswith('//') or selector.startswith('('):
                    elements = self.driver.find_elements(By.XPATH, selector)
                    selector_type = "XPath"
                else:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    selector_type = "CSS"

                logger.info(f"타입: {selector_type}, 발견: {len(elements)}개")

                if elements:
                    for i, element in enumerate(elements):
                        try:
                            if element.is_displayed():
                                text1 = element.text.strip()
                                text2 = element.get_attribute('textContent')
                                text3 = element.get_attribute('innerText')

                                text2 = text2.strip() if text2 else ""
                                text3 = text3.strip() if text3 else ""

                                text = max([text1, text2, text3], key=len)

                                if text:
                                    logger.info(f"추출 성공: '{text}'")
                                    return text
                        except Exception as e:
                            logger.debug(f"요소 처리 중 오류: {e}")

            except Exception as e:
                logger.debug(f"선택자 오류: {str(e)}")
                continue

        logger.error(f"{element_name} 추출 완전 실패")
        return None

    def parse_price_by_country(self, price_text, country_code):
        """국가별 가격 파싱 - 호주는 미국과 동일한 형식"""
        try:
            price_text = price_text.strip()
            logger.debug(f"파싱할 가격 텍스트: {price_text}")

            # 호주 달러 파싱 (미국과 동일: $1,234.56)
            cleaned = re.sub(r'[$\s]', '', price_text)
            if re.match(r'^\d{1,3}(,\d{3})*(\.\d{1,2})?$', cleaned) or re.match(r'^\d+(\.\d{1,2})?$', cleaned):
                return cleaned

        except Exception as e:
            logger.debug(f"가격 파싱 오류: {price_text} - {e}")

        return None

    def extract_price(self, country_code):
        """가격 추출"""
        logger.info(f"가격 추출 시작 - 국가: {country_code}")

        price_selectors = self.selectors[country_code].get('price', [])

        for idx, selector in enumerate(price_selectors, 1):
            try:
                logger.info(f"[{idx}/{len(price_selectors)}] 시도: {selector}")

                if selector.startswith('//'):
                    elements = self.driver.find_elements(By.XPATH, selector)
                else:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)

                for element in elements:
                    if element.is_displayed():
                        text_methods = [
                            element.get_attribute('textContent'),
                            element.get_attribute('innerText'),
                            element.text
                        ]

                        for text in text_methods:
                            if text and text.strip():
                                price_text = text.strip()
                                logger.info(f"텍스트: {price_text}")

                                price = self.parse_price_by_country(price_text, country_code)
                                if price:
                                    logger.info(f"가격 추출 성공: {price}")
                                    return price

            except Exception as e:
                logger.debug(f"선택자 오류: {e}")

        logger.error("모든 방법으로 가격 추출 실패")
        return None

    def handle_block_page_with_refresh(self, url, row_data, refresh_count):
        """
        차단 페이지 처리 - title이 수집 안되면 차단으로 인식
        3회 새로고침 시도, 그래도 안되면 홈페이지 접속 후 첫번째 url 접속하고 현재 URL 재시도
        """
        logger.warning(f"차단 페이지 감지 - 처리 시도 {refresh_count + 1}/4")

        if refresh_count < 3:
            # 새로고침 시도 (0, 1, 2)
            logger.info(f"새로고침 시도 {refresh_count + 1}/3")
            time.sleep(random.uniform(3, 5))
            self.driver.refresh()
            time.sleep(random.uniform(2, 4))
            self.wait_for_page_load()
            return False  # 재시도 필요
        elif refresh_count == 3:
            # 3회 실패 후 홈페이지 접속
            logger.info("3회 새로고침 실패 - 홈페이지 접속 시도")
            try:
                self.driver.get("https://www.centrecom.com.au/")
                time.sleep(random.uniform(3, 5))
                self.wait_for_page_load()

                # 첫번째 URL 접속
                if self.urls_data and len(self.urls_data) > 0:
                    first_url = self.urls_data[0].get('url')
                    logger.info(f"첫번째 URL 접속: {first_url}")
                    self.driver.get(first_url)
                    time.sleep(random.uniform(2, 4))
                    self.wait_for_page_load()

                # 현재 URL은 재귀 호출에서 접속하게 함
                logger.info("홈페이지 접속 완료 - 현재 URL 재시도 예정")
                return False  # 재시도 (extract_product_info가 현재 URL을 다시 접속)
            except Exception as e:
                logger.error(f"홈페이지 접속 실패: {e}")
                return True  # 더 이상 재시도하지 않음
        else:
            # refresh_count > 3
            logger.error("최대 재시도 횟수 초과")
            return True  # 더 이상 재시도하지 않음

    def extract_product_info(self, url, row_data, retry_count=0, max_retries=4):
        """제품 정보 추출"""
        try:
            logger.info("=" * 60)
            logger.info("제품 정보 추출 시작")
            logger.info(f"URL: {url}")
            logger.info(f"브랜드: {row_data.get('brand', 'N/A')}")
            logger.info(f"제품: {row_data.get('item', 'N/A')}")

            self.driver.get(url)
            time.sleep(random.uniform(2, 4))
            self.wait_for_page_load()

            # V2: 타임존 분리
            now_time = datetime.now(self.korea_tz)
            local_time = datetime.now(self.local_tz)

            # ISO 8601 형식
            crawl_dt = local_time.strftime("%Y-%m-%dT%H:%M:%S")
            tz_offset = local_time.strftime("%z")
            tz_formatted = f"{tz_offset[:3]}:{tz_offset[3:]}" if tz_offset else "+00:00"
            crawl_datetime_iso = f"{crawl_dt}{tz_formatted}"

            result = {
                'retailerid': row_data.get('retailerid', ''),
                'country_code': self.country_code,
                'ships_from': 'AU',  # 고정값
                'channel_name': 'centrecom.com.au',
                'channel': row_data.get('channel', 'Online'),
                'retailersku': row_data.get('retailersku', ''),
                'brand': row_data.get('brand', ''),
                'brand_eng': row_data.get('brand_eng', row_data.get('brand', '')),
                'form_factor': row_data.get('form_factor', ''),
                'segment_lv1': row_data.get('seg_lv1', ''),
                'segment_lv2': row_data.get('seg_lv2', ''),
                'segment_lv3': row_data.get('seg_lv3', ''),
                'capacity': row_data.get('capacity', ''),
                'item': row_data.get('item', ''),
                'retailprice': None,
                'sold_by': 'Centrecom',  # 고정값
                'imageurl': None,
                'producturl': url,
                'crawl_datetime': crawl_datetime_iso,
                'kr_crawl_datetime': now_time.strftime('%Y-%m-%d %H:%M:%S'),  # V2: 한국시간
                'kr_crawl_strdatetime': now_time.strftime('%Y%m%d%H%M%S') + f"{now_time.microsecond:06d}"[:4],  # V2: 한국시간 문자열
                'crawl_strdatetime': local_time.strftime('%Y%m%d%H%M%S') + f"{local_time.microsecond:06d}"[:4],
                'title': None,
                'vat': row_data.get('vat', 'x')
            }

            # title 추출
            result['title'] = self.extract_element_text(
                self.selectors[self.country_code].get('title', []),
                "제목"
            )

            # title이 없으면 차단 페이지로 간주
            if not result['title']:
                logger.warning("Title 추출 실패 - 차단 페이지로 간주")
                should_stop = self.handle_block_page_with_refresh(url, row_data, retry_count)

                if not should_stop and retry_count < max_retries:
                    # 재시도
                    return self.extract_product_info(url, row_data, retry_count + 1, max_retries)
                else:
                    # 실패 - 빈 결과 반환
                    logger.error("차단 페이지 해결 실패")
                    return result

            # 가격 추출
            logger.info("가격 추출 시도")
            result['retailprice'] = self.extract_price(self.country_code)

            # 이미지 URL 추출
            for selector in self.selectors[self.country_code].get('imageurl', []):
                try:
                    if selector.startswith('//'):
                        element = self.driver.find_element(By.XPATH, selector)
                    else:
                        element = self.driver.find_element(By.CSS_SELECTOR, selector)

                    result['imageurl'] = element.get_attribute('src')
                    if result['imageurl']:
                        logger.debug("이미지 URL 추출 성공")
                        break
                except:
                    continue

            logger.info("추출 결과:")
            logger.info(f"제목: {result['title'][:50] + '...' if result['title'] and len(result['title']) > 50 else result['title']}")
            logger.info(f"가격: {result['retailprice']}")
            logger.info(f"이미지: {'있음' if result['imageurl'] else '없음'}")
            logger.info(f"판매자: {result['sold_by']}")
            logger.info(f"배송지: {result['ships_from']}")

            return result

        except Exception as e:
            logger.error(f"페이지 처리 오류: {e}")

            if retry_count < max_retries:
                wait_time = (retry_count + 1) * 10
                logger.info(f"{wait_time}초 후 재시도... ({retry_count + 1}/{max_retries})")
                time.sleep(wait_time)

                try:
                    self.driver.refresh()
                except:
                    logger.info("드라이버 재시작 중...")
                    self.driver.quit()
                    self.setup_driver()

                return self.extract_product_info(url, row_data, retry_count + 1, max_retries)

            # V2: 타임존 분리
            now_time = datetime.now(self.korea_tz)
            local_time = datetime.now(self.local_tz)

            crawl_dt = local_time.strftime("%Y-%m-%dT%H:%M:%S")
            tz_offset = local_time.strftime("%z")
            tz_formatted = f"{tz_offset[:3]}:{tz_offset[3:]}" if tz_offset else "+00:00"
            crawl_datetime_iso = f"{crawl_dt}{tz_formatted}"

            return {
                'retailerid': row_data.get('retailerid', ''),
                'country_code': self.country_code,
                'ships_from': 'AU',
                'channel_name': 'centrecom.com.au',
                'channel': row_data.get('channel', 'Online'),
                'retailersku': row_data.get('retailersku', ''),
                'brand': row_data.get('brand', ''),
                'brand_eng': row_data.get('brand_eng', row_data.get('brand', '')),
                'form_factor': row_data.get('form_factor', ''),
                'segment_lv1': row_data.get('seg_lv1', ''),
                'segment_lv2': row_data.get('seg_lv2', ''),
                'segment_lv3': row_data.get('seg_lv3', ''),
                'capacity': row_data.get('capacity', ''),
                'item': row_data.get('item', ''),
                'retailprice': None,
                'sold_by': 'Centrecom',
                'imageurl': None,
                'producturl': url,
                'crawl_datetime': crawl_datetime_iso,
                'kr_crawl_datetime': now_time.strftime('%Y-%m-%d %H:%M:%S'),  # V2: 한국시간
                'kr_crawl_strdatetime': now_time.strftime('%Y%m%d%H%M%S') + f"{now_time.microsecond:06d}"[:4],  # V2: 한국시간 문자열
                'crawl_strdatetime': local_time.strftime('%Y%m%d%H%M%S') + f"{local_time.microsecond:06d}"[:4],
                'title': None,
                'vat': row_data.get('vat', 'x')
            }

    def get_crawl_targets(self, limit=None):
        """DB에서 크롤링 대상 URL 목록 조회"""
        try:
            query = f"""
            SELECT *
            FROM samsung_price_tracking_list
            WHERE country = '{self.country_code}'
              AND mall_name = 'centrecom'
              AND is_active = TRUE
            """

            if limit:
                query += f" LIMIT {limit}"

            df = pd.read_sql(query, self.db_engine)
            logger.info(f"크롤링 대상 {len(df)}개 조회 완료")
            return df.to_dict('records')

        except Exception as e:
            logger.error(f"크롤링 대상 조회 실패: {e}")
            return []

    def save_to_db(self, df):
        """DB에 결과 저장"""
        if self.db_engine is None:
            logger.warning("DB 연결이 없어 DB 저장을 건너뜁니다")
            return False

        try:
            table_name = f'centrecom_price_crawl_tbl_{self.country_code}'

            df.to_sql(table_name, self.db_engine, if_exists='append', index=False)
            logger.info(f"DB 저장 완료: {len(df)}개 레코드 -> {table_name}")

            log_records = []
            for _, row in df.iterrows():
                log_records.append({
                    'country_code': self.country_code,
                    'url': row['producturl'],
                    'status': 'success' if row['retailprice'] is not None else 'failed',
                    'error_message': None if row['retailprice'] is not None else 'Price not found',
                    'execution_time': random.uniform(3, 10),
                    'retailprice': row['retailprice'],
                    'crawl_datetime': row['crawl_datetime']
                })

            if log_records:
                log_df = pd.DataFrame(log_records)
                log_df.to_sql('centrecom_crawl_logs', self.db_engine, if_exists='append', index=False)
                logger.info(f"크롤링 로그 저장 완료: {len(log_records)}개")

            return True

        except Exception as e:
            logger.error(f"DB 저장 실패: {e}")
            return False

    def upload_to_file_server(self, local_file_path, date_folder):
        """파일서버에 업로드"""
        try:
            transport = paramiko.Transport((FILE_SERVER_CONFIG['host'], FILE_SERVER_CONFIG['port']))
            transport.connect(
                username=FILE_SERVER_CONFIG['username'],
                password=FILE_SERVER_CONFIG['password']
            )
            sftp = paramiko.SFTPClient.from_transport(transport)

            # 국가별 디렉토리 경로
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

    def save_results(self, df, save_db=True, upload_server=True):
        """결과 저장"""
        now = datetime.now(self.korea_tz)
        date_str = now.strftime('%Y%m%d')
        time_str = now.strftime('%H%M%S')
        base_filename = f"{date_str}_{time_str}_au_centrecom"

        results = {'db_saved': False, 'server_uploaded': False}

        if save_db:
            results['db_saved'] = self.save_to_db(df)

        if upload_server:
            try:
                # 1. CSV 파일 생성
                csv_filename = f'{base_filename}.csv'
                # Header를 대문자로 변환
                df.columns = df.columns.str.upper()
                df.to_csv(csv_filename, index=False, encoding='utf-8', lineterminator='\r\n')

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

                # 4. MD5 파일 생성 (정합성 확인)
                md5_filename = f'{base_filename}.md5'
                with open(md5_filename, 'w', encoding='utf-8') as f:
                    f.write(f"{os.path.basename(zip_filename)} {zip_md5}\n")
                    f.write(f"{os.path.basename(csv_filename)} {csv_md5}\n")

                # 5. ZIP과 MD5를 날짜 폴더에 업로드
                if self.upload_to_file_server(zip_filename, date_str):
                    if self.upload_to_file_server(md5_filename, date_str):
                        results['server_uploaded'] = True

                # 6. 로컬 임시 파일 삭제
                for temp_file in [csv_filename, zip_filename, md5_filename]:
                    if os.path.exists(temp_file):
                        os.remove(temp_file)

                logger.info("임시 파일 삭제 완료")
            except Exception as e:
                logger.error(f"파일 저장 실패: {e}")

        return results

    def scrape_urls(self, urls_data, max_items=None):
        """여러 URL 스크래핑"""
        if max_items:
            urls_data = urls_data[:max_items]

        # urls_data를 인스턴스 변수에 저장 (차단 페이지 복구용)
        self.urls_data = urls_data

        logger.info("=" * 80)
        logger.info("크롤링 시작")
        logger.info(f"국가: {self.country_code.upper()}")
        logger.info(f"대상: {len(urls_data)}개 제품")
        logger.info("=" * 80)

        if not self.setup_driver():
            logger.error("드라이버 설정 실패")
            return None

        # 홈페이지 먼저 접속
        logger.info("=" * 80)
        logger.info("Centrecom 홈페이지 접속 중...")
        logger.info("=" * 80)
        try:
            self.driver.get("https://www.centrecom.com.au/")
            time.sleep(random.uniform(3, 5))
            self.wait_for_page_load()
            logger.info("홈페이지 접속 완료")
        except Exception as e:
            logger.error(f"홈페이지 접속 실패: {e}")
            self.driver.quit()
            return None

        results = []
        failed_urls = []

        try:
            for idx, row in enumerate(urls_data):
                logger.info(f"진행률: {idx + 1}/{len(urls_data)} ({(idx + 1)/len(urls_data)*100:.1f}%)")

                url = row.get('url')

                result = self.extract_product_info(url, row)

                if result['retailprice'] is None and result['title'] is None:
                    failed_urls.append({
                        'url': url,
                        'item': row.get('item', ''),
                        'brand': row.get('brand', ''),
                        'reason': '가격과 제목 모두 없음'
                    })
                elif result['retailprice'] is None:
                    failed_urls.append({
                        'url': url,
                        'item': row.get('item', ''),
                        'brand': row.get('brand', ''),
                        'reason': '가격 없음'
                    })

                results.append(result)

                if (idx + 1) % 10 == 0:
                    interim_df = pd.DataFrame(results[-10:])
                    if self.db_engine:
                        try:
                            table_name = f'centrecom_price_crawl_tbl_{self.country_code}'
                            interim_df.to_sql(table_name, self.db_engine,
                                            if_exists='append', index=False)
                            logger.info("중간 저장: 10개 레코드 DB 저장")
                        except Exception as e:
                            logger.error(f"중간 저장 실패: {e}")

                if idx < len(urls_data) - 1:
                    wait_time = random.uniform(5, 10)
                    logger.info(f"{wait_time:.1f}초 대기 중...")
                    time.sleep(wait_time)

                    if (idx + 1) % 20 == 0:
                        logger.info("20개 처리 완료, 30초 휴식...")
                        time.sleep(30)

        except Exception as e:
            logger.error(f"스크래핑 중 오류: {e}")

        finally:
            if failed_urls:
                logger.warning(f"문제 발생한 URL {len(failed_urls)}개:")
                for fail in failed_urls[:5]:
                    logger.warning(f"  - {fail['brand']} {fail['item']}: {fail.get('reason', '알 수 없음')}")
                if len(failed_urls) > 5:
                    logger.warning(f"  ... 외 {len(failed_urls) - 5}개")

            if self.driver:
                self.driver.quit()
                logger.info("드라이버 종료")

        return pd.DataFrame(results)

    def analyze_results(self, df):
        """결과 분석"""
        logger.info("=" * 80)
        logger.info("결과 분석")
        logger.info("=" * 80)

        total = len(df)
        with_price = df['retailprice'].notna().sum()
        without_price = df['retailprice'].isna().sum()
        success_rate = (with_price / total * 100) if total > 0 else 0

        logger.info(f"전체 제품: {total}개")
        logger.info(f"가격 추출 성공: {with_price}개")
        logger.info(f"가격 추출 실패: {without_price}개")
        logger.info(f"성공률: {success_rate:.1f}%")

        if with_price > 0:
            price_df = df[df['retailprice'].notna()].copy()

            try:
                price_df['price_numeric'] = price_df['retailprice'].astype(str).str.replace(',', '').astype(float)

                logger.info("가격 통계:")
                logger.info(f"   평균가: {price_df['price_numeric'].mean():.2f}")
                logger.info(f"   최저가: {price_df['price_numeric'].min():.2f}")
                logger.info(f"   최고가: {price_df['price_numeric'].max():.2f}")
                logger.info(f"   중간값: {price_df['price_numeric'].median():.2f}")
            except Exception as e:
                logger.warning(f"가격 통계 계산 오류: {e}")
                logger.info("가격 통계: 계산 불가 (문자열 형태 가격)")

            brand_stats = df.groupby('brand').agg({
                'retailprice': ['count', lambda x: x.notna().sum()]
            })
            brand_stats.columns = ['total', 'success']
            brand_stats['success_rate'] = (brand_stats['success'] / brand_stats['total'] * 100).round(1)

            logger.info("브랜드별 성공률:")
            for brand, row in brand_stats.iterrows():
                logger.info(f"   {brand}: {row['success_rate']:.1f}% ({row['success']}/{row['total']})")

def main():
    """메인 실행 함수"""
    country_code = os.getenv('COUNTRY_CODE', 'au').lower()
    test_mode = os.getenv('TEST_MODE', 'false').lower() == 'true'
    max_items = int(os.getenv('MAX_ITEMS', '0')) or None

    print("=" * 80)
    print("Centrecom 가격 추출 시스템 v1.0")
    print("=" * 80)
    print(f"국가: {country_code.upper()}")
    print(f"모드: {'테스트' if test_mode else '실제'}")
    print("ships_from: AU (고정)")
    print("sold_by: Centrecom (고정)")
    print("차단 감지: title 수집 실패 시")
    print("차단 복구: 3회 새로고침 → 홈페이지 접속 → 첫 URL 재접속")
    if max_items:
        print(f"최대 처리 수: {max_items}개")
    print("=" * 80)

    scraper = CentrecomScraper(country_code)

    if scraper.db_engine is None:
        logger.error("DB 연결 실패로 종료합니다.")
        monitor_and_alert('au_centrecom', 0, None, error_message="DB 연결 실패")
        return

    if test_mode:
        logger.info("테스트 모드 실행 중...")
        test_data = [{
            'url': 'https://www.centrecom.com.au/samsung-t7-2tb-portable-ssd-titan-grey',
            'brand': 'Samsung',
            'item': 'T7 2TB',
            'retailerid': 'TEST001',
            'retailersku': 'TEST001',
            'channel': 'Online',
            'seg_lv1': 'SSD',
            'seg_lv2': 'Consumer',
            'seg_lv3': 'Portable',
            'capacity': '2TB',
            'form_factor': 'External'
        }]

        results_df = scraper.scrape_urls(test_data)
        if results_df is not None and not results_df.empty:
            scraper.analyze_results(results_df)
            scraper.save_results(results_df, save_db=False, upload_server=True)
        return

    logger.info("전체 크롤링 시작")

    # 변수 초기화 (except 블록에서 사용하기 위해)
    urls_data = []
    results_df = None

    try:
        urls_data = scraper.get_crawl_targets(limit=max_items)

        if not urls_data:
            logger.warning("크롤링 대상이 없습니다.")
            monitor_and_alert('au_centrecom', 0, None, error_message="크롤링 대상 URL이 없습니다")
            return

        logger.info(f"크롤링 대상: {len(urls_data)}개")

        results_df = scraper.scrape_urls(urls_data, max_items)

        if results_df is None or results_df.empty:
            logger.error("크롤링 결과가 없습니다.")
            monitor_and_alert('au_centrecom', len(urls_data), None, error_message="크롤링 결과가 없습니다")
            return

        scraper.analyze_results(results_df)

        save_results = scraper.save_results(
            results_df,
            save_db=True,
            upload_server=True
        )

        logger.info("=" * 80)
        logger.info("저장 결과")
        logger.info("=" * 80)
        logger.info(f"DB 저장: {'성공' if save_results['db_saved'] else '실패'}")
        logger.info(f"파일서버 업로드: {'성공' if save_results['server_uploaded'] else '실패'}")

        logger.info("=" * 80)
        logger.info("크롤링 프로세스 완료!")
        logger.info("=" * 80)

        # 알림 발송
        monitor_and_alert('au_centrecom', len(urls_data), results_df)

    except Exception as e:
        # 예외 발생 시 알림
        logger.error(f"크롤링 중 예외 발생: {e}")
        import traceback
        error_detail = traceback.format_exc()
        logger.error(error_detail)
        monitor_and_alert('au_centrecom', len(urls_data), results_df,
                         error_message=str(e))

    finally:
        # 드라이버 종료
        if scraper.driver:
            scraper.driver.quit()
            logger.info("🔧 드라이버 종료")

if __name__ == "__main__":
    required_packages = [
        'undetected-chromedriver',
        'selenium',
        'pandas',
        'pymysql',
        'sqlalchemy',
        'paramiko',
        'openpyxl'
    ]

    print("필요한 패키지:")
    print("pip install " + " ".join(required_packages))
    print("환경변수 설정:")
    print("export COUNTRY_CODE=au  # 호주")
    print("export TEST_MODE=false  # 테스트 모드")
    print("export MAX_ITEMS=10     # 최대 처리 개수 (선택사항)")
    print()

    main()

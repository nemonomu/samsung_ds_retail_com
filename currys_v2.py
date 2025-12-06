"""
Currys 가격 추출 시스템 - DB 기반 버전
DB에서 URL 읽어와서 크롤링 후 결과 저장
파일명 형식: {수집일자}{수집시간}_{국가코드}_{쇼핑몰}.csv
"""
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
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
import zipfile
import hashlib

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# Import database configuration V2
from config import DB_CONFIG_V2 as DB_CONFIG
from config import FILE_SERVER_CONFIG
from alert_monitor import monitor_and_alert

class CurrysScraper:
    def __init__(self):
        self.driver = None
        self.db_engine = None
        self.sftp_client = None
        self.country_code = 'gb'

        # 타임존 설정 추가
        # V2: 타임존 분리 (현지시간 + 한국시간)
        self.korea_tz = pytz.timezone('Asia/Seoul')
        self.local_tz = pytz.timezone('Europe/London')  # Currys 영국 현지 시간

        # DB 연결 설정
        self.setup_db_connection()
        
        # DB에서 XPath 로드
        self.load_xpaths_from_db()
        
    def setup_db_connection(self):
        """DB 연결 설정"""
        try:
            # SQLAlchemy 엔진 생성
            connection_string = (
                f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
                f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            )
            self.db_engine = create_engine(connection_string)
            logger.info("✅ DB 연결 설정 완료")
            
        except Exception as e:
            logger.error(f"❌ DB 연결 실패: {e}")
            self.db_engine = None
    
    def load_xpaths_from_db(self):
        """DB에서 Currys용 선택자 로드"""
        try:
            query = """
            SELECT element_type, selector_value, priority
            FROM mall_selectors
            WHERE mall_name = 'currys'
              AND country_code = 'gb'
              AND is_active = TRUE
            ORDER BY element_type, priority DESC
            """

            df = pd.read_sql(query, self.db_engine)

            # element_type별로 그룹화 (price, title, imageurl 제외)
            self.XPATHS = {}
            for element_type in df['element_type'].unique():
                if element_type not in ['price', 'title', 'imageurl']:
                    type_selectors = df[df['element_type'] == element_type]['selector_value'].tolist()
                    self.XPATHS[element_type] = type_selectors

            logger.info(f"✅ DB에서 선택자 로드 완료: {len(df)}개")

            # price, title, imageurl 선택자는 항상 하드코딩된 값 사용 (DB 무시)
            self.XPATHS['price'] = [
                '/html/body/div[1]/div[4]/div[3]/div[4]/div[1]/div[2]/div[2]/div[2]/div[1]/div[1]/div/div/div/div/div/span/span/span',
                '/html/body/div[1]/div[3]/div[3]/div[4]/div[1]/div[2]/div[2]/div[2]/div[1]/div[1]/div/div/div/div/div/span/span/span',
                '/html/body/div[4]/div[3]/div[3]/div[4]/div[1]/div[2]/div[2]/div[2]/div[1]/div[1]/div/div/div/div/div/span/span/span',
                '/html/body/div[4]/div[3]/div[3]/div[4]/div[1]/div[2]/div[2]/div[2]/div[1]/div[1]/div/div/div/div[1]/div/span[1]/span/span'
            ]

            self.XPATHS['title'] = [
                '//*[@id="js-product-detail"]/div[1]/div[2]/div[1]/div/div[1]/div/h1',
                '/html/body/div[1]/div[3]/div[3]/div[4]/div[1]/div[2]/div[1]/div/div[1]/div/h1',
                '/html/body/div[7]/div[3]/div[3]/div[4]/div[1]/div[2]/div[1]/div/div[1]/div/h1',
                '/html/body/div[4]/div[3]/div[3]/div[4]/div[1]/div[2]/div[1]/div/div[1]/div/h1'
            ]

            self.XPATHS['imageurl'] = [
                '//*[starts-with(@id, "pdpCarousel-")]/div[1]/div/div/div[1]/div/div/a/img',  # 최우선순위 - 범용 ID 기반
                '/html/body/div[1]/div[4]/div[3]/div[4]/div[1]/div[2]/div[1]/div/div[5]/div/div/div[1]/div[1]/div/div/div[1]/div/div/a/img',
                '/html/body/div[1]/div[3]/div[3]/div[4]/div[1]/div[2]/div[1]/div/div[5]/div/div/div[1]/div[1]/div/div/div[1]/div/div/a/img',
                '/html/body/div[7]/div[3]/div[3]/div[4]/div[1]/div[2]/div[1]/div/div[5]/div/div/div[1]/div[1]/div/div/div[1]/div/div/a/img',
                '/html/body/div[4]/div[3]/div[3]/div[4]/div[1]/div[2]/div[1]/div/div[5]/div/div/div[1]/div[1]/div/div/div[1]/div/div/a/img'
            ]

            # 기본값 설정 (DB에 없는 경우 - price, title, imageurl 제외)
            if not self.XPATHS.get('cookie_accept'):
                self.XPATHS['cookie_accept'] = []

        except Exception as e:
            logger.error(f"선택자 로드 실패: {e}")
            # 기본값 사용
            self.XPATHS = {
                'price': [
                    '/html/body/div[1]/div[4]/div[3]/div[4]/div[1]/div[2]/div[2]/div[2]/div[1]/div[1]/div/div/div/div/div/span/span/span'
                ],
                'title': [],
                'imageurl': []
            }
    
    def get_crawl_targets(self, limit=None, include_failed=False):
        """DB에서 크롤링 대상 URL 목록 조회"""
        try:
            if include_failed:
                # 최근 실패한 URL도 포함 (24시간 이내 실패 3회 미만)
                query = """
                WITH failed_counts AS (
                    SELECT url, COUNT(*) as fail_count
                    FROM amazon_crawl_logs
                    WHERE status = 'failed'
                      AND crawl_datetime >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
                      AND country_code = 'gb'
                    GROUP BY url
                )
                SELECT DISTINCT t.*
                FROM samsung_price_tracking_list t
                LEFT JOIN failed_counts f ON t.url = f.url
                WHERE t.country = 'gb'
                  AND t.mall_name = 'currys'
                  AND t.is_active = TRUE
                  AND (f.fail_count IS NULL OR f.fail_count < 3)
                ORDER BY COALESCE(f.fail_count, 0) DESC  -- 실패한 것 우선
                """
            else:
                query = """
                SELECT *
                FROM samsung_price_tracking_list
                WHERE country = 'gb'
                  AND mall_name = 'currys'
                  AND is_active = TRUE
                """
                
            if limit:
                query += f" LIMIT {limit}"
            
            df = pd.read_sql(query, self.db_engine)
            logger.info(f"✅ 크롤링 대상 {len(df)}개 조회 완료")
            return df.to_dict('records')
            
        except Exception as e:
            logger.error(f"크롤링 대상 조회 실패: {e}")
            return []
    
    def setup_driver(self):
        """Chrome 드라이버 설정"""
        logger.info("🔧 Chrome 드라이버 설정 중...")
        
        try:
            self.driver = uc.Chrome()
            self.driver.maximize_window()
            logger.info("✅ 드라이버 설정 완료")
            return True
        except Exception as e:
            logger.error(f"❌ 드라이버 설정 실패: {e}")
            return False
    
    def handle_cookie_consent(self):
        """쿠키 동의 팝업 처리"""
        try:
            # DB에서 쿠키 관련 선택자 조회 (있는 경우)
            cookie_selectors = self.XPATHS.get('cookie_accept', [])
            
            # 기본 선택자 추가
            default_selectors = [
                "button#onetrust-accept-btn-handler",
                "button[aria-label='Accept all cookies']",
                "//button[contains(text(), 'Accept')]"
            ]
            
            all_selectors = cookie_selectors + default_selectors
            
            time.sleep(2)
            
            for selector in all_selectors:
                try:
                    if selector.startswith('//'):
                        element = self.driver.find_element(By.XPATH, selector)
                    else:
                        element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    
                    if element.is_displayed():
                        element.click()
                        logger.info("🍪 쿠키 동의 팝업 처리 완료")
                        time.sleep(1)
                        return True
                except:
                    continue
            
            return False
            
        except Exception as e:
            logger.debug(f"쿠키 팝업 처리 중 오류 (무시): {e}")
            return False
    
    def extract_product_info(self, url, row_data, retry_count=0, max_retries=3):
        """제품 정보 추출 (재시도 로직 포함)"""
        try:
            logger.info(f"🔍 페이지 접속: {url} (시도: {retry_count + 1}/{max_retries + 1})")
            self.driver.get(url)
            
            # 쿠키 동의 팝업 처리
            self.handle_cookie_consent()
            
            # 페이지 로드 대기
            wait = WebDriverWait(self.driver, 20)
            time.sleep(random.uniform(3, 5))
            
            # 현재 시간
            # V2: 타임존 분리

            now_time = datetime.now(self.korea_tz)

            local_time = datetime.now(self.local_tz)


            # ISO 8601 형식

            crawl_dt = local_time.strftime("%Y-%m-%dT%H:%M:%S")

            tz_offset = local_time.strftime("%z")

            tz_formatted = f"{tz_offset[:3]}:{tz_offset[3:]}" if tz_offset else "+00:00"

            crawl_datetime_iso = f"{crawl_dt}{tz_formatted}"


            # 기본 결과 구조
            result = {
                'retailerid': row_data.get('retailerid', ''),
                'country_code': row_data.get('country', 'gb'),
                'ships_from': 'gb',
                'channel_name': 'currys',
                'channel': row_data.get('channel', 'Offline'),
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
                'sold_by': 'Currys',
                'imageurl': None,
                'producturl': url,
                'crawl_datetime': crawl_datetime_iso,
                'crawl_strdatetime': local_time.strftime('%Y%m%d%H%M%S') + f"{local_time.microsecond:06d}"[:4],
                'kr_crawl_datetime': now_time.strftime('%Y-%m-%d %H:%M:%S'),
                'kr_crawl_strdatetime': now_time.strftime('%Y%m%d%H%M%S') + f"{now_time.microsecond:06d}"[:4],
                'title': None,
                'vat': row_data.get('vat', 'o'),
            }
            
            # 가격 추출
            try:
                price_found = False
                for xpath in self.XPATHS.get('price', []):
                    try:
                        price_element = self.driver.find_element(By.XPATH, xpath)
                        price_text = price_element.text.strip()
                        
                        if price_text:
                            price_match = re.search(r'£?([\d,]+\.?\d*)', price_text)
                            if price_match:
                                price_number = price_match.group(1).replace(',', '')
                                result['retailprice'] = float(price_number)
                                logger.info(f"✅ 가격 추출 성공: {result['retailprice']}")
                                price_found = True
                                break
                    except:
                        continue
                
                # CSS 선택자로 재시도
                if not price_found:
                    try:
                        price_element = self.driver.find_element(By.CSS_SELECTOR, "span.pdp-pricing__now-price")
                        price_text = price_element.text
                        if '£' in price_text:
                            price_match = re.search(r'£?([\d,]+\.?\d*)', price_text)
                            if price_match:
                                result['retailprice'] = float(price_match.group(1).replace(',', ''))
                                logger.info(f"✅ 가격 추출 성공 (CSS): {result['retailprice']}")
                    except:
                        logger.warning("모든 가격 추출 방법 실패")
                        
            except Exception as e:
                logger.warning(f"가격 추출 실패: {e}")
            
            # 제목 추출
            try:
                for xpath in self.XPATHS.get('title', []):
                    try:
                        title_element = self.driver.find_element(By.XPATH, xpath)
                        result['title'] = title_element.text.strip()
                        logger.info(f"제목: {result['title']}")
                        break
                    except:
                        continue
            except Exception as e:
                logger.warning(f"제목 추출 실패: {e}")
            
            # 이미지 URL 추출
            try:
                for xpath in self.XPATHS.get('imageurl', []):
                    try:
                        image_element = self.driver.find_element(By.XPATH, xpath)
                        result['imageurl'] = image_element.get_attribute('src')
                        logger.info(f"이미지 URL: {result['imageurl']}")
                        break
                    except:
                        continue
            except Exception as e:
                logger.warning(f"이미지 URL 추출 실패: {e}")
              
            # VAT 텍스트 확인 (DB에서 가져온 경우)
            # vat_texts = []
            # if self.db_engine:
            #     try:
            #         vat_query = """
            #         SELECT selector_value 
            #         FROM mall_selectors
            #         WHERE mall_name = 'currys' 
            #           AND element_type = 'vat_text_list'
            #           AND is_active = TRUE
            #         """
            #         vat_df = pd.read_sql(vat_query, self.db_engine)
            #         vat_texts = vat_df['selector_value'].tolist()
            #     except:
            #         pass
            
            # if vat_texts:
            #     page_source = self.driver.page_source
            #     result['vat'] = 'o' if any(text in page_source for text in vat_texts) else 'x'
            
            return result
            
        except Exception as e:
            logger.error(f"❌ 페이지 처리 오류: {e}")
            
            # 재시도 로직
            if retry_count < max_retries:
                wait_time = (retry_count + 1) * 10  # 재시도마다 대기 시간 증가
                logger.info(f"🔄 {wait_time}초 후 재시도합니다... (재시도 {retry_count + 1}/{max_retries})")
                time.sleep(wait_time)
                
                # 드라이버 새로고침
                try:
                    self.driver.refresh()
                except:
                    # 드라이버가 죽었으면 재시작
                    logger.info("🔧 드라이버 재시작 중...")
                    self.driver.quit()
                    self.setup_driver()
                
                # 재귀 호출로 재시도
                return self.extract_product_info(url, row_data, retry_count + 1, max_retries)
            
            # 최대 재시도 횟수 초과 시 기본값 반환
            logger.error(f"❌ 최대 재시도 횟수 초과: {url}")
            # V2: 타임존 분리

            now_time = datetime.now(self.korea_tz)

            local_time = datetime.now(self.local_tz)

            return {
                'retailerid': row_data.get('retailerid', ''),
                'country_code': row_data.get('country', 'gb'),
                'ships_from': 'gb',
                'channel_name': 'currys',
                'channel': row_data.get('channel', 'Offline'),
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
                'sold_by': 'Currys',
                'imageurl': None,
                'producturl': url,
                'crawl_datetime': crawl_datetime_iso,
                'crawl_strdatetime': local_time.strftime('%Y%m%d%H%M%S') + f"{local_time.microsecond:06d}"[:4],
                'kr_crawl_datetime': now_time.strftime('%Y-%m-%d %H:%M:%S'),
                'kr_crawl_strdatetime': now_time.strftime('%Y%m%d%H%M%S') + f"{now_time.microsecond:06d}"[:4],
                'title': None,
                'vat': row_data.get('vat', 'o')
            }
    
    def save_to_db(self, df):
        """DB에 결과 저장"""
        if self.db_engine is None:
            logger.warning("⚠️ DB 연결이 없어 DB 저장을 건너뜁니다")
            return False
        
        try:
            # currys_price_crawl_tbl_gb_v2 테이블에 저장
            df.to_sql('currys_price_crawl_tbl_gb_v2', self.db_engine, if_exists='append', index=False)
            logger.info(f"✅ DB 저장 완료: {len(df)}개 레코드")
            
            # 크롤링 로그를 pandas DataFrame으로 만들어서 한번에 저장
            log_records = []
            for _, row in df.iterrows():
                log_records.append({
                    'country_code': 'gb',
                    'url': row['producturl'],
                    'status': 'success' if row['retailprice'] is not None else 'failed',
                    'error_message': None if row['retailprice'] is not None else 'Price not found',
                    'execution_time': random.uniform(3, 10),
                    'retailprice': row['retailprice'],
                    'crawl_datetime': row['crawl_datetime']
                })
            
            if log_records:
                log_df = pd.DataFrame(log_records)
                log_df.to_sql('amazon_crawl_logs', self.db_engine, if_exists='append', index=False)
                logger.info(f"✅ 크롤링 로그 저장 완료: {len(log_records)}개")
            
            # 저장된 데이터 확인
            with self.db_engine.connect() as conn:
                count_query = "SELECT COUNT(*) FROM currys_price_crawl_tbl_gb_v2 WHERE DATE(crawl_datetime) = CURDATE()"
                result = conn.execute(count_query)
                today_count = result.scalar()
                logger.info(f"📊 오늘 저장된 총 레코드: {today_count}개")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ DB 저장 실패: {e}")
            import traceback
            logger.error(traceback.format_exc())
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
        base_filename = f"{date_str}_{time_str}_gb_currys"

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
        
        logger.info(f"📊 총 {len(urls_data)}개 제품 처리 시작")
        
        if not self.setup_driver():
            return None
        
        results = []
        failed_urls = []  # 실패한 URL 추적
        
        try:
            for idx, row in enumerate(urls_data):
                logger.info(f"\n{'='*50}")
                logger.info(f"진행률: {idx + 1}/{len(urls_data)} ({(idx + 1)/len(urls_data)*100:.1f}%)")
                
                # URL 추출
                url = row.get('url')
                
                # 제품 정보 추출 (재시도 로직 포함)
                result = self.extract_product_info(url, row)
                
                # 실패 여부 확인
                if result['retailprice'] is None:
                    failed_urls.append({
                        'url': url,
                        'item': row.get('item', ''),
                        'brand': row.get('brand', '')
                    })
                
                results.append(result)
                
                # 10개마다 DB에 중간 저장
                if (idx + 1) % 10 == 0:
                    interim_df = pd.DataFrame(results[-10:])
                    if self.db_engine:
                        try:
                            interim_df.to_sql('currys_price_crawl_tbl_gb_v2', self.db_engine, 
                                            if_exists='append', index=False)
                            logger.info(f"💾 중간 저장: 10개 레코드 DB 저장")
                        except Exception as e:
                            logger.error(f"중간 저장 실패: {e}")
                
                # 다음 요청 전 대기
                if idx < len(urls_data) - 1:
                    wait_time = random.uniform(5, 10)
                    logger.info(f"⏳ {wait_time:.1f}초 대기 중...")
                    time.sleep(wait_time)
                    
                    # 10개마다 긴 휴식
                    if (idx + 1) % 10 == 0:
                        logger.info("☕ 10개 처리 완료, 30초 휴식...")
                        time.sleep(30)
        
        except Exception as e:
            logger.error(f"❌ 스크래핑 중 오류: {e}")
        
        finally:
            # 실패 URL 로그
            if failed_urls:
                logger.warning(f"\n⚠️ 가격 추출 실패한 URL {len(failed_urls)}개:")
                for fail in failed_urls[:5]:  # 처음 5개만 표시
                    logger.warning(f"  - {fail['brand']} {fail['item']}: {fail['url']}")
                if len(failed_urls) > 5:
                    logger.warning(f"  ... 외 {len(failed_urls) - 5}개")
            
            if self.driver:
                self.driver.quit()
                logger.info("🔧 드라이버 종료")
        
        return pd.DataFrame(results)
    
    def analyze_results(self, df):
        """결과 분석"""
        logger.info("\n📊 === 결과 분석 ===")
        
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
            price_df['numeric_price'] = price_df['retailprice']
            
            logger.info(f"\n💰 가격 통계:")
            logger.info(f"평균가: £{price_df['numeric_price'].mean():.2f}")
            logger.info(f"최저가: £{price_df['numeric_price'].min():.2f}")
            logger.info(f"최고가: £{price_df['numeric_price'].max():.2f}")
            logger.info(f"중간값: £{price_df['numeric_price'].median():.2f}")
            
            # 브랜드별 통계
            if 'brand' in df.columns:
                brand_stats = price_df['brand'].value_counts()
                logger.info(f"\n📈 브랜드별 성공:")
                for brand, count in brand_stats.items():
                    logger.info(f"  {brand}: {count}개")
            
            # 용량별 평균 가격
            if 'capacity' in df.columns:
                capacity_stats = price_df.groupby('capacity')['numeric_price'].agg(['mean', 'count'])
                logger.info(f"\n💾 용량별 평균 가격:")
                for capacity, stats in capacity_stats.iterrows():
                    logger.info(f"  {capacity}: £{stats['mean']:.2f} ({int(stats['count'])}개)")


def get_db_history(engine, days=7):
    """DB에서 최근 기록 조회"""
    try:
        query = f"""
        SELECT DATE(crawl_datetime) as date, 
               COUNT(*) as total_count,
               SUM(CASE WHEN retailprice IS NOT NULL THEN 1 ELSE 0 END) as with_price,
               COUNT(DISTINCT brand) as brands,
               COUNT(DISTINCT item) as items
        FROM currys_price_crawl_tbl_gb_v2
        WHERE crawl_datetime >= DATE_SUB(NOW(), INTERVAL {days} DAY)
        GROUP BY DATE(crawl_datetime)
        ORDER BY date DESC
        """
        
        df = pd.read_sql(query, engine)
        logger.info(f"\n📅 최근 {days}일 크롤링 기록:")
        if not df.empty:
            print(df.to_string(index=False))
        else:
            logger.info("최근 크롤링 기록이 없습니다.")
        
    except Exception as e:
        logger.error(f"DB 조회 오류: {e}")

def main():
    """메인 실행 함수"""
    print("\n🚀 Currys 가격 추출 시스템 - DB 기반 버전")
    print("="*60)
    
    # 스크래퍼 초기화
    scraper = CurrysScraper()
    
    if scraper.db_engine is None:
        logger.error("DB 연결 실패로 종료합니다.")
        monitor_and_alert('gb_currys', 0, None, error_message="DB 연결 실패")
        return

    # 최근 크롤링 기록 확인
    get_db_history(scraper.db_engine, 7)
    
    # 1단계: 전체 크롤링 실행
    logger.info("\n📊 1단계: 전체 크롤링 시작")
    urls_data = scraper.get_crawl_targets()
    
    if not urls_data:
        logger.warning("크롤링 대상이 없습니다.")
        monitor_and_alert('gb_currys', 0, None, error_message="크롤링 대상 URL이 없습니다")
        return

    logger.info(f"✅ 크롤링 대상: {len(urls_data)}개")

    # 첫 번째 크롤링 실행
    first_results_df = scraper.scrape_urls(urls_data)
    
    if first_results_df is None or first_results_df.empty:
        logger.error("크롤링 결과가 없습니다.")
        monitor_and_alert('gb_currys', len(urls_data), None, error_message="크롤링 결과가 없습니다")
        return
    
    # 첫 번째 결과 분석
    logger.info("\n📊 1단계 결과:")
    first_failed = first_results_df['retailprice'].isna().sum()
    first_success = first_results_df['retailprice'].notna().sum()
    logger.info(f"성공: {first_success}개, 실패: {first_failed}개")
    
    # 2단계: 실패한 URL 재시도 (실패가 있는 경우만)
    final_results_df = first_results_df.copy()
    
    if first_failed > 0:
        logger.info(f"\n🔄 2단계: 실패한 {first_failed}개 URL 재시도")
        logger.info("60초 대기 후 재시도합니다...")
        time.sleep(60)
        
        # 실패한 URL들만 추출 (원본 urls_data에서)
        failed_product_urls = first_results_df[first_results_df['retailprice'].isna()]['producturl'].tolist()
        
        # 원본 urls_data에서 실패한 URL에 해당하는 데이터만 추출
        failed_urls_data = [
            row for row in urls_data 
            if row.get('url') in failed_product_urls
        ]
        
        if failed_urls_data:
            logger.info(f"재시도 대상: {len(failed_urls_data)}개 (실패한 URL만)")
            
            # 새 드라이버로 재시도
            scraper.driver = None
            retry_results_df = scraper.scrape_urls(failed_urls_data)
            
            if retry_results_df is not None and not retry_results_df.empty:
                # 재시도 결과 분석
                retry_success = retry_results_df['retailprice'].notna().sum()
                retry_failed = retry_results_df['retailprice'].isna().sum()
                logger.info(f"\n📊 재시도 결과: 성공 {retry_success}개, 실패 {retry_failed}개")
                
                # 기존 실패한 결과를 재시도 결과로 업데이트
                for _, retry_row in retry_results_df.iterrows():
                    if retry_row['retailprice'] is not None:
                        # 성공한 경우 기존 데이터 업데이트
                        mask = final_results_df['producturl'] == retry_row['producturl']
                        if mask.any():
                            final_results_df.loc[mask, 'retailprice'] = retry_row['retailprice']
                            final_results_df.loc[mask, 'title'] = retry_row['title']
                            final_results_df.loc[mask, 'imageurl'] = retry_row['imageurl']
                            final_results_df.loc[mask, 'crawl_datetime'] = retry_row['crawl_datetime']
                            final_results_df.loc[mask, 'crawl_strdatetime'] = retry_row['crawl_strdatetime']
    
    # 3단계: 최종 결과 저장
    logger.info("\n💾 3단계: 최종 결과 저장")
    
    # 최종 통계
    final_success = final_results_df['retailprice'].notna().sum()
    final_failed = final_results_df['retailprice'].isna().sum()
    success_rate = (final_success / len(final_results_df) * 100) if len(final_results_df) > 0 else 0
    
    logger.info(f"\n📊 === 최종 결과 ===")
    logger.info(f"전체: {len(final_results_df)}개")
    logger.info(f"성공: {final_success}개")
    logger.info(f"실패: {final_failed}개")
    logger.info(f"성공률: {success_rate:.1f}%")
    
    # 개선율 표시
    if first_failed > 0 and first_failed > final_failed:
        improvement = first_failed - final_failed
        logger.info(f"✨ 재시도로 {improvement}개 추가 성공!")
    
    # DB와 파일서버에 최종 결과 저장
    save_results = scraper.save_results(
        final_results_df,
        save_db=True,
        upload_server=True
    )
    
    # 상세 분석
    scraper.analyze_results(final_results_df)
    
    # 저장 결과 출력
    logger.info("\n📊 저장 결과:")
    logger.info(f"DB 저장: {'✅ 성공' if save_results['db_saved'] else '❌ 실패'}")
    logger.info(f"파일서버 업로드: {'✅ 성공' if save_results['server_uploaded'] else '❌ 실패'}")
    
    # 여전히 실패한 URL 로그
    if final_failed > 0:
        logger.warning(f"\n⚠️ 최종적으로 {final_failed}개 URL에서 가격 추출 실패")
        failed_items = final_results_df[final_results_df['retailprice'].isna()]
        logger.warning("실패 목록 (상위 5개):")
        for idx, row in failed_items.head().iterrows():
            logger.warning(f"  - {row['brand']} {row['item']}: {row['producturl'][:50]}...")
    
    logger.info("\n✅ 크롤링 프로세스 완료!")

    # 크롤링 완료 후 알림 (빈 값 50% 이상 시 경고)
    monitor_and_alert('gb_currys', len(urls_data), final_results_df)

if __name__ == "__main__":
    # 필요한 패키지 설치 확인
    required_packages = [
        'undetected-chromedriver',
        'selenium',
        'pandas',
        'pymysql',
        'sqlalchemy',
        'paramiko',
        'openpyxl'
    ]
    
    print("📦 필요한 패키지:")
    print("pip install " + " ".join(required_packages))
    print("\n⚠️ DB 설정을 먼저 확인하세요:")
    print("DB_CONFIG 딕셔너리의 user, password, host 정보를 실제 값으로 변경해야 합니다.")
    print()
    
    main()
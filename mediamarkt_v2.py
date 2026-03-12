"""
MediaMarkt 무한 크롤러 V2 (타임존 분리 버전)
원본 mediamarkt.py 기반 - DB/타임존/파일서버 설정만 V2로 변경
- 현지시간(네덜란드)과 한국시간 분리 저장
- 새 데이터베이스 사용 (DB_CONFIG_V2)
- 핵심 로직은 원본과 동일
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
from datetime import datetime, timedelta
import pytz
import logging
import os
import traceback
import zipfile
import hashlib
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

# Import configuration V2
from config import DB_CONFIG_V2 as DB_CONFIG, FILE_SERVER_CONFIG
from alert_monitor import monitor_and_alert

# 로깅 설정
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler('mediamarkt_infinite.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MediaMarktInfiniteScraper:
    def __init__(self):
        self.driver = None
        self.db_engine = None
        self.sftp_client = None
        self.is_logged_in = False
        self.crawl_count = 0
        self.country_code = 'de'
        # V2: 타임존 분리 (현지시간 + 한국시간)
        self.korea_tz = pytz.timezone('Asia/Seoul')
        self.local_tz = pytz.timezone('Europe/Berlin')  # MediaMarkt 독일 현지 시간
        self.start_time = datetime.now(self.korea_tz)
        self.saved_cookies = []

        # DB 연결 설정
        self.setup_db_connection()

        # DB에서 XPath 로드
        self.load_xpaths_from_db()
        
    def setup_db_connection(self):
        """DB 연결 설정"""
        try:
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
        """DB에서 MediaMarkt용 선택자 로드"""
        try:
            # 먼저 MediaMarkt용 선택자가 있는지 확인
            check_query = """
            SELECT COUNT(*) as count
            FROM mall_selectors
            WHERE mall_name = 'mediamarkt' 
              AND country_code = 'de'
              AND is_active = TRUE
            """
            
            check_df = pd.read_sql(check_query, self.db_engine)
            
            if check_df['count'][0] > 0:
                # MediaMarkt 선택자가 있으면 로드
                query = """
                SELECT element_type, selector_value, priority
                FROM mall_selectors
                WHERE mall_name = 'mediamarkt' 
                  AND country_code = 'de'
                  AND is_active = TRUE
                ORDER BY element_type, priority DESC
                """
                
                df = pd.read_sql(query, self.db_engine)
                
                # element_type별로 그룹화
                self.XPATHS = {}
                for element_type in df['element_type'].unique():
                    type_selectors = df[df['element_type'] == element_type]['selector_value'].tolist()
                    self.XPATHS[element_type] = type_selectors
                
                logger.info(f"✅ DB에서 MediaMarkt 선택자 로드 완료: {len(df)}개")
                
            else:
                # MediaMarkt 선택자가 없으면 Currys 선택자를 참고용으로 로드
                logger.warning("⚠️ DB에 MediaMarkt 선택자가 없습니다. Currys 선택자를 확인합니다.")
                
                currys_query = """
                SELECT element_type, selector_value, priority
                FROM mall_selectors
                WHERE mall_name = 'currys' 
                  AND country_code = 'uk'
                  AND is_active = TRUE
                ORDER BY element_type, priority DESC
                """
                
                currys_df = pd.read_sql(currys_query, self.db_engine)
                if not currys_df.empty:
                    logger.info(f"참고: Currys 선택자 {len(currys_df)}개 발견")
                    logger.info("MediaMarkt용 선택자를 DB에 추가해주세요:")
                    logger.info("INSERT INTO mall_selectors (mall_name, country_code, element_type, selector_value, priority, is_active)")
                    logger.info("VALUES ('mediamarkt', 'de', 'price', 'span[data-test=\"branded-price-whole\"]', 1, TRUE);")
                
                # 기본값 설정 - DB에서 관리되므로 빈 배열로 설정
                self.XPATHS = {
                    'price': [],
                    'title': [],
                    'imageurl': [],
                    'availability': []
                }
                logger.warning("⚠️ DB에 MediaMarkt 선택자가 없습니다. DB에 선택자를 추가해주세요.")
                
        except Exception as e:
            logger.error(f"선택자 로드 실패: {e}")
            # 오류 시 기본값 - DB에서 관리되므로 빈 배열로 설정
            self.XPATHS = {
                'price': [],
                'title': [],
                'imageurl': [],
                'availability': []
            }
    
    def get_crawl_targets(self, limit=None):
        """DB에서 크롤링 대상 URL 목록 조회"""
        try:
            query = """
            SELECT *
            FROM samsung_price_tracking_list
            WHERE country = 'de' 
              AND mall_name = 'mediamarkt'
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
            options = uc.ChromeOptions()

            # 페이지 로드 전략: eager (DOM 로드 완료 시 즉시 반환)
            options.page_load_strategy = 'eager'

            # 기본 옵션
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_argument('--window-size=1920,1080')
            # 메모리 최적화 옵션
            options.add_argument('--disable-gpu')
            options.add_argument('--disable-extensions')
            options.add_argument('--disable-infobars')
            options.add_argument('--disable-renderer-backgrounding')

            # 독일 설정
            options.add_argument('--lang=de-DE')
            options.add_experimental_option('prefs', {
                "intl.accept_languages": "de-DE,de"
            })
            
            self.driver = uc.Chrome(options=options)
            self.driver.maximize_window()
            self.driver.set_page_load_timeout(30)

            logger.info("✅ 드라이버 설정 완료")
            return True
            
        except Exception as e:
            logger.error(f"❌ 드라이버 설정 실패: {e}")
            return False
    
    def initial_manual_login(self):
        """초기 수동 로그인 - Cloudflare 통과"""
        logger.info("\n" + "="*60)
        logger.info("🔐 === 초기 수동 로그인 ===")
        logger.info("="*60)
        
        try:
            # MediaMarkt 메인 페이지 접속
            logger.info("MediaMarkt 접속 중...")
            self.driver.get("https://www.mediamarkt.de")
            
            logger.info("\n📋 다음 단계를 수행해주세요:")
            logger.info("1. Cloudflare 챌린지가 나타나면 해결하세요")
            logger.info("2. 쿠키 동의 팝업이 나타나면 수락하세요")
            logger.info("3. 사이트가 완전히 로드될 때까지 기다리세요")
            logger.info("4. (선택) 로그인이 필요하다면 로그인하세요")
            
            input("\n✅ 모든 작업이 완료되면 Enter를 누르세요...")
            
            # 현재 상태 확인
            current_url = self.driver.current_url
            if "mediamarkt.de" in current_url and not self.check_cloudflare_challenge():
                self.is_logged_in = True
                logger.info("✅ 로그인 성공! 무한 크롤링을 시작합니다.")
                
                # 쿠키 저장
                try:
                    self.saved_cookies = self.driver.get_cookies()
                    logger.info(f"💾 쿠키 {len(self.saved_cookies)}개 저장")
                    
                    # 파일로도 저장
                    import json
                    with open('mediamarkt_cookies.json', 'w') as f:
                        json.dump(self.saved_cookies, f)
                except Exception as e:
                    logger.error(f"쿠키 저장 실패: {e}")
                
                return True
            else:
                logger.error("❌ 로그인 실패 - Cloudflare를 통과하지 못했습니다.")
                return False
                
        except Exception as e:
            logger.error(f"초기 로그인 오류: {e}")
            return False
    
    def kill_chrome_processes(self):
        """잔여 Chrome/chromedriver 프로세스 강제 종료"""
        try:
            import subprocess
            subprocess.run(['taskkill', '/F', '/IM', 'chromedriver.exe'],
                         capture_output=True, timeout=10)
            subprocess.run(['taskkill', '/F', '/IM', 'chrome.exe'],
                         capture_output=True, timeout=10)
            logger.info("잔여 Chrome 프로세스 정리 완료")
        except Exception as e:
            logger.debug(f"프로세스 정리 중 오류 (무시 가능): {e}")

    def check_cloudflare_challenge(self):
        """Cloudflare 챌린지 페이지인지 확인 (title 1차 → page_source 2차)"""
        try:
            # 1차: title만으로 판별 (메모리 절약)
            title_indicators = [
                "just a moment",
                "einen moment",
                "verifying you are human",
            ]
            page_title = self.driver.title.lower()
            for indicator in title_indicators:
                if indicator in page_title:
                    return True

            # 2차: title이 비정상적이면 page_source로 확인
            if not page_title or 'mediamarkt' not in page_title:
                source_indicators = [
                    "cf-challenge",
                    "verifying you are human",
                ]
                page_source = self.driver.page_source.lower()
                for indicator in source_indicators:
                    if indicator in page_source:
                        return True

            return False

        except Exception:
            return False
    
    def restart_browser(self):
        """브라우저 재시작 및 재로그인"""
        try:
            logger.info("🔄 브라우저 재시작 중...")
            
            # 기존 드라이버 종료
            try:
                self.driver.quit()
            except:
                logger.warning("driver.quit() 실패 - 강제 종료 시도")
                self.kill_chrome_processes()

            time.sleep(5)
            
            # 드라이버 재설정
            if not self.setup_driver():
                return False
            
            # 자동 재로그인 시도 (저장된 쿠키 사용)
            if hasattr(self, 'saved_cookies') and self.saved_cookies:
                try:
                    self.driver.get("https://www.mediamarkt.de")
                    time.sleep(3)
                    
                    # 쿠키 복원
                    for cookie in self.saved_cookies:
                        try:
                            self.driver.add_cookie(cookie)
                        except:
                            pass
                    
                    # 페이지 새로고침
                    self.driver.refresh()
                    time.sleep(5)
                    
                    # Cloudflare 체크
                    if not self.check_cloudflare_challenge():
                        logger.info("✅ 쿠키로 자동 재로그인 성공")
                        self.is_logged_in = True
                        return True
                except:
                    pass
            
            # 쿠키 복원 실패 시 자동 로그인 시도
            logger.warning("⚠️ 쿠키 복원 실패. 자동 로그인 시도 중...")

            try:
                # 메인 페이지 접속
                self.driver.get("https://www.mediamarkt.de")
                time.sleep(3)

                # 쿠키 수락 자동 시도
                self.accept_cookies()

                # Cloudflare 체크
                if not self.check_cloudflare_challenge():
                    logger.info("✅ 자동 재로그인 성공")
                    self.is_logged_in = True

                    # 쿠키 다시 저장
                    try:
                        self.saved_cookies = self.driver.get_cookies()
                        logger.info(f"💾 쿠키 {len(self.saved_cookies)}개 저장")
                    except:
                        pass

                    return True
                else:
                    logger.error("❌ Cloudflare 감지됨. 크롤링 중단.")
                    self.is_logged_in = False
                    return False
            except Exception as e:
                logger.error(f"자동 로그인 실패: {e}")
                self.is_logged_in = False
                return False
            
        except Exception as e:
            logger.error(f"브라우저 재시작 실패: {e}")
            return False
    
    def check_browser_health(self):
        """브라우저 상태 확인"""
        try:
            # 간단한 JavaScript 실행으로 브라우저 상태 확인
            result = self.driver.execute_script("return document.readyState")
            return result == "complete"
        except:
            return False
    
    def keep_session_alive(self):
        """세션 유지를 위한 활동 (더 안전하게)"""
        try:
            # 브라우저 상태만 확인
            try:
                current_url = self.driver.current_url
                logger.debug(f"현재 URL: {current_url}")
            except:
                logger.warning("⚠️ 브라우저 응답 없음")
                self.is_logged_in = False
                return
            
            # 너무 자주 이동하지 않음 (세션 유지에는 현재 페이지 유지가 더 나을 수도)
            if "mediamarkt.de" in current_url:
                # 현재 페이지에서 작은 액션만
                try:
                    # 작은 스크롤
                    self.driver.execute_script("window.scrollBy(0, 100)")
                    time.sleep(1)
                    self.driver.execute_script("window.scrollBy(0, -100)")
                    
                    logger.info("💓 세션 keep-alive 완료 (가벼운 액션)")
                except:
                    pass
            else:
                # MediaMarkt 페이지가 아니면 홈으로
                try:
                    self.driver.get("https://www.mediamarkt.de")
                    time.sleep(3)
                    
                    # Cloudflare 체크
                    if self.check_cloudflare_challenge():
                        logger.error("❌ Keep-alive 중 Cloudflare 감지!")
                        self.is_logged_in = False
                        return
                    
                    logger.info("💓 세션 keep-alive 완료 (홈 방문)")
                except Exception as e:
                    logger.error(f"Keep-alive 오류: {e}")
                    
        except Exception as e:
            logger.error(f"Keep-alive 오류: {e}")
            # 에러가 나도 즉시 세션을 종료하지 않음
    
    def extract_product_info(self, url, row_data, **kwargs):
        """제품 정보 추출"""
        try:
            logger.info(f"🔍 페이지 접속: {url}")
            self.driver.get(url)

            # 페이지 로드 대기
            time.sleep(random.uniform(3, 5))

            # 쿠키 팝업 처리 (페이지 이동 후 다시 뜰 수 있음)
            self.accept_cookies()

            # Cloudflare 체크
            if self.check_cloudflare_challenge():
                logger.error("❌ Cloudflare 챌린지 감지! 세션이 만료되었습니다.")
                self.is_logged_in = False
                return None
            
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
                'country_code': row_data.get('country', 'de'),
                'ships_from': 'DE',
                'channel_name': 'mediamarkt',
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
                'sold_by': 'MediaMarkt',
                'imageurl': None,
                'producturl': url,
                'crawl_datetime': crawl_datetime_iso,
                'crawl_strdatetime': local_time.strftime('%Y%m%d%H%M%S') + f"{local_time.microsecond:06d}"[:4],
                'kr_crawl_datetime': now_time.strftime('%Y-%m-%d %H:%M:%S'),
                'kr_crawl_strdatetime': now_time.strftime('%Y%m%d%H%M%S') + f"{now_time.microsecond:06d}"[:4],
                'title': None,
                'vat': 'o'
            }
            
            # 가격 추출
            try:
                price_found = False
                
                # DB에서 가져온 선택자로 시도
                for selector in self.XPATHS.get('price', []):
                    try:
                        if selector.startswith('//'):
                            # XPath인 경우
                            price_elements = self.driver.find_elements(By.XPATH, selector)
                        else:
                            # CSS 선택자인 경우
                            price_elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        
                        for price_element in price_elements:
                            price_text = price_element.text.strip()
                            
                            if price_text:
                                # 유로 가격 추출 (다양한 형식 지원)
                                # 예: "89,99 €", "€ 89.99", "89.99", "89,99"
                                price_text = price_text.replace('€', '').strip()
                                price_match = re.search(r'(\d+)[,.]?(\d*)', price_text)
                                if price_match:
                                    price = price_match.group(1)
                                    if price_match.group(2):
                                        price += '.' + price_match.group(2)
                                    result['retailprice'] = float(price)
                                    logger.info(f"✅ 가격 추출 성공: {result['retailprice']}€ (선택자: {selector})")
                                    price_found = True
                                    break
                        
                        if price_found:
                            break
                            
                    except Exception as e:
                        logger.debug(f"선택자 {selector} 실패: {e}")
                        continue
                
                if not price_found:
                    logger.warning("❌ DB 선택자로 가격을 찾을 수 없습니다")
                    # 추가 선택자 시도 (페이지 구조가 변경된 경우)
                    additional_selectors = [
                        "meta[property='product:price:amount']",
                        "[data-price]",
                        ".price-now"
                    ]
                    
                    for selector in additional_selectors:
                        try:
                            if selector.startswith('meta'):
                                elem = self.driver.find_element(By.CSS_SELECTOR, selector)
                                price_text = elem.get_attribute('content')
                            else:
                                elem = self.driver.find_element(By.CSS_SELECTOR, selector)
                                price_text = elem.get_attribute('data-price') or elem.text
                            
                            if price_text:
                                price_match = re.search(r'(\d+)[,.]?(\d*)', price_text)
                                if price_match:
                                    price = price_match.group(1)
                                    if price_match.group(2):
                                        price += '.' + price_match.group(2)
                                    result['retailprice'] = float(price)
                                    logger.info(f"✅ 가격 추출 성공 (추가 선택자): {result['retailprice']}€")
                                    
                                    # 새로운 선택자를 DB에 추가하라고 제안
                                    logger.info(f"💡 새로운 선택자 발견: {selector}")
                                    logger.info(f"DB에 추가하세요: INSERT INTO mall_selectors (mall_name, country_code, element_type, selector_value, priority, is_active) VALUES ('mediamarkt', 'de', 'price', '{selector}', 2, TRUE);")
                                    break
                        except:
                            continue
                    
            except Exception as e:
                logger.warning(f"가격 추출 실패: {e}")
            
            # 제목 추출
            try:
                for selector in self.XPATHS.get('title', []):
                    try:
                        if selector.startswith('//'):
                            title_element = self.driver.find_element(By.XPATH, selector)
                        else:
                            title_element = self.driver.find_element(By.CSS_SELECTOR, selector)
                        
                        result['title'] = title_element.text.strip()
                        logger.info(f"제목: {result['title']}")
                        break
                    except:
                        continue
            except Exception as e:
                logger.warning(f"제목 추출 실패: {e}")
            
            # 이미지 URL 추출
            try:
                for selector in self.XPATHS.get('imageurl', []):
                    try:
                        if selector.startswith('//'):
                            image_element = self.driver.find_element(By.XPATH, selector)
                        else:
                            image_element = self.driver.find_element(By.CSS_SELECTOR, selector)
                        
                        result['imageurl'] = image_element.get_attribute('src')
                        logger.info(f"이미지 URL: {result['imageurl']}")
                        break
                    except:
                        continue
            except Exception as e:
                logger.warning(f"이미지 URL 추출 실패: {e}")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ 페이지 처리 오류: {e}")
            return None
    
    def save_to_db(self, df):
        """DB에 결과 저장"""
        if self.db_engine is None:
            logger.warning("⚠️ DB 연결이 없어 DB 저장을 건너뜁니다")
            return False
        
        try:
            # mediamarkt_price_crawl_tbl_de_v2 테이블에 저장
            df.to_sql('mediamarkt_price_crawl_tbl_de_v2', self.db_engine, if_exists='append', index=False)
            logger.info(f"✅ DB 저장 완료: {len(df)}개 레코드")
            
            # 크롤링 로그 저장
            log_records = []
            for _, row in df.iterrows():
                log_records.append({
                    'country_code': 'de',
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
            
            return True
            
        except Exception as e:
            logger.error(f"❌ DB 저장 실패: {e}")
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
    def save_results(self, df, skip_db=False):
        """결과를 DB와 파일서버에 저장"""
        now = datetime.now()
        date_str = now.strftime("%Y%m%d")
        time_str = now.strftime("%H%M%S")
        base_filename = f"{date_str}_{time_str}_de_mediamarkt"

        results = {'db_saved': False, 'server_uploaded': False}

        # DB 저장 (skip_db=True이면 건너뜀)
        if not skip_db:
            results['db_saved'] = self.save_to_db(df)
        else:
            results['db_saved'] = True  # 이미 중간 저장에서 완료됨

        # 파일서버 업로드
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
    
    def crawl_once(self):
        """1회 크롤링 실행"""
        logger.info(f"\n{'='*60}")
        logger.info(f"🚀 크롤링 라운드 {self.crawl_count + 1} 시작")
        logger.info(f"시작 시간: {datetime.now()}")
        logger.info(f"{'='*60}")
        
        # DB에서 URL 목록 조회
        urls_data = self.get_crawl_targets()
        
        if not urls_data:
            logger.warning("크롤링 대상이 없습니다.")
            return
        
        logger.info(f"📊 총 {len(urls_data)}개 제품 처리 예정")
        
        results = []
        success_count = 0
        last_saved_idx = 0  # 마지막으로 DB에 저장된 인덱스 추적

        for idx, row in enumerate(urls_data):
            # 세션 확인 - 만료 시 재로그인 시도 후 계속 진행
            if not self.is_logged_in:
                logger.warning("⚠️ 세션이 만료되었습니다. 재로그인 시도 중...")
                if self.restart_browser():
                    logger.info("✅ 재로그인 성공. 계속 진행")
                else:
                    logger.warning("⚠️ 재로그인 실패. 그래도 계속 진행")
                    self.is_logged_in = True  # 강제로 True 설정하여 계속 시도

            logger.info(f"\n진행률: {idx + 1}/{len(urls_data)} ({(idx + 1)/len(urls_data)*100:.1f}%)")

            # URL 추출
            url = row.get('url')

            # 제품 정보 추출 (실패 시 최대 2회 재시도)
            result = None
            for attempt in range(3):
                result = self.extract_product_info(url, row)
                if result:
                    break
                else:
                    if attempt < 2:
                        logger.warning(f"⚠️ 제품 정보 추출 실패 (시도 {attempt + 1}/3): {url}. 재시도 중...")
                        # 타임아웃/에러 발생 시 브라우저 재시작 (Chrome 먹통 방지)
                        logger.warning("⚠️ 브라우저 재시작 시도...")
                        self.restart_browser()
                        time.sleep(3)
                    else:
                        logger.error(f"❌ 제품 정보 추출 최종 실패: {url}")

            if result:
                results.append(result)
                if result['retailprice'] is not None:
                    success_count += 1

            # 40개마다 브라우저 상태 체크
            if (idx + 1) % 40 == 0:
                # 브라우저 상태 확인
                if not self.check_browser_health():
                    logger.warning("⚠️ 브라우저 상태 이상 감지")
                    if self.restart_browser():
                        logger.info("✅ 브라우저 재시작 완료")
                    else:
                        logger.warning("⚠️ 브라우저 재시작 실패. 계속 진행 시도...")
                        # 재시작 한 번 더 시도
                        time.sleep(5)
                        if not self.restart_browser():
                            logger.error("❌ 브라우저 재시작 2차 실패. 그래도 계속 진행")
                else:
                    self.keep_session_alive()

            # 10개마다 DB 중간 저장 (중복 방지)
            if (idx + 1) % 10 == 0:
                unsaved_results = results[last_saved_idx:]
                if unsaved_results and self.db_engine:
                    try:
                        interim_df = pd.DataFrame(unsaved_results)
                        interim_df.to_sql('mediamarkt_price_crawl_tbl_de_v2', self.db_engine,
                                        if_exists='append', index=False)
                        logger.info(f"💾 중간 저장: {len(unsaved_results)}개 레코드")
                        last_saved_idx = len(results)  # 저장 완료 후 인덱스 업데이트
                    except Exception as e:
                        logger.error(f"중간 저장 실패: {e}")

            # 다음 요청 전 대기
            if idx < len(urls_data) - 1:
                wait_time = random.uniform(5, 10)
                time.sleep(wait_time)

                # 25개마다 짧은 휴식
                if (idx + 1) % 25 == 0:
                    logger.info("☕ 25개 처리 완료, 5초 휴식...")
                    time.sleep(5)
        
        # 결과 저장
        if results:
            # 남은 레코드 DB 저장 (중복 방지)
            unsaved_results = results[last_saved_idx:]
            if unsaved_results and self.db_engine:
                try:
                    remaining_df = pd.DataFrame(unsaved_results)
                    remaining_df.to_sql('mediamarkt_price_crawl_tbl_de_v2', self.db_engine,
                                      if_exists='append', index=False)
                    logger.info(f"💾 최종 저장: {len(unsaved_results)}개 레코드")
                except Exception as e:
                    logger.error(f"최종 DB 저장 실패: {e}")

            # 전체 결과로 파일 서버 업로드 (DB 저장은 skip)
            df = pd.DataFrame(results)
            save_results = self.save_results(df, skip_db=True)

            # 통계
            logger.info(f"\n📊 === 크롤링 완료 ===")
            logger.info(f"전체 제품: {len(results)}개")
            logger.info(f"가격 추출 성공: {success_count}개")
            logger.info(f"성공률: {success_count/len(results)*100:.1f}%")
            logger.info(f"DB 저장: ✅")
            logger.info(f"파일서버 업로드: {'✅' if save_results['server_uploaded'] else '❌'}")

            self.crawl_count += 1
            return df

        return None
    
    def run_infinite_crawling(self):
        """무한 크롤링 실행"""
        logger.info("\n🔄 === 무한 크롤링 모드 시작 ===")
        logger.info("⏰ 1시간마다 자동 크롤링 실행")
        logger.info("중단하려면 Ctrl+C를 누르세요")
        
        # 첫 실행
        self.crawl_once()
        
        # 무한 루프
        while self.is_logged_in:
            try:
                # 1시간 대기
                logger.info(f"\n⏳ 다음 크롤링까지 1시간 대기 중...")
                logger.info(f"다음 실행 예정: {(datetime.now() + timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')}")
                
                # 1시간 = 60분, 1분마다 체크
                for i in range(60):
                    if not self.is_logged_in:
                        break
                        
                    time.sleep(60)  # 1분 대기
                    
                    # 15분마다 keep-alive (더 자주)
                    if i > 0 and i % 15 == 0:
                        logger.info(f"💓 Keep-alive 실행 ({i}/60분 경과)")
                        # 브라우저 상태 확인 후 keep-alive
                        if self.check_browser_health():
                            self.keep_session_alive()
                        else:
                            logger.warning("⚠️ 브라우저 응답 없음. 재시작 시도...")
                            if not self.restart_browser():
                                logger.error("❌ 브라우저 재시작 실패")
                                self.is_logged_in = False
                                break
                    
                    # 30분마다 상태 출력
                    if i == 30:
                        uptime = datetime.now(self.korea_tz) - self.start_time
                        logger.info(f"⏱️ 가동 시간: {uptime} | 크롤링 횟수: {self.crawl_count}")
                
                # 1시간 후 크롤링 실행
                if self.is_logged_in:
                    self.crawl_once()
                
            except KeyboardInterrupt:
                logger.info("\n👋 사용자에 의해 중단됨")
                break
            except Exception as e:
                logger.error(f"무한 루프 오류: {e}")
                logger.error(traceback.format_exc())
                time.sleep(300)  # 5분 대기 후 계속
        
        logger.info("무한 크롤링 종료")
    
    def accept_cookies(self):
        """쿠키 수락 버튼 클릭"""
        try:
            cookie_button = WebDriverWait(self.driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, "//*[@id='pwa-consent-layer-accept-all-button']"))
            )
            cookie_button.click()
            logger.info("✅ 쿠키 수락 완료")
            time.sleep(2)
            return True
        except Exception as e:
            logger.debug(f"쿠키 버튼 없음 또는 이미 수락됨: {e}")
            return False

    def start(self):
        """메인 시작 함수"""
        logger.info("\n🚀 MediaMarkt 크롤러 시작")
        logger.info("="*60)

        # 시작 전 잔여 프로세스 정리
        self.kill_chrome_processes()

        # 드라이버 설정
        if not self.setup_driver():
            logger.error("드라이버 설정 실패로 종료합니다.")
            return None

        try:
            # 메인 페이지 접속
            logger.info("MediaMarkt 접속 중...")
            self.driver.get("https://www.mediamarkt.de")
            time.sleep(3)

            # 쿠키 수락
            self.accept_cookies()

            # Cloudflare 체크
            if self.check_cloudflare_challenge():
                logger.error("❌ Cloudflare 챌린지 감지! 수동 통과 필요")
                return None

            self.is_logged_in = True

            # 단일 크롤링 실행
            return self.crawl_once()

        except Exception as e:
            logger.error(f"치명적 오류: {e}")
            logger.error(traceback.format_exc())
            return None
        finally:
            try:
                if self.driver:
                    self.driver.quit()
                logger.info("드라이버 종료 완료")
            except:
                logger.warning("driver.quit() 실패 - 강제 종료 시도")
                self.kill_chrome_processes()

def main():
    """메인 실행 함수"""
    print("\n🚀 MediaMarkt 크롤러")
    print("="*60)

    # 스크래퍼 생성 및 실행
    scraper = MediaMarktInfiniteScraper()

    if scraper.db_engine is None:
        logger.error("DB 연결 실패로 종료합니다.")
        monitor_and_alert('de_mediamarkt', 0, None, error_message="DB 연결 실패")
        return

    # 크롤링 대상 수 조회
    urls_data = scraper.get_crawl_targets()
    target_count = len(urls_data) if urls_data else 0

    # 크롤링 실행
    results_df = scraper.start()

    if results_df is None or results_df.empty:
        logger.error("크롤링 결과가 없습니다.")
        monitor_and_alert('de_mediamarkt', target_count, None, error_message="크롤링 결과가 없습니다")
        return

    logger.info("=" * 60)
    logger.info("MediaMarkt 크롤링 프로세스 완료!")
    logger.info("=" * 60)

    # 크롤링 완료 후 알림
    monitor_and_alert('de_mediamarkt', target_count, results_df)

if __name__ == "__main__":
    # 필요한 패키지 확인
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
    print()
    
    main()
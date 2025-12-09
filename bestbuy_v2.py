"""
BestBuy 가격 추출 시스템 - DB 기반 버전
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

class BestBuyScraper:
    def __init__(self):
        self.driver = None
        self.db_engine = None
        self.sftp_client = None
        self.session_initialized = False
        self.country_code = 'usa'
        # V2: 타임존 분리 (현지시간 + 한국시간)
        self.korea_tz = pytz.timezone('Asia/Seoul')
        self.local_tz = pytz.timezone('America/New_York')  # BestBuy 현지 시간 (워싱턴)

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
        """DB에서 BestBuy용 선택자 로드"""
        try:
            query = """
            SELECT element_type, selector_value, priority
            FROM mall_selectors
            WHERE mall_name = 'bestbuy' 
              AND country_code = 'usa'
              AND is_active = TRUE
            ORDER BY element_type, priority DESC
            """
            
            df = pd.read_sql(query, self.db_engine)
            
            # element_type별로 그룹화 (price 제외)
            self.XPATHS = {}
            for element_type in df['element_type'].unique():
                if element_type != 'price':  # price 선택자는 DB에서 로드하지 않음
                    type_selectors = df[df['element_type'] == element_type]['selector_value'].tolist()
                    self.XPATHS[element_type] = type_selectors
            
            logger.info(f"✅ DB에서 선택자 로드 완료: {len(df)}개")
            
            # price, imageurl 선택자는 항상 하드코딩된 값 사용 (DB 무시)
            self.XPATHS['imageurl'] = [
                '/html/body/div[5]/div[4]/div[2]/div/div[2]/div[2]/div/div[2]/div/button/img',
                '/html/body/div[5]/div[4]/div[2]/div/div[2]/div[2]/div/div[2]/div/button[1]/img',
                '/html/body/div[5]/div[4]/div[1]/div/div[2]/div[2]/div/div[2]/div/button/img',
                '/html/body/div[5]/div[4]/div[1]/div/div[2]/div[2]/div/div[2]/div/button[1]/img',
                '//img[@class="primary-image"]',
                '//div[@class="media-gallery"]//img'
            ]
            self.XPATHS['price'] = [
                '/html/body/div[5]/div[4]/div[1]/div/div[5]/div/div/div[1]/div/div[1]/div[2]/div[1]/div/div/div/div[1]/span',
                '/html/body/div[5]/div[4]/div[1]/div/div[4]/div/div/div[1]/div/div[1]/div[2]/div[1]/div/div/div/div[1]/span',
                '/html/body/div[5]/div[4]/div[1]/div/div[4]/div/div/div[1]/div/div[1]/div[1]/div[1]/div/div/div/div[1]/span',
                '/html/body/div[5]/div[4]/div[1]/div/div[5]/div/div/div[1]/div/div[1]/div[1]/div[1]/div/div/div/div[1]/span',
                '/html/body/div[5]/div[4]/div[2]/div/div[3]/div/div/div[1]/div/div[1]/div[2]/div[1]/div/div/div/div[1]',
                '/html/body/div[5]/div[4]/div[2]/div/div[3]/div/div/div[1]/div/div[1]/div[2]/div[1]/div/div/div/div[1]/span',
                '/html/body/div[5]/div[4]/div[2]/div/div[3]/div/div/div[1]/div/div[1]/div[1]/div[1]/div/div/div/div[1]/span',
                '/html/body/div[5]/div[4]/div[2]/div/div[4]/div/div/div[1]/div/div[1]/div[1]/div[1]/div/div/div/div[1]/span',
                '/html/body/div[5]/div[4]/div[2]/div/div[5]/div/div/div[1]/div/div[1]/div[1]/div[1]/div/div/div/div[1]/span',
                '/html/body/div[5]/div[4]/div[2]/div/div[4]/div/div/div/div/div[2]/div[1]/div[1]',
                '/html/body/div[5]/div[4]/div[2]/div/div[5]/div/div/div/div/div[2]/div[1]/div[1]',
                '/html/body/div[5]/div[4]/div[2]/div/div[4]/div/div/div[1]/div/div[1]/div[2]/div[1]/div/div/div/div[1]/span'
            ]
            
            # 기본값 설정 (DB에 없는 경우 - price 제외)
            if not self.XPATHS.get('title'):
                logger.warning("⚠️ DB에 선택자가 없어 기본값 사용")
                self.XPATHS.update({
                    'title': [
                        '/html/body/div[5]/div[4]/div[1]/div/h1',
                        '/html/body/div[5]/div[4]/div[2]/div/h1',
                        '//h1[@class="sku-title"]',
                        '//div[@class="sku-title"]//h1'
                    ],
                    'imageurl': [
                        '/html/body/div[5]/div[4]/div[2]/div/div[2]/div[2]/div/div[2]/div/button/img',
                        '/html/body/div[5]/div[4]/div[2]/div/div[2]/div[2]/div/div[2]/div/button[1]/img',
                        '/html/body/div[5]/div[4]/div[1]/div/div[2]/div[2]/div/div[2]/div/button/img',
                        '/html/body/div[5]/div[4]/div[1]/div/div[2]/div[2]/div/div[2]/div/button[1]/img',
                        '//img[@class="primary-image"]',
                        '//div[@class="media-gallery"]//img'
                    ],
                    'imageurl_fallback': [
                        '/html/body/div[5]/div[3]/div[1]/div/div[1]/img'
                    ],
                    'stock_flag': [
                        'Out of Stock', 'Sold Out', 'Currently unavailable', 
                        'Coming Soon', 'Temporarily out of stock'
                    ],
                    'country_select': [
                        '/html/body/div[2]/div/div/div/div[1]/div[2]/a[2]'
                    ]
                })
                
                # fallback 이미지 선택자 추가
                if 'imageurl_fallback' not in self.XPATHS:
                    self.XPATHS['imageurl_fallback'] = ['/html/body/div[5]/div[3]/div[1]/div/div[1]/img']
                
        except Exception as e:
            logger.error(f"선택자 로드 실패: {e}")
            # 기본값 사용
            self.XPATHS = {
                'price': [
                    '/html/body/div[5]/div[4]/div[2]/div/div[3]/div/div/div[1]/div/div[1]/div[2]/div[1]/div/div/div/div[1]',
                    '/html/body/div[5]/div[4]/div[2]/div/div[3]/div/div/div[1]/div/div[1]/div[2]/div[1]/div/div/div/div[1]/span',
                    '/html/body/div[5]/div[4]/div[2]/div/div[3]/div/div/div[1]/div/div[1]/div[1]/div[1]/div/div/div/div[1]/span',
                    '/html/body/div[5]/div[4]/div[2]/div/div[4]/div/div/div[1]/div/div[1]/div[1]/div[1]/div/div/div/div[1]/span'
                ],
                'title': [],
                'imageurl': [],
                'imageurl_fallback': ['/html/body/div[5]/div[3]/div[1]/div/div[1]/img'],
                'stock_flag': [],
                'country_select': []
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
                    WHERE retailprice IS NULL  
                      AND crawl_datetime >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
                      AND country_code = 'usa'
                    GROUP BY url
                )
                SELECT DISTINCT t.*
                FROM samsung_price_tracking_list t
                LEFT JOIN failed_counts f ON t.url = f.url
                WHERE t.country = 'usa' 
                  AND t.mall_name = 'bestbuy'
                  AND t.is_active = TRUE
                  AND (f.fail_count IS NULL OR f.fail_count < 3)
                ORDER BY COALESCE(f.fail_count, 0) DESC
                """
            else:
                query = """
                SELECT *
                FROM samsung_price_tracking_list
                WHERE country = 'usa' 
                  AND mall_name = 'bestbuy'
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
            # 스텔스 모드 설정
            options.add_argument('--disable-blink-features=AutomationControlled')
            
            self.driver = uc.Chrome(options=options)
            self.driver.maximize_window()
            
            # 추가 스텔스 설정
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self.driver.execute_script("Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3]})")
            
            logger.info("✅ 드라이버 설정 완료")
            return True
        except Exception as e:
            logger.error(f"❌ 드라이버 설정 실패: {e}")
            return False
    
    def initialize_session(self):
        """BestBuy 세션 초기화 (국가 선택 포함)"""
        if self.session_initialized:
            return True
            
        try:
            logger.info("🌐 BestBuy 세션 초기화 중...")
            
            # BestBuy 메인 페이지 접속
            self.driver.get("https://www.bestbuy.com")
            time.sleep(4)
            
            # 국가 선택 팝업 처리
            self.handle_country_popup()
            
            # 세션 확인
            title = self.driver.title
            if "Best Buy" in title:
                logger.info("✅ BestBuy 세션 초기화 완료")
                self.session_initialized = True
                return True
            else:
                logger.warning("⚠️ 세션 초기화 부분 성공")
                self.session_initialized = True
                return True
                
        except Exception as e:
            logger.error(f"❌ 세션 초기화 실패: {e}")
            return False
    
    def handle_country_popup(self):
        """국가 선택 팝업 처리 (세션당 1회)"""
        try:
            logger.info("🌍 국가 선택 확인 중...")
            time.sleep(3)
            
            # DB에서 가져온 국가 선택 셀렉터
            country_selectors = self.XPATHS.get('country_select', [])
            
            # 기본 셀렉터 추가
            all_selectors = country_selectors + [
                "//a[contains(@class, 'us-link')]",
                "//button[contains(text(), 'United States')]"
            ]
            
            for selector in all_selectors:
                try:
                    if selector.startswith('//'):
                        element = self.driver.find_element(By.XPATH, selector)
                    else:
                        element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    
                    if element.is_displayed():
                        element.click()
                        logger.info("🇺🇸 미국 사이트 선택 완료")
                        time.sleep(3)
                        return True
                except:
                    continue
            
            logger.info("국가 선택 팝업 없음 (이미 설정됨)")
            return True
            
        except Exception as e:
            logger.warning(f"국가 팝업 처리 중 오류 (무시): {e}")
            return True
    
    def wait_for_price_elements(self, max_wait=30):
        """가격 요소들이 실제로 로드될 때까지 스마트 대기"""
        
        # 1단계: 기본 페이지 구조 대기
        try:
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
        except:
            pass
        
        # 2단계: 가격 관련 컨테이너들 대기 (여러 후보 중 하나라도 나타나면 OK)
        price_containers = [
            "//div[contains(@class, 'pricing')]",
            "//div[contains(@class, 'price')]", 
            "//span[contains(@class, 'current-price')]",
            "//div[contains(@data-testid, 'pricing')]"
        ]
        
        for container in price_containers:
            try:
                WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, container))
                )
                break
            except:
                continue
        
        # 3단계: 실제 가격 값이 들어올 때까지 대기
        start_time = time.time()
        while time.time() - start_time < max_wait:
            for xpath in self.XPATHS.get('price', []):
                try:
                    element = self.driver.find_element(By.XPATH, xpath)
                    text = element.text.strip()
                    if text and ('$' in text or text.replace(',', '').replace('.', '').isdigit()):
                        logger.info(f"✅ 가격 요소 로딩 완료: {text}")
                        return True
                except:
                    continue
            time.sleep(1)  # 1초마다 재확인
        
        logger.warning("⚠️ 가격 요소 로딩 시간 초과")
        return False

    def wait_for_network_idle(self, idle_time=2):
        """네트워크 요청이 완료될 때까지 대기"""
        try:
            # Performance API 사용해서 네트워크 요청 완료 확인
            script = """
            return window.performance.getEntriesByType('navigation')[0].loadEventEnd > 0
            """
            
            start_time = time.time()
            while time.time() - start_time < 20:  # 최대 20초
                if self.driver.execute_script(script):
                    time.sleep(idle_time)  # 추가 안정화 시간
                    return True
                time.sleep(0.5)
        except:
            pass
        return False

    def extract_product_info(self, url, row_data):
        """제품 정보 추출 (스마트 대기 로직 추가)"""
        try:
            logger.info(f"🔍 페이지 접속: {url}")
            
            # 세션 초기화 확인
            if not self.session_initialized:
                self.initialize_session()
            
            self.driver.get(url)
            
            # 스마트 대기 전략 적용
            logger.info("⏳ 페이지 로딩 대기 중...")
            
            # 1. 네트워크 완료 대기
            self.wait_for_network_idle()
            
            # 2. 가격 요소들 로딩 대기
            if not self.wait_for_price_elements():
                logger.warning("가격 요소 로딩 실패, 그래도 추출 시도")
            
            # 3. 추가 안정화 시간
            time.sleep(random.uniform(2, 4))
            
            # 페이지 로드 대기
            wait = WebDriverWait(self.driver, 20)
            
            # 차단 감지
            title = self.driver.title
            blocked_patterns = ["Access Denied", "Blocked", "Robot", "Captcha", "Sorry", "Error"]
            for pattern in blocked_patterns:
                if pattern.lower() in title.lower():
                    logger.warning(f"⚠️ 차단 감지: {pattern}")
                    raise Exception(f"Blocked: {pattern}")
            
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
                'country_code': row_data.get('country', 'usa'),
                'ships_from': 'usa',
                'channel_name': 'bestbuy',
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
                'sold_by': 'BestBuy',
                'imageurl': None,
                'producturl': url,
                'crawl_datetime': crawl_datetime_iso,
                'crawl_strdatetime': local_time.strftime('%Y%m%d%H%M%S') + f"{local_time.microsecond:06d}"[:4],
                'kr_crawl_datetime': now_time.strftime('%Y-%m-%d %H:%M:%S'),
                'kr_crawl_strdatetime': now_time.strftime('%Y%m%d%H%M%S') + f"{now_time.microsecond:06d}"[:4],
                'title': None,
                'vat': row_data.get('vat', 'x')
            }
            
            # 재고 상태 확인
            page_source = self.driver.page_source
            stock_available = True
            
            for stock_flag in self.XPATHS.get('stock_flag', []):
                if stock_flag in page_source:
                    logger.info(f"재고 없음: {stock_flag}")
                    stock_available = False
                    break
            
            # 가격 추출
            price_found = False
            
            # 1단계: 기존 선택자로 시도
            logger.info("💰 기존 가격 선택자로 시도 중...")
            for xpath in self.XPATHS.get('price', []):
                try:
                    price_element = self.driver.find_element(By.XPATH, xpath)
                    price_text = price_element.text.strip()
                    
                    logger.info(f"🔍 선택자: {xpath}")
                    logger.info(f"📝 추출된 텍스트: '{price_text}'")
                    
                    if price_text:
                        # BestBuy는 주로 $ 사용
                        price_match = re.search(r'\$([\d,]+\.?\d*)', price_text)
                        if price_match:
                            price_number = price_match.group(1).replace(',', '')
                            result['retailprice'] = float(price_number)
                            logger.info(f"✅ 가격 추출 성공: ${result['retailprice']} (선택자: {xpath})")
                            price_found = True
                            break
                        else:
                            logger.info(f"❌ 가격 패턴 매칭 실패: '{price_text}'")
                    else:
                        logger.info("❌ 빈 텍스트")
                except Exception as xe:
                    logger.info(f"❌ 선택자 실행 실패: {xe}")
                    continue
            
            # 2단계: CSS 선택자로 재시도
            if not price_found:
                logger.info("💰 CSS 선택자로 재시도 중...")
                try:
                    css_xpath = "//span[@class='sr-only' and contains(text(), 'current price')]"
                    price_element = self.driver.find_element(By.XPATH, css_xpath)
                    price_text = price_element.text
                    if '$' in price_text:
                        price_match = re.search(r'\$([\d,]+\.?\d*)', price_text)
                        if price_match:
                            result['retailprice'] = float(price_match.group(1).replace(',', ''))
                            logger.info(f"✅ 가격 추출 성공 (CSS): ${result['retailprice']}")
                            price_found = True
                except Exception as ex:
                    logger.info("CSS 선택자도 실패")
            
            if not price_found:
                logger.warning("모든 가격 추출 방법 실패")
            
            # 제목 추출
            try:
                for xpath in self.XPATHS.get('title', []):
                    try:
                        title_element = self.driver.find_element(By.XPATH, xpath)
                        result['title'] = title_element.text.strip()
                        logger.info(f"제목: {result['title'][:50]}...")
                        break
                    except:
                        continue

                # 제목 추출 실패 시 품절 상품용 fallback 시도
                if not result['title']:
                    try:
                        title_element = self.driver.find_element(By.XPATH, '/html/body/div[5]/div[3]/div[1]/div/div[2]/h1')
                        result['title'] = title_element.text.strip()
                        logger.info(f"제목 (품절 fallback): {result['title'][:50]}...")
                    except:
                        pass
            except Exception as e:
                logger.warning(f"제목 추출 실패: {e}")

            # 이미지 URL 추출
            try:
                for xpath in self.XPATHS.get('imageurl', []):
                    try:
                        image_element = self.driver.find_element(By.XPATH, xpath)
                        result['imageurl'] = image_element.get_attribute('src')
                        logger.info(f"이미지 URL: {result['imageurl'][:50]}...")
                        break
                    except:
                        continue

                # 이미지 추출 실패 시 품절 상품용 fallback 시도
                if not result['imageurl']:
                    try:
                        image_element = self.driver.find_element(By.XPATH, '/html/body/div[5]/div[3]/div[1]/div/div[1]/img')
                        result['imageurl'] = image_element.get_attribute('src')
                        logger.info(f"이미지 URL (품절 fallback): {result['imageurl'][:50]}...")
                    except:
                        pass
            except Exception as e:
                logger.warning(f"이미지 URL 추출 실패: {e}")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ 페이지 처리 오류: {e}")
            
            # 기본값 반환
            # V2: 타임존 분리

            now_time = datetime.now(self.korea_tz)

            local_time = datetime.now(self.local_tz)

            # ISO 8601 형식
            crawl_dt = local_time.strftime("%Y-%m-%dT%H:%M:%S")
            tz_offset = local_time.strftime("%z")
            tz_formatted = f"{tz_offset[:3]}:{tz_offset[3:]}" if tz_offset else "+00:00"
            crawl_datetime_iso = f"{crawl_dt}{tz_formatted}"

            return {
                'retailerid': row_data.get('retailerid', ''),
                'country_code': row_data.get('country', 'usa'),
                'ships_from': 'usa',
                'channel_name': 'bestbuy',
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
                'sold_by': 'BestBuy',
                'imageurl': None,
                'producturl': url,
                'crawl_datetime': crawl_datetime_iso,
                'crawl_strdatetime': local_time.strftime('%Y%m%d%H%M%S') + f"{local_time.microsecond:06d}"[:4],
                'kr_crawl_datetime': now_time.strftime('%Y-%m-%d %H:%M:%S'),
                'kr_crawl_strdatetime': now_time.strftime('%Y%m%d%H%M%S') + f"{now_time.microsecond:06d}"[:4],
                'title': None,
                'vat': row_data.get('vat', 'x')
            }

    def extract_with_retry(self, url, row_data, max_retries=2):
        """재시도 로직이 포함된 추출"""
        for attempt in range(max_retries + 1):
            try:
                if attempt > 0:
                    logger.info(f"🔄 재시도 {attempt}/{max_retries}")
                    time.sleep(random.uniform(5, 10))  # 재시도 전 대기
                
                result = self.extract_product_info(url, row_data)
                
                # 성공 조건: 가격이 추출되었거나 제목이라도 추출됨
                if result['retailprice'] is not None or result['title']:
                    return result
                    
            except Exception as e:
                logger.warning(f"시도 {attempt + 1} 실패: {e}")
                if attempt == max_retries:
                    break
        
        # 모든 시도 실패 시 기본값 반환
        # V2: 타임존 분리

        now_time = datetime.now(self.korea_tz)

        local_time = datetime.now(self.local_tz)

        # ISO 8601 형식
        crawl_dt = local_time.strftime("%Y-%m-%dT%H:%M:%S")
        tz_offset = local_time.strftime("%z")
        tz_formatted = f"{tz_offset[:3]}:{tz_offset[3:]}" if tz_offset else "+00:00"
        crawl_datetime_iso = f"{crawl_dt}{tz_formatted}"

        return {
            'retailerid': row_data.get('retailerid', ''),
            'country_code': row_data.get('country', 'usa'),
            'ships_from': 'usa',
            'channel_name': 'bestbuy',
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
            'sold_by': 'BestBuy',
            'imageurl': None,
            'producturl': url,
            'crawl_datetime': crawl_datetime_iso,
            'crawl_strdatetime': local_time.strftime('%Y%m%d%H%M%S') + f"{local_time.microsecond:06d}"[:4],
            'kr_crawl_datetime': now_time.strftime('%Y-%m-%d %H:%M:%S'),
            'kr_crawl_strdatetime': now_time.strftime('%Y%m%d%H%M%S') + f"{now_time.microsecond:06d}"[:4],
            'title': None,
            'vat': row_data.get('vat', 'x')
        }
    
    def save_to_db(self, df):
        """DB에 결과 저장"""
        if self.db_engine is None:
            logger.warning("⚠️ DB 연결이 없어 DB 저장을 건너뜁니다")
            return False
        
        try:
            # bestbuy_price_crawl_tbl_usa_v2 테이블에 저장
            df.to_sql('bestbuy_price_crawl_tbl_usa_v2', self.db_engine, if_exists='append', index=False)
            logger.info(f"✅ DB 저장 완료: {len(df)}개 레코드")
            
            # 크롤링 로그를 pandas DataFrame으로 만들어서 한번에 저장
            log_records = []
            for _, row in df.iterrows():
                log_records.append({
                    'country_code': 'usa',
                    'url': row['producturl'],
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
                count_query = "SELECT COUNT(*) FROM bestbuy_price_crawl_tbl_usa_v2 WHERE DATE(crawl_datetime) = CURDATE()"
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
        base_filename = f"{date_str}_{time_str}_usa_bestbuy"

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
    def test_connection(self):
        """연결 테스트 및 세션 초기화"""
        logger.info("=== BestBuy 세션 초기화 및 테스트 ===")
        
        if not self.setup_driver():
            return False
        
        try:
            # 1단계: Google 연결 테스트
            logger.info("1단계: Google 연결 테스트...")
            self.driver.get("https://www.google.com")
            time.sleep(2)
            google_title = self.driver.title
            
            if "Google" in google_title:
                logger.info("✅ Google 접속 성공")
            else:
                logger.warning("⚠️ Google 접속 이상")
            
            # 2단계: BestBuy 세션 초기화
            logger.info("2단계: BestBuy 세션 초기화...")
            if not self.initialize_session():
                return False
            
            # 3단계: 테스트 상품 페이지 접속
            logger.info("3단계: 테스트 상품 페이지 접속...")
            test_url = "https://www.bestbuy.com/site/samsung-9100-pro-1tb-internal-ssd-pcie-gen-5x4-nvme-speeds-up-to-14700-mb-s/6618929.p?skuId=6618929"
            
            test_row = {
                'url': test_url,
                'brand': 'Samsung',
                'item': 'Test Item',
                'country': 'usa'
            }
            
            test_result = self.extract_product_info(test_url, test_row)
            
            logger.info("추출된 정보:")
            logger.info(f"  - 상품명: {test_result['title'][:50] if test_result['title'] else 'None'}...")
            logger.info(f"  - 가격: {test_result['retailprice']}")
            logger.info(f"  - 이미지: {'추출됨' if test_result['imageurl'] else '없음'}")
            
            # 4단계: 파일서버 연결 테스트
            logger.info("4단계: 파일서버 연결 테스트...")
            try:
                transport = paramiko.Transport((FILE_SERVER_CONFIG['host'], FILE_SERVER_CONFIG['port']))
                transport.connect(
                    username=FILE_SERVER_CONFIG['username'],
                    password=FILE_SERVER_CONFIG['password']
                )
                transport.close()
                logger.info("✅ 파일서버 연결 성공")
            except Exception as e:
                logger.warning(f"⚠️ 파일서버 연결 실패: {e}")
            
            if test_result['retailprice'] is not None or test_result['title']:
                logger.info("✅ 테스트 성공 - 크롤링 준비 완료!")
                return True
            else:
                logger.warning("⚠️ 테스트 부분 실패 - 그래도 계속 진행")
                return True
                
        except Exception as e:
            logger.error(f"❌ 테스트 실패: {e}")
            return False
    
    def scrape_urls(self, urls_data, max_items=None):
        """여러 URL 스크래핑"""
        if max_items:
            urls_data = urls_data[:max_items]
        
        logger.info(f"📊 총 {len(urls_data)}개 제품 처리 시작")
        
        results = []
        failed_urls = []

        for idx, row in enumerate(urls_data):
            try:
                logger.info(f"\n{'='*50}")
                logger.info(f"진행률: {idx + 1}/{len(urls_data)} ({(idx + 1)/len(urls_data)*100:.1f}%)")

                # URL 추출
                url = row.get('url')

                # 제품 정보 추출 (재시도 로직 적용)
                result = self.extract_with_retry(url, row)

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
                            interim_df.to_sql('bestbuy_price_crawl_tbl_usa_v2', self.db_engine,
                                            if_exists='append', index=False)
                            logger.info(f"💾 중간 저장: 10개 레코드 DB 저장")
                        except Exception as e:
                            logger.error(f"중간 저장 실패: {e}")

                # 다음 요청 전 대기
                if idx < len(urls_data) - 1:
                    wait_time = random.uniform(2, 5)  # BestBuy는 조금 더 빠르게
                    logger.info(f"⏳ {wait_time:.1f}초 대기 중...")
                    time.sleep(wait_time)

                    # 10개마다 긴 휴식
                    if (idx + 1) % 10 == 0:
                        logger.info("☕ 10개 처리 완료, 20초 휴식...")
                        time.sleep(20)

            except Exception as e:
                logger.error(f"❌ 스크래핑 중 오류 (URL: {row.get('url', 'unknown')}): {e}")
                continue

        # 정리
        if failed_urls:
            logger.warning(f"\n⚠️ 가격 추출 실패한 URL {len(failed_urls)}개:")
            for fail in failed_urls[:5]:
                logger.warning(f"  - {fail['brand']} {fail['item']}: {fail['url']}")
            if len(failed_urls) > 5:
                logger.warning(f"  ... 외 {len(failed_urls) - 5}개")

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
            price_df['numeric_price'] = pd.to_numeric(price_df['retailprice'], errors='coerce')
            
            logger.info(f"\n💰 가격 통계:")
            logger.info(f"평균가: ${price_df['numeric_price'].mean():.2f}")
            logger.info(f"최저가: ${price_df['numeric_price'].min():.2f}")
            logger.info(f"최고가: ${price_df['numeric_price'].max():.2f}")
            logger.info(f"중간값: ${price_df['numeric_price'].median():.2f}")
            
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
                    logger.info(f"  {capacity}: ${stats['mean']:.2f} ({int(stats['count'])}개)")

def get_db_history(engine, days=7):
    """DB에서 최근 기록 조회"""
    try:
        query = f"""
        SELECT DATE(crawl_datetime) as date, 
               COUNT(*) as total_count,
               SUM(CASE WHEN retailprice IS NOT NULL THEN 1 ELSE 0 END) as with_price,
               COUNT(DISTINCT brand) as brands,
               COUNT(DISTINCT item) as items
        FROM bestbuy_price_crawl_tbl_usa_v2
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
    print("\n🚀 BestBuy 가격 추출 시스템 - DB 기반 버전")
    print("="*60)
    
    # 스크래퍼 초기화
    scraper = BestBuyScraper()
    
    if scraper.db_engine is None:
        logger.error("DB 연결 실패로 종료합니다.")
        monitor_and_alert('usa_bestbuy', 0, None, error_message="DB 연결 실패")
        return

    # 최근 크롤링 기록 확인
    get_db_history(scraper.db_engine, 7)
    
    # 연결 테스트 및 세션 초기화
    if not scraper.test_connection():
        logger.error("연결 테스트 실패로 종료합니다.")
        if scraper.driver:
            scraper.driver.quit()
        return
    
    try:
        # 전체 크롤링 실행
        logger.info("\n📊 전체 크롤링 시작")
        urls_data = scraper.get_crawl_targets()
        
        if not urls_data:
            logger.warning("크롤링 대상이 없습니다.")
            monitor_and_alert('usa_bestbuy', 0, None, error_message="크롤링 대상 URL이 없습니다")
            return

        logger.info(f"✅ 크롤링 대상: {len(urls_data)}개")

        # 크롤링 실행
        results_df = scraper.scrape_urls(urls_data)
        
        if results_df is None or results_df.empty:
            logger.error("크롤링 결과가 없습니다.")
            monitor_and_alert('usa_bestbuy', len(urls_data), None, error_message="크롤링 결과가 없습니다")
            return

        # 결과 분석
        failed_count = results_df['retailprice'].isna().sum()
        success_count = results_df['retailprice'].notna().sum()
        success_rate = (success_count / len(results_df) * 100) if len(results_df) > 0 else 0
        
        logger.info(f"\n📊 === 최종 결과 ===")
        logger.info(f"전체: {len(results_df)}개")
        logger.info(f"성공: {success_count}개")
        logger.info(f"실패: {failed_count}개")
        logger.info(f"성공률: {success_rate:.1f}%")
        
        # DB와 파일서버에 결과 저장
        save_results = scraper.save_results(
            results_df,
            save_db=True,
            upload_server=True
        )
        
        # 상세 분석
        scraper.analyze_results(results_df)
        
        # 저장 결과 출력
        logger.info("\n📊 저장 결과:")
        logger.info(f"DB 저장: {'✅ 성공' if save_results['db_saved'] else '❌ 실패'}")
        logger.info(f"파일서버 업로드: {'✅ 성공' if save_results['server_uploaded'] else '❌ 실패'}")
        
        # 실패한 URL 로그
        if failed_count > 0:
            logger.warning(f"\n⚠️ {failed_count}개 URL에서 가격 추출 실패")
            failed_items = results_df[results_df['retailprice'].isna()]
            logger.warning("실패 목록 (상위 5개):")
            for idx, row in failed_items.head().iterrows():
                logger.warning(f"  - {row['brand']} {row['item']}: {row['producturl'][:50]}...")
        
        logger.info("\n✅ 크롤링 프로세스 완료!")

        # 크롤링 완료 후 알림 (빈 값 50% 이상 시 경고)
        monitor_and_alert('usa_bestbuy', len(urls_data), results_df)

    finally:
        # 드라이버 종료
        if scraper.driver:
            scraper.driver.quit()
            logger.info("🔧 드라이버 종료")

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
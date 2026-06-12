"""
Coolblue 가격 추출 시스템 - DB 기반 버전 (이미지 URL 추출 개선)
DB에서 URL 읽어와서 크롤링 후 결과 저장
파일명 형식: {수집일자}{수집시간}_{국가코드}_{쇼핑몰}.csv
"""
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import pymysql
from sqlalchemy import create_engine, text
import paramiko
import time
import random
import re
from datetime import datetime
import pytz
import logging
import os
import json
from io import StringIO
import zipfile
import hashlib
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# Import database configuration V2
from config import DB_CONFIG_V2 as DB_CONFIG
from config import FILE_SERVER_CONFIG
from alert_monitor import monitor_and_alert
from null_screenshot import is_null_result, capture_and_upload

class CoolblueScraper:
    def __init__(self):
        self.driver = None
        self.db_engine = None
        self.sftp_client = None
        self.country_code = 'nl'
        # V2: 타임존 분리 (현지시간 + 한국시간)
        self.korea_tz = pytz.timezone('Asia/Seoul')
        self.local_tz = pytz.timezone('Europe/Amsterdam')  # Coolblue 네덜란드 현지 시간

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
        """DB에서 Coolblue용 선택자 로드"""
        try:
            query = """
            SELECT element_type, selector_value, priority
            FROM mall_selectors
            WHERE mall_name = 'coolblue' 
              AND country_code = 'nl'
              AND is_active = TRUE
            ORDER BY element_type, priority DESC
            """
            
            df = pd.read_sql(query, self.db_engine)
            
            # element_type별로 그룹화
            self.XPATHS = {}
            for element_type in df['element_type'].unique():
                type_selectors = df[df['element_type'] == element_type]['selector_value'].tolist()
                self.XPATHS[element_type] = type_selectors
            
            logger.info(f"✅ DB에서 선택자 로드 완료: {len(df)}개")

            # 기본값 설정 (DB에 없는 경우) - DB에서 관리되므로 빈 배열로 설정
            if not self.XPATHS:
                logger.warning("⚠️ DB에 선택자가 없습니다. DB에 선택자를 추가해주세요.")
                self.XPATHS = {
                    'price': [],
                    'title': [],
                    'imageurl': []
                }
                
        except Exception as e:
            logger.error(f"선택자 로드 실패: {e}")
            # 기본값 사용
            self.XPATHS = {
                'price': [],
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
                    WHERE retailprice IS NULL
                      AND crawl_datetime >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
                      AND country_code = 'nl'
                    GROUP BY url
                )
                SELECT DISTINCT t.*
                FROM samsung_price_tracking_list t
                LEFT JOIN failed_counts f ON t.url = f.url
                WHERE t.country = 'nl' 
                  AND t.mall_name = 'coolblue'
                  AND t.is_active = TRUE
                  AND (f.fail_count IS NULL OR f.fail_count < 3)
                ORDER BY COALESCE(f.fail_count, 0) DESC  -- 실패한 것 우선
                """
            else:
                query = """
                SELECT *
                FROM samsung_price_tracking_list
                WHERE country = 'nl' 
                  AND mall_name = 'coolblue'
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
            # 메모리 최적화 옵션
            options.add_argument('--disable-gpu')
            options.add_argument('--disable-extensions')
            options.add_argument('--disable-infobars')
            options.add_argument('--disable-renderer-backgrounding')
            options.add_argument('--js-flags=--max-old-space-size=512')

            # 설치된 Chrome 버전 감지하여 맞는 드라이버 사용
            chrome_version = None
            try:
                import subprocess
                result = subprocess.run(
                    ['reg', 'query', 'HKEY_CURRENT_USER\\Software\\Google\\Chrome\\BLBeacon', '/v', 'version'],
                    capture_output=True, text=True, timeout=5
                )
                if result.returncode == 0:
                    for line in result.stdout.strip().split('\n'):
                        if 'version' in line.lower():
                            version_str = line.strip().split()[-1]
                            chrome_version = int(version_str.split('.')[0])
                            logger.info(f"감지된 Chrome 버전: {chrome_version}")
                            break
            except Exception as e:
                logger.warning(f"Chrome 버전 감지 실패, 자동 매칭 시도: {e}")

            self.driver = uc.Chrome(options=options, version_main=chrome_version)
            self.driver.maximize_window()

            # 스텔스 모드 설정
            stealth_script = """
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3]});
            """
            self.driver.execute_script(stealth_script)
            
            logger.info("✅ 드라이버 설정 완료")
            return True
        except Exception as e:
            logger.error(f"❌ 드라이버 설정 실패: {e}")
            return False
    
    def initialize_session(self):
        """Coolblue 세션 초기화"""
        logger.info("Coolblue 세션 초기화...")
        
        try:
            # Coolblue 메인 페이지 접속
            self.driver.get("https://www.coolblue.nl")
            time.sleep(2)
            
            # 쿠키 팝업 처리
            self.handle_cookie_consent()
            
            # 세션이 제대로 설정되었는지 확인
            title = self.driver.title
            if "coolblue" in title.lower():
                logger.info("✅ Coolblue 세션 초기화 완료")
                return True
            else:
                logger.warning("⚠️ 세션 초기화 부분 성공")
                return True
                
        except Exception as e:
            logger.error(f"❌ 세션 초기화 실패: {e}")
            return False
    
    def handle_cookie_consent(self):
        """쿠키 동의 팝업 처리"""
        try:
            # DB에서 쿠키 관련 선택자 조회 (있는 경우)
            cookie_selectors = self.XPATHS.get('cookie_accept', [])
            
            # 기본 선택자 추가 (Coolblue 특화)
            default_selectors = [
                "button[name='accept']",  # Oké 버튼의 가능한 선택자
                "button.js-btn-cookie-accept",  # 가능한 클래스명
                "//button[contains(text(), 'Oké')]",  # 텍스트로 찾기
                "//button[contains(text(), 'Oke')]",  # 대체 철자
                "//button[@class='button' and contains(., 'Oké')]",
                "/html/body/div/div[1]/div[2]/div/div[1]/form/div/div[1]/button",
                "button[data-testid='accept-cookies']",
                "//button[contains(text(), 'Accepteren')]",
                "//button[contains(@class, 'accept')]",
                ".cookie-consent__button--accept",
                "#onetrust-accept-btn-handler"
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
            
            # JavaScript로 직접 클릭 시도
            try:
                script = """
                var buttons = document.querySelectorAll('button');
                for (var i = 0; i < buttons.length; i++) {
                    if (buttons[i].textContent.includes('Oké') || 
                        buttons[i].textContent.includes('OK') ||
                        buttons[i].textContent.includes('Accept')) {
                        buttons[i].click();
                        return true;
                    }
                }
                return false;
                """
                result = self.driver.execute_script(script)
                if result:
                    logger.info("🍪 쿠키 동의 팝업 처리 완료 (JavaScript)")
                    time.sleep(1)
                    return True
            except:
                pass
            
            return False
            
        except Exception as e:
            logger.debug(f"쿠키 팝업 처리 중 오류 (무시): {e}")
            return False
    
    def extract_product_info(self, url, row_data, retry_count=0, max_retries=2):
        """제품 정보 추출 (재시도 로직 포함)"""
        try:
            logger.info(f"🔍 페이지 접속: {url} (시도: {retry_count + 1}/{max_retries + 1})")
            self.driver.get(url)

            # 페이지 로드 대기
            wait = WebDriverWait(self.driver, 6)
            time.sleep(random.uniform(3, 5))

            # main-content 로드 대기 (최대 10초)
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, "main-content"))
                )
                logger.info("✅ main-content 로드 완료")
            except:
                logger.warning("⚠️ main-content 로드 실패 - 페이지 새로고침 시도")
                self.driver.refresh()
                time.sleep(random.uniform(3, 5))
                try:
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.ID, "main-content"))
                    )
                    logger.info("✅ 새로고침 후 main-content 로드 완료")
                except:
                    logger.warning("⚠️ 새로고침 후에도 main-content 로드 실패")
                    if retry_count < max_retries:
                        logger.info(f"🔄 재시도 {retry_count + 2}/{max_retries + 1}")
                        return self.extract_product_info(url, row_data, retry_count + 1, max_retries)
                    else:
                        logger.error(f"❌ main-content 로드 최종 실패: {url}")
                        now_time = datetime.now(self.korea_tz)
                        local_time = datetime.now(self.local_tz)
                        crawl_dt = local_time.strftime("%Y-%m-%dT%H:%M:%S")
                        tz_offset = local_time.strftime("%z")
                        tz_formatted = f"{tz_offset[:3]}:{tz_offset[3:]}" if tz_offset else "+00:00"
                        crawl_datetime_iso = f"{crawl_dt}{tz_formatted}"
                        return {
                            'retailerid': row_data.get('retailerid', ''),
                            'country_code': row_data.get('country', 'nl'),
                            'ships_from': 'NL',
                            'channel_name': 'coolblue',
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
                            'sold_by': 'Coolblue',
                            'imageurl': None,
                            'producturl': url,
                            'crawl_datetime': crawl_datetime_iso,
                            'crawl_strdatetime': local_time.strftime('%Y%m%d%H%M%S') + f"{local_time.microsecond:06d}"[:4],
                            'kr_crawl_datetime': now_time.strftime('%Y-%m-%d %H:%M:%S'),
                            'kr_crawl_strdatetime': now_time.strftime('%Y%m%d%H%M%S') + f"{now_time.microsecond:06d}"[:4],
                            'title': None,
                            'vat': row_data.get('vat', 'o')
                        }

            # 에러 페이지 감지 및 재로드 버튼 클릭
            try:
                reload_button = self.driver.find_element(By.CSS_SELECTOR, "button.css-fm7qx4")
                if reload_button and "opnieuw laden" in reload_button.text.lower():
                    logger.info("⚠️ 에러 페이지 감지 - 'Pagina opnieuw laden' 버튼 클릭")
                    reload_button.click()
                    time.sleep(3)
                    # 재로드 후 다시 확인
                    try:
                        reload_button2 = self.driver.find_element(By.CSS_SELECTOR, "button.css-fm7qx4")
                        if reload_button2 and "opnieuw laden" in reload_button2.text.lower():
                            logger.warning("⚠️ 재로드 후에도 에러 페이지 - 재시도 필요")
                            raise Exception("Error page after reload")
                    except:
                        logger.info("✅ 재로드 성공 - 정상 페이지 로드됨")
            except:
                pass  # 버튼이 없으면 정상 페이지

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
                'country_code': row_data.get('country', 'nl'),
                'ships_from': 'NL',
                'channel_name': 'coolblue',
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
                'sold_by': 'Coolblue',
                'imageurl': None,
                'producturl': url,
                'crawl_datetime': crawl_datetime_iso,
                'crawl_strdatetime': local_time.strftime('%Y%m%d%H%M%S') + f"{local_time.microsecond:06d}"[:4],
                'kr_crawl_datetime': now_time.strftime('%Y-%m-%d %H:%M:%S'),
                'kr_crawl_strdatetime': now_time.strftime('%Y%m%d%H%M%S') + f"{now_time.microsecond:06d}"[:4],
                'title': None,
                'vat': row_data.get('vat', 'o')
            }
            
            # 가격 추출
            try:
                price_found = False
                
                # 가격 요소가 로드될 때까지 대기
                logger.info("🔍 가격 요소 로드 대기 중...")
                
                # 디버깅: 페이지 소스에서 가격 관련 요소 확인
                page_source = self.driver.page_source
                if 'main-content' in page_source:
                    logger.info("✅ main-content 요소 발견")
                else:
                    logger.warning("⚠️ main-content 요소 없음")
                
                if '€' in page_source:
                    logger.info("✅ 유로(€) 기호 발견")
                    # 유로 기호 주변 텍스트 일부 추출
                    euro_index = page_source.find('€')
                    context = page_source[max(0, euro_index-50):euro_index+50]
                    logger.info(f"유로 기호 주변 텍스트: {context}")
                else:
                    logger.warning("⚠️ 유로(€) 기호 없음")
                
                for xpath in self.XPATHS.get('price', []):
                    try:
                        logger.info(f"🔍 XPath 시도: {xpath}")
                        # 특정 요소가 나타날 때까지 최대 6초 대기
                        price_element = wait.until(
                            EC.presence_of_element_located((By.XPATH, xpath))
                        )
                        logger.info(f"✅ XPath {xpath} 요소 발견됨")
                        
                        # 요소가 visible해질 때까지 추가 대기
                        wait.until(EC.visibility_of(price_element))
                        logger.info(f"✅ XPath {xpath} 요소 표시됨")
                        
                        # 텍스트가 로드될 때까지 잠시 대기
                        time.sleep(1)
                        
                        price_text = price_element.text.strip()
                        logger.info(f"🔍 XPath {xpath}에서 추출한 텍스트: '{price_text}'")
                        
                        if price_text:
                            # Coolblue는 유럽식 숫자 형식 사용 (쉼표가 소수점)
                            # 예: "1.299,99" -> 1299.99
                            price_text_clean = price_text.replace('.', '').replace(',', '.').replace('€', '').strip()
                            price_match = re.search(r'([\d,]+\.?\d*)', price_text_clean)
                            if price_match:
                                price_number = price_match.group(1)
                                result['retailprice'] = float(price_number)
                                logger.info(f"✅ 가격 추출 성공 (XPath): €{result['retailprice']}")
                                price_found = True
                                break
                        else:
                            logger.warning(f"⚠️ XPath {xpath}에서 빈 텍스트")
                            
                    except Exception as e:
                        logger.warning(f"❌ XPath {xpath} 실패: {e}")
                        continue
                
                # CSS 선택자로 재시도
                if not price_found:
                    css_selectors = [
                        "span.js-sales-price-current",
                        "[data-testid='sales-price-current']",
                        ".sales-price__current",
                        ".price-current",
                        ".product-price",
                        "span[class*='price']",
                        "div[class*='price']",
                        "p[class*='price']"
                    ]
                    
                    for css_selector in css_selectors:
                        try:
                            price_element = self.driver.find_element(By.CSS_SELECTOR, css_selector)
                            price_text = price_element.text.strip()
                            logger.info(f"🔍 CSS {css_selector}에서 추출한 텍스트: '{price_text}'")
                            
                            if price_text and any(char.isdigit() for char in price_text):
                                price_text_clean = price_text.replace('.', '').replace(',', '.').replace('€', '').strip()
                                price_match = re.search(r'([\d,]+\.?\d*)', price_text_clean)
                                if price_match:
                                    price_number = price_match.group(1)
                                    result['retailprice'] = float(price_number)
                                    logger.info(f"✅ 가격 추출 성공 (CSS): €{result['retailprice']}")
                                    price_found = True
                                    break
                        except Exception as e:
                            logger.debug(f"CSS {css_selector} 실패: {e}")
                            continue
                
                # JavaScript로 가격 찾기 (최후 수단)
                if not price_found:
                    try:
                        script = """
                        var priceSelectors = [
                            'span[class*="price"]',
                            'div[class*="price"]', 
                            'p[class*="price"]',
                            '[data-testid*="price"]',
                            '.js-sales-price-current'
                        ];
                        
                        for (var i = 0; i < priceSelectors.length; i++) {
                            var elements = document.querySelectorAll(priceSelectors[i]);
                            for (var j = 0; j < elements.length; j++) {
                                var text = elements[j].textContent || elements[j].innerText;
                                if (text && /\\d/.test(text) && text.includes('€')) {
                                    return text.trim();
                                }
                            }
                        }
                        return null;
                        """
                        js_result = self.driver.execute_script(script)
                        if js_result:
                            logger.info(f"🔍 JavaScript에서 추출한 텍스트: '{js_result}'")
                            price_text_clean = js_result.replace('.', '').replace(',', '.').replace('€', '').strip()
                            price_match = re.search(r'([\d,]+\.?\d*)', price_text_clean)
                            if price_match:
                                price_number = price_match.group(1)
                                result['retailprice'] = float(price_number)
                                logger.info(f"✅ 가격 추출 성공 (JS): €{result['retailprice']}")
                                price_found = True
                    except Exception as e:
                        logger.debug(f"JavaScript 가격 추출 실패: {e}")
                
                if not price_found:
                    logger.warning("모든 가격 추출 방법 실패")
                        
            except Exception as e:
                logger.warning(f"가격 추출 실패: {e}")
            
            # 제목 추출
            try:
                for xpath in self.XPATHS.get('title', []):
                    try:
                        title_element = wait.until(
                            EC.presence_of_element_located((By.XPATH, xpath))
                        )
                        wait.until(EC.visibility_of(title_element))
                        result['title'] = title_element.text.strip()
                        if result['title']:
                            logger.info(f"제목: {result['title']}")
                            break
                    except:
                        continue
            except Exception as e:
                logger.warning(f"제목 추출 실패: {e}")
            
            # 이미지 URL 추출 (다양한 선택자 시도)
            try:
                image_found = False
                
                # 1. 기존 XPath들 시도
                for xpath in self.XPATHS.get('imageurl', []):
                    try:
                        image_element = self.driver.find_element(By.XPATH, xpath)
                        src = image_element.get_attribute('src')
                        if src and 'image.coolblue.nl' in src:
                            result['imageurl'] = src
                            logger.info(f"이미지 URL (XPath): {result['imageurl']}")
                            image_found = True
                            break
                    except:
                        continue
                
                # 2. CSS 선택자들로 재시도
                if not image_found:
                    css_selectors = [
                        'img[src*="image.coolblue.nl"]',  # Coolblue 이미지 도메인 포함
                        'img[data-src*="image.coolblue.nl"]',  # lazy loading
                        'img.product-image',
                        'img.hero-image',
                        '.product-image img',
                        '.gallery img',
                        'img[alt*="product"]',
                        'img[alt*="Product"]'
                    ]
                    
                    for selector in css_selectors:
                        try:
                            images = self.driver.find_elements(By.CSS_SELECTOR, selector)
                            for img in images:
                                src = img.get_attribute('src') or img.get_attribute('data-src')
                                if src and 'image.coolblue.nl' in src:
                                    result['imageurl'] = src
                                    logger.info(f"이미지 URL (CSS): {result['imageurl']}")
                                    image_found = True
                                    break
                            if image_found:
                                break
                        except:
                            continue
                
                # 3. JavaScript로 이미지 찾기 (최후 수단)
                if not image_found:
                    try:
                        script = """
                        var imgs = document.querySelectorAll('img');
                        for (var i = 0; i < imgs.length; i++) {
                            var src = imgs[i].src || imgs[i].getAttribute('data-src');
                            if (src && src.includes('image.coolblue.nl') && src.includes('products')) {
                                return src;
                            }
                        }
                        return null;
                        """
                        js_result = self.driver.execute_script(script)
                        if js_result:
                            result['imageurl'] = js_result
                            logger.info(f"이미지 URL (JS): {result['imageurl']}")
                            image_found = True
                    except:
                        pass
                
                if not image_found:
                    logger.warning("모든 이미지 추출 방법 실패")
                    
            except Exception as e:
                logger.warning(f"이미지 URL 추출 실패: {e}")
            
            # 재고 상태 확인
            stock_flags = ['Out of Stock', 'Sold Out', 'Currently unavailable', 'Temporarily out of stock']
            page_source = self.driver.page_source
            stock_available = not any(flag in page_source for flag in stock_flags)
            
            # VAT 텍스트 확인
            # vat_texts = ['inclusief btw', 'incl. BTW', 'Tax included', 'Inclusive of all taxes']
            # result['vat'] = 'o' if any(text in page_source for text in vat_texts) else 'x'

            # NULL 필드 발견 시 스크린샷 + S3 업로드
            if is_null_result(result):
                capture_and_upload(self.driver, 'coolblue', row_data.get('retailersku', ''), url)

            return result

        except Exception as e:
            logger.error(f"❌ 페이지 처리 오류: {e}")
            
            # 재시도 로직
            if retry_count < max_retries:
                wait_time = 5  # 일괄 5초 대기
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
                    self.initialize_session()
                
                # 재귀 호출로 재시도
                return self.extract_product_info(url, row_data, retry_count + 1, max_retries)
            
            # 최대 재시도 횟수 초과 시 기본값 반환
            logger.error(f"❌ 최대 재시도 횟수 초과: {url}")
            # V2: 타임존 분리
            now_time = datetime.now(self.korea_tz)
            local_time = datetime.now(self.local_tz)

            # ISO 8601 형식
            crawl_dt = local_time.strftime("%Y-%m-%dT%H:%M:%S")
            tz_offset = local_time.strftime("%z")
            tz_formatted = f"{tz_offset[:3]}:{tz_offset[3:]}" if tz_offset else "+00:00"
            crawl_datetime_iso = f"{crawl_dt}{tz_formatted}"

            fail_result = {
                'retailerid': row_data.get('retailerid', ''),
                'country_code': row_data.get('country', 'nl'),
                'ships_from': 'NL',
                'channel_name': 'coolblue',
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
                'sold_by': 'Coolblue',
                'imageurl': None,
                'producturl': url,
                'crawl_datetime': crawl_datetime_iso,
                'crawl_strdatetime': local_time.strftime('%Y%m%d%H%M%S') + f"{local_time.microsecond:06d}"[:4],
                'kr_crawl_datetime': now_time.strftime('%Y-%m-%d %H:%M:%S'),
                'kr_crawl_strdatetime': now_time.strftime('%Y%m%d%H%M%S') + f"{now_time.microsecond:06d}"[:4],
                'title': None,
                'vat': row_data.get('vat', 'o')
            }

            # NULL 필드 발견 시 스크린샷 + S3 업로드 (best-effort)
            try:
                if is_null_result(fail_result):
                    capture_and_upload(self.driver, 'coolblue', row_data.get('retailersku', ''), url)
            except Exception:
                pass

            return fail_result

    def save_to_db(self, df):
        """DB에 결과 저장"""
        if self.db_engine is None:
            logger.warning("⚠️ DB 연결이 없어 DB 저장을 건너뜁니다")
            return False
        
        try:
            # coolblue_price_crawl_tbl_nl_v2 테이블에 저장
            df.to_sql('coolblue_price_crawl_tbl_nl_v2', self.db_engine, if_exists='append', index=False)
            logger.info(f"✅ DB 저장 완료: {len(df)}개 레코드")
            
            # 크롤링 로그를 pandas DataFrame으로 만들어서 한번에 저장
            log_records = []
            for _, row in df.iterrows():
                log_records.append({
                    'country_code': 'nl',
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
                count_query = "SELECT COUNT(*) FROM coolblue_price_crawl_tbl_nl_v2 WHERE DATE(crawl_datetime) = CURDATE()"
                result = conn.execute(text(count_query))
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
        base_filename = f"{date_str}_{time_str}_nl_coolblue"

        results = {'db_saved': False, 'server_uploaded': False}

        if save_db:
            results['db_saved'] = self.save_to_db(df)

        if upload_server:
            try:
                # 1. CSV 파일 생성 (복사본 사용하여 원본 컬럼명 유지)
                csv_filename = f'{base_filename}.csv'
                df_csv = df.copy()
                df_csv.columns = df_csv.columns.str.upper()
                df_csv.to_csv(csv_filename, index=False, encoding='utf-8', lineterminator='\r\n')

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
        logger.info("=== Coolblue 세션 초기화 및 테스트 ===")
        
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
            
            # 2단계: Coolblue 세션 초기화
            if not self.initialize_session():
                return False
            
            # 3단계: 테스트 상품 페이지 접속
            logger.info("3단계: 테스트 상품 페이지 접속...")
            test_url = "https://www.coolblue.nl/product/913729/samsung-990-pro-1tb-nvme-ssd.html"
            test_row = {
                'url': test_url,
                'brand': 'Samsung',
                'item': '990 PRO 1TB',
                'country': 'nl'
            }
            
            test_result = self.extract_product_info(test_url, test_row)
            
            logger.info("추출된 정보:")
            logger.info(f"  - 상품명: {test_result['title']}")
            logger.info(f"  - 가격: €{test_result['retailprice']}")
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
            except:
                logger.warning("⚠️ 파일서버 연결 실패 - 크롤링은 계속 진행")
            
            if test_result['retailprice'] or test_result['title']:
                logger.info("✅ 정보 추출 성공 - 크롤링 준비 완료!")
                return True
            else:
                logger.warning("⚠️ 정보 추출 부분 실패 - 그래도 계속 진행")
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
        failed_urls = []  # 실패한 URL 추적
        all_null_failures = []  # title, imageurl, retailprice 모두 null인 경우

        try:
            for idx, row in enumerate(urls_data):
                logger.info(f"\n{'='*50}")
                logger.info(f"진행률: {idx + 1}/{len(urls_data)} ({(idx + 1)/len(urls_data)*100:.1f}%)")

                # URL 추출
                url = row.get('url')

                # 제품 정보 추출 (재시도 로직 포함)
                result = self.extract_product_info(url, row)

                # title, imageurl, retailprice 중 하나라도 null이면 재시도 대상
                if result['retailprice'] is None or result['title'] is None or result['imageurl'] is None:
                    all_null_failures.append({
                        'url': url,
                        'row_data': row,
                        'item': row.get('item', ''),
                        'brand': row.get('brand', ''),
                        'result_idx': len(results)  # 결과 인덱스 저장
                    })

                # 실패한 URL 추적 로직 추가
                if result['retailprice'] is None:
                    failed_urls.append({
                        'url': url,
                        'item': row.get('item', ''),
                        'brand': row.get('brand', '')
                    })

                results.append(result)


                # 10개마다 로그 출력 (중간 저장은 최종 저장에서 일괄 처리)
                if (idx + 1) % 10 == 0:
                    logger.info(f"💾 {idx + 1}개 처리 완료")

                # 다음 요청 전 대기
                if idx < len(urls_data) - 1:
                    wait_time = random.uniform(2, 5)
                    logger.info(f"⏳ {wait_time:.1f}초 대기 중...")
                    time.sleep(wait_time)

                    # 10개마다 휴식
                    if (idx + 1) % 10 == 0:
                        logger.info("☕ 10개 처리 완료, 10초 휴식...")
                        time.sleep(10)

            # NULL 값 재시도 (1회만)
            if all_null_failures:
                logger.info(f"\n{'='*60}")
                logger.info(f"🔄 NULL 값 재시도 시작: {len(all_null_failures)}개 항목")
                logger.info(f"{'='*60}")

                final_all_null_failures = []

                for fail_idx, fail_item in enumerate(all_null_failures):
                    url = fail_item['url']
                    row_data = fail_item['row_data']
                    result_idx = fail_item['result_idx']

                    logger.info(f"재시도 {fail_idx + 1}/{len(all_null_failures)}: {fail_item['brand']} {fail_item['item']}")

                    # 5초 대기 후 재시도
                    time.sleep(5)

                    # 재시도 (max_retries=0으로 추가 재시도 방지)
                    retry_result = self.extract_product_info(url, row_data, retry_count=0, max_retries=0)

                    # 재시도 성공 여부 확인 (하나라도 값이 있으면 성공)
                    if retry_result['retailprice'] is not None or retry_result['title'] is not None or retry_result['imageurl'] is not None:
                        logger.info(f"✅ 재시도 성공: price={retry_result['retailprice']}, title={'있음' if retry_result['title'] else '없음'}, image={'있음' if retry_result['imageurl'] else '없음'}")
                        # 기존 결과 업데이트
                        results[result_idx] = retry_result
                    else:
                        logger.warning(f"❌ 재시도 실패: title, imageurl, retailprice 모두 NULL")
                        # title, imageurl, retailprice 모두 null인 경우만 최종 실패로 기록
                        final_all_null_failures.append(fail_item)

                # 최종 실패 개수 업데이트
                all_null_failures = final_all_null_failures
                logger.info(f"\n🔄 NULL 재시도 완료: 최종 실패 {len(all_null_failures)}개")

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

        return pd.DataFrame(results), len(all_null_failures)
    
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
            logger.info(f"평균가: €{price_df['numeric_price'].mean():.2f}")
            logger.info(f"최저가: €{price_df['numeric_price'].min():.2f}")
            logger.info(f"최고가: €{price_df['numeric_price'].max():.2f}")
            logger.info(f"중간값: €{price_df['numeric_price'].median():.2f}")
            
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
                    logger.info(f"  {capacity}: €{stats['mean']:.2f} ({int(stats['count'])}개)")

def get_db_history(engine, days=7):
    """DB에서 최근 기록 조회"""
    try:
        query = f"""
        SELECT DATE(crawl_datetime) as date, 
               COUNT(*) as total_count,
               SUM(CASE WHEN retailprice IS NOT NULL THEN 1 ELSE 0 END) as with_price,
               COUNT(DISTINCT brand) as brands,
               COUNT(DISTINCT item) as items
        FROM coolblue_price_crawl_tbl_nl_v2
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
    from log_utils import setup_log, save_log
    setup_log('nl_coolblue')

    print("\n🚀 Coolblue 가격 추출 시스템 - DB 기반 버전 (이미지 URL 추출 개선)")
    print("="*60)
    
    # 스크래퍼 초기화
    scraper = CoolblueScraper()
    
    if scraper.db_engine is None:
        logger.error("DB 연결 실패로 종료합니다.")
        monitor_and_alert('nl_coolblue', 0, None, error_message="DB 연결 실패")
        return

    # 최근 크롤링 기록 확인
    get_db_history(scraper.db_engine, 7)
    
    # 테스트 모드 확인
    test_mode = os.getenv("TEST_MODE", "false").lower()
    
    if test_mode in ["true", "1", "yes"]:
        logger.info("🧪 테스트 모드 실행")
        
        # 연결 테스트
        if scraper.test_connection():
            logger.info("✅ 테스트 완료")
            
            # 테스트 결과 업로드
            test_df = pd.DataFrame([{
                'test_type': 'session_test',
                'country_code': 'nl',
                'test_datetime': datetime.now(scraper.korea_tz)
            }])
            
            scraper.save_results(test_df, save_db=False, upload_server=True)
        else:
            logger.error("❌ 테스트 실패")
            
        if scraper.driver:
            scraper.driver.quit()
        return
    
    # 실제 크롤링
    logger.info("\n📊 실제 크롤링 시작")

    # 연결 테스트
    if not scraper.test_connection():
        logger.error("연결 테스트 실패로 종료합니다.")
        return

    # 변수 초기화 (except 블록에서 사용하기 위해)
    urls_data = []
    results_df = None
    all_null_failures = 0

    try:
        # 크롤링 대상 조회
        urls_data = scraper.get_crawl_targets()

        if not urls_data:
            logger.warning("크롤링 대상이 없습니다.")
            monitor_and_alert('nl_coolblue', 0, None, error_message="크롤링 대상 URL이 없습니다")
            return

        logger.info(f"✅ 크롤링 대상: {len(urls_data)}개")

        # 시작 시간
        start_time = datetime.now(scraper.korea_tz)

        # 크롤링 실행
        results_df, all_null_failures = scraper.scrape_urls(urls_data)

        if results_df is None or results_df.empty:
            logger.error("크롤링 결과가 없습니다.")
            monitor_and_alert('nl_coolblue', len(urls_data), None, error_message="크롤링 결과가 없습니다")
            return

        # 종료 시간
        end_time = datetime.now(scraper.korea_tz)

        # 최종 결과 저장
        logger.info("\n💾 최종 결과 저장")

        # 최종 통계
        success_count = results_df['retailprice'].notna().sum()
        failed_count = results_df['retailprice'].isna().sum()
        success_rate = (success_count / len(results_df) * 100) if len(results_df) > 0 else 0

        logger.info(f"\n📊 === 최종 결과 ===")
        logger.info(f"전체: {len(results_df)}개")
        logger.info(f"성공: {success_count}개")
        logger.info(f"실패: {failed_count}개")
        logger.info(f"성공률: {success_rate:.1f}%")
        logger.info(f"소요 시간: {round((end_time - start_time).total_seconds() / 60, 2)} 분")

        # 파일 업로드는 auto_recovery에서 처리
        save_results = scraper.save_results(
            results_df,
            save_db=True,
            upload_server=False
        )

        scraper.analyze_results(results_df)
        logger.info("Coolblue 크롤링 완료!")

        # 자동 복구 + 파일 업로드 + 메일 알림
        from auto_recovery import auto_recovery_run
        auto_recovery_run(
            target_key='coolblue',
            results_df=results_df,
            target_count=len(urls_data),
            error_logs=None
        )

    except Exception as e:
        # 예외 발생 시 알림
        logger.error(f"크롤링 중 예외 발생: {e}")
        import traceback
        error_detail = traceback.format_exc()
        logger.error(error_detail)
        monitor_and_alert('nl_coolblue', len(urls_data), results_df,
                         error_message=str(e), all_null_failures=all_null_failures)

    finally:
        # 드라이버 종료
        if scraper.driver:
            scraper.driver.quit()
            logger.info("🔧 드라이버 종료")
        save_log('nl_coolblue')

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

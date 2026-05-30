# -*- coding: utf-8 -*-
"""
Amazon 영국 가격 추출 시스템 V2 (타임존 분리 버전)
원본 uk.py 기반 - DB/타임존/파일서버 설정만 V2로 변경
- 현지시간(영국)과 한국시간 분리 저장
- 새 데이터베이스 사용 (DB_CONFIG_V2)
- 핵심 로직은 원본과 동일
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
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Import database configuration V2
from config import DB_CONFIG_V2 as DB_CONFIG
from config import FILE_SERVER_CONFIG
from alert_monitor import monitor_and_alert
from null_screenshot import is_null_result, capture_and_upload
from cookie_consent import accept_cookies

class AmazonUKScraper:
    def __init__(self):
        self.driver = None
        self.db_engine = None
        self.country_code = 'gb'
        self.wait = None
        # V2: 타임존 분리 (현지시간 + 한국시간)
        self.korea_tz = pytz.timezone('Asia/Seoul')
        self.local_tz = pytz.timezone('Europe/London')  # 영국 현지 시간
        
        self.setup_db_connection()
        self.setup_uk_selectors()
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
    
    def setup_uk_selectors(self):
        """영국 전용 선택자 설정 - DB에서 관리되지 않는 항목만"""
        self.selectors = {
            # price, price_fraction, title, ships_from, sold_by, imageurl, availability는 DB에서 로드
            'price': [],
            'price_fraction': [],
            'title': [],
            'ships_from': [],
            'sold_by': [],
            'imageurl': [],
            'availability': [],
            'excluded_price_areas': [
                'product-comparison',
                'comparison-desktop',
                'non-deal-price',
                'strikethrough',
                'list-price',
                'rrp-price',
                'was-price',
                'usedBuySection',
                'capacity-selection',
                'gebraucht'
            ]
        }
    
    def load_selectors_from_db(self):
        """DB에서 선택자 로드"""
        if not self.db_engine:
            logger.warning("DB 연결이 없어 선택자 로드 불가")
            return

        try:
            query = """
            SELECT element_type, selector_value, priority
            FROM amazon_selectors
            WHERE country_code = 'gb'
              AND is_active = TRUE
            ORDER BY element_type, priority DESC
            """

            df = pd.read_sql(query, self.db_engine)
            logger.info(f"DB에서 UK 선택자 로드: {len(df)}개")

            if len(df) > 0:
                for element_type in df['element_type'].unique():
                    selectors = df[df['element_type'] == element_type]['selector_value'].tolist()
                    if element_type in self.selectors:
                        existing = self.selectors[element_type]
                        self.selectors[element_type] = selectors + [s for s in existing if s not in selectors]
                    else:
                        self.selectors[element_type] = selectors

                logger.info("DB 선택자 로드 완료")
        except Exception as e:
            logger.error(f"DB 선택자 로드 실패: {e}")
    
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
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            ]
            options.add_argument(f'--user-agent={random.choice(user_agents)}')
            options.add_experimental_option('prefs', {'intl.accept_languages': 'en-GB,en,de-DE,de'})
            
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
            self.wait = WebDriverWait(self.driver, 20)

            logger.info("드라이버 설정 완료")
            return True
        except Exception as e:
            logger.error(f"드라이버 설정 실패: {e}")
            return False
    
    def handle_captcha_or_block_page(self):
        """차단 페이지나 캡차 처리"""
        try:
            logger.info("차단/캡차 페이지 확인 중...")
            
            continue_texts = ['Continue shopping', 'Continue', 'Weiter einkaufen', 'Weiter']
            
            all_selectors = []
            for text in continue_texts:
                all_selectors.extend([
                    f"//button[contains(text(), '{text}')]",
                    f"//input[@value='{text}']",
                    f"//a[contains(text(), '{text}')]",
                    f"//span[contains(text(), '{text}')]/ancestor::button"
                ])
            
            all_selectors.extend([
                "//button[contains(@class, 'a-button-primary')]",
                "button.a-button-primary",
                "button[type='submit']"
            ])
            
            for selector in all_selectors:
                try:
                    if selector.startswith('//'):
                        button = self.driver.find_element(By.XPATH, selector)
                    else:
                        button = self.driver.find_element(By.CSS_SELECTOR, selector)
                    
                    if button and button.is_displayed():
                        try:
                            button.click()
                            logger.info("Continue 버튼 클릭 성공")
                        except:
                            self.driver.execute_script("arguments[0].click();", button)
                            logger.info("JavaScript 클릭 성공")
                        
                        time.sleep(3)
                        return True
                except Exception:
                    continue
            
            return False
        except Exception as e:
            logger.error(f"차단 페이지 처리 중 오류: {e}")
            return False
    
    def is_excluded_price_element(self, element):
        """가격 요소가 제외 대상인지 확인"""
        try:
            element_html = self.driver.execute_script("return arguments[0].outerHTML;", element)
            excluded_areas = self.selectors.get('excluded_price_areas', [])
            
            for pattern in excluded_areas:
                if pattern in element_html.lower():
                    return True
            
            return False
        except Exception:
            return False
    
    def detect_currency_and_parse_price(self, price_text, url):
        """URL을 기반으로 통화를 감지하고 가격 파싱"""
        try:
            price_text = price_text.strip()
            
            # URL 기반으로 국가 감지
            is_german = '.de/' in url or 'amazon.de' in url
            
            # 무효한 패턴 확인
            invalid_patterns = [
                r'^[a-zA-Z\s]+$',
                r'^\d+\s*[a-zA-Z]',
            ]
            
            if is_german:
                invalid_patterns.extend([
                    r'war\s*[€]',
                    r'uvp\s*[€]',
                    r'gebraucht'
                ])
            else:
                invalid_patterns.extend([
                    r'was\s*[£]',
                    r'list\s*price',
                    r'buy\s*used'
                ])
            
            for pattern in invalid_patterns:
                if re.search(pattern, price_text, re.IGNORECASE):
                    return None
            
            if is_german:
                # 독일: 유로 처리
                cleaned = re.sub(r'[€\s]', '', price_text)
                
                # 독일 형식: 1.234,99
                if re.match(r'^\d{1,3}(?:\.\d{3})*(?:,\d{1,2})?$', cleaned):
                    try:
                        if ',' in cleaned:
                            parts = cleaned.split(',')
                            if len(parts) == 2:
                                whole_part = parts[0].replace('.', '')
                                decimal_part = parts[1]
                                price_value = float(f"{whole_part}.{decimal_part}")
                            else:
                                return None
                        else:
                            price_value = float(cleaned.replace('.', ''))
                        
                        if 5 <= price_value <= 50000:
                            return str(price_value)
                    except:
                        pass
            else:
                # 영국: 파운드 처리
                cleaned = re.sub(r'[£\s]', '', price_text)
                
                # 영국 형식: 1,234.99
                if re.match(r'^\d{1,4}(?:,\d{3})*(?:\.\d{1,2})?$', cleaned):
                    try:
                        price_value = float(cleaned.replace(',', ''))
                        if 5 <= price_value <= 50000:
                            return cleaned.replace(',', '')
                    except:
                        pass
            
            return None
        except Exception:
            return None
    
    def extract_price(self, url):
        """가격 추출"""
        logger.info("가격 추출 시작")
        
        price_selectors = self.selectors.get('price', [])
        
        for idx, selector in enumerate(price_selectors, 1):
            try:
                logger.info(f"[{idx}/{len(price_selectors)}] 시도: {selector}")
                
                if selector.startswith('//') or selector.startswith('('):
                    elements = self.driver.find_elements(By.XPATH, selector)
                else:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                
                for element in elements:
                    if element.is_displayed():
                        if self.is_excluded_price_element(element):
                            continue
                        
                        text = self.extract_clean_text_from_element(element)
                        if text:
                            price = self.detect_currency_and_parse_price(text, url)
                            if price:
                                logger.info(f"가격 추출 성공: {price}")
                                return price
            except Exception:
                continue
        
        # whole + fraction 조합 시도
        try:
            whole_elem = self.driver.find_element(By.XPATH, "//*[@id='corePrice_feature_div']//span[@class='a-price-whole']")
            fraction_elem = self.driver.find_element(By.XPATH, "//*[@id='corePrice_feature_div']//span[@class='a-price-fraction']")
            
            if whole_elem and fraction_elem and whole_elem.is_displayed() and fraction_elem.is_displayed():
                whole_text = whole_elem.text.strip()
                fraction_text = fraction_elem.text.strip()
                
                if whole_text and fraction_text:
                    fraction_clean = re.sub(r'[^\d]', '', fraction_text)
                    if fraction_clean:
                        # URL에 따라 형식 결정
                        if '.de/' in url or 'amazon.de' in url:
                            combined_price = f"{whole_text},{fraction_clean}"
                        else:
                            combined_price = f"{whole_text}.{fraction_clean}"
                        
                        price = self.detect_currency_and_parse_price(combined_price, url)
                        if price:
                            logger.info(f"조합 가격 추출 성공: {price}")
                            return price
        except Exception:
            pass
        
        logger.warning("가격 추출 실패")
        return None
    
    def extract_clean_text_from_element(self, element):
        """요소에서 깨끗한 텍스트 추출"""
        try:
            text_methods = [
                element.get_attribute('textContent'),
                element.get_attribute('innerText'),
                element.text
            ]
            
            for text in text_methods:
                if text and text.strip():
                    return text.strip()
            
            return None
        except Exception:
            return None
    
    def extract_element_text(self, selectors, element_name="요소"):
        """선택자 목록에서 텍스트 추출"""
        for selector in selectors:
            try:
                if selector.startswith('//') or selector.startswith('('):
                    elements = self.driver.find_elements(By.XPATH, selector)
                else:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                
                if elements:
                    for element in elements:
                        try:
                            if element.is_displayed():
                                text = self.extract_clean_text_from_element(element)
                                if text:
                                    return text
                        except Exception:
                            continue
            except Exception:
                continue
        
        return None
    
    def extract_ships_from(self, selectors):
        """Ships From 정보 추출 (Fulfilled by Amazon 처리)"""
        for selector in selectors:
            try:
                if selector.startswith('//') or selector.startswith('('):
                    elements = self.driver.find_elements(By.XPATH, selector)
                else:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                
                if elements:
                    for element in elements:
                        try:
                            if element.is_displayed():
                                text = self.extract_clean_text_from_element(element)
                                if text:
                                    # Fulfilled by Amazon인 경우 Amazon만 반환
                                    if "Fulfilled by Amazon" in text:
                                        return "Amazon"
                                    return text
                        except Exception:
                            continue
            except Exception:
                continue
        
        return None
    
    def check_stock_availability(self, url):
        """재고 상태 확인"""
        try:
            is_german = '.de/' in url or 'amazon.de' in url
            
            try:
                availability_elem = self.driver.find_element(By.ID, "availability")
                availability_text = availability_elem.text.lower()
                
                if is_german:
                    unavailable_phrases = [
                        'derzeit nicht verfügbar', 'nicht auf lager', 'ausverkauft',
                        'currently unavailable', 'out of stock', 'temporarily out of stock'
                    ]
                    available_phrases = [
                        'auf lager', 'verfügbar', 'in stock', 'only', 'left in stock'
                    ]
                else:
                    unavailable_phrases = [
                        'currently unavailable', 'out of stock', 'temporarily out of stock'
                    ]
                    available_phrases = [
                        'in stock', 'only', 'left in stock'
                    ]
                
                if any(phrase in availability_text for phrase in unavailable_phrases):
                    return False
                
                if any(phrase in availability_text for phrase in available_phrases):
                    return True
            except:
                pass
            
            buy_buttons = ["add-to-cart-button", "buy-now-button"]
            for button_id in buy_buttons:
                try:
                    button = self.driver.find_element(By.ID, button_id)
                    if button and button.is_enabled():
                        return True
                except:
                    continue
            
            return True
        except Exception:
            return True
    
    def extract_product_info(self, url, row_data, retry_count=0, max_retries=2):
        """제품 정보 추출"""
        try:
            logger.info("=" * 60)
            logger.info(f"제품 정보 추출 시작: {url}")
            
            # URL 확인 (독일 사이트 여부 판별용)
            is_german_site = '.de/' in url or 'amazon.de' in url
            
            self.driver.get(url)
            time.sleep(random.uniform(2, 4))

            # 페이지 로드 대기
            try:
                self.wait.until(lambda driver: driver.execute_script("return document.readyState") == "complete")
            except:
                pass

            # 쿠키 동의 팝업 자동 수락 (있으면 클릭)
            accept_cookies(self.driver, 'amazon_gb')

            # 차단 페이지 확인
            page_source_lower = self.driver.page_source.lower()
            if any(text in page_source_lower for text in ['continue shopping', 'weiter einkaufen']):
                logger.info("차단 페이지 감지 - Continue 버튼 시도")
                self.handle_captcha_or_block_page()
                time.sleep(3)
            
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
                'country_code': 'gb',  # 항상 gb로 고정
                'ships_from': None,
                'channel_name': 'amazon.co.uk',
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
                'sold_by': None,
                'imageurl': None,
                'producturl': url,
                'crawl_datetime': crawl_datetime_iso,
                'crawl_strdatetime': local_time.strftime('%Y%m%d%H%M%S') + f"{local_time.microsecond:06d}"[:4],
                'kr_crawl_datetime': now_time.strftime('%Y-%m-%d %H:%M:%S'),  # V2: 한국시간
                'kr_crawl_strdatetime': now_time.strftime('%Y%m%d%H%M%S') + f"{now_time.microsecond:06d}"[:4],  # V2: 한국시간 문자열
                'title': None,
                'vat': row_data.get('vat', 'x')
            }

            # 제목 추출
            result['title'] = self.extract_element_text(self.selectors.get('title', []), "제목")
            
            # 재고 상태 확인
            has_stock = self.check_stock_availability(url)
            
            # 판매자 정보 추출 (가격 재시도 판단을 위해 먼저 추출)
            # extract_ships_from은 "Fulfilled by Amazon" → "Amazon" 정규화 포함
            # DB selector가 combined-label predicate를 포함하므로 통합 라벨도 자동 처리
            result['ships_from'] = self.extract_ships_from(self.selectors.get('ships_from', []))
            result['sold_by'] = self.extract_element_text(self.selectors.get('sold_by', []), "Sold By")

            has_seller = bool(result['ships_from'] or result['sold_by'])

            # 가격 요소 렌더링 대기 (동적 로딩 지연 대응)
            try:
                self.wait.until(lambda d: d.find_elements(By.CSS_SELECTOR,
                    "span.a-price-whole, .a-price .a-offscreen, #corePrice_feature_div .a-price"))
            except Exception:
                logger.debug("가격 요소 대기 타임아웃, 계속 진행")

            # 가격 추출
            result['retailprice'] = self.extract_price(url)

            # 판매자 정보 있는데 가격 없으면 3초 대기 후 1회 재시도
            if result['retailprice'] is None and has_seller:
                logger.info("판매자 정보 있지만 가격 없음, 3초 대기 후 재시도")
                time.sleep(3)
                result['retailprice'] = self.extract_price(url)

            # 가격 검증
            if result['retailprice']:
                try:
                    price_value = float(re.sub(r'[^\d.]', '', str(result['retailprice'])))
                    if price_value < 5 or price_value > 50000:
                        logger.warning(f"비정상적인 가격 범위: {result['retailprice']}")
                        result['retailprice'] = None
                except:
                    result['retailprice'] = None

            # 이미지 URL 추출
            for selector in self.selectors.get('imageurl', []):
                try:
                    if selector.startswith('//'):
                        element = self.driver.find_element(By.XPATH, selector)
                    else:
                        element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    
                    result['imageurl'] = element.get_attribute('src')
                    if result['imageurl']:
                        break
                except:
                    continue
            
            # VAT 확인
            # page_source = self.driver.page_source.lower()
            # for vat_text in self.selectors.get('vat_text_list', []):
            #     if vat_text.lower() in page_source:
            #         result['vat'] = 'o'
            #         break
            
            site_type = "독일" if is_german_site else "영국"
            logger.info(f"사이트: {site_type} (DB 저장: gb)")
            logger.info(f"제목: {result['title']}")
            logger.info(f"가격: {result['retailprice']}")
            logger.info(f"판매자: {result['sold_by']}")
            logger.info(f"배송지: {result['ships_from']}")
            logger.info(f"이미지: {result['imageurl']}")

            # NULL 필드 발견 시 스크린샷 + S3 업로드
            if is_null_result(result):
                capture_and_upload(self.driver, 'amazon_gb', row_data.get('retailersku', ''), url)

            return result
            
        except Exception as e:
            logger.error(f"페이지 처리 오류: {e}")
            
            if retry_count < max_retries:
                wait_time = (retry_count + 1) * 10
                logger.info(f"{wait_time}초 후 재시도... ({retry_count + 1}/{max_retries})")
                time.sleep(wait_time)
                return self.extract_product_info(url, row_data, retry_count + 1, max_retries)
            
            # 실패 시에도 기본 구조 반환 (가격은 0으로)
            # V2: 타임존 분리

            now_time = datetime.now(self.korea_tz)

            local_time = datetime.now(self.local_tz)

            # ISO 8601 형식 (에러 시에도 필요)
            crawl_dt = local_time.strftime("%Y-%m-%dT%H:%M:%S")
            tz_offset = local_time.strftime("%z")
            tz_formatted = f"{tz_offset[:3]}:{tz_offset[3:]}" if tz_offset else "+00:00"
            crawl_datetime_iso = f"{crawl_dt}{tz_formatted}"

            fail_result = {
                'retailerid': row_data.get('retailerid', ''),
                'country_code': 'gb',  # 항상 gb로 고정
                'ships_from': None,
                'channel_name': 'amazon.co.uk',
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
                'retailprice': "0",
                'sold_by': None,
                'imageurl': None,
                'producturl': url,
                'crawl_datetime': crawl_datetime_iso,
                'crawl_strdatetime': local_time.strftime('%Y%m%d%H%M%S') + f"{local_time.microsecond:06d}"[:4],
                'kr_crawl_datetime': now_time.strftime('%Y-%m-%d %H:%M:%S'),  # V2: 한국시간
                'kr_crawl_strdatetime': now_time.strftime('%Y%m%d%H%M%S') + f"{now_time.microsecond:06d}"[:4],  # V2: 한국시간 문자열
                'title': None,
                'vat': row_data.get('vat', 'x')
            }

            # NULL 필드 발견 시 스크린샷 + S3 업로드 (best-effort)
            try:
                if is_null_result(fail_result):
                    capture_and_upload(self.driver, 'amazon_gb', row_data.get('retailersku', ''), url)
            except Exception:
                pass

            return fail_result

    def get_uk_crawl_targets(self, limit=None):
        """DB에서 UK 크롤링 대상 조회"""
        try:
            query = """
            SELECT *
            FROM samsung_price_tracking_list
            WHERE country = 'gb' 
              AND mall_name = 'amazon'
              AND is_active = TRUE
            """
            
            if limit:
                query += f" LIMIT {limit}"
            
            df = pd.read_sql(query, self.db_engine)
            logger.info(f"UK 크롤링 대상 {len(df)}개 조회")
            return df.to_dict('records')
        except Exception as e:
            logger.error(f"UK 크롤링 대상 조회 실패: {e}")
            return []
    
    def save_to_db(self, df):
        """DB에 결과 저장"""
        if self.db_engine is None:
            logger.warning("DB 연결 없음")
            return False
        
        try:
            table_name = 'amazon_price_crawl_tbl_uk_v2'
            df.to_sql(table_name, self.db_engine, if_exists='append', index=False)
            logger.info(f"DB 저장 완료: {len(df)}개")
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
        base_filename = f"{date_str}_{time_str}_gb_amazon"

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
        """URL 스크래핑"""
        if max_items:
            urls_data = urls_data[:max_items]

        logger.info(f"UK 크롤링 시작 - {len(urls_data)}개 URL")

        if not self.setup_driver():
            logger.error("드라이버 설정 실패")
            return None, []

        results = []
        failed_urls = []
        blocked_page_failures = []  # 차단 페이지로 인한 실패 목록

        for idx, row in enumerate(urls_data):
            try:
                url = row.get('url')
                item_name = row.get('item', 'Unknown')

                logger.info(f"진행률: {idx + 1}/{len(urls_data)} - {item_name}")

                result = self.extract_product_info(url, row)

                # title이 null인 경우 재시도 대상으로 추가 (비정상 상황)
                if result['title'] is None:
                    blocked_page_failures.append({
                        'url': url,
                        'row_data': row,
                        'item': row.get('item', ''),
                        'reason': 'title null - 재시도 필요'
                    })
                    logger.warning(f"title null - 나중에 재시도 예정: {url}")
                elif result['retailprice'] == '0' or result['retailprice'] is None:
                    failed_urls.append({
                        'url': url,
                        'item': row.get('item', ''),
                        'reason': '가격 없음'
                    })

                results.append(result)

                # 중간 저장 (title null 제외 - 재시도 후 저장)
                if (idx + 1) % 10 == 0:
                    interim_df = pd.DataFrame(results[-10:])
                    # title이 있는 것만 저장 (title null은 재시도 후 저장)
                    valid_df = interim_df[interim_df['title'].notna()]
                    if len(valid_df) > 0 and self.db_engine:
                        try:
                            table_name = 'amazon_price_crawl_tbl_uk_v2'
                            valid_df.to_sql(table_name, self.db_engine, if_exists='append', index=False)
                            logger.info(f"UK 중간 저장: {len(valid_df)}개 레코드 (title null 제외: {10 - len(valid_df)}개)")
                        except Exception as e:
                            logger.error(f"중간 저장 실패: {e}")

                if idx < len(urls_data) - 1:
                    wait_time = random.uniform(5, 10)
                    time.sleep(wait_time)

                    if (idx + 1) % 20 == 0:
                        logger.info("20개 처리 완료, 5초 휴식")
                        time.sleep(5)

            except Exception as e:
                logger.error(f"스크래핑 중 오류 (URL: {row.get('url', 'unknown')}): {e}")
                continue

        # 마지막으로 저장되지 않은 나머지 데이터 저장 (10의 배수가 아닌 경우, title null 제외)
        remainder = len(results) % 10
        if remainder > 0 and self.db_engine:
            try:
                remainder_df = pd.DataFrame(results[-remainder:])
                # title이 있는 것만 저장 (title null은 재시도 후 저장)
                valid_df = remainder_df[remainder_df['title'].notna()]
                if len(valid_df) > 0:
                    table_name = 'amazon_price_crawl_tbl_uk_v2'
                    valid_df.to_sql(table_name, self.db_engine, if_exists='append', index=False)
                    logger.info(f"UK 마지막 저장: {len(valid_df)}개 레코드 (title null 제외: {remainder - len(valid_df)}개)")
            except Exception as e:
                logger.error(f"마지막 저장 실패: {e}")

        # 차단 페이지로 인한 실패 목록 재시도 (2회씩)
        if blocked_page_failures:
            logger.info("=" * 60)
            logger.info(f"차단 페이지 실패 {len(blocked_page_failures)}개 재시도 시작")
            logger.info("=" * 60)

            final_blocked_failures = []  # 최종 실패 목록

            for fail_idx, fail_item in enumerate(blocked_page_failures):
                url = fail_item['url']
                row_data = fail_item['row_data']
                logger.info(f"재시도 진행: {fail_idx + 1}/{len(blocked_page_failures)} - {fail_item['item']}")

                # 1회 재시도
                retry_success = False
                logger.info(f"차단 페이지 재시도: {url}")
                result = self.extract_product_info(url, row_data, retry_count=0, max_retries=1)

                # 성공 여부 확인 (title이 있으면 성공)
                if result['title'] is not None:
                    logger.info(f"재시도 성공! title={result['title']}")
                    # 기존 결과에서 해당 URL 결과를 업데이트
                    for i, r in enumerate(results):
                        if r['producturl'] == url:
                            results[i] = result
                            break
                    retry_success = True
                else:
                    logger.warning(f"재시도 실패")

                if not retry_success:
                    logger.error(f"최종 실패 (재시도 후에도 실패): {url}")
                    final_blocked_failures.append({
                        'url': url,
                        'item': fail_item['item'],
                        'reason': '차단 페이지로 인한 최종 실패 (재시도 후)'
                    })

            # 재시도한 데이터 DB 저장 (성공/실패 모두 - 실패는 recovery.py로 복구)
            retry_urls = set(f['url'] for f in blocked_page_failures)
            retry_results = [r for r in results if r['producturl'] in retry_urls]
            if retry_results and self.db_engine:
                try:
                    retry_df = pd.DataFrame(retry_results)
                    table_name = 'amazon_price_crawl_tbl_uk_v2'
                    retry_df.to_sql(table_name, self.db_engine, if_exists='append', index=False)
                    retry_success_count = len(blocked_page_failures) - len(final_blocked_failures)
                    retry_fail_count = len(final_blocked_failures)
                    logger.info(f"재시도 데이터 DB 저장: {len(retry_results)}개 (성공: {retry_success_count}, 실패(title null): {retry_fail_count}개 - recovery.py로 복구 예정)")
                except Exception as e:
                    logger.error(f"재시도 데이터 DB 저장 실패: {e}")

            if final_blocked_failures:
                logger.warning(f"차단 페이지 최종 실패 {len(final_blocked_failures)}개:")
                for fail in final_blocked_failures:
                    logger.warning(f"  - {fail['item']}: {fail['reason']}")
            else:
                logger.info("모든 차단 페이지 실패 항목 재시도 성공!")

            # 최종 실패 목록을 failed_urls에 추가
            failed_urls.extend(final_blocked_failures)
            # blocked_page_failures를 final로 업데이트
            blocked_page_failures = final_blocked_failures

        # 정리
        if failed_urls:
            logger.warning(f"실패 URL {len(failed_urls)}개")

        if self.driver:
            try:
                self.driver.quit()
            except:
                pass

        return pd.DataFrame(results), blocked_page_failures
    
    def analyze_results(self, df):
        """결과 분석"""
        logger.info("결과 분석 시작")
        
        total = len(df)
        with_price = df[df['retailprice'] != '0'].shape[0]
        zero_price = df[df['retailprice'] == '0'].shape[0]
        success_rate = (with_price / total * 100) if total > 0 else 0
        
        logger.info(f"전체: {total}개")
        logger.info(f"가격 성공: {with_price}개")
        logger.info(f"가격 0: {zero_price}개")
        logger.info(f"성공률: {success_rate:.1f}%")
        
        if with_price > 0:
            try:
                price_df = df[df['retailprice'] != '0'].copy()
                price_df['price_numeric'] = price_df['retailprice'].astype(str).str.replace(',', '').astype(float)
                
                logger.info("가격 통계:")
                logger.info(f"  평균: {price_df['price_numeric'].mean():.2f}")
                logger.info(f"  최저: {price_df['price_numeric'].min():.2f}")
                logger.info(f"  최고: {price_df['price_numeric'].max():.2f}")
            except Exception as e:
                logger.warning(f"가격 통계 오류: {e}")

def main():
    """메인 실행 함수"""
    from log_utils import setup_log, save_log
    setup_log('gb_amazon')

    max_items = int(os.getenv('MAX_ITEMS', '0')) or None
    
    print("=" * 60)
    print("Amazon UK Price Scraper (독일 URL 호환)")
    print("=" * 60)
    if max_items:
        print(f"최대: {max_items}개")
    print("=" * 60)
    
    scraper = AmazonUKScraper()
    
    logger.info("크롤링 시작")
    if scraper.db_engine is None:
        logger.error("DB 연결 실패")
        monitor_and_alert('gb', 0, None, error_message="DB 연결 실패")
        return

    urls_data = scraper.get_uk_crawl_targets(limit=max_items)
    if not urls_data:
        logger.warning("크롤링 대상 없음")
        monitor_and_alert('gb', 0, None, error_message="크롤링 대상 없음")
        return

    results_df, blocked_failures = scraper.scrape_urls(urls_data, max_items)
    if results_df is None or results_df.empty:
        logger.error("크롤링 결과 없음")
        monitor_and_alert('gb', len(urls_data), None, error_message="크롤링 결과 없음")
        return

    scraper.analyze_results(results_df)
    # 중간 저장(10개마다)에서 이미 DB 저장했으므로, 여기서는 파일 업로드만 수행
    save_results = scraper.save_results(results_df, save_db=False, upload_server=False)

    logger.info("크롤링 완료!")

    # 자동 복구 + 파일 업로드 + 메일 알림
    from auto_recovery import auto_recovery_run
    auto_recovery_run(
        target_key='gb',
        results_df=results_df,
        target_count=len(urls_data),
        error_logs=None
    )

    save_log('gb_amazon')

if __name__ == "__main__":
    main()
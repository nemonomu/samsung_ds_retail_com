# -*- coding: utf-8 -*-
"""
Amazon UK 가격 추출 시스템 (독일 URL 호환 추가)
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

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# DB 설정
DB_CONFIG = {
    'user': 'admin',
    'password': 'samsungcrawl1!',
    'host': 'samsung-ssd-crawl-db.csnixzmkuppn.ap-northeast-2.rds.amazonaws.com',
    'port': 3306,
    'database': 'ssd_crawl_db'
}

# 파일서버 설정
FILE_SERVER_CONFIG = {
    'host': '3.36.101.24',
    'port': 22,
    'username': 'ftpuser',
    'password': 'samsung0701!',
    'upload_path': '/home/ftpuser/uploads'
}

class AmazonUKScraper:
    def __init__(self):
        self.driver = None
        self.db_engine = None
        self.country_code = 'gb'
        self.wait = None
        self.korea_tz = pytz.timezone('Asia/Seoul')
        
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
        """영국 전용 선택자 설정 (독일 선택자 추가)"""
        self.selectors = {
            'price': [
                # 독일 전용 xpath 추가 (최우선)
                "//*[@id='corePrice_feature_div']/div/div/span[1]/span[1]",
                "/html/body/div[2]/div/div/div[4]/div[1]/div[3]/div/div[1]/div/div/div/form/div/div/div/div/div[3]/div/div[1]/div/div/span[1]/span[1]",
                
                # 요청된 추가 선택자
                "//*[@id='corePriceDisplay_desktop_feature_div']/div[1]/span[1]",
                "//*[@id='usedBuySection']/div[1]/div/span[2]",
                
                # 메인 가격 영역
                "//*[@id='corePrice_feature_div']//span[@class='a-offscreen']",
                "//*[@id='corePriceDisplay_desktop_feature_div']//span[@class='a-offscreen']",
                "//*[@id='apex_desktop']//span[@class='a-price']//span[@class='a-offscreen']",
                
                # 첫 번째 가격만
                "(//span[@class='a-price']//span[@class='a-offscreen'])[1]",
                "(//span[@class='a-price-whole'])[1]",
                
                # 기본 가격 요소들
                "//*[@id='priceblock_ourprice']",
                "//*[@id='priceblock_dealprice']",
                "//*[@id='listPrice']",
                
                # Whole 가격
                "//*[@id='corePrice_feature_div']//span[@class='a-price-whole']",
                "//*[@id='corePriceDisplay_desktop_feature_div']//span[@class='a-price-whole']",
                "//*[@id='apex_desktop']//span[@class='a-price-whole']"
            ],
            'price_fraction': [
                "//*[@id='corePrice_feature_div']//span[@class='a-price-fraction']",
                "//*[@id='corePriceDisplay_desktop_feature_div']//span[@class='a-price-fraction']",
                "//*[@id='apex_desktop']//span[@class='a-price-fraction']",
                "//span[@class='a-price-fraction']"
            ],
            'title': [
                # 독일 전용 xpath 추가 (최우선)
                "//*[@id='productTitle']",
                "/html/body/div[2]/div/div/div[4]/div[4]/div[1]/div/h1/span",
                
                "#productTitle",
                "//span[@id='productTitle']",
                "//h1/span[@id='productTitle']"
            ],
            'ships_from': [
                # 독일 전용 xpath 추가 (최우선)
                "//*[@id='fulfillerInfoFeature_feature_div']/div[2]/div[1]/span",
                
                "//*[@id='SSOFpopoverLink_ubb']",
                "//a[@id='SSOFpopoverLink_ubb']",
                "//div[@id='fulfillerInfoFeature_feature_div']//span"
            ],
            'sold_by': [
                # 독일 전용 xpath 추가 (최우선)
                "//*[@id='merchantInfoFeature_feature_div']/div[2]/div[1]/span",
                
                "//a[@id='sellerProfileTriggerId']",
                "//*[@id='sellerProfileTriggerId']",
                "//div[@id='merchantInfoFeature_feature_div']//a",
                "//div[@id='merchantInfoFeature_feature_div']//span"
            ],
            'imageurl': [
                # 독일 전용 xpath 추가 (최우선)
                "//*[@id='landingImage']",
                "/html/body/div[2]/div/div/div[4]/div[3]/div[1]/div[1]/div/div/div[2]/div[1]/div[1]/ul/li[1]/span/span/div/img",
                
                "//div[@id='imageBlock']//img[@id='landingImage']",
                "//div[@id='main-image-container']//img",
                "//img[@class='a-dynamic-image']"
            ],
            'availability': [
                "//div[@id='availability']//span",
                "//div[@id='availability_feature_div']//span"
            # ],
            # 'vat_text_list': [
            #     # 독일 VAT 텍스트 추가
            #     "inkl. MwSt.",
            #     "inklusive MwSt.", 
            #     "Steuer inbegriffen",
            #     # 영국 VAT 텍스트
            #     "Tax included", 
            #     "include VAT.",
            #     "VAT included"
            ],
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
            ORDER BY element_type, priority ASC
            """
            
            df = pd.read_sql(query, self.db_engine)
            logger.info(f"DB에서 UK 선택자 로드: {len(df)}개")
            
            if len(df) > 0:
                for element_type in df['element_type'].unique():
                    selectors = df[df['element_type'] == element_type]['selector_value'].tolist()
                    if element_type in self.selectors:
                        existing = self.selectors[element_type]
                        self.selectors[element_type] = existing + selectors
                    else:
                        self.selectors[element_type] = selectors
                
                logger.info("DB 선택자 병합 완료")
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
            options.add_argument('--disable-gpu')
            options.add_argument('--disable-software-rasterizer')
            
            user_agents = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            ]
            options.add_argument(f'--user-agent={random.choice(user_agents)}')
            options.add_experimental_option('prefs', {'intl.accept_languages': 'en-GB,en,de-DE,de'})
            
            self.driver = uc.Chrome(options=options)
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
    
    def extract_product_info(self, url, row_data, retry_count=0, max_retries=3):
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
            
            # 차단 페이지 확인
            page_source_lower = self.driver.page_source.lower()
            if any(text in page_source_lower for text in ['continue shopping', 'weiter einkaufen']):
                logger.info("차단 페이지 감지 - Continue 버튼 시도")
                self.handle_captcha_or_block_page()
                time.sleep(3)
            
            now_time = datetime.now(self.korea_tz)
            
            result = {
                'retailerid': row_data.get('retailerid', ''),
                'country_code': 'gb',  # 항상 gb로 고정
                'ships_from': None,
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
                'crawl_datetime': now_time.strftime('%Y-%m-%d %H:%M:%S'),
                'crawl_strdatetime': now_time.strftime('%Y%m%d%H%M%S') + f"{now_time.microsecond:06d}"[:4],
                'title': None,
                'vat': row_data.get('vat', 'x')
            }
            
            # 제목 추출
            result['title'] = self.extract_element_text(self.selectors.get('title', []), "제목")
            
            # 재고 상태 확인
            has_stock = self.check_stock_availability(url)
            
            # 가격 추출
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
            
            # 가격이 없거나 재고가 없으면 0으로 설정
            if result['retailprice'] is None:
                result['retailprice'] = None
            
            # 판매자 정보 추출 (수정된 함수 사용)
            result['ships_from'] = self.extract_ships_from(self.selectors.get('ships_from', []))
            result['sold_by'] = self.extract_element_text(self.selectors.get('sold_by', []), "Sold By")
            
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
            
            return result
            
        except Exception as e:
            logger.error(f"페이지 처리 오류: {e}")
            
            if retry_count < max_retries:
                wait_time = (retry_count + 1) * 10
                logger.info(f"{wait_time}초 후 재시도... ({retry_count + 1}/{max_retries})")
                time.sleep(wait_time)
                return self.extract_product_info(url, row_data, retry_count + 1, max_retries)
            
            # 실패 시에도 기본 구조 반환 (가격은 0으로)
            now_time = datetime.now(self.korea_tz)
            return {
                'retailerid': row_data.get('retailerid', ''),
                'country_code': 'gb',  # 항상 gb로 고정
                'ships_from': None,
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
                'crawl_datetime': now_time.strftime('%Y-%m-%d %H:%M:%S'),
                'crawl_strdatetime': now_time.strftime('%Y%m%d%H%M%S') + f"{now_time.microsecond:06d}"[:4],
                'title': None,
                'vat': row_data.get('vat', 'x')
            }
    
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
            table_name = 'amazon_price_crawl_tbl_uk'
            df.to_sql(table_name, self.db_engine, if_exists='append', index=False)
            logger.info(f"DB 저장 완료: {len(df)}개")
            return True
        except Exception as e:
            logger.error(f"DB 저장 실패: {e}")
            return False
    
    def upload_to_file_server(self, local_file_path, remote_filename=None):
        """파일서버에 업로드"""
        try:
            transport = paramiko.Transport((FILE_SERVER_CONFIG['host'], FILE_SERVER_CONFIG['port']))
            transport.connect(
                username=FILE_SERVER_CONFIG['username'],
                password=FILE_SERVER_CONFIG['password']
            )
            sftp = paramiko.SFTPClient.from_transport(transport)
            
            if remote_filename is None:
                remote_filename = os.path.basename(local_file_path)
            
            gb_dir = f"{FILE_SERVER_CONFIG['upload_path']}/gb"
            
            try:
                sftp.stat(gb_dir)
            except FileNotFoundError:
                logger.info(f"UK 디렉토리 생성: {gb_dir}")
                sftp.mkdir(gb_dir)
            
            remote_path = f"{gb_dir}/{remote_filename}"
            sftp.put(local_file_path, remote_path)
            logger.info(f"파일서버 업로드 완료: {remote_path}")
            
            sftp.close()
            transport.close()
            
            return True
        except Exception as e:
            logger.error(f"파일서버 업로드 실패: {e}")
            return False
    
    def save_results(self, df, save_db=True, upload_server=True):
        """결과 저장"""
        now = datetime.now(self.korea_tz)
        base_filename = f"{now.strftime('%Y%m%d%H%M%S')}_gb_amazon"
        
        results = {'db_saved': False, 'server_uploaded': False}
        
        if save_db:
            results['db_saved'] = self.save_to_db(df)
        
        if upload_server:
            try:
                csv_filename = f'{base_filename}.csv'
                df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
                
                if self.upload_to_file_server(csv_filename):
                    results['server_uploaded'] = True
                
            #    excel_filename = f'{base_filename}.xlsx'
             #   with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
              #      df.to_excel(writer, sheet_name='All_Results', index=False)
                    
               #     price_df = df[df['retailprice'] != '0']
                #    if not price_df.empty:
                 #       price_df.to_excel(writer, sheet_name='With_Prices', index=False)
                    
               #     zero_price_df = df[df['retailprice'] == '0']
               #     if not zero_price_df.empty:
               #         zero_price_df.to_excel(writer, sheet_name='Zero_Prices', index=False)
                
              #  self.upload_to_file_server(excel_filename)
                
                if os.path.exists(csv_filename):
                    os.remove(csv_filename)
               # if os.path.exists(excel_filename):
                #    os.remove(excel_filename)
                
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
            return None
        
        results = []
        failed_urls = []
        
        try:
            for idx, row in enumerate(urls_data):
                url = row.get('url')
                item_name = row.get('item', 'Unknown')
                
                logger.info(f"진행률: {idx + 1}/{len(urls_data)} - {item_name}")
                
                result = self.extract_product_info(url, row)
                
                if result['retailprice'] == '0':
                    failed_urls.append({
                        'url': url,
                        'item': row.get('item', ''),
                        'reason': '가격 없음'
                    })
                
                results.append(result)
                
                # 중간 저장
                if (idx + 1) % 10 == 0:
                    interim_df = pd.DataFrame(results[-10:])
                    if self.db_engine:
                        try:
                            table_name = 'amazon_price_crawl_tbl_uk'
                            interim_df.to_sql(table_name, self.db_engine, if_exists='append', index=False)
                            logger.info("중간 저장: 10개 레코드")
                        except Exception as e:
                            logger.error(f"중간 저장 실패: {e}")
                
                if idx < len(urls_data) - 1:
                    wait_time = random.uniform(5, 10)
                    time.sleep(wait_time)
                    
                    if (idx + 1) % 20 == 0:
                        logger.info("20개 처리 완료, 30초 휴식")
                        time.sleep(30)
        
        except Exception as e:
            logger.error(f"스크래핑 중 오류: {e}")
        
        finally:
            if failed_urls:
                logger.warning(f"실패 URL {len(failed_urls)}개")
            
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass
        
        return pd.DataFrame(results)
    
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
        return
    
    urls_data = scraper.get_uk_crawl_targets(limit=max_items)
    if not urls_data:
        logger.warning("크롤링 대상 없음")
        return
    
    results_df = scraper.scrape_urls(urls_data, max_items)
    if results_df is None or results_df.empty:
        logger.error("크롤링 결과 없음")
        return
    
    scraper.analyze_results(results_df)
    save_results = scraper.save_results(results_df, save_db=True, upload_server=True)
    
    logger.info("저장 결과:")
    logger.info(f"DB: {'성공' if save_results['db_saved'] else '실패'}")
    logger.info(f"파일: {'성공' if save_results['server_uploaded'] else '실패'}")
    logger.info("크롤링 완료!")

if __name__ == "__main__":
    main()
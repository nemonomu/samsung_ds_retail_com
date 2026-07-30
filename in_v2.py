"""
Amazon India 가격 추출 시스템 - 완전 강화 버전
주요 특징:
1. 인도 전용 선택자 및 설정
2. 개선된 ships_from 선택자
3. 루피(₹) 가격 처리
4. 인도 특화 VAT/GST 처리
5. ships_from과 sold_by 모두 없을 경우 가격 0 처리
6. 파란색 링크 우회 기능 강화
7. 추천상품/관련상품 필터링 시스템 추가
8. centerCol 내부만 타겟팅하도록 가격 추출 개선
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
from null_screenshot import FULL_NULL_FIELDS, is_null_result, capture_and_upload
from cookie_consent import accept_cookies

class AmazonIndiaScraper:
    def __init__(self):
        self.driver = None
        self.db_engine = None
        self.country_code = 'in'
        self.wait = None
        # V2: 타임존 분리 (현지시간 + 한국시간)
        self.korea_tz = pytz.timezone('Asia/Seoul')
        self.local_tz = pytz.timezone('Asia/Kolkata')  # 인도 현지 시간
        
        # DB 연결 설정
        self.setup_db_connection()
        
        # 인도 전용 선택자 설정
        self.setup_india_selectors()
        
        # DB에서 선택자 로드 (덮어쓰기/병합)
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
    
    def setup_india_selectors(self):
        """인도 전용 선택자 설정 - price, title, ships_from, sold_by, imageurl은 DB에서 관리"""
        self.selectors = {
            'in': {
                # DB에서 로드되는 선택자들 (빈 배열로 초기화)
                'price': [],
                'title': [],
                'ships_from': [],
                'sold_by': [],
                'imageurl': [],
                # 하드코딩 유지하는 선택자들
                'availability': [
                    "//div[@id='availability']//span",
                    "//div[@id='availability_feature_div']//span",
                    "//span[@class='a-size-medium a-color-success']",
                    "//span[@class='a-size-medium a-color-price']",
                    "//div[@id='availability']//span[@class='a-size-medium']",
                    "//span[contains(text(), 'In stock')]",
                    "//span[contains(text(), 'Available')]"
                ],
                'stock_flag': [
                    'Currently unavailable',
                    'Out of Stock',
                    'Temporarily out of stock',
                    'Currently not available',
                    'This item is currently unavailable'
                ],
                'blocked_patterns': [
                    'sorry',
                    'robot check',
                    '503 Service Unavailable',
                    'Something went wrong',
                    'access denied',
                    'enter the characters',
                    'verify you are human'
                ],
                # 추천상품/관련상품 제외 영역들
                'excluded_price_areas': [
                    # 추천상품 영역들
                    "//div[@id='similarities_feature_div']",
                    "//div[@id='sp_detail2']",
                    "//div[@id='sp_detail']",
                    "//div[@cel_widget_id='sp_detail-huc-feature']",
                    "//div[@cel_widget_id='sp_detail2-huc-feature']",
                    "//div[@id='HLCXComparisonWidget_feature_div']",
                    "//div[@id='productDetails_feature_div']//div[contains(@class, 'comparison')]",
                    "//div[contains(@class, 'a-carousel')]",
                    "//div[contains(@class, 'similar-products')]",
                    "//div[contains(@class, 'recommended')]",
                    "//div[contains(@class, 'sp-huc')]",
                    "//div[@id='vse-related-videos-widget_feature_div']",
                    "//div[@id='purchase-sims-feature_div']",
                    "//div[@id='sims-fbt']",
                    "//div[@id='comparison-widget']",
                    # 페이지 하단 추천
                    "//div[@id='rhf']",
                    "//div[@id='customer_review_foreign_language_text']",
                    "//div[@id='reviewsMedley']",
                    "//div[@class='a-row a-expander-container']",
                    "//div[contains(@class, 'a-expander-content')]"
                ],
                # XPath 패턴 제외 목록
                'excluded_xpath_patterns': [
                    # 추천상품 관련 패턴들
                    r".*similarities.*",
                    r".*sp_detail.*",
                    r".*comparison.*", 
                    r".*carousel.*",
                    r".*similar.*",
                    r".*recommend.*",
                    r".*related.*",
                    r".*also-bought.*",
                    r".*frequently-bought.*",
                    r".*customers-who.*",
                    r".*sponsored.*",
                    r".*ad-feedback.*"
                ]
            }
        }
    
    def load_selectors_from_db(self):
        """DB에서 Amazon India용 선택자 로드"""
        if not self.db_engine:
            logger.warning("DB 연결이 없어 선택자 로드 불가")
            return
            
        try:
            query = """
            SELECT element_type, selector_value, priority
            FROM amazon_selectors
            WHERE country_code = 'in'
              AND is_active = TRUE
            ORDER BY element_type, priority ASC
            """
            
            df = pd.read_sql(query, self.db_engine)
            logger.info(f"DB에서 인도 선택자 로드: {len(df)}개")
            
            # DB에서 로드한 선택자로 덮어쓰기
            db_selectors = {'in': {}}
            
            for element_type in df['element_type'].unique():
                db_selectors['in'][element_type] = df[df['element_type'] == element_type]['selector_value'].tolist()
                logger.info(f"  - {element_type}: {len(db_selectors['in'][element_type])}개")
            
            # 기본값과 병합 (DB 우선)
            for element_type, selectors in db_selectors['in'].items():
                if element_type in self.selectors['in']:
                    existing = self.selectors['in'][element_type]
                    self.selectors['in'][element_type] = selectors + [s for s in existing if s not in selectors]
                else:
                    self.selectors['in'][element_type] = selectors
            
            logger.info("인도 DB 선택자 로드 완료")
            
        except Exception as e:
            logger.error(f"DB 선택자 로드 실패: {e}")
    
    def setup_driver(self):
        """Chrome 드라이버 설정 - 인도 전용"""
        logger.info("Chrome 드라이버 설정 중 (인도 전용)...")
        
        try:
            options = uc.ChromeOptions()
            
            # 기본 옵션들
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-setuid-sandbox')
            options.add_argument('--disable-web-security')
            options.add_argument('--disable-features=VizDisplayCompositor')
            # 메모리 최적화 옵션
            options.add_argument('--disable-gpu')
            options.add_argument('--disable-extensions')
            options.add_argument('--disable-infobars')
            options.add_argument('--disable-renderer-backgrounding')
            options.add_argument('--js-flags=--max-old-space-size=512')

            # 인도 전용 User-Agent
            india_user_agents = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            ]
            options.add_argument(f'--user-agent={random.choice(india_user_agents)}')
            
            # 인도 언어 설정
            options.add_experimental_option('prefs', {
                'intl.accept_languages': 'en-IN,en,hi',
                'profile.default_content_settings.popups': 0,
                'profile.default_content_setting_values.notifications': 2
            })
            
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
            
            # WebDriverWait 객체 생성
            self.wait = WebDriverWait(self.driver, 20)
            
            logger.info("인도 전용 드라이버 설정 완료")
            return True
            
        except Exception as e:
            logger.error(f"드라이버 설정 실패: {e}")
            return False
    
    def is_excluded_price_element(self, element):
        """가격 요소가 추천상품/관련상품 영역에 있는지 확인"""
        try:
            # 요소의 부모들을 거슬러 올라가며 제외 영역인지 확인
            parent = element
            for _ in range(20):  # 최대 20단계까지 확인
                try:
                    parent = parent.find_element(By.XPATH, "..")
                    
                    # ID나 클래스로 제외 영역 확인
                    element_id = parent.get_attribute('id') or ''
                    element_class = parent.get_attribute('class') or ''
                    
                    # 제외할 영역들 확인
                    excluded_keywords = [
                        'similarities', 'sp_detail', 'comparison', 'carousel',
                        'similar', 'recommend', 'related', 'sponsored',
                        'also-bought', 'frequently-bought', 'customers-who',
                        'ad-feedback', 'rhf', 'reviewsMedley'
                    ]
                    
                    for keyword in excluded_keywords:
                        if keyword in element_id.lower() or keyword in element_class.lower():
                            logger.debug(f"제외 영역 발견: {keyword} in {element_id or element_class}")
                            return True
                    
                    # centerCol 영역에 도달하면 메인 영역으로 판단
                    if element_id == 'centerCol':
                        logger.debug("centerCol 영역 - 메인 제품 영역으로 판단")
                        return False
                        
                except Exception:
                    break
                    
            return False
            
        except Exception as e:
            logger.debug(f"제외 영역 확인 중 오류: {e}")
            return False
    
    @staticmethod
    def get_longest_element_text(element):
        """Return the most complete visible or DOM text for an element."""
        texts = []

        try:
            texts.append(element.text or '')
        except Exception:
            pass

        for attribute in ('textContent', 'innerText'):
            try:
                texts.append(element.get_attribute(attribute) or '')
            except Exception:
                pass

        return max((text.strip() for text in texts), key=len, default='')

    def extract_complete_price_text(self, element):
        """Restore the full price from the selected element's a-price container."""
        fallback_text = self.get_longest_element_text(element)

        try:
            price_containers = element.find_elements(
                By.XPATH,
                "ancestor-or-self::*[contains(concat(' ', normalize-space(@class), ' '), ' a-price ')][1]"
            )
            if not price_containers:
                return fallback_text

            price_container = price_containers[0]
            offscreen_elements = price_container.find_elements(
                By.XPATH,
                ".//*[contains(concat(' ', normalize-space(@class), ' '), ' a-offscreen ')]"
            )
            for offscreen in offscreen_elements:
                full_text = self.get_longest_element_text(offscreen)
                if full_text:
                    logger.debug(f'Full price found in the same price container: {full_text!r}')
                    return full_text

            whole_elements = price_container.find_elements(
                By.XPATH,
                ".//*[contains(concat(' ', normalize-space(@class), ' '), ' a-price-whole ')]"
            )
            fraction_elements = price_container.find_elements(
                By.XPATH,
                ".//*[contains(concat(' ', normalize-space(@class), ' '), ' a-price-fraction ')]"
            )

            if whole_elements and fraction_elements:
                whole_text = self.get_longest_element_text(whole_elements[0])
                fraction_text = self.get_longest_element_text(fraction_elements[0])
                whole_digits = re.sub(r'[^\d]', '', whole_text)
                fraction_digits = re.sub(r'[^\d]', '', fraction_text)

                if whole_digits and re.fullmatch(r'\d{2}', fraction_digits):
                    combined_text = f'{whole_digits}.{fraction_digits}'
                    logger.debug(f'Whole/fraction price restored in the same container: {combined_text!r}')
                    return combined_text

        except Exception as e:
            logger.debug(f'Failed to restore full price; using selected text: {e}')

        return fallback_text

    def click_blue_link_and_return(self, original_url):
        """파란색 링크 클릭 후 원래 URL로 돌아가기 - 강화된 버전"""
        try:
            logger.info("파란색 링크 찾는 중...")
            
            # 파란색 링크 선택자들 (인도 특화 + 강화)
            blue_link_selectors = [
                # 힌디어
                "//a[contains(text(), 'वापस जाएं')]",
                "//a[contains(text(), 'होमपेज पर वापस')]",
                "//a[contains(text(), 'Amazon पर वापस')]",
                # 영어
                "//a[contains(text(), 'Click here to go back')]",
                "//a[contains(text(), 'back to Amazon')]",
                "//a[contains(text(), 'Go back to Amazon')]",
                "//a[contains(text(), 'Return to Amazon')]",
                "//a[contains(text(), 'Continue shopping')]",
                "//a[contains(text(), 'Back to results')]",
                "//a[contains(text(), 'Try again')]",
                # 버튼 형태의 링크들
                "//button[contains(text(), 'Continue')]",
                "//button[contains(text(), 'Go back')]",
                "//input[@type='submit' and contains(@value, 'Continue')]",
                # 일반적인 패턴들
                "//a[contains(@href, 'amazon.in')]",
                "//a[contains(@href, 'amazon.')]",
                "//a[contains(@class, 'a-link-normal')]",
                "//a[contains(@class, 'a-button')]",
                # CSS 선택자들
                "a.a-link-normal",
                "a.a-button",
                "button.a-button-primary",
                ".a-button-inner",
                "#a-autoid-0"
            ]
            
            # 파란색 링크 클릭 시도
            for idx, selector in enumerate(blue_link_selectors, 1):
                try:
                    logger.debug(f"[{idx}/{len(blue_link_selectors)}] 선택자 시도: {selector}")
                    
                    # XPath와 CSS 선택자 구분
                    if selector.startswith('//') or selector.startswith('('):
                        elements = self.driver.find_elements(By.XPATH, selector)
                    else:
                        elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    
                    for element in elements:
                        if element.is_displayed() and element.is_enabled():
                            link_text = element.text.strip()
                            href = element.get_attribute('href') or ''
                            
                            logger.info(f"파란색 링크 발견: '{link_text}' (href: {href[:50]}...)")
                            
                            # 링크 클릭
                            try:
                                # 스크롤해서 요소를 보이게 하기
                                self.driver.execute_script("arguments[0].scrollIntoView(true);", element)
                                time.sleep(1)
                                
                                # 클릭 시도
                                element.click()
                                logger.info("파란색 링크 클릭 완료")
                                
                                # 잠시 대기
                                time.sleep(random.uniform(2, 4))
                                
                                # 원래 URL로 다시 접속
                                logger.info(f"원래 URL로 재접속: {original_url}")
                                self.driver.get(original_url)
                                
                                # 페이지 로드 대기
                                time.sleep(random.uniform(3, 5))
                                
                                return True
                                
                            except Exception as click_error:
                                logger.debug(f"클릭 실패: {click_error}")
                                continue
                        
                except Exception as selector_error:
                    logger.debug(f"선택자 시도 실패: {selector} - {selector_error}")
                    continue
            
            logger.warning("파란색 링크를 찾을 수 없음")
            return False
            
        except Exception as e:
            logger.error(f"파란색 링크 처리 오류: {e}")
            return False

    def handle_captcha_or_block_page(self, original_url=None):
        """차단 페이지나 캡차 처리 - 강화된 버전"""
        try:
            logger.info("차단/캡차 페이지 확인 중...")
            
            # 파란색 링크 우회 시도 (우선순위)
            if original_url and self.click_blue_link_and_return(original_url):
                logger.info("파란색 링크 우회 성공")
                return True
            
            # Continue shopping 버튼 찾기 - 확장된 버전
            continue_selectors = [
                # 기본 버튼들
                "//button[contains(text(), 'Continue shopping')]",
                "//button[contains(@class, 'a-button-primary')]",
                "//input[@type='submit' and contains(@value, 'Continue')]",
                "//a[contains(text(), 'Continue shopping')]",
                "//span[contains(text(), 'Continue shopping')]/ancestor::button",
                # 다양한 언어 지원
                "//button[contains(text(), 'खरीदारी जारी रखें')]", # 힌디어
                "//button[contains(text(), 'Try again')]",
                "//button[contains(text(), 'Go back')]",
                "//button[contains(text(), 'Retry')]",
                # CSS 선택자들
                "button.a-button-primary",
                "button[type='submit']",
                "#a-autoid-0",
                ".a-button-inner",
                "input[type='submit'][value*='Continue']",
                # 추가 패턴들
                "//div[@class='a-button a-button-primary']",
                "//span[@class='a-button-text'][contains(text(), 'Continue')]/parent::span/parent::button",
                "//form//button[contains(@class, 'primary')]",
                "//input[@class='a-button-input' and @type='submit']"
            ]
            
            for idx, selector in enumerate(continue_selectors, 1):
                try:
                    logger.debug(f"[{idx}/{len(continue_selectors)}] Continue 버튼 선택자 시도: {selector}")
                    
                    if selector.startswith('//') or selector.startswith('('):
                        elements = self.driver.find_elements(By.XPATH, selector)
                    else:
                        elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    
                    for element in elements:
                        if element and element.is_displayed() and element.is_enabled():
                            button_text = element.text.strip()
                            logger.info(f"✅ Continue 버튼 발견: '{button_text}' (선택자: {selector})")
                            
                            # 스크롤해서 보이게 하기
                            self.driver.execute_script("arguments[0].scrollIntoView(true);", element)
                            time.sleep(1)
                            
                            # 클릭
                            element.click()
                            time.sleep(3)
                            logger.info("✅ Continue 버튼 클릭 완료")
                            return True
                        
                except Exception as e:
                    logger.debug(f"Continue 버튼 시도 실패: {e}")
                    continue
            
            logger.warning("Continue 버튼을 찾을 수 없음")
            return False
            
        except Exception as e:
            logger.error(f"차단 페이지 처리 중 오류: {e}")
            return False
    
    def is_page_blocked(self):
        """페이지 차단 감지 - 개선된 로직"""
        try:
            page_title = self.driver.title.lower()
            page_source = self.driver.page_source.lower()
            current_url = self.driver.current_url.lower()
            
            # 1. 정상 페이지 확인 (우선 체크)
            normal_indicators = [
                'add to cart',
                'buy now',
                'product title',
                'price',
                'availability',
                'customer reviews',
                'product details',
                'ships from',
                'sold by'
            ]
            
            normal_count = sum(1 for indicator in normal_indicators if indicator in page_source)
            
            # 정상 지표가 3개 이상이면 정상 페이지
            if normal_count >= 3:
                logger.info(f"✅ 정상 페이지 확인: {normal_count}개 지표 발견")
                return False
            
            # 2. 명확한 차단 징후만 체크
            serious_blocked_indicators = [
                'enter the characters you see below',
                'to continue shopping, please type the characters',
                'verify you are human',
                'access denied',
                'automated access',
                'suspicious activity',
                '503 service unavailable',
                'sorry, we just need to make sure you',
                'are you a robot',
                'the web address you entered is not a functioning page on our site',
                "we're sorry. the web address"
            ]
            
            for pattern in serious_blocked_indicators:
                if pattern in page_source:
                    logger.warning(f"🚫 명확한 차단 감지: '{pattern}'")
                    return True
            
            # 3. Amazon India 도메인 확인
            if 'amazon.in' not in current_url:
                logger.warning(f"Amazon India 페이지가 아님: {current_url}")
                return True
            
            # 4. 페이지 제목 확인
            if 'sorry' in page_title or 'error' in page_title:
                logger.warning(f"🚫 오류 페이지 제목: {page_title}")
                return True
            
            # 5. 기본적인 Amazon 요소 확인
            essential_elements = ['productTitle', 'price', 'availability', 'add-to-cart']
            found_elements = 0
            
            for element_id in essential_elements:
                try:
                    self.driver.find_element(By.ID, element_id)
                    found_elements += 1
                except:
                    pass
            
            # 필수 요소가 하나도 없으면 차단 가능성
            if found_elements == 0:
                logger.warning("⚠️ 필수 요소 없음 - 차단 가능성 있음")
                # 하지만 바로 차단으로 판단하지 말고 다른 방법으로 확인
                return False
            
            logger.info(f"✅ 정상 페이지로 판단 (필수 요소: {found_elements}개)")
            return False
            
        except Exception as e:
            logger.error(f"페이지 차단 확인 중 오류: {e}")
            return False
    
    def extract_price_india(self):
        """인도 루피 가격 추출 - centerCol 타겟팅 및 추천상품 필터링 강화"""
        price_selectors = self.selectors['in']['price']
        
        logger.info(f"\n루피 가격 추출 시작 - 선택자: {len(price_selectors)}개")
        
        for idx, selector in enumerate(price_selectors, 1):
            try:
                logger.info(f"\n  [{idx}/{len(price_selectors)}] 가격 선택자 시도: {selector}")
                
                if selector.startswith('//'):
                    elements = WebDriverWait(self.driver, 1).until(
                        EC.presence_of_all_elements_located((By.XPATH, selector))
                    )
                else:
                    elements = WebDriverWait(self.driver, 1).until(
                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, selector))
                    )
                
                logger.info(f"      발견된 요소: {len(elements)}개")
                
                for i, element in enumerate(elements):
                    try:
                        if element.is_displayed():
                            # 추천상품/관련상품 영역 제외
                            if self.is_excluded_price_element(element):
                                logger.debug(f"      추천상품 영역 제외: 요소 {i+1}")
                                continue
                            
                            # 여러 방법으로 텍스트 추출
                            price_text = self.extract_complete_price_text(element)
                            
                            if price_text:
                                logger.info(f"        텍스트: '{price_text}'")
                                
                                # 인도 루피 파싱
                                price = self.parse_rupee_price(price_text)
                                if price:
                                    logger.info(f"      ✅ 루피 가격 추출 성공: {price} (원본: {price_text})")
                                    return price
                    
                    except Exception as e:
                        logger.error(f"      요소 처리 오류: {e}")
                
            except TimeoutException:
                logger.info("      타임아웃")
            except Exception as e:
                logger.error(f"      오류: {str(e)}")
        
        # JavaScript로 루피 가격 찾기 (centerCol 내부만 검색)
        logger.info("\n💡 JavaScript로 centerCol 내부 루피 가격 검색...")
        try:
            js_result = self.driver.execute_script(r"""
                const centerCol = document.getElementById('centerCol');
                if (!centerCol) {
                    return [];
                }
                
                const elements = centerCol.querySelectorAll('span, div');
                const results = [];
                
                // 제외할 영역들의 선택자
                const excludeSelectors = [
                    '[id*="similarities"]', '[id*="sp_detail"]', '[id*="comparison"]',
                    '[class*="carousel"]', '[class*="similar"]', '[class*="recommend"]',
                    '[class*="related"]', '[class*="sponsored"]', '[class*="ad-feedback"]'
                ];
                
                for (let elem of elements) {
                    // 제외 영역 체크
                    let isExcluded = false;
                    for (let selector of excludeSelectors) {
                        if (elem.closest(selector)) {
                            isExcluded = true;
                            break;
                        }
                    }
                    
                    if (isExcluded) continue;
                    
                    const text = elem.textContent.trim();
                    // 루피 가격 패턴 매칭
                    if (text.match(/₹[\d,]+\.?\d*/) || text.match(/\d+[.,]\d{2}/) || text.match(/₹\s*\d/)) {
                        if (text.length < 30) {
                            results.push({
                                text: text,
                                tag: elem.tagName,
                                class: elem.className
                            });
                        }
                    }
                }
                
                return results.slice(0, 10);
            """)
            
            if js_result:
                logger.info(f"  JavaScript 결과: {len(js_result)}개")
                for r in js_result:
                    logger.info(f"    - '{r['text']}'")
                    price = self.parse_rupee_price(r['text'])
                    if price:
                        logger.info(f"  ✅ JavaScript 루피 가격 추출 성공: {price}")
                        return price
                        
        except Exception as e:
            logger.error(f"  JavaScript 가격 추출 실패: {e}")
        
        logger.error("\n루피 가격 추출 완전 실패")
        return None
    
    def parse_rupee_price(self, price_text):
        """루피 가격 파싱 - 통화기호 완전 제거, 정수/소수점 자동 처리"""
        try:
            # 기본 정리
            price_text = price_text.strip()
            logger.debug(f"원본 가격 텍스트: '{price_text}'")
            
            # 루피 기호와 공백 제거
            price_text = re.sub(r'[₹\s]', '', price_text)
            
            # 콤마 제거
            # Treat a final two-digit comma group as a decimal separator.
            if '.' not in price_text and ',' in price_text:
                comma_parts = price_text.split(',')
                if (
                    len(comma_parts) >= 2
                    and len(comma_parts[-1]) == 2
                    and all(part.isdigit() for part in comma_parts)
                ):
                    price_text = ''.join(comma_parts[:-1]) + '.' + comma_parts[-1]
                else:
                    price_text = ''.join(comma_parts)
            else:
                price_text = price_text.replace(',', '')
            
            # 숫자만 추출
            match = re.search(r'(\d+\.?\d*)', price_text)
            if match:
                price = float(match.group(1))
                
                # 소수점 이하가 0이면 정수로 변환
                if price == int(price):
                    price = int(price)
                    logger.debug(f"파싱된 가격 (정수): {price}")
                else:
                    logger.debug(f"파싱된 가격 (소수): {price}")
                
                return price
                
        except Exception as e:
            logger.debug(f"루피 가격 파싱 오류: {price_text} - {e}")
            
        return None
    
    def extract_element_text(self, selectors, element_name="요소"):
        """선택자 목록에서 텍스트 추출"""
        logger.info(f"\n{element_name} 추출 시작 - 총 {len(selectors)}개 선택자")
        
        for idx, selector in enumerate(selectors, 1):
            try:
                logger.info(f"\n  [{idx}/{len(selectors)}] 시도: {selector}")
                
                if selector.startswith('//') or selector.startswith('('):
                    elements = self.driver.find_elements(By.XPATH, selector)
                else:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                
                logger.info(f"      발견된 요소: {len(elements)}개")
                
                if elements:
                    for i, element in enumerate(elements):
                        try:
                            if element.is_displayed():
                                text1 = element.text.strip()
                                text2 = element.get_attribute('textContent').strip() if element.get_attribute('textContent') else ""
                                text3 = element.get_attribute('innerText').strip() if element.get_attribute('innerText') else ""
                                
                                text = max([text1, text2, text3], key=len)
                                
                                if text:
                                    logger.info(f"      추출 성공: '{text[:100]}'")
                                    return text
                        except Exception as e:
                            logger.error(f"      요소 처리 중 오류: {e}")
                
            except Exception as e:
                logger.error(f"      선택자 오류: {str(e)}")
        
        logger.error(f"\n{element_name} 추출 완전 실패")
        return None
    
    def check_stock_availability(self):
        """재고 상태 확인"""
        try:
            # availability div 확인
            try:
                availability_elem = self.driver.find_element(By.ID, "availability")
                availability_text = availability_elem.text.lower()
                
                if any(phrase in availability_text for phrase in [
                    'currently unavailable',
                    'out of stock',
                    'temporarily out of stock',
                    'currently not available'
                ]):
                    logger.info(f"재고 없음: {availability_text}")
                    return False
                    
                if any(phrase in availability_text for phrase in [
                    'in stock',
                    'available',
                    'only',
                    'left in stock'
                ]):
                    logger.info(f"재고 있음: {availability_text}")
                    return True
                    
            except NoSuchElementException:
                logger.debug("availability 요소를 찾을 수 없음")
            
            # 구매 버튼 확인
            buy_buttons = [
                "add-to-cart-button",
                "buy-now-button",
                "add-to-cart-button-ubb"
            ]
            
            for button_id in buy_buttons:
                try:
                    button = self.driver.find_element(By.ID, button_id)
                    if button and button.is_enabled():
                        logger.info("구매 버튼 활성화 - 재고 있음")
                        return True
                except:
                    continue
            
            # 기본값: 재고 있음
            logger.info("재고 상태 불명확 - 기본값: 재고 있음")
            return True
            
        except Exception as e:
            logger.warning(f"재고 확인 중 오류: {e}")
            return True
    
    def apply_price_zero_rule(self, ships_from, sold_by, price):
        """ships_from과 sold_by가 모두 없을 경우 가격을 0으로 설정하는 함수"""
        try:
            # None이거나 빈 문자열이거나 공백만 있는 경우를 모두 체크
            ships_from_empty = not ships_from or not ships_from.strip()
            sold_by_empty = not sold_by or not sold_by.strip()
            
            if ships_from_empty and sold_by_empty:
                logger.info("⚠️ ships_from과 sold_by가 모두 없음 -> 가격을 0으로 설정")
                return None
            else:
                logger.info(f"✅ ships_from 또는 sold_by 중 하나 이상 존재 -> 기존 가격 유지")
                return price
                
        except Exception as e:
            logger.error(f"가격 0 규칙 적용 중 오류: {e}")
            return price
    
    def extract_product_info(self, url, row_data, retry_count=0, max_retries=3):
        """제품 정보 추출"""
        try:
            logger.info(f"\n{'='*60}")
            logger.info("인도 Amazon 제품 정보 추출")
            logger.info(f"URL: {url}")
            logger.info(f"브랜드: {row_data.get('brand', 'N/A')}")
            logger.info(f"제품: {row_data.get('item', 'N/A')}")
            
            # 페이지 로드
            self.driver.get(url)
            time.sleep(random.uniform(3, 6))

            # 쿠키 동의 팝업 자동 수락 (있으면 클릭)
            accept_cookies(self.driver, 'amazon_in')

            # 차단 페이지 처리
            page_source_lower = self.driver.page_source.lower()
            if 'continue shopping' in page_source_lower:
                logger.info("⚠️ 차단 페이지 감지")
                self.handle_captcha_or_block_page(url)
                time.sleep(3)
            
            # 차단 확인
            if self.is_page_blocked():
                logger.error("❌ 페이지 차단됨")
                raise Exception("페이지 차단됨")
            
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
                'country_code': 'in',
                'ships_from': None,
                'channel_name': 'amazon.in',
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
                'vat': row_data.get('vat', 'o')
            }

            # 제목 추출
            result['title'] = self.extract_element_text(
                self.selectors['in']['title'],
                "제목"
            )

            # 재고 확인
            has_stock = self.check_stock_availability()

            # 재고 없으면 price, ships_from, sold_by 수집 건너뛰기
            if not has_stock:
                logger.info("⏭️ 재고 없음 - price, ships_from, sold_by 수집 건너뛰기")
                result['retailprice'] = None
                result['ships_from'] = None
                result['sold_by'] = None
            else:
                # 루피 가격 추출 (강화된 필터링)
                result['retailprice'] = self.extract_price_india()

                # Ships From / Sold By: DB selector가 combined-label predicate를 포함하므로 자동 처리
                result['ships_from'] = self.extract_element_text(
                    self.selectors['in']['ships_from'],
                    "Ships From"
                )
                result['sold_by'] = self.extract_element_text(
                    self.selectors['in']['sold_by'],
                    "Sold By"
                )

                # ships_from과 sold_by가 모두 없을 경우 가격을 0으로 설정
                result['retailprice'] = self.apply_price_zero_rule(
                    result['ships_from'],
                    result['sold_by'],
                    result['retailprice']
                )
            
            # 이미지 URL 추출
            for selector in self.selectors['in']['imageurl']:
                try:
                    if selector.startswith('//'):
                        element = self.driver.find_element(By.XPATH, selector)
                    else:
                        element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    
                    result['imageurl'] = element.get_attribute('src')
                    if result['imageurl']:
                        logger.debug("✅ 이미지 URL 추출 성공")
                        break
                except:
                    continue
            
            # 결과 요약
            logger.info(f"\n📊 인도 추출 결과:")
            logger.info(f"   📌 제목: {result['title'][:50] + '...' if result['title'] and len(result['title']) > 50 else result['title']}")
            logger.info(f"   💰 가격: ₹{result['retailprice']}" if result['retailprice'] else "   💰 가격: 없음")
            logger.info(f"   🚢 Ships From: {result['ships_from']}")
            logger.info(f"   🏪 판매자: {result['sold_by']}")

            # NULL 필드 발견 시 스크린샷 + S3 업로드
            if is_null_result(result, FULL_NULL_FIELDS):
                capture_and_upload(self.driver, 'amazon_in', row_data.get('retailersku', ''), url, result)

            return result

        except Exception as e:
            logger.error(f"❌ 페이지 처리 오류: {e}")
            
            if retry_count < max_retries:
                wait_time = (retry_count + 1) * 10
                logger.info(f"🔄 {wait_time}초 후 재시도... ({retry_count + 1}/{max_retries})")
                time.sleep(wait_time)
                
                try:
                    self.driver.refresh()
                except:
                    logger.info("🔧 드라이버 재시작")
                    self.driver.quit()
                    self.setup_driver()
                
                return self.extract_product_info(url, row_data, retry_count + 1, max_retries)
            
            # 최종 실패 시 기본값
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
                'country_code': 'in',
                'ships_from': None,
                'channel_name': 'amazon.in',
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
                'vat': row_data.get('vat', 'o')
            }

            # NULL 필드 발견 시 스크린샷 + S3 업로드 (best-effort)
            try:
                if is_null_result(fail_result, FULL_NULL_FIELDS):
                    capture_and_upload(self.driver, 'amazon_in', row_data.get('retailersku', ''), url, fail_result)
            except Exception:
                pass

            return fail_result

    def get_crawl_targets(self, limit=None):
        """DB에서 인도 크롤링 대상 조회"""
        try:
            query = """
            SELECT *
            FROM samsung_price_tracking_list
            WHERE country = 'in' 
              AND mall_name = 'amazon'
              AND is_active = TRUE
            """
                
            if limit:
                query += f" LIMIT {limit}"
            
            df = pd.read_sql(query, self.db_engine)
            logger.info(f"✅ 인도 크롤링 대상 {len(df)}개 조회")
            return df.to_dict('records')
            
        except Exception as e:
            logger.error(f"크롤링 대상 조회 실패: {e}")
            return []
    
    def save_to_db(self, df):
        """DB에 결과 저장 - 통화기호 제거 및 정수/소수점 자동 처리"""
        if self.db_engine is None:
            logger.info("DB 연결이 없어 DB 저장 건너뜀")
            return False
        
        try:
            # 가격 컬럼에서 통화기호 제거 및 정수/소수점 처리
            if 'retailprice' in df.columns:
                # 문자열로 저장된 가격이 있다면 숫자로 변환
                df['retailprice'] = pd.to_numeric(df['retailprice'], errors='coerce')
                
                # 소수점 이하가 0인 경우 정수로 변환
                mask = df['retailprice'].notna()
                df.loc[mask, 'retailprice'] = df.loc[mask, 'retailprice'].apply(
                    lambda x: int(x) if x == int(x) else x
                )
                
                logger.info("✅ 가격 데이터 정수/소수점 자동 처리 완료")
            
            table_name = 'amazon_price_crawl_tbl_ind_v2'
            df.to_sql(table_name, self.db_engine, if_exists='append', index=False)
            logger.info(f"✅ 인도 DB 저장: {len(df)}개 → {table_name}")
            
            # 저장된 가격 데이터 샘플 로그 (천단위 구분자 포함)
            price_data = df[df['retailprice'].notna()]['retailprice'].head(3)
            if not price_data.empty:
                formatted_prices = []
                for price in price_data:
                    if price == int(price):
                        formatted_prices.append(f"{int(price):,}")
                    else:
                        formatted_prices.append(f"{price:,.2f}")
                logger.info(f"💰 저장된 가격 샘플: {formatted_prices}")
            
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
    def save_results(self, df, save_db=True, upload_server=True):
        """결과 저장"""
        now = datetime.now(self.korea_tz)
        date_str = now.strftime('%Y%m%d')
        time_str = now.strftime('%H%M%S')
        base_filename = f"{date_str}_{time_str}_in_amazon"

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
        
        logger.info(f"\n{'='*80}")
        logger.info("🇮🇳 Amazon India 크롤링 시작 (강화 버전)")
        logger.info(f"📌 대상: {len(urls_data)}개 제품")
        logger.info(f"{'='*80}\n")
        
        if not self.setup_driver():
            logger.error("드라이버 설정 실패")
            return None
        
        results = []

        try:
            for idx, row in enumerate(urls_data):
                logger.info(f"\n진행률: {idx + 1}/{len(urls_data)} ({(idx + 1)/len(urls_data)*100:.1f}%)")

                url = row.get('url')

                # 개별 제품마다 try-except 처리
                try:
                    result = self.extract_product_info(url, row)
                    results.append(result)
                except Exception as product_error:
                    logger.error(f"❌ 제품 수집 실패 (URL: {url}): {product_error}")
                    logger.info("⏭️  다음 제품으로 계속 진행...")
                    # 실패한 제품도 기본값으로 결과에 추가
                    now_time = datetime.now(self.korea_tz)
                    local_time = datetime.now(self.local_tz)
                    crawl_dt = local_time.strftime("%Y-%m-%dT%H:%M:%S")
                    tz_offset = local_time.strftime("%z")
                    tz_formatted = f"{tz_offset[:3]}:{tz_offset[3:]}" if tz_offset else "+00:00"
                    crawl_datetime_iso = f"{crawl_dt}{tz_formatted}"

                    failed_result = {
                        'retailerid': row.get('retailerid', ''),
                        'country_code': 'in',
                        'ships_from': None,
                        'channel_name': 'amazon.in',
                        'channel': row.get('channel', 'Online'),
                        'retailersku': row.get('retailersku', ''),
                        'brand': row.get('brand', ''),
                        'brand_eng': row.get('brand_eng', row.get('brand', '')),
                        'form_factor': row.get('form_factor', ''),
                        'segment_lv1': row.get('seg_lv1', ''),
                        'segment_lv2': row.get('seg_lv2', ''),
                        'segment_lv3': row.get('seg_lv3', ''),
                        'capacity': row.get('capacity', ''),
                        'item': row.get('item', ''),
                        'retailprice': None,
                        'sold_by': None,
                        'imageurl': None,
                        'producturl': url,
                        'crawl_datetime': crawl_datetime_iso,
                        'crawl_strdatetime': local_time.strftime('%Y%m%d%H%M%S') + f"{local_time.microsecond:06d}"[:4],
                        'kr_crawl_datetime': now_time.strftime('%Y-%m-%d %H:%M:%S'),
                        'kr_crawl_strdatetime': now_time.strftime('%Y%m%d%H%M%S') + f"{now_time.microsecond:06d}"[:4],
                        'title': None,
                        'vat': row.get('vat', 'o')
                    }
                    results.append(failed_result)

                # 대기
                if idx < len(urls_data) - 1:
                    wait_time = random.uniform(5, 10)
                    logger.info(f"⏳ {wait_time:.1f}초 대기...")
                    time.sleep(wait_time)

        except Exception as e:
            logger.error(f"❌ 전체 스크래핑 오류: {e}")

        finally:
            if self.driver:
                self.driver.quit()

        return pd.DataFrame(results)
    
    def analyze_results(self, df):
        """결과 분석"""
        logger.info("\nAmazon India 결과 분석 (강화 버전)")
        logger.info("="*50)
        
        total = len(df)
        with_price = df['retailprice'].notna().sum()
        success_rate = (with_price / total * 100) if total > 0 else 0
        
        logger.info(f"전체 제품: {total}개")
        logger.info(f"가격 추출 성공: {with_price}개")
        logger.info(f"성공률: {success_rate:.1f}%")
        
        if with_price > 0:
            price_df = df[df['retailprice'].notna()]
            logger.info("\n가격 통계:")
            
            # 통계값도 정수/소수점 자동 처리
            mean_price = price_df['retailprice'].mean()
            min_price = price_df['retailprice'].min()
            max_price = price_df['retailprice'].max()
            median_price = price_df['retailprice'].median()
            
            # 소수점 이하가 0이면 정수로 표시 (천단위 구분자 포함)
            def format_price(price):
                if price == int(price):
                    return f"{int(price):,}"  # 천단위 구분자 포함
                else:
                    return f"{price:,.2f}"    # 소수점도 천단위 구분자 포함
            
            logger.info(f"   평균가: {format_price(mean_price)}")
            logger.info(f"   최저가: {format_price(min_price)}")
            logger.info(f"   최고가: {format_price(max_price)}")
            logger.info(f"   중간값: {format_price(median_price)}")

def main():
    """메인 실행 함수"""
    from log_utils import setup_log, save_log
    setup_log('in_amazon')

    test_mode = os.getenv('TEST_MODE', 'false').lower() == 'true'
    max_items = int(os.getenv('MAX_ITEMS', '0')) or None

    print(f"\n{'='*80}")
    print("🇮🇳 Amazon India 가격 추출 시스템 v2.0 (완전 강화 버전)")
    print(f"{'='*80}")
    print("📌 국가: India")
    print(f"📌 모드: {'테스트' if test_mode else '실제'}")
    print("📌 강화 기능:")
    print("   - 파란색 링크 우회 시스템")
    print("   - 추천상품/관련상품 필터링")
    print("   - centerCol 타겟팅 가격 추출")
    print("   - 인도 특화 힌디어/영어 지원")
    print("   - 루피 가격 처리 개선")
    if max_items:
        print(f"📌 최대 처리 수: {max_items}개")
    print(f"{'='*80}\n")
    
    # 스크래퍼 초기화
    scraper = AmazonIndiaScraper()
    
    if scraper.db_engine is None:
        logger.error("DB 연결 실패로 종료")
        monitor_and_alert('in', 0, None, error_message="DB 연결 실패")
        return
    
    # 테스트 모드
    if test_mode:
        logger.info("🧪 테스트 모드 실행...")
        test_data = [{
            'url': 'https://www.amazon.in/dp/B0CTRXBKHP',
            'brand': 'Crucial',
            'item': 'T705 1TB',
            'retailerid': 'TEST001',
            'retailersku': 'TEST001',
            'channel': 'Online',
            'seg_lv1': 'SSD',
            'seg_lv2': 'Consumer',
            'seg_lv3': 'NVMe',
            'capacity': '1TB',
            'form_factor': 'M.2'
        }]
        
        results_df = scraper.scrape_urls(test_data)
        if results_df is not None and not results_df.empty:
            scraper.analyze_results(results_df)
            scraper.save_results(results_df, save_db=True, upload_server=True)
        return
    
    # 실제 크롤링
    logger.info("📊 인도 전체 크롤링 시작")
    urls_data = scraper.get_crawl_targets(limit=max_items)
    
    if not urls_data:
        logger.warning("크롤링 대상이 없습니다.")
        monitor_and_alert('in', 0, None, error_message="크롤링 대상 URL이 없습니다")
        return
    
    logger.info(f"✅ 크롤링 대상: {len(urls_data)}개")
    
    results_df = scraper.scrape_urls(urls_data, max_items)
    
    if results_df is None or results_df.empty:
        logger.error("크롤링 결과가 없습니다.")
        monitor_and_alert('in', len(urls_data), None, error_message="크롤링 결과가 없습니다")
        return
    
    scraper.analyze_results(results_df)
    
    save_results = scraper.save_results(
        results_df,
        save_db=True,
        upload_server=False  # auto_recovery에서 처리
    )

    logger.info(f"DB 저장: {'성공' if save_results['db_saved'] else '실패'}")
    logger.info("인도 크롤링 완료!")

    # 자동 복구 + 파일 업로드 + 메일 알림
    from auto_recovery import auto_recovery_run
    auto_recovery_run(
        target_key='in',
        results_df=results_df,
        target_count=len(urls_data),
        error_logs=None
    )

    save_log('in_amazon')

if __name__ == "__main__":
    print("\n📦 필요한 패키지:")
    print("pip install undetected-chromedriver selenium pandas pymysql sqlalchemy paramiko openpyxl")
    print("\n⚠️ 환경변수 설정:")
    print("export TEST_MODE=false")
    print("export MAX_ITEMS=10")
    print()
    
    main()

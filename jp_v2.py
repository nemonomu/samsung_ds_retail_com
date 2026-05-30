"""
Amazon 가격 추출 시스템 - 완전체 버전 (일본 아마존 차단 페이지 처리 개선)
핵심 로직:
1. ships_from과 sold_by 둘 다 비어있으면 가격 0
2. 하나라도 있으면 기존 가격 추출 로직 사용
3. 차단 페이지 감지 시 "ショッピングを続ける" 버튼 클릭 후 원래 URL 재시도
4. 재고 확인 등 복잡한 로직 제거
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

class AmazonScraper:
    def __init__(self, country_code='usa'):
        self.driver = None
        self.db_engine = None
        self.country_code = country_code.lower()
        self.wait = None
        # V2: 타임존 분리 (현지시간 + 한국시간)
        self.korea_tz = pytz.timezone('Asia/Seoul')
        self.local_tz = pytz.timezone('Asia/Tokyo')  # 일본 현지 시간
        
        # DB 연결 설정
        self.setup_db_connection()
        
        # 기본 선택자 설정
        self.setup_default_selectors()
        
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
            logger.info("✅ DB 연결 설정 완료")
            
        except Exception as e:
            logger.error(f"❌ DB 연결 실패: {e}")
            self.db_engine = None
    
    def setup_default_selectors(self):
        """기본 선택자 설정 - price, title, ships_from, sold_by, imageurl은 DB에서 관리"""
        self.selectors = {
            self.country_code: {
                # price, title, ships_from, sold_by, imageurl은 DB(amazon_selectors)에서 로드
                'price': [],
                'title': [],
                'ships_from': [],
                'sold_by': [],
                'imageurl': [],
                'availability': [
                    "//div[@id='availability']//span",
                    "//div[@id='availability_feature_div']//span",
                    "//span[@class='a-size-medium a-color-success']",
                    "//span[@class='a-size-medium a-color-price']"
                ],
                'stock_flag': [
                    'Currently unavailable',
                    'Out of Stock',
                    'Temporarily out of stock'
                ],
                'blocked_patterns': [
                    'sorry',
                    'robot check',
                    '503 Service Unavailable',
                    'Something went wrong',
                    'access denied'
                ]
            }
        }
    
    def load_selectors_from_db(self):
        """DB에서 Amazon용 선택자 로드"""
        if not self.db_engine:
            logger.warning("DB 연결이 없어 선택자 로드 불가")
            return
            
        try:
            query = """
            SELECT element_type, selector_value, priority
            FROM amazon_selectors
            WHERE country_code = %s
              AND is_active = TRUE
            ORDER BY element_type, priority ASC
            """
            
            df = pd.read_sql(query, self.db_engine, params=(self.country_code,))
            logger.info(f"✅ DB에서 선택자 로드 시작: {len(df)}개")
            
            # DB에서 로드한 선택자로 덮어쓰기
            db_selectors = {self.country_code: {}}
            
            for element_type in df['element_type'].unique():
                db_selectors[self.country_code][element_type] = df[df['element_type'] == element_type]['selector_value'].tolist()
                logger.info(f"  - {element_type}: {len(db_selectors[self.country_code][element_type])}개")
            
            # 기본값과 병합 (DB 우선)
            for element_type, selectors in db_selectors[self.country_code].items():
                if element_type in self.selectors[self.country_code]:
                    # DB 선택자를 앞에 추가 (우선순위)
                    existing = self.selectors[self.country_code][element_type]
                    self.selectors[self.country_code][element_type] = selectors + [s for s in existing if s not in selectors]
                else:
                    self.selectors[self.country_code][element_type] = selectors
            
            logger.info(f"✅ DB 선택자 로드 완료")
            
        except Exception as e:
            logger.error(f"❌ DB 선택자 로드 실패: {e}")
    
    def setup_driver(self):
        """Chrome 드라이버 설정"""
        logger.info("🔧 Chrome 드라이버 설정 중...")
        
        try:
            options = uc.ChromeOptions()
            
            # 기본 옵션들
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

            # User-Agent 설정
            user_agents = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            ]
            options.add_argument(f'--user-agent={random.choice(user_agents)}')
            
            # 언어 설정 (국가별)
            language_map = {
                'usa': 'en-US,en',
                'gb': 'en-GB,en',
                'de': 'de-DE,de',
                'fr': 'fr-FR,fr',
                'es': 'es-ES,es',
                'it': 'it-IT,it',
                'jp': 'ja-JP,ja',
                'in': 'en-IN,en'
            }
            lang = language_map.get(self.country_code, 'en-US,en')
            options.add_experimental_option('prefs', {'intl.accept_languages': lang})
            
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
            
            logger.info("✅ 드라이버 설정 완료")
            return True
            
        except Exception as e:
            logger.error(f"❌ 드라이버 설정 실패: {e}")
            return False
    
    def handle_captcha_or_block_page(self, target_url=None):
        """차단 페이지나 캡차 처리 - Continue shopping 버튼 클릭 후 원래 URL로 이동"""
        try:
            logger.info("🔍 차단/캡차 페이지 확인 중...")
            
            # 일본 아마존 전용 선택자를 우선으로 배치 (실제 XPath 최우선)
            continue_selectors = [
                # 실제 일본 아마존 "ショッピングを続ける" 버튼 XPath (최우선)
                "/html/body/center/center/p/table/tbody/tr/td/p[5]/a/img",
                "/html/body/center/center/p/table/tbody/tr/td/p[5]/a",
                "//table//p[5]//a//img",
                "//table//p[5]//a",
                
                # 기존 일본 아마존 선택자들
                "//input[@alt='ショッピングを続ける']",
                "//button[contains(text(), 'ショッピングを続ける')]",
                "//a[contains(text(), 'ショッピングを続ける')]",
                "//span[contains(text(), 'ショッピングを続ける')]/ancestor::button",
                "//span[contains(text(), 'ショッピングを続ける')]/ancestor::a",
                "input[alt*='ショッピングを続ける']",
                "input[value*='ショッピングを続ける']",
                
                # 기존 영어 선택자들
                "//button[contains(text(), 'Continue shopping')]",
                "//button[contains(@class, 'a-button-primary')]",
                "//input[@type='submit' and contains(@value, 'Continue')]",
                "//a[contains(text(), 'Continue shopping')]",
                "//span[contains(text(), 'Continue shopping')]/ancestor::button",
                "button.a-button-primary",
                "button[type='submit']",
                "#a-autoid-0",
                ".a-button-inner"
            ]
            
            button_clicked = False
            
            for selector in continue_selectors:
                try:
                    logger.debug(f"  시도 중: {selector}")
                    
                    if selector.startswith('//'):
                        # XPath
                        elements = self.driver.find_elements(By.XPATH, selector)
                        selector_type = "XPath"
                    elif selector.startswith('#') or selector.startswith('.') or '[' in selector:
                        # CSS Selector
                        elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        selector_type = "CSS"
                    else:
                        # CSS Selector (default)
                        elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        selector_type = "CSS"
                    
                    logger.debug(f"      타입: {selector_type}, 발견: {len(elements)}개")
                    
                    if elements:
                        for element in elements:
                            try:
                                if element.is_displayed() and element.is_enabled():
                                    logger.info(f"✅ Continue 버튼 발견: {selector}")
                                    
                                    # 스크롤해서 버튼이 보이도록
                                    self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
                                    time.sleep(1)
                                    
                                    # 클릭 시도 (여러 방법)
                                    try:
                                        element.click()
                                        logger.info("✅ 일반 클릭 성공")
                                        button_clicked = True
                                    except:
                                        try:
                                            # JavaScript 클릭
                                            self.driver.execute_script("arguments[0].click();", element)
                                            logger.info("✅ JavaScript 클릭 성공")
                                            button_clicked = True
                                        except:
                                            # ActionChains 클릭
                                            from selenium.webdriver.common.action_chains import ActionChains
                                            ActionChains(self.driver).move_to_element(element).click().perform()
                                            logger.info("✅ ActionChains 클릭 성공")
                                            button_clicked = True
                                    
                                    if button_clicked:
                                        break
                                        
                            except Exception as e:
                                logger.debug(f"      요소 클릭 오류: {e}")
                                continue
                        
                        if button_clicked:
                            break
                            
                except Exception as e:
                    logger.debug(f"      선택자 오류: {str(e)}")
                    continue
            
            if not button_clicked:
                # 모든 버튼/링크를 검사해서 관련 텍스트가 있는지 확인
                try:
                    clickable_elements = self.driver.find_elements(By.TAG_NAME, "button") + \
                                       self.driver.find_elements(By.TAG_NAME, "input") + \
                                       self.driver.find_elements(By.TAG_NAME, "a")
                    
                    continue_keywords = ['continue', 'shopping', 'ショッピング', '続ける', '継続']
                    
                    for element in clickable_elements:
                        try:
                            if not element.is_displayed() or not element.is_enabled():
                                continue
                                
                            # 텍스트, alt, value 속성 확인
                            element_text = (element.text or '').lower()
                            element_alt = (element.get_attribute('alt') or '').lower()
                            element_value = (element.get_attribute('value') or '').lower()
                            
                            all_text = f"{element_text} {element_alt} {element_value}"
                            
                            if any(keyword in all_text for keyword in continue_keywords):
                                logger.info(f"✅ 버튼 발견 (텍스트 매칭): '{element.text or element_alt or element_value}'")
                                
                                # 스크롤해서 버튼이 보이도록
                                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
                                time.sleep(1)
                                
                                try:
                                    element.click()
                                    button_clicked = True
                                    logger.info("✅ 텍스트 매칭 버튼 클릭 성공")
                                    break
                                except:
                                    try:
                                        self.driver.execute_script("arguments[0].click();", element)
                                        button_clicked = True
                                        logger.info("✅ 텍스트 매칭 JavaScript 클릭 성공")
                                        break
                                    except:
                                        continue
                                        
                        except Exception:
                            continue
                            
                except Exception as e:
                    logger.debug(f"전체 버튼 검사 오류: {e}")
            
            if button_clicked:
                logger.info("🔄 Continue 버튼 클릭 완료, 페이지 로딩 대기 중...")
                time.sleep(5)  # 페이지 전환 대기
                
                # 원래 URL로 이동 (target_url이 제공된 경우)
                if target_url:
                    logger.info(f"🎯 원래 URL로 이동: {target_url}")
                    self.driver.get(target_url)
                    time.sleep(3)
                    self.wait_for_page_load()
                
                return True
            else:
                logger.debug("Continue 버튼을 찾을 수 없음")
                return False
            
        except Exception as e:
            logger.error(f"차단 페이지 처리 중 오류: {e}")
            return False
    
    def is_page_blocked(self):
        """페이지 차단 감지 - 정상 제품 페이지 요소가 있으면 차단으로 간주하지 않음"""
        try:
            page_title = self.driver.title.lower()
            page_source = self.driver.page_source.lower()
            current_url = self.driver.current_url.lower()

            # 먼저 정상 제품 페이지 요소 확인 (차단 페이지 오판 방지)
            normal_page_indicators = [
                (By.ID, "productTitle"),
                (By.CLASS_NAME, "a-price-whole"),
                (By.ID, "landingImage"),
                (By.ID, "availability"),
                (By.ID, "centerCol")
            ]

            normal_elements_found = 0
            for by, value in normal_page_indicators:
                try:
                    element = self.driver.find_element(by, value)
                    if element and element.is_displayed():
                        normal_elements_found += 1
                except:
                    continue

            # 정상 페이지 요소가 2개 이상 있으면 정상 페이지로 판단
            if normal_elements_found >= 2:
                logger.info(f"✅ 정상 제품 페이지 확인됨 (요소 {normal_elements_found}개 발견)")
                return False

            # 정상 페이지 요소가 없을 때만 차단 페이지 텍스트 확인
            # 일본 아마존 차단 페이지 특징
            japanese_block_indicators = [
                'ご迷惑をおかけしています',  # "ご迷惑をおかけしています"
                'お客様のリクエストの処理中にエラーが発生しました',
                'しばらくしてから',
                'amazon.co.jpホームへ'
            ]

            # 일본어 차단 페이지 확인
            for indicator in japanese_block_indicators:
                if indicator in page_source:
                    logger.warning(f"🚫 일본 아마존 차단 페이지 감지: '{indicator}'")
                    return True

            # 기존 영어 차단 징후들
            serious_blocked_indicators = {
                'title': [
                    '503',
                    'access denied',
                    'error has occurred',
                    'sorry'
                ],
                'content': [
                    'enter the characters',
                    'verify you are human',
                    'access denied',
                    'automated access',
                    'suspicious activity',
                    'robot check'
                ]
            }
            
            # 제목 확인
            for pattern in serious_blocked_indicators['title']:
                if pattern in page_title:
                    logger.warning(f"🚫 차단 감지 (제목): '{pattern}' in '{page_title}'")
                    return True

            # Continue shopping이 있으면 차단 페이지로 간주
            if 'continue shopping' in page_source or 'ショッピングを続ける' in page_source:
                logger.warning("🚫 Continue shopping 페이지 감지")
                return True

            # 본문 확인
            for pattern in serious_blocked_indicators['content']:
                if pattern in page_source:
                    logger.warning(f"🚫 차단 감지 (본문): '{pattern}'")
                    
                    # 스크린샷 저장
                    try:
                        screenshot_name = f"blocked_{self.country_code}_{datetime.now(self.korea_tz).strftime('%Y%m%d_%H%M%S')}.png"
                        self.driver.save_screenshot(screenshot_name)
                        logger.info(f"📸 차단 페이지 스크린샷 저장: {screenshot_name}")
                    except:
                        pass
                    
                    return True
            
            # Amazon 페이지가 아닌 경우
            if 'amazon' not in current_url:
                logger.warning(f"🚫 Amazon 페이지가 아님: {current_url}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"페이지 차단 확인 중 오류: {e}")
            return False
    
    def wait_for_page_load(self, timeout=10):
        """페이지 로드 대기"""
        try:
            # JavaScript 실행 완료 대기
            self.wait.until(
                lambda driver: driver.execute_script("return document.readyState") == "complete"
            )
            
            # 추가로 Amazon 특정 요소 대기
            possible_elements = [
                (By.ID, "productTitle"),
                (By.ID, "priceblock_ourprice"),
                (By.CLASS_NAME, "a-price-whole"),
                (By.ID, "availability"),
                (By.ID, "imageBlock")
            ]
            
            for by, value in possible_elements:
                try:
                    WebDriverWait(self.driver, 3).until(
                        EC.presence_of_element_located((by, value))
                    )
                    logger.debug(f"✅ 요소 발견: {by}={value}")
                    return True
                except:
                    continue
            
            return True
            
        except Exception as e:
            logger.warning(f"페이지 로드 대기 중 오류: {e}")
            return False
    
    def extract_element_text(self, selectors, element_name="요소"):
        """선택자 목록에서 텍스트 추출"""
        log = logger.debug

        log(f"🔍 {element_name} 추출 시작 - 총 {len(selectors)}개 선택자")

        for idx, selector in enumerate(selectors, 1):
            try:
                log(f"  [{idx}/{len(selectors)}] 시도 중: {selector}")

                # XPath인지 CSS 선택자인지 구분
                if selector.startswith('//') or selector.startswith('('):
                    elements = self.driver.find_elements(By.XPATH, selector)
                    selector_type = "XPath"
                else:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    selector_type = "CSS"

                log(f"      타입: {selector_type}, 발견: {len(elements)}개")

                if elements:
                    for i, element in enumerate(elements):
                        try:
                            # 여러 방법으로 텍스트 추출 (is_displayed 여부와 무관하게 시도)
                            displayed = element.is_displayed()
                            text1 = element.text.strip() if displayed else ""
                            text2 = element.get_attribute('textContent').strip() if element.get_attribute('textContent') else ""
                            text3 = element.get_attribute('innerText').strip() if element.get_attribute('innerText') else ""

                            # 가장 긴 텍스트 선택
                            text = max([text1, text2, text3], key=len)
                            log(f"      displayed={displayed}, text='{text[:50]}'" if text else f"      displayed={displayed}, text=빈값")

                            if text:
                                log(f"      ✅ 추출 성공: '{text[:100]}'")
                                return text
                        except Exception as e:
                            log(f"      요소 처리 중 오류: {e}")

            except Exception as e:
                log(f"      ❌ 선택자 오류: {str(e)}")
                continue

        log(f"❌ {element_name} 추출 실패 - 모든 선택자 소진")
        return None
    
    def extract_price(self, country_code):
        """가격 추출 (추천 상품 영역 완전 제외)"""
        logger.info(f"💰 가격 추출 시작 - 국가: {country_code}")
        
        # 추천 상품 영역 제외 (이 영역들은 완전히 무시)
        excluded_areas = [
            "#similarities_feature_div",  # 추천 상품
            "#sp_detail",  # 스폰서 상품
            "#bookDescription_feature_div",  # 도서 설명
            "#books-entity-details",  # 도서 상세
            "#compare-table",  # 비교 테이블
            "[data-cel-widget*='comparison']",  # 비교 위젯
            "[data-cel-widget*='sponsored']",  # 스폰서
            ".s-result-item",  # 검색 결과 아이템
            ".a-carousel-card",  # 카루셀 카드
            ".acs-shwcs-widget",  # 추천 위젯
            "#customer_also_viewed_feature_div",  # 고객이 본 상품
            "#similarities-widget",  # 유사 상품
            ".a-accordion-inner"  # 아코디언 내부
        ]
        
        # 메인 제품 영역만 확인
        main_product_areas = [
            "#dp-container",  # 메인 제품 컨테이너
            "#centerCol",     # 중앙 컬럼
            "#rightCol",      # 우측 컬럼 (구매 박스)
            "#apex_desktop"   # 데스크톱 메인 영역
        ]
        
        price_selectors = self.selectors[country_code].get('price', [])
        
        for idx, selector in enumerate(price_selectors, 1):
            try:
                logger.debug(f"  [{idx}/{len(price_selectors)}] 가격 선택자 시도: {selector}")
                
                # 요소 찾기
                if selector.startswith('//'):
                    elements = self.driver.find_elements(By.XPATH, selector)
                    selector_type = "XPath"
                else:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    selector_type = "CSS"
                
                logger.debug(f"      타입: {selector_type}, 발견: {len(elements)}개")
                
                for i, element in enumerate(elements):
                    try:
                        if not element.is_displayed():
                            continue
                        
                        # 추천 상품 영역에 있는지 확인 (제외)
                        is_in_excluded_area = False
                        
                        for excluded_selector in excluded_areas:
                            try:
                                if excluded_selector.startswith('['):
                                    # 속성 선택자
                                    excluded_elements = self.driver.find_elements(By.CSS_SELECTOR, excluded_selector)
                                else:
                                    # ID나 클래스 선택자
                                    excluded_elements = self.driver.find_elements(By.CSS_SELECTOR, excluded_selector)
                                
                                for excluded_area in excluded_elements:
                                    if self.driver.execute_script(
                                        "return arguments[0].contains(arguments[1]);", 
                                        excluded_area, element
                                    ):
                                        is_in_excluded_area = True
                                        break
                                
                                if is_in_excluded_area:
                                    break
                            except:
                                continue
                        
                        if is_in_excluded_area:
                            continue
                        
                        # 메인 제품 영역에 있는지 확인 (포함)
                        is_in_main_area = False
                        
                        for main_selector in main_product_areas:
                            try:
                                main_area = self.driver.find_element(By.CSS_SELECTOR, main_selector)
                                if self.driver.execute_script(
                                    "return arguments[0].contains(arguments[1]);", 
                                    main_area, element
                                ):
                                    is_in_main_area = True
                                    break
                            except:
                                continue
                        
                        # 메인 영역에도 없고 가격 클래스도 아니면 제외
                        if not is_in_main_area:
                            element_class = element.get_attribute('class') or ''
                            if 'a-price' in element_class:
                                is_in_main_area = True
                            else:
                                continue
                        
                        # 텍스트 추출
                        text1 = element.text.strip()
                        text2 = element.get_attribute('textContent').strip() if element.get_attribute('textContent') else ""
                        text3 = element.get_attribute('innerText').strip() if element.get_attribute('innerText') else ""
                        
                        price_text = max([text1, text2, text3], key=len)
                        
                        if price_text:
                            # 국가별 가격 파싱
                            price = self.parse_price_by_country(price_text, country_code)
                            if price and price > 0:  # 0보다 큰 값만 반환
                                logger.info(f"✅ 가격 추출 성공: {price} (원본: {price_text})")
                                return price
                
                    except Exception as e:
                        logger.debug(f"요소 처리 오류: {e}")
                
            except Exception as e:
                logger.debug(f"선택자 오류: {str(e)}")
        
        # JavaScript로 메인 영역에서만 가격 찾기
        logger.debug("JavaScript로 메인 영역 가격 검색...")
        try:
            js_result = self.driver.execute_script(r"""
                // 메인 제품 영역만 검색
                const mainAreas = [
                    '#dp-container',
                    '#centerCol',
                    '#rightCol', 
                    '#apex_desktop'
                ];
                
                // 제외할 영역들
                const excludedAreas = [
                    '#similarities_feature_div',
                    '#sp_detail',
                    '.s-result-item',
                    '.a-carousel-card',
                    '.acs-shwcs-widget',
                    '#customer_also_viewed_feature_div',
                    '[data-cel-widget*="comparison"]',
                    '[data-cel-widget*="sponsored"]'
                ];
                
                const results = [];
                
                // 메인 영역에서 검색
                for (let mainSelector of mainAreas) {
                    const mainArea = document.querySelector(mainSelector);
                    if (mainArea) {
                        const priceElements = mainArea.querySelectorAll('span, div');
                        
                        for (let elem of priceElements) {
                            // 제외 영역에 있는지 확인
                            let isExcluded = false;
                            for (let excludedSelector of excludedAreas) {
                                const excludedArea = document.querySelector(excludedSelector);
                                if (excludedArea && excludedArea.contains(elem)) {
                                    isExcluded = true;
                                    break;
                                }
                            }
                            
                            if (isExcluded) continue;
                            
                            const text = elem.textContent.trim();
                            // 일본 엔화 및 기타 가격 패턴 매칭
                            if (text.match(/[¥￥][\d,]+/) || text.match(/[$£€₹][\d,]+\.?\d*/) || text.match(/\d+[.,]\d{2}$/)) {
                                if (text.length < 20 && text.length > 1) {
                                    results.push({
                                        text: text,
                                        tag: elem.tagName,
                                        class: elem.className,
                                        id: elem.id,
                                        area: mainSelector
                                    });
                                }
                            }
                        }
                    }
                }
                
                return results.slice(0, 3);  // 상위 3개만
            """)
            
            if js_result:
                for r in js_result:
                    price = self.parse_price_by_country(r['text'], country_code)
                    if price and price > 0:  # 0보다 큰 값만 반환
                        logger.info(f"✅ JavaScript 가격 추출 성공: {price}")
                        return price
                        
        except Exception as e:
            logger.debug(f"JavaScript 가격 추출 실패: {e}")
        
        logger.warning("❌ 메인 영역에서 가격을 찾을 수 없음")
        return None
    
    def parse_price_by_country(self, price_text, country_code):
        """국가별 가격 파싱"""
        try:
            # 기본 정리
            price_text = price_text.strip()
            
            if country_code in ['fr', 'it', 'es', 'de']:
                # 유럽: 1.234,56 형식
                price_text = re.sub(r'[€\s]', '', price_text)
                price_text = price_text.replace('.', '')
                price_text = price_text.replace(',', '.')
                match = re.search(r'(\d+\.?\d*)', price_text)
                if match:
                    result = float(match.group(1))
                    return result if result > 0 else None
                    
            elif country_code == 'jp':
                # 일본: ¥1,234 형식
                price_text = re.sub(r'[¥￥\s]', '', price_text)
                price_text = price_text.replace(',', '')
                match = re.search(r'(\d+)', price_text)
                if match:
                    result = float(match.group(1))
                    return result if result > 0 else None
                    
            elif country_code == 'in':
                # 인도: ₹1,234.56 형식
                price_text = re.sub(r'[₹\s]', '', price_text)
                price_text = price_text.replace(',', '')
                match = re.search(r'(\d+\.?\d*)', price_text)
                if match:
                    result = float(match.group(1))
                    return result if result > 0 else None
                    
            else:
                # 영미권: $1,234.56 형식
                price_text = re.sub(r'[$£\s]', '', price_text)
                price_text = price_text.replace(',', '')
                match = re.search(r'(\d+\.?\d*)', price_text)
                if match:
                    result = float(match.group(1))
                    return result if result > 0 else None
                    
        except Exception as e:
            logger.debug(f"가격 파싱 오류: {price_text} - {e}")
            
        return None
    
    def validate_seller_info(self, ships_from, sold_by):
        """ships_from과 sold_by 정보 검증 - 둘 다 비어있을 때만 False"""
        try:
            # 간단한 비어있음 체크
            ships_from_exists = ships_from and str(ships_from).strip()
            sold_by_exists = sold_by and str(sold_by).strip()
            
            logger.info(f"🔍 판매자 정보 확인: ships_from='{ships_from}', sold_by='{sold_by}'")
            logger.info(f"   ships_from 존재: {ships_from_exists}, sold_by 존재: {sold_by_exists}")
            
            # 둘 다 비어있으면 False
            if not ships_from_exists and not sold_by_exists:
                logger.warning("⚠️ ships_from과 sold_by 모두 비어있음 - 가격 0 설정")
                return False
            
            logger.info("✅ 판매자 정보 있음 - 정상 가격 추출 진행")
            return True
            
        except Exception as e:
            logger.error(f"판매자 정보 검증 오류: {e}")
            return True  # 오류시 정상 처리
    
    def extract_product_info(self, url, row_data, retry_count=0, max_retries=3):
        """제품 정보 추출 - 차단 페이지 처리 개선"""
        try:
            logger.info(f"\n{'='*60}")
            logger.info(f"🔍 제품 정보 추출 시작")
            logger.info(f"📌 URL: {url}")
            logger.info(f"📌 브랜드: {row_data.get('brand', 'N/A')}")
            logger.info(f"📌 제품: {row_data.get('item', 'N/A')}")
            
            # 페이지 로드
            self.driver.get(url)
            time.sleep(random.uniform(2, 4))

            # 쿠키 동의 팝업 자동 수락 (있으면 클릭)
            accept_cookies(self.driver, 'amazon_jp')

            # 차단 페이지 확인 및 처리
            if self.is_page_blocked():
                logger.warning("⚠️ 차단 페이지 감지됨")
                
                # Continue shopping 버튼 클릭하고 원래 URL로 재시도
                if self.handle_captcha_or_block_page(target_url=url):
                    logger.info("✅ 차단 페이지 처리 완료, 페이지 재로드됨")
                    time.sleep(3)
                    
                    # 여전히 차단 페이지인지 재확인
                    if self.is_page_blocked():
                        logger.error("❌ 여전히 차단 페이지임")
                        raise Exception("페이지 차단 지속")
                else:
                    logger.error("❌ 차단 페이지 처리 실패")
                    raise Exception("차단 페이지 처리 실패")
            
            # 페이지 로드 대기
            self.wait_for_page_load()
            
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
                'country_code': self.country_code,
                'ships_from': None,
                'channel_name': 'amazon.co.jp',
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
                self.selectors[self.country_code].get('title', []),
                "제목"
            )

            # Ships From / Sold By: DB selector가 combined-label predicate를 포함하므로 자동 처리
            result['sold_by'] = self.extract_element_text(
                self.selectors[self.country_code].get('sold_by', []),
                "판매자"
            )
            result['ships_from'] = self.extract_element_text(
                self.selectors[self.country_code].get('ships_from', []),
                "배송지"
            )
            logger.info(f"✅ 추출: ships_from={result['ships_from']}, sold_by={result['sold_by']}")

            # "Fulfilled by Amazon"이면 "Amazon"으로 변환
            if result['ships_from'] and 'Fulfilled by Amazon' in result['ships_from']:
                result['ships_from'] = 'Amazon'
            
            # 판매자 정보 검증 후 가격 결정
            seller_info_valid = self.validate_seller_info(result['ships_from'], result['sold_by'])
            
            if not seller_info_valid:
                # ships_from과 sold_by 둘 다 없으면 가격 0
                result['retailprice'] = None
                logger.info("💰 최종 가격: None (이유: 판매자 정보 없음)")
            else:
                # 하나라도 있으면 기존 가격 추출 로직 사용
                logger.info("💰 가격 추출 시작 (판매자 정보 있음)")
                result['retailprice'] = self.extract_price(self.country_code)
                
                if result['retailprice'] is not None:
                    logger.info(f"💰 최종 가격: {result['retailprice']} (정상 추출)")
                else:
                    logger.warning("💰 최종 가격: None (추출 실패)")
            
            # 이미지 URL 추출
            for selector in self.selectors[self.country_code].get('imageurl', []):
                try:
                    if selector.startswith('//'):
                        element = self.driver.find_element(By.XPATH, selector)
                    else:
                        element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    
                    result['imageurl'] = element.get_attribute('src')
                    if result['imageurl']:
                        logger.debug(f"✅ 이미지 URL 추출 성공")
                        break
                except:
                    continue
            
            # VAT 확인 (대소문자 구분 없이)
            page_source = self.driver.page_source
            page_source_lower = page_source.lower()
            
            # for vat_text in self.selectors[self.country_code].get('vat_text_list', []):
            #     if vat_text.lower() in page_source_lower:
            #         result['vat'] = 'o'
            #         logger.info(f"✅ VAT/Tax 포함 확인: {vat_text}")
            #         break
                    
            # 추가 VAT 패턴 확인
            # vat_patterns = [
            #     r'inclusive\s+of\s+all\s+tax',
            #     r'including\s+all\s+tax',
            #     r'price\s+includes?\s+(vat|tax|gst)',
            #     r'incl\.?\s*(vat|tax|gst)',
            #     r'tax\s+includ'
            # ]
            
            # for pattern in vat_patterns:
            #     if re.search(pattern, page_source_lower):
            #         result['vat'] = 'o'
            #         logger.info(f"✅ VAT/Tax 포함 확인 (패턴): {pattern}")
            #         break
            
            # 결과 요약
            logger.info(f"\n📊 추출 결과:")
            logger.info(f"   📌 제목: {result['title'][:50] + '...' if result['title'] and len(result['title']) > 50 else result['title']}")
            logger.info(f"   💰 가격: {result['retailprice']}")
            logger.info(f"   🖼️ 이미지: {'있음' if result['imageurl'] else '없음'}")
            logger.info(f"   🏪 판매자: {result['sold_by']}")
            logger.info(f"   📦 배송지: {result['ships_from']}")
            # logger.info(f"   💸 VAT: {result['vat']}")

            # NULL 필드 발견 시 스크린샷 + S3 업로드
            if is_null_result(result):
                capture_and_upload(self.driver, 'amazon_jp', row_data.get('retailersku', ''), url)

            return result

        except Exception as e:
            logger.error(f"❌ 페이지 처리 오류: {e}")
            
            if retry_count < max_retries:
                wait_time = (retry_count + 1) * 10
                logger.info(f"🔄 {wait_time}초 후 재시도... ({retry_count + 1}/{max_retries})")
                time.sleep(wait_time)
                
                # 드라이버 재시작
                try:
                    self.driver.refresh()
                except:
                    logger.info("🔧 드라이버 재시작 중...")
                    self.driver.quit()
                    self.setup_driver()
                
                return self.extract_product_info(url, row_data, retry_count + 1, max_retries)
            
            # 최종 실패 - 기본값 반환
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
                'country_code': self.country_code,
                'ships_from': None,
                'channel_name': 'amazon.co.jp',
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
                if is_null_result(fail_result):
                    capture_and_upload(self.driver, 'amazon_jp', row_data.get('retailersku', ''), url)
            except Exception:
                pass

            return fail_result

    def get_crawl_targets(self, limit=None):
        """DB에서 크롤링 대상 URL 목록 조회"""
        try:
            query = f"""
            SELECT *
            FROM samsung_price_tracking_list
            WHERE country = '{self.country_code}' 
              AND mall_name = 'amazon'
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
    
    def save_to_db(self, df):
        """DB에 결과 저장"""
        if self.db_engine is None:
            logger.warning("⚠️ DB 연결이 없어 DB 저장을 건너뜁니다")
            return False
        
        try:
            # 테이블명 설정
            table_name = f'amazon_price_crawl_tbl_{self.country_code}_v2'
            
            # 데이터 저장
            df.to_sql(table_name, self.db_engine, if_exists='append', index=False)
            logger.info(f"✅ DB 저장 완료: {len(df)}개 레코드 → {table_name}")
            
            # 크롤링 로그 저장
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
                log_df.to_sql('amazon_crawl_logs', self.db_engine, if_exists='append', index=False)
                logger.info(f"✅ 크롤링 로그 저장 완료: {len(log_records)}개")
            
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
        base_filename = f"{date_str}_{time_str}_jp_amazon"

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
    def scrape_urls(self, urls_data, max_items=None, save_interim=True):
        """여러 URL 스크래핑

        Args:
            urls_data: 크롤링 대상 URL 목록
            max_items: 최대 처리 개수 (None이면 전체)
            save_interim: 중간 저장 여부
        """
        if max_items:
            urls_data = urls_data[:max_items]
        
        logger.info(f"\n{'='*80}")
        logger.info(f"📊 크롤링 시작")
        logger.info(f"📌 국가: {self.country_code.upper()}")
        logger.info(f"📌 대상: {len(urls_data)}개 제품")
        logger.info(f"{'='*80}\n")
        
        if not self.setup_driver():
            logger.error("드라이버 설정 실패")
            return None
        
        results = []
        failed_urls = []
        
        try:
            for idx, row in enumerate(urls_data):
                logger.info(f"\n진행률: {idx + 1}/{len(urls_data)} ({(idx + 1)/len(urls_data)*100:.1f}%)")
                
                url = row.get('url')
                
                # 제품 정보 추출
                result = self.extract_product_info(url, row)
                
                # 실패 판단
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

                # 10개마다 중간 저장 (save_interim=True일 때만)
                if save_interim and (idx + 1) % 10 == 0:
                    interim_df = pd.DataFrame(results[-10:])
                    if self.db_engine:
                        try:
                            table_name = f'amazon_price_crawl_tbl_{self.country_code}_v2'
                            interim_df.to_sql(table_name, self.db_engine,
                                            if_exists='append', index=False)
                            logger.info(f"💾 중간 저장: 10개 레코드 DB 저장")
                        except Exception as e:
                            logger.error(f"중간 저장 실패: {e}")
                
                # 다음 요청 전 대기
                if idx < len(urls_data) - 1:
                    wait_time = random.uniform(1, 2)
                    logger.info(f"⏳ {wait_time:.1f}초 대기 중...")
                    time.sleep(wait_time)

                    # 20개마다 짧은 휴식
                    if (idx + 1) % 20 == 0:
                        logger.info("☕ 20개 처리 완료, 5초 휴식...")
                        time.sleep(5)
        
            # 남은 항목 저장 (10개 단위로 나누어 떨어지지 않는 경우)
            if save_interim and len(results) % 10 != 0:
                remaining_count = len(results) % 10
                remaining_df = pd.DataFrame(results[-remaining_count:])
                if self.db_engine:
                    try:
                        table_name = f'amazon_price_crawl_tbl_{self.country_code}_v2'
                        remaining_df.to_sql(table_name, self.db_engine,
                                          if_exists='append', index=False)
                        logger.info(f"💾 나머지 저장: {remaining_count}개 레코드 DB 저장")
                    except Exception as e:
                        logger.error(f"나머지 저장 실패: {e}")

        except Exception as e:
            logger.error(f"❌ 스크래핑 중 오류: {e}")

        finally:
            # 실패 URL 요약
            if failed_urls:
                logger.warning(f"\n⚠️ 문제 발생한 URL {len(failed_urls)}개:")
                for fail in failed_urls[:5]:
                    logger.warning(f"  - {fail['brand']} {fail['item']}: {fail.get('reason', '알 수 없음')}")
                if len(failed_urls) > 5:
                    logger.warning(f"  ... 외 {len(failed_urls) - 5}개")
            
            # 드라이버 종료
            if self.driver:
                self.driver.quit()
                logger.info("🔧 드라이버 종료")
        
        return pd.DataFrame(results)
    
    def analyze_results(self, df):
        """결과 분석"""
        logger.info(f"\n{'='*80}")
        logger.info("📊 결과 분석")
        logger.info(f"{'='*80}")
        
        total = len(df)
        with_price = df['retailprice'].notna().sum()
        without_price = df['retailprice'].isna().sum()
        zero_price = (df['retailprice'] == 0).sum()
        success_rate = (with_price / total * 100) if total > 0 else 0
        
        logger.info(f"전체 제품: {total}개")
        logger.info(f"가격 추출 성공: {with_price}개")
        logger.info(f"가격 추출 실패: {without_price}개")
        logger.info(f"가격 0 (판매자 정보 없음): {zero_price}개")
        logger.info(f"성공률: {success_rate:.1f}%")
        
        if with_price > 0:
            price_df = df[df['retailprice'].notna()].copy()
            
            # 0이 아닌 가격만으로 통계 계산
            non_zero_price_df = price_df[price_df['retailprice'] > 0]
            
            if not non_zero_price_df.empty:
                logger.info(f"\n💰 가격 통계 (0 제외):")
                logger.info(f"   평균가: {non_zero_price_df['retailprice'].mean():.2f}")
                logger.info(f"   최저가: {non_zero_price_df['retailprice'].min():.2f}")
                logger.info(f"   최고가: {non_zero_price_df['retailprice'].max():.2f}")
                logger.info(f"   중간값: {non_zero_price_df['retailprice'].median():.2f}")
            
            # 브랜드별 성공률
            brand_stats = df.groupby('brand').agg({
                'retailprice': ['count', lambda x: x.notna().sum()]
            })
            brand_stats.columns = ['total', 'success']
            brand_stats['success_rate'] = (brand_stats['success'] / brand_stats['total'] * 100).round(1)
            
            logger.info(f"\n📊 브랜드별 성공률:")
            for brand, row in brand_stats.iterrows():
                logger.info(f"   {brand}: {row['success_rate']:.1f}% ({row['success']}/{row['total']})")

def main():
    """메인 실행 함수"""
    from log_utils import setup_log, save_log
    setup_log('jp_amazon')

    # 환경변수에서 국가 코드 읽기
    country_code = os.getenv('COUNTRY_CODE', 'jp').lower()  # 기본값을 jp로 변경
    test_mode = os.getenv('TEST_MODE', 'false').lower() == 'true'
    max_items = int(os.getenv('MAX_ITEMS', '0')) or None
    
    print(f"\n{'='*80}")
    print(f"🚀 Amazon 일본 가격 추출 시스템 v5.1 (차단 페이지 처리 개선)")
    print(f"{'='*80}")
    print(f"📌 국가: {country_code.upper()}")
    print(f"📌 모드: {'테스트' if test_mode else '실제'}")
    if max_items:
        print(f"📌 최대 처리 수: {max_items}개")
    print(f"📌 개선사항: 일본 아마존 'ショッピングを続ける' 버튼 자동 클릭 및 원래 URL 재시도")
    print(f"{'='*80}\n")
    
    # 스크래퍼 초기화
    scraper = AmazonScraper(country_code)

    if scraper.db_engine is None:
        logger.error("DB 연결 실패로 종료합니다.")
        monitor_and_alert(country_code, 0, None, error_message="DB 연결 실패")
        return
    
    # 테스트 모드
    if test_mode:
        logger.info("🧪 테스트 모드 실행 중...")
        test_data = [{
            'url': 'https://www.amazon.co.jp/dp/B0CTRXBKHP',
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
            
            # 테스트 모드에서는 파일만 저장 (DB 저장 안함)
            scraper.save_results(results_df, save_db=False, upload_server=True)
        return
    
    # 실제 크롤링
    logger.info("📊 전체 크롤링 시작")
    urls_data = scraper.get_crawl_targets(limit=max_items)
    
    if not urls_data:
        logger.warning("크롤링 대상이 없습니다.")
        monitor_and_alert(country_code, 0, None, error_message="크롤링 대상 없음")
        return
    
    logger.info(f"✅ 크롤링 대상: {len(urls_data)}개")
    
    # 크롤링 실행
    results_df = scraper.scrape_urls(urls_data, max_items)
    
    if results_df is None or results_df.empty:
        logger.error("크롤링 결과가 없습니다.")
        monitor_and_alert(country_code, len(urls_data), None, error_message="크롤링 결과 없음")
        return
    
    # 결과 분석
    scraper.analyze_results(results_df)
    
    # 결과 저장 (DB는 중간 저장으로 이미 완료됨)
    save_results = scraper.save_results(
        results_df,
        save_db=False,  # 중간 저장으로 이미 DB에 저장됨
        upload_server=False  # auto_recovery에서 처리
    )

    logger.info("일본 크롤링 완료!")

    # 자동 복구 + 파일 업로드 + 메일 알림
    from auto_recovery import auto_recovery_run
    auto_recovery_run(
        target_key='jp',
        results_df=results_df,
        target_count=len(urls_data),
        error_logs=None
    )

    save_log('jp_amazon')

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
    
    print("\n📦 필요한 패키지:")
    print("pip install " + " ".join(required_packages))
    print("\n⚠️ 환경변수 설정:")
    print("export COUNTRY_CODE=jp  # usa, gb, de, fr, jp, in 등")
    print("export TEST_MODE=false  # 테스트 모드")
    print("export MAX_ITEMS=10     # 최대 처리 개수 (선택사항)")
    print("\n🎯 일본 아마존 전용 개선사항:")
    print("- ショッピングを続ける 버튼 자동 감지 및 클릭")
    print("- 차단 페이지 처리 후 원래 URL로 자동 재시도")
    print("- 일본어 차단 페이지 패턴 추가 감지")
    print("- 다양한 클릭 방법 시도 (일반 클릭, JavaScript, ActionChains)")
    print()
    
    main()
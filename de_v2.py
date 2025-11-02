# -*- coding: utf-8 -*-
"""
Amazon 독일 가격 추출 시스템 V2 (타임존 분리 버전)
원본 de.py 기반 - DB/타임존/파일서버 설정만 V2로 변경
- 현지시간(독일)과 한국시간 분리 저장
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

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Import database configuration V2
from config import DB_CONFIG_V2 as DB_CONFIG

from config import FILE_SERVER_CONFIG

class AmazonDEScraper:
    def __init__(self):
        self.driver = None
        self.db_engine = None
        self.country_code = 'de'
        self.wait = None
        # V2: 타임존 분리 (현지시간 + 한국시간)
        self.korea_tz = pytz.timezone('Asia/Seoul')
        self.local_tz = pytz.timezone('Europe/Berlin')  # 독일 현지 시간
        
        self.setup_db_connection()
        self.setup_de_selectors()
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
    
    def setup_de_selectors(self):
        """독일 전용 선택자 설정 - 메인 상품 영역만 타겟팅"""
        self.selectors = {
            'price': [
                # 메인 가격 영역만 타겟팅 (우선순위 높음)
                "//*[@id='corePriceDisplay_desktop_feature_div']/div[1]/span[1]",
                "//*[@id='corePrice_feature_div']/div/div/div/div/span[1]/span[1]",
                "//*[@id='corePrice_feature_div']/div/div/span[1]/span[1]",
                
                # 메인 가격 영역 (더 구체적한 순서로) - centerCol 내부만
                "//*[@id='centerCol']//*[@id='corePrice_feature_div']//span[@class='a-offscreen']",
                "//*[@id='centerCol']//*[@id='corePriceDisplay_desktop_feature_div']//span[@class='a-offscreen']",
                "//*[@id='centerCol']//*[@id='apex_desktop']//span[@class='a-price']//span[@class='a-offscreen']",
                
                # 기존 독일 Amazon 특화 가격 선택자 (centerCol 제한)
                "//*[@id='centerCol']//span[@class='a-offscreen']",
                
                # 첫 번째 가격만 (centerCol 내부만, 개선된 선택자)
                "(//*[@id='centerCol']//span[@class='a-price']//span[@class='a-offscreen'])[1]",
                "(//*[@id='centerCol']//span[@class='a-price-whole'])[1]",
                
                # 기본 가격 요소들 (centerCol 내부만)
                "//*[@id='centerCol']//*[@id='priceblock_ourprice']",
                "//*[@id='centerCol']//*[@id='priceblock_dealprice']",
                "//*[@id='centerCol']//*[@id='listPrice']",
                
                # whole 가격 (centerCol 내부만)
                "//*[@id='centerCol']//*[@id='corePrice_feature_div']//span[@class='a-price-whole']",
                "//*[@id='centerCol']//*[@id='corePriceDisplay_desktop_feature_div']//span[@class='a-price-whole']",
                "//*[@id='centerCol']//*[@id='apex_desktop']//span[@class='a-price-whole']",
                
                # 백업용 일반 선택자 (최후 수단)
                "//div[@id='centerCol']//span[@class='a-price']//span[@class='a-offscreen']",
                "//div[@id='centerCol']//span[@class='a-price-whole']",
            ],
            'price_used': [
                # 중고 제품 가격 (신품이 없을 때만 사용) - centerCol 내부만
                "//*[@id='centerCol']//*[@id='usedBuySection']/div[1]/div/span[2]",
                "//*[@id='centerCol']//*[@id='usedBuySection']//span[@class='a-offscreen']",
                "//div[@id='centerCol']//div[@id='usedBuySection']//span[@class='a-price']//span[@class='a-offscreen']"
            ],
            'price_fraction': [
                "//*[@id='centerCol']//*[@id='corePrice_feature_div']//span[@class='a-price-fraction']",
                "//*[@id='centerCol']//*[@id='corePriceDisplay_desktop_feature_div']//span[@class='a-price-fraction']",
                "//*[@id='centerCol']//*[@id='apex_desktop']//span[@class='a-price-fraction']",
                "//div[@id='centerCol']//span[@class='a-price-fraction']"
            ],
            'title': [
                "#productTitle",
                "//span[@id='productTitle']",
                "//h1/span[@id='productTitle']"
            ],
            'ships_from': [
                "//*[@id='SSOFpopoverLink_ubb']",
                "/html/body/div[2]/div/div/div[4]/div[1]/div[3]/div/div[1]/div/div/div/form/div/div[1]/div/div/div/div[2]/div[2]/div[3]/a",
                "//a[@id='SSOFpopoverLink_ubb']",
                "//*[@id='fulfillerInfoFeature_feature_div']/div[2]/div[1]/span",
                "//div[@id='fulfillerInfoFeature_feature_div']//span"
            ],
            'sold_by': [
                "//a[@id='sellerProfileTriggerId']",
                "//*[@id='sellerProfileTriggerId']",
                "//*[@id='merchantInfoFeature_feature_div']/div[2]/div[1]/span",
                "//div[@id='merchantInfoFeature_feature_div']//a",
                "//div[@id='merchantInfoFeature_feature_div']//span"
            ],
            'imageurl': [
                "//div[@id='imageBlock']//img[@id='landingImage']",
                "//div[@id='main-image-container']//img",
                "//img[@class='a-dynamic-image']"
            ],
            'availability': [
                "//div[@id='availability']//span",
                "//div[@id='availability_feature_div']//span"
            ],
            'excluded_price_areas': [
                'product-comparison',
                'comparison-desktop',
                'non-deal-price',
                'strikethrough',
                'list-price',
                'rrp-price',
                'was-price',
                'capacity-selection',
                'recommendations',      # 추천상품 섹션
                'sponsored',            # 스폰서 상품
                'similarities',         # 유사상품
                'related-products',     # 관련 상품
                'also-bought',          # 함께 구매한 상품
                'customers-also',       # 고객이 함께 본 상품
                'also-viewed',          # 함께 본 상품
                'compare-with',         # 비교 상품
                'frequently-bought',    # 자주 함께 구매
                'product-ads',          # 상품 광고
                'sponsored-products',   # 스폰서 상품들
                'aplus-module',         # A+ 컨텐츠 모듈
                'dp-ads',               # 디스플레이 광고
                'cart-desktop',         # 장바구니 영역
                'desktop-dp-sims',      # 유사 상품 데스크톱
                'bundleV2',             # 번들 상품
                'purchase-info',        # 구매 정보 (다른 판매자)
                # usedBuySection은 별도 처리하므로 여기서 제외
            ],
            # 추가: 제외할 xpath 패턴들 (정규식)
            'excluded_xpath_patterns': [
                r'/html/body/div\[2\]/div/div/div\[4\]/div\[1\]/div\[[4-9]\]/',  # div[4] 이후 영역들
                r'/html/body/div\[2\]/div/div/div\[4\]/div\[1\]/div\[1[0-9]\]/', # div[10] 이후
                r'.*span\[3\]/span\[1\]$',  # span[3]/span[1]로 끝나는 패턴 (문제 xpath 패턴)
                r'.*dp-sims.*',             # 유사 상품 영역
                r'.*recommendations.*',      # 추천 영역
                r'.*also-bought.*',         # 함께 구매 영역
                r'.*also-viewed.*',         # 함께 본 영역
                r'.*sponsored.*',           # 스폰서 영역
                r'.*ads.*',                 # 광고 영역
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
            WHERE country_code = %s 
              AND is_active = TRUE
            ORDER BY element_type, priority ASC
            """
            
            df = pd.read_sql(query, self.db_engine, params=(self.country_code,))
            logger.info(f"DB에서 선택자 로드: {len(df)}개")
            
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
            
            # 독일 전용 User-Agent
            user_agents = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            ]
            options.add_argument(f'--user-agent={random.choice(user_agents)}')
            
            # 독일어 언어 설정
            options.add_experimental_option('prefs', {'intl.accept_languages': 'de-DE,de'})
            
            self.driver = uc.Chrome(options=options)
            self.driver.maximize_window()
            self.wait = WebDriverWait(self.driver, 20)
            
            logger.info("드라이버 설정 완료")
            return True
        except Exception as e:
            logger.error(f"드라이버 설정 실패: {e}")
            return False
    
    def handle_captcha_or_block_page(self, original_url=None):
        """차단 페이지나 캡차 처리"""
        try:
            logger.info("독일 차단/캡차 페이지 확인 중...")
            
            # 먼저 정상 페이지인지 확인 (불필요한 처리 방지)
            try:
                normal_check = self.driver.find_element(By.XPATH, "//span[@id='productTitle']")
                if normal_check and normal_check.is_displayed():
                    logger.info("정상 제품 페이지 확인됨 - 처리 불필요")
                    return True
            except:
                pass
            
            # 독일 503 오류 페이지 감지 (더 구체적으로)
            page_title = self.driver.title.lower()
            page_source = self.driver.page_source.lower()
            
            # 503 오류 페이지의 명확한 특징만 확인
            is_503_page = (
                page_title == '503 - service nicht verfügbar' or
                (('tut uns leid' in page_source and 
                  'fehler beim verarbeiten ihrer anforderung' in page_source) and
                 'klicken sie hier' in page_source)
            )
            
            if is_503_page:
                logger.info("명확한 독일 503 오류 페이지 감지됨")
                
                # 파란색 링크 선택자들 (이미지에서 보이는 링크)
                amazon_home_link_selectors = [
                    "//a[contains(text(), 'Klicken Sie hier, um zur Amazon-Startseite zurückzukehren')]",
                    "//a[contains(@href, 'amazon.de') and contains(text(), 'Amazon')]",
                    "//a[contains(text(), 'zur Amazon-Startseite')]",
                    "//a[contains(text(), 'zurückzukehren')]",
                    "//a[contains(@href, '/ref=')]"
                ]
                
                for selector in amazon_home_link_selectors:
                    try:
                        link = self.driver.find_element(By.XPATH, selector)
                        if link and link.is_displayed():
                            link_text = link.text
                            logger.info(f"Amazon 홈페이지 링크 발견: {link_text}")
                            
                            try:
                                link.click()
                                logger.info("파란색 링크 클릭 성공")
                            except:
                                self.driver.execute_script("arguments[0].click();", link)
                                logger.info("JavaScript로 링크 클릭 성공")
                            
                            # 홈페이지 로딩 대기
                            time.sleep(3)
                            
                            # 원래 URL로 다시 접속
                            if original_url:
                                logger.info(f"원래 URL로 재접속: {original_url}")
                                self.driver.get(original_url)
                                time.sleep(2)
                            
                            return True
                    except Exception as e:
                        continue
            
            # 일반적인 차단 페이지 처리 (더 구체적으로)
            if ('weiter shoppen' in page_source and 'amazon-startseite' in page_source):
                continue_button_texts = ['Weiter shoppen', 'Weiter einkaufen', 'Fortfahren']
                
                all_selectors = []
                for text in continue_button_texts:
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
                            button_text = button.text
                            logger.info(f"Continue 버튼 발견: {button_text}")
                            
                            try:
                                button.click()
                                logger.info("버튼 클릭 성공")
                            except:
                                self.driver.execute_script("arguments[0].click();", button)
                                logger.info("JavaScript 클릭 성공")
                            
                            time.sleep(3)
                            return True
                    except Exception as e:
                        continue
            
            return False
        except Exception as e:
            logger.error(f"차단 페이지 처리 중 오류: {e}")
            return False
    
    def is_excluded_price_element(self, element):
        """가격 요소가 제외 대상인지 확인 (추천상품/관련상품 영역 강화)"""
        try:
            # 1. 요소의 HTML 확인
            element_html = self.driver.execute_script("return arguments[0].outerHTML;", element)
            excluded_areas = self.selectors.get('excluded_price_areas', [])
            
            for pattern in excluded_areas:
                if pattern in element_html.lower():
                    logger.debug(f"제외 영역 감지: {pattern}")
                    return True
            
            # 2. centerCol 외부의 요소는 제외
            try:
                center_col = self.driver.execute_script(r"""
                    return arguments[0].closest('#centerCol');
                """, element)
                if not center_col:
                    logger.info("centerCol 외부 요소 - 제외")
                    return True
            except Exception as e:
                logger.debug(f"centerCol 확인 오류: {e}")
            
            # 3. XPath로 요소 위치 확인
            try:
                element_xpath = self.driver.execute_script(r"""
                    function getXPath(element) {
                        var xpath = '';
                        for (; element && element.nodeType == 1; element = element.parentNode) {
                            var id = element.id;
                            if (id) {
                                xpath = '//' + element.tagName.toLowerCase() + '[@id="' + id + '"]' + xpath;
                                break;
                            } else {
                                var sameTag = [];
                                for (var i = 0; i < element.parentNode.childNodes.length; i++) {
                                    var child = element.parentNode.childNodes[i];
                                    if (child.nodeType == 1 && child.tagName == element.tagName) {
                                        sameTag.push(child);
                                    }
                                }
                                xpath = '/' + element.tagName.toLowerCase() + '[' + (sameTag.indexOf(element) + 1) + ']' + xpath;
                            }
                        }
                        return xpath;
                    }
                    return getXPath(arguments[0]);
                """, element)
                
                logger.debug(f"요소 xpath: {element_xpath}")
                
                # 제외할 xpath 패턴 확인
                excluded_patterns = self.selectors.get('excluded_xpath_patterns', [])
                for pattern in excluded_patterns:
                    if re.search(pattern, element_xpath):
                        logger.info(f"제외 xpath 패턴 매치: {pattern}")
                        return True
                
                # div[4] 이후 영역들은 추천상품으로 간주하여 제외
                if ('div[1]/div[4]' in element_xpath or 
                    'div[1]/div[5]' in element_xpath or 
                    'div[1]/div[6]' in element_xpath or
                    'div[1]/div[7]' in element_xpath or
                    'div[1]/div[8]' in element_xpath or
                    'div[1]/div[9]' in element_xpath or
                    re.search(r'div\[1\]/div\[1[0-9]\]', element_xpath)):
                    logger.info(f"div[4] 이후 영역 제외: {element_xpath}")
                    return True
                
                # 문제 xpath 패턴 직접 차단
                if 'span[3]/span[1]' in element_xpath:
                    logger.info(f"문제 xpath 패턴 차단: {element_xpath}")
                    return True
                    
            except Exception as e:
                logger.debug(f"XPath 확인 오류: {e}")
            
            # 4. 부모 요소들 확인
            parent_checks = [
                "arguments[0].closest('.product-comparison-desktop')",
                "arguments[0].closest('[class*=\"comparison\"]')",
                "arguments[0].closest('[class*=\"non-deal\"]')",
                "arguments[0].closest('[class*=\"recommendations\"]')",
                "arguments[0].closest('[class*=\"sponsored\"]')",
                "arguments[0].closest('[class*=\"similarities\"]')",
                "arguments[0].closest('[class*=\"also-bought\"]')",
                "arguments[0].closest('[class*=\"also-viewed\"]')",
                "arguments[0].closest('[class*=\"related\"]')",
                "arguments[0].closest('[class*=\"frequently\"]')",
                "arguments[0].closest('[class*=\"customers-also\"]')",
                "arguments[0].closest('[id*=\"sims\"]')",
                "arguments[0].closest('[id*=\"ads\"]')",
                "arguments[0].closest('[data-component-type*=\"sims\"]')",
                "arguments[0].closest('[data-feature-name*=\"sims\"]')",
                # usedBuySection은 별도 처리하므로 여기서 제외
            ]
            
            for check in parent_checks:
                try:
                    parent = self.driver.execute_script(f"return {check}", element)
                    if parent:
                        logger.info(f"제외 상위 요소 발견")
                        return True
                except:
                    continue
            
            # 5. 특정 ID나 클래스를 가진 상위 요소 확인
            try:
                # 추천/관련 상품 영역의 상위 컨테이너 확인
                excluded_containers = self.driver.execute_script(r"""
                    var element = arguments[0];
                    var excludedIds = ['dp-sims-desktop', 'similarities-desktop', 'desktop-dp-sims', 
                                      'recommendations', 'also-bought', 'also-viewed', 'bundleV2'];
                    var excludedClasses = ['sims-', 'recommendations', 'also-', 'sponsored', 'ads-'];
                    
                    while (element && element !== document.body) {
                        if (element.id) {
                            for (var i = 0; i < excludedIds.length; i++) {
                                if (element.id.indexOf(excludedIds[i]) !== -1) {
                                    return true;
                                }
                            }
                        }
                        if (element.className && typeof element.className === 'string') {
                            for (var i = 0; i < excludedClasses.length; i++) {
                                if (element.className.indexOf(excludedClasses[i]) !== -1) {
                                    return true;
                                }
                            }
                        }
                        element = element.parentElement;
                    }
                    return false;
                """, element)
                
                if excluded_containers:
                    logger.info("제외 컨테이너 내부 요소 감지")
                    return True
            except Exception as e:
                logger.debug(f"컨테이너 확인 오류: {e}")
            
            return False
        except Exception as e:
            logger.debug(f"제외 요소 확인 오류: {e}")
            return False
    
    def is_page_blocked(self):
        """페이지 차단 감지 (정상 페이지는 우선 확인)"""
        try:
            page_title = self.driver.title.lower()
            page_source = self.driver.page_source.lower()
            current_url = self.driver.current_url.lower()
            
            # 기본 도메인 확인
            if 'amazon' not in current_url:
                logger.info("Amazon 도메인이 아닌 페이지")
                return True
            
            # 먼저 정상 제품 페이지인지 확인 (우선순위)
            try:
                normal_page_indicators = [
                    "//span[@id='productTitle']",
                    "//div[@id='feature-bullets']", 
                    "//div[@id='centerCol']",
                    "//div[@id='dp-container']",
                    "//div[@id='apex_desktop']"
                ]
                
                for selector in normal_page_indicators:
                    try:
                        element = self.driver.find_element(By.XPATH, selector)
                        if element and element.is_displayed():
                            logger.debug("정상 제품 페이지 요소 발견 - 정상 페이지로 판단")
                            return False  # 정상 페이지
                    except:
                        continue
                        
            except Exception as e:
                logger.debug(f"정상 페이지 확인 중 오류: {e}")
            
            # 정상 페이지 요소가 없을 때만 차단 페이지 확인
            # 503 오류 페이지의 명확한 특징만 확인
            if (page_title == '503 - service nicht verfügbar' or
                'tut uns leid' in page_source and 'fehler beim verarbeiten ihrer anforderung' in page_source):
                logger.info("명확한 503 오류 페이지 감지")
                return True
            
            # 일반 차단 페이지 감지 (더 구체적으로)
            if (page_title == 'access denied' or 
                'weiter shoppen' in page_source and 'amazon-startseite' in page_source):
                logger.info("일반 차단 페이지 감지")
                return True
            
            return False  # 기본적으로 정상으로 판단
            
        except Exception as e:
            logger.error(f"페이지 차단 확인 중 오류: {e}")
            return False  # 오류 시 정상으로 판단
    
    def wait_for_page_load(self, timeout=10):
        """페이지 로드 대기"""
        try:
            self.wait.until(lambda driver: driver.execute_script("return document.readyState") == "complete")
            return True
        except Exception as e:
            logger.warning(f"페이지 로드 대기 중 오류: {e}")
            return False
    
    def extract_clean_text_from_element(self, element, element_name="요소"):
        """요소에서 깨끗한 텍스트 추출"""
        try:
            text_methods = [
                element.get_attribute('textContent'),
                element.get_attribute('innerText'),
                element.text
            ]
            
            for text in text_methods:
                if text and text.strip():
                    clean_text = text.strip()
                    
                    if element_name in ["Ships From", "Sold By"]:
                        clean_text = self.clean_ships_sold_text(clean_text, element_name)
                    
                    if clean_text:
                        return clean_text
            
            return None
        except Exception as e:
            logger.debug(f"텍스트 추출 오류: {e}")
            return None
    
    def clean_ships_sold_text(self, text, element_type):
        """Ships From / Sold By 텍스트 정리"""
        try:
            label_patterns = {
                'Ships From': ['ships from', 'fulfilled by', 'versand durch'],
                'Sold By': ['sold by', 'verkauft von', 'vendu par']
            }
            
            text_lower = text.lower().strip()
            patterns = label_patterns.get(element_type, [])
            
            if text_lower in patterns:
                return None
            
            for pattern in patterns:
                if text_lower.startswith(pattern + ' '):
                    actual_value = text[len(pattern):].strip()
                    if actual_value:
                        return actual_value
                
                if pattern in text_lower and 'amazon' in text_lower:
                    return 'Amazon'
            
            return text
        except Exception as e:
            logger.debug(f"텍스트 정리 오류: {e}")
            return text
    
    def extract_element_text(self, selectors, element_name="요소"):
        """선택자 목록에서 텍스트 추출"""
        logger.info(f"{element_name} 추출 시작 - 총 {len(selectors)}개 선택자")
        
        for idx, selector in enumerate(selectors, 1):
            try:
                logger.info(f"[{idx}/{len(selectors)}] 시도 중: {selector}")
                
                if selector.startswith('//') or selector.startswith('('):
                    elements = self.driver.find_elements(By.XPATH, selector)
                else:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                
                logger.info(f"발견된 요소: {len(elements)}개")
                
                if elements:
                    for element in elements:
                        try:
                            if element.is_displayed():
                                if element_name == "가격" and self.is_excluded_price_element(element):
                                    continue
                                
                                text = self.extract_clean_text_from_element(element, element_name)
                                if text:
                                    logger.info(f"최종 추출 성공: '{text}'")
                                    return text
                        except Exception as e:
                            logger.debug(f"요소 처리 중 오류: {e}")
            except Exception as e:
                logger.debug(f"선택자 오류: {e}")
                continue
        
        logger.error(f"{element_name} 추출 완전 실패")
        return None
    
    def parse_german_price(self, price_text):
        """독일 가격 파싱 (개선된 버전)"""
        try:
            price_text = price_text.strip()
            logger.debug(f"파싱할 가격 텍스트: '{price_text}'")
            
            # 무효한 패턴 확인
            invalid_patterns = [
                r'^[a-zA-Z\s]+$',
                r'^\d+\s*[a-zA-Z]',
                r'was\s*€',
                r'list\s*price',
                r'buy\s*used'
            ]
            
            for pattern in invalid_patterns:
                if re.search(pattern, price_text, re.IGNORECASE):
                    logger.debug(f"무효한 가격 패턴 감지: {pattern}")
                    return None
            
            # 유로 기호와 공백 제거
            cleaned = re.sub(r'[€\s]', '', price_text)
            logger.debug(f"통화 제거 후: '{cleaned}'")
            
            # 빈 문자열 체크
            if not cleaned:
                return None
            
            # 4자리 숫자인 경우 센트로 간주 (예: 1299 → 12.99)
            if re.match(r'^\d{4}$', cleaned) and len(cleaned) == 4:
                try:
                    cents = int(cleaned)
                    euros = cents / 100
                    if 10 <= euros <= 10000:
                        formatted_price = f"{euros:.2f}"
                        logger.debug(f"독일 센트→유로 변환: {cleaned} → {formatted_price}")
                        return formatted_price
                except:
                    pass
            
            # 독일식 숫자 형식 처리 (예: 1.299,99 → 1299.99)
            if ',' in cleaned and '.' not in cleaned:
                # 단순히 쉼표를 소수점으로 변경
                cleaned = cleaned.replace(',', '.')
            elif ',' in cleaned and '.' in cleaned:
                # 천단위 구분자(.)와 소수점 구분자(,) 구분
                parts = cleaned.rsplit(',', 1)
                if len(parts) == 2 and len(parts[1]) <= 2:
                    integer_part = parts[0].replace('.', '')
                    decimal_part = parts[1]
                    cleaned = f"{integer_part}.{decimal_part}"
            
            # 최종 검증
            if re.match(r'^\d+(?:\.\d{1,2})?$', cleaned):
                try:
                    price_value = float(cleaned)
                    if 1 <= price_value <= 50000:  # 가격 범위 확대
                        return cleaned
                except:
                    pass
            
        except Exception as e:
            logger.debug(f"가격 파싱 오류: {e}")
        
        return None
    
    def extract_price(self):
        """가격 추출 (개선된 버전) - 메인 상품 영역만 타겟팅"""
        logger.info("독일 가격 추출 시작 (메인 상품 영역만)")
        
        # 1단계: 메인 신품 가격 추출 (centerCol 내부만)
        price_selectors = self.selectors.get('price', [])
        
        for idx, selector in enumerate(price_selectors, 1):
            try:
                logger.info(f"[신품 {idx}/{len(price_selectors)}] 시도: {selector}")
                
                if selector.startswith('//') or selector.startswith('('):
                    elements = self.driver.find_elements(By.XPATH, selector)
                else:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                
                for element in elements:
                    try:
                        if element.is_displayed():
                            if self.is_excluded_price_element(element):
                                continue
                            
                            text = self.extract_clean_text_from_element(element, "가격")
                            if text:
                                price = self.parse_german_price(text)
                                if price:
                                    logger.info(f"신품 가격 추출 성공: {price}")
                                    return price
                    except Exception as e:
                        logger.debug(f"요소 처리 오류: {e}")
                        continue
            except Exception as e:
                logger.debug(f"선택자 오류: {e}")
                continue
        
        # 2단계: whole + fraction 조합 시도 (centerCol 내부만)
        logger.info("독일 whole + fraction 조합 시도 (centerCol 내부만)")
        whole_fraction_selectors = [
            ("//*[@id='centerCol']//*[@id='corePrice_feature_div']//span[@class='a-price-whole']", 
             "//*[@id='centerCol']//*[@id='corePrice_feature_div']//span[@class='a-price-fraction']"),
            ("//*[@id='centerCol']//*[@id='corePriceDisplay_desktop_feature_div']//span[@class='a-price-whole']", 
             "//*[@id='centerCol']//*[@id='corePriceDisplay_desktop_feature_div']//span[@class='a-price-fraction']"),
            ("//div[@id='centerCol']//span[@class='a-price-whole']", 
             "//div[@id='centerCol']//span[@class='a-price-fraction']")
        ]
        
        for whole_xpath, fraction_xpath in whole_fraction_selectors:
            try:
                whole_elem = self.driver.find_element(By.XPATH, whole_xpath)
                fraction_elem = self.driver.find_element(By.XPATH, fraction_xpath)
                
                if (whole_elem and fraction_elem and 
                    whole_elem.is_displayed() and fraction_elem.is_displayed()):
                    
                    # 제외 요소 확인
                    if (self.is_excluded_price_element(whole_elem) or 
                        self.is_excluded_price_element(fraction_elem)):
                        continue
                    
                    whole_text = whole_elem.text.strip()
                    fraction_text = fraction_elem.text.strip()
                    
                    if whole_text and fraction_text:
                        # 숫자만 추출
                        whole_clean = re.sub(r'[^\d]', '', whole_text)
                        fraction_clean = re.sub(r'[^\d]', '', fraction_text)
                        
                        if whole_clean and fraction_clean:
                            combined_price = f"{whole_clean}.{fraction_clean}"
                            price = self.parse_german_price(combined_price)
                            if price:
                                logger.info(f"조합 가격 추출 성공: {price}")
                                return price
            except Exception as e:
                logger.debug(f"조합 오류: {e}")
                continue
        
        # 3단계: 중고 가격 시도 (신품이 없을 때만, centerCol 내부만)
        logger.info("중고 가격 시도 (centerCol 내부만)")
        used_price_selectors = self.selectors.get('price_used', [])
        
        for idx, selector in enumerate(used_price_selectors, 1):
            try:
                logger.info(f"[중고 {idx}/{len(used_price_selectors)}] 시도: {selector}")
                
                if selector.startswith('//') or selector.startswith('('):
                    elements = self.driver.find_elements(By.XPATH, selector)
                else:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                
                for element in elements:
                    try:
                        if element.is_displayed():
                            text = self.extract_clean_text_from_element(element, "중고가격")
                            if text:
                                price = self.parse_german_price(text)
                                if price:
                                    logger.info(f"중고 가격 추출 성공: {price}")
                                    return price
                    except Exception as e:
                        logger.debug(f"중고 요소 처리 오류: {e}")
                        continue
            except Exception as e:
                logger.debug(f"중고 선택자 오류: {e}")
                continue
        
        logger.error("모든 방법으로 가격 추출 실패 (메인 상품 영역만 검색)")
        return None
    
    def check_stock_availability(self):
        """재고 상태 확인"""
        try:
            try:
                availability_elem = self.driver.find_element(By.ID, "availability")
                availability_text = availability_elem.text.lower()
                
                if any(phrase in availability_text for phrase in [
                    'nicht verfügbar', 'ausverkauft', 'temporarily out of stock',
                    'currently unavailable', 'out of stock'
                ]):
                    logger.info(f"재고 없음: {availability_text}")
                    return False
                
                if any(phrase in availability_text for phrase in [
                    'auf lager', 'only', 'left in stock', 'in stock'
                ]):
                    logger.info(f"재고 있음: {availability_text}")
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
        except Exception as e:
            logger.warning(f"재고 확인 중 오류: {e}")
            return True

    def extract_product_info(self, url, row_data, retry_count=0, max_retries=3):
        """제품 정보 추출"""
        try:
            logger.info("=" * 60)
            logger.info(f"독일 제품 정보 추출 시작: {url}")
            
            self.driver.get(url)
            time.sleep(random.uniform(2, 4))
            
            # 503 오류 페이지 확인 및 처리
            if self.is_page_blocked():
                logger.info("503 또는 차단 페이지 감지 - 복구 시도")
                if self.handle_captcha_or_block_page(original_url=url):
                    logger.info("차단 페이지 복구 완료")
                    time.sleep(3)
                    # 복구 후 페이지 로딩 대기
                    self.wait_for_page_load()
                else:
                    raise Exception("차단 페이지 복구 실패")
            
            # 일반적인 차단 페이지 확인
            page_source_lower = self.driver.page_source.lower()
            continue_patterns = ['weiter shoppen', 'weiter einkaufen', 'fortfahren']
            
            if any(pattern in page_source_lower for pattern in continue_patterns):
                logger.info("일반 차단 페이지 감지 - Continue 버튼 시도")
                self.handle_captcha_or_block_page(original_url=url)
                time.sleep(3)
                self.wait_for_page_load()
            
            # 복구 후 현재 URL 확인
            current_url = self.driver.current_url.lower()
            logger.info(f"현재 페이지 URL: {current_url}")
            
            # 정상 Amazon 제품 페이지인지 확인 (차단 페이지가 아닌)
            if 'amazon.de' not in current_url:
                logger.warning("Amazon 도메인이 아닌 페이지")
                raise Exception("Amazon 도메인이 아닌 페이지로 이동됨")
            
            # 제품 페이지 요소 존재 확인으로 정상 페이지 판단
            try:
                # 제품 페이지의 기본 요소들 확인
                basic_elements = [
                    "//span[@id='productTitle']",
                    "//div[@id='feature-bullets']",
                    "//div[@id='centerCol']",
                    "//div[@id='dp-container']"
                ]
                
                page_valid = False
                for selector in basic_elements:
                    try:
                        element = self.driver.find_element(By.XPATH, selector)
                        if element:
                            page_valid = True
                            break
                    except:
                        continue
                
                if not page_valid:
                    logger.warning("제품 페이지 요소를 찾을 수 없음")
                    raise Exception("유효한 제품 페이지가 아님")
                    
                logger.info("정상 제품 페이지 확인됨")
                
            except Exception as e:
                logger.error(f"제품 페이지 검증 실패: {e}")
                raise Exception("제품 페이지 접근 실패")
            
            # V2: 타임존 분리

            
            now_time = datetime.now(self.korea_tz)

            
            local_time = datetime.now(self.local_tz)
            
            result = {
                'retailerid': row_data.get('retailerid', ''),
                'country_code': 'de',
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
                'crawl_datetime': local_time.strftime('%Y-%m-%d %H:%M:%S'),
                'kr_crawl_datetime': now_time.strftime('%Y-%m-%d %H:%M:%S'),  # V2: 한국시간
                'kr_crawl_strdatetime': now_time.strftime('%Y%m%d%H%M%S') + f"{now_time.microsecond:06d}"[:4],  # V2: 한국시간 문자열
                'crawl_strdatetime': local_time.strftime('%Y%m%d%H%M%S') + f"{local_time.microsecond:06d}"[:4],
                'title': None,
                'vat': row_data.get('vat', 'o'),
            }
            
            # 제목 추출
            result['title'] = self.extract_element_text(
                self.selectors.get('title', []), 
                "제목"
            )
            
            # 재고 확인
            has_stock = self.check_stock_availability()
            
            # Ships From 추출
            result['ships_from'] = self.extract_element_text(
                self.selectors.get('ships_from', []), 
                "Ships From"
            )
            
            # Sold By 추출
            result['sold_by'] = self.extract_element_text(
                self.selectors.get('sold_by', []), 
                "Sold By"
            )
            
            # Ships From과 Sold By가 모두 없으면 가격도 빈 값으로 처리
            if not result['ships_from'] and not result['sold_by']:
                logger.warning("Ships From과 Sold By가 모두 없음 - 가격을 빈 값으로 설정")
                result['retailprice'] = None
            else:
                # 가격 추출 (개선된 메서드 사용 - 메인 영역만)
                result['retailprice'] = self.extract_price()
                
                # 가격 범위 검증
                if result['retailprice']:
                    try:
                        price_value = float(re.sub(r'[^\d.]', '', str(result['retailprice'])))
                        if price_value < 1 or price_value > 50000:  # 범위 확대
                            logger.warning(f"독일 비정상적인 가격: {result['retailprice']}")
                            result['retailprice'] = None
                    except:
                        result['retailprice'] = None
                
                # 재고 없을 때 가격 처리
                if not has_stock and result['retailprice'] is None:
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
            
            logger.info(f"제목: {result['title']}")
            logger.info(f"가격: {result['retailprice']}")
            logger.info(f"판매자: {result['sold_by']}")
            logger.info(f"배송지: {result['ships_from']}")
            
            return result
            
        except Exception as e:
            logger.error(f"독일 페이지 처리 오류: {e}")
            
            if retry_count < max_retries:
                wait_time = (retry_count + 1) * 10
                logger.info(f"{wait_time}초 후 재시도... ({retry_count + 1}/{max_retries})")
                time.sleep(wait_time)
                return self.extract_product_info(url, row_data, retry_count + 1, max_retries)
            
            # V2: 타임존 분리

            
            now_time = datetime.now(self.korea_tz)

            
            local_time = datetime.now(self.local_tz)
            return {
                'retailerid': row_data.get('retailerid', ''),
                'country_code': 'de',
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
                'crawl_datetime': local_time.strftime('%Y-%m-%d %H:%M:%S'),
                'kr_crawl_datetime': now_time.strftime('%Y-%m-%d %H:%M:%S'),  # V2: 한국시간
                'kr_crawl_strdatetime': now_time.strftime('%Y%m%d%H%M%S') + f"{now_time.microsecond:06d}"[:4],  # V2: 한국시간 문자열
                'crawl_strdatetime': local_time.strftime('%Y%m%d%H%M%S') + f"{local_time.microsecond:06d}"[:4],
                'title': None,
                'vat': row_data.get('vat', 'o'),
            }
    
    def get_crawl_targets(self, limit=None):
        """DB에서 크롤링 대상 조회"""
        try:
            query = """
            SELECT *
            FROM samsung_price_tracking_list
            WHERE country = 'de' 
              AND mall_name = 'amazon'
              AND is_active = TRUE
            """
            
            if limit:
                query += f" LIMIT {limit}"
            
            df = pd.read_sql(query, self.db_engine)
            logger.info(f"독일 크롤링 대상 {len(df)}개 조회")
            return df.to_dict('records')
        except Exception as e:
            logger.error(f"크롤링 대상 조회 실패: {e}")
            return []
    
    def save_to_db(self, df):
        """DB에 결과 저장"""
        if self.db_engine is None:
            logger.warning("DB 연결 없음")
            return False
        
        try:
            table_name = 'amazon_price_crawl_tbl_de_v2'
            df.to_sql(table_name, self.db_engine, if_exists='append', index=False)
            logger.info(f"독일 DB 저장 완료: {len(df)}개")
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
            
            country_dir = f"{FILE_SERVER_CONFIG['upload_path']}/de"
            
            try:
                sftp.stat(country_dir)
            except FileNotFoundError:
                logger.info(f"독일 디렉토리 생성: {country_dir}")
                sftp.mkdir(country_dir)
            
            remote_path = f"{country_dir}/{remote_filename}"
            
            sftp.put(local_file_path, remote_path)
            logger.info(f"독일 파일서버 업로드 완료: {remote_path}")
            
            sftp.close()
            transport.close()
            
            return True
        except Exception as e:
            logger.error(f"파일서버 업로드 실패: {e}")
            return False
    
    def save_results(self, df, save_db=True, upload_server=True):
        """결과 저장"""
        now = datetime.now(self.korea_tz)
        base_filename = f"{now.strftime('%Y%m%d%H%M%S')}_de_amazon"
        
        results = {'db_saved': False, 'server_uploaded': False}
        
        if save_db:
            results['db_saved'] = self.save_to_db(df)
        
        if upload_server:
            try:
                csv_filename = f'{base_filename}.csv'
                df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
                
                if self.upload_to_file_server(csv_filename):
                    results['server_uploaded'] = True
                
                if os.path.exists(csv_filename):
                    os.remove(csv_filename)
                
                logger.info("임시 파일 삭제 완료")
            except Exception as e:
                logger.error(f"파일 저장 실패: {e}")
        
        return results
    
    def scrape_urls(self, urls_data, max_items=None):
        """URL 스크래핑"""
        if max_items:
            urls_data = urls_data[:max_items]
        
        logger.info(f"독일 크롤링 시작 - {len(urls_data)}개 URL")
        
        if not self.setup_driver():
            logger.error("드라이버 설정 실패")
            return None
        
        results = []
        failed_urls = []
        
        try:
            for idx, row in enumerate(urls_data):
                logger.info(f"진행률: {idx + 1}/{len(urls_data)}")
                
                url = row.get('url')
                result = self.extract_product_info(url, row)
                
                if result['retailprice'] is None and result['title'] is None:
                    failed_urls.append({
                        'url': url,
                        'item': row.get('item', ''),
                        'reason': '가격과 제목 모두 없음'
                    })
                elif result['retailprice'] is None:
                    failed_urls.append({
                        'url': url,
                        'item': row.get('item', ''),
                        'reason': '가격 없음'
                    })
                
                results.append(result)
                
                # 10개마다 중간 저장
                if (idx + 1) % 10 == 0:
                    interim_df = pd.DataFrame(results[-10:])
                    if self.db_engine:
                        try:
                            table_name = 'amazon_price_crawl_tbl_de_v2'
                            interim_df.to_sql(table_name, self.db_engine, if_exists='append', index=False)
                            logger.info("독일 중간 저장: 10개 레코드")
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
                logger.warning(f"실패 URL {len(failed_urls)}개:")
                for fail in failed_urls[:3]:
                    logger.warning(f"  - {fail['item']}: {fail['reason']}")
            
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass
        
        return pd.DataFrame(results)
    
    def analyze_results(self, df):
        """결과 분석"""
        logger.info("독일 결과 분석 시작")
        
        total = len(df)
        with_price = df['retailprice'].notna().sum()
        success_rate = (with_price / total * 100) if total > 0 else 0
        
        logger.info(f"전체: {total}개")
        logger.info(f"가격 성공: {with_price}개")
        logger.info(f"성공률: {success_rate:.1f}%")
        
        if with_price > 0:
            try:
                price_df = df[df['retailprice'].notna()].copy()
                price_df['price_numeric'] = price_df['retailprice'].astype(str).str.replace(',', '').astype(float)
                
                logger.info("독일 가격 통계:")
                logger.info(f"  평균: {price_df['price_numeric'].mean():.2f}€")
                logger.info(f"  최저: {price_df['price_numeric'].min():.2f}€")
                logger.info(f"  최고: {price_df['price_numeric'].max():.2f}€")
            except Exception as e:
                logger.warning(f"가격 통계 오류: {e}")

def main():
    """메인 실행 함수"""
    test_mode = os.getenv('TEST_MODE', 'false').lower() == 'true'
    max_items = int(os.getenv('MAX_ITEMS', '0')) or None
    
    print("=" * 60)
    print("Amazon 독일 크롤러 v2.0 - 추천상품/관련상품 영역 필터링 강화판")
    print("=" * 60)
    print(f"국가: 독일 (DE)")
    print(f"모드: {'테스트' if test_mode else '전체 크롤링'}")
    if max_items:
        print(f"최대: {max_items}개")
    else:
        print("전체 제품 크롤링")
    print("=" * 60)
    
    scraper = AmazonDEScraper()
    
    if test_mode:
        logger.info("독일 테스트 모드 실행")
        
        # 문제가 있던 URL들로 테스트
        test_urls = [
            'https://www.amazon.de/dp/B09QV692XY?th=1',
            'https://www.amazon.de/dp/B0B7CKZGN6?th=1',
            'https://www.amazon.de/dp/B0BKPV953L?th=1',
            'https://www.amazon.de/dp/B0F3H2TLX1'  # 문제 URL
        ]
        
        test_data = []
        for i, url in enumerate(test_urls):
            test_data.append({
                'url': url,
                'brand': 'Samsung',
                'item': f'Test Item {i+1}',
                'retailerid': f'TEST00{i+1}',
                'retailersku': f'TEST00{i+1}',
                'channel': 'Online',
                'seg_lv1': 'SSD',
                'seg_lv2': 'Consumer',
                'seg_lv3': 'External',
                'capacity': '2TB',
                'form_factor': 'External'
            })
        
        results_df = scraper.scrape_urls(test_data)
        if results_df is not None and not results_df.empty:
            scraper.analyze_results(results_df)
            scraper.save_results(results_df, save_db=False, upload_server=True)
        return
    
    logger.info("독일 전체 크롤링 시작")
    if scraper.db_engine is None:
        logger.error("DB 연결 실패")
        return
    
    urls_data = scraper.get_crawl_targets(limit=max_items)
    if not urls_data:
        logger.warning("독일 크롤링 대상 없음")
        return
    
    results_df = scraper.scrape_urls(urls_data, max_items)
    if results_df is None or results_df.empty:
        logger.error("크롤링 결과 없음")
        return
    
    scraper.analyze_results(results_df)
    save_results = scraper.save_results(results_df, save_db=True, upload_server=True)
    
    logger.info("독일 저장 결과:")
    logger.info(f"DB: {'성공' if save_results['db_saved'] else '실패'}")
    logger.info(f"파일: {'성공' if save_results['server_uploaded'] else '실패'}")
    logger.info("독일 크롤링 완료!")

if __name__ == "__main__":
    print("필요 패키지:")
    print("pip install undetected-chromedriver selenium pandas pymysql sqlalchemy paramiko openpyxl")
    print()
    print("환경변수 (선택사항):")
    print("export TEST_MODE=true      # 테스트 모드")
    print("export MAX_ITEMS=100       # 선택사항 (없으면 전체)")
    print()
    print("독일 전용 크롤링 모드로 실행 중...")
    print()
    
    main()
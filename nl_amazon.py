# -*- coding: utf-8 -*-
"""
Amazon 네덜란드 가격 추출 시스템 V2 (타임존 분리 버전)
원본 it.py 기반 - DB/타임존/파일서버 설정만 V2로 변경
- 현지시간(네덜란드)과 한국시간 분리 저장
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

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Import database configuration V2
from config import DB_CONFIG_V2 as DB_CONFIG

from config import FILE_SERVER_CONFIG

class AmazonNLScraper:
    def __init__(self):
        self.driver = None
        self.db_engine = None
        self.country_code = 'nl'
        self.wait = None
        # V2: 타임존 분리 (현지시간 + 한국시간)
        self.korea_tz = pytz.timezone('Asia/Seoul')
        self.local_tz = pytz.timezone('Europe/Amsterdam')  # 네덜란드 현지 시간
        
        self.setup_db_connection()
        self.setup_it_selectors()
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
    
    def setup_it_selectors(self):
        """네덜란드 전용 선택자 설정 - 메인 상품 영역만 타겟팅"""
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
                
                # 네덜란드 가격 선택자 (더 구체적인 제한)
                "//*[@id='centerCol']//*[@id='corePrice_feature_div']//span[@class='a-offscreen'][1]",
                
                # 첫 번째 가격만 (centerCol 내부만, 개선된 선택자)
                "(//*[@id='centerCol']//span[@class='a-price']//span[@class='a-offscreen'])[1]",
                "(//*[@id='centerCol']//*[@id='corePriceDisplay_desktop_feature_div']//span[@class='a-offscreen'])[1]",
                
                # 기본 가격 요소들 (centerCol 내부만)
                "//*[@id='centerCol']//*[@id='priceblock_ourprice']",
                "//*[@id='centerCol']//*[@id='priceblock_dealprice']",
                "//*[@id='centerCol']//*[@id='listPrice']",
                
                # whole 가격 (centerCol 내부만)
                "//*[@id='centerCol']//*[@id='corePrice_feature_div']//span[@class='a-price-whole']",
                "//*[@id='centerCol']//*[@id='corePriceDisplay_desktop_feature_div']//span[@class='a-price-whole']",
                "//*[@id='centerCol']//*[@id='apex_desktop']//span[@class='a-price-whole']",
                
                # 네덜란드 전용 가격 선택자 (centerCol 제한)
                "//*[@id='centerCol']//span[@class='a-price-symbol' and contains(text(), '€')]/following-sibling::span",
                "//*[@id='centerCol']//span[contains(@class, 'a-price') and contains(text(), '€')]",
                
                # 백업용 일반 선택자 (최후 수단)
                "//div[@id='centerCol']//span[@class='a-price']//span[@class='a-offscreen']",
                "//div[@id='centerCol']//span[@class='a-price-whole']",
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
                "//h1/span[@id='productTitle']",
                "h1#title span",
                "//div[@id='titleSection']//h1//span",
                "//div[@id='title_feature_div']//h1//span"
            ],
            'ships_from': [
                # 네덜란드 "Spedito da" 선택자 (centerCol 제한 제거 - 정상 영역)
                "//*[@id='SSOFpopoverLink_ubb']",
                "//a[@id='SSOFpopoverLink_ubb']",
                "//*[@id='fulfillerInfoFeature_feature_div']/div[2]/div[1]/span",
                "//div[@id='fulfillerInfoFeature_feature_div']//span",
                "//*[@id='fulfillerInfoFeature_feature_div']//div[2]//span",
                "//div[contains(@id, 'fulfillerInfo')]//span",
                "//span[contains(text(), 'Spedito da')]/following-sibling::span",
                "//span[contains(text(), 'Spedito da')]/parent::div//span[2]",
                "//div[contains(text(), 'Spedito da')]//span",
                "//div[@data-feature-name='shipsFromSoldBy']//span"
            ],
            'sold_by': [
                # 우선 추출 xpath (최우선순위)
                "/html/body/div[2]/div/div/div[4]/div[1]/div[4]/div/div[1]/div/div[1]/div/div/div[1]/div/div[2]/div/form/div/div/div[21]/div/div/div[1]/div/div[3]/div[2]/div[1]/span",
                # 네덜란드 "Venduto da" 선택자 (기존)
                "//a[@id='sellerProfileTriggerId']",
                "//*[@id='sellerProfileTriggerId']",
                "//*[@id='merchantInfoFeature_feature_div']/div[2]/div[1]/span",
                "//div[@id='merchantInfoFeature_feature_div']//a",
                "//div[@id='merchantInfoFeature_feature_div']//span",
                "//*[@id='merchantInfoFeature_feature_div']//div[2]//span",
                "//div[contains(@id, 'merchantInfo')]//span",
                "//span[contains(text(), 'Venduto da')]/following-sibling::a",
                "//span[contains(text(), 'Venduto da')]/following-sibling::span",
                "//span[contains(text(), 'Venduto da')]/parent::div//span[2]",
                "//div[contains(text(), 'Venduto da')]//span",
                "//div[@data-feature-name='shipsFromSoldBy']//a"
            ],
            'imageurl': [
                "//div[@id='imageBlock']//img[@id='landingImage']",
                "//div[@id='main-image-container']//img",
                "//img[@class='a-dynamic-image']",
                "//div[@class='imgTagWrapper']//img",
                "//div[@id='imageBlock_feature_div']//img"
            ],
            'availability': [
                "//div[@id='availability']//span",
                "//div[@id='availability_feature_div']//span",
                "//span[@class='a-size-medium a-color-success']",
                "//span[@class='a-size-medium a-color-price']",
                "//div[@id='availability']//div[@class='a-row']//span"
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
                r'.*div\[4\].*div\[5\].*',  # div[4] 이후 깊은 중첩
            ],
            # Sold By 전용 제외 xpath 패턴
            'excluded_sold_by_xpaths': [
                '/html/body/div[2]/div/div/div[4]/div[1]/div[4]/div/div[1]/div/div[1]/div/div/div[3]/div/div[1]/h5/div[5]/div/div/div/div[3]/div[2]/div/span'
            ],
            'stock_flag': [
                # 네덜란드 재고 부족 메시지
                'Attualmente non disponibile',
                'Non disponibile', 
                'Esaurito',
                'Temporaneamente non disponibile',
                'Currently unavailable', 
                'Out of Stock',
                'Temporarily out of stock'
            ],
            'blocked_patterns': [
                'sorry', 
                'robot check', 
                '503 Service Unavailable',
                'Something went wrong',
                'access denied',
                'qualcosa è andato storto',
                'accesso negato',
                'controlla di essere umano',
                'inserisci i caratteri'
            ],
            'continue_buttons': [
                # 네덜란드 "계속 쇼핑" 버튼
                "//button[contains(text(), 'Continua lo shopping')]",
                "//a[contains(text(), 'Continua lo shopping')]",
                "//span[contains(text(), 'Continua lo shopping')]/ancestor::button",
                "//input[@value='Continua lo shopping']",
                "//button[contains(@class, 'a-button') and contains(., 'Continua')]",
                "//div[contains(@class, 'a-button') and contains(., 'Continua')]//button",
                "//button[contains(text(), 'Continue shopping')]",
                "//button[contains(@class, 'a-button-primary')]",
                "//input[@type='submit' and contains(@value, 'Continue')]"
            ]
        }
    
    def load_selectors_from_db(self):
        """DB에서 네덜란드 특화 선택자 로드"""
        if not self.db_engine:
            logger.warning("DB 연결이 없어 선택자 로드 불가")
            return
            
        try:
            query = """
            SELECT element_type, selector_value, priority
            FROM amazon_selectors
            WHERE country_code = 'nl'
              AND is_active = TRUE
              AND selector_value NOT LIKE '/html/%'
            ORDER BY element_type, priority ASC
            """
            
            df = pd.read_sql(query, self.db_engine)
            logger.info(f"DB에서 네덜란드 선택자 로드: {len(df)}개")
            
            for element_type in df['element_type'].unique():
                db_selectors = df[df['element_type'] == element_type]['selector_value'].tolist()
                
                if element_type in self.selectors:
                    existing = self.selectors[element_type]
                    # DB 선택자를 기존 선택자 앞에 추가 (우선순위)
                    self.selectors[element_type] = db_selectors + [s for s in existing if s not in db_selectors]
                else:
                    self.selectors[element_type] = db_selectors
            
            logger.info("네덜란드 DB 선택자 로드 완료")
            
        except Exception as e:
            logger.error(f"DB 선택자 로드 실패: {e}")
    
    def setup_driver(self):
        """네덜란드 전용 Chrome 드라이버 설정"""
        logger.info("네덜란드 전용 Chrome 드라이버 설정 중...")
        
        try:
            options = uc.ChromeOptions()
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-setuid-sandbox')
            options.add_argument('--disable-gpu')
            options.add_argument('--disable-software-rasterizer')
            
            # 네덜란드 사용자 에이전트
            dutch_user_agents = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            ]
            
            options.add_argument(f'--user-agent={random.choice(dutch_user_agents)}')
            
            # 네덜란드 언어 설정
            options.add_experimental_option('prefs', {
                'intl.accept_languages': 'nl-NL,nl,en-US,en'
            })
            
            self.driver = uc.Chrome(options=options)
            self.driver.maximize_window()
            self.wait = WebDriverWait(self.driver, 20)
            
            logger.info("네덜란드 드라이버 설정 완료")
            return True
            
        except Exception as e:
            logger.error(f"드라이버 설정 실패: {e}")
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
        """네덜란드 페이지 차단 감지"""
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
            # 네덜란드 심각한 차단 지표
            serious_blocked_indicators = {
                'title': [
                    '503',
                    'access denied',
                    'accesso negato',
                    'error has occurred',
                    'errore',
                    'ci dispiace'
                ],
                'content': [
                    'enter the characters',
                    'inserisci i caratteri',
                    'verify you are human',
                    'controlla di essere umano',
                    'access denied',
                    'accesso negato',
                    'automated access',
                    'suspicious activity',
                    'attività sospetta',
                    'si è verificato un errore',
                    'ci dispiace'
                ]
            }
            
            for pattern in serious_blocked_indicators['title']:
                if pattern in page_title:
                    logger.warning(f"네덜란드 심각한 차단 감지 (제목): {pattern}")
                    return True
            
            # "Ci dispiace" 오류는 차단이 아닌 처리 가능한 오류로 분류
            if ('ci dispiace' in page_source and 
                'continua lo shopping' not in page_source and 
                'continue shopping' not in page_source and
                'clicca qui per tornare' not in page_source):
                # 홈페이지 링크도 없으면 심각한 차단
                for pattern in serious_blocked_indicators['content']:
                    if pattern in page_source and pattern not in ['ci dispiace', 'si è verificato un errore']:
                        logger.warning(f"네덜란드 심각한 차단 감지 (본문): {pattern}")
                        return True
            
            return False  # 기본적으로 정상으로 판단
            
        except Exception as e:
            logger.error(f"네덜란드 페이지 차단 확인 중 오류: {e}")
            return False  # 오류 시 정상으로 판단
    
    def wait_for_page_load(self, timeout=10):
        """네덜란드 페이지 로드 대기"""
        try:
            self.wait.until(
                lambda driver: driver.execute_script("return document.readyState") == "complete"
            )
            
            # 네덜란드 페이지 요소 확인
            possible_elements = [
                (By.ID, "productTitle"),
                (By.ID, "priceblock_ourprice"),
                (By.CLASS_NAME, "a-price-whole"),
                (By.ID, "availability"),
                (By.ID, "imageBlock"),
                (By.ID, "corePrice_feature_div"),
                (By.ID, "corePriceDisplay_desktop_feature_div")
            ]
            
            for by, value in possible_elements:
                try:
                    WebDriverWait(self.driver, 3).until(
                        EC.presence_of_element_located((by, value))
                    )
                    logger.debug(f"네덜란드 요소 발견: {by}={value}")
                    return True
                except:
                    continue
            
            return True
            
        except Exception as e:
            logger.warning(f"네덜란드 페이지 로드 대기 중 오류: {e}")
            return False

    def handle_captcha_or_block_page(self, original_url=None):
        """네덜란드 차단 페이지나 캡차 처리"""
        try:
            logger.info("네덜란드 차단/캡차 페이지 확인 중...")
            
            # 먼저 정상 페이지인지 확인 (불필요한 처리 방지)
            try:
                normal_check = self.driver.find_element(By.XPATH, "//span[@id='productTitle']")
                if normal_check and normal_check.is_displayed():
                    logger.info("정상 제품 페이지 확인됨 - 처리 불필요")
                    return True
            except:
                pass
            
            page_source = self.driver.page_source.lower()
            page_title = self.driver.title.lower()
            
            # 1. "Ci dispiace" 오류 페이지 정확한 감지
            ci_dispiace_indicators = [
                'ci dispiace' in page_title,
                'si è verificato un errore quando abbiamo tentato di elaborare la richiesta' in page_source,
                'stiamo lavorando al problema' in page_source,
                'clicca qui per tornare alla home page di amazon.nl' in page_source,
                'non sarà stato elaborato per il momento' in page_source
            ]
            
            if sum(ci_dispiace_indicators) >= 2:
                logger.info("네덜란드 'Ci dispiace' 오류 페이지 확인됨")
                
                # "Clicca qui per tornare alla home page di Amazon.nl" 링크 찾기
                home_link_selectors = [
                    "//a[contains(text(), 'Clicca qui per tornare alla home page')]",
                    "//a[contains(text(), 'tornare alla home page')]",
                    "//a[contains(text(), 'home page di Amazon')]",
                    "//a[contains(@href, 'amazon.nl') and contains(text(), 'home')]",
                    "//a[contains(text(), 'Amazon.nl') and contains(text(), 'home')]"
                ]
                
                for selector in home_link_selectors:
                    try:
                        logger.info(f"네덜란드 홈페이지 링크 찾기 시도: {selector}")
                        
                        link = self.driver.find_element(By.XPATH, selector)
                        
                        if link and link.is_displayed():
                            link_text = link.text
                            logger.info(f"네덜란드 홈페이지 링크 발견: '{link_text}'")
                            
                            # 홈페이지 링크 클릭
                            self.driver.execute_script("arguments[0].scrollIntoView();", link)
                            time.sleep(1)
                            
                            try:
                                link.click()
                                logger.info("네덜란드 홈페이지 링크 클릭 성공")
                            except:
                                self.driver.execute_script("arguments[0].click();", link)
                                logger.info("네덜란드 홈페이지 링크 JavaScript 클릭 성공")
                            
                            time.sleep(3)
                            
                            # 원래 URL로 다시 이동
                            if original_url:
                                logger.info(f"원래 URL로 재접속: {original_url}")
                                self.driver.get(original_url)
                                time.sleep(3)
                                return True
                            else:
                                logger.info("네덜란드 홈페이지 이동 완료")
                                return True
                                
                    except Exception as e:
                        logger.debug(f"네덜란드 홈페이지 링크 오류: {e}")
                        continue
                
                # 홈페이지 링크를 찾지 못했으면 직접 홈페이지로 이동
                logger.info("네덜란드 홈페이지 링크를 찾지 못함, 직접 이동")
                self.driver.get("https://www.amazon.nl/")
                time.sleep(3)
                
                if original_url:
                    logger.info(f"원래 URL로 재접속: {original_url}")
                    self.driver.get(original_url)
                    time.sleep(3)
                
                return True
            
            # 2. 일반 Continue 버튼 처리 (Ci dispiace가 아닌 경우에만)
            if 'ci dispiace' not in page_title and 'ci dispiace' not in page_source:
                for selector in self.selectors['continue_buttons']:
                    try:
                        logger.info(f"네덜란드 Continue 버튼 찾기 시도: {selector}")
                        
                        if selector.startswith('//'):
                            button = self.driver.find_element(By.XPATH, selector)
                        else:
                            button = self.driver.find_element(By.CSS_SELECTOR, selector)
                        
                        if button and button.is_displayed():
                            button_text = button.text
                            logger.info(f"네덜란드 Continue 버튼 발견: {selector} (텍스트: '{button_text}')")
                            
                            self.driver.execute_script("arguments[0].scrollIntoView();", button)
                            time.sleep(1)
                            
                            try:
                                button.click()
                                logger.info("네덜란드 Continue 버튼 클릭 성공")
                            except:
                                self.driver.execute_script("arguments[0].click();", button)
                                logger.info("네덜란드 Continue 버튼 JavaScript 클릭 성공")
                            
                            time.sleep(3)
                            return True
                            
                    except Exception as e:
                        logger.debug(f"네덜란드 Continue 버튼 오류: {e}")
                        continue
            
            logger.debug("네덜란드 처리 가능한 버튼을 찾을 수 없음")
            return False
            
        except Exception as e:
            logger.error(f"네덜란드 차단 페이지 처리 중 오류: {e}")
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
        """Ships From / Sold By 텍스트 정리 (네덜란드 특화)"""
        try:
            label_patterns = {
                'Ships From': ['ships from', 'fulfilled by', 'spedito da'],
                'Sold By': ['sold by', 'venduto da']
            }
            
            text_lower = text.lower().strip()
            patterns = label_patterns.get(element_type, [])
            
            # 라벨만 있는 경우 스킵
            if text_lower in patterns:
                return None
            
            # 패턴 매칭 및 텍스트 추출
            for pattern in patterns:
                if text_lower.startswith(pattern + ' '):
                    # "Fulfilled by Amazon" -> "Amazon" 추출
                    actual_value = text[len(pattern):].strip()
                    if actual_value:
                        return actual_value
                
                # 패턴이 텍스트 중간에 있는 경우도 처리
                if pattern in text_lower:
                    # 패턴 이후의 텍스트 추출
                    pattern_index = text_lower.find(pattern)
                    if pattern_index >= 0:
                        after_pattern = text[pattern_index + len(pattern):].strip()
                        if after_pattern:
                            return after_pattern
            
            return text
        except Exception as e:
            logger.debug(f"텍스트 정리 오류: {e}")
            return text
    
    def extract_element_text(self, selectors, element_name="요소"):
        """선택자 목록에서 텍스트 추출 (가격만 필터링 적용)"""
        logger.info(f"네덜란드 {element_name} 추출 시작 - 총 {len(selectors)}개 선택자")
        
        for idx, selector in enumerate(selectors, 1):
            try:
                logger.info(f"[{idx}/{len(selectors)}] 시도: {selector}")
                
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
                                # 가격 요소만 강화된 필터링 적용
                                if element_name == "가격" and self.is_excluded_price_element(element):
                                    continue
                                
                                text = self.extract_clean_text_from_element(element, element_name)
                                if text:
                                    logger.info(f"네덜란드 최종 추출 성공: '{text}'")
                                    return text
                        except Exception as e:
                            logger.debug(f"요소 처리 중 오류: {e}")
                
            except Exception as e:
                logger.debug(f"선택자 오류: {str(e)}")
                continue
        
        logger.error(f"네덜란드 {element_name} 추출 완전 실패")
        return None
    
    def parse_dutch_price(self, price_text):
        """네덜란드 가격 파싱 (€ 통화, 쉼표 소수점)"""
        try:
            price_text = price_text.strip()
            logger.debug(f"네덜란드 가격 파싱: '{price_text}'")
            
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
            
            # 네덜란드 형식: 61,84 또는 1.234,56
            if ',' in cleaned:
                # 쉼표가 있으면 마지막 쉼표를 소수점으로 처리
                parts = cleaned.rsplit(',', 1)
                if len(parts) == 2 and len(parts[1]) <= 2 and parts[1].isdigit():
                    # 정수 부분에서 점 제거 (천단위 구분자)
                    integer_part = parts[0].replace('.', '')
                    decimal_part = parts[1]
                    final_price = f"{integer_part}.{decimal_part}"
                    logger.debug(f"네덜란드 쉼표→점 변환: '{final_price}'")
                    
                    # 최종 검증
                    if re.match(r'^\d+\.\d{1,2}$', final_price):
                        return final_price
                else:
                    # 쉼표가 있지만 소수점이 아닌 경우
                    cleaned = cleaned.replace(',', '')
            
            # 끝에 점만 있는 경우 처리 (예: "294." -> "294")
            if cleaned.endswith('.') and len(cleaned) > 1:
                cleaned_no_dot = cleaned[:-1]
                if cleaned_no_dot.isdigit():
                    logger.debug(f"끝 점 제거: '{cleaned}' -> '{cleaned_no_dot}'")
                    cleaned = cleaned_no_dot
            
            # 정수 가격 또는 점으로 구분된 가격
            if re.match(r'^\d+(\.\d{1,2})?$', cleaned):
                try:
                    price_value = float(cleaned)
                    logger.info(f"네덜란드 최종 가격 검증: '{cleaned}' -> {price_value}")
                    if 1 <= price_value <= 50000:
                        logger.info(f"네덜란드 가격 파싱 성공: {cleaned}")
                        return cleaned
                    else:
                        logger.debug(f"가격 범위 벗어남: {price_value}")
                except Exception as e:
                    logger.debug(f"float 변환 실패: {cleaned} - {e}")
                    pass
                
        except Exception as e:
            logger.debug(f"네덜란드 가격 파싱 오류: {price_text} - {e}")
            
        return None
    
    def extract_price(self):
        """네덜란드 가격 추출 (개선된 버전) - 메인 상품 영역만 타겟팅"""
        logger.info("네덜란드 가격 추출 시작 (메인 상품 영역만)")
        
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
                                logger.info(f"네덜란드 1단계 추출된 텍스트: '{text}'")
                                
                                # 끝에 점이나 쉼표가 있거나 2자리 이하 숫자만 있으면 분리된 가격으로 판단
                                if text.endswith('.') or text.endswith(',') or (text.isdigit() and len(text) <= 2):
                                    logger.info(f"분리된 가격 감지: '{text}' - whole+fraction 조합 시도")
                                    continue  # 1단계 건너뛰고 2단계 whole+fraction으로
                                
                                price = self.parse_dutch_price(text)
                                if price:
                                    logger.info(f"네덜란드 가격 추출 성공: {price}")
                                    return price
                                else:
                                    logger.debug(f"파싱 실패한 텍스트: '{text}'")
                    except Exception as e:
                        logger.debug(f"요소 처리 오류: {e}")
                        continue
            except Exception as e:
                logger.debug(f"선택자 오류: {e}")
                continue
        
        # 2단계: whole + fraction 조합 시도 (centerCol 내부만)
        logger.info("네덜란드 whole + fraction 조합 시도 (centerCol 내부만)")
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
                    
                    logger.info(f"네덜란드 정수부: {whole_text}, 소수부: {fraction_text}")
                    
                    if whole_text and fraction_text:
                        # 정수부에서도 숫자가 아닌 문자 제거
                        whole_clean = re.sub(r'[^\d]', '', whole_text)
                        fraction_clean = re.sub(r'[^\d]', '', fraction_text)
                        if whole_clean and fraction_clean:
                            combined_price = f"{whole_clean}.{fraction_clean}"
                            logger.info(f"네덜란드 조합된 가격: {combined_price}")
                            
                            price = self.parse_dutch_price(combined_price)
                            if price:
                                logger.info(f"네덜란드 조합 가격 추출 성공: {price}")
                                return price
            except Exception as e:
                logger.debug(f"조합 오류: {e}")
                continue
        
        logger.error("네덜란드 모든 방법으로 가격 추출 실패 (메인 상품 영역만 검색)")
        return None
    
    def check_stock_availability(self):
        """네덜란드 재고 상태 확인"""
        try:
            # 네덜란드 availability 요소 확인
            try:
                availability_elem = self.driver.find_element(By.ID, "availability")
                availability_text = availability_elem.text.lower()
                
                # 네덜란드 재고 없음 메시지 확인
                dutch_out_of_stock = [
                    'attualmente non disponibile',
                    'non disponibile', 
                    'esaurito',
                    'temporaneamente non disponibile',
                    'currently unavailable',
                    'out of stock',
                    'temporarily out of stock'
                ]
                
                if any(phrase in availability_text for phrase in dutch_out_of_stock):
                    logger.info(f"네덜란드 재고 없음: {availability_text}")
                    return False
                    
                # 네덜란드 재고 있음 메시지 확인
                dutch_in_stock = [
                    'disponibile',
                    'in stock',
                    'disponibilità immediata',
                    'pronto per la spedizione',
                    'only',
                    'left in stock',
                    'disponibili'
                ]
                
                if any(phrase in availability_text for phrase in dutch_in_stock):
                    logger.info(f"네덜란드 재고 있음: {availability_text}")
                    return True
                    
            except NoSuchElementException:
                logger.debug("네덜란드 availability 요소를 찾을 수 없음")
            
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
                        logger.info("네덜란드 구매 버튼 활성화 - 재고 있음")
                        return True
                except:
                    continue
            
            logger.info("네덜란드 재고 상태 불명확 - 기본값: 재고 있음")
            return True
            
        except Exception as e:
            logger.warning(f"네덜란드 재고 확인 중 오류: {e}")
            return True

    def extract_product_info(self, url, row_data, retry_count=0, max_retries=3):
        """네덜란드 제품 정보 추출"""
        try:
            logger.info("=" * 60)
            logger.info("네덜란드 제품 정보 추출 시작")
            logger.info(f"URL: {url}")
            logger.info(f"브랜드: {row_data.get('brand', 'N/A')}")
            logger.info(f"제품: {row_data.get('item', 'N/A')}")
            
            self.driver.get(url)
            time.sleep(random.uniform(3, 5))
            
            # 차단 페이지 확인 및 처리
            if self.is_page_blocked():
                logger.info("네덜란드 차단 페이지 감지 - 복구 시도")
                if self.handle_captcha_or_block_page(original_url=url):
                    logger.info("네덜란드 차단 페이지 복구 완료")
                    time.sleep(3)
                    self.wait_for_page_load()
                else:
                    raise Exception("네덜란드 차단 페이지 복구 실패")
            
            # 복구 후 현재 URL 확인
            current_url = self.driver.current_url.lower()
            logger.info(f"현재 페이지 URL: {current_url}")
            
            # 정상 Amazon 제품 페이지인지 확인
            if 'amazon.nl' not in current_url:
                logger.warning("Amazon 네덜란드 도메인이 아닌 페이지")
                raise Exception("Amazon 네덜란드 도메인이 아닌 페이지로 이동됨")
            
            # 제품 페이지 요소 존재 확인으로 정상 페이지 판단
            try:
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

            # ISO 8601 형식
            crawl_dt = local_time.strftime("%Y-%m-%dT%H:%M:%S")
            tz_offset = local_time.strftime("%z")
            tz_formatted = f"{tz_offset[:3]}:{tz_offset[3:]}" if tz_offset else "+00:00"
            crawl_datetime_iso = f"{crawl_dt}{tz_formatted}"

            result = {
                'retailerid': row_data.get('retailerid', ''),
                'country_code': 'nl',
                'ships_from': None,
                'channel_name': 'amazon.nl',
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
                'kr_crawl_datetime': now_time.strftime('%Y-%m-%d %H:%M:%S'),  # V2: 한국시간
                'kr_crawl_strdatetime': now_time.strftime('%Y%m%d%H%M%S') + f"{now_time.microsecond:06d}"[:4],  # V2: 한국시간 문자열
                'crawl_strdatetime': local_time.strftime('%Y%m%d%H%M%S') + f"{local_time.microsecond:06d}"[:4],
                'title': None,
                'vat': row_data.get('vat', 'o')
            }
            
            # 제목 추출
            result['title'] = self.extract_element_text(
                self.selectors['title'], 
                "제목"
            )
            
            # 재고 확인
            has_stock = self.check_stock_availability()

            # Ships From 추출 (기본 추출만, 추가 필터링 없음)
            result['ships_from'] = self.extract_element_text(
                self.selectors['ships_from'],
                "Ships From"
            )

            # Ships From에서 "verzonden door " 접두사 제거
            if result['ships_from']:
                ships_from_lower = result['ships_from'].lower()
                if 'verzonden door ' in ships_from_lower:
                    # "verzonden door " 뒤의 텍스트만 추출
                    idx = ships_from_lower.find('verzonden door ')
                    result['ships_from'] = result['ships_from'][idx + len('verzonden door '):].strip()
                    logger.info(f"Ships From 접두사 제거 후: {result['ships_from']}")

            # Sold By 추출 (기본 추출만, 추가 필터링 없음)
            result['sold_by'] = self.extract_element_text(
                self.selectors['sold_by'],
                "Sold By"
            )

            # ships_from이 None이고 sold_by가 있을 때, 통합 라벨 확인
            if not result['ships_from'] and result['sold_by']:
                try:
                    # "Verzender / Verkoper" 라벨이 있는지 확인 (배송자/판매자 통합)
                    combined_label_selectors = [
                        "//span[contains(text(), 'Verzender / Verkoper')]",
                        "//span[contains(text(), 'Verzender/Verkoper')]",
                        "//*[@id='merchantInfoFeature_feature_div']//span[contains(@class, 'a-color-tertiary')][contains(text(), 'Verzender')]"
                    ]
                    for label_selector in combined_label_selectors:
                        try:
                            label_element = self.driver.find_element(By.XPATH, label_selector)
                            if label_element and label_element.is_displayed():
                                # 통합 라벨 발견 - sold_by 값을 ships_from에도 저장
                                result['ships_from'] = result['sold_by']
                                logger.info(f"Verzender / Verkoper 통합 라벨 발견 - ships_from에 sold_by 값 복사: {result['ships_from']}")
                                break
                        except:
                            continue
                except Exception as e:
                    logger.debug(f"통합 라벨 확인 중 오류: {e}")

            # Ships From과 Sold By가 모두 없으면 가격도 빈 값으로 처리
            if not result['ships_from'] and not result['sold_by']:
                logger.warning("Ships From과 Sold By가 모두 없음 - 가격을 빈 값으로 설정")
                result['retailprice'] = None
            else:
                # 가격 추출 (개선된 메서드 사용 - 메인 영역만)
                logger.info("네덜란드 가격 추출 시도")
                result['retailprice'] = self.extract_price()
                
                # 가격 범위 검증
                if result['retailprice']:
                    try:
                        price_value = float(re.sub(r'[^\d.]', '', str(result['retailprice'])))
                        if price_value < 1 or price_value > 50000:
                            logger.warning(f"네덜란드 비정상적인 가격: {result['retailprice']}")
                            result['retailprice'] = None
                    except:
                        result['retailprice'] = None
                
                # 재고 없을 때 가격 처리
                if not has_stock and result['retailprice'] is None:
                    result['retailprice'] = None
            
            # 이미지 URL 추출
            for selector in self.selectors['imageurl']:
                try:
                    if selector.startswith('//'):
                        element = self.driver.find_element(By.XPATH, selector)
                    else:
                        element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    
                    result['imageurl'] = element.get_attribute('src')
                    if result['imageurl']:
                        logger.debug("네덜란드 이미지 URL 추출 성공")
                        break
                except:
                    continue
            
            logger.info("네덜란드 추출 결과:")
            logger.info(f"제목: {result['title'][:50] + '...' if result['title'] and len(result['title']) > 50 else result['title']}")
            logger.info(f"가격: {result['retailprice']}")
            logger.info(f"이미지: {'있음' if result['imageurl'] else '없음'}")
            logger.info(f"판매자: {result['sold_by']}")
            logger.info(f"배송지: {result['ships_from']}")
            
            return result
            
        except Exception as e:
            logger.error(f"네덜란드 페이지 처리 오류: {e}")
            
            if retry_count < max_retries:
                wait_time = (retry_count + 1) * 10
                logger.info(f"네덜란드 {wait_time}초 후 재시도... ({retry_count + 1}/{max_retries})")
                time.sleep(wait_time)
                
                try:
                    self.driver.refresh()
                except:
                    logger.info("네덜란드 드라이버 재시작 중...")
                    self.driver.quit()
                    self.setup_driver()
                
                return self.extract_product_info(url, row_data, retry_count + 1, max_retries)
            
            # 실패 시 기본 결과 반환
            # V2: 타임존 분리

            now_time = datetime.now(self.korea_tz)

            local_time = datetime.now(self.local_tz)
            return {
                'retailerid': row_data.get('retailerid', ''),
                'country_code': 'nl',
                'ships_from': None,
                'channel_name': 'amazon.nl',
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
                'kr_crawl_datetime': now_time.strftime('%Y-%m-%d %H:%M:%S'),  # V2: 한국시간
                'kr_crawl_strdatetime': now_time.strftime('%Y%m%d%H%M%S') + f"{now_time.microsecond:06d}"[:4],  # V2: 한국시간 문자열
                'crawl_strdatetime': local_time.strftime('%Y%m%d%H%M%S') + f"{local_time.microsecond:06d}"[:4],
                'title': None,
                'vat': row_data.get('vat', 'o')
            }
    
    def get_crawl_targets(self, limit=None):
        """네덜란드 크롤링 대상 URL 목록 조회"""
        try:
            query = """
            SELECT *
            FROM samsung_price_tracking_list
            WHERE country = 'nl'
              AND mall_name = 'amazon'
              AND is_active = TRUE
            """
                
            if limit:
                query += f" LIMIT {limit}"
            
            df = pd.read_sql(query, self.db_engine)
            logger.info(f"네덜란드 크롤링 대상 {len(df)}개 조회 완료")
            return df.to_dict('records')
            
        except Exception as e:
            logger.error(f"네덜란드 크롤링 대상 조회 실패: {e}")
            return []
    
    def save_to_db(self, df):
        """네덜란드 DB에 결과 저장"""
        if self.db_engine is None:
            logger.warning("DB 연결이 없어 네덜란드 DB 저장을 건너뜁니다")
            return False
        
        try:
            table_name = 'amazon_price_crawl_tbl_nl'

            df.to_sql(table_name, self.db_engine, if_exists='append', index=False)
            logger.info(f"네덜란드 DB 저장 완료: {len(df)}개 레코드 -> {table_name}")
            
            return True
            
        except Exception as e:
            logger.error(f"네덜란드 DB 저장 실패: {e}")
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
        base_filename = f"{date_str}_{time_str}_nl_amazon"

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
        """네덜란드 URL 스크래핑"""
        if max_items:
            urls_data = urls_data[:max_items]
        
        logger.info("=" * 80)
        logger.info("네덜란드 Amazon 크롤링 시작")
        logger.info(f"대상: {len(urls_data)}개 제품")
        logger.info("특화 기능: 쉼표 소수점 가격 파싱, 추천상품 영역 필터링 강화")
        logger.info("=" * 80)
        
        if not self.setup_driver():
            logger.error("네덜란드 드라이버 설정 실패")
            return None
        
        results = []
        failed_urls = []
        
        try:
            for idx, row in enumerate(urls_data):
                logger.info(f"네덜란드 진행률: {idx + 1}/{len(urls_data)} ({(idx + 1)/len(urls_data)*100:.1f}%)")
                
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
                
                # 중간 저장 (10개마다)
                if (idx + 1) % 10 == 0:
                    interim_df = pd.DataFrame(results[-10:])
                    if self.db_engine:
                        try:
                            interim_df.to_sql('amazon_price_crawl_tbl_nl', self.db_engine, 
                                            if_exists='append', index=False)
                            logger.info("네덜란드 중간 저장: 10개 레코드 DB 저장")
                        except Exception as e:
                            logger.error(f"네덜란드 중간 저장 실패: {e}")
                
                # 대기 시간
                if idx < len(urls_data) - 1:
                    wait_time = random.uniform(5, 10)
                    logger.info(f"네덜란드 {wait_time:.1f}초 대기 중...")
                    time.sleep(wait_time)
                    
                    # 20개마다 긴 휴식
                    if (idx + 1) % 20 == 0:
                        logger.info("네덜란드 20개 처리 완료, 30초 휴식...")
                        time.sleep(30)
        
        except Exception as e:
            logger.error(f"네덜란드 스크래핑 중 오류: {e}")
        
        finally:
            if failed_urls:
                logger.warning(f"네덜란드 문제 발생한 URL {len(failed_urls)}개:")
                for fail in failed_urls[:5]:
                    logger.warning(f"  - {fail['brand']} {fail['item']}: {fail.get('reason', '알 수 없음')}")
                if len(failed_urls) > 5:
                    logger.warning(f"  ... 외 {len(failed_urls) - 5}개")
            
            if self.driver:
                self.driver.quit()
                logger.info("네덜란드 드라이버 종료")
        
        return pd.DataFrame(results)
    
    def analyze_results(self, df):
        """네덜란드 결과 분석"""
        logger.info("=" * 80)
        logger.info("네덜란드 결과 분석")
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
                # 네덜란드 가격 숫자 변환
                price_df['price_numeric'] = price_df['retailprice'].astype(str).str.replace(',', '').astype(float)
                
                logger.info("네덜란드 가격 통계 (€):")
                logger.info(f"   평균가: €{price_df['price_numeric'].mean():.2f}")
                logger.info(f"   최저가: €{price_df['price_numeric'].min():.2f}")
                logger.info(f"   최고가: €{price_df['price_numeric'].max():.2f}")
                logger.info(f"   중간값: €{price_df['price_numeric'].median():.2f}")
            except Exception as e:
                logger.warning(f"네덜란드 가격 통계 계산 오류: {e}")
            
            # 브랜드별 성공률
            brand_stats = df.groupby('brand').agg({
                'retailprice': ['count', lambda x: x.notna().sum()]
            })
            brand_stats.columns = ['total', 'success']
            brand_stats['success_rate'] = (brand_stats['success'] / brand_stats['total'] * 100).round(1)
            
            logger.info("네덜란드 브랜드별 성공률:")
            for brand, row in brand_stats.iterrows():
                logger.info(f"   {brand}: {row['success_rate']:.1f}% ({row['success']}/{row['total']})")

def main():
    """네덜란드 메인 실행 함수"""
    test_mode = os.getenv('TEST_MODE', 'false').lower() == 'true'
    max_items = int(os.getenv('MAX_ITEMS', '0')) or None
    
    print("=" * 80)
    print("Amazon 네덜란드 크롤러 v2.0 - 추천상품/관련상품 영역 필터링 강화판")
    print("=" * 80)
    print("국가: 네덜란드 (IT)")
    print(f"모드: {'테스트' if test_mode else '전체 크롤링'}")
    print("특화 기능:")
    print("  - 네덜란드 가격 형식 지원 (61,84 → 61.84)")
    print("  - 네덜란드 언어 지원 (Venduto da, Spedito da)")
    print("  - 'Continua lo shopping' 버튼 자동 처리")
    print("  - 추천상품/관련상품 영역 강화된 필터링")
    if max_items:
        print(f"최대 처리 수: {max_items}개")
    else:
        print("전체 제품 크롤링")
    print("=" * 80)
    
    scraper = AmazonNLScraper()
    
    if scraper.db_engine is None:
        logger.error("DB 연결 실패로 종료합니다.")
        return
    
    if test_mode:
        logger.info("네덜란드 테스트 모드 실행 중...")
        
        # 네덜란드 테스트 URL들
        test_urls = [
            'https://www.amazon.nl/dp/B08N5WRWNW',
            'https://www.amazon.nl/dp/B087DFFJRD?th=1',
            'https://www.amazon.nl/dp/B09QV692XY?th=1',
        ]
        
        test_data = []
        for i, url in enumerate(test_urls):
            test_data.append({
                'url': url,
                'brand': 'Samsung',
                'item': f'Test Item IT {i+1}',
                'retailerid': f'TESTIT00{i+1}',
                'retailersku': f'TESTIT00{i+1}',
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
    
    logger.info("네덜란드 전체 크롤링 시작")
    urls_data = scraper.get_crawl_targets(limit=max_items)
    
    if not urls_data:
        logger.warning("네덜란드 크롤링 대상이 없습니다.")
        return
    
    logger.info(f"네덜란드 크롤링 대상: {len(urls_data)}개")
    
    results_df = scraper.scrape_urls(urls_data, max_items)
    
    if results_df is None or results_df.empty:
        logger.error("네덜란드 크롤링 결과가 없습니다.")
        return
    
    scraper.analyze_results(results_df)
    
    save_results = scraper.save_results(
        results_df,
        save_db=True,
        upload_server=True
    )
    
    logger.info("=" * 80)
    logger.info("네덜란드 저장 결과")
    logger.info("=" * 80)
    logger.info(f"DB 저장: {'성공' if save_results['db_saved'] else '실패'}")
    logger.info(f"파일서버 업로드: {'성공' if save_results['server_uploaded'] else '실패'}")
    
    logger.info("=" * 80)
    logger.info("네덜란드 크롤링 프로세스 완료!")
    logger.info("=" * 80)

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
    print("export TEST_MODE=false  # 테스트 모드")
    print("export MAX_ITEMS=10     # 최대 처리 개수 (선택사항)")
    print()
    
    main()
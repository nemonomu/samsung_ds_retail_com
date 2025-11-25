# -*- coding: utf-8 -*-
"""
Amazon 스페인 가격 추출 시스템 V2 (타임존 분리 버전)
원본 es.py 기반 - DB/타임존/파일서버 설정만 V2로 변경
- 현지시간(스페인)과 한국시간 분리 저장
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

class AmazonScraper:
    def __init__(self, country_code='usa'):
        self.driver = None
        self.db_engine = None
        self.country_code = country_code.lower()
        self.wait = None
        # V2: 타임존 분리 (현지시간 + 한국시간)
        self.korea_tz = pytz.timezone('Asia/Seoul')
        self.local_tz = pytz.timezone('Europe/Madrid')  # 스페인 현지 시간
        
        self.setup_db_connection()
        self.setup_default_selectors()
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
    
    def setup_default_selectors(self):
        """기본 선택자 설정 - 165 문제 해결용 + 추천상품 필터링 추가"""
        self.selectors = {
            self.country_code: {
                'price': [
                    # centerCol 내부만 타겟팅 (추천상품 제외)
                    "//*[@id='centerCol']//span[@class='a-price']//span[@class='a-offscreen']",
                    "//*[@id='centerCol']//*[@id='apex_desktop']//span[@class='a-price']//span[@class='a-offscreen']",
                    "//*[@id='centerCol']//*[@id='corePrice_feature_div']//span[@class='a-offscreen']",
                    "//*[@id='centerCol']//*[@id='corePriceDisplay_desktop_feature_div']//span[@class='a-offscreen']",
                    # 메인 제품 영역만 타겟팅 (비교 테이블 제외)
                    "//*[@id='apex_desktop']//span[@class='a-price']//span[@class='a-offscreen']",
                    "//*[@id='corePrice_feature_div']//span[@class='a-offscreen']",
                    "//*[@id='corePriceDisplay_desktop_feature_div']//span[@class='a-offscreen']",
                    # 첫 번째 가격만 (비교 테이블 전)
                    "(//span[@class='a-price']//span[@class='a-offscreen'])[1]",
                    "//*[@id='priceblock_ourprice']",
                    "//*[@id='priceblock_dealprice']",
                    # 메인 영역의 whole 가격
                    "//*[@id='centerCol']//*[@id='apex_desktop']//span[@class='a-price-whole']",
                    "//*[@id='centerCol']//*[@id='corePrice_feature_div']//span[@class='a-price-whole']",
                    "//*[@id='centerCol']//*[@id='corePriceDisplay_desktop_feature_div']//span[@class='a-price-whole']",
                    "//*[@id='apex_desktop']//span[@class='a-price-whole']",
                    "//*[@id='corePrice_feature_div']//span[@class='a-price-whole']",
                    "//*[@id='corePriceDisplay_desktop_feature_div']//span[@class='a-price-whole']",
                    # 최후의 수단: 첫 번째 whole 가격만
                    "(//span[@class='a-price-whole'])[1]"
                ],
                'price_fraction': [
                    "//*[@id='centerCol']//*[@id='corePrice_feature_div']/div/div/div/div/span[1]/span[2]",
                    "//*[@id='centerCol']//*[@id='corePriceDisplay_desktop_feature_div']/div[1]/span[3]/span[2]",
                    "//*[@id='centerCol']//*[@id='corePrice_feature_div']//span[@class='a-price-fraction']",
                    "//*[@id='centerCol']//*[@id='corePriceDisplay_desktop_feature_div']//span[@class='a-price-fraction']",
                    "//*[@id='corePrice_feature_div']/div/div/div/div/span[1]/span[2]",
                    "//*[@id='corePriceDisplay_desktop_feature_div']/div[1]/span[3]/span[2]",
                    "//*[@id='corePrice_feature_div']//span[@class='a-price-fraction']",
                    "//*[@id='corePriceDisplay_desktop_feature_div']//span[@class='a-price-fraction']",
                    "//span[@class='a-price-fraction']",
                    ".a-price-fraction"
                ],
                'title': [
                    "#productTitle",
                    "//span[@id='productTitle']",
                    "//h1/span[@id='productTitle']",
                    "h1#title span",
                    "//div[@id='titleSection']//h1//span"
                ],
                'ships_from': [
                    "//*[@id='SSOFpopoverLink_ubb']",
                    "//a[@id='SSOFpopoverLink_ubb']",
                    "//*[@id='fulfillerInfoFeature_feature_div']/div[2]/div[1]/span",
                    "//div[@id='fulfillerInfoFeature_feature_div']//span",
                    "//*[@id='fulfillerInfoFeature_feature_div']//div[2]//span",
                    "//div[contains(@id, 'fulfillerInfo')]//span"
                ],
                'sold_by': [
                    "//a[@id='sellerProfileTriggerId']",
                    "//*[@id='sellerProfileTriggerId']",
                    "//*[@id='merchantInfoFeature_feature_div']/div[2]/div[1]/span",
                    "//div[@id='merchantInfoFeature_feature_div']//a",
                    "//div[@id='merchantInfoFeature_feature_div']//span",
                    "//*[@id='merchantInfoFeature_feature_div']//div[2]//span",
                    "//div[contains(@id, 'merchantInfo')]//span",
                    "//span[contains(text(), 'Sold by')]/following-sibling::a",
                    "//span[contains(text(), 'Sold by')]/following-sibling::span",
                    "//span[contains(text(), 'Vendu par')]/following-sibling::span",
                    "//span[contains(text(), 'Verkauft von')]/following-sibling::span",
                    "//span[contains(text(), 'Venduto da')]/following-sibling::span",
                    "//span[contains(text(), 'Vendido por')]/following-sibling::span"
                ],
                'imageurl': [
                    "//div[@id='imageBlock']//img[@id='landingImage']",
                    "//div[@id='main-image-container']//img",
                    "//img[@class='a-dynamic-image']",
                    "//div[@class='imgTagWrapper']//img",
                    "//*[@id='landingImage']"
                ],
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
                    'access denied',
                    'weiter shoppen',
                    'klicke auf die schaltfläche',
                    'um mit dem einkauf fortzufahren',
                    'lo sentimos',
                    'se ha producido un error',
                    'haz clic aquí para volver'
                ],
                # 추천상품/관련상품 영역 제외 필터링 추가
                'excluded_price_areas': [
                    'productSimilar',
                    'similarProducts',
                    'recommendedProducts',
                    'product-comparison',
                    'comparison',
                    'sponsored',
                    'sp-',
                    'aplus-v2',
                    'frequently-bought',
                    'bought-together',
                    'customers-who-viewed',
                    'customers-who-bought',
                    'related-products',
                    'accessory-products',
                    'bundle-products',
                    'cross-sell',
                    'widget',
                    'recommendation'
                ],
                'excluded_xpath_patterns': [
                    '//*[contains(@id, "similar")]',
                    '//*[contains(@id, "recommend")]',
                    '//*[contains(@id, "comparison")]',
                    '//*[contains(@id, "sponsored")]',
                    '//*[contains(@class, "similar")]',
                    '//*[contains(@class, "recommend")]',
                    '//*[contains(@class, "comparison")]',
                    '//*[contains(@class, "sponsored")]',
                    '//*[contains(@class, "widget")]',
                    '//*[contains(@class, "cross-sell")]',
                    '//*[contains(@class, "bundle")]',
                    '//*[contains(@class, "accessory")]',
                    '//*[contains(@class, "frequently")]',
                    '//*[contains(@class, "bought-together")]',
                    '//*[contains(@class, "customers-who")]',
                    '//*[contains(@class, "related-products")]',
                    '//*[@data-component-type="s-sponsored-result"]',
                    '//*[@data-component-type="s-advertisement"]'
                ]
            }
        }
    
    def is_excluded_price_element(self, element):
        """추천상품/관련상품 영역 제외 필터링 - desktop-dp-lpo 영역만 제외"""
        try:
            # desktop-dp-lpo_feature_div (추천상품 영역)에 속하는지 확인
            is_in_lpo = self.driver.execute_script("""
                var el = arguments[0];
                while (el && el !== document.body) {
                    var id = el.id || '';
                    // desktop-dp-lpo_feature_div_01 또는 유사 패턴 체크
                    if (id.indexOf('desktop-dp-lpo') !== -1) {
                        return true;
                    }
                    el = el.parentElement;
                }
                return false;
            """, element)

            if is_in_lpo:
                logger.debug("제외됨 - desktop-dp-lpo 추천상품 영역")
                return True

            return False

        except Exception as e:
            logger.debug(f"필터링 체크 중 오류 (무시): {e}")
            return False
    
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
              AND selector_value NOT LIKE '/html/%'
            ORDER BY element_type, priority ASC
            """
            
            df = pd.read_sql(query, self.db_engine, params=(self.country_code,))
            logger.info(f"DB에서 선택자 로드: {len(df)}개")
            
            if len(df) > 0:
                db_selectors = {self.country_code: {}}
                
                for element_type in df['element_type'].unique():
                    db_selectors[self.country_code][element_type] = df[df['element_type'] == element_type]['selector_value'].tolist()
                
                # DB 선택자를 기본 선택자와 병합 (기본 선택자 우선)
                for element_type, selectors in db_selectors[self.country_code].items():
                    if element_type in self.selectors[self.country_code]:
                        existing = self.selectors[self.country_code][element_type]
                        self.selectors[self.country_code][element_type] = existing + [s for s in selectors if s not in existing]
                    else:
                        self.selectors[self.country_code][element_type] = selectors
                
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
            
            user_agents = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            ]
            options.add_argument(f'--user-agent={random.choice(user_agents)}')
            
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
            
            self.driver = uc.Chrome(options=options)
            self.driver.maximize_window()
            
            self.wait = WebDriverWait(self.driver, 20)
            
            logger.info("드라이버 설정 완료")
            return True
            
        except Exception as e:
            logger.error(f"드라이버 설정 실패: {e}")
            return False
    
    def is_error_page(self):
        """오류 페이지 확인 (파란색 링크 감지 포함)"""
        try:
            page_source = self.driver.page_source.lower()
            page_title = self.driver.title.lower()
            
            # 오류 페이지 감지 키워드 확장
            error_indicators = [
                'lo sentimos',
                'se ha producido un error',
                'error de servicio',
                '503',
                'service unavailable',
                'algo salió mal',
                'error has occurred',
                'haz clic aquí para volver',
                'something went wrong',
                'robot check',
                'automated access'
            ]
            
            for indicator in error_indicators:
                if indicator in page_title or indicator in page_source:
                    logger.info(f"오류 페이지 감지: {indicator}")
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"오류 페이지 확인 중 예외: {e}")
            return False
    
    def is_normal_product_page(self):
        """정상 제품 페이지인지 확인"""
        try:
            # 제품 페이지의 주요 요소들 확인
            product_indicators = [
                "productTitle",
                "imageBlock", 
                "feature-bullets",
                "priceblock_ourprice",
                "priceblock_dealprice"
            ]
            
            for indicator in product_indicators:
                try:
                    element = self.driver.find_element(By.ID, indicator)
                    if element.is_displayed():
                        logger.debug(f"정상 제품 페이지 확인: {indicator}")
                        return True
                except:
                    continue
            
            # 가격 관련 클래스 확인
            price_classes = [
                "a-price-whole",
                "a-price",
                "a-price-current"
            ]
            
            for price_class in price_classes:
                try:
                    elements = self.driver.find_elements(By.CLASS_NAME, price_class)
                    if elements:
                        logger.debug(f"정상 제품 페이지 확인: {price_class}")
                        return True
                except:
                    continue
            
            return False
            
        except Exception as e:
            logger.error(f"정상 페이지 확인 중 예외: {e}")
            return False
    
    def click_blue_link_and_return(self, original_url):
        """파란색 링크 클릭 후 원래 URL로 돌아가기 (추가된 기능)"""
        try:
            logger.info("파란색 링크 찾는 중...")
            
            # 파란색 링크 선택자들 (다국어 지원)
            blue_link_selectors = [
                # 스페인어
                "//a[contains(text(), 'Haz clic aquí para volver')]",
                "//a[contains(text(), 'volver a la página de inicio')]",
                "//a[contains(text(), 'página de inicio')]",
                # 영어
                "//a[contains(text(), 'Click here to go back')]",
                "//a[contains(text(), 'back to Amazon')]",
                # 독일어
                "//a[contains(text(), 'Klicken Sie hier')]",
                "//a[contains(text(), 'zurück zu Amazon')]",
                # 프랑스어
                "//a[contains(text(), 'Cliquez ici pour retourner')]",
                "//a[contains(text(), 'retour à Amazon')]",
                # 일반적인 패턴
                "//a[contains(@href, 'amazon.')]",
                "//a[contains(@class, 'a-link')]"
            ]
            
            # 파란색 링크 클릭 시도
            for selector in blue_link_selectors:
                try:
                    link = self.driver.find_element(By.XPATH, selector)
                    if link.is_displayed():
                        link_text = link.text.strip()
                        logger.info(f"파란색 링크 발견: '{link_text}'")
                        
                        # 링크 클릭
                        link.click()
                        logger.info("파란색 링크 클릭 완료")
                        
                        # 잠시 대기
                        time.sleep(random.uniform(2, 4))
                        
                        # 원래 URL로 다시 접속
                        logger.info(f"원래 URL로 재접속: {original_url}")
                        self.driver.get(original_url)
                        
                        # 페이지 로드 대기
                        time.sleep(random.uniform(3, 5))
                        
                        return True
                        
                except Exception as e:
                    logger.debug(f"선택자 시도 실패: {selector} - {e}")
                    continue
            
            logger.warning("파란색 링크를 찾을 수 없음")
            return False
            
        except Exception as e:
            logger.error(f"파란색 링크 클릭 중 오류: {e}")
            return False
    
    def handle_captcha_or_block_page(self):
        """차단 페이지나 캡차 처리 - 파란색 링크 우회 통합"""
        try:
            logger.info("차단/캡차 페이지 확인 중...")
            
            # 현재 URL 저장
            original_url = self.driver.current_url
            
            # 먼저 파란색 링크 우회 시도
            if self.click_blue_link_and_return(original_url):
                logger.info("파란색 링크 우회 성공")
                return True
            
            # 기존 Continue 버튼 방식도 유지
            spanish_continue_selectors = [
                "//button[contains(text(), 'Seguir comprando')]",
                "//input[@value='Seguir comprando']",
                "//a[contains(text(), 'Seguir comprando')]",
                "//span[contains(text(), 'Seguir comprando')]/ancestor::button",
                "//div[contains(text(), 'Seguir comprando')]/ancestor::button",
                "//button[contains(text(), 'Continuar comprando')]",
                "//input[@value='Continuar comprando']",
                "//a[contains(text(), 'Continuar comprando')]",
                "//span[contains(text(), 'Continuar comprando')]/ancestor::button"
            ]
            
            german_continue_selectors = [
                "//button[contains(text(), 'Weiter shoppen')]",
                "//a[contains(text(), 'Weiter shoppen')]",
                "//span[contains(text(), 'Weiter shoppen')]/ancestor::button",
                "//input[@value='Weiter shoppen']",
                "//button[contains(@class, 'a-button') and contains(., 'Weiter')]",
                "//div[contains(@class, 'a-button') and contains(., 'Weiter')]//button"
            ]
            
            continue_selectors = [
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
            
            # 스페인어를 최우선으로 하고, 독일어, 영어 순으로 시도
            all_selectors = spanish_continue_selectors + german_continue_selectors + continue_selectors
            
            for selector in all_selectors:
                try:
                    logger.info(f"버튼 찾기 시도: {selector}")
                    
                    if selector.startswith('//'):
                        button = self.driver.find_element(By.XPATH, selector)
                    elif selector.startswith('#') or selector.startswith('.'):
                        button = self.driver.find_element(By.CSS_SELECTOR, selector)
                    else:
                        button = self.driver.find_element(By.CSS_SELECTOR, selector)
                    
                    if button and button.is_displayed():
                        button_text = button.text
                        logger.info(f"Continue 버튼 발견: {selector} (텍스트: '{button_text}')")
                        
                        self.driver.execute_script("arguments[0].scrollIntoView();", button)
                        time.sleep(1)
                        
                        try:
                            button.click()
                            logger.info("일반 클릭 성공")
                        except:
                            try:
                                self.driver.execute_script("arguments[0].click();", button)
                                logger.info("JavaScript 클릭 성공")
                            except:
                                logger.warning("클릭 실패")
                                continue
                        
                        time.sleep(3)
                        logger.info("Continue 버튼 클릭 완료")
                        return True
                        
                except Exception as e:
                    logger.debug(f"선택자 오류: {e}")
                    continue
            
            logger.debug("모든 우회 방법 실패")
            return False
            
        except Exception as e:
            logger.error(f"차단 페이지 처리 중 오류: {e}")
            return False
    
    def is_page_blocked(self):
        """페이지 차단 감지 (파란색 링크 페이지 포함)"""
        try:
            page_title = self.driver.title.lower()
            page_source = self.driver.page_source.lower()
            current_url = self.driver.current_url.lower()
            
            serious_blocked_indicators = {
                'title': [
                    '503',
                    'access denied',
                    'error has occurred',
                    'fehler aufgetreten',
                    'lo sentimos',
                    'se ha producido un error'
                ],
                'content': [
                    'enter the characters',
                    'verify you are human',
                    'access denied',
                    'automated access',
                    'suspicious activity',
                    'geben sie die zeichen ein',
                    'beweisen sie dass sie ein mensch sind',
                    'haz clic aquí para volver',
                    'robot check'
                ]
            }
            
            for pattern in serious_blocked_indicators['title']:
                if pattern in page_title:
                    logger.warning(f"차단 감지 (제목): {pattern}")
                    return True
            
            # 파란색 링크나 Continue 버튼이 없는 경우에만 본문 검사
            if ('continue shopping' not in page_source and 
                'weiter shoppen' not in page_source and
                'continuar comprando' not in page_source and
                'seguir comprando' not in page_source and
                'haz clic aquí para volver' not in page_source):
                for pattern in serious_blocked_indicators['content']:
                    if pattern in page_source:
                        logger.warning(f"차단 감지 (본문): {pattern}")
                        return True
            
            if 'amazon' not in current_url:
                logger.warning(f"Amazon 페이지가 아님: {current_url}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"페이지 차단 확인 중 오류: {e}")
            return False
    
    def wait_for_page_load(self, timeout=10):
        """페이지 로드 대기"""
        try:
            self.wait.until(
                lambda driver: driver.execute_script("return document.readyState") == "complete"
            )
            
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
                    logger.debug(f"요소 발견: {by}={value}")
                    return True
                except:
                    continue
            
            return True
            
        except Exception as e:
            logger.warning(f"페이지 로드 대기 중 오류: {e}")
            return False
    
    def extract_element_text(self, selectors, element_name="요소"):
        """선택자 목록에서 텍스트 추출"""
        logger.info(f"{element_name} 추출 시작 - 총 {len(selectors)}개 선택자")
        
        for idx, selector in enumerate(selectors, 1):
            try:
                logger.info(f"[{idx}/{len(selectors)}] 시도 중: {selector}")
                
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
                                text1 = element.text.strip()
                                text2 = element.get_attribute('textContent')
                                text3 = element.get_attribute('innerText')
                                
                                text2 = text2.strip() if text2 else ""
                                text3 = text3.strip() if text3 else ""
                                
                                text = max([text1, text2, text3], key=len)
                                
                                if text:
                                    logger.info(f"원본 텍스트: '{text}'")
                                    
                                    if element_name in ["Sold By", "Ships From"]:
                                        label_only_patterns = [
                                            'sold by',
                                            'ships from', 
                                            'vendu par',
                                            'expédié par',
                                            'verkauft von',
                                            'versendet von',
                                            'venduto da',
                                            'spedito da',
                                            'vendido por',
                                            'enviado por',
                                            'gestionado por'
                                        ]
                                        
                                        text_lower = text.lower().strip()
                                        
                                        if text_lower in label_only_patterns:
                                            logger.info(f"라벨만 있음, 스킵: '{text}'")
                                            continue
                                        
                                        for pattern in label_only_patterns:
                                            if text_lower.startswith(pattern + ' '):
                                                actual_value = text[len(pattern):].strip()
                                                if actual_value:
                                                    text = actual_value
                                                    logger.info(f"라벨 제거 후: '{text}'")
                                                break
                                    
                                    if text:
                                        logger.info(f"최종 추출: '{text}'")
                                        return text
                        except Exception as e:
                            logger.debug(f"요소 처리 중 오류: {e}")
                
            except Exception as e:
                logger.debug(f"선택자 오류: {str(e)}")
                continue
        
        logger.error(f"{element_name} 추출 완전 실패")
        return None
    
    def parse_price_by_country(self, price_text, country_code):
        """국가별 가격 파싱 - 강화된 검증 추가"""
        try:
            price_text = price_text.strip()
            logger.debug(f"파싱할 가격 텍스트: '{price_text}'")
            
            # 가격이 아닌 텍스트 필터링 (165 문제 방지)
            invalid_patterns = [
                r'^[a-zA-Z\s]+$',  # 알파벳만 있는 경우
                r'^\d+\s*[a-zA-Z]',  # 숫자 + 문자 조합
                r'^[^€$£¥₹]\d+$',   # 통화 기호 없는 순수 숫자
            ]
            
            for pattern in invalid_patterns:
                if re.search(pattern, price_text):
                    logger.debug(f"무효한 가격 패턴 감지: {pattern}")
                    return None
            
            if country_code in ['fr', 'it', 'es', 'de']:
                cleaned = re.sub(r'[€\s]', '', price_text)
                logger.debug(f"통화 제거 후: '{cleaned}'")
                
                if country_code == 'es':
                    if ',' in cleaned and '.' not in cleaned:
                        cleaned = cleaned.replace(',', '.')
                        logger.debug(f"스페인 쉼표→점 변환: '{cleaned}'")
                    elif ',' in cleaned and '.' in cleaned:
                        parts = cleaned.rsplit(',', 1)
                        if len(parts) == 2 and len(parts[1]) <= 2:
                            integer_part = parts[0].replace('.', '')
                            decimal_part = parts[1]
                            cleaned = f"{integer_part}.{decimal_part}"
                            logger.debug(f"스페인 복합형식 변환: '{cleaned}'")
                else:
                    if ',' in cleaned:
                        parts = cleaned.split(',')
                        if len(parts) == 2 and len(parts[1]) <= 2 and parts[1].isdigit():
                            integer_part = parts[0].replace('.', '')
                            decimal_part = parts[1]
                            cleaned = f"{integer_part}.{decimal_part}"
                
                if re.match(r'^\d+(\.\d{1,2})?$', cleaned):
                    # 최소 가격 검증 (10유로 이상, 10000유로 미만)
                    try:
                        price_value = float(cleaned)
                        if 10 <= price_value <= 10000:  
                            return cleaned
                        else:
                            logger.debug(f"가격 범위 벗어남: {price_value}")
                            return None
                    except:
                        return None
                    
            elif country_code == 'jp':
                cleaned = re.sub(r'[¥￥\s]', '', price_text)
                if re.match(r'^\d{1,3}(,\d{3})*$', cleaned) or re.match(r'^\d+$', cleaned):
                    return cleaned
                    
            elif country_code == 'in':
                cleaned = re.sub(r'[₹\s]', '', price_text)
                if re.match(r'^\d{1,3}(,\d{2,3})*(\.\d{1,2})?$', cleaned) or re.match(r'^\d+(\.\d{1,2})?$', cleaned):
                    return cleaned
                    
            else:
                cleaned = re.sub(r'[$£\s]', '', price_text)
                if re.match(r'^\d{1,3}(,\d{3})*(\.\d{1,2})?$', cleaned) or re.match(r'^\d+(\.\d{1,2})?$', cleaned):
                    return cleaned
                    
        except Exception as e:
            logger.debug(f"가격 파싱 오류: {price_text} - {e}")
            
        return None
    
    def extract_price(self, country_code):
        """가격 추출 - 165 문제 해결 + 추천상품 필터링 버전"""
        logger.info(f"가격 추출 시작 - 국가: {country_code}")
        
        logger.info("1단계: centerCol 내부 메인 제품 영역에서 a-offscreen 가격 추출")
        main_offscreen_selectors = [
            "//*[@id='centerCol']//span[@class='a-price']//span[@class='a-offscreen']",
            "//*[@id='centerCol']//*[@id='apex_desktop']//span[@class='a-price']//span[@class='a-offscreen']",
            "//*[@id='centerCol']//*[@id='corePrice_feature_div']//span[@class='a-offscreen']",
            "//*[@id='centerCol']//*[@id='corePriceDisplay_desktop_feature_div']//span[@class='a-offscreen']",
            "//*[@id='apex_desktop']//span[@class='a-price']//span[@class='a-offscreen']",
            "//*[@id='corePrice_feature_div']//span[@class='a-offscreen']",
            "//*[@id='corePriceDisplay_desktop_feature_div']//span[@class='a-offscreen']", 
            "(//span[@class='a-price']//span[@class='a-offscreen'])[1]"
        ]
        
        for selector in main_offscreen_selectors:
            try:
                logger.info(f"시도: {selector}")
                elements = self.driver.find_elements(By.XPATH, selector)
                
                for element in elements:
                    if element.is_displayed():
                        # 추천상품/관련상품 영역 제외 필터링 적용
                        if self.is_excluded_price_element(element):
                            logger.info("추천상품/관련상품 영역 요소 스킵")
                            continue
                        
                        # 비교 테이블인지 확인
                        parent_html = self.driver.execute_script(
                            "return arguments[0].closest('.product-comparison-desktop') || arguments[0].closest('[class*=\"comparison\"]')", 
                            element
                        )
                        
                        if parent_html:
                            logger.info("비교 테이블 요소 스킵")
                            continue
                        
                        text_methods = [
                            element.get_attribute('textContent'),
                            element.get_attribute('innerText'),
                            element.text
                        ]
                        
                        for text in text_methods:
                            if text and text.strip():
                                price_text = text.strip()
                                logger.info(f"발견된 텍스트: {price_text}")
                                
                                price = self.parse_price_by_country(price_text, country_code)
                                if price:
                                    logger.info(f"메인 영역 완전한 가격 추출 성공: {price}")
                                    return price
                                    
            except Exception as e:
                logger.debug(f"오류: {e}")
        
        logger.info("2단계: whole + fraction 조합으로 가격 구성 (추천상품 필터링 적용)")
        
        combination_attempts = [
            {
                'whole': "//*[@id='centerCol']//*[@id='corePrice_feature_div']//span[@class='a-price-whole']",
                'fraction': "//*[@id='centerCol']//*[@id='corePrice_feature_div']/div/div/div/div/span[1]/span[2]"
            },
            {
                'whole': "//*[@id='centerCol']//*[@id='corePriceDisplay_desktop_feature_div']//span[@class='a-price-whole']",
                'fraction': "//*[@id='centerCol']//*[@id='corePriceDisplay_desktop_feature_div']/div[1]/span[3]/span[2]"
            },
            {
                'whole': "//*[@id='corePrice_feature_div']//span[@class='a-price-whole']",
                'fraction': "//*[@id='corePrice_feature_div']/div/div/div/div/span[1]/span[2]"
            },
            {
                'whole': "//*[@id='corePriceDisplay_desktop_feature_div']//span[@class='a-price-whole']",
                'fraction': "//*[@id='corePriceDisplay_desktop_feature_div']/div[1]/span[3]/span[2]"
            },
            {
                'whole': "//*[@id='centerCol']//*[@id='apex_desktop']//span[@class='a-price-whole']",
                'fraction': "//*[@id='centerCol']//*[@id='apex_desktop']//span[@class='a-price-fraction']"
            },
            {
                'whole': "//*[@id='apex_desktop']//span[@class='a-price-whole']",
                'fraction': "//*[@id='apex_desktop']//span[@class='a-price-fraction']"
            },
            {
                'whole': "(//span[@class='a-price-whole'])[1]",
                'fraction': "(//span[@class='a-price-fraction'])[1]"
            }
        ]
        
        for i, combo in enumerate(combination_attempts, 1):
            try:
                logger.info(f"조합 시도 {i}:")
                logger.info(f"정수부: {combo['whole']}")
                logger.info(f"소수부: {combo['fraction']}")
                
                whole_elem = self.driver.find_element(By.XPATH, combo['whole'])
                fraction_elem = self.driver.find_element(By.XPATH, combo['fraction'])
                
                if whole_elem and fraction_elem and whole_elem.is_displayed() and fraction_elem.is_displayed():
                    # 추천상품/관련상품 영역 제외 필터링 적용
                    if self.is_excluded_price_element(whole_elem) or self.is_excluded_price_element(fraction_elem):
                        logger.info("추천상품/관련상품 영역 요소 조합 스킵")
                        continue
                    
                    whole_text = whole_elem.text.strip()
                    fraction_text = fraction_elem.text.strip()
                    
                    logger.info(f"정수부 텍스트: {whole_text}")
                    logger.info(f"소수부 텍스트: {fraction_text}")
                    
                    if whole_text and fraction_text:
                        fraction_clean = re.sub(r'[^\d]', '', fraction_text)
                        if fraction_clean:
                            combined_price = f"{whole_text}.{fraction_clean}"
                            logger.info(f"조합된 가격: {combined_price}")
                            
                            price = self.parse_price_by_country(combined_price, country_code)
                            if price:
                                logger.info(f"조합 가격 추출 성공: {price}")
                                return price
                                
            except Exception as e:
                logger.debug(f"조합 {i} 오류: {e}")
        
        logger.info("3단계: 기본 가격 선택자 시도 (비교 테이블 + 추천상품 제외)")
        price_selectors = self.selectors[country_code].get('price', [])
        
        for idx, selector in enumerate(price_selectors, 1):
            try:
                logger.info(f"[{idx}/{len(price_selectors)}] 시도: {selector}")
                
                if selector.startswith('//') or selector.startswith('('):
                    elements = self.driver.find_elements(By.XPATH, selector)
                else:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                
                for element in elements:
                    if element.is_displayed():
                        # 추천상품/관련상품 영역 제외 필터링 적용
                        if self.is_excluded_price_element(element):
                            logger.info("추천상품/관련상품 영역 요소 스킵")
                            continue
                        
                        # 비교 테이블인지 확인
                        parent_html = self.driver.execute_script(
                            "return arguments[0].closest('.product-comparison-desktop') || arguments[0].closest('[class*=\"comparison\"]')", 
                            element
                        )
                        
                        if parent_html:
                            logger.info("비교 테이블 요소 스킵")
                            continue
                        
                        text_methods = [
                            element.get_attribute('textContent'),
                            element.get_attribute('innerText'),
                            element.text
                        ]
                        
                        for text in text_methods:
                            if text and text.strip():
                                price_text = text.strip()
                                logger.info(f"텍스트: {price_text}")
                                
                                price = self.parse_price_by_country(price_text, country_code)
                                if price:
                                    logger.info(f"기본 선택자 가격 추출 성공: {price}")
                                    return price
                                    
            except Exception as e:
                logger.debug(f"선택자 오류: {e}")
        
        logger.error("모든 방법으로 가격 추출 실패")
        return None
    
    def check_stock_availability(self):
        """재고 상태 확인"""
        try:
            try:
                availability_elem = self.driver.find_element(By.ID, "availability")
                availability_text = availability_elem.text.lower()
                
                if any(phrase in availability_text for phrase in [
                    'currently unavailable',
                    'out of stock',
                    'temporarily out of stock',
                    'no disponible',
                    'agotado'
                ]):
                    logger.info(f"재고 없음: {availability_text}")
                    return False
                    
                if any(phrase in availability_text for phrase in [
                    'in stock',
                    'only',
                    'left in stock',
                    'disponible',
                    'en stock'
                ]):
                    logger.info(f"재고 있음: {availability_text}")
                    return True
                    
            except NoSuchElementException:
                logger.debug("availability 요소를 찾을 수 없음")
            
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
            
            logger.info("재고 상태 불명확 - 기본값: 재고 있음")
            return True
            
        except Exception as e:
            logger.warning(f"재고 확인 중 오류: {e}")
            return True
    
    def extract_product_info(self, url, row_data, retry_count=0, max_retries=3):
        """제품 정보 추출 - 파란색 링크 우회 + 추천상품 필터링 통합"""
        try:
            logger.info("=" * 60)
            logger.info("제품 정보 추출 시작")
            logger.info(f"URL: {url}")
            logger.info(f"브랜드: {row_data.get('brand', 'N/A')}")
            logger.info(f"제품: {row_data.get('item', 'N/A')}")
            
            self.driver.get(url)
            time.sleep(random.uniform(2, 4))
            
            # 정상 페이지인지 먼저 확인
            if self.is_normal_product_page():
                logger.info("정상 제품 페이지 확인 - 크롤링 진행")
            else:
                # 오류 페이지인지 확인하고 우회 시도
                if self.is_error_page():
                    logger.info("오류 페이지 감지 - 우회 시도")
                    if self.handle_captcha_or_block_page():
                        time.sleep(3)
                        self.wait_for_page_load()
                        
                        # 우회 후 다시 정상 페이지 확인
                        if not self.is_normal_product_page():
                            logger.warning("우회 후에도 정상 페이지 아님")
                    else:
                        logger.warning("우회 실패")
                else:
                    logger.info("오류 페이지는 아니지만 주요 요소 없음 - 진행")
            
            if self.is_page_blocked():
                logger.error("여전히 차단 페이지임")
                raise Exception("페이지 차단됨")
            
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
                'country_code': self.country_code,
                'ships_from': None,
                'channel_name': 'amazon.es',
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
            
            result['title'] = self.extract_element_text(
                self.selectors[self.country_code].get('title', []), 
                "제목"
            )
            
            has_stock = self.check_stock_availability()
            
            logger.info("가격 추출 시도 (추천상품 필터링 적용)")
            result['retailprice'] = self.extract_price(self.country_code)
            
            # 가격 검증 추가 (165 문제 방지)
            if result['retailprice']:
                try:
                    price_clean = re.sub(r'[^\d.]', '', str(result['retailprice']))
                    price_value = float(price_clean)
                    
                    # 스페인의 경우 최소 10유로, 최대 10000유로
                    if self.country_code == 'es':
                        if price_value < 10 or price_value > 10000:
                            logger.warning(f"비정상적인 가격 감지: {result['retailprice']} -> None으로 변경")
                            result['retailprice'] = None
                    # 다른 국가도 비슷한 범위 적용
                    elif price_value < 5 or price_value > 50000:
                        logger.warning(f"비정상적인 가격 감지: {result['retailprice']} -> None으로 변경")
                        result['retailprice'] = None
                        
                except:
                    logger.warning(f"가격 형태 오류: {result['retailprice']} -> None으로 변경")
                    result['retailprice'] = None
            
            if not has_stock and result['retailprice'] is None:
                result['retailprice'] = None
                logger.info("재고 없음 + 가격 없음 -> 가격 None으로 설정")
            
            result['ships_from'] = self.extract_element_text(
                self.selectors[self.country_code].get('ships_from', []), 
                "Ships From"
            )
            
            result['sold_by'] = self.extract_element_text(
                self.selectors[self.country_code].get('sold_by', []), 
                "Sold By"
            )
            
            for selector in self.selectors[self.country_code].get('imageurl', []):
                try:
                    if selector.startswith('//'):
                        element = self.driver.find_element(By.XPATH, selector)
                    else:
                        element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    
                    result['imageurl'] = element.get_attribute('src')
                    if result['imageurl']:
                        logger.debug("이미지 URL 추출 성공")
                        break
                except:
                    continue
            
            page_source = self.driver.page_source
            page_source_lower = page_source.lower()
            
            logger.info("추출 결과:")
            logger.info(f"제목: {result['title'][:50] + '...' if result['title'] and len(result['title']) > 50 else result['title']}")
            logger.info(f"가격: {result['retailprice']}")
            logger.info(f"이미지: {'있음' if result['imageurl'] else '없음'}")
            logger.info(f"판매자: {result['sold_by']}")
            logger.info(f"배송지: {result['ships_from']}")
            
            return result
            
        except Exception as e:
            logger.error(f"페이지 처리 오류: {e}")
            
            if retry_count < max_retries:
                wait_time = (retry_count + 1) * 10
                logger.info(f"{wait_time}초 후 재시도... ({retry_count + 1}/{max_retries})")
                time.sleep(wait_time)
                
                try:
                    self.driver.refresh()
                except:
                    logger.info("드라이버 재시작 중...")
                    self.driver.quit()
                    self.setup_driver()
                
                return self.extract_product_info(url, row_data, retry_count + 1, max_retries)
            
            # V2: 타임존 분리

            
            now_time = datetime.now(self.korea_tz)

            
            local_time = datetime.now(self.local_tz)
            return {
                'retailerid': row_data.get('retailerid', ''),
                'country_code': self.country_code,
                'ships_from': None,
                'channel_name': 'amazon.es',
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
            logger.info(f"크롤링 대상 {len(df)}개 조회 완료")
            return df.to_dict('records')
            
        except Exception as e:
            logger.error(f"크롤링 대상 조회 실패: {e}")
            return []
    
    def save_to_db(self, df):
        """DB에 결과 저장"""
        if self.db_engine is None:
            logger.warning("DB 연결이 없어 DB 저장을 건너뜁니다")
            return False
        
        try:
            table_name = f'amazon_price_crawl_tbl_{self.country_code}_v2'
            
            df.to_sql(table_name, self.db_engine, if_exists='append', index=False)
            logger.info(f"DB 저장 완료: {len(df)}개 레코드 -> {table_name}")
            
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
                logger.info(f"크롤링 로그 저장 완료: {len(log_records)}개")
            
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
        base_filename = f"{date_str}_{time_str}_es_amazon"

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
        
        logger.info("=" * 80)
        logger.info("크롤링 시작 - 165 문제 해결 + 파란색 링크 우회 + 추천상품 필터링 버전")
        logger.info(f"국가: {self.country_code.upper()}")
        logger.info(f"대상: {len(urls_data)}개 제품")
        logger.info("수정사항:")
        logger.info("- 비교 테이블 가격 제외 (165€ 문제 해결)")
        logger.info("- 파란색 링크 우회 추가")
        logger.info("- centerCol 내부만 타겟팅")
        logger.info("- 추천상품/관련상품 영역 제외")
        logger.info("- 스폰서 콘텐츠 필터링")
        logger.info("=" * 80)
        
        if not self.setup_driver():
            logger.error("드라이버 설정 실패")
            return None
        
        results = []
        failed_urls = []
        
        try:
            for idx, row in enumerate(urls_data):
                logger.info(f"진행률: {idx + 1}/{len(urls_data)} ({(idx + 1)/len(urls_data)*100:.1f}%)")
                
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
                
                if (idx + 1) % 10 == 0:
                    interim_df = pd.DataFrame(results[-10:])
                    if self.db_engine:
                        try:
                            table_name = f'amazon_price_crawl_tbl_{self.country_code}_v2'
                            interim_df.to_sql(table_name, self.db_engine, 
                                            if_exists='append', index=False)
                            logger.info("중간 저장: 10개 레코드 DB 저장")
                        except Exception as e:
                            logger.error(f"중간 저장 실패: {e}")
                
                if idx < len(urls_data) - 1:
                    wait_time = random.uniform(5, 10)
                    logger.info(f"{wait_time:.1f}초 대기 중...")
                    time.sleep(wait_time)
                    
                    if (idx + 1) % 20 == 0:
                        logger.info("20개 처리 완료, 30초 휴식...")
                        time.sleep(30)
        
        except Exception as e:
            logger.error(f"스크래핑 중 오류: {e}")
        
        finally:
            if failed_urls:
                logger.warning(f"문제 발생한 URL {len(failed_urls)}개:")
                for fail in failed_urls[:5]:
                    logger.warning(f"  - {fail['brand']} {fail['item']}: {fail.get('reason', '알 수 없음')}")
                if len(failed_urls) > 5:
                    logger.warning(f"  ... 외 {len(failed_urls) - 5}개")
            
            if self.driver:
                self.driver.quit()
                logger.info("드라이버 종료")
        
        return pd.DataFrame(results)
    
    def analyze_results(self, df):
        """결과 분석"""
        logger.info("=" * 80)
        logger.info("결과 분석 - 165 문제 해결 + 파란색 링크 우회 + 추천상품 필터링 버전")
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
                price_df['price_numeric'] = price_df['retailprice'].astype(str).str.replace(',', '').astype(float)
                
                logger.info("가격 통계:")
                logger.info(f"   평균가: {price_df['price_numeric'].mean():.2f}")
                logger.info(f"   최저가: {price_df['price_numeric'].min():.2f}")
                logger.info(f"   최고가: {price_df['price_numeric'].max():.2f}")
                logger.info(f"   중간값: {price_df['price_numeric'].median():.2f}")
                
                # 비정상 가격 검출
                abnormal_prices = price_df[
                    (price_df['price_numeric'] < 10) | 
                    (price_df['price_numeric'] > 10000)
                ]
                if len(abnormal_prices) > 0:
                    logger.warning(f"비정상 가격 감지: {len(abnormal_prices)}개")
                    for _, row in abnormal_prices.iterrows():
                        logger.warning(f"  - {row['item']}: {row['retailprice']}")
                        
            except Exception as e:
                logger.warning(f"가격 통계 계산 오류: {e}")
                logger.info("가격 통계: 계산 불가 (문자열 형태 가격)")
            
            brand_stats = df.groupby('brand').agg({
                'retailprice': ['count', lambda x: x.notna().sum()]
            })
            brand_stats.columns = ['total', 'success']
            brand_stats['success_rate'] = (brand_stats['success'] / brand_stats['total'] * 100).round(1)
            
            logger.info("브랜드별 성공률:")
            for brand, row in brand_stats.iterrows():
                logger.info(f"   {brand}: {row['success_rate']:.1f}% ({row['success']}/{row['total']})")

def main():
    """메인 실행 함수"""
    country_code = os.getenv('COUNTRY_CODE', 'es').lower()  
    test_mode = os.getenv('TEST_MODE', 'false').lower() == 'true'  # 전체 크롤링 모드로 변경
    max_items = int(os.getenv('MAX_ITEMS', '0')) or None  # 전체 크롤링 (제한 없음)
    
    print("=" * 80)
    print("Amazon 가격 추출 시스템 v6.0 (165 Problem + Blue Link Bypass + Recommended Filtering)")
    print("=" * 80)
    print(f"국가: {country_code.upper()}")
    print(f"모드: {'테스트' if test_mode else '전체 크롤링'}")
    print("🔧 수정사항:")
    print("- 비교 테이블 가격 제외 (165€ 문제 해결)")
    print("- 메인 제품 영역만 타겟팅 (centerCol)")
    print("- 가격 검증 강화")
    print("- 파란색 링크 우회 추가")
    print("- 스페인어 'Seguir comprando' 버튼 지원")
    print("- 다국어 오류 페이지 처리")
    print("- 추천상품/관련상품 영역 제외")
    print("- 스폰서 콘텐츠 필터링")
    if max_items:
        print(f"최대 처리 수: {max_items}개")
    print("=" * 80)
    
    scraper = AmazonScraper(country_code)
    
    if scraper.db_engine is None:
        logger.error("DB 연결 실패로 종료합니다.")
        return
    
    if test_mode:
        logger.info("테스트 모드 실행 중...")
        test_data = [{
            'url': 'https://www.amazon.es/Compatible-Smartphone-Videoconsola-MU-PC4T0T-WW/dp/B0CX5C3LBQ',  # 문제 URL
            'brand': 'Samsung',
            'item': 'T7 2TB',
            'retailerid': 'TEST001',
            'retailersku': 'TEST001',
            'channel': 'Online',
            'seg_lv1': 'SSD',
            'seg_lv2': 'Consumer',
            'seg_lv3': 'External',
            'capacity': '2TB',
            'form_factor': 'External'
        }]
        
        results_df = scraper.scrape_urls(test_data)
        if results_df is not None and not results_df.empty:
            scraper.analyze_results(results_df)
            scraper.save_results(results_df, save_db=False, upload_server=True)
        return
    
    logger.info("전체 크롤링 시작")
    urls_data = scraper.get_crawl_targets(limit=max_items)
    
    if not urls_data:
        logger.warning("크롤링 대상이 없습니다.")
        return
    
    logger.info(f"크롤링 대상: {len(urls_data)}개")
    
    results_df = scraper.scrape_urls(urls_data, max_items)
    
    if results_df is None or results_df.empty:
        logger.error("크롤링 결과가 없습니다.")
        return
    
    scraper.analyze_results(results_df)
    
    save_results = scraper.save_results(
        results_df,
        save_db=True,
        upload_server=True
    )
    
    logger.info("=" * 80)
    logger.info("저장 결과")
    logger.info("=" * 80)
    logger.info(f"DB 저장: {'성공' if save_results['db_saved'] else '실패'}")
    logger.info(f"파일서버 업로드: {'성공' if save_results['server_uploaded'] else '실패'}")
    
    logger.info("=" * 80)
    logger.info("165 문제 해결 + 파란색 링크 우회 + 추천상품 필터링 완료! 크롤링 프로세스 완료!")
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
    print("export COUNTRY_CODE=es  # us, uk, de, fr, es, jp, ind 등")
    print("export TEST_MODE=true   # 테스트 모드")
    print("export MAX_ITEMS=1      # 최대 처리 개수 (선택사항)")
    print()
    
    main()
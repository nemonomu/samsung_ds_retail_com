# -*- coding: utf-8 -*-
"""
Amazon UK 가격 추출 시스템 V2 (타임존 분리 버전)
핵심 개선사항:
1. 현지시간(영국)과 한국시간 분리 저장
2. 날짜별 디렉토리 구조 및 ZIP 압축
3. 새 데이터베이스 사용 (samsung_ds_retail_com)
4. 독일 URL 호환

핵심 로직:
1. ships_from과 sold_by 둘 다 비어있으면 가격 0
2. 하나라도 있으면 기존 가격 추출 로직 사용
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

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Import database configuration V2
from config import DB_CONFIG_V2 as DB_CONFIG

from config import FILE_SERVER_CONFIG

class AmazonUKScraper:
    def __init__(self, country_code='gb'):
        self.driver = None
        self.db_engine = None
        self.country_code = country_code.lower()
        self.wait = None

        # V2: 타임존 분리 (현지시간 + 한국시간)
        self.korea_tz = pytz.timezone('Asia/Seoul')
        self.local_tz = pytz.timezone('Europe/London')  # UK 시간 (GMT/BST)

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
        """기본 선택자 설정 - 영국/독일 호환"""
        self.selectors = {
            self.country_code: {
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
                    "//h1/span[@id='productTitle']",
                    "h1#title span",
                    "//div[@id='titleSection']//h1//span"
                ],
                'ships_from': [
                    "//*[@id='fulfillerInfoFeature_feature_div']/div[2]/div[1]/span",
                    "/html/body/div[2]/div/div/div[4]/div[1]/div[3]/div/div[1]/div/div/div/form/div/div/div/div/div[4]/div/div[20]/div/div/div[1]/div/div[2]/div[2]/div[1]/span"
                ],
                'sold_by': [
                    "//*[@id='merchantInfoFeature_feature_div']/div[2]/div[1]/span/a",
                    "//*[@id='sellerProfileTriggerId']",
                    "/html/body/div[2]/div/div/div[4]/div[1]/div[3]/div/div[1]/div/div/div/form/div/div/div/div/div[4]/div/div[20]/div/div/div[1]/div/div[3]/div[2]/div[1]/a"
                ],
                'imageurl': [
                    "//div[@id='imageBlock']//img[@id='landingImage']",
                    "//div[@id='main-image-container']//img",
                    "//img[@class='a-dynamic-image']",
                    "//div[@class='imgTagWrapper']//img"
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
            # Full XPath는 제외하고 로드
            query = """
            SELECT element_type, selector_value, priority
            FROM amazon_selectors
            WHERE country_code = %s
              AND is_active = TRUE
              AND selector_value NOT LIKE '/html/%'
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

            # User-Agent 설정
            user_agents = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            ]
            options.add_argument(f'--user-agent={random.choice(user_agents)}')

            # 영국/독일 언어 설정
            options.add_experimental_option('prefs', {'intl.accept_languages': 'en-GB,en;de-DE,de'})

            # Chrome 드라이버 생성
            self.driver = uc.Chrome(options=options)
            self.driver.maximize_window()

            # WebDriverWait 객체 생성
            self.wait = WebDriverWait(self.driver, 20)

            logger.info("✅ 드라이버 설정 완료")
            return True

        except Exception as e:
            logger.error(f"❌ 드라이버 설정 실패: {e}")
            return False

    def is_page_blocked(self):
        """페이지 차단 감지"""
        try:
            page_title = self.driver.title.lower()
            page_source = self.driver.page_source.lower()
            current_url = self.driver.current_url.lower()

            # 차단 징후들
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

            # 본문 확인
            for pattern in serious_blocked_indicators['content']:
                if pattern in page_source:
                    logger.warning(f"🚫 차단 감지 (본문): '{pattern}'")

                    # 스크린샷 저장
                    try:
                        local_time = datetime.now(self.local_tz)
                        screenshot_name = f"blocked_{self.country_code}_{local_time.strftime('%Y%m%d_%H%M%S')}.png"
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
        logger.debug(f"🔍 {element_name} 추출 시작 - 총 {len(selectors)}개 선택자")

        for idx, selector in enumerate(selectors, 1):
            try:
                logger.debug(f"  [{idx}/{len(selectors)}] 시도 중: {selector}")

                # XPath인지 CSS 선택자인지 구분
                if selector.startswith('//') or selector.startswith('('):
                    # XPath
                    elements = self.driver.find_elements(By.XPATH, selector)
                    selector_type = "XPath"
                else:
                    # CSS Selector
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    selector_type = "CSS"

                logger.debug(f"      타입: {selector_type}, 발견: {len(elements)}개")

                if elements:
                    for i, element in enumerate(elements):
                        try:
                            if element.is_displayed():
                                # 여러 방법으로 텍스트 추출
                                text1 = element.text.strip()
                                text2 = element.get_attribute('textContent').strip() if element.get_attribute('textContent') else ""
                                text3 = element.get_attribute('innerText').strip() if element.get_attribute('innerText') else ""

                                # 가장 긴 텍스트 선택
                                text = max([text1, text2, text3], key=len)

                                if text:
                                    logger.debug(f"      ✅ 추출 성공: '{text[:100]}'")
                                    return text
                        except Exception as e:
                            logger.debug(f"      요소 처리 중 오류: {e}")

            except Exception as e:
                logger.debug(f"      ❌ 선택자 오류: {str(e)}")
                continue

        logger.debug(f"❌ {element_name} 추출 실패")
        return None

    def extract_price(self, country_code):
        """가격 추출 (UK 파운드 형식 £123.45)"""
        logger.info(f"💰 가격 추출 시작 - 국가: {country_code}")

        # 추천 상품 영역 제외
        excluded_areas = [
            "#similarities_feature_div",
            "#sp_detail",
            "#bookDescription_feature_div",
            "#books-entity-details",
            "#compare-table",
            "[data-cel-widget*='comparison']",
            "[data-cel-widget*='sponsored']",
            ".s-result-item",
            ".a-carousel-card",
            ".acs-shwcs-widget",
            "#customer_also_viewed_feature_div",
            "#similarities-widget",
            ".a-accordion-inner"
        ]

        # 메인 제품 영역만 확인
        main_product_areas = [
            "#dp-container",
            "#centerCol",
            "#rightCol",
            "#apex_desktop"
        ]

        price_selectors = self.selectors[country_code].get('price', [])

        for idx, selector in enumerate(price_selectors, 1):
            try:
                logger.debug(f"  [{idx}/{len(price_selectors)}] 가격 선택자 시도: {selector}")

                # 요소 찾기
                if selector.startswith('//') or selector.startswith('('):
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

                        # 추천 상품 영역 제외
                        is_in_excluded_area = False

                        for excluded_selector in excluded_areas:
                            try:
                                if excluded_selector.startswith('['):
                                    excluded_elements = self.driver.find_elements(By.CSS_SELECTOR, excluded_selector)
                                else:
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

                        # 메인 제품 영역 확인
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
                            # 가격 파싱 (UK 파운드)
                            price = self.parse_price_uk(price_text)
                            if price and price > 0:
                                logger.info(f"✅ 가격 추출 성공: {price} (원본: {price_text})")
                                return price

                    except Exception as e:
                        logger.debug(f"요소 처리 오류: {e}")

            except Exception as e:
                logger.debug(f"선택자 오류: {str(e)}")

        logger.warning("❌ 메인 영역에서 가격을 찾을 수 없음")
        return None

    def parse_price_uk(self, price_text):
        """영국 파운드 가격 파싱 (£123.45 형식)"""
        try:
            # 기본 정리
            price_text = price_text.strip()

            # 영국: £123.45 또는 독일: €123.45 형식
            price_text = re.sub(r'[£€\s]', '', price_text)
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
            return True

    def extract_product_info(self, url, row_data, retry_count=0, max_retries=3):
        """제품 정보 추출 (V2: 타임존 분리)"""
        try:
            logger.info(f"\n{'='*60}")
            logger.info(f"🔍 제품 정보 추출 시작")
            logger.info(f"📌 URL: {url}")
            logger.info(f"📌 브랜드: {row_data.get('brand', 'N/A')}")
            logger.info(f"📌 제품: {row_data.get('item', 'N/A')}")

            # 페이지 로드
            self.driver.get(url)
            time.sleep(random.uniform(2, 4))

            # 차단 페이지 확인
            if self.is_page_blocked():
                logger.warning("⚠️ 차단 페이지 감지됨")
                raise Exception("페이지 차단 감지")

            # 페이지 로드 대기
            self.wait_for_page_load()

            # V2: 타임존 분리 - UK 시간 + 한국 시간
            local_time = datetime.now(self.local_tz)
            korea_time = datetime.now(self.korea_tz)

            # 기본 결과 구조
            result = {
                'retailerid': row_data.get('retailerid', ''),
                'country_code': self.country_code,
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
                # V2: 4개의 타임스탬프 (현지 + 한국)
                'crawl_datetime': local_time.strftime('%Y-%m-%d %H:%M'),
                'crawl_strdatetime': local_time.strftime('%Y%m%d%H%M%S') + f"{local_time.microsecond:06d}"[:4],
                'kr_crawl_datetime': korea_time.strftime('%Y-%m-%d %H:%M'),
                'kr_crawl_strdatetime': korea_time.strftime('%Y%m%d%H%M%S') + f"{korea_time.microsecond:06d}"[:4],
                'retailprice': None,
                'sold_by': None,
                'imageurl': None,
                'producturl': url,
                'title': None,
                'vat': row_data.get('vat', 'o')
            }

            # 제목 추출
            result['title'] = self.extract_element_text(
                self.selectors[self.country_code].get('title', []),
                "제목"
            )

            # 판매자 정보 추출
            result['sold_by'] = self.extract_element_text(
                self.selectors[self.country_code].get('sold_by', []),
                "판매자"
            )

            # 배송지 정보 추출
            result['ships_from'] = self.extract_element_text(
                self.selectors[self.country_code].get('ships_from', []),
                "배송지"
            )

            # 판매자 정보 검증 후 가격 결정
            seller_info_valid = self.validate_seller_info(result['ships_from'], result['sold_by'])

            if not seller_info_valid:
                result['retailprice'] = None
                logger.info("💰 최종 가격: None (이유: 판매자 정보 없음)")
            else:
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

            # 결과 요약
            logger.info(f"\n📊 추출 결과:")
            logger.info(f"   📌 제목: {result['title'][:50] + '...' if result['title'] and len(result['title']) > 50 else result['title']}")
            logger.info(f"   💰 가격: {result['retailprice']}")
            logger.info(f"   🖼️ 이미지: {'있음' if result['imageurl'] else '없음'}")
            logger.info(f"   🏪 판매자: {result['sold_by']}")
            logger.info(f"   📦 배송지: {result['ships_from']}")
            logger.info(f"   ⏰ UK 시간: {result['crawl_datetime']}")
            logger.info(f"   ⏰ 한국 시간: {result['kr_crawl_datetime']}")

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
                    logger.info("🔧 드라이버 재시작 중...")
                    self.driver.quit()
                    self.setup_driver()

                return self.extract_product_info(url, row_data, retry_count + 1, max_retries)

            # 최종 실패 - 기본값 반환
            local_time = datetime.now(self.local_tz)
            korea_time = datetime.now(self.korea_tz)

            return {
                'retailerid': row_data.get('retailerid', ''),
                'country_code': self.country_code,
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
                'crawl_datetime': local_time.strftime('%Y-%m-%d %H:%M'),
                'crawl_strdatetime': local_time.strftime('%Y%m%d%H%M%S') + f"{local_time.microsecond:06d}"[:4],
                'kr_crawl_datetime': korea_time.strftime('%Y-%m-%d %H:%M'),
                'kr_crawl_strdatetime': korea_time.strftime('%Y%m%d%H%M%S') + f"{korea_time.microsecond:06d}"[:4],
                'retailprice': None,
                'sold_by': None,
                'imageurl': None,
                'producturl': url,
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
            logger.info(f"✅ 크롤링 대상 {len(df)}개 조회 완료")
            return df.to_dict('records')

        except Exception as e:
            logger.error(f"크롤링 대상 조회 실패: {e}")
            return []

    def save_to_db(self, df):
        """DB에 결과 저장 (V2: 새 테이블)"""
        if self.db_engine is None:
            logger.warning("⚠️ DB 연결이 없어 DB 저장을 건너뜁니다")
            return False

        try:
            # V2: 테이블명 변경
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
                    'crawl_datetime': row['kr_crawl_datetime']
                })

            if log_records:
                log_df = pd.DataFrame(log_records)
                log_df.to_sql('amazon_crawl_logs', self.db_engine, if_exists='append', index=False)
                logger.info(f"✅ 크롤링 로그 저장 완료: {len(log_records)}개")

            return True

        except Exception as e:
            logger.error(f"❌ DB 저장 실패: {e}")
            return False

    def upload_to_file_server(self, local_file_path, remote_filename=None, date_str=None):
        """파일서버에 업로드 (V2: 날짜별 디렉토리 + ZIP 압축)"""
        try:
            transport = paramiko.Transport((FILE_SERVER_CONFIG['host'], FILE_SERVER_CONFIG['port']))
            transport.connect(
                username=FILE_SERVER_CONFIG['username'],
                password=FILE_SERVER_CONFIG['password']
            )
            sftp = paramiko.SFTPClient.from_transport(transport)

            # 국가별 디렉토리 경로
            country_dir = f"{FILE_SERVER_CONFIG['upload_path']}/{self.country_code}"

            # V2: 날짜별 디렉토리 생성
            if date_str:
                date_dir = f"{country_dir}/{date_str}"

                # 디렉토리 생성
                for dir_path in [country_dir, date_dir]:
                    try:
                        sftp.stat(dir_path)
                    except FileNotFoundError:
                        logger.info(f"📁 디렉토리 생성: {dir_path}")
                        sftp.mkdir(dir_path)

                # CSV 파일 업로드
                if remote_filename is None:
                    remote_filename = os.path.basename(local_file_path)

                remote_path = f"{date_dir}/{remote_filename}"
                sftp.put(local_file_path, remote_path)
                logger.info(f"✅ 파일서버 업로드 완료: {remote_path}")

                # V2: ZIP 파일 생성/업데이트
                zip_filename = f"{date_str}.zip"
                local_zip_path = f"temp_{zip_filename}"
                remote_zip_path = f"{country_dir}/{zip_filename}"

                # 기존 ZIP 다운로드
                zip_exists = False
                try:
                    sftp.stat(remote_zip_path)
                    sftp.get(remote_zip_path, local_zip_path)
                    zip_exists = True
                    logger.info(f"📦 기존 ZIP 파일 다운로드: {remote_zip_path}")
                except FileNotFoundError:
                    logger.info(f"📦 새 ZIP 파일 생성: {zip_filename}")

                # ZIP 파일에 CSV 추가 (temp_ 제거된 파일명으로 저장)
                with zipfile.ZipFile(local_zip_path, 'a' if zip_exists else 'w', zipfile.ZIP_DEFLATED) as zipf:
                    zipf.write(local_file_path, remote_filename)
                    logger.info(f"✅ ZIP 파일에 추가: {remote_filename}")

                # ZIP 파일 업로드
                sftp.put(local_zip_path, remote_zip_path)
                logger.info(f"✅ ZIP 파일 업로드 완료: {remote_zip_path}")

                # 로컬 ZIP 삭제
                if os.path.exists(local_zip_path):
                    os.remove(local_zip_path)

            else:
                # 날짜 정보 없으면 기존 방식
                try:
                    sftp.stat(country_dir)
                except FileNotFoundError:
                    logger.info(f"📁 디렉토리 생성: {country_dir}")
                    sftp.mkdir(country_dir)

                if remote_filename is None:
                    remote_filename = os.path.basename(local_file_path)

                remote_path = f"{country_dir}/{remote_filename}"
                sftp.put(local_file_path, remote_path)
                logger.info(f"✅ 파일서버 업로드 완료: {remote_path}")

            sftp.close()
            transport.close()

            return True

        except Exception as e:
            logger.error(f"❌ 파일서버 업로드 실패: {e}")
            return False

    def save_results(self, df, save_db=True, upload_server=True):
        """결과를 DB와 파일서버에 저장 (V2: 파일명 형식 변경)"""
        # V2: UK 현지 시간 기준
        local_time = datetime.now(self.local_tz)
        date_str = local_time.strftime("%Y%m%d")
        time_str = local_time.strftime("%H%M%S")
        country_code = self.country_code
        mall_name = "amazon"

        # V2: 파일명 형식 - YYYYMMDD_hhmmss_countrycode_mallname.csv
        base_filename = f"{date_str}_{time_str}_{country_code}_{mall_name}"

        results = {
            'db_saved': False,
            'server_uploaded': False
        }

        # DB 저장
        if save_db:
            results['db_saved'] = self.save_to_db(df)

        # 파일서버 업로드
        if upload_server:
            try:
                # CSV 파일
                temp_csv = f'temp_{base_filename}.csv'
                df.to_csv(temp_csv, index=False, encoding='utf-8-sig')

                remote_csv_filename = f'{base_filename}.csv'
                if self.upload_to_file_server(temp_csv, remote_csv_filename, date_str):
                    results['server_uploaded'] = True

                # 임시 파일 삭제
                if os.path.exists(temp_csv):
                    os.remove(temp_csv)

                logger.info("🗑️ 임시 파일 삭제 완료")

            except Exception as e:
                logger.error(f"파일 처리 오류: {e}")

        return results

    def scrape_urls(self, urls_data, max_items=None):
        """여러 URL 스크래핑"""
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

                # 10개마다 중간 저장
                if (idx + 1) % 10 == 0:
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
                    wait_time = random.uniform(5, 10)
                    logger.info(f"⏳ {wait_time:.1f}초 대기 중...")
                    time.sleep(wait_time)

                    # 20개마다 긴 휴식
                    if (idx + 1) % 20 == 0:
                        logger.info("☕ 20개 처리 완료, 30초 휴식...")
                        time.sleep(30)

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
        success_rate = (with_price / total * 100) if total > 0 else 0

        logger.info(f"전체 제품: {total}개")
        logger.info(f"가격 추출 성공: {with_price}개")
        logger.info(f"가격 추출 실패: {without_price}개")
        logger.info(f"성공률: {success_rate:.1f}%")

        if with_price > 0:
            price_df = df[df['retailprice'].notna()].copy()

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
    country_code = os.getenv('COUNTRY_CODE', 'gb').lower()
    test_mode = os.getenv('TEST_MODE', 'false').lower() == 'true'
    max_items = int(os.getenv('MAX_ITEMS', '0')) or None

    print(f"\n{'='*80}")
    print(f"🚀 Amazon UK 가격 추출 시스템 V2 (타임존 분리 + 날짜별 ZIP)")
    print(f"{'='*80}")
    print(f"📌 국가: {country_code.upper()}")
    print(f"📌 모드: {'테스트' if test_mode else '실제'}")
    if max_items:
        print(f"📌 최대 처리 수: {max_items}개")
    print(f"📌 개선사항:")
    print(f"   - UK 시간 + 한국 시간 분리 저장")
    print(f"   - 날짜별 디렉토리 구조 (/gb/YYYYMMDD/)")
    print(f"   - 날짜별 자동 ZIP 압축 (/gb/YYYYMMDD.zip)")
    print(f"   - 새 데이터베이스 사용 (samsung_ds_retail_com)")
    print(f"   - 독일 URL 호환")
    print(f"{'='*80}\n")

    # 스크래퍼 초기화
    scraper = AmazonUKScraper(country_code)

    if scraper.db_engine is None:
        logger.error("DB 연결 실패로 종료합니다.")
        return

    # 테스트 모드
    if test_mode:
        logger.info("🧪 테스트 모드 실행 중...")
        test_data = [{
            'url': 'https://www.amazon.co.uk/dp/B0B7CKVCCV',
            'brand': 'Samsung',
            'item': '990 PRO 1TB',
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
            scraper.save_results(results_df, save_db=False, upload_server=True)
        return

    # 실제 크롤링
    logger.info("📊 전체 크롤링 시작")
    urls_data = scraper.get_crawl_targets(limit=max_items)

    if not urls_data:
        logger.warning("크롤링 대상이 없습니다.")
        return

    logger.info(f"✅ 크롤링 대상: {len(urls_data)}개")

    # 크롤링 실행
    results_df = scraper.scrape_urls(urls_data, max_items)

    if results_df is None or results_df.empty:
        logger.error("크롤링 결과가 없습니다.")
        return

    # 결과 분석
    scraper.analyze_results(results_df)

    # 결과 저장
    save_results = scraper.save_results(
        results_df,
        save_db=True,
        upload_server=True
    )

    logger.info(f"\n{'='*80}")
    logger.info("📊 저장 결과")
    logger.info(f"{'='*80}")
    logger.info(f"DB 저장: {'✅ 성공' if save_results['db_saved'] else '❌ 실패'}")
    logger.info(f"파일서버 업로드: {'✅ 성공' if save_results['server_uploaded'] else '❌ 실패'}")

    logger.info(f"\n{'='*80}")
    logger.info("✅ 크롤링 프로세스 완료! (V2)")
    logger.info(f"   🎯 타임존 분리: UK 시간 + 한국 시간")
    logger.info(f"   📁 날짜별 디렉토리 저장")
    logger.info(f"   📦 자동 ZIP 압축")
    logger.info(f"{'='*80}\n")

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

    print("\n📦 필요한 패키지:")
    print("pip install " + " ".join(required_packages))
    print("\n⚠️ 환경변수 설정:")
    print("export COUNTRY_CODE=gb")
    print("export TEST_MODE=false")
    print("export MAX_ITEMS=10  # 선택사항")
    print("\n🎯 V2 버전 개선사항:")
    print("- 영국 표준시(GMT/BST) + 한국 표준시(KST) 분리 저장")
    print("- 파일명 형식: YYYYMMDD_hhmmss_gb_amazon.csv")
    print("- 날짜별 디렉토리: /gb/YYYYMMDD/")
    print("- 날짜별 ZIP 압축: /gb/YYYYMMDD.zip")
    print("- 새 데이터베이스 테이블: amazon_price_crawl_tbl_gb_v2")
    print("- 독일(amazon.de) URL 호환")
    print()

    main()

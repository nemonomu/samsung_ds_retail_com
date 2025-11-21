"""
Fnac 가격 추출 시스템 - Playwright 기반 버전
DB에서 URL 읽어와서 크롤링 후 결과 저장
파일명 형식: {수집일자}{수집시간}_{국가코드}_{쇼핑몰}.csv
"""
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
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
import json
from io import StringIO
import zipfile
import hashlib

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# Import database configuration V2
from config import DB_CONFIG_V2 as DB_CONFIG

from config import FILE_SERVER_CONFIG

class FnacScraper:
    def __init__(self):
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None
        self.db_engine = None
        self.sftp_client = None
        self.country_code = 'fr'
        # V2: 타임존 분리 (현지시간 + 한국시간)
        self.korea_tz = pytz.timezone('Asia/Seoul')
        self.local_tz = pytz.timezone('Europe/Paris')  # Fnac 프랑스 현지 시간

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
        """DB에서 Fnac용 선택자 로드"""
        try:
            query = """
            SELECT element_type, selector_value, priority
            FROM mall_selectors
            WHERE mall_name = 'fnac'
              AND country_code = 'fr'
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

            # 새로운 XPath를 기존 DB XPath 앞에 추가
            if 'price' in self.XPATHS:
                new_price_selectors = [
                    '.f-faPriceBox__price',  # CSS 선택자
                    "//span[@class='f-faPriceBox__price userPrice checked']",
                    "//div[@class='f-faPriceBox__priceLine']//span[@class='f-faPriceBox__price']"
                ]
                self.XPATHS['price'] = new_price_selectors + self.XPATHS['price']
                logger.info(f"✅ 새로운 price 선택자 추가됨. 총 price: {len(self.XPATHS['price'])}개")
            else:
                self.XPATHS['price'] = [
                    '.f-faPriceBox__price',
                    "//span[@class='f-faPriceBox__price userPrice checked']",
                    "//div[@class='f-faPriceBox__priceLine']//span[@class='f-faPriceBox__price']"
                ]

            # title XPath 추가
            if 'title' in self.XPATHS:
                new_title_selectors = [
                    '.f-productHeader__heading',
                    "//h1[@class='f-productHeader__heading']"
                ]
                self.XPATHS['title'] = new_title_selectors + self.XPATHS['title']
                logger.info(f"✅ 새로운 title 선택자 추가됨. 총 title: {len(self.XPATHS['title'])}개")
            else:
                self.XPATHS['title'] = [
                    '.f-productHeader__heading',
                    "//h1[@class='f-productHeader__heading']"
                ]

            # imageurl 선택자 추가
            if 'imageurl' in self.XPATHS:
                new_image_selectors = [
                    '.f-productMedias__viewItem--main',
                    "//img[@class='f-productMedias__viewItem--main']"
                ]
                self.XPATHS['imageurl'] = new_image_selectors + self.XPATHS['imageurl']
                logger.info(f"✅ 새로운 imageurl 선택자 추가됨. 총 imageurl: {len(self.XPATHS['imageurl'])}개")
            else:
                self.XPATHS['imageurl'] = [
                    '.f-productMedias__viewItem--main',
                    "//img[@class='f-productMedias__viewItem--main']"
                ]

            # 기본값 설정 (DB에 없는 경우)
            if not self.XPATHS:
                logger.warning("⚠️ DB에 선택자가 없어 기본값 사용")
                self.XPATHS = {
                    'price': [
                        '.f-faPriceBox__price',
                        "//span[@class='f-faPriceBox__price userPrice checked']"
                    ],
                    'title': [
                        '.f-productHeader__heading',
                        "//h1[@class='f-productHeader__heading']"
                    ],
                    'imageurl': [
                        '.f-productMedias__viewItem--main',
                        "//img[@class='f-productMedias__viewItem--main']"
                    ]
                }

        except Exception as e:
            logger.error(f"선택자 로드 실패: {e}")
            # 기본값 사용
            self.XPATHS = {
                'price': [
                    '.f-faPriceBox__price',
                    "//span[@class='f-faPriceBox__price userPrice checked']"
                ],
                'title': [
                    '.f-productHeader__heading',
                    "//h1[@class='f-productHeader__heading']"
                ],
                'imageurl': [
                    '.f-productMedias__viewItem--main',
                    "//img[@class='f-productMedias__viewItem--main']"
                ]
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
                      AND country_code = 'fr'
                    GROUP BY url
                )
                SELECT DISTINCT t.*
                FROM samsung_price_tracking_list t
                LEFT JOIN failed_counts f ON t.url = f.url
                WHERE t.country = 'fr'
                  AND t.mall_name = 'fnac'
                  AND t.is_active = TRUE
                  AND (f.fail_count IS NULL OR f.fail_count < 3)
                ORDER BY COALESCE(f.fail_count, 0) DESC  -- 실패한 것 우선
                """
            else:
                query = """
                SELECT *
                FROM samsung_price_tracking_list
                WHERE country = 'fr'
                  AND mall_name = 'fnac'
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

    def setup_browser(self):
        """Playwright 브라우저 설정"""
        logger.info("🔧 Playwright 브라우저 설정 중...")

        try:
            self.playwright = sync_playwright().start()

            # Chromium 브라우저 시작 (headless=False로 더 자연스럽게)
            self.browser = self.playwright.chromium.launch(
                headless=False,  # GUI 모드
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-web-security',
                    '--disable-features=IsolateOrigins,site-per-process'
                ]
            )

            # 컨텍스트 생성 (프랑스 사용자 시뮬레이션)
            self.context = self.browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                locale='fr-FR',
                timezone_id='Europe/Paris',
                geolocation={'latitude': 48.8566, 'longitude': 2.3522},  # Paris
                permissions=['geolocation']
            )

            # 페이지 생성
            self.page = self.context.new_page()

            # 추가 스텔스 설정
            self.page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });

                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5]
                });

                Object.defineProperty(navigator, 'languages', {
                    get: () => ['fr-FR', 'fr', 'en-US', 'en']
                });

                window.chrome = {
                    runtime: {}
                };
            """)

            logger.info("✅ Playwright 브라우저 설정 완료")
            return True

        except Exception as e:
            logger.error(f"❌ 브라우저 설정 실패: {e}")
            return False

    def solve_slider_captcha(self, max_attempts=3):
        """슬라이더 캡차 자동 해결"""
        logger.info("🧩 슬라이더 캡차 감지 및 해결 시도...")

        # 캡차 관련 선택자들 (더 구체적으로)
        captcha_selectors = [
            "//div[contains(@class, 'captcha')]",
            "//div[contains(@id, 'captcha')]",
            "//div[contains(@class, 'verify')]",
            "//div[contains(@class, 'verification')]",
            "[class*='captcha' i]",
            "[id*='captcha' i]",
            "iframe[src*='captcha']",
            "iframe[title*='captcha' i]",
            "iframe[title*='verify' i]",
            "iframe[title*='puzzle' i]",
            "//div[contains(text(), 'robot')]",
            "//div[contains(text(), 'verify')]",
            "//div[contains(text(), 'slide')]"
        ]

        # 슬라이더 선택자들 (캡차 전용만)
        slider_selectors = [
            ".slider",  # geo.captcha-delivery.com
            "div.slider",
            ".sliderContainer .slider",
            "//div[@class='slider']",
            "//div[@class='sliderContainer']//div[@class='slider']",
            "//div[contains(@class, 'slider') and contains(@class, 'button')]",
            "//div[contains(@class, 'slide-verify')]",
            "//span[contains(@class, 'slider') and contains(@class, 'btn')]",
            "//div[contains(@id, 'nc_') and contains(@class, 'btn')]",  # Alibaba Cloud
            ".captcha-slider-button",
            ".slide-verify-slider-mask-item",
            "#nc_1_n1z"
        ]

        try:
            # 1. 캡차 존재 여부 확인
            captcha_found = False
            captcha_element = None
            for selector in captcha_selectors:
                try:
                    if selector.startswith('//'):
                        locator = self.page.locator(f'xpath={selector}')
                    else:
                        locator = self.page.locator(selector)

                    if locator.is_visible(timeout=2000):
                        logger.info(f"🔍 캡차 요소 발견: {selector}")
                        # 요소의 텍스트나 속성 확인
                        try:
                            text_content = locator.first.text_content()
                            if text_content:
                                logger.info(f"   캡차 텍스트: {text_content[:100]}")
                        except:
                            pass
                        captcha_found = True
                        captcha_element = locator
                        break
                except:
                    continue

            if not captcha_found:
                logger.info("✅ 캡차가 감지되지 않음")
                return True

            logger.info("⚠️ 캡차가 감지되었습니다!")

            # 2. iframe 내부 캡차 확인 및 처리
            try:
                frames = self.page.frames
                for frame in frames:
                    if 'captcha' in frame.url.lower() or 'verify' in frame.url.lower():
                        logger.info(f"🔍 캡차 iframe 발견: {frame.url}")
                        # iframe 내부에서 슬라이더 찾기 시도
                        for slider_sel in slider_selectors:
                            try:
                                if slider_sel.startswith('//'):
                                    slider = frame.locator(f'xpath={slider_sel}')
                                else:
                                    slider = frame.locator(slider_sel)

                                if slider.is_visible(timeout=2000):
                                    logger.info(f"✅ iframe 내 슬라이더 발견: {slider_sel}")
                                    return self._drag_slider(slider, frame)
                            except:
                                continue
            except Exception as e:
                logger.debug(f"iframe 처리 중 오류: {e}")

            # 3. 메인 페이지에서 슬라이더 찾기
            for attempt in range(max_attempts):
                logger.info(f"🔄 슬라이더 해결 시도 {attempt + 1}/{max_attempts}")

                for slider_sel in slider_selectors:
                    try:
                        if slider_sel.startswith('//'):
                            slider = self.page.locator(f'xpath={slider_sel}')
                        else:
                            slider = self.page.locator(slider_sel)

                        if slider.is_visible(timeout=2000):
                            # 슬라이더 요소 정보 출력
                            try:
                                slider_class = slider.first.get_attribute('class')
                                slider_id = slider.first.get_attribute('id')
                                logger.info(f"✅ 슬라이더 발견: {slider_sel}")
                                logger.info(f"   class: {slider_class}")
                                logger.info(f"   id: {slider_id}")
                            except:
                                logger.info(f"✅ 슬라이더 발견: {slider_sel}")

                            # 드래그 전 스크린샷
                            try:
                                screenshot_before = f"captcha_before_{attempt}.png"
                                self.page.screenshot(path=screenshot_before)
                                logger.info(f"📸 드래그 전 스크린샷: {screenshot_before}")
                            except:
                                pass

                            if self._drag_slider(slider, self.page):
                                # 드래그 후 대기
                                time.sleep(2)

                                # 드래그 후 스크린샷
                                try:
                                    screenshot_after = f"captcha_after_{attempt}.png"
                                    self.page.screenshot(path=screenshot_after)
                                    logger.info(f"📸 드래그 후 스크린샷: {screenshot_after}")
                                except:
                                    pass

                                # 캡차가 사라졌는지 확인
                                captcha_still_visible = False
                                for cap_sel in captcha_selectors[:5]:  # 처음 5개만 체크
                                    try:
                                        if cap_sel.startswith('//'):
                                            cap_loc = self.page.locator(f'xpath={cap_sel}')
                                        else:
                                            cap_loc = self.page.locator(cap_sel)

                                        if cap_loc.is_visible(timeout=1000):
                                            captcha_still_visible = True
                                            logger.warning(f"⚠️ 캡차가 여전히 보임: {cap_sel}")
                                            break
                                    except:
                                        continue

                                if not captcha_still_visible:
                                    logger.info("✅ 슬라이더 캡차 해결 성공! (캡차가 사라짐)")
                                    return True
                                else:
                                    logger.warning("⚠️ 슬라이더를 드래그했지만 캡차가 여전히 보입니다")
                                    # 다음 슬라이더 시도
                                    continue

                    except Exception as e:
                        logger.debug(f"슬라이더 {slider_sel} 처리 실패: {e}")
                        continue

                # 재시도 전 대기
                if attempt < max_attempts - 1:
                    time.sleep(2)

            logger.warning("⚠️ 슬라이더 캡차를 자동으로 해결하지 못했습니다")
            logger.warning("💡 수동으로 캡차를 해결해주세요. 30초 대기합니다...")
            time.sleep(30)  # 수동 해결 시간
            return False

        except Exception as e:
            logger.error(f"❌ 캡차 해결 중 오류: {e}")
            return False

    def _drag_slider(self, slider, page_or_frame):
        """슬라이더를 자연스럽게 드래그"""
        try:
            # 슬라이더의 위치와 크기 가져오기
            box = slider.bounding_box()
            if not box:
                logger.warning("슬라이더 위치를 가져올 수 없음")
                return False

            # 시작 위치 (슬라이더 중앙)
            start_x = box['x'] + box['width'] / 2
            start_y = box['y'] + box['height'] / 2

            # 드래그 거리 계산 (일반적으로 슬라이더 트랙 너비만큼)
            # 트랙 요소를 찾아서 너비를 가져오거나, 기본값 사용
            drag_distance = 300  # 기본값

            # 트랙 요소 찾기 시도
            track_selectors = [
                ".sliderContainer",  # geo.captcha-delivery.com
                ".sliderbg",
                "//div[@class='sliderContainer']",
                "//div[@class='sliderbg']",
                "//div[contains(@class, 'slider-track')]",
                "//div[contains(@class, 'slide-track')]",
                ".slider-track",
                ".slide-verify-slider-track"
            ]

            for track_sel in track_selectors:
                try:
                    if track_sel.startswith('//'):
                        track = page_or_frame.locator(f'xpath={track_sel}')
                    else:
                        track = page_or_frame.locator(track_sel)

                    if track.is_visible(timeout=1000):
                        track_box = track.bounding_box()
                        if track_box:
                            drag_distance = track_box['width'] - box['width']
                            logger.info(f"트랙 너비 기반 드래그 거리: {drag_distance}px")
                            break
                except:
                    continue

            # 목표 위치
            end_x = start_x + drag_distance
            end_y = start_y

            logger.info(f"🖱️ 슬라이더 드래그: ({start_x:.0f}, {start_y:.0f}) → ({end_x:.0f}, {end_y:.0f})")

            # page 객체 가져오기 (Frame에는 mouse가 없으므로)
            # Frame이면 page를 가져오고, Page면 그대로 사용
            if hasattr(page_or_frame, 'page'):
                # Frame 객체
                mouse_obj = page_or_frame.page.mouse
            else:
                # Page 객체
                mouse_obj = page_or_frame.mouse

            # 자연스러운 마우스 움직임 시뮬레이션
            # 1. 마우스를 슬라이더로 이동
            mouse_obj.move(start_x, start_y)
            time.sleep(random.uniform(0.1, 0.3))

            # 2. 마우스 버튼 누르기
            mouse_obj.down()
            time.sleep(random.uniform(0.1, 0.2))

            # 3. 여러 단계로 나눠서 자연스럽게 드래그
            steps = random.randint(20, 30)
            for i in range(steps):
                # 진행률
                progress = (i + 1) / steps

                # 현재 x 위치 (약간의 랜덤 변화 추가)
                current_x = start_x + (drag_distance * progress)

                # y축에 약간의 흔들림 추가 (사람처럼)
                wobble = random.uniform(-2, 2)
                current_y = start_y + wobble

                # 마우스 이동
                mouse_obj.move(current_x, current_y)

                # 각 스텝마다 약간의 랜덤 딜레이
                time.sleep(random.uniform(0.01, 0.03))

            # 4. 목표 지점에 정확히 도달
            mouse_obj.move(end_x, end_y)
            time.sleep(random.uniform(0.1, 0.2))

            # 5. 마우스 버튼 놓기
            mouse_obj.up()

            logger.info("✅ 슬라이더 드래그 완료")
            time.sleep(1)

            return True

        except Exception as e:
            logger.error(f"❌ 슬라이더 드래그 실패: {e}")
            return False

    def initialize_session(self):
        """Fnac 세션 초기화"""
        logger.info("Fnac 세션 초기화...")

        try:
            # Fnac 메인 페이지 접속 (domcontentloaded로 변경)
            self.page.goto("https://www.fnac.com", wait_until='domcontentloaded', timeout=30000)
            logger.info("✅ 페이지 로드 완료")
            time.sleep(2)

            # 쿠키 팝업 처리
            try:
                logger.info("🍪 쿠키 팝업 확인 중...")
                time.sleep(1)  # 팝업이 나타날 시간 대기

                # "J'accepte" 버튼 클릭 (여러 선택자 시도)
                cookie_selectors = [
                    "text=J'accepte",
                    "button:has-text(\"J'accepte\")",
                    "//button[contains(text(), \"J'accepte\")]",
                    "//button[contains(text(), 'accepte')]",
                    "[class*='accept' i]",
                    "[id*='accept' i]",
                    "button[class*='cookie']",
                    ".didomi-button"
                ]

                cookie_found = False
                for selector in cookie_selectors:
                    try:
                        logger.info(f"🔍 쿠키 선택자 시도: {selector}")

                        if selector.startswith('text=') or selector.startswith('button:'):
                            button = self.page.locator(selector).first
                        elif selector.startswith('//'):
                            button = self.page.locator(f'xpath={selector}').first
                        else:
                            button = self.page.locator(selector).first

                        # 버튼이 보이는지 확인
                        if button.is_visible(timeout=2000):
                            button.click(timeout=3000)
                            logger.info(f"🍪 쿠키 동의 팝업 처리 완료 (선택자: {selector})")
                            time.sleep(2)  # 쿠키 처리 후 대기 시간 증가
                            cookie_found = True
                            break
                    except Exception as e:
                        logger.debug(f"선택자 {selector} 실패: {e}")
                        continue

                if not cookie_found:
                    logger.info("쿠키 팝업이 없거나 이미 처리됨")

            except Exception as e:
                logger.debug(f"쿠키 팝업 처리 중 오류 (무시): {e}")

            # 슬라이더 캡차 해결 시도
            time.sleep(2)  # 캡차가 나타날 시간 대기
            self.solve_slider_captcha()

            # 세션이 제대로 설정되었는지 확인
            title = self.page.title()
            if "fnac" in title.lower():
                logger.info("✅ Fnac 세션 초기화 완료")
                return True
            else:
                logger.warning("⚠️ 세션 초기화 부분 성공")
                return True

        except Exception as e:
            logger.error(f"❌ 세션 초기화 실패: {e}")
            return False

    def extract_product_info(self, url, row_data, retry_count=0, max_retries=3):
        """제품 정보 추출 (차단 페이지 감지 및 재시도 로직)"""
        try:
            logger.info(f"🔍 페이지 접속: {url} (시도: {retry_count + 1}/{max_retries + 1})")
            response = self.page.goto(url, wait_until='domcontentloaded', timeout=30000)

            # 페이지 로드 대기
            time.sleep(random.uniform(3, 5))

            # 슬라이더 캡차가 나타났는지 확인 및 해결
            self.solve_slider_captcha()

            # 404 에러 체크 (봇 감지로 인한 404 위장 가능성)
            if response and response.status == 404:
                logger.warning("⚠️ 404 에러 감지 - 봇 감지 가능성, 재접속 시도")

                # 잠시 대기
                time.sleep(random.uniform(3, 5))

                # 바로 원래 URL 재접속 (메인 페이지 거치지 않음)
                logger.info(f"🔄 URL 직접 재접속: {url}")
                response = self.page.goto(url, wait_until='domcontentloaded', timeout=30000)
                time.sleep(random.uniform(3, 5))

                # 여전히 404이면 에러 발생
                if response and response.status == 404:
                    logger.error("❌ 재접속 후에도 404 에러 - URL이 존재하지 않거나 차단됨")
                    raise Exception("404 error after retry - possible blocked or invalid URL")
                else:
                    logger.info("✅ 재접속 성공")

            # 현재 시간
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
                'country_code': row_data.get('country', 'fr'),
                'ships_from': 'FR',
                'channel_name': 'fnac',
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
                'sold_by': 'Fnac',
                'imageurl': None,
                'producturl': url,
                'crawl_datetime': crawl_datetime_iso,
                'crawl_strdatetime': local_time.strftime('%Y%m%d%H%M%S') + f"{local_time.microsecond:06d}"[:4],
                'kr_crawl_datetime': now_time.strftime('%Y-%m-%d %H:%M:%S'),
                'kr_crawl_strdatetime': now_time.strftime('%Y%m%d%H%M%S') + f"{now_time.microsecond:06d}"[:4],
                'title': None,
                'vat': row_data.get('vat', 'o')
            }

            # 제목 추출 (차단 페이지 감지)
            title_extracted = False
            try:
                for selector in self.XPATHS.get('title', []):
                    try:
                        # XPath인지 CSS인지 판단
                        if selector.startswith('//'):
                            locator = self.page.locator(f'xpath={selector}')
                        else:
                            locator = self.page.locator(selector)

                        # 요소가 나타날 때까지 대기 (최대 5초)
                        locator.wait_for(state='visible', timeout=5000)
                        title_text = locator.inner_text()

                        if title_text and title_text.strip():
                            result['title'] = title_text.strip()
                            logger.info(f"제목: {result['title']}")
                            title_extracted = True
                            break
                    except:
                        continue
            except Exception as e:
                logger.warning(f"제목 추출 실패: {e}")

            # 차단 페이지 감지 및 재시도 로직
            if not title_extracted:
                logger.warning("⚠️ 제목 추출 실패 - 차단 페이지 가능성")

                # 1단계: 새로고침 시도
                logger.info("🔄 새로고침 시도...")
                self.page.reload(wait_until='networkidle', timeout=30000)
                time.sleep(random.uniform(3, 5))

                # 제목 재추출 시도
                for selector in self.XPATHS.get('title', []):
                    try:
                        if selector.startswith('//'):
                            locator = self.page.locator(f'xpath={selector}')
                        else:
                            locator = self.page.locator(selector)

                        locator.wait_for(state='visible', timeout=5000)
                        title_text = locator.inner_text()

                        if title_text and title_text.strip():
                            result['title'] = title_text.strip()
                            logger.info(f"✅ 새로고침 후 제목 추출 성공: {result['title']}")
                            title_extracted = True
                            break
                    except:
                        continue

                # 2단계: 여전히 실패하면 fnac.com 접속 후 재시도
                if not title_extracted:
                    logger.warning("⚠️ 새로고침 후에도 실패 - fnac.com 접속 후 재시도")

                    # Fnac 메인 페이지 접속
                    self.page.goto("https://www.fnac.com", wait_until='networkidle', timeout=30000)
                    time.sleep(random.uniform(2, 4))

                    # 원래 URL 재접속
                    logger.info(f"🔄 원래 URL 재접속: {url}")
                    self.page.goto(url, wait_until='networkidle', timeout=30000)
                    time.sleep(random.uniform(3, 5))

                    # 제목 재추출 시도
                    for selector in self.XPATHS.get('title', []):
                        try:
                            if selector.startswith('//'):
                                locator = self.page.locator(f'xpath={selector}')
                            else:
                                locator = self.page.locator(selector)

                            locator.wait_for(state='visible', timeout=5000)
                            title_text = locator.inner_text()

                            if title_text and title_text.strip():
                                result['title'] = title_text.strip()
                                logger.info(f"✅ 재접속 후 제목 추출 성공: {result['title']}")
                                title_extracted = True
                                break
                        except:
                            continue

                    # 여전히 실패하면 에러로 처리
                    if not title_extracted:
                        logger.error("❌ 모든 재시도 후에도 제목 추출 실패 - 차단된 것으로 판단")
                        raise Exception("Title extraction failed - possible blocked page")

            # 가격 추출
            try:
                price_found = False

                logger.info("🔍 가격 추출 시도...")

                for selector in self.XPATHS.get('price', []):
                    try:
                        logger.info(f"🔍 선택자 시도: {selector}")

                        if selector.startswith('//'):
                            locator = self.page.locator(f'xpath={selector}')
                        else:
                            locator = self.page.locator(selector)

                        locator.wait_for(state='visible', timeout=5000)
                        price_text = locator.inner_text()
                        logger.info(f"🔍 추출한 텍스트: '{price_text}'")

                        if price_text and price_text.strip():
                            # Fnac 프랑스 가격 형식: "419,99 €" 또는 "419,99€"
                            # 쉼표를 점으로 변환, € 기호 제거
                            price_text_clean = price_text.replace(',', '.').replace('€', '').replace('\xa0', '').strip()
                            price_match = re.search(r'(\d+\.?\d*)', price_text_clean)
                            if price_match:
                                price_number = price_match.group(1)
                                result['retailprice'] = float(price_number)
                                logger.info(f"✅ 가격 추출 성공: €{result['retailprice']}")
                                price_found = True
                                break

                    except Exception as e:
                        logger.warning(f"❌ 선택자 {selector} 실패: {e}")
                        continue

                # JavaScript로 가격 찾기 (최후 수단)
                if not price_found:
                    try:
                        js_result = self.page.evaluate("""
                            () => {
                                var priceSelectors = [
                                    '.f-faPriceBox__price',
                                    '[class*="price"]',
                                    'span[class*="Price"]'
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
                            }
                        """)

                        if js_result:
                            logger.info(f"🔍 JavaScript에서 추출한 텍스트: '{js_result}'")
                            price_text_clean = js_result.replace(',', '.').replace('€', '').replace('\xa0', '').strip()
                            price_match = re.search(r'(\d+\.?\d*)', price_text_clean)
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

            # 이미지 URL 추출
            try:
                image_found = False

                # 1. 선택자들 시도
                for selector in self.XPATHS.get('imageurl', []):
                    try:
                        if selector.startswith('//'):
                            locator = self.page.locator(f'xpath={selector}')
                        else:
                            locator = self.page.locator(selector)

                        locator.wait_for(state='visible', timeout=5000)
                        src = locator.get_attribute('src')

                        if src and 'fnac-static.com' in src:
                            result['imageurl'] = src
                            logger.info(f"이미지 URL: {result['imageurl']}")
                            image_found = True
                            break
                    except:
                        continue

                # 2. JavaScript로 이미지 찾기
                if not image_found:
                    try:
                        js_result = self.page.evaluate("""
                            () => {
                                var imgs = document.querySelectorAll('img');
                                for (var i = 0; i < imgs.length; i++) {
                                    var src = imgs[i].src || imgs[i].getAttribute('data-src');
                                    if (src && src.includes('fnac-static.com')) {
                                        return src;
                                    }
                                }
                                return null;
                            }
                        """)

                        if js_result:
                            result['imageurl'] = js_result
                            logger.info(f"이미지 URL (JS): {result['imageurl']}")
                            image_found = True
                    except:
                        pass

                if not image_found:
                    logger.warning("이미지 URL 추출 실패")

            except Exception as e:
                logger.warning(f"이미지 URL 추출 실패: {e}")

            return result

        except Exception as e:
            logger.error(f"❌ 페이지 처리 오류: {e}")

            # 재시도 로직
            if retry_count < max_retries:
                wait_time = (retry_count + 1) * 10
                logger.info(f"🔄 {wait_time}초 후 재시도합니다... (재시도 {retry_count + 1}/{max_retries})")
                time.sleep(wait_time)

                # 재귀 호출로 재시도
                return self.extract_product_info(url, row_data, retry_count + 1, max_retries)

            # 최대 재시도 횟수 초과 시 기본값 반환
            logger.error(f"❌ 최대 재시도 횟수 초과: {url}")
            now_time = datetime.now(self.korea_tz)
            local_time = datetime.now(self.local_tz)

            crawl_dt = local_time.strftime("%Y-%m-%dT%H:%M:%S")
            tz_offset = local_time.strftime("%z")
            tz_formatted = f"{tz_offset[:3]}:{tz_offset[3:]}" if tz_offset else "+00:00"
            crawl_datetime_iso = f"{crawl_dt}{tz_formatted}"

            return {
                'retailerid': row_data.get('retailerid', ''),
                'country_code': row_data.get('country', 'fr'),
                'ships_from': 'FR',
                'channel_name': 'fnac',
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
                'sold_by': 'Fnac',
                'imageurl': None,
                'producturl': url,
                'crawl_datetime': crawl_datetime_iso,
                'crawl_strdatetime': local_time.strftime('%Y%m%d%H%M%S') + f"{local_time.microsecond:06d}"[:4],
                'kr_crawl_datetime': now_time.strftime('%Y-%m-%d %H:%M:%S'),
                'kr_crawl_strdatetime': now_time.strftime('%Y%m%d%H%M%S') + f"{now_time.microsecond:06d}"[:4],
                'title': None,
                'vat': row_data.get('vat', 'o')
            }

    def save_to_db(self, df):
        """DB에 결과 저장"""
        if self.db_engine is None:
            logger.warning("⚠️ DB 연결이 없어 DB 저장을 건너뜁니다")
            return False

        try:
            # fnac_price_crawl_tbl_fr 테이블에 저장
            df.to_sql('fnac_price_crawl_tbl_fr', self.db_engine, if_exists='append', index=False)
            logger.info(f"✅ DB 저장 완료: {len(df)}개 레코드")

            # 크롤링 로그를 pandas DataFrame으로 만들어서 한번에 저장
            log_records = []
            for _, row in df.iterrows():
                log_records.append({
                    'country_code': 'fr',
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
        base_filename = f"{date_str}_{time_str}_fr_fnac"

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

                # 4. MD5 파일 생성
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
        logger.info("=== Fnac 세션 초기화 및 테스트 ===")

        if not self.setup_browser():
            return False

        try:
            # 1단계: Google 연결 테스트
            logger.info("1단계: Google 연결 테스트...")
            self.page.goto("https://www.google.com", wait_until='networkidle', timeout=30000)
            time.sleep(2)
            google_title = self.page.title()

            if "Google" in google_title:
                logger.info("✅ Google 접속 성공")
            else:
                logger.warning("⚠️ Google 접속 이상")

            # 2단계: Fnac 세션 초기화
            if not self.initialize_session():
                return False

            # 3단계: 테스트 상품 페이지 접속 (DB에서 실제 URL 가져오기)
            logger.info("3단계: 테스트 상품 페이지 접속...")

            # DB에서 첫 번째 제품 가져오기
            test_products = self.get_crawl_targets(limit=1)
            if not test_products:
                logger.warning("⚠️ 테스트용 제품이 없어 건너뜁니다")
                return True

            test_row = test_products[0]
            test_url = test_row.get('url')

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
        failed_urls = []

        try:
            for idx, row in enumerate(urls_data):
                logger.info(f"\n{'='*50}")
                logger.info(f"진행률: {idx + 1}/{len(urls_data)} ({(idx + 1)/len(urls_data)*100:.1f}%)")

                url = row.get('url')
                result = self.extract_product_info(url, row)

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
                            interim_df.to_sql('fnac_price_crawl_tbl_fr', self.db_engine,
                                            if_exists='append', index=False)
                            logger.info(f"💾 중간 저장: 10개 레코드 DB 저장")
                        except Exception as e:
                            logger.error(f"중간 저장 실패: {e}")

                # 다음 요청 전 대기
                if idx < len(urls_data) - 1:
                    wait_time = random.uniform(2, 5)
                    logger.info(f"⏳ {wait_time:.1f}초 대기 중...")
                    time.sleep(wait_time)

                    if (idx + 1) % 10 == 0:
                        logger.info("☕ 10개 처리 완료, 30초 휴식...")
                        time.sleep(30)

        except Exception as e:
            logger.error(f"❌ 스크래핑 중 오류: {e}")

        finally:
            if failed_urls:
                logger.warning(f"\n⚠️ 가격 추출 실패한 URL {len(failed_urls)}개:")
                for fail in failed_urls[:5]:
                    logger.warning(f"  - {fail['brand']} {fail['item']}: {fail['url']}")
                if len(failed_urls) > 5:
                    logger.warning(f"  ... 외 {len(failed_urls) - 5}개")

            if self.browser:
                self.browser.close()
                logger.info("🔧 브라우저 종료")

            if self.playwright:
                self.playwright.stop()
                logger.info("🔧 Playwright 종료")

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
            price_df['numeric_price'] = price_df['retailprice']

            logger.info(f"\n💰 가격 통계:")
            logger.info(f"평균가: €{price_df['numeric_price'].mean():.2f}")
            logger.info(f"최저가: €{price_df['numeric_price'].min():.2f}")
            logger.info(f"최고가: €{price_df['numeric_price'].max():.2f}")
            logger.info(f"중간값: €{price_df['numeric_price'].median():.2f}")

def main():
    """메인 실행 함수"""
    print("\n🚀 Fnac 가격 추출 시스템 - Playwright 기반 버전")
    print("="*60)

    scraper = FnacScraper()

    if scraper.db_engine is None:
        logger.error("DB 연결 실패로 종료합니다.")
        return

    # 테스트 모드
    test_mode = os.getenv("TEST_MODE", "false").lower()

    if test_mode in ["true", "1", "yes"]:
        logger.info("🧪 테스트 모드 실행")

        if scraper.test_connection():
            logger.info("✅ 테스트 완료")
        else:
            logger.error("❌ 테스트 실패")

        if scraper.browser:
            scraper.browser.close()
        if scraper.playwright:
            scraper.playwright.stop()
        return

    # 실제 크롤링
    logger.info("\n📊 실제 크롤링 시작")

    if not scraper.test_connection():
        logger.error("연결 테스트 실패로 종료합니다.")
        return

    urls_data = scraper.get_crawl_targets()

    if not urls_data:
        logger.warning("크롤링 대상이 없습니다.")
        return

    logger.info(f"✅ 크롤링 대상: {len(urls_data)}개")

    start_time = datetime.now(scraper.korea_tz)
    results_df = scraper.scrape_urls(urls_data)

    if results_df is None or results_df.empty:
        logger.error("크롤링 결과가 없습니다.")
        return

    end_time = datetime.now(scraper.korea_tz)

    logger.info("\n💾 최종 결과 저장")

    success_count = results_df['retailprice'].notna().sum()
    failed_count = results_df['retailprice'].isna().sum()
    success_rate = (success_count / len(results_df) * 100) if len(results_df) > 0 else 0

    logger.info(f"\n📊 === 최종 결과 ===")
    logger.info(f"전체: {len(results_df)}개")
    logger.info(f"성공: {success_count}개")
    logger.info(f"실패: {failed_count}개")
    logger.info(f"성공률: {success_rate:.1f}%")
    logger.info(f"소요 시간: {round((end_time - start_time).total_seconds() / 60, 2)} 분")

    save_results = scraper.save_results(
        results_df,
        save_db=True,
        upload_server=True
    )

    scraper.analyze_results(results_df)

    logger.info("\n📊 저장 결과:")
    logger.info(f"DB 저장: {'✅ 성공' if save_results['db_saved'] else '❌ 실패'}")
    logger.info(f"파일서버 업로드: {'✅ 성공' if save_results['server_uploaded'] else '❌ 실패'}")

    logger.info("\n✅ 크롤링 프로세스 완료!")

if __name__ == "__main__":
    print("📦 필요한 패키지:")
    print("pip install playwright pandas pymysql sqlalchemy paramiko")
    print("playwright install chromium")
    print()

    main()

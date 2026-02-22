"""
모니터링 스크린샷 캡쳐 - 리테일러별 드라이버 세팅 사용
실행: python monitoring_capture.py --retailer danawa --crawl_date 2026-01-31
"""
import io
import gc
import os
import sys
import json
import time
import logging
import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path

# 상위 디렉토리 config 참조
sys.path.append(str(Path(__file__).parent.parent))

import boto3
import pymysql
from botocore.exceptions import ClientError
from PIL import Image, ImageDraw, ImageFont

from config import AWS_CONFIG, DB_CONFIG
from retailer_settings import RETAILERS

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ScreenshotMonitor:
    """스크린샷 캡쳐 및 S3 업로드"""

    def __init__(self, retailer, crawl_date, created_id=None, test_mode=False):
        self.retailer = retailer
        self.crawl_date = crawl_date  # 크롤링 날짜 (폴더 경로용)
        self.created_id = created_id  # 생성자 ID (DB 저장용)
        self.test_mode = test_mode  # 테스트 모드 (S3/DB 저장 없이 로컬 저장)
        self.driver = None
        self.settings = None
        self.s3_client = None
        self.db_conn = None
        if not test_mode:
            self.setup_s3_client()
            self.setup_db_connection()

    def setup_s3_client(self):
        """S3 클라이언트 설정"""
        try:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=AWS_CONFIG['access_key_id'],
                aws_secret_access_key=AWS_CONFIG['secret_access_key'],
                region_name=AWS_CONFIG['region']
            )
            logger.info("✅ S3 클라이언트 설정 완료")
        except Exception as e:
            logger.error(f"❌ S3 클라이언트 설정 실패: {e}")
            raise

    def setup_db_connection(self):
        """DB 연결 설정"""
        try:
            self.db_conn = pymysql.connect(
                host=DB_CONFIG['host'],
                port=DB_CONFIG['port'],
                user=DB_CONFIG['user'],
                password=DB_CONFIG['password'],
                database='ssd_crawl_db',
                charset='utf8mb4',
                autocommit=True
            )
            logger.info("✅ DB 연결 설정 완료")
        except Exception as e:
            logger.error(f"❌ DB 연결 실패: {e}")
            raise

    def insert_file_record(self, file_name, file_path, file_size, created_id=None):
        """파일 정보를 DB에 저장하고 file_id 반환"""
        try:
            # 한국 시간대 (UTC+9) - UTC 기준으로 변환
            kst = timezone(timedelta(hours=9))
            created_at = datetime.now(timezone.utc).astimezone(kst)

            with self.db_conn.cursor() as cursor:
                query = """
                    INSERT INTO ssd_crawl_db.ds_monitoring_file
                    (file_name, file_path, file_size, file_type, is_del, created_at, created_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(query, (
                    file_name,
                    file_path,
                    file_size,
                    'image/png',
                    0,
                    created_at,
                    created_id or 'crawl'
                ))
                file_id = cursor.lastrowid
                logger.info(f"✅ 파일 레코드 저장 완료: file_id={file_id}")
                return file_id
        except Exception as e:
            logger.error(f"❌ 파일 레코드 저장 실패: {e}")
            return None

    def update_anomaly_screenshot(self, anomaly_id, file_id):
        """anomaly 레코드의 screenshot_id 업데이트"""
        try:
            with self.db_conn.cursor() as cursor:
                query = """
                    UPDATE ssd_crawl_db.ds_monitoring_report_anomaly
                    SET screenshot_id = %s
                    WHERE id = %s
                """
                cursor.execute(query, (file_id, anomaly_id))
                logger.info(f"✅ anomaly 레코드 업데이트 완료: id={anomaly_id}, screenshot_id={file_id}")
                return True
        except Exception as e:
            logger.error(f"❌ anomaly 레코드 업데이트 실패: {e}")
            return False

    def setup_driver(self):
        """리테일러별 드라이버 설정"""
        logger.info(f"🔧 {self.retailer} 드라이버 설정 중...")

        if self.retailer not in RETAILERS:
            logger.error(f"❌ 지원하지 않는 리테일러: {self.retailer}")
            logger.info(f"지원 리테일러: {list(RETAILERS.keys())}")
            return False

        try:
            setup_func = RETAILERS[self.retailer]
            self.driver, self.settings = setup_func()
            logger.info(f"✅ {self.retailer} 드라이버 설정 완료")
            return True
        except Exception as e:
            logger.error(f"❌ 드라이버 설정 실패: {e}")
            return False

    def add_watermark(self, screenshot_bytes, url):
        """스크린샷에 URL(상단)과 타임스탬프(우측 하단) 워터마크 추가"""
        try:
            image = Image.open(io.BytesIO(screenshot_bytes))
            draw = ImageDraw.Draw(image)

            try:
                font = ImageFont.truetype("arial.ttf", 20)
            except:
                font = ImageFont.load_default()

            padding = 4

            # 좌측 상단: URL
            url_bbox = draw.textbbox((0, 0), url, font=font)
            url_width = url_bbox[2] - url_bbox[0]
            url_height = url_bbox[3] - url_bbox[1]
            url_x = 10
            url_y = 10
            draw.rectangle(
                [url_x - padding, url_y - padding, url_x + url_width + padding, url_y + url_height + padding],
                fill=(0, 0, 0, 200)
            )
            draw.text((url_x, url_y), url, font=font, fill=(255, 255, 255))

            # 우측 하단: 타임스탬프
            timestamp_text = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ts_bbox = draw.textbbox((0, 0), timestamp_text, font=font)
            ts_width = ts_bbox[2] - ts_bbox[0]
            ts_height = ts_bbox[3] - ts_bbox[1]
            ts_x = image.width - ts_width - 10
            ts_y = image.height - ts_height - 10
            draw.rectangle(
                [ts_x - padding, ts_y - padding, ts_x + ts_width + padding, ts_y + ts_height + padding],
                fill=(0, 0, 0, 200)
            )
            draw.text((ts_x, ts_y), timestamp_text, font=font, fill=(255, 255, 255))

            output = io.BytesIO()
            image.save(output, format='PNG')
            output.seek(0)

            return output.getvalue()
        except Exception as e:
            logger.error(f"❌ 워터마크 추가 실패: {e}")
            return screenshot_bytes

    def is_amazon_retailer(self):
        """Amazon 리테일러 여부 확인"""
        return self.retailer.startswith('amazon_')

    def _click_element(self, element, selector_desc=""):
        """요소 클릭 시도 — 일반 click 실패 시 JS click fallback"""
        try:
            element.click()
            return True
        except Exception as e:
            logger.debug(f"일반 클릭 실패 ({selector_desc}): {e}")
        # JS click fallback (오버레이가 가리는 경우)
        try:
            self.driver.execute_script("arguments[0].click();", element)
            return True
        except Exception as e:
            logger.debug(f"JS 클릭도 실패 ({selector_desc}): {e}")
        return False

    def _find_and_click(self, selectors, popup_name="팝업"):
        """셀렉터 목록에서 요소를 찾아 클릭 — find_elements로 즉시 탐색, JS click fallback"""
        from selenium.webdriver.common.by import By

        for selector in selectors:
            try:
                if selector.startswith('//') or selector.startswith('/html'):
                    elements = self.driver.find_elements(By.XPATH, selector)
                else:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)

                for elem in elements:
                    if elem.is_displayed():
                        if self._click_element(elem, selector):
                            logger.info(f"✅ {popup_name} 클릭 완료 (selector: {selector})")
                            return True
            except Exception as e:
                logger.debug(f"{popup_name} 셀렉터 실패 ({selector}): {e}")
                continue
        return False

    def handle_continue_popup(self):
        """Continue shopping 팝업 처리 (Amazon 리테일러 전용) - 국가별 셀렉터 적용"""
        try:
            # Amazon 리테일러가 아니면 스킵
            if not self.is_amazon_retailer():
                return False

            driver_type = self.settings.get('driver_type', 'selenium')
            if driver_type != 'selenium':
                return False

            # 리테일러별 Continue shopping 셀렉터 (국가별 언어 지원)
            retailer_continue_selectors = {
                'amazon_usa': [
                    '//button[contains(text(), "Continue shopping")]',
                    '//a[contains(text(), "Continue shopping")]',
                    '//span[contains(text(), "Continue shopping")]/ancestor::button',
                    '//span[contains(text(), "Continue shopping")]/ancestor::a',
                    '//input[@type="submit" and contains(@value, "Continue")]',
                ],
                'amazon_gb': [
                    '//button[contains(text(), "Continue shopping")]',
                    '//a[contains(text(), "Continue shopping")]',
                    '//span[contains(text(), "Continue shopping")]/ancestor::button',
                    '//button[contains(text(), "Continue")]',
                ],
                'amazon_jp': [
                    '/html/body/center/center/p/table/tbody/tr/td/p[5]/a/img',
                    '//a[contains(text(), "ショッピングを続ける")]',
                    '//button[contains(text(), "ショッピングを続ける")]',
                    '//span[contains(text(), "ショッピングを続ける")]/ancestor::button',
                    '//a[contains(@alt, "ショッピングを続ける")]',
                    '//img[contains(@alt, "ショッピングを続ける")]/ancestor::a',
                    '//button[contains(text(), "Continue shopping")]',
                ],
                'amazon_in': [
                    '//button[contains(text(), "Continue shopping")]',
                    '//a[contains(text(), "Continue shopping")]',
                    '//span[contains(text(), "Continue shopping")]/ancestor::button',
                ],
                'amazon_it': [
                    '//button[contains(text(), "Continua lo shopping")]',
                    '//a[contains(text(), "Continua lo shopping")]',
                    '//span[contains(text(), "Continua lo shopping")]/ancestor::button',
                    '//button[contains(text(), "Continue shopping")]',
                ],
                'amazon_es': [
                    '//button[contains(text(), "Seguir comprando")]',
                    '//a[contains(text(), "Seguir comprando")]',
                    '//span[contains(text(), "Seguir comprando")]/ancestor::button',
                    '//button[contains(text(), "Continue shopping")]',
                ],
                'amazon_fr': [
                    '//button[contains(text(), "Continuer vos achats")]',
                    '//a[contains(text(), "Continuer vos achats")]',
                    '//span[contains(text(), "Continuer vos achats")]/ancestor::button',
                    '//button[contains(text(), "Continue shopping")]',
                ],
                'amazon_de': [
                    '//input[@data-nav-ref="dismissBtn"]',
                    '//button[contains(text(), "Weiter shoppen")]',
                    '//a[contains(text(), "Weiter shoppen")]',
                    '//span[contains(text(), "Weiter shoppen")]/ancestor::button',
                    '//button[contains(text(), "Continue shopping")]',
                ],
                'amazon_nl': [
                    '//button[contains(text(), "Doorgaan met winkelen")]',
                    '//a[contains(text(), "Doorgaan met winkelen")]',
                    '//span[contains(text(), "Doorgaan met winkelen")]/ancestor::button',
                    '//button[contains(text(), "Continue shopping")]',
                ],
            }

            continue_selectors = retailer_continue_selectors.get(self.retailer, [])

            # 공통 셀렉터 추가 (fallback)
            common_selectors = [
                '//button[contains(text(), "Continue shopping")]',
                '//a[contains(text(), "Continue shopping")]',
                '//span[contains(text(), "Continue shopping")]/ancestor::button',
                '//input[@value="Continue shopping"]',
                'input[value="Continue shopping"]',
                'button.a-button-primary',
            ]
            continue_selectors.extend(common_selectors)

            if self._find_and_click(continue_selectors, "Continue shopping"):
                time.sleep(2)
                return True
            return False
        except:
            return False

    def handle_cookie_popup(self):
        """쿠키 동의 팝업 처리 (Accept 클릭) - 리테일러별 셀렉터 적용"""
        try:
            driver_type = self.settings.get('driver_type', 'selenium')
            if driver_type != 'selenium':
                return

            # 리테일러별 쿠키 셀렉터
            retailer_cookie_selectors = {
                'amazon_usa': [
                    '#sp-cc-accept',
                    'input#sp-cc-accept',
                    'input[data-action="sp-cc-accept"]',
                    '//input[@name="accept"]',
                    '//span[text()="Accept"]/ancestor::button',
                ],
                'amazon_gb': [
                    '#sp-cc-accept',
                    'input#sp-cc-accept',
                    'input[data-action="sp-cc-accept"]',
                    '//input[@name="accept"]',
                    '//span[text()="Accept"]/ancestor::button',
                ],
                'amazon_jp': [
                    '#sp-cc-accept',
                    'input#sp-cc-accept',
                    '//input[@name="accept"]',
                    '//span[text()="同意する"]/ancestor::button',
                ],
                'amazon_in': [
                    '#sp-cc-accept',
                    'input#sp-cc-accept',
                    '//input[@name="accept"]',
                    '//span[text()="Accept"]/ancestor::button',
                ],
                'amazon_it': [
                    '#sp-cc-accept',
                    'input#sp-cc-accept',
                    '//input[@name="accept"]',
                    '//span[text()="Accetta"]/ancestor::button',
                ],
                'amazon_es': [
                    '#sp-cc-accept',
                    'input#sp-cc-accept',
                    '//input[@name="accept"]',
                    '//span[text()="Aceptar"]/ancestor::button',
                    '//button[contains(text(), "Aceptar")]',
                ],
                'amazon_fr': [
                    '#sp-cc-accept',
                    'input#sp-cc-accept',
                    '//input[@name="accept"]',
                    '//span[text()="Accepter"]/ancestor::button',
                    '//button[contains(text(), "Accepter")]',
                ],
                'amazon_de': [
                    '#sp-cc-accept',
                    'input#sp-cc-accept',
                    '//input[@name="accept"]',
                    '//span[text()="Annehmen"]/ancestor::button',
                    '//button[contains(text(), "Annehmen")]',
                ],
                'amazon_nl': [
                    '#sp-cc-accept',
                    'input#sp-cc-accept',
                    '//input[@name="accept"]',
                    '//span[text()="Accepteren"]/ancestor::button',
                    '//button[contains(text(), "Accepteren")]',
                ],
                'currys': [
                    '#onetrust-accept-btn-handler',
                    'button#onetrust-accept-btn-handler',
                    "button[aria-label='Accept all cookies']",
                    '//button[contains(text(), "Allow all")]',
                    '//button[contains(text(), "Accept")]',
                ],
                'bestbuy': [
                    '#onetrust-accept-btn-handler',
                    'button.accept-all',
                ],
                'centrecom': [
                    '#onetrust-accept-btn-handler',
                    'button.accept-all',
                    '//button[contains(text(), "Accept")]',
                ],
                'danawa': [],
                'fnac': [
                    '#onetrust-accept-btn-handler',
                    '//button[contains(text(), "J\'accepte")]',
                    '//button[contains(text(), "Accepter")]',
                    '//button[contains(text(), "Non, merci")]',
                    '//a[contains(text(), "Non, merci")]',
                    '//span[contains(text(), "Non, merci")]/ancestor::button',
                ],
                'coolblue': [
                    '#onetrust-accept-btn-handler',
                    'button[name="accept"]',
                    '//button[contains(text(), "Oké")]',
                ],
                'mediamarkt': [
                    '#pwa-consent-layer-accept-all-button',
                    '//button[contains(text(), "Alle akzeptieren")]',
                ],
                'x-kom': [
                    '#onetrust-accept-btn-handler',
                    '//button[contains(text(), "Akceptuję")]',
                ],
            }

            cookie_selectors = retailer_cookie_selectors.get(self.retailer, [])

            # 공통 셀렉터 추가
            common_selectors = [
                '#sp-cc-accept',
                '#onetrust-accept-btn-handler',
                'button.accept-all',
                '//button[contains(text(), "Accept")]',
                '//button[text()="Accept"]',
                '//span[text()="Accept"]/ancestor::button',
            ]
            cookie_selectors.extend(common_selectors)

            if self._find_and_click(cookie_selectors, "쿠키 팝업"):
                time.sleep(1)
        except Exception as e:
            pass

    def handle_newsletter_popup(self):
        """뉴스레터 구독 팝업 처리 (centrecom 전용)"""
        if self.retailer != 'centrecom':
            return

        try:
            time.sleep(3)  # 뉴스레터 팝업 비동기 로드 대기
            newsletter_selectors = [
                'div.lightbox-nope',
                '//div[contains(text(), "Nope")]',
                'span.featherlight-close-icon',
                'span.featherlight-close',
            ]

            if self._find_and_click(newsletter_selectors, "뉴스레터 팝업"):
                time.sleep(1)
        except Exception as e:
            pass

    def is_error_page(self):
        """에러 페이지 여부 확인 (Amazon 리테일러 전용) - 페이지 타이틀 + 본문 키워드"""
        try:
            # Amazon 리테일러가 아니면 항상 False
            if not self.is_amazon_retailer():
                return False

            driver_type = self.settings.get('driver_type', 'selenium')

            # 1. 페이지 타이틀로 에러 감지 (오탐 없음)
            if driver_type == 'selenium':
                page_title = self.driver.title.lower()
            elif driver_type == 'drission':
                page_title = self.driver.title.lower()
            elif driver_type == 'playwright':
                page_title = self.driver.title().lower()
            else:
                return False

            # 숫자 상태 코드는 단어 경계로 매칭 (제품 모델명 오탐 방지: T500, 404K 등)
            import re
            status_code_keywords = ['503', '502', '500', '404']
            for code in status_code_keywords:
                if re.search(r'\b' + code + r'\b', page_title):
                    logger.warning(f"⚠️ 에러 페이지 타이틀 감지: '{page_title}' (keyword: {code})")
                    return True

            # 텍스트 키워드는 부분 문자열 매칭
            text_error_keywords = [
                'error', 'fout', 'erreur', 'fehler', 'errore', 'エラー',
                'sorry', 'service unavailable',
            ]

            for keyword in text_error_keywords:
                if keyword in page_title:
                    logger.warning(f"⚠️ 에러 페이지 타이틀 감지: '{page_title}' (keyword: {keyword})")
                    return True

            # 2. 본문 키워드 검사 (봇 체크 전용)
            if driver_type == 'selenium':
                page_source = self.driver.page_source
            elif driver_type == 'drission':
                page_source = self.driver.html
            elif driver_type == 'playwright':
                page_source = self.driver.content()
            else:
                return False

            page_source_lower = page_source.lower()

            body_keywords = [
                'sorry, we just need to make sure you\'re not a robot',
                'robot check',
                'enter the characters you see below',
            ]

            for keyword in body_keywords:
                if keyword in page_source_lower:
                    logger.warning(f"⚠️ 에러 페이지 키워드 감지: {keyword}")
                    return True

            return False
        except:
            return False

    def capture_screenshot(self, url, max_retries=3):
        """페이지 스크린샷 캡쳐 (에러 페이지 재시도)"""
        try:
            logger.info(f"📸 스크린샷 캡쳐: {url}")

            wait_time = self.settings.get('wait_time', 3)
            driver_type = self.settings.get('driver_type', 'selenium')

            for attempt in range(max_retries):
                # 드라이버 타입별 처리
                if driver_type == 'selenium':
                    self.driver.get(url)
                    time.sleep(wait_time)
                elif driver_type == 'drission':
                    self.driver.get(url)
                    time.sleep(wait_time)
                elif driver_type == 'playwright':
                    self.driver.goto(url)
                    time.sleep(wait_time)
                else:
                    raise ValueError(f"지원하지 않는 드라이버 타입: {driver_type}")

                # 1. 에러 페이지 확인
                if self.is_error_page():
                    if attempt < max_retries - 1:
                        logger.warning(f"⚠️ 에러 페이지 감지, 재시도 중... ({attempt + 1}/{max_retries})")
                        time.sleep(2)
                        continue
                    else:
                        logger.warning(f"⚠️ 에러 페이지 - 최대 재시도 횟수 초과, 현재 상태로 캡쳐")
                        break

                # 2. Continue shopping 팝업 처리
                if self.handle_continue_popup():
                    time.sleep(wait_time)  # 페이지 로드 대기

                # 3. 쿠키 팝업 처리 (비동기 로드 대기)
                time.sleep(2)
                self.handle_cookie_popup()

                # 4. 뉴스레터 팝업 처리 (centrecom)
                self.handle_newsletter_popup()
                break

            # 스크린샷 캡쳐
            if driver_type == 'selenium':
                screenshot_bytes = self.driver.get_screenshot_as_png()
            elif driver_type == 'drission':
                screenshot_bytes = self.driver.get_screenshot(as_bytes=True)
            elif driver_type == 'playwright':
                screenshot_bytes = self.driver.screenshot()

            screenshot_bytes = self.add_watermark(screenshot_bytes, url)
            logger.info("✅ 스크린샷 캡쳐 완료 (URL + 타임스탬프 워터마크 포함)")
            return screenshot_bytes

        except Exception as e:
            logger.error(f"❌ 스크린샷 캡쳐 실패: {e}")
            return None

    def upload_to_s3(self, screenshot_bytes, s3_key):
        """스크린샷을 S3에 업로드"""
        try:
            logger.info(f"☁️ S3 업로드: {s3_key}")
            self.s3_client.put_object(
                Bucket=AWS_CONFIG['bucket_name'],
                Key=s3_key,
                Body=screenshot_bytes,
                ContentType='image/png'
            )
            logger.info(f"✅ S3 업로드 완료: s3://{AWS_CONFIG['bucket_name']}/{s3_key}")
            return True
        except ClientError as e:
            logger.error(f"❌ S3 업로드 실패: {e}")
            return False

    def process_url(self, url, identifier=None, anomaly_id=None):
        """단일 URL 처리: 캡쳐 -> S3 업로드 -> DB 저장 -> 메모리 해제"""
        screenshot_bytes = None
        try:
            screenshot_bytes = self.capture_screenshot(url)
            if screenshot_bytes is None:
                return False

            kst = timezone(timedelta(hours=9))
            creation_timestamp = datetime.now(timezone.utc).astimezone(kst).strftime('%Y%m%d%H%M%S')

            if identifier:
                file_name = f"{self.retailer}_{identifier}_{creation_timestamp}.png"
            else:
                file_name = f"{self.retailer}_{creation_timestamp}.png"

            # 테스트 모드: 로컬 저장만
            if self.test_mode:
                test_dir = os.path.join(os.path.dirname(__file__), 'test_capture')
                os.makedirs(test_dir, exist_ok=True)
                save_path = os.path.join(test_dir, file_name)
                with open(save_path, 'wb') as f:
                    f.write(screenshot_bytes)
                logger.info(f"✅ 테스트 캡쳐 저장: {save_path}")
                return True

            # 운영 모드: S3 업로드 + DB 저장
            crawl_dt = datetime.strptime(self.crawl_date, '%Y-%m-%d')
            year = crawl_dt.strftime('%Y')
            year_month = crawl_dt.strftime('%Y%m')
            year_month_day = crawl_dt.strftime('%Y%m%d')

            file_path = f"{year}/{year_month}/{year_month_day}/{self.retailer}/"
            s3_key = f"{file_path}{file_name}"

            upload_success = self.upload_to_s3(screenshot_bytes, s3_key)

            if upload_success and anomaly_id:
                file_size = len(screenshot_bytes)
                file_id = self.insert_file_record(file_name, file_path, file_size, self.created_id)

                if file_id:
                    self.update_anomaly_screenshot(anomaly_id, file_id)

            return upload_success

        finally:
            if screenshot_bytes:
                del screenshot_bytes
            gc.collect()
            logger.info("🧹 메모리 해제 완료")

    def process_urls(self, urls_data):
        """여러 URL 처리

        Args:
            urls_data: list of dict [{'url': '...', 'identifier': '...'}, ...]
                       또는 list of str ['url1', 'url2', ...]
        """
        if not self.setup_driver():
            return []

        results = []
        try:
            for idx, item in enumerate(urls_data):
                logger.info(f"\n{'='*50}")
                logger.info(f"진행률: {idx + 1}/{len(urls_data)}")

                if isinstance(item, dict):
                    url = item.get('url')
                    identifier = item.get('identifier') or item.get('retailersku')
                    anomaly_id = item.get('id')
                else:
                    url = item
                    identifier = None
                    anomaly_id = None

                success = self.process_url(url, identifier, anomaly_id)
                results.append({
                    'url': url,
                    'identifier': identifier,
                    'success': success
                })

                if idx < len(urls_data) - 1:
                    time.sleep(2)

        finally:
            self.cleanup()

        return results

    def cleanup(self):
        """리소스 정리"""
        logger.info("🧹 리소스 정리 중...")

        driver_type = self.settings.get('driver_type', 'selenium') if self.settings else 'selenium'

        if self.driver:
            try:
                if driver_type == 'selenium':
                    # Chrome PID 저장 (프로세스 강제 종료용)
                    chrome_pid = None
                    try:
                        chrome_pid = self.driver.service.process.pid
                    except:
                        pass

                    # 1. 브라우저 창 닫기
                    try:
                        self.driver.close()
                    except:
                        pass

                    # 2. 드라이버 종료
                    try:
                        self.driver.quit()
                    except:
                        pass

                    # 3. 서비스 프로세스 강제 종료
                    try:
                        self.driver.service.process.kill()
                    except:
                        pass

                    # 4. Chrome 프로세스 강제 종료 (PID 기반)
                    if chrome_pid:
                        try:
                            import os
                            import signal
                            os.kill(chrome_pid, signal.SIGTERM)
                        except:
                            pass

                    # __del__ 중복 호출 방지
                    self.driver.service = None
                elif driver_type == 'drission':
                    self.driver.quit()
                elif driver_type == 'playwright':
                    self.driver.context.browser.close()
            except:
                pass
            self.driver = None

        if self.s3_client:
            self.s3_client = None

        if self.db_conn:
            try:
                self.db_conn.close()
            except:
                pass
            self.db_conn = None

        gc.collect()
        logger.info("✅ 리소스 정리 완료")


def get_anomaly_urls(retailer, crawl_date):
    """DB에서 이상 감지된 URL 조회

    Args:
        retailer: 리테일러 이름 (예: danawa, currys)
        crawl_date: 크롤링 날짜 (예: 2026-01-31)

    Returns:
        list of dict: [{'id': ..., 'producturl': '...', 'retailersku': '...'}, ...]
    """
    try:
        connection = pymysql.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            database='ssd_crawl_db',
            charset='utf8mb4'
        )

        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            # retailer_id를 통해 ds_monitoring_targets와 JOIN (screenshot_id 없는 것만)
            query = """
                SELECT a.id, a.producturl, a.retailersku
                FROM ssd_crawl_db.ds_monitoring_report_anomaly a
                JOIN ssd_crawl_db.ds_monitoring_targets t ON a.retailer_id = t.retailer_id
                WHERE LOWER(t.retailer) = LOWER(%s) AND a.crawl_date = %s
                  AND a.screenshot_id IS NULL AND a.is_del = 0
            """
            cursor.execute(query, (retailer, crawl_date))
            results = cursor.fetchall()

        connection.close()

        logger.info(f"📊 조회된 URL: {len(results)}개")
        return results

    except Exception as e:
        logger.error(f"❌ DB 조회 실패: {e}")
        return []


def update_capture_log(retailer, crawl_date, status):
    """캡쳐 완료/실패 시 capture_log 업데이트"""
    try:
        conn = pymysql.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            database='ssd_crawl_db',
            charset='utf8mb4',
            autocommit=True
        )
        kst = timezone(timedelta(hours=9))
        now_kst = datetime.now(timezone.utc).astimezone(kst)

        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT retailer_id FROM ds_monitoring_targets
                WHERE LOWER(retailer) = LOWER(%s)
            """, (retailer,))
            row = cursor.fetchone()
            if not row:
                logger.warning(f"⚠️ capture_log 업데이트 실패: retailer '{retailer}' 미존재")
                conn.close()
                return

            retailer_id = row[0]

            cursor.execute("""
                UPDATE ds_monitoring_capture_log
                SET status = %s, completed_at = %s
                WHERE retailer_id = %s AND crawl_date = %s AND status = 'running'
                ORDER BY triggered_at DESC LIMIT 1
            """, (status, now_kst, retailer_id, crawl_date))

        conn.close()
        logger.info(f"✅ capture_log 업데이트: {retailer} → {status}")
    except Exception as e:
        logger.error(f"❌ capture_log 업데이트 실패: {e}")


def main():
    parser = argparse.ArgumentParser(description='모니터링 스크린샷 캡쳐')
    parser.add_argument('--retailer', '-r', help='리테일러 이름 (예: danawa)')
    parser.add_argument('--crawl_date', '-d', help='크롤링 날짜 (예: 2026-01-31)')
    parser.add_argument('--created_id', '-c', help='생성자 ID')

    args = parser.parse_args()

    # 자동 실행 모드 플래그 (SSM 원격 실행 시 True)
    is_auto_mode = False
    created_id = args.created_id  # CLI 인수에서 먼저 가져옴

    # CLI 인수가 없으면 파라미터 파일에서 읽기 (SSM 원격 실행용)
    param_file = os.path.join(os.path.dirname(__file__), 'capture_params.json')
    if not args.retailer and os.path.exists(param_file):
        try:
            with open(param_file, 'r', encoding='utf-8-sig') as f:
                params = json.load(f)
                args.retailer = params.get('retailer')
                args.crawl_date = params.get('crawl_date')
                created_id = params.get('created_id') or created_id
                logger.info(f"📁 파라미터 파일에서 로드: retailer={args.retailer}, crawl_date={args.crawl_date}, created_id={created_id}")
            # 사용 후 파일 삭제 (다음 실행 시 수동 입력 모드로 동작)
            os.remove(param_file)
            is_auto_mode = True  # 파라미터 파일에서 로드 = 자동 실행 모드
        except Exception as e:
            logger.error(f"❌ 파라미터 파일 읽기 실패: {e}")

    # 인수 없으면 직접 입력 받기
    test_mode = False
    if not args.retailer:
        # 모드 선택
        print("\n📋 실행 모드 선택:")
        print("  1. 운영 (S3 업로드 + DB 저장)")
        print("  2. 테스트 (로컬 저장만)")
        mode_choice = input("모드 번호 선택: ").strip()
        if mode_choice == '2':
            test_mode = True
            logger.info("🧪 테스트 모드로 실행합니다.")

        # 리테일러 선택
        retailer_list = list(RETAILERS.keys())
        print("\n📋 지원 리테일러:")
        for idx, name in enumerate(retailer_list, 1):
            print(f"  {idx}. {name}")
        choice = input("리테일러 번호 선택: ").strip()
        try:
            choice_idx = int(choice) - 1
            if 0 <= choice_idx < len(retailer_list):
                args.retailer = retailer_list[choice_idx]
            else:
                logger.error("❌ 잘못된 번호입니다.")
                input("\n종료하려면 Enter를 누르세요...")
                return
        except ValueError:
            logger.error("❌ 숫자를 입력해주세요.")
            input("\n종료하려면 Enter를 누르세요...")
            return

    if not args.crawl_date:
        today_str = datetime.now().strftime('%Y-%m-%d')
        args.crawl_date = input(f"날짜입력(예:2026-02-02 엔터 입력시 오늘 {today_str}): ").strip()
        if not args.crawl_date:
            args.crawl_date = today_str
            logger.info(f"📅 오늘 날짜 자동 설정: {today_str}")

    if not args.retailer or not args.crawl_date:
        logger.error("❌ 리테일러와 크롤링 날짜를 입력해주세요.")
        if not is_auto_mode:
            input("\n종료하려면 Enter를 누르세요...")
        return

    # 테스트 모드: URL 직접 입력
    if test_mode:
        test_url = input("캡쳐할 URL 입력: ").strip()
        if not test_url:
            logger.error("❌ URL을 입력해주세요.")
            input("\n종료하려면 Enter를 누르세요...")
            return
        urls_to_capture = [{'url': test_url, 'identifier': 'test'}]
    else:
        # 운영 모드: DB 조회
        logger.info(f"🔍 {args.retailer} / {args.crawl_date} 이상 감지 URL 조회 중...")
        urls_data = get_anomaly_urls(args.retailer, args.crawl_date)

        if not urls_data:
            logger.warning("⚠️ 조회된 URL이 없습니다.")
            if is_auto_mode:
                update_capture_log(args.retailer, args.crawl_date, 'completed')
            if not is_auto_mode:
                input("\n종료하려면 Enter를 누르세요...")
            return

        urls_to_capture = [
            {'id': row['id'], 'url': row['producturl'], 'identifier': row.get('retailersku')}
            for row in urls_data
        ]

    logger.info(f"📸 캡쳐 대상: {len(urls_to_capture)}개")

    monitor = ScreenshotMonitor(args.retailer, args.crawl_date, created_id, test_mode)
    capture_status = None
    try:
        results = monitor.process_urls(urls_to_capture)

        success_count = sum(1 for r in results if r['success'])
        logger.info(f"\n📊 === 처리 결과 ===")
        logger.info(f"성공: {success_count}/{len(results)}")
        capture_status = 'completed'

    except Exception as e:
        logger.error(f"❌ 실행 오류: {e}")
        capture_status = 'failed'
    finally:
        if is_auto_mode and not test_mode:
            update_capture_log(args.retailer, args.crawl_date, capture_status)
        monitor.cleanup()

    if not is_auto_mode:
        input("\n종료하려면 Enter를 누르세요...")


if __name__ == "__main__":
    main()

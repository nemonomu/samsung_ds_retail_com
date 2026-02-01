"""
모니터링 스크린샷 캡쳐 - 리테일러별 드라이버 세팅 사용
실행: python monitoring_capture.py --retailer danawa --crawl_date 2026-01-31
"""
import io
import gc
import sys
import time
import logging
import argparse
from datetime import datetime
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

    def __init__(self, retailer):
        self.retailer = retailer
        self.driver = None
        self.settings = None
        self.s3_client = None
        self.setup_s3_client()

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

    def add_timestamp_watermark(self, screenshot_bytes):
        """스크린샷 우측 하단에 타임스탬프 워터마크 추가"""
        try:
            image = Image.open(io.BytesIO(screenshot_bytes))
            draw = ImageDraw.Draw(image)

            timestamp_text = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            try:
                font = ImageFont.truetype("arial.ttf", 20)
            except:
                font = ImageFont.load_default()

            bbox = draw.textbbox((0, 0), timestamp_text, font=font)
            text_width = bbox[2] - bbox[0]
            text_height = bbox[3] - bbox[1]

            x = image.width - text_width - 10
            y = image.height - text_height - 10

            padding = 5
            draw.rectangle(
                [x - padding, y - padding, x + text_width + padding, y + text_height + padding],
                fill=(0, 0, 0, 180)
            )
            draw.text((x, y), timestamp_text, font=font, fill=(255, 255, 255))

            output = io.BytesIO()
            image.save(output, format='PNG')
            output.seek(0)

            return output.getvalue()
        except Exception as e:
            logger.error(f"❌ 워터마크 추가 실패: {e}")
            return screenshot_bytes

    def capture_screenshot(self, url):
        """페이지 스크린샷 캡쳐"""
        try:
            logger.info(f"📸 스크린샷 캡쳐: {url}")

            wait_time = self.settings.get('wait_time', 3)
            driver_type = self.settings.get('driver_type', 'selenium')

            # 드라이버 타입별 처리
            if driver_type == 'selenium':
                self.driver.get(url)
                time.sleep(wait_time)
                screenshot_bytes = self.driver.get_screenshot_as_png()
            elif driver_type == 'drission':
                self.driver.get(url)
                time.sleep(wait_time)
                screenshot_bytes = self.driver.get_screenshot(as_bytes=True)
            elif driver_type == 'playwright':
                self.driver.goto(url)
                time.sleep(wait_time)
                screenshot_bytes = self.driver.screenshot()
            else:
                raise ValueError(f"지원하지 않는 드라이버 타입: {driver_type}")

            screenshot_bytes = self.add_timestamp_watermark(screenshot_bytes)
            logger.info("✅ 스크린샷 캡쳐 완료 (워터마크 포함)")
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

    def process_url(self, url, identifier=None):
        """단일 URL 처리: 캡쳐 -> S3 업로드 -> 메모리 해제"""
        screenshot_bytes = None
        try:
            screenshot_bytes = self.capture_screenshot(url)
            if screenshot_bytes is None:
                return False

            # S3 키 생성
            s3_prefix = self.settings.get('s3_prefix', self.retailer)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            if identifier:
                s3_key = f"{s3_prefix}/{timestamp}_{identifier}.png"
            else:
                s3_key = f"{s3_prefix}/{timestamp}.png"

            return self.upload_to_s3(screenshot_bytes, s3_key)

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
                else:
                    url = item
                    identifier = None

                success = self.process_url(url, identifier)
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
                    self.driver.quit()
                elif driver_type == 'drission':
                    self.driver.quit()
                elif driver_type == 'playwright':
                    self.driver.context.browser.close()
            except:
                pass
            self.driver = None

        if self.s3_client:
            self.s3_client = None

        gc.collect()
        logger.info("✅ 리소스 정리 완료")


def get_anomaly_urls(retailer, crawl_date):
    """DB에서 이상 감지된 URL 조회

    Args:
        retailer: 리테일러 이름 (예: danawa)
        crawl_date: 크롤링 날짜 (예: 2026-01-31)

    Returns:
        list of dict: [{'product_url': '...', 'retailersku': '...'}, ...]
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
            query = """
                SELECT product_url, retailersku
                FROM ssd_crawl_db.ds_monitoring_report_anomaly
                WHERE retailer = %s AND crawl_date = %s
            """
            cursor.execute(query, (retailer, crawl_date))
            results = cursor.fetchall()

        connection.close()

        logger.info(f"📊 조회된 URL: {len(results)}개")
        return results

    except Exception as e:
        logger.error(f"❌ DB 조회 실패: {e}")
        return []


def main():
    parser = argparse.ArgumentParser(description='모니터링 스크린샷 캡쳐')
    parser.add_argument('--retailer', '-r', help='리테일러 이름 (예: danawa)')
    parser.add_argument('--crawl_date', '-d', help='크롤링 날짜 (예: 2026-01-31)')

    args = parser.parse_args()

    # 인수 없으면 직접 입력 받기
    if not args.retailer:
        print("\n📋 지원 리테일러:", list(RETAILERS.keys()))
        args.retailer = input("리테일러 입력: ").strip()
    if not args.crawl_date:
        args.crawl_date = input("크롤링 날짜 입력 (예: 2026-01-31): ").strip()

    if not args.retailer or not args.crawl_date:
        logger.error("❌ 리테일러와 크롤링 날짜를 입력해주세요.")
        input("\n종료하려면 Enter를 누르세요...")
        return

    # DB에서 URL 조회
    logger.info(f"🔍 {args.retailer} / {args.crawl_date} 이상 감지 URL 조회 중...")
    urls_data = get_anomaly_urls(args.retailer, args.crawl_date)

    if not urls_data:
        logger.warning("⚠️ 조회된 URL이 없습니다.")
        return

    # URL 데이터 변환
    urls_to_capture = [
        {'url': row['product_url'], 'identifier': row.get('retailersku')}
        for row in urls_data
    ]

    logger.info(f"📸 캡쳐 대상: {len(urls_to_capture)}개")

    monitor = ScreenshotMonitor(args.retailer)
    try:
        results = monitor.process_urls(urls_to_capture)

        # 결과 출력
        success_count = sum(1 for r in results if r['success'])
        logger.info(f"\n📊 === 처리 결과 ===")
        logger.info(f"성공: {success_count}/{len(results)}")

    except Exception as e:
        logger.error(f"❌ 실행 오류: {e}")
    finally:
        monitor.cleanup()

    input("\n종료하려면 Enter를 누르세요...")


if __name__ == "__main__":
    main()

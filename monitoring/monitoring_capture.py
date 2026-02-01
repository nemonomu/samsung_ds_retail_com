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

    def __init__(self, retailer, crawl_date):
        self.retailer = retailer
        self.crawl_date = crawl_date  # 크롤링 날짜 (폴더 경로용)
        self.driver = None
        self.settings = None
        self.s3_client = None
        self.db_conn = None
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

    def insert_file_record(self, file_name, file_path, file_size):
        """파일 정보를 DB에 저장하고 file_id 반환"""
        try:
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
                    datetime.now(),
                    'crawl'
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
                font = ImageFont.truetype("arial.ttf", 28)
            except:
                font = ImageFont.load_default()

            padding = 5

            # 상단: URL
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

            # S3 키 생성: 리테일러명/년도/년월/년월일/리테일러명_retailersku_생성타임스탬프.png
            # 폴더: 크롤링 날짜 기준, 파일명 타임스탬프: 생성 시점
            crawl_dt = datetime.strptime(self.crawl_date, '%Y-%m-%d')
            year = crawl_dt.strftime('%Y')
            year_month = crawl_dt.strftime('%Y%m')
            year_month_day = crawl_dt.strftime('%Y%m%d')
            creation_timestamp = datetime.now().strftime('%Y%m%d%H%M%S')

            # 폴더 경로 (file_path)
            file_path = f"{self.retailer}/{year}/{year_month}/{year_month_day}/"

            if identifier:
                file_name = f"{self.retailer}_{identifier}_{creation_timestamp}.png"
            else:
                file_name = f"{self.retailer}_{creation_timestamp}.png"

            s3_key = f"{file_path}{file_name}"

            # S3 업로드
            upload_success = self.upload_to_s3(screenshot_bytes, s3_key)

            if upload_success and anomaly_id:
                # 파일 레코드 저장
                file_size = len(screenshot_bytes)
                file_id = self.insert_file_record(file_name, file_path, file_size)

                if file_id:
                    # anomaly 레코드 업데이트
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
                    # undetected-chromedriver Windows 오류 방지
                    try:
                        self.driver.service.process.kill()
                    except:
                        pass
                    try:
                        self.driver.quit()
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
                SELECT id, producturl, retailersku
                FROM ssd_crawl_db.ds_monitoring_report_anomaly
                WHERE retailer = %s AND crawl_date = %s
                LIMIT 1
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
        {'id': row['id'], 'url': row['producturl'], 'identifier': row.get('retailersku')}
        for row in urls_data
    ]

    logger.info(f"📸 캡쳐 대상: {len(urls_to_capture)}개")

    monitor = ScreenshotMonitor(args.retailer, args.crawl_date)
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

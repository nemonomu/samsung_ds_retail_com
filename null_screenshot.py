"""
NULL 필드 발생 시 스크린샷 캡처 + S3 업로드 헬퍼
- ships_from / sold_by / retailprice 중 하나라도 NULL이면 호출
- Selenium / DrissionPage / Playwright 드라이버 모두 지원
- 버킷은 AWS_CONFIG['bucket_name'], S3 키는 'ds-null-screenshots/' prefix 사용

사용 예 (각 크롤러의 extract_product_info 내부, return 직전):
    from null_screenshot import is_null_result, capture_and_upload
    if is_null_result(result):
        capture_and_upload(self.driver, 'amazon_gb', row_data.get('retailersku', ''), url)
"""
import io
import logging
from datetime import datetime, timezone, timedelta

import pandas as pd

logger = logging.getLogger(__name__)

S3_PATH_PREFIX = 'ds-null-screenshots'
KST = timezone(timedelta(hours=9))

# auto_recovery target_key → retailer 디렉토리/파일명에 사용할 리테일러명
RETAILER_NAME_BY_TARGET_KEY = {
    'gb': 'amazon_gb',
    'fr': 'amazon_fr',
    'de': 'amazon_de',
    'es': 'amazon_es',
    'in': 'amazon_in',
    'it': 'amazon_it',
    'jp': 'amazon_jp',
    'nl': 'amazon_nl',
    'usa': 'amazon_usa',
    'currys': 'currys',
    'coolblue': 'coolblue',
    'danawa': 'danawa',
    'fnac': 'fnac',
    'mediamarkt': 'mediamarkt',
    'centrecom': 'centrecom',
    'xkom': 'x-kom',
    'bestbuy': 'bestbuy',
}

_s3_client = None


def _get_s3_client():
    """boto3 S3 클라이언트 lazy 초기화"""
    global _s3_client
    if _s3_client is None:
        import boto3
        from config import AWS_CONFIG
        _s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_CONFIG['access_key_id'],
            aws_secret_access_key=AWS_CONFIG['secret_access_key'],
            region_name=AWS_CONFIG['region']
        )
    return _s3_client


def is_null_result(result):
    """ships_from / sold_by / retailprice 중 하나라도 NULL/empty 인지 확인"""
    if not result:
        return True

    def _is_empty(v):
        if v is None:
            return True
        if isinstance(v, str) and v == '':
            return True
        try:
            if pd.isna(v):
                return True
        except (TypeError, ValueError):
            pass
        return False

    return _is_empty(result.get('ships_from')) or \
           _is_empty(result.get('sold_by')) or \
           _is_empty(result.get('retailprice'))


def _capture_bytes(driver):
    """드라이버 타입에 따라 PNG bytes 반환"""
    # Selenium WebDriver (uc.Chrome 등)
    if hasattr(driver, 'get_screenshot_as_png'):
        return driver.get_screenshot_as_png()
    # DrissionPage ChromiumPage
    if hasattr(driver, 'get_screenshot'):
        try:
            return driver.get_screenshot(as_bytes=True)
        except TypeError:
            pass
    # Playwright Page
    if hasattr(driver, 'screenshot') and callable(getattr(driver, 'screenshot')):
        return driver.screenshot()
    raise ValueError(f"지원하지 않는 드라이버 타입: {type(driver).__name__}")


def _add_watermark(screenshot_bytes, url):
    """스크린샷에 워터마크 추가 (URL 좌상단, KST 타임스탬프 우하단)"""
    try:
        from PIL import Image, ImageDraw, ImageFont

        image = Image.open(io.BytesIO(screenshot_bytes))
        draw = ImageDraw.Draw(image)

        try:
            font = ImageFont.truetype("arial.ttf", 20)
        except Exception:
            font = ImageFont.load_default()

        padding = 4

        # 좌상단: URL
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

        # 우하단: KST 타임스탬프
        timestamp_text = datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S KST')
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
        return output.getvalue()
    except Exception as e:
        logger.warning(f"워터마크 추가 실패 (원본 그대로 업로드): {e}")
        return screenshot_bytes


def _delete_existing_screenshots(s3_client, bucket, prefix):
    """기존 (retailer, sku, 날짜) 패턴과 일치하는 S3 객체 모두 삭제.
    초기 수집 후 auto_recovery 재크롤링 시 동일 SKU의 이전 스크린샷을 교체하기 위함.
    """
    try:
        resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        contents = resp.get('Contents') or []
        if not contents:
            return 0
        objects = [{'Key': obj['Key']} for obj in contents]
        s3_client.delete_objects(Bucket=bucket, Delete={'Objects': objects})
        logger.info(f"기존 NULL 스크린샷 {len(objects)}개 삭제 (prefix={prefix})")
        return len(objects)
    except Exception as e:
        logger.warning(f"기존 NULL 스크린샷 삭제 실패 (prefix={prefix}): {e}")
        return 0


def delete_screenshots_for_sku(retailer, retailsku, date_yyyymmdd):
    """특정 (retailer, sku, 날짜) 조합의 S3 스크린샷 모두 삭제.
    auto_recovery에서 1차 수집 스크린샷을 삭제할 때 사용 (날짜 폴더가 다를 수 있음).

    Args:
        retailer: 리테일러명 (예: 'amazon_gb')
        retailsku: 제품 SKU
        date_yyyymmdd: 'YYYYMMDD' 형식 (예: '20260507')

    Returns:
        삭제된 객체 수
    """
    try:
        if not retailer or not retailsku or not date_yyyymmdd or len(date_yyyymmdd) != 8:
            return 0
        year = date_yyyymmdd[:4]
        year_month = date_yyyymmdd[:6]
        sku = str(retailsku).strip()
        date_dir = f"{S3_PATH_PREFIX}/{year}/{year_month}/{date_yyyymmdd}/{retailer}"
        prefix = f"{date_dir}/{retailer}_{sku}_"

        from config import AWS_CONFIG
        s3_client = _get_s3_client()
        return _delete_existing_screenshots(s3_client, AWS_CONFIG['bucket_name'], prefix)
    except Exception as e:
        logger.warning(f"delete_screenshots_for_sku 실패 (retailer={retailer}, sku={retailsku}): {e}")
        return 0


def capture_and_upload(driver, retailer, retailsku, url):
    """스크린샷 캡처 후 S3 업로드

    동일 (retailer, retailsku, 날짜) 의 기존 스크린샷이 있으면 삭제 후 새로 업로드.
    이를 통해 초기 수집 → auto_recovery 재크롤링 시 자연스럽게 파일이 교체됨.

    Args:
        driver: Selenium WebDriver / DrissionPage ChromiumPage / Playwright Page
        retailer: 리테일러명 (예: 'amazon_gb', 'amazon_es', 'currys', 'fnac', 'x-kom')
        retailsku: 제품 SKU (파일명에 포함)
        url: 현재 페이지 URL (로깅용)

    Returns:
        S3 key 문자열 (성공 시) / None (실패 시)
    """
    try:
        screenshot_bytes = _capture_bytes(driver)
        if not screenshot_bytes:
            logger.warning(f"NULL 스크린샷 캡처 결과 비어있음 (url={url})")
            return None

        # 워터마크: URL 좌상단, KST 시각 우하단
        screenshot_bytes = _add_watermark(screenshot_bytes, url)

        now_kst = datetime.now(KST)
        year = now_kst.strftime('%Y')
        year_month = now_kst.strftime('%Y%m')
        year_month_day = now_kst.strftime('%Y%m%d')
        timestamp = now_kst.strftime('%Y%m%d%H%M%S')

        sku = str(retailsku).strip() if retailsku else 'no_sku'
        file_name = f"{retailer}_{sku}_{timestamp}.png"
        date_dir = f"{S3_PATH_PREFIX}/{year}/{year_month}/{year_month_day}/{retailer}"
        delete_prefix = f"{date_dir}/{retailer}_{sku}_"
        s3_key = f"{date_dir}/{file_name}"

        from config import AWS_CONFIG
        s3_client = _get_s3_client()

        # 동일 SKU의 같은 날짜 기존 스크린샷 제거 (auto_recovery 시 파일 교체)
        _delete_existing_screenshots(s3_client, AWS_CONFIG['bucket_name'], delete_prefix)

        s3_client.put_object(
            Bucket=AWS_CONFIG['bucket_name'],
            Key=s3_key,
            Body=screenshot_bytes,
            ContentType='image/png'
        )
        logger.info(f"NULL 스크린샷 S3 업로드 완료: s3://{AWS_CONFIG['bucket_name']}/{s3_key}")
        return s3_key
    except Exception as e:
        logger.error(f"NULL 스크린샷 처리 실패 (url={url}): {e}")
        return None

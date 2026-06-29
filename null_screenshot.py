"""
NULL 필드 발생 시 스크린샷 캡처 + S3 업로드 헬퍼
- title / imageurl / retailprice 중 하나라도 NULL이면 호출
- Selenium / DrissionPage / Playwright 드라이버 모두 지원
- 버킷은 S3_CONFIG 또는 AWS_CONFIG, S3 키는 모니터링 경로(YYYY/YYYYMM/YYYYMMDD/retailer/) 사용

사용 예 (각 크롤러의 extract_product_info 내부, return 직전):
    from null_screenshot import is_null_result, capture_and_upload
    if is_null_result(result):
        capture_and_upload(self.driver, 'amazon_gb', row_data.get('retailersku', ''), url)
"""
import io
import importlib.util
import logging
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd

from crawler_session import MONITORING_CREATED_ID

logger = logging.getLogger(__name__)

S3_PATH_PREFIX = 'ds-null-screenshots'
KST = timezone(timedelta(hours=9))
CAPTURE_WIDTH = 1920
CAPTURE_HEIGHT = 1080
DEFAULT_NULL_FIELDS = ('title', 'imageurl', 'retailprice')
FULL_NULL_FIELDS = DEFAULT_NULL_FIELDS + ('ships_from', 'sold_by')

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
    'au': 'centrecom',
    'xkom': 'x-kom',
    'bestbuy': 'bestbuy',
}

_s3_client = None
_s3_config = None


def _normalize_retailer(retailer):
    return str(retailer or '').strip().lower()


def _file_sku(retailsku):
    return str(retailsku or '').strip()


def _monitoring_date_parts(date_yyyymmdd=None):
    if date_yyyymmdd:
        year = date_yyyymmdd[:4]
        year_month = date_yyyymmdd[:6]
        year_month_day = date_yyyymmdd
        crawl_date = f"{year}-{date_yyyymmdd[4:6]}-{date_yyyymmdd[6:8]}"
        return year, year_month, year_month_day, crawl_date

    now_kst = datetime.now(KST)
    return (
        now_kst.strftime('%Y'),
        now_kst.strftime('%Y%m'),
        now_kst.strftime('%Y%m%d'),
        now_kst.strftime('%Y-%m-%d')
    )


def _monitoring_file_path(retailer, date_yyyymmdd=None):
    year, year_month, year_month_day, _ = _monitoring_date_parts(date_yyyymmdd)
    retailer_dir = _normalize_retailer(retailer)
    return f"{year}/{year_month}/{year_month_day}/{retailer_dir}/"


def _get_db_connection():
    import pymysql
    from config import DB_CONFIG

    return pymysql.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        database='ssd_crawl_db',
        charset='utf8mb4',
        autocommit=False
    )


def _get_monitoring_target(cursor, retailer):
    retailer_key = _normalize_retailer(retailer)
    cursor.execute("""
        SELECT retailer_id, retailer, country
        FROM ssd_crawl_db.ds_monitoring_targets
        WHERE is_active = 1
          AND is_del = 0
          AND (
              LOWER(retailer) = %s
              OR LOWER(REPLACE(retailer, '_', '-')) = REPLACE(%s, '_', '-')
          )
        ORDER BY is_del ASC
        LIMIT 1
    """, (retailer_key, retailer_key))
    return cursor.fetchone()


def _result_value(result_data, key):
    if result_data is None:
        return None
    try:
        value = result_data.get(key)
    except AttributeError:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _insert_monitoring_file_and_anomaly(retailer, retailsku, url, file_name, file_path, file_size, crawl_date, result_data=None):
    sku_for_db = str(retailsku).strip() if retailsku else ''
    title = _result_value(result_data, 'title')
    retailprice = _result_value(result_data, 'retailprice')
    ships_from = _result_value(result_data, 'ships_from')
    sold_by = _result_value(result_data, 'sold_by')
    imageurl = _result_value(result_data, 'imageurl')
    conn = None
    try:
        conn = _get_db_connection()
        with conn.cursor() as cursor:
            target = _get_monitoring_target(cursor, retailer)
            if not target:
                logger.warning(f"monitoring target 없음: retailer={retailer}")
                conn.rollback()
                return None

            retailer_id, target_retailer, country = target
            now = datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')
            file_prefix = f"{_normalize_retailer(retailer)}_{_file_sku(retailsku)}_%"

            cursor.execute("""
                UPDATE ssd_crawl_db.ds_monitoring_file
                SET is_del = 1, updated_at = %s, updated_id = %s
                WHERE file_path = %s
                  AND file_name LIKE %s
                  AND is_del = 0
            """, (now, MONITORING_CREATED_ID, file_path, file_prefix))

            cursor.execute("""
                SELECT id, screenshot_id
                FROM ssd_crawl_db.ds_monitoring_report_anomaly
                WHERE crawl_date = %s
                  AND retailer_id = %s
                  AND retailersku = %s
                  AND is_del = 0
                ORDER BY id DESC
                LIMIT 1
            """, (crawl_date, retailer_id, sku_for_db))
            existing = cursor.fetchone()

            if existing and existing[1]:
                cursor.execute("""
                    UPDATE ssd_crawl_db.ds_monitoring_file
                    SET is_del = 1, updated_at = %s, updated_id = %s
                    WHERE file_id = %s AND is_del = 0
                """, (now, MONITORING_CREATED_ID, existing[1]))

            cursor.execute("""
                INSERT INTO ssd_crawl_db.ds_monitoring_file
                (file_name, file_path, file_size, file_type, is_del, created_at, created_id)
                VALUES (%s, %s, %s, 'image/png', 0, %s, %s)
            """, (file_name, file_path, file_size, now, MONITORING_CREATED_ID))
            file_id = cursor.lastrowid

            if existing:
                cursor.execute("""
                    UPDATE ssd_crawl_db.ds_monitoring_report_anomaly
                    SET screenshot_id = %s,
                        title = COALESCE(%s, title),
                        retailprice = COALESCE(%s, retailprice),
                        ships_from = COALESCE(%s, ships_from),
                        sold_by = COALESCE(%s, sold_by),
                        imageurl = COALESCE(%s, imageurl),
                        producturl = COALESCE(NULLIF(producturl, ''), %s),
                        country_code = COALESCE(NULLIF(country_code, ''), %s),
                        updated_at = %s,
                        updated_id = %s
                    WHERE id = %s
                """, (
                    file_id,
                    title,
                    retailprice,
                    ships_from,
                    sold_by,
                    imageurl,
                    url,
                    country,
                    now,
                    MONITORING_CREATED_ID,
                    existing[0]
                ))
            else:
                cursor.execute("""
                    INSERT INTO ssd_crawl_db.ds_monitoring_report_anomaly
                    (crawl_date, retailer_id, country_code, title, retailprice, ships_from, sold_by,
                     imageurl, producturl, retailersku, screenshot_id, cause, memo, is_del,
                     created_at, created_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, NULL, 0,
                            %s, %s)
                """, (
                    crawl_date,
                    retailer_id,
                    country or '',
                    title,
                    retailprice,
                    ships_from,
                    sold_by,
                    imageurl,
                    url,
                    sku_for_db,
                    file_id,
                    'crawler_null_capture',
                    now,
                    MONITORING_CREATED_ID
                ))

            conn.commit()
            logger.info(
                f"monitoring screenshot DB 연결 완료: retailer={target_retailer}, "
                f"sku={sku_for_db}, file_id={file_id}"
            )
            return file_id
    except Exception as e:
        if conn:
            conn.rollback()
        logger.warning(f"monitoring screenshot DB 연결 실패 (retailer={retailer}, sku={retailsku}): {e}")
        return None
    finally:
        if conn:
            conn.close()


def _delete_monitoring_db_records(retailer, retailsku, date_yyyymmdd):
    sku_for_db = str(retailsku).strip() if retailsku else ''
    file_sku = _file_sku(retailsku)
    file_path = _monitoring_file_path(retailer, date_yyyymmdd)
    _, _, _, crawl_date = _monitoring_date_parts(date_yyyymmdd)
    retailer_key = _normalize_retailer(retailer)
    file_prefix = f"{retailer_key}_{file_sku}_"
    conn = None

    try:
        conn = _get_db_connection()
        with conn.cursor() as cursor:
            target = _get_monitoring_target(cursor, retailer)
            if not target:
                conn.rollback()
                return 0

            retailer_id = target[0]
            now = datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')

            cursor.execute("""
                UPDATE ssd_crawl_db.ds_monitoring_file
                SET is_del = 1, updated_at = %s, updated_id = %s
                WHERE file_path = %s
                  AND file_name LIKE %s
                  AND is_del = 0
            """, (now, MONITORING_CREATED_ID, file_path, f"{file_prefix}%"))
            file_count = cursor.rowcount

            cursor.execute("""
                UPDATE ssd_crawl_db.ds_monitoring_report_anomaly
                SET is_del = 1,
                    screenshot_id = NULL,
                    updated_at = %s,
                    updated_id = %s
                WHERE crawl_date = %s
                  AND retailer_id = %s
                  AND retailersku = %s
                  AND is_del = 0
            """, (now, MONITORING_CREATED_ID, crawl_date, retailer_id, sku_for_db))
            anomaly_count = cursor.rowcount

            conn.commit()
            return file_count + anomaly_count
    except Exception as e:
        if conn:
            conn.rollback()
        logger.warning(f"monitoring screenshot DB 삭제 실패 (retailer={retailer}, sku={retailsku}): {e}")
        return 0
    finally:
        if conn:
            conn.close()


def _get_s3_client():
    """boto3 S3 클라이언트 lazy 초기화"""
    global _s3_client
    if _s3_client is None:
        import boto3
        s3_config = _get_s3_config()
        _s3_client = boto3.client(
            's3',
            aws_access_key_id=s3_config['access_key_id'],
            aws_secret_access_key=s3_config['secret_access_key'],
            region_name=s3_config['region']
        )
    return _s3_client


def _normalize_s3_config(config):
    if 'bucket_name' in config:
        return config
    return {
        'bucket_name': config['bucket'],
        'access_key_id': config['access_key'],
        'secret_access_key': config['secret_key'],
        'region': config['region'],
    }


def _get_s3_config():
    """Return S3 config from crawler config or monitoring app config."""
    global _s3_config
    if _s3_config is not None:
        return _s3_config

    try:
        from config import S3_CONFIG
        _s3_config = _normalize_s3_config(S3_CONFIG)
        return _s3_config
    except (ImportError, KeyError):
        pass

    try:
        from config import AWS_CONFIG
        _s3_config = _normalize_s3_config(AWS_CONFIG)
        return _s3_config
    except (ImportError, KeyError):
        pass

    monitoring_config = Path(r'C:\samsung_dsdx_monitoring\config\config.py')
    if monitoring_config.exists():
        spec = importlib.util.spec_from_file_location('dsdx_monitoring_config', monitoring_config)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        _s3_config = _normalize_s3_config(module.S3_CONFIG)
        return _s3_config

    raise ImportError('S3 config not found: expected AWS_CONFIG or S3_CONFIG')


def is_null_result(result, fields=None):
    """Return True when monitored product fields are NULL/empty."""
    if result is None:
        return True
    if isinstance(result, dict) and not result:
        return True

    def _is_empty(v):
        if v is None:
            return True
        if isinstance(v, str) and v == '':
            return True
        try:
            empty = pd.isna(v)
            if hasattr(empty, 'all'):
                return bool(empty.all())
            if empty:
                return True
        except (TypeError, ValueError):
            pass
        return False

    monitored_fields = fields or DEFAULT_NULL_FIELDS
    for field in monitored_fields:
        try:
            value = result.get(field)
        except AttributeError:
            value = None
        if _is_empty(value):
            return True
    return False


def _prepare_selenium_for_capture(driver):
    """Normalize Selenium viewport before taking evidence screenshots."""
    try:
        driver.set_window_size(CAPTURE_WIDTH, CAPTURE_HEIGHT)
    except Exception as e:
        logger.debug(f"set_window_size failed before screenshot: {e}")

    try:
        driver.execute_cdp_cmd('Emulation.setDeviceMetricsOverride', {
            'width': CAPTURE_WIDTH,
            'height': CAPTURE_HEIGHT,
            'deviceScaleFactor': 1,
            'mobile': False
        })
    except Exception as e:
        logger.debug(f"CDP viewport override failed before screenshot: {e}")

    try:
        driver.execute_script("""
            window.scrollTo(0, 0);
            document.documentElement.style.overflowX = 'hidden';
            if (document.body) document.body.style.overflowX = 'hidden';
        """)
    except Exception as e:
        logger.debug(f"scroll/overflow normalization failed before screenshot: {e}")

    time.sleep(0.3)


def _prepare_drission_for_capture(driver):
    """Normalize DrissionPage viewport before taking evidence screenshots."""
    try:
        driver.set.window.size(CAPTURE_WIDTH, CAPTURE_HEIGHT)
    except Exception as e:
        logger.debug(f"Drission window resize failed before screenshot: {e}")

    try:
        driver.run_js("""
            window.scrollTo(0, 0);
            document.documentElement.style.overflowX = 'hidden';
            if (document.body) document.body.style.overflowX = 'hidden';
        """)
    except Exception as e:
        logger.debug(f"Drission scroll/overflow normalization failed before screenshot: {e}")

    time.sleep(0.3)


def _prepare_playwright_for_capture(page):
    """Normalize Playwright viewport before taking evidence screenshots."""
    viewport = getattr(page, '_null_screenshot_viewport_size', None) or {'width': CAPTURE_WIDTH, 'height': CAPTURE_HEIGHT}
    try:
        page.set_viewport_size(viewport)
    except Exception as e:
        logger.debug(f"Playwright viewport resize failed before screenshot: {e}")

    try:
        page.evaluate("""
            () => {
                window.scrollTo(0, 0);
                document.documentElement.style.overflowX = 'hidden';
                if (document.body) document.body.style.overflowX = 'hidden';
            }
        """)
    except Exception as e:
        logger.debug(f"Playwright scroll/overflow normalization failed before screenshot: {e}")

    time.sleep(0.3)


def _capture_bytes(driver):
    """드라이버 타입에 따라 PNG bytes 반환"""
    # Selenium WebDriver (uc.Chrome 등)
    if hasattr(driver, 'get_screenshot_as_png'):
        _prepare_selenium_for_capture(driver)
        return driver.get_screenshot_as_png()
    # DrissionPage ChromiumPage
    if hasattr(driver, 'get_screenshot'):
        _prepare_drission_for_capture(driver)
        try:
            return driver.get_screenshot(as_bytes=True)
        except TypeError:
            pass
    # Playwright Page
    if hasattr(driver, 'screenshot') and callable(getattr(driver, 'screenshot')):
        _prepare_playwright_for_capture(driver)
        try:
            return driver.screenshot(full_page=False)
        except TypeError:
            return driver.screenshot()
    raise ValueError(f"지원하지 않는 드라이버 타입: {type(driver).__name__}")


def _add_watermark(screenshot_bytes, url):
    """Add DS-style URL and timestamp labels without changing image dimensions."""
    try:
        from PIL import Image, ImageDraw, ImageFont

        image = Image.open(io.BytesIO(screenshot_bytes)).convert('RGB')
        draw = ImageDraw.Draw(image, 'RGBA')

        try:
            font = ImageFont.truetype("arial.ttf", 20)
        except Exception:
            font = ImageFont.load_default()

        padding = 6
        margin = 10
        max_url_width = max(200, image.width - (margin * 2) - (padding * 2))

        display_url = url
        url_bbox = draw.textbbox((0, 0), display_url, font=font)
        url_width = url_bbox[2] - url_bbox[0]
        while url_width > max_url_width and len(display_url) > 20:
            display_url = display_url[:-4] + '...'
            url_bbox = draw.textbbox((0, 0), display_url, font=font)
            url_width = url_bbox[2] - url_bbox[0]
        url_height = url_bbox[3] - url_bbox[1]

        timestamp_text = datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S KST')
        ts_bbox = draw.textbbox((0, 0), timestamp_text, font=font)
        ts_width = ts_bbox[2] - ts_bbox[0]
        ts_height = ts_bbox[3] - ts_bbox[1]

        url_x = margin
        url_y = margin
        draw.rectangle(
            [url_x - padding, url_y - padding, url_x + url_width + padding, url_y + url_height + padding],
            fill=(0, 0, 0, 200)
        )
        draw.text((url_x, url_y), display_url, font=font, fill=(255, 255, 255, 255))

        ts_x = max(margin, image.width - ts_width - margin)
        ts_y = max(margin, image.height - ts_height - margin)
        draw.rectangle(
            [ts_x - padding, ts_y - padding, ts_x + ts_width + padding, ts_y + ts_height + padding],
            fill=(0, 0, 0, 200)
        )
        draw.text((ts_x, ts_y), timestamp_text, font=font, fill=(255, 255, 255, 255))

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
    if not _is_safe_screenshot_delete_prefix(prefix):
        logger.error(f"Unsafe screenshot delete prefix blocked: {prefix}")
        return 0

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


def _is_safe_screenshot_delete_prefix(prefix):
    """Allow deleting only one SKU's screenshot prefix, never a whole folder."""
    if not prefix or not isinstance(prefix, str):
        return False
    if '*' in prefix or '?' in prefix or '..' in prefix or '\\' in prefix:
        return False
    if not prefix.endswith('_'):
        return False

    parts = prefix.strip('/').split('/')
    if len(parts) == 6 and parts[0] == S3_PATH_PREFIX:
        _, year, year_month, year_month_day, retailer, filename_prefix = parts
    elif len(parts) == 5:
        year, year_month, year_month_day, retailer, filename_prefix = parts
    else:
        return False

    if not (
        year.isdigit() and len(year) == 4 and
        year_month.isdigit() and len(year_month) == 6 and year_month.startswith(year) and
        year_month_day.isdigit() and len(year_month_day) == 8 and year_month_day.startswith(year_month)
    ):
        return False

    retailer = _normalize_retailer(retailer)
    expected_prefix = f"{retailer}_"
    if not retailer or not filename_prefix.startswith(expected_prefix):
        return False

    sku_part = filename_prefix[len(expected_prefix):-1]
    if not sku_part:
        return False

    return True


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
        retailer_key = _normalize_retailer(retailer)
        sku = _file_sku(retailsku)
        legacy_date_dir = f"{S3_PATH_PREFIX}/{year}/{year_month}/{date_yyyymmdd}/{retailer_key}"
        legacy_prefix = f"{legacy_date_dir}/{retailer_key}_{sku}_"
        monitoring_file_path = _monitoring_file_path(retailer_key, date_yyyymmdd)
        monitoring_prefix = f"{monitoring_file_path}{retailer_key}_{sku}_"

        s3_client = _get_s3_client()
        bucket_name = _get_s3_config()['bucket_name']
        deleted = _delete_existing_screenshots(s3_client, bucket_name, legacy_prefix)
        deleted += _delete_existing_screenshots(s3_client, bucket_name, monitoring_prefix)
        deleted += _delete_monitoring_db_records(retailer_key, retailsku, date_yyyymmdd)
        return deleted
    except Exception as e:
        logger.warning(f"delete_screenshots_for_sku 실패 (retailer={retailer}, sku={retailsku}): {e}")
        return 0


def capture_and_upload(driver, retailer, retailsku, url, result_data=None):
    """스크린샷 캡처 후 S3 업로드

    동일 (retailer, retailsku, 날짜) 의 기존 스크린샷이 있으면 삭제 후 새로 업로드.
    이를 통해 초기 수집 → auto_recovery 재크롤링 시 자연스럽게 파일이 교체됨.

    Args:
        driver: Selenium WebDriver / DrissionPage ChromiumPage / Playwright Page
        retailer: 리테일러명 (예: 'amazon_gb', 'amazon_es', 'currys', 'fnac', 'x-kom')
        retailsku: 제품 SKU (파일명에 포함)
        url: 현재 페이지 URL (로깅용)
        result_data: 크롤링 결과 dict/Series. 전달되면 모니터링 anomaly row에 가능한 값을 함께 저장.

    Returns:
        S3 key 문자열 (성공 시) / None (실패 시)
    """
    try:
        if not str(retailsku or '').strip():
            logger.warning(f"NULL screenshot skipped because retailersku is empty (retailer={retailer}, url={url})")
            return None

        screenshot_bytes = _capture_bytes(driver)
        if not screenshot_bytes:
            logger.warning(f"NULL 스크린샷 캡처 결과 비어있음 (url={url})")
            return None

        # 워터마크: URL 좌상단, KST 시각 우하단
        screenshot_bytes = _add_watermark(screenshot_bytes, url)

        now_kst = datetime.now(KST)
        year_month_day = now_kst.strftime('%Y%m%d')
        crawl_date = now_kst.strftime('%Y-%m-%d')
        timestamp = now_kst.strftime('%Y%m%d%H%M%S')

        retailer_key = _normalize_retailer(retailer)
        sku = _file_sku(retailsku)
        file_name = f"{retailer_key}_{sku}_{timestamp}.png"
        file_path = _monitoring_file_path(retailer_key, year_month_day)
        delete_prefix = f"{file_path}{retailer_key}_{sku}_"
        s3_key = f"{file_path}{file_name}"

        s3_client = _get_s3_client()
        bucket_name = _get_s3_config()['bucket_name']

        # 동일 SKU의 같은 날짜 기존 스크린샷 제거 (auto_recovery 시 파일 교체)
        _delete_existing_screenshots(s3_client, bucket_name, delete_prefix)

        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=screenshot_bytes,
            ContentType='image/png'
        )
        file_id = _insert_monitoring_file_and_anomaly(
            retailer_key,
            retailsku,
            url,
            file_name,
            file_path,
            len(screenshot_bytes),
            crawl_date,
            result_data
        )
        logger.info(f"NULL 스크린샷 S3 업로드 완료: s3://{bucket_name}/{s3_key}, file_id={file_id}")
        return s3_key
    except Exception as e:
        logger.error(f"NULL 스크린샷 처리 실패 (url={url}): {e}")
        return None

"""
FNAC price crawler v3 - ZenRows HTML version.

Collection target:
  - title
  - retailprice
  - imageurl

Main rule aligned with fnac_v2.py:
  - If the page says "Stock en ligne epuise", retailprice is NULL even when a
    price is visible.
  - Otherwise, collect the visible representative price from
    .f-faPriceBox__price. Marketplace representative prices are accepted.
  - "Autres offres" prices alone are not used when there is no representative
    price box.
"""

import argparse
import hashlib
import html as html_lib
import json
import logging
import os
import random
import re
import sys
import threading
import time
import unicodedata
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import pandas as pd
import paramiko
import pymysql
import pytz
import requests
from sqlalchemy import create_engine
from sqlalchemy.exc import DBAPIError, OperationalError

from alert_monitor import monitor_and_alert
from config import DB_CONFIG_V2 as DB_CONFIG
from config import FILE_SERVER_CONFIG, ZENROWS_API_KEY
from null_screenshot import capture_and_upload, is_null_result


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)

ZENROWS_API_URL = "https://api.zenrows.com/v1/"
FNAC_TABLE = "fnac_price_crawl_tbl_fr"


def normalize_product_url(url: str) -> str:
    parts = urlsplit(url or "")
    if not parts.query:
        return url or ""
    query = [(key, value) for key, value in parse_qsl(parts.query, keep_blank_values=True) if key.lower() != "oref"]
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def safe_filename(value: Any) -> str:
    text = str(value or "").strip() or "unknown"
    return re.sub(r"[^0-9A-Za-z_.-]+", "_", text)


def setup_stdout() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass


def normalize_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = html_lib.unescape(str(value))
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def normalize_for_match(value: Any) -> str:
    text = normalize_text(value) or ""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return text.lower()


def parse_price(value: Any) -> Optional[float]:
    text = normalize_text(value)
    if not text:
        return None
    text = text.replace("\xa0", " ").replace("€", "")
    text = re.sub(r"[^0-9,.\-]", "", text)
    if not text:
        return None
    if "," in text and "." in text:
        text = text.replace(".", "").replace(",", ".")
    elif "," in text:
        text = text.replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def first_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, list) and value and isinstance(value[0], dict):
        return value[0]
    if isinstance(value, dict):
        return value
    return {}


def nested_get(obj: Dict[str, Any], path: Iterable[str]) -> Any:
    current: Any = obj
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def extract_json_script(page_html: str, script_id: str) -> Optional[Dict[str, Any]]:
    pattern = re.compile(
        r"<script\b[^>]*\bid=[\"']" + re.escape(script_id) + r"[\"'][^>]*>(.*?)</script>",
        re.IGNORECASE | re.DOTALL,
    )
    match = pattern.search(page_html or "")
    if not match:
        return None
    try:
        parsed = json.loads(html_lib.unescape(match.group(1)).strip())
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def extract_json_ld_products(page_html: str) -> List[Dict[str, Any]]:
    products = []
    pattern = re.compile(
        r"<script\b[^>]*type=[\"']application/ld\+json[\"'][^>]*>(.*?)</script>",
        re.IGNORECASE | re.DOTALL,
    )
    for match in pattern.finditer(page_html or ""):
        raw = html_lib.unescape(match.group(1)).strip()
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            continue
        candidates = parsed if isinstance(parsed, list) else [parsed]
        for item in candidates:
            if isinstance(item, dict) and str(item.get("@type", "")).lower() == "product":
                products.append(item)
    return products


def extract_visible_price_texts(page_html: str) -> List[str]:
    patterns = [
        r"<[^>]+class=[\"'][^\"']*f-faPriceBox__price[^\"']*[\"'][^>]*>(.*?)</[^>]+>",
    ]
    autres_match = re.search(r"autres\s+offres", page_html or "", re.IGNORECASE)
    autres_start = autres_match.start() if autres_match else None
    prices = []
    for pattern in patterns:
        for match in re.finditer(pattern, page_html or "", re.IGNORECASE | re.DOTALL):
            if autres_start is not None and match.start() >= autres_start:
                continue
            raw_context = (page_html or "")[max(0, match.start() - 2500):match.end() + 350]
            offer_conditions = re.findall(r"data-condition=[\"']([^\"']+)[\"']", raw_context, re.IGNORECASE)
            if offer_conditions and normalize_for_match(offer_conditions[-1]) not in {"new", "neuf"}:
                continue
            context = normalize_for_match(raw_context)
            if "neufs des" in context or "neuf des" in context:
                continue
            text = normalize_text(match.group(1))
            if text and re.search(r"\d", text):
                prices.append(text)
    deduped = []
    seen = set()
    for price in prices:
        key = normalize_for_match(price)
        if key not in seen:
            deduped.append(price)
            seen.add(key)
    return deduped


def has_online_stock_exhausted(page_html: str) -> bool:
    text = normalize_for_match(page_html)
    return bool(re.search(r"stock\s+en\s+ligne.{0,30}puis", text))


def current_offer_condition(digital_data: Dict[str, Any]) -> Optional[str]:
    product = first_dict(digital_data.get("product"))
    attributes = first_dict(product.get("attributes"))
    current_offer = first_dict(attributes.get("currentOffer"))
    return normalize_text(
        current_offer.get("condition")
        or current_offer.get("productCondition")
        or nested_get(current_offer, ["condition", "label"])
    )


def current_offer_seller(digital_data: Dict[str, Any]) -> Optional[str]:
    product = first_dict(digital_data.get("product"))
    attributes = first_dict(product.get("attributes"))
    current_offer = first_dict(attributes.get("currentOffer"))
    return normalize_text(
        current_offer.get("seller")
        or current_offer.get("sellerName")
        or nested_get(current_offer, ["seller", "name"])
    )


def is_click_and_collect_only(digital_data: Dict[str, Any]) -> bool:
    seller = normalize_for_match(current_offer_seller(digital_data))
    return seller == "clickandcollectonly"


def is_new_condition(condition: Optional[str]) -> bool:
    if not condition:
        return True
    normalized = normalize_for_match(condition)
    return normalized in {"new", "neuf"}


def find_first_image_url(page_html: str, digital_data: Dict[str, Any]) -> Optional[str]:
    for product in extract_json_ld_products(page_html):
        images = product.get("image")
        if isinstance(images, str):
            images = [images]
        if isinstance(images, list):
            for image in images:
                if isinstance(image, str) and "fnac-static.com" in image:
                    return html_lib.unescape(image)

    url_match = re.search(
        r"https?://[^\"'<>\s]+fnac-static\.com[^\"'<>\s]+?\.(?:jpg|jpeg|png|webp)(?:[?#][^\"'<>\s]*)?",
        page_html or "",
        re.IGNORECASE,
    )
    if url_match:
        return html_lib.unescape(url_match.group(0))

    def walk(obj: Any):
        if isinstance(obj, dict):
            for value in obj.values():
                yield value
                yield from walk(value)
        elif isinstance(obj, list):
            for value in obj:
                yield value
                yield from walk(value)

    for value in walk(digital_data):
        if isinstance(value, str) and "fnac-static.com" in value:
            return html_lib.unescape(value)
    return None


def extract_title(page_html: str, digital_data: Dict[str, Any]) -> Optional[str]:
    product = first_dict(digital_data.get("product"))
    product_info = first_dict(product.get("productInfo"))
    title = normalize_text(product_info.get("productName") or product_info.get("name"))
    if title:
        return title

    for product_ld in extract_json_ld_products(page_html):
        title = normalize_text(product_ld.get("name"))
        if title:
            return title

    title_match = re.search(r"<title[^>]*>(.*?)</title>", page_html or "", re.IGNORECASE | re.DOTALL)
    return normalize_text(title_match.group(1)) if title_match else None


class ZenRowsScreenshotPage:
    """Small adapter accepted by null_screenshot.capture_and_upload."""

    def __init__(self, url: str, timeout: int = 30, wait: int = 150):
        self.url = url
        self.timeout = timeout
        self.wait = wait

    def set_viewport_size(self, size: Dict[str, int]) -> None:
        self.viewport = size

    def evaluate(self, script: str) -> None:
        return None

    def screenshot(self, full_page: bool = False) -> bytes:
        session = requests.Session()
        session.trust_env = False
        params = {
            "apikey": ZENROWS_API_KEY,
            "url": self.url,
            "js_render": "true",
            "screenshot": "true",
            "wait": str(self.wait),
        }
        response = session.get(ZENROWS_API_URL, params=params, timeout=self.timeout)
        response.raise_for_status()
        return response.content


class FnacZenRowsScraper:
    def __init__(
        self,
        capture_null: bool = True,
        save_html_dir: Optional[str] = None,
        html_dir: Optional[str] = None,
        fetch_timeout: int = 30,
        fetch_wait: int = 150,
    ):
        self.db_engine = None
        self.country_code = "fr"
        self.korea_tz = pytz.timezone("Asia/Seoul")
        self.local_tz = pytz.timezone("Europe/Paris")
        self.capture_null = capture_null
        self.fetch_timeout = fetch_timeout
        self.fetch_wait = fetch_wait
        self.save_html_dir = Path(save_html_dir) if save_html_dir else None
        self.html_dir = Path(html_dir) if html_dir else None
        if self.save_html_dir:
            self.save_html_dir.mkdir(parents=True, exist_ok=True)
        self._counter_lock = threading.Lock()
        self.total_zenrows_calls = 0
        self.total_screenshot_calls = 0
        self.total_call_seconds = 0.0
        self.total_zenrows_cost = 0.0
        self.error_logs: List[str] = []
        self.setup_db_connection()

    def setup_db_connection(self) -> None:
        try:
            if self.db_engine is not None:
                self.db_engine.dispose()
            connection_string = (
                f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
                f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            )
            self.db_engine = create_engine(connection_string, pool_pre_ping=True, pool_recycle=3600)
            logger.info("DB connection ready")
        except Exception as exc:
            logger.error("DB connection failed: %s", exc)
            self.db_engine = None

    def read_sql_with_retry(self, query: str, retries: int = 3, delay: int = 5) -> pd.DataFrame:
        last_error = None
        for attempt in range(1, retries + 1):
            try:
                if self.db_engine is None:
                    self.setup_db_connection()
                return pd.read_sql(query, self.db_engine)
            except (OperationalError, DBAPIError, pymysql.err.OperationalError) as exc:
                last_error = exc
                logger.warning("DB query failed (%s/%s): %s", attempt, retries, exc)
                try:
                    if self.db_engine is not None:
                        self.db_engine.dispose()
                except Exception:
                    pass
                self.db_engine = None
                if attempt < retries:
                    time.sleep(delay * attempt)
                    self.setup_db_connection()
        raise last_error

    def get_crawl_targets(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        query = """
            SELECT *
            FROM samsung_price_tracking_list
            WHERE country = 'fr'
              AND mall_name = 'fnac'
              AND is_active = TRUE
        """
        if limit:
            query += f" LIMIT {int(limit)}"
        df = self.read_sql_with_retry(query)
        logger.info("FNAC targets loaded: %s", len(df))
        return df.to_dict("records")

    def record_zenrows_call(self, elapsed: float, cost: Optional[str]) -> None:
        with self._counter_lock:
            self.total_zenrows_calls += 1
            self.total_call_seconds += elapsed
            try:
                self.total_zenrows_cost += float(cost or 0)
            except ValueError:
                pass

    def fetch_html(self, url: str, timeout: Optional[int] = None, max_attempts: int = 4) -> Tuple[int, str, Optional[str], float]:
        retry_statuses = {408, 409, 422, 425, 429, 500, 502, 503, 504}
        timeout = timeout or self.fetch_timeout
        last_status = 0
        last_text = ""
        last_cost = None
        total_elapsed = 0.0

        for attempt in range(1, max_attempts + 1):
            session = requests.Session()
            session.trust_env = False
            params = {
                "apikey": ZENROWS_API_KEY,
                "url": url,
                "js_render": "true",
                "wait": str(self.fetch_wait),
            }
            start = time.time()
            try:
                response = session.get(ZENROWS_API_URL, params=params, timeout=timeout)
                elapsed = time.time() - start
                total_elapsed += elapsed
                cost = response.headers.get("X-Request-Cost")
                self.record_zenrows_call(elapsed, cost)

                if response.status_code == 200:
                    if attempt > 1:
                        logger.info("ZenRows fetch recovered on attempt %s/%s", attempt, max_attempts)
                    return response.status_code, response.text or "", cost, total_elapsed

                last_status = response.status_code
                last_text = response.text or ""
                last_cost = cost
                logger.warning(
                    "ZenRows fetch attempt failed (%s/%s): status=%s cost=%s",
                    attempt,
                    max_attempts,
                    response.status_code,
                    cost,
                )
            except requests.RequestException as exc:
                elapsed = time.time() - start
                total_elapsed += elapsed
                self.record_zenrows_call(elapsed, None)
                last_status = 0
                last_text = str(exc)
                last_cost = None
                logger.warning(
                    "ZenRows fetch exception (%s/%s): %s",
                    attempt,
                    max_attempts,
                    exc,
                )

            if attempt < max_attempts and (last_status == 0 or last_status in retry_statuses):
                time.sleep(min(3 * attempt, 10))
                continue
            break

        return last_status, last_text, last_cost, total_elapsed

    def base_result(self, row: Dict[str, Any]) -> Dict[str, Any]:
        now_time = datetime.now(self.korea_tz)
        local_time = datetime.now(self.local_tz)
        crawl_dt = local_time.strftime("%Y-%m-%dT%H:%M:%S")
        tz_offset = local_time.strftime("%z")
        tz_formatted = f"{tz_offset[:3]}:{tz_offset[3:]}" if tz_offset else "+00:00"
        crawl_datetime_iso = f"{crawl_dt}{tz_formatted}"
        return {
            "retailerid": row.get("retailerid", ""),
            "country_code": row.get("country", "fr"),
            "ships_from": "FR",
            "channel_name": "fnac",
            "channel": row.get("channel", "Online"),
            "retailersku": row.get("retailersku", ""),
            "brand": row.get("brand", ""),
            "brand_eng": row.get("brand_eng", row.get("brand", "")),
            "form_factor": row.get("form_factor", ""),
            "segment_lv1": row.get("seg_lv1", ""),
            "segment_lv2": row.get("seg_lv2", ""),
            "segment_lv3": row.get("seg_lv3", ""),
            "capacity": row.get("capacity", ""),
            "item": row.get("item", ""),
            "retailprice": None,
            "sold_by": "Fnac",
            "imageurl": None,
            "producturl": row.get("url", ""),
            "crawl_datetime": crawl_datetime_iso,
            "crawl_strdatetime": local_time.strftime("%Y%m%d%H%M%S") + f"{local_time.microsecond:06d}"[:4],
            "kr_crawl_datetime": now_time.strftime("%Y-%m-%d %H:%M:%S"),
            "kr_crawl_strdatetime": now_time.strftime("%Y%m%d%H%M%S") + f"{now_time.microsecond:06d}"[:4],
            "title": None,
            "vat": row.get("vat", "o"),
        }

    def parse_product(self, page_html: str, row: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
        result = self.base_result(row)
        digital_data = extract_json_script(page_html, "digitalData") or {}
        result["title"] = extract_title(page_html, digital_data)
        result["imageurl"] = find_first_image_url(page_html, digital_data)

        price_texts = extract_visible_price_texts(page_html)
        online_oos = has_online_stock_exhausted(page_html)
        condition = current_offer_condition(digital_data)

        if price_texts:
            visible_price = parse_price(price_texts[0])
            if visible_price is not None:
                if online_oos:
                    return result, "ONLINE_STOCK_EXHAUSTED"
                result["retailprice"] = visible_price
                return result, "VISIBLE_PRICE_BOX"

        if is_click_and_collect_only(digital_data):
            return result, "CLICK_AND_COLLECT_ONLY"

        if not is_new_condition(condition):
            return result, "NON_NEW_OFFER_IGNORED"

        if online_oos:
            return result, "ONLINE_STOCK_EXHAUSTED"

        return result, "PRICE_NOT_FOUND"

    def capture_null_screenshot(self, result: Dict[str, Any], url: str) -> None:
        if not self.capture_null or not is_null_result(result):
            return
        try:
            with self._counter_lock:
                self.total_screenshot_calls += 1
            page = ZenRowsScreenshotPage(url, timeout=self.fetch_timeout, wait=self.fetch_wait)
            capture_and_upload(page, "fnac", result.get("retailersku", ""), url)
        except Exception as exc:
            logger.warning("NULL screenshot failed for sku=%s: %s", result.get("retailersku"), exc)

    def html_cache_path(self, row: Dict[str, Any]) -> Optional[Path]:
        if not self.save_html_dir:
            return None
        idx = row.get("_crawl_index")
        prefix = f"{int(idx):02d}_" if idx else ""
        return self.save_html_dir / f"{prefix}{safe_filename(row.get('retailersku'))}.html"

    def save_html(self, row: Dict[str, Any], page_html: str) -> None:
        path = self.html_cache_path(row)
        if not path:
            return
        try:
            path.write_text(page_html or "", encoding="utf-8", errors="replace")
        except Exception as exc:
            logger.warning("HTML save failed sku=%s path=%s: %s", row.get("retailersku"), path, exc)

    def load_html(self, row: Dict[str, Any]) -> Optional[str]:
        if not self.html_dir:
            return None
        sku = safe_filename(row.get("retailersku"))
        candidates = sorted(self.html_dir.glob(f"*_{sku}.html")) + sorted(self.html_dir.glob(f"{sku}.html"))
        if not candidates:
            logger.warning("HTML cache missing sku=%s dir=%s", row.get("retailersku"), self.html_dir)
            return None
        return candidates[0].read_text(encoding="utf-8", errors="replace")

    def collect_one(self, row: Dict[str, Any]) -> Dict[str, Any]:
        url = row.get("url", "")
        request_url = normalize_product_url(url)
        try:
            if self.html_dir:
                page_html = self.load_html(row)
                status_code, cost, elapsed = (200, None, 0.0) if page_html is not None else (0, None, 0.0)
            else:
                status_code, page_html, cost, elapsed = self.fetch_html(request_url)
                if status_code == 200:
                    self.save_html(row, page_html)
            if status_code != 200:
                result = self.base_result(row)
                reason = f"HTTP_{status_code}"
                logger.warning("Fetch failed sku=%s status=%s cost=%s", row.get("retailersku"), status_code, cost)
            else:
                result, reason = self.parse_product(page_html, row)
            logger.info(
                "sku=%s price=%s title=%s image=%s reason=%s",
                result.get("retailersku"),
                result.get("retailprice"),
                "Y" if result.get("title") else "N",
                "Y" if result.get("imageurl") else "N",
                reason,
            )
            if status_code == 200 and not self.html_dir:
                self.capture_null_screenshot(result, request_url)
            else:
                if status_code != 200:
                    self.error_logs.append(f"{url}: fetch failed {reason}")
                    logger.warning("Skip NULL screenshot for fetch failure sku=%s reason=%s", row.get("retailersku"), reason)
            return result
        except Exception as exc:
            logger.error("Product collection failed url=%s: %s", url, exc)
            self.error_logs.append(f"{url}: {exc}")
            result = self.base_result(row)
            return result

    def collect(self, targets: List[Dict[str, Any]], sleep_seconds: float = 0.2, workers: int = 1) -> List[Dict[str, Any]]:
        indexed_targets = []
        for idx, row in enumerate(targets, start=1):
            row_copy = dict(row)
            row_copy["_crawl_index"] = idx
            indexed_targets.append(row_copy)

        if workers <= 1:
            results = []
            for idx, row in enumerate(indexed_targets, start=1):
                logger.info("[%s/%s] %s", idx, len(indexed_targets), row.get("url"))
                results.append(self.collect_one(row))
                if sleep_seconds and idx < len(indexed_targets):
                    time.sleep(sleep_seconds)
            return results

        results: List[Optional[Dict[str, Any]]] = [None] * len(indexed_targets)
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_idx = {}
            for idx, row in enumerate(indexed_targets, start=1):
                logger.info("[%s/%s] %s", idx, len(indexed_targets), row.get("url"))
                future_to_idx[executor.submit(self.collect_one, row)] = idx - 1
                if sleep_seconds and idx < len(indexed_targets):
                    time.sleep(sleep_seconds)

            for future in as_completed(future_to_idx):
                result_idx = future_to_idx[future]
                try:
                    results[result_idx] = future.result()
                except Exception as exc:
                    row = indexed_targets[result_idx]
                    logger.error("Parallel collection failed url=%s: %s", row.get("url"), exc)
                    self.error_logs.append(f"{row.get('url')}: {exc}")
                    results[result_idx] = self.base_result(row)

        return [result for result in results if result is not None]

    def save_to_db(self, df: pd.DataFrame) -> bool:
        if self.db_engine is None:
            logger.warning("DB is not connected; skip DB save")
            return False
        try:
            df.to_sql(FNAC_TABLE, self.db_engine, if_exists="append", index=False)
            logger.info("DB saved: %s rows to %s", len(df), FNAC_TABLE)

            avg_time = self.total_call_seconds / max(1, self.total_zenrows_calls)
            log_records = []
            for _, row in df.iterrows():
                log_records.append({
                    "country_code": "fr",
                    "url": row["producturl"],
                    "error_message": None if row["retailprice"] is not None else "Price not found",
                    "execution_time": avg_time,
                    "retailprice": row["retailprice"],
                    "crawl_datetime": row["crawl_datetime"],
                })
            if log_records:
                pd.DataFrame(log_records).to_sql("amazon_crawl_logs", self.db_engine, if_exists="append", index=False)
                logger.info("Crawl logs saved: %s rows", len(log_records))
            return True
        except Exception as exc:
            logger.error("DB save failed: %s", exc)
            import traceback
            logger.error(traceback.format_exc())
            return False

    def upload_to_file_server(self, local_file_path: str, date_folder: str) -> bool:
        try:
            transport = paramiko.Transport((FILE_SERVER_CONFIG["host"], FILE_SERVER_CONFIG["port"]))
            transport.connect(
                username=FILE_SERVER_CONFIG["username"],
                password=FILE_SERVER_CONFIG["password"],
            )
            sftp = paramiko.SFTPClient.from_transport(transport)
            country_dir = f"{FILE_SERVER_CONFIG['upload_path']}/{self.country_code}"
            try:
                sftp.stat(country_dir)
            except FileNotFoundError:
                sftp.mkdir(country_dir)

            date_dir = f"{country_dir}/{date_folder}"
            try:
                sftp.stat(date_dir)
            except FileNotFoundError:
                sftp.mkdir(date_dir)

            remote_path = f"{date_dir}/{os.path.basename(local_file_path)}"
            sftp.put(local_file_path, remote_path)
            logger.info("File uploaded: %s", remote_path)
            sftp.close()
            transport.close()
            return True
        except Exception as exc:
            logger.error("File upload failed: %s", exc)
            return False

    def save_results(self, df: pd.DataFrame, save_db: bool = True, upload_server: bool = True) -> Dict[str, bool]:
        local_time = datetime.now(self.local_tz)
        date_str = local_time.strftime("%Y%m%d")
        base_filename = f"{date_str}_{local_time.strftime('%H%M%S')}_fr_fnac"
        results = {"db_saved": False, "server_uploaded": False}

        if save_db:
            results["db_saved"] = self.save_to_db(df)

        if upload_server:
            csv_filename = f"{base_filename}.csv"
            zip_filename = f"{base_filename}.zip"
            md5_filename = f"{base_filename}.md5"
            try:
                column_order = [
                    "retailerid", "country_code", "ships_from", "channel_name", "channel",
                    "retailersku", "brand", "brand_eng", "form_factor",
                    "segment_lv1", "segment_lv2", "segment_lv3", "capacity", "item",
                    "retailprice", "sold_by", "imageurl", "producturl",
                    "crawl_datetime", "crawl_strdatetime", "kr_crawl_datetime", "kr_crawl_strdatetime",
                    "title", "vat",
                ]
                df_csv = df.copy()
                existing_cols = [col for col in column_order if col in df_csv.columns]
                df_csv = df_csv[existing_cols]
                df_csv.columns = df_csv.columns.str.upper()
                df_csv.to_csv(
                    csv_filename,
                    index=False,
                    encoding="utf-8",
                    lineterminator="\r\n",
                    float_format="%g",
                )

                with zipfile.ZipFile(zip_filename, "w", zipfile.ZIP_DEFLATED) as zipf:
                    zipf.write(csv_filename, arcname=csv_filename)

                def calculate_md5(filename: str) -> str:
                    md5 = hashlib.md5()
                    with open(filename, "rb") as f:
                        for chunk in iter(lambda: f.read(4096), b""):
                            md5.update(chunk)
                    return md5.hexdigest()

                with open(md5_filename, "w", encoding="utf-8") as f:
                    f.write(f"{os.path.basename(zip_filename)} {calculate_md5(zip_filename)}\n")
                    f.write(f"{os.path.basename(csv_filename)} {calculate_md5(csv_filename)}\n")

                if self.upload_to_file_server(zip_filename, date_str):
                    if self.upload_to_file_server(md5_filename, date_str):
                        results["server_uploaded"] = True
            except Exception as exc:
                logger.error("Result file save/upload failed: %s", exc)
            finally:
                for temp_file in [csv_filename, zip_filename, md5_filename]:
                    if os.path.exists(temp_file):
                        os.remove(temp_file)

        return results

    def analyze_results(self, df: pd.DataFrame) -> None:
        total = len(df)
        with_price = int(df["retailprice"].notna().sum()) if "retailprice" in df else 0
        without_price = total - with_price
        logger.info("=== FNAC v3 summary ===")
        logger.info("targets=%s", total)
        logger.info("price_non_null=%s", with_price)
        logger.info("price_null=%s", without_price)
        logger.info("title_non_null=%s", int(df["title"].notna().sum()) if "title" in df else 0)
        logger.info("image_non_null=%s", int(df["imageurl"].notna().sum()) if "imageurl" in df else 0)
        logger.info("zenrows_calls=%s", self.total_zenrows_calls)
        logger.info("screenshot_calls=%s", self.total_screenshot_calls)
        logger.info("zenrows_cost_sum_usd=%0.10f", self.total_zenrows_cost)
        logger.info("elapsed_sec=%0.1f", self.total_call_seconds)


def main() -> None:
    setup_stdout()
    parser = argparse.ArgumentParser(description="FNAC ZenRows crawler v3")
    parser.add_argument("--limit", type=int, default=None, help="Limit active FNAC targets")
    parser.add_argument("--dry-run", action="store_true", help="Skip DB/SFTP save and screenshots")
    parser.add_argument("--no-db", action="store_true", help="Skip DB save")
    parser.add_argument("--no-upload", action="store_true", help="Skip SFTP upload")
    parser.add_argument("--no-capture-null", action="store_true", help="Skip NULL screenshots")
    parser.add_argument("--sleep", type=float, default=0.0, help="Delay between ZenRows request submissions")
    parser.add_argument("--workers", type=int, default=3, help="Parallel product workers")
    parser.add_argument("--timeout", type=int, default=30, help="ZenRows HTML request timeout seconds")
    parser.add_argument("--wait", type=int, default=150, help="ZenRows js_render wait milliseconds")
    parser.add_argument("--save-html", default=None, help="Override fetched HTML save directory")
    parser.add_argument("--no-save-html", action="store_true", help="Disable default HTML saving to fnac_log/YYYYMMDD")
    parser.add_argument("--html-dir", default=None, help="Parse saved HTML from this directory instead of calling ZenRows")
    args = parser.parse_args()

    log_enabled = False
    try:
        from log_utils import save_log, setup_log
        setup_log("fr_fnac_v3")
        log_enabled = True
    except Exception:
        save_log = None

    if args.html_dir:
        html_dir = Path(args.html_dir)
        if not html_dir.is_dir():
            logger.error("--html-dir does not exist or is not a directory: %s", args.html_dir)
            raise SystemExit(2)
        if args.save_html:
            logger.error("--html-dir cannot be combined with --save-html")
            raise SystemExit(2)
        if not args.dry_run:
            logger.error("--html-dir is replay-only. Use --dry-run so DB/SFTP/NULL screenshots are not skipped in production.")
            raise SystemExit(2)

    save_html_dir = None
    if not args.html_dir and not args.no_save_html:
        save_html_dir = args.save_html or os.path.join(
            "fnac_log",
            datetime.now(pytz.timezone("Asia/Seoul")).strftime("%Y%m%d"),
        )
        logger.info("HTML save enabled: %s", save_html_dir)

    if args.html_dir:
        logger.info("HTML replay enabled: %s", args.html_dir)

    capture_null = (not args.no_capture_null) and (not args.dry_run) and (not args.html_dir)
    scraper = FnacZenRowsScraper(
        capture_null=capture_null,
        save_html_dir=save_html_dir,
        html_dir=args.html_dir,
        fetch_timeout=args.timeout,
        fetch_wait=args.wait,
    )
    target_count = 0
    results_df = None

    def send_alert(error_message=None):
        if args.dry_run:
            return
        try:
            monitor_and_alert(
                "fr_fnac_v3",
                target_count,
                results_df,
                error_message=error_message,
                error_logs=scraper.error_logs,
            )
        except Exception as exc:
            logger.warning("Alert failed: %s", exc)

    try:
        if scraper.db_engine is None:
            send_alert("DB connection failed")
            return

        targets = scraper.get_crawl_targets(limit=args.limit)
        target_count = len(targets)
        if not targets:
            send_alert("No FNAC targets")
            return

        results = scraper.collect(targets, sleep_seconds=args.sleep, workers=max(1, args.workers))
        results_df = pd.DataFrame(results)
        scraper.analyze_results(results_df)

        if args.dry_run:
            preview_cols = ["retailersku", "title", "retailprice", "imageurl", "producturl"]
            preview_cols = [col for col in preview_cols if col in results_df.columns]
            print(results_df[preview_cols].to_string(index=False, max_colwidth=80))
            logger.info("--dry-run: skipped DB/SFTP/screenshots")
        else:
            scraper.save_results(
                results_df,
                save_db=not args.no_db,
                upload_server=not args.no_upload,
            )
            send_alert()

        logger.info("FNAC v3 completed")
    except Exception as exc:
        logger.error("FNAC v3 fatal error: %s", exc)
        import traceback
        detail = traceback.format_exc()
        logger.error(detail)
        scraper.error_logs.append(detail)
        send_alert(str(exc))
    finally:
        if log_enabled and save_log:
            try:
                save_log("fr_fnac_v3")
            except Exception:
                pass


if __name__ == "__main__":
    main()

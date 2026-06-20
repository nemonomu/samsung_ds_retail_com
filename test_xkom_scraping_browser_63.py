import asyncio
import csv
import json
import os
import time
from datetime import datetime
from pathlib import Path

from playwright.async_api import async_playwright
from sqlalchemy import text

from test_xkom_scraping_browser import (
    connection_url,
    db_engine,
    detect_state,
    extract_product_info_v2_logic,
    load_xpaths_from_db,
    safe_text,
)

try:
    from config import (
        XKOM_SCRAPING_BROWSER_63_LIMIT,
        XKOM_SCRAPING_BROWSER_63_SAVE_HTML,
        XKOM_SCRAPING_BROWSER_63_SAVE_SCREENSHOTS,
    )
except ImportError:
    XKOM_SCRAPING_BROWSER_63_LIMIT = 63
    XKOM_SCRAPING_BROWSER_63_SAVE_HTML = True
    XKOM_SCRAPING_BROWSER_63_SAVE_SCREENSHOTS = False


OUT_ROOT = Path("xkom_probe_outputs") / "scraping_browser_63"
OUT_ROOT.mkdir(parents=True, exist_ok=True)


def bool_config(name, default):
    value = os.environ.get(name)
    if value is None:
        value = default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def int_config(name, default):
    value = os.environ.get(name)
    if value is None:
        value = default
    return int(value)


def load_targets(limit):
    query = text("""
        SELECT id, retailersku, url
        FROM samsung_price_tracking_list
        WHERE country = 'pl'
          AND mall_name = 'x-kom'
          AND is_active = TRUE
        ORDER BY id
        LIMIT :limit
    """)
    with db_engine().connect() as conn:
        rows = conn.execute(query, {"limit": limit}).mappings().all()
    return [dict(row, source="db") for row in rows]


def safe_name(value):
    return "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in str(value))[:80]


def compact_record(record):
    x = record.get("xkom_v2_logic") or {}
    state = record.get("state_diagnostics") or {}
    target = record.get("target") or {}
    return {
        "idx": record.get("idx"),
        "id": target.get("id"),
        "retailersku": target.get("retailersku"),
        "status": record.get("status"),
        "retailprice": x.get("retailprice"),
        "is_wycofany": x.get("is_wycofany"),
        "cloudflare_or_captcha": state.get("cloudflare_or_captcha"),
        "temporary_unavailable_text_seen": state.get("temporary_unavailable_text_seen"),
        "notify_availability_text_seen": state.get("notify_availability_text_seen"),
        "add_to_cart_text_seen": state.get("add_to_cart_text_seen"),
        "price_selector": x.get("price_selector"),
        "title_selector": x.get("title_selector"),
        "image_selector": x.get("image_selector"),
        "elapsed_seconds": record.get("elapsed_seconds"),
        "error": record.get("error"),
        "url": target.get("url"),
        "current_url": record.get("current_url"),
    }


async def collect_one(page, target, xpaths, idx, total, run_dir, save_html, save_screenshots):
    item_start = time.perf_counter()
    sku = target.get("retailersku") or target.get("id") or idx
    html_path = run_dir / f"{idx:03d}_{safe_name(sku)}.html" if save_html else None
    png_path = run_dir / f"{idx:03d}_{safe_name(sku)}.png" if save_screenshots else None

    record = {
        "idx": idx,
        "target": target,
        "url": target.get("url"),
        "current_url": None,
        "status": None,
        "page_title": None,
        "state_diagnostics": None,
        "xkom_v2_logic": None,
        "html_path": str(html_path) if html_path else None,
        "screenshot_path": str(png_path) if png_path else None,
        "elapsed_seconds": None,
        "error": None,
    }

    try:
        response = await page.goto(target["url"], wait_until="domcontentloaded", timeout=90000)
        await page.wait_for_timeout(12000)
        try:
            await page.wait_for_load_state("networkidle", timeout=20000)
        except Exception:
            pass

        title = await page.title()
        body_text = await safe_text(page)
        html = await page.content()
        state = detect_state(title, body_text, html)
        extracted = await extract_product_info_v2_logic(page, xpaths)

        if html_path:
            html_path.write_text(html, encoding="utf-8")
        if png_path:
            await page.screenshot(path=str(png_path), full_page=True)

        record.update({
            "current_url": page.url,
            "status": response.status if response else None,
            "page_title": title,
            "state_diagnostics": state,
            "xkom_v2_logic": extracted,
            "body_text_sample": body_text[:2000],
        })
    except Exception as exc:
        record["error"] = repr(exc)
    finally:
        record["elapsed_seconds"] = round(time.perf_counter() - item_start, 3)

    compact = compact_record(record)
    print(json.dumps({"progress": f"{idx}/{total}", **compact}, ensure_ascii=False))
    return record


async def run():
    limit = int_config("XKOM_SCRAPING_BROWSER_63_LIMIT", XKOM_SCRAPING_BROWSER_63_LIMIT or 63)
    save_html = bool_config("XKOM_SCRAPING_BROWSER_63_SAVE_HTML", XKOM_SCRAPING_BROWSER_63_SAVE_HTML)
    save_screenshots = bool_config("XKOM_SCRAPING_BROWSER_63_SAVE_SCREENSHOTS", XKOM_SCRAPING_BROWSER_63_SAVE_SCREENSHOTS)

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = OUT_ROOT / f"run_{run_id}"
    run_dir.mkdir(parents=True, exist_ok=True)
    summary_json = run_dir / f"xkom_scraping_browser_63_summary_{run_id}.json"
    summary_csv = run_dir / f"xkom_scraping_browser_63_summary_{run_id}.csv"

    targets = load_targets(limit)
    if not targets:
        raise RuntimeError("No active x-kom targets found")
    xpaths = load_xpaths_from_db()

    start = time.perf_counter()
    records = []

    async with async_playwright() as p:
        browser = await p.chromium.connect_over_cdp(connection_url())
        context = browser.contexts[0] if browser.contexts else await browser.new_context()
        page = await context.new_page()
        try:
            for idx, target in enumerate(targets, start=1):
                records.append(await collect_one(
                    page=page,
                    target=target,
                    xpaths=xpaths,
                    idx=idx,
                    total=len(targets),
                    run_dir=run_dir,
                    save_html=save_html,
                    save_screenshots=save_screenshots,
                ))
        finally:
            await browser.close()

    total_elapsed = round(time.perf_counter() - start, 3)
    compact_rows = [compact_record(record) for record in records]
    non_null = sum(1 for row in compact_rows if row.get("retailprice") is not None)
    null_count = len(compact_rows) - non_null
    error_count = sum(1 for row in compact_rows if row.get("error"))
    captcha_count = sum(1 for row in compact_rows if row.get("cloudflare_or_captcha"))
    wycofany_count = sum(1 for row in compact_rows if row.get("is_wycofany"))

    summary = {
        "run_id": run_id,
        "requested_limit": limit,
        "target_count": len(targets),
        "completed_count": len(records),
        "non_null_price_count": non_null,
        "null_price_count": null_count,
        "error_count": error_count,
        "captcha_count": captcha_count,
        "wycofany_count": wycofany_count,
        "total_elapsed_seconds": total_elapsed,
        "avg_elapsed_seconds_per_item": round(total_elapsed / len(records), 3) if records else None,
        "save_html": save_html,
        "save_screenshots": save_screenshots,
        "run_dir": str(run_dir),
        "summary_json": str(summary_json),
        "summary_csv": str(summary_csv),
        "selector_counts": {key: len(value) for key, value in xpaths.items()},
        "records": records,
        "compact_rows": compact_rows,
    }
    summary_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    with summary_csv.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(compact_rows[0].keys()))
        writer.writeheader()
        writer.writerows(compact_rows)

    final_log = {
        "target_count": len(targets),
        "completed_count": len(records),
        "non_null_price_count": non_null,
        "null_price_count": null_count,
        "error_count": error_count,
        "captcha_count": captcha_count,
        "wycofany_count": wycofany_count,
        "total_elapsed_seconds": total_elapsed,
        "avg_elapsed_seconds_per_item": summary["avg_elapsed_seconds_per_item"],
        "summary_json": str(summary_json),
        "summary_csv": str(summary_csv),
        "run_dir": str(run_dir),
    }
    print("FINAL_SUMMARY " + json.dumps(final_log, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(run())

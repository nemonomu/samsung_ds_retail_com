import asyncio
import json
import os
import re
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode

from playwright.async_api import async_playwright
from sqlalchemy import create_engine, text

from config import DB_CONFIG_V2, ZENROWS_API_KEY


OUT_DIR = Path("xkom_probe_outputs") / "scraping_browser"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def db_engine():
    cfg = DB_CONFIG_V2
    return create_engine(
        f"mysql+pymysql://{cfg['user']}:{cfg['password']}@"
        f"{cfg['host']}:{cfg['port']}/{cfg['database']}"
    )


def load_test_target_from_db():
    sku = os.environ.get("XKOM_SCRAPING_BROWSER_RETAILERSKU")
    limit = int(os.environ.get("XKOM_SCRAPING_BROWSER_DB_LIMIT", "1"))
    where = [
        "country = 'pl'",
        "mall_name = 'x-kom'",
        "is_active = TRUE",
    ]
    params = {"limit": limit}
    if sku:
        where.append("retailersku = :retailersku")
        params["retailersku"] = sku

    query = text(f"""
        SELECT id, retailersku, url
        FROM samsung_price_tracking_list
        WHERE {' AND '.join(where)}
        ORDER BY id
        LIMIT :limit
    """)
    with db_engine().connect() as conn:
        row = conn.execute(query, params).mappings().first()
    if not row:
        raise RuntimeError("No active x-kom target found in samsung_price_tracking_list")
    return dict(row)


def default_test_target():
    env_url = os.environ.get("XKOM_SCRAPING_BROWSER_TEST_URL")
    if env_url:
        return {"id": None, "retailersku": None, "url": env_url, "source": "env_url"}
    target = load_test_target_from_db()
    target["source"] = "db"
    return target


def connection_url():
    query = {
        "apikey": ZENROWS_API_KEY,
        "proxy_country": os.environ.get("ZENROWS_SCRAPING_BROWSER_COUNTRY", "pl"),
        "session_ttl": os.environ.get("ZENROWS_SCRAPING_BROWSER_SESSION_TTL", "5m"),
    }
    return "wss://browser.zenrows.com?" + urlencode(query)


def extract_price_from_text(text_value):
    patterns = [
        r"Cena:\s*([0-9][0-9\s]*[,.][0-9]{2})\s*z(?:l|ł)",
        r"([0-9][0-9\s]*[,.][0-9]{2})\s*z(?:l|ł)",
        r"([0-9][0-9\s]{2,})\s*z(?:l|ł)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text_value or "", flags=re.IGNORECASE)
        if match:
            raw = match.group(1).replace(" ", "").replace(",", ".")
            try:
                return float(raw)
            except ValueError:
                continue
    return None


def detect_state(title, body_text, html):
    merged = "\n".join([title or "", body_text or "", html or ""]).lower()
    return {
        "cloudflare_or_captcha": any(
            marker in merged
            for marker in [
                "verify you are human",
                "verifying you are human",
                "just a moment",
                "checking your browser",
                "cf-turnstile",
                "challenges.cloudflare.com",
                "g-recaptcha",
            ]
        ),
        "wycofany": "produkt wycofany" in merged or "wycofany" in merged,
        "temporary_unavailable": "czasowo niedost" in merged,
        "notify_availability": "powiadom mnie o dost" in merged,
        "add_to_cart": "dodaj do koszyka" in merged,
    }


async def safe_text(page):
    try:
        return await page.locator("body").inner_text(timeout=10000)
    except Exception:
        return ""


async def meta_price(page):
    for selector in [
        'meta[property="product:price:amount"]',
        'meta[itemprop="price"]',
        '[data-name="Price"]',
    ]:
        try:
            locator = page.locator(selector).first
            if await locator.count() == 0:
                continue
            content = await locator.get_attribute("content", timeout=3000)
            text_value = content or await locator.inner_text(timeout=3000)
            price = extract_price_from_text(text_value)
            if price is not None:
                return price, selector, text_value
        except Exception:
            continue
    return None, None, None


async def run():
    target = default_test_target()
    url = target["url"]
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    html_path = OUT_DIR / f"xkom_scraping_browser_{run_id}.html"
    png_path = OUT_DIR / f"xkom_scraping_browser_{run_id}.png"
    meta_path = OUT_DIR / f"xkom_scraping_browser_{run_id}.json"

    async with async_playwright() as p:
        browser = await p.chromium.connect_over_cdp(connection_url())
        context = browser.contexts[0] if browser.contexts else await browser.new_context()
        page = await context.new_page()
        try:
            response = await page.goto(url, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(12000)
            try:
                await page.wait_for_load_state("networkidle", timeout=20000)
            except Exception:
                pass

            title = await page.title()
            current_url = page.url
            body_text = await safe_text(page)
            html = await page.content()
            price_meta, price_source, price_source_text = await meta_price(page)
            price_text = extract_price_from_text(body_text)
            state = detect_state(title, body_text, html)

            await page.screenshot(path=str(png_path), full_page=True)
            html_path.write_text(html, encoding="utf-8")

            result = {
                "target": target,
                "url": url,
                "current_url": current_url,
                "status": response.status if response else None,
                "title": title,
                "state": state,
                "price_meta": price_meta,
                "price_text": price_text,
                "price_source": price_source,
                "price_source_text": price_source_text,
                "body_text_sample": body_text[:3000],
                "html_path": str(html_path),
                "screenshot_path": str(png_path),
            }
            meta_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
            print(json.dumps({
                "target": target,
                "status": result["status"],
                "title": title,
                "current_url": current_url,
                "state": state,
                "price_meta": price_meta,
                "price_text": price_text,
                "html_path": str(html_path),
                "screenshot_path": str(png_path),
                "meta_path": str(meta_path),
            }, ensure_ascii=False, indent=2))
        finally:
            await browser.close()


if __name__ == "__main__":
    asyncio.run(run())

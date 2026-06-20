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

try:
    from config import (
        XKOM_SCRAPING_BROWSER_DB_LIMIT,
        XKOM_SCRAPING_BROWSER_RETAILERSKU,
        XKOM_SCRAPING_BROWSER_TEST_URL,
        ZENROWS_SCRAPING_BROWSER_COUNTRY,
        ZENROWS_SCRAPING_BROWSER_SESSION_TTL,
    )
except ImportError:
    XKOM_SCRAPING_BROWSER_DB_LIMIT = 1
    XKOM_SCRAPING_BROWSER_RETAILERSKU = None
    XKOM_SCRAPING_BROWSER_TEST_URL = None
    ZENROWS_SCRAPING_BROWSER_COUNTRY = "pl"
    ZENROWS_SCRAPING_BROWSER_SESSION_TTL = "5m"


OUT_DIR = Path("xkom_probe_outputs") / "scraping_browser"
OUT_DIR.mkdir(parents=True, exist_ok=True)


WYCOFANY_XPATH = '//*[@id="app"]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div/div[1]/div/button/span/span[1]/span'


def db_engine():
    cfg = DB_CONFIG_V2
    return create_engine(
        f"mysql+pymysql://{cfg['user']}:{cfg['password']}@"
        f"{cfg['host']}:{cfg['port']}/{cfg['database']}"
    )


def load_xpaths_from_db():
    query = text("""
        SELECT element_type, selector_value, priority
        FROM mall_selectors
        WHERE mall_name = 'x-kom'
          AND country_code = 'pl'
          AND is_active = TRUE
        ORDER BY element_type, priority DESC
    """)
    xpaths = {"price": [], "title": [], "imageurl": [], "availability": []}
    with db_engine().connect() as conn:
        rows = conn.execute(query).mappings().all()
    for row in rows:
        xpaths.setdefault(row["element_type"], []).append(row["selector_value"])
    return xpaths


def load_test_target_from_db():
    sku = os.environ.get("XKOM_SCRAPING_BROWSER_RETAILERSKU") or XKOM_SCRAPING_BROWSER_RETAILERSKU
    limit = int(os.environ.get("XKOM_SCRAPING_BROWSER_DB_LIMIT") or XKOM_SCRAPING_BROWSER_DB_LIMIT or 1)
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
    env_url = os.environ.get("XKOM_SCRAPING_BROWSER_TEST_URL") or XKOM_SCRAPING_BROWSER_TEST_URL
    if env_url:
        return {"id": None, "retailersku": None, "url": env_url, "source": "env_url"}
    target = load_test_target_from_db()
    target["source"] = "db"
    return target


def connection_url():
    query = {
        "apikey": ZENROWS_API_KEY,
        "proxy_country": os.environ.get("ZENROWS_SCRAPING_BROWSER_COUNTRY") or ZENROWS_SCRAPING_BROWSER_COUNTRY or "pl",
        "session_ttl": os.environ.get("ZENROWS_SCRAPING_BROWSER_SESSION_TTL") or ZENROWS_SCRAPING_BROWSER_SESSION_TTL or "5m",
    }
    return "wss://browser.zenrows.com?" + urlencode(query)


def parse_xkom_price(price_text):
    text_value = (price_text or "").replace("zł", "").replace("z\u0142", "").replace("PLN", "").replace(" ", "").strip()
    match = re.search(r"(\d+)[,.]?(\d*)", text_value)
    if not match:
        return None
    price = match.group(1)
    if match.group(2):
        price += "." + match.group(2)
    try:
        return float(price)
    except ValueError:
        return None


def locator_for_selector(page, selector):
    if selector.startswith("//"):
        return page.locator(f"xpath={selector}")
    return page.locator(selector)


async def first_locator_text(locator, timeout=3000):
    if await locator.count() == 0:
        return None
    first = locator.first
    try:
        return (await first.inner_text(timeout=timeout)).strip()
    except Exception:
        return None


async def first_locator_attr(locator, attr, timeout=3000):
    if await locator.count() == 0:
        return None
    try:
        return await locator.first.get_attribute(attr, timeout=timeout)
    except Exception:
        return None


async def detect_wycofany_v2(page):
    try:
        text_value = await first_locator_text(page.locator(f"xpath={WYCOFANY_XPATH}"))
        if text_value and "Produkt wycofany" in text_value:
            return True, "xpath"
    except Exception:
        pass
    return False, None


async def extract_product_info_v2_logic(page, xpaths):
    result = {
        "retailprice": None,
        "title": None,
        "imageurl": None,
        "is_wycofany": False,
        "wycofany_source": None,
        "price_selector": None,
        "title_selector": None,
        "image_selector": None,
    }

    is_wycofany, wycofany_source = await detect_wycofany_v2(page)
    result["is_wycofany"] = is_wycofany
    result["wycofany_source"] = wycofany_source

    price_found = False
    for selector in ([] if is_wycofany else xpaths.get("price", [])):
        try:
            if selector.startswith("meta"):
                text_value = await first_locator_attr(page.locator(selector), "content")
                price = parse_xkom_price(text_value)
                if price is not None:
                    result["retailprice"] = price
                    result["price_selector"] = selector
                    price_found = True
                    break
                continue

            locator = locator_for_selector(page, selector)
            count = await locator.count()
            for index in range(count):
                price_text = (await locator.nth(index).inner_text(timeout=3000)).strip()
                if not price_text:
                    continue
                price = parse_xkom_price(price_text)
                if price is not None:
                    result["retailprice"] = price
                    result["price_selector"] = selector
                    price_found = True
                    break
            if price_found:
                break
        except Exception:
            continue

    for selector in xpaths.get("title", []):
        try:
            if selector.startswith("meta"):
                value = await first_locator_attr(page.locator(selector), "content")
            else:
                value = await first_locator_text(locator_for_selector(page, selector))
            if value:
                result["title"] = value
                result["title_selector"] = selector
                break
        except Exception:
            continue

    for selector in xpaths.get("imageurl", []):
        try:
            if selector.startswith("meta"):
                value = await first_locator_attr(page.locator(selector), "content")
            else:
                value = await first_locator_attr(locator_for_selector(page, selector), "src")
            if value:
                result["imageurl"] = value
                result["image_selector"] = selector
                break
        except Exception:
            continue

    return result


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
        "wycofany_text_seen": "produkt wycofany" in merged or "wycofany" in merged,
        "temporary_unavailable_text_seen": "czasowo niedost" in merged,
        "notify_availability_text_seen": "powiadom mnie o dost" in merged,
        "add_to_cart_text_seen": "dodaj do koszyka" in merged,
    }


async def safe_text(page):
    try:
        return await page.locator("body").inner_text(timeout=10000)
    except Exception:
        return ""


async def run():
    target = default_test_target()
    xpaths = load_xpaths_from_db()
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
            state = detect_state(title, body_text, html)
            extracted = await extract_product_info_v2_logic(page, xpaths)

            await page.screenshot(path=str(png_path), full_page=True)
            html_path.write_text(html, encoding="utf-8")

            result = {
                "target": target,
                "url": url,
                "current_url": current_url,
                "status": response.status if response else None,
                "page_title": title,
                "state_diagnostics": state,
                "xkom_v2_logic": extracted,
                "selector_counts": {key: len(value) for key, value in xpaths.items()},
                "body_text_sample": body_text[:3000],
                "html_path": str(html_path),
                "screenshot_path": str(png_path),
            }
            meta_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
            print(json.dumps({
                "target": target,
                "status": result["status"],
                "page_title": title,
                "current_url": current_url,
                "state_diagnostics": state,
                "xkom_v2_logic": extracted,
                "selector_counts": result["selector_counts"],
                "html_path": str(html_path),
                "screenshot_path": str(png_path),
                "meta_path": str(meta_path),
            }, ensure_ascii=False, indent=2))
        finally:
            await browser.close()


if __name__ == "__main__":
    asyncio.run(run())

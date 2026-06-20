import asyncio
import json
import os
import re
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode

import pandas as pd
from playwright.async_api import async_playwright

from config import ZENROWS_API_KEY


OUT_DIR = Path("xkom_probe_outputs") / "scraping_browser"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def default_test_url():
    env_url = os.environ.get("XKOM_SCRAPING_BROWSER_TEST_URL")
    if env_url:
        return env_url

    sample_csv = Path("xkom_probe_outputs/xkom_compare_63_output_20260613_180215.csv")
    if sample_csv.exists():
        df = pd.read_csv(sample_csv, encoding="utf-8-sig")
        if "producturl" in df.columns and df["producturl"].notna().any():
            return str(df[df["producturl"].notna()].iloc[0]["producturl"])

    return "https://www.x-kom.pl"


def connection_url():
    query = {
        "apikey": ZENROWS_API_KEY,
        "proxy_country": os.environ.get("ZENROWS_SCRAPING_BROWSER_COUNTRY", "pl"),
        "session_ttl": os.environ.get("ZENROWS_SCRAPING_BROWSER_SESSION_TTL", "5m"),
    }
    return "wss://browser.zenrows.com?" + urlencode(query)


def extract_price_from_text(text):
    patterns = [
        r"Cena:\s*([0-9][0-9\s]*[,.][0-9]{2})\s*zł",
        r"([0-9][0-9\s]*[,.][0-9]{2})\s*zł",
        r"([0-9][0-9\s]{2,})\s*zł",
    ]
    for pattern in patterns:
        match = re.search(pattern, text or "", flags=re.IGNORECASE)
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
        "temporary_unavailable": "czasowo niedostępny" in merged,
        "notify_availability": "powiadom mnie o dostępności" in merged,
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
            text = content or await locator.inner_text(timeout=3000)
            price = extract_price_from_text(text)
            if price is not None:
                return price, selector, text
        except Exception:
            continue
    return None, None, None


async def run():
    url = default_test_url()
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

"""Inspect Coolblue NL retail price XPath candidates on random DB URLs.

Examples:
    python coolblue_xpath_inspector.py --xpath "//span[contains(@class,'sales-price')]"
    python coolblue_xpath_inspector.py --limit 10 --seed 42 --include-db-xpaths --xpath "..."
"""
import argparse
import os
import random
import re
import sys
import time
from datetime import datetime

import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from coolblue_nl_v2 import CoolblueScraper


DEFAULT_XPATHS = [
    '//*[@id="order-form"]/div[1]/div[1]/div/div/div/div/div/p/span',
]


def parse_price(text):
    """Return a float from a Coolblue-style EUR price text, or None."""
    if not text:
        return None

    normalized = (
        text.replace("\xa0", " ")
        .replace("EUR", "")
        .replace("€", "")
        .strip()
    )
    match = re.search(r"(\d{1,3}(?:\.\d{3})*(?:,\d{2})?|\d+(?:,\d{2})?)", normalized)
    if not match:
        return None

    number = match.group(1).replace(".", "").replace(",", ".")
    try:
        return float(number)
    except ValueError:
        return None


def short_text(text, max_len=120):
    if text is None:
        return ""
    compact = re.sub(r"\s+", " ", text).strip()
    if len(compact) <= max_len:
        return compact
    return compact[: max_len - 3] + "..."


def load_random_targets(scraper, limit, seed=None):
    targets = scraper.get_crawl_targets()
    if not targets:
        return []

    rng = random.Random(seed)
    sample_size = min(limit, len(targets))
    return rng.sample(targets, sample_size)


def inspect_xpath(driver, wait, xpath):
    started = time.perf_counter()
    try:
        element = wait.until(EC.presence_of_element_located((By.XPATH, xpath)))
        visible = element.is_displayed()
        text = element.text.strip()
        price = parse_price(text)
        return {
            "matched": True,
            "visible": visible,
            "text": text,
            "price": price,
            "elapsed_ms": round((time.perf_counter() - started) * 1000),
            "error": "",
        }
    except Exception as exc:
        return {
            "matched": False,
            "visible": False,
            "text": "",
            "price": None,
            "elapsed_ms": round((time.perf_counter() - started) * 1000),
            "error": short_text(str(exc), 180),
        }


def build_xpath_list(scraper, cli_xpaths, include_db_xpaths):
    xpaths = []
    if include_db_xpaths or not cli_xpaths:
        xpaths.extend(DEFAULT_XPATHS)
        if include_db_xpaths:
            xpaths.extend(scraper.XPATHS.get("price", []))
    xpaths.extend(cli_xpaths or [])

    unique = []
    seen = set()
    for xpath in xpaths:
        if xpath and xpath not in seen:
            unique.append(xpath)
            seen.add(xpath)
    return unique


def print_summary(results, xpaths):
    print()
    print("=" * 100)
    print("XPath success summary")
    print("=" * 100)
    for idx, xpath in enumerate(xpaths, start=1):
        rows = [row for row in results if row["xpath_index"] == idx]
        matched = sum(1 for row in rows if row["matched"])
        priced = sum(1 for row in rows if row["price"] is not None)
        visible = sum(1 for row in rows if row["visible"])
        total = len(rows)
        print(
            f"[{idx}] priced={priced}/{total}, matched={matched}/{total}, "
            f"visible={visible}/{total} :: {xpath}"
        )


def main():
    parser = argparse.ArgumentParser(
        description="Test Coolblue NL retail price XPath candidates on 10 random DB target URLs."
    )
    parser.add_argument(
        "--xpath",
        action="append",
        default=[],
        help="XPath candidate to test. Repeat this option to test multiple candidates.",
    )
    parser.add_argument("--limit", type=int, default=10, help="Number of random DB URLs to test.")
    parser.add_argument("--seed", type=int, default=None, help="Random seed for reproducible samples.")
    parser.add_argument(
        "--include-db-xpaths",
        action="store_true",
        help="Also test current active DB price XPath selectors when --xpath is provided.",
    )
    parser.add_argument(
        "--wait",
        type=int,
        default=8,
        help="Seconds to wait for each XPath on each page.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Optional CSV output path. Defaults to log/coolblue_xpath_inspector/<timestamp>.csv.",
    )
    args = parser.parse_args()

    scraper = CoolblueScraper()
    targets = load_random_targets(scraper, args.limit, args.seed)
    if not targets:
        print("No Coolblue NL targets loaded from DB.")
        return 1

    xpaths = build_xpath_list(scraper, args.xpath, args.include_db_xpaths)
    if not xpaths:
        print("No XPath candidates found. Pass --xpath or add active price selectors in DB.")
        return 1

    if not scraper.setup_driver():
        print("Chrome driver setup failed.")
        return 1

    results = []
    try:
        scraper.initialize_session()
        wait = WebDriverWait(scraper.driver, args.wait)

        print("=" * 100)
        print(f"Coolblue XPath inspector: {len(targets)} URLs, {len(xpaths)} XPath candidates")
        print("=" * 100)
        for xpath_idx, xpath in enumerate(xpaths, start=1):
            print(f"[{xpath_idx}] {xpath}")
        print("=" * 100)

        for url_idx, row in enumerate(targets, start=1):
            url = row.get("url")
            print()
            print(f"URL {url_idx}/{len(targets)}: {url}")
            scraper.driver.get(url)
            time.sleep(2)

            for xpath_idx, xpath in enumerate(xpaths, start=1):
                inspected = inspect_xpath(scraper.driver, wait, xpath)
                result = {
                    "url_index": url_idx,
                    "xpath_index": xpath_idx,
                    "url": url,
                    "brand": row.get("brand", ""),
                    "item": row.get("item", ""),
                    "retailersku": row.get("retailersku", ""),
                    "xpath": xpath,
                    **inspected,
                    "text": short_text(inspected["text"], 500),
                }
                results.append(result)

                status = "PRICE" if inspected["price"] is not None else "MATCH" if inspected["matched"] else "FAIL"
                print(
                    f"  [{xpath_idx}] {status:<5} price={inspected['price']!r} "
                    f"visible={inspected['visible']} text={short_text(inspected['text'], 90)!r}"
                )

        print_summary(results, xpaths)

        output = args.output
        if not output:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_dir = os.path.join("log", "coolblue_xpath_inspector")
            os.makedirs(output_dir, exist_ok=True)
            output = os.path.join(output_dir, f"{timestamp}.csv")
        pd.DataFrame(results).to_csv(output, index=False, encoding="utf-8-sig")
        print()
        print(f"Saved detail CSV: {output}")

    finally:
        if scraper.driver:
            scraper.driver.quit()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

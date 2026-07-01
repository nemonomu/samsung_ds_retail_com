# -*- coding: utf-8 -*-
"""
JSON-LD 가격 vs 렌더 가격 일관성 검증 (비침습).

목적: 2단계 아키텍처(1단계=SSR JSON-LD로 고속 수집) 도입 전에,
      MediaMarkt PDP의 JSON-LD offers.price 가 실제 화면(렌더) 가격과
      항상 일치하는지 전량 대조한다. 저장 테이블은 건드리지 않는다(READ-ONLY).

동작:
  - 프로덕션 클래스(MediaMarktInfiniteScraper)를 그대로 재사용해
    동일한 드라이버 옵션 / DB selector / 쿠키·Cloudflare 처리로 접속.
  - 각 PDP에서 같은 page_source 로부터
       (a) 렌더 가격  : DB active price selector + 프로덕션과 동일한 정규식
       (b) JSON-LD    : offers.price (SSR, head)
    를 뽑아 비교.
  - 결과를 콘솔 요약 + CSV(verify_jsonld_price_result.csv) 로 남긴다.

⚠️ 라이브 크롤러(mediamarkt_v2.py)와 **동시에 실행하지 말 것**
   (UC Chrome 2개 동시 = 봇탐지/충돌 위험). 크롤러가 멈춘 뒤 실행하세요.

사용:
  python verify_jsonld_price.py            # 전량(72개)
  python verify_jsonld_price.py --limit 5  # 앞 5개만 스모크 테스트
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException

from mediamarkt_v2 import MediaMarktInfiniteScraper, logger

OUT_CSV = "verify_jsonld_price_result.csv"
HOME = "https://www.mediamarkt.de"


def parse_rendered_price(text: str):
    """프로덕션(extract_product_info)과 동일한 가격 파싱 로직."""
    if not text:
        return None
    text = text.replace("€", "").strip()
    m = re.search(r"(\d+)[,.]?(\d*)", text)
    if not m:
        return None
    price = m.group(1)
    if m.group(2):
        price += "." + m.group(2)
    try:
        return float(price)
    except ValueError:
        return None


def rendered_price_from_dom(driver, selectors):
    """DB active price selector로 화면 가격 추출 (프로덕션과 동일 순서/방식)."""
    for selector in selectors:
        try:
            if selector.startswith("//"):
                els = driver.find_elements(By.XPATH, selector)
            else:
                els = driver.find_elements(By.CSS_SELECTOR, selector)
            for el in els:
                raw = (el.text or "").strip() or (el.get_attribute("innerText") or "").strip()
                if raw:
                    val = parse_rendered_price(raw)
                    if val is not None:
                        return val, selector, raw
        except Exception:
            continue
    return None, None, None


def jsonld_from_source(html: str):
    """SSR JSON-LD(BuyAction)에서 price/title/image 추출."""
    price = title = image = None
    for m in re.finditer(r'<script[^>]*application/ld\+json[^>]*>(.*?)</script>', html, re.S):
        try:
            d = json.loads(m.group(1))
        except Exception:
            continue
        obj = d.get("object") if isinstance(d, dict) else None
        if isinstance(obj, dict) and obj.get("offers"):
            title = obj.get("name")
            offers = obj.get("offers") or {}
            p = offers.get("price")
            try:
                price = float(p) if p is not None else None
            except (TypeError, ValueError):
                price = None
            img = obj.get("image")
            image = img[0] if isinstance(img, list) and img else (img if isinstance(img, str) else None)
            break
    return price, title, image


def wait_rendered(driver, selectors, timeout=12):
    """렌더 가격 span이 뜰 때까지 대기(비교 유효성 확보)."""
    def ready(d):
        for sel in selectors:
            try:
                els = d.find_elements(By.XPATH, sel) if sel.startswith("//") else d.find_elements(By.CSS_SELECTOR, sel)
                for el in els:
                    if (el.text or "").strip() or (el.get_attribute("innerText") or "").strip():
                        return True
            except Exception:
                continue
        return False
    try:
        WebDriverWait(driver, timeout).until(ready)
        return True
    except TimeoutException:
        return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="앞 N개만 검증(0=전량)")
    ap.add_argument("--restart-every", type=int, default=20, help="N개마다 브라우저 재시작")
    args = ap.parse_args()

    scraper = MediaMarktInfiniteScraper()
    if scraper.db_engine is None:
        logger.error("DB 연결 실패로 종료")
        sys.exit(1)

    price_selectors = scraper.XPATHS.get("price", [])
    logger.info(f"검증에 사용할 active price selector {len(price_selectors)}개: {price_selectors}")

    targets = scraper.get_crawl_targets()
    if args.limit:
        targets = targets[: args.limit]
    total = len(targets)
    logger.info(f"검증 대상 {total}개")

    if not scraper.setup_driver():
        logger.error("드라이버 셋업 실패")
        sys.exit(1)

    # 워밍업 (홈 접속 + 쿠키 수락 + Cloudflare 확인) — 수동 로그인 프롬프트 회피
    scraper.driver.get(HOME)
    time.sleep(3)
    scraper.accept_cookies()
    if scraper.check_cloudflare_challenge():
        logger.error("❌ 홈에서 Cloudflare 감지 — 검증 중단(수동 통과 필요)")
        scraper.driver.quit()
        sys.exit(1)

    rows = []
    match = mismatch = rendered_null = jsonld_null = 0

    for idx, row in enumerate(targets):
        url = row.get("url")
        sku = row.get("retailersku", "")
        logger.info(f"[{idx+1}/{total}] {url}")

        try:
            scraper.driver.get(url)
        except Exception as e:
            logger.warning(f"  get 오류(계속): {e}")

        wait_rendered(scraper.driver, price_selectors)
        try:
            scraper.driver.execute_script("window.stop()")
        except Exception:
            pass
        scraper.accept_cookies()

        html = scraper.driver.page_source or ""
        r_price, r_sel, r_raw = rendered_price_from_dom(scraper.driver, price_selectors)
        j_price, j_title, j_image = jsonld_from_source(html)

        note = ""
        is_match = None
        if r_price is None:
            rendered_null += 1
            note = "RENDERED NULL"
        if j_price is None:
            jsonld_null += 1
            note = (note + "; " if note else "") + "JSONLD NULL"
        if r_price is not None and j_price is not None:
            is_match = abs(r_price - j_price) < 0.005
            if is_match:
                match += 1
            else:
                mismatch += 1
                note = (note + "; " if note else "") + "MISMATCH"

        flag = "✅" if is_match else ("❌" if is_match is False else "⚠️")
        logger.info(f"  {flag} rendered={r_price} (raw={r_raw!r}) | jsonld={j_price} "
                    f"| title={'Y' if j_title else 'N'} image={'Y' if j_image else 'N'} {note}")

        rows.append({
            "idx": idx + 1,
            "retailersku": sku,
            "url": url,
            "rendered_price": r_price,
            "rendered_raw": r_raw,
            "rendered_selector": r_sel,
            "jsonld_price": j_price,
            "match": is_match,
            "jsonld_title_present": bool(j_title),
            "jsonld_image_present": bool(j_image),
            "note": note,
        })

        # 안정성: N개마다 브라우저 재시작 (메모리 누적 방지)
        if args.restart_every and (idx + 1) % args.restart_every == 0 and idx < total - 1:
            logger.info("  🔄 브라우저 재시작")
            try:
                scraper.restart_browser()
            except Exception:
                scraper.kill_chrome_processes()
                scraper.setup_driver()
                scraper.driver.get(HOME)
                time.sleep(3)
                scraper.accept_cookies()

        time.sleep(1.0)  # 가벼운 예의상 대기

    try:
        scraper.driver.quit()
    except Exception:
        pass

    with open(OUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else [])
        w.writeheader()
        w.writerows(rows)

    logger.info("=" * 60)
    logger.info("검증 요약")
    logger.info(f"  총 {total}개")
    logger.info(f"  ✅ 일치           : {match}")
    logger.info(f"  ❌ 불일치         : {mismatch}")
    logger.info(f"  ⚠️ 렌더 NULL      : {rendered_null}")
    logger.info(f"  ⚠️ JSON-LD NULL   : {jsonld_null}")
    logger.info(f"  결과 CSV          : {OUT_CSV}")
    if mismatch or jsonld_null:
        logger.info("  → 불일치/NULL SKU를 CSV note 컬럼에서 확인하세요.")


if __name__ == "__main__":
    main()

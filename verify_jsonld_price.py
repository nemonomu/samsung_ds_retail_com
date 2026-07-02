# -*- coding: utf-8 -*-
"""
JSON-LD 가격 신뢰성 검증 (경량 · 비침습 · 프로덕션 무수정).

방식(방법2):
  - 각 PDP를 열되 **렌더 대기 없이** page_source 만 즉시 확보 → SSR JSON-LD
    offers.price 만 파싱 (페이지당 ~2-3초).
  - 렌더 가격은 새로 뽑지 않고, 크롤러가 **이미 DB에 저장한 최신 retailprice**
    (mediamarkt_price_crawl_tbl_de_v2) 와 대조.
  - 결과 콘솔 요약 + CSV. 저장 테이블·프로덕션 코드 전혀 안 건드림(READ-ONLY).

⚠️ 라이브 크롤러(mediamarkt_v2.py)와 동시에 실행하지 말 것 (UC Chrome 2개 동시 금지).

사용:
  python verify_jsonld_price.py --limit 5   # 스모크
  python verify_jsonld_price.py             # 전량
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time

import pandas as pd

from mediamarkt_v2 import MediaMarktInfiniteScraper, logger

OUT_CSV = "verify_jsonld_price_result.csv"
HOME = "https://www.mediamarkt.de"
CRAWL_TABLE = "mediamarkt_price_crawl_tbl_de_v2"


def _offer_price(offers):
    offer_list = offers if isinstance(offers, list) else ([offers] if isinstance(offers, dict) else [])
    for off in offer_list:
        if not isinstance(off, dict):
            continue
        p = off.get("price")
        if p is None:
            spec = off.get("priceSpecification")
            specs = spec if isinstance(spec, list) else ([spec] if isinstance(spec, dict) else [])
            for s in specs:
                if isinstance(s, dict) and s.get("price") is not None:
                    p = s.get("price")
                    break
        if p is not None:
            try:
                return float(p)
            except (TypeError, ValueError):
                continue
    return None


def jsonld_from_source(html):
    """SSR JSON-LD 에서 price/title/image (BuyAction 래핑 / Product / list 모두 대응)."""
    for m in re.finditer(r'<script[^>]*application/ld\+json[^>]*>(.*?)</script>', html or "", re.S):
        try:
            d = json.loads(m.group(1))
        except Exception:
            continue
        for node in (d if isinstance(d, list) else [d]):
            if not isinstance(node, dict):
                continue
            obj = node.get("object") if isinstance(node.get("object"), dict) else node
            if not isinstance(obj, dict):
                continue
            price = _offer_price(obj.get("offers"))
            if price is not None:
                img = obj.get("image")
                image = img[0] if isinstance(img, list) and img else (img if isinstance(img, str) else None)
                return price, obj.get("name"), image
    return None, None, None


def load_db_rendered(engine):
    """SKU별 최신 렌더가 {retailersku: (retailprice, crawl_datetime)}."""
    df = pd.read_sql(
        f"SELECT retailersku, retailprice, crawl_datetime FROM {CRAWL_TABLE} "
        f"WHERE retailprice IS NOT NULL ORDER BY crawl_datetime DESC", engine)
    df["retailersku"] = df["retailersku"].astype(str)
    df = df.drop_duplicates("retailersku", keep="first")
    return {r.retailersku: (float(r.retailprice), str(r.crawl_datetime)) for r in df.itertuples()}


def _blocked(html):
    head = (html or "")[:4000]
    return "Nur einen Moment" in head or "Just a moment" in head or "ein Mensch sind" in head


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="앞 N개만(0=전량)")
    ap.add_argument("--settle", type=float, default=1.2, help="get 후 대기(초)")
    ap.add_argument("--restart-every", type=int, default=25, help="N개마다 브라우저 재시작")
    args = ap.parse_args()

    scraper = MediaMarktInfiniteScraper()
    if scraper.db_engine is None:
        logger.error("DB 연결 실패로 종료")
        sys.exit(1)

    db_rendered = load_db_rendered(scraper.db_engine)
    logger.info(f"DB 렌더가 확보: {len(db_rendered)} SKU")

    targets = scraper.get_crawl_targets()
    if args.limit:
        targets = targets[: args.limit]
    total = len(targets)
    logger.info(f"검증 대상 {total}개 (렌더 대기 없이 JSON-LD만 파싱)")

    if not scraper.setup_driver():
        logger.error("드라이버 셋업 실패")
        sys.exit(1)

    # 워밍업 (홈 + 쿠키 + Cloudflare) — 수동 로그인 프롬프트 회피
    scraper.driver.get(HOME)
    time.sleep(3)
    scraper.accept_cookies()
    if scraper.check_cloudflare_challenge():
        logger.error("❌ 홈에서 Cloudflare 감지 — 중단")
        scraper.driver.quit()
        sys.exit(1)

    rows = []
    match = mismatch = jsonld_null = no_db = blocked = 0

    for idx, row in enumerate(targets):
        url = row.get("url")
        sku = str(row.get("retailersku", ""))
        try:
            try:
                scraper.driver.get(url)
            except Exception as e:
                logger.warning(f"  get 오류(계속): {e}")
            time.sleep(args.settle)
            try:
                scraper.driver.execute_script("window.stop()")
            except Exception:
                pass

            html = scraper.driver.page_source or ""
            is_blocked = _blocked(html)
            j_price, j_title, j_image = jsonld_from_source(html)
            db = db_rendered.get(sku)
            db_price = db[0] if db else None

            note = ""
            is_match = None
            if is_blocked:
                blocked += 1
                note = "BLOCKED(CF)"
            if j_price is None:
                jsonld_null += 1
                note = (note + "; " if note else "") + "JSONLD NULL"
            if db_price is None:
                no_db += 1
                note = (note + "; " if note else "") + "NO DB PRICE"
            if j_price is not None and db_price is not None:
                is_match = abs(j_price - db_price) < 0.005
                if is_match:
                    match += 1
                else:
                    mismatch += 1
                    note = (note + "; " if note else "") + "MISMATCH"

            flag = "✅" if is_match else ("❌" if is_match is False else "⚠️")
            logger.info(f"[{idx+1}/{total}] {flag} sku={sku} db={db_price} jsonld={j_price} "
                        f"title={'Y' if j_title else 'N'} image={'Y' if j_image else 'N'} {note}")

            rows.append({
                "idx": idx + 1, "retailersku": sku, "url": url,
                "db_rendered_price": db_price, "db_crawl_datetime": db[1] if db else None,
                "jsonld_price": j_price, "match": is_match,
                "jsonld_title_present": bool(j_title), "jsonld_image_present": bool(j_image),
                "note": note,
            })
        except Exception as e:
            logger.warning(f"[{idx+1}/{total}] 처리 오류(계속): {e}")
            rows.append({"idx": idx + 1, "retailersku": sku, "url": url,
                         "db_rendered_price": None, "db_crawl_datetime": None,
                         "jsonld_price": None, "match": None,
                         "jsonld_title_present": False, "jsonld_image_present": False,
                         "note": f"ERROR {type(e).__name__}"})

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

    try:
        scraper.driver.quit()
    except Exception:
        pass

    if rows:
        with open(OUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)

    logger.info("=" * 60)
    logger.info("검증 요약")
    logger.info(f"  총 {total}개")
    logger.info(f"  ✅ 일치         : {match}")
    logger.info(f"  ❌ 불일치       : {mismatch}")
    logger.info(f"  ⚠️ JSON-LD NULL : {jsonld_null}")
    logger.info(f"  ⚠️ DB가 없음    : {no_db}")
    logger.info(f"  ⚠️ CF 차단      : {blocked}")
    logger.info(f"  결과 CSV        : {OUT_CSV}")


if __name__ == "__main__":
    main()

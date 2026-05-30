"""
Production collection verifier - run AFTER the next v2 crawler run.

Inspects amazon_price_crawl_tbl_<country>_v2 tables in DB_CONFIG_V2 and answers:
  1) Canary check: for each known combined-label / used-buy ASIN, did the
     latest crawl populate ships_from + sold_by + retailprice?
  2) NULL audit: per country, what % of rows have NULL ships_from /
     sold_by / retailprice in the latest batch (and previous batch for diff)?
  3) Used-buy specific: are the canonical used-buy ASINs (DE B0B7CKZGN6,
     NL B0CTRVZKG7 etc.) now extracted?

Single command, prints a clean per-country summary table + canary roll-up.
"""
import csv
import json
import os
import sys
from collections import defaultdict

import pymysql

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import DB_CONFIG_V2


COUNTRY_TABLE = {
    'de': 'amazon_price_crawl_tbl_de_v2',
    'es': 'amazon_price_crawl_tbl_es_v2',
    'fr': 'amazon_price_crawl_tbl_fr_v2',
    'gb': 'amazon_price_crawl_tbl_gb_v2',
    'in': 'amazon_price_crawl_tbl_in_v2',
    'it': 'amazon_price_crawl_tbl_it_v2',
    'jp': 'amazon_price_crawl_tbl_jp_v2',
    'nl': 'amazon_price_crawl_tbl_nl',
    'usa': 'amazon_price_crawl_tbl_usa_v2',
}

CANARY_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            'full_verify_run', 'canaries.json')


def load_canaries():
    if not os.path.exists(CANARY_PATH):
        print(f'[warn] canary file not found at {CANARY_PATH}')
        return {}
    with open(CANARY_PATH, encoding='utf-8') as f:
        return json.load(f)


def fmt_pct(n, d):
    if d == 0:
        return '   - '
    return f'{n*100/d:5.1f}%'


def fetch_latest_per_asin(cur, table):
    """Return dict {asin: (kr_crawl_datetime, row_dict)} for the latest row per ASIN
    in the most-recent crawl date. Latest crawl date is identified by max
    kr_crawl_datetime substring (first 10 chars = YYYY-MM-DD).
    """
    try:
        cur.execute(
            f"SELECT retailersku, kr_crawl_datetime, ships_from, sold_by, retailprice, title "
            f"FROM {table} "
            f"WHERE kr_crawl_datetime IS NOT NULL "
            f"ORDER BY kr_crawl_datetime DESC "
            f"LIMIT 5000"
        )
    except Exception as e:
        print(f'[err] {table}: {type(e).__name__}: {e}')
        return {}, None
    rows = cur.fetchall()
    if not rows:
        return {}, None
    latest_date = rows[0][1][:10] if rows[0][1] else ''
    latest_per_asin = {}
    for asin, dt, ships, sold, price, title in rows:
        if not dt or not dt.startswith(latest_date):
            continue
        if asin not in latest_per_asin:
            latest_per_asin[asin] = {
                'kr_crawl_datetime': dt,
                'ships_from': ships,
                'sold_by': sold,
                'retailprice': price,
                'title': title,
            }
    return latest_per_asin, latest_date


def fetch_prev_per_asin(cur, table, exclude_date):
    """Latest rows per ASIN from the most recent date *before* exclude_date."""
    try:
        cur.execute(
            f"SELECT retailersku, kr_crawl_datetime, ships_from, sold_by, retailprice "
            f"FROM {table} "
            f"WHERE kr_crawl_datetime IS NOT NULL AND kr_crawl_datetime NOT LIKE %s "
            f"ORDER BY kr_crawl_datetime DESC "
            f"LIMIT 5000",
            (exclude_date + '%',)
        )
    except Exception as e:
        return {}, None
    rows = cur.fetchall()
    if not rows:
        return {}, None
    prev_date = rows[0][1][:10] if rows[0][1] else ''
    prev_per_asin = {}
    for asin, dt, ships, sold, price in rows:
        if not dt or not dt.startswith(prev_date):
            continue
        if asin not in prev_per_asin:
            prev_per_asin[asin] = {
                'kr_crawl_datetime': dt,
                'ships_from': ships,
                'sold_by': sold,
                'retailprice': price,
            }
    return prev_per_asin, prev_date


def _is_empty(v):
    return v is None or str(v).strip() == '' or str(v).strip().lower() == 'none'


def main():
    canaries_by_country = load_canaries()
    canary_count_total = sum(len(v) for v in canaries_by_country.values())
    print(f'[setup] {canary_count_total} canary ASINs loaded')

    conn = pymysql.connect(**DB_CONFIG_V2)
    cur = conn.cursor()

    # Per-country summary table
    print('\n' + '=' * 110)
    print(f'{"Country":<8} {"Latest batch":<14} {"Rows":>6}  {"ships_from NULL":>15} {"sold_by NULL":>15} {"retailprice NULL":>17}  {"vs prev":>12}')
    print('-' * 110)

    canary_pass_by_country = defaultdict(lambda: {'pass': 0, 'fail': 0, 'no_row': 0, 'detail': []})

    for country, table in COUNTRY_TABLE.items():
        latest, latest_date = fetch_latest_per_asin(cur, table)
        if not latest:
            print(f'{country:<8} {"(no data)":<14}')
            continue
        n = len(latest)
        n_ships_null = sum(1 for r in latest.values() if _is_empty(r['ships_from']))
        n_sold_null = sum(1 for r in latest.values() if _is_empty(r['sold_by']))
        n_price_null = sum(1 for r in latest.values() if _is_empty(r['retailprice']))

        prev, prev_date = fetch_prev_per_asin(cur, table, latest_date)
        if prev:
            p_ships = sum(1 for r in prev.values() if _is_empty(r['ships_from']))
            p_total = len(prev)
            ships_delta = (n_ships_null * 100 / n) - (p_ships * 100 / p_total)
            vs = f'{ships_delta:+5.1f}pp'
        else:
            vs = ''

        print(f'{country:<8} {latest_date:<14} {n:>6}  '
              f'{n_ships_null:>5}/{n} {fmt_pct(n_ships_null, n):>7}  '
              f'{n_sold_null:>5}/{n} {fmt_pct(n_sold_null, n):>7}  '
              f'{n_price_null:>5}/{n} {fmt_pct(n_price_null, n):>7}  '
              f'{vs:>12}')

        # Canary check for this country
        for canary in canaries_by_country.get(country, []):
            asin = canary['asin']
            row = latest.get(asin)
            if row is None:
                canary_pass_by_country[country]['no_row'] += 1
                continue
            ships_ok = not _is_empty(row['ships_from'])
            sold_ok = not _is_empty(row['sold_by'])
            price_ok = not _is_empty(row['retailprice'])
            if ships_ok and sold_ok and price_ok:
                canary_pass_by_country[country]['pass'] += 1
            else:
                canary_pass_by_country[country]['fail'] += 1
                canary_pass_by_country[country]['detail'].append({
                    'asin': asin,
                    'case': canary['case'],
                    'expected_ships': canary['expected_ships_from'],
                    'expected_sold': canary['expected_sold_by'],
                    'actual_ships': row['ships_from'],
                    'actual_sold': row['sold_by'],
                    'actual_price': row['retailprice'],
                })

    # Canary roll-up
    print('\n' + '=' * 110)
    print('Canary check (combined-label / used-buy ASINs that NEW selectors should now extract):')
    print(f'{"Country":<8} {"Canaries":>9} {"PASS":>6} {"FAIL":>6} {"missing":>8} {"pass%":>7}')
    print('-' * 50)
    total_pass = total_fail = total_missing = total_canaries = 0
    for country in sorted(canaries_by_country):
        stats = canary_pass_by_country[country]
        n = len(canaries_by_country[country])
        p, f, m = stats['pass'], stats['fail'], stats['no_row']
        total_canaries += n
        total_pass += p
        total_fail += f
        total_missing += m
        pct = fmt_pct(p, n)
        print(f'{country:<8} {n:>9} {p:>6} {f:>6} {m:>8} {pct:>7}')
    print('-' * 50)
    print(f'{"TOTAL":<8} {total_canaries:>9} {total_pass:>6} {total_fail:>6} {total_missing:>8} {fmt_pct(total_pass, total_canaries):>7}')

    # Show FAIL detail per country (limited)
    print('\n' + '=' * 110)
    print('Canary FAIL details (first 8 per country):')
    for country in sorted(canaries_by_country):
        details = canary_pass_by_country[country]['detail']
        if not details:
            continue
        print(f'\n--- {country} ({len(details)} failures) ---')
        for d in details[:8]:
            exp = (d['expected_ships'] or '')[:25]
            act_s = (d['actual_ships'] or 'NULL')[:25]
            act_b = (d['actual_sold'] or 'NULL')[:25]
            act_p = (d['actual_price'] or 'NULL')[:15]
            print(f'  {d["asin"]} [{d["case"]:<20}] expected_ships={exp!r:<27} actual_ships={act_s!r:<27} actual_sold={act_b!r:<27} actual_price={act_p!r}')

    print('\n' + '=' * 110)
    print('Done. Phase A is working if ships_from NULL count dropped and canary PASS rate increased.')

    conn.close()


if __name__ == '__main__':
    main()

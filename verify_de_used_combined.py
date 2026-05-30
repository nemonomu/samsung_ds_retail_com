"""
Crawl ONLY DE combined-label / used-buy canary ASINs using the production
AmazonDEScraper, skip DB save and file-server upload. Result printed
to console only.

Tests whether the legacy used-layout XPath unwrap fix (priority 119-120
DE selectors id 619, 620, 621, ...) restored ships_from extraction for:
  - B0CTRVZKG7 (regular_combined): previously sold_by='GER-POL', ships=NULL
  - B0B7CKZGN6 (used_combined):    previously all NULL (page state issue)
  - B0CTS93WML (used_separate):    previously all NULL (page state issue)
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from de_v2 import AmazonDEScraper


TEST_ASINS = ['B0CTRVZKG7', 'B0B7CKZGN6', 'B0CTS93WML']


def main():
    scraper = AmazonDEScraper()

    test_data = [
        {
            'url': f'https://www.amazon.de/dp/{asin}?th=1',
            'brand': 'Samsung',
            'item': f'DE canary {asin}',
            'retailerid': asin,
            'retailersku': asin,
            'channel': 'Online',
            'seg_lv1': 'SSD',
            'seg_lv2': 'Consumer',
            'seg_lv3': 'External',
            'capacity': '2TB',
            'form_factor': 'External',
        }
        for asin in TEST_ASINS
    ]

    results_df = scraper.scrape_urls(test_data)
    if results_df is None or results_df.empty:
        print('[result] no rows returned')
        return

    print()
    print('=' * 100)
    print('DE canary extraction result (NOT saved to DB):')
    print('=' * 100)
    for _, row in results_df.iterrows():
        print(f'\nASIN={row.get("retailersku")}')
        print(f'  ships_from  = {row.get("ships_from")!r}')
        print(f'  sold_by     = {row.get("sold_by")!r}')
        print(f'  retailprice = {row.get("retailprice")!r}')
        print(f'  title       = {(row.get("title") or "")[:80]!r}')
    print('=' * 100)


if __name__ == '__main__':
    main()

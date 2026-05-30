"""
Crawl ONLY NL used-combined canary ASINs (B0CTRVZKG7, B0CK2S298S) using
the production AmazonNLScraper, but skip DB save and file-server upload.
Result is printed to console only.

Run after fix_legacy_used_layout_parens.py to verify that ships_from is
now extracted for NL used-combined ('Verzender / Verkoper' label) pages.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from nl import AmazonNLScraper


TEST_ASINS = ['B0CTRVZKG7', 'B0CK2S298S']


def main():
    scraper = AmazonNLScraper()

    test_data = [
        {
            'url': f'https://www.amazon.nl/dp/{asin}?th=1',
            'brand': 'Samsung',
            'item': f'NL canary {asin}',
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
    print('NL used-combined canary extraction result (NOT saved to DB):')
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

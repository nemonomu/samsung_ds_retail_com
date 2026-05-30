"""
Crawl ONLY IT combined-label / used-buy canary ASINs using the production
AmazonITScraper, skip DB save and file-server upload. Result printed to
console only.

Tests whether legacy used-layout XPath unwrap fix restored IT extraction.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from it_v2 import AmazonITScraper


TEST_ASINS = [
    ('B0DWFMD52T', 'used_separate',    'Amazon'),
    ('B08PC5ZC73', 'regular_combined', 'coseon-de'),
    ('B0F3BMBQ75', 'used_separate',    'Amazon'),
]


def main():
    scraper = AmazonITScraper()

    test_data = [
        {
            'url': f'https://www.amazon.it/dp/{asin}?th=1',
            'brand': 'Samsung',
            'item': f'IT canary {asin}',
            'retailerid': asin,
            'retailersku': asin,
            'channel': 'Online',
            'seg_lv1': 'SSD',
            'seg_lv2': 'Consumer',
            'seg_lv3': 'External',
            'capacity': '2TB',
            'form_factor': 'External',
        }
        for asin, _case, _exp in TEST_ASINS
    ]

    results_df = scraper.scrape_urls(test_data)
    if results_df is None or results_df.empty:
        print('[result] no rows returned')
        return

    print()
    print('=' * 100)
    print('IT canary extraction result (NOT saved to DB):')
    print('=' * 100)
    expected = {asin: (case, exp) for asin, case, exp in TEST_ASINS}
    for _, row in results_df.iterrows():
        asin = row.get('retailersku')
        case, exp = expected.get(asin, ('?', '?'))
        print(f'\nASIN={asin} [{case}] expected_ships={exp!r}')
        print(f'  ships_from  = {row.get("ships_from")!r}')
        print(f'  sold_by     = {row.get("sold_by")!r}')
        print(f'  retailprice = {row.get("retailprice")!r}')
        print(f'  title       = {(row.get("title") or "")[:80]!r}')
    print('=' * 100)


if __name__ == '__main__':
    main()

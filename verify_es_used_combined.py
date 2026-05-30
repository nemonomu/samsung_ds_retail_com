"""
Crawl ONLY ES combined-label / used-buy canary ASINs using the production
AmazonScraper(country_code='es'), skip DB save and file-server upload.
Result printed to console only.

Tests whether legacy used-layout XPath unwrap fix restored ES extraction.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from es_v2 import AmazonScraper


TEST_ASINS = [
    ('B0CTRVZKG7', 'used_combined',    'GER-POL'),
    ('B0CX5C3LBQ', 'regular_combined', 'BEATRIX20 S.R.L.'),
    ('B0B7CMZ3QH', 'regular_combined', 'Morelenet'),
]


def main():
    scraper = AmazonScraper(country_code='es')

    test_data = [
        {
            'url': f'https://www.amazon.es/dp/{asin}?th=1',
            'brand': 'Samsung',
            'item': f'ES canary {asin}',
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
    print('ES canary extraction result (NOT saved to DB):')
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

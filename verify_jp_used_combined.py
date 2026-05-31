"""
Crawl ONLY JP combined-label canary ASIN using the production
AmazonScraper(country_code='jp'), skip DB save and file-server upload.
Result printed to console only.

Tests whether fix_jp_combined_label_predicate.py (//div -> //*) restored
JP ships_from extraction for '出荷元 / 販売元' combined-label pages
after Phase B refactor.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from jp_v2 import AmazonScraper


TEST_ASINS = ['B0DM47M71X']


def main():
    scraper = AmazonScraper(country_code='jp')

    test_data = [
        {
            'url': f'https://www.amazon.co.jp/dp/{asin}?th=1',
            'brand': 'Samsung',
            'item': f'JP canary {asin}',
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
    print('JP canary extraction result (NOT saved to DB):')
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

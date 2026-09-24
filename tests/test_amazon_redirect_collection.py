"""Offline tests of real crawler methods; no DB, browser, config or network imports."""
import ast
from datetime import datetime, timezone
from pathlib import Path
import re
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from amazon_page_guard import (
    AmazonProductPageError, capture_product_page_snapshot, extract_asin,
    wait_for_product_page,
)

MARKETS = {'it': ('it_v2.py', 'it'), 'fr': ('fr_v2.py', 'fr'),
           'de': ('de_v2.py', 'de'), 'usa': ('usa_v2.py', 'com'), 'es': ('es_v2.py', 'es')}
ORIGINAL = 'B087DFLF9S'
DESTINATION = 'B0GJF1GQFX'


class Driver:
    def __init__(self, url, signals, redirect=None):
        self.current_url = url
        self.redirect = redirect
        self.signals = signals
        self.title = 'Amazon'
        self.page_source = ''
        self.visits = []
        self.refresh = Mock()
        self.quit = Mock()

    def get(self, url):
        self.visits.append(url)
        self.current_url = self.redirect or url

    def execute_script(self, script):
        return self.signals

    def find_elements(self, *args):
        return []


def make_scraper(market, signals, redirect=None):
    filename, domain = MARKETS[market]
    path = ROOT / filename
    tree = ast.parse(path.read_text(encoding='utf-8-sig'))
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef))
    ns = dict(datetime=datetime, re=re, logger=Mock(),
              time=SimpleNamespace(sleep=Mock()), random=SimpleNamespace(uniform=lambda *a: 0),
              accept_cookies=Mock(), AmazonProductPageError=AmazonProductPageError,
              capture_product_page_snapshot=capture_product_page_snapshot, extract_asin=extract_asin,
              wait_for_product_page=lambda driver, **kwargs: wait_for_product_page(
                  driver, **dict(kwargs, timeout_seconds=0)),
              FULL_NULL_FIELDS=(), is_null_result=lambda *a: False, capture_and_upload=Mock(),
              By=SimpleNamespace(XPATH='xpath', CSS_SELECTOR='css'))
    exec(compile(ast.Module(body=[cls], type_ignores=[]), str(path), 'exec'), ns)
    obj = object.__new__(ns[cls.name])
    obj.country_code = market
    obj.korea_tz = obj.local_tz = timezone.utc
    obj.wait = None
    obj.page_timeout_seconds = 0
    obj.browser_needs_restart = False
    obj.last_failure_reason = None
    obj.selectors = {k: [] for k in ('title', 'ships_from', 'sold_by', 'imageurl')}
    if market in ('usa', 'es'):
        obj.selectors = {market: obj.selectors}
    url = f'https://www.amazon.{domain}/dp/{ORIGINAL}'
    obj.driver = Driver(url, signals, redirect)
    obj.extract_element_text = Mock(side_effect=lambda _, label: {
        '제목': signals.get('productTitle'), 'Ships From': 'Destination shipper',
        'Sold By': 'Destination seller'}[label])
    obj.extract_price = Mock(return_value='61.84')
    obj.check_stock_availability = Mock(return_value=True)
    obj.wait_for_page_load = Mock(return_value=True)
    obj.restart_driver = Mock(return_value=True)
    obj.setup_driver = Mock(return_value=True)
    # IT's real continue-button handler is also exercised.
    if market != 'it':
        obj.handle_captcha_or_block_page = Mock(return_value=False)
    return obj, ns, url


class RedirectCollectionTests(unittest.TestCase):
    def collect(self, obj, url, market, recovery=False):
        row = {'retailersku': 'original-tracking-sku', 'url': url}
        if not recovery:
            return obj.extract_product_info(url, row)
        path = ROOT / 'recovery.py'
        tree = ast.parse(path.read_text(encoding='utf-8-sig'))
        fn = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == 'recrawl_url')
        ns = dict(pd=SimpleNamespace(Series=type('UnusedSeries', (), {})),
                  TARGET_CONFIG={}, logger=Mock())
        exec(compile(ast.Module(body=[fn], type_ignores=[]), str(path), 'exec'), ns)
        return ns['recrawl_url'](object(), obj, url, row, market)

    def test_redirect_and_dom_mismatch_collect_destination_in_first_pass_and_recovery(self):
        for market, (_, domain) in MARKETS.items():
            for redirect in (None, f'https://www.amazon.{domain}/dp/{DESTINATION}'):
                for recovery in (False, True):
                    with self.subTest(market=market, redirect=bool(redirect), recovery=recovery):
                        obj, ns, url = make_scraper(market, {'productTitle': 'Destination SSD',
                                                           'domAsin': DESTINATION}, redirect)
                        result = self.collect(obj, url, market, recovery)
                        self.assertEqual(result['title'], 'Destination SSD')
                        self.assertEqual(result['retailprice'], '61.84')
                        self.assertEqual(result['ships_from'], 'Destination shipper')
                        self.assertEqual(result['sold_by'], 'Destination seller')
                        self.assertEqual(result['retailersku'], 'original-tracking-sku')
                        self.assertEqual(result['producturl'], url)
                        self.assertEqual(obj.driver.visits, [url])
                        obj.driver.refresh.assert_not_called()
                        obj.restart_driver.assert_not_called()
                        self.assertTrue(any('ASIN mismatch accepted' in str(c)
                                            for c in ns['logger'].warning.call_args_list))

    def test_matching_product_still_collects_without_mismatch_warning(self):
        for market in MARKETS:
            with self.subTest(market=market):
                obj, ns, url = make_scraper(market, {'productTitle': 'Original SSD', 'domAsin': ORIGINAL})
                result = self.collect(obj, url, market)
                self.assertEqual(result['title'], 'Original SSD')
                self.assertEqual(result['retailprice'], '61.84')
                self.assertEqual(result['producturl'], url)
                self.assertFalse(any('ASIN mismatch accepted' in str(c)
                                     for c in ns['logger'].warning.call_args_list))

    def test_listing_missing_title_and_robot_remain_rejected_even_with_different_asin(self):
        cases = [
            {'domAsin': DESTINATION, 'hasRecommendations': True, 'hasDpContainer': True},
            {'domAsin': DESTINATION},
            {'productTitle': 'Destination SSD', 'domAsin': DESTINATION,
             'bodyText': 'automated access to Amazon data'},
        ]
        for market in MARKETS:
            for signals in cases:
                for recovery in (False, True):
                    with self.subTest(market=market, signals=signals, recovery=recovery):
                        obj, _, url = make_scraper(market, signals)
                        result = self.collect(obj, url, market, recovery)
                        self.assertIsNone(result['title'])
                        self.assertIsNone(result['retailprice'])
                        self.assertEqual(result['retailersku'], 'original-tracking-sku')
                        self.assertEqual(result['producturl'], url)
                        obj.extract_price.assert_not_called()
                        obj.extract_element_text.assert_not_called()

    def test_foreign_marketplace_remains_rejected(self):
        for market in MARKETS:
            for recovery in (False, True):
                with self.subTest(market=market, recovery=recovery):
                    obj, _, url = make_scraper(market, {'productTitle': 'Foreign SSD', 'domAsin': DESTINATION},
                                              f'https://www.amazon.in/dp/{DESTINATION}')
                    result = self.collect(obj, url, market, recovery)
                    self.assertIsNone(result['title'])
                    obj.extract_price.assert_not_called()

    def test_it_precheck_allows_mismatch_but_rejects_listing(self):
        obj, _, _ = make_scraper('it', {'productTitle': 'Destination SSD', 'domAsin': DESTINATION})
        self.assertFalse(obj.is_page_blocked())
        obj.driver.signals.pop('productTitle')
        self.assertTrue(obj.is_page_blocked())

    def test_destination_asin_missing_or_malformed_is_not_collected(self):
        for market, (_, domain) in MARKETS.items():
            for dom_asin in (None, 'invalid'):
                for recovery in (False, True):
                    with self.subTest(market=market, dom_asin=dom_asin, recovery=recovery):
                        obj, ns, url = make_scraper(
                            market, {'productTitle': 'Unidentified product', 'domAsin': dom_asin},
                            f'https://www.amazon.{domain}/product-page')
                        result = self.collect(obj, url, market, recovery)
                        self.assertIsNone(result['title'])
                        self.assertIsNone(result['retailprice'])
                        self.assertEqual(result['retailersku'], 'original-tracking-sku')
                        self.assertEqual(result['producturl'], url)
                        obj.extract_element_text.assert_not_called()
                        obj.extract_price.assert_not_called()
                        self.assertTrue(any('Destination ASIN unavailable' in str(c)
                                            for c in ns['logger'].warning.call_args_list))
                        self.assertFalse(any('ASIN mismatch accepted' in str(c)
                                             for c in ns['logger'].warning.call_args_list))
                        # Preserve each crawler's existing attempt limits, including recovery.
                        attempts = (2 if market in ('usa', 'es') else 1) if recovery else {
                            'it': 1, 'fr': 2, 'de': 2, 'usa': 4, 'es': 4}[market]
                        self.assertEqual(obj.driver.visits, [url] * attempts)

    def test_one_destination_asin_source_is_sufficient(self):
        for market, (_, domain) in MARKETS.items():
            for source in ('url', 'dom'):
                for recovery in (False, True):
                    with self.subTest(market=market, source=source, recovery=recovery):
                        signals = {'productTitle': 'Destination SSD'}
                        if source == 'dom':
                            signals['domAsin'] = DESTINATION
                        redirect = (f'https://www.amazon.{domain}/dp/{DESTINATION}' if source == 'url'
                                    else f'https://www.amazon.{domain}/product-page')
                        obj, _, url = make_scraper(market, signals, redirect)
                        result = self.collect(obj, url, market, recovery)
                        self.assertEqual(result['title'], 'Destination SSD')
                        self.assertEqual(result['retailprice'], '61.84')
                        self.assertEqual(result['retailersku'], 'original-tracking-sku')
                        self.assertEqual(result['producturl'], url)
                        self.assertEqual(obj.driver.visits, [url])
                        obj.restart_driver.assert_not_called()

    def test_it_precheck_rejects_unidentified_destination(self):
        obj, _, _ = make_scraper('it', {'productTitle': 'Unidentified product'})
        obj.driver.current_url = 'https://www.amazon.it/product-page'
        self.assertTrue(obj.is_page_blocked())
        obj.driver.signals['domAsin'] = DESTINATION
        self.assertFalse(obj.is_page_blocked())

    def test_shared_guard_still_reports_mismatch(self):
        obj, _, url = make_scraper('fr', {'productTitle': 'Destination SSD', 'domAsin': DESTINATION})
        snapshot = capture_product_page_snapshot(obj.driver, expected_url=url,
                                                marketplace_host='amazon.fr', locale_code='fr')
        self.assertEqual(snapshot.kind, 'asin_mismatch')
        self.assertFalse(snapshot.is_valid)


if __name__ == '__main__':
    unittest.main()

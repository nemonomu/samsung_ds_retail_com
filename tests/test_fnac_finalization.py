"""Cross-stage regression tests for final decisions, replay, and retry budgets."""
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import Mock, patch

import test_fnac_auto_recovery as fixtures

URL = fixtures.URL.split('?')[0]
ROW = {'url': URL, 'retailersku': 'synthetic'}
PRODUCT = fixtures.PRODUCT
AMBIGUOUS = PRODUCT + '<script id="digitalData">' + json.dumps({'product': {'attributes': {
    'offer': [{'sellerType': 'fnac'}], 'currentOffer': {'sellerType': 'professional'}}}}) + '</script>'
OOS = PRODUCT.replace('En stock', 'Stock en ligne épuisé')
STORE_ONLY = PRODUCT + '<script id="digitalData">' + json.dumps({'product': {'attributes': {
    'offer': [{'sellerType': 'store', 'seller': 'clickandcollectonly'}],
    'availabilityType': 'discontinued', 'currentOffer': {'sellerType': 'professional'}}}}) + '</script>'
LOADING = '<html data-fnac-rendered-verification="complete"><body><h1>Loading</h1></body></html>'


class FinalizationTests(unittest.TestCase):
    def setUp(self):
        self.case = fixtures.FnacRecoveryTests()
        self.case.setUp()
        self.m = self.case.m
        self.scraper = self.case.scraper
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)

    def configure_verification(self, verified=OOS):
        self.scraper.fetch_html = Mock(return_value=(200, AMBIGUOUS, None, 0))
        self.scraper.fetch_verified_html = Mock(return_value=(200, verified, 0))

    def replay(self):
        replay = self.m.FnacZenRowsScraper(html_dir=str(self.directory), capture_null=False)
        replay.fetch_html = Mock(side_effect=AssertionError('Replay must not fetch'))
        result = replay.collect_one(ROW)
        replay.fetch_html.assert_not_called()
        return result

    def test_final_browser_replaces_initial_store_exclusion_only_when_capture_enabled(self):
        for capture in (False, True):
            self.configure_verification(STORE_ONLY)
            self.scraper.capture_null = capture
            fake, _, *_ = self.case.screenshot_context('ready')
            with patch.dict(sys.modules, {'playwright.sync_api': fake}):
                row = self.scraper.collect_one(ROW)
            self.assertEqual(row['retailprice'], 123.45 if capture else None)
            self.assertEqual(row['_crawl_reason'], 'BROWSER_RECOVERED_VISIBLE_PRICE_BOX' if capture
                             else 'BROWSER_BASE_CLICK_COLLECT_FIRST_MARKETPLACE')
            self.assertEqual(row['_s3_upload'], 'skip')

    def test_out_of_stock_policy_outranks_visible_screenshot_price_and_replays_it(self):
        self.configure_verification(OOS)
        self.scraper.save_html_dir = self.directory
        fake, _, page, *_ = self.case.screenshot_context('ready')
        page.content.return_value = OOS
        with patch.dict(sys.modules, {'playwright.sync_api': fake}):
            row = self.scraper.collect_one(ROW)
        self.assertIsNone(row['retailprice'])
        self.assertEqual(row['_crawl_reason'], 'BROWSER_RECOVERED_ONLINE_STOCK_EXHAUSTED')
        self.assertIsNone(self.replay()['retailprice'])
        self.assertEqual(self.replay()['_crawl_reason'], row['_crawl_reason'])

    def test_out_of_stock_without_visible_price_stays_null(self):
        self.configure_verification(OOS)
        fake, _, page, *_ = self.case.screenshot_context('ready')
        page.content.return_value = OOS
        page.state['visiblePriceTexts'] = []  # price remains in HTML but is hidden in the screen
        with patch.dict(sys.modules, {'playwright.sync_api': fake}):
            row = self.scraper.collect_one(ROW)
        self.assertIsNone(row['retailprice'])
        self.assertEqual(row['_crawl_reason'], 'BROWSER_RECOVERED_ONLINE_STOCK_EXHAUSTED')
        self.assertEqual(row['_s3_upload'], 'ok')

    def test_replay_of_retired_screenshot_override_restores_policy_without_fetch(self):
        self.scraper.save_html_dir = self.directory
        row = self.scraper.base_result(ROW)
        row.update(title='Samsung SSD', imageurl='image', retailprice=123.45,
                   _crawl_reason='BROWSER_RECOVERED_SCREENSHOT_VISIBLE_PRICE')
        self.scraper.save_final_snapshot(ROW, OOS, row)
        replay = self.replay()
        self.assertIsNone(replay['retailprice'])
        self.assertEqual(replay['_crawl_reason'], 'ONLINE_STOCK_EXHAUSTED')

    def test_out_of_stock_blocked_screenshot_does_not_invent_a_price(self):
        self.configure_verification(OOS)
        fake, _, *_ = self.case.screenshot_context('restricted')
        with patch.dict(sys.modules, {'playwright.sync_api': fake}), patch.object(self.m.time, 'sleep'):
            row = self.scraper.collect_one(ROW)
        self.assertIsNone(row['retailprice'])
        self.assertEqual(row['_s3_upload'], 'fail')

    def test_unfinished_auto_and_browser_pages_preserve_initial_fields(self):
        self.scraper.capture_null = False
        self.scraper.fetch_html = Mock(side_effect=[(200, AMBIGUOUS, None, 0), (200, LOADING, None, 0)])
        self.scraper.fetch_browser_html = Mock(return_value=(200, LOADING, 0))
        row = self.scraper.collect_one(ROW)
        self.assertEqual(row['title'], 'Samsung SSD')
        self.assertTrue(row['imageurl'])
        self.assertIsNone(row['retailprice'])
        self.assertEqual(row['_crawl_reason'], 'BROWSER_VERIFY_FAILED_VISIBLE_PRICE_BOX')
        self.scraper.fetch_browser_html.assert_called_once()

    def test_wrong_product_canonical_is_rejected(self):
        wrong = '<html data-fnac-rendered-verification="complete"><link rel="canonical" href="https://www.fnac.com/item/a999/w-4">' + PRODUCT + '</html>'
        self.scraper.fetch_html = Mock(return_value=(200, wrong, None, 0))
        self.scraper.fetch_browser_html = Mock(return_value=(403, '', 0))
        self.assertEqual(self.scraper.fetch_verified_html(URL)[0], 403)
        self.scraper.fetch_browser_html.assert_called_once()

    def test_ready_verification_without_image_keeps_existing_image(self):
        self.scraper.capture_null = False
        self.configure_verification('<h1 class="f-productHeader__heading">Samsung SSD</h1><span class="f-faPriceBox__price">123,45</span>')
        result = self.scraper.collect_one(ROW)
        self.assertEqual(result['retailprice'], 123.45)
        self.assertTrue(result['imageurl'])

    def test_initial_product_shell_gets_rendered_verification(self):
        self.scraper.capture_null = False
        shell = PRODUCT.replace('<span class="f-faPriceBox__price">123,45</span>', '')
        for url in (URL, fixtures.URL):
            with self.subTest(url=url):
                self.scraper.fetch_html = Mock(side_effect=[(200, shell, None, 0),
                    (200, '<html data-fnac-rendered-verification="complete">' + PRODUCT + '</html>', None, 0)])
                self.scraper.fetch_browser_html = Mock()
                result = self.scraper.collect_one(dict(ROW, url=url))
                self.assertEqual(result['retailprice'], 123.45)
                self.assertEqual(result['_crawl_reason'], 'BROWSER_VISIBLE_PRICE_BOX')
                self.scraper.fetch_browser_html.assert_not_called()

    def test_visible_price_rule_does_not_collect_other_offers(self):
        self.configure_verification(OOS)
        fake, _, page, *_ = self.case.screenshot_context('ready')
        page.content.return_value = OOS.replace('<span class="f-faPriceBox__price">', '<h2>Autres offres</h2><span class="f-faPriceBox__price">')
        with patch.dict(sys.modules, {'playwright.sync_api': fake}):
            result = self.scraper.collect_one(ROW)
        self.assertIsNone(result['retailprice'])

    def test_confirmed_stock_and_store_only_pages_are_ready_without_a_price(self):
        for body in (OOS, STORE_ONLY):
            self.assertTrue(self.scraper.product_html_ready(body, URL))
        self.assertFalse(self.scraper.product_html_ready(LOADING, URL))

    def test_final_html_and_decision_replay_match(self):
        self.scraper.capture_null = False
        self.scraper.save_html_dir = self.directory
        self.configure_verification()
        live = self.scraper.collect_one(ROW)
        self.assertEqual((self.directory / 'synthetic.initial.html').read_text(encoding='utf-8'), AMBIGUOUS)
        self.assertEqual((self.directory / 'synthetic.html').read_text(encoding='utf-8'), OOS)
        replay = self.replay()
        for key in ('title', 'imageurl', 'retailprice', '_crawl_reason'):
            self.assertEqual(replay[key], live[key])

    def test_verification_failure_null_is_not_replayed_as_initial_price(self):
        self.scraper.capture_null = False
        self.scraper.save_html_dir = self.directory
        self.scraper.fetch_html = Mock(return_value=(200, AMBIGUOUS, None, 0))
        self.scraper.fetch_verified_html = Mock(return_value=(403, '', 0))
        live = self.scraper.collect_one(ROW)
        replay = self.replay()
        self.assertIsNone(replay['retailprice'])
        self.assertEqual(replay['_crawl_reason'], live['_crawl_reason'])

    def test_screenshot_recovery_saves_accepted_html_and_reason(self):
        self.scraper.save_html_dir = self.directory
        self.scraper.fetch_html = Mock(return_value=(422, '', None, 0))
        self.scraper.fetch_browser_html = Mock(return_value=(403, '', 0))
        fake, _, *_ = self.case.screenshot_context('ready')
        with patch.dict(sys.modules, {'playwright.sync_api': fake}):
            live = self.scraper.collect_one(ROW)
        self.assertEqual(live['retailprice'], 123.45)
        self.assertEqual((self.directory / 'synthetic.html').read_text(encoding='utf-8'), PRODUCT)
        self.assertEqual(self.replay()['_crawl_reason'], live['_crawl_reason'])

    def test_mixed_html_and_metadata_are_refused(self):
        self.scraper.save_html_dir = self.directory
        self.scraper.capture_null = False
        self.configure_verification()
        self.scraper.collect_one(ROW)
        (self.directory / 'synthetic.html').write_text(PRODUCT, encoding='utf-8')
        row = self.replay()
        self.assertIsNone(row['retailprice'])
        self.assertEqual(row['_crawl_reason'], 'HTML_CACHE_INVALID')

    def test_interrupted_snapshot_cannot_be_replayed_as_legacy_html(self):
        self.scraper.save_html_dir = self.directory
        original_replace = Path.replace
        def interrupted(path, target):
            if str(target).endswith('.html'):
                raise OSError('synthetic interrupted write')
            return original_replace(path, target)
        with patch.object(Path, 'replace', interrupted):
            self.scraper.save_final_snapshot(ROW, PRODUCT, {'retailprice': None, '_crawl_reason': 'BROWSER_VERIFY_FAILED_VISIBLE_PRICE_BOX'})
        self.assertTrue((self.directory / 'synthetic.result.json').exists())
        self.assertFalse((self.directory / 'synthetic.html').exists())
        self.assertIsNone(self.replay()['retailprice'])

    def test_legacy_html_replay_still_works_and_chooses_latest(self):
        older = self.directory / '01_synthetic.html'
        newer = self.directory / '02_synthetic.html'
        older.write_text(PRODUCT, encoding='utf-8')
        newer.write_text(OOS, encoding='utf-8')
        os.utime(older, (1, 1))
        os.utime(newer, (2, 2))
        self.assertEqual(self.replay()['_crawl_reason'], 'ONLINE_STOCK_EXHAUSTED')

    def test_expired_budget_stops_retries_and_browser_fallback(self):
        clock = [0.0]
        self.scraper.capture_null = False
        self.scraper.product_time_budget = 10
        session = Mock()
        def timeout(*args, **kwargs):
            self.assertAlmostEqual(kwargs['timeout'], 10 - 10 / 3)
            clock[0] = 10
            raise self.m.requests.ReadTimeout()
        session.get.side_effect = timeout
        self.scraper.fetch_browser_html = Mock()
        with patch.object(self.m.time, 'monotonic', side_effect=lambda: clock[0]), patch.object(self.m.requests, 'Session', return_value=session):
            row = self.scraper.collect_one(ROW)
        session.get.assert_called_once()
        session.close.assert_called_once()
        self.scraper.fetch_browser_html.assert_not_called()
        self.assertEqual(row['_crawl_reason'], 'TIME_BUDGET_EXHAUSTED')
        self.assertFalse(hasattr(self.scraper._product_clock, 'deadline'))

    def test_budget_during_verification_clears_price_but_keeps_fields(self):
        self.scraper.capture_null = False
        self.scraper.fetch_html = Mock(return_value=(200, AMBIGUOUS, None, 0))
        self.scraper.fetch_verified_html = Mock(side_effect=self.m.ProductTimeBudgetExceeded())
        row = self.scraper.collect_one(ROW)
        self.assertEqual(row['title'], 'Samsung SSD')
        self.assertTrue(row['imageurl'])
        self.assertIsNone(row['retailprice'])
        self.assertEqual(row['_crawl_reason'], 'TIME_BUDGET_EXHAUSTED_VERIFY')

    def test_evidence_budget_keeps_confirmed_null(self):
        self.configure_verification()
        self.scraper.capture_null_screenshot = Mock(side_effect=self.m.ProductTimeBudgetExceeded())
        row = self.scraper.collect_one(ROW)
        self.assertIsNone(row['retailprice'])
        self.assertEqual(row['_crawl_reason'], 'BROWSER_ONLINE_STOCK_EXHAUSTED')
        self.assertEqual(row['_s3_upload'], 'fail')

    def test_parallel_products_have_independent_budgets(self):
        self.scraper.capture_null = False
        barrier = threading.Barrier(2)
        observed = {}
        def fetch(url, **kwargs):
            self.scraper._product_clock.deadline = time.monotonic() + (2 if url.endswith('short') else 60)
            barrier.wait(timeout=5)
            observed[url] = self.scraper.operation_timeout(30)
            return 200, PRODUCT, None, 0
        self.scraper.fetch_html = fetch
        rows = self.scraper.collect([dict(ROW, url=URL + '/short'), dict(ROW, url=URL + '/long')], sleep_seconds=0, workers=2)
        self.assertEqual([r['retailprice'] for r in rows], [123.45, 123.45])
        self.assertLessEqual(observed[URL + '/short'], 2)
        self.assertEqual(observed[URL + '/long'], 30)


if __name__ == '__main__':
    unittest.main()

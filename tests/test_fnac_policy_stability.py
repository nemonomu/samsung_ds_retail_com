"""Policy priority and waiting-room recovery, without live services or credentials."""
import json
import sys
import types
import unittest
from unittest.mock import Mock, patch

import test_fnac_auto_recovery as fixtures
from test_fnac_finalization import AMBIGUOUS, OOS, STORE_ONLY, URL, ROW

QUEUE = """<html><h1>Merci de votre visite. Vous pourrez accéder à notre site Web sous peu.</h1>
<p>Vous serez redirigé automatiquement.</p>
<p>Votre position dans la file d'attente : 891/897</p></html>"""


class PolicyStabilityTests(unittest.TestCase):
    def setUp(self):
        self.case = fixtures.FnacRecoveryTests()
        self.case.setUp()
        self.scraper, self.m = self.case.scraper, self.case.m

    def product(self, attrs):
        return fixtures.PRODUCT + '<script id="digitalData">' + json.dumps({'product': {'attributes': attrs}}) + '</script>'

    def test_all_exclusions_outrank_prices_before_and_after_capture(self):
        cases = [
            (OOS, 'ONLINE_STOCK_EXHAUSTED'),
            (STORE_ONLY, 'BASE_CLICK_COLLECT_FIRST_MARKETPLACE'),
            (self.product({'currentOffer': {'condition': 'used'}}), 'NON_NEW_OFFER_IGNORED'),
            (self.product({'offer': [{'sellerType': 'fnac'}], 'currentOffer': {'sellerType': 'professional'},
                           'salesCategory': '8', 'availabilityType': 'discontinued'}), 'DISCONTINUED_FNAC_FIRST_MARKETPLACE'),
            (self.product({'currentOffer': {'seller': 'ClickAndCollectOnly'}}), 'CLICK_AND_COLLECT_ONLY'),
        ]
        for body, reason in cases:
            with self.subTest(reason=reason):
                row, actual = self.scraper.parse_product(body, ROW)
                self.assertEqual(actual, reason)
                self.assertIsNone(row['retailprice'])
                row['_crawl_reason'] = reason
                fake, _, *_ = self.case.screenshot_context('ready')
                with patch.dict(sys.modules, {'playwright.sync_api': fake}):
                    self.assertEqual(self.scraper.capture_null_screenshot(row, URL), 'ok')
                self.assertIsNone(row['retailprice'])
                self.assertEqual(row['_crawl_reason'], reason)
                self.assertNotIn('_browser_reparse_reason', row)

    def test_new_marketplace_offer_is_allowed_without_exclusion(self):
        body = self.product({'offer': [{'sellerType': 'professional', 'seller': 'Example'}],
                             'sellerType': 'professional', 'condition': 'new',
                             'salesCategory': '9', 'availabilityType': 'discontinued'})
        row, reason = self.scraper.parse_product(body, ROW)
        self.assertEqual((row['retailprice'], reason), (123.45, 'VISIBLE_PRICE_BOX'))

    def test_explicit_offer_does_not_trigger_base_url_store_exclusion(self):
        row, reason = self.scraper.parse_product(STORE_ONLY, dict(ROW, url=fixtures.URL))
        self.assertEqual((row['retailprice'], reason), (123.45, 'VISIBLE_PRICE_BOX'))

    def test_recovery_can_confirm_oos_but_never_override_it_with_price(self):
        row = {'retailersku': 'synthetic', '_crawl_reason': 'HTTP_422'}
        fake, _, page, *_ = self.case.screenshot_context('ready')
        page.content.return_value = OOS
        with patch.dict(sys.modules, {'playwright.sync_api': fake}):
            self.assertEqual(self.scraper.capture_null_screenshot(row, URL), 'ok')
        self.assertIsNone(row['retailprice'])
        self.assertEqual(row['_browser_reparse_reason'], 'ONLINE_STOCK_EXHAUSTED')

    def test_waiting_room_is_not_a_product_or_stock_null(self):
        row, reason = self.scraper.parse_product(QUEUE, ROW)
        self.assertEqual(reason, 'WAITING_ROOM')
        self.assertIsNone(row['title'])
        self.assertIsNone(row['retailprice'])
        self.assertFalse(self.scraper.product_html_ready(QUEUE, URL))
        self.assertFalse(self.m.is_confirmed_null_reason(reason))
        self.assertFalse(self.m.is_waiting_room(fixtures.PRODUCT + '<script>' + QUEUE + '</script>'))

    def test_queue_wait_uses_same_page_until_redirect(self):
        page = Mock()
        page.content.side_effect = [QUEUE, QUEUE, fixtures.PRODUCT]
        self.scraper.pause = Mock()
        self.assertEqual(self.scraper.wait_for_queue_exit(page, 10), fixtures.PRODUCT)
        self.assertEqual(self.scraper.pause.call_count, 2)
        page.goto.assert_not_called()
        page.reload.assert_not_called()

    def test_queue_deadline_is_finite(self):
        page = Mock()
        page.content.return_value = QUEUE
        clock = [0.0]
        self.scraper.pause = lambda seconds, page=None: clock.__setitem__(0, clock[0] + seconds)
        with patch.object(self.m.time, 'monotonic', side_effect=lambda: clock[0]):
            self.assertEqual(self.scraper.wait_for_queue_exit(page, 3), QUEUE)
        self.assertEqual(clock[0], 3)
        page.reload.assert_not_called()

    def test_queue_product_budget_remains_enforced(self):
        self.scraper._product_clock.deadline = 2
        page = Mock()
        page.content.return_value = QUEUE
        clock = [0.0]
        page.wait_for_timeout.side_effect = lambda ms: clock.__setitem__(0, clock[0] + ms / 1000)
        with patch.object(self.m.time, 'monotonic', side_effect=lambda: clock[0]):
            with self.assertRaises(self.m.ProductTimeBudgetExceeded):
                self.scraper.wait_for_queue_exit(page, 90)
        self.assertEqual(clock[0], 2)

    def test_queue_capture_stops_without_reload_or_new_session(self):
        fake, playwright, page, context, browser = self.case.screenshot_context('waiting_room')
        with patch.dict(sys.modules, {'playwright.sync_api': fake}):
            self.assertEqual(self.scraper.capture_null_screenshot(dict(ROW), URL), 'fail')
        self.assertEqual(self.scraper.load_screenshot_page.call_count, 1)
        self.assertEqual(playwright.chromium.connect_over_cdp.call_count, 1)
        browser.close.assert_called_once()
        self.m.capture_and_upload.assert_not_called()

    def test_initial_queue_uses_browser_and_preserves_policy(self):
        self.scraper.capture_null = False
        self.scraper.fetch_html = Mock(return_value=(200, QUEUE, None, 0))
        self.scraper.fetch_verified_html = Mock()
        self.scraper.fetch_browser_html = Mock(return_value=(200, STORE_ONLY, 0))
        row = self.scraper.collect_one(ROW)
        self.assertIsNone(row['retailprice'])
        self.assertEqual(row['_crawl_reason'], 'BROWSER_BASE_CLICK_COLLECT_FIRST_MARKETPLACE')
        self.scraper.fetch_verified_html.assert_not_called()

    def test_browser_queue_redirect_accepts_product_after_initial_503(self):
        fake, playwright, page, _, browser = self.case.screenshot_context('ready')
        browser.new_page.return_value = page
        page.goto = Mock(return_value=types.SimpleNamespace(status=503))
        page.content.side_effect = [QUEUE, QUEUE, fixtures.PRODUCT]
        self.scraper.pause = Mock()
        with patch.dict(sys.modules, {'playwright.sync_api': fake}):
            status, body, _ = self.scraper.fetch_browser_html(URL)
        self.assertEqual((status, body), (200, fixtures.PRODUCT))
        self.assertEqual(playwright.chromium.connect_over_cdp.call_count, 1)
        page.goto.assert_called_once()
        browser.close.assert_called_once()

    def test_browser_queue_timeout_does_not_start_a_second_browser(self):
        fake, playwright, page, _, browser = self.case.screenshot_context('ready')
        browser.new_page.return_value = page
        page.goto = Mock(return_value=types.SimpleNamespace(status=200))
        page.content.return_value = QUEUE
        self.scraper.wait_for_queue_exit = Mock(return_value=QUEUE)
        self.scraper.pause = Mock()
        with patch.dict(sys.modules, {'playwright.sync_api': fake}):
            status, body, _ = self.scraper.fetch_browser_html(URL)
        self.assertEqual((status, body), (429, QUEUE))
        self.assertEqual(playwright.chromium.connect_over_cdp.call_count, 1)
        browser.close.assert_called_once()

    def test_persistent_queue_is_reported_and_not_restarted_for_capture(self):
        self.scraper.fetch_html = Mock(return_value=(200, QUEUE, None, 0))
        self.scraper.fetch_browser_html = Mock(return_value=(429, QUEUE, 0))
        self.scraper.capture_null_screenshot = Mock()
        row = self.scraper.collect_one(ROW)
        self.assertEqual(row['_crawl_reason'], 'WAITING_ROOM_TIMEOUT')
        self.assertIsNone(row['retailprice'])
        self.scraper.capture_null_screenshot.assert_not_called()

    def test_initial_retry_budget_leaves_time_for_browser_fallback(self):
        self.scraper.capture_null = False
        self.scraper.product_time_budget = 90
        clock = [0.0]
        session = Mock()
        def timeout(*args, **kwargs):
            self.assertEqual(kwargs['timeout'], 60)
            clock[0] += kwargs['timeout']
            raise self.m.requests.ReadTimeout()
        session.get.side_effect = timeout
        def browser(url):
            self.assertEqual(self.scraper.operation_timeout(90), 30)
            return 200, fixtures.PRODUCT, 0
        self.scraper.fetch_browser_html = Mock(side_effect=browser)
        with patch.object(self.m.time, 'monotonic', side_effect=lambda: clock[0]), patch.object(self.m.requests, 'Session', return_value=session):
            row = self.scraper.collect_one(ROW)
        session.get.assert_called_once()
        self.assertEqual(row['retailprice'], 123.45)
        self.assertFalse(hasattr(self.scraper._product_clock, 'deadline'))

    def test_verification_request_preserves_browser_fallback_budget(self):
        clock = [0.0]
        self.scraper._product_clock.deadline = 90
        session = Mock()
        def timeout(*args, **kwargs):
            self.assertEqual(kwargs['timeout'], 45)
            clock[0] += kwargs['timeout']
            raise self.m.requests.ReadTimeout()
        session.get.side_effect = timeout
        def browser(url):
            self.assertEqual(self.scraper.operation_timeout(90), 45)
            return 200, fixtures.PRODUCT, 0
        self.scraper.fetch_browser_html = Mock(side_effect=browser)
        with patch.object(self.m.time, 'monotonic', side_effect=lambda: clock[0]), patch.object(self.m.requests, 'Session', return_value=session):
            status, body, _ = self.scraper.fetch_verified_html(URL)
        self.assertEqual((status, body), (200, fixtures.PRODUCT))


if __name__ == '__main__':
    unittest.main()

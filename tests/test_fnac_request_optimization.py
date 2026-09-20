"""Request budgets, browser ownership, and event-based waits without services."""
import sys
import time
import types
import unittest
from unittest.mock import Mock, patch

import test_fnac_auto_recovery as fixtures
from test_fnac_finalization import OOS, ROW, URL


class RequestOptimizationTests(unittest.TestCase):
    def setUp(self):
        self.case = fixtures.FnacRecoveryTests()
        self.case.setUp()
        self.s, self.m = self.case.scraper, self.case.m

    def browser(self, body=OOS):
        fake, pw, page, context, browser = self.case.screenshot_context('ready')
        page.content.return_value = body
        page.goto = Mock(return_value=types.SimpleNamespace(status=200))
        self.s.pause = Mock()
        self.s.wait_for_product_html = Mock(return_value=body)
        self.s.accept_cookie_popup = Mock(return_value=False)
        return fake, pw, page, context, browser

    def test_timeout_uses_one_http_attempt_instead_of_repeating_full_timeout(self):
        session = Mock()
        session.get.side_effect = self.m.requests.ReadTimeout()
        self.s.pause = Mock()
        with patch.object(self.m.requests, 'Session', return_value=session):
            self.assertEqual(self.s.fetch_html(URL)[0], 0)
        session.get.assert_called_once()
        session.close.assert_called_once()
        self.s.pause.assert_not_called()

    def test_transient_status_is_limited_to_two_attempts(self):
        session = Mock()
        session.get.return_value = self.case.response(503, 'unavailable')
        self.s.pause = Mock()
        with patch.object(self.m.requests, 'Session', return_value=session):
            self.s.fetch_html(URL)
        self.assertEqual(session.get.call_count, 2)

    def test_fallback_null_reuses_browser_and_closes_every_resource_once(self):
        fake, pw, page, context, browser = self.browser()
        self.s.fetch_html = Mock(return_value=(503, '', None, 0))
        with patch.dict(sys.modules, {'playwright.sync_api': fake}):
            result = self.s.collect_one(ROW)
        self.assertIsNone(result['retailprice'])
        self.assertEqual(result['_s3_upload'], 'ok')
        self.assertEqual(pw.chromium.connect_over_cdp.call_count, 1)
        self.assertEqual(self.s.total_browser_calls, 1)
        self.assertEqual(self.s.total_screenshot_success, 1)
        self.s.load_screenshot_page.assert_not_called()
        self.s.warmup_fnac_screenshot_session.assert_not_called()
        for resource in (page, context, browser):
            resource.close.assert_called_once()
        pw.stop.assert_called_once()
        self.assertFalse(hasattr(self.s._product_clock, 'browser_lease'))

    def test_successful_price_closes_retained_browser_without_capture(self):
        fake, pw, page, context, browser = self.browser(fixtures.PRODUCT)
        self.s.fetch_html = Mock(return_value=(503, '', None, 0))
        with patch.dict(sys.modules, {'playwright.sync_api': fake}):
            result = self.s.collect_one(ROW)
        self.assertEqual(result['retailprice'], 123.45)
        self.assertEqual(result['_s3_upload'], 'skip')
        self.m.capture_and_upload.assert_not_called()
        browser.close.assert_called_once()
        pw.stop.assert_called_once()

    def test_capture_disabled_does_not_keep_paid_browser_alive(self):
        fake, pw, page, context, browser = self.browser()
        self.s.capture_null = False
        self.s._product_clock.deadline = time.monotonic() + 90
        with patch.dict(sys.modules, {'playwright.sync_api': fake}):
            self.s.fetch_browser_html(URL)
        self.assertFalse(hasattr(self.s._product_clock, 'browser_lease'))
        browser.close.assert_called_once()
        pw.stop.assert_called_once()

    def test_page_creation_failure_still_releases_paid_browser(self):
        fake, pw, page, context, browser = self.browser()
        self.s.browser_retries = 1
        context.new_page.side_effect = RuntimeError('do-not-log')
        with patch.dict(sys.modules, {'playwright.sync_api': fake}):
            self.assertEqual(self.s.fetch_browser_html(URL)[0], 0)
        context.close.assert_called_once()
        browser.close.assert_called_once()
        pw.stop.assert_called_once()

    def test_borrowed_upload_failure_respects_total_attempt_limit(self):
        fake, pw, page, context, browser = self.browser()
        self.s.screenshot_max_attempts = 1
        self.m.capture_and_upload.return_value = None
        self.s.fetch_html = Mock(return_value=(503, '', None, 0))
        with patch.dict(sys.modules, {'playwright.sync_api': fake}):
            result = self.s.collect_one(ROW)
        self.assertEqual(result['_s3_upload'], 'fail')
        self.assertIsNone(result['retailprice'])
        self.assertEqual(pw.chromium.connect_over_cdp.call_count, 1)
        browser.close.assert_called_once()

    def test_capture_exception_always_releases_browser(self):
        fake, pw, page, context, browser = self.browser()
        self.s.fetch_html = Mock(return_value=(503, '', None, 0))
        self.s.capture_null_screenshot = Mock(side_effect=self.m.ProductTimeBudgetExceeded())
        with patch.dict(sys.modules, {'playwright.sync_api': fake}):
            result = self.s.collect_one(ROW)
        self.assertIsNone(result['retailprice'])
        browser.close.assert_called_once()
        pw.stop.assert_called_once()

    def test_stable_product_proceeds_after_one_second_without_network_idle(self):
        page = Mock()
        page.content.return_value = fixtures.PRODUCT
        clock = [0.0]
        self.s.pause = lambda seconds, page=None: clock.__setitem__(0, clock[0]+seconds)
        with patch.object(self.m.time, 'monotonic', side_effect=lambda: clock[0]):
            self.assertEqual(self.s.wait_for_product_html(page, URL, 6), fixtures.PRODUCT)
        self.assertEqual(clock[0], 1)
        page.wait_for_load_state.assert_not_called()

    def test_changing_price_is_not_accepted_at_first_observation(self):
        page = Mock()
        changed = fixtures.PRODUCT.replace('123,45', '234,56')
        page.content.side_effect = [fixtures.PRODUCT, changed, changed]
        clock = [0.0]
        self.s.pause = lambda seconds, page=None: clock.__setitem__(0, clock[0]+seconds)
        with patch.object(self.m.time, 'monotonic', side_effect=lambda: clock[0]):
            self.assertEqual(self.s.wait_for_product_html(page, URL, 6), changed)
        self.assertEqual(clock[0], 2)

    def test_screenshot_load_has_no_implicit_twelve_second_or_ad_wait(self):
        page = Mock()
        self.s.prepare_playwright_page_for_capture = Mock()
        self.s.accept_cookie_popup = Mock()
        self.s.wait_for_product_html = Mock()
        self.s.pause = Mock()
        self.s.load_screenshot_page(page, URL, 150)
        self.s.pause.assert_called_once_with(0.15, page)
        page.wait_for_load_state.assert_not_called()

    def test_absent_cookie_popup_has_no_fixed_sleep(self):
        page = Mock()
        page.locator.return_value.first.count.return_value = 0
        page.evaluate.return_value = False
        self.s.pause = Mock()
        self.assertFalse(self.s.accept_cookie_popup(page))
        self.s.pause.assert_not_called()

    def test_new_screenshot_session_prepares_homepage_before_product(self):
        fake, pw, page, *_ = self.case.screenshot_context('ready')
        page.content.return_value = OOS
        row = dict(ROW, retailprice=None, _crawl_reason='ONLINE_STOCK_EXHAUSTED')
        with patch.dict(sys.modules, {'playwright.sync_api': fake}):
            self.assertEqual(self.s.capture_null_screenshot(row, URL), 'ok')
        self.s.warmup_fnac_screenshot_session.assert_called_once_with(page)

    def test_wrong_product_cannot_be_uploaded_as_evidence(self):
        page = Mock()
        page.content.return_value = '<link rel="canonical" href="https://www.fnac.com/other/a999/w-4">'+fixtures.PRODUCT
        self.assertEqual(self.s.capture_ready_page(page, ROW, URL, URL), 'fail')
        self.m.capture_and_upload.assert_not_called()

    def test_denied_browser_response_skips_product_wait(self):
        fake,pw,page,context,browser=self.browser('<h1>fnac.com</h1>')
        self.s.browser_retries=1
        page.goto.return_value=types.SimpleNamespace(status=403)
        with patch.dict(sys.modules,{'playwright.sync_api':fake}):
            self.assertEqual(self.s.fetch_browser_html(URL)[0],403)
        self.s.wait_for_product_html.assert_not_called()
        browser.close.assert_called_once()

    def test_denied_screenshot_skips_wait_and_reload(self):
        fake,pw,page,*_=self.browser('<h1>fnac.com</h1>')
        self.s.screenshot_max_attempts=1
        self.s.load_screenshot_page.return_value=types.SimpleNamespace(status=403)
        with patch.dict(sys.modules,{'playwright.sync_api':fake}):
            self.assertEqual(self.s.capture_null_screenshot(dict(ROW),URL),'fail')
        self.s.load_screenshot_page.assert_called_once()
        self.s.wait_for_screenshot_ready.assert_not_called()
        self.m.capture_and_upload.assert_not_called()

    def test_denied_status_does_not_discard_a_queue_or_redirected_product(self):
        from test_fnac_policy_stability import QUEUE
        page=Mock()
        self.s.wait_for_screenshot_ready=Mock(return_value='ready')
        for body in (QUEUE,fixtures.PRODUCT):
            page.content.return_value=body
            self.assertEqual(self.s.screenshot_response_readiness(page,types.SimpleNamespace(status=403),'sku',URL),'ready')
        self.assertEqual(self.s.wait_for_screenshot_ready.call_count,2)

    def test_valid_product_without_price_can_still_have_null_evidence(self):
        page = Mock()
        page.content.return_value = fixtures.PRODUCT.replace('<span class="f-faPriceBox__price">123,45</span>','')
        result,reason=self.s.parse_product(page.content(),ROW)
        self.assertEqual(reason,'PRICE_NOT_FOUND')
        result['_crawl_reason']=reason
        self.assertEqual(self.s.capture_ready_page(page,result,URL,URL),'ok')
        self.assertIsNone(result['retailprice'])
        self.m.capture_and_upload.assert_called_once()

    def test_request_metrics_reset_between_products(self):
        session = Mock()
        session.get.return_value = self.case.response()
        self.s.capture_null = False
        with patch.object(self.m.requests, 'Session', return_value=session), self.assertLogs(self.m.logger,level='INFO') as logs:
            self.s.collect_one(ROW)
            self.s.collect_one(ROW)
        completed=[line for line in logs.output if 'FNAC product completed' in line]
        self.assertEqual(len(completed),2)
        self.assertTrue(all('api_calls=1 browser_sessions=0' in line for line in completed))
        self.assertFalse(hasattr(self.s._product_clock,'stats'))

    def test_browser_ownership_is_isolated_between_workers(self):
        from concurrent.futures import ThreadPoolExecutor
        import threading
        barrier=threading.Barrier(2)
        def worker(index):
            lease={'started':time.time(),'browser':Mock()}
            self.s._product_clock.browser_lease=lease
            self.s._product_clock.stats={'api_calls':0,'browser_sessions':0}
            barrier.wait(timeout=5)
            self.s.close_product_browser()
            lease['browser'].close.assert_called_once()
            return self.s._product_clock.stats['browser_sessions'],hasattr(self.s._product_clock,'browser_lease')
        with ThreadPoolExecutor(max_workers=2) as pool:
            self.assertEqual(list(pool.map(worker,range(2))),[(1,False),(1,False)])
        self.assertEqual(self.s.total_browser_calls,2)


if __name__ == '__main__':
    unittest.main()

"""Final browser decision, matching evidence, and bounded retries; no live services."""
import json
import sys
import tempfile
import time
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

import test_fnac_auto_recovery as fixtures
from test_fnac_finalization import OOS, STORE_ONLY, ROW, URL
from test_fnac_monitoring_registration import load_uploader


class FinalCaptureTests(unittest.TestCase):
    def setUp(self):
        self.case = fixtures.FnacRecoveryTests()
        self.case.setUp()
        self.s, self.m = self.case.scraper, self.case.m
        self.s.pause = Mock()

    def result(self, body=STORE_ONLY):
        result, reason = self.s.parse_product(body, ROW)
        result['_crawl_reason'] = reason
        return result

    def page(self, body):
        page = fixtures.Page()
        page.content.return_value = body
        return page

    def test_initial_store_null_becomes_deliverable_marketplace_price(self):
        body = fixtures.PRODUCT.replace('123,45', '352,00') + '<script id="digitalData">' + json.dumps({
            'product': {'attributes': {'currentOffer': {'seller': 'AGEM', 'sellerType': 'professional', 'condition': 'new'}}}
        }) + '</script>'
        page = self.page(body)
        page.state['visiblePriceTexts'] = ['352,00']
        row = self.result()
        self.assertEqual(self.s.capture_ready_page(page, row, URL, URL), 'skip')
        self.assertEqual(row['retailprice'], 352.0)
        self.assertEqual(row['_browser_reparse_html'], body)
        self.assertEqual(row['_browser_reparse_reason'], 'VISIBLE_PRICE_BOX')
        page.screenshot.assert_called_once()
        self.m.capture_and_upload.assert_not_called()

    def test_final_null_updates_title_reason_html_and_uploads_exact_capture(self):
        body = OOS.replace('Samsung SSD', 'Final SSD title')
        page = self.page(body)
        row = self.result()
        self.assertEqual(self.s.capture_ready_page(page, row, URL, URL), 'ok')
        self.assertIsNone(row['retailprice'])
        self.assertEqual(row['title'], 'Final SSD title')
        self.assertEqual(row['_browser_reparse_reason'], 'ONLINE_STOCK_EXHAUSTED')
        self.assertEqual(row['_browser_reparse_html'], body)
        page.screenshot.assert_called_once()
        args, kwargs = self.m.capture_and_upload.call_args
        self.assertIs(args[4], row)
        self.assertEqual(kwargs['screenshot_bytes'], b'synthetic PNG')

    def test_final_null_snapshot_is_the_capture_page_and_replays(self):
        with tempfile.TemporaryDirectory() as folder:
            self.s.save_html_dir = Path(folder)
            self.s.fetch_html = Mock(return_value=(200, STORE_ONLY, None, 0))
            fake, _, page, *_ = self.case.screenshot_context('ready')
            page.content.return_value = OOS
            with patch.dict(sys.modules, {'playwright.sync_api': fake}):
                row = self.s.collect_one(ROW)
            self.assertEqual(row['_crawl_reason'], 'BROWSER_RECOVERED_ONLINE_STOCK_EXHAUSTED')
            self.assertEqual((Path(folder)/'synthetic.html').read_text(encoding='utf-8'), OOS)
            self.assertEqual((Path(folder)/'synthetic.initial.html').read_text(encoding='utf-8'), STORE_ONLY)
            replay = self.m.FnacZenRowsScraper(html_dir=folder, capture_null=False).collect_one(ROW)
            self.assertEqual((replay['retailprice'], replay['_crawl_reason']), (None, row['_crawl_reason']))

    def test_page_changes_during_capture_keeps_previous_result_and_does_not_upload(self):
        row = self.result()
        before = dict(row)
        page = self.page(OOS)
        page.screenshot.side_effect = lambda **kwargs: (setattr(page.content, 'return_value', fixtures.PRODUCT) or b'image')
        self.assertEqual(self.s.capture_ready_page(page, row, URL, URL), 'fail')
        self.assertEqual(row, before)
        self.m.capture_and_upload.assert_not_called()

    def test_seller_change_even_with_same_price_and_null_reason_is_rejected(self):
        def offer(seller):
            return OOS + '<script id="digitalData">' + json.dumps({'product': {'attributes': {
                'currentOffer': {'seller': seller, 'condition': 'new'}}}}) + '</script>'
        page = self.page(offer('seller A'))
        page.content.side_effect = [offer('seller A'), offer('seller B')]
        row = self.result()
        self.assertEqual(self.s.capture_ready_page(page, row, URL, URL), 'fail')
        self.m.capture_and_upload.assert_not_called()

    def test_unrelated_ad_change_does_not_reject_evidence(self):
        page = self.page(OOS)
        page.content.side_effect = [OOS+'<aside>ad A</aside>', OOS+'<aside>ad B</aside>']
        self.assertEqual(self.s.capture_ready_page(page, self.result(), URL, URL), 'ok')

    def test_wrong_product_and_screenshot_exception_preserve_initial_fields(self):
        for body, failure in [(OOS, RuntimeError('synthetic')), ('<link rel="canonical" href="https://www.fnac.com/item/a999/w-4">'+OOS, None)]:
            row = self.result(); before = dict(row)
            page = self.page(body)
            page.screenshot.side_effect = failure
            self.assertEqual(self.s.capture_ready_page(page, row, URL, URL), 'fail')
            self.assertEqual(row, before)
        self.m.capture_and_upload.assert_not_called()

    def test_third_attempt_succeeds_with_exact_waits(self):
        fake, pw, page, *_ = self.case.screenshot_context('ready')
        page.content.return_value = OOS
        self.s.wait_for_screenshot_ready.side_effect = ['restricted', 'restricted', 'ready']
        self.assertEqual(self.s.screenshot_max_attempts, 3)
        with patch.dict(sys.modules, {'playwright.sync_api': fake}):
            self.assertEqual(self.s.capture_null_screenshot(self.result(), URL), 'ok')
        self.assertEqual(pw.chromium.connect_over_cdp.call_count, 3)
        self.assertEqual([c.args[0] for c in self.s.pause.call_args_list], [5, 10])
        self.m.capture_and_upload.assert_called_once()

    def test_failed_reused_page_counts_toward_three_and_uses_same_waits(self):
        fake, pw, page, context, browser = self.case.screenshot_context('restricted')
        self.s.accept_cookie_popup = Mock(return_value=False)
        self.s._product_clock.browser_lease = {'page': page, 'context': context, 'browser': browser,
                                             'url': URL, 'started': time.time()}
        row = self.result(); before = row.copy()
        with patch.dict(sys.modules, {'playwright.sync_api': fake}):
            self.assertEqual(self.s.capture_null_screenshot(row, URL), 'fail')
        self.assertEqual(pw.chromium.connect_over_cdp.call_count, 2)
        self.assertEqual(self.s.wait_for_screenshot_ready.call_count, 3)
        self.assertEqual([c.args[0] for c in self.s.pause.call_args_list], [5, 10])
        for field in ('title','imageurl','retailprice','_crawl_reason'):
            self.assertEqual(row[field], before[field])

    def test_no_retry_wait_after_first_success(self):
        fake, pw, page, *_ = self.case.screenshot_context('ready')
        page.content.return_value = OOS
        with patch.dict(sys.modules, {'playwright.sync_api': fake}):
            self.assertEqual(self.s.capture_null_screenshot(self.result(), URL), 'ok')
        self.s.pause.assert_not_called()
        self.assertEqual(pw.chromium.connect_over_cdp.call_count, 1)

    def test_exhausted_budget_prevents_another_connection_during_retry_wait(self):
        fake, pw, *_ = self.case.screenshot_context('restricted')
        self.s.pause.side_effect = self.m.ProductTimeBudgetExceeded()
        self.s.fetch_html = Mock(return_value=(200, OOS, None, 0))
        with patch.dict(sys.modules, {'playwright.sync_api': fake}):
            row = self.s.collect_one(ROW)
        self.assertEqual(pw.chromium.connect_over_cdp.call_count, 1)
        self.assertEqual(row['_screenshot_reason'], 'time_budget_exhausted')
        self.assertEqual(row['_crawl_reason'], 'ONLINE_STOCK_EXHAUSTED')

    def test_pre_captured_upload_never_touches_browser_again(self):
        uploader = load_uploader()
        uploader._delete_existing_screenshots = Mock()
        driver = Mock()
        self.assertTrue(uploader.capture_and_upload(driver,'fnac','synthetic',URL,self.result(),
            require_monitoring_link=True,screenshot_bytes=b'already captured'))
        uploader._capture_bytes.assert_not_called()
        self.assertEqual(uploader._get_s3_client().put_object.call_args.kwargs['Body'],b'already captured')
        self.assertEqual(driver.mock_calls, [])

    def test_empty_pre_captured_bytes_do_not_trigger_an_unrelated_recapture(self):
        uploader = load_uploader()
        self.assertIsNone(uploader.capture_and_upload(Mock(),'fnac','synthetic',URL,
                                                    screenshot_bytes=b''))
        uploader._capture_bytes.assert_not_called()
        uploader._get_s3_client().put_object.assert_not_called()


if __name__=='__main__':
    unittest.main()

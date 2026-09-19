"""Late photo-only recovery; no real config, network, database or storage."""
import ast
import json
import logging
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import Mock, patch

import pandas as pd
import test_fnac_auto_recovery as fixtures
from test_fnac_finalization import ROW, URL, OOS, STORE_ONLY, PRODUCT
from test_fnac_monitoring_registration import load_uploader


def load_coordinator():
    path = Path(__file__).resolve().parents[1] / 'auto_recovery.py'
    tree = ast.parse(path.read_text(encoding='utf-8-sig'))
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'auto_recovery_fnac_v3')
    module = types.ModuleType('offline_fnac_recovery')
    module.time = types.SimpleNamespace(sleep=Mock())
    module.logger = logging.getLogger('offline.fnac.recovery')
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), 'exec'), module.__dict__)
    return module


class EvidenceRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.case = fixtures.FnacRecoveryTests()
        self.case.setUp()
        self.s, self.m = self.case.scraper, self.case.m
        self.s.pause = Mock()
        self.coordinator = load_coordinator()
        self.s.fetch_html = Mock(side_effect=AssertionError('HTML API must not repeat'))

    def pending(self, body=STORE_ONLY):
        result, reason = self.s.parse_product(body, ROW)
        result.update(_crawl_reason=reason, _s3_upload='fail', _screenshot_reason='restricted',
                      _evidence_recovery_html=body, _evidence_recovery_row=dict(ROW))
        return result

    def run_recovery(self, rows):
        return self.coordinator.auto_recovery_fnac_v3(self.s, rows)

    def test_only_price_null_and_failed_photo_is_selected(self):
        self.s.recover_null_evidence = Mock(side_effect=lambda row, **kw: dict(row, _s3_upload='ok'))
        normal = dict(self.pending(), retailprice=300)
        photographed = dict(self.pending(), _s3_upload='ok')
        disabled = dict(self.pending(), _s3_upload='skip')
        missing_image_only = dict(normal, imageurl=None)
        pending = self.pending()
        results = self.run_recovery([normal, photographed, disabled, missing_image_only, pending])
        self.coordinator.time.sleep.assert_called_once_with(120)
        self.s.recover_null_evidence.assert_called_once_with(pending, max_attempts=2)
        self.assertIs(results[0], normal)
        self.assertIs(results[1], photographed)
        self.assertEqual(results[4]['_s3_upload'], 'ok')

    def test_no_candidates_means_no_wait_or_recovery(self):
        self.s.recover_null_evidence = Mock()
        self.run_recovery([dict(self.pending(), _s3_upload='ok')])
        self.coordinator.time.sleep.assert_not_called()
        self.s.recover_null_evidence.assert_not_called()

    def test_disabled_capture_and_replay_do_not_retry(self):
        self.s.recover_null_evidence = Mock()
        for capture, replay in ((False, None), (True, Path('offline'))):
            self.s.capture_null, self.s.html_dir = capture, replay
            self.run_recovery([self.pending()])
        self.coordinator.time.sleep.assert_not_called()
        self.s.recover_null_evidence.assert_not_called()

    def test_keeps_null_rule_and_registers_from_final_browser(self):
        fake, pw, page, *_ = self.case.screenshot_context('ready')
        page.content.return_value = OOS
        with patch.dict(sys.modules, {'playwright.sync_api': fake}):
            result = self.run_recovery([self.pending()])[0]
        self.assertIsNone(result['retailprice'])
        self.assertEqual(result['_crawl_reason'], 'BROWSER_RECOVERED_ONLINE_STOCK_EXHAUSTED')
        self.assertEqual(result['_s3_upload'], 'ok')
        self.assertEqual(pw.chromium.connect_over_cdp.call_count, 1)
        self.s.fetch_html.assert_not_called()

    def test_normal_recovery_skips_photo_and_preserves_result_identity(self):
        fake, pw, page, *_ = self.case.screenshot_context('ready')
        page.content.return_value = PRODUCT
        original = self.pending()
        self.s.error_logs = [f'{URL}: NULL screenshot upload failed', 'other product error']
        with patch.dict(sys.modules, {'playwright.sync_api': fake}):
            result = self.run_recovery([original])[0]
        self.assertEqual(result['retailprice'], 123.45)
        self.assertEqual(result['_s3_upload'], 'skip')
        self.assertEqual(result['_collection_status'], 'complete')
        for key in ('retailersku', 'producturl', 'crawl_strdatetime'):
            self.assertEqual(result[key], original[key])
        self.m.capture_and_upload.assert_not_called()
        self.assertEqual(self.s.error_logs, ['other product error'])

    def test_persistent_block_ends_after_two_extra_sessions(self):
        fake, pw, *_ = self.case.screenshot_context('restricted')
        with patch.dict(sys.modules, {'playwright.sync_api': fake}):
            result = self.run_recovery([self.pending()])[0]
        self.assertEqual(pw.chromium.connect_over_cdp.call_count, 2)
        self.assertEqual([call.args[0] for call in self.s.pause.call_args_list], [5])
        self.assertEqual(result['_s3_upload'], 'fail')
        self.assertEqual(result['_screenshot_reason'], 'restricted')
        self.assertEqual(self.s.screenshot_max_attempts, 3)
        self.assertFalse(hasattr(self.s._product_clock, 'deadline'))

    def test_cached_png_registration_uses_no_browser_or_html_api(self):
        row = self.pending(OOS)
        row['_pending_screenshot_bytes'] = b'accepted screenshot'
        row['_screenshot_reason'] = 'monitoring_link_failed'
        self.s.capture_null_screenshot = Mock(side_effect=AssertionError('No fresh browser'))
        result = self.run_recovery([row])[0]
        self.assertEqual(result['_s3_upload'], 'ok')
        self.assertEqual(self.m.capture_and_upload.call_args.kwargs['screenshot_bytes'], b'accepted screenshot')
        self.assertIsNone(self.m.capture_and_upload.call_args.args[0])
        self.s.capture_null_screenshot.assert_not_called()
        self.s.fetch_html.assert_not_called()
        self.assertNotIn('_pending_screenshot_bytes', result)

    def test_registration_still_fails_without_buying_new_browser(self):
        row = self.pending(OOS)
        row['_pending_screenshot_bytes'] = b'accepted screenshot'
        row['_screenshot_reason'] = 'monitoring_link_failed'
        self.m.capture_and_upload.return_value = None
        self.s.capture_null_screenshot = Mock()
        result = self.run_recovery([row])[0]
        self.assertEqual(result['_s3_upload'], 'fail')
        self.s.capture_null_screenshot.assert_not_called()
        self.assertNotIn('_pending_screenshot_bytes', result)

    def test_initial_upload_failure_retains_png_without_more_browsers(self):
        fake, pw, page, *_ = self.case.screenshot_context('ready')
        page.content.return_value = OOS
        self.m.capture_and_upload.return_value = None
        row = self.pending(OOS)
        with patch.dict(sys.modules, {'playwright.sync_api': fake}):
            self.assertEqual(self.s.capture_null_screenshot(row, URL), 'fail')
        self.assertEqual(pw.chromium.connect_over_cdp.call_count, 1)
        self.assertEqual(row['_pending_screenshot_bytes'], b'synthetic PNG')
        self.assertEqual(row['_screenshot_reason'], 'upload_failed')

    def test_updated_snapshot_reuses_original_indexed_path(self):
        fake, pw, page, *_ = self.case.screenshot_context('ready')
        page.content.return_value = OOS
        row = self.pending()
        row['_evidence_recovery_row']['_crawl_index'] = 23
        with tempfile.TemporaryDirectory() as directory:
            self.s.save_html_dir = Path(directory)
            with patch.dict(sys.modules, {'playwright.sync_api': fake}):
                result = self.run_recovery([row])[0]
            snapshot = json.loads((Path(directory) / '23_synthetic.result.json').read_text(encoding='utf-8'))
            self.assertEqual(snapshot['result']['_crawl_reason'], result['_crawl_reason'])
            self.assertEqual(snapshot['diagnostics']['_s3_upload'], 'ok')
            self.assertEqual((Path(directory) / '23_synthetic.html').read_text(encoding='utf-8'), OOS)

    def test_blank_image_is_not_cached_as_registration_failure(self):
        uploader = load_uploader()
        uploader._is_blank_or_white_screenshot.return_value = True
        self.m.capture_and_upload = uploader.capture_and_upload
        page = fixtures.Page()
        page.content.return_value = OOS
        row = self.pending(OOS)
        self.assertEqual(self.s.capture_ready_page(page, row, URL, URL), 'fail')
        self.assertEqual(row['_screenshot_reason'], 'blank_image')
        self.assertNotIn('_pending_screenshot_bytes', row)
        uploader._get_s3_client().put_object.assert_not_called()

    def test_one_recovery_exception_does_not_stop_other_candidates(self):
        self.s.recover_null_evidence = Mock(side_effect=[RuntimeError('synthetic'), dict(self.pending(), _s3_upload='ok')])
        result = self.run_recovery([self.pending(), self.pending()])
        self.assertEqual([row['_s3_upload'] for row in result], ['fail', 'ok'])
        self.assertEqual(self.s.recover_null_evidence.call_count, 2)

    def test_budget_exhaustion_keeps_initial_null_and_cleans_resources(self):
        self.s.capture_null_screenshot = Mock(side_effect=self.m.ProductTimeBudgetExceeded())
        row = self.run_recovery([self.pending()])[0]
        self.assertEqual(row['_screenshot_reason'], 'time_budget_exhausted')
        self.assertEqual(row['_crawl_reason'], 'BASE_CLICK_COLLECT_FIRST_MARKETPLACE')
        self.assertFalse(hasattr(self.s._product_clock, 'deadline'))

    def test_main_merges_recovery_before_final_save(self):
        events = []
        scraper = Mock()
        scraper.db_engine = object()
        scraper.error_logs = []
        scraper.get_crawl_targets.return_value = [ROW]
        scraper.collect.return_value = [self.pending()]
        def recover(instance, rows, **options):
            events.append('recovery')
            self.assertEqual(options, {'wait_seconds': 120, 'max_attempts': 2})
            return [dict(rows[0], retailprice=300, _s3_upload='skip')]
        def save(frame, **kwargs):
            events.append('save')
            self.assertEqual(frame.iloc[0]['retailprice'], 300)
            self.assertFalse(any(str(c).startswith('_') for c in frame.columns))
            return {'db_saved': True, 'server_uploaded': True}
        scraper.save_results.side_effect = save
        module = types.ModuleType('auto_recovery')
        module.auto_recovery_fnac_v3 = recover
        self.m.pd = pd
        self.m.os = types.SimpleNamespace(path=__import__('os').path)
        self.m.monitor_and_alert = Mock()
        with patch.object(self.m, 'FnacZenRowsScraper', return_value=scraper), \
             patch.object(sys, 'argv', ['fnac_v3.py', '--no-save-html']), \
             patch.dict(sys.modules, {'auto_recovery': module}):
            self.m.main()
        self.assertEqual(events, ['recovery', 'save'])


if __name__ == '__main__':
    unittest.main()

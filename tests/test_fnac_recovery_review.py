"""Regression tests for exceptional collection, final errors and evidence time."""
import ast
from datetime import datetime, timezone
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import Mock, patch

import test_fnac_evidence_recovery as evidence
from test_fnac_finalization import ROW, URL, PRODUCT, OOS
from test_fnac_monitoring_registration import load_uploader


class RecoveryReviewTests(unittest.TestCase):
    def setUp(self):
        self.case = evidence.EvidenceRecoveryTests()
        self.case.setUp()
        self.s, self.m = self.case.s, self.case.m

    def failed_collection(self):
        self.s.fetch_html = Mock(return_value=(200, PRODUCT, None, 0.1))
        with patch.object(self.s, 'parse_product', side_effect=ValueError('synthetic')):
            return self.s.collect_one(dict(ROW))

    def test_exception_is_eligible_and_recovers_with_browser(self):
        result = self.failed_collection()
        self.assertEqual(result['_s3_upload'], 'fail')
        self.assertEqual(result['_screenshot_reason'], 'collection_exception')
        self.assertEqual(result['_evidence_recovery_row']['url'], URL)
        fake, pw, page, *_ = self.case.case.screenshot_context('ready')
        page.content.return_value = PRODUCT
        with patch.dict(sys.modules, {'playwright.sync_api': fake}):
            result = self.case.run_recovery([result])[0]
        self.assertEqual(result['retailprice'], 123.45)
        self.assertEqual(result['_collection_status'], 'complete')
        self.assertEqual(self.s.error_logs, [])
        self.assertEqual(pw.chromium.connect_over_cdp.call_count, 1)

    def test_disabled_capture_stays_out_of_recovery_even_after_exception(self):
        self.s.capture_null = False
        result = self.failed_collection()
        self.s.recover_null_evidence = Mock()
        self.case.run_recovery([result])
        self.assertEqual(result['_s3_upload'], 'skip')
        self.s.recover_null_evidence.assert_not_called()

    def test_parallel_worker_exception_preserves_retry_index_and_error(self):
        self.s.collect_one = Mock(side_effect=RuntimeError('synthetic'))
        result = self.s.collect([ROW], workers=2, sleep_seconds=0)[0]
        self.assertEqual(result['_s3_upload'], 'fail')
        self.assertEqual(result['_evidence_recovery_row']['_crawl_index'], 1)
        self.assertEqual(result['_collection_errors'], self.s.error_logs)
        self.s.recover_null_evidence = Mock(return_value=dict(result, _s3_upload='ok'))
        self.case.run_recovery([result])
        self.s.recover_null_evidence.assert_called_once()

    def test_recovered_policy_null_clears_only_its_errors(self):
        result = self.failed_collection()
        self.s.error_logs.append('another product failure')
        fake, _, page, *_ = self.case.case.screenshot_context('ready')
        page.content.return_value = OOS
        with patch.dict(sys.modules, {'playwright.sync_api': fake}):
            result = self.case.run_recovery([result])[0]
        self.assertEqual(result['_s3_upload'], 'ok')
        self.assertIsNone(result['retailprice'])
        self.assertEqual(self.s.error_logs, ['another product failure'])

    def test_persistent_failure_does_not_clear_initial_errors(self):
        result = self.failed_collection()
        errors = list(self.s.error_logs)
        fake, *_ = self.case.case.screenshot_context('restricted')
        with patch.dict(sys.modules, {'playwright.sync_api': fake}):
            result = self.case.run_recovery([result])[0]
        self.assertEqual(result['_s3_upload'], 'fail')
        self.assertEqual(self.s.error_logs, errors)

    def test_cached_capture_time_survives_upload_retry(self):
        fake, _, page, *_ = self.case.case.screenshot_context('ready')
        page.content.return_value = OOS
        self.m.capture_and_upload.return_value = None
        result = self.case.pending(OOS)
        with patch.dict(sys.modules, {'playwright.sync_api': fake}):
            self.s.capture_null_screenshot(result, URL)
        original_time = self.m.capture_and_upload.call_args.kwargs['captured_at']
        self.assertIsInstance(original_time, datetime)
        self.assertEqual(result['_pending_screenshot_at'], original_time)
        self.m.capture_and_upload.return_value = 'registered'
        self.s.capture_null_screenshot = Mock(side_effect=AssertionError('No browser'))
        result = self.case.run_recovery([result])[0]
        self.assertEqual(self.m.capture_and_upload.call_args.kwargs['captured_at'], original_time)
        self.assertNotIn('_pending_screenshot_at', result)
        self.s.capture_null_screenshot.assert_not_called()

    def test_known_null_without_photo_keeps_failure_record(self):
        result = self.case.pending(OOS)
        error = f'{URL}: NULL screenshot upload failed'
        result['_collection_errors'] = [error]
        self.s.error_logs = [error]
        fake, *_ = self.case.case.screenshot_context('restricted')
        with patch.dict(sys.modules, {'playwright.sync_api': fake}):
            final = self.case.run_recovery([result])[0]
        self.assertEqual(final['_collection_status'], 'policy_null')
        self.assertEqual(self.s.error_logs, [error])

    def test_photo_success_does_not_hide_unresolved_product_data_failure(self):
        result = self.failed_collection()
        errors = list(self.s.error_logs)
        self.s.capture_null_screenshot = Mock(return_value='ok')
        final = self.case.run_recovery([result])[0]
        self.assertEqual(final['_collection_status'], 'collection_failed')
        self.assertEqual(self.s.error_logs, errors)

    def test_upload_passes_capture_time_without_changing_legacy_calls(self):
        uploader = load_uploader()
        uploader._delete_existing_screenshots = Mock()
        instant = datetime(2026, 9, 19, 8, 1, tzinfo=timezone.utc)
        key = uploader.capture_and_upload(None, 'fnac', 'test', URL, {},
            require_monitoring_link=True, screenshot_bytes=b'PNG', captured_at=instant)
        self.assertTrue(key)
        uploader._add_watermark.assert_called_once_with(b'PNG', URL, captured_at=instant)

    def test_watermark_displays_capture_time_instead_of_registration_time(self):
        path = Path(__file__).resolve().parents[1] / 'null_screenshot.py'
        tree = ast.parse(path.read_text(encoding='utf-8-sig'))
        node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == '_add_watermark')
        module = types.ModuleType('watermark_subject')
        from datetime import timedelta
        import io
        module.__dict__.update(datetime=datetime, KST=timezone(timedelta(hours=9)), io=io, logger=Mock())
        exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), 'exec'), module.__dict__)
        canvas = Mock(width=1920, height=1080)
        draw = Mock()
        draw.textbbox.return_value = (0, 0, 100, 20)
        pil = types.ModuleType('PIL')
        pil.Image = Mock()
        pil.Image.open.return_value.convert.return_value = canvas
        pil.ImageDraw = Mock()
        pil.ImageDraw.Draw.return_value = draw
        pil.ImageFont = Mock()
        instant = datetime(2026, 9, 19, 0, 1, 2, tzinfo=timezone.utc)
        with patch.dict(sys.modules, {'PIL': pil}):
            module._add_watermark(b'PNG', URL, captured_at=instant)
        self.assertEqual(draw.text.call_args_list[1].args[1], '2026-09-19 09:01:02 KST')


if __name__ == '__main__':
    unittest.main()

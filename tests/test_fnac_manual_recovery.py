"""Local human-assisted FNAC recovery contracts, without production imports."""
import ast
from datetime import datetime, timezone
import logging
import os
from pathlib import Path
import re
import sys
import socket
import subprocess
import types
import unittest
from unittest.mock import Mock, patch
from urllib.parse import urlsplit

import pandas as pd
import test_fnac_auto_recovery as fixtures
from test_fnac_finalization import OOS, STORE_ONLY, ROW, URL


ROOT = Path(__file__).resolve().parents[1]


def load_manual():
    base = fixtures.load_crawler()
    path = ROOT / 'fnac_manual_recovery.py'
    tree = ast.parse(path.read_text(encoding='utf-8-sig'))
    nodes = [n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.ClassDef))]
    module = types.ModuleType('fnac_manual_offline')
    module.__dict__.update(
        FnacZenRowsScraper=base.FnacZenRowsScraper,
        normalize_product_url=base.normalize_product_url,
        is_confirmed_null_reason=base.is_confirmed_null_reason,
        time=fixtures.time, os=os, socket=socket, subprocess=subprocess, Path=Path,
        re=re, urlsplit=urlsplit, datetime=datetime,
        logger=logging.getLogger('fnac.manual.offline'),
        capture_and_upload=Mock(return_value='registered'),
        delete_screenshots_for_sku=Mock(),
        sync_saved_fnac_results=Mock(return_value={'success': True}),
    )
    exec(compile(ast.fix_missing_locations(ast.Module(body=nodes, type_ignores=[])), str(path), 'exec'), module.__dict__)
    return module, base


def load_recovery():
    path = ROOT / 'recovery.py'
    tree = ast.parse(path.read_text(encoding='utf-8-sig'))
    constants = {'TARGET_CONFIG', 'CRAWL_COLUMN_ORDER', 'FULL_NULL_FIELD_TARGETS'}
    nodes = [n for n in tree.body if isinstance(n, (ast.ClassDef, ast.FunctionDef))
             or isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id in constants for t in n.targets)]
    module = types.ModuleType('recovery_manual_offline')
    module.__dict__.update(
        pd=pd, datetime=datetime, pytz=types.SimpleNamespace(timezone=lambda _: timezone.utc),
        logger=Mock(), monitor_and_alert=Mock(), text=lambda value: value,
    )
    exec(compile(ast.fix_missing_locations(ast.Module(body=nodes, type_ignores=[])), str(path), 'exec'), module.__dict__)
    return module


class ManualRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.m, self.base = load_manual()
        self.s = self.m.FnacManualRecoveryScraper()
        self.p = fixtures.Page(ready=True)
        self.p.url = URL
        self.p.goto = Mock()
        self.p.reload = Mock()
        self.p.is_closed = Mock(return_value=False)
        self.p.bring_to_front = Mock()
        self.session = Mock()
        self.session.send.return_value = {'windowId': 1, 'bounds': {'windowState': 'maximized'}}
        self.p.context = Mock()
        self.p.context.new_cdp_session.return_value = self.session
        self.s.page = self.p
        self.clock = 0
        self.m.time = types.SimpleNamespace(monotonic=lambda: self.clock)
        self.p.wait_for_timeout.side_effect = self.advance
        self.s.fetch_html = Mock(side_effect=AssertionError('No API requests'))
        self.s.fetch_browser_html = Mock(side_effect=AssertionError('No ZenRows browser'))
        self.s.scraping_browser_wss = Mock(side_effect=AssertionError('No ZenRows connection'))

    def advance(self, milliseconds):
        self.clock += milliseconds / 1000

    def extract(self, body=fixtures.PRODUCT, row=ROW):
        self.p.content.return_value = body
        return self.s.extract_product_info(URL, row)

    def test_setup_attaches_existing_chrome_without_isolated_context(self):
        pw = Mock()
        context = Mock()
        browser = pw.chromium.connect_over_cdp.return_value
        browser.contexts = [context]
        self.s._chrome_listening = Mock(return_value=True)
        self.s._launch_recovery_chrome = Mock()
        factory = Mock(return_value=types.SimpleNamespace(start=Mock(return_value=pw)))
        fake = types.ModuleType('playwright.sync_api')
        fake.sync_playwright = factory
        with patch.dict(sys.modules, {'playwright.sync_api': fake}):
            self.assertTrue(self.s.setup_browser())
        pw.chromium.connect_over_cdp.assert_called_once_with('http://127.0.0.1:9222', timeout=15000)
        pw.chromium.launch.assert_not_called()
        browser.new_context.assert_not_called()
        context.new_page.assert_not_called()
        self.s._launch_recovery_chrome.assert_not_called()
        self.assertIs(self.s.context, context)
        self.assertIsNone(self.s.db_engine)
        self.s.close()
        self.assertIsNone(self.s.page)
        pw.stop.assert_called_once()
        browser.close.assert_not_called()
        context.close.assert_not_called()

    def test_failed_browser_setup_stops_playwright(self):
        pw = Mock()
        self.s._chrome_listening = Mock(return_value=True)
        pw.chromium.connect_over_cdp.side_effect = RuntimeError('synthetic setup failure')
        fake = types.SimpleNamespace(sync_playwright=lambda: types.SimpleNamespace(start=lambda: pw))
        with patch.dict(sys.modules, {'playwright.sync_api': fake}):
            with self.assertRaises(RuntimeError):
                self.s.setup_browser()
        pw.stop.assert_called_once()
        pw.chromium.launch.assert_not_called()

    def test_normal_price_has_image_url_and_needs_no_capture_or_api(self):
        result = self.extract()
        self.assertEqual(result['retailprice'], 123.45)
        self.assertEqual(result['imageurl'], 'https://static.fnac-static.com/example.jpg')
        self.assertTrue(result['_manual_recovery_ok'])
        self.p.screenshot.assert_not_called()
        self.m.capture_and_upload.assert_not_called()
        for forbidden in (self.s.fetch_html, self.s.fetch_browser_html, self.s.scraping_browser_wss):
            forbidden.assert_not_called()

    def test_normal_price_with_missing_image_still_does_not_register_null_proof(self):
        result = self.extract(re.sub(r'<img[^>]*>', '', fixtures.PRODUCT))
        self.assertEqual(result['retailprice'], 123.45)
        self.assertIsNone(result['imageurl'])
        self.p.screenshot.assert_not_called()
        self.s.finalize_saved_result(result)
        self.m.capture_and_upload.assert_not_called()

    def test_out_of_stock_visible_price_is_still_null_with_same_page_png(self):
        result = self.extract(OOS)
        self.assertIsNone(result['retailprice'])
        self.assertEqual(result['_crawl_reason'], 'ONLINE_STOCK_EXHAUSTED')
        self.assertEqual(result['_manual_screenshot_bytes'], b'synthetic PNG')
        self.p.screenshot.assert_called_once()
        self.m.capture_and_upload.assert_not_called()
        self.m.sync_saved_fnac_results.assert_not_called()

    def test_store_only_keeps_v3_priority(self):
        result = self.extract(STORE_ONLY)
        self.assertIsNone(result['retailprice'])
        self.assertEqual(result['_crawl_reason'], 'BASE_CLICK_COLLECT_FIRST_MARKETPLACE')

    def test_botcheck_waits_for_user_and_never_refreshes(self):
        self.p.state['isRestricted'] = True
        def solve(_):
            self.p.state['isRestricted'] = False
            return ''
        with patch('builtins.input', side_effect=solve) as user:
            result = self.extract()
        user.assert_called_once()
        self.assertEqual(result['retailprice'], 123.45)
        self.p.goto.assert_not_called()
        self.p.reload.assert_not_called()

    def test_enter_does_not_accept_still_blocked_page(self):
        self.p.state['isRestricted'] = True
        with patch('builtins.input', side_effect=['', 's']) as user:
            result = self.extract()
        self.assertIsNone(result)
        self.assertEqual(user.call_count, 2)
        self.p.goto.assert_not_called()
        self.p.screenshot.assert_not_called()

    def test_navigation_timeout_can_be_completed_on_same_tab(self):
        self.p.url = 'about:blank'
        def timeout(*args, **kwargs):
            self.p.url = URL
            raise TimeoutError('synthetic')
        self.p.goto.side_effect = timeout
        self.p.state['isRestricted'] = True
        def solve(_):
            self.p.state['isRestricted'] = False
            return ''
        with patch('builtins.input', side_effect=solve):
            result = self.extract()
        self.assertEqual(result['retailprice'], 123.45)
        self.p.goto.assert_called_once()

    def test_wrong_product_or_foreign_host_cannot_overwrite_original(self):
        for actual in ('https://www.fnac.com/other/a999/w-4', 'https://fakefnac.com/a123/w-4'):
            with self.subTest(actual=actual):
                self.p.url = actual
                with patch('builtins.input', return_value='s'):
                    self.assertIsNone(self.extract())
        self.p.screenshot.assert_not_called()

    def test_wrong_canonical_product_is_rejected(self):
        body = '<link rel="canonical" href="https://www.fnac.com/other/a999/w-4">' + fixtures.PRODUCT
        with patch('builtins.input', return_value='s'):
            self.assertIsNone(self.extract(body))

    def test_noninteractive_terminal_skips_without_capture(self):
        self.p.state['isRestricted'] = True
        with patch('builtins.input', side_effect=EOFError):
            self.assertIsNone(self.extract())
        self.p.screenshot.assert_not_called()

    def test_condition_changes_during_capture_does_not_upload_or_accept(self):
        self.p.screenshot.side_effect = lambda **kwargs: (setattr(self.p.content, 'return_value', fixtures.PRODUCT) or b'png')
        with patch('builtins.input', return_value='s'):
            self.assertIsNone(self.extract(OOS))
        self.m.capture_and_upload.assert_not_called()

    def test_capture_exception_preserves_existing_data(self):
        self.p.screenshot.side_effect = TimeoutError('synthetic capture')
        with patch('builtins.input', return_value='s'):
            self.assertIsNone(self.extract(OOS))
        self.m.capture_and_upload.assert_not_called()

    def test_verified_product_image_fallback_is_preserved(self):
        body = re.sub(r'<img[^>]*>', '', OOS)
        result = self.extract(body, dict(ROW, imageurl='https://example.test/verified-image.jpg'))
        self.assertEqual(result['imageurl'], 'https://example.test/verified-image.jpg')

    def test_registration_retry_uses_exact_png_without_browser_work(self):
        result = self.extract(OOS)
        captured_at = result['_manual_screenshot_at']
        self.m.capture_and_upload.side_effect = [None, 'linked']
        self.s.finalize_saved_result(result)
        self.assertEqual(result['_s3_upload'], 'ok')
        self.assertEqual(self.m.capture_and_upload.call_count, 2)
        for call in self.m.capture_and_upload.call_args_list:
            self.assertIsNone(call.args[0])
            self.assertEqual(call.kwargs['screenshot_bytes'], b'synthetic PNG')
            self.assertEqual(call.kwargs['captured_at'], captured_at)
            self.assertTrue(call.kwargs['require_monitoring_link'])
        self.p.screenshot.assert_called_once()
        self.p.goto.assert_not_called()
        self.m.delete_screenshots_for_sku.assert_not_called()
        self.m.sync_saved_fnac_results.assert_called_once()

    def test_registration_failure_keeps_old_photo_and_syncs_saved_null(self):
        result = self.extract(OOS)
        self.m.capture_and_upload.return_value = None
        self.s.finalize_saved_result(result)
        self.assertEqual(result['_s3_upload'], 'fail')
        self.m.delete_screenshots_for_sku.assert_not_called()
        self.m.sync_saved_fnac_results.assert_called_once_with([result])

    def test_normal_saved_result_skips_upload_and_cleans_old_null_proof(self):
        result = self.extract()
        self.s.finalize_saved_result(result, '2026-10-07 08:00:00')
        self.m.capture_and_upload.assert_not_called()
        self.assertEqual(result['_s3_upload'], 'skip')
        self.assertTrue(self.m.delete_screenshots_for_sku.called)
        for call in self.m.delete_screenshots_for_sku.call_args_list:
            self.assertEqual(call.kwargs, {'preserve_anomaly': True})
        self.m.sync_saved_fnac_results.assert_called_once()

    def test_normal_and_confirmed_null_do_not_add_decision_wait(self):
        for body in (fixtures.PRODUCT, OOS, STORE_ONLY):
            with self.subTest(body=body):
                self.p.wait_for_timeout.reset_mock()
                self.assertIsNotNone(self.extract(body))
                self.p.wait_for_timeout.assert_not_called()

    def test_slow_price_loading_is_waited_for_on_same_tab(self):
        pending = re.sub(r'<span class="f-faPriceBox__price">.*?</span>', '', fixtures.PRODUCT)
        def finish(milliseconds):
            self.advance(milliseconds)
            self.p.content.return_value = fixtures.PRODUCT
        self.p.wait_for_timeout.side_effect = finish
        with patch('builtins.input', side_effect=AssertionError('Normal page must not prompt')):
            result = self.extract(pending)
        self.assertEqual(result['retailprice'], 123.45)
        self.assertEqual(self.clock, 1)
        self.p.goto.assert_not_called()
        self.p.reload.assert_not_called()
        self.p.screenshot.assert_not_called()

    def test_slow_null_condition_keeps_policy_priority(self):
        pending = '<h1 class="f-productHeader__heading">Samsung SSD</h1>'
        def finish(milliseconds):
            self.advance(milliseconds)
            self.p.content.return_value = OOS
        self.p.wait_for_timeout.side_effect = finish
        result = self.extract(pending)
        self.assertIsNone(result['retailprice'])
        self.assertEqual(result['_crawl_reason'], 'ONLINE_STOCK_EXHAUSTED')
        self.p.screenshot.assert_called_once()

    def test_title_image_only_cannot_automatically_become_null(self):
        pending = '<h1 class="f-productHeader__heading">Samsung SSD</h1><img src="https://example.test/img">'
        with patch('builtins.input', return_value='s'):
            self.assertIsNone(self.extract(pending))
        self.assertEqual(self.clock, 10)
        self.p.screenshot.assert_not_called()
        self.m.capture_and_upload.assert_not_called()

    def test_enter_without_ready_decision_does_not_confirm_null(self):
        pending = '<h1 class="f-productHeader__heading">Samsung SSD</h1>'
        with patch('builtins.input', side_effect=['', 's']):
            self.assertIsNone(self.extract(pending))
        self.p.screenshot.assert_not_called()
        self.p.goto.assert_not_called()

    def test_human_can_confirm_fully_loaded_product_has_no_price(self):
        pending = '<h1 class="f-productHeader__heading">Samsung SSD</h1>'
        with patch('builtins.input', return_value='n') as user:
            result = self.extract(pending)
        user.assert_called_once()
        self.assertIsNone(result['retailprice'])
        self.assertEqual(result['_crawl_reason'], 'PRICE_NOT_FOUND')
        self.assertTrue(result['_manual_recovery_ok'])
        self.p.screenshot.assert_called_once()
        self.p.goto.assert_not_called()

    def test_no_price_confirmation_is_invalid_when_decision_changes(self):
        self.s.decision_timeout = 0
        pending = '<h1 class="f-productHeader__heading">Samsung SSD</h1>'
        def confirm_then_change(_):
            self.p.content.return_value = pending.replace('Samsung SSD', 'Samsung SSD changed')
            return 'n'
        choices = iter(('n', 's'))
        def choose(prompt):
            choice = next(choices)
            if choice == 'n':
                confirm_then_change(prompt)
            return choice
        with patch('builtins.input', side_effect=choose):
            self.assertIsNone(self.extract(pending))
        self.p.screenshot.assert_not_called()

    def test_no_price_confirmation_cannot_bypass_botcheck(self):
        self.p.state['isRestricted'] = True
        with patch('builtins.input', side_effect=['n', 's']):
            self.assertIsNone(self.extract())
        self.p.screenshot.assert_not_called()

    def test_new_price_after_human_confirmation_is_saved_as_normal(self):
        self.s.decision_timeout = 0
        pending = '<h1 class="f-productHeader__heading">Samsung SSD</h1>'
        def choose(_):
            self.p.content.return_value = fixtures.PRODUCT
            return 'n'
        with patch('builtins.input', side_effect=choose):
            result = self.extract(pending)
        self.assertEqual(result['retailprice'], 123.45)
        self.p.screenshot.assert_not_called()

    def test_missing_price_confirmation_does_not_carry_to_next_product(self):
        self.s.decision_timeout = 0
        pending = '<h1 class="f-productHeader__heading">Samsung SSD</h1>'
        with patch('builtins.input', return_value='n'):
            self.assertIsNotNone(self.extract(pending))
        self.p.screenshot.reset_mock()
        next_url = URL.replace('/a123/', '/a124/')
        self.p.url = next_url
        with patch('builtins.input', return_value='s') as user:
            self.assertIsNone(self.s.extract_product_info(next_url, dict(ROW, retailersku='next-synthetic')))
        user.assert_called_once()
        self.p.screenshot.assert_not_called()

    def test_hidden_price_waits_instead_of_immediate_null(self):
        self.p.state['visiblePriceTexts'] = []
        with patch('builtins.input', return_value='s'):
            self.assertIsNone(self.extract())
        self.assertEqual(self.clock, 10)
        self.p.screenshot.assert_not_called()


class RecoveryIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.m = load_recovery()
        self.manager = object.__new__(self.m.RecoveryManager)
        self.manager.korea_tz = timezone.utc
        self.scraper = Mock()
        self.row = dict(ROW, producturl=URL, kr_crawl_datetime='2026-10-07 08:00:00',
                        title='Existing title', imageurl='https://example.test/image.jpg', retailprice=None)
        for name, value in (
            ('get_null_records', pd.DataFrame([self.row])),
            ('get_missing_urls', pd.DataFrame()),
            ('get_session_all_records', pd.DataFrame([self.row])),
            ('load_scraper', self.scraper),
            ('update_db_record', True), ('insert_missing_record', True),
            ('generate_and_upload_file', True),
        ):
            setattr(self.manager, name, Mock(return_value=value))
        self.manager.db_engine = Mock()
        self.result = dict(self.row, title='Accepted product', _manual_recovery_ok=True,
                           _manual_screenshot_bytes=b'png', _manual_screenshot_at=datetime.now(timezone.utc))
        self.manager.recrawl_url = Mock(return_value=self.result)
        self.delete = Mock()
        self.null_module = types.SimpleNamespace(
            FULL_NULL_FIELDS=('title', 'imageurl', 'retailprice'),
            RETAILER_NAME_BY_TARGET_KEY={'fnac': 'fnac', 'gb': 'amazon_gb'},
            delete_screenshots_for_sku=self.delete,
            is_null_result=lambda result, fields=None: result.get('retailprice') is None,
        )

    def run_subject(self, target='fnac'):
        with (patch.dict(sys.modules, {'null_screenshot': self.null_module}),
              patch('builtins.input', return_value=''),
              patch.object(pd, 'read_sql', return_value=pd.DataFrame([{'cnt': 1}]))):
            return self.manager.run_recovery(target, '2026-10-07 08:00:00')

    def test_fnac_configuration_routes_only_fnac_to_new_adapter(self):
        self.assertEqual(self.m.TARGET_CONFIG['fnac']['scraper_module'], 'fnac_manual_recovery')
        # All other target routes remain the original production modules.
        expected = {'fr': 'fr_v2', 'gb': 'uk_v2', 'currys': 'currys_v2',
                    'it': 'it_v2', 'de': 'de_v2', 'bestbuy': 'bestbuy_v2',
                    'es': 'es_v2', 'mediamarkt': 'mediamarkt_v2', 'xkom': 'xkom_v2',
                    'usa': 'usa_v2', 'nl': 'nl_amazon', 'danawa': 'danawa_v2',
                    'in': 'in_v2', 'jp': 'jp_v2', 'au': 'centrecom', 'coolblue': 'coolblue_nl_v2'}
        for target, module in expected.items():
            self.assertEqual(self.m.TARGET_CONFIG[target]['scraper_module'], module)

    def test_failed_fnac_recrawl_never_deletes_old_photo_or_updates_db(self):
        self.manager.recrawl_url.return_value = None
        self.assertFalse(self.run_subject())
        self.delete.assert_not_called()
        self.manager.update_db_record.assert_not_called()
        self.scraper.finalize_saved_result.assert_not_called()
        self.scraper.close.assert_called_once()

    def test_fnac_result_without_page_acceptance_cannot_save(self):
        self.manager.recrawl_url.return_value = dict(self.row)
        self.assertFalse(self.run_subject())
        self.manager.update_db_record.assert_not_called()
        self.delete.assert_not_called()

    def test_raw_db_commit_precedes_proof_finalization_and_export_is_public(self):
        calls = []
        self.manager.update_db_record.side_effect = lambda *args: calls.append('db_saved') or True
        self.scraper.finalize_saved_result.side_effect = lambda *args: calls.append('proof_registered')
        self.assertTrue(self.run_subject())
        self.assertEqual(calls, ['db_saved', 'proof_registered'])
        self.delete.assert_not_called()
        exported = self.manager.generate_and_upload_file.call_args.args[1]
        self.assertFalse(any(str(c).startswith('_') for c in exported.columns))

    def test_raw_db_failure_does_not_register_or_delete_proof(self):
        self.manager.update_db_record.return_value = False
        self.assertFalse(self.run_subject())
        self.scraper.finalize_saved_result.assert_not_called()
        self.delete.assert_not_called()

    def test_missing_fnac_insert_precedes_finalize_and_private_fields_never_export(self):
        self.manager.get_null_records.return_value = pd.DataFrame()
        self.manager.get_missing_urls.return_value = pd.DataFrame([ROW])
        calls = []
        self.manager.insert_missing_record.side_effect = lambda *args: calls.append('insert') or True
        self.scraper.finalize_saved_result.side_effect = lambda *args: calls.append('finalize')
        self.assertTrue(self.run_subject())
        self.assertEqual(calls, ['insert', 'finalize'])
        exported = self.manager.generate_and_upload_file.call_args.args[1]
        self.assertFalse(any(str(c).startswith('_') for c in exported.columns))

    def test_failed_missing_insert_does_not_finalize(self):
        self.manager.get_null_records.return_value = pd.DataFrame()
        self.manager.get_missing_urls.return_value = pd.DataFrame([ROW])
        self.manager.insert_missing_record.return_value = False
        self.assertFalse(self.run_subject())
        self.scraper.finalize_saved_result.assert_not_called()

    def test_other_site_keeps_existing_recrawl_and_photo_cleanup(self):
        self.manager.recrawl_url.return_value = dict(self.row, retailprice=123.45)
        self.assertTrue(self.run_subject('gb'))
        self.assertTrue(self.delete.called)
        self.scraper.finalize_saved_result.assert_not_called()
        self.scraper.driver.quit.assert_called_once()

    def test_other_sites_keep_original_recrawl_arguments(self):
        for target in ('gb', 'in', 'es', 'xkom'):
            with self.subTest(target=target):
                scraper = Mock()
                self.m.RecoveryManager.recrawl_url(self.manager, scraper, URL, ROW, target)
                kwargs = scraper.extract_product_info.call_args.kwargs
                self.assertEqual(kwargs, {} if target == 'xkom' else {'retry_count': 0, 'max_retries': 1})

    def test_loader_routes_fnac_to_local_adapter_without_driver_setup(self):
        scraper = types.SimpleNamespace(setup_browser=Mock(return_value=True))
        module = types.ModuleType('fnac_manual_recovery')
        module.FnacManualRecoveryScraper = Mock(return_value=scraper)
        with patch.dict(sys.modules, {'fnac_manual_recovery': module}):
            actual = self.m.RecoveryManager.load_scraper(self.manager, 'fnac')
        self.assertIs(actual, scraper)
        scraper.setup_browser.assert_called_once()

    def test_non_fnac_loader_keeps_original_module_and_setup_driver(self):
        scraper = types.SimpleNamespace(setup_driver=Mock(return_value=True))
        module = types.ModuleType('uk_v2')
        module.AmazonUKScraper = Mock(return_value=scraper)
        with patch.dict(sys.modules, {'uk_v2': module}):
            actual = self.m.RecoveryManager.load_scraper(self.manager, 'gb')
        self.assertIs(actual, scraper)
        scraper.setup_driver.assert_called_once()

    def test_finalize_exception_keeps_saved_data_and_discards_private_fields(self):
        self.scraper.finalize_saved_result.side_effect = RuntimeError('synthetic')
        result = self.manager.finalize_fnac_recovery(self.scraper, self.result)
        self.assertEqual(result['title'], 'Accepted product')
        self.assertFalse(any(k.startswith('_') for k in result))


if __name__ == '__main__':
    unittest.main()

"""Dedicated Chrome attachment and actual-window NULL capture contracts."""
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import Mock, patch

import test_fnac_manual_recovery as manual
from test_fnac_finalization import OOS, STORE_ONLY, ROW, URL


class RecoveryChromeTests(unittest.TestCase):
    setUp = manual.ManualRecoveryTests.setUp
    advance = manual.ManualRecoveryTests.advance
    extract = manual.ManualRecoveryTests.extract

    def setup_subject(self, existing=True, responses=None):
        pw = Mock()
        self.context = Mock()
        self.context.pages = [self.p]
        browser = Mock()
        browser.contexts = [self.context]
        pw.chromium.connect_over_cdp.side_effect = responses or [browser]
        self.s._chrome_listening = Mock(return_value=existing)
        self.s._launch_recovery_chrome = Mock()
        self.m.time.sleep = lambda seconds: self.advance(seconds * 1000)
        fake = types.SimpleNamespace(sync_playwright=lambda: types.SimpleNamespace(start=lambda: pw))
        with patch.dict('sys.modules', {'playwright.sync_api': fake}):
            self.s.setup_browser()
        return pw, browser

    def test_absent_chrome_starts_once_then_connects(self):
        pw, browser = self.setup_subject(existing=False)
        self.s._launch_recovery_chrome.assert_called_once()
        pw.chromium.launch.assert_not_called()
        browser.new_context.assert_not_called()
        self.context.new_page.assert_not_called()

    def test_started_chrome_connection_wait_is_bounded(self):
        with self.assertRaises(RuntimeError):
            self.setup_subject(existing=False, responses=RuntimeError('synthetic'))
        self.assertLessEqual(self.clock, 15.25)
        self.s._launch_recovery_chrome.assert_called_once()
        self.assertIsNone(self.s.playwright)

    def test_occupied_port_never_starts_a_second_chrome(self):
        with self.assertRaises(RuntimeError):
            self.setup_subject(existing=True, responses=RuntimeError('synthetic'))
        self.s._launch_recovery_chrome.assert_not_called()
        self.assertEqual(self.clock, 0)

    def test_close_only_disconnects_and_is_repeatable(self):
        pw, browser = self.setup_subject()
        self.s.page = self.p
        self.s.close()
        self.s.close()
        pw.stop.assert_called_once()
        browser.close.assert_not_called()
        self.context.close.assert_not_called()
        self.p.close.assert_not_called()

    def test_existing_matching_tab_keeps_current_challenge_and_url(self):
        self.s.context = Mock(pages=[self.p])
        self.s.page = None
        self.assertTrue(self.s._select_product_tab(URL))
        self.assertIs(self.s.page, self.p)
        self.p.goto.assert_not_called()
        self.p.reload.assert_not_called()
        self.s.context.new_page.assert_not_called()

    def test_other_offer_tab_is_not_treated_as_the_requested_offer(self):
        self.p.url = URL.split('?')[0] + '?oref=other-offer'
        self.s.context = Mock(pages=[self.p])
        self.assertFalse(self.s._select_product_tab(URL.split('?')[0]))
        self.assertIs(self.s.page, self.p)

    def test_external_tabs_are_not_reused_or_navigated(self):
        other = Mock(url='https://example.test/a123/w-4?oref=offer-example')
        other.is_closed.return_value = False
        self.s.context = Mock(pages=[other])
        self.s.context.new_page.return_value = self.p
        self.s.page = None
        self.assertFalse(self.s._select_product_tab(URL))
        self.s.context.new_page.assert_called_once()
        other.goto.assert_not_called()
        other.close.assert_not_called()

    def test_blank_startup_tab_is_reused(self):
        self.p.url = 'about:blank'
        self.s.context = Mock(pages=[self.p])
        self.s.page = None
        self.assertFalse(self.s._select_product_tab(URL))
        self.s.context.new_page.assert_not_called()

    def test_closed_recovery_tab_is_replaced_without_closing_others(self):
        self.p.is_closed.return_value = True
        fresh = Mock(url='about:blank')
        fresh.is_closed.return_value = False
        self.s.context = Mock(pages=[self.p, fresh])
        self.assertFalse(self.s._select_product_tab(URL))
        self.assertIs(self.s.page, fresh)
        self.p.close.assert_not_called()

    def test_unsafe_target_is_rejected_before_tab_mutation(self):
        for url in ('https://example.test/a123/w-4', 'http://www.fnac.com/a123/w-4'):
            with self.subTest(url=url), self.assertRaises(ValueError):
                self.s._select_product_tab(url)
        self.p.goto.assert_not_called()
        self.p.bring_to_front.assert_not_called()

    def window(self, initial, *, transition=True, on_maximize=None):
        self.window_state = initial
        def send(method, params=None):
            if method == 'Browser.getWindowForTarget':
                return {'windowId': 7, 'bounds': {'windowState': self.window_state}}
            if method == 'Browser.getWindowBounds':
                return {'bounds': {'windowState': self.window_state}}
            if method == 'Browser.setWindowBounds':
                if transition:
                    self.window_state = params['bounds']['windowState']
                if params['bounds']['windowState'] == 'maximized' and on_maximize:
                    on_maximize()
                return {}
            raise AssertionError(method)
        self.session.send.side_effect = send

    def test_maximized_window_is_kept_without_resize_or_delay(self):
        self.window('maximized')
        self.assertTrue(self.s._maximize_for_capture())
        self.assertEqual(self.session.send.call_count, 1)
        self.assertEqual(self.clock, 0)
        self.session.detach.assert_called_once()

    def test_normal_window_is_maximized_before_screenshot(self):
        self.window('normal')
        def screenshot(**kwargs):
            self.assertEqual(self.window_state, 'maximized')
            return b'synthetic PNG'
        self.p.screenshot.side_effect = screenshot
        result = self.extract(OOS)
        self.assertEqual(result['_crawl_reason'], 'ONLINE_STOCK_EXHAUSTED')
        self.assertEqual(self.clock, 0.2)
        self.session.detach.assert_called_once()

    def test_minimized_and_fullscreen_windows_restore_then_maximize(self):
        for state in ('minimized', 'fullscreen'):
            with self.subTest(state=state):
                self.session.reset_mock()
                self.window(state)
                self.assertTrue(self.s._maximize_for_capture())
                changes = [call.args[1]['bounds']['windowState'] for call in self.session.send.call_args_list
                           if call.args[0] == 'Browser.setWindowBounds']
                self.assertEqual(changes, ['normal', 'maximized'])

    def test_normal_price_does_not_resize_or_capture(self):
        result = self.extract()
        self.assertEqual(result['retailprice'], 123.45)
        self.p.context.new_cdp_session.assert_not_called()
        self.p.screenshot.assert_not_called()

    def test_price_recovers_during_resize_so_null_proof_is_not_captured(self):
        self.window('normal', on_maximize=lambda: setattr(self.p.content, 'return_value', manual.fixtures.PRODUCT))
        result = self.extract(OOS)
        self.assertEqual(result['retailprice'], 123.45)
        self.assertIsNone(result['_manual_screenshot_bytes'])
        self.p.screenshot.assert_not_called()

    def test_null_reason_after_resize_is_used_for_the_picture(self):
        self.window('normal', on_maximize=lambda: setattr(self.p.content, 'return_value', STORE_ONLY))
        result = self.extract(OOS)
        self.assertEqual(result['_crawl_reason'], 'BASE_CLICK_COLLECT_FIRST_MARKETPLACE')
        self.assertIsNone(result['retailprice'])
        self.p.screenshot.assert_called_once()

    def test_resize_cannot_complete_so_capture_is_deferred(self):
        self.window('normal', transition=False)
        with patch('builtins.input', return_value='s'):
            result = self.extract(OOS)
        self.assertIsNone(result)
        self.assertLessEqual(self.clock, 3.1)
        self.p.screenshot.assert_not_called()
        self.session.detach.assert_called_once()

    def test_cdp_resize_error_keeps_existing_data_and_detaches(self):
        self.session.send.side_effect = RuntimeError('synthetic')
        with patch('builtins.input', return_value='s'):
            self.assertIsNone(self.extract(OOS))
        self.session.detach.assert_called_once()
        self.p.screenshot.assert_not_called()

    def test_chrome_launch_uses_dedicated_directory_and_loopback_only(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            chrome = root / 'Google' / 'Chrome' / 'Application' / 'chrome.exe'
            chrome.parent.mkdir(parents=True)
            chrome.write_bytes(b'not executable')
            self.m.os = types.SimpleNamespace(name='nt', environ={'LOCALAPPDATA': str(root)})
            self.m.subprocess = types.SimpleNamespace(Popen=Mock(), DEVNULL=-3)
            self.s._launch_recovery_chrome()
            args = self.m.subprocess.Popen.call_args.args[0]
            self.assertEqual(args[0], str(chrome))
            self.assertIn('--remote-debugging-address=127.0.0.1', args)
            self.assertIn('--remote-debugging-port=9222', args)
            self.assertIn(f'--user-data-dir={root / "FnacRecoveryChrome"}', args)
            self.assertNotIn('--enable-automation', args)
            self.assertNotIn('--headless', args)
            self.assertFalse((root / 'FnacRecoveryChrome').exists())
            self.assertNotIn('shell', self.m.subprocess.Popen.call_args.kwargs)

    def test_invalid_port_is_not_used(self):
        for port in (0, 80, 65536, 'remote-host'):
            with self.subTest(port=port), self.assertRaises(ValueError):
                self.m.FnacManualRecoveryScraper(chrome_port=port)

    def test_missing_chrome_fails_without_launching_or_fallback(self):
        self.m.os = types.SimpleNamespace(name='nt', environ={})
        self.m.subprocess = types.SimpleNamespace(Popen=Mock(), DEVNULL=-3)
        with self.assertRaises(RuntimeError):
            self.s._launch_recovery_chrome()
        self.m.subprocess.Popen.assert_not_called()


if __name__ == '__main__':
    unittest.main()

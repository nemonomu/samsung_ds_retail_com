"""Offline FNAC contracts: no config import, DB, API, S3, or alert side effects."""
import argparse
import ast
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import html
import hashlib
import json
import logging
from pathlib import Path
import re
import sys
import threading
import time
import types
import unicodedata
import unittest
from unittest.mock import Mock, patch
from urllib.parse import parse_qsl, quote, urlsplit, urlunsplit

import requests


def load_crawler():
    # Keep the real method bodies, avoiding production-only module side effects.
    path = Path(__file__).resolve().parents[1] / 'fnac_v3.py'
    tree = ast.parse(path.read_text(encoding='utf-8-sig'))
    constants = {'ZENROWS_API_URL', 'FNAC_TABLE', 'FNAC_SCREENSHOT_VIEWPORT',
                 'DEFAULT_FETCH_TIMEOUT', 'DEFAULT_SCREENSHOT_MAX_ATTEMPTS', 'DEFAULT_PRODUCT_TIME_BUDGET'}
    nodes = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.ClassDef))
             or (isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id in constants for t in node.targets))]
    code = ast.Module(body=[ast.ImportFrom(module='__future__', names=[ast.alias(name='annotations')], level=0)] + nodes, type_ignores=[])
    module = types.ModuleType('fnac_test_subject')
    module.__dict__.update(
        argparse=argparse, re=re, json=json, html_lib=html, hashlib=hashlib, unicodedata=unicodedata,
        quote=quote, parse_qsl=parse_qsl, urlsplit=urlsplit, urlunsplit=urlunsplit,
        requests=requests, threading=threading, time=time, sys=sys, Path=Path, datetime=datetime,
        ThreadPoolExecutor=ThreadPoolExecutor, as_completed=as_completed,
        pytz=types.SimpleNamespace(timezone=lambda _: timezone.utc),
        logger=logging.getLogger('fnac.offline_test'), ZENROWS_API_KEY='test-only-placeholder',
        ZENROWS_SCRAPING_BROWSER_COUNTRY='fr',
        is_null_result=lambda result: any(result.get(k) in (None, '') for k in ('title', 'imageurl', 'retailprice')),
        capture_and_upload=Mock(return_value='uploaded'),
    )
    exec(compile(ast.fix_missing_locations(code), str(path), 'exec'), module.__dict__)
    return module


URL = 'https://www.fnac.com/example/a123/w-4?oref=offer-example'
PRODUCT = '''<h1 class="f-productHeader__heading">Samsung SSD</h1>
<img class="f-productMedias__viewItem--main" src="https://static.fnac-static.com/example.jpg">
<div data-automation-id="product-availability">En stock</div>
<span class="f-faPriceBox__price">123,45</span>'''


class Frame:
    def __init__(self, signals, visible=True, url='https://geo.captcha-delivery.com/example'):
        self.signals, self.visible, self.url = signals, visible, url

    def frame_element(self):
        return types.SimpleNamespace(is_visible=lambda: self.visible)

    def evaluate(self, script):
        return self.signals


class Page:
    def __init__(self, *, ready=False, frames=(), maintenance=False):
        self.main_frame = object()
        self.frames = [self.main_frame, *frames]
        self.state = {'productContentReady': ready, 'productReady': ready, 'cookieVisible': False,
                      'isRestricted': False, 'isMaintenance': maintenance, 'textLength': 200 if ready else 0,
                      'visiblePriceTexts': ['123,45']}
        self.wait_for_timeout = Mock()
        self.close = Mock()
        self.content = Mock(return_value=PRODUCT)

    def evaluate(self, _script):
        return self.state


class FnacRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.m = load_crawler()
        self.m.FnacZenRowsScraper.setup_db_connection = Mock()
        self.scraper = self.m.FnacZenRowsScraper()

    def response(self, status=200, text=PRODUCT, code='RESP001'):
        return types.SimpleNamespace(status_code=status, text=text, headers={}, json=lambda: {'code': code})

    def test_auto_request_has_no_conflicting_manual_flags(self):
        session = Mock()
        session.get.return_value = self.response()
        with patch.object(self.m.requests, 'Session', return_value=session):
            status, body, _, _ = self.scraper.fetch_html(URL)
        self.assertEqual(status, 200)
        self.assertEqual(body, PRODUCT)
        self.assertEqual(session.get.call_args.kwargs['params'],
                         {'apikey': 'test-only-placeholder', 'url': URL, 'mode': 'auto'})
        self.assertEqual(session.get.call_args.kwargs['timeout'], 180)
        session.close.assert_called_once()

    def test_manual_mode_keeps_legacy_wait_and_explicit_override(self):
        for wait, expected in ((None, '150'), (5000, '5000')):
            scraper = self.m.FnacZenRowsScraper(fetch_mode='manual', fetch_wait=wait)
            session = Mock()
            session.get.return_value = self.response()
            with patch.object(self.m.requests, 'Session', return_value=session):
                scraper.fetch_html(URL)
            params = session.get.call_args.kwargs['params']
            self.assertNotIn('mode', params)
            self.assertEqual(params['js_render'], 'true')
            self.assertEqual(params['wait'], expected)

    def test_transient_failure_retries_then_returns_product(self):
        sessions = [Mock(), Mock()]
        sessions[0].get.return_value = self.response(422, text='service failure')
        sessions[1].get.return_value = self.response()
        with patch.object(self.m.requests, 'Session', side_effect=sessions), patch.object(self.m.time, 'sleep'):
            self.assertEqual(self.scraper.fetch_html(URL)[0], 200)
        self.assertEqual(self.scraper.total_zenrows_calls, 2)
        for session in sessions:
            session.close.assert_called_once()

    def test_rendered_verification_requests_browser_execution_without_manual_conflicts(self):
        session = Mock()
        session.get.return_value = self.response()
        with patch.object(self.m.requests, 'Session', return_value=session):
            self.scraper.fetch_html(URL, rendered_verification=True)
        params = session.get.call_args.kwargs['params']
        self.assertEqual(params['mode'], 'auto')
        self.assertNotIn('js_render', params)
        self.assertNotIn('premium_proxy', params)
        instructions = json.loads(params['js_instructions'])
        self.assertEqual(instructions[0], {'wait': 6000})
        self.assertIn('data-fnac-rendered-verification', instructions[1]['evaluate'])

    def test_auto_verification_requires_executed_page_marker(self):
        rendered = '<html data-fnac-rendered-verification="complete">' + PRODUCT + '</html>'
        self.scraper.fetch_html = Mock(return_value=(200, rendered, None, 1.0))
        self.scraper.fetch_browser_html = Mock()
        self.assertEqual(self.scraper.fetch_verified_html(URL), (200, rendered, 1.0))
        self.scraper.fetch_html.assert_called_once_with(URL, max_attempts=1, rendered_verification=True, reserve_seconds=90)
        self.scraper.fetch_browser_html.assert_not_called()

    def test_unrendered_auto_response_uses_browser_fallback(self):
        self.scraper.fetch_html = Mock(return_value=(200, PRODUCT, None, 1.0))
        self.scraper.fetch_browser_html = Mock(return_value=(403, '', 2.0))
        self.assertEqual(self.scraper.fetch_verified_html(URL), (403, '', 3.0))

    def test_manual_mode_keeps_original_browser_verification_route(self):
        self.scraper.fetch_mode = 'manual'
        self.scraper.fetch_html = Mock()
        self.scraper.fetch_browser_html = Mock(return_value=(200, PRODUCT, 2.0))
        self.assertEqual(self.scraper.fetch_verified_html(URL), (200, PRODUCT, 2.0))
        self.scraper.fetch_html.assert_not_called()

    def test_ambiguous_price_uses_verified_dom_including_stock_and_store_only_rules(self):
        base_url = URL.split('?')[0]
        ambiguous_data = {'product': {'attributes': {'offer': [{'sellerType': 'fnac'}],
                          'currentOffer': {'sellerType': 'professional'}}}}
        initial = PRODUCT + '<script id="digitalData">' + json.dumps(ambiguous_data) + '</script>'
        store_data = {'product': {'attributes': {'offer': [{'sellerType': 'store', 'seller': 'clickandcollectonly'}],
                      'availabilityType': 'discontinued', 'currentOffer': {'sellerType': 'professional'}}}}
        cases = [
            (PRODUCT, 123.45, 'BROWSER_VISIBLE_PRICE_BOX'),
            (PRODUCT.replace('En stock', 'Stock en ligne épuisé'), None, 'BROWSER_ONLINE_STOCK_EXHAUSTED'),
            (PRODUCT + '<script id="digitalData">' + json.dumps(store_data) + '</script>', None, 'BROWSER_BASE_CLICK_COLLECT_FIRST_MARKETPLACE'),
        ]
        self.scraper.capture_null = False
        self.scraper.fetch_browser_html = Mock()
        for verified, expected_price, expected_reason in cases:
            with self.subTest(reason=expected_reason):
                self.scraper.fetch_html = Mock(side_effect=[(200, initial, None, 1),
                    (200, '<html data-fnac-rendered-verification="complete">' + verified + '</html>', None, 1)])
                result = self.scraper.collect_one({'url': base_url})
                self.assertEqual(result['retailprice'], expected_price)
                self.assertEqual(result['_crawl_reason'], expected_reason)
        self.scraper.fetch_browser_html.assert_not_called()

    def test_failed_verification_still_clears_ambiguous_price(self):
        self.scraper.capture_null = False
        self.scraper.fetch_html = Mock(return_value=(200, PRODUCT, None, 1))
        self.scraper.should_browser_verify = Mock(return_value=True)
        self.scraper.fetch_verified_html = Mock(return_value=(403, '', 1))
        result = self.scraper.collect_one({'url': URL})
        self.assertIsNone(result['retailprice'])
        self.assertEqual(result['_crawl_reason'], 'BROWSER_VERIFY_FAILED_VISIBLE_PRICE_BOX')

    def test_auth_error_is_not_retried_and_body_is_not_logged(self):
        session = Mock()
        session.get.return_value = self.response(401, text='DO_NOT_LOG_BODY', code='AUTH003')
        with patch.object(self.m.requests, 'Session', return_value=session), self.assertLogs('fnac.offline_test') as logs:
            self.assertEqual(self.scraper.fetch_html(URL)[0], 401)
        session.get.assert_called_once()
        self.assertIn('AUTH003', ' '.join(logs.output))
        self.assertNotIn('DO_NOT_LOG_BODY', ' '.join(logs.output))

    def test_exception_does_not_log_credential_bearing_message(self):
        session = Mock()
        session.get.side_effect = requests.ReadTimeout('DO_NOT_LOG_URL_OR_VALUE')
        with patch.object(self.m.requests, 'Session', return_value=session), self.assertLogs('fnac.offline_test') as logs:
            result = self.scraper.fetch_html(URL, max_attempts=1)
        self.assertEqual(result[1], 'ReadTimeout')
        self.assertNotIn('DO_NOT_LOG_URL_OR_VALUE', ' '.join(logs.output))
        session.close.assert_called_once()

    def test_visible_block_frame_explains_empty_parent(self):
        page = Page(frames=[Frame({'restricted': True})])
        self.assertEqual(self.scraper.wait_for_screenshot_ready(page, 'example', URL), 'restricted')
        page.wait_for_timeout.assert_not_called()

    def test_normal_product_with_hidden_challenge_is_ready(self):
        page = Page(ready=True, frames=[Frame({'restricted': True}, visible=False)])
        self.assertEqual(self.scraper.wait_for_screenshot_ready(page, 'example', URL), 'ready')

    def test_unrelated_ad_error_does_not_reject_product(self):
        page = Page(ready=True, frames=[Frame({'restricted': True}, url='https://ads.example.test/frame')])
        self.assertEqual(self.scraper.wait_for_screenshot_ready(page, 'example', URL), 'ready')

    def test_maintenance_is_not_reloaded_as_empty_page(self):
        self.assertEqual(self.scraper.wait_for_screenshot_ready(Page(maintenance=True), 'example', URL), 'unavailable')

    def test_blank_page_without_block_stops_readiness_wait(self):
        with patch.object(self.m.time, 'monotonic', side_effect=[0, 0, 21]):
            self.assertEqual(self.scraper.wait_for_screenshot_ready(Page(), 'example', URL), 'not_ready')

    def screenshot_context(self, ready_state='restricted', parse_result=None):
        page = Page()
        context = Mock()
        context.new_page.return_value = page
        browser = Mock()
        browser.new_context.return_value = context
        playwright = Mock()
        playwright.chromium.connect_over_cdp.return_value = browser
        manager = Mock()
        manager.start = Mock(return_value=playwright)
        manager.__enter__ = Mock(return_value=playwright)
        manager.__exit__ = Mock(return_value=False)
        fake = types.ModuleType('playwright.sync_api')
        fake.sync_playwright = Mock(return_value=manager)
        self.scraper.prepare_playwright_page_for_capture = Mock()
        self.scraper.warmup_fnac_screenshot_session = Mock()
        self.scraper.load_screenshot_page = Mock(return_value=types.SimpleNamespace(status=200))
        self.scraper.wait_for_screenshot_ready = Mock(return_value=ready_state)
        if parse_result is not None:
            self.scraper.parse_product = Mock(return_value=parse_result)
        return fake, playwright, page, context, browser

    def test_blocked_capture_finishes_after_two_sessions_and_no_extra_sleep(self):
        fake, playwright, page, context, browser = self.screenshot_context()
        with patch.dict(sys.modules, {'playwright.sync_api': fake}), patch.object(self.m.time, 'sleep') as sleep:
            self.assertEqual(self.scraper.capture_null_screenshot({'retailersku': 'example'}, URL), 'fail')
        self.assertEqual(playwright.chromium.connect_over_cdp.call_count, 2)
        self.assertEqual(self.scraper.load_screenshot_page.call_count, 2)  # no reload on a confirmed block
        self.assertEqual(page.close.call_count, 2)
        self.assertEqual(context.close.call_count, 2)
        self.assertEqual(browser.close.call_count, 2)
        sleep.assert_called_once()
        self.m.capture_and_upload.assert_not_called()

    def test_old_zero_attempt_option_is_finite(self):
        self.assertEqual(self.m.FnacZenRowsScraper(screenshot_max_attempts=0).screenshot_max_attempts, 2)

    def test_cli_defaults_match_constructor(self):
        class ArgumentsCaptured(Exception):
            pass

        captured = {}
        original = argparse.ArgumentParser.parse_args

        def stop_after_arguments(parser):
            captured.update(vars(original(parser, [])))
            raise ArgumentsCaptured()

        with patch.object(self.m, 'setup_stdout'), patch.object(argparse.ArgumentParser, 'parse_args', stop_after_arguments):
            with self.assertRaises(ArgumentsCaptured):
                self.m.main()
        self.assertEqual(captured['fetch_mode'], self.scraper.fetch_mode)
        self.assertEqual(captured['timeout'], self.scraper.fetch_timeout)
        self.assertIsNone(captured['wait'])
        self.assertEqual(captured['screenshot_max_attempts'], 2)
        self.assertEqual(captured['screenshot_wait'], self.scraper.screenshot_wait)

    def test_browser_connection_errors_stop_at_attempt_limit(self):
        fake, playwright, *_ = self.screenshot_context()
        playwright.chromium.connect_over_cdp.side_effect = RuntimeError('private-connection-placeholder')
        with patch.dict(sys.modules, {'playwright.sync_api': fake}), patch.object(self.m.time, 'sleep'), self.assertLogs(self.m.logger, level='WARNING') as logs:
            self.assertEqual(self.scraper.capture_null_screenshot({'retailersku': 'example'}, URL), 'fail')
        self.assertEqual(playwright.chromium.connect_over_cdp.call_count, 2)
        self.assertNotIn('private-connection-placeholder', ' '.join(logs.output))

    def test_capture_returns_on_success_without_retrying(self):
        fake, playwright, page, *_ = self.screenshot_context('ready', ({'title': 'SSD', 'imageurl': 'image', 'retailprice': None}, 'ONLINE_STOCK_EXHAUSTED'))
        page.state['visiblePriceTexts'] = []
        with patch.dict(sys.modules, {'playwright.sync_api': fake}), patch.object(self.m.time, 'sleep') as sleep:
            self.assertEqual(self.scraper.capture_null_screenshot({'retailersku': 'example'}, URL), 'ok')
        self.assertEqual(playwright.chromium.connect_over_cdp.call_count, 1)
        sleep.assert_not_called()

    def test_browser_recovery_updates_result_and_reason(self):
        self.scraper.fetch_html = Mock(return_value=(0, '', None, 0))
        self.scraper.fetch_browser_html = Mock(return_value=(403, '', 0))
        fake, _, *_ = self.screenshot_context('ready',
            ({'title': 'SSD', 'imageurl': 'image', 'retailprice': 123.45}, 'VISIBLE_PRICE_BOX'))
        with patch.dict(sys.modules, {'playwright.sync_api': fake}):
            result = self.scraper.collect_one({'retailersku': 'example', 'url': URL})
        self.assertEqual(result['retailprice'], 123.45)
        self.assertEqual(result['_crawl_reason'], 'BROWSER_RECOVERED_VISIBLE_PRICE_BOX')
        self.assertNotIn('_browser_reparse_reason', result)
        self.m.capture_and_upload.assert_not_called()

    def test_product_price_title_image_and_offer_url_are_preserved(self):
        result, reason = self.scraper.parse_product(PRODUCT, {'retailersku': 'sku-example', 'url': URL})
        self.assertEqual(reason, 'VISIBLE_PRICE_BOX')
        self.assertEqual(result['retailprice'], 123.45)
        self.assertEqual(result['retailersku'], 'sku-example')
        self.assertEqual(result['producturl'], URL)
        self.assertEqual(result['title'], 'Samsung SSD')
        self.assertTrue(result['imageurl'])

    def test_online_stock_exhausted_stays_null_even_with_visible_price(self):
        result, reason = self.scraper.parse_product(PRODUCT.replace('En stock', 'Stock en ligne épuisé'), {'url': URL})
        self.assertEqual(reason, 'ONLINE_STOCK_EXHAUSTED')
        self.assertIsNone(result['retailprice'])

    def test_other_offers_do_not_become_representative_price(self):
        result, _ = self.scraper.parse_product('Autres offres' + PRODUCT, {'url': URL})
        self.assertIsNone(result['retailprice'])

    def test_failed_product_does_not_prevent_next_product_completion(self):
        bad = {'retailersku': 'bad', 'url': URL}
        good = {'retailersku': 'good', 'url': URL}
        self.scraper.capture_null = False
        self.scraper.fetch_html = Mock(side_effect=[(422, '', None, 0), (200, PRODUCT, None, 0)])
        self.scraper.fetch_browser_html = Mock(return_value=(403, '', 0))
        rows = self.scraper.collect([bad, good], sleep_seconds=0, workers=1)
        self.assertEqual(len(rows), 2)
        self.assertIsNone(rows[0]['retailprice'])
        self.assertEqual(rows[1]['retailprice'], 123.45)


if __name__ == '__main__':
    unittest.main()

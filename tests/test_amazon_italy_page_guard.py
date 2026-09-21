"""Offline behavior tests: compile the real crawler without importing config/services.

Run with: python -m unittest discover -s tests -p test_amazon_italy_page_guard.py
Requires pandas, already required by the crawler. No browser, DB or network is used.
"""
import ast
from datetime import datetime, timezone
import logging
import os
from pathlib import Path
import re
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from amazon_page_guard import (
    AmazonProductPageError, capture_product_page_snapshot, extract_asin,
    wait_for_product_page,
)

SOURCE_PATH = Path(__file__).resolve().parents[1] / 'it_v2.py'
TREE = ast.parse(SOURCE_PATH.read_text(encoding='utf-8-sig'))
URL = 'https://www.amazon.it/dp/B087DFLF9S'
ROW = {'url': URL, 'retailersku': 'original-sku', 'item': 'SSD', 'brand': 'Samsung'}


class Driver:
    def __init__(self, valid=True, *, dom_asin='B087DFLF9S', redirect=None, body=''):
        self.current_url = URL
        self.title = 'Amazon.it'
        self.redirect = redirect
        self.signals = dict(productTitle='Samsung SSD' if valid else '', domAsin=dom_asin,
                            hasDpContainer=True, hasRecommendations=True, bodyText=body)
        self.visits = []
        self.quit = Mock()
        self.refresh = Mock()
        self.set_page_load_timeout = Mock()
        self.maximize_window = Mock()
        self.buttons = []

    def get(self, url):
        self.visits.append(url)
        self.current_url = self.redirect or url

    def execute_script(self, script):
        return self.signals

    def find_elements(self, by, selector):
        return self.buttons


class ItalyCrawlerTests(unittest.TestCase):
    def setUp(self):
        self.ns = dict(
            __name__='isolated_it_tests', os=os, pd=pd, re=re, datetime=datetime,
            pytz=SimpleNamespace(timezone=lambda _: timezone.utc),
            logger=logging.getLogger('it_test'), time=SimpleNamespace(sleep=Mock()),
            random=SimpleNamespace(uniform=lambda a, b: a),
            By=SimpleNamespace(XPATH='xpath', CSS_SELECTOR='css'),
            AmazonProductPageError=AmazonProductPageError,
            capture_product_page_snapshot=capture_product_page_snapshot,
            extract_asin=extract_asin,
            wait_for_product_page=lambda driver, **kwargs: wait_for_product_page(
                driver, **dict(kwargs, timeout_seconds=0)),
            accept_cookies=Mock(), is_null_result=Mock(return_value=False),
            FULL_NULL_FIELDS=(), capture_and_upload=Mock(), WebDriverWait=Mock(),
        )
        node = next(n for n in TREE.body if isinstance(n, ast.ClassDef) and n.name == 'AmazonITScraper')
        exec(compile(ast.Module(body=[node], type_ignores=[]), str(SOURCE_PATH), 'exec'), self.ns)
        self.cls = self.ns['AmazonITScraper']
        with patch.object(self.cls, 'setup_db_connection'), patch.object(self.cls, 'load_selectors_from_db'), patch.dict(os.environ, {}, clear=True):
            self.scraper = self.cls()
        self.scraper.extract_element_text = Mock(side_effect=lambda _, label: 'Samsung SSD' if label == '제목' else 'Amazon EU')
        self.scraper.extract_price = Mock(return_value='61.84')
        self.scraper.check_stock_availability = Mock(return_value=True)
        self.drivers = []
        self.ns['logger'].addHandler(logging.NullHandler())
        self.ns['logger'].propagate = False

    def browsers(self, *drivers):
        queue = iter(drivers)
        def setup():
            self.scraper.driver = next(queue)
            self.drivers.append(self.scraper.driver)
            return True
        self.scraper.setup_driver = Mock(side_effect=setup)

    def start(self, first, *replacements):
        self.scraper.driver = first
        self.drivers.append(first)
        self.browsers(*replacements)

    def collect(self, **kwargs):
        return self.scraper.extract_product_info(URL, ROW, **kwargs)

    def test_valid_page_keeps_original_price_seller_identity(self):
        driver = Driver()
        self.start(driver)
        result = self.collect()
        self.assertEqual(result['retailprice'], '61.84')
        self.assertEqual(result['sold_by'], 'Amazon EU')
        self.assertEqual(result['retailersku'], ROW['retailersku'])
        self.assertEqual(result['producturl'], URL)
        self.assertEqual(driver.visits, [URL])
        self.scraper.setup_driver.assert_not_called()

    def test_empty_db_title_selector_uses_verified_title(self):
        self.start(Driver())
        self.scraper.extract_element_text.side_effect = lambda _, label: None if label == '제목' else 'Amazon EU'
        self.assertEqual(self.collect()['title'], 'Samsung SSD')

    def test_no_seller_is_missing_price_not_browser_failure(self):
        self.start(Driver())
        self.scraper.extract_element_text.side_effect = lambda _, label: 'Samsung SSD' if label == '제목' else None
        self.scraper.check_stock_availability.return_value = False
        result = self.collect()
        self.assertIsNone(result['retailprice'])
        self.assertEqual(result['title'], 'Samsung SSD')
        self.scraper.extract_price.assert_not_called()
        self.scraper.setup_driver.assert_not_called()

    def test_original_italian_price_and_seller_normalization(self):
        for raw, expected in [('61,84 €', '61.84'), ('1.234,56 €', '1234.56'),
                              ('304,', None), ('was € 61,84', None)]:
            with self.subTest(raw=raw):
                self.assertEqual(self.scraper.parse_italian_price(raw), expected)
        self.assertEqual(self.scraper.clean_ships_sold_text('Venduto da Amazon EU', 'Sold By'), 'Amazon EU')

    def test_failure_result_preserves_success_schema(self):
        self.start(Driver())
        good = self.collect()
        failed = self.scraper.build_failed_result(URL, ROW)
        self.assertEqual(set(good), set(failed))
        for name in ['retailersku', 'producturl', 'brand', 'country_code', 'channel_name']:
            self.assertEqual(good[name], failed[name])

    def test_explicit_robot_page_never_enters_price_extraction(self):
        first = Driver(body="sorry, we just need to make sure you're not a robot")
        self.start(first, Driver())
        self.assertEqual(self.collect()['retailprice'], '61.84')
        first.quit.assert_called_once()
        self.scraper.extract_price.assert_called_once()

    def test_recommendations_retry_in_fresh_browser_then_succeed(self):
        bad, good = Driver(False), Driver()
        self.start(bad, good)
        self.assertEqual(self.collect()['retailprice'], '61.84')
        self.assertEqual(bad.visits, [URL])
        self.assertEqual(good.visits, [URL])
        bad.quit.assert_called_once()
        bad.refresh.assert_not_called()
        self.scraper.extract_price.assert_called_once()
        self.assertIsNone(self.scraper.last_failure_reason)

    def test_two_attempt_limit_including_legacy_request_for_ten(self):
        first, second = Driver(False), Driver(False)
        self.start(first, second)
        result = self.collect(max_retries=10)
        self.assertIsNone(result['title'])
        self.assertEqual(len(first.visits) + len(second.visits), 2)
        self.assertTrue(self.scraper.browser_needs_restart)
        self.scraper.extract_price.assert_not_called()

    def test_later_product_is_still_visited_after_four_consecutive_failures(self):
        drivers = [Driver(False) for _ in range(4)] + [Driver()]
        self.start(drivers[0], *drivers[1:])
        for _ in range(4):
            self.assertIsNone(self.collect(max_retries=1)['title'])
        self.assertEqual(self.collect(max_retries=1)['retailprice'], '61.84')
        self.assertEqual([len(d.visits) for d in drivers], [1] * 5)

    def test_legacy_recovery_one_attempt_and_fresh_browser_next_call(self):
        bad, good = Driver(False), Driver()
        self.start(bad, good)
        self.assertIsNone(self.collect(retry_count=0, max_retries=1)['title'])
        self.scraper.setup_driver.assert_not_called()
        self.assertEqual(self.collect(retry_count=0, max_retries=1)['retailprice'], '61.84')
        self.assertEqual(bad.visits, [URL])
        self.assertEqual(good.visits, [URL])

    def test_identity_or_domain_mismatch_never_relabels_product(self):
        for driver, reason in [(Driver(dom_asin='B0GJF1GQFX'), 'asin_mismatch'),
                               (Driver(redirect='https://www.amazon.de/dp/B087DFLF9S'), 'invalid_domain')]:
            with self.subTest(reason=reason):
                self.start(driver)
                result = self.collect()
                self.assertIsNone(result['retailprice'])
                self.assertEqual(result['retailersku'], ROW['retailersku'])
                self.assertEqual(result['producturl'], URL)
                self.assertEqual(self.scraper.last_failure_reason, reason)
                self.scraper.setup_driver.assert_not_called()
        self.scraper.extract_price.assert_not_called()

    def test_evidence_upload_failure_does_not_refetch_valid_product(self):
        self.start(Driver())
        self.ns['is_null_result'].return_value = True
        self.ns['capture_and_upload'].side_effect = RuntimeError('offline test')
        self.assertEqual(self.collect()['retailprice'], '61.84')
        self.scraper.setup_driver.assert_not_called()

    def test_navigation_timeout_uses_same_bounded_retry(self):
        bad, good = Driver(), Driver()
        bad.get = Mock(side_effect=TimeoutError())
        self.start(bad, good)
        self.assertEqual(self.collect()['retailprice'], '61.84')
        bad.get.assert_called_once_with(URL)
        self.assertEqual(good.visits, [URL])

    def test_continue_button_clicked_once_then_validated(self):
        driver = Driver(False)
        button = Mock()
        button.click.side_effect = lambda: driver.signals.update(productTitle='Samsung SSD')
        driver.buttons = [button]
        self.start(driver)
        self.assertEqual(self.collect()['retailprice'], '61.84')
        button.click.assert_called_once()
        self.assertEqual(driver.visits, [URL])
        self.scraper.setup_driver.assert_not_called()

    def test_continue_button_does_not_make_wrong_product_valid(self):
        driver = Driver(False)
        button = Mock()
        button.click.side_effect = lambda: driver.signals.update(productTitle='Other SSD', domAsin='B0GJF1GQFX')
        driver.buttons = [button]
        self.start(driver)
        self.assertIsNone(self.collect()['retailprice'])
        self.assertEqual(self.scraper.last_failure_reason, 'asin_mismatch')

    def test_113_persistent_blocks_visit_all_targets_then_retry_each_once(self):
        drivers = [Driver(False) for _ in range(113 * 3)]
        self.browsers(*drivers)
        self.scraper.db_engine = object()
        rows = [dict(ROW, retailersku=f'sku-{i}', url=f'https://www.amazon.it/dp/B{i:09d}')
                for i in range(113)]
        saved = []
        def save(frame, *args, **kwargs):
            saved.extend(frame.to_dict('records'))
        with patch.object(pd.DataFrame, 'to_sql', save):
            result, failures = self.scraper.scrape_urls(rows)
        self.assertEqual(len(result), 113)
        self.assertEqual(len(failures), 113)
        self.assertEqual([r['retailersku'] for r in saved], [r['retailersku'] for r in rows])
        self.assertTrue(result['title'].isna().all())
        visits = [url for driver in drivers for url in driver.visits]
        self.assertEqual(visits, [row['url'] for row in rows for _ in range(2)]
                         + [row['url'] for row in rows])
        self.assertIsNone(self.scraper.driver)
        for driver in drivers:
            driver.quit.assert_called_once()

    def test_failed_sweep_runs_once_and_keeps_distinct_rows_with_same_url(self):
        driver = Driver(dom_asin='B0GJF1GQFX')
        self.browsers(driver)
        rows = [dict(ROW, retailersku=f'sku-{i}') for i in range(3)]
        results, failures = self.scraper.scrape_urls(rows)
        self.assertEqual(len(results), 3)
        self.assertEqual(len(failures), 3)
        self.assertEqual(results['retailersku'].tolist(), [r['retailersku'] for r in rows])
        self.assertEqual(driver.visits, [URL] * 6)
        self.scraper.setup_driver.assert_called_once()

    def test_restart_startup_failure_records_failure_but_next_product_can_run(self):
        first, partial, good = Driver(False), Driver(), Driver()
        self.start(first)
        setups = iter([(partial, False), (good, True)])
        def setup():
            self.scraper.driver, success = next(setups)
            return success
        self.scraper.setup_driver.side_effect = setup
        self.assertIsNone(self.collect()['title'])
        self.assertEqual(self.scraper.last_failure_reason, 'browser_start_failed')
        self.assertIsNone(self.scraper.driver)
        partial.quit.assert_called_once()
        self.assertEqual(self.collect()['retailprice'], '61.84')
        self.assertEqual(good.visits, [URL])

    def test_initial_startup_failure_closes_partial_driver(self):
        partial = Driver()
        def failed_setup():
            self.scraper.driver = partial
            return False
        self.scraper.setup_driver = Mock(side_effect=failed_setup)
        self.assertEqual(self.scraper.scrape_urls([ROW]), (None, []))
        partial.quit.assert_called_once()

    def test_setup_uses_actual_chrome_and_bounded_navigation(self):
        driver, options = Driver(), Mock()
        self.ns['uc'] = SimpleNamespace(ChromeOptions=Mock(return_value=options), Chrome=Mock(return_value=driver))
        with patch('subprocess.run', return_value=SimpleNamespace(returncode=0, stdout='version REG_SZ 153.0.8010.50')):
            self.assertTrue(self.scraper.setup_driver())
        arguments = [call.args[0] for call in options.add_argument.call_args_list]
        self.assertFalse(any('user-agent' in arg for arg in arguments))
        self.assertEqual(self.ns['uc'].Chrome.call_args.kwargs['version_main'], 153)
        driver.set_page_load_timeout.assert_called_once_with(45)

    def test_main_passes_all_failed_results_to_existing_recovery(self):
        main = next(n for n in TREE.body if isinstance(n, ast.FunctionDef) and n.name == 'main')
        exec(compile(ast.Module(body=[main], type_ignores=[]), str(SOURCE_PATH), 'exec'), self.ns)
        scraper = Mock()
        rows = [dict(ROW, retailersku=f'sku-{i}') for i in range(6)]
        scraper.get_crawl_targets.return_value = rows
        frame = pd.DataFrame([self.scraper.build_failed_result(URL, row) for row in rows])
        scraper.scrape_urls.return_value = (frame, rows)
        self.ns['AmazonITScraper'] = Mock(return_value=scraper)
        self.ns['monitor_and_alert'] = Mock()
        log_utils, auto_recovery = Mock(), Mock()
        with patch.dict(sys.modules, log_utils=log_utils, auto_recovery=auto_recovery), patch.dict(os.environ, {}, clear=True), patch('builtins.print'):
            self.ns['main']()
        scraper.save_results.assert_called_once_with(frame, save_db=False, upload_server=False)
        auto_recovery.auto_recovery_run.assert_called_once_with(
            target_key='it', results_df=frame, target_count=6, error_logs=None)
        self.ns['monitor_and_alert'].assert_not_called()
        log_utils.save_log.assert_called_once_with('it_amazon')

    def test_main_completed_run_hands_off_to_existing_auto_recovery(self):
        main = next(n for n in TREE.body if isinstance(n, ast.FunctionDef) and n.name == 'main')
        exec(compile(ast.Module(body=[main], type_ignores=[]), str(SOURCE_PATH), 'exec'), self.ns)
        scraper = Mock()
        scraper.get_crawl_targets.return_value = [ROW]
        frame = pd.DataFrame([self.scraper.build_failed_result(URL, ROW)])
        scraper.scrape_urls.return_value = (frame, [])
        self.ns['AmazonITScraper'] = Mock(return_value=scraper)
        self.ns['monitor_and_alert'] = Mock()
        log_utils, auto_recovery = Mock(), Mock()
        with patch.dict(sys.modules, log_utils=log_utils, auto_recovery=auto_recovery), patch.dict(os.environ, {}, clear=True), patch('builtins.print'):
            self.ns['main']()
        auto_recovery.auto_recovery_run.assert_called_once_with(target_key='it', results_df=frame, target_count=1, error_logs=None)

    def test_retry_success_saved_once_in_correct_row_with_duplicate_url(self):
        first, retry, healthy = Driver(False), Driver(False), Driver()
        self.browsers(first, retry, healthy)
        self.scraper.db_engine = object()
        rows = [dict(ROW, retailersku='failed-first'), dict(ROW, retailersku='good-first')]
        saved = []
        def save(frame, *args, **kwargs):
            saved.extend(frame.to_dict('records'))
        with patch.object(pd.DataFrame, 'to_sql', save):
            result, failures = self.scraper.scrape_urls(rows)
        self.assertEqual(failures, [])
        self.assertEqual(result['retailersku'].tolist(), ['failed-first', 'good-first'])
        self.assertEqual([len(d.visits) for d in [first, retry, healthy]], [1, 1, 2])
        self.assertEqual(len(saved), 2)
        pd.testing.assert_frame_equal(
            pd.DataFrame(saved).sort_values('retailersku').reset_index(drop=True),
            result.sort_values('retailersku').reset_index(drop=True))
        self.assertTrue(result['title'].notna().all())

    def test_mixed_batches_and_remainder_save_latest_retry_results_once(self):
        rows = [dict(ROW, retailersku=f'sku-{i:02}', item=f'item-{i}') for i in range(23)]
        deferred = {0, 9, 10, 20, 22}
        self.browsers(Driver())
        self.scraper.db_engine = object()
        attempts, saved, saved_at_attempt = [], [], []
        def extract(url, row, retry_count=0, max_retries=2):
            attempts.append((row['retailersku'], max_retries))
            index = int(row['retailersku'].split('-')[1])
            result = self.scraper.build_failed_result(url, row)
            if index not in deferred or (max_retries == 1 and index != 22):
                result.update(title='Recovered' if max_retries == 1 else 'First pass', retailprice='61.84')
            return result
        self.scraper.extract_product_info = Mock(side_effect=extract)
        def save(frame, *args, **kwargs):
            saved.extend(frame.to_dict('records'))
            saved_at_attempt.append(len(attempts))
        with patch.object(pd.DataFrame, 'to_sql', save):
            result, failures = self.scraper.scrape_urls(rows)
        self.assertEqual(attempts, [(r['retailersku'], 2) for r in rows]
                         + [(rows[i]['retailersku'], 1) for i in sorted(deferred)])
        self.assertEqual(len(saved), 23)
        self.assertEqual(len({r['retailersku'] for r in saved}), 23)
        self.assertEqual(saved_at_attempt[:2], [10, 20])
        pd.testing.assert_frame_equal(
            pd.DataFrame(saved).sort_values('retailersku').reset_index(drop=True), result)
        self.assertEqual([f['row_data']['retailersku'] for f in failures], ['sku-22'])

    def test_normal_page_without_price_is_saved_without_extra_page_retry(self):
        self.browsers(Driver())
        self.scraper.db_engine = object()
        self.scraper.extract_element_text.side_effect = lambda _, label: 'Samsung SSD' if label == '제목' else None
        with patch.object(pd.DataFrame, 'to_sql') as save:
            result, failures = self.scraper.scrape_urls([ROW])
        self.assertEqual(failures, [])
        self.assertIsNone(result.iloc[0]['retailprice'])
        self.assertEqual(self.drivers[0].visits, [URL])
        save.assert_called_once()

    def test_interrupted_extra_sweep_keeps_all_deferred_rows_for_recovery(self):
        self.browsers(Driver())
        self.scraper.db_engine = object()
        rows = [dict(ROW, retailersku=f'sku-{i}') for i in range(3)]
        def extract(url, row, retry_count=0, max_retries=2):
            if max_retries == 1:
                raise RuntimeError('simulated retry interruption')
            return self.scraper.build_failed_result(url, row)
        self.scraper.extract_product_info = Mock(side_effect=extract)
        saved = []
        def save(frame, *args, **kwargs):
            saved.extend(frame.to_dict('records'))
        with patch.object(pd.DataFrame, 'to_sql', save):
            result, failures = self.scraper.scrape_urls(rows)
        self.assertEqual(len(result), 3)
        self.assertEqual(len(failures), 3)
        self.assertEqual(len(saved), 3)
        self.drivers[0].quit.assert_called_once()

    def test_existing_auto_recovery_stops_after_five_failures_and_uploads(self):
        # Execute the unchanged coordinator and adapter with all external effects mocked.
        root = SOURCE_PATH.parent
        coordinator = ast.parse((root / 'auto_recovery.py').read_text(encoding='utf-8-sig'))
        adapter = ast.parse((root / 'recovery.py').read_text(encoding='utf-8-sig'))
        nodes = [next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == name)
                 for tree, name in [(coordinator, 'auto_recovery_run'), (adapter, 'recrawl_url')]]
        rows = [dict(ROW, retailersku=f'sku-{i}') for i in range(6)]
        frame = pd.DataFrame([self.scraper.build_failed_result(URL, r) for r in rows])
        drivers = [Driver(False) for _ in range(5)]
        self.start(drivers[0], *drivers[1:])
        manager = Mock()
        manager.load_scraper.return_value = self.scraper
        ns = dict(TARGET_CONFIG={'it': {'name': 'IT', 'alert_code': 'it'}}, logger=Mock(), pd=pd,
                  RecoveryManager=Mock(return_value=manager), time=Mock(), monitor_and_alert=Mock(),
                  _load_title_null_thresholds=Mock(return_value={}),
                  _get_missing_urls_between=Mock(return_value=pd.DataFrame()),
                  _get_null_records_between=Mock(return_value=frame),
                  _compare_with_first_crawl=Mock(return_value=False), _upload_and_alert=Mock())
        exec(compile(ast.Module(body=nodes, type_ignores=[]), 'isolated_recovery', 'exec'), ns)
        manager.recrawl_url.side_effect = lambda *args: ns['recrawl_url'](manager, *args)
        screenshots = SimpleNamespace(RETAILER_NAME_BY_TARGET_KEY={}, delete_screenshots_for_sku=Mock())
        with patch.dict(sys.modules, null_screenshot=screenshots):
            ns['auto_recovery_run']('it', frame, len(rows))
        self.assertEqual(manager.recrawl_url.call_count, 5)
        self.assertEqual(sum(len(d.visits) for d in drivers), 5)
        ns['_upload_and_alert'].assert_called_once()
        self.assertIs(ns['_upload_and_alert'].call_args.args[3], frame)


if __name__ == '__main__':
    unittest.main()

"""ES recovery actions and fresh-browser retries, without external services."""
import unittest
from unittest.mock import Mock, call, patch
from types import SimpleNamespace

import pandas as pd

from test_amazon_redirect_collection import (
    make_scraper, Driver, ORIGINAL, DESTINATION,
)
import test_amazon_redirect_collection as redirect_support


class Browser(Driver):
    def __init__(self, url, signals=None, *, body='', title='Amazon', controls=None):
        super().__init__(url, signals or {})
        self.body = body
        self.title = title
        self.controls = controls or []

    def execute_script(self, script, *args):
        if script == "return document.body ? document.body.innerText : '';":
            return self.body
        if args:
            return None
        return self.signals

    def find_elements(self, *args):
        return self.controls


def control(text, href='https://www.amazon.es/', *, tag='a', displayed=True):
    item = Mock()
    item.text = text
    item.tag_name = tag
    item.is_displayed.return_value = displayed
    item.get_attribute.side_effect = lambda name: href if name == 'href' else ''
    return item


class SpainRecoveryTests(unittest.TestCase):
    def scraper(self, signals=None, **kwargs):
        obj, ns, url = make_scraper('es', signals or {})
        obj.driver = Browser(url, signals, **kwargs)
        return obj, ns, url

    def use_actual_restart(self, obj, *drivers):
        del obj.restart_driver
        queue = iter(drivers)
        def setup():
            obj.driver = next(queue)
            return True
        obj.setup_driver = Mock(side_effect=setup)

    def test_html_503_in_scripts_is_not_an_error(self):
        obj, _, _ = self.scraper({'hasRecommendations': True})
        obj.driver.page_source = '<script>var value = 1503;</script>'
        self.assertFalse(obj.is_error_page())
        self.assertFalse(obj.is_page_blocked())

    def test_product_number_503_in_title_does_not_trigger_error(self):
        obj, _, _ = self.scraper(title='SSD model 503 1TB')
        self.assertFalse(obj.is_error_page())
        self.assertFalse(obj.is_page_blocked())

    def test_valid_product_with_error_words_is_not_rejected(self):
        obj, _, _ = self.scraper({'productTitle': 'SSD 503', 'domAsin': ORIGINAL},
                                body='Lo sentimos, no disponible en tu dirección')
        self.assertFalse(obj.is_error_page())
        self.assertFalse(obj.is_page_blocked())

    def test_visible_service_error_and_standalone_error_title_are_detected(self):
        for kwargs in ({'body': 'Se ha producido un error'}, {'title': '503'},
                       {'title': '503 Service Unavailable'}):
            with self.subTest(kwargs=kwargs):
                obj, _, _ = self.scraper(**kwargs)
                self.assertTrue(obj.is_error_page())
                self.assertTrue(obj.is_page_blocked())

    def test_robot_and_foreign_marketplace_remain_blocked(self):
        obj, _, _ = self.scraper({'productTitle': 'SSD', 'domAsin': ORIGINAL,
                                 'bodyText': 'automated access to Amazon data'})
        self.assertTrue(obj.is_page_blocked())
        obj.driver.current_url = 'https://www.amazon.in/'
        self.assertTrue(obj.is_page_blocked())

    def test_login_link_and_generic_primary_button_are_never_clicked(self):
        login = control('Hola, identifícate', 'https://www.amazon.es/ap/signin')
        generic = control('Aceptar', tag='button')
        obj, _, url = self.scraper(controls=[login, generic])
        self.assertFalse(type(obj).handle_captcha_or_block_page(obj, original_url=url))
        login.click.assert_not_called()
        generic.click.assert_not_called()
        self.assertEqual(obj.driver.visits, [])

    def test_error_title_is_detected_when_body_script_fails(self):
        obj, _, _ = self.scraper(title='503')
        obj.driver.execute_script = Mock(side_effect=RuntimeError('script unavailable'))
        self.assertTrue(obj.is_error_page())

    def test_home_link_requires_both_meaning_and_home_destination(self):
        links = [control('Haz clic aquí para volver', 'https://www.amazon.es/ap/signin'),
                 control('Haz clic aquí para volver', 'https://www.amazon.es.attacker.invalid/'),
                 control('Oferta de SSD', 'https://www.amazon.es/'),
                 control('Haz clic aquí para volver', '/ref=error_home')]
        obj, _, url = self.scraper(controls=links)
        self.assertTrue(obj.click_blue_link_and_return(url))
        for link in links[:-1]:link.click.assert_not_called()
        links[-1].click.assert_called_once()
        self.assertEqual(obj.driver.visits, [url])

    def test_hidden_home_link_is_not_clicked(self):
        link = control('Haz clic aquí para volver', displayed=False)
        obj, _, url = self.scraper(controls=[link])
        self.assertFalse(obj.click_blue_link_and_return(url))
        link.click.assert_not_called()

    def test_labelled_continue_button_returns_to_requested_product(self):
        button = control('Seguir comprando', tag='button')
        obj, _, url = self.scraper(controls=[button])
        obj.driver.current_url = 'https://www.amazon.es/error'
        self.assertTrue(type(obj).handle_captcha_or_block_page(obj, original_url=url))
        button.click.assert_called_once()
        self.assertEqual(obj.driver.visits, [url])

    def test_continue_button_javascript_fallback_is_retained(self):
        button = control('Continue shopping', tag='button')
        button.click.side_effect = RuntimeError('click intercepted')
        obj, _, url = self.scraper(controls=[button])
        obj.driver.execute_script = Mock(wraps=obj.driver.execute_script)
        self.assertTrue(type(obj).handle_captcha_or_block_page(obj, original_url=url))
        obj.driver.execute_script.assert_any_call('arguments[0].click();', button)

    def test_foreign_error_page_cannot_execute_recovery_controls(self):
        button = control('Continue shopping', tag='button')
        obj, _, url = self.scraper(controls=[button])
        obj.driver.current_url = 'https://www.amazon.es.attacker.invalid/'
        self.assertFalse(type(obj).handle_captcha_or_block_page(obj, original_url=url))
        button.click.assert_not_called()

    def test_error_recovery_has_one_product_wait_and_no_legacy_load_wait(self):
        obj, ns, url = self.scraper(title='503')
        def recover(**kwargs):
            obj.driver.title = 'Amazon'
            obj.driver.signals = {'productTitle': 'Recovered SSD', 'domAsin': ORIGINAL}
            return True
        obj.handle_captcha_or_block_page.side_effect = recover
        obj.extract_element_text.side_effect = lambda _, label: 'Recovered SSD' if label == '제목' else 'Amazon'
        ns['wait_for_product_page'] = Mock(wraps=ns['wait_for_product_page'])
        result = obj.extract_product_info(url, {}, max_retries=0)
        self.assertEqual(result['title'], 'Recovered SSD')
        ns['wait_for_product_page'].assert_called_once()
        self.assertEqual(ns['wait_for_product_page'].call_args.kwargs['timeout_seconds'], 12)
        obj.wait_for_page_load.assert_not_called()
        self.assertNotIn(call(3), ns['time'].sleep.call_args_list)

    def test_failed_listing_recovers_using_a_distinct_browser(self):
        obj, ns, url = self.scraper({'hasRecommendations': True})
        first = obj.driver
        fresh = Browser(url, {'productTitle': 'Fresh SSD', 'domAsin': ORIGINAL})
        self.use_actual_restart(obj, fresh)
        obj.extract_element_text.side_effect = lambda _, label: 'Fresh SSD' if label == '제목' else 'Amazon'
        result = obj.extract_product_info(url, {'retailersku': 'original-sku'})
        self.assertEqual(result['title'], 'Fresh SSD')
        self.assertEqual(result['retailersku'], 'original-sku')
        self.assertEqual(result['producturl'], url)
        self.assertEqual(first.visits, [url])
        self.assertEqual(fresh.visits, [url])
        first.quit.assert_called_once()
        first.refresh.assert_not_called()
        self.assertFalse(obj.browser_needs_restart)
        self.assertEqual(ns['time'].sleep.call_args_list, [call(0), call(3), call(0)])

    def test_retry_limit_is_capped_even_if_legacy_caller_requests_three(self):
        obj, ns, url = self.scraper({'hasRecommendations': True})
        first = obj.driver
        second = Browser(url, {'hasRecommendations': True})
        self.use_actual_restart(obj, second)
        ns['is_null_result'] = lambda *args: True
        result = obj.extract_product_info(url, {'retailersku': 'failed'}, max_retries=3)
        self.assertIsNone(result['title'])
        self.assertEqual(first.visits, [url])
        self.assertEqual(second.visits, [url])
        obj.setup_driver.assert_called_once()
        ns['capture_and_upload'].assert_called_once_with(second, 'amazon_es', 'failed', url, result)

    def test_zero_retries_is_respected(self):
        obj, _, url = self.scraper({'hasRecommendations': True})
        result = obj.extract_product_info(url, {}, max_retries=0)
        self.assertIsNone(result['title'])
        obj.restart_driver.assert_not_called()
        self.assertEqual(obj.driver.visits, [url])

    def test_browser_setup_failure_returns_null_and_next_row_can_start_browser(self):
        obj, ns, url = self.scraper({'hasRecommendations': True})
        old = obj.driver
        del obj.restart_driver
        obj.setup_driver = Mock(return_value=False)
        result = obj.extract_product_info(url, {'retailersku': 'first'})
        self.assertIsNone(result['title'])
        old.quit.assert_called_once()
        self.assertTrue(obj.browser_needs_restart)
        next_url = f'https://www.amazon.es/dp/{DESTINATION}'
        fresh = Browser(next_url, {'productTitle': 'Next SSD', 'domAsin': DESTINATION})
        def setup():
            obj.driver = fresh
            return True
        obj.setup_driver.side_effect = setup
        obj.extract_element_text.side_effect = lambda _, label: 'Next SSD' if label == '제목' else 'Amazon'
        next_result = obj.extract_product_info(next_url, {'retailersku': 'second'})
        self.assertEqual(next_result['title'], 'Next SSD')
        self.assertFalse(obj.browser_needs_restart)

    def test_periodic_setup_clears_pending_restart_so_fresh_browser_is_not_discarded(self):
        obj, ns, url = self.scraper({'productTitle': 'SSD', 'domAsin': ORIGINAL})
        obj.browser_needs_restart = True
        fresh = Browser(url, {'productTitle': 'SSD', 'domAsin': ORIGINAL})
        fresh.maximize_window = Mock()
        ns['uc'] = SimpleNamespace(ChromeOptions=Mock(return_value=Mock()), Chrome=Mock(return_value=fresh))
        ns['WebDriverWait'] = Mock()
        with patch('subprocess.run', return_value=SimpleNamespace(returncode=1)):
            self.assertTrue(type(obj).setup_driver(obj))
        self.assertFalse(obj.browser_needs_restart)
        result = obj.extract_product_info(url, {})
        self.assertEqual(result['title'], 'SSD')
        obj.restart_driver.assert_not_called()

    def test_setup_uses_default_user_agent_and_keeps_chrome_matching_and_spanish_language(self):
        for installed_major in (153, None):
            with self.subTest(installed_major=installed_major):
                obj, ns, url = self.scraper({'productTitle': 'SSD', 'domAsin': ORIGINAL})
                fresh = Browser(url, {'productTitle': 'SSD', 'domAsin': ORIGINAL})
                fresh.maximize_window = Mock()
                options = Mock()
                ns['uc'] = SimpleNamespace(ChromeOptions=Mock(return_value=options),
                                           Chrome=Mock(return_value=fresh))
                ns['WebDriverWait'] = Mock()
                ns['random'].choice = Mock(side_effect=AssertionError('obsolete User-Agent choice'))
                registry = SimpleNamespace(
                    returncode=0 if installed_major is not None else 1,
                    stdout='    version    REG_SZ    153.0.8010.50' if installed_major is not None else '',
                )
                with patch('subprocess.run', return_value=registry):
                    self.assertTrue(type(obj).setup_driver(obj))
                arguments = [args.args[0] for args in options.add_argument.call_args_list]
                self.assertFalse(any(value.startswith('--user-agent=') for value in arguments))
                ns['random'].choice.assert_not_called()
                ns['uc'].Chrome.assert_called_once_with(options=options, version_main=installed_major)
                options.add_experimental_option.assert_called_once_with(
                    'prefs', {'intl.accept_languages': 'es-ES,es'})

    def test_actual_recovery_adapter_can_retry_and_collect_after_first_pass_failure(self):
        obj, _, url = self.scraper({'hasRecommendations': True})
        failed_second = Browser(url, {'hasRecommendations': True})
        recovered = Browser(url, {'productTitle': 'Recovered SSD', 'domAsin': ORIGINAL})
        self.use_actual_restart(obj, failed_second, recovered)
        self.assertIsNone(obj.extract_product_info(url, {'retailersku': 'original-tracking-sku'})['title'])
        obj.extract_element_text.side_effect = lambda _, label: 'Recovered SSD' if label == '제목' else 'Amazon'
        result = redirect_support.RedirectCollectionTests().collect(obj, url, 'es', recovery=True)
        self.assertEqual(result['title'], 'Recovered SSD')
        self.assertEqual(result['retailersku'], 'original-tracking-sku')
        self.assertEqual(result['producturl'], url)

    def test_null_price_without_sellers_keeps_screenshot_and_does_not_retry(self):
        obj, ns, url = self.scraper({'productTitle': 'Unavailable SSD', 'domAsin': ORIGINAL})
        obj.extract_element_text.side_effect = lambda _, label: 'Unavailable SSD' if label == '제목' else None
        ns['is_null_result'] = lambda *args: True
        result = obj.extract_product_info(url, {'retailersku': 'unavailable'})
        self.assertIsNone(result['retailprice'])
        obj.restart_driver.assert_not_called()
        obj.extract_price.assert_not_called()
        ns['capture_and_upload'].assert_called_once_with(obj.driver, 'amazon_es', 'unavailable', url, result)

    def test_real_restart_keeps_batch_running_after_final_product_failure(self):
        obj, ns, url = self.scraper({'hasRecommendations': True})
        first = obj.driver
        failed_second = Browser(url, {'hasRecommendations': True})
        second_url = f'https://www.amazon.es/dp/{DESTINATION}'
        next_browser = Browser(second_url, {'productTitle': 'Next SSD', 'domAsin': DESTINATION})
        self.use_actual_restart(obj, failed_second, next_browser)
        # scrape_urls starts its initial browser; keep it separate from restart setup.
        original_setup = obj.setup_driver
        calls = 0
        def setup():
            nonlocal calls
            calls += 1
            return True if calls == 1 else original_setup()
        obj.setup_driver = setup
        ns['pd'] = pd
        obj.db_engine = object()
        obj.extract_element_text.side_effect = lambda _, label: 'Next SSD' if label == '제목' else 'Amazon'
        rows = [{'url':url,'retailersku':'first'},{'url':second_url,'retailersku':'second'}]
        with patch.object(pd.DataFrame, 'to_sql', Mock()):
            result = obj.scrape_urls(rows)
        self.assertEqual(result['retailersku'].tolist(), ['first', 'second'])
        self.assertTrue(pd.isna(result.iloc[0]['title']))
        self.assertEqual(result.iloc[1]['title'], 'Next SSD')
        self.assertEqual(result['producturl'].tolist(), [url, second_url])
        for driver in (first, failed_second, next_browser):driver.quit.assert_called_once()


if __name__ == '__main__':
    unittest.main()

"""ES page validation and retry regressions without browser/DB/network access."""
import unittest
from unittest.mock import Mock, patch

import pandas as pd

from test_amazon_redirect_collection import make_scraper, ORIGINAL, DESTINATION


class SpainPageGuardTests(unittest.TestCase):
    def test_price_elements_on_listing_are_not_product_evidence(self):
        obj, _, _ = make_scraper('es', {'hasDpContainer': True, 'hasRecommendations': True})
        price = Mock()
        price.is_displayed.return_value = True
        obj.driver.find_element = Mock(return_value=price)
        obj.driver.find_elements = Mock(return_value=[price])
        self.assertFalse(obj.is_normal_product_page())
        obj.driver.find_element.assert_not_called()
        obj.driver.find_elements.assert_not_called()

    def test_initial_listing_retries_original_url_then_collects_product(self):
        obj, _, url = make_scraper('es', {})
        original_get = obj.driver.get
        def navigate(target):
            original_get(target)
            obj.driver.signals = ({'hasRecommendations': True} if len(obj.driver.visits) == 1
                                  else {'productTitle': 'Recovered SSD', 'domAsin': ORIGINAL})
        obj.driver.get = navigate
        obj.extract_element_text.side_effect = lambda _, label: (
            'Recovered SSD' if label == '제목' else 'Amazon')
        result = obj.extract_product_info(url, {'retailersku': 'original-sku'})
        self.assertEqual(result['title'], 'Recovered SSD')
        self.assertEqual(result['producturl'], url)
        self.assertEqual(obj.driver.visits, [url, url])
        obj.driver.refresh.assert_called_once()
        obj.extract_price.assert_called_once()

    def test_persistent_listing_respects_existing_retry_limit(self):
        obj, _, url = make_scraper('es', {'hasRecommendations': True})
        result = obj.extract_product_info(url, {'retailersku': 'original-sku'})
        self.assertEqual(obj.driver.visits, [url] * 4)
        self.assertEqual(obj.driver.refresh.call_count, 3)
        obj.extract_element_text.assert_not_called()
        obj.extract_price.assert_not_called()
        self.assertIsNone(result['title'])
        self.assertIsNone(result['retailprice'])

    def test_error_recovery_returning_listing_is_rejected(self):
        obj, _, url = make_scraper('es', {'hasRecommendations': True})
        obj.driver.title = '503'
        def recover(**kwargs):
            obj.driver.title = 'Amazon'
            return True
        obj.handle_captcha_or_block_page.side_effect = recover
        result = obj.extract_product_info(url, {}, max_retries=0)
        obj.handle_captcha_or_block_page.assert_called_once_with(original_url=url)
        self.assertIsNone(result['title'])
        obj.extract_price.assert_not_called()

    def test_error_recovery_returning_destination_product_is_allowed(self):
        obj, _, url = make_scraper('es', {})
        obj.driver.title = '503'
        def recover(**kwargs):
            obj.driver.title = 'Amazon'
            obj.driver.current_url = f'https://www.amazon.es/dp/{DESTINATION}'
            obj.driver.signals = {'productTitle': 'Destination SSD', 'domAsin': DESTINATION}
            return True
        obj.handle_captcha_or_block_page.side_effect = recover
        obj.extract_element_text.side_effect = lambda _, label: 'Destination SSD' if label == '제목' else 'Amazon'
        result = obj.extract_product_info(url, {'retailersku': 'original-sku'}, max_retries=0)
        self.assertEqual(result['title'], 'Destination SSD')
        self.assertEqual(result['retailersku'], 'original-sku')
        self.assertEqual(result['producturl'], url)
        obj.extract_price.assert_called_once()

    def test_error_handler_uses_requested_url_instead_of_error_destination(self):
        obj, _, url = make_scraper('es', {})
        obj.driver.current_url = 'https://www.amazon.es/error'
        obj.click_blue_link_and_return = Mock(return_value=True)
        result = type(obj).handle_captcha_or_block_page(obj, original_url=url)
        self.assertTrue(result)
        obj.click_blue_link_and_return.assert_called_once_with(url)

    def test_verified_title_is_used_if_db_title_selector_misses(self):
        obj, _, url = make_scraper('es', {'productTitle': 'Verified SSD', 'domAsin': ORIGINAL})
        obj.extract_element_text.side_effect = lambda _, label: None if label == '제목' else 'Amazon'
        result = obj.extract_product_info(url, {}, max_retries=0)
        self.assertEqual(result['title'], 'Verified SSD')
        self.assertEqual(obj.driver.visits, [url])

    def test_no_seller_still_means_no_price_without_page_retry(self):
        obj, _, url = make_scraper('es', {'productTitle': 'Unavailable SSD', 'domAsin': ORIGINAL})
        obj.extract_element_text.side_effect = lambda _, label: 'Unavailable SSD' if label == '제목' else None
        result = obj.extract_product_info(url, {}, max_retries=0)
        self.assertEqual(result['title'], 'Unavailable SSD')
        self.assertIsNone(result['retailprice'])
        obj.extract_price.assert_not_called()
        self.assertEqual(obj.driver.visits, [url])

    def test_failed_product_does_not_stop_next_row_or_change_storage_identity(self):
        obj, ns, url = make_scraper('es', {})
        ns['pd'] = pd
        obj.db_engine = object()
        second_url = f'https://www.amazon.es/dp/{DESTINATION}'
        rows = [{'url': url, 'retailersku': 'first'}, {'url': second_url, 'retailersku': 'second'}]
        original_get = obj.driver.get
        def navigate(target):
            original_get(target)
            obj.driver.signals = ({} if target == url else
                                  {'productTitle': 'Second SSD', 'domAsin': DESTINATION})
        obj.driver.get = navigate
        obj.extract_element_text.side_effect = lambda _, label: 'Second SSD' if label == '제목' else 'Amazon'
        saved = []
        def save(frame, *args, **kwargs):
            saved.extend(frame.to_dict('records'))
        with patch.object(pd.DataFrame, 'to_sql', save):
            frame = obj.scrape_urls(rows)
        self.assertEqual(frame['retailersku'].tolist(), ['first', 'second'])
        self.assertEqual(frame['producturl'].tolist(), [url, second_url])
        self.assertTrue(pd.isna(frame.iloc[0]['title']))
        self.assertEqual(frame.iloc[1]['title'], 'Second SSD')
        self.assertEqual([row['retailersku'] for row in saved], ['first', 'second'])
        self.assertEqual(obj.driver.visits, [url] * 4 + [second_url])
        obj.driver.quit.assert_called_once()


if __name__ == '__main__':
    unittest.main()

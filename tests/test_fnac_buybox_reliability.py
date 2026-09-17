"""Independent page evidence: live-offer stock, new layout prices and NULL policy."""
import json
import sys
import tempfile
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

import test_fnac_auto_recovery as fixtures
from test_fnac_finalization import ROW, URL, OOS, STORE_ONLY


def buybox(price='181,99', availability='En stock en ligne', extra=''):
    return ('<div class="BuyBox-module__buyBoxMainContainer">'
            '<p class="PricingUI-module__pricingLabelMain">' + price + ' €</p>'
            '<span class="ProductAvailability-module__status">' + availability + '</span>'
            + extra + '</div>')


def product(body):
    return '<h1>Crucial SSD</h1><script type="application/ld+json">' + json.dumps({
        '@type': 'Product', 'name': 'Crucial SSD', 'image': 'https://static.fnac-static.com/example.jpg'
    }) + '</script>' + body


class BuyboxReliabilityTests(unittest.TestCase):
    def setUp(self):
        self.case = fixtures.FnacRecoveryTests()
        self.case.setUp()
        self.s, self.m = self.case.scraper, self.case.m

    def parse(self, body):
        return self.s.parse_product(product(body), ROW)

    def test_in_stock_is_not_overridden_by_translations_and_scripts(self):
        body = buybox() + '<script>const labels={soldOut:"Stock en ligne épuisé",since:"depuis"}</script>'
        row, reason = self.parse(body)
        self.assertEqual((row['retailprice'], reason), (181.99, 'VISIBLE_PRICE_BOX'))

    def test_old_stock_scope_does_not_include_neighboring_sold_out_offer(self):
        body = fixtures.PRODUCT + '<div>Stock en ligne épuisé</div>'
        row, reason = self.s.parse_product(body, ROW)
        self.assertEqual((row['retailprice'], reason), (123.45, 'VISIBLE_PRICE_BOX'))

    def test_true_online_out_of_stock_overrides_price(self):
        row, reason = self.parse(buybox(availability='Stock en ligne <strong>épuisé</strong>'))
        self.assertEqual(reason, 'ONLINE_STOCK_EXHAUSTED')
        self.assertIsNone(row['retailprice'])

    def test_partner_stock_price_is_collected(self):
        row, reason = self.parse(buybox('256,99', 'En stock vendeur partenaire'))
        self.assertEqual((row['retailprice'], reason), (256.99, 'VISIBLE_PRICE_BOX'))

    def test_other_offer_stock_does_not_override_main_stock(self):
        row, _ = self.parse(buybox() + '<aside class="ProductAvailability-module__status">Stock en ligne épuisé</aside>')
        self.assertEqual(row['retailprice'], 181.99)

    def test_old_markup_outside_new_buybox_cannot_mark_it_sold_out(self):
        row, _ = self.parse(buybox() + '<aside data-automation-id="product-availability">Stock en ligne épuisé</aside>')
        self.assertEqual(row['retailprice'], 181.99)

    def test_crossed_out_installment_and_other_offer_prices_are_excluded(self):
        body = buybox('322,48', extra='<del>341,98 €</del><p>4 x 80,62 €</p>')
        body += '<h2>Autres offres</h2><p class="PricingUI-module__pricingLabelMain">199 €</p>'
        self.assertEqual(self.parse(body)[0]['retailprice'], 322.48)

    def test_hidden_template_and_old_mobile_box_are_excluded(self):
        for wrapper in ('<template>{}</template>', '<div hidden>{}</div>',
                        '<div style="display: none">{}</div>', '<div aria-hidden="true">{}</div>'):
            row, _ = self.parse(buybox() + wrapper.format(buybox('9,99', 'Stock en ligne épuisé')))
            self.assertEqual(row['retailprice'], 181.99)

    def test_desktop_and_mobile_identical_prices_do_not_conflict(self):
        self.assertEqual(self.parse(buybox() + buybox())[0]['retailprice'], 181.99)

    def test_conflicting_prices_require_verification(self):
        body = product(buybox('181,99') + buybox('201,99'))
        row, reason = self.s.parse_product(body, ROW)
        self.assertIsNone(row['retailprice'])
        self.assertEqual(reason, 'PRICE_NOT_FOUND')
        self.assertTrue(self.s.should_browser_verify(body, ROW, row, reason))

    def test_conflicting_availability_is_not_confirmed_policy_null(self):
        body = product(buybox() + buybox(availability='Stock en ligne épuisé'))
        row, reason = self.s.parse_product(body, ROW)
        self.assertEqual(reason, 'PRICE_NOT_FOUND')
        self.assertFalse(self.m.is_confirmed_null_reason(reason))

    def test_store_and_non_new_policy_still_outrank_new_layout_price(self):
        row, reason = self.s.parse_product(STORE_ONLY + buybox(), ROW)
        self.assertIsNone(row['retailprice'])
        self.assertEqual(reason, 'BASE_CLICK_COLLECT_FIRST_MARKETPLACE')
        metadata = '<script id="digitalData">' + json.dumps({'product': {'attributes': {
            'currentOffer': {'condition': 'Used'}}}}) + '</script>'
        row, reason = self.parse(buybox() + metadata)
        self.assertIsNone(row['retailprice'])
        self.assertEqual(reason, 'NON_NEW_OFFER_IGNORED')

    def test_ready_product_and_browser_price_support_new_layout(self):
        body = product(buybox())
        self.assertTrue(self.s.product_html_ready(body, URL))
        page = Mock()
        page.evaluate.return_value = {'visiblePriceTexts': ['181,99 €']}
        self.assertEqual(self.s.screenshot_visible_price(page, body), 181.99)
        self.assertIn('pricingLabelMain', page.evaluate.call_args.args[0])

    def test_normal_next_product_uses_one_api_and_no_photo_browser(self):
        self.s.fetch_html = Mock(return_value=(200, product(buybox()), None, 0))
        self.s.fetch_verified_html = Mock()
        result = self.s.collect_one(ROW)
        self.assertEqual(result['retailprice'], 181.99)
        self.assertEqual(result['_collection_status'], 'complete')
        self.assertEqual(result['_s3_upload'], 'skip')
        self.s.fetch_verified_html.assert_not_called()
        self.m.capture_and_upload.assert_not_called()

    def test_missing_price_is_reported_as_collection_failure_even_with_photo(self):
        body = product(buybox(''))
        self.s.fetch_html = Mock(return_value=(200, body, None, 0))
        self.s.fetch_verified_html = Mock(return_value=(200, body, 0))
        self.s.capture_null_screenshot = Mock(return_value='ok')
        result = self.s.collect_one(ROW)
        self.assertEqual(result['_collection_status'], 'collection_failed')
        self.assertFalse(self.m.is_confirmed_null_reason(result['_crawl_reason']))
        self.assertTrue(any('collection incomplete' in entry for entry in self.s.error_logs))

    def test_snapshot_records_capture_failure_without_changing_price_policy(self):
        with tempfile.TemporaryDirectory() as directory:
            self.s.save_html_dir = Path(directory)
            self.s.fetch_html = Mock(return_value=(200, OOS, None, 0))
            fake, _, *_ = self.case.screenshot_context('restricted')
            self.s.pause = Mock()
            with patch.dict(sys.modules, {'playwright.sync_api': fake}):
                row = self.s.collect_one(ROW)
            snapshot = json.loads((Path(directory) / 'synthetic.result.json').read_text(encoding='utf-8'))
            self.assertEqual(snapshot['diagnostics']['_s3_upload'], 'fail')
            self.assertEqual(snapshot['diagnostics']['_screenshot_reason'], 'restricted')
            self.assertEqual(snapshot['diagnostics']['_collection_status'], 'policy_null')
            self.assertIsNone(row['retailprice'])

    def test_monitoring_link_failure_does_not_buy_another_browser(self):
        fake, pw, page, *_ = self.case.screenshot_context('ready')
        result = dict(ROW, title='SSD', imageurl='image', retailprice=None, _crawl_reason='ONLINE_STOCK_EXHAUSTED')
        def upload(*args, **kwargs):
            self.assertTrue(kwargs['require_monitoring_link'])
            args[4]['_screenshot_reason'] = 'monitoring_link_failed'
        self.m.capture_and_upload.side_effect = upload
        with patch.dict(sys.modules, {'playwright.sync_api': fake}):
            self.assertEqual(self.s.capture_null_screenshot(result, URL), 'fail')
        self.assertEqual(pw.chromium.connect_over_cdp.call_count, 1)
        self.assertEqual(result['_screenshot_reason'], 'monitoring_link_failed')

    def test_homepage_preparation_has_bounded_timeout(self):
        page = Mock()
        self.s.pause = Mock()
        self.s.prepare_playwright_page_for_capture = Mock()
        self.s.accept_cookie_popup = Mock()
        self.s.warmup_fnac_screenshot_session(page)
        self.assertEqual(page.goto.call_args.kwargs['timeout'], 20000)

    def visibility_case(self, visible_stock='En stock en ligne'):
        visible = buybox('181,99', visible_stock)
        hidden = buybox('201,99')
        raw = product(visible + hidden)
        filtered = '<html data-fnac-rendered-verification="complete" data-fnac-visibility-filtered="true">' + product(
            visible + hidden.replace('<div ', '<div data-fnac-not-visible="true" ', 1)) + '</html>'
        return raw, filtered

    def test_css_price_conflict_recovers_before_screenshot_upload(self):
        raw, filtered = self.visibility_case()
        row, reason = self.s.parse_product(raw, ROW)
        row['_crawl_reason'] = reason
        page = Mock()
        page.content.return_value = raw
        page.evaluate.side_effect = lambda script: filtered if 'const snapshot =' in script else {'visiblePriceTexts': ['181,99']}
        self.assertEqual(self.s.capture_ready_page(page, row, URL, URL), 'skip')
        self.assertEqual(row['retailprice'], 181.99)
        self.m.capture_and_upload.assert_not_called()
        self.assertEqual(row['_browser_reparse_html'], filtered)

    def test_visible_stock_and_price_are_filtered_together(self):
        _, filtered = self.visibility_case('Stock en ligne épuisé')
        row, reason = self.s.parse_product(filtered, ROW)
        self.assertIsNone(row['retailprice'])
        self.assertEqual(reason, 'ONLINE_STOCK_EXHAUSTED')

    def test_visible_recovery_does_not_override_confirmed_null_policy(self):
        raw, filtered = self.visibility_case()
        row = dict(ROW, title='SSD', imageurl='image', retailprice=None,
                   _crawl_reason='BASE_CLICK_COLLECT_FIRST_MARKETPLACE')
        page = Mock()
        page.content.return_value = raw
        page.evaluate.return_value = filtered
        self.assertEqual(self.s.capture_ready_page(page, row, URL, URL), 'ok')
        self.assertIsNone(row['retailprice'])
        self.m.capture_and_upload.assert_called_once()

    def test_auto_verification_resolves_conflict_without_another_browser(self):
        raw, filtered = self.visibility_case()
        self.s.fetch_html = Mock(side_effect=[(200, raw, None, 0), (200, filtered, None, 0)])
        self.s.fetch_browser_html = Mock()
        row = self.s.collect_one(ROW)
        self.assertEqual(row['retailprice'], 181.99)
        self.assertEqual(self.s.fetch_html.call_count, 2)
        self.s.fetch_browser_html.assert_not_called()
        self.m.capture_and_upload.assert_not_called()

    def test_browser_fallback_resolves_conflict_before_ready_check(self):
        raw, filtered = self.visibility_case()
        fake, pw, page, _, browser = self.case.screenshot_context('ready')
        page.goto = Mock(return_value=type('Response', (), {'status': 200})())
        page.content.return_value = raw
        page.evaluate = Mock(return_value=filtered)
        self.s.wait_for_product_html = Mock(return_value=raw)
        with patch.dict(sys.modules, {'playwright.sync_api': fake}):
            status, body, _ = self.s.fetch_browser_html(URL)
        self.assertEqual(status, 200)
        self.assertEqual(self.s.parse_product(body, ROW)[0]['retailprice'], 181.99)
        self.assertEqual(pw.chromium.connect_over_cdp.call_count, 1)
        browser.close.assert_called_once()

    def test_visibility_evaluation_failure_does_not_guess_price(self):
        raw, _ = self.visibility_case()
        page = Mock()
        page.content.return_value = raw
        page.evaluate.side_effect = RuntimeError('synthetic')
        body = self.s.browser_product_html(page)
        self.assertEqual(body, raw)
        self.assertIsNone(self.s.parse_product(body, ROW)[0]['retailprice'])

    def test_legacy_browser_page_does_not_add_visibility_evaluation(self):
        page = Mock()
        page.content.return_value = fixtures.PRODUCT
        self.assertEqual(self.s.browser_product_html(page), fixtures.PRODUCT)
        page.evaluate.assert_not_called()


if __name__ == '__main__':
    unittest.main()

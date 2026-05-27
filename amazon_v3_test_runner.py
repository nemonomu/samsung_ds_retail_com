import importlib
import os
import re
import shutil
import sys
from datetime import datetime

import pandas as pd
import pytz
from selenium.webdriver.common.by import By


SEOUL_TZ = pytz.timezone('Asia/Seoul')


OFFER_SOURCE_SECTION_IDS = [
    'merchantInfoFeature_feature_div',
    'fulfillerInfoFeature_feature_div',
    'usedOnlyLayoutMerchantInfoFeature_feature_div',
    'usedOnlyLayoutFulfillerInfoFeature_feature_div',
    'shipsFromSoldBy_feature_div',
    'shipFromSoldByAbbreviated_feature_div',
    'shipsFromSoldByAbbreviatedPSUFeature_feature_div',
    'sfsbFallbackExpanded_feature_div',
]


COMMON_COMBINED_LABEL_MARKERS = [
    'shipper / seller',
    'shipper/seller',
]


MERCHANT_COMBINED_SECTION_IDS = [
    'merchantInfoFeature_feature_div',
    'usedOnlyLayoutMerchantInfoFeature_feature_div',
    'shipsFromSoldBy_feature_div',
    'shipFromSoldByAbbreviated_feature_div',
    'shipsFromSoldByAbbreviatedPSUFeature_feature_div',
    'sfsbFallbackExpanded_feature_div',
]


FULFILLER_SECTION_IDS = [
    'fulfillerInfoFeature_feature_div',
    'usedOnlyLayoutFulfillerInfoFeature_feature_div',
]


COMMON_PRICE_SELECTORS = [
    "//*[@id='centerCol']//*[@id='corePriceDisplay_desktop_feature_div']//span[contains(@class,'a-price') and not(contains(@class,'a-text-price'))]",
    "//*[@id='centerCol']//*[@id='corePrice_feature_div']//span[contains(@class,'a-price') and not(contains(@class,'a-text-price'))]",
    "//*[@id='centerCol']//*[@id='corePriceDisplay_desktop_feature_div']//span[@class='a-offscreen' and normalize-space(.)!='']",
    "//*[@id='centerCol']//*[@id='corePrice_feature_div']//span[@class='a-offscreen' and normalize-space(.)!='']",
]


COUNTRY_CONFIGS = {
    'gb': {
        'module': 'uk_v2',
        'class': 'AmazonUKScraper',
        'display': 'GB',
        'selector_mode': 'flat',
        'target_method': 'get_uk_crawl_targets',
        'log_name': 'gb_amazon_v3',
        'test_urls': [
            'https://www.amazon.co.uk/dp/B003J5JB12?th=1',
        ],
        'selectors': {
            'ships_from': [
                "//div[@id='fulfillerInfoFeature_feature_div']//span[contains(@class,'offer-display-feature-text-message') and normalize-space(.)!='']",
                "//div[@id='usedOnlyLayoutFulfillerInfoFeature_feature_div']//span[contains(@class,'offer-display-feature-text-message') and normalize-space(.)!='']",
                "//div[@id='merchantInfoFeature_feature_div'][.//div[contains(@class,'offer-display-feature-label')][contains(normalize-space(.),'Shipper') and contains(normalize-space(.),'Seller')]]//a[@id='sellerProfileTriggerId' and normalize-space(.)!='']",
                "//div[@id='usedOnlyLayoutMerchantInfoFeature_feature_div'][.//div[contains(@class,'offer-display-feature-label')][contains(normalize-space(.),'Shipper') and contains(normalize-space(.),'Seller')]]//a[@id='sellerProfileTriggerId' and normalize-space(.)!='']",
            ],
            'sold_by': [
                "//div[@id='merchantInfoFeature_feature_div']//a[@id='sellerProfileTriggerId' and normalize-space(.)!='']",
                "//div[@id='usedOnlyLayoutMerchantInfoFeature_feature_div']//a[@id='sellerProfileTriggerId' and normalize-space(.)!='']",
                "//div[@id='merchantInfoFeature_feature_div']//span[contains(@class,'offer-display-feature-text-message') and normalize-space(.)!='']",
                "//div[@id='usedOnlyLayoutMerchantInfoFeature_feature_div']//span[contains(@class,'offer-display-feature-text-message') and normalize-space(.)!='']",
            ],
            'price': COMMON_PRICE_SELECTORS,
        },
    },
    'it': {
        'module': 'it_v2',
        'class': 'AmazonITScraper',
        'display': 'IT',
        'selector_mode': 'flat',
        'target_method': 'get_crawl_targets',
        'log_name': 'it_amazon_v3',
        'test_urls': [
            'https://www.amazon.it/dp/B0F3BMBQ75?th=1',
            'https://www.amazon.it/dp/B09QV5KJHV?th=1',
        ],
        'selectors': {
            'ships_from': [
                "//div[@id='fulfillerInfoFeature_feature_div']//span[contains(@class,'offer-display-feature-text-message') and normalize-space(.)!='']",
                "//div[@id='usedOnlyLayoutFulfillerInfoFeature_feature_div']//span[contains(@class,'offer-display-feature-text-message') and normalize-space(.)!='']",
                "//div[@id='merchantInfoFeature_feature_div'][.//div[contains(@class,'offer-display-feature-label')][(contains(normalize-space(.),'Sped') or contains(normalize-space(.),'Mittente') or contains(normalize-space(.),'Shipper')) and (contains(normalize-space(.),'Vend') or contains(normalize-space(.),'Seller'))]]//a[@id='sellerProfileTriggerId' and normalize-space(.)!='']",
                "//div[@id='usedOnlyLayoutMerchantInfoFeature_feature_div'][.//div[contains(@class,'offer-display-feature-label')][(contains(normalize-space(.),'Sped') or contains(normalize-space(.),'Mittente') or contains(normalize-space(.),'Shipper')) and (contains(normalize-space(.),'Vend') or contains(normalize-space(.),'Seller'))]]//a[@id='sellerProfileTriggerId' and normalize-space(.)!='']",
            ],
            'sold_by': [
                "//div[@id='merchantInfoFeature_feature_div']//a[@id='sellerProfileTriggerId' and normalize-space(.)!='']",
                "//div[@id='usedOnlyLayoutMerchantInfoFeature_feature_div']//a[@id='sellerProfileTriggerId' and normalize-space(.)!='']",
                "//div[@id='merchantInfoFeature_feature_div']//span[contains(@class,'offer-display-feature-text-message') and normalize-space(.)!='']",
                "//div[@id='usedOnlyLayoutMerchantInfoFeature_feature_div']//span[contains(@class,'offer-display-feature-text-message') and normalize-space(.)!='']",
            ],
            'price': COMMON_PRICE_SELECTORS,
        },
    },
    'es': {
        'module': 'es_v2',
        'class': 'AmazonScraper',
        'display': 'ES',
        'selector_mode': 'nested',
        'target_method': 'get_crawl_targets',
        'log_name': 'es_amazon_v3',
        'init_country_code': True,
        'test_urls': [
            'https://www.amazon.es/dp/B0CTRVZKG7?th=1',
        ],
        'selectors': {
            'ships_from': [
                "//div[@id='fulfillerInfoFeature_feature_div']//span[contains(@class,'offer-display-feature-text-message') and normalize-space(.)!='']",
                "//div[@id='usedOnlyLayoutFulfillerInfoFeature_feature_div']//span[contains(@class,'offer-display-feature-text-message') and normalize-space(.)!='']",
                "//div[@id='merchantInfoFeature_feature_div'][.//div[contains(@class,'offer-display-feature-label')][(contains(normalize-space(.),'Remitente') or contains(normalize-space(.),'Enviado') or contains(normalize-space(.),'Shipper')) and (contains(normalize-space(.),'Vendedor') or contains(normalize-space(.),'Vendido') or contains(normalize-space(.),'Seller'))]]//a[@id='sellerProfileTriggerId' and normalize-space(.)!='']",
                "//div[@id='usedOnlyLayoutMerchantInfoFeature_feature_div'][.//div[contains(@class,'offer-display-feature-label')][(contains(normalize-space(.),'Remitente') or contains(normalize-space(.),'Enviado') or contains(normalize-space(.),'Shipper')) and (contains(normalize-space(.),'Vendedor') or contains(normalize-space(.),'Vendido') or contains(normalize-space(.),'Seller'))]]//a[@id='sellerProfileTriggerId' and normalize-space(.)!='']",
            ],
            'sold_by': [
                "//div[@id='merchantInfoFeature_feature_div']//a[@id='sellerProfileTriggerId' and normalize-space(.)!='']",
                "//div[@id='usedOnlyLayoutMerchantInfoFeature_feature_div']//a[@id='sellerProfileTriggerId' and normalize-space(.)!='']",
                "//div[@id='merchantInfoFeature_feature_div']//span[contains(@class,'offer-display-feature-text-message') and normalize-space(.)!='']",
                "//div[@id='usedOnlyLayoutMerchantInfoFeature_feature_div']//span[contains(@class,'offer-display-feature-text-message') and normalize-space(.)!='']",
            ],
            'price': COMMON_PRICE_SELECTORS,
        },
    },
    'jp': {
        'module': 'jp_v2',
        'class': 'AmazonScraper',
        'display': 'JP',
        'selector_mode': 'nested',
        'target_method': 'get_crawl_targets',
        'log_name': 'jp_amazon_v3',
        'init_country_code': True,
        'test_urls': [
            'https://www.amazon.co.jp/dp/B07KCKG77L?th=1',
        ],
        'selectors': {
            'ships_from': [
                "//div[@id='fulfillerInfoFeature_feature_div']//span[contains(@class,'offer-display-feature-text-message') and normalize-space(.)!='']",
                "//div[@id='usedOnlyLayoutFulfillerInfoFeature_feature_div']//span[contains(@class,'offer-display-feature-text-message') and normalize-space(.)!='']",
                "//div[@id='merchantInfoFeature_feature_div'][.//div[contains(@class,'offer-display-feature-label')][(contains(normalize-space(.),'出荷元') or contains(normalize-space(.),'発送元') or contains(normalize-space(.),'Shipper')) and (contains(normalize-space(.),'販売元') or contains(normalize-space(.),'Seller'))]]//a[@id='sellerProfileTriggerId' and normalize-space(.)!='']",
                "//div[@id='usedOnlyLayoutMerchantInfoFeature_feature_div'][.//div[contains(@class,'offer-display-feature-label')][(contains(normalize-space(.),'出荷元') or contains(normalize-space(.),'発送元') or contains(normalize-space(.),'Shipper')) and (contains(normalize-space(.),'販売元') or contains(normalize-space(.),'Seller'))]]//a[@id='sellerProfileTriggerId' and normalize-space(.)!='']",
            ],
            'sold_by': [
                "//div[@id='merchantInfoFeature_feature_div']//a[@id='sellerProfileTriggerId' and normalize-space(.)!='']",
                "//div[@id='usedOnlyLayoutMerchantInfoFeature_feature_div']//a[@id='sellerProfileTriggerId' and normalize-space(.)!='']",
                "//div[@id='merchantInfoFeature_feature_div']//span[contains(@class,'offer-display-feature-text-message') and normalize-space(.)!='']",
                "//div[@id='usedOnlyLayoutMerchantInfoFeature_feature_div']//span[contains(@class,'offer-display-feature-text-message') and normalize-space(.)!='']",
            ],
            'price': COMMON_PRICE_SELECTORS,
        },
    },
    'usa': {
        'module': 'usa_v2',
        'class': 'AmazonScraper',
        'display': 'USA',
        'selector_mode': 'nested',
        'target_method': 'get_crawl_targets',
        'log_name': 'usa_amazon_v3',
        'init_country_code': True,
        'test_urls': [
            'https://www.amazon.com/dp/B0CTRVZKG7?th=1',
            'https://www.amazon.com/dp/B0CX5C3LBQ?th=1',
        ],
        'selectors': {
            'ships_from': [
                "//div[@id='fulfillerInfoFeature_feature_div']//span[contains(@class,'offer-display-feature-text-message') and normalize-space(.)!='']",
                "//div[@id='usedOnlyLayoutFulfillerInfoFeature_feature_div']//span[contains(@class,'offer-display-feature-text-message') and normalize-space(.)!='']",
                "//div[@id='merchantInfoFeature_feature_div'][.//div[contains(@class,'offer-display-feature-label')][contains(normalize-space(.),'Shipper') and contains(normalize-space(.),'Seller')]]//a[@id='sellerProfileTriggerId' and normalize-space(.)!='']",
                "//div[@id='usedOnlyLayoutMerchantInfoFeature_feature_div'][.//div[contains(@class,'offer-display-feature-label')][contains(normalize-space(.),'Shipper') and contains(normalize-space(.),'Seller')]]//a[@id='sellerProfileTriggerId' and normalize-space(.)!='']",
            ],
            'sold_by': [
                "//div[@id='merchantInfoFeature_feature_div']//a[@id='sellerProfileTriggerId' and normalize-space(.)!='']",
                "//div[@id='usedOnlyLayoutMerchantInfoFeature_feature_div']//a[@id='sellerProfileTriggerId' and normalize-space(.)!='']",
                "//div[@id='merchantInfoFeature_feature_div']//span[contains(@class,'offer-display-feature-text-message') and normalize-space(.)!='']",
                "//div[@id='usedOnlyLayoutMerchantInfoFeature_feature_div']//span[contains(@class,'offer-display-feature-text-message') and normalize-space(.)!='']",
            ],
            'price': COMMON_PRICE_SELECTORS,
        },
    },
    'fr': {
        'module': 'fr_v2',
        'class': 'AmazonFRScraper',
        'display': 'FR',
        'selector_mode': 'flat',
        'target_method': 'get_crawl_targets',
        'log_name': 'fr_amazon_v3',
        'test_urls': [
            'https://www.amazon.fr/dp/B0CTRVZKG7?th=1',
            'https://www.amazon.fr/dp/B0CX5C3LBQ?th=1',
        ],
        'selectors': {
            'ships_from': [
                "//div[@id='fulfillerInfoFeature_feature_div']//span[contains(@class,'offer-display-feature-text-message') and normalize-space(.)!='']",
                "//div[@id='usedOnlyLayoutFulfillerInfoFeature_feature_div']//span[contains(@class,'offer-display-feature-text-message') and normalize-space(.)!='']",
                "//div[@id='merchantInfoFeature_feature_div'][.//div[contains(@class,'offer-display-feature-label')][(contains(normalize-space(.),'Exp') or contains(normalize-space(.),'Shipper')) and (contains(normalize-space(.),'Vend') or contains(normalize-space(.),'Seller'))]]//a[@id='sellerProfileTriggerId' and normalize-space(.)!='']",
                "//div[@id='usedOnlyLayoutMerchantInfoFeature_feature_div'][.//div[contains(@class,'offer-display-feature-label')][(contains(normalize-space(.),'Exp') or contains(normalize-space(.),'Shipper')) and (contains(normalize-space(.),'Vend') or contains(normalize-space(.),'Seller'))]]//a[@id='sellerProfileTriggerId' and normalize-space(.)!='']",
            ],
            'sold_by': [
                "//div[@id='merchantInfoFeature_feature_div']//a[@id='sellerProfileTriggerId' and normalize-space(.)!='']",
                "//div[@id='usedOnlyLayoutMerchantInfoFeature_feature_div']//a[@id='sellerProfileTriggerId' and normalize-space(.)!='']",
                "//div[@id='merchantInfoFeature_feature_div']//span[contains(@class,'offer-display-feature-text-message') and normalize-space(.)!='']",
                "//div[@id='usedOnlyLayoutMerchantInfoFeature_feature_div']//span[contains(@class,'offer-display-feature-text-message') and normalize-space(.)!='']",
            ],
            'price': COMMON_PRICE_SELECTORS,
        },
    },
    'in': {
        'module': 'in_v2',
        'class': 'AmazonIndiaScraper',
        'display': 'IN',
        'selector_mode': 'nested',
        'target_method': 'get_crawl_targets',
        'log_name': 'in_amazon_v3',
        'test_urls': [
            'https://www.amazon.in/dp/B0CTRVZKG7?th=1',
            'https://www.amazon.in/dp/B0CX5C3LBQ?th=1',
        ],
        'selectors': {
            'ships_from': [
                "//div[@id='fulfillerInfoFeature_feature_div']//span[contains(@class,'offer-display-feature-text-message') and normalize-space(.)!='']",
                "//div[@id='usedOnlyLayoutFulfillerInfoFeature_feature_div']//span[contains(@class,'offer-display-feature-text-message') and normalize-space(.)!='']",
                "//div[@id='merchantInfoFeature_feature_div'][.//div[contains(@class,'offer-display-feature-label')][contains(normalize-space(.),'Shipper') and contains(normalize-space(.),'Seller')]]//a[@id='sellerProfileTriggerId' and normalize-space(.)!='']",
                "//div[@id='usedOnlyLayoutMerchantInfoFeature_feature_div'][.//div[contains(@class,'offer-display-feature-label')][contains(normalize-space(.),'Shipper') and contains(normalize-space(.),'Seller')]]//a[@id='sellerProfileTriggerId' and normalize-space(.)!='']",
            ],
            'sold_by': [
                "//div[@id='merchantInfoFeature_feature_div']//a[@id='sellerProfileTriggerId' and normalize-space(.)!='']",
                "//div[@id='usedOnlyLayoutMerchantInfoFeature_feature_div']//a[@id='sellerProfileTriggerId' and normalize-space(.)!='']",
                "//div[@id='merchantInfoFeature_feature_div']//span[contains(@class,'offer-display-feature-text-message') and normalize-space(.)!='']",
                "//div[@id='usedOnlyLayoutMerchantInfoFeature_feature_div']//span[contains(@class,'offer-display-feature-text-message') and normalize-space(.)!='']",
            ],
            'price': COMMON_PRICE_SELECTORS,
        },
    },
}


def create_output_dir(country_code):
    prefix = country_code.upper()
    explicit_dir = (
        os.getenv(f'{prefix}_V3_RUN_DIR')
        or os.getenv(f'{prefix}_V3_OUTPUT_DIR')
        or os.getenv('AMAZON_V3_RUN_DIR')
        or os.getenv('AMAZON_V3_OUTPUT_DIR')
    )

    if explicit_dir:
        output_dir = explicit_dir
    else:
        timestamp = datetime.now(SEOUL_TZ).strftime('%Y%m%d%H%M%S')
        output_dir = os.path.join(
            os.path.dirname(__file__),
            'references',
            'verifying',
            f'{country_code}_v3_run_{timestamp}',
        )

    os.makedirs(output_dir, exist_ok=True)
    print(f'{prefix}_V3_OUTPUT_DIR={output_dir}')
    return output_dir


def disable_external_uploads(module, country_code):
    prefix = country_code.upper()
    if os.getenv(f'{prefix}_V3_ALLOW_UPLOAD', 'false').lower() == 'true':
        return

    def noop_capture_and_upload(*args, **kwargs):
        module.logger.info(f'{prefix} V3 external upload skipped')
        return None

    if hasattr(module, 'capture_and_upload'):
        module.capture_and_upload = noop_capture_and_upload
        module.logger.info(f'{prefix} V3 external uploads disabled')


def selector_bucket(scraper, cfg, country_code):
    if cfg['selector_mode'] == 'nested':
        return scraper.selectors.setdefault(country_code, {})
    return scraper.selectors


def apply_selector_overrides(scraper, cfg, country_code):
    bucket = selector_bucket(scraper, cfg, country_code)
    for element_type, candidates in cfg['selectors'].items():
        existing = bucket.setdefault(element_type, [])
        if element_type == 'ships_from':
            bucket[element_type] = candidates
        else:
            bucket[element_type] = candidates + [s for s in existing if s not in candidates]


def make_test_data(country_code, cfg):
    return [
        {
            'url': url,
            'brand': '',
            'item': f"{cfg['display']} V3 Test {idx + 1}",
            'retailerid': f"{country_code.upper()}_V3_{idx + 1:03d}",
            'retailersku': f"{country_code.upper()}_V3_{idx + 1:03d}",
            'channel': 'Online',
            'seg_lv1': '',
            'seg_lv2': '',
            'seg_lv3': '',
            'capacity': '',
            'form_factor': '',
            'vat': 'o',
        }
        for idx, url in enumerate(cfg['test_urls'])
    ]


def save_debug_html(scraper, output_dir, country_code, url, row_data, reason):
    try:
        page_source = scraper.driver.page_source if scraper.driver else ''
    except Exception:
        page_source = ''

    if not page_source:
        return

    sku = row_data.get('retailersku') or row_data.get('retailerid') or ''
    if not sku:
        match = re.search(r'/dp/([A-Z0-9]{10})', url or '', re.IGNORECASE)
        sku = match.group(1).upper() if match else 'unknown'

    timestamp = datetime.now(SEOUL_TZ).strftime('%Y%m%d%H%M%S')
    safe_reason = re.sub(r'[^A-Za-z0-9_-]+', '_', reason).strip('_') or 'debug'
    html_path = os.path.join(output_dir, f'{country_code}_v3_{sku}_{safe_reason}_{timestamp}.html')
    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(page_source)


def get_offer_section_texts(scraper):
    section_texts = {}
    driver = getattr(scraper, 'driver', None)
    if driver is None:
        return section_texts

    for section_id in OFFER_SOURCE_SECTION_IDS:
        texts = []
        try:
            elements = driver.find_elements(By.ID, section_id)
            for element in elements:
                text = (element.text or element.get_attribute('textContent') or '').strip()
                if text:
                    texts.append(text)
        except Exception:
            pass
        section_texts[section_id] = ' '.join(texts).casefold()

    return section_texts


def has_combined_shipper_seller_label(text, country_code):
    if not text:
        return False

    if any(marker in text for marker in COMMON_COMBINED_LABEL_MARKERS):
        return True

    if country_code == 'it':
        return ('sped' in text or 'mittente' in text) and ('vend' in text or 'seller' in text)
    if country_code == 'es':
        return ('remitente' in text or 'enviado' in text or 'shipper' in text) and (
            'vendedor' in text or 'vendido' in text or 'seller' in text
        )
    if country_code == 'fr':
        return ('exp' in text or 'shipper' in text) and ('vend' in text or 'seller' in text)
    if country_code == 'jp':
        return (('\u51fa\u8377\u5143' in text or '\u767a\u9001\u5143' in text) and '\u8ca9\u58f2\u5143' in text)

    return False


def has_explicit_ship_from_source(scraper, country_code, ships_from=None):
    section_texts = get_offer_section_texts(scraper)
    ships_from_cf = (ships_from or '').strip().casefold()

    for section_id in MERCHANT_COMBINED_SECTION_IDS:
        if has_combined_shipper_seller_label(section_texts.get(section_id, ''), country_code):
            return True

    if not ships_from_cf:
        return False

    for section_id in FULFILLER_SECTION_IDS:
        text = section_texts.get(section_id, '')
        if text and ships_from_cf in text:
            return True

    return False


def clear_seller_only_ship_from(scraper, result, country_code, module):
    ships_from = (result.get('ships_from') or '').strip()
    sold_by = (result.get('sold_by') or '').strip()

    if not ships_from or not sold_by:
        return

    if ships_from.casefold() != sold_by.casefold():
        return

    if has_explicit_ship_from_source(scraper, country_code, ships_from):
        return

    result['ships_from'] = None
    module.logger.info(
        f"{country_code.upper()} V3 ships_from cleared because page exposes seller only: "
        f"ships_from={ships_from}, sold_by={sold_by}"
    )


def wrap_extract_for_debug(scraper, output_dir, country_code):
    cfg = COUNTRY_CONFIGS[country_code]
    module = importlib.import_module(cfg['module'])
    original_extract = scraper.extract_product_info

    def wrapped_extract(url, row_data, *args, **kwargs):
        result = original_extract(url, row_data, *args, **kwargs)
        if not result:
            save_debug_html(scraper, output_dir, country_code, url, row_data, 'empty_result')
            return result

        clear_seller_only_ship_from(scraper, result, country_code, module)

        if not result.get('title') or (not result.get('ships_from') and not result.get('sold_by')):
            save_debug_html(scraper, output_dir, country_code, url, row_data, 'null_fields')

        return result

    scraper.extract_product_info = wrapped_extract


def disable_result_db_writes(scraper, module, country_code):
    prefix = country_code.upper()
    scraper.db_engine = None

    def skipped_save_to_db(df):
        module.logger.info(f'{prefix} V3 save_to_db skipped')
        return False

    scraper.save_to_db = skipped_save_to_db
    module.logger.info(f'{prefix} V3 result DB writes disabled')


def save_local_results(df, output_dir, country_code):
    timestamp = datetime.now(SEOUL_TZ).strftime('%Y%m%d%H%M%S')
    output_path = os.path.join(output_dir, f'{country_code}_v3_results_{timestamp}.csv')
    df.to_csv(output_path, index=False, encoding='utf-8-sig', lineterminator='\r\n')
    print(f'{country_code.upper()}_V3_RESULT_CSV={output_path}')
    return output_path


def copy_latest_log(output_dir, log_name):
    log_path = os.path.join(os.path.dirname(__file__), 'logs', f'{log_name}_latest.log')
    if not os.path.exists(log_path):
        return

    copied_path = os.path.join(output_dir, os.path.basename(log_path))
    shutil.copy2(log_path, copied_path)
    print(f'{log_name.upper()}_LOG_COPY={copied_path}')


def normalize_scrape_result(scrape_result):
    if isinstance(scrape_result, tuple):
        df = scrape_result[0]
        blocked = scrape_result[1] if len(scrape_result) > 1 else []
        return df, blocked
    return scrape_result, []


def run_country_v3(country_code):
    country_code = country_code.lower()
    if country_code not in COUNTRY_CONFIGS:
        raise ValueError(f'Unsupported country for v3 runner: {country_code}')

    cfg = COUNTRY_CONFIGS[country_code]
    from log_utils import setup_log, save_log

    setup_log(cfg['log_name'])
    output_dir = None

    try:
        test_mode = os.getenv('TEST_MODE', 'false').lower() == 'true'
        max_items = int(os.getenv('MAX_ITEMS', '0')) or None

        print('=' * 60)
        print(f"Amazon {cfg['display']} scraper v3.0 - runtime selector verification")
        print('=' * 60)
        print('DB selector read: ON')
        print('DB target read: ON')
        print('DB result write: OFF')
        print('File/S3 upload: OFF')
        if test_mode:
            print('Mode: TEST URLs')
        if max_items:
            print(f'Max items: {max_items}')
        print('=' * 60)

        module = importlib.import_module(cfg['module'])
        disable_external_uploads(module, country_code)
        output_dir = create_output_dir(country_code)

        scraper_class = getattr(module, cfg['class'])
        if cfg.get('init_country_code'):
            scraper = scraper_class(country_code)
        else:
            scraper = scraper_class()

        apply_selector_overrides(scraper, cfg, country_code)
        wrap_extract_for_debug(scraper, output_dir, country_code)

        if test_mode:
            urls_data = make_test_data(country_code, cfg)
        else:
            if scraper.db_engine is None:
                module.logger.error(f'{country_code.upper()} V3 DB connection failed')
                return
            target_method = getattr(scraper, cfg['target_method'])
            urls_data = target_method(limit=max_items)

        if not urls_data:
            module.logger.warning(f'{country_code.upper()} V3 has no crawl targets')
            return

        disable_result_db_writes(scraper, module, country_code)
        scrape_result = scraper.scrape_urls(urls_data, max_items)
        results_df, blocked_failures = normalize_scrape_result(scrape_result)

        if results_df is None or results_df.empty:
            module.logger.error(f'{country_code.upper()} V3 produced no results')
            return

        scraper.analyze_results(results_df)
        save_local_results(results_df, output_dir, country_code)

        if blocked_failures:
            module.logger.warning(f"{country_code.upper()} V3 blocked/final failures: {len(blocked_failures)}")
    finally:
        save_log(cfg['log_name'])
        if output_dir:
            copy_latest_log(output_dir, cfg['log_name'])


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python amazon_v3_test_runner.py <gb|it|es|jp|usa|fr|in>')
        sys.exit(1)

    run_country_v3(sys.argv[1])

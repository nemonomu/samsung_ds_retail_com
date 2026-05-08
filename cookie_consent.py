"""
쿠키 동의 팝업 자동 수락 헬퍼 (Selenium 전용)
- monitoring/monitoring_capture.py 의 retailer_cookie_selectors 를 재활용
- 페이지 로드 직후 호출해 쿠키 팝업 dismiss

사용 예 (각 크롤러의 extract_product_info 내부, page.get(url) 직후):
    from cookie_consent import accept_cookies
    accept_cookies(self.driver, 'amazon_gb')
"""
import logging

logger = logging.getLogger(__name__)


# 리테일러별 쿠키 동의 버튼 셀렉터 (monitoring_capture.py:334-433 기반)
RETAILER_COOKIE_SELECTORS = {
    'amazon_usa': [
        '#sp-cc-accept',
        'input#sp-cc-accept',
        'input[data-action="sp-cc-accept"]',
        '//input[@name="accept"]',
        '//span[text()="Accept"]/ancestor::button',
    ],
    'amazon_gb': [
        '#sp-cc-accept',
        'input#sp-cc-accept',
        'input[data-action="sp-cc-accept"]',
        '//input[@name="accept"]',
        '//span[text()="Accept"]/ancestor::button',
    ],
    'amazon_jp': [
        '#sp-cc-accept',
        'input#sp-cc-accept',
        '//input[@name="accept"]',
        '//span[text()="同意する"]/ancestor::button',
    ],
    'amazon_in': [
        '#sp-cc-accept',
        'input#sp-cc-accept',
        '//input[@name="accept"]',
        '//span[text()="Accept"]/ancestor::button',
    ],
    'amazon_it': [
        '#sp-cc-accept',
        'input#sp-cc-accept',
        '//input[@name="accept"]',
        '//span[text()="Accetta"]/ancestor::button',
    ],
    'amazon_es': [
        '#sp-cc-accept',
        'input#sp-cc-accept',
        '//input[@name="accept"]',
        '//span[text()="Aceptar"]/ancestor::button',
        '//button[contains(text(), "Aceptar")]',
    ],
    'amazon_fr': [
        '#sp-cc-accept',
        'input#sp-cc-accept',
        '//input[@name="accept"]',
        '//span[text()="Accepter"]/ancestor::button',
        '//button[contains(text(), "Accepter")]',
    ],
    'amazon_de': [
        '#sp-cc-accept',
        'input#sp-cc-accept',
        '//input[@name="accept"]',
        '//span[text()="Annehmen"]/ancestor::button',
        '//button[contains(text(), "Annehmen")]',
    ],
    'amazon_nl': [
        '#sp-cc-accept',
        'input#sp-cc-accept',
        '//input[@name="accept"]',
        '//span[text()="Accepteren"]/ancestor::button',
        '//button[contains(text(), "Accepteren")]',
    ],
    'currys': [
        '#onetrust-accept-btn-handler',
        'button#onetrust-accept-btn-handler',
        "button[aria-label='Accept all cookies']",
        '//button[contains(text(), "Allow all")]',
        '//button[contains(text(), "Accept")]',
    ],
    'bestbuy': [
        '#onetrust-accept-btn-handler',
        'button.accept-all',
    ],
    'centrecom': [
        '#onetrust-accept-btn-handler',
        'button.accept-all',
        '//button[contains(text(), "Accept")]',
    ],
    'danawa': [],
    'fnac': [
        '#onetrust-accept-btn-handler',
        '//button[contains(text(), "J\'accepte")]',
        '//button[contains(text(), "Accepter")]',
        '//button[contains(text(), "Non, merci")]',
        '//a[contains(text(), "Non, merci")]',
        '//span[contains(text(), "Non, merci")]/ancestor::button',
    ],
    'coolblue': [
        '#onetrust-accept-btn-handler',
        'button[name="accept"]',
        '//button[contains(text(), "Oké")]',
    ],
    'mediamarkt': [
        '#pwa-consent-layer-accept-all-button',
        '//button[contains(text(), "Alle akzeptieren")]',
    ],
    'x-kom': [
        '#onetrust-accept-btn-handler',
        '//button[contains(text(), "Akceptuję")]',
    ],
}

# 공통 fallback 셀렉터 (리테일러별 셀렉터가 모두 실패했을 때 시도)
COMMON_COOKIE_SELECTORS = [
    '#sp-cc-accept',
    '#onetrust-accept-btn-handler',
    'button.accept-all',
    '//button[contains(text(), "Accept")]',
    '//button[text()="Accept"]',
    '//span[text()="Accept"]/ancestor::button',
]


def _click_element_selenium(driver, element, selector_desc=""):
    """Selenium 일반 클릭 → 실패 시 JS 클릭 fallback"""
    try:
        element.click()
        return True
    except Exception as e:
        logger.debug(f"일반 클릭 실패 ({selector_desc}): {e}")
    try:
        driver.execute_script("arguments[0].click();", element)
        return True
    except Exception as e:
        logger.debug(f"JS 클릭도 실패 ({selector_desc}): {e}")
    return False


def _accept_cookies_selenium(driver, retailer, selectors):
    """Selenium WebDriver용 쿠키 수락"""
    from selenium.webdriver.common.by import By

    for selector in selectors:
        try:
            if selector.startswith('//') or selector.startswith('/html'):
                elements = driver.find_elements(By.XPATH, selector)
            else:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)

            for elem in elements:
                if elem.is_displayed():
                    if _click_element_selenium(driver, elem, selector):
                        logger.info(f"쿠키 팝업 수락 완료 (retailer={retailer}, selector={selector})")
                        return True
        except Exception as e:
            logger.debug(f"쿠키 셀렉터 실패 ({selector}): {e}")
            continue
    return False


def _accept_cookies_drission(page, retailer, selectors):
    """DrissionPage ChromiumPage용 쿠키 수락"""
    for selector in selectors:
        try:
            if selector.startswith('//') or selector.startswith('/html'):
                elements = page.eles(f'xpath:{selector}', timeout=1)
            else:
                elements = page.eles(f'css:{selector}', timeout=1)

            for elem in elements:
                try:
                    elem.click()
                    logger.info(f"쿠키 팝업 수락 완료 (retailer={retailer}, selector={selector})")
                    return True
                except Exception as e:
                    logger.debug(f"DrissionPage 클릭 실패 ({selector}): {e}")
                    continue
        except Exception as e:
            logger.debug(f"쿠키 셀렉터 실패 ({selector}): {e}")
            continue
    return False


def accept_cookies(driver, retailer):
    """쿠키 동의 팝업 자동 수락 (Selenium / DrissionPage 자동 분기).

    리테일러별 셀렉터를 우선 시도하고, 실패 시 공통 fallback 셀렉터 시도.
    팝업 자체가 없는 경우 조용히 False 반환.

    Args:
        driver: Selenium WebDriver 또는 DrissionPage ChromiumPage 인스턴스
        retailer: 'amazon_gb', 'bestbuy' 등 리테일러명

    Returns:
        True (클릭 성공) / False (셀렉터 일치 없음 또는 모두 실패)
    """
    try:
        retailer_selectors = list(RETAILER_COOKIE_SELECTORS.get(retailer, []))
        all_selectors = retailer_selectors + COMMON_COOKIE_SELECTORS

        # DrissionPage는 .eles 메서드를 가지고 Selenium WebDriver에는 없음
        if hasattr(driver, 'eles'):
            success = _accept_cookies_drission(driver, retailer, all_selectors)
        elif hasattr(driver, 'find_elements'):
            success = _accept_cookies_selenium(driver, retailer, all_selectors)
        else:
            logger.warning(f"지원하지 않는 driver 타입: {type(driver).__name__}")
            return False

        if not success:
            logger.debug(f"쿠키 팝업 미발견 (retailer={retailer})")
        return success

    except Exception as e:
        logger.warning(f"쿠키 수락 처리 중 오류 (retailer={retailer}): {e}")
        return False

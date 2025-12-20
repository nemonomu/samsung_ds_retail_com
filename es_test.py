# -*- coding: utf-8 -*-
"""
Amazon ES 테스트 스크립트 - title, imageurl, sold_by, ships_from 추출 테스트
DB 저장 없음
"""
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import random
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

TEST_URLS = [
    "https://www.amazon.es/dp/B0CTSSMTZK",
    "https://www.amazon.es/dp/B0D9WTKV1B",
    "https://www.amazon.es/dp/B0DGH2FH7T",
    "https://www.amazon.es/dp/B0DGH3HBHJ",
    "https://www.amazon.es/dp/B0DGHB9V34",
    "https://www.amazon.es/dp/B0DWFLPMM5",
    "https://www.amazon.es/dp/B0DWFMD4FQ",
    "https://www.amazon.es/dp/B0DWFMD52T",
    "https://www.amazon.es/Interno-Samsung-hasta-450MB-MZ-V9P1T0CW/dp/B0BM42X58C",
    "https://www.amazon.es/Samsung-Memorie-MZ-V9P4T0B-compatibile-Playstation/dp/B0CBYZ6DD1",
    "https://www.amazon.es/Samsung-MZ-VAP1T0CW-Internal-Compatible-Playstation/dp/B0DWT6PN9N",
    "https://www.amazon.es/Samsung-MZ-VAP2T0CW-Internal-Compatible-Playstation/dp/B0DWT6JGQH",
    "https://www.amazon.es/Samsung-MZ-VAP4T0CW-Internal-Compatible-Playstation/dp/B0DWT52KHY",
    "https://www.amazon.es/dp/B0B9C3ZVHR",
    "https://www.amazon.es/dp/B0F3BMBQ75",
    "https://www.amazon.es/dp/B0F9XN4DCC",
    "https://www.amazon.es/dp/B0F3GGX4SK",
    "https://www.amazon.es/dp/B0F3BD1W6R",
    "https://www.amazon.es/dp/B0F9XMYR15",
    "https://www.amazon.es/dp/B0F3H14JQG",
    "https://www.amazon.es/dp/B0F3B6MZM9",
]

class AmazonTestScraper:
    def __init__(self):
        self.driver = None
        self.wait = None
        self.selectors = {
            'title': [
                "#productTitle",
                "//span[@id='productTitle']",
                "//h1/span[@id='productTitle']",
                "h1#title span",
                "//div[@id='titleSection']//h1//span"
            ],
            'ships_from': [
                "//*[@id='SSOFpopoverLink_ubb']",
                "//a[@id='SSOFpopoverLink_ubb']",
                "//*[@id='fulfillerInfoFeature_feature_div']/div[2]/div[1]/span",
                "//div[@id='fulfillerInfoFeature_feature_div']//span",
                "//*[@id='fulfillerInfoFeature_feature_div']//div[2]//span",
                "//div[contains(@id, 'fulfillerInfo')]//span"
            ],
            'sold_by': [
                "//a[@id='sellerProfileTriggerId']",
                "//*[@id='sellerProfileTriggerId']",
                "//*[@id='merchantInfoFeature_feature_div']/div[2]/div[1]/span",
                "//div[@id='merchantInfoFeature_feature_div']//a",
                "//div[@id='merchantInfoFeature_feature_div']//span",
                "//*[@id='merchantInfoFeature_feature_div']//div[2]//span",
                "//div[contains(@id, 'merchantInfo')]//span",
                "//span[contains(text(), 'Vendido por')]/following-sibling::span"
            ],
            'imageurl': [
                "//div[@id='imageBlock']//img[@id='landingImage']",
                "//div[@id='main-image-container']//img",
                "//img[@class='a-dynamic-image']",
                "//div[@class='imgTagWrapper']//img",
                "//*[@id='landingImage']"
            ]
        }

    def setup_driver(self):
        logger.info("Chrome 드라이버 설정 중...")
        try:
            options = uc.ChromeOptions()
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--no-sandbox')
            options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
            options.add_experimental_option('prefs', {'intl.accept_languages': 'es-ES,es'})

            self.driver = uc.Chrome(options=options)
            self.driver.maximize_window()
            self.wait = WebDriverWait(self.driver, 20)

            logger.info("드라이버 설정 완료")
            return True
        except Exception as e:
            logger.error(f"드라이버 설정 실패: {e}")
            return False

    def extract_element_text(self, selectors, element_name="요소"):
        logger.info(f"[{element_name}] 추출 시작 - 선택자 {len(selectors)}개")
        for idx, selector in enumerate(selectors, 1):
            try:
                if selector.startswith('//') or selector.startswith('('):
                    elements = self.driver.find_elements(By.XPATH, selector)
                else:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)

                logger.info(f"  [{idx}/{len(selectors)}] {selector[:60]}... -> {len(elements)}개 발견")

                if elements:
                    for element in elements:
                        try:
                            if element.is_displayed():
                                text1 = element.text.strip()
                                text2 = element.get_attribute('textContent')
                                text3 = element.get_attribute('innerText')

                                text2 = text2.strip() if text2 else ""
                                text3 = text3.strip() if text3 else ""

                                text = max([text1, text2, text3], key=len)

                                if text:
                                    logger.info(f"    원본: '{text[:80]}...' " if len(text) > 80 else f"    원본: '{text}'")

                                    if element_name in ["Sold By", "Ships From"]:
                                        label_only_patterns = [
                                            'sold by', 'ships from', 'vendido por', 'enviado por',
                                            'gestionado por', 'remitente', 'vendedor'
                                        ]
                                        text_lower = text.lower().strip()

                                        if text_lower in label_only_patterns:
                                            logger.info(f"    라벨만 있음, 스킵")
                                            continue

                                        for pattern in label_only_patterns:
                                            if text_lower.startswith(pattern + ' '):
                                                actual_value = text[len(pattern):].strip()
                                                if actual_value:
                                                    text = actual_value
                                                    logger.info(f"    라벨 제거 후: '{text}'")
                                                break

                                    if text:
                                        logger.info(f"[{element_name}] 추출 성공: '{text[:60]}...'" if len(text) > 60 else f"[{element_name}] 추출 성공: '{text}'")
                                        return text
                        except:
                            pass
            except:
                continue
        logger.warning(f"[{element_name}] 추출 실패")
        return None

    def extract_imageurl(self):
        logger.info(f"[ImageURL] 추출 시작 - 선택자 {len(self.selectors['imageurl'])}개")
        for idx, selector in enumerate(self.selectors['imageurl'], 1):
            try:
                if selector.startswith('//'):
                    element = self.driver.find_element(By.XPATH, selector)
                else:
                    element = self.driver.find_element(By.CSS_SELECTOR, selector)

                src = element.get_attribute('src')
                logger.info(f"  [{idx}/{len(self.selectors['imageurl'])}] {selector[:50]}...")
                if src:
                    logger.info(f"[ImageURL] 추출 성공: {src[:80]}...")
                    return src
            except:
                continue
        logger.warning(f"[ImageURL] 추출 실패")
        return None

    def extract_product_info(self, url):
        try:
            logger.info(f"URL 접속: {url}")
            self.driver.get(url)
            time.sleep(random.uniform(3, 5))

            result = {
                'url': url,
                'title': None,
                'imageurl': None,
                'sold_by': None,
                'ships_from': None
            }

            # Title 추출
            result['title'] = self.extract_element_text(self.selectors['title'], "Title")

            # ImageURL 추출
            result['imageurl'] = self.extract_imageurl()

            # Ships From 추출
            result['ships_from'] = self.extract_element_text(self.selectors['ships_from'], "Ships From")

            # Sold By 추출
            result['sold_by'] = self.extract_element_text(self.selectors['sold_by'], "Sold By")

            # ships_from이 None이고 sold_by가 있을 때, 통합 라벨 확인
            if not result['ships_from'] and result['sold_by']:
                try:
                    label_element = self.driver.find_element(By.XPATH, "//*[@id='merchantInfoFeature_feature_div']/div[1]/div/span")
                    if label_element:
                        label_text = label_element.text.strip() if label_element.text else ""
                        if not label_text:
                            label_text = (label_element.get_attribute('textContent') or "").strip()
                        if "Remitente" in label_text and "Vendedor" in label_text:
                            result['ships_from'] = result['sold_by']
                            logger.info(f"통합 라벨 발견 - ships_from = sold_by")
                except:
                    pass

            return result

        except Exception as e:
            logger.error(f"추출 오류: {e}")
            return {
                'url': url,
                'title': None,
                'imageurl': None,
                'sold_by': None,
                'ships_from': None
            }

    def run_test(self):
        if not self.setup_driver():
            return

        results = []

        try:
            for idx, url in enumerate(TEST_URLS, 1):
                print(f"\n{'='*80}")
                print(f"[{idx}/{len(TEST_URLS)}] 테스트 중...")
                print(f"{'='*80}")

                result = self.extract_product_info(url)
                results.append(result)

                print(f"\n결과:")
                print(f"  URL: {result['url']}")
                print(f"  Title: {result['title'][:60] + '...' if result['title'] and len(result['title']) > 60 else result['title']}")
                print(f"  ImageURL: {'있음' if result['imageurl'] else 'None'}")
                print(f"  Sold By: {result['sold_by']}")
                print(f"  Ships From: {result['ships_from']}")

                if idx < len(TEST_URLS):
                    wait_time = random.uniform(3, 5)
                    print(f"\n{wait_time:.1f}초 대기...")
                    time.sleep(wait_time)

        finally:
            if self.driver:
                self.driver.quit()

        # 최종 요약
        print(f"\n{'='*80}")
        print("최종 결과 요약")
        print(f"{'='*80}")

        for i, r in enumerate(results, 1):
            print(f"\n[{i}] {r['url']}")
            print(f"    Title: {r['title'][:50] + '...' if r['title'] and len(r['title']) > 50 else r['title']}")
            print(f"    ImageURL: {'O' if r['imageurl'] else 'X'}")
            print(f"    Sold By: {r['sold_by']}")
            print(f"    Ships From: {r['ships_from']}")

        # 통계
        print(f"\n{'='*80}")
        print("통계")
        print(f"{'='*80}")
        print(f"전체: {len(results)}")
        print(f"Title 성공: {sum(1 for r in results if r['title'])}")
        print(f"ImageURL 성공: {sum(1 for r in results if r['imageurl'])}")
        print(f"Sold By 성공: {sum(1 for r in results if r['sold_by'])}")
        print(f"Ships From 성공: {sum(1 for r in results if r['ships_from'])}")

if __name__ == "__main__":
    scraper = AmazonTestScraper()
    scraper.run_test()

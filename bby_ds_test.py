"""
BestBuy 테스트 스크립트 - 타이틀, 이미지URL, 가격 추출 테스트
"""
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import random
import re
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# 테스트 URL 목록
TEST_URLS = [
    "https://www.bestbuy.com/site/6512130.p",
    "https://www.bestbuy.com/site/6512129.p",
    "https://www.bestbuy.com/site/6593301.p",
    "https://www.bestbuy.com/site/6633573.p",
    "https://www.bestbuy.com/site/6636636.p",
    "https://www.bestbuy.com/site/6633571.p",
    "https://www.bestbuy.com/site/6636632.p",
    "https://www.bestbuy.com/site/6633572.p",
    "https://www.bestbuy.com/site/6636635.p",
    "https://www.bestbuy.com/site/6541324.p",
    "https://www.bestbuy.com/site/6541326.p",
    "https://www.bestbuy.com/site/6509713.p",
    "https://www.bestbuy.com/site/6559270.p",
    "https://www.bestbuy.com/site/6541327.p",
    "https://www.bestbuy.com/site/6601260.p",
    "https://www.bestbuy.com/site/6594263.p",
    "https://www.bestbuy.com/site/6447127.p",
    "https://www.bestbuy.com/site/6394313.p",
    "https://www.bestbuy.com/site/6447128.p",
    "https://www.bestbuy.com/site/6465362.p",
    "https://www.bestbuy.com/site/6447130.p",
    "https://www.bestbuy.com/site/6598876.p",
    "https://www.bestbuy.com/site/6636631.p",
    "https://www.bestbuy.com/site/6636637.p",
    "https://www.bestbuy.com/site/6636638.p",
    "https://www.bestbuy.com/site/6559271.p",
]

# XPath 선택자
XPATHS = {
    'title': [
        '/html/body/div[5]/div[4]/div[1]/div/h1',
        '/html/body/div[5]/div[4]/div[2]/div/h1',
        '//h1[contains(@class, "heading")]',
        '//h1[contains(@class, "sku-title")]',
    ],
    'imageurl': [
        '/html/body/div[5]/div[4]/div[2]/div/div[2]/div[2]/div/div[2]/div/button/img',
        '/html/body/div[5]/div[4]/div[2]/div/div[2]/div[2]/div/div[2]/div/button[1]/img',
        '/html/body/div[5]/div[4]/div[1]/div/div[2]/div[2]/div/div[2]/div/button[1]/img',
        '/html/body/div[5]/div[4]/div[1]/div/div[2]/div[2]/div/div[2]/div/button/img',
        '//button[contains(@class, "image-button")]//img',
        '//div[contains(@class, "primary-image")]//img',
    ],
    'price': [
        '/html/body/div[5]/div[4]/div[1]/div/div[4]/div/div/div[1]/div/div[1]/div[1]/div[1]/div/div/div/div[1]/span',
        '/html/body/div[5]/div[4]/div[1]/div/div[5]/div/div/div[1]/div/div[1]/div[2]/div[1]/div/div/div/div[1]/span',
        '/html/body/div[5]/div[4]/div[1]/div/div[4]/div/div/div[1]/div/div[1]/div[2]/div[1]/div/div/div/div[1]/span',
        '/html/body/div[5]/div[4]/div[1]/div/div[5]/div/div/div[1]/div/div[1]/div[1]/div[1]/div/div/div/div[1]/span',
        '/html/body/div[5]/div[4]/div[2]/div/div[3]/div/div/div[1]/div/div[1]/div[2]/div[1]/div/div/div/div[1]/span',
        '/html/body/div[5]/div[4]/div[2]/div/div[3]/div/div/div[1]/div/div[1]/div[1]/div[1]/div/div/div/div[1]/span',
        '/html/body/div[5]/div[4]/div[2]/div/div[4]/div/div/div[1]/div/div[1]/div[1]/div[1]/div/div/div/div[1]/span',
        '/html/body/div[5]/div[4]/div[2]/div/div[5]/div/div/div[1]/div/div[1]/div[1]/div[1]/div/div/div/div[1]/span',
    ],
}


class BestBuyTester:
    def __init__(self):
        self.driver = None
        self.session_initialized = False

    def setup_driver(self):
        """Chrome 드라이버 설정"""
        logger.info("Chrome 드라이버 설정 중...")
        try:
            options = uc.ChromeOptions()
            options.add_argument('--disable-blink-features=AutomationControlled')
            self.driver = uc.Chrome(options=options)
            self.driver.maximize_window()
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            logger.info("드라이버 설정 완료")
            return True
        except Exception as e:
            logger.error(f"드라이버 설정 실패: {e}")
            return False

    def initialize_session(self):
        """BestBuy 세션 초기화"""
        if self.session_initialized:
            return True
        try:
            logger.info("BestBuy 세션 초기화 중...")
            self.driver.get("https://www.bestbuy.com")
            time.sleep(4)

            # 국가 선택 팝업 처리
            try:
                country_selector = '/html/body/div[2]/div/div/div/div[1]/div[2]/a[2]'
                element = self.driver.find_element(By.XPATH, country_selector)
                if element.is_displayed():
                    element.click()
                    logger.info("미국 사이트 선택 완료")
                    time.sleep(3)
            except:
                logger.info("국가 선택 팝업 없음")

            self.session_initialized = True
            logger.info("세션 초기화 완료")
            return True
        except Exception as e:
            logger.error(f"세션 초기화 실패: {e}")
            return False

    def extract_product_info(self, url):
        """제품 정보 추출"""
        result = {
            'url': url,
            'title': None,
            'imageurl': None,
            'price': None,
        }

        try:
            self.driver.get(url)
            time.sleep(random.uniform(3, 5))

            # 타이틀 추출
            for xpath in XPATHS['title']:
                try:
                    element = self.driver.find_element(By.XPATH, xpath)
                    text = element.text.strip()
                    if text:
                        result['title'] = text
                        break
                except:
                    continue

            # 이미지 URL 추출
            for xpath in XPATHS['imageurl']:
                try:
                    element = self.driver.find_element(By.XPATH, xpath)
                    src = element.get_attribute('src')
                    if src:
                        result['imageurl'] = src
                        break
                except:
                    continue

            # 가격 추출
            for xpath in XPATHS['price']:
                try:
                    element = self.driver.find_element(By.XPATH, xpath)
                    text = element.text.strip()
                    if text and '$' in text:
                        price_match = re.search(r'\$([\d,]+\.?\d*)', text)
                        if price_match:
                            result['price'] = float(price_match.group(1).replace(',', ''))
                            break
                except:
                    continue

            return result

        except Exception as e:
            logger.error(f"추출 오류: {e}")
            return result

    def run_test(self):
        """테스트 실행"""
        if not self.setup_driver():
            return

        if not self.initialize_session():
            return

        logger.info(f"\n{'='*80}")
        logger.info(f"테스트 시작: 총 {len(TEST_URLS)}개 URL")
        logger.info(f"{'='*80}\n")

        success_count = 0

        for idx, url in enumerate(TEST_URLS, 1):
            logger.info(f"\n[{idx}/{len(TEST_URLS)}] {url}")
            logger.info("-" * 60)

            result = self.extract_product_info(url)

            # 결과 출력
            logger.info(f"URL: {result['url']}")
            logger.info(f"Title: {result['title'][:80] if result['title'] else 'None'}...")
            logger.info(f"ImageURL: {result['imageurl'][:80] if result['imageurl'] else 'None'}...")
            logger.info(f"Price: ${result['price']}" if result['price'] else "Price: None")

            # 성공 여부 체크
            if result['title'] and result['imageurl'] and result['price']:
                logger.info(">>> 성공")
                success_count += 1
            else:
                missing = []
                if not result['title']:
                    missing.append('title')
                if not result['imageurl']:
                    missing.append('imageurl')
                if not result['price']:
                    missing.append('price')
                logger.warning(f">>> 실패 - 누락: {', '.join(missing)}")

            # 다음 요청 전 대기
            if idx < len(TEST_URLS):
                time.sleep(random.uniform(2, 4))

        # 최종 결과
        logger.info(f"\n{'='*80}")
        logger.info(f"테스트 완료")
        logger.info(f"성공: {success_count}/{len(TEST_URLS)} ({success_count/len(TEST_URLS)*100:.1f}%)")
        logger.info(f"{'='*80}")

        # 드라이버 종료
        if self.driver:
            self.driver.quit()
            logger.info("드라이버 종료")


if __name__ == "__main__":
    tester = BestBuyTester()
    tester.run_test()

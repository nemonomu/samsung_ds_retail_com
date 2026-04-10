"""jp_v2.py sold_by 추출 테스트 (DB 저장 없음, 로그 출력만)"""
import sys, logging

# 로그 파일 출력 (인코딩 문제 방지)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('jp_test_260410.log', encoding='utf-8'),
        logging.StreamHandler(open(sys.stdout.fileno(), mode='w', encoding='utf-8', closefd=False))
    ]
)
logger = logging.getLogger(__name__)

from jp_v2 import AmazonScraper

scraper = AmazonScraper(country_code='jp')
scraper.setup_driver()

row_data = {
    'retailerid': '', 'country': 'jp', 'channel': 'Online',
    'retailersku': 'B07KCKG77L', 'brand': 'crucial', 'brand_eng': 'crucial',
    'form_factor': '', 'seg_lv1': '', 'seg_lv2': '', 'seg_lv3': '',
    'capacity': '', 'item': 'MX500', 'vat': 'o'
}

result = scraper.extract_product_info('https://www.amazon.co.jp/dp/B07KCKG77L', row_data)

if result:
    logger.info("=== 추출 결과 ===")
    logger.info(f"title: {result.get('title')}")
    logger.info(f"retailprice: {result.get('retailprice')}")
    logger.info(f"ships_from: {result.get('ships_from')}")
    logger.info(f"sold_by: {result.get('sold_by')}")
    logger.info(f"imageurl: {str(result.get('imageurl', ''))[:80]}")
else:
    logger.error("추출 실패 - result is None")

if scraper.driver:
    try:
        scraper.driver.quit()
    except:
        pass

logger.info("테스트 완료. 로그 파일: jp_test_260410.log")

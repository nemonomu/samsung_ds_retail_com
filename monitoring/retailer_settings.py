"""
리테일러별 드라이버 세팅 함수
"""
import undetected_chromedriver as uc


def setup_danawa():
    """다나와 - undetected-chromedriver"""
    options = uc.ChromeOptions()
    options.add_argument('--accept-lang=ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7')
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-infobars')
    options.add_argument('--disable-renderer-backgrounding')
    options.add_argument('--js-flags=--max-old-space-size=512')

    driver = uc.Chrome(options=options)
    driver.maximize_window()

    settings = {
        'wait_time': 3,
        's3_prefix': 'danawa',
        'driver_type': 'selenium'  # selenium | drission | playwright
    }

    return driver, settings


# 리테일러 매핑
RETAILERS = {
    'danawa': setup_danawa,
}

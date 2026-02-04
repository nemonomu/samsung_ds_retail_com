"""
리테일러별 드라이버 세팅 함수
"""
import re
import subprocess
import undetected_chromedriver as uc


def get_chrome_version():
    """설치된 Chrome 브라우저의 메이저 버전 감지"""
    try:
        result = subprocess.run(
            ['reg', 'query', 'HKEY_CURRENT_USER\\Software\\Google\\Chrome\\BLBeacon', '/v', 'version'],
            capture_output=True, text=True
        )
        if result.returncode == 0:
            match = re.search(r'(\d+)\.\d+\.\d+\.\d+', result.stdout)
            if match:
                version = int(match.group(1))
                print(f"[INFO] Chrome 버전 감지: {version}")
                return version
    except Exception as e:
        print(f"[WARNING] Chrome 버전 감지 실패: {e}")
    return None


def setup_danawa():
    """다나와 - undetected-chromedriver"""
    chrome_version = get_chrome_version()

    options = uc.ChromeOptions()
    options.add_argument('--accept-lang=ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-infobars')
    options.add_argument('--disable-renderer-backgrounding')
    options.add_argument('--js-flags=--max-old-space-size=512')

    driver = uc.Chrome(options=options, use_subprocess=True, version_main=chrome_version)
    driver.maximize_window()

    settings = {
        'wait_time': 3,
        's3_prefix': 'danawa',
        'driver_type': 'selenium'  # selenium | drission | playwright
    }

    return driver, settings


def setup_amazon_usa():
    """Amazon USA - undetected-chromedriver"""
    chrome_version = get_chrome_version()

    options = uc.ChromeOptions()
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-setuid-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-infobars')
    options.add_argument('--disable-renderer-backgrounding')
    options.add_argument('--js-flags=--max-old-space-size=512')
    options.add_experimental_option('prefs', {'intl.accept_languages': 'en-US,en'})

    driver = uc.Chrome(options=options, use_subprocess=True, version_main=chrome_version)
    driver.maximize_window()

    settings = {
        'wait_time': 3,
        's3_prefix': 'amazon_usa',
        'driver_type': 'selenium'
    }

    return driver, settings


def setup_bestbuy():
    """BestBuy - undetected-chromedriver"""
    chrome_version = get_chrome_version()

    options = uc.ChromeOptions()
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-infobars')
    options.add_argument('--disable-renderer-backgrounding')
    options.add_argument('--js-flags=--max-old-space-size=512')

    driver = uc.Chrome(options=options, use_subprocess=True, version_main=chrome_version)
    driver.maximize_window()

    # 스텔스 설정
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    driver.execute_script("Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3]})")

    settings = {
        'wait_time': 3,
        's3_prefix': 'bestbuy',
        'driver_type': 'selenium'
    }

    return driver, settings


def setup_amazon_gb():
    """Amazon UK - undetected-chromedriver"""
    chrome_version = get_chrome_version()

    options = uc.ChromeOptions()
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-setuid-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-infobars')
    options.add_argument('--disable-renderer-backgrounding')
    options.add_argument('--js-flags=--max-old-space-size=512')
    options.add_experimental_option('prefs', {'intl.accept_languages': 'en-GB,en'})

    driver = uc.Chrome(options=options, use_subprocess=True, version_main=chrome_version)
    driver.maximize_window()

    settings = {
        'wait_time': 3,
        's3_prefix': 'amazon_gb',
        'driver_type': 'selenium'
    }

    return driver, settings


def setup_amazon_jp():
    """Amazon Japan - undetected-chromedriver"""
    chrome_version = get_chrome_version()

    options = uc.ChromeOptions()
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-setuid-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-infobars')
    options.add_argument('--disable-renderer-backgrounding')
    options.add_argument('--js-flags=--max-old-space-size=512')
    options.add_experimental_option('prefs', {'intl.accept_languages': 'ja-JP,ja'})

    driver = uc.Chrome(options=options, use_subprocess=True, version_main=chrome_version)
    driver.maximize_window()

    settings = {
        'wait_time': 3,
        's3_prefix': 'amazon_jp',
        'driver_type': 'selenium'
    }

    return driver, settings


def setup_amazon_in():
    """Amazon India - undetected-chromedriver"""
    chrome_version = get_chrome_version()

    options = uc.ChromeOptions()
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-setuid-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-infobars')
    options.add_argument('--disable-renderer-backgrounding')
    options.add_argument('--js-flags=--max-old-space-size=512')
    options.add_argument('--disable-web-security')
    options.add_argument('--disable-features=VizDisplayCompositor')
    options.add_experimental_option('prefs', {'intl.accept_languages': 'en-IN,en'})

    driver = uc.Chrome(options=options, use_subprocess=True, version_main=chrome_version)
    driver.maximize_window()

    settings = {
        'wait_time': 3,
        's3_prefix': 'amazon_in',
        'driver_type': 'selenium'
    }

    return driver, settings


def setup_currys():
    """Currys UK - undetected-chromedriver"""
    chrome_version = get_chrome_version()

    options = uc.ChromeOptions()
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-infobars')
    options.add_argument('--disable-renderer-backgrounding')
    options.add_argument('--js-flags=--max-old-space-size=512')

    driver = uc.Chrome(options=options, use_subprocess=True, version_main=chrome_version)
    driver.maximize_window()

    settings = {
        'wait_time': 3,
        's3_prefix': 'currys',
        'driver_type': 'selenium'
    }

    return driver, settings


def setup_amazon_it():
    """Amazon Italy - undetected-chromedriver"""
    chrome_version = get_chrome_version()

    options = uc.ChromeOptions()
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-setuid-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-infobars')
    options.add_argument('--disable-renderer-backgrounding')
    options.add_argument('--js-flags=--max-old-space-size=512')
    options.add_experimental_option('prefs', {'intl.accept_languages': 'it-IT,it'})

    driver = uc.Chrome(options=options, use_subprocess=True, version_main=chrome_version)
    driver.maximize_window()

    settings = {
        'wait_time': 3,
        's3_prefix': 'amazon_it',
        'driver_type': 'selenium'
    }

    return driver, settings


def setup_amazon_es():
    """Amazon Spain - undetected-chromedriver"""
    chrome_version = get_chrome_version()

    options = uc.ChromeOptions()
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-setuid-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-infobars')
    options.add_argument('--disable-renderer-backgrounding')
    options.add_argument('--js-flags=--max-old-space-size=512')
    options.add_experimental_option('prefs', {'intl.accept_languages': 'es-ES,es'})

    driver = uc.Chrome(options=options, use_subprocess=True, version_main=chrome_version)
    driver.maximize_window()

    settings = {
        'wait_time': 3,
        's3_prefix': 'amazon_es',
        'driver_type': 'selenium'
    }

    return driver, settings


def setup_amazon_fr():
    """Amazon France - undetected-chromedriver"""
    chrome_version = get_chrome_version()

    options = uc.ChromeOptions()
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-setuid-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-infobars')
    options.add_argument('--disable-renderer-backgrounding')
    options.add_argument('--js-flags=--max-old-space-size=512')
    options.add_experimental_option('prefs', {'intl.accept_languages': 'fr-FR,fr'})

    driver = uc.Chrome(options=options, use_subprocess=True, version_main=chrome_version)
    driver.maximize_window()

    settings = {
        'wait_time': 3,
        's3_prefix': 'amazon_fr',
        'driver_type': 'selenium'
    }

    return driver, settings


def setup_centrecom():
    """Centre Com Australia - undetected-chromedriver"""
    chrome_version = get_chrome_version()

    options = uc.ChromeOptions()
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-setuid-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-infobars')
    options.add_argument('--disable-renderer-backgrounding')
    options.add_argument('--js-flags=--max-old-space-size=512')
    options.add_experimental_option('prefs', {'intl.accept_languages': 'en-AU,en'})

    driver = uc.Chrome(options=options, use_subprocess=True, version_main=chrome_version)
    driver.maximize_window()

    settings = {
        'wait_time': 3,
        's3_prefix': 'centrecom',
        'driver_type': 'selenium'
    }

    return driver, settings


def setup_amazon_de():
    """Amazon Germany - undetected-chromedriver"""
    chrome_version = get_chrome_version()

    options = uc.ChromeOptions()
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-setuid-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-infobars')
    options.add_argument('--disable-renderer-backgrounding')
    options.add_argument('--js-flags=--max-old-space-size=512')
    options.add_experimental_option('prefs', {'intl.accept_languages': 'de-DE,de'})

    driver = uc.Chrome(options=options, use_subprocess=True, version_main=chrome_version)
    driver.maximize_window()

    settings = {
        'wait_time': 3,
        's3_prefix': 'amazon_de',
        'driver_type': 'selenium'
    }

    return driver, settings


def setup_fnac():
    """Fnac France - undetected-chromedriver"""
    chrome_version = get_chrome_version()

    options = uc.ChromeOptions()
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-setuid-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-infobars')
    options.add_argument('--disable-renderer-backgrounding')
    options.add_argument('--js-flags=--max-old-space-size=512')
    options.add_experimental_option('prefs', {'intl.accept_languages': 'fr-FR,fr'})

    driver = uc.Chrome(options=options, use_subprocess=True, version_main=chrome_version)
    driver.maximize_window()

    settings = {
        'wait_time': 3,
        's3_prefix': 'fnac',
        'driver_type': 'selenium'
    }

    return driver, settings


def setup_amazon_nl():
    """Amazon Netherlands - undetected-chromedriver"""
    chrome_version = get_chrome_version()

    options = uc.ChromeOptions()
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-setuid-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-infobars')
    options.add_argument('--disable-renderer-backgrounding')
    options.add_argument('--js-flags=--max-old-space-size=512')
    options.add_experimental_option('prefs', {'intl.accept_languages': 'nl-NL,nl'})

    driver = uc.Chrome(options=options, use_subprocess=True, version_main=chrome_version)
    driver.maximize_window()

    settings = {
        'wait_time': 3,
        's3_prefix': 'amazon_nl',
        'driver_type': 'selenium'
    }

    return driver, settings


def setup_coolblue():
    """Coolblue Netherlands - undetected-chromedriver"""
    chrome_version = get_chrome_version()

    options = uc.ChromeOptions()
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-setuid-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-infobars')
    options.add_argument('--disable-renderer-backgrounding')
    options.add_argument('--js-flags=--max-old-space-size=512')
    options.add_experimental_option('prefs', {'intl.accept_languages': 'nl-NL,nl'})

    driver = uc.Chrome(options=options, use_subprocess=True, version_main=chrome_version)
    driver.maximize_window()

    settings = {
        'wait_time': 3,
        's3_prefix': 'coolblue',
        'driver_type': 'selenium'
    }

    return driver, settings


def setup_mediamarkt():
    """MediaMarkt Germany - undetected-chromedriver"""
    chrome_version = get_chrome_version()

    options = uc.ChromeOptions()
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-setuid-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-infobars')
    options.add_argument('--disable-renderer-backgrounding')
    options.add_argument('--js-flags=--max-old-space-size=512')
    options.add_experimental_option('prefs', {'intl.accept_languages': 'de-DE,de'})

    driver = uc.Chrome(options=options, use_subprocess=True, version_main=chrome_version)
    driver.maximize_window()

    settings = {
        'wait_time': 3,
        's3_prefix': 'mediamarkt',
        'driver_type': 'selenium'
    }

    return driver, settings


def setup_x_kom():
    """X-Kom Poland - undetected-chromedriver"""
    chrome_version = get_chrome_version()

    options = uc.ChromeOptions()
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-setuid-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-infobars')
    options.add_argument('--disable-renderer-backgrounding')
    options.add_argument('--js-flags=--max-old-space-size=512')
    options.add_experimental_option('prefs', {'intl.accept_languages': 'pl-PL,pl'})

    driver = uc.Chrome(options=options, use_subprocess=True, version_main=chrome_version)
    driver.maximize_window()

    settings = {
        'wait_time': 3,
        's3_prefix': 'x-kom',
        'driver_type': 'selenium'
    }

    return driver, settings


# 리테일러 매핑 (번호 순서)
RETAILERS = {
    'amazon_usa': setup_amazon_usa,      # 1
    'bestbuy': setup_bestbuy,            # 2
    'amazon_jp': setup_amazon_jp,        # 3
    'amazon_in': setup_amazon_in,        # 4
    'danawa': setup_danawa,              # 5
    'amazon_gb': setup_amazon_gb,        # 6
    'currys': setup_currys,              # 7
    'amazon_it': setup_amazon_it,        # 8
    'amazon_es': setup_amazon_es,        # 9
    'amazon_fr': setup_amazon_fr,        # 10
    'fnac': setup_fnac,                  # 11
    'amazon_nl': setup_amazon_nl,        # 12
    'coolblue': setup_coolblue,          # 13
    'amazon_de': setup_amazon_de,        # 14
    'mediamarkt': setup_mediamarkt,      # 15
    'x-kom': setup_x_kom,                 # 16
    'centrecom': setup_centrecom,        # 17
}

import os

from amazon_v3_test_runner import run_country_v3


def configure_production_run():
    for name in ('TEST_MODE', 'MAX_ITEMS'):
        os.environ.pop(name, None)

    os.environ.update({
        'ES_V3_PRODUCTION_MODE': 'true',
        'ES_V3_VERIFY_MODE': 'false',
        'ES_V3_ALLOW_DB_WRITE': 'true',
        'ES_V3_ALLOW_UPLOAD': 'true',
        'ES_V3_ALLOW_AUTO_RECOVERY': 'true',
        'ES_V3_SAVE_TOP_SCREENSHOT': 'true',
        'ES_V3_SAVE_HTML': 'false',
        'ES_V3_SAVE_LOCAL_RESULTS': 'false',
        'ES_V3_USE_CODE_SELECTORS': 'false',
    })


if __name__ == '__main__':
    configure_production_run()
    run_country_v3('es')

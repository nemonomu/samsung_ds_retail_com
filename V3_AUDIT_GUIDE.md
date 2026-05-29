# Amazon v3 자동검수 / v3 recovery 적용 가이드

이 가이드는 `de_v3.py`에 반영한 구조를 다른 Amazon `*_v3.py`에 옮길 때 쓰는 기준이다.

## 목표

수집 직후 같은 run 안에서 아래 소스를 서로 비교한다.

- crawler 메모리 결과 `results_df`
- 로컬 결과 CSV
- fileserver 업로드용 원본 CSV
- DB 저장 row
- crawler log
- 수집 당시 HTML

불일치가 없으면 HTML/screenshot은 삭제한다. 불일치 SKU만 `run_folder/error/<SKU>/` 아래에 HTML, screenshot, row CSV, log excerpt, 비교 JSON을 보존한다. 메일 제목은 `[DS] ERROR ...`로 발송한다.

## 공통 코드

공통 검수 함수는 `v3_audit.py`의 `run_artifact_audit()`이다.

현재 구현은 Amazon DE HTML 구조에 맞춘 DOM 추출기를 포함한다. 다른 국가에 적용할 때는 먼저 그 국가 HTML에서 아래 필드 추출 규칙을 추가/분기한다.

- `title`
- `retailprice`
- `ships_from`
- `sold_by`

CSV / DB / LOG 비교는 국가 공통으로 사용할 수 있다.

## v3 파일에 넣을 것

각 `*_v3.py`의 production 경로에서 `auto_recovery_run()`을 호출할 때 `scraper_factory`와 `audit_context`를 반드시 넘긴다.

```python
auto_recovery_run(
    target_key='de',
    results_df=results_df,
    target_count=len(urls_data),
    error_logs=blocked_failures or None,
    scraper_factory=create_de_v3_recovery_scraper_factory(output_dir),
    local_output_dir=output_dir,
    local_file_prefix='de_v3',
    audit_context={
        'enabled': True,
        'country_code': 'de',
        'table_name': 'amazon_price_crawl_tbl_de_v2',
        'output_dir': output_dir,
        'log_path': os.path.join(os.path.dirname(__file__), 'logs', 'de_amazon_v3_latest.log'),
        'artifact_prefix': 'de_v3',
        'compare_db': True,
        'compare_log': True,
        'compare_html': True,
        'cleanup_matched': True,
    },
)
```

국가별로 바꿀 값:

- `target_key`: `jp`, `it`, `es`, `usa`, `in`, `gb` 등
- `country_code`: 같은 국가 코드
- `table_name`: 해당 v2/v3가 실제 저장하는 DB 테이블
- `log_path`: 해당 v3 latest log 파일
- `artifact_prefix`: 저장 파일 prefix, 예: `jp_v3`
- `scraper_factory`: 반드시 해당 v3 scraper를 생성해야 한다. v2 scraper를 참조하면 안 된다.

## v3 recovery factory

각 v3 파일에 recovery factory를 만든다.

```python
def create_xx_v3_recovery_scraper_factory(output_dir=None):
    def create_recovery_scraper():
        scraper = AmazonXXV3Scraper(output_dir=output_dir)
        scraper.xx_v3_verification_mode = False
        scraper.xx_v3_save_all_html = os.getenv('XX_V3_SAVE_HTML', 'false').lower() == 'true'

        if not scraper.setup_driver():
            return None
        return scraper

    return create_recovery_scraper
```

핵심은 `auto_recovery_run(..., scraper_factory=...)`다. 이 값이 있으면 `auto_recovery.py`는 `recovery.TARGET_CONFIG`의 v2 loader보다 factory를 우선 사용한다.

## HTML / screenshot 저장 정책

검수 정확도를 위해 production 기본값은 다음처럼 둔다.

```python
XX_V3_SAVE_HTML=true
XX_V3_SAVE_TOP_SCREENSHOT=true
XX_V3_AUDIT_ENABLED=true
XX_V3_AUDIT_CLEANUP=true
```

동작:

1. 수집 중 모든 SKU의 HTML/screenshot을 run folder에 저장한다.
2. 업로드용 CSV 생성 직후 audit를 실행한다.
3. 정상 SKU의 HTML/screenshot은 삭제한다.
4. 오류 SKU의 HTML/screenshot만 `error/<SKU>/`로 이동한다.
5. 기본 log와 결과 CSV는 그대로 남긴다.

## alert mail

`alert_monitor.py`는 `audit_errors`가 1건 이상이면 subject를 `[DS] ERROR ...`로 만든다. 본문에는 최대 50건의 audit mismatch가 표로 들어간다.

## 적용 순서

1. 해당 국가 v3 파일이 DB selector를 로드하는지 확인한다.
2. 해당 국가 v3 파일에 HTML/screenshot 저장 함수가 있는지 확인한다.
3. 없는 경우 DE v3의 `save_debug_html()`, `save_debug_screenshot()` 구조를 복사하되 prefix만 바꾼다.
4. v3 recovery factory를 추가한다.
5. `auto_recovery_run()`에 `scraper_factory`, `local_output_dir`, `local_file_prefix`, `audit_context`를 넣는다.
6. 해당 국가 HTML 샘플 3종으로 `v3_audit.py`의 HTML 추출 규칙을 검증한다.
   - 정상 새 제품 buybox
   - 정상 중고/retour buybox
   - buybox 없음 또는 품절
7. RDP에서 전체 tracking list 테스트를 돌린다.
8. run folder에 `audit_summary.json`이 있고 `error/` 폴더가 없거나 비어 있으면 정상이다.

## 검증 기준

정상 판정은 아래가 모두 맞을 때만 한다.

- CSV row와 DB row의 `title`, `retailprice`, `ships_from`, `sold_by` 일치
- CSV row와 fileserver 업로드용 CSV row 일치
- CSV row와 log 추출값 일치
- CSV row와 HTML DOM 추출값 일치
- 일치 SKU의 HTML/screenshot 삭제 완료
- 불일치 SKU의 증거가 `error/<SKU>/`에 보존됨

OCR은 1차 검수 기준으로 쓰지 않는다. screenshot은 사람이 확인하는 증거로 남기고, 자동 판정 기준은 HTML DOM과 구조화 데이터로 둔다.

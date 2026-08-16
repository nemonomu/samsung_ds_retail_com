import builtins
import importlib.util
import sys
import types
from pathlib import Path

import pandas as pd
import pytest
import pytz
from sqlalchemy import create_engine, text


ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def modules():
    config_stub = types.ModuleType("config")
    config_stub.DB_CONFIG_V2 = {
        "user": "test",
        "password": "test",
        "host": "localhost",
        "port": 3306,
        "database": "test",
    }
    config_stub.FILE_SERVER_CONFIG = {}

    alert_stub = types.ModuleType("alert_monitor")
    alert_stub.monitor_and_alert = lambda *args, **kwargs: None

    previous_config = sys.modules.get("config")
    previous_alert = sys.modules.get("alert_monitor")
    previous_recovery = sys.modules.get("recovery")
    try:
        sys.modules["config"] = config_stub
        sys.modules["alert_monitor"] = alert_stub

        recovery_spec = importlib.util.spec_from_file_location(
            "recovery", ROOT / "recovery.py"
        )
        recovery_module = importlib.util.module_from_spec(recovery_spec)
        sys.modules["recovery"] = recovery_module
        recovery_spec.loader.exec_module(recovery_module)

        auto_spec = importlib.util.spec_from_file_location(
            "auto_recovery_under_test", ROOT / "auto_recovery.py"
        )
        auto_module = importlib.util.module_from_spec(auto_spec)
        auto_spec.loader.exec_module(auto_module)
        yield recovery_module, auto_module
    finally:
        for name, previous in (
            ("config", previous_config),
            ("alert_monitor", previous_alert),
            ("recovery", previous_recovery),
        ):
            if previous is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = previous


def _manager(recovery_module):
    manager = object.__new__(recovery_module.RecoveryManager)
    manager.db_engine = create_engine("sqlite:///:memory:")
    manager.korea_tz = pytz.timezone("Asia/Seoul")
    return manager


def _tracking_row(url, sku):
    return {
        "url": url,
        "retailersku": sku,
        "brand": "test-brand",
        "brand_eng": "test-brand",
        "form_factor": "PSSD",
        "seg_lv1": "PSSD",
        "seg_lv2": "PSSD 1000MB/s",
        "seg_lv3": "",
        "capacity": "1TB",
        "item": "test-item",
        "channel": "Online",
        "retailerid": "",
        "vat": "o",
    }


@pytest.mark.parametrize("raw_result", [None, {"title": None, "retailprice": None}])
def test_build_missing_record_preserves_all_null_attempt(modules, raw_result):
    recovery_module, _ = modules
    manager = _manager(recovery_module)
    row = _tracking_row("https://example.test/missing", "MISSING-SKU")

    record = manager.build_missing_record(
        "in", row, raw_result=raw_result,
        session_start="2026-08-15 18:26:55"
    )

    assert list(record) == recovery_module.CRAWL_COLUMN_ORDER
    assert record["producturl"] == row["url"]
    assert record["retailersku"] == "MISSING-SKU"
    assert record["country_code"] == "in"
    assert record["channel_name"] == "amazon.in"
    assert record["title"] is None
    assert record["retailprice"] is None
    assert record["kr_crawl_datetime"] == "2026-08-15 18:26:55"


def test_manual_recovery_keeps_success_and_all_null_missing_rows(modules, monkeypatch):
    recovery_module, _ = modules
    manager = _manager(recovery_module)
    session_start = ["2026-08-16 02:17:29", "2026-08-15 18:26:55"]
    session_anchor = "2026-08-15 18:26:55"
    table = recovery_module.TARGET_CONFIG["in"]["table"]

    base_records = pd.DataFrame([
        {
            "producturl": f"https://example.test/base/{index}",
            "retailersku": f"BASE-{index:02d}",
            "title": f"base {index}",
            "retailprice": 1000 + index,
            "imageurl": f"https://example.test/image/{index}.jpg",
            "kr_crawl_datetime": session_anchor,
        }
        for index in range(74)
    ])
    missing_rows = pd.DataFrame([
        _tracking_row("https://example.test/recovered", "RECOVERED-SKU"),
        _tracking_row("https://example.test/all-null", "ALL-NULL-SKU"),
    ])
    tracking_records = pd.concat([
        pd.DataFrame({"url": base_records["producturl"]}),
        missing_rows,
    ], ignore_index=True, sort=False)

    manager.get_null_records = lambda target, session: pd.DataFrame()
    manager.get_missing_urls = lambda target, session: missing_rows.copy()
    manager.get_tracking_records = lambda target: tracking_records.copy()

    def get_session_records(target, session):
        try:
            inserted = pd.read_sql_table(table, manager.db_engine)
        except ValueError:
            inserted = pd.DataFrame()
        return pd.concat([base_records, inserted], ignore_index=True, sort=False)

    manager.get_session_all_records = get_session_records
    manager.get_session_raw_records = get_session_records
    manager.load_scraper = lambda target: types.SimpleNamespace(driver=None, page=None)
    recrawl_results = iter([
        {"title": "Recovered title", "retailprice": 18999},
        {"title": None, "retailprice": None},
    ])
    manager.recrawl_url = lambda scraper, url, row, target: next(recrawl_results)

    uploaded = {}
    manager.generate_and_upload_file = (
        lambda target, df, session, custom_filename=None, local_copy_dir=None:
        uploaded.setdefault("df", df.copy()) is not None
    )
    monkeypatch.setattr(builtins, "input", lambda prompt="": "20260815_235959")
    monkeypatch.setattr(recovery_module, "monitor_and_alert", lambda *args, **kwargs: None)

    assert manager.run_recovery("in", session_start) is True

    inserted = pd.read_sql_table(table, manager.db_engine)
    assert len(inserted) == 2
    assert inserted["producturl"].nunique() == 2
    failed = inserted[inserted["retailersku"] == "ALL-NULL-SKU"].iloc[0]
    assert pd.isna(failed["title"])
    assert pd.isna(failed["retailprice"])
    recovered = inserted[inserted["retailersku"] == "RECOVERED-SKU"].iloc[0]
    assert recovered["title"] == "Recovered title"
    assert recovered["retailprice"] == 18999

    final_df = uploaded["df"]
    assert len(final_df) == 76
    assert final_df["producturl"].nunique() == 76
    final_recovered = final_df[
        final_df["producturl"] == "https://example.test/recovered"
    ].iloc[0]
    assert final_recovered["title"] == "Recovered title"
    assert final_recovered["retailprice"] == 18999


def test_india_update_keeps_original_session_timestamp(modules):
    recovery_module, _ = modules
    manager = _manager(recovery_module)
    session_start = "2026-08-15 18:26:55"
    table = recovery_module.TARGET_CONFIG["in"]["table"]
    row = _tracking_row("https://example.test/null-row", "NULL-SKU")
    placeholder = manager.build_missing_record(
        "in", row, session_start=session_start
    )
    assert manager.insert_missing_record("in", placeholder) is True

    assert manager.update_db_record(
        "in", session_start,
        {
            "producturl": row["url"],
            "title": "Recovered title",
            "retailprice": 12345,
            "imageurl": "https://example.test/recovered.jpg",
            "ships_from": "Amazon",
            "sold_by": "Seller",
        },
    ) is True

    saved = pd.read_sql_table(table, manager.db_engine).iloc[0]
    assert saved["title"] == "Recovered title"
    assert str(saved["kr_crawl_datetime"]) == session_start


def test_india_missing_batch_rolls_back_together(modules):
    recovery_module, _ = modules
    manager = _manager(recovery_module)
    table = recovery_module.TARGET_CONFIG["in"]["table"]
    records = [
        manager.build_missing_record(
            "in",
            _tracking_row(f"https://example.test/batch-{index}", "DUPLICATE-SKU"),
            session_start="2026-08-15 18:26:55",
        )
        for index in range(2)
    ]

    pd.DataFrame(records).head(0).to_sql(table, manager.db_engine, index=False)
    with manager.db_engine.begin() as conn:
        conn.execute(text(f"CREATE UNIQUE INDEX uq_test_sku ON {table} (retailersku)"))

    assert manager.insert_missing_records("in", records) is False
    assert pd.read_sql_table(table, manager.db_engine).empty


def test_auto_recovery_keeps_all_null_missing_row(modules, monkeypatch):
    recovery_module, auto_module = modules
    manager = _manager(recovery_module)
    session_start = "2026-08-15 18:26:55"
    first_results = pd.DataFrame([
        {
            "producturl": f"https://example.test/base/{index}",
            "retailersku": f"BASE-{index:02d}",
            "title": f"base {index}",
            "retailprice": 1000 + index,
            "imageurl": f"https://example.test/image/{index}.jpg",
            "ships_from": None,
            "sold_by": None,
            "kr_crawl_datetime": session_start,
        }
        for index in range(74)
    ])
    missing_rows = pd.DataFrame([
        _tracking_row("https://example.test/recovered", "RECOVERED-SKU"),
        _tracking_row("https://example.test/all-null", "ALL-NULL-SKU"),
    ])
    manager.load_scraper = lambda target: types.SimpleNamespace(driver=None, page=None)
    recrawl_results = iter([
        {"title": "Recovered", "retailprice": 18999},
        {"title": None, "retailprice": None},
    ])
    manager.recrawl_url = lambda scraper, url, row, target: next(recrawl_results)
    manager.get_tracking_records = lambda target: pd.concat([
        pd.DataFrame({"url": first_results["producturl"]}),
        missing_rows[["url"]],
    ], ignore_index=True)

    inserted = []
    manager.insert_missing_records = (
        lambda target, results: inserted.extend(
            result.copy() for result in results
        ) or True
    )
    uploaded = {}

    monkeypatch.setattr(auto_module, "RecoveryManager", lambda: manager)
    monkeypatch.setattr(auto_module, "_load_title_null_thresholds", lambda engine: {"in": 0})
    monkeypatch.setattr(
        auto_module, "_get_missing_urls_between",
        lambda *args, **kwargs: missing_rows.copy(),
    )
    monkeypatch.setattr(
        auto_module, "_get_null_records_between",
        lambda *args, **kwargs: pd.DataFrame(),
    )
    monkeypatch.setattr(
        auto_module, "_get_session_records_between",
        lambda *args, **kwargs: auto_module._merge_india_recovery_results(
            first_results, {}, inserted
        ),
    )
    manager.generate_and_upload_file = (
        lambda target, df, session, custom_filename=None, local_copy_dir=None:
        uploaded.setdefault("df", df.copy()) is not None
    )
    monkeypatch.setattr(auto_module, "monitor_and_alert", lambda *args, **kwargs: None)

    auto_module.auto_recovery_run("in", first_results, 76)

    assert len(inserted) == 2
    failed = next(row for row in inserted if row["retailersku"] == "ALL-NULL-SKU")
    assert failed["title"] is None
    assert failed["retailprice"] is None
    recovered = next(
        row for row in inserted if row["retailersku"] == "RECOVERED-SKU"
    )
    assert recovered["title"] == "Recovered"
    assert recovered["retailprice"] == 18999
    assert len(uploaded["df"]) == 76
    uploaded_recovered = uploaded["df"][
        uploaded["df"]["producturl"] == "https://example.test/recovered"
    ].iloc[0]
    assert uploaded_recovered["title"] == "Recovered"
    assert uploaded_recovered["retailprice"] == 18999


def test_india_upload_rejects_wrong_tracking_url_set(modules, monkeypatch):
    recovery_module, auto_module = modules
    expected_urls = [f"https://example.test/item/{index}" for index in range(76)]
    actual_urls = expected_urls[:-1] + ["https://example.test/unexpected"]
    results = pd.DataFrame({"producturl": actual_urls})
    manager = types.SimpleNamespace(
        get_tracking_records=lambda target: pd.DataFrame({"url": expected_urls}),
        generate_and_upload_file=lambda *args, **kwargs: pytest.fail(
            "wrong URL set must not be uploaded"
        ),
    )
    alerts = []
    monkeypatch.setattr(
        auto_module, "_get_session_records_between",
        lambda *args, **kwargs: results.copy(),
    )
    monkeypatch.setattr(
        auto_module, "monitor_and_alert",
        lambda *args, **kwargs: alerts.append(kwargs.get("error_message")),
    )

    result = auto_module._upload_and_alert(
        manager, recovery_module.TARGET_CONFIG["in"], "in", results, 76,
        "2026-08-15 18:26:55"
    )

    assert result is False
    assert alerts and "tracking URL 불일치" in alerts[0]


def test_india_upload_rejects_75_row_db_session(modules, monkeypatch):
    recovery_module, auto_module = modules
    expected_urls = [f"https://example.test/item/{index}" for index in range(76)]
    memory_results = pd.DataFrame({"producturl": expected_urls})
    db_results = pd.DataFrame({"producturl": expected_urls[:-1]})
    manager = types.SimpleNamespace(
        get_tracking_records=lambda target: pd.DataFrame({"url": expected_urls}),
        generate_and_upload_file=lambda *args, **kwargs: pytest.fail(
            "75-row DB session must not be uploaded"
        ),
    )
    alerts = []
    monkeypatch.setattr(
        auto_module, "_get_session_records_between",
        lambda *args, **kwargs: db_results.copy(),
    )
    monkeypatch.setattr(
        auto_module, "monitor_and_alert",
        lambda *args, **kwargs: alerts.append(kwargs.get("error_message")),
    )

    result = auto_module._upload_and_alert(
        manager, recovery_module.TARGET_CONFIG["in"], "in", memory_results, 76,
        "2026-08-15 18:26:55"
    )

    assert result is False
    assert alerts and "rows=75" in alerts[0]

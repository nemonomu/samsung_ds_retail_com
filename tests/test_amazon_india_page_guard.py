"""Exercise India scraper methods without importing production integrations."""

import ast
from datetime import datetime, timezone
import logging
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from amazon_page_guard import (
    AmazonProductPageError,
    capture_product_page_snapshot,
    wait_for_product_page,
)


URL = "https://www.amazon.in/Samsung-SSD/dp/B087DFLF9S?th=1"


class Driver:
    def __init__(self, signals, redirect=None):
        self.signals = signals
        self.redirect = redirect
        self.current_url = URL
        self.title = "Amazon.in"
        self.page_source = ""
        self.visits = []
        self.quit = Mock()
        self.refresh = Mock()

    def get(self, url):
        self.visits.append(url)
        self.current_url = self.redirect or url

    def execute_script(self, script):
        return self.signals


@pytest.fixture
def scraper():
    # Compile the real class only, excluding imports that connect to services.
    path = Path(__file__).resolve().parents[1] / "in_v2.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef))
    namespace = {
        "logger": logging.getLogger(__name__),
        "datetime": datetime,
        "time": SimpleNamespace(sleep=lambda _: None),
        "random": SimpleNamespace(uniform=lambda *args: 0),
        "accept_cookies": Mock(),
        "AmazonProductPageError": AmazonProductPageError,
        "capture_product_page_snapshot": capture_product_page_snapshot,
        "wait_for_product_page": lambda *args, **kwargs: wait_for_product_page(
            *args, **dict(kwargs, timeout_seconds=0)
        ),
        "FULL_NULL_FIELDS": (),
        "is_null_result": lambda result, fields: result["title"] is None,
        "capture_and_upload": Mock(),
    }
    exec(compile(ast.Module(body=[cls], type_ignores=[]), str(path), "exec"), namespace)
    scraper_class = namespace[cls.name]
    obj = object.__new__(scraper_class)
    obj.korea_tz = obj.local_tz = timezone.utc
    obj.wait = None
    obj.selectors = {"in": {"title": [], "imageurl": []}}
    obj.extract_element_text = Mock(return_value="Requested SSD")
    obj.check_stock_availability = Mock(return_value=False)
    obj.handle_captcha_or_block_page = Mock(return_value=True)
    obj.setup_driver = Mock(return_value=True)
    return obj, namespace


@pytest.mark.parametrize("signals", [
    {},
    {"hasDpContainer": True, "hasRecommendations": True},
    {"bodyText": "automated access to Amazon data"},
])
def test_blocked_session_is_replaced_and_original_url_retried(scraper, signals):
    obj, _ = scraper
    blocked = Driver(signals)
    healthy = Driver({"productTitle": "Requested SSD", "domAsin": "B087DFLF9S"})
    obj.driver = blocked

    def setup():
        obj.driver = healthy
        return True

    obj.setup_driver.side_effect = setup
    result = obj.extract_product_info(URL, {"retailersku": "B087DFLF9S"}, max_retries=1)

    assert blocked.visits == healthy.visits == [URL]
    blocked.quit.assert_called_once()
    blocked.refresh.assert_not_called()
    obj.extract_element_text.assert_called_once()
    assert result["title"] == "Requested SSD"
    assert result["producturl"] == URL


@pytest.mark.parametrize("redirect,dom_asin", [
    ("https://www.amazon.in/dp/B0GJF1GQFX", "B0GJF1GQFX"),
    (None, "B0GJF1GQFX"),
])
def test_asin_mismatch_collects_destination_and_logs_original_identity(
    scraper, caplog, redirect, dom_asin,
):
    obj, _ = scraper
    obj.driver = Driver({"productTitle": "Destination SSD", "domAsin": dom_asin}, redirect)
    obj.extract_element_text.side_effect = ["Destination SSD", "Amazon", "Destination seller"]
    obj.check_stock_availability.return_value = True
    obj.extract_price_india = Mock(return_value=31999)
    obj.selectors["in"].update(ships_from=[], sold_by=[])

    with caplog.at_level(logging.WARNING):
        result = obj.extract_product_info(URL, {"retailersku": "B087DFLF9S"}, max_retries=1)

    assert result["title"] == "Destination SSD"
    assert result["retailprice"] == 31999
    assert result["ships_from"] == "Amazon"
    assert result["sold_by"] == "Destination seller"
    assert result["producturl"] == URL
    assert result["retailersku"] == "B087DFLF9S"
    assert obj.driver.visits == [URL]
    obj.driver.refresh.assert_not_called()
    assert "ASIN mismatch accepted" in caplog.text
    assert "expected_asin=B087DFLF9S" in caplog.text
    assert "dom_asin=B0GJF1GQFX" in caplog.text
    assert f"final_url={redirect or URL}" in caplog.text
    assert f"requested_url={URL}" in caplog.text


def test_wrong_marketplace_is_never_extracted(scraper):
    obj, _ = scraper
    obj.driver = Driver(
        {"productTitle": "Wrong product", "domAsin": "B087DFLF9S"},
        "https://www.amazon.com/dp/B087DFLF9S",
    )
    result = obj.extract_product_info(URL, {"retailersku": "B087DFLF9S"}, max_retries=0)

    obj.extract_element_text.assert_not_called()
    assert result["title"] is None
    assert result["producturl"] == URL
    assert result["retailersku"] == "B087DFLF9S"


def test_exhausted_block_captures_failure_before_replacing_session(scraper):
    obj, namespace = scraper
    blocked = Driver({})
    obj.driver = blocked
    events = []
    namespace["capture_and_upload"].side_effect = lambda *args: events.append("capture")
    obj.setup_driver.side_effect = lambda: events.append("restart")

    result = obj.extract_product_info(URL, {}, max_retries=0)

    assert result["title"] is None
    assert events == ["capture", "restart"]
    blocked.quit.assert_called_once()
    assert obj.wait is None

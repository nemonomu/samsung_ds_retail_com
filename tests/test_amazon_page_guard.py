from amazon_page_guard import (
    capture_product_page_snapshot,
    extract_asin,
    wait_for_product_page,
)


class FakeDriver:
    def __init__(self, *, url, title="", source="", signals=None, signal_sequence=None):
        self.current_url = url
        self.title = title
        self.page_source = source
        self._signals = signals or {}
        self._signal_sequence = list(signal_sequence or [])

    def execute_script(self, _script):
        if self._signal_sequence:
            self._signals = self._signal_sequence.pop(0)
        return self._signals


def test_extract_asin_supports_dp_and_gp_product_urls():
    assert extract_asin("https://www.amazon.fr/dp/B087DFLF9S?th=1") == "B087DFLF9S"
    assert extract_asin("https://www.amazon.de/gp/product/b0gjf1gqfx/") == "B0GJF1GQFX"
    assert extract_asin("https://www.amazon.de/") is None


def test_title_and_matching_asin_are_required_for_valid_product_page():
    driver = FakeDriver(
        url="https://www.amazon.fr/dp/B087DFLF9S?th=1",
        title="Samsung SSD",
        signals={
            "productTitle": "Samsung SSD",
            "domAsin": "B087DFLF9S",
            "hasDpContainer": True,
            "hasCenterCol": True,
            "hasFeatureBullets": True,
            "hasRecommendations": True,
        },
    )

    snapshot = capture_product_page_snapshot(
        driver,
        expected_url="https://www.amazon.fr/dp/B087DFLF9S",
        marketplace_host="amazon.fr",
        locale_code="fr",
    )

    assert snapshot.is_valid
    assert snapshot.kind == "valid_product"


def test_recommendation_only_shell_is_soft_block_not_valid_pdp():
    driver = FakeDriver(
        url="https://www.amazon.fr/dp/B087DFLF9S",
        title="Amazon.fr",
        signals={
            "productTitle": "",
            "domAsin": "",
            "hasDpContainer": True,
            "hasCenterCol": False,
            "hasFeatureBullets": False,
            "hasRecommendations": True,
        },
    )

    snapshot = capture_product_page_snapshot(
        driver,
        expected_url="https://www.amazon.fr/dp/B087DFLF9S",
        marketplace_host="amazon.fr",
        locale_code="fr",
    )

    assert not snapshot.is_valid
    assert snapshot.kind == "soft_block_recommendations_only"
    assert snapshot.restart_recommended


def test_matching_url_with_conflicting_dom_asin_is_rejected():
    driver = FakeDriver(
        url="https://www.amazon.de/dp/B087DFLF9S",
        title="Wrong variation",
        signals={"productTitle": "Wrong variation", "domAsin": "B0GJF1GQFX"},
    )

    snapshot = capture_product_page_snapshot(
        driver,
        expected_url="https://www.amazon.de/dp/B087DFLF9S",
        marketplace_host="amazon.de",
        locale_code="de",
    )

    assert not snapshot.is_valid
    assert snapshot.kind == "asin_mismatch"


def test_normal_german_page_text_containing_fortfahren_is_not_a_block():
    driver = FakeDriver(
        url="https://www.amazon.de/dp/B087DFLF9S",
        title="Samsung SSD",
        source="Allgemeine Hinweise: fortfahren",
        signals={
            "productTitle": "Samsung SSD",
            "domAsin": "B087DFLF9S",
            "bodyText": "Allgemeine Hinweise: fortfahren",
        },
    )

    snapshot = capture_product_page_snapshot(
        driver,
        expected_url="https://www.amazon.de/dp/B087DFLF9S",
        marketplace_host="amazon.de",
        locale_code="de",
    )

    assert snapshot.kind == "valid_product"


def test_explicit_french_continue_challenge_is_a_hard_block():
    driver = FakeDriver(
        url="https://www.amazon.fr/dp/B087DFLF9S",
        title="Amazon.fr",
        signals={
            "productTitle": "",
            "domAsin": "",
            "bodyText": "Cliquez sur le bouton ci-dessous pour continuer vos achats",
        },
    )

    snapshot = capture_product_page_snapshot(
        driver,
        expected_url="https://www.amazon.fr/dp/B087DFLF9S",
        marketplace_host="amazon.fr",
        locale_code="fr",
    )

    assert snapshot.kind == "hard_block"
    assert snapshot.restart_recommended


def test_wait_allows_delayed_product_title_to_appear():
    driver = FakeDriver(
        url="https://www.amazon.fr/dp/B087DFLF9S",
        signal_sequence=[
            {"productTitle": "", "domAsin": "", "hasDpContainer": True},
            {"productTitle": "Samsung SSD", "domAsin": "B087DFLF9S", "hasDpContainer": True},
        ],
    )

    snapshot = wait_for_product_page(
        driver,
        expected_url="https://www.amazon.fr/dp/B087DFLF9S",
        marketplace_host="amazon.fr",
        locale_code="fr",
        timeout_seconds=0.2,
        poll_seconds=0.01,
    )

    assert snapshot.is_valid

"""Amazon product-detail page validation shared by marketplace scrapers.

Amazon can return a HTTP-200 page that keeps the requested ``/dp/ASIN`` URL
while omitting the primary product content and rendering only recommendation
modules.  Broad containers such as ``#dp-container`` are therefore not proof
that a usable product-detail page was loaded.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
import time
from typing import Optional
from urllib.parse import urlparse


ASIN_PATTERN = re.compile(r"/(?:dp|gp/product)/([A-Z0-9]{10})(?:[/?]|$)", re.IGNORECASE)

COMMON_HARD_BLOCK_MARKERS = (
    "api-services-support@amazon.com",
    "sorry, we just need to make sure you're not a robot",
    "enter the characters you see below",
    "automated access to amazon data",
)

LOCALE_HARD_BLOCK_MARKERS = {
    "de": (
        "503 - service nicht verfügbar",
        "fehler beim verarbeiten ihrer anforderung",
    ),
    "fr": (
        "cliquez sur le bouton ci-dessous pour continuer vos achats",
    ),
}


def extract_asin(url: Optional[str]) -> Optional[str]:
    """Return a normalized ASIN from an Amazon product URL."""
    if not url:
        return None
    match = ASIN_PATTERN.search(url)
    return match.group(1).upper() if match else None


def _host_matches(url: str, marketplace_host: str) -> bool:
    hostname = (urlparse(url).hostname or "").lower()
    expected = marketplace_host.lower().lstrip(".")
    return hostname == expected or hostname.endswith(f".{expected}")


@dataclass(frozen=True)
class AmazonPageSnapshot:
    expected_url: str
    marketplace_host: str
    current_url: str
    page_title: str
    expected_asin: Optional[str]
    url_asin: Optional[str]
    dom_asin: Optional[str]
    product_title: str
    has_dp_container: bool
    has_center_col: bool
    has_feature_bullets: bool
    has_recommendations: bool
    hard_block_marker: Optional[str]

    @property
    def domain_matches(self) -> bool:
        return _host_matches(self.current_url, self.marketplace_host)

    @property
    def asin_matches(self) -> bool:
        if not self.expected_asin:
            return False

        observed = [asin for asin in (self.url_asin, self.dom_asin) if asin]
        return bool(observed) and all(asin == self.expected_asin for asin in observed)

    @property
    def kind(self) -> str:
        if not self.domain_matches:
            return "invalid_domain"
        if self.hard_block_marker:
            return "hard_block"
        if not self.product_title:
            return "soft_block_recommendations_only" if self.has_recommendations else "soft_block_missing_title"
        if not self.asin_matches:
            return "asin_mismatch"
        return "valid_product"

    @property
    def is_valid(self) -> bool:
        return self.kind == "valid_product"

    @property
    def restart_recommended(self) -> bool:
        return self.kind in {
            "hard_block",
            "soft_block_recommendations_only",
            "soft_block_missing_title",
        }

    def summary(self) -> str:
        title_preview = self.product_title[:80].replace("\n", " ")
        page_title_preview = self.page_title[:80].replace("\n", " ")
        return (
            f"kind={self.kind}, expected_asin={self.expected_asin}, "
            f"url_asin={self.url_asin}, dom_asin={self.dom_asin}, "
            f"title_present={bool(self.product_title)}, title='{title_preview}', "
            f"page_title='{page_title_preview}', block_marker={self.hard_block_marker}, "
            f"dp_container={self.has_dp_container}, center_col={self.has_center_col}, "
            f"feature_bullets={self.has_feature_bullets}, "
            f"recommendations={self.has_recommendations}, url={self.current_url}"
        )


class AmazonProductPageError(RuntimeError):
    """Raised when Amazon did not return the requested usable PDP."""

    def __init__(self, snapshot: AmazonPageSnapshot):
        self.snapshot = snapshot
        super().__init__(snapshot.summary())

    @property
    def restart_recommended(self) -> bool:
        return self.snapshot.restart_recommended


def capture_product_page_snapshot(
    driver,
    expected_url: str,
    marketplace_host: str,
    locale_code: str,
) -> AmazonPageSnapshot:
    """Capture stable PDP signals without trusting broad Amazon containers."""
    try:
        current_url = driver.current_url or ""
    except Exception:
        current_url = ""

    try:
        page_title = driver.title or ""
    except Exception:
        page_title = ""

    signals = {}
    try:
        signals = driver.execute_script(
            """
            const title = document.querySelector('#productTitle');
            const asinInput = document.querySelector('#ASIN, input[name="ASIN"]');
            return {
                productTitle: title ? (title.innerText || title.textContent || '').trim() : '',
                domAsin: asinInput ? (asinInput.value || asinInput.getAttribute('value') || '').trim() : '',
                hasDpContainer: Boolean(document.querySelector('#dp-container')),
                hasCenterCol: Boolean(document.querySelector('#centerCol')),
                hasFeatureBullets: Boolean(document.querySelector('#feature-bullets')),
                hasRecommendations: Boolean(document.querySelector(
                    '#desktop-dp-sims, #dp-sims-desktop, #similarities-desktop, '
                    + '#recommendations, #rhf, [data-a-carousel-options]'
                )),
                bodyText: (document.body ? document.body.innerText : '').slice(0, 8000),
            };
            """
        ) or {}
    except Exception:
        signals = {}

    dom_asin = str(signals.get("domAsin") or "").strip().upper() or None
    if dom_asin and not re.fullmatch(r"[A-Z0-9]{10}", dom_asin):
        dom_asin = None

    hard_block_marker = None
    markers = COMMON_HARD_BLOCK_MARKERS + LOCALE_HARD_BLOCK_MARKERS.get(locale_code, ())
    combined_text = f"{page_title.lower()}\n{str(signals.get('bodyText') or '').lower()}"
    for marker in markers:
        if marker in combined_text:
            hard_block_marker = marker
            break

    return AmazonPageSnapshot(
        expected_url=expected_url,
        marketplace_host=marketplace_host,
        current_url=current_url,
        page_title=page_title,
        expected_asin=extract_asin(expected_url),
        url_asin=extract_asin(current_url),
        dom_asin=dom_asin,
        product_title=str(signals.get("productTitle") or "").strip(),
        has_dp_container=bool(signals.get("hasDpContainer")),
        has_center_col=bool(signals.get("hasCenterCol")),
        has_feature_bullets=bool(signals.get("hasFeatureBullets")),
        has_recommendations=bool(signals.get("hasRecommendations")),
        hard_block_marker=hard_block_marker,
    )


def wait_for_product_page(
    driver,
    expected_url: str,
    marketplace_host: str,
    locale_code: str,
    timeout_seconds: float = 12,
    poll_seconds: float = 0.5,
) -> AmazonPageSnapshot:
    """Wait for a title-bearing PDP, returning the final diagnostic snapshot."""
    deadline = time.monotonic() + max(timeout_seconds, 0)

    while True:
        snapshot = capture_product_page_snapshot(
            driver,
            expected_url=expected_url,
            marketplace_host=marketplace_host,
            locale_code=locale_code,
        )

        if snapshot.is_valid or snapshot.kind in {"hard_block", "invalid_domain", "asin_mismatch"}:
            return snapshot
        if time.monotonic() >= deadline:
            return snapshot

        time.sleep(max(poll_seconds, 0.05))

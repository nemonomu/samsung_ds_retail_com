"""Human-assisted FNAC recovery, using local Chrome and the v3 decision rules.

No ZenRows requests. Connect to the dedicated recovery Chrome; Chrome manages
its own profile, which this adapter never reads or copies. Proof is captured on
the accepted tab and registered only after the caller has saved the crawl row.
"""
import logging
import os
import re
import socket
import subprocess
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit

from fnac_v3 import FnacZenRowsScraper, is_confirmed_null_reason, normalize_product_url
from null_screenshot import capture_and_upload, delete_screenshots_for_sku
from fnac_monitoring import sync_saved_fnac_results

logger = logging.getLogger(__name__)


class FnacManualRecoveryScraper(FnacZenRowsScraper):
    def __init__(self, decision_timeout=10, chrome_port=9222):
        super().__init__(browser_verify_ambiguous=False, screenshot_timeout=15)
        self.playwright = self.browser = self.context = self.page = None
        self.chrome_port = int(chrome_port)
        if not 1024 <= self.chrome_port <= 65535:
            raise ValueError('FNAC Chrome port must be between 1024 and 65535')
        self.decision_timeout = max(0, float(decision_timeout))
        self._decision_not_ready = False
        self._confirm_missing_price = False
        self._confirmed_missing_signature = None
        self._pending_missing_signature = None

    def setup_db_connection(self):
        # RecoveryManager owns writes; this adapter only reads the open page.
        self.db_engine = None

    def setup_browser(self):
        from playwright.sync_api import sync_playwright

        try:
            self.playwright = sync_playwright().start()
            existing = self._chrome_listening()
            if not existing:
                self._launch_recovery_chrome()
            # Match the successful manual probe: use Chrome's existing default
            # context. Never create an isolated context or override its identity.
            deadline = time.monotonic() + 15
            while True:
                try:
                    self.browser = self.playwright.chromium.connect_over_cdp(
                        f'http://127.0.0.1:{self.chrome_port}',
                        timeout=15000 if existing else min(2000, max(1, int((deadline - time.monotonic()) * 1000))))
                    break
                except Exception:
                    if existing or time.monotonic() >= deadline:
                        raise
                    time.sleep(0.25)
            if not self.browser.contexts:
                raise RuntimeError('Recovery Chrome has no default context')
            self.context = self.browser.contexts[0]
            logger.info('FNAC manual recovery: dedicated Chrome %s, ZenRows requests=0',
                        'attached' if existing else 'started')
            return True
        except Exception as exc:
            self.close()
            logger.error('FNAC local Chrome setup failed error=%s', type(exc).__name__)
            raise RuntimeError('FNAC recovery Chrome connection failed; check Chrome and its local debugging port') from None

    def _chrome_listening(self):
        try:
            with socket.create_connection(('127.0.0.1', self.chrome_port), timeout=1):
                return True
        except OSError:
            return False

    def _launch_recovery_chrome(self):
        if os.name != 'nt':
            raise RuntimeError('FNAC recovery Chrome auto-start requires Windows')
        candidates = [Path(root) / 'Google' / 'Chrome' / 'Application' / 'chrome.exe'
                      for name in ('ProgramFiles', 'ProgramFiles(x86)', 'LOCALAPPDATA')
                      if (root := os.environ.get(name))]
        chrome = next((path for path in candidates if path.is_file()), None)
        local_root = os.environ.get('LOCALAPPDATA')
        if chrome is None or not local_root:
            raise RuntimeError('Installed Chrome or LOCALAPPDATA is unavailable')
        # Same directory used in the successful human-operated remote test.
        # Do not open, export, copy, clear or inspect browser session files.
        profile = Path(local_root) / 'FnacRecoveryChrome'
        subprocess.Popen(
            [str(chrome), '--remote-debugging-address=127.0.0.1',
             f'--remote-debugging-port={self.chrome_port}', f'--user-data-dir={profile}',
             '--no-first-run', '--new-window', 'about:blank'],
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )

    def close(self):
        # This Chrome stays open for human interaction and future recovery.
        # Stopping the client disconnects without closing shared tabs or Chrome.
        if self.playwright is not None:
            try:
                self.playwright.stop()
            except Exception:
                pass
        self.playwright = self.browser = self.context = self.page = None

    @staticmethod
    def _product_tab_key(url):
        parts = urlsplit(normalize_product_url(url))
        if parts.scheme != 'https' or parts.hostname not in ('fnac.com', 'www.fnac.com'):
            return None
        match = re.search(r'/a(\d+)(?:/|$)', parts.path)
        # Query includes offer selection; never silently borrow another offer.
        return (match.group(1), parts.query) if match else None

    def _select_product_tab(self, url):
        desired = self._product_tab_key(url)
        if desired is None:
            raise ValueError('FNAC recovery requires a valid FNAC product URL')
        pages = self.context.pages if self.context is not None else [self.page]
        matching = [page for page in pages if page is not None and not page.is_closed()
                    and self._product_tab_key(page.url) == desired]
        if matching:
            self.page = self.page if self.page in matching else matching[0]
            self.page.bring_to_front()
            return True
        if self.page is None or self.page.is_closed():
            blank = next((page for page in pages if page is not None and not page.is_closed()
                          and page.url == 'about:blank'), None)
            self.page = blank if blank is not None else self.context.new_page()
        self.page.bring_to_front()
        return False

    def _maximize_for_capture(self):
        session = self.page.context.new_cdp_session(self.page)
        try:
            info = session.send('Browser.getWindowForTarget')
            window_id = info['windowId']
            state = info['bounds']['windowState']
            if state == 'maximized':
                return True
            if state in {'minimized', 'fullscreen'}:
                session.send('Browser.setWindowBounds',
                             {'windowId': window_id, 'bounds': {'windowState': 'normal'}})
            session.send('Browser.setWindowBounds',
                         {'windowId': window_id, 'bounds': {'windowState': 'maximized'}})
            deadline = time.monotonic() + 3
            while True:
                bounds = session.send('Browser.getWindowBounds', {'windowId': window_id})['bounds']
                if bounds['windowState'] == 'maximized':
                    self.page.wait_for_timeout(200)
                    return True
                if time.monotonic() >= deadline:
                    logger.warning('FNAC window maximization incomplete; capture deferred')
                    return False
                self.page.wait_for_timeout(100)
        finally:
            try:
                session.detach()
            except Exception:
                pass

    def _same_product_page(self, url):
        expected = re.search(r'/a(\d+)(?:/|$)', urlsplit(url).path)
        actual_url = urlsplit(self.page.url)
        host = (actual_url.hostname or '').lower()
        actual = re.search(r'/a(\d+)(?:/|$)', actual_url.path)
        return bool(expected and actual and expected.group(1) == actual.group(1)
                    and actual_url.scheme == 'https'
                    and (host == 'fnac.com' or host.endswith('.fnac.com')))

    def _ready_state(self, url, sku):
        if not self._same_product_page(url):
            return 'wrong_product_or_challenge'
        state = self.wait_for_screenshot_ready(self.page, sku, url)
        if state != 'ready':
            return state
        if not self.product_html_ready(self.browser_product_html(self.page), url, require_decision=False):
            return 'product_html_not_ready'
        return 'ready'

    def _ask_user(self, sku, state):
        print(f'\nFNAC SKU {sku}: {state}')
        self._confirm_missing_price = False
        if state == 'decision_not_ready':
            print('가격·판매 조건이 아직 확인되지 않았습니다. 같은 상품 화면을 확인하세요.')
            print('Enter: 로딩 후 다시 확인 / n: 로딩 완료·가격 없음 직접 확인 / s: 건너뛰기')
        else:
            print('브라우저에서 봇체크를 직접 처리하고 상품 페이지를 확인하세요.')
            print('Enter: 같은 탭에서 다시 확인 / s: 이 상품 건너뛰기')
        try:
            choice = input('선택: ').strip().lower()
            self._confirm_missing_price = state == 'decision_not_ready' and choice == 'n'
            return choice != 's'
        except (EOFError, OSError):
            logger.warning('FNAC manual recovery needs an interactive terminal sku=%s', sku)
            return False

    def extract_product_info(self, url, row_data, retry_count=0, max_retries=1):
        # The recovery interface stays compatible, but there is no automatic
        # reload/reconnect while the person is completing the challenge.
        row = dict(row_data)
        row['url'] = url
        row.setdefault('country', row.get('country_code') or 'fr')
        for level in (1, 2, 3):
            row.setdefault(f'seg_lv{level}', row.get(f'segment_lv{level}', ''))
        sku = row.get('retailersku', '')
        self._confirmed_missing_signature = None
        self._pending_missing_signature = None
        try:
            already_open = self._select_product_tab(url)
        except Exception as exc:
            logger.warning('FNAC recovery tab unavailable sku=%s error=%s', sku, type(exc).__name__)
            return None
        try:
            if not already_open:
                self.page.goto(normalize_product_url(url), wait_until='domcontentloaded', timeout=30000)
        except Exception as exc:
            # Even after goto times out the browser can display a challenge.
            # Preserve it for the person, rather than navigating away.
            logger.warning('FNAC manual navigation incomplete sku=%s error=%s', sku, type(exc).__name__)

        while True:
            try:
                state = self._ready_state(url, sku)
                if state == 'ready':
                    result = self._read_current_page(url, row)
                    if result is not None:
                        return result
                    state = 'decision_not_ready' if self._decision_not_ready else 'page_changed_or_capture_failed'
            except Exception as exc:
                state = type(exc).__name__
            if not self._ask_user(sku, state):
                logger.info('FNAC manual recovery skipped; previous data retained sku=%s', sku)
                return None
            # Pump browser events after input(); no navigation or refresh.
            try:
                self.page.wait_for_timeout(300)
                if self._confirm_missing_price:
                    # Consent applies only to the current product decision, not
                    # later conditions or the next SKU. Known rules still win.
                    self._confirmed_missing_signature = self._pending_missing_signature
            except Exception:
                return None

    def _read_current_page(self, url, row):
        self._decision_not_ready = False
        self._pending_missing_signature = None
        deadline = time.monotonic() + self.decision_timeout
        capture_prepared = False
        while True:
            before = self.browser_product_html(self.page)
            if (not self._same_product_page(url)
                    or not self.product_html_ready(before, url, require_decision=False)
                    or self.frame_block_state(self.page, True)):
                return None
            result, reason = self.parse_product(before, row)
            if reason in {'VISIBLE_PRICE_BOX', 'CURRENT_OFFER_PRICE'}:
                result['retailprice'] = self.screenshot_visible_price(self.page, before)
                if result['retailprice'] is None:
                    reason = 'SCREENSHOT_PRICE_NOT_VISIBLE'
            confirmed = self._confirmed_missing_signature
            accepted = (is_confirmed_null_reason(reason) or result['retailprice'] is not None
                        or (confirmed is not None
                    and confirmed == self.capture_decision_signature(before, row)
                    and reason in {'PRICE_NOT_FOUND', 'SCREENSHOT_PRICE_NOT_VISIBLE'}))
            if accepted:
                if result['retailprice'] is None and not capture_prepared:
                    if not self._maximize_for_capture():
                        return None
                    capture_prepared = True
                    deadline = time.monotonic() + self.decision_timeout
                    # Resize can change the rendered offer. Re-read and decide
                    # from the maximized page before capturing its proof.
                    continue
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self._decision_not_ready = True
                self._pending_missing_signature = self.capture_decision_signature(before, row)
                return None
            # Only incomplete decisions wait. Keep the tab and process its
            # render events; do not refresh, reconnect or spend API credits.
            self.page.wait_for_timeout(min(1, remaining) * 1000)
        self._confirmed_missing_signature = None
        # Keep a previously verified image for this same SKU if lazy loading
        # omitted its URL. Never borrow images from another product/page.
        previous_image = row.get('imageurl')
        if not result.get('imageurl') and isinstance(previous_image, str) and previous_image.startswith('https://'):
            result['imageurl'] = previous_image
        # A successful price does not require proof, including recovery from an
        # earlier NULL. Product-image URLs remain part of the normal result.
        png = captured_at = None
        if result['retailprice'] is None:
            captured_at = datetime.now(self.korea_tz)
            png = self.page.screenshot(full_page=False, timeout=15000)
            if not png:
                return None
        after = self.browser_product_html(self.page)
        if (not self._same_product_page(url)
                or self.frame_block_state(self.page, True)
                or not self.product_html_ready(after, url, require_decision=False)
                or self.capture_decision_signature(before, row) != self.capture_decision_signature(after, row)):
            logger.warning('FNAC manual page changed; same-tab recheck needed sku=%s', row.get('retailersku'))
            return None
        result['_crawl_reason'] = reason
        result['_manual_recovery_ok'] = True
        result['_manual_screenshot_bytes'] = png
        result['_manual_screenshot_at'] = captured_at
        logger.info('FNAC manual decision sku=%s price=%s reason=%s', row.get('retailersku'), result['retailprice'], reason)
        return result

    def finalize_saved_result(self, result, original_datetime=None):
        """Called only after successful raw DB UPDATE/INSERT; never navigate."""
        png = result.pop('_manual_screenshot_bytes', None)
        captured_at = result.pop('_manual_screenshot_at', None)
        result.pop('_manual_recovery_ok', None)
        if result.get('retailprice') is None:
            result['_s3_upload'] = 'fail'
            if png:
                # Registration retry reuses exactly the same image, at no
                # additional browser/ZenRows connection cost.
                for _ in range(2):
                    uploaded = capture_and_upload(
                        None, 'fnac', result.get('retailersku', ''), result['producturl'], result,
                        require_monitoring_link=True, screenshot_bytes=png, captured_at=captured_at,
                    )
                    if uploaded:
                        result['_s3_upload'] = 'ok'
                        break
            if result['_s3_upload'] != 'ok':
                logger.warning('FNAC data saved but proof registration failed; previous proof retained sku=%s', result.get('retailersku'))
        else:
            result['_s3_upload'] = 'skip'
            # Only remove old NULL proof after the new normal row is committed.
            dates = {str(value)[:10].replace('-', '') for value in
                     (original_datetime, result.get('kr_crawl_datetime')) if value}
            for day in dates:
                if re.fullmatch(r'\d{8}', day):
                    try:
                        delete_screenshots_for_sku(
                            'fnac', result.get('retailersku', ''), day, preserve_anomaly=True,
                        )
                    except Exception as exc:
                        logger.warning('FNAC old proof cleanup failed sku=%s error=%s', result.get('retailersku'), type(exc).__name__)
        try:
            synced = sync_saved_fnac_results([result])
            if not synced.get('success'):
                logger.warning('FNAC data saved but monitoring sync failed sku=%s', result.get('retailersku'))
        except Exception as exc:
            logger.warning('FNAC monitoring sync failed sku=%s error=%s', result.get('retailersku'), type(exc).__name__)

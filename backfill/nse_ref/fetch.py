"""Downloads from NSE with a raw-file cache: every file is saved in reference/raw/ and never fetched twice.

NseClient opens the home page first (cookies; NSE answers scripts there with 403, which is ignored),
spaces request starts by GAP seconds and, once NSE blocks a request (401/403, a page instead of the
file, repeated network errors), switches for the rest of the run to BrowserFetcher: headless Chromium
through Playwright, which loads the home page and then asks for the file with the browser's cookies."""
import os
import re
import time
from urllib.parse import unquote, urlsplit

import requests

HOME = "https://www.nseindia.com/"
GAP = 2.0   # seconds between request starts
TRIES = 2   # tries with requests before the browser takes over
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                         "Chrome/126.0 Safari/537.36",
           "Accept": "*/*", "Accept-Language": "en-US,en;q=0.9", "Referer": HOME}


class FetchError(Exception):
    """The file could not be downloaded (after the browser too, when allowed)."""


class NotFound(FetchError):
    """404: the browser would not help."""


class Blocked(Exception):
    """NSE refused the script (or sent a page instead of the file); try the browser."""


def looks_like(kind, data):
    """True if data is a file of kind: json, pdf, xlsx or html."""
    head = data.lstrip()[:8]
    if kind == "json":
        return head[:1] in (b"{", b"[")
    if kind == "pdf":
        return data[:5] == b"%PDF-" or data[:2] == b"PK"   # a few circulars are zipped
    if kind == "xlsx":
        return data[:2] == b"PK"
    if kind == "html":
        return head[:1] == b"<"
    return bool(data)


def safe_name(url):
    """A file name for url: its last path part, unquoted, with anything odd turned into _."""
    name = unquote(urlsplit(url).path.rstrip("/").rsplit("/", 1)[-1]) or "index"
    return re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("_")[:150] or "index"


class Throttle:
    def __init__(self, gap=GAP, sleep=time.sleep, clock=time.monotonic):
        self.gap, self.sleep, self.clock, self.last = gap, sleep, clock, None

    def wait(self):
        if self.last is not None:
            rest = self.gap - (self.clock() - self.last)
            if rest > 0:
                self.sleep(rest)
        self.last = self.clock()


def launch_browser():
    """(playwright, browser, context) with NSE's home page loaded. Imported here so the CLI and the
    tests work without Playwright installed."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        raise FetchError("NSE blocked the script and Playwright is not installed: "
                         "pip install playwright, then playwright install chromium") from None
    pw = sync_playwright().start()
    try:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(user_agent=HEADERS["User-Agent"], locale="en-US")
        page = context.new_page()
        try:
            page.goto(HOME, wait_until="domcontentloaded", timeout=45000)
            page.wait_for_timeout(2000)
        except Exception:   # noqa: BLE001 - the cookies are a bonus; the file request decides
            pass
        page.close()
        return pw, browser, context
    except Exception as e:
        pw.stop()
        raise FetchError(f"Could not start headless Chromium ({e}); run: playwright install chromium") from None


class BrowserFetcher:
    def __init__(self, launch=launch_browser):
        self.launch, self.parts = launch, None
        self.dead = ""   # why the browser could not start; not tried again this run

    def get(self, url):
        """(status, bytes)."""
        if self.dead:
            raise FetchError(self.dead)
        if self.parts is None:
            try:
                self.parts = self.launch()
            except FetchError as e:
                self.dead = str(e)
                raise
        r = self.parts[2].request.get(url, headers={"Referer": HOME}, timeout=60000)
        return r.status, r.body()

    def close(self):
        if self.parts:
            pw, browser, _ = self.parts
            try:
                browser.close()
            finally:
                pw.stop()
            self.parts = None


class NseClient:
    def __init__(self, session=None, throttle=None, browser=None, use_browser=True, log=print):
        self.session = session or requests.Session()
        self.session.headers.update(HEADERS)
        self.throttle = throttle or Throttle()
        self.browser = browser if browser is not None else BrowserFetcher()
        self.use_browser, self.log = use_browser, log
        self.warmed = False
        self.blocked = False   # after the first block every request goes through the browser
        self.requests = 0

    def warm(self):
        if self.warmed:
            return
        self.warmed = True
        self.throttle.wait()
        self.requests += 1
        try:
            self.session.get(HOME, timeout=20)
        except requests.RequestException:
            pass

    def _requests_get(self, url, kind):
        err = ""
        for _ in range(TRIES):
            self.throttle.wait()
            self.requests += 1
            try:
                r = self.session.get(url, timeout=60)
            except requests.RequestException as e:
                err = f"{type(e).__name__}: {e}"
                continue
            if r.status_code == 404:
                raise NotFound("404 not found")
            if r.status_code in (401, 403):
                raise Blocked(f"HTTP {r.status_code}")
            if r.ok and looks_like(kind, r.content):
                return r.content
            err = f"HTTP {r.status_code}" if not r.ok else f"a page instead of the {kind}"
            if r.ok:
                raise Blocked(err)
        raise Blocked(err)

    def _browser_get(self, url, kind):
        if getattr(self.browser, "dead", ""):
            raise FetchError(self.browser.dead)
        self.throttle.wait()
        self.requests += 1
        try:
            status, data = self.browser.get(url)
        except FetchError:
            raise
        except Exception as e:   # noqa: BLE001 - Playwright raises its own error types
            raise FetchError(f"browser: {type(e).__name__}: {e}") from None
        if status == 404:
            raise NotFound("404 not found")
        if 200 <= status < 300 and looks_like(kind, data):
            return data
        raise FetchError(f"browser: HTTP {status}" if not 200 <= status < 300 else f"browser: a page instead of the {kind}")

    def get(self, url, kind):
        """The bytes of url (checked to be a kind file); FetchError / NotFound when it can't be had."""
        self.warm()
        if not self.blocked:
            try:
                return self._requests_get(url, kind)
            except Blocked as e:
                if not self.use_browser:
                    raise FetchError(f"NSE blocked the script ({e}); browser fallback is off") from None
                self.log(f"NSE blocked the script ({e}); using headless Chrome from now on")
                self.blocked = True
        return self._browser_get(url, kind)

    def close(self):
        self.browser.close()


class Cache:
    """reference/raw/<kind>/<name>. A file there is never downloaded again, whether this script saved it
    or the user put it there by hand. offline: never download, only read."""

    def __init__(self, raw_dir, client=None, offline=False, log=print):
        self.raw, self.client, self.offline, self.log = raw_dir, client, offline, log
        self.downloads = 0
        self.failures = []   # (what, url, path the user should save it as, error)
        self.not_found = []  # (what, url): NSE links that answer 404 (tried again next run)

    def path(self, sub, name):
        return os.path.join(self.raw, sub, name)

    def get(self, url, sub, kind, name=None, what=""):
        """Bytes of the cached file, else downloaded and saved; None on failure (recorded)."""
        path = self.path(sub, name or safe_name(url))
        if os.path.exists(path):
            with open(path, "rb") as f:
                return f.read()
        if self.offline or self.client is None:
            self.failures.append((what or url, url, path, "not in raw/ (offline)"))
            return None
        try:
            data = self.client.get(url, kind)
        except NotFound:
            self.not_found.append((what or url, url))
            self.log(f"  404 (broken link on NSE): {url}")
            return None
        except FetchError as e:
            self.failures.append((what or url, url, path, str(e)))
            self.log(f"  failed: {url}: {e}")
            return None
        write_bytes(path, data)
        self.downloads += 1
        return data


def write_bytes(path, data):
    """Atomic: a half-written file never counts as downloaded."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".part"
    with open(tmp, "wb") as f:
        f.write(data)
    os.replace(tmp, path)

"""Downloads for the index membership (index_membership.py fetch): the AMFI half-yearly market-cap lists, the
niftyindices current constituent lists, the NSE Indices press releases (index changes) and Wayback Machine copies of
old constituent lists (a check only). Sources checked 2026-10-17 (see docs/dev/index_membership.md).

Everything lands unchanged in <folder>/index_membership/input/. A file already there (downloaded, or saved by hand)
is never downloaded again unless force; the "current" files (the two listing pages, the constituent lists, the
Wayback indexes) are fetched on every run as a copy dated with today's IST date. The client is
backfill.nse_ref.fetch.NseClient (GAP seconds between requests, headless Chromium once a site blocks the script),
warmed on each site's own home page instead of NSE's."""
import csv
import io
import json
import os
import re
from datetime import date, datetime
from urllib.parse import unquote, urljoin, urlsplit
from zoneinfo import ZoneInfo

import requests

from backfill.locks import RunLock
from backfill.nse_ref.fetch import FetchError, NotFound, NseClient, safe_name, write_bytes

IST = ZoneInfo("Asia/Kolkata")
OUT = "index_membership"

AMFI_PAGE = "https://www.amfiindia.com/otherdata/categorisation-of-stocks"
NIFTY_SITE = "https://www.niftyindices.com"
CURRENT_URL = NIFTY_SITE + "/IndexConstituent/{file}"
PRESS_PAGE = NIFTY_SITE + "/press-release"
CDX = "https://web.archive.org/cdx/search/cdx?url={url}&output=json&fl=timestamp,statuscode,digest,original"
WAYBACK = "https://web.archive.org/web/{ts}id_/{url}"
SNAPSHOT_HOSTS = ("niftyindices.com/IndexConstituent/", "www1.nseindia.com/content/indices/")
CHANGES_FROM = date(2014, 10, 1)   # notices announced since then (late-2014 notices take effect in 2015)

# index -> niftyindices constituent file (the same name is used on the old www1.nseindia.com site)
INDEX_FILES = {
    "NIFTY 50": "ind_nifty50list.csv",
    "NIFTY NEXT 50": "ind_niftynext50list.csv",
    "NIFTY 100": "ind_nifty100list.csv",
    "NIFTY 200": "ind_nifty200list.csv",
    "NIFTY 500": "ind_nifty500list.csv",
    "NIFTY MIDCAP 150": "ind_niftymidcap150list.csv",
    "NIFTY SMALLCAP 250": "ind_niftysmallcap250list.csv",
    "NIFTY MIDCAP 100": "ind_niftymidcap100list.csv",
}
CURRENT_HEADER = ["Company Name", "Industry", "Symbol", "Series", "ISIN Code"]

# press-release titles: keep index changes; drop fixed income, launches, IPO / SME-only notices and the like.
# A corporate (action) adjustment is always kept: demergers add the spun-off company to the broad indices.
KEEP_TITLE = re.compile(r"replac|inclusion|exclusion|change|revision in criteria|reconstitut|rebalanc|corporate", re.I)
SKIP_TITLE = re.compile(r"fixed income|g-?sec|\bbond|\bsdl\b|t-?bill|target maturity|money market|launch|sme emerge|"
                        r"nifty ipo|dissemination|forex|reference rate|constant maturity", re.I)
CORPORATE = re.compile(r"corporate", re.I)
NOTICE_NAME = re.compile(r"ind_prs(\d{2})(\d{2})(\d{4})(?:_\d+)?\.pdf$", re.I)


def today_ist():
    return datetime.now(IST).date()


def out_dir(folder):
    return os.path.join(folder, OUT)


def input_dir(folder, sub=""):
    return os.path.join(out_dir(folder), "input", sub) if sub else os.path.join(out_dir(folder), "input")


def file_key(index):
    """'NIFTY MIDCAP 150' -> 'NIFTY_MIDCAP_150' (file names)."""
    return index.replace(" ", "_")


class SiteClient(NseClient):
    """NseClient warmed on the home page of each site it is asked for (cookies), not on NSE's. The headers of the
    last plain response are kept for the AMFI Last-Modified."""

    def __init__(self, session=None, **kw):
        super().__init__(session=session or requests.Session(), **kw)
        self.session.headers.pop("Referer", None)
        self.homes = set()
        self.last_headers = {}

    def warm(self):
        self.warmed = True   # NSE's home page is not needed; get() warms each site instead

    def get(self, url, kind):
        parts = urlsplit(url)
        home = f"{parts.scheme}://{parts.netloc}/"
        if home not in self.homes and not self.blocked:
            self.homes.add(home)
            self.throttle.wait()
            self.requests += 1
            try:
                self.session.get(home, timeout=20)
            except requests.RequestException:
                pass
        self.last_headers = {}
        if hasattr(self.session, "last_headers"):
            self.session.last_headers = {}
        data = super().get(url, kind)
        self.last_headers = dict(getattr(self.session, "last_headers", {}) or {})
        return data


class HeaderSession(requests.Session):
    """A requests session that remembers the headers of the last answer."""

    def get(self, url, **kw):
        r = super().get(url, **kw)
        self.last_headers = dict(r.headers)
        return r


def new_client(use_browser=True, log=print):
    return SiteClient(session=HeaderSession(), use_browser=use_browser, log=log)


class Results:
    """What a fetch did: rows (part, what, status, detail); status saved / skipped / refreshed / failed / not_found."""

    def __init__(self, log=print):
        self.rows, self.log = [], log

    def add(self, part, what, status, detail=""):
        self.rows.append((part, what, status, detail))
        if status in ("failed", "not_found"):
            self.log(f"  {status}: {what}: {detail}")

    def count(self, status, part=None):
        return sum(1 for p, _, s, _ in self.rows if s == status and (part is None or p == part))

    @property
    def failed(self):
        return [r for r in self.rows if r[2] == "failed"]


def drop_in(url, path, page=None):
    """How to save a file by hand when the script is blocked."""
    where = f"open {page} and download {url}" if page else f"open {url}"
    return f"{where}; save it unchanged as {path}"


def get_current(client, url, kind, path, res, part, what, page=None):
    """A file fetched on every run: saved as path (overwritten). Returns the bytes, or None (recorded)."""
    try:
        data = client.get(url, kind)
    except NotFound:
        res.add(part, what, "not_found", f"404 {url}")
        return None
    except FetchError as e:
        res.add(part, what, "failed", f"{e}; {drop_in(url, path, page)}")
        return None
    write_bytes(path, data)
    res.add(part, what, "refreshed", path)
    return data


def get_once(client, url, kind, path, res, part, what, force=False, page=None, check=None):
    """A file fetched once: skipped when path exists (unless force). check(data) -> error text or ''."""
    if os.path.exists(path) and not force:
        res.add(part, what, "skipped", path)
        return True
    try:
        data = client.get(url, kind)
        bad = check(data) if check else ""
        if bad:
            raise FetchError(bad)
    except NotFound:
        res.add(part, what, "not_found", f"404 {url}")
        return False
    except FetchError as e:
        res.add(part, what, "failed", f"{e}; {drop_in(url, path, page)}")
        return False
    write_bytes(path, data)
    res.add(part, what, "saved", path)
    return True


def newest(folder, prefix, suffix):
    """The newest dated copy <prefix>_<YYYY-MM-DD><suffix> in folder, else None."""
    if not os.path.isdir(folder):
        return None
    names = sorted(n for n in os.listdir(folder) if n.startswith(prefix + "_") and n.endswith(suffix)
                   and re.fullmatch(r"\d{4}-\d{2}-\d{2}", n[len(prefix) + 1:len(n) - len(suffix)]))
    return os.path.join(folder, names[-1]) if names else None


def read_text(path):
    with open(path, "rb") as f:
        return f.read().decode("utf-8", "replace")


# ---- AMFI ----

def amfi_links(html):
    """[(year, period, excel_url)] from the categorisation page (its embedded JSON, else the .xlsx links)."""
    text = html.replace('\\"', '"').replace("\\u0026", "&")
    out, seen = [], set()
    for block in re.finditer(r'"(\d{4})":\[(.*?)\]', text):
        for m in re.finditer(r'"period":"([^"]+)","pdfUrl":"[^"]*","excelUrl":"([^"]+)"', block.group(2)):
            url = m.group(2).strip()
            if url not in seen:
                seen.add(url)
                out.append((block.group(1), m.group(1).strip(), url))
    if not out:
        for url in re.findall(r'href="(https?://[^"]+\.xlsx)\s*"', text, re.I):
            if url.strip() not in seen:
                seen.add(url.strip())
                out.append(("", "", url.strip()))
    return out


def amfi_file_name(url):
    return safe_name(url.strip())


def check_xlsx(data):
    return "" if data[:2] == b"PK" else "not an Excel file"


def fetch_amfi(folder, client, res, force=False, today=None):
    sub = input_dir(folder, "amfi")
    os.makedirs(sub, exist_ok=True)
    stem = os.path.join(sub, f"categorisation-of-stocks_{today}.html")
    data = get_current(client, AMFI_PAGE, "html", stem, res, "amfi", "AMFI categorisation page")
    if data is None:
        old = newest(sub, "categorisation-of-stocks", ".html")
        if not old:
            return
        res.log(f"  using the saved page {old}")
        html = read_text(old)
    else:
        html = data.decode("utf-8", "replace")
    links = amfi_links(html)
    res.log(f"AMFI: {len(links)} periods on the page")
    sources_path = os.path.join(sub, "sources.csv")
    sources = {}
    if os.path.isfile(sources_path):
        with open(sources_path, encoding="utf-8", newline="") as f:
            sources = {r["file"]: r for r in csv.DictReader(f)}
    for year, period, url in links:
        name = amfi_file_name(url)
        path = os.path.join(sub, name)
        existed = os.path.exists(path)
        ok = get_once(client, url, "xlsx", path, res, "amfi", f"{period} {year}".strip() or name, force,
                      page=AMFI_PAGE, check=check_xlsx)
        row = sources.get(name, {})
        row.update({"year": year, "period": period, "url": url, "file": name})
        if ok and (force or not existed):
            row["last_modified"] = client.last_headers.get("Last-Modified", "")
            row["fetched"] = str(today)
        row.setdefault("last_modified", "")
        row.setdefault("fetched", "")
        sources[name] = row
    write_csv_rows(sources_path, ["year", "period", "url", "file", "last_modified", "fetched"], sources.values())


def write_csv_rows(path, columns, rows):
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=columns, lineterminator="\n", extrasaction="ignore")
    w.writeheader()
    for r in rows:
        w.writerow(r)
    write_bytes(path, buf.getvalue().encode("utf-8"))


# ---- NIFTY current lists ----

def check_current(data):
    head = data.lstrip(b"\xef\xbb\xbf").split(b"\n", 1)[0].decode("utf-8", "replace").strip()
    cols = [c.strip() for c in head.split(",")]
    return "" if all(c in cols for c in ("Symbol", "ISIN Code")) else f"not a constituent list (first line {head[:80]!r})"


def fetch_current(folder, client, res, today=None):
    sub = input_dir(folder, "nifty_current")
    os.makedirs(sub, exist_ok=True)
    for index, name in INDEX_FILES.items():
        url = CURRENT_URL.format(file=name)
        path = os.path.join(sub, f"{file_key(index)}_{today}.csv")
        try:
            data = client.get(url, "csv")
            bad = check_current(data)
            if bad:
                raise FetchError(bad)
        except NotFound:
            res.add("nifty", index, "not_found", f"404 {url}")
            continue
        except FetchError as e:
            res.add("nifty", index, "failed", f"{e}; {drop_in(url, path)}")
            continue
        write_bytes(path, data)
        res.add("nifty", index, "refreshed", path)


# ---- NIFTY changes (press releases) ----

def notice_date(name):
    m = NOTICE_NAME.search(name)
    if not m:
        return None
    try:
        return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    except ValueError:
        return None


def press_links(html):
    """[(announced date, title, absolute url, file name)] of every press release on the page, page order."""
    out, seen = [], set()
    for href, title in re.findall(r"""<a\s+href=['"]([^'"]*Press_Release/[^'"]+\.pdf)['"][^>]*>\s*([^<]+?)\s*</a>""",
                                  html, re.I):
        url = urljoin(NIFTY_SITE + "/", href.strip())
        name = unquote(urlsplit(url).path.rsplit("/", 1)[-1])
        if name in seen:
            continue
        seen.add(name)
        title = re.sub(r"\s+", " ", title.replace("&amp;", "&").replace("&#39;", "'")).strip()
        out.append((notice_date(name), title, url, name))
    return out


def wanted_notice(day, title, first=CHANGES_FROM):
    if day is None or day < first:
        return False
    if CORPORATE.search(title):
        return True
    return bool(KEEP_TITLE.search(title)) and not SKIP_TITLE.search(title)


def check_pdf(data):
    return "" if data[:5] == b"%PDF-" else "not a PDF"


def fetch_changes(folder, client, res, force=False, today=None):
    sub = input_dir(folder, "nifty_changes")
    os.makedirs(sub, exist_ok=True)
    stem = os.path.join(sub, f"press-release_{today}.html")
    data = get_current(client, PRESS_PAGE, "html", stem, res, "changes", "press-release page")
    if data is None:
        old = newest(sub, "press-release", ".html")
        if not old:
            return
        res.log(f"  using the saved page {old}")
        html = read_text(old)
    else:
        html = data.decode("utf-8", "replace")
    links = [x for x in press_links(html) if wanted_notice(x[0], x[1])]
    res.log(f"Press releases: {len(links)} index-change notices since {CHANGES_FROM}")
    rows = []
    for day, title, url, name in links:
        path = os.path.join(sub, name)
        get_once(client, url, "pdf", path, res, "changes", name, force, page=PRESS_PAGE, check=check_pdf)
        rows.append({"announced_date": str(day), "title": title, "url": url, "file": name})
    rows.sort(key=lambda r: (r["announced_date"], r["file"]))
    write_csv_rows(os.path.join(sub, "notices.csv"), ["announced_date", "title", "url", "file"], rows)


# ---- snapshots (Wayback Machine, a check only) ----

def cdx_rows(data):
    """[(timestamp, original url)] of the distinct 200 captures in a CDX JSON answer."""
    try:
        rows = json.loads(data or b"[]")
    except ValueError as e:
        raise FetchError(f"CDX answer is not JSON: {e}") from None
    out, digests = [], set()
    for r in rows[1:] if rows and rows[0] and rows[0][0] == "timestamp" else rows:
        if len(r) < 4 or r[1] != "200" or r[2] in digests:
            continue
        digests.add(r[2])
        out.append((r[0], r[3]))
    return out


def fetch_snapshots(folder, client, res, force=False, today=None):
    sub = input_dir(folder, "snapshots")
    os.makedirs(sub, exist_ok=True)
    for index, name in INDEX_FILES.items():
        captures = []
        for host in SNAPSHOT_HOSTS:
            url = CDX.format(url=host + name)
            path = os.path.join(sub, f"cdx_{file_key(index)}_{host.split('/')[0]}_{today}.json")
            data = get_current(client, url, "json", path, res, "snapshots", f"{index} index of {host}")
            if data is None:
                continue
            try:
                captures += cdx_rows(data)
            except FetchError as e:
                res.add("snapshots", f"{index} index of {host}", "failed", str(e))
        for ts, original in captures:
            path = os.path.join(sub, f"{file_key(index)}_{ts}.csv")
            get_once(client, WAYBACK.format(ts=ts, url=original), "csv", path, res, "snapshots",
                     f"{index} {ts}", force, check=check_current)


PARTS = ("amfi", "nifty", "changes", "snapshots")


def fetch(folder, parts=PARTS, client=None, force=False, today=None, log=print):
    """Run the fetch parts under <folder>/index_membership/.lock; returns Results."""
    today = today or today_ist()
    os.makedirs(out_dir(folder), exist_ok=True)
    lock = RunLock(os.path.join(out_dir(folder), ".lock")).acquire("index_membership.py fetch")
    own = client is None
    client = client or new_client(log=log)
    res = Results(log)
    try:
        if "amfi" in parts:
            log(f"AMFI lists: {AMFI_PAGE}")
            fetch_amfi(folder, client, res, force, today)
        if "nifty" in parts:
            log(f"NIFTY current lists: {CURRENT_URL.format(file='...')}")
            fetch_current(folder, client, res, today)
        if "changes" in parts:
            log(f"NIFTY changes: {PRESS_PAGE}")
            fetch_changes(folder, client, res, force, today)
        if "snapshots" in parts:
            log("Snapshots: web.archive.org")
            fetch_snapshots(folder, client, res, force, today)
    finally:
        if own:
            client.close()
        lock.release()
    return res

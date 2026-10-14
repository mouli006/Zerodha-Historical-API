"""collect(): download what is missing into reference/raw/ and rebuild the five CSVs from raw/.

Every CSV is rebuilt from the saved files on each run (so it never holds duplicates); corrections go
into manual_mergers.csv, not into the CSVs. Pages and the current year's circular list are saved with
_fetched_<date> in the name (at most one download a day); documents (PDFs, Excel files, past years'
lists) are downloaded once."""
import glob
import json
import os
from datetime import date, datetime
from zoneinfo import ZoneInfo

from . import circulars, compulsory, pdftext, suspensions, voluntary
from .common import name_key, write_csv
from .fetch import Cache, NseClient, safe_name

IST = ZoneInfo("Asia/Kolkata")
START = date(2015, 1, 1)
PARTS = ["voluntary", "compulsory", "circulars", "suspensions"]
FILES = {"voluntary": "voluntary_delisting.csv", "compulsory": "compulsory_delisting.csv",
         "circulars": "delisting_circulars.csv", "mergers": "mergers.csv", "suspensions": "suspensions.csv"}
TEXT_VERSION = 1


def today_ist():
    return datetime.now(IST).date()


def reference_dir(folder):
    return os.path.join(folder, "NSE_CM_bhav", "reference")


class Collector:
    def __init__(self, folder, offline=False, client=None, use_browser=True, log=print, today=None):
        self.ref = reference_dir(folder)
        self.raw = os.path.join(self.ref, "raw")
        self.text_dir = os.path.join(self.ref, "text_cache")
        self.today = today or today_ist()
        self.log = log
        if client is None and not offline:
            client = NseClient(use_browser=use_browser, log=log)
        self.client = client
        self.cache = Cache(self.raw, client, offline, log)
        self.offline = offline
        self.notes = []   # warnings for the summary

    # --- files -------------------------------------------------------------------------------

    def latest(self, sub, pattern):
        found = sorted(glob.glob(os.path.join(self.raw, sub, pattern)))
        return found[-1] if found else None

    def daily(self, url, sub, stem, kind):
        """A page saved as <stem>_fetched_<today>.<kind>; offline or on failure the newest saved copy."""
        name = f"{stem}_fetched_{self.today.isoformat()}.{kind}"
        if not self.offline or os.path.exists(os.path.join(self.raw, sub, name)):
            before = len(self.cache.failures)
            data = self.cache.get(url, sub, kind, name, what=stem.replace("_", " "))
            if data is not None:
                return data, name
            if not self.offline:
                old = self.latest(sub, f"{stem}_fetched_*.{kind}")
                if old:
                    del self.cache.failures[before:]
                    self.notes.append(f"{stem}: download failed, used the saved copy {os.path.basename(old)}")
                    with open(old, "rb") as f:
                        return f.read(), os.path.basename(old)
                return None, name
        old = self.latest(sub, f"{stem}_fetched_*.{kind}")
        if old:
            with open(old, "rb") as f:
                return f.read(), os.path.basename(old)
        self.cache.failures.append((stem.replace("_", " "), url, os.path.join(self.raw, sub, name), "not in raw/ (offline)"))
        return None, name

    def doc(self, url, sub, what):
        """pdftext.extract() of a PDF (downloaded once; the text is cached in text_cache/), None if the PDF
        could not be had."""
        name = safe_name(url)
        tpath = os.path.join(self.text_dir, sub, name + ".json")
        pdf_path = self.cache.path(sub, name)
        if os.path.exists(tpath) and os.path.exists(pdf_path):
            try:
                with open(tpath, encoding="utf-8") as f:
                    saved = json.load(f)
                if saved.get("v") == TEXT_VERSION and saved.get("size") == os.path.getsize(pdf_path):
                    return saved["doc"]
            except (OSError, ValueError):
                pass
        data = self.cache.get(url, sub, "pdf", what=what)
        if data is None:
            return None
        try:
            d = pdftext.extract(data)
        except Exception as e:   # noqa: BLE001 - a broken PDF must not stop the run
            self.notes.append(f"{name}: could not read the PDF ({type(e).__name__})")
            d = {"text": "", "tables": [], "pages": 0}
        os.makedirs(os.path.dirname(tpath), exist_ok=True)
        with open(tpath, "w", encoding="utf-8") as f:
            json.dump({"v": TEXT_VERSION, "size": len(data), "doc": d}, f)
        return d

    # --- parts -------------------------------------------------------------------------------

    def voluntary(self, since):
        """(rows since `since`, {name_key: symbol} from the whole delisted list)."""
        page, _ = self.daily(voluntary.PAGE_URL, "pages", "delisting_of_companies", "html")
        links = voluntary.excel_links(page.decode("utf-8", "replace")) if page else {}
        rows, lookup = [], {}
        for which in ("delisted", "proposed"):
            url = links.get(which)
            data, name = None, ""
            if url:
                name = safe_name(url)
                data = self.cache.get(url, "delisting", "xlsx", what=f"List of companies {which}")
            if data is None:
                old = self.latest("delisting", "*delisted*.xlsx" if which == "delisted" else "*proposed*.xlsx")
                if old:
                    name = os.path.basename(old)
                    self.notes.append(f"{which} list: using the saved {name}")
                    with open(old, "rb") as f:
                        data = f.read()
                elif page and not url:
                    self.notes.append(f"{which} list: no link found on the Delisting of Companies page")
            if data is None:
                continue
            every = voluntary.parse(data, name, which)
            for r in every:
                if r["symbol"] and r["company"]:
                    lookup.setdefault(name_key(r["company"]), r["symbol"])
            rows += voluntary.parse(data, name, which, since) if which == "delisted" else every
        return rows, lookup

    def compulsory(self, since, lookup):
        page, name = self.daily(compulsory.PAGE_URL, "pages", "orders_of_delisting", "html")
        if page is None:
            return []
        entries = compulsory.parse_page(page.decode("utf-8", "replace"))

        def order_text(url):
            d = self.doc(url, "orders", "delisting order PDF")
            return None if d is None else d["text"]

        return compulsory.build(entries, since, lookup, order_text, name)

    def circular_lists(self, since):
        out = []
        for year in range(since.year, self.today.year + 1):
            url, name = circulars.year_list(year, self.today)
            if year < self.today.year:
                path = self.cache.path("circular_lists", name)
                data = None
                if self.offline and not os.path.exists(path):
                    old = self.latest("circular_lists", f"circulars_{year}_fetched_*.json")
                    if old:
                        with open(old, "rb") as f:
                            data = f.read()
                if data is None:
                    data = self.cache.get(url, "circular_lists", "json", name, what=f"circular list {year}")
            else:
                data, _ = self.daily(url, "circular_lists", f"circulars_{year}", "json")
            if data is not None:
                out += circulars.entries(data)
        return out


def collect(folder, since=START, parts=PARTS, offline=False, client=None, use_browser=True, log=print, today=None):
    """Run the selected parts; returns a summary dict (counts, review counts, downloads, failures)."""
    c = Collector(folder, offline, client, use_browser, log, today)
    os.makedirs(c.ref, exist_ok=True)
    manual_path = suspensions.ensure_manual(c.ref)
    out = {}
    summary = {"reference": c.ref, "files": {}, "skipped": {}, "unmatched_manual": []}
    try:
        log("Delisting of Companies (Excel lists)...")
        vol, lookup = c.voluntary(since)
        if "voluntary" in parts:
            out["voluntary"] = (voluntary.COLUMNS, vol)
        if "compulsory" in parts:
            log("Orders of delisting...")
            out["compulsory"] = (compulsory.COLUMNS, c.compulsory(since, lookup))
        if "circulars" in parts or "suspensions" in parts:
            log(f"Circular lists {since.year}-{c.today.year}...")
            listed = c.circular_lists(since)
            subjects = {e["no"]: e["subject"] for e in listed}   # to see what a circular's cited circulars are about
            delist, susp, skipped = circulars.pick(listed, since)
            summary["skipped"] = skipped
            log(f"Delisting circulars: {len(delist)}, suspension circulars (equity): {len(susp)}")
            docs = {}
            for i, e in enumerate(delist + susp, 1):
                if e["url"] not in docs:
                    docs[e["url"]] = c.doc(e["url"], "circulars", f"circular {e['no']}")
                if i % 50 == 0:
                    log(f"  {i} / {len(delist) + len(susp)}")
            # two passes: symbols that one circular gives for a company fill the others that lack them
            for n in range(2):
                drows = [r for e in delist for r in circulars.parse_delisting(e, docs[e["url"]], lookup)]
                merg, other = [], []
                for e in susp:
                    m, o = suspensions.parse(e, docs[e["url"]], lookup, skipped if n else None, subjects)
                    merg += m
                    other += o
                lookup = dict(lookup)
                circulars.learn_symbols(drows, lookup)
                circulars.learn_symbols(other, lookup)
                circulars.learn_symbols(merg, lookup, "old_symbol", "old_company")
            if "circulars" in parts:
                out["circulars"] = (circulars.COLUMNS, drows)
            # one row per corrected suspension; CANCELLED rows point at the suspension they call off
            merg = suspensions.merge_corrections(merg, "old_symbol", "old_company",
                                                 ("suspended_from", "last_trading_date", "record_date"))
            other = suspensions.link_cancellations(suspensions.merge_corrections(other))
            if "suspensions" in parts:
                summary["unmatched_manual"] = suspensions.apply_manual(merg, suspensions.read_manual(manual_path))
                out["mergers"] = (suspensions.MERGER_COLUMNS, merg)
                out["suspensions"] = (suspensions.SUSPENSION_COLUMNS, other)
    finally:
        if c.client is not None:
            c.client.close()
    for key, (cols, rows) in out.items():
        path = os.path.join(c.ref, FILES[key])
        write_csv(path, cols, rows)
        summary["files"][FILES[key]] = (len(rows), sum(1 for r in rows if r.get("needs_review")))
    summary.update(downloads=c.cache.downloads, failures=c.cache.failures, not_found=c.cache.not_found, notes=c.notes,
                   requests=getattr(c.client, "requests", 0))
    return summary

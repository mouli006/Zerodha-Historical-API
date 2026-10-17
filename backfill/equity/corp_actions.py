"""Downloads NSE's equity corporate actions, one calendar year per request, into <folder>/adjustments/input/.

Source: the JSON API behind https://www.nseindia.com/companies-listing/corporate-filings-actions (Equity tab),
checked 2026-10-17. NSE also serves the same rows as CSV (&csv=true) but without the ISIN, so the JSON is saved,
converted to NSE's CSV column names plus ISIN (corporate_actions_<YYYY>.csv). The client is
backfill.nse_ref.fetch.NseClient: home page for cookies, GAP seconds between requests, headless Chromium once NSE
blocks the script. A past year already saved is skipped unless force; the current year is always fetched again."""
import io
import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

from backfill.locks import RunLock
from backfill.nse_ref.fetch import FetchError, NotFound, write_bytes

IST = ZoneInfo("Asia/Kolkata")
FIRST_YEAR = 2015
PAGE = "https://www.nseindia.com/companies-listing/corporate-filings-actions"
API = "https://www.nseindia.com/api/corporates-corporateActions?index=equities&from_date=01-01-{y}&to_date=31-12-{y}"
# JSON key -> NSE's CSV column name (the CSV download has the same columns, without ISIN)
COLUMNS = {"symbol": "SYMBOL", "comp": "COMPANY NAME", "series": "SERIES", "subject": "PURPOSE",
           "faceVal": "FACE VALUE", "exDate": "EX-DATE", "recDate": "RECORD DATE",
           "bcStartDate": "BOOK CLOSURE START DATE", "bcEndDate": "BOOK CLOSURE END DATE", "isin": "ISIN"}


def this_year():
    return datetime.now(IST).year


def url_for(year):
    return API.format(y=year)


def file_name(year):
    return f"corporate_actions_{year}.csv"


def input_dir(folder):
    return os.path.join(folder, "adjustments", "input")


def to_csv(data):
    """NSE's JSON (a list of rows) -> CSV bytes with NSE's column names plus ISIN, in NSE's order."""
    try:
        rows = json.loads(data)
    except ValueError as e:
        raise FetchError(f"not JSON: {e}") from None
    if not isinstance(rows, list):
        raise FetchError(f"expected a list of rows, got {type(rows).__name__}"
                         + (f" with keys {', '.join(sorted(rows))}" if isinstance(rows, dict) else ""))
    if rows:
        missing = [k for k in COLUMNS if k not in rows[0]]
        if missing:
            raise FetchError(f"rows lack {', '.join(missing)}; keys: {', '.join(sorted(rows[0]))}")
    df = pd.DataFrame([{v: ("" if r.get(k) is None else str(r.get(k))) for k, v in COLUMNS.items()} for r in rows],
                      columns=list(COLUMNS.values()))
    buf = io.StringIO()
    df.to_csv(buf, index=False, lineterminator="\n")
    return buf.getvalue().encode("utf-8"), len(df)


def fetch_years(folder, first, last, client, force=False, today_year=None, log=print):
    """[(year, status, rows, detail)]: status saved / skipped / failed; detail is the path or the error."""
    today_year = today_year or this_year()
    out = input_dir(folder)
    os.makedirs(out, exist_ok=True)
    lock = RunLock(os.path.join(folder, "adjustments", ".lock")).acquire("adjustments.py fetch-actions")
    results = []
    try:
        for year in range(first, last + 1):
            path = os.path.join(out, file_name(year))
            if os.path.exists(path) and year < today_year and not force:
                results.append((year, "skipped", None, path))
                continue
            log(f"{year}: {url_for(year)}")
            try:
                data = client.get(url_for(year), "json")
                body, n = to_csv(data)
            except (FetchError, NotFound) as e:
                results.append((year, "failed", None, str(e)))
                log(f"  failed: {e}")
                continue
            write_bytes(path, body)
            results.append((year, "saved", n, path))
            log(f"  {n:,} rows -> {path}")
    finally:
        lock.release()
    return results


def drop_in(folder, year):
    """How to download a year by hand when the script is blocked."""
    return (f"{year}: open {PAGE}, Equity tab, From 01-01-{year} To 31-12-{year}, click 'Download (.csv)' and save "
            f"it unchanged as {os.path.join(input_dir(folder), file_name(year))}")

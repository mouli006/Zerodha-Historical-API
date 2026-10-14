"""'Delisting of Companies' page: the two Excel lists -> voluntary_delisting.csv.

Verified 2026-10-14: "List of Companies Delisted from NSE" has a sheet 'delisted' (Symbol, ISIN, Company
Name, Board, Delisted Date, Type of Delisting: Voluntary / Compulsory / Delisting - Liquidation / Exit
from ITP platform / ...), plus sheets with notice dates and exit prices for some of them (merged in by
symbol and date). "List of Companies Proposed to be Delisted": Sr.no, Symbol, Name of Company, Promoter.
Despite the file name the delisted list holds every type of delisting; delisting_type says which."""
import html as htmlmod
import io
import re

import openpyxl

from .common import clean, iso, parse_date, review, valid_symbol

PAGE_URL = "https://www.nseindia.com/static/list/list-of-companies-proposed-to-be-delisted"
LINKS = {"delisted": r"delisted\s+from\s+nse", "proposed": r"proposed\s+to\s+be\s+delisted"}
COLUMNS = ["list", "symbol", "isin", "company", "board", "delisting_date", "delisting_type",
           "initial_notice_date", "final_notice_date", "exit_price", "source_file", "needs_review", "review_note"]
FIELDS = [("symbol", r"^symbol"), ("isin", r"^isin"), ("company", r"company|^name of"),
          ("board", r"^board"), ("delisting_date", r"delisted date|date of delisting"),
          ("delisting_type", r"type of delisting"), ("initial_notice_date", r"initial public notice"),
          ("final_notice_date", r"final public notice"), ("exit_price", r"exit price")]
DATE_FIELDS = ("delisting_date", "initial_notice_date", "final_notice_date")


def excel_links(page):
    """{"delisted": url, "proposed": url} from the page's links (whichever are found)."""
    out = {}
    for m in re.finditer(r'<a[^>]+href="([^"]+\.xlsx?)"[^>]*>(.*?)</a>', page, re.I | re.S):
        text = clean(htmlmod.unescape(re.sub(r"<[^>]+>", " ", m.group(2))))
        for key, rx in LINKS.items():
            if key not in out and re.search(rx, text, re.I):
                out[key] = htmlmod.unescape(m.group(1))
    return out


def header_fields(row):
    out = {}
    for i, c in enumerate(row):
        c = clean(c).lower()
        for field, rx in FIELDS:
            if c and field not in out and re.search(rx, c):
                out[field] = i
                break
    return out if "symbol" in out and "company" in out else None


def sheet_records(ws):
    hmap = None
    for row in ws.iter_rows(values_only=True):
        if hmap is None:
            hmap = header_fields(row)
            continue
        rec = {f: (row[i] if i < len(row) else None) for f, i in hmap.items()}
        if not any(v not in (None, "") for v in rec.values()):
            continue
        yield rec


def fmt(field, v):
    if field in DATE_FIELDS:
        d = parse_date(v)
        return iso(d) if d else clean(v).strip("- ")
    if field == "exit_price" and isinstance(v, (int, float)):
        return f"{v:g}"
    return clean(v)


def parse(data, source_file, which, since=None):
    """Rows of one of the Excel files; which = 'delisted' or 'proposed'. Delisted rows before since
    are dropped."""
    wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    merged, order = {}, []
    for ws in wb.worksheets:
        for rec in sheet_records(ws):
            row = {f: fmt(f, v) for f, v in rec.items()}
            if which == "delisted" and not row.get("delisting_date") and not valid_symbol(row.get("symbol", "")):
                continue   # notes under the table
            key = (row.get("symbol", ""), row.get("delisting_date", ""), row.get("company", "") if not row.get("symbol") else "")
            if key in merged:
                for f, v in row.items():
                    if v and not merged[key].get(f):
                        merged[key][f] = v
            else:
                merged[key] = row
                order.append(key)
    wb.close()
    out = []
    for key in order:
        row = merged[key]
        row.update(list=which, source_file=source_file)
        d = parse_date(row.get("delisting_date"))
        if which == "delisted" and since and d and d < since:
            continue
        notes = []
        if not valid_symbol(row.get("symbol", "")):
            notes.append("symbol missing or odd")
        if not row.get("company"):
            notes.append("company missing")
        if which == "delisted" and not d:
            notes.append("delisting date missing or unreadable")
        out.append(review(row, notes))
    return out

"""Orders of (compulsory) delisting -> compulsory_delisting.csv.

The page https://www.nseindia.com/static/list/orders-of-delisting is static HTML (verified 2026-10-14):
one table DATE | SUBJECT | LINK, the subject being the company name and the link the order PDF;
dates like 09-Oct-2026 and sometimes '26- Jun-2025'. The symbol is not in the table: it is taken from
the delisted-companies Excel list by company name, else from the order PDF ('In the matter of the
Company: X Limited (SYMBOL)'); old order PDFs are scans without text."""
import difflib
import html as htmlmod
import re

from .fetch import safe_name
from .common import clean, iso, name_key, parse_date, review, symbol_in_parens, valid_symbol

PAGE_URL = "https://www.nseindia.com/static/list/orders-of-delisting"
COLUMNS = ["order_date", "company", "symbol", "symbol_source", "pdf_url", "source_file", "needs_review",
           "review_note"]


def cell_text(s):
    return clean(htmlmod.unescape(re.sub(r"<[^>]+>", " ", s)))


def parse_page(page):
    """[(date text, company, pdf url)] from the orders table."""
    out = []
    for t in re.findall(r"<table.*?</table>", page, re.I | re.S):
        if not re.search(r"<th[^>]*>\s*DATE\s*</th>", t, re.I) or "SUBJECT" not in t.upper():
            continue
        for tr in re.findall(r"<tr.*?</tr>", t, re.I | re.S):
            tds = re.findall(r"<td[^>]*>(.*?)</td>", tr, re.I | re.S)
            if len(tds) < 3:
                continue
            link = re.search(r'href="([^"]+)"', tds[2], re.I)
            out.append((cell_text(tds[0]), cell_text(tds[1]), htmlmod.unescape(link.group(1)) if link else ""))
    return out


def symbol_from_order(text, company):
    """The symbol in an order PDF: '(SYMBOL)' right after the company's name, else the first one."""
    flat = clean(text)
    head = flat[:3000]
    m = re.search(r"In the matter of(?: the Company)?\s*:?\s*(.{0,150}?)\(\s*([A-Z][A-Z0-9&\-]{1,19})\s*\)", head)
    if m and valid_symbol(m.group(2)):
        return m.group(2)
    return symbol_in_parens(head)


def build(entries, since, symbols_by_name, order_text, page_file=""):
    """Rows from parse_page() entries (source_file: the saved page, or the order PDF the symbol came from).
    symbols_by_name: name_key -> symbol (from the delisted list); order_text(url) -> the PDF's text, '' for a scan, None when it could not be downloaded."""
    out = []
    for date_text, company, url in entries:
        d = parse_date(date_text)
        if since and d and d < since:
            continue
        row = {"order_date": iso(d) or date_text, "company": company, "pdf_url": url, "source_file": page_file}
        notes = []
        if not d:
            notes.append("order date unreadable")
        key = name_key(company)
        sym = symbols_by_name.get(key, "")
        source = "delisted list" if sym else ""
        if not sym and url:
            text = order_text(url)
            if text is None:
                notes.append("order PDF not downloaded (or a broken link)")
            elif not text:
                notes.append("order PDF is a scan (no text)")
            else:
                sym = symbol_from_order(text, company)
                if sym:
                    source, row["source_file"] = "order PDF", safe_name(url)
        if not sym:
            close = difflib.get_close_matches(key, list(symbols_by_name), n=1, cutoff=0.9)
            if close:
                sym, source = symbols_by_name[close[0]], "delisted list (close name)"
                notes = [n for n in notes if not n.startswith("order PDF")]
                notes.append("symbol by close name match, check it")
            else:
                notes.append("symbol not found")
        if not url:
            notes.append("no order PDF link")
        row.update(symbol=sym, symbol_source=source)
        out.append(review(row, notes))
    return out

"""Parsers for the index membership inputs (no network): NSE Indices press releases (PDF), the AMFI half-yearly
average market-cap lists (xlsx) and the niftyindices constituent lists (CSV, current or archived).

A press release is read as a stream of text lines and tables in page order (extract_stream, cached by the build as
JSON); parse_notice walks it: a numbered heading names an index ("2) Nifty 500", "(1) CNX Nifty Index"), "being
excluded" / "being included" sets the action, and the next stock tables (Sr. No. | Company Name | Symbol) give the
symbols. A table headed "Index Name" lists the indices a demerger's spun-off company enters or leaves: such a
notice is reported, never applied."""
import io
import re
from datetime import date

import pdfplumber

STREAM_VERSION = 1   # bump when extract_stream changes (invalidates the build's cache)

SIZES = {"NIFTY 50": 50, "NIFTY NEXT 50": 50, "NIFTY 100": 100, "NIFTY 200": 200, "NIFTY 500": 500,
         "NIFTY MIDCAP 150": 150, "NIFTY SMALLCAP 250": 250, "NIFTY MIDCAP 100": 100}
INDEXES = list(SIZES)
# names in the notices (lower case, without a trailing "index" or "(formerly ...)") -> index. The current NIFTY
# Midcap 100 is the old CNX Midcap: Nifty Midcap 100 (Nov 2015), Nifty Free Float Midcap 100 (Apr 2016), Nifty
# Midcap 100 again from 2018-04-02 (ind_prs05012018.pdf). "Nifty Full Midcap 100" (Apr 2016 to Mar 2018) was another
# index and is not mapped.
ALIASES = {
    "cnx nifty": "NIFTY 50", "nifty 50": "NIFTY 50", "nifty": "NIFTY 50", "s&p cnx nifty": "NIFTY 50",
    "cnx nifty junior": "NIFTY NEXT 50", "nifty junior": "NIFTY NEXT 50", "nifty next 50": "NIFTY NEXT 50",
    "cnx 100": "NIFTY 100", "nifty 100": "NIFTY 100",
    "cnx 200": "NIFTY 200", "nifty 200": "NIFTY 200",
    "cnx 500": "NIFTY 500", "nifty 500": "NIFTY 500",
    "nifty midcap 150": "NIFTY MIDCAP 150", "nifty midcap150": "NIFTY MIDCAP 150",
    "nifty smallcap 250": "NIFTY SMALLCAP 250", "nifty smallcap250": "NIFTY SMALLCAP 250",
    "cnx midcap": "NIFTY MIDCAP 100", "nifty midcap 100": "NIFTY MIDCAP 100",
    "nifty free float midcap 100": "NIFTY MIDCAP 100",
}

MONTHS = {m: i + 1 for i, m in enumerate(["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct",
                                          "nov", "dec"])}
DATE_TEXT = r"([A-Z][a-z]{2,8}\.?\s+\d{1,2}\s*,?\s*\d{4})"
WEEKDAY = r"(?:(?:mon|tues|wednes|thurs|fri|satur|sun)day\s*,?\s*)?"
BECOME = re.compile(r"(?:become|be)\s+effective\s+(?:from|on|w\.e\.f\.?)\s*" + WEEKDAY + DATE_TEXT, re.I)
EFFECTIVE = re.compile(r"effective\s+(?:from|on)\s*" + WEEKDAY + DATE_TEXT, re.I)
WEF = re.compile(r"w\.?\s?e\.?\s?f\.?\s*" + WEEKDAY + DATE_TEXT, re.I)
HEADING = re.compile(r"^\(?(\d{1,2}|[a-z]{1,4})\)\s*(.+?)\s*$")   # 2) Nifty 500, (1) CNX Nifty, a) Nifty 50
SECTION = re.compile(r"^([A-H])\.\s+(.*)$")
EXCLUDED = re.compile(r"\bbeing\s+excluded\b", re.I)
INCLUDED = re.compile(r"\bbeing\s+included\b", re.I)
REVIEW = re.compile(r"periodic review|semi-?\s?annual review|half[- ]yearly review", re.I)
TEXT_ROW = re.compile(r"^(\d{1,3})\s+(.+?)\s+([A-Z0-9][A-Z0-9&\-]{0,19})[*#]*$")   # a table row read as text
DEMERGER = re.compile(r"spun[- ]?off|demerger|spin[- ]?off", re.I)
REPLACES = re.compile(r"stands?\s+replaced|shall\s+stand\s+replaced", re.I)   # "the earlier list ... stands replaced"
SYMBOL_NAMED = re.compile(r"\(\s*symbol\s*:\s*([A-Z0-9][A-Z0-9&\-]*)\s*\)", re.I)
LIST_OUT = re.compile(r"\bexcluded\s+from\b", re.I)
LIST_IN = re.compile(r"\bincluded\s+in\b", re.I)
NO_TEXT = 200   # characters: a notice with less text is an image PDF (text drawn as pictures)


class NoticeError(Exception):
    pass


def parse_date(text):
    """'March 27, 2015' / 'Sept 30, 2016' / 'Oct. 1 2018' -> date, else None."""
    m = re.match(r"([A-Za-z]{3,9})\.?\s+(\d{1,2})\s*,?\s*(\d{4})", text.strip())
    if not m:
        return None
    mon = MONTHS.get(m.group(1)[:3].lower())
    try:
        return date(int(m.group(3)), mon, int(m.group(2))) if mon else None
    except ValueError:
        return None


def dates_in(rx, text):
    out = []
    for m in rx.finditer(text):
        d = parse_date(m.group(1))
        if d and d not in out:
            out.append(d)
    return out


def clean_name(text):
    t = re.sub(r"\s+", " ", text.replace("’", "'")).strip()
    t = re.sub(r"\(\s*formerly[^)]*\)", "", t, flags=re.I)
    t = t.strip().rstrip(":*").strip()
    t = re.sub(r"\s+index$", "", t, flags=re.I).strip()
    return t.lower()


def map_name(name):
    """The index a notice heading names, else None."""
    return ALIASES.get(clean_name(name))


def clean_symbol(text):
    return re.sub(r"[\s*#^@]+", "", str(text or "")).upper()


# ---- PDF -> stream ----

def extract_stream(data):
    """[{"page", "top", "text"} | {"page", "top", "table": [[cell, ...], ...]}] in reading order. Text lines inside a
    table's box are dropped (the table carries them)."""
    items = []
    with pdfplumber.open(io.BytesIO(data)) as pdf:
        for n, page in enumerate(pdf.pages, 1):
            page = page.dedupe_chars()
            tables = page.find_tables()
            boxes = [t.bbox for t in tables]
            for t in tables:
                rows = [[(c or "").replace("\n", " ").strip() for c in row] for row in t.extract()]
                items.append({"page": n, "top": round(t.bbox[1], 1), "table": rows})
            for line in page.extract_text_lines():
                mid = (line["top"] + line["bottom"]) / 2
                if any(b[1] <= mid <= b[3] and b[0] <= (line["x0"] + line["x1"]) / 2 <= b[2] for b in boxes):
                    continue
                items.append({"page": n, "top": round(line["top"], 1), "text": line["text"].strip()})
    items.sort(key=lambda i: (i["page"], i["top"], "table" in i))
    return items


def stream_text(items):
    return "\n".join(i["text"] for i in items if "text" in i)


# ---- notice -> changes ----

def intro_text(items):
    """The text before the first numbered heading (where a notice states when its changes take effect)."""
    lines = []
    for i in items:
        if "text" in i:
            if HEADING.match(i["text"]):
                break
            lines.append(i["text"])
    return "\n".join(lines)


def effective_dates(text, title="", intro=None):
    """[dates] the notice says its changes take effect: 'become effective from', else 'effective from', in the
    introduction (later dates usually belong to other indices or cite earlier notices), else in the whole text, else
    the title's 'w.e.f.'. Several when the introduction's sections differ."""
    for part in ([intro] if intro else []) + [text]:
        for rx in (BECOME, EFFECTIVE):
            found = dates_in(rx, part)
            if found:
                return found
    return dates_in(WEF, title)


def section_dates(items):
    """{index: date} from the 'A. ... effective from DATE' intro sections of a notice with several dates."""
    out, cur = {}, None
    texts = []
    for i in items:
        if "text" not in i:
            continue
        line = i["text"]
        if HEADING.match(line):
            break
        m = SECTION.match(line)
        if m:
            if cur is not None:
                texts.append(cur)
            cur = m.group(2)
        elif cur is not None:
            cur += " " + line
    if cur is not None:
        texts.append(cur)
    for sec in texts:
        found = dates_in(BECOME, sec) or dates_in(EFFECTIVE, sec)
        if len(found) != 1:
            continue
        flat = re.sub(r"\s+", " ", sec)
        for alias, index in ALIASES.items():
            if alias == "nifty":
                continue
            # "Nifty 50" inside "Nifty 50 Equal Weight" is taken as naming the index too
            if re.search(r"(?<![\w&])" + re.escape(alias) + r"(?![\w])", flat, re.I):
                out.setdefault(index, found[0])
    return out


def is_stock_header(row):
    cells = [c.lower() for c in row]
    return any("symbol" in c for c in cells) and any(("name" in c) or ("company" in c) or ("scrip" in c) for c in cells)


def is_index_list(row):
    cells = [c.lower() for c in row]
    return any("index name" in c for c in cells) and not any("symbol" in c for c in cells)


def parse_notice(items, announced, title="", file=""):
    """{"changes", "not_applied", "unresolved", "dates", "review", "headings", "demerger", "replaces", "no_text"}.
    A change: index, effective_date, action (IN / OUT), symbol, company, announced_date, file. not_applied: indices
    named in an "Index Name" table (a corporate action without a stock table). replaces: the notice says an earlier
    list "stands replaced" (its lists replace the earlier notices' lists of the same indices and date)."""
    text = stream_text(items)
    dates = effective_dates(text, title, intro_text(items))
    per_index = section_dates(items) if len(dates) > 1 else {}
    out = {"changes": [], "not_applied": [], "unresolved": [], "dates": dates, "review": bool(REVIEW.search(text)),
           "headings": [], "demerger": bool(DEMERGER.search(text) or DEMERGER.search(title)),
           "replaces": bool(REPLACES.search(text)), "no_text": len(text) < NO_TEXT}

    def when(index):
        if len(dates) == 1:
            return dates[0]
        return per_index.get(index)

    index, action, symbol_col = None, None, None
    unresolved, block = set(), set()
    context = []   # text lines since the last table (an index-name table is explained just above it)

    def add(symbol, company):
        day = when(index)
        if day is None:
            unresolved.add(index)
            return
        sym = clean_symbol(symbol)
        if not sym or sym in block:
            return
        block.add(sym)
        out["changes"].append({"index": index, "effective_date": day, "action": action, "symbol": sym,
                               "company": re.sub(r"\s+", " ", company or "").strip().rstrip("*#").strip(),
                               "announced_date": announced, "file": file})

    for it in items:
        if "text" in it:
            line = it["text"]
            context.append(line)
            h = HEADING.match(line)
            if h:
                index = map_name(h.group(2))
                action, symbol_col, block = None, None, set()
                out["headings"].append((h.group(2), index))
                continue
            if EXCLUDED.search(line):
                action, symbol_col, block = "OUT", None, set()
            elif INCLUDED.search(line):
                action, symbol_col, block = "IN", None, set()
            elif index and action:
                r = TEXT_ROW.match(line)
                if r and not re.search(r"[a-z]", r.group(3)):
                    add(r.group(3), r.group(2))
            continue
        rows = it["table"]
        above, context = " ".join(context), []
        if not rows:
            continue
        if is_index_list(rows[0]):
            # one stock entering / leaving the listed indices. Applied when the text names it ("(Symbol: X) shall be
            # excluded from the following indices", e.g. the Tata Motors DVR in 2024); a demerger's spun-off company
            # (often not trading yet) and anything else is only reported
            named = SYMBOL_NAMED.findall(above)
            act = "OUT" if LIST_OUT.search(above) else "IN" if LIST_IN.search(above) else None
            apply = not out["demerger"] and len(set(s.upper() for s in named)) == 1 and act
            for r in rows[1:]:
                name = next((c for c in reversed(r) if c and not c.isdigit()), "")
                idx = map_name(name)
                if not idx:
                    continue
                if apply:
                    index, action, block = idx, act, set()
                    add(named[0], "")
                else:
                    out["not_applied"].append({"index": idx, "effective_date": when(idx) or (dates[0] if dates else None),
                                               "announced_date": announced, "file": file, "title": title})
            if apply:
                index, action = None, None
            continue
        if index is None or action is None:
            continue
        body = rows
        if is_stock_header(rows[0]):
            symbol_col = next(j for j, c in enumerate(rows[0]) if "symbol" in c.lower())
            name_col = next((j for j, c in enumerate(rows[0]) if ("name" in c.lower() or "company" in c.lower()
                                                                    or "scrip" in c.lower())), None)
            body = rows[1:]
        elif symbol_col is None:
            # a table cut by a page break without its header: Sr. No. | name | symbol
            if not (rows[0] and rows[0][0].strip().rstrip(".").isdigit() and len(rows[0]) >= 3):
                continue
            symbol_col, name_col = len(rows[0]) - 1, 1
        else:
            name_col = 1 if len(rows[0]) > 2 else None
        for r in body:
            if symbol_col >= len(r) or not r[0].strip().rstrip(".").isdigit():
                continue
            add(r[symbol_col], r[name_col] if name_col is not None and name_col < len(r) else "")
    out["unresolved"] = sorted(unresolved)
    return out


# ---- AMFI ----

CATEGORY = {"large cap": "LARGE", "mid cap": "MID", "small cap": "SMALL", "largecap": "LARGE", "midcap": "MID",
            "smallcap": "SMALL"}
AMFI_COLUMNS = {"rank": ("sr. no", "sr no", "sl. no", "rank"), "company": ("company name", "company"),
                "isin": ("isin",), "bse_symbol": ("bse symbol", "bse code", "bse scrip"),
                "symbol": ("nse symbol",), "avg_mcap": ("average of all exchanges",),
                "category": ("categori", "category")}
PERIOD_TITLE = re.compile(r"ended\s+(\d{1,2})(?:st|nd|rd|th)?[\s\-]*([A-Za-z]{3,9})[\s,\-]*(\d{4})", re.I)
PERIOD_FILE = [
    (re.compile(r"(\d{2})[_\s-]*([A-Za-z]{3,9})[_\s-]*(\d{4})", re.I), "dmy"),   # 31Dec2024, 30_Jun2024, 30June2022
    (re.compile(r"(jan|jul)[a-z]*[\s_-]*-?[\s_-]*(jun|dec)[a-z]*[\s_-]*(\d{4})", re.I), "range"),  # Jul-Dec 2017
]


def period_end(title, file_name=""):
    """The last day of the half year a list covers, from its title, else its file name; None if neither."""
    m = PERIOD_TITLE.search(title or "")
    if m:
        mon = MONTHS.get(m.group(2)[:3].lower())
        if mon:
            return date(int(m.group(3)), mon, 30 if mon == 6 else 31)
    for rx, kind in PERIOD_FILE:
        m = rx.search(file_name or "")
        if not m:
            continue
        if kind == "dmy":
            mon = MONTHS.get(m.group(2)[:3].lower())
            if mon in (6, 12):
                return date(int(m.group(3)), mon, 30 if mon == 6 else 31)
        else:
            mon = 6 if m.group(2)[:3].lower() == "jun" else 12
            return date(int(m.group(3)), mon, 30 if mon == 6 else 31)
    return None


def period_label(end):
    return f"{end.year}H{1 if end.month == 6 else 2}"


def number(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    t = str(value).replace(",", "").strip()
    try:
        return float(t)
    except ValueError:
        return None


def text(value):
    if value is None:
        return ""
    t = str(value).strip()
    return "" if t in ("-", "--", "NA", "N.A.") else t


def read_amfi(data, file_name=""):
    """(period_end, rows, how): rows = [{rank, company, isin, symbol, bse_symbol, avg_mcap, category}];
    how = 'column' when the file has a category column, else 'rank' (1-100 LARGE, 101-250 MID, 251+ SMALL)."""
    import openpyxl
    wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    try:
        ws = wb.worksheets[0]
        rows = list(ws.iter_rows(values_only=True))
    finally:
        wb.close()
    head_at, cols = None, {}
    for i, row in enumerate(rows[:15]):
        cells = [text(c).lower() for c in row]
        if "isin" in cells and any("company" in c for c in cells):
            head_at = i
            for key, names in AMFI_COLUMNS.items():
                for j, c in enumerate(cells):
                    if any(c.startswith(n) for n in names) and key not in cols:
                        cols[key] = j
            break
    if head_at is None:
        raise NoticeError(f"{file_name}: no header row with ISIN and Company name")
    missing = [k for k in ("rank", "company", "isin", "symbol") if k not in cols]
    if missing:
        raise NoticeError(f"{file_name}: header lacks {', '.join(missing)}")
    title = " ".join(text(c) for row in rows[:head_at] for c in row if text(c))
    end = period_end(title, file_name)
    if end is None:
        raise NoticeError(f"{file_name}: no period end in the title {title[:100]!r} or the file name")
    how = "column" if "category" in cols else "rank"
    out = []
    for row in rows[head_at + 1:]:
        cell = (lambda k: row[cols[k]] if k in cols and cols[k] < len(row) else None)
        if not text(cell("company")) and not text(cell("isin")):
            continue   # empty or numbered-but-empty rows at the end
        rank = number(cell("rank"))
        if rank is None or rank != int(rank):
            # the H2-2017 file stops numbering after rank 3452: the rows stay in rank order
            rank = out[-1]["rank"] + 1 if out else 1
        rank = int(rank)
        if how == "column":
            cat = CATEGORY.get(text(cell("category")).lower(), "")
        else:
            cat = "LARGE" if rank <= 100 else "MID" if rank <= 250 else "SMALL"
        out.append({"rank": rank, "company": text(cell("company")), "isin": text(cell("isin")).upper(),
                    "symbol": text(cell("symbol")).upper(), "bse_symbol": text(cell("bse_symbol")),
                    "avg_mcap": number(cell("avg_mcap")), "category": cat})
    return end, out, how


# ---- constituent lists ----

def read_constituents(data):
    """[{symbol, isin, company, series, industry}] of a niftyindices constituent CSV (current or archived)."""
    import csv
    body = data.decode("utf-8-sig", "replace")
    rows = list(csv.DictReader(io.StringIO(body)))
    out = []
    for r in rows:
        r = {(k or "").strip(): (v or "").strip() for k, v in r.items()}
        sym = clean_symbol(r.get("Symbol", ""))
        if not sym:
            continue
        out.append({"symbol": sym, "isin": r.get("ISIN Code", "").upper(), "company": r.get("Company Name", ""),
                    "series": r.get("Series", ""), "industry": r.get("Industry", "")})
    return out

"""Shared parsing helpers: dates, symbols, company names, circular tables, CSV output."""
import csv
import os
import re
from datetime import date, datetime

MONTHS = {m: i for i, m in enumerate(["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct",
                                      "nov", "dec"], 1)}
MONTH = r"(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|June?|July?|Aug(?:ust)?|Sept?(?:ember)?|" \
        r"Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
# "March 09, 2023", "October 17,2019", "December\n24, 2015"; "17th October 2019"; "26- Jun-2025"
DATE_PATTERNS = [re.compile(MONTH + r"\.?\s+(\d{1,2})(?:st|nd|rd|th)?\s*,?\s*(\d{4})", re.I),
                 re.compile(r"(\d{1,2})(?:st|nd|rd|th)?\s+" + MONTH + r",?\s+(\d{4})", re.I),
                 re.compile(r"(\d{1,2})\s*-\s*" + MONTH + r"\s*-\s*(\d{4})", re.I)]
SYMBOL_RE = re.compile(r"^[A-Z0-9&\-]{1,20}$")
SERIES = {"EQ", "BE", "BZ", "SM", "ST", "BL", "EQ/BL", "EQ / BL", "IL", "E1", "X1"}
YES = "Y"


def to_date(m):
    """A date from one of DATE_PATTERNS' matches, or None."""
    g = [x for x in m.groups() if x]
    if g[0][:1].isdigit():
        day, mon, year = g
    else:
        mon, day, year = g
    try:
        return date(int(year), MONTHS[mon[:3].lower()], int(day))
    except (ValueError, KeyError):
        return None


def parse_date(value):
    """date from a cell or text: a datetime, YYYYMMDD, YYYY-MM-DD or one of DATE_PATTERNS."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    s = re.sub(r"\s+", " ", str(value or "")).strip()
    if re.fullmatch(r"\d{8}", s):
        try:
            return datetime.strptime(s, "%Y%m%d").date()
        except ValueError:
            return None
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return date.fromisoformat(s)
    for p in DATE_PATTERNS:
        m = p.search(s)
        if m:
            return to_date(m)
    return None


def dates_after(text, lead):
    """Dates that follow the regex lead (within a few characters), in order."""
    flat = re.sub(r"\s+", " ", text or "")
    out = []
    for m in re.finditer(lead, flat, re.I):
        tail = flat[m.end():m.end() + 40]
        hits = [mm for p in DATE_PATTERNS for mm in [p.search(tail)] if mm and mm.start() <= 6]
        if hits:
            d = to_date(min(hits, key=lambda mm: mm.start()))
            if d:
                out.append(d)
    return out


def first_after(text, *leads):
    """The first date following any of leads (tried in order)."""
    for lead in leads:
        found = dates_after(text, lead)
        if found:
            return found[0]
    return None


def iso(d):
    return d.isoformat() if d else ""


def clean(s):
    """One line, single spaces, NSE's broken dashes (mojibake) as '-'."""
    s = str(s or "").replace("�", "-").replace("–", "-").replace("—", "-").replace("\xa0", " ")
    s = re.sub(r"-{2,}", "-", s)
    return re.sub(r"\s+", " ", s).strip()


def valid_symbol(s):
    return bool(SYMBOL_RE.match(s or "")) and s not in SERIES


def name_key(name):
    """Company name for matching across sources: lower case, Ltd = Limited, no punctuation."""
    s = clean(name).lower().replace("&", " and ")
    s = re.sub(r"\bltd\b\.?", "limited", s)
    s = re.sub(r"\(\s*i\s*\)", "(india)", s)
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\bthe\b", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def symbol_in_parens(text):
    """A symbol written as '(SYMBOL)' or '(Symbol: SYMBOL)' in text, or ''."""
    for m in re.finditer(r"\(\s*(?:Symbol\s*[:\-]?\s*)?([A-Z][A-Z0-9&\-]{1,19})\s*\)", text or ""):
        if valid_symbol(m.group(1)) and m.group(1) not in NOT_SYMBOLS:
            return m.group(1)
    return ""


NOT_SYMBOLS = {"NCLT", "IBC", "SEBI", "LODR", "NSE", "NSEIL", "SOP", "CP", "IB", "MB", "SCRR", "ICDR", "BSE", "CIRP",
               "IRP", "RP", "IA", "SAT", "NCLAT", "ISIN", "PAN", "EQ", "BE", "BZ", "SM", "ST"}
PAREN_SYMBOL = r"\(\s*(?:Symbol\s*[:\-]?\s*)?[A-Z][A-Z0-9&\-]{1,19}\s*\)"


def symbols_in_cell(cell):
    """(symbols, info) from a table's Symbol cell: footnote marks (* #) and quotes removed, a symbol
    wrapped onto two lines ('WINSOME DJ') joined, lower case raised, several symbols ('"A" & "B"') split."""
    # '&' inside a symbol is real (SB&TINTL); it separates symbols only between quotes or spaces
    pieces = re.split(r"[\"\u201c\u201d]\s*&\s*[\"\u201c\u201d]|\s+&\s+|\s*,\s*|\s+and\s+|\s*/\s*", cell or "")
    s = [re.sub(r"[*#^\"\u201c\u201d']", " ", p).strip() for p in pieces]
    info = ["footnote mark removed from the symbol"] if re.search(r"[*#^]", cell or "") else []
    parts = [p for p in s if p]
    out = []
    for p in parts:
        joined = p.replace(" ", "")
        if " " in p and valid_symbol(joined.upper()):
            info.append("symbol joined from a wrapped cell")
        if joined != joined.upper() and valid_symbol(joined.upper()):
            info.append("symbol upper-cased")
        out.append(joined.upper() if valid_symbol(joined.upper()) else p)
    if len(out) > 1:
        info.append(f"several symbols in one row: {', '.join(out)}")
    return out or [""], info


def strip_marks(company):
    """A company name without footnote marks ('Vimal Oil & Foods Ltd#')."""
    return re.sub(r"\s*[*#^]+\s*", " ", company or "").strip()


def strip_symbol(company):
    """A company name without a '(SYMBOL)' in it ('Moser-Baer (I) Limited' keeps its '(I)')."""
    return re.sub(r"\s+", " ", re.sub(PAREN_SYMBOL, " ", company or "")).strip()


def body(text):
    """A circular's text after its header and 'Sub:' line (and the subject's wrapped second line)."""
    lines = (text or "").splitlines()
    for i, line in enumerate(lines):
        if re.match(r"\s*sub(?:ject)?\s*[:.\-]", line, re.I):
            rest = lines[i + 1:]
            if rest and rest[0][:1].islower():
                rest = rest[1:]
            return "\n".join(rest)
    return text or ""


COMPANY_END = r"(?:Limited|Ltd\.?)"


def companies_in_text(text):
    """Companies a circular lists without a table: numbered lines under a 'Name of the Company' heading
    ('1. Crest Animation Studios Limited.', wrapped lines joined), else a sentence 'securities of A
    Limited, B Limited and C Limited will be suspended'."""
    lines = body(text).splitlines()
    out, cur, started = [], None, False
    for line in lines:
        line = line.strip()
        if not started:
            started = bool(re.search(r"name of (?:the )?compan", line, re.I))
            continue
        m = re.match(r"^(\d{1,3})[.)]?\s+(\S.*)$", line)
        if m:
            if cur:
                out.append(cur)
            cur = m.group(2)
        elif cur and line and not re.search(COMPANY_END + r"\.?$", cur) and not re.match(r"(This|For|All|The|Members)\b", line):
            cur += " " + line
        elif cur:
            out.append(cur)
            cur = None
            break
    if cur:
        out.append(cur)
    out = [c.strip(" .;") for c in out if re.search(COMPANY_END + r"\.?$", c.strip(" .;") + ".")]
    out = [c for c in out if re.search(COMPANY_END + "$", c)]
    if out:
        return out
    flat = clean(body(text))
    m = re.search(r"(?i:securities|shares) of ((?:[A-Z][^,;:]*?" + COMPANY_END + r"(?:,\s*|\s+and\s+|\s+&\s+))+[A-Z][^,;:]*?"
                  + COMPANY_END + r")\s+(?:will|shall|is|are|has|have)\b", flat)
    if m:
        return split_companies(m.group(1))
    return []


def split_companies(s):
    """'A Limited, B Limited and C Limited' (or 'A Limited & B Limited') -> the names (one name comes back
    alone; an '&' inside a name, 'Johnson & Johnson Limited', is kept)."""
    if len(re.findall(COMPANY_END, s or "")) < 2:
        return [s] if s else []
    return [c.strip() for c in re.split(r"(?<=Limited)\s*,\s*|(?<=Ltd\.)\s*,\s*|(?<=Ltd)\s*,\s*|\s+and\s+(?=[A-Z])|"
                                        r"(?<=Limited)\s+&\s+|(?<=Ltd\.)\s+&\s+|(?<=Ltd)\s+&\s+", s)
            if c.strip()]


def company_in_text(text):
    """The first 'X Limited' in a circular's body that is not the exchange, SEBI or a depository."""
    for m in re.finditer(r"((?:[A-Z][\w&.'()\-]*\s+){1,8}(?:Limited|Ltd\.?))", clean(body(text))):
        name = m.group(1)
        name = re.sub(r"^(?:(?:The|That|Hence|Further|As|Whereas|Since|In|Of|M/s\.?)\s+)+", "", name)
        if not re.search(r"Exchange|Depositor|Clearing|Securities and|Board of India", name):
            return name.strip()
    return ""


def company_from_subject(subject):
    """The company in a circular subject: 'Suspension of trading in equity shares - X Limited (SYM)',
    '... of X Limited for non-compliance', 'Delisting - X Limited'."""
    s = clean(subject)
    m = re.search(r"\s-\s*(.+)$", s) or re.search(r".*(?:shares|securities|delisting)\s+of\s+(?:the\s+)?(.+)$", s, re.I)
    if not m:
        return ""
    c = m.group(1)
    c = strip_symbol(c)
    c = re.split(r"\s+(?:w\.?e\.?f\.?|with effect|for non|pursuant|on account)\b", c, flags=re.I)[0]
    c = c.strip(" .,-")
    if not re.search(r"(limited|ltd\.?|bank|corporation|company|industries|inc\.?)$", c, re.I):
        return ""
    if re.match(r"(?:the\s+)?compan(?:y|ies)\b", c, re.I) or re.search(r"\b\d+\s+companies\b", c, re.I):
        return ""
    return c


# --- circular tables ---------------------------------------------------------------------------

HEADER_KEYS = [("symbol", r"symbol"), ("company", r"name|company"), ("series", r"series"),
               ("isin", r"isin"), ("reason", r"reason|nclt|order|remark|purpose"), ("sr", r"^s(?:r)?\.?\s*no")]


def header_map(row):
    """{field: column} for a table header row, or None if it is not a company table header."""
    cells = [clean(c).lower() for c in row]
    out = {}
    for i, c in enumerate(cells):
        if not c:
            continue
        for field, rx in HEADER_KEYS:
            if field not in out and re.search(rx, c):
                if field == "company" and "symbol" in c:
                    continue
                out[field] = i
                break
    if "symbol" in out or ("company" in out and "sr" in out):
        return out
    return None


def table_rows(tables):
    """Company rows of the circular's tables: dicts with symbol, company, series, isin, reason."""
    out = []
    for t in tables:
        hmap, hcells = None, None
        for row in t:
            if not row:
                continue
            h = header_map(row)
            if h:
                hmap, hcells = h, row
                continue
            if hmap is None:
                continue
            cells = list(row)
            m = hmap
            # header with blank spacer cells (and data with None ones): map by the non-empty cells
            if any(c in (None, "") for c in cells) and any(c in (None, "") for c in hcells):
                hc = [c for c in hcells if clean(c)]
                dc = [c for c in cells if clean(c)]
                if len(hc) == len(dc):
                    m, cells = header_map(hc) or m, dc
            rec = {f: clean(cells[i]) if i < len(cells) else "" for f, i in m.items()}
            if not (rec.get("symbol") or rec.get("company")):
                continue
            if rec.get("sr") and not re.match(r"^\d+\.?$", rec["sr"]):
                continue
            out.append(rec)
    return out


# --- output ------------------------------------------------------------------------------------

def write_csv(path, columns, rows):
    """Atomic UTF-8 CSV with LF line endings."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore", lineterminator="\n")
        w.writeheader()
        for r in rows:
            w.writerow({c: r.get(c, "") for c in columns})
    os.replace(tmp, path)


def review(row, notes, info=()):
    """Set needs_review from notes (empty = fine); review_note lists notes, then info (which alone does
    not ask for a review, e.g. where a value came from)."""
    notes = [n for n in dict.fromkeys(notes) if n]
    row["needs_review"] = YES if notes else ""
    row["review_note"] = "; ".join(notes + [n for n in dict.fromkeys(info) if n and n not in notes])
    return row

"""NSE circulars: the yearly lists, which ones to read, and the delisting circulars ->
delisting_circulars.csv. Suspension circulars are read by suspensions.py."""
import difflib
import json
import re
from datetime import date

from .common import (clean, companies_in_text, split_companies, company_from_subject, company_in_text, first_after, iso, name_key, parse_date,
                     review, strip_marks, strip_symbol, symbol_in_parens, symbols_in_cell, table_rows,
                     valid_symbol)
from .fetch import safe_name

API = "https://www.nseindia.com/api/circulars?fromDate={f:%d-%m-%Y}&toDate={t:%d-%m-%Y}"
DELISTING_DEPTS = {"Listing", "Emerge"}   # Emerge (SME) circulars are Listing's (file dept CML), often both
# Every word is bounded (\b): a bare 'unit' matched 'United Bank of India' (fixed 2026-10-15)
NON_EQUITY = r"\bbonds?\b|\bdebentures?\b|\bncds?\b|\bnon[- ]convertible\b|\bt-?\s?bills?\b|\btreasury\b|" \
             r"\bgovernment\b|\bg-?secs?\b|\bcommercial papers?\b|\bsdls?\b|\bunits?\b|\bunites\b|\bmutual funds?\b|" \
             r"\basset management\b|\betfs?\b|\bexchange traded funds?\b|\binvits?\b|\breits?\b"
SUSPENSION_SKIPS = [("revocation / cancellation", r"\brevoc\w*|\bcancel\w*|\bwithdraw\w*|\blifting\b|\bresumption\b"),
                    ("member suspension", r"\bmembers?\b|\bmembership\b"),
                    ("not a stock (futures, repo, preference shares, warrants...)",
                     r"\bfutures\b|\boptions\b|\bcommodit\w*|\brepo\b|clearing & settlement|\bpreference shares?\b|"
                     r"\bwarrants?\b|\bocrps\b|\bocps\b|\bccps\b|availability of trading|\bnon-?\s*suspension\b|"
                     r"\brights entitlements?\b"),
                    ("not equity (debt, units, funds)", NON_EQUITY)]
DELISTING_SKIPS = [("Dissemination Board", r"\bdiss\w*\s+board\b"),
                   ("delisting offer / calendar", r"\boffer\b|\bsettlement calendar\b|\bbidding\b|\bprice discovery\b"),
                   ("restriction notice", r"\brestriction in trading\b"),
                   ("not equity (debt, units, funds)", NON_EQUITY)]
COLUMNS = ["circular_no", "circular_date", "department", "subject", "delisting_type", "symbol", "company",
           "effective_date", "pdf_url", "source_file", "needs_review", "review_note"]
# "decided to delist (withdraw the admission to dealings in) ... w.e.f. DATE", "withdrawn (delisted) w.e.f. DATE"
DELIST_LEAD = r"delist\w*\)?[^.]{0,80}?(?:w\.?\s?e\.?\s?f\.?|with effect from)(?:\s*from)?"
WEF = r"(?:w\.?\s?e\.?\s?f\.?|with effect from)(?:\s*from)?"
EFFECTIVE = r"(?:shall|will) be effective from|effective from"


def year_list(year, today):
    """(url, file name) of a year's circular list: a past year is saved once as circulars_<year>.json;
    the current one as circulars_<year>_fetched_<today>.json, so it is fetched at most once a day."""
    if year < today.year:
        return API.format(f=date(year, 1, 1), t=date(year, 12, 31)), f"circulars_{year}.json"
    return API.format(f=date(year, 1, 1), t=today), f"circulars_{year}_fetched_{today.isoformat()}.json"


def entries(data):
    """Circulars of a list (bytes of the API's JSON) as dicts no, date, dept, subject, url."""
    rows = json.loads(data.decode("utf-8-sig"))
    rows = rows.get("data", []) if isinstance(rows, dict) else rows
    out = []
    for r in rows:
        out.append({"no": clean(r.get("circDisplayNo")), "date": parse_date(r.get("cirDate")),
                    "dept": clean(r.get("circDepartment")), "subject": clean(r.get("sub")),
                    "url": clean(r.get("circFilelink"))})
    return out


def skip_reason(subject, skips):
    s = subject.lower().replace("_", " ")   # 'Suspension of trading in bonds_IDFC Limited'
    for label, rx in skips:
        if re.search(rx, s, re.I):
            return label
    return ""


CITED_RX = re.compile(r"\b(NSE|NCL)\s*/\s*([A-Z]{2,6})\s*/\s*(\d{4,6})\b")


def cited(text):
    """Circular numbers ('NSE/CML/64455') a circular's text refers to, in order."""
    out = []
    for m in CITED_RX.finditer(text or ""):
        no = f"{m.group(1)}/{m.group(2)}/{m.group(3)}"
        if no not in out:
            out.append(no)
    return out


def only_non_equity(nos, subjects):
    """True when every circular in nos is in subjects (no -> subject) and is about debt, units or funds."""
    return bool(nos) and all(n in subjects and re.search(NON_EQUITY, subjects[n], re.I) for n in nos)


def pick(all_entries, since):
    """(delisting circulars, suspension circulars, skipped counts) from the lists, oldest first, one per
    circular number."""
    delist, susp, skipped, seen = [], [], {}, set()
    for e in sorted(all_entries, key=lambda e: (e["date"] or date.min, e["no"])):
        if (since and e["date"] and e["date"] < since) or not e["url"]:
            continue
        s = e["subject"].lower()
        if "delist" in s and e["dept"] in DELISTING_DEPTS:
            why = skip_reason(s, DELISTING_SKIPS)
            if why:
                skipped[f"delisting: {why}"] = skipped.get(f"delisting: {why}", 0) + 1
            elif ("d", e["no"]) not in seen:
                seen.add(("d", e["no"]))
                delist.append(e)
        if "suspen" in s:
            why = skip_reason(s, SUSPENSION_SKIPS)
            if why:
                skipped[f"suspension: {why}"] = skipped.get(f"suspension: {why}", 0) + 1
            elif ("s", e["no"]) not in seen:
                seen.add(("s", e["no"]))
                susp.append(e)
    return delist, susp, skipped


def companies(entry, doc, symbols_by_name):
    """[[symbol, company, row dict from the table or {}, notes, info]] named by a circular: its table
    rows, else the subject. A missing symbol is looked up by company name in symbols_by_name."""
    rows = table_rows(doc["tables"]) if doc else []
    found = []
    for r in rows:
        comp = strip_marks(r.get("company", ""))
        syms, info = symbols_in_cell(r.get("symbol", ""))
        if comp and not syms[0]:
            syms = [symbol_in_parens(comp)]
        for sym in syms:
            found.append([sym, strip_symbol(comp), r, [], list(info)])
    if not found:
        text = doc["text"] if doc else ""
        notes = [] if text else ["PDF has no text (scan)" if doc else "PDF not downloaded"]
        info = ["no company table; taken from the subject"] if text else []
        listed = companies_in_text(text) if text else []
        if len(listed) > 1:
            for comp in listed:
                found.append([symbol_in_parens(comp), strip_symbol(comp), {}, list(notes), ["company list from the text"]])
            return with_symbols(found, symbols_by_name)
        comp = company_from_subject(entry["subject"])
        several = split_companies(comp)
        if len(several) > 1:
            for c in several:
                found.append(["", c, {}, list(notes), ["several companies in the subject"]])
            return with_symbols(found, symbols_by_name)
        sym = symbol_in_parens(entry["subject"])
        bare = re.search(r"\s-\s*([A-Z][A-Z0-9&\-]{1,19})\s*$", entry["subject"])   # 'Delisting - ESSARPORTS'
        if not sym and bare and valid_symbol(bare.group(1)):
            sym = bare.group(1)
        if not comp and text:
            comp = company_in_text(text)
            if comp:
                notes.append("no company table; company taken from the text")
                info = []
        if comp and not sym and text:
            m = re.search(re.escape(comp) + r"\s*\(\s*(?:Symbol\s*[:\-]?\s*)?([A-Z][A-Z0-9&\-]{1,19})\s*\)", clean(text))
            sym = m.group(1) if m and valid_symbol(m.group(1)) else ""
        found.append([sym, comp, {}, notes, info])
    return with_symbols(found, symbols_by_name)


def with_symbols(found, symbols_by_name):
    """Fill missing symbols by company name from the delisted list (a close name is flagged)."""
    for f in found:
        if not f[0] and f[1]:
            key = name_key(f[1])
            f[0] = symbols_by_name.get(key, "")
            if f[0]:
                f[4].append("symbol looked up by company name")
            else:
                close = difflib.get_close_matches(key, list(symbols_by_name), n=1, cutoff=0.9)
                if close:
                    f[0] = symbols_by_name[close[0]]
                    f[3].append("symbol by close name match, check it")
    return found


def learn_symbols(rows, lookup, sym_field="symbol", comp_field="company"):
    """Add name -> symbol pairs that circular rows show together (the symbol from the circular itself)."""
    for r in rows:
        sym, comp = r.get(sym_field, ""), r.get(comp_field, "")
        guessed = "symbol looked up" in r["review_note"] or "close name" in r["review_note"]
        if sym and comp and valid_symbol(sym) and not guessed:
            lookup.setdefault(name_key(comp), sym)


def row_notes(sym, comp, eff, circ_date, what="effective date"):
    notes = []
    if not sym:
        notes.append("symbol missing")
    elif not valid_symbol(sym):
        notes.append("symbol looks odd")
    if not comp:
        notes.append("company missing")
    if not eff:
        notes.append(f"{what} not found")
    elif circ_date and (eff - circ_date).days < -7:
        notes.append(f"{what} before the circular date")
    return notes


def delisting_type(subject, text):
    s = subject.lower()
    t = (subject + " " + text).lower()
    if re.search(r"withdrawal of delisting|revocation of delisting", s):
        return "withdrawal"
    if re.search(r"stayed|put on hold|kept in abeyance|kept on hold", t):
        return "on hold"
    if re.search(r"liquidat|winding[ -]up", t):
        return "liquidation"
    if re.search(r"resolution plan|insolvency|\bibc\b", t):
        return "ibc"
    if "compulsor" in t:
        return "compulsory"
    if "voluntar" in t:
        return "voluntary"
    return "other"


def parse_delisting(entry, doc, symbols_by_name):
    """Rows of one delisting circular (one per company)."""
    text = doc["text"] if doc else ""
    eff = (first_after(text, DELIST_LEAD) or first_after(entry["subject"], WEF)
           or first_after(text, WEF, EFFECTIVE))
    base = {"circular_no": entry["no"], "circular_date": iso(entry["date"]), "department": entry["dept"],
            "subject": entry["subject"], "pdf_url": entry["url"], "source_file": safe_name(entry["url"]),
            "effective_date": iso(eff)}
    out = []
    for sym, comp, r, notes, info in companies(entry, doc, symbols_by_name):
        row = dict(base, symbol=sym, company=comp,
                   delisting_type=delisting_type(entry["subject"], (r.get("reason", "") + " " + text)))
        out.append(review(row, notes + row_notes(sym, comp, eff, entry["date"]), info))
    return out

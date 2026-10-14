"""Suspension circulars -> mergers.csv (scheme / amalgamation / merger, decided from the PDF's text, not
the subject) and suspensions.csv (every other reason, with a short label and the sentence it came from).

Verified 2026-10-14: a merger suspension reads 'the following security will be suspended from trading
w.e.f. March 09, 2023 (i.e., closing hours of trading on March 08, 2023)' with a table Name of the
Company | Symbol | Series | Reason for Suspension = 'Scheme of Amalgamation'. The new company and the
swap ratio are rarely there (ING Vysya: 'Scheme of Amalgamation with Kotak Mahindra Bank Limited');
reference/manual_mergers.csv fills them."""
import csv
import os
import re

from .circulars import EFFECTIVE, WEF, cited, companies, only_non_equity, row_notes
from .common import body, clean, first_after, iso, name_key, review, valid_symbol
from .fetch import safe_name

MERGER_RX = r"amalgamat|\bmerger\b|\bmerged\b|scheme of arrangement|composite scheme|scheme of (?:compromise|reconstruction)"
REASONS = [("partly paid call", r"partly[ -]paid|final call"),
           ("liquidation", r"liquidat|winding[ -]up|wound up"),
           ("IBC / resolution plan", r"resolution plan|insolvency|\bibc\b"),
           ("court / NCLT order", r"\bnclt\b|tribunal|court|restrain"),
           ("delisting", r"delist"),
           ("non-compliance with listing rules", r"non[- ]?complian|not submitted|regulation 33|regulation 55a|"
                                                 r"listing agreement|lodr|failed to comply|regulation 76|\bsop\b"),
           ("not responding to the exchange", r"not (?:been )?respond|show cause"),
           ("capital reduction", r"reduction of (?:share )?capital|capital reduction"),
           ("SEBI / surveillance order", r"surveillance|sebi order|interim order|forensic")]
WEAK = {"court / NCLT order"}   # used only when no other reason is found
# exit_expected is a guess from the circular (whether the stock traded again comes later from bhavcopies):
# Y for these suspension reasons and for schemes with an amalgamation / merger part
EXIT_REASONS = {"delisting", "liquidation", "IBC / resolution plan"}
SCHEME_PARTS = [("MERGER", r"amalgamat|\bmerger\b|\bmerged\b"), ("DEMERGER", r"demerg"),
                ("CAPITAL_REDUCTION", r"capital reduction|reduction (?:of|in) (?:the )?(?:share |equity )?capital")]
# status from the subject: an earlier (proposed) suspension called off, or a suspension that goes on
STATUS = [("CANCELLED", r"not to be carried out"), ("CONTINUED", r"continuation of suspension")]
# a corrigendum or a changed date (by the subject, or by these words in the text; a 'Corrigendum order' of
# the NCLT inside the text is not one): its new dates follow CORRECTED_LEAD ('shall be considered as Oct 28, 2024')
CORRECTION_SUBJECT = r"corrigendum|change in (?:the )?effective date"
CORRECTION_RX = r"stands revised|revised (?:as|to)|shall be considered as"
CORRECTED_LEAD = r"(?:shall be considered as|stands revised (?:as|to)|revised (?:as|to)|instead of)"
MERGER_COLUMNS = ["circular_no", "circular_date", "department", "old_symbol", "old_company", "scheme_type",
                  "exit_expected", "new_company", "new_symbol", "swap_ratio", "new_per_old", "suspended_from",
                  "last_trading_date", "record_date", "reason_text", "corrected_by", "pdf_url", "source_file",
                  "needs_review", "review_note"]
SUSPENSION_COLUMNS = ["circular_no", "circular_date", "department", "status", "symbol", "company", "effective_date",
                      "reason", "exit_expected", "reason_text", "cancels_circular", "corrected_by", "subject", "pdf_url",
                      "source_file", "needs_review", "review_note"]
MANUAL_FILE = "manual_mergers.csv"
# scheme_type / exit_expected are optional overrides of the parsed values; note is for the person editing
MANUAL_COLUMNS = ["old_symbol", "new_company", "new_symbol", "swap_ratio", "scheme_type", "exit_expected", "note"]
MANUAL_EXAMPLE = {"old_symbol": "HDFC", "new_company": "HDFC Bank Limited", "new_symbol": "HDFCBANK",
                  "swap_ratio": "42 for 25"}
SCHEME_TYPE_RX = r"^(?:OTHER|(?:MERGER|DEMERGER|CAPITAL_REDUCTION)(?:\+(?:MERGER|DEMERGER|CAPITAL_REDUCTION))*)$"
SUSPEND_LEAD = r"suspen\w*[^.]{0,60}?" + WEF
LAST_TRADING = r"closing hours? of trading on"
RATIO_RX = re.compile(r"(\d+(?:\.\d+)?)\s*(?:\([a-z ]+\)\s*)?(?:new\s+|fully[ -]paid[ -](?:up\s+)?)*equity\s+shares?"
                      r"[^.]{0,150}?for\s+every\s+(\d+(?:\.\d+)?)", re.I)
NEW_COMPANY_RX = re.compile(r"(?:amalgamation|merger)\s+(?:of\s+.{0,80}?\s+)?(?:with|into)\s+(?:the\s+)?"
                            r"(.{3,80}?(?:Limited|Ltd\.?|Bank(?:\s+Limited)?))(?:\s*\(\s*([A-Z][A-Z0-9&\-]{1,19})\s*\))?", re.I)


def sentence_with(text, rx):
    """The sentence of text matching rx, trimmed to 200 characters."""
    flat = clean(text)
    m = re.search(rx, flat, re.I)
    if not m:
        return ""
    dot = flat.rfind(". ", 0, m.start())
    start = dot + 2 if dot >= 0 else 0
    end = flat.find(". ", m.end())
    end = end + 1 if end >= 0 else len(flat)
    if end - start <= 200:
        return flat[start:end].strip()
    a = max(start, m.start() - 80)   # keep the words around the match
    s = flat[a:min(end, a + 194)].strip()
    return ("..." if a > start else "") + s + ("..." if a + 194 < end else "")


def classify(reason_cell, text, subject=""):
    """('merger' | 'other', label, reason text, extra notes). The row's 'Reason for Suspension' cell
    decides when it names a known reason, else the circular's text (after its header), else the subject."""
    text = body(text)
    weak = None
    for src in (reason_cell, text, subject):
        if not src:
            continue
        merger = re.search(MERGER_RX, src, re.I)
        others = [(label, rx) for label, rx in REASONS if re.search(rx, src, re.I)]
        why = reason_cell if src is reason_cell else subject if src is subject else None
        if merger:
            serious = [o for o, _ in others if o in ("liquidation", "IBC / resolution plan")]
            notes = [f"text also mentions {serious[0]}"] if serious else []
            return "merger", "merger", why or sentence_with(text, MERGER_RX), notes
        strong = [(label, rx) for label, rx in others if label not in WEAK]
        if strong:
            label, rx = strong[0]
            return "other", label, why or sentence_with(text, rx), []
        if others and weak is None:   # e.g. only 'NCLT': keep looking for the actual reason
            label, rx = others[0]
            weak = ("other", label, why or sentence_with(text, rx), [])
    if weak:
        return weak
    return "other", "", reason_cell or sentence_with(text, r"suspen"), ["reason not recognised"]


def scheme_type(text):
    """'MERGER', 'DEMERGER', 'CAPITAL_REDUCTION', or several joined with '+' (Ortin Laboratories:
    'Demerger and Capital Reduction' -> 'DEMERGER+CAPITAL_REDUCTION'); 'OTHER' when none is named."""
    parts = [name for name, rx in SCHEME_PARTS if re.search(rx, text or "", re.I)]
    return "+".join(parts) or "OTHER"


def status_of(subject):
    for name, rx in STATUS:
        if re.search(rx, subject or "", re.I):
            return name
    return "SUSPENDED"


def ratios(text):
    """Distinct 'A for B' swap ratios in text."""
    out = []
    for m in RATIO_RX.finditer(clean(text)):
        r = f"{float(m.group(1)):g} for {float(m.group(2)):g}"
        if r not in out:
            out.append(r)
    return out


def per_old(ratio):
    """new shares per old share from 'A for B' or 'A:B'."""
    m = re.match(r"^\s*(\d+(?:\.\d+)?)\s*(?:for|:)\s*(\d+(?:\.\d+)?)\s*$", ratio or "", re.I)
    if not m or float(m.group(2)) == 0:
        return ""
    return f"{float(m.group(1)) / float(m.group(2)):.6g}"


# table rows that are not stocks: series of debt / funds / InvITs / REITs / preference shares / warrants, or
# a reason that only fits them
NOT_STOCK_SERIES = r"^(?:MF|IV|RR|GB|GS|TB|SG|[NYZDPW][0-9A-Z])$"
NOT_STOCK_REASON = r"^(?:suspension (?:on account )?of\s+)?(?:redemption|.*\bconversion\b|.*\bmaturity\b)"


def not_a_stock(r):
    return bool(re.match(NOT_STOCK_SERIES, r.get("series", "").replace(" ", ""), re.I)
                or re.match(NOT_STOCK_REASON, r.get("reason", ""), re.I))


def count(skipped, label):
    if skipped is not None:
        skipped[label] = skipped.get(label, 0) + 1


def parse(entry, doc, symbols_by_name, skipped=None, subjects=None):
    """(merger rows, suspension rows) of one suspension circular. Table rows that are not stocks are
    left out and counted in skipped (a dict) when given. subjects (circular no -> subject) lets a circular
    without a table that only refers to debt / unit circulars be left out too (Tata Motors Finance,
    NSE/CML/68018, points to its NCD and CP circulars). Rows carry _cites (circulars the text refers to),
    _correction and _date for link_cancellations() and merge_corrections()."""
    text = doc["text"] if doc else ""
    refs = [n for n in cited(text) if n != entry["no"]]   # the header names the circular itself
    rows = companies(entry, doc, symbols_by_name)
    if subjects is not None and all(not r for _, _, r, _, _ in rows) and only_non_equity(refs, subjects):
        count(skipped, "suspension: refers only to debt / unit circulars")
        return [], []
    correction = bool(re.search(CORRECTION_SUBJECT, entry["subject"], re.I)
                      or re.search(CORRECTION_RX, body(text), re.I))
    flat = clean(body(text))
    fix = re.search(CORRECTED_LEAD, flat, re.I) if correction else None
    tail = flat[fix.start():] if fix else ""   # the corrected part of a corrigendum
    eff = (first_after(tail, CORRECTED_LEAD) or first_after(text, SUSPEND_LEAD) or first_after(text, EFFECTIVE, WEF)
           or first_after(entry["subject"], WEF))
    last = first_after(tail, LAST_TRADING) or (None if tail else first_after(text, LAST_TRADING))
    record = first_after(text, r"record date(?:\s+(?:fixed|is|was|has been fixed))?(?:\s+(?:on|as))?")
    status = status_of(entry["subject"])
    base = {"circular_no": entry["no"], "circular_date": iso(entry["date"]), "department": entry["dept"],
            "pdf_url": entry["url"], "source_file": safe_name(entry["url"]),
            "_cites": refs, "_correction": correction, "_date": entry["date"]}
    corr_note = ["correction without its original circular"] if correction else []
    mergers, others = [], []
    for sym, comp, r, notes, info in rows:
        if not_a_stock(r):
            count(skipped, "suspension rows: not a stock (units, debt, redemption)")
            continue
        kind, label, why, extra = classify(r.get("reason", ""), text, entry["subject"]) if text else ("other", "", "", [])
        if kind == "merger":
            new = NEW_COMPANY_RX.search(clean(r.get("reason", "") + " " + text))
            found = ratios(text)
            stype = scheme_type(why or body(text))
            row = dict(base, old_symbol=sym, old_company=comp, suspended_from=iso(eff), record_date=iso(record),
                       reason_text=why, new_company=clean(new.group(1)) if new else "",
                       new_symbol=(new.group(2) or "") if new else "",
                       swap_ratio=found[0] if len(found) == 1 else "", scheme_type=stype,
                       exit_expected="Y" if "MERGER" in stype.split("+") else "N")
            row["new_per_old"] = per_old(row["swap_ratio"])
            n = notes + extra + corr_note + row_notes(sym, comp, eff, entry["date"], "suspension date")
            if stype == "OTHER":
                n.append("scheme type unclear")
            if last:
                row["last_trading_date"] = iso(last)
            else:
                n.append("last trading date not found")
            if len(found) > 1:
                n.append(f"several swap ratios: {', '.join(found)}")
            elif "MERGER" in stype.split("+") and (not found or not row["new_company"]):
                n.append("swap ratio not in NSE circular" if not found else "new company not in NSE circular")
            mergers.append(review(row, n, info))
        else:
            row = dict(base, status=status, symbol=sym, company=comp, effective_date=iso(eff), reason=label,
                       reason_text=why, subject=entry["subject"],
                       exit_expected="Y" if label in EXIT_REASONS and status != "CANCELLED" else "N")
            n = notes + extra + corr_note + row_notes(sym, comp, eff, entry["date"])
            if status == "CONTINUED":   # its date is the original suspension's, before this circular
                n = [x for x in n if x != "effective date before the circular date"]
            others.append(review(row, n, info))
    return mergers, others


def same_company(a, b, sym_field, comp_field):
    if a.get(sym_field) and b.get(sym_field):
        return a[sym_field] == b[sym_field]
    return bool(a.get(comp_field)) and name_key(a[comp_field]) == name_key(b.get(comp_field, ""))


def original_of(row, rows, sym_field, comp_field, ok=lambda r: True):
    """The earlier row of the same company that row refers to: one of the circulars its text cites, else
    the latest earlier one."""
    same = [r for r in rows if r is not row and ok(r) and same_company(row, r, sym_field, comp_field)
            and r["_date"] and row["_date"] and r["_date"] <= row["_date"]]
    cited_rows = [r for r in same if r["circular_no"] in row["_cites"]]
    pool = cited_rows or same
    return max(pool, key=lambda r: (r["_date"], r["circular_no"])) if pool else None


def merge_corrections(rows, sym_field="symbol", comp_field="company",
                      dates=("effective_date",)):
    """Fold each corrigendum / changed-date circular into the row of the circular it corrects (same
    company): its dates win, corrected_by lists it, the corrigendum's own row goes. Returns the rows."""
    out = []
    for row in sorted(rows, key=lambda r: (r["_date"] is None, r["_date"], r["circular_no"])):
        if row["_correction"]:
            orig = original_of(row, out, sym_field, comp_field, lambda r: not r["_correction"])
            if orig is not None:
                for f in dates:
                    if row.get(f):
                        orig[f] = row[f]
                orig["corrected_by"] = "; ".join(x for x in (orig.get("corrected_by"), row["circular_no"]) if x)
                orig["review_note"] = "; ".join(x for x in (orig["review_note"], f"corrected by {row['circular_no']}") if x)
                continue
        out.append(row)
    return out


def link_cancellations(rows):
    """cancels_circular for CANCELLED rows: the SUSPENDED row of the same company that the text cites,
    else the latest earlier one; not found -> needs_review."""
    for row in rows:
        if row.get("status") != "CANCELLED":
            continue
        orig = original_of(row, rows, "symbol", "company", lambda r: r.get("status") == "SUSPENDED")
        if orig is not None:
            row["cancels_circular"] = orig["circular_no"]
        else:
            notes = row["review_note"].split("; ") if row["review_note"] else []
            row["review_note"] = "; ".join(notes + ["cancelled circular not found"])
            row["needs_review"] = "Y"
    return rows


# --- manual_mergers.csv ------------------------------------------------------------------------

def ensure_manual(ref_dir):
    """Create reference/manual_mergers.csv (header + the HDFC example) if it does not exist."""
    path = os.path.join(ref_dir, MANUAL_FILE)
    if not os.path.exists(path):
        os.makedirs(ref_dir, exist_ok=True)
        with open(path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=MANUAL_COLUMNS, lineterminator="\n")
            w.writeheader()
            w.writerow(MANUAL_EXAMPLE)
    return path


def read_manual(path):
    """{old_symbol: row} from manual_mergers.csv (missing file: {})."""
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8-sig", newline="") as f:
        out = {}
        for r in csv.DictReader(f):
            r = {k.strip().lower(): clean(v) for k, v in r.items() if k}
            sym = r.get("old_symbol", "").upper()
            if sym:
                out[sym] = r
    return out


def apply_manual(rows, manual):
    """Fill new_company / new_symbol / swap_ratio from manual_mergers.csv, override scheme_type /
    exit_expected when the manual row has them, and clear needs_review on the rows it covers (unless a
    manual value is not understood). Returns the manual symbols that matched no row."""
    used = set()
    for row in rows:
        m = manual.get(row.get("old_symbol", "").upper())
        if not m:
            continue
        used.add(row["old_symbol"].upper())
        for field in ("new_company", "new_symbol", "swap_ratio"):
            if m.get(field):
                row[field] = m[field]
        row["new_per_old"] = per_old(row["swap_ratio"])
        bad = []
        stype = m.get("scheme_type", "").upper().replace(" ", "")
        if stype:
            if re.match(SCHEME_TYPE_RX, stype):
                row["scheme_type"] = stype
            else:
                bad.append(f"manual scheme_type {m['scheme_type']!r} not understood")
        exit_ = m.get("exit_expected", "").upper()
        if exit_:
            if exit_ in ("Y", "N"):
                row["exit_expected"] = exit_
            else:
                bad.append(f"manual exit_expected {m['exit_expected']!r} not understood")
        if m.get("new_symbol") and not valid_symbol(m["new_symbol"]):
            row["review_note"] = f"manual new_symbol looks odd; {row['review_note']}".strip("; ")
        was = row["review_note"]
        row["review_note"] = "; ".join(bad + ["filled from manual_mergers.csv" + (f" (was: {was})" if was else "")])
        row["needs_review"] = "Y" if bad else ""
    return sorted(set(manual) - used)

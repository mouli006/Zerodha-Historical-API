"""backfill/equity/index_membership.py, index_notices.py, index_fetch.py and index_membership.py (the CLI).

A small synthetic bhavcopy store in tmp_path with the security master built from it; notices are given as
extract_stream() items (im.notice_stream patched), current lists and AMFI lists are written in the real formats.
Saved real samples in tests/fixtures/index_membership/ (cut from the files fetched on 2026-10-18): two notice PDFs,
the pdfplumber streams of eight more (notices.json), 40 rows of the H2-2025 AMFI list (with and without the category
column) and 10 rows of the NIFTY 50 list. No network (requests and the browser are refused), .env never read."""
import hashlib
import io
import json
import os
import time
from datetime import date, timedelta

import openpyxl
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests

import config
import index_membership as cli
from backfill.equity import index_fetch as fe
from backfill.equity import index_membership as im
from backfill.equity import index_notices as nt
from backfill.equity import security_master as sm
from backfill.locks import RunLock
from backfill.nse_ref import fetch as nse_fetch
from backfill.nse_ref.fetch import FetchError, NotFound

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "index_membership")
COLUMNS = ["date", "symbol", "series", "isin", "open", "high", "low", "close", "last", "prev_close", "volume",
           "turnover", "trades", "fin_id", "format"]
AMFI_HEADER = ["Sr. No.", "Company name", "ISIN", "BSE Symbol", "BSE 6 month Avg Total Market Cap in (Rs. Crs.)",
               "NSE Symbol", "NSE 6 month Avg Total Market Cap (Rs. Crs.)", "MSEI Symbol",
               "MSEI 6 month Avg Total Market Cap in (Rs Crs.)", "Average of All Exchanges (Rs. Cr.)",
               "Categorization as per SEBI Circular dated Oct 6, 2017"]


def fixture(name):
    with open(os.path.join(FIX, name), "rb") as f:
        return f.read()


def streams():
    with open(os.path.join(FIX, "notices.json"), encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def refuse(*a, **k):
        raise AssertionError("a test tried to reach the network")
    monkeypatch.setattr(requests.Session, "request", refuse)
    monkeypatch.setattr(nse_fetch, "launch_browser", refuse)
    monkeypatch.setattr(config, "load_env", lambda: {})


# ---- the synthetic world ----

def weekdays(first, n):
    out, d = [], first
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


N = 200
DAYS = weekdays(date(2021, 1, 4), N)
R1, ADHOC, R2 = 60, 90, 190          # effective days: March review, ad hoc change, September review
ISIN = {"AAA": "INE001A01011", "BBB": "INE002A01011", "CCC": "INE003A01011", "DDD": "INE004A01011",
        "GONE": "INE005A01011", "NEWCO": "INE006A01011", "SPLIT": "INE007A01011", "SPLIT2": "INE007A01029",
        "ZZZDVR": "IN9008A01011", "EMBASSY": "INE041025011"}
COMPANY = {"EMBASSY": "Embassy Office Parks REIT"}   # not in the equity master (a REIT)
SIZES = {**nt.SIZES, "NIFTY 50": 2, "NIFTY NEXT 50": 2, "NIFTY 100": 4}


def trade(symbol, isin, idx, series="EQ"):
    return [{"date": DAYS[i], "symbol": symbol, "series": series, "isin": isin, "open": 100.0, "high": 100.0,
             "low": 100.0, "close": 100.0, "last": 100.0, "prev_close": 100.0, "volume": 10, "turnover": 1000.0,
             "trades": 1, "fin_id": "1", "format": "udiff"} for i in idx]


def store_rows():
    rows = []
    for s in ("AAA", "BBB", "CCC", "DDD"):
        rows += trade(s, ISIN[s], range(N))
    rows += trade("GONE", ISIN["GONE"], range(0, 61))                    # stops trading; out of Next 50 at ADHOC
    rows += trade("NEWCO", ISIN["NEWCO"], range(120, N))                 # lists on day 120, in Next 50 at R2
    rows += trade("SPLIT", ISIN["SPLIT"], range(0, 100)) + trade("SPLIT", ISIN["SPLIT2"], range(100, N))
    rows += trade("ZZZDVR", ISIN["ZZZDVR"], range(N))                   # an additional share class
    rows += trade("FILLER", "INE999Z01011", range(N), series="N1")      # every day in the calendar
    return rows


def write_store(folder, rows=None):
    store = os.path.join(folder, "NSE_CM_bhav")
    os.makedirs(store, exist_ok=True)
    df = pd.DataFrame(rows or store_rows(), columns=COLUMNS)
    for year, part in df.groupby(df["date"].map(lambda x: x.year)):
        path = os.path.join(store, f"{year}.parquet")
        pq.write_table(pa.Table.from_pandas(part.reset_index(drop=True), preserve_index=False), path)
        old = time.time() - 120
        os.utime(path, (old, old))
    return store


def stream(eff, blocks, extra=(), page_text=True):
    """extract_stream() items of a notice: the effective-date sentence, then per index a heading and its tables."""
    items, top = [], [0]

    def text(t):
        top[0] += 10
        items.append({"page": 1, "top": top[0], "text": t})

    def table(rows):
        top[0] += 10
        items.append({"page": 1, "top": top[0], "table": rows})

    text("PRESS RELEASE")
    text("The Index Maintenance Sub-Committee has decided to make the following replacements as part of its "
         "periodic review.")
    text(f"These changes shall become effective from {eff:%B} {eff.day}, {eff.year} (close of the day before).")
    for line in extra:
        text(line)
    for k, (name, acts) in enumerate(blocks, 1):
        text(f"{k}) {name}")
        for action, syms in acts:
            text("The following companies are being " + ("excluded:" if action == "OUT" else "included:"))
            table([["Sr. No.", "Company Name", "Symbol"]] + [[str(j), f"{s} Ltd.", s] for j, s in enumerate(syms, 1)])
    text("About NSE Indices Limited: " + "x" * 200)
    return items


def notice_name(day, k=""):
    return f"ind_prs{day:%d%m%Y}{k}.pdf"


def base_notices():
    """{file: items}: the March review, an ad hoc change and the September review."""
    return {
        notice_name(DAYS[R1 - 20]): stream(DAYS[R1], [("Nifty 50", [("OUT", ["BBB"]), ("IN", ["CCC"])]),
                                                      ("Nifty Next 50", [("OUT", ["CCC"]), ("IN", ["BBB"])]),
                                                      ("Nifty 50 Equal Weight", [("OUT", ["BBB"])])]),
        notice_name(DAYS[ADHOC - 5]): stream(DAYS[ADHOC], [("Nifty Next 50", [("OUT", ["GONE"]), ("IN", ["DDD"])]),
                                                           ("Nifty 100", [("OUT", ["GONE"]), ("IN", ["DDD"])])]),
        notice_name(DAYS[R2 - 20]): stream(DAYS[R2], [("Nifty Next 50", [("OUT", ["DDD"]), ("IN", ["NEWCO"])]),
                                                      ("Nifty 100", [("OUT", ["DDD"]), ("IN", ["NEWCO"])])]),
    }


LIST_DATE = DAYS[-1] + timedelta(days=3)
CURRENT = {"NIFTY 50": ["AAA", "CCC"], "NIFTY NEXT 50": ["BBB", "NEWCO"], "NIFTY 100": ["AAA", "CCC", "BBB", "NEWCO"]}


def write_current(folder, index, symbols, day=LIST_DATE, isins=None):
    sub = fe.input_dir(folder, "nifty_current")
    os.makedirs(sub, exist_ok=True)
    lines = [",".join(nt_header())] + [f"{COMPANY.get(s, s + ' Ltd.')},Finance,{s},EQ,{(isins or ISIN).get(s, '')}"
                                       for s in symbols]
    with open(os.path.join(sub, f"{fe.file_key(index)}_{day}.csv"), "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(lines) + "\n")


def nt_header():
    return fe.CURRENT_HEADER


def amfi_rows(special):
    """260 rows: ranks 1-100 LARGE, 101-250 MID, 251-260 SMALL; special = {rank: (symbol, isin)} for real ones."""
    rows = []
    for rank in range(1, 261):
        sym, isin = special.get(rank, (f"FAKE{rank}", f"INE9{rank:03d}Z01011"))
        cat = "Large Cap" if rank <= 100 else "Mid Cap" if rank <= 250 else "Small Cap"
        rows.append([rank, f"{sym} Ltd", isin, sym, 1000.0 - rank, sym, 1000.0 - rank, "-", "-", 1000.0 - rank, cat])
    return rows


def write_amfi(folder, name, ended, rows, category=True):
    sub = fe.input_dir(folder, "amfi")
    os.makedirs(sub, exist_ok=True)
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append([f"Average Market Capitalization of listed companies during the six months ended {ended}"])
    ws.append(AMFI_HEADER if category else AMFI_HEADER[:-1])
    for r in rows:
        ws.append(r if category else r[:-1])
    wb.save(os.path.join(sub, name))


def base_amfi(folder):
    # H2-2020: AAA LARGE, CCC MID (rank 120), SPLIT by its old ISIN; H1-2021: CCC LARGE, SPLIT by its new ISIN,
    # NEWCO by symbol only (ISIN unknown to the master), an unknown company at rank 300 (SMALL, no review)
    write_amfi(folder, "AverageMarketCapitalization31Dec2020.xlsx", "31 December 2020",
               amfi_rows({1: ("AAA", ISIN["AAA"]), 120: ("CCC", ISIN["CCC"]), 200: ("SPLIT", ISIN["SPLIT"])}))
    write_amfi(folder, "AverageMarketCapitalization30Jun2021.xlsx", "30 June 2021",
               amfi_rows({1: ("AAA", ISIN["AAA"]), 2: ("CCC", ISIN["CCC"]), 150: ("SPLIT", ISIN["SPLIT2"]),
                          160: ("NEWCO", "INE000X01011")}))


def prepare(folder, notices=None, current=None, manual=None, amfi=True, snapshots=None):
    write_store(folder)
    sm.build(folder, log=lambda *a: None)
    sub = fe.input_dir(folder, "nifty_changes")
    os.makedirs(sub, exist_ok=True)
    notices = base_notices() if notices is None else notices
    for name in notices:
        with open(os.path.join(sub, name), "wb") as f:
            f.write(b"%PDF-1.4 synthetic")
    for index, syms in (CURRENT if current is None else current).items():
        write_current(folder, index, syms)
    if manual is not None:
        with open(os.path.join(fe.input_dir(folder), "manual_changes.csv"), "w", encoding="utf-8", newline="\n") as f:
            f.write(manual)
    if amfi:
        base_amfi(folder)
    for name, syms in (snapshots or {}).items():
        os.makedirs(fe.input_dir(folder, "snapshots"), exist_ok=True)
        lines = [",".join(fe.CURRENT_HEADER)] + [f"{s} Ltd.,Finance,{s},EQ,{ISIN.get(s, '')}" for s in syms]
        with open(os.path.join(fe.input_dir(folder, "snapshots"), name), "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
    return notices


@pytest.fixture
def world(tmp_path, monkeypatch):
    """(folder, notices); call build(folder) after preparing."""
    monkeypatch.setattr(im, "SIZES", SIZES)
    state = {}

    def fake_stream(path, cache_dir):
        return state["notices"][os.path.basename(path)]
    monkeypatch.setattr(im, "notice_stream", fake_stream)

    def make(**kw):
        state["notices"] = prepare(str(tmp_path), **kw)
        return str(tmp_path)
    return make


def build(folder, **kw):
    return im.build(folder, log=lambda *a: None, **kw)


def read(folder, name):
    return pd.read_csv(os.path.join(im.out_dir(folder), name), dtype=str, keep_default_na=False)


def sid(folder, symbol, day=DAYS[-1]):
    return sm.security_id_for(symbol, day, folder)


# ---- parsing: AMFI ----

def test_read_amfi_real_sample_with_category():
    end, rows, how = nt.read_amfi(fixture("amfi_31Dec2025_sample.xlsx"), "amfi_31Dec2025_sample.xlsx")
    assert end == date(2025, 12, 31) and how == "column" and len(rows) == 40
    first = rows[0]
    assert (first["rank"], first["symbol"], first["isin"], first["category"]) == (1, "RELIANCE", "INE002A01018", "LARGE")
    assert first["avg_mcap"] == pytest.approx(1970797.087539265)
    cats = {r["rank"]: r["category"] for r in rows}
    assert cats[100] == "LARGE" and cats[101] == "MID" and cats[250] == "MID" and cats[251] == "SMALL"
    assert all(r["avg_mcap"] is not None for r in rows)   # "-" in the MSEI columns is not read as a number


def test_read_amfi_without_category_uses_rank():
    end, rows, how = nt.read_amfi(fixture("amfi_no_category_sample.xlsx"), "x.xlsx")
    assert end == date(2025, 12, 31) and how == "rank"
    cats = {r["rank"]: r["category"] for r in rows}
    assert (cats[1], cats[100], cats[101], cats[250], cats[251], cats[504]) == \
        ("LARGE", "LARGE", "MID", "MID", "SMALL", "SMALL")


def test_period_end_from_title_or_file_name():
    assert nt.period_end("... during the six months ended 31-Dec 2017") == date(2017, 12, 31)
    assert nt.period_end("... six months ended 30 June 2018") == date(2018, 6, 30)
    assert nt.period_end("", "Avg._Market_Capitalization_of_listed_companies_during_-Jan-June_2018.xlsx") == date(2018, 6, 30)
    assert nt.period_end("", "Average_Market_Capitalization_30_Jun2024_2a1ab4c1d8.xlsx") == date(2024, 6, 30)
    assert nt.period_end("", "AverageMarketCapitalizationoflistedcompaniesduringthesixmonthsended30June2022.xlsx") == \
        date(2022, 6, 30)
    assert nt.period_end("no date", "nothing.xlsx") is None


def test_amfi_rank_continues_when_numbering_stops(tmp_path):
    rows = amfi_rows({})[:5]
    for r in rows[3:]:
        r[0] = None                       # the H2-2017 file stops numbering after rank 3452
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["... six months ended 31 December 2017"])
    ws.append(AMFI_HEADER)
    for r in rows:
        ws.append(r)
    ws.append([None] * 11)
    ws.append([9999] + [None] * 10)       # numbered but empty
    buf = io.BytesIO()
    wb.save(buf)
    _, out, _ = nt.read_amfi(buf.getvalue(), "x.xlsx")
    assert [r["rank"] for r in out] == [1, 2, 3, 4, 5]


# ---- parsing: notices ----

def test_parse_real_2015_notice_old_names():
    items = nt.extract_stream(fixture("ind_prs20022015.pdf"))
    p = nt.parse_notice(items, date(2015, 2, 20), "", "ind_prs20022015.pdf")
    assert p["dates"] == [date(2015, 3, 27)] and p["review"]
    got = {(c["index"], c["action"], c["symbol"]) for c in p["changes"]}
    assert {("NIFTY 50", "OUT", "DLF"), ("NIFTY 50", "OUT", "JINDALSTEL"), ("NIFTY 50", "IN", "IDEA"),
            ("NIFTY 50", "IN", "YESBANK"), ("NIFTY NEXT 50", "IN", "MCDOWELL-N"), ("NIFTY 100", "OUT", "DLF"),
            ("NIFTY 200", "IN", "GSKCONS"), ("NIFTY 500", "OUT", "IL&FSENGG"), ("NIFTY MIDCAP 100", "IN", "DLF")} <= got
    assert all(c["effective_date"] == date(2015, 3, 27) for c in p["changes"])
    # "CNX 100 Equal Weight" and the sectoral indices are not ours
    assert {c["index"] for c in p["changes"]} <= set(nt.INDEXES)


def test_parse_real_2023_hdfc_notice():
    items = nt.extract_stream(fixture("ind_prs04072023.pdf"))
    p = nt.parse_notice(items, date(2023, 7, 4), "", "ind_prs04072023.pdf")
    assert p["dates"] == [date(2023, 7, 13)]
    got = {(c["index"], c["action"], c["symbol"]) for c in p["changes"]}
    assert {("NIFTY 50", "OUT", "HDFC"), ("NIFTY 50", "IN", "LTIM"), ("NIFTY NEXT 50", "OUT", "LTIM"),
            ("NIFTY 100", "IN", "JINDALSTEL"), ("NIFTY 500", "IN", "MANKIND"),
            ("NIFTY MIDCAP 150", "OUT", "JINDALSTEL")} <= got
    assert "LTIM*" not in {c["symbol"] for c in p["changes"]}   # footnote marks are stripped


def test_parse_multi_date_notice_by_section():
    p = nt.parse_notice(streams()["ind_prs17102016.pdf"], date(2016, 10, 17), "", "ind_prs17102016.pdf")
    assert set(p["dates"]) == {date(2016, 11, 15), date(2016, 10, 24)} and not p["unresolved"]
    got = {(c["index"], str(c["effective_date"]), c["action"], c["symbol"]) for c in p["changes"]}
    assert ("NIFTY NEXT 50", "2016-11-15", "OUT", "CAIRN") in got and ("NIFTY 100", "2016-11-15", "IN", "HAVELLS") in got


def test_parse_unresolved_date_is_reported():
    items = stream(date(2021, 3, 31), [("Nifty 50", [("OUT", ["BBB"])])],
                   extra=["Other changes shall become effective from April 5, 2021."])
    p = nt.parse_notice(items, date(2021, 3, 1))
    assert p["dates"] == [date(2021, 3, 31), date(2021, 4, 5)] and p["unresolved"] == ["NIFTY 50"] and not p["changes"]


def test_parse_yes_bank_and_restructure_notices():
    s = streams()
    p = nt.parse_notice(s["ind_prs16032020.pdf"], date(2020, 3, 16))
    assert p["dates"] == [date(2020, 3, 19)]
    got = {(c["index"], c["action"], c["symbol"]) for c in p["changes"]}
    assert {("NIFTY 50", "OUT", "YESBANK"), ("NIFTY 50", "IN", "SHREECEM")} <= got
    p = nt.parse_notice(s["ind_prs22022016_2.pdf"], date(2016, 2, 22))
    n50 = [c for c in p["changes"] if c["index"] == "NIFTY 50"]
    assert {c["symbol"] for c in n50 if c["action"] == "IN"} == {"AUROPHARMA", "INFRATEL", "EICHERMOT", "TATAMTRDVR"}
    # "Nifty Free Float Midcap 100 Index (Formerly Nifty Midcap 100)" is NIFTY MIDCAP 100
    assert any(c["index"] == "NIFTY MIDCAP 100" for c in p["changes"])


def test_parse_lettered_headings():
    p = nt.parse_notice(streams()["ind_prs23082024.pdf"], date(2024, 8, 23))
    got = {(c["index"], c["action"], c["symbol"]) for c in p["changes"]}
    assert got == {("NIFTY 50", "OUT", "DIVISLAB"), ("NIFTY 50", "OUT", "LTIM"), ("NIFTY 50", "IN", "BEL"),
                   ("NIFTY 50", "IN", "TRENT")}


def test_parse_demerger_index_list_is_not_applied():
    p = nt.parse_notice(streams()["ind_prs17072023.pdf"], date(2023, 7, 17), "t", "ind_prs17072023.pdf")
    assert p["demerger"] and not p["changes"]
    assert [x["index"] for x in p["not_applied"]] == ["NIFTY 50", "NIFTY 100", "NIFTY 200", "NIFTY 500"]
    assert {x["effective_date"] for x in p["not_applied"]} == {date(2023, 7, 20)}


def test_parse_symbol_named_index_list_is_applied():
    p = nt.parse_notice(streams()["ind_prs23082024_1.pdf"], date(2024, 8, 23))
    assert not p["not_applied"]
    assert {(c["index"], c["action"], c["symbol"]) for c in p["changes"]} == \
        {("NIFTY 100", "OUT", "TATAMTRDVR"), ("NIFTY 200", "OUT", "TATAMTRDVR"), ("NIFTY 500", "OUT", "TATAMTRDVR")}


def test_parse_revision_tables():
    s = streams()
    p = nt.parse_notice(s["ind_prs19032024.pdf"], date(2024, 3, 19))
    got = {(c["index"], str(c["effective_date"]), c["action"], c["symbol"]) for c in p["changes"]}
    assert {("NIFTY MIDCAP 150", "2024-03-28", "IN", "BSE"), ("NIFTY SMALLCAP 250", "2024-03-28", "OUT", "BSE"),
            ("NIFTY MIDCAP 100", "2024-03-28", "IN", "BSE"), ("NIFTY 200", "2024-03-28", "IN", "BSE"),
            ("NIFTY 500", "2024-03-28", "REVOKE_IN", "IREDA"), ("NIFTY 500", "2024-03-28", "REVOKE_OUT", "VGUARD")} <= got
    assert not any(c["symbol"] == "BLUESTARCO" for c in p["changes"])        # Smallcap 50 / 100: not ours
    # two dates (Sept 30, and Oct 25 for Midcap Select only): "all indices listed above ... effective from" decides
    p = nt.parse_notice(s["ind_prs25092024.pdf"], date(2024, 9, 25))
    assert not p["unresolved"] and not p["odd_rows"]
    got = {(c["index"], str(c["effective_date"]), c["action"], c["symbol"]) for c in p["changes"]}
    assert got == {("NIFTY 500", "2024-09-30", "REVOKE_OUT", "IDEA"), ("NIFTY 500", "2024-09-30", "OUT", "PRSMJOHNSN"),
                   ("NIFTY MIDCAP 150", "2024-09-30", "REVOKE_OUT", "IDEA"),
                   ("NIFTY MIDCAP 150", "2024-09-30", "REVOKE_IN", "CENTRALBK"),
                   ("NIFTY SMALLCAP 250", "2024-09-30", "REVOKE_OUT", "CENTRALBK"),
                   ("NIFTY SMALLCAP 250", "2024-09-30", "OUT", "PRSMJOHNSN"),
                   ("NIFTY MIDCAP 100", "2024-09-30", "REVOKE_OUT", "IDEA"),
                   ("NIFTY MIDCAP 100", "2024-09-30", "REVOKE_IN", "CENTRALBK"),
                   ("NIFTY 200", "2024-09-30", "REVOKE_OUT", "IDEA"), ("NIFTY 200", "2024-09-30", "REVOKE_IN", "CENTRALBK")}


def revision(eff, rows):
    """A revision notice: [(index, symbol, remark)] in an Index Name | Security Name | Symbol | Remarks table."""
    table = [["Sr. No.", "Index Name", "Security Name", "Symbol", "Remarks"]]
    last = None
    for k, (index, sym, remark) in enumerate(rows, 1):
        table.append([str(k) if index != last else "", index if index != last else "", f"{sym} Ltd.", sym, remark])
        last = index
    return [{"page": 1, "top": 10, "text": f"These changes shall become effective from {eff:%B} {eff.day}, {eff.year}."},
            {"page": 1, "top": 20, "table": table}, {"page": 1, "top": 30, "text": "x" * 300}]


def test_revocation_cancels_the_earlier_row(world):
    notices = base_notices()
    eff = DAYS[110]
    notices[notice_name(DAYS[100])] = stream(eff, [("Nifty 50", [("OUT", ["AAA"]), ("IN", ["DDD"])])])
    notices[notice_name(DAYS[105])] = revision(eff, [("Nifty 50", "DDD", "Inclusion revoked"),
                                                     ("Nifty 50", "AAA", "Exclusion revoked"),
                                                     ("Nifty 50", "SPLIT", "Inclusion revoked"),       # nothing to revoke
                                                     ("Nifty 50", "BBB", "Weight capped"),             # unknown remark
                                                     ("Nifty Bank", "AAA", "Exclusion")])               # not ours
    f = world(notices=notices)
    build(f)
    ch = read(f, "nifty_changes.csv")
    early = ch[ch["file"] == notice_name(DAYS[100])]
    assert set(early["status"]) == {"revoked"} and set(early["note"]) == {f"revoked by {notice_name(DAYS[105])}"}
    rev = ch[ch["file"] == notice_name(DAYS[105])]
    assert set(rev["status"]) == {"revocation"} and set(rev["action"]) == {"REVOKE_IN", "REVOKE_OUT"}
    assert im.members("NIFTY 50", DAYS[-1], f) == sorted([sid(f, "AAA"), sid(f, "CCC")])
    rv = read(f, "review.csv")
    assert rv.query("kind == 'undo_mismatch'").empty
    assert any("matches no earlier parsed row" in d and "SPLIT" in s
               for d, s in zip(rv["details"], rv["symbol"]))
    assert any("revision table row NIFTY 50 BBB: remark 'Weight capped'" in d for d in rv["details"])


def test_parse_replacing_and_image_notices():
    s = streams()
    assert nt.parse_notice(s["ind_prs15092021.pdf"], date(2021, 9, 15))["replaces"]
    p = nt.parse_notice(s["ind_prs23082021.pdf"], date(2021, 8, 23))
    assert p["no_text"] and not p["changes"]


def test_parse_table_row_read_as_text():
    items = stream(date(2021, 3, 31), [("Nifty 500", [("IN", ["AAA"])])])
    at = next(k for k, i in enumerate(items) if "table" in i)
    items.insert(at + 1, {"page": 1, "top": items[at]["top"] + 1, "text": "2 Cairn India Ltd. CAIRN"})
    p = nt.parse_notice(items, date(2021, 3, 1))
    assert [c["symbol"] for c in p["changes"]] == ["AAA", "CAIRN"]


def test_map_name_aliases():
    assert nt.map_name("CNX Nifty Index") == "NIFTY 50"
    assert nt.map_name("CNX Nifty Junior Index") == "NIFTY NEXT 50"
    assert nt.map_name("Nifty Free Float Midcap 100 Index (Formerly Nifty Midcap 100)") == "NIFTY MIDCAP 100"
    assert nt.map_name("CNX Midcap Index") == "NIFTY MIDCAP 100"
    assert nt.map_name("Nifty Full Midcap 100") is None           # another index, ceased 2018
    assert nt.map_name("Nifty50 Equal Weight") is None and nt.map_name("Nifty 500 Index*") == "NIFTY 500"


def test_read_constituents_real_sample():
    rows = nt.read_constituents(fixture("ind_nifty50list.csv"))
    assert len(rows) == 10 and rows[0]["symbol"] == "ADANIENT" and rows[0]["isin"] == "INE423A01024"


# ---- build: walk, changes, manual ----

def test_backward_walk_over_two_reviews_and_an_ad_hoc_change(world):
    f = world()
    r = build(f)
    m = read(f, "nifty_membership.csv")

    def periods(index, symbol):
        x = m[(m["index"] == index) & (m["security_id"] == sid(f, symbol, DAYS[0] if symbol != "NEWCO" else DAYS[-1]))]
        return sorted((a, b) for a, b in zip(x["from_date"], x["to_date"]))
    d = lambda i: str(DAYS[i])   # noqa: E731
    assert periods("NIFTY 50", "AAA") == [(d(0), "")]
    assert periods("NIFTY 50", "BBB") == [(d(0), d(R1 - 1))]                 # out from the effective date
    assert periods("NIFTY 50", "CCC") == [(d(R1), "")]
    assert periods("NIFTY NEXT 50", "CCC") == [(d(0), d(R1 - 1))]
    assert periods("NIFTY NEXT 50", "GONE") == [(d(0), d(ADHOC - 1))]
    assert periods("NIFTY NEXT 50", "DDD") == [(d(ADHOC), d(R2 - 1))]
    assert periods("NIFTY NEXT 50", "NEWCO") == [(d(R2), "")]
    assert periods("NIFTY 100", "GONE") == [(d(0), d(ADHOC - 1))]
    ch = read(f, "nifty_changes.csv")
    assert set(ch["status"]) == {"applied"}
    assert "Nifty 50 Equal Weight" not in set(ch["index"])
    sizes = {s["index"]: s for s in json.load(open(os.path.join(im.out_dir(f), "build_info.json")))["size_checks"]}
    assert sizes["NIFTY 50"]["share"] == 1.0 and sizes["NIFTY NEXT 50"]["share"] == 1.0
    assert "NIFTY 100 = NIFTY 50 + NIFTY NEXT 50 (from" in r["report"] and "holds on 200 of 200 days (100.0%)" in r["report"]


def test_point_in_time_loaders(world):
    f = world()
    build(f)
    a, b, c = (sid(f, s, DAYS[0]) for s in ("AAA", "BBB", "CCC"))
    assert im.members("NIFTY 50", DAYS[R1 - 1], f) == sorted([a, b])
    assert im.members("NIFTY 50", DAYS[R1], f) == sorted([a, c])
    assert im.members("nifty50", DAYS[R1 + 1], f) == sorted([a, c])
    assert im.is_member(b, "NIFTY 50", DAYS[R1 - 1], f) and not im.is_member(b, "NIFTY 50", DAYS[R1], f)
    assert im.coverage("NIFTY 50", f) == (DAYS[0], DAYS[-1], DAYS[0])
    assert im.coverage("NIFTY 50", f).reliable_from == DAYS[0]
    with pytest.raises(im.OutOfCoverage):
        im.members("NIFTY 50", DAYS[0] - timedelta(days=1), f)
    with pytest.raises(im.OutOfCoverage):
        im.members("NIFTY 50", DAYS[-1] + timedelta(days=1), f)
    with pytest.raises(im.OutOfCoverage):
        im.members("NIFTY 500", DAYS[10], f)                                # no current list: not covered
    with pytest.raises(KeyError):
        im.members("NIFTY 999", DAYS[10], f)


def test_amfi_periods_valid_from_lag_and_mapping(world):
    f = world()
    build(f)
    a = read(f, "amfi.csv")
    p1, p2 = sorted(a["period"].unique())
    assert (p1, p2) == ("2020H2", "2021H1")
    first = a[a["period"] == p1].iloc[0]
    assert first["valid_from"] == str(DAYS[5])                               # 2021-01-04 + 5 trading days
    july = next(i for i, d in enumerate(DAYS) if d >= date(2021, 7, 1))
    assert first["valid_to"] == str(DAYS[july + 5 - 1])
    assert a[a["period"] == p2].iloc[0]["valid_from"] == str(DAYS[july + 5]) and a[a["period"] == p2].iloc[0]["valid_to"] == ""
    split = sid(f, "SPLIT")
    assert set(a[a["security_id"] == split]["isin"]) == {ISIN["SPLIT"], ISIN["SPLIT2"]}   # both ISINs, one security
    assert a[(a["symbol"] == "NEWCO")]["security_id"].iloc[0] == sid(f, "NEWCO")            # by symbol
    rv = read(f, "review.csv")
    unmatched = rv[rv["kind"] == "amfi_unmatched"]
    assert len(unmatched) == 257 + 256 and all(" rank " in d for d in unmatched["details"])
    # point in time
    ccc = sid(f, "CCC")
    assert im.amfi_category(ccc, DAYS[5], f) == ("MID", "2020H2")
    assert im.amfi_category(ccc, DAYS[july + 4], f) == ("MID", "2020H2")      # the day before the new list counts
    assert im.amfi_category(ccc, DAYS[july + 5], f) == ("LARGE", "2021H1")
    assert im.amfi_category(sid(f, "GONE"), DAYS[50], f) == (None, "2020H2")
    assert list(im.amfi_list(DAYS[50], "LARGE", f)["rank"])[:2] == [1, 2] and len(im.amfi_list(DAYS[50], "MID", f)) == 150
    with pytest.raises(im.OutOfCoverage):
        im.amfi_category(ccc, DAYS[4], f)
    with pytest.raises(im.OutOfCoverage):
        im.amfi_list(DAYS[-1] + timedelta(days=1), folder=f)


def test_amfi_lag_option(world):
    f = world()
    build(f, amfi_lag=0)
    assert read(f, "amfi.csv")["valid_from"].iloc[0] == str(DAYS[0])


def test_amfi_count_and_duplicate_reviews(world, tmp_path):
    f = world(amfi=False)
    rows = amfi_rows({1: ("AAA", ISIN["AAA"]), 2: ("AAA", ISIN["AAA"])})
    rows[150][10] = "Large Cap"                       # 101 LARGE, 149 MID
    write_amfi(f, "AverageMarketCapitalization31Dec2020.xlsx", "31 December 2020", rows)
    build(f)
    rv = read(f, "review.csv")
    assert any("101 LARGE and 149 MID" in d for d in rv[rv["kind"] == "amfi_count"]["details"])
    assert len(rv[rv["kind"] == "amfi_duplicate"]) == 1


def test_superseded_by_a_later_notice_before_the_effective_date(world):
    notices = base_notices()
    first = DAYS[R1 + 20]
    later = DAYS[R1 + 30]
    notices[notice_name(DAYS[R1 + 10])] = stream(first, [("Nifty 50", [("OUT", ["AAA"]), ("IN", ["BBB"])])])
    notices[notice_name(DAYS[R1 + 15])] = stream(later, [("Nifty 50", [("OUT", ["AAA"]), ("IN", ["BBB"])])])
    f = world(notices=notices, current={**CURRENT, "NIFTY 50": ["BBB", "CCC"]})
    build(f)
    ch = read(f, "nifty_changes.csv")
    early = ch[ch["file"] == notice_name(DAYS[R1 + 10])]
    assert set(early["status"]) == {"superseded"} and all("superseded by" in n for n in early["note"])
    assert set(ch[ch["file"] == notice_name(DAYS[R1 + 15])]["status"]) == {"applied"}
    a, b, c = (sid(f, x) for x in ("AAA", "BBB", "CCC"))
    assert im.members("NIFTY 50", first, f) == sorted([a, c]) and im.members("NIFTY 50", later, f) == sorted([b, c])


def test_list_replaced_by_a_later_notice(world):
    notices = base_notices()
    n1, n2 = notice_name(DAYS[R2 - 20]), notice_name(DAYS[R2 - 10])
    notices[n1] = stream(DAYS[R2], [("Nifty Next 50", [("OUT", ["DDD"]), ("IN", ["NEWCO"])]),
                                   ("Nifty 100", [("OUT", ["DDD", "AAA"]), ("IN", ["NEWCO", "SPLIT"])])])
    notices[n2] = stream(DAYS[R2], [("Nifty 100", [("OUT", ["DDD"]), ("IN", ["NEWCO"])])],
                         extra=["The earlier list of replacement of these indices stands replaced with the list given "
                                "hereunder:"])
    f = world(notices=notices)
    build(f)
    ch = read(f, "nifty_changes.csv")
    old100 = ch[(ch["file"] == n1) & (ch["index"] == "NIFTY 100")]
    assert set(old100["status"]) == {"superseded"}
    assert set(old100[old100["symbol"].isin(["AAA", "SPLIT"])]["note"]) == {f"list replaced by {n2}"}
    assert set(ch[(ch["file"] == n1) & (ch["index"] == "NIFTY NEXT 50")]["status"]) == {"applied"}
    assert read(f, "review.csv").query("kind == 'undo_mismatch'").empty


def test_undo_mismatch_is_skipped_and_reviewed(world):
    notices = base_notices()
    notices[notice_name(DAYS[100])] = stream(DAYS[110], [("Nifty 50", [("IN", ["DDD"])])])   # DDD never in NIFTY 50
    f = world(notices=notices)
    build(f)
    rv = read(f, "review.csv")
    bad = rv[rv["kind"] == "undo_mismatch"]
    assert list(bad["symbol"]) == ["DDD"] and "not a member" in bad["details"].iloc[0]
    ch = read(f, "nifty_changes.csv")
    assert ch[(ch["symbol"] == "DDD") & (ch["index"] == "NIFTY 50")]["status"].iloc[0] == "skipped"


def test_manual_in_out_ignore_and_conflicts(world):
    manual = ("index,effective_date,action,symbol,isin,note\n"
              "# a comment\n"
              f"NIFTY 50,{DAYS[R1]},IGNORE,BBB,,wrong row\n"            # drops the parsed OUT BBB...
              f"NIFTY 50,{DAYS[R1]},IGNORE,CCC,,\n"                     # ... and IN CCC: BBB stays, CCC never in
              f"NIFTY NEXT 50,{DAYS[ADHOC]},IN,DDD,{ISIN['DDD']},by isin\n"   # same as the parsed IN: duplicate
              f"NIFTY 100,{DAYS[ADHOC]},IN,GONE,,contradicts\n"         # contradicts the parsed OUT GONE
              f"NIFTY 100,{DAYS[ADHOC]},IN,DDD,,\n"
              f"NIFTY 50,{DAYS[5]},IGNORE,ZZZ,,no match\n"
              "NIFTY 999,2021-01-01,IN,AAA,,bad index\n")
    f = world(manual=manual, current={**CURRENT, "NIFTY 50": ["AAA", "BBB"],
                                      "NIFTY 100": ["AAA", "CCC", "BBB", "NEWCO", "GONE"]})
    build(f)
    ch = read(f, "nifty_changes.csv")
    n50 = ch[(ch["index"] == "NIFTY 50") & (ch["source"] == "file")]
    assert set(n50["status"]) == {"ignored"}
    assert im.members("NIFTY 50", DAYS[-1], f) == sorted([sid(f, "AAA"), sid(f, "BBB")])
    man = ch[ch["source"] == "manual"]
    assert set(man[man["index"] == "NIFTY NEXT 50"]["status"]) == {"duplicate"}
    gone = ch[(ch["index"] == "NIFTY 100") & (ch["symbol"] == "GONE")]
    assert dict(zip(gone["source"], gone["status"])) == {"file": "ignored", "manual": "applied"}
    rv = read(f, "review.csv")
    conflicts = " | ".join(rv[rv["kind"] == "conflict"]["details"])
    assert "contradicts OUT" in conflicts and "IGNORE matches no parsed row" in conflicts and "row 7 not used" in conflicts


def test_manual_ignore_without_symbol_drops_the_whole_index_date(world):
    manual = f"index,effective_date,action,symbol,isin,note\nNIFTY NEXT 50,{DAYS[ADHOC]},IGNORE,,,null and void\n"
    f = world(manual=manual)
    build(f)
    ch = read(f, "nifty_changes.csv")
    assert set(ch[(ch["index"] == "NIFTY NEXT 50") & (ch["effective_date"] == str(DAYS[ADHOC]))]["status"]) == {"ignored"}
    assert set(ch[(ch["index"] == "NIFTY 100") & (ch["effective_date"] == str(DAYS[ADHOC]))]["status"]) == {"applied"}


def test_manual_file_created_once_and_never_overwritten(world):
    f = world()
    build(f)
    path = os.path.join(fe.input_dir(f), "manual_changes.csv")
    with open(path, encoding="utf-8") as fh:
        text = fh.read()
    assert text.startswith("index,effective_date,action,symbol,isin,note\n# ") and "IGNORE" in text
    with open(path, "a", encoding="utf-8") as fh:
        fh.write("# mine\n")
    build(f)
    with open(path, encoding="utf-8") as fh:
        assert fh.read() == text + "# mine\n"


def test_pending_unmapped_and_not_applied(world):
    notices = base_notices()
    notices[notice_name(DAYS[-1])] = stream(LIST_DATE + timedelta(days=10), [("Nifty 50", [("OUT", ["AAA"])])])
    notices[notice_name(DAYS[30])] = stream(DAYS[40], [("Nifty 500", [("IN", ["NOSUCH"])])])
    demerger = [{"page": 1, "top": 10, "text": "Corporate Adjustment: the spun-off entity shall be included in the "
                                               "following indices effective from March 1, 2021 (close of the day before)."},
                {"page": 1, "top": 20, "table": [["Sr. No.", "Index Name"], ["1", "Nifty 50"], ["2", "Nifty Bank"]]},
                {"page": 1, "top": 30, "text": "x" * 300}]
    notices[notice_name(DAYS[20])] = demerger
    f = world(notices=notices, current={**CURRENT, "NIFTY 500": ["AAA", "UNKNOWN1"]})
    r = build(f)
    ch = read(f, "nifty_changes.csv")
    assert ch[ch["effective_date"] == str(LIST_DATE + timedelta(days=10))]["status"].iloc[0] == "pending"
    assert ch[ch["symbol"] == "NOSUCH"]["status"].iloc[0] == "unmapped"
    rv = read(f, "review.csv")
    kinds = dict(rv["kind"].value_counts())
    assert kinds["unmapped_change"] == 1 and kinds["unmapped_current"] == 1 and kinds["not_applied"] == 1
    assert im.members("NIFTY 50", DAYS[-1], f) == sorted([sid(f, "AAA"), sid(f, "CCC")])
    assert "pending (effective after the current list): NIFTY 50 OUT AAA" in r["report"]


# ---- checks ----

def test_count_mismatch_ranges(world):
    f = world(current={**CURRENT, "NIFTY 50": ["AAA", "CCC", "DDD"]})
    build(f)
    rv = read(f, "review.csv")
    rows = rv[(rv["kind"] == "count_mismatch") & (rv["index"] == "NIFTY 50")]
    assert list(rows["details"]) == [f"3 members (size 2) from {DAYS[0]} to {DAYS[-1]}"]
    info = json.load(open(os.path.join(im.out_dir(f), "build_info.json")))
    n50 = next(s for s in info["size_checks"] if s["index"] == "NIFTY 50")
    assert n50["exact"] == 0 and n50["exact_adjusted"] == 0 and n50["days"] == N
    assert info["coverage"]["NIFTY 50"]["reliable_from"] is None
    with pytest.raises(im.OutOfCoverage, match="on no day"):
        im.members("NIFTY 50", DAYS[-1], f)
    assert len(im.members("NIFTY 50", DAYS[-1], f, allow_unreliable=True)) == 3


# ---- reliable_from ----

def test_reliable_from_rule():
    days = DAYS[:100]
    ok = [True] * 100
    assert im.reliable_from(days, ok, [])[0] == days[0]
    early = [i >= 10 for i in range(100)]                  # 10 days off at the start
    assert im.reliable_from(days, early, [])[0] == days[9]   # 1 off in 91 days = 98.9%; from day 8: 2 in 92 = 97.8%
    two = [i >= 2 for i in range(100)]
    assert im.reliable_from(days, two, [])[0] == days[0]     # exactly 98%
    snaps = [{"status": "compared", "day": str(days[50]), "mismatch": 1},
             {"status": "compared", "day": str(days[70]), "mismatch": 0},          # matching: no effect
             {"status": "outside coverage", "day": str(days[80])}]
    day, why = im.reliable_from(days, ok, snaps)
    assert day == days[51] and f"after the snapshot mismatch on {days[50]}" in why
    assert im.reliable_from(days, [False] * 100, [])[0] is None


def test_expected_sizes_rules():
    sets = [{"A", "B", "DVR1"}, {"A", "B", "DVR1"}, {"A", "B"}]
    assert im.expected_sizes("NIFTY 50", DAYS[:3], sets, {"DVR1"}, [DAYS[1]]) == [51, 50, 49]


def test_dvr_and_reit_are_expected_deviations(world):
    notices = base_notices()
    notices[notice_name(DAYS[140])] = stream(DAYS[150], [("Nifty 100", [("OUT", ["BBB"]), ("IN", ["EMBASSY"])])])
    f = world(notices=notices, current={**CURRENT, "NIFTY 50": ["AAA", "CCC", "ZZZDVR"],
                                        "NIFTY 100": ["AAA", "CCC", "NEWCO", "EMBASSY"]})
    r = build(f)
    info = json.load(open(os.path.join(im.out_dir(f), "build_info.json")))
    sizes = {s["index"]: s for s in info["size_checks"]}
    assert sizes["NIFTY 50"]["share"] == 0.0 and sizes["NIFTY 50"]["share_adjusted"] == 1.0      # 3 = 2 + DVR
    assert sizes["NIFTY 100"]["share"] < 1.0 and sizes["NIFTY 100"]["share_adjusted"] == 1.0     # 3 = 4 - REIT
    assert info["coverage"]["NIFTY 50"]["reliable_from"] == str(DAYS[0])
    assert info["coverage"]["NIFTY 100"]["reliable_from"] == str(DAYS[0])
    assert info["expected_rules"] == im.EXPECTED_RULES and info["reliable_share"] == 0.98
    rv = read(f, "review.csv")
    assert rv[(rv["kind"] == "count_mismatch") & rv["index"].isin(["NIFTY 50", "NIFTY 100"])].empty
    assert "Reliable from" in r["report"] and "expected deviations:" in r["report"]
    assert "raw 0 of 200 days (0.0%), adjusted 200 (100.0%)" in r["report"]


def test_members_before_reliable_from(world, capsys):
    f = world(snapshots={f"NIFTY_NEXT_50_{DAYS[30]:%Y%m%d}060000.csv": ["CCC", "AAA"]})   # mismatch on day 30
    build(f)
    assert im.coverage("NIFTY NEXT 50", f).reliable_from == DAYS[31]
    assert im.coverage("NIFTY 50", f).reliable_from == DAYS[0]
    with pytest.raises(im.OutOfCoverage, match=f"reliable \\(from {DAYS[31]}\\)"):
        im.members("NIFTY NEXT 50", DAYS[10], f)
    bbb = sid(f, "BBB")
    with pytest.raises(im.OutOfCoverage):
        im.is_member(bbb, "NIFTY NEXT 50", DAYS[10], f)
    assert len(im.members("NIFTY NEXT 50", DAYS[10], f, allow_unreliable=True)) == 2
    assert not im.is_member(bbb, "NIFTY NEXT 50", DAYS[10], f, allow_unreliable=True)
    assert len(im.members("NIFTY NEXT 50", DAYS[31], f)) == 2
    with pytest.raises(im.OutOfCoverage):                     # outside first..last even with the override
        im.members("NIFTY NEXT 50", DAYS[0] - timedelta(days=1), f, allow_unreliable=True)
    capsys.readouterr()
    assert cli.main(["members", "NIFTY NEXT 50", str(DAYS[10]), "--folder", f]) == 1
    assert "allow_unreliable" in capsys.readouterr().err
    assert cli.main(["members", "NIFTY NEXT 50", str(DAYS[10]), "--allow-unreliable", "--folder", f]) == 0
    out = capsys.readouterr().out
    assert "Note: before NIFTY NEXT 50 is reliable" in out and "2 members" in out
    rep = open(os.path.join(im.out_dir(f), "build_report.txt"), encoding="utf-8").read()
    assert f"{'NIFTY NEXT 50':20} {DAYS[31]}" in rep and "snapshots from then: 0 of 0 match" in rep


def test_composite_check_reports_the_days_off(world):
    f = world(current={**CURRENT, "NIFTY 100": ["AAA", "CCC", "BBB", "NEWCO", "SPLIT"]})
    r = build(f)
    assert "NIFTY 100 = NIFTY 50 + NIFTY NEXT 50 (from 2021-01-04): holds on 0 of 200 days (0.0%)" in r["report"]
    assert "in whole only: SPLIT" in r["report"]


def test_member_not_trading_review(world):
    f = world()
    build(f)
    rv = read(f, "review.csv")
    rows = rv[rv["kind"] == "member_not_trading"]
    assert set(rows["index"]) == {"NIFTY NEXT 50", "NIFTY 100"} and set(rows["symbol"]) == {"GONE"}
    assert f"({DAYS[61]} to {DAYS[ADHOC - 1]})" in rows["details"].iloc[0] and "29 trading days" in rows["details"].iloc[0]


def test_snapshot_timing_vs_mismatch(world):
    snaps = {f"NIFTY_NEXT_50_{DAYS[ADHOC - 2]:%Y%m%d}060000.csv": ["BBB", "DDD"],     # 2 days early: timing
             f"NIFTY_NEXT_50_{DAYS[30]:%Y%m%d}060000.csv": ["CCC", "AAA"],            # wrong: mismatch
             "NIFTY_NEXT_50_20190101060000.csv": ["AAA"]}                              # before the coverage
    f = world(snapshots=snaps)
    r = build(f)
    assert "same 1, timing 2, mismatch 0" in r["report"] and "same 1, timing 0, mismatch 2" in r["report"]
    assert "3 files, 2 inside the coverage" in r["report"]
    rv = read(f, "review.csv")
    assert set(rv[rv["kind"] == "snapshot_mismatch"]["symbol"]) == {"AAA", "GONE"}


def test_report_and_build_info(world):
    f = world()
    r = build(f)
    rep = r["report"]
    for part in ("Sources:", "Coverage (reliable from the sources):", "Members per index on the last trading day",
                 "Applied changes per index per year", "Size check", "Composite checks", "Snapshots", "AMFI half years:",
                 "Known-case checks:", "Review rows:"):
        assert part in rep
    info = json.load(open(os.path.join(im.out_dir(f), "build_info.json")))
    assert info["coverage"]["NIFTY 50"]["first"] == str(DAYS[0]) and info["amfi_lag"] == 5
    assert info["coverage"]["NIFTY MIDCAP 150"]["why"] == "no current list"
    assert info["reviews"]["2021"]["March"]["nifty50"] and not info["reviews"]["2021"]["September"]["nifty50"]
    for name in ("nifty_membership.parquet", "nifty_membership.csv", "nifty_changes.csv", "amfi.parquet", "amfi.csv",
                 "review.csv", "calendar.csv", "build_report.txt", "build_info.json"):
        assert os.path.isfile(os.path.join(im.out_dir(f), name))
    assert not [n for n in os.listdir(im.out_dir(f)) if n.endswith(".tmp")]


def test_known_checks_found_and_missed(world, monkeypatch):
    f = world()
    monkeypatch.setattr(im, "KNOWN_ALWAYS", [("AAA", "NIFTY 50")])
    monkeypatch.setattr(im, "KNOWN_LARGE", ["AAA"])
    monkeypatch.setattr(im, "KNOWN_HDFC", {"out": "BBB", "index": "NIFTY 50", "last_day": str(DAYS[R1 - 1]),
                                           "in_from": str(DAYS[R1]), "in": "CCC"})
    monkeypatch.setattr(im, "KNOWN_AMFI_PERIODS", 2)
    r = build(f)
    assert r["checks_ok"], r["report"]
    assert "CCC (" in r["report"] and "in NIFTY 100 the day before, AMFI MID then" in r["report"]
    lines, ok = im.check(f)
    assert ok and any("BBB" in x and "until" in x for x in lines)
    monkeypatch.setattr(im, "KNOWN_ALWAYS", [("BBB", "NIFTY 50")])
    lines, ok = im.check(f)
    assert not ok and any("NOT FOUND" in x and "BBB" in x for x in lines)


# ---- refusals, locks, inputs ----

def tree(folder):
    """{path: sha256} of every input file (the index_membership output folder and lock files left out)."""
    out = {}
    for base, _, files in os.walk(folder):
        parts = os.path.relpath(base, folder).split(os.sep)
        if parts[0] == im.OUT and (len(parts) < 2 or parts[1] != "input"):
            continue
        for n in files:
            if n == ".lock":
                continue
            p = os.path.join(base, n)
            with open(p, "rb") as fh:
                out[os.path.relpath(p, folder)] = hashlib.sha256(fh.read()).hexdigest()
    return out


def test_inputs_are_only_read(world):
    f = world()
    build(f)   # creates manual_changes.csv once
    before = tree(f)
    build(f)
    assert tree(f) == before


def test_locks_and_stale_master(world):
    f = world()
    lock = RunLock(os.path.join(f, "NSE_CM_bhav", ".lock")).acquire("test")
    try:
        with pytest.raises(im.StoreBusy):
            build(f)
        assert cli.main(["build", "--folder", f]) == 2
    finally:
        lock.release()
    lock = RunLock(os.path.join(sm.out_dir(f), ".lock")).acquire("test")
    try:
        with pytest.raises(im.StoreBusy):
            build(f)
    finally:
        lock.release()
    os.makedirs(im.out_dir(f), exist_ok=True)
    lock = RunLock(os.path.join(im.out_dir(f), ".lock")).acquire("test")
    try:
        with pytest.raises(im.LockBusy):
            build(f)
        assert cli.main(["build", "--folder", f]) == 2
    finally:
        lock.release()
    year = sm.year_files(os.path.join(f, "NSE_CM_bhav"))[0]
    later = os.path.getmtime(os.path.join(sm.out_dir(f), "build_info.json")) + 60
    os.utime(year, (later, later))
    with pytest.raises(im.InputStale):
        build(f)
    assert cli.main(["build", "--folder", f]) == 1


def test_not_built(tmp_path):
    write_store(str(tmp_path))
    with pytest.raises(im.NotBuilt):
        im.build(str(tmp_path), log=lambda *a: None)       # no security master
    sm.build(str(tmp_path), log=lambda *a: None)
    with pytest.raises(im.NotBuilt):
        im.members("NIFTY 50", DAYS[3], str(tmp_path))
    assert cli.main(["check", "--folder", str(tmp_path)]) == 1


# ---- CLI ----

def test_cli_codes(world, capsys):
    f = world()
    assert cli.main(["build", "--folder", f]) == 0
    assert "Index membership build" in capsys.readouterr().out
    assert cli.main(["members", "NIFTY 50", str(DAYS[R1]), "--folder", f]) == 0
    out = capsys.readouterr().out
    assert "NIFTY 50 on" in out and "2 members" in out and "CCC" in out
    assert cli.main(["members", "NIFTY 50", str(DAYS[0] - timedelta(days=5)), "--folder", f]) == 1
    assert cli.main(["show", "CCC", "--folder", f]) == 0
    out = capsys.readouterr().out
    assert "NIFTY NEXT 50" in out and "2020H2" in out
    assert cli.main(["show", "NOPE", "--folder", f]) == 1
    assert cli.main(["check", "--folder", f]) == 1                 # the real known cases are not in this data
    with pytest.raises(SystemExit) as e:
        cli.main(["members", "NIFTY 999", "2021-01-05", "--folder", f])
    assert e.value.code == 2
    with pytest.raises(SystemExit) as e:
        cli.main(["build", "--folder", "relative\\path"])
    assert e.value.code == 2
    with pytest.raises(SystemExit) as e:
        cli.main(["build", "--amfi-lag", "-1", "--folder", f])
    assert e.value.code == 2


# ---- fetch ----

class FakeClient:
    """get(url, kind) answers from a dict: bytes, an exception to raise, or missing (404)."""

    def __init__(self, answers):
        self.answers, self.calls, self.last_headers = dict(answers), [], {}

    def get(self, url, kind):
        self.calls.append(url)
        a = self.answers.get(url)
        if a is None:
            raise NotFound("404 not found")
        if isinstance(a, Exception):
            raise a
        self.last_headers = {"Last-Modified": "Mon, 05 Jan 2026 11:23:43 GMT"}
        return a

    def close(self):
        pass


XLSX = "https://www.amfiindia.com/Themes/Theme1/downloads/AverageMarketCapitalization31Dec2025.xlsx"
XLSX2 = "https://portal.amfiindia.com/spages/AverageMarketCapitalization30Jun2026.xlsx"
AMFI_HTML = ('<html><script>self.__next_f.push([1,"{\\"2025\\":[{\\"period\\":\\"Jul - Dec\\",\\"pdfUrl\\":\\"x.pdf\\",'
             f'\\"excelUrl\\":\\"{XLSX}\\"}}],\\"2026\\":[{{\\"period\\":\\"Jan - June\\",\\"pdfUrl\\":\\"y.pdf\\",'
             f'\\"excelUrl\\":\\"{XLSX2} \\"}}]}}"])</script></html>').encode()
PRESS_HTML = ("<html><a href='/Press_Release/ind_prs20022015.pdf' target=\"_blank\">Change in indices w.e.f March 27, "
              "2015.</a> <a href='/Press_Release/ind_prs14102026.pdf' target=\"_blank\">Changes in Nifty Fixed Income "
              "indices w.e.f. October 19, 2026</a> <a href='/Press_Release/ind_prs20022013.pdf'>Change in indices</a> "
              "<a href='/Press_Release/ind_prs17072023.pdf'>Corporate Adjustment for Reliance Industries Ltd. in Nifty "
              "indices</a> <a href='/Press_Release/ind_prs19072024.pdf'>NSE Indices launches Nifty IPO Index</a></html>").encode()


def answers():
    a = {fe.AMFI_PAGE: AMFI_HTML, XLSX: fixture("amfi_31Dec2025_sample.xlsx"), XLSX2: fixture("amfi_no_category_sample.xlsx"),
         fe.PRESS_PAGE: PRESS_HTML,
         fe.NIFTY_SITE + "/Press_Release/ind_prs20022015.pdf": fixture("ind_prs20022015.pdf"),
         fe.NIFTY_SITE + "/Press_Release/ind_prs17072023.pdf": fixture("ind_prs04072023.pdf")}
    for index, name in fe.INDEX_FILES.items():
        a[fe.CURRENT_URL.format(file=name)] = fixture("ind_nifty50list.csv")
    a[fe.CDX.format(url=fe.SNAPSHOT_HOSTS[0] + "ind_nifty50list.csv")] = json.dumps(
        [["timestamp", "statuscode", "digest", "original"],
         ["20180309064727", "200", "D1", "http://www.niftyindices.com:80/IndexConstituent/ind_nifty50list.csv"],
         ["20180310000000", "200", "D1", "http://www.niftyindices.com:80/IndexConstituent/ind_nifty50list.csv"],
         ["20190101000000", "404", "D2", "http://www.niftyindices.com:80/IndexConstituent/ind_nifty50list.csv"]]).encode()
    a[fe.WAYBACK.format(ts="20180309064727", url="http://www.niftyindices.com:80/IndexConstituent/ind_nifty50list.csv")] = \
        fixture("ind_nifty50list.csv")
    return a


def test_fetch_saves_skips_and_forces(tmp_path):
    f = str(tmp_path)
    client = FakeClient(answers())
    res = fe.fetch(f, client=client, today=date(2026, 10, 18), log=lambda *a: None)
    assert not res.failed
    amfi = os.listdir(fe.input_dir(f, "amfi"))
    assert {"AverageMarketCapitalization31Dec2025.xlsx", "AverageMarketCapitalization30Jun2026.xlsx",
            "categorisation-of-stocks_2026-10-18.html", "sources.csv"} == set(amfi)
    src = pd.read_csv(os.path.join(fe.input_dir(f, "amfi"), "sources.csv"), dtype=str, keep_default_na=False)
    assert set(src["period"]) == {"Jul - Dec", "Jan - June"} and set(src["last_modified"]) == {"Mon, 05 Jan 2026 11:23:43 GMT"}
    changes = set(os.listdir(fe.input_dir(f, "nifty_changes")))
    assert changes == {"ind_prs20022015.pdf", "ind_prs17072023.pdf", "press-release_2026-10-18.html", "notices.csv"}
    assert len(os.listdir(fe.input_dir(f, "nifty_current"))) == len(fe.INDEX_FILES)
    snaps = set(os.listdir(fe.input_dir(f, "snapshots")))
    assert "NIFTY_50_20180309064727.csv" in snaps and not any("20190101" in n for n in snaps)
    with open(os.path.join(fe.input_dir(f, "amfi"), "AverageMarketCapitalization31Dec2025.xlsx"), "rb") as fh:
        assert fh.read() == fixture("amfi_31Dec2025_sample.xlsx")             # saved unchanged

    client = FakeClient(answers())
    res = fe.fetch(f, client=client, today=date(2026, 10, 19), log=lambda *a: None)
    assert XLSX not in client.calls and fe.NIFTY_SITE + "/Press_Release/ind_prs20022015.pdf" not in client.calls
    assert res.count("skipped", "amfi") == 2 and res.count("refreshed", "nifty") == len(fe.INDEX_FILES)
    assert os.path.isfile(os.path.join(fe.input_dir(f, "nifty_current"), "NIFTY_50_2026-10-19.csv"))
    assert os.path.isfile(os.path.join(fe.input_dir(f, "nifty_current"), "NIFTY_50_2026-10-18.csv"))

    client = FakeClient(answers())
    res = fe.fetch(f, parts=("amfi",), client=client, force=True, today=date(2026, 10, 19), log=lambda *a: None)
    assert XLSX in client.calls and res.count("saved", "amfi") == 2
    assert fe.PRESS_PAGE not in client.calls


def test_fetch_blocked_prints_the_path(tmp_path, monkeypatch, capsys):
    f = str(tmp_path)
    a = answers()
    a[XLSX] = FetchError("HTTP 403; browser: HTTP 403")
    a[fe.CURRENT_URL.format(file="ind_nifty500list.csv")] = b"<html>blocked</html>"
    monkeypatch.setattr(fe, "new_client", lambda use_browser=True, log=print: FakeClient(a))
    assert cli.main(["fetch", "--amfi", "--nifty", "--folder", f]) == 1
    out = capsys.readouterr().out
    want = os.path.join(fe.input_dir(f, "amfi"), "AverageMarketCapitalization31Dec2025.xlsx")
    assert f"save it unchanged as {want}" in out and fe.AMFI_PAGE in out
    assert "not a constituent list" in out and "NIFTY_500_" in out
    assert not os.path.exists(want)


def test_fetch_uses_the_saved_page_when_the_page_fails(tmp_path):
    f = str(tmp_path)
    fe.fetch(f, parts=("changes",), client=FakeClient(answers()), today=date(2026, 10, 18), log=lambda *a: None)
    a = answers()
    a[fe.PRESS_PAGE] = FetchError("HTTP 403")
    res = fe.fetch(f, parts=("changes",), client=FakeClient(a), today=date(2026, 10, 19), log=lambda *a: None)
    assert len(res.failed) == 1 and res.count("skipped", "changes") == 2


def test_fetch_lock_and_cli_exit_codes(tmp_path, monkeypatch):
    f = str(tmp_path)
    monkeypatch.setattr(fe, "new_client", lambda use_browser=True, log=print: FakeClient(answers()))
    assert cli.main(["fetch", "--changes", "--folder", f]) == 0
    os.makedirs(fe.out_dir(f), exist_ok=True)
    lock = RunLock(os.path.join(fe.out_dir(f), ".lock")).acquire("test")
    try:
        assert cli.main(["fetch", "--folder", f]) == 2
    finally:
        lock.release()


def test_title_filter_and_links():
    links = fe.press_links(PRESS_HTML.decode())
    assert [x[3] for x in links] == ["ind_prs20022015.pdf", "ind_prs14102026.pdf", "ind_prs20022013.pdf",
                                     "ind_prs17072023.pdf", "ind_prs19072024.pdf"]
    kept = [x[3] for x in links if fe.wanted_notice(x[0], x[1])]
    assert kept == ["ind_prs20022015.pdf", "ind_prs17072023.pdf"]
    assert fe.notice_date("ind_prs23082024_1.pdf") == date(2024, 8, 23) and fe.notice_date("other.pdf") is None
    assert [u for _, _, u in fe.amfi_links(AMFI_HTML.decode())] == [XLSX, XLSX2]


def test_site_client_warms_each_site_once():
    class Session:
        def __init__(self):
            self.headers, self.calls = {}, []

        def get(self, url, timeout=None):
            self.calls.append(url)

            class R:
                status_code, content, ok = 200, b"PK\x03\x04", True
            return R()
    s = Session()
    c = fe.SiteClient(session=s, throttle=nse_fetch.Throttle(gap=0, sleep=lambda x: None), use_browser=False)
    c.get(XLSX, "xlsx")
    c.get(XLSX, "xlsx")
    c.get(XLSX2, "xlsx")
    assert s.calls == ["https://www.amfiindia.com/", XLSX, XLSX, "https://portal.amfiindia.com/", XLSX2]
    assert "Referer" not in s.headers

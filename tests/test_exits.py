"""backfill/equity/exits.py and exits.py on a small synthetic bhavcopy store in tmp_path, with the security master
and the adjustment factors built from it and synthetic reference lists with the real headers. No network; .env is
never read (config.load_env stubbed); the inputs are only read."""
import hashlib
import os
import time
from datetime import date, timedelta

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import config
import exits as cli
from backfill.equity import adjustments as adj
from backfill.equity import exits as ex
from backfill.equity import security_master as sm
from backfill.locks import RunLock

COLUMNS = ["date", "symbol", "series", "isin", "open", "high", "low", "close", "last", "prev_close", "volume",
           "turnover", "trades", "fin_id", "format"]


def weekdays(first, n):
    out, d = [], first
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


N = 160
DAYS = weekdays(date(2021, 1, 4), N)

# the real headers of the reference lists (nse_reference.py, 2026-10-17)
HEADERS = {
    "mergers.csv": ["circular_no", "circular_date", "department", "old_symbol", "old_company", "scheme_type",
                    "exit_expected", "new_company", "new_symbol", "swap_ratio", "new_per_old", "suspended_from",
                    "last_trading_date", "record_date", "reason_text", "corrected_by", "pdf_url", "source_file",
                    "needs_review", "review_note"],
    "manual_mergers.csv": ["old_symbol", "new_company", "new_symbol", "swap_ratio", "scheme_type", "exit_expected",
                           "note"],
    "voluntary_delisting.csv": ["list", "symbol", "isin", "company", "board", "delisting_date", "delisting_type",
                                "initial_notice_date", "final_notice_date", "exit_price", "source_file",
                                "needs_review", "review_note"],
    "delisting_circulars.csv": ["circular_no", "circular_date", "department", "subject", "delisting_type", "symbol",
                                "company", "effective_date", "pdf_url", "source_file", "needs_review", "review_note"],
    "compulsory_delisting.csv": ["order_date", "company", "symbol", "symbol_source", "pdf_url", "source_file",
                                 "needs_review", "review_note"],
    "suspensions.csv": ["circular_no", "circular_date", "department", "status", "symbol", "company", "effective_date",
                        "reason", "exit_expected", "reason_text", "cancels_circular", "corrected_by", "subject",
                        "pdf_url", "source_file", "needs_review", "review_note"],
}
CA_HEADER = ["SYMBOL", "COMPANY NAME", "SERIES", "PURPOSE", "FACE VALUE", "EX-DATE", "RECORD DATE",
             "BOOK CLOSURE START DATE", "BOOK CLOSURE END DATE"]


@pytest.fixture(autouse=True)
def no_env(monkeypatch):
    monkeypatch.setattr(config, "load_env", lambda: {})


def r(a, b):
    return list(range(a, b))


def trade(symbol, isin, idx, close=100.0, series="EQ", turnover=1000.0, closes=None):
    rows = []
    for j, i in enumerate(idx):
        c = closes[j] if closes is not None else close
        t = turnover(i) if callable(turnover) else turnover
        s = series(i) if callable(series) else series
        rows.append({"date": DAYS[i], "symbol": symbol, "series": s, "isin": isin, "open": c, "high": c, "low": c,
                     "close": c, "last": c, "prev_close": c, "volume": 10, "turnover": t, "trades": 1,
                     "fin_id": "1", "format": "udiff"})
    return rows


def falling():
    """FALLER: flat at 100 for 0..38, then down to 30 by 98 in BE from 59, turnover a tenth in the last 60 days."""
    idx = r(0, 99)
    closes = [100.0 if i < 39 else round(100 - 70 * (i - 38) / 60, 2) for i in idx]
    return trade("FALLER", "INE060A01011", idx, closes=closes, series=lambda i: "BE" if i >= 59 else "EQ",
                 turnover=lambda i: 100.0 if i >= 39 else 1000.0)


def split_co():
    """SPLITCO: 500 until a 1:5 split on day 70, then 100; last day 98 (the corporate action confirms the split)."""
    return trade("SPLITCO", "INE070A01011", r(0, 70), close=500.0) + \
        trade("SPLITCO", "INE070A01011", r(70, 99), close=100.0)


def rows_all():
    rows = []
    rows += trade("ACTIVE1", "INE001A01011", r(0, N))
    rows += trade("DORM", "INE002A01011", r(0, 130))                         # 30 trading days ago: DORMANT
    rows += trade("GAPPER", "INE003A01011", r(0, 41)) + trade("GAPPER", "INE003A01011", r(101, N))
    rows += trade("OLDCO", "INE010A01011", r(0, 50))                         # merger with a ratio
    rows += trade("BUYER", "INE011A01011", r(0, 100)) + trade("BUYERNEW", "INE011A01011", r(100, N))
    rows += trade("NORATIO", "INE012A01011", r(0, 50))                       # merger, ratio unknown
    rows += trade("GHOSTED", "INE013A01011", r(0, 41))                       # merger, acquirer unknown
    rows += trade("MANUALCO", "INE014A01011", r(0, 41))                      # only in manual_mergers.csv
    rows += trade("VOLDEL", "INE015A01011", r(0, 61))                        # voluntary delisting (by ISIN)
    rows += trade("COMPDEL", "INE016A01011", r(0, 31))                       # compulsory order years later
    rows += trade("LIQ", "INE017A01011", r(0, 41))                           # suspension, reason liquidation
    rows += trade("LIQTXT", "INE018A01011", r(0, 41))                        # reason blank, winding-up text
    rows += trade("SUSP", "INE019A01011", r(0, 41))                          # suspension, non-compliance
    rows += trade("PRIO", "INE022A01011", r(0, 41))                          # merger + suspension + delisting
    rows += trade("SMEMV", "INE020A01011", r(0, 50)) + trade("SMEMV", "INE020A01011", r(60, N), series="SM")
    rows += trade("SMEUP", "INE021A01011", r(0, 80), series="SM") + trade("SMEUP", "INE021A01011", r(90, N))
    rows += trade("RELI", "INE030A01011", r(0, 41)) + trade("RELI", "INE030A01029", r(120, N))
    rows += trade("ABC-RE", "INE040A01011", r(10, 31))
    rows += trade("XYZ-RE1", "INE041A01011", r(10, 31))
    rows += trade("ETFX", "INF050A01011", r(0, 41))
    rows += falling() + split_co()
    rows += trade("QUIET", "INE080A01011", r(0, 41))                         # suspension cancelled: UNKNOWN
    rows += trade("LATE1", "INE090A01011", r(1, N))                          # day 2 of the data
    rows += trade("IPO1", "INE091A01011", r(50, N))
    for k in (1, 2, 3):
        rows += trade(f"BATCH{k}", f"INE10{k}A01011", r(100, N))
    return rows


def ref_rows():
    d = DAYS
    return {
        "mergers.csv": [
            {"circular_no": "C1", "old_symbol": "OLDCO", "scheme_type": "MERGER", "exit_expected": "Y",
             "new_company": "Buyer Limited", "new_symbol": "BUYER", "swap_ratio": "3 for 2",
             "last_trading_date": str(d[49])},
            {"circular_no": "C2", "old_symbol": "NORATIO", "scheme_type": "MERGER", "exit_expected": "Y",
             "last_trading_date": str(d[52])},
            {"circular_no": "C3", "old_symbol": "GHOSTED", "scheme_type": "MERGER", "exit_expected": "Y",
             "new_symbol": "GHOST", "swap_ratio": "1 for 1", "last_trading_date": str(d[40])},
            {"circular_no": "C4", "old_symbol": "PRIO", "scheme_type": "MERGER", "exit_expected": "Y",
             "new_symbol": "BUYER", "last_trading_date": str(d[40])},
            {"circular_no": "C5", "old_symbol": "ACTIVE1", "scheme_type": "CAPITAL_REDUCTION", "exit_expected": "N",
             "last_trading_date": str(d[40])},
        ],
        "manual_mergers.csv": [
            {"old_symbol": "NORATIO", "new_company": "Buyer Limited", "new_symbol": "BUYER"},
            {"old_symbol": "MANUALCO", "new_company": "Buyer Limited", "new_symbol": "BUYERNEW",
             "scheme_type": "MERGER", "exit_expected": "Y"},
        ],
        "voluntary_delisting.csv": [
            {"list": "delisted", "symbol": "OTHERNAME", "isin": "INE015A01011", "board": "Main Board",
             "delisting_date": str(d[70]), "delisting_type": "Voluntary Delisting"},
            {"list": "delisted", "symbol": "PRIO", "isin": "", "board": "Main Board",
             "delisting_date": str(d[50]), "delisting_type": "Voluntary Delisting"},
            {"list": "delisted", "symbol": "NOSUCH", "isin": "INE999X01011", "board": "Main Board",
             "delisting_date": str(d[50]), "delisting_type": "Voluntary Delisting"},
            {"list": "proposed", "symbol": "ACTIVE1", "isin": "", "board": "Main Board", "delisting_date": "",
             "delisting_type": ""},
        ],
        "delisting_circulars.csv": [
            {"circular_no": "D1", "circular_date": str(d[60]), "delisting_type": "voluntary", "symbol": "LATECIRC",
             "effective_date": str(d[60])},
        ],
        "compulsory_delisting.csv": [
            {"order_date": "2023-06-01", "symbol": "COMPDEL"},
            {"order_date": str(d[100]), "symbol": "ACTIVE1"},
        ],
        "suspensions.csv": [
            {"circular_no": "S1", "circular_date": str(d[38]), "status": "SUSPENDED", "symbol": "LIQ",
             "effective_date": str(d[41]), "reason": "liquidation", "exit_expected": "Y"},
            {"circular_no": "S2", "circular_date": str(d[38]), "status": "SUSPENDED", "symbol": "LIQTXT",
             "effective_date": str(d[41]), "reason": "", "reason_text": "order of winding up passed by the court",
             "exit_expected": "Y"},
            {"circular_no": "S3", "circular_date": str(d[38]), "status": "SUSPENDED", "symbol": "SUSP",
             "effective_date": str(d[41]), "reason": "non-compliance with listing rules", "exit_expected": "N"},
            {"circular_no": "S4", "circular_date": str(d[38]), "status": "SUSPENDED", "symbol": "PRIO",
             "effective_date": str(d[41]), "reason": "non-compliance with listing rules", "exit_expected": "N"},
            {"circular_no": "S5", "circular_date": str(d[38]), "status": "SUSPENDED", "symbol": "QUIET",
             "effective_date": str(d[41]), "reason": "non-compliance with listing rules", "exit_expected": "N"},
            {"circular_no": "S6", "circular_date": str(d[40]), "status": "CANCELLED", "symbol": "QUIET",
             "effective_date": str(d[41]), "reason": "", "exit_expected": "N", "cancels_circular": "S5"},
            {"circular_no": "S7", "circular_date": str(d[38]), "status": "SUSPENDED", "symbol": "ACTIVE1",
             "effective_date": str(d[41]), "reason": "liquidation", "exit_expected": "Y"},
            {"circular_no": "S8", "circular_date": str(d[38]), "status": "SUSPENDED", "symbol": "DORM",
             "effective_date": str(d[41]), "reason": "non-compliance with listing rules", "exit_expected": "N"},
        ],
    }


def write_store(folder, rows):
    store = os.path.join(folder, "NSE_CM_bhav")
    os.makedirs(store, exist_ok=True)
    fill = trade("FILLER", "INE999Z01011", r(0, N), series="N1")
    df = pd.DataFrame(rows + fill, columns=COLUMNS)
    for year, part in df.groupby(df["date"].map(lambda x: x.year)):
        path = os.path.join(store, f"{year}.parquet")
        pq.write_table(pa.Table.from_pandas(part.reset_index(drop=True), preserve_index=False), path)
        old = time.time() - 120
        os.utime(path, (old, old))
    return store


def write_refs(folder, refs=None, skip=()):
    ref = os.path.join(folder, "NSE_CM_bhav", "reference")
    os.makedirs(ref, exist_ok=True)
    for name, cols in HEADERS.items():
        if name in skip:
            continue
        rows = (refs or ref_rows()).get(name, [])
        pd.DataFrame([{c: x.get(c, "") for c in cols} for x in rows], columns=cols) \
            .to_csv(os.path.join(ref, name), index=False, lineterminator="\n")


def write_ca(folder):
    path = os.path.join(folder, "adjustments", "input")
    os.makedirs(path, exist_ok=True)
    pd.DataFrame([["SPLITCO", "SPLITCO LTD", "EQ", "Face Value Split From Rs 10 To Rs 2", "10",
                   f"{DAYS[70]:%d-%b-%Y}", "-", "-", "-"]], columns=CA_HEADER) \
        .to_csv(os.path.join(path, "ca.csv"), index=False)


def prepare(tmp_path, rows=None, refs=None):
    f = str(tmp_path)
    write_store(f, rows if rows is not None else rows_all())
    sm.build(f, log=lambda *_: None)
    write_ca(f)
    adj.build(f, log=lambda *_: None)
    write_refs(f, refs)
    return f


def run(f, **kw):
    kw = {"batch_min": 3, "start_days": 2, **kw}
    return ex.build(f, log=lambda *_: None, **kw)


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("exits")
    mp = pytest.MonkeyPatch()
    mp.setattr(config, "load_env", lambda: {})
    f = prepare(tmp)
    result = run(f)
    yield f, result
    mp.undo()


def read(f, name):
    return pd.read_csv(os.path.join(f, "exits", name), dtype=str, keep_default_na=False)


def exit_of(f, symbol):
    e = read(f, "exits.csv")
    rows = e[e["last_symbol"] == symbol]
    assert len(rows) == 1, f"{symbol}: {len(rows)} exit rows"
    return rows.iloc[0]


def status_of(f, symbol):
    s = read(f, "status.csv").merge(read(f, "exits.csv")[["security_id"]], how="left")
    ids = sm.lookup(symbol, f)
    return s[s["security_id"].isin(ids)].reset_index(drop=True)


def sid(f, symbol, i=0):
    return sm.lookup(symbol, f)[i]


def tree(folder):
    out = {}
    for root, _, files in os.walk(folder):
        for n in files:
            if n == ".lock":
                continue
            p = os.path.join(root, n)
            with open(p, "rb") as fh:
                out[os.path.relpath(p, folder)] = hashlib.sha256(fh.read()).hexdigest()
    return out


# ---- Part 1: status ----

def test_status_thresholds():
    assert ex.status_of(0) == "ACTIVE" and ex.status_of(19) == "ACTIVE"
    assert ex.status_of(20) == "DORMANT" and ex.status_of(60) == "DORMANT"
    assert ex.status_of(61) == "EXITED"
    assert ex.status_of(5, active_days=5, exit_days=10) == "DORMANT"
    assert ex.status_of(11, active_days=5, exit_days=10) == "EXITED"


def test_status_per_security(built):
    f, _ = built
    assert status_of(f, "ACTIVE1")["status"].tolist() == ["ACTIVE"]
    assert status_of(f, "DORM")["status"].tolist() == ["DORMANT"]
    assert status_of(f, "OLDCO")["status"].tolist() == ["EXITED"]
    e = read(f, "exits.csv")
    assert "ACTIVE1" not in set(e["last_symbol"]) and "DORM" not in set(e["last_symbol"])


def test_gap_then_return_is_not_exited(built):
    f, _ = built
    s = status_of(f, "GAPPER")
    assert s["status"].tolist() == ["ACTIVE"]
    assert f"gaps: {DAYS[41]} to {DAYS[100]} (60 days)" in s["listing_note"].iloc[0]


def test_thresholds_as_options(tmp_path):
    f = prepare(tmp_path)
    run(f, exit_days=200, active_days=100)
    s = read(f, "status.csv")
    assert "EXITED" not in set(s["status"])
    assert read(f, "exits.csv").empty
    assert s[s["security_id"] == sid(f, "DORM")]["status"].iloc[0] == "ACTIVE"


# ---- Part 2: reasons ----

def test_merger_with_ratio_and_acquirer_through_a_rename(built):
    f, _ = built
    x = exit_of(f, "OLDCO")
    assert (x.reason, x.reason_source, x.reason_date, x.reason_distance) == \
        ("MERGER", "mergers.csv row 1", str(DAYS[49]), "0")
    assert x.acquirer_security_id == sid(f, "BUYER") and x.acquirer_symbol == "BUYER"
    assert x.swap_ratio == "3 for 2" and "1.5 new per old" in x.note
    assert "acquirer now BUYERNEW" in x.note
    assert x.suggested_treatment == "CONVERT"


def test_merger_without_ratio_filled_from_manual(built):
    f, _ = built
    x = exit_of(f, "NORATIO")
    assert x.reason == "MERGER" and x.reason_distance == "3"
    assert x.acquirer_security_id == sid(f, "BUYER") and x.swap_ratio == ""
    assert x.suggested_treatment == "CASH_AT_LAST"


def test_manual_merger_without_a_list_row(built):
    f, _ = built
    x = exit_of(f, "MANUALCO")
    assert x.reason == "MERGER" and x.reason_source == "manual_mergers.csv row 2" and x.reason_date == ""
    # named by the acquirer's later symbol: still resolved, with a note
    assert x.acquirer_security_id == sid(f, "BUYER") and "BUYERNEW is not that security's symbol" in x.note
    assert x.suggested_treatment == "CASH_AT_LAST"


def test_unresolved_acquirer_goes_to_review(built):
    f, _ = built
    x = exit_of(f, "GHOSTED")
    assert x.reason == "MERGER" and x.acquirer_security_id == "" and x.acquirer_symbol == "GHOST"
    assert x.suggested_treatment == "REVIEW"           # a ratio but no acquirer
    rv = read(f, "review.csv")
    assert ((rv["kind"] == "unresolved_acquirer") & (rv["symbol"] == "GHOSTED")).any()


def test_exit_expected_n_is_not_a_merger(built):
    f, _ = built
    rv = read(f, "review.csv")
    assert not rv["details"].str.contains("mergers.csv row 5").any()   # skipped, not unmatched
    assert "mergers.csv: exit_expected N 1" in built[1]["report"]


def test_voluntary_delisting_by_isin(built):
    f, _ = built
    x = exit_of(f, "VOLDEL")
    assert (x.reason, x.reason_source, x.reason_distance) == ("VOLUNTARY_DELISTING", "voluntary_delisting.csv row 1",
                                                              "10")
    assert x.suggested_treatment == "CASH_AT_LAST"


def test_compulsory_delisting_years_later(built):
    f, _ = built
    x = exit_of(f, "COMPDEL")
    assert x.reason == "COMPULSORY_DELISTING" and x.reason_date == "2023-06-01"
    assert int(x.reason_distance) > 400
    assert x.suggested_treatment == "WRITE_DOWN"


def test_liquidation_from_the_reason_and_from_the_text(built):
    f, _ = built
    for sym in ("LIQ", "LIQTXT"):
        x = exit_of(f, sym)
        assert (x.reason, x.reason_source, x.reason_distance) == ("LIQUIDATION", x.reason_source, "1")
        assert x.reason_source.startswith("suspensions.csv")
        assert x.suggested_treatment == "WRITE_DOWN"


def test_liquidation_regex():
    for text in ("liquidation", "IBC / resolution plan", "winding up", "wound up", "NCLT order", "insolvency"):
        assert ex.LIQUIDATION_RX.search(text), text
    assert not ex.LIQUIDATION_RX.search("non-compliance with listing rules")


def test_capital_reduction_reason_with_nclt_text_is_not_liquidation(tmp_path):
    refs = ref_rows()
    refs["suspensions.csv"].append({"circular_no": "S9", "status": "SUSPENDED", "symbol": "QUIET",
                                    "effective_date": str(DAYS[42]), "reason": "capital reduction",
                                    "reason_text": "scheme approved by the NCLT", "exit_expected": "N"})
    f = prepare(tmp_path, refs=refs)
    run(f)
    assert exit_of(f, "QUIET").reason == "SUSPENDED"


def test_suspension(built):
    f, _ = built
    x = exit_of(f, "SUSP")
    assert (x.reason, x.reason_distance, x.suggested_treatment) == ("SUSPENDED", "1", "REVIEW")


def test_cancelled_suspension_is_ignored(built):
    f, _ = built
    x = exit_of(f, "QUIET")
    assert x.reason == "UNKNOWN" and x.reason_source == ""
    assert "suspensions.csv: cancelled later 1" in built[1]["report"]


def test_priority_and_conflict(built):
    f, _ = built
    x = exit_of(f, "PRIO")
    assert x.reason == "MERGER" and x.reason_source == "mergers.csv row 4"
    assert "also VOLUNTARY_DELISTING voluntary_delisting.csv row 2" in x.note
    assert "also SUSPENDED suspensions.csv row 4" in x.note
    rv = read(f, "review.csv")
    c = rv[(rv["kind"] == "conflict") & (rv["symbol"] == "PRIO")]
    assert len(c) == 1 and c["details"].iloc[0].startswith("chose MERGER")


def late_rows():
    return rows_all() + sum((trade(s, f"INE2{k:02d}A01011", r(0, 41)) for k, s in
                             enumerate(("LIQV", "LIQC", "LIQS", "VOLFAR", "SUSFAR", "COMPC", "VOLIBC",
                                        "VOLLATE"))), [])


def late_refs():
    """Rows far after the last date (day 40): liquidation and compulsory ones match, the others do not."""
    refs = ref_rows()
    far = str(DAYS[150])
    refs["voluntary_delisting.csv"] += [
        {"list": "delisted", "symbol": "LIQV", "board": "Main Board", "delisting_date": far,
         "delisting_type": "Delisting - Liquidation"},
        {"list": "delisted", "symbol": "VOLFAR", "board": "Main Board", "delisting_date": far,
         "delisting_type": "Voluntary Delisting"},
        {"list": "delisted", "symbol": "VOLIBC", "board": "Main Board", "delisting_date": str(DAYS[45]),
         "delisting_type": "Voluntary Delisting"},
        {"list": "delisted", "symbol": "VOLLATE", "board": "Main Board", "delisting_date": str(DAYS[45]),
         "delisting_type": "Voluntary Delisting"}]
    refs["delisting_circulars.csv"] += [
        {"circular_no": "D2", "delisting_type": "ibc", "symbol": "LIQC", "effective_date": "2022-06-01"},
        {"circular_no": "D3", "delisting_type": "compulsory", "symbol": "COMPC", "effective_date": far},
        {"circular_no": "D4", "delisting_type": "ibc", "symbol": "VOLIBC", "effective_date": str(DAYS[60])}]
    refs["suspensions.csv"] += [
        {"circular_no": "S10", "status": "SUSPENDED", "symbol": "LIQS", "effective_date": far,
         "reason": "IBC / resolution plan", "exit_expected": "Y"},
        {"circular_no": "S11", "status": "SUSPENDED", "symbol": "SUSFAR", "effective_date": str(DAYS[70]),
         "reason": "non-compliance with listing rules", "exit_expected": "Y"},
        {"circular_no": "S12", "status": "SUSPENDED", "symbol": "VOLLATE", "effective_date": far,
         "reason": "liquidation", "exit_expected": "Y"}]
    return refs


@pytest.fixture(scope="module")
def late(tmp_path_factory):
    mp = pytest.MonkeyPatch()
    mp.setattr(config, "load_env", lambda: {})
    f = prepare(tmp_path_factory.mktemp("late"), rows=late_rows(), refs=late_refs())
    result = run(f)
    yield f, result
    mp.undo()


def test_liquidation_and_compulsory_match_any_later_date(late):
    f, _ = late
    for sym, source in (("LIQV", "voluntary_delisting.csv"), ("LIQC", "delisting_circulars.csv"),
                        ("LIQS", "suspensions.csv")):
        x = exit_of(f, sym)
        assert x.reason == "LIQUIDATION" and x.reason_source.startswith(source), sym
        assert int(x.reason_distance) >= 110
    x = exit_of(f, "COMPC")
    assert x.reason == "COMPULSORY_DELISTING" and x.reason_distance == "110"


def test_voluntary_and_ordinary_suspension_keep_their_windows(late):
    f, _ = late
    assert exit_of(f, "VOLFAR").reason == "UNKNOWN"     # voluntary: 0..60 only
    assert exit_of(f, "SUSFAR").reason == "UNKNOWN"     # ordinary suspension: 0..10 only
    u = read(f, "review.csv")
    u = u[u["kind"] == "unmatched_reference"]["details"]
    assert u.str.contains(r"date outside the window: \S+ last \S+ \(\+110 trading days\)").any()


def test_liquidation_beats_voluntary(late):
    f, _ = late
    x = exit_of(f, "VOLIBC")
    assert x.reason == "LIQUIDATION" and x.reason_source.startswith("delisting_circulars.csv")
    assert "also VOLUNTARY_DELISTING voluntary_delisting.csv" in x.note
    assert x.suggested_treatment == "WRITE_DOWN"
    rv = read(f, "review.csv")
    c = rv[(rv["kind"] == "conflict") & (rv["symbol"] == "VOLIBC")]
    assert len(c) == 1 and c["details"].iloc[0].startswith("chose LIQUIDATION: liquidation")
    assert "part of the insolvency" in c["details"].iloc[0]


def test_liquidation_long_after_voluntary_leaves_voluntary(late):
    f, _ = late
    x = exit_of(f, "VOLLATE")                            # voluntary day 45, liquidation suspension day 150
    assert x.reason == "VOLUNTARY_DELISTING" and x.suggested_treatment == "CASH_AT_LAST"
    assert "also LIQUIDATION suspensions.csv" in x.note
    rv = read(f, "review.csv")
    c = rv[(rv["kind"] == "conflict") & (rv["symbol"] == "VOLLATE")]
    assert len(c) == 1
    assert c["details"].iloc[0].startswith("chose VOLUNTARY_DELISTING: liquidation")
    assert "is 105 trading days after the voluntary delisting" in c["details"].iloc[0]
    assert "shareholders paid out first" in c["details"].iloc[0]


def test_pick_liquidation_over_voluntary():
    vol = ex.match("S1", "VOLUNTARY_DELISTING", "voluntary_delisting.csv", 1, DAYS[5], 5)

    def liq(dist, row=2):
        return ex.match("S1", "LIQUIDATION", "suspensions.csv", row, DAYS[dist], dist)
    at, past, before = liq(5 + 60), liq(5 + 61), liq(1)
    chosen, why = ex.pick_with_why([vol, at])            # exactly 60 trading days after: part of the insolvency
    assert chosen is at and "part of the insolvency" in why
    chosen, why = ex.pick_with_why([vol, past])          # 61 after: shareholders were paid out first
    assert chosen is vol and "is 61 trading days after the voluntary delisting" in why
    assert ex.pick([vol, before]) is before              # liquidation before the delisting
    assert ex.pick([vol, past, at]) is at                # one within the window is enough
    merger = ex.match("S1", "MERGER", "mergers.csv", 3, DAYS[1], 1)
    assert ex.pick_with_why([vol, at, merger]) == (merger, "")   # only the voluntary choice gives way


@pytest.mark.parametrize("day, expected", [(100, "LIQUIDATION"), (130, "RELISTED")])
def test_no_match_on_or_after_a_relisting(tmp_path, day, expected):
    """RELI stops on day 40 and comes back under a new ISIN on day 120: a liquidation circular for RELI dated on or
    after day 120 belongs to the new security, never to the old one."""
    refs = ref_rows()
    refs["delisting_circulars.csv"].append({"circular_no": "D9", "delisting_type": "liquidation", "symbol": "RELI",
                                            "effective_date": str(DAYS[day])})
    f = prepare(tmp_path, refs=refs)
    run(f)
    x = exit_of(f, "RELI")
    assert x.reason == expected and x.relisted_as == sm.lookup("RELI", f)[1]
    if expected == "RELISTED":
        u = read(f, "review.csv")["details"]
        assert u.str.contains(f"after relisting as {x.relisted_as} on {DAYS[120]}").any()


def test_pick_order():
    m = [ex.match("S1", "SUSPENDED", "suspensions.csv", 1, DAYS[1], 1),
         ex.match("S1", "LIQUIDATION", "delisting_circulars.csv", 2, DAYS[9], 9),
         ex.match("S1", "LIQUIDATION", "suspensions.csv", 3, DAYS[2], 2),
         ex.match("S1", None, "delisting_circulars.csv", 4, DAYS[1], 1)]
    chosen = ex.pick(m)
    assert chosen["reason"] == "LIQUIDATION" and chosen["row"] == 3
    assert ex.pick([m[3]]) is None
    assert ex.REASONS[:4] == ["MERGER", "VOLUNTARY_DELISTING", "COMPULSORY_DELISTING", "LIQUIDATION"]


def test_moved_to_sme(built):
    f, _ = built
    x = exit_of(f, "SMEMV")
    assert x.reason == "MOVED_TO_SME" and x.reason_date == str(DAYS[60]) and x.reason_source == "bhavcopy SM"
    assert x.suggested_treatment == "CASH_AT_LAST"


def test_relisted_and_relisting(built):
    f, _ = built
    old, new = sm.lookup("RELI", f)
    x = exit_of(f, "RELI")
    assert x.security_id == old and x.reason == "RELISTED" and x.relisted_as == new
    assert x.reason_date == str(DAYS[120]) and x.suggested_treatment == "CASH_AT_LAST"
    s = read(f, "status.csv").set_index("security_id")
    assert s.at[new, "listing_kind"] == "RELISTING" and old in s.at[new, "listing_note"]


def test_rights_and_etf_ended(built):
    f, _ = built
    for sym in ("ABC-RE", "XYZ-RE1"):                   # numbered rights entitlements are RIGHTS too
        x = exit_of(f, sym)
        assert (x.reason, x.instrument_kind, x.suggested_treatment) == ("RIGHTS_ENDED", "RIGHTS", "NOT_APPLICABLE")
    x = exit_of(f, "ETFX")
    assert x.reason == "ETF_ENDED" and x.instrument_kind == "ETF" and x.suggested_treatment == "NOT_APPLICABLE"


def test_unmatched_reference_rows_go_to_review(built):
    f, result = built
    rv = read(f, "review.csv")
    u = rv[rv["kind"] == "unmatched_reference"]
    # symbols / ISINs that never trade in the store are only counted in the report
    assert not u["details"].str.contains("NOSUCH|LATECIRC").any()
    assert ("never appears in the bhavcopy (not in review.csv): delisting_circulars.csv 1, "
            "voluntary_delisting.csv 1") in result["report"]
    assert u["details"].str.contains("compulsory_delisting.csv row 2").any()     # ACTIVE1 still trades
    assert u["details"].str.contains("suspensions.csv row 7").any()              # exit expected, none seen
    assert not u["details"].str.contains("suspensions.csv row 8").any()          # DORM: no exit expected
    assert not u["details"].str.contains("voluntary_delisting.csv row 4").any()  # proposed: skipped


# ---- Part 3: facts for a backtest ----

def test_distress_hints_and_write_down(built):
    f, _ = built
    x = exit_of(f, "FALLER")
    assert x.reason == "UNKNOWN" and x.suggested_treatment == "WRITE_DOWN" and "distress hints" in x.note
    assert float(x.distress_price_change) == pytest.approx(-70.0, abs=1.5)
    assert int(x.distress_be_bz_days) == 40
    assert float(x.distress_turnover_ratio) == pytest.approx(0.1)


def test_unknown_without_distress_is_review(built):
    f, _ = built
    x = exit_of(f, "QUIET")
    assert x.suggested_treatment == "REVIEW" and x.distress_turnover_ratio == ""
    rv = read(f, "review.csv")
    assert set(rv[rv["kind"] == "unknown_large"]["symbol"]) >= {"QUIET", "FALLER"}


def test_distress_uses_adjusted_closes(built):
    f, _ = built
    x = exit_of(f, "SPLITCO")
    assert float(x.distress_price_change) == pytest.approx(0.0)     # raw closes fall 80%: a split, not distress
    assert x.suggested_treatment == "REVIEW"


def test_last_close_and_adjusted(built, monkeypatch):
    f, _ = built
    x = exit_of(f, "SPLITCO")
    s = sid(f, "SPLITCO")
    assert float(x.last_close) == 100.0
    assert float(x.last_close_adj) == pytest.approx(100.0 / adj.factor_on(s, DAYS[98], f))
    # a factor other than 1 on the last date divides the last close
    real = adj.load_factors(f)
    periods = dict(real["periods"])
    periods[s] = ([DAYS[0]], [DAYS[98]], [4.0])
    monkeypatch.setattr(adj, "load_factors", lambda folder=None: {**real, "periods": periods})
    frames = ex.build_frames(f, ex.ACTIVE_DAYS, ex.EXIT_DAYS, 3, 2, log=lambda *_: None)
    row = frames["exits"].set_index("security_id").loc[s]
    assert row["last_close_adj"] == pytest.approx(25.0)


def test_treatment_table():
    assert ex.treatment("MERGER", "S1", "2 for 1", False) == "CONVERT"
    assert ex.treatment("MERGER", "S1", "", False) == "CASH_AT_LAST"
    assert ex.treatment("MERGER", None, "", False) == "CASH_AT_LAST"
    assert ex.treatment("MERGER", None, "2 for 1", False) == "REVIEW"
    for reason in ("VOLUNTARY_DELISTING", "MOVED_TO_SME", "RELISTED"):
        assert ex.treatment(reason, None, "", True) == "CASH_AT_LAST"
    for reason in ("LIQUIDATION", "COMPULSORY_DELISTING"):
        assert ex.treatment(reason, None, "", False) == "WRITE_DOWN"
    assert ex.treatment("UNKNOWN", None, "", True) == "WRITE_DOWN"
    for reason in ("UNKNOWN", "SUSPENDED"):
        assert ex.treatment(reason, None, "", False) == "REVIEW"
    for reason in ("RIGHTS_ENDED", "ETF_ENDED"):
        assert ex.treatment(reason, None, "", True) == "NOT_APPLICABLE"


def test_exits_columns(built):
    f, _ = built
    assert list(read(f, "exits.csv").columns) == ex.EXIT_COLUMNS
    assert list(read(f, "status.csv").columns) == ex.STATUS_COLUMNS
    assert list(read(f, "review.csv").columns) == ex.REVIEW_COLUMNS
    assert list(read(f, "listings.csv").columns) == ex.LISTING_COLUMNS
    p = pq.read_table(os.path.join(f, "exits", "exits.parquet")).to_pandas()
    assert list(p.columns) == ex.EXIT_COLUMNS and len(p) == len(read(f, "exits.csv"))


# ---- Part 4: listing kinds ----

def test_listing_kinds(built):
    f, _ = built
    s = read(f, "status.csv").set_index("security_id")
    kind = {sym: s.at[sid(f, sym), "listing_kind"] for sym in
            ("ACTIVE1", "LATE1", "SMEUP", "BATCH1", "BATCH2", "BATCH3", "IPO1", "ABC-RE")}
    assert kind == {"ACTIVE1": "IN_DATA_FROM_START", "LATE1": "IN_DATA_FROM_START", "SMEUP": "SME_MIGRATION",
                    "BATCH1": "BATCH_ADMISSION", "BATCH2": "BATCH_ADMISSION", "BATCH3": "BATCH_ADMISSION",
                    "IPO1": "NEW_LISTING", "ABC-RE": "NEW_LISTING"}
    assert s.at[sid(f, "LATE1"), "listing_note"] == "first seen on trading day 2 of the data"
    assert f"in SM {DAYS[0]} to {DAYS[79]}" in s.at[sid(f, "SMEUP"), "listing_note"]
    li = read(f, "listings.csv")
    assert li.values.tolist() == [["BATCH_ADMISSION", str(DAYS[100]), "3", "BATCH1 BATCH2 BATCH3"],
                                  ["SME_MIGRATION", str(DAYS[90]), "1", "SMEUP"]]
    rep = built[1]["report"]
    assert f"  {DAYS[100]}: 3" in rep and f"day  2 {DAYS[1]}: 1 / 1" in rep


def test_start_window_and_batch_min(tmp_path):
    f = prepare(tmp_path)
    run(f, start_days=1, batch_min=4)
    s = read(f, "status.csv").set_index("security_id")
    assert s.at[sid(f, "LATE1"), "listing_kind"] == "NEW_LISTING"
    assert s.at[sid(f, "BATCH1"), "listing_kind"] == "NEW_LISTING"
    assert read(f, "listings.csv")["kind"].tolist() == ["SME_MIGRATION"]


# ---- loaders ----

def test_loaders(built):
    f, _ = built
    old = sid(f, "OLDCO")
    assert ex.exit_info(old, f)["reason"] == "MERGER"
    assert ex.exit_info(sid(f, "ACTIVE1"), f) is None
    between = ex.exits_between(DAYS[45], DAYS[55], folder=f)
    assert set(between["last_symbol"]) == {"OLDCO", "NORATIO", "SMEMV"}
    assert "ETFX" not in set(ex.exits_between(DAYS[0], DAYS[-1], folder=f)["last_symbol"])
    assert "ETFX" in set(ex.exits_between(DAYS[0], DAYS[-1], kinds=None, folder=f)["last_symbol"])
    info = ex.listing_info(sid(f, "SMEUP"), f)
    assert info["listing_kind"] == "SME_MIGRATION" and info["first_date"] == DAYS[90]


def test_status_on_point_in_time(built):
    f, _ = built
    old, gap = sid(f, "OLDCO"), sid(f, "GAPPER")
    assert ex.status_on(old, DAYS[0] - timedelta(days=3), f) is None
    assert ex.status_on(old, DAYS[10], f) == "ACTIVE"
    assert ex.status_on(old, DAYS[49], f) == "ACTIVE"
    assert ex.status_on(old, DAYS[49 + 19], f) == "ACTIVE"
    assert ex.status_on(old, DAYS[49 + 20], f) == "DORMANT"
    assert ex.status_on(old, DAYS[49 + 61], f) == "EXITED"
    assert ex.status_on(gap, DAYS[45], f) == "ACTIVE"           # 5 days into its gap
    assert ex.status_on(gap, DAYS[70], f) == "DORMANT"          # 30 days since its last trade before the gap
    assert ex.status_on(gap, DAYS[N - 1], f) == "ACTIVE"
    with pytest.raises(ValueError):
        ex.status_on(old, DAYS[-1] + timedelta(days=3), f)
    with pytest.raises(KeyError):
        ex.status_on("S999999", DAYS[10], f)


def test_loaders_not_built(tmp_path):
    with pytest.raises(ex.NotBuilt):
        ex.exit_info("S000001", str(tmp_path))


# ---- known cases ----

def test_known_checks(built, monkeypatch):
    f, _ = built
    monkeypatch.setattr(ex, "KNOWN_MERGERS", [("OLDCO", str(DAYS[49]), ("BUYERNEW",), "3 for 2", "CONVERT")])
    monkeypatch.setattr(ex, "KNOWN_SME", ["SMEUP"])
    monkeypatch.setattr(ex, "KNOWN_BATCH_DAYS", {str(DAYS[100]): 3})
    monkeypatch.setattr(ex, "KNOWN_ACTIVE", ["ACTIVE1"])
    monkeypatch.setattr(ex, "KNOWN_RELISTED", ["RELI"])
    lines, ok = ex.check(f)
    assert ok, "\n".join(lines)
    assert any("example VOLDEL" in x for x in lines) and any("example COMPDEL" in x for x in lines)
    monkeypatch.setattr(ex, "KNOWN_MERGERS", [("OLDCO", None, ("SOMEONE",), None, None)])
    lines, ok = ex.check(f)
    assert not ok and any("[NOT FOUND] OLDCO" in x for x in lines)


def test_report(built):
    rep = built[1]["report"]
    assert "voluntary_delisting.csv (4 rows): list, symbol, isin, company" in rep
    assert "a suggestion only: the backtest decides the value" in rep
    assert "Review rows:" in rep and "Known cases: SOME NOT FOUND" in rep


# ---- inputs, locks, freshness ----

def test_inputs_unchanged(tmp_path):
    f = prepare(tmp_path)
    before = {p: tree(os.path.join(f, p)) for p in ("NSE_CM_bhav", "security_master", "adjustments")}
    run(f)
    assert {p: tree(os.path.join(f, p)) for p in before} == before


@pytest.mark.parametrize("which", ["NSE_CM_bhav", "security_master", "adjustments"])
def test_input_locks(tmp_path, which):
    f = prepare(tmp_path)
    lock = RunLock(os.path.join(f, which, ".lock")).acquire("test")
    try:
        with pytest.raises(ex.StoreBusy):
            run(f)
        assert cli.main(["build", "--folder", f]) == 2
    finally:
        lock.release()
    assert not os.path.exists(os.path.join(f, "exits", "exits.csv"))


def test_own_lock(tmp_path):
    f = prepare(tmp_path)
    lock = RunLock(os.path.join(f, "exits", ".lock")).acquire("test")
    try:
        with pytest.raises(ex.LockBusy):
            run(f)
        assert cli.main(["build", "--folder", f]) == 2
    finally:
        lock.release()


def test_stale_inputs(tmp_path):
    f = prepare(tmp_path)
    year = os.path.join(f, "NSE_CM_bhav", "2021.parquet")
    later = time.time() + 60
    os.utime(year, (later, later))
    with pytest.raises(ex.InputStale, match="security_master"):
        run(f)
    assert cli.main(["build", "--folder", f]) == 1
    sm.build(f, log=lambda *_: None)
    os.utime(year, (time.time() - 120, time.time() - 120))
    with pytest.raises(ex.InputStale, match="adjustments"):
        run(f)


def test_not_built_inputs(tmp_path):
    f = str(tmp_path)
    write_store(f, rows_all())
    with pytest.raises(ex.NotBuilt, match="security_master.py"):
        run(f)
    sm.build(f, log=lambda *_: None)
    with pytest.raises(ex.NotBuilt, match="adjustments.py"):
        run(f)


def test_reference_errors(tmp_path):
    f = prepare(tmp_path)
    os.remove(os.path.join(f, "NSE_CM_bhav", "reference", "mergers.csv"))
    with pytest.raises(ex.ReferenceListError, match="nse_reference.py"):
        run(f)
    write_refs(f)
    path = os.path.join(f, "NSE_CM_bhav", "reference", "suspensions.csv")
    pd.read_csv(path, dtype=str).drop(columns="effective_date").to_csv(path, index=False)
    with pytest.raises(ex.ReferenceListError, match="effective_date"):
        run(f)
    assert cli.main(["build", "--folder", f]) == 1
    write_refs(f, skip=("manual_mergers.csv",))
    os.remove(os.path.join(f, "NSE_CM_bhav", "reference", "manual_mergers.csv"))
    run(f)                                                       # manual_mergers.csv is optional
    assert exit_of(f, "OLDCO").reason == "MERGER"


# ---- CLI ----

def test_cli(built, capsys, monkeypatch):
    f, _ = built
    assert cli.main(["show", "OLDCO", "--folder", f]) == 0
    out = capsys.readouterr().out
    assert "EXITED" in out and "MERGER" in out and "CONVERT" in out
    assert cli.main(["show", "NOSUCH", "--folder", f]) == 1
    assert cli.main(["check", "--folder", f]) == 1                # the real known cases are not in this data
    monkeypatch.setattr(ex, "KNOWN_MERGERS", [])
    monkeypatch.setattr(ex, "KNOWN_SME", [])
    monkeypatch.setattr(ex, "KNOWN_BATCH_DAYS", {})
    monkeypatch.setattr(ex, "KNOWN_ACTIVE", [])
    monkeypatch.setattr(ex, "KNOWN_RELISTED", [])
    assert cli.main(["check", "--folder", f]) == 0


def test_cli_build_and_arguments(tmp_path, capsys):
    f = prepare(tmp_path)
    assert cli.main(["check", "--folder", f]) == 1               # not built yet
    assert cli.main(["build", "--folder", f, "--batch-min", "3", "--start-days", "2"]) == 0
    assert "Exit list and listing types build" in capsys.readouterr().out
    for bad in (["build", "--folder", "relative"], ["build", "--active-days", "0"],
                ["build", "--active-days", "30", "--exit-days", "20"], ["nope"]):
        with pytest.raises(SystemExit) as e:
            cli.main(bad)
        assert e.value.code == 2

"""backfill/equity/adjustments.py and adjustments.py on small synthetic bhavcopy parquet files in tmp_path, with the
security master built from them. No network; .env is never read (config.load_env stubbed); NSE_CM_bhav/ and
security_master/ are only read."""
import hashlib
import os
import time
from datetime import date, timedelta

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import adjustments as cli
import config
from backfill.equity import adjustments as adj
from backfill.equity import security_master as sm
from backfill.locks import LockBusy, RunLock

COLUMNS = ["date", "symbol", "series", "isin", "open", "high", "low", "close", "last", "prev_close", "volume",
           "turnover", "trades", "fin_id", "format"]


def weekdays(first, n):
    out, d = [], first
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


DAYS = weekdays(date(2021, 1, 4), 60)
CA_HEADER = ["SYMBOL", "COMPANY NAME", "SERIES", "PURPOSE", "FACE VALUE", "EX-DATE ", "RECORD DATE",
             "BOOK CLOSURE START DATE", "BOOK CLOSURE END DATE"]


@pytest.fixture(autouse=True)
def no_env(monkeypatch):
    monkeypatch.setattr(config, "load_env", lambda: {})


def r(a, b):
    return list(range(a, b))


def trade(symbol, isin, idx, close=100.0, prev=None, opn=None, series="EQ", volume=10):
    """Rows for the given day indexes at a flat close; the first row's prev_close / open can be set (else the
    close), as NSE leaves prev_close at the old, unadjusted close on an ex-date."""
    rows = []
    for j, i in enumerate(idx):
        p = prev if (j == 0 and prev is not None) else close
        o = opn if (j == 0 and opn is not None) else close
        rows.append({"date": DAYS[i], "symbol": symbol, "series": series, "isin": isin, "open": o, "high": max(o, close),
                     "low": min(o, close), "close": close, "last": close, "prev_close": p, "volume": volume,
                     "turnover": 1000.0, "trades": 1, "fin_id": "1", "format": "udiff"})
    return rows


def split(symbol, isin, ex, before, after, start=0, end=40, series_after="EQ", new_isin=None):
    """A flat price before ex, the move on ex (prev_close still the old close), flat after."""
    return trade(symbol, isin, r(start, ex), close=before) + trade(symbol, new_isin or isin, r(ex, end), close=after,
                                                                    prev=before, opn=after, series=series_after)


def write_store(folder, rows):
    store = os.path.join(folder, "NSE_CM_bhav")
    os.makedirs(store, exist_ok=True)
    fill = trade("FILLER", "INE999Z01011", r(0, len(DAYS)), series="N1")
    df = pd.DataFrame(rows + fill, columns=COLUMNS)
    for year, part in df.groupby(df["date"].map(lambda d: d.year)):
        path = os.path.join(store, f"{year}.parquet")
        pq.write_table(pa.Table.from_pandas(part.reset_index(drop=True), preserve_index=False), path)
        old = time.time() - 60
        os.utime(path, (old, old))
    return store


def write_ca(folder, rows, name="CF-CA-equities.csv"):
    """NSE corporate-actions CSV rows: (symbol, purpose, ex-date)."""
    path = os.path.join(folder, "adjustments", "input")
    os.makedirs(path, exist_ok=True)
    df = pd.DataFrame([[s, f"{s} LTD", "EQ", p, "10", d if isinstance(d, str) else f"{d:%d-%b-%Y}", "-", "-", "-"]
                       for s, p, d in rows], columns=CA_HEADER)
    df.to_csv(os.path.join(path, name), index=False)


def build(tmp_path, rows, ca=None, manual=None, **kw):
    f = str(tmp_path)
    write_store(f, rows)
    sm.build(f, log=lambda *_: None)
    if ca is not None:
        write_ca(f, ca)
    if manual is not None:
        write_manual(f, manual)
    return adj.build(f, log=lambda *_: None, **kw)


def write_manual(folder, rows):
    os.makedirs(os.path.join(folder, "adjustments"), exist_ok=True)
    text = "security_id,ex_date,action,factor,note\n# comment line\n" + "".join(",".join(map(str, x)) + "\n" for x in rows)
    with open(os.path.join(folder, "adjustments", "manual_factors.csv"), "w", encoding="utf-8") as fh:
        fh.write(text)


def read(tmp_path, name):
    return pd.read_csv(os.path.join(str(tmp_path), "adjustments", name), dtype=str, keep_default_na=False)


def events(tmp_path, symbol=None):
    ev = read(tmp_path, "events.csv")
    return ev if symbol is None else ev[ev["symbol"] == symbol].reset_index(drop=True)


def factors(tmp_path, sid="S000001"):
    f = read(tmp_path, "factors.csv")
    f = f[f["security_id"] == sid]
    return [(a, b, float(c)) for a, b, c in zip(f["from_date"], f["to_date"], f["factor"])]


def sid_of(tmp_path, symbol):
    sec = pd.read_csv(os.path.join(str(tmp_path), "security_master", "securities.csv"), dtype=str)
    return sec.loc[sec["first_symbol"] == symbol, "security_id"].iloc[0]


SPLIT_10 = "Face Value Split (Sub-Division) - From Rs 10/- Per Share To Rs 1/- Per Share"


def test_split_before_isin_change_is_found_on_the_ex_date(tmp_path):
    # TATASTEEL-like: the old ISIN already trades at the split price on the ex-date, the new ISIN starts the next day
    rows = (trade("SPL", "INE000001011", r(0, 20), close=1000.0)
            + trade("SPL", "INE000001011", [20], close=100.0, prev=1000.0, opn=98.0)
            + trade("SPL", "INE000001029", r(21, 40), close=100.0))
    build(tmp_path, rows)                                  # without the CSV: ratio_open of the ISIN change is ~1
    e = events(tmp_path, "SPL")
    assert len(e) == 1 and (e.ex_date[0], e.applied[0], e.snapped_to[0], e.confirmed_by[0]) == \
        (str(DAYS[20]), "N", "10.0", "")
    assert e.near_isin_change[0] == str(DAYS[21]) and e.isin_change_distance[0] == "1"
    rv = read(tmp_path, "review.csv")
    assert list(rv["kind"]) == ["price_only"] and "suggested 10" in rv.details[0]
    assert factors(tmp_path) == [(str(DAYS[0]), str(DAYS[39]), 1.0)]
    write_ca(str(tmp_path), [("SPL", SPLIT_10, DAYS[20])])
    adj.build(str(tmp_path), log=lambda *_: None)
    e = events(tmp_path, "SPL")
    assert (e.ex_date[0], e.applied[0], e.factor[0], e.confirmed_by[0], e.kind[0], e.source[0]) == \
        (str(DAYS[20]), "Y", "10.0", "corp_action", "SPLIT_OR_BONUS", "detected")
    assert factors(tmp_path) == [(str(DAYS[0]), str(DAYS[19]), 10.0), (str(DAYS[20]), str(DAYS[39]), 1.0)]
    assert read(tmp_path, "review.csv").empty


def test_isin_change_with_matching_ratio_open_confirms(tmp_path):
    build(tmp_path, split("KOT", "INE000002011", 20, 500.0, 100.0, new_isin="INE000002029"))
    e = events(tmp_path, "KOT")
    assert (e.ex_date[0], e.applied[0], e.factor[0], e.confirmed_by[0], e.ratio_label[0]) == \
        (str(DAYS[20]), "Y", "5.0", "isin_change", "split 1:5 | bonus 4:1")
    assert e.isin_change_distance[0] == "0"
    assert factors(tmp_path)[0] == (str(DAYS[0]), str(DAYS[19]), 5.0)


def test_bonus_without_isin_change(tmp_path):
    build(tmp_path, split("BON", "INE000003011", 20, 1000.0, 500.0), ca=[("BON", "Bonus 1:1", DAYS[20])])
    e = events(tmp_path, "BON")
    assert (e.applied[0], e.factor[0], e.confirmed_by[0], e.near_isin_change[0]) == ("Y", "2.0", "corp_action", "")
    lk = read(tmp_path, "links.csv")
    assert list(lk["kind"]) == ["event_no_isin_change"]


def test_split_and_bonus_on_one_day(tmp_path):
    ca = [("BAJ", "Bonus 4:1", DAYS[20]), ("BAJ", "Face Value Split (Sub-Division) - From Rs 2/- Per Share To Rs 1/- "
                                           "Per Share", DAYS[20]), ("BAJ", "Dividend - Rs 5 Per Share", DAYS[20])]
    build(tmp_path, split("BAJ", "INE000004011", 20, 9500.0, 976.0), ca=ca)
    e = events(tmp_path, "BAJ")
    assert (e.applied[0], e.factor[0], e.ratio_label[0]) == ("Y", "10.0", adj.label_of(10))
    assert "split 1:2 + bonus 4:1" in e.ratio_label[0] and "Bonus 4:1" in e.ca_purpose[0] and "Dividend" not in e.ca_purpose[0]


def test_consolidation(tmp_path):
    build(tmp_path, split("CON", "INE000005011", 20, 10.0, 100.0),
          ca=[("CON", "Consolidation Of Shares From Rs 1/- Per Share To Rs 10/- Per Share", DAYS[20])])
    e = events(tmp_path, "CON")
    assert (e.kind[0], e.applied[0], float(e.factor[0])) == ("CONSOLIDATION", "Y", 0.1)
    assert factors(tmp_path)[0][2] == 0.1


def test_small_bonus_found_below_the_price_threshold(tmp_path):
    build(tmp_path, split("SMB", "INE000006011", 20, 110.0, 100.0), ca=[("SMB", "BONUS 1:10", f"{DAYS[19]:%d-%b-%Y}")])
    e = events(tmp_path, "SMB")
    assert (e.ex_date[0], e.source[0], e.applied[0], float(e.factor[0]), e.ca_ex_date[0]) == \
        (str(DAYS[20]), "corp_action", "Y", 1.1, str(DAYS[19]))
    assert f"corporate action ex-date {DAYS[19]}" in e.note[0]


def test_price_only_and_other_not_applied(tmp_path):
    build(tmp_path, split("PO", "INE000007011", 20, 1000.0, 500.0) + split("OTH", "INE000008011", 20, 1000.0, 1600.0))
    ev = events(tmp_path)
    assert list(ev["applied"]) == ["N", "N"] and sorted(ev["kind"]) == ["OTHER", "SPLIT_OR_BONUS"]
    rv = read(tmp_path, "review.csv")
    assert list(rv["kind"]) == ["price_only", "price_only"]
    assert any("not a known ratio (OTHER)" in d for d in rv["details"])
    assert all(f[2] == 1.0 for f in factors(tmp_path)) and len(read(tmp_path, "factors.csv")) == 2


def test_corp_action_without_price_move_is_suspect(tmp_path):
    build(tmp_path, trade("FLAT", "INE000009011", r(0, 40), close=1000.0), ca=[("FLAT", "Bonus 1:1", DAYS[20])])
    assert events(tmp_path).empty
    rv = read(tmp_path, "review.csv")
    assert list(rv["kind"]) == ["ca_no_move"] and "no price move of 2" in rv.details[0]
    assert factors(tmp_path) == [(str(DAYS[0]), str(DAYS[39]), 1.0)]


def test_penny_stock_below_min_price_ignored(tmp_path):
    build(tmp_path, split("PEN", "INE000010011", 20, 0.2, 0.1))
    assert events(tmp_path).empty and read(tmp_path, "review.csv").empty
    build(tmp_path, split("PEN", "INE000010011", 20, 0.2, 0.1), min_price=0.1)
    assert len(events(tmp_path)) == 1


def test_missing_session_day_listed_not_reviewed(tmp_path):
    rows = []
    for k in range(12):
        isin = f"INE1{k:02d}A01011"
        rows += trade(f"M{k}", isin, r(0, 30), close=100.0) + trade(f"M{k}", isin, r(30, 40), close=101.0, prev=102.0)
    res = build(tmp_path, rows)
    assert events(tmp_path).empty and read(tmp_path, "review.csv").empty
    rep = res["report"]
    assert f"{DAYS[30]} (previous day in the store {DAYS[29]}): likely session" in rep
    assert "12 of 12 stocks" in rep
    assert "outside 0.01%: 12" in rep


def test_missing_session_names_the_weekend_between():
    days = [date(2024, 1, 19), date(2024, 1, 23)]
    rows = []
    for k in range(10):
        rows.append({"security_id": f"S{k}", "date": days[1], "ratio_prev_close": 1.01})
    x = pd.DataFrame(rows)
    m = adj.measure(x, days)
    s = m["sessions"][0]
    assert s["likely"] == [date(2024, 1, 20), date(2024, 1, 21)] and s["mismatch"] == 10


def test_gap_uses_previous_trading_day_of_the_security(tmp_path):
    rows = trade("GAP", "INE000011011", r(0, 10), close=1000.0) + trade("GAP", "INE000011011", r(15, 40), close=200.0,
                                                                         prev=1000.0, opn=200.0)
    build(tmp_path, rows, ca=[("GAP", "Bonus 4:1", DAYS[15])])
    e = events(tmp_path, "GAP")
    assert (e.ex_date[0], e.prev_day[0], e.gap[0], e.factor[0]) == (str(DAYS[15]), str(DAYS[9]), "6", "5.0")
    assert factors(tmp_path) == [(str(DAYS[0]), str(DAYS[9]), 5.0), (str(DAYS[15]), str(DAYS[39]), 1.0)]


def test_event_across_eq_to_be(tmp_path):
    build(tmp_path, split("SER", "INE000012011", 20, 600.0, 300.0, series_after="BE"),
          ca=[("SER", "Bonus 1:1", DAYS[20])])
    e = events(tmp_path, "SER")
    assert (e.series[0], e.applied[0], e.factor[0]) == ("BE", "Y", "2.0")


def test_manual_add_remove_set_and_errors(tmp_path):
    rows = (split("AD", "INE000013011", 20, 100.0, 100.0) + split("RM", "INE000014011", 20, 1000.0, 200.0,
                                                                  new_isin="INE000014029")
            + split("ST", "INE000015011", 20, 1000.0, 370.0))
    write_store(str(tmp_path), rows)
    sm.build(str(tmp_path), log=lambda *_: None)
    ids = {s: sid_of(tmp_path, s) for s in ("AD", "RM", "ST")}
    manual = [(ids["AD"], DAYS[10], "ADD", 3, "bonus 2:1 from the circular"), (ids["RM"], DAYS[20], "REMOVE", "", ""),
              (ids["ST"], DAYS[20], "set", 2.7, ""), (ids["ST"], DAYS[5], "SET", 2, "no event there"),
              ("S999999", DAYS[5], "ADD", 2, ""), (ids["AD"], "not-a-date", "ADD", 2, ""),
              (ids["AD"], DAYS[11], "ADD", 0, ""), (ids["ST"], DAYS[20], "ADD", 2, ""),
              (ids["AD"], DAYS[12], "MOVE", 2, "")]
    write_manual(str(tmp_path), manual)
    res = adj.build(str(tmp_path), log=lambda *_: None)
    ev = events(tmp_path).set_index("symbol")
    assert (ev.loc["AD", "ex_date"], ev.loc["AD", "applied"], ev.loc["AD", "source"], ev.loc["AD", "factor"]) == \
        (str(DAYS[10]), "Y", "manual", "3.0")
    assert ev.loc["RM", "applied"] == "N" and "removed by manual REMOVE" in ev.loc["RM", "note"]
    assert (ev.loc["ST", "applied"], ev.loc["ST", "factor"], ev.loc["ST", "confirmed_by"]) == ("Y", "2.7", "manual")
    rv = read(tmp_path, "review.csv")
    errors = rv[rv["kind"] == "manual_error"]
    assert len(errors) == 6 and res["manual_errors"] == 6
    text = " ".join(errors["details"])
    for part in ("no event of", "unknown security_id S999999", "needs a positive factor", "use SET",
                 "unknown action 'MOVE'"):
        assert part in text
    assert any("bad ex_date" in d for d in errors["details"])
    assert rv[rv["kind"] == "price_only"].empty                     # RM removed, ST set
    assert factors(tmp_path, ids["AD"]) == [(str(DAYS[0]), str(DAYS[9]), 3.0), (str(DAYS[10]), str(DAYS[39]), 1.0)]
    lines, ok = adj.check(str(tmp_path))
    assert not ok and "Manual errors: 6" in lines


def test_manual_file_created_once_never_overwritten(tmp_path):
    build(tmp_path, trade("AAA", "INE000016011", r(0, 10)))
    path = os.path.join(str(tmp_path), "adjustments", "manual_factors.csv")
    assert open(path, encoding="utf-8").read() == adj.MANUAL_TEMPLATE
    with open(path, "a", encoding="utf-8") as fh:
        fh.write("# mine\n")
    adj.build(str(tmp_path), log=lambda *_: None)
    assert open(path, encoding="utf-8").read().endswith("# mine\n")


def two_events(tmp_path):
    rows = (trade("TWO", "INE000017011", r(0, 10), close=1000.0, volume=10)
            + trade("TWO", "INE000017011", r(10, 25), close=500.0, prev=1000.0, opn=500.0, volume=20)
            + trade("TWO", "INE000017011", r(25, 40), close=100.0, prev=500.0, opn=100.0, volume=100))
    build(tmp_path, rows, ca=[("TWO", "Bonus 1:1", DAYS[10]), ("TWO", "Face Value Split From Rs 5 To Rs 1", DAYS[25])])


def test_cumulative_factors_over_two_events(tmp_path):
    two_events(tmp_path)
    assert factors(tmp_path) == [(str(DAYS[0]), str(DAYS[9]), 10.0), (str(DAYS[10]), str(DAYS[24]), 5.0),
                                 (str(DAYS[25]), str(DAYS[39]), 1.0)]
    f = str(tmp_path)
    assert adj.factor_on("S000001", DAYS[3], f) == 10.0
    assert adj.factor_on("S000001", str(DAYS[10]), f) == 5.0
    assert adj.factor_on("S000001", DAYS[30], f) == 1.0
    assert adj.factor_on("S000001", date(2020, 1, 1), f) == 10.0 and adj.factor_on("S000001", date(2030, 1, 1), f) == 1.0
    with pytest.raises(KeyError):
        adj.factor_on("S999999", DAYS[3], f)


def test_adjusted_history_is_continuous(tmp_path):
    two_events(tmp_path)
    h = adj.adjusted_history("S000001", folder=str(tmp_path))
    assert len(h) == 40 and set(h["adj_close"]) == {100.0} and set(h["adj_open"]) == {100.0}
    assert set(h["adj_prev_close"]) == {100.0}                      # prev_close is unadjusted on the ex-dates
    assert set(h["adj_volume"]) == {100.0}
    assert list(h["close"].iloc[[0, 10, 25]]) == [1000.0, 500.0, 100.0]          # raw kept
    part = adj.adjusted_history("S000001", DAYS[24], DAYS[26], str(tmp_path))
    assert list(part["date"]) == DAYS[24:27] and set(part["adj_prev_close"]) == {100.0}
    with pytest.raises(KeyError):
        adj.adjusted_history("S999999", folder=str(tmp_path))


def test_parse_purpose():
    p = adj.parse_purpose
    assert p("Bonus 1:1") == ([("bonus 1:1", 2.0)], False)
    assert p("BONUS 2:5") == ([("bonus 2:5", 1.4)], False)
    assert p(SPLIT_10) == ([("split Rs 10 -> Rs 1", 10.0)], False)
    assert p("Face Value Split (Sub-Division) - From Re 1/- Per Share To Re 0.50/- Per Share")[0][0][1] == 2.0
    assert p("SUBDIVISION FROM RS.10 TO RS.2")[0] == [("split Rs 10 -> Rs 2", 5.0)]
    assert p("Annual General Meeting / Dividend - Rs 8 Per Share / Bonus 1:2")[0] == [("bonus 1:2", 1.5)]
    assert p("Dividend - Rs 10 Per Share") == ([], False)
    assert p("Interim Dividend - Rs 2.50 Per Share / Special Dividend") == ([], False)
    assert p("Bonus Issue") == ([], True)
    assert p("Consolidation of shares from Rs 2 to Rs 10")[0] == [("consolidation Rs 2 -> Rs 10", 0.2)]
    assert adj.parse_date("28-Oct-2024") == date(2024, 10, 28) == adj.parse_date("28-OCT-2024")
    assert adj.parse_date("2024-10-28") == date(2024, 10, 28) and adj.parse_date("-") is None
    with pytest.raises(ValueError):
        adj.parse_date("someday")


def test_corp_action_input_errors_and_empty_folder(tmp_path):
    f = str(tmp_path)
    assert adj.read_corp_actions(f).empty                                    # no folder
    os.makedirs(os.path.join(f, "adjustments", "input"))
    assert adj.read_corp_actions(f).empty                                    # empty folder
    write_ca(f, [("A", "Bonus 1:1", DAYS[3]), ("A", "Bonus 1:1", DAYS[3])], name="one.csv")
    write_ca(f, [("A", "BONUS 1:1", DAYS[3]), ("B", "Bonus 1:1", "bad date")], name="two.csv")
    ca = adj.read_corp_actions(f)
    assert list(ca["symbol"]) == ["A", "B"]                                  # duplicates across files dropped
    pd.DataFrame({"SYMBOL": ["A"], "PURPOSE": ["Bonus 1:1"]}).to_csv(os.path.join(f, "adjustments", "input",
                                                                                  "bad.csv"), index=False)
    with pytest.raises(adj.CorpActionError, match="missing column"):
        adj.read_corp_actions(f)


def test_corp_action_bad_rows_go_to_review(tmp_path):
    build(tmp_path, trade("AAA", "INE000018011", r(0, 10)),
          ca=[("AAA", "Bonus 1:1", "bad date"), ("AAA", "Bonus issue", DAYS[3]), ("ZZZ", "Bonus 1:1", DAYS[3]),
              ("AAA", "Dividend - Rs 2", DAYS[3])])
    rv = read(tmp_path, "review.csv")
    assert list(rv["kind"]) == ["ca_unparsed", "ca_unparsed", "ca_unknown"]          # the dividend is ignored
    assert "ex-date 'bad date' not read" in rv.details[0] and "ZZZ" == rv.symbol[2]


def test_locks_and_stale_master(tmp_path):
    f = str(tmp_path)
    store = write_store(f, trade("AAA", "INE000019011", r(0, 10)))
    with pytest.raises(adj.NotBuilt):
        adj.build(f, log=lambda *_: None)
    sm.build(f, log=lambda *_: None)
    for path in (os.path.join(store, ".lock"), os.path.join(f, "security_master", ".lock")):
        lock = RunLock(path).acquire("other")
        try:
            with pytest.raises(adj.StoreBusy):
                adj.build(f, log=lambda *_: None)
            assert cli.main(["build", "--folder", f]) == 2
        finally:
            lock.release()
    lock = RunLock(os.path.join(f, "adjustments", ".lock")).acquire("other build")
    try:
        with pytest.raises(LockBusy):
            adj.build(f, log=lambda *_: None)
    finally:
        lock.release()
    adj.build(f, log=lambda *_: None)
    os.utime(os.path.join(store, "2021.parquet"))                            # a newer bhavcopy year file
    with pytest.raises(adj.MasterStale, match="Rebuild it first"):
        adj.build(f, log=lambda *_: None)
    assert cli.main(["build", "--folder", f]) == 1


def digest(folder):
    out = {}
    for sub in ("NSE_CM_bhav", "security_master"):
        for name in sorted(os.listdir(os.path.join(folder, sub))):
            if name != ".lock":
                with open(os.path.join(folder, sub, name), "rb") as fh:
                    out[(sub, name)] = hashlib.sha256(fh.read()).hexdigest()
    return out


def test_inputs_are_never_written(tmp_path):
    f = str(tmp_path)
    write_store(f, split("BON", "INE000003011", 20, 1000.0, 500.0))
    sm.build(f, log=lambda *_: None)
    write_ca(f, [("BON", "Bonus 1:1", DAYS[20])])
    before = digest(f)
    adj.build(f, log=lambda *_: None)
    assert digest(f) == before
    assert sorted(os.listdir(os.path.join(f, "adjustments"))) == [
        ".lock", "build_info.json", "build_report.txt", "events.csv", "factors.csv", "factors.parquet", "input",
        "links.csv", "manual_factors.csv", "review.csv"]


def test_known_cases_and_cli(tmp_path, monkeypatch, capsys):
    f = str(tmp_path)
    assert cli.main(["check", "--folder", f]) == 1                           # not built
    for argv in ([], ["nope"], ["build", "--tol", "0"], ["build", "--snap-tol", "0.6"], ["build", "--kinds", "X"],
                 ["show"], ["build", "--folder", "relative/path"]):
        with pytest.raises(SystemExit) as e:
            cli.main(argv)
        assert e.value.code == 2
    rows = split("KNW", "INE000020011", 20, 1000.0, 500.0, new_isin="INE000020029")
    write_store(f, rows)
    sm.build(f, log=lambda *_: None)
    monkeypatch.setattr(cli, "equity_data_dir", lambda: f)
    capsys.readouterr()
    assert cli.main(["build"]) == 0                                          # known cases missing: still 0
    out = capsys.readouterr().out
    assert "[NOT FOUND] TATASTEEL 2022-07 expected 10: no event in the window" in out
    assert cli.main(["check"]) == 1
    monkeypatch.setattr(adj, "KNOWN_ADJUSTMENTS", [("KNW", str(DAYS[0]), str(DAYS[-1]), 2)])
    lines, ok = adj.check(f)
    assert ok and any("[found] KNW" in x for x in lines)
    assert any(f"close 1000 on {DAYS[19]} / 2 = 500.00 vs ex-date open 500, close 500" in x for x in lines)
    assert cli.main(["check"]) == 0
    capsys.readouterr()
    assert cli.main(["show", "KNW"]) == 0
    out = capsys.readouterr().out
    assert "split 1:2 | bonus 1:1" in out and f"Around {DAYS[20]} (applied Y)" in out
    assert cli.main(["show", "NOPE"]) == 1

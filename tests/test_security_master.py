"""backfill/equity/security_master.py and security_master.py on small synthetic bhavcopy parquet files in tmp_path.
No network; .env is never read (config.load_env stubbed); NSE_CM_bhav/ is only read."""
import os
from datetime import date, timedelta

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import config
import security_master as cli
from backfill import locks
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
LATER = weekdays(date(2022, 1, 3), 10)


@pytest.fixture(autouse=True)
def no_env(monkeypatch):
    monkeypatch.setattr(config, "load_env", lambda: {})


def trade(symbol, isin, idx, series="EQ", close=100.0, prev=None, opn=None, days=DAYS):
    """Rows for the given day indexes; the first row's prev_close/open can be set (else the close)."""
    rows = []
    for j, i in enumerate(idx):
        p = prev if (j == 0 and prev is not None) else close
        o = opn if (j == 0 and opn is not None) else close
        rows.append({"date": days[i], "symbol": symbol, "series": series, "isin": isin, "open": o, "high": close,
                     "low": close, "close": close, "last": close, "prev_close": p, "volume": 10, "turnover": 1000.0,
                     "trades": 1, "fin_id": "1", "format": "udiff"})
    return rows


def write_store(folder, rows):
    """Yearly files of rows plus one out-of-scope N1 row on every day of DAYS (and of LATER when used), as the real
    bhavcopy has rows on every trading day."""
    store = os.path.join(folder, "NSE_CM_bhav")
    os.makedirs(store, exist_ok=True)
    years = {row["date"].year for row in rows}
    fill = trade("FILLER", "INE999Z01011", r(0, len(DAYS)), series="N1")
    if LATER[0].year in years:
        fill += trade("FILLER", "INE999Z01011", r(0, len(LATER)), series="N1", days=LATER)
    df = pd.DataFrame(rows + fill, columns=COLUMNS)
    for year, part in df.groupby(df["date"].map(lambda d: d.year)):
        pq.write_table(pa.Table.from_pandas(part.reset_index(drop=True), preserve_index=False),
                       os.path.join(store, f"{year}.parquet"))
    return store


def r(a, b):
    return list(range(a, b))


def build(tmp_path, rows, **kw):
    write_store(str(tmp_path), rows)
    return sm.build(str(tmp_path), log=lambda *_: None, **kw)


def read(tmp_path, name):
    return pd.read_csv(os.path.join(str(tmp_path), "security_master", name), dtype=str, keep_default_na=False)


def ids_of(tmp_path):
    sec = read(tmp_path, "securities.csv")
    return dict(zip(sec["first_symbol"], sec["security_id"]))


def events_of(tmp_path, kind=None):
    ev = read(tmp_path, "events.csv")
    return ev if kind is None else ev[ev["type"] == kind].reset_index(drop=True)


def test_rename_keeps_one_security(tmp_path):
    build(tmp_path, trade("OLDN", "INE000001011", r(0, 10)) + trade("NEWN", "INE000001011", r(10, 20)))
    sec = read(tmp_path, "securities.csv")
    assert len(sec) == 1
    row = sec.iloc[0]
    assert (row.first_symbol, row.last_symbol, row.symbols, row.isins, row.trading_days) == \
        ("OLDN", "NEWN", "OLDN|NEWN", "INE000001011", "20")
    ev = events_of(tmp_path, "RENAME")
    assert len(ev) == 1 and (ev.date[0], ev.old_symbol[0], ev.new_symbol[0]) == (str(DAYS[10]), "OLDN", "NEWN")
    hist = read(tmp_path, "symbol_history.csv")
    assert list(hist["symbol"]) == ["OLDN", "NEWN"] and list(hist["trading_days"]) == ["10", "10"]


def test_split_with_isin_change_records_ratios(tmp_path):
    build(tmp_path, trade("SPL", "INE000002011", r(0, 10), close=1000.0)
          + trade("SPL", "INE000002029", r(10, 20), close=201.0, prev=200.0, opn=205.0))
    sec = read(tmp_path, "securities.csv")
    assert len(sec) == 1 and sec.isins[0] == "INE000002011|INE000002029" and sec.last_isin[0] == "INE000002029"
    ev = events_of(tmp_path, "ISIN_CHANGE")
    assert len(ev) == 1
    e = ev.iloc[0]
    assert (e.date, e.old_isin, e.new_isin) == (str(DAYS[10]), "INE000002011", "INE000002029")
    assert float(e.old_close) == 1000 and float(e.new_prev_close) == 200 and float(e.new_open) == 205
    assert float(e.ratio_prev_close) == 5.0 and float(e.ratio_open) == round(1000 / 205, 6)
    assert events_of(tmp_path, "RENAME").empty


def test_bonus_without_change_is_one_period(tmp_path):
    build(tmp_path, trade("BON", "INE000003011", r(0, 10), close=1000.0)
          + trade("BON", "INE000003011", r(10, 20), close=500.0, prev=500.0))
    assert len(read(tmp_path, "securities.csv")) == 1
    assert len(read(tmp_path, "symbol_history.csv")) == 1
    assert list(events_of(tmp_path)["type"]) == ["LISTED"]


def test_series_eq_be_eq(tmp_path):
    build(tmp_path, trade("SER", "INE000004011", r(0, 5)) + trade("SER", "INE000004011", r(5, 10), series="BE")
          + trade("SER", "INE000004011", r(10, 15)))
    sec = read(tmp_path, "securities.csv")
    assert len(sec) == 1 and sec.series_seen[0] == "EQ|BE" and sec.last_series[0] == "EQ"
    ev = events_of(tmp_path, "SERIES_CHANGE")
    assert list(zip(ev.old_series, ev.new_series, ev.date)) == [("EQ", "BE", str(DAYS[5])), ("BE", "EQ", str(DAYS[10]))]
    assert list(read(tmp_path, "symbol_history.csv")["series"]) == ["EQ", "BE", "EQ"]


def test_gap_event_only_after_more_than_20_days(tmp_path):
    build(tmp_path, trade("GAPPY", "INE000005011", r(0, 5) + r(30, 35))
          + trade("SHORTGAP", "INE000005029", r(0, 5) + r(20, 25)))
    ev = events_of(tmp_path, "GAP")
    assert len(ev) == 1
    e = ev.iloc[0]
    assert e.security_id == ids_of(tmp_path)["GAPPY"] and e.gap_days == "25" and e.date == str(DAYS[30])
    assert e.note == f"absent {DAYS[5]} to {DAYS[29]}"
    hist = read(tmp_path, "symbol_history.csv")
    assert len(hist[hist["symbol"] == "GAPPY"]) == 1          # a gap alone does not break a period


def test_listing(tmp_path):
    build(tmp_path, trade("FIRST", "INE000006011", r(0, 60)) + trade("LATE", "INE000006029", r(15, 20)))
    ev = events_of(tmp_path, "LISTED")
    assert list(ev.new_symbol) == ["FIRST", "LATE"]
    assert list(ev.note) == ["in data from first day", ""] and ev.date[1] == str(DAYS[15])
    assert ids_of(tmp_path) == {"FIRST": "S000001", "LATE": "S000002"}
    sec = read(tmp_path, "securities.csv").set_index("first_symbol")
    assert sec.loc["FIRST", "days_since_last"] == "0" and sec.loc["LATE", "days_since_last"] == "40"
    assert sec.loc["LATE", "trading_days"] == "5"


def test_symbol_reuse_after_long_gap_is_new_security_with_review(tmp_path):
    build(tmp_path, trade("REU", "INE000007011", r(0, 5)) + trade("REU", "INE000007029", r(30, 35)))
    sec = read(tmp_path, "securities.csv")
    assert list(sec.isins) == ["INE000007011", "INE000007029"]
    assert events_of(tmp_path, "ISIN_CHANGE").empty
    rv = read(tmp_path, "review.csv")
    assert list(rv.kind) == ["symbol_reused?"] and (rv.security_id_old[0], rv.security_id_new[0]) == ("S000001", "S000002")
    assert "more than 10" in rv.details[0]


def test_symbol_reuse_while_old_isin_still_trades(tmp_path):
    build(tmp_path, trade("DUAL", "INE000008011", r(0, 10)) + trade("DUAL", "INE000008029", r(11, 13))
          + trade("DUALX", "INE000008011", r(15, 20)))
    sec = read(tmp_path, "securities.csv")
    assert len(sec) == 2
    rv = read(tmp_path, "review.csv")
    assert list(rv.kind) == ["symbol_reused?"] and "still trades" in rv.details[0]


def test_link_gap_option(tmp_path):
    rows = trade("LG", "INE000009011", r(0, 5)) + trade("LG", "INE000009029", r(12, 15))   # 8 days apart
    build(tmp_path, rows)
    assert len(read(tmp_path, "securities.csv")) == 1
    sm.build(str(tmp_path), link_gap=5, log=lambda *_: None)
    assert len(read(tmp_path, "securities.csv")) == 2 and list(read(tmp_path, "review.csv").kind) == ["symbol_reused?"]


def test_both_changed_same_day_is_never_linked(tmp_path):
    build(tmp_path, trade("BOTHA", "INE000010011", r(0, 10), close=300.0)
          + trade("BOTHB", "INE000010029", r(10, 20), close=151.0, prev=150.2)
          + trade("ENDS", "INE000010037", r(0, 10), close=77.0)
          + trade("UNRELATED", "INE000010045", r(10, 20), close=999.0))
    sec = read(tmp_path, "securities.csv")
    assert len(sec) == 4
    rv = read(tmp_path, "review.csv")
    assert len(rv) == 1 and rv.kind[0] == "both_changed?"
    ids = ids_of(tmp_path)
    assert (rv.security_id_old[0], rv.security_id_new[0]) == (ids["BOTHA"], ids["BOTHB"])
    assert "(close / 2)" in rv.details[0]
    assert events_of(tmp_path, "ISIN_CHANGE").empty and events_of(tmp_path, "RENAME").empty


def test_conflict_stops_and_writes_nothing(tmp_path):
    rows = trade("C1", "INE000011011", r(0, 3)) + trade("C2", "INE000011011", [1])
    with pytest.raises(sm.ConflictError) as e:
        build(tmp_path, rows)
    assert "ISIN on two symbols: INE000011011 -> C1, C2" in str(e.value)
    out = os.path.join(str(tmp_path), "security_master")
    assert [n for n in os.listdir(out) if n != ".lock"] == []
    rows = trade("C3", "INE000011029", r(0, 3)) + trade("C3", "INE000011037", [2])
    with pytest.raises(sm.ConflictError, match="symbol with two ISINs: C3 -> INE000011029, INE000011037"):
        build(tmp_path, rows)


def test_ids_stable_on_rebuild_and_after_a_later_year(tmp_path):
    rows = (trade("BBB", "INE000012029", r(0, 20)) + trade("AAA", "INE000012037", r(0, 20))
            + trade("CCC", "INE000012011", r(5, 20)))
    build(tmp_path, rows)
    first = read(tmp_path, "securities.csv")
    assert list(first.first_symbol) == ["BBB", "AAA", "CCC"]       # same day: by ISIN; then first appearance
    sm.build(str(tmp_path), log=lambda *_: None)
    assert read(tmp_path, "securities.csv").equals(first)
    later = (trade("AAA", "INE000012037", r(0, 10), days=LATER) + trade("NEWCO", "INE000012045", r(0, 10), days=LATER)
             + trade("CCCNEW", "INE000012011", r(0, 10), days=LATER))
    build(tmp_path, rows + later)
    again = read(tmp_path, "securities.csv").set_index("first_symbol")
    for row in first.itertuples():
        assert again.loc[row.first_symbol, "security_id"] == row.security_id
    assert again.loc["NEWCO", "security_id"] == "S000004" and again.loc["CCC", "last_symbol"] == "CCCNEW"


def test_security_id_for_point_in_time(tmp_path):
    build(tmp_path, trade("OLDN", "INE000013011", r(0, 10)) + trade("NEWN", "INE000013011", r(10, 20))
          + trade("REU", "INE000013029", r(0, 5)) + trade("REU", "INE000013037", r(30, 35)))
    f, ids = str(tmp_path), ids_of(tmp_path)
    sid = ids["OLDN"]
    assert sm.security_id_for("OLDN", DAYS[5], f) == sid
    assert sm.security_id_for("oldn", str(DAYS[9]), f) == sid
    assert sm.security_id_for("OLDN", DAYS[10], f) is None
    assert sm.security_id_for("NEWN", DAYS[10], f) == sid and sm.security_id_for("NEWN", DAYS[59], f) == sid
    assert sm.security_id_for("NEWN", DAYS[9], f) is None
    assert sm.security_id_for("INE000013011", DAYS[15], f) == sid
    assert sm.security_id_for("OLDN", DAYS[0] - timedelta(days=1), f) is None
    reu = read(tmp_path, "securities.csv").query("first_symbol == 'REU'")["security_id"].tolist()
    assert sm.security_id_for("REU", DAYS[2], f) == reu[0]
    assert sm.security_id_for("REU", DAYS[20], f) == reu[0]          # after it stopped, until the reuse
    assert sm.security_id_for("REU", DAYS[31], f) == reu[1]
    assert sm.security_id_for("NOPE", DAYS[5], f) is None


def test_history_across_rename_and_split(tmp_path):
    build(tmp_path, trade("OLDN", "INE000014011", r(0, 10), close=1000.0)
          + trade("NEWN", "INE000014011", r(10, 20), close=1000.0)
          + trade("NEWN", "INE000014029", r(20, 30), close=200.0, prev=200.0)
          + trade("OTHER", "INE000014037", r(0, 30)) + trade("NEWN", "INE000014011", [40], series="N1"))
    sid = ids_of(tmp_path)["OLDN"]
    h = sm.history(sid, folder=str(tmp_path))
    assert len(h) == 30 and list(h.columns[:2]) == ["security_id", "date"] and set(h["security_id"]) == {sid}
    assert list(h["date"]) == DAYS[:30] and set(h["symbol"]) == {"OLDN", "NEWN"}
    assert set(h["isin"]) == {"INE000014011", "INE000014029"} and h["close"].iloc[-1] == 200.0   # raw prices
    assert "turnover" in h.columns
    part = sm.history(sid, start=DAYS[8], end=str(DAYS[11]), folder=str(tmp_path))
    assert list(part["date"]) == DAYS[8:12]
    with pytest.raises(KeyError):
        sm.history("S999999", folder=str(tmp_path))


def test_universe(tmp_path):
    build(tmp_path, trade("AAA", "INE000015011", r(0, 10)) + trade("BBB", "INE000015029", r(5, 10), series="BE")
          + trade("CCC", "INE000015037", r(0, 10), series="N1"))
    f, ids = str(tmp_path), ids_of(tmp_path)
    assert list(sm.universe(DAYS[0], folder=f)["security_id"]) == [ids["AAA"]]
    u = sm.universe(DAYS[6], folder=f)
    assert list(u["symbol"]) == ["AAA", "BBB"] and list(u["security_id"]) == [ids["AAA"], ids["BBB"]]
    assert list(sm.universe(DAYS[6], series=("EQ",), folder=f)["symbol"]) == ["AAA"]
    assert sm.universe(DAYS[6], series=("N1",), folder=f).empty        # N1 is not in the built master
    assert "CCC" not in ids and "FILLER" not in ids


def test_series_option(tmp_path):
    build(tmp_path, trade("AAA", "INE000016011", r(0, 5)) + trade("NNN", "INE000016029", r(0, 5), series="N1"),
          series=("EQ", "N1"))
    assert set(read(tmp_path, "securities.csv")["first_symbol"]) == {"AAA", "NNN", "FILLER"}


def test_build_never_writes_the_bhavcopy_store(tmp_path):
    store = write_store(str(tmp_path), trade("AAA", "INE000017011", r(0, 5)))
    RunLock(os.path.join(store, ".lock")).acquire("cm_bhav.py").release()
    before = {n: open(os.path.join(store, n), "rb").read() for n in os.listdir(store)}
    sm.build(str(tmp_path), log=lambda *_: None)
    assert {n: open(os.path.join(store, n), "rb").read() for n in os.listdir(store)} == before
    out = os.path.join(str(tmp_path), "security_master")
    assert sorted(os.listdir(out)) == [".lock", "build_info.json", "build_report.txt", "events.csv", "review.csv",
                                       "securities.csv", "securities.parquet", "symbol_history.csv",
                                       "symbol_history.parquet"]
    assert b"\r\n" not in open(os.path.join(out, "events.csv"), "rb").read()


def test_locks_held_reads_only(tmp_path):
    path = os.path.join(str(tmp_path), "x", ".lock")
    assert locks.held(path) is False and not os.path.exists(path)
    lock = RunLock(path).acquire("someone")
    text = open(path, "rb").read()
    assert locks.held(path) is True and open(path, "rb").read() == text
    lock.release()
    assert locks.held(path) is False


def test_refuses_while_bhavcopy_lock_held(tmp_path, capsys):
    store = write_store(str(tmp_path), trade("AAA", "INE000018011", r(0, 5)))
    lock = RunLock(os.path.join(store, ".lock")).acquire("cm_bhav.py")
    try:
        text = open(os.path.join(store, ".lock"), "rb").read()
        with pytest.raises(sm.BhavcopyBusy):
            sm.build(str(tmp_path), log=lambda *_: None)
        assert cli.main(["build", "--folder", str(tmp_path)]) == 2
        assert "bhavcopy download" in capsys.readouterr().err
        assert open(os.path.join(store, ".lock"), "rb").read() == text
        assert not os.path.exists(os.path.join(str(tmp_path), "security_master"))
    finally:
        lock.release()


def test_refuses_while_another_build_runs(tmp_path, capsys):
    write_store(str(tmp_path), trade("AAA", "INE000019011", r(0, 5)))
    lock = RunLock(os.path.join(str(tmp_path), "security_master", ".lock")).acquire("other build")
    try:
        with pytest.raises(LockBusy):
            sm.build(str(tmp_path), log=lambda *_: None)
        assert cli.main(["build", "--folder", str(tmp_path)]) == 2
        assert "in progress" in capsys.readouterr().err
    finally:
        lock.release()


KNOWN_ROWS = (
    trade("AMARAJABAT", "INE885A01032", r(0, 10)) + trade("ARE&M", "INE885A01032", r(10, 20))
    + trade("ADANITRANS", "INE931S01010", r(0, 10)) + trade("ADANIENSOL", "INE931S01010", r(10, 20))
    + trade("AKZOINDIA", "INE133A01011", r(0, 10)) + trade("JSWDULUX", "INE133A01011", r(10, 20))
    + [row for k, s in enumerate(sm.KNOWN_ISIN_CHANGES)
       for row in trade(s, f"INE{k:03d}X01011", r(0, 10), close=1000.0)
       + trade(s, f"INE{k:03d}X01029", r(10, 20), close=100.0, prev=100.0)]
    + trade("RELIANCE", "INE002A01018", r(0, 20)) + trade("HDFCBANK", "INE040A01034", r(0, 20))
    + trade("HDFC", "INE001A01036", r(0, 12)) + trade("MINDTREE", "INE018I01017", r(0, 15))
)


def test_known_checks_and_cli(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(sm, "KNOWN_ENDED", {"HDFC": str(DAYS[11] + timedelta(days=3)), "MINDTREE": None})
    f = str(tmp_path)
    assert cli.main(["check", "--folder", f]) == 1                      # not built yet
    assert "python security_master.py build" in capsys.readouterr().err
    write_store(f, KNOWN_ROWS)
    assert cli.main(["build", "--folder", f]) == 0
    out = capsys.readouterr().out
    assert "Known-case checks: all found" in out and "NOT FOUND" not in out
    assert "[found] AMARAJABAT -> ARE&M" in out and "ratios 10.0 / 10.0" in out
    report = open(os.path.join(f, "security_master", "build_report.txt"), encoding="utf-8").read()
    assert "ISIN_CHANGE events: 6" in report and "RENAME events: 3" in report and "securities: 13" in report
    assert cli.main(["check", "--folder", f]) == 0
    assert "all found" in capsys.readouterr().out
    assert cli.main(["show", "ARE&M", "--folder", f]) == 0
    shown = capsys.readouterr().out
    assert "AMARAJABAT|ARE&M" in shown and "RENAME" in shown
    assert cli.main(["show", "INE002A01018", "--folder", f]) == 0 and "RELIANCE" in capsys.readouterr().out
    assert cli.main(["show", "NOSUCH", "--folder", f]) == 1
    monkeypatch.setattr(sm, "KNOWN_ENDED", {"HDFC": "2023-07-12", "MINDTREE": None})
    lines, ok = sm.check(f)
    assert not ok and any("NOT FOUND] HDFC (expected around 2023-07-12)" in x for x in lines)
    assert cli.main(["check", "--folder", f]) == 1


def test_cli_errors_and_default_folder(tmp_path, monkeypatch, capsys):
    f = str(tmp_path)
    assert cli.main(["build", "--folder", f]) == 1                      # no bhavcopy files
    assert "No bhavcopy parquet files" in capsys.readouterr().err
    for argv in ([], ["nope"], ["build", "--link-gap", "0"], ["build", "--series", ","], ["show"],
                 ["build", "--folder", "relative/path"]):
        with pytest.raises(SystemExit) as e:
            cli.main(argv)
        assert e.value.code == 2
    write_store(f, trade("AAA", "INE000020011", r(0, 5)))
    monkeypatch.setattr(cli, "equity_data_dir", lambda: f)
    assert cli.main(["build"]) == 0
    assert os.path.isfile(os.path.join(f, "security_master", "securities.parquet"))
    assert cli.main(["show", "AAA"]) == 0

"""backfill/equity/minute_combined.py (#31, P74): the combined Kite + ICICI day rule (decide, pure), day_sources,
load_minute_best and the read-only report, on tests/test_icici_plan.py's synthetic world (real bhavcopy store,
master, factors, exits) with hand-written Kite and ICICI stock folders. No network; .env is never read
(config.load_env stubbed by the imported autouse guard); the stores are only read."""
import os
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from backfill.equity import icici_store as st
from backfill.equity import minute_combined as mc
from backfill.equity import minute_plan as mp
from backfill.equity import minute_store as ms
from backfill.equity import minute_tokencheck as mt
from backfill.equity import special_sessions as ss
from backfill.locks import RunLock
from tests.test_equity_imports import EQUITY, violations
from tests.test_icici_plan import DAYS, build_world, hashes, no_network, q, sid

assert no_network                 # the autouse guard of test_icici_plan.py (no requests, load_env stubbed)

SATURDAY = next(d for d in (DAYS[4] + timedelta(days=k) for k in range(1, 7)) if d.weekday() == 5)
SPECIAL = DAYS[10]                # an OUTAGE day in the test calendar: its bars are never trimmed
NOW = datetime(2015, 9, 1, 20, 0, tzinfo=mp.IST)


# ---- hand-made frames for the pure rule ----

def kite_rows(rows):
    """Kite days rows from (date, minute_rows, check[, conversion]) tuples; conversion False = ratio NaN."""
    out = []
    for r in rows:
        d, n, check = r[:3]
        conv = r[3] if len(r) > 3 else True
        out.append({"date": d, "minute_rows": n, "check": check, "minute_volume": 100.0 * n,
                    "kind": "MULTIPLICATIVE", "ratio": 1.0 if conv else np.nan, "kite_multiplier": np.nan,
                    "kite_difference": np.nan, "minute_scale": 1.0})
    return pd.DataFrame(out, columns=mc.KITE_COLUMNS)


def icici_rows(rows):
    return pd.DataFrame([{"date": d, "minute_rows": n, "check": c, "minute_volume": 50.0 * n} for d, n, c in rows],
                        columns=mc.ICICI_COLUMNS)


def row(t, d):
    return t[t["date"] == d].iloc[0]


D = [date(2016, 1, 1) + timedelta(days=k) for k in range(12)]


def test_kite_ok_day_is_kite():                                                          # 1
    t = mc.decide({D[0]}, kite_rows([(D[0], 375, "OK")]), None)
    r = row(t, D[0])
    assert (r.source, r.reason, r.day_flags, r.kite_bars, r.traded) == ("KITE", "KITE", "", 375, True)


def test_kite_day_empty_with_bars_is_kite_flag_kept():                                   # 2: the P74 regression
    t = mc.decide({D[0]}, kite_rows([(D[0], 58, "DAY_EMPTY|HIGHLOW_DIFF")]), icici_rows([(D[0], 0, "ICICI_EMPTY")]))
    r = row(t, D[0])
    assert (r.source, r.day_flags) == ("KITE", "DAY_EMPTY|HIGHLOW_DIFF")


def test_minute_empty_and_no_kite_row_take_icici():                                      # 3, 4
    t = mc.decide({D[0], D[1]}, kite_rows([(D[0], 0, "MINUTE_EMPTY")]),
                  icici_rows([(D[0], 375, "OK"), (D[1], 376, "OK|SHORT_DAY")]))
    assert (row(t, D[0]).source, row(t, D[0]).reason) == ("ICICI", "KITE_MINUTE_EMPTY")
    assert (row(t, D[1]).source, row(t, D[1]).reason, row(t, D[1]).day_flags) == ("ICICI", "NO_KITE_ROW",
                                                                                  "SHORT_DAY")
    t = mc.decide({D[0]}, None, icici_rows([(D[0], 375, "HIGHLOW_DIFF")]))
    assert (row(t, D[0]).source, row(t, D[0]).reason, row(t, D[0]).day_flags) == ("ICICI", "NO_KITE_FOLDER",
                                                                                  "HIGHLOW_DIFF")


def test_both_sources_kite_wins_and_short_day_is_not_mixed():                            # 5, 6
    t = mc.decide({D[0], D[1]}, kite_rows([(D[0], 375, "OK"), (D[1], 40, "SHORT_DAY")]),
                  icici_rows([(D[0], 375, "OK"), (D[1], 375, "OK")]))
    assert list(t["source"]) == ["KITE", "KITE"]
    assert row(t, D[1]).day_flags == "SHORT_DAY"
    assert (row(t, D[1]).kite_bars, row(t, D[1]).icici_bars) == (40, 375)


def test_neither_source_gives_a_reason_for_each_icici_case():                            # 7
    traded = set(D[:7])
    kite = kite_rows([(D[0], 0, "MINUTE_EMPTY"), (D[1], 0, "EXTRA_SESSION")])
    icici = icici_rows([(D[0], 0, "ICICI_EMPTY"), (D[1], 0, "ICICI_BEFORE_START"), (D[2], 0, "SCALE_UNSURE")])
    ctx = {"match_kind": "ISIN", "plan_days": {D[3]}}
    t = mc.decide(traded, kite, icici, ctx)
    want = {D[0]: "KITE_MINUTE_EMPTY+ICICI_EMPTY", D[1]: "KITE_NO_MINUTES+ICICI_BEFORE_START",
            D[2]: "NO_KITE_ROW+ICICI_NO_MINUTES", D[3]: "NO_KITE_ROW+ICICI_NOT_YET_FETCHED",
            D[4]: "NO_KITE_ROW+NOT_IN_ICICI_PLAN"}
    for d, reason in want.items():
        assert (row(t, d).source, row(t, d).reason) == ("NONE", reason)
    assert row(mc.decide({D[0]}, None, None, {"match_kind": "NOT_IN_ICICI"}), D[0]).reason == \
        "NO_KITE_FOLDER+NOT_IN_ICICI"
    assert row(mc.decide({D[0]}, None, None, {"match_kind": "REVIEW", "plan_days": {D[0]}}), D[0]).reason == \
        "NO_KITE_FOLDER+ICICI_REVIEW"


def test_kite_minutes_without_a_conversion_are_none():                                   # D-a addition
    t = mc.decide({D[0]}, kite_rows([(D[0], 53, "DAY_EMPTY", False)]), icici_rows([(D[0], 0, "ICICI_EMPTY")]))
    assert (row(t, D[0]).source, row(t, D[0]).reason, row(t, D[0]).day_flags) == ("NONE", "KITE_NO_CONVERSION", "")


def test_stale_extra_session_on_a_traded_day_is_kite():                                 # RELIANCE 2020-02-01
    t = mc.decide({D[0]}, kite_rows([(D[0], 375, "EXTRA_SESSION")]), None)
    assert (row(t, D[0]).source, row(t, D[0]).day_flags, row(t, D[0]).traded) == ("KITE", "EXTRA_SESSION", True)


def test_minutes_without_a_bhav_row_are_no_bhav_row():                                   # 8 (pure)
    t = mc.decide({D[0]}, kite_rows([(D[0], 375, "OK"), (D[1], 1, "EXTRA_SESSION"), (D[2], 0, "EXTRA_SESSION")]),
                  icici_rows([(D[3], 5, "EXTRA_SESSION")]))
    assert list(t["date"]) == [D[0], D[1], D[3]]                   # a 0-bar row without trading is not a day
    assert (row(t, D[1]).traded, row(t, D[1]).source, row(t, D[1]).reason) == (False, "KITE", "NO_BHAV_ROW")
    assert (row(t, D[3]).traded, row(t, D[3]).source, row(t, D[3]).reason) == (False, "ICICI", "NO_BHAV_ROW")


def test_frozen_and_session_kind_are_carried():                                         # 10 (pure)
    t = mc.decide({D[0], D[1]}, kite_rows([(D[0], 375, "OK")]), None, {"frozen": True, "special": {D[1]: "MUHURAT"}})
    assert t["frozen"].all()
    assert list(t["session_kind"]) == ["", "MUHURAT"]


def test_traded_days_definition():
    rows = pd.DataFrame({"date": [D[0], D[0], D[1], D[2], D[3]], "series": ["EQ", "BE", "BZ", "EQ", "N1"],
                         "volume": [10, 5, 3, 0, 9]})
    assert mc.traded_days(rows) == {D[0], D[1]}


# ---- the synthetic store ----

def candles(d, times, price):
    return [{"date": datetime.combine(d, datetime.strptime(x, "%H:%M").time()), "open": price, "high": price + 1,
             "low": price - 1, "close": price + 0.5, "volume": 100} for x in times]


def frame(rows, maker):
    return pd.concat([maker(c) for c in rows if c], ignore_index=True)


def write_kite(f, s, days, bars):
    path = ms.stock_dir(f, s)
    os.makedirs(path, exist_ok=True)
    rows = []
    for d, n, check in days:
        rows.append({"date": d, "symbol": "X", "kind": "MULTIPLICATIVE", "ratio": 1.0, "kite_multiplier": np.nan,
                     "kite_difference": np.nan, "vol_ratio": 1.0, "minute_scale": 1.0, "minute_volume_scale": 1.0,
                     "minute_rows": n, "check": check, "minute_volume": 100.0 * n, "our_factor": 1.0})
    ms.write_days(pd.DataFrame(rows), os.path.join(path, ms.DAYS_FILE))
    ms.write_minutes(frame(bars, ms.minute_frame), os.path.join(path, "2015.parquet"))


def write_icici(f, s, days, bars, scale=2.0):
    path = st.stock_dir(f, s)
    os.makedirs(path, exist_ok=True)
    rows = [{"date": d, "symbol": "X", "icici_scale": scale, "icici_volume_scale": 1.0, "minute_rows": n,
             "check": check, "minute_volume": 100.0 * n, "our_factor": 1.0} for d, n, check in days]
    ms.write_days(pd.DataFrame(rows), os.path.join(path, ms.DAYS_FILE))
    icici = [[{"datetime": c["date"].strftime("%Y-%m-%d %H:%M:%S"), **{k: c[k] for k in
              ("open", "high", "low", "close", "volume")}} for c in b] for b in bars]
    ms.write_minutes(frame(icici, st.minute_frame), os.path.join(path, "2015.parquet"))


@pytest.fixture(scope="module")
def world(tmp_path_factory):
    f = str(tmp_path_factory.mktemp("combined") / "data")
    mpatch = pytest.MonkeyPatch()
    mpatch.setattr("config.load_env", lambda: {})
    ids = build_world(f)
    mpatch.undo()
    L, A = ids["LIVE1"], ids["DEADA"]
    write_kite(f, L, [(DAYS[0], 3, "OK"), (DAYS[1], 2, "DAY_EMPTY"), (DAYS[2], 0, "MINUTE_EMPTY"),
                      (DAYS[4], 1, "SHORT_DAY"), (DAYS[5], 1, "OK"), (SATURDAY, 1, "EXTRA_SESSION"),
                      (DAYS[6], 2, "OK"), (SPECIAL, 2, "OK")],
               [candles(DAYS[0], ["09:15", "09:16", "15:29"], 500), candles(DAYS[1], ["17:30", "17:31"], 501),
                candles(DAYS[4], ["14:00"], 504), candles(DAYS[5], ["09:15"], 505), candles(SATURDAY, ["10:00"], 506),
                candles(DAYS[6], ["09:15", "15:30"], 507), candles(SPECIAL, ["09:15", "16:30"], 510)])
    write_icici(f, L, [(DAYS[2], 4, "OK"), (DAYS[3], 1, "OK"), (DAYS[4], 3, "OK"), (DAYS[5], 1, "OK"),
                       (DAYS[7], 0, "ICICI_EMPTY")],
                [candles(DAYS[2], ["09:07", "09:15", "15:30", "15:31"], 251), candles(DAYS[3], ["10:00"], 252),
                 candles(DAYS[4], ["09:15", "12:00", "15:29"], 252), candles(DAYS[5], ["09:15"], 253)])
    write_kite(f, A, [(DAYS[10], 1, "OK")], [candles(DAYS[10], ["09:15"], 1010)])
    write_icici(f, A, [(DAYS[11], 1, "OK")], [candles(DAYS[11], ["09:15"], 505)])
    ref = os.path.dirname(ss.data_copy(f))
    os.makedirs(ref, exist_ok=True)
    pd.DataFrame([{"date": str(SPECIAL), "weekday": SPECIAL.strftime("%a"), "kind": "OUTAGE", "start": "09:15",
                   "end": "17:00", "in_bhavcopy": "yes", "evidence": "test", "note": ""}],
                 columns=ss.COLUMNS).to_csv(ss.data_copy(f), index=False)
    review = pd.DataFrame([{"security_id": ids["LIVE2"], "state": mt.REJECTED},
                           {"security_id": ids["DEADB"], "state": mt.PENDING},
                           {"security_id": A, "state": mt.ACCEPTED}], columns=mt.REVIEW_COLUMNS).fillna("")
    os.makedirs(mt.out_dir(f), exist_ok=True)
    review.to_csv(mt.review_path(f), index=False)
    return f, ids


def test_day_sources_on_the_store(world):
    f, ids = world
    t = mc.day_sources(ids["LIVE1"], DAYS[0], DAYS[12], folder=f)
    src = dict(zip(t["date"], t["source"]))
    assert src[DAYS[0]] == "KITE" and src[DAYS[1]] == "KITE" and src[DAYS[2]] == "ICICI" and src[DAYS[3]] == "ICICI"
    assert src[DAYS[4]] == "KITE" and src[DAYS[5]] == "KITE" and src[DAYS[7]] == "NONE"
    assert row(t, DAYS[7]).reason == "NO_KITE_ROW+ICICI_EMPTY"
    assert row(t, DAYS[8]).reason == "NO_KITE_ROW+NOT_IN_ICICI_PLAN"          # no ICICI plan in this world
    assert (row(t, SATURDAY).traded, row(t, SATURDAY).reason) == (False, "NO_BHAV_ROW")
    assert row(t, SPECIAL).session_kind == "OUTAGE"
    assert t.attrs["security_id"] == ids["LIVE1"]
    assert list(t["date"]) == sorted(t["date"])
    assert not t["frozen"].any()


def test_frozen_stocks(world):                                                           # 10
    f, ids = world
    assert mc.day_sources(ids["LIVE2"], folder=f)["frozen"].all()        # REJECTED
    assert mc.day_sources(ids["DEADB"], folder=f)["frozen"].all()        # PENDING
    assert not mc.day_sources(ids["DEADA"], folder=f)["frozen"].any()    # ACCEPTED is not frozen
    assert set(mc.day_sources(ids["LIVE2"], folder=f)["source"]) == {"NONE"}   # loads what is stored (here none)


def test_load_one_source_per_day_in_date_order(world):                                   # 6, 12
    f, ids = world
    m = mc.load_minute_best(ids["LIVE1"], DAYS[0], DAYS[12], folder=f, regular_hours=False)   # DAYS[1] is evening
    per_day = m.groupby(m["ts"].dt.date)["source"].agg(set)
    assert all(len(s) == 1 for s in per_day)
    assert {d: s.pop() for d, s in per_day.items()} == {DAYS[0]: "KITE", DAYS[1]: "KITE", DAYS[2]: "ICICI",
                                                       DAYS[3]: "ICICI", DAYS[4]: "KITE", DAYS[5]: "KITE",
                                                       DAYS[6]: "KITE", SPECIAL: "KITE"}
    assert m["ts"].is_monotonic_increasing
    assert list(m.columns) == mc.BAR_COLUMNS
    short = m[m["ts"].dt.date == DAYS[4]]
    assert len(short) == 1 and short["day_flags"].iat[0] == "SHORT_DAY"          # no ICICI bar mixed in
    assert m.loc[m["ts"].dt.date == DAYS[1], "day_flags"].eq("DAY_EMPTY").all()  # the P74 day is loaded
    assert len(m.attrs["days"]) and set(m.attrs["clean"]) == {"KITE", "ICICI"}


def test_regular_hours_trim(world):                                                      # 9
    f, ids = world
    m = mc.load_minute_best(ids["LIVE1"], DAYS[0], DAYS[12], folder=f)
    hm = dict(m.groupby(m["ts"].dt.date)["ts"].agg(lambda s: [x.strftime("%H:%M") for x in s]))
    assert hm[DAYS[2]] == ["09:15"]                       # ICICI 09:07 / 15:30 / 15:31 dropped on a normal day
    assert hm[DAYS[6]] == ["09:15"]                       # Kite's 15:30 too
    assert hm[SPECIAL] == ["09:15", "16:30"]              # a calendar day is not trimmed
    assert DAYS[1] not in hm                              # an evening-only day outside the calendar: all trimmed
    full = mc.load_minute_best(ids["LIVE1"], DAYS[0], DAYS[12], folder=f, regular_hours=False)
    hf = dict(full.groupby(full["ts"].dt.date)["ts"].agg(lambda s: [x.strftime("%H:%M") for x in s]))
    assert hf[DAYS[2]] == ["09:07", "09:15", "15:30", "15:31"] and hf[DAYS[1]] == ["17:30", "17:31"]


def test_include_no_trade(world):                                                        # 8
    f, ids = world
    m = mc.load_minute_best(ids["LIVE1"], DAYS[0], DAYS[12], folder=f)
    assert SATURDAY not in set(m["ts"].dt.date)
    m = mc.load_minute_best(ids["LIVE1"], DAYS[0], DAYS[12], folder=f, include_no_trade=True)
    assert set(m.loc[m["ts"].dt.date == SATURDAY, "source"]) == {"KITE"}


@pytest.mark.parametrize("prices", ["raw", "ours"])
def test_prices_pass_through(world, prices):                                             # 11
    f, ids = world
    A = ids["DEADA"]
    m = mc.load_minute_best(A, DAYS[10], DAYS[11], prices=prices, folder=f)
    k = ms.load_minute(A, DAYS[10], DAYS[10], prices=prices, folder=f)
    i = st.load_minute_icici(A, DAYS[11], DAYS[11], prices=prices, folder=f)
    cols = ["open", "high", "low", "close", "volume"]
    assert np.allclose(m.loc[m["source"] == "KITE", cols].to_numpy(float), k[cols].to_numpy(float))
    assert np.allclose(m.loc[m["source"] == "ICICI", cols].to_numpy(float), i[cols].to_numpy(float))
    if prices == "ours":                                  # before DEADA's bonus our factor is not 1
        raw = mc.load_minute_best(A, DAYS[10], DAYS[11], prices="raw", folder=f)
        assert not np.allclose(raw["close"].to_numpy(), m["close"].to_numpy())
    with pytest.raises(ValueError):
        mc.load_minute_best(A, prices="kite", folder=f)


def test_no_bars_raises(world):
    f, ids = world
    with pytest.raises(ms.NoMinuteData):
        mc.load_minute_best(ids["DEADD"], folder=f)


# ---- the report ----

def read_out(out, name):
    return pd.read_csv(os.path.join(out, name), comment="#")


def test_report_totals_and_store_unchanged(world, tmp_path):                             # 13
    f, ids = world
    subs = ("equity_1min", "equity_1min_icici", "NSE_CM_bhav", "security_master", "adjustments", "exits")
    before = {s: hashes(f, s) for s in subs}
    out = str(tmp_path / "combined")
    assert mc.report(f, out=out, log=q, now=NOW) == 0
    assert {s: hashes(f, s) for s in subs} == before
    bs, summ, nd = read_out(out, "by_stock.csv"), read_out(out, "summary.csv"), read_out(out, "none_days.csv")
    bd = read_out(out, "by_date.csv")
    assert summ["days"].sum() == bs["traded_days"].sum() == bs[["kite", "icici", "none"]].to_numpy().sum()
    assert len(nd) == bs["none"].sum()
    assert bd[["kite", "icici", "none"]].to_numpy().sum() == bs["traded_days"].sum()
    live1 = bs[bs["security_id"] == ids["LIVE1"]].iloc[0]
    assert (live1.kite, live1.icici, live1.both, live1.kite_day_empty, live1.no_bhav_row) == (6, 2, 2, 1, 1)
    assert set(bs.loc[bs["frozen"], "security_id"]) == {ids["LIVE2"], ids["DEADB"]}
    nb = read_out(out, "no_bhav_row.csv")
    assert list(nb["date"]) == [str(SATURDAY)] and nb["weekday"].iat[0] == "Sat"
    assert len(read_out(out, "overlap.csv")) == 2
    assert "OK" in open(os.path.join(out, "report.txt"), encoding="utf-8").read()
    assert not open(os.path.join(out, "summary.csv"), encoding="utf-8").readline().startswith("#")


def test_report_refuses_existing_folder_and_busy_lock(world, tmp_path):
    f, ids = world
    out = tmp_path / "exists"
    out.mkdir()
    assert mc.report(f, out=str(out), log=q, now=NOW) == 2
    assert os.listdir(out) == []
    for path in (mp.lock_path(f), st.lock_path(f)):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        lock = RunLock(path).acquire("test")
        try:
            assert mc.report(f, out=str(tmp_path / "busy"), log=q, now=NOW) == 2
        finally:
            lock.release()
        assert not os.path.exists(tmp_path / "busy")


def test_report_flags_a_lock_taken_during_the_run(world, tmp_path, monkeypatch):
    f, ids = world
    real = mc.stock_sources
    taken = []

    def during(*a, **k):
        if not taken:
            RunLock(mp.lock_path(f)).acquire("update").release()      # an update starts and ends mid-run
            taken.append(1)
        return real(*a, **k)

    monkeypatch.setattr(mc, "stock_sources", during)
    out = str(tmp_path / "changed")
    assert mc.report(f, out=out, log=q, now=NOW) == 1
    assert open(os.path.join(out, "summary.csv"), encoding="utf-8").readline().startswith(
        "# stores changed during the run")


def test_report_cli_default_folder_and_stocks(world, monkeypatch, capsys):
    f, ids = world
    monkeypatch.setattr(mc, "datetime", type("Fixed", (), {"now": staticmethod(lambda tz=None: NOW)}))
    assert mc.main(["report", "--folder", f, "--stocks", "LIVE1"]) == 0
    out = os.path.join(f, "analysis", "combined_2015-09-01")
    assert list(read_out(out, "by_stock.csv")["security_id"]) == [ids["LIVE1"]]
    assert mc.main(["report", "--folder", f, "--stocks", "LIVE1"]) == 2               # the folder exists now
    assert "exists" in capsys.readouterr().out
    with pytest.raises(SystemExit):
        mc.main(["report", "--folder", "relative/path"])


def test_no_fno_imports():                                                               # 14
    path = os.path.join(EQUITY, "minute_combined.py")
    with open(path, encoding="utf-8") as fh:
        assert violations(fh.read(), "backfill.equity") == []

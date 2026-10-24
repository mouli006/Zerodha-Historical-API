"""backfill/equity/icici_download.py, icici_store.py and icici_minute.py's download / status / checks / coverage / show
on tests/test_icici_plan.py's synthetic world and FakeBreeze. No network; .env is never read; the Kite store is only
read; pacing is a recording limiter (no sleep) except where a test checks it."""
import json
import os
import shutil
from datetime import datetime, time as dtime, timedelta

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest

import config
import icici_minute as cli
from backfill import fetch
from backfill.equity import adjustments as adj
from backfill.equity import icici_client as ic
from backfill.equity import icici_download as idl
from backfill.equity import icici_plan as ip
from backfill.equity import icici_store as st
from backfill.equity import minute_download as md
from backfill.equity import minute_plan as mp
from backfill.equity import minute_store as ms
from backfill.equity import security_master as sm
from backfill.locks import RunLock
from tests.test_equity_minute_download import FastStop
from tests.test_icici_plan import (BONUS, DAYS, NOW, STOCKS, FakeBreeze, build_world, hashes, icici_bars, no_network,
                                   q, raw, sid)

assert no_network                 # the autouse guard of test_icici_plan.py, active here too


class NoWait(fetch.RateLimiter):
    """RateLimiter without the sleep: counts the waits."""
    waits = 0
    rates = []

    def __init__(self, rate):
        super().__init__(rate)
        NoWait.rates.append(rate)

    def wait(self):
        NoWait.waits += 1


@pytest.fixture(autouse=True)
def no_wait(monkeypatch):
    NoWait.waits, NoWait.rates = 0, []
    monkeypatch.setattr(idl, "RateLimiter", NoWait)


@pytest.fixture(scope="module")
def planned(tmp_path_factory):
    f = str(tmp_path_factory.mktemp("icici_dl") / "data")
    mpatch = pytest.MonkeyPatch()
    mpatch.setattr(config, "load_env", lambda: {})
    build_world(f)
    ip.plan(f, client=FakeBreeze(), now=NOW, log=q)
    mpatch.undo()
    return f


@pytest.fixture
def world(planned, tmp_path):
    f = str(tmp_path / "data")
    shutil.copytree(planned, f)
    return f


@pytest.fixture(scope="module")
def downloaded(planned, tmp_path_factory):
    f = str(tmp_path_factory.mktemp("icici_done") / "data")
    shutil.copytree(planned, f)
    mpatch = pytest.MonkeyPatch()
    mpatch.setattr(config, "load_env", lambda: {})
    mpatch.setattr(fetch, "STOP", FastStop())
    mpatch.setattr(idl, "RateLimiter", NoWait)
    kite = hashes(f, "equity_1min")
    fake = FakeBreeze()
    res = idl.download(f, client=fake, now=NOW, log=q)
    mpatch.undo()
    return f, res, fake, kite


@pytest.fixture
def done_copy(downloaded, tmp_path):
    f = str(tmp_path / "done")
    shutil.copytree(downloaded[0], f)
    return f


def days_of(f, sym):
    return ms.read_days(st.stock_dir(f, sid(f, sym))).set_index("date")


def dl(f, client=None, **kw):
    kw.setdefault("now", NOW)
    kw.setdefault("log", q)
    return idl.download(f, client=client or FakeBreeze(), **kw)


def tmp_files(f):
    return [os.path.join(r, n) for r, _, files in os.walk(f) for n in files if n.endswith(".tmp")]


def stable(df):
    return df.drop(columns=["fetched_at", "run_id"]).reset_index(drop=True)


# ---- the full download ----

def test_download_stores_unchanged_candles_and_converts(downloaded):
    f, res, fake, kite = downloaded
    s = res["stats"]
    assert not (s["cap"] or s["stopped"] or s["auth"] or s["rate_stop"]) and s["failed_chunks"] == 0
    assert hashes(f, "equity_1min") == kite and tmp_files(f) == []
    hist = fake.historical_calls()
    assert len(hist) == len(set(hist))                     # nothing fetched twice
    assert fake.calls[0] == ("session",)
    a = days_of(f, "DEADA")
    assert len(a) == 51
    # the first-minute-day search: days 0, 3, 4 probed empty; the start check sampled 1 and 2 (the only unprobed
    # days before day 5): fetched empty too, so no day is inferred; day 33 fetched empty
    for i in (0, 1, 2, 3, 4, 33):
        assert a.loc[DAYS[i], "check"] == idl.EMPTY
    for i in (1, 2):
        assert a.loc[DAYS[i], "fetched_at"] != ""
    assert not a["check"].str.contains(idl.BEFORE_START).any()
    # candles stored unchanged, ICICI's 09:07 and 15:31 bars included
    y = pq.read_table(os.path.join(st.stock_dir(f, sid(f, "DEADA")), "2015.parquet")).to_pandas()
    day6 = y[y["ts"].dt.date == DAYS[6]].reset_index(drop=True)
    want = st.minute_frame(icici_bars("DEADA", 6, DAYS[6]))
    pd.testing.assert_frame_equal(day6, want)
    assert list(day6["ts"].dt.strftime("%H:%M")) == ["09:07", "09:15", "12:00", "15:29", "15:31"]
    assert a.loc[DAYS[6], "bars_outside"] == 2 and a.loc[DAYS[6], "minute_rows"] == 5
    # the conversion: x2 (volume x0.505) before the bonus, x1 after; the step is explained by the bonus
    before, after = a.loc[DAYS[10]], a.loc[DAYS[40]]
    assert before["icici_scale"] == pytest.approx(2.0, abs=1e-3) and after["icici_scale"] == 1.0
    assert before["icici_volume_scale"] == pytest.approx(1 / (2 * 0.99), rel=0.01) and after["icici_volume_scale"] == 1
    assert before["check"] == "OK" and after["check"] == "OK"
    assert "icici_scale 2 -> 1" in a.loc[DAYS[BONUS], "note"] and "corporate action" in a.loc[DAYS[BONUS], "note"]
    assert "SCALE_STEP" not in a.loc[DAYS[BONUS], "check"]
    assert a.loc[DAYS[10], "ratio"] == pytest.approx(2.0, rel=1e-3)          # information: bhav / ICICI close
    # no day candle: the previous day's scale; a short day; a bad volume day
    assert a.loc[DAYS[20], "check"] == "DAY_EMPTY" and a.loc[DAYS[20], "conversion_from"] == str(DAYS[19])
    assert a.loc[DAYS[20], "icici_scale"] == pytest.approx(2.0, abs=1e-3)
    assert a.loc[DAYS[15], "check"] == "SHORT_DAY" and a.loc[DAYS[15], "first_bar"] == "12:30"
    assert "VOLUME_DIFF" in a.loc[DAYS[25], "check"]
    db = idl.Progress(f, write=False)
    stocks = db.stocks()
    db.close()
    assert stocks[sid(f, "DEADA")]["status"] == "DONE" and stocks[sid(f, "DEADA")]["first_minute_day"] == str(DAYS[5])


def test_symbol_only_pass_and_review(downloaded):
    f, _, fake, _ = downloaded
    db = idl.Progress(f, write=False)
    stocks = db.stocks()
    db.close()
    b = stocks[sid(f, "DEADB")]
    assert b["status"] == "DONE" and b["price_check"].startswith("PASS at scale 1")
    assert days_of(f, "DEADB")["check"].eq("OK").all()
    c = stocks[sid(f, "DEADC")]
    assert c["status"] == "REVIEW" and c["price_check"].startswith("FAIL")
    assert not os.path.exists(st.stock_dir(f, sid(f, "DEADC")))               # nothing stored
    archived = os.listdir(st.archive_dir(f))
    assert len(archived) == 1 and archived[0].startswith(sid(f, "DEADC"))
    assert not any(c[0] == STOCKS["DEADC"]["code"] and c[1] == "1minute" and c[2] != c[3]
                   for c in fake.historical_calls())                         # no chunk after the failed check
    assert not any(c[0] is None for c in fake.historical_calls())             # DEADD (not in ICICI) never asked


def test_live_stock_gets_only_the_kite_gaps(downloaded):
    f, _, fake, _ = downloaded
    live = days_of(f, "LIVE1")
    assert list(live.index) == [DAYS[i] for i in (0, 1, 2, 40, 41, 42)]
    assert (live["icici_scale"] == 1).all() and (live["check"] == "OK").all()
    assert not os.path.exists(st.stock_dir(f, sid(f, "LIVE2")))


def test_loader_raw_ours_icici(downloaded):
    f = downloaded[0]
    raw_ = st.load_minute_icici("DEADA", DAYS[6], DAYS[6], folder=f)
    stored = st.load_minute_icici("DEADA", DAYS[6], DAYS[6], prices="icici", folder=f)
    ours = st.load_minute_icici("DEADA", DAYS[6], DAYS[6], prices="ours", folder=f)
    assert list(raw_.columns) == ["ts", "open", "high", "low", "close", "volume", "security_id", "symbol", "source"]
    assert (raw_["source"] == "ICICI").all() and (raw_["symbol"] == "DEADA").all()
    scale = days_of(f, "DEADA").loc[DAYS[6], "icici_scale"]
    assert np.allclose(raw_["high"], stored["high"] * scale)
    assert raw_["high"].max() == pytest.approx(raw("DEADA", 6)["high"], rel=1e-4)       # back on the bhavcopy
    f6 = adj.factor_on(sid(f, "DEADA"), DAYS[6], f)
    assert f6 == 2.0 and np.allclose(ours["close"], raw_["close"] / f6)
    vs = days_of(f, "DEADA").loc[DAYS[6], "icici_volume_scale"]
    assert np.allclose(raw_["volume"], np.round(stored["volume"] * vs))
    short = st.load_minute_icici("DEADA", DAYS[15], DAYS[15], folder=f)
    assert short["volume"].sum() < raw("DEADA", 15)["volume"] * 0.7                    # a short day stays short
    with pytest.raises(ValueError):
        st.load_minute_icici("DEADA", prices="kite", folder=f)
    with pytest.raises(st.NoMinuteData):
        st.load_minute_icici("DEADD", folder=f)


def test_checks_status_coverage(downloaded, capsys):
    f = downloaded[0]
    res = idl.rebuild_checks(f)
    assert res["checks"]["ICICI_EMPTY"] == 6 and "ICICI_BEFORE_START" not in res["checks"]     # days 1, 2 sampled
    assert res["bars_outside"] > 0
    steps = pd.read_csv(os.path.join(st.check_dir(f), "scale_steps.csv"))
    assert set(steps["flagged"]) == {"N"} and str(DAYS[BONUS]) in set(steps["date"])
    cov = idl.coverage(f).set_index("reason")
    assert cov.loc["MINUTE_EMPTY_DAYS", "state"] == "FILLED" and cov.loc["KITE_MINUTE_START", "state"] == "FILLED"
    assert cov.loc["NO_KITE_DATA", "state"] == "KITE"
    merged = idl.coverage(f)
    merged = merged[merged["reason"] == "MERGED"].set_index("symbols")
    deada = merged[merged.index.str.startswith("DEADA")].iloc[0]
    assert deada["state"] == "PARTLY" and deada["icici_empty"] == 6 and deada["icici_before_start"] == 0
    assert deada["icici_stored"] == 45 and deada["to_do"] == 0
    assert merged[merged.index.str.startswith("DEADD")].iloc[0]["state"] == "NOT_IN_ICICI"
    assert merged[merged.index.str.startswith("DEADC")].iloc[0]["state"] == "REVIEW"
    assert merged[merged.index.str.startswith("DEADB")].iloc[0]["state"] == "FILLED"
    out = idl.status(f, now=NOW, log=q)["text"]
    assert "ICICI_EMPTY 6, ICICI_BEFORE_START 0" in out and "REVIEW stocks: 1" in out
    assert f"Calls used today ({NOW.date()})" in out and "Not in ICICI: 1 stocks" in out
    assert cli.main(["coverage", "--folder", f]) == 0
    assert "ICICI_BEFORE_START" in capsys.readouterr().out
    assert cli.main(["show", "DEADA", "--folder", f]) == 0
    assert "icici_scale" in capsys.readouterr().out
    assert cli.main(["show", "NOSUCH", "--folder", f]) == 1


def test_checks_command_writes_only_check_files(done_copy):
    data = hashes(done_copy, os.path.join("equity_1min_icici", "data"))
    assert cli.main(["checks", "--folder", done_copy]) == 0
    assert hashes(done_copy, os.path.join("equity_1min_icici", "data")) == data
    for name in ("icici_check.csv", "summary.json", "scale_steps.csv"):
        assert os.path.isfile(os.path.join(st.check_dir(done_copy), name))


def test_second_run_does_nothing(done_copy):
    fake = FakeBreeze()
    res = dl(done_copy, fake)
    assert res["items"] == [] and fake.historical_calls() == []


# ---- calls: truncation, cap, pacing, errors ----

def test_thousand_bars_are_split_and_fetched_again(world):
    fake = FakeBreeze(big={STOCKS["LIVE1"]["code"]})
    dl(world, fake, symbols=["LIVE1"])
    code = STOCKS["LIVE1"]["code"]
    calls = [c[2:] for c in fake.historical_calls("1minute") if c[0] == code]
    assert (DAYS[40], DAYS[41]) in calls and (DAYS[40], DAYS[40]) in calls and (DAYS[41], DAYS[41]) in calls
    live = days_of(world, "LIVE1")
    assert (live["minute_rows"] == 600).all() and len(live) == 6


def test_truncated_single_day_fails(world, monkeypatch):
    orig = FakeBreeze.historical

    def thousand(self, code, interval, start, end):
        out = orig(self, code, interval, start, end)
        return (out * 400)[:1000] if interval == "1minute" and start <= DAYS[41] <= end else out
    monkeypatch.setattr(FakeBreeze, "historical", thousand)
    res = dl(world, FakeBreeze(), symbols=["LIVE1"])
    assert res["stats"]["failed_chunks"] == 1
    db = idl.Progress(world, write=False)
    bad = [c for c in db.chunks(sid(world, "LIVE1")) if c["status"] == "FAILED"]
    db.close()
    assert len(bad) == 1 and "truncated" in bad[0]["error"]
    assert DAYS[41] not in days_of(world, "LIVE1").index


def test_daily_cap_stops_cleanly_and_resume_continues(world, downloaded):
    fake = FakeBreeze()
    res = dl(world, fake, max_calls=6)
    assert res["stats"]["cap"] and res["stats"]["calls_today"] == 6 and tmp_files(world) == []
    db = idl.Progress(world, write=False)
    assert db.calls_on(NOW.date()) == 6
    db.close()
    first = fake.historical_calls()
    db = idl.Progress(world, write=False)
    assert set(db.stocks()) == {sid(world, "DEADA")}                   # only the stock the run touched has a row
    db.close()
    text = idl.status(world, max_calls=6, now=NOW, log=q)["text"]
    assert "0 done, 1 partly, 1 not started" in text                    # priority 1: DEADA started, LIVE1 not yet
    res2 = dl(world, fake, max_calls=6)
    assert res2["stats"]["cap"] and res2["stats"]["calls"] == 0        # the cap counts across runs
    later = FakeBreeze()
    dl(world, later, now=NOW + timedelta(days=1))                       # the next day: a fresh quota
    both = first + later.historical_calls()
    assert len(both) == len(set(both))                                  # nothing fetched twice
    full = downloaded[0]
    for sym in ("DEADA", "DEADB", "LIVE1"):
        pd.testing.assert_frame_equal(stable(days_of(world, sym)), stable(days_of(full, sym)))


def test_pacing(world):
    dl(world, symbols=["LIVE1"])
    db = idl.Progress(world, write=False)
    n = db.calls_on(NOW.date())
    db.close()
    assert NoWait.rates == [ip.SPEED / 60] and NoWait.waits == n            # 90 a minute = 1.5 a second
    with pytest.raises(ValueError):
        dl(world, speed=ip.MAX_SPEED + 1)


def test_auth_error_mid_run_and_resume(world, downloaded):
    fake = FakeBreeze(auth_after=9)
    with pytest.raises(mp.SignInNeeded) as e:
        dl(world, fake)
    assert "log in to ICICI again" in str(e.value)
    assert tmp_files(world) == []
    a = days_of(world, "DEADA")                                         # what was fetched is built and readable
    assert len(a) > 0 and all(os.path.isfile(p) for p in [os.path.join(st.stock_dir(world, sid(world, "DEADA")),
                                                                       "2015.parquet")])
    later = FakeBreeze()
    dl(world, later)
    both = fake.historical_calls()[:9] + later.historical_calls()
    assert len(both) == len(set(both))
    pd.testing.assert_frame_equal(stable(days_of(world, "DEADA")), stable(days_of(downloaded[0], "DEADA")))


def test_session_rejected_at_start(world, monkeypatch):
    with pytest.raises(mp.SignInNeeded):
        dl(world, FakeBreeze(reject=True))
    assert not os.path.isdir(st.data_dir(world))
    monkeypatch.setattr(ic.BreezeClient, "from_env", classmethod(lambda cls, env=None, session=None:
                                                                 FakeBreeze(reject=True)))
    assert cli.main(["download", "--folder", world]) == 2


def test_no_credentials_exit_2(world):
    assert cli.main(["download", "--folder", world]) == 2           # config.load_env is stubbed to {}


def test_rate_limit_once_backs_off_twice_stops(world):
    res = dl(world, FakeBreeze(rate_at={3}), symbols=["LIVE1"])
    assert not res["stats"]["rate_stop"] and res["stats"]["failed_chunks"] == 0
    assert len(days_of(world, "LIVE1")) == 6
    other = world + "_2"
    shutil.copytree(world, other)
    shutil.rmtree(st.out_dir(other))
    ip.plan(other, client=FakeBreeze(), now=NOW, log=q)
    res = dl(other, FakeBreeze(rate_at={3, 4}))
    assert res["stats"]["rate_stop"] and "twice" in res["stats"]["rate_stop"]
    assert cli.download_result(res) == 1


def test_failed_chunk_is_retried_next_run(world):
    code = STOCKS["LIVE1"]["code"]
    fake = FakeBreeze(fail={(code, "1minute", DAYS[40])})
    res = dl(world, fake, symbols=["LIVE1"])
    assert res["stats"]["failed_chunks"] == 1 and cli.download_result(res) == 1
    assert sum(1 for c in fake.historical_calls() if c[:3] == (code, "1minute", DAYS[40])) == idl.RETRIES
    dl(world, FakeBreeze(), symbols=["LIVE1"])
    assert len(days_of(world, "LIVE1")) == 6


# ---- finished days, days per stock, refusals ----

def test_finished_day_rule(world, monkeypatch):
    evening = datetime.combine(DAYS[2], dtime(20, 0), tzinfo=mp.IST)
    res = dl(world, symbols=["LIVE1"], now=evening)
    live = days_of(world, "LIVE1")
    assert list(live.index) == [DAYS[0], DAYS[1]] and res["stats"]["deferred_days"] >= 4     # day 2, 40-42 wait
    # a chunk fetched before (day + 1) 01:00 IST is not stored, and fetched again later
    other = world + "_b"
    shutil.copytree(world, other)
    shutil.rmtree(st.out_dir(other))
    ip.plan(other, client=FakeBreeze(), now=NOW, log=q)
    orig = idl.work_items
    late = datetime.combine(DAYS[3], dtime(2, 0), tzinfo=mp.IST)
    monkeypatch.setattr(idl, "work_items", lambda *a: orig(*a[:8], late, *a[9:]))
    dl(other, symbols=["LIVE1"], now=evening)
    assert DAYS[2] not in days_of(other, "LIVE1").index
    monkeypatch.setattr(idl, "work_items", orig)
    dl(other, symbols=["LIVE1"])
    assert list(days_of(other, "LIVE1").index) == [DAYS[i] for i in (0, 1, 2, 40, 41, 42)]


def test_days_per_stock_trial(world, downloaded):
    fake = FakeBreeze()
    dl(world, fake, priority=1, days_per_stock=2)
    a = days_of(world, "DEADA")
    with_min = a[a["minute_rows"] > 0]
    # the search (days 0, 50, 25, 12, 6, 3, 4: not counted) found minutes on 50, 25, 12, 6; then 2 days: 5, 7-8
    assert sorted(with_min.index) == [DAYS[i] for i in (5, 6, 7, 8, 12, 25, 50)]
    db = idl.Progress(world, write=False)
    stocks = db.stocks()
    db.close()
    assert stocks[sid(world, "DEADA")]["status"] == "PARTIAL"
    assert not any(c[0] == STOCKS["DEADB"]["code"] for c in fake.historical_calls())          # priority 2
    dl(world, FakeBreeze())
    pd.testing.assert_frame_equal(stable(days_of(world, "DEADA")), stable(days_of(downloaded[0], "DEADA")))


def test_refusals_and_aborted_runs(world):
    lock = RunLock(st.lock_path(world)).acquire("test")
    try:
        with pytest.raises(mp.LockBusy):
            dl(world)
        assert cli.main(["checks", "--folder", world]) == 2
    finally:
        lock.release()
    up = RunLock(os.path.join(world, "equity_update.lock")).acquire("test")
    try:
        with pytest.raises(mp.StoreBusy):
            dl(world)
    finally:
        up.release()
    db = idl.Progress(world)
    db.save("runs", ["run_id", "command", "started", "result"],
            {"run_id": "old", "command": "download", "started": "2015-01-01 00:00:00", "result": "running"})
    db.close()
    dl(world, symbols=["LIVE1"])
    db = idl.Progress(world, write=False)
    runs = {r["run_id"]: r["result"] for r in db.rows("SELECT * FROM runs")}
    db.close()
    assert runs["old"] == "ABORTED" and "done" in runs.values()


# ---- the conversion on a hand-built example ----

def example(no_day=(), short=None, bad_vol=None):
    days = list(DAYS[:20])
    rows, dcs, bhav = [], [], []
    for i, d in enumerate(days):
        s = 2.0 if i < 10 else 1.0
        bh, bl, bc, bv = 110.0 + i, 90.0 + i, 100.0 + i, 10000
        mvol = bv * s * (0.6 if i == short else 1.0) * (3 if i == bad_vol else 1)
        rows.append({"date": d, "minute_rows": 375, "bars_outside": 2, "first_bar": "12:30" if i == short else "09:07",
                     "last_bar": "15:31", "minute_open": bc / s, "minute_high": bh / s, "minute_low": bl / s,
                     "minute_close": bc / s, "minute_volume": mvol, "fetched": True, "before_start": False,
                     "fetched_at": "2015-06-01 10:00:00", "run_id": "r"})
        if i not in no_day:
            dcs.append({"date": d, "open": bc / s, "high": bh / s, "low": bl / s, "close": bc / s, "volume": mvol})
        bhav.append({"date": d, "symbol": "EX", "series": "EQ", "open": bc, "high": bh, "low": bl, "close": bc,
                     "volume": bv})
    factors = pd.DataFrame({"security_id": ["S9"], "from_date": [days[0]], "to_date": [days[-1]], "factor": [1.0]})
    return idl.derive(pd.DataFrame(rows), pd.DataFrame(dcs), pd.DataFrame(bhav), factors, "S9")


def test_derive_known_example():
    d = example(short=3, bad_vol=18).set_index("date")
    assert list(d["icici_scale"]) == [2.0] * 10 + [1.0] * 10
    assert list(d["icici_volume_scale"]) == [0.5] * 10 + [1.0] * 10
    assert d.iloc[3]["check"] == "SHORT_DAY" and d.iloc[3]["vol_ratio"] == pytest.approx(1 / 1.2)   # info only
    assert d.iloc[18]["check"] == "VOLUME_DIFF" and d.iloc[0]["check"] == "OK"
    assert "SCALE_STEP" in d.iloc[10]["check"] and "no corporate action near" in d.iloc[10]["note"]
    assert list(d.columns) == idl.DAY_COLUMNS[1:]
    d = example(no_day=(0, 10, 15)).set_index("date")
    assert d.iloc[15]["check"] == "DAY_EMPTY" and d.iloc[15]["conversion_from"] == str(DAYS[14])
    assert d.iloc[10]["icici_scale"] == 2.0 and "SCALE_UNSURE" in d.iloc[10]["check"]      # between two levels
    assert d.iloc[0]["icici_scale"] == 2.0 and d.iloc[0]["conversion_from"] == str(DAYS[1])  # no previous: next
    assert "SCALE_UNSURE" not in d.iloc[0]["check"]


def test_symbol_only_scales():
    factors = pd.DataFrame({"security_id": ["S1", "S1"], "from_date": [DAYS[0], DAYS[30]],
                            "to_date": [DAYS[29], DAYS[60]], "factor": [4.0, 2.0]})
    assert idl.allowed_scales(factors, "S1", DAYS[5]) == [1.0, 2.0, 4.0]
    assert idl.allowed_scales(factors, "S2", DAYS[5]) == [1.0]
    ok, s = idl.scale_ok({"close": 50.0, "high": 55.0, "low": 45.0}, {"close": 100.0, "high": 110.0, "low": 90.0},
                         [1.0, 2.0])
    assert ok and s == 2.0
    assert not idl.scale_ok({"close": 70.0, "high": 77.0, "low": 63.0},
                            {"close": 100.0, "high": 110.0, "low": 90.0}, [1.0, 2.0])[0]


# ---- the start check of the first-minute-day search ----

def check_row(f, sym):
    db = idl.Progress(f, write=False)
    row = db.start_check(sid(f, sym))
    db.close()
    return row


def samples_of(row, rnd=None):
    return [(s["date"], s["bars"] > 0) for s in json.loads(row["samples"]) if rnd is None or s["round"] == rnd]


def minute_calls(fake, sym):
    return [c[2:] for c in fake.historical_calls("1minute") if c[0] == STOCKS[sym]["code"]]


def test_start_check_sample_with_minutes_searches_again(world, monkeypatch):
    monkeypatch.setitem(STOCKS["DEADA"], "empty", {12})        # an empty day on the search path: it says day 13
    dl(world, symbols=["DEADA"])
    row = check_row(world, "DEADA")
    assert (row["first_before"], row["first_after"], row["result"], row["rounds"]) == \
        (str(DAYS[13]), str(DAYS[5]), "MOVED", 2)
    assert samples_of(row, 1) == [(str(DAYS[6]), True), (str(DAYS[3]), False), (str(DAYS[9]), True)]
    assert samples_of(row, 2) == [(str(DAYS[1]), False), (str(DAYS[2]), False)]
    a = days_of(world, "DEADA")
    assert all(a.loc[DAYS[i], "minute_rows"] > 0 for i in range(5, 12)) and a.loc[DAYS[12], "check"] == idl.EMPTY
    db = idl.Progress(world, write=False)
    assert db.stock(sid(world, "DEADA"))["first_minute_day"] == str(DAYS[5])
    db.close()
    assert "1 moved" in idl.status(world, now=NOW, log=q)["text"]


def test_start_check_keeps_the_start_and_is_not_repeated(world):
    fake = FakeBreeze()
    dl(world, fake, symbols=["DEADA"], days_per_stock=2)
    row = check_row(world, "DEADA")
    assert (row["first_before"], row["first_after"], row["result"], row["rounds"]) == \
        (str(DAYS[5]), str(DAYS[5]), "KEPT", 1)
    assert samples_of(row) == [(str(DAYS[1]), False), (str(DAYS[2]), False)]   # the only unprobed days before 5
    assert (DAYS[1], DAYS[1]) in minute_calls(fake, "DEADA") and (DAYS[2], DAYS[2]) in minute_calls(fake, "DEADA")
    again = FakeBreeze()
    dl(world, again, symbols=["DEADA"], days_per_stock=2)
    assert check_row(world, "DEADA") == row                                     # never repeated
    assert not any(a == b and a < DAYS[5] for a, b in minute_calls(again, "DEADA"))
    text = idl.status(world, now=NOW, log=q)["text"]
    assert "Start checks: 1 done (1 kept, of which 0 still NONE; 0 moved, of which 0 from NONE), 2 sample calls; " \
           "0 pending" in text


def test_start_check_of_a_stock_searched_before_runs_first(world):
    dl(world, symbols=["DEADA"], days_per_stock=2)
    db = idl.Progress(world)                     # as a progress file from before the start check: no row, no samples
    db.write("DELETE FROM start_checks")
    for d in (DAYS[1], DAYS[2]):
        db.write("DELETE FROM chunks WHERE security_id = ? AND interval = 'minute' AND chunk_from = ?",
                 (sid(world, "DEADA"), str(d)))
    db.close()
    fake = FakeBreeze()
    dl(world, fake, symbols=["DEADA"], days_per_stock=2)
    assert minute_calls(fake, "DEADA")[:2] == [(DAYS[1], DAYS[1]), (DAYS[2], DAYS[2])]   # before any minute chunk
    assert check_row(world, "DEADA")["result"] == "KEPT"


def test_start_check_of_none_finds_the_minutes(world, monkeypatch):
    monkeypatch.setitem(STOCKS["DEADA"], "empty", set(range(41, 51)))   # the last needed days are empty: NONE
    dl(world, symbols=["DEADA"])
    row = check_row(world, "DEADA")
    assert (row["first_before"], row["first_after"], row["result"]) == (idl.NONE, str(DAYS[5]), "MOVED")
    assert samples_of(row, 1) == [(str(DAYS[25]), True), (str(DAYS[12]), True), (str(DAYS[37]), True)]
    a = days_of(world, "DEADA")
    assert all(a.loc[DAYS[i], "minute_rows"] > 0 for i in range(5, 41) if i != 33)
    assert all(a.loc[DAYS[i], "check"] == idl.EMPTY for i in range(41, 51))
    db = idl.Progress(world, write=False)
    assert db.stock(sid(world, "DEADA"))["first_minute_day"] == str(DAYS[5])     # not NONE
    db.close()
    assert "1 moved, of which 1 from NONE" in idl.status(world, now=NOW, log=q)["text"]


def test_start_check_keeps_none(world, monkeypatch):
    monkeypatch.setitem(STOCKS["DEADA"], "start", 99)                   # ICICI has no minutes at all
    dl(world, symbols=["DEADA"])
    row = check_row(world, "DEADA")
    assert (row["first_before"], row["first_after"], row["result"]) == (idl.NONE, idl.NONE, "KEPT")
    assert samples_of(row) == [(str(DAYS[25]), False), (str(DAYS[12]), False), (str(DAYS[37]), False)]
    a = days_of(world, "DEADA")
    note = a.loc[DAYS[1], "note"]
    assert a.loc[DAYS[1], "check"] == idl.BEFORE_START and \
        f"samples {DAYS[25]}, {DAYS[12]}, {DAYS[37]} without minutes" in note
    assert a.loc[DAYS[25], "check"] == idl.EMPTY
    assert "1 kept, of which 1 still NONE" in idl.status(world, now=NOW, log=q)["text"]


def test_failed_sample_records_nothing_and_checks_next_run(world):
    code = STOCKS["DEADA"]["code"]
    fake = FakeBreeze(fail={(code, "1minute", DAYS[1])})
    dl(world, fake, symbols=["DEADA"], days_per_stock=2)
    assert check_row(world, "DEADA") == {}
    assert sum(1 for c in fake.historical_calls() if c[:3] == (code, "1minute", DAYS[1])) == idl.RETRIES
    later = FakeBreeze()
    dl(world, later, symbols=["DEADA"], days_per_stock=2)
    assert check_row(world, "DEADA")["result"] == "KEPT"
    assert (DAYS[1], DAYS[1]) in minute_calls(later, "DEADA")


def test_session_timeout_at_start_ends_the_run(world, monkeypatch, capsys):
    res = dl(world, FakeBreeze(session_error=ic.IciciFailed("customerdetails: ReadTimeout: timed out")))
    assert "ReadTimeout" in res["stats"]["failed"]
    db = idl.Progress(world, write=False)
    runs = db.rows("SELECT * FROM runs")
    db.close()
    assert len(runs) == 1 and runs[0]["result"] == "failed: session" and runs[0]["ended"]
    assert os.path.isfile(os.path.join(st.check_dir(world), "summary.json"))
    assert os.path.isfile(os.path.join(st.check_dir(world), "coverage.csv"))
    monkeypatch.setattr(ic.BreezeClient, "from_env", classmethod(
        lambda cls, env=None, session=None: FakeBreeze(session_error=ic.IciciFailed("timed out"))))
    assert cli.main(["download", "--folder", world]) == 1
    assert "Not run: timed out" in capsys.readouterr().err
    db = idl.Progress(world, write=False)
    assert all(r["result"] != "running" for r in db.rows("SELECT * FROM runs"))
    db.close()

def test_before_start_counted_apart_after_the_start_check(world, monkeypatch, capsys):
    monkeypatch.setitem(STOCKS["DEADA"], "start", 20)     # ICICI's minutes begin on day 20
    dl(world, symbols=["DEADA"])
    row = check_row(world, "DEADA")
    assert (row["first_before"], row["first_after"], row["result"], row["rounds"]) == \
        (str(DAYS[20]), str(DAYS[20]), "KEPT", 1)
    assert samples_of(row) == [(str(DAYS[6]), False), (str(DAYS[3]), False), (str(DAYS[9]), False)]
    a = days_of(world, "DEADA")
    empty = [0, 12, 18, 19, 3, 6, 9, 33]                 # search probes, start-check samples, day 33
    before = [1, 2, 4, 5, 7, 8, 10, 11, 13, 14, 15, 16, 17]  # inferred, never fetched
    for i in empty:
        assert a.loc[DAYS[i], "check"] == idl.EMPTY
    for i in before:
        assert a.loc[DAYS[i], "check"] == idl.BEFORE_START and a.loc[DAYS[i], "fetched_at"] == ""
        assert f"samples {DAYS[6]}, {DAYS[3]}, {DAYS[9]} without minutes" in a.loc[DAYS[i], "note"]
    assert cli.main(["checks", "--folder", world]) == 0
    capsys.readouterr()
    with open(os.path.join(st.check_dir(world), "summary.json"), encoding="utf-8") as fh:
        summary = json.load(fh)
    assert summary["checks"]["ICICI_EMPTY"] == 8 and summary["checks"]["ICICI_BEFORE_START"] == 13
    text = idl.status(world, now=NOW, log=q)["text"]
    assert "ICICI_EMPTY 8, ICICI_BEFORE_START 13" in text
    cov = pd.read_csv(os.path.join(st.check_dir(world), "coverage.csv"), dtype=str, keep_default_na=False)
    deada = cov[(cov["reason"] == "MERGED") & cov["symbols"].str.startswith("DEADA")].iloc[0]
    assert (deada["icici_empty"], deada["icici_before_start"], deada["state"]) == ("8", "13", "PARTLY")

# ---- HIGHLOW_DIFF / CLOSE_DIFF on the 09:15-15:29 bars; checks --rederive ----

def bar_frame(day, bars):
    return pd.DataFrame([{"ts": pd.Timestamp(datetime.combine(day, t), tz=mp.IST), "open": o, "high": h, "low": l,
                          "close": c, "volume": 10} for t, o, h, l, c in bars])


def test_session_range_skips_pre_open_and_keeps_evening_sessions():
    m = pd.concat([bar_frame(DAYS[0], [(dtime(9, 7), 100, 150, 100, 100), (dtime(9, 15), 100, 110, 90, 100),
                                       (dtime(15, 31), 100, 100, 60, 100)]),
                   bar_frame(DAYS[1], [(dtime(18, 15), 100, 120, 95, 100), (dtime(19, 0), 100, 101, 80, 100)])])
    agg = idl.aggregates(m)
    a, b = agg[DAYS[0]], agg[DAYS[1]]
    assert (a["minute_high"], a["minute_low"], a["session_high"], a["session_low"]) == (150, 60, 110, 90)
    assert a["bars_outside"] == 2 and a["minute_rows"] == 3
    assert (b["session_high"], b["session_low"]) == (120, 80) and b["bars_outside"] == 2     # evening: every bar


def edit_bars(f, sym, edits):
    """Change ICICI's stored bars of sym: edits = {day index: {"HH:MM": {column: value}}}."""
    path = os.path.join(st.stock_dir(f, sid(f, sym)), f"{DAYS[0].year}.parquet")
    y = pq.read_table(path).to_pandas()
    for i, by_time in edits.items():
        for hm, values in by_time.items():
            at = (y["ts"].dt.date == DAYS[i]) & (y["ts"].dt.strftime("%H:%M") == hm)
            assert at.sum() == 1
            for c, v in values.items():
                y.loc[at, c] = v
    ms.write_minutes(y, path)


def test_pre_open_bar_outside_the_bhav_range_is_ok_a_session_bar_is_not(done_copy):
    f = done_copy
    d = days_of(f, "DEADA")
    bh, bl, bc = (d.loc[DAYS[i], f"bhav_{x}"] for i, x in ((40, "high"), (41, "low"), (44, "close")))
    hi42 = d.loc[DAYS[42], "bhav_high"]
    edit_bars(f, "DEADA", {40: {"09:07": {"high": bh * 1.05}},            # pre-open print above the bhav high
                           41: {"09:07": {"low": bl * 0.9}},              # pre-open print below the bhav low
                           42: {"12:00": {"high": hi42 * 1.05}},          # a real session bar above the bhav high
                           44: {"09:07": {"open": bc, "high": bc, "low": bc * 0.99, "close": bc},   # the bhav close
                                "09:15": {"open": bc * 1.01, "low": bc * 1.01, "close": bc * 1.01},   # only pre-open
                                "12:00": {"open": bc * 1.01, "low": bc * 1.01, "close": bc * 1.01},
                                "15:29": {"open": bc * 1.01, "low": bc * 1.01, "close": bc * 1.01,
                                          "high": bc * 1.011}}})
    bars = hashes(f, os.path.join("equity_1min_icici", "data"))
    days_files = {k: v for k, v in bars.items() if k.endswith(st.DAYS_FILE)}
    res = idl.rederive(f, log=q)                                           # the dry run writes nothing
    assert hashes(f, os.path.join("equity_1min_icici", "data")) == bars
    assert not os.path.exists(st.archive_dir(f)) or not any(n.startswith("rederive_")
                                                            for n in os.listdir(st.archive_dir(f)))
    changed = {(s, d_): (a, b) for s, _, d_, a, b in res["labels"]}
    deada = sid(f, "DEADA")
    assert changed == {(deada, DAYS[42]): ("OK", "HIGHLOW_DIFF"), (deada, DAYS[44]): ("OK", "CLOSE_DIFF")}
    assert res["after"]["HIGHLOW_DIFF"] == res["before"].get("HIGHLOW_DIFF", 0) + 1
    assert {(s, d_) for s, _, d_, c, _, _ in res["other"] if c == "check"} == set(changed)   # new labels, not removed
    assert "dry run, nothing written" in idl.rederive_text(res)
    res = idl.rederive(f, apply=True, now=NOW, log=q)
    assert res["applied"] and os.path.isdir(res["backup"]) and os.path.isdir(os.path.join(res["backup"], "check"))
    backed = hashes(res["backup"], "data")
    assert sorted(backed.values()) == sorted(days_files.values())         # every old days.parquet backed up
    after = hashes(f, os.path.join("equity_1min_icici", "data"))
    assert {k: v for k, v in after.items() if not k.endswith(st.DAYS_FILE)} == \
        {k: v for k, v in bars.items() if not k.endswith(st.DAYS_FILE)}   # stored bars unchanged
    a = days_of(f, "DEADA")
    assert a.loc[DAYS[40], "check"] == "OK" and a.loc[DAYS[41], "check"] == "OK"
    assert a.loc[DAYS[40], "minute_high"] > bh and a.loc[DAYS[40], "session_high"] < a.loc[DAYS[40], "minute_high"]
    assert a.loc[DAYS[41], "minute_low"] < bl and (a.loc[DAYS[40:42], "bars_outside"] == 2).all()
    assert a.loc[DAYS[42], "check"] == "HIGHLOW_DIFF" and a.loc[DAYS[44], "check"] == "CLOSE_DIFF"
    rows = pd.read_csv(os.path.join(st.check_dir(f), "icici_check.csv"))
    assert {"session_high", "session_low"} <= set(rows.columns)
    assert set(rows[rows["symbol"] == "DEADA"]["date"]) >= {str(DAYS[42]), str(DAYS[44])}
    again = idl.rederive(f, log=q)
    assert again["labels"] == [] and again["other"] == []                  # derived once, stable


def test_rederive_apply_restores_on_an_error(done_copy, monkeypatch):
    f = done_copy
    before = hashes(f, os.path.join("equity_1min_icici", "data"))
    real, calls = ms.write_days, []

    def failing(df, path):
        calls.append(path)
        real(df.assign(note="changed"), path)
        if len(calls) == 2:
            raise OSError("disk full")
    monkeypatch.setattr(ms, "write_days", failing)
    with pytest.raises(OSError):
        idl.rederive(f, apply=True, now=NOW, log=q)
    assert len(calls) == 2 and hashes(f, os.path.join("equity_1min_icici", "data")) == before
    assert any(n.startswith("rederive_") for n in os.listdir(st.archive_dir(f)))
    assert tmp_files(f) == []


def test_rederive_refusals_and_cli(done_copy, capsys):
    f = done_copy
    data = hashes(f, os.path.join("equity_1min_icici", "data"))
    lock = RunLock(st.lock_path(f)).acquire("test")
    try:
        assert cli.main(["checks", "--rederive", "--folder", f]) == 2
        assert cli.main(["checks", "--rederive", "--apply", "--folder", f]) == 2
    finally:
        lock.release()
    up = RunLock(os.path.join(f, "equity_update.lock")).acquire("test")
    try:
        assert cli.main(["checks", "--rederive", "--folder", f]) == 2
    finally:
        up.release()
    with pytest.raises(SystemExit) as e:
        cli.main(["checks", "--apply", "--folder", f])
    assert e.value.code == 2
    capsys.readouterr()
    assert cli.main(["checks", "--rederive", "--folder", f]) == 0
    assert "dry run, nothing written" in capsys.readouterr().out
    assert hashes(f, os.path.join("equity_1min_icici", "data")) == data


# ---- plan --dry-run: what a new work list adds, progress kept ----

def preview(f, market_wide=2):
    return ip.plan(f, market_wide=market_wide, dry_run=True, preview=idl.plan_preview, now=NOW, log=q)


def test_plan_dry_run_writes_nothing_and_keeps_progress(done_copy, monkeypatch, capsys):
    f = done_copy
    monkeypatch.setattr(config, "load_env", lambda: {})
    plan, days = ip.read_plan(f)
    live1 = sid(f, "LIVE1")
    old = days[~((days["security_id"] == live1) & days["date"].isin(DAYS[:3]))]    # as if added by the new list
    sm.write_parquet(old, os.path.join(st.plan_dir(f), "icici_plan_days.parquet"))
    keep = {sub: hashes(f, os.path.join("equity_1min_icici", sub)) for sub in ("plan", "data", "reference")}
    db = open(idl.db_path(f), "rb").read()
    res = preview(f)
    for sub, h in keep.items():
        assert hashes(f, os.path.join("equity_1min_icici", sub)) == h
    assert open(idl.db_path(f), "rb").read() == db
    pv = res["preview"]
    assert [r["symbol"] for r in pv["new_stocks"]] == ["LIVE2"]
    assert pv["new_stocks"][0]["priority"] == 2 and pv["new_stocks"][0]["trading_days"] == 3
    g = {x["symbol"]: x for x in pv["grown"]}
    assert g["LIVE1"]["added"] == 3 and g["LIVE1"]["stored"] == 3 and g["LIVE1"]["fetch"] == 0
    assert g["LIVE1"]["minute_calls"] == 0 and g["LIVE1"]["day_calls"] == 0
    assert pv["refetch"] == [] and pv["removed"] == [] and pv["reprio"] == []
    assert pv["kept"]["stocks"]["DONE"] >= 3 and pv["kept"]["chunks"]["minute"] > 0
    assert pv["new_calls"] == int(res["plan"].set_index("symbol").loc["LIVE2", "est_calls"])
    assert "nothing written" in pv["text"] and "LIVE2" in pv["text"]
    assert cli.main(["plan", "--market-wide", "2", "--dry-run", "--folder", f]) == 0
    assert "Progress kept" in capsys.readouterr().out
    assert hashes(f, os.path.join("equity_1min_icici", "plan")) == keep["plan"]
    lock = RunLock(st.lock_path(f)).acquire("test")
    try:
        assert cli.main(["plan", "--market-wide", "2", "--dry-run", "--folder", f]) == 2
    finally:
        lock.release()


def test_plan_dry_run_shows_the_partly_done_stock(world, monkeypatch):
    monkeypatch.setattr(config, "load_env", lambda: {})
    dl(world, symbols=["DEADA"], days_per_stock=4)
    pv = preview(world)["preview"]
    part = {p["symbol"]: p for p in pv["kept"]["partial"]}
    a = part["DEADA"]
    assert a["old_needed"] == a["new_needed"] == 51 and 0 < a["left_old"] == a["left_new"] < 51
    assert a["old_pos"] == a["new_pos"] == 1
    assert pv["refetch"] == [] and "partly done" in pv["text"]

"""The ICICI answer guard (P162 / P163 / P164; backfill/equity/icici_guard.py and its use in icici_download.py,
icici_store.py and minute_combined.py) on tests/test_icici_download.py's synthetic world and FakeBreeze. No network;
.env is never read (config.load_env stubbed by the imported autouse guard)."""
import os
from datetime import datetime, time as dtime, timedelta

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest

from backfill.equity import icici_download as idl
from backfill.equity import icici_guard as guard
from backfill.equity import icici_plan as ip
from backfill.equity import icici_store as st
from backfill.equity import minute_combined as mc
from backfill.equity import minute_plan as mp
from backfill.equity import minute_store as ms
from tests.test_icici_download import (days_of, dl, done_copy, downloaded, no_wait, planned,  # noqa: F401
                                       world)
from tests.test_icici_plan import DAYS, NOW, STOCKS, FakeBreeze, hashes, icici_bars, no_network, q, sid

assert no_network and no_wait     # the autouse guards (no requests, load_env stubbed, no sleep)

LIVE = STOCKS["LIVE1"]["code"]


def chunks_of(f, sym, status=None):
    db = idl.Progress(f, write=False)
    try:
        return [c for c in db.chunks(sid(f, sym)) if status is None or c["status"] == status]
    finally:
        db.close()


def patch_answers(monkeypatch, change):
    """FakeBreeze.historical with change(code, interval, start, end, out) -> out applied to every answer."""
    orig = FakeBreeze.historical

    def wrapped(self, code, interval, start, end):
        return change(code, interval, start, end, orig(self, code, interval, start, end))
    monkeypatch.setattr(FakeBreeze, "historical", wrapped)


def on(day, start, end):
    return start <= day <= end


def second_series(bars, k=4.7):
    """The same timestamps at another price level (JUBLFOOD's JUBILANT bars)."""
    return [{**b, "open": round(b["open"] / k, 2), "high": round(b["high"] / k, 2), "low": round(b["low"] / k, 2),
             "close": round(b["close"] / k, 2), "volume": b["volume"] * 3} for b in bars]


# ---- the pure checks ----

def test_guard_functions_on_normal_and_special_answers():
    d = DAYS[6]
    bars = icici_bars("DEADA", 6, d)
    df = st.minute_frame(bars)
    assert guard.minute_answer_problems(df, [b["stock_code"] for b in bars], "DEAA") == []
    assert guard.dup_days(df) == {}
    many = st.minute_frame([{**bars[1], "datetime": f"{d} {(datetime.combine(d, dtime(9, 15)) + timedelta(minutes=k)).time()}"}
                            for k in range(380)] + [dict(bars[1]) for _ in range(30)])
    probs = guard.minute_answer_problems(many, [], "DEAA")
    assert len(probs) == 1 and probs[0].startswith("TOO_MANY_BARS: 410 bars")        # same price: not MIXED
    assert guard.minute_answer_problems(many, [], "DEAA", special={d}) == []          # a calendar day is exempt
    assert guard.dup_days(many)[d][2] == guard.DUPLICATE
    mixed = st.minute_frame(bars + second_series(bars))
    assert guard.dup_days(mixed)[d][2] == guard.MIXED
    assert guard.minute_answer_problems(mixed, [None] * 10, "DEAA")[0].startswith("MIXED_SERIES: 5 duplicated")
    assert guard.minute_answer_problems(df, ["deaa", "JUBLIF"], "DEAA") == ["MIXED_SERIES: answer code JUBLIF != DEAA"]
    kept = guard.keep_last(st.minute_frame(bars + [{**bars[2], "open": 1.0}]))
    assert len(kept) == 5 and kept[kept["ts"].dt.strftime("%H:%M") == "12:00"]["open"].iat[0] == 1.0


# ---- the download ----

def test_two_series_answer_is_failed_and_nothing_built(world, monkeypatch):
    mixed = {"on": True}

    def change(code, interval, a, b, out):
        if mixed["on"] and code == LIVE and interval == "1minute" and on(DAYS[40], a, b):
            out = out + second_series([x for x in out if x["datetime"].startswith(str(DAYS[40]))])
        return out
    patch_answers(monkeypatch, change)
    res = dl(world, symbols=["LIVE1"])
    assert res["stats"]["failed_chunks"] == 1 and res["stats"]["guard_failed"] == 1
    bad = chunks_of(world, "LIVE1", "FAILED")
    assert len(bad) == 1 and bad[0]["error"].startswith("MIXED_SERIES: 5 duplicated timestamps on " + str(DAYS[40]))
    days = days_of(world, "LIVE1")
    assert DAYS[40] not in days.index and DAYS[41] not in days.index               # the whole answer refused
    assert not os.path.isdir(st.staging_dir(world, sid(world, "LIVE1")))
    y = pq.read_table(os.path.join(st.stock_dir(world, sid(world, "LIVE1")), "2015.parquet")).to_pandas()
    assert not (y["ts"].dt.date == DAYS[40]).any() and not y["ts"].duplicated().any()
    out = idl.status(world, now=NOW, log=q)["text"]
    assert "Answer guard: FAILED chunks MIXED_SERIES 1, TOO_MANY_BARS 0" in out
    assert f"FAILED {sid(world, 'LIVE1')} minute {DAYS[40]}..{DAYS[41]}: MIXED_SERIES" in out
    mixed["on"] = False
    fake = FakeBreeze()
    dl(world, fake, symbols=["LIVE1"])                                                 # retried next run, unchanged
    assert (LIVE, "1minute", DAYS[40], DAYS[41]) in fake.historical_calls()
    assert {DAYS[40], DAYS[41]} <= set(days_of(world, "LIVE1").index)


def test_other_stock_code_in_the_answer_fails(world, monkeypatch):
    def change(code, interval, a, b, out):
        if code == LIVE and interval == "1minute" and on(DAYS[41], a, b):
            out = [{**x, "stock_code": "JUBLIF"} if x["datetime"].startswith(str(DAYS[41])) else x for x in out]
        return out
    patch_answers(monkeypatch, change)
    dl(world, symbols=["LIVE1"])
    bad = chunks_of(world, "LIVE1", "FAILED")
    assert len(bad) == 1 and bad[0]["error"] == f"MIXED_SERIES: answer code JUBLIF != {LIVE}"


def test_more_than_400_bars_fails(world, monkeypatch):
    def change(code, interval, a, b, out):
        if code == LIVE and interval == "1minute" and on(DAYS[42], a, b):
            base = next(x for x in out if x["datetime"].startswith(str(DAYS[42])))
            t0 = datetime.combine(DAYS[42], dtime(9, 15))
            out = [x for x in out if not x["datetime"].startswith(str(DAYS[42]))] + \
                [{**base, "datetime": f"{t0 + timedelta(minutes=k):%Y-%m-%d %H:%M:%S}"} for k in range(380)] + \
                [{**base, "datetime": f"{t0:%Y-%m-%d %H:%M:%S}"} for _ in range(25)]
        return out
    patch_answers(monkeypatch, change)
    dl(world, symbols=["LIVE1"])
    bad = chunks_of(world, "LIVE1", "FAILED")
    assert len(bad) == 1 and bad[0]["error"].startswith(f"TOO_MANY_BARS: 405 bars on {DAYS[42]}")
    assert DAYS[42] not in days_of(world, "LIVE1").index


def test_revised_copy_inside_one_answer_is_stored_and_the_last_copy_used(world, monkeypatch):
    def change(code, interval, a, b, out):
        if code == LIVE and interval == "1minute" and on(DAYS[41], a, b):
            noon = next(x for x in out if x["datetime"] == f"{DAYS[41]} 12:00:00")
            out = out + [{**noon, "open": round(noon["open"] * 0.999, 2), "volume": noon["volume"] + 7}]
        return out
    patch_answers(monkeypatch, change)
    res = dl(world, symbols=["LIVE1"])
    assert res["stats"]["failed_chunks"] == 0
    s = sid(world, "LIVE1")
    y = pq.read_table(os.path.join(st.stock_dir(world, s), "2015.parquet")).to_pandas()
    day = y[y["ts"].dt.date == DAYS[41]]
    assert len(day) == 6 and int(day["ts"].duplicated().sum()) == 1                   # stored as received
    r = days_of(world, "LIVE1").loc[DAYS[41]]
    assert r["check"] == guard.DUPLICATE and r["minute_rows"] == 5
    assert "1 duplicated timestamps (revised copies; the last copy used)" in r["note"]
    want = sum(x["volume"] for x in icici_bars("LIVE1", 41, DAYS[41])) + 7              # the last copy's volume
    assert r["minute_volume"] == want
    m = st.load_minute_icici("LIVE1", DAYS[41], DAYS[41], prices="icici", folder=world, clean=False)
    assert len(m) == 5 and not m["ts"].duplicated().any()
    assert m.loc[m["ts"].dt.strftime("%H:%M") == "12:00", "volume"].iat[0] == day["volume"].iat[3]    # last copy
    assert "DUPLICATE_MINUTE 1 (last copy used)" in idl.status(world, now=NOW, log=q)["text"]


def test_two_answers_of_one_day_keep_the_later_one(world):
    dl(world, symbols=["LIVE1"])
    s = sid(world, "LIVE1")
    plan, pdays = ip.read_plan(world)
    item = plan[plan["security_id"] == s].iloc[0].to_dict()
    need = sorted(pdays[pdays["security_id"] == s]["date"])
    stage = st.staging_dir(world, s)
    os.makedirs(stage)
    bars = st.minute_frame(icici_bars("LIVE1", 41, DAYS[41]))
    older = bars.assign(close=bars["close"] + 0.05, fetched_at="2015-09-01 09:00:00", run_id="a")
    newer = bars.assign(fetched_at="2015-09-01 09:30:00", run_id="b")
    older.to_parquet(os.path.join(stage, f"m_{DAYS[40]}.parquet"))                    # a 2-day answer, earlier
    newer.to_parquet(os.path.join(stage, f"m_{DAYS[41]}.parquet"))                    # the day fetched again later
    runner = idl.Runner(world, mp.Inputs(world, bhav=False), None, ip.SPEED, ip.MAX_CALLS, None, "t",
                        lambda: NOW, q)
    try:
        runner.build(item, need)
    finally:
        runner.db.close()
    assert runner.stats["overlap_days"] == 1
    y = pq.read_table(os.path.join(st.stock_dir(world, s), "2015.parquet")).to_pandas()
    day = y[y["ts"].dt.date == DAYS[41]].reset_index(drop=True)
    assert len(day) == 5 and np.allclose(day["close"], newer["close"])


def test_day_candles_duplicate_date_fails_and_holds_the_minutes(world, monkeypatch):
    def change(code, interval, a, b, out):
        if code == LIVE and interval == "1day":
            out = out + [dict(out[0])]
        return out
    patch_answers(monkeypatch, change)
    fake = FakeBreeze()
    res = dl(world, fake, symbols=["LIVE1"])
    assert res["stats"]["guard_failed"] == 1
    bad = chunks_of(world, "LIVE1", "FAILED")
    assert len(bad) == 1 and bad[0]["interval"] == "day" and bad[0]["error"].startswith("DUPLICATE_DATES: 1")
    assert fake.historical_calls("1minute") == []                                     # minutes held this run
    assert not os.path.exists(os.path.join(st.stock_dir(world, sid(world, "LIVE1")), st.DAYS_FILE))


def test_day_mismatch_twice_goes_to_review(world, monkeypatch):
    def change(code, interval, a, b, out):
        if code == LIVE and interval == "1day":           # a quarter of the candles from another stock (x 1/5)
            out = [{**x, "high": round(x["high"] / 5, 2), "low": round(x["low"] / 5, 2),
                    "close": round(x["close"] / 5, 2)} if k % 4 == 0 else x for k, x in enumerate(out)]
        return out
    patch_answers(monkeypatch, change)
    dl(world, symbols=["LIVE1"])
    bad = chunks_of(world, "LIVE1", "FAILED")
    assert len(bad) == 1 and bad[0]["error"].startswith("DAY_MISMATCH: ")
    db = idl.Progress(world, write=False)
    assert db.stock(sid(world, "LIVE1")).get("status") != "REVIEW"
    db.close()
    res = dl(world, symbols=["LIVE1"], now=NOW + timedelta(hours=2))                 # a second run: REVIEW
    assert res["stats"]["review"] == [sid(world, "LIVE1")]
    db = idl.Progress(world, write=False)
    row = db.stock(sid(world, "LIVE1"))
    db.close()
    assert row["status"] == "REVIEW" and row["reason"].startswith("DAY_MISMATCH on 2 runs")
    fake = FakeBreeze()
    assert dl(world, fake, symbols=["LIVE1"], now=NOW + timedelta(hours=4))["items"] == []
    assert fake.historical_calls() == []                                             # never retried again
    assert "DAY_MISMATCH on 2 runs" in idl.status(world, now=NOW, log=q)["text"]


# ---- days stored before the guard: rederive and the loaders ----

def add_bars(f, sym, rows):
    path = os.path.join(st.stock_dir(f, sid(f, sym)), "2015.parquet")
    y = pq.read_table(path).to_pandas()
    ms.write_minutes(pd.concat([y, st.minute_frame(rows)], ignore_index=True).sort_values("ts", kind="stable"), path)


def test_stored_mixed_day_is_labelled_and_never_loaded(done_copy, tmp_path):
    f = done_copy
    add_bars(f, "DEADA", second_series(icici_bars("DEADA", 6, DAYS[6])))               # two series on day 6
    noon = next(x for x in icici_bars("DEADA", 7, DAYS[7]) if x["datetime"].endswith("12:00:00"))
    add_bars(f, "DEADA", [{**noon, "open": round(noon["open"] * 0.999, 2)}])           # a revised copy on day 7
    s = sid(f, "DEADA")
    data = os.path.join("equity_1min_icici", "data")
    before = hashes(f, data)
    # before any label: the loaders refuse the mixed day from its bars
    t = mc.day_sources("DEADA", DAYS[5], DAYS[7], folder=f).set_index("date")
    assert t.loc[DAYS[6], "source"] == "NONE" and t.loc[DAYS[6], "reason"].endswith("+ICICI_MIXED_SERIES")
    assert t.loc[DAYS[5], "source"] == "ICICI" and t.loc[DAYS[7], "source"] == "ICICI"
    m = mc.load_minute_best("DEADA", DAYS[5], DAYS[7], folder=f)
    assert not m["ts"].duplicated().any() and DAYS[6] not in set(m["ts"].dt.date)
    assert len(m[m["ts"].dt.date == DAYS[7]]) == 3                                    # 09:15, 12:00, 15:29
    ic_ = st.load_minute_icici("DEADA", DAYS[5], DAYS[7], folder=f)
    assert ic_.attrs["mixed_days"] == [DAYS[6]] and not ic_["ts"].duplicated().any()
    # the rederive dry run proposes the labels and writes nothing
    res = idl.rederive(f, log=q)
    assert hashes(f, data) == before
    changed = {(x[0], x[2]): (x[3], x[4]) for x in res["labels"]}
    assert changed == {(s, DAYS[6]): ("OK", "HIGHLOW_DIFF|VOLUME_DIFF|MIXED_SERIES"),          # as JUBLFOOD
                       (s, DAYS[7]): ("OK", guard.DUPLICATE)}
    assert res["after"][guard.MIXED] == 1 and res["after"][guard.DUPLICATE] == 1
    # applied (temp data only): labels written, bars unchanged, the day still NONE; the report counts the reason
    bars_before = hashes(f, os.path.join("equity_1min_icici", "data", s, "2015.parquet"))
    idl.rederive(f, apply=True, log=q)
    assert hashes(f, os.path.join("equity_1min_icici", "data", s, "2015.parquet")) == bars_before
    d = days_of(f, "DEADA")
    assert guard.MIXED in d.loc[DAYS[6], "check"] and "from two series" in d.loc[DAYS[6], "note"]
    t = mc.day_sources("DEADA", DAYS[6], DAYS[6], folder=f)
    assert t["reason"].iat[0] == "NO_KITE_FOLDER+ICICI_MIXED_SERIES"
    out = str(tmp_path / "rep")
    assert mc.report(f, out=out, stocks=["DEADA"], log=q) == 0
    none = pd.read_csv(os.path.join(out, "none_days.csv"))
    assert list(none.loc[none["reason"].str.contains("ICICI_MIXED_SERIES"), "date"]) == [str(DAYS[6])]
    status = idl.status(f, now=NOW, log=q)["text"]
    assert "stored days MIXED_SERIES 1 (not loaded), DUPLICATE_MINUTE 1" in status


def test_decide_mixed_label_and_the_load_net(monkeypatch, done_copy):
    from datetime import date
    d0, d1 = date(2016, 1, 1), date(2016, 1, 4)
    icici = pd.DataFrame([{"date": d0, "minute_rows": 700, "check": "HIGHLOW_DIFF|MIXED_SERIES", "minute_volume": 1.0},
                          {"date": d1, "minute_rows": 375, "check": "OK", "minute_volume": 1.0}],
                         columns=mc.ICICI_COLUMNS)
    t = mc.decide({d0, d1}, None, icici).set_index("date")
    assert (t.loc[d0, "source"], t.loc[d0, "reason"], t.loc[d0, "day_flags"]) == \
        ("NONE", "NO_KITE_FOLDER+ICICI_MIXED_SERIES", "")
    assert t.loc[d1, "source"] == "ICICI"
    t = mc.decide({d1}, None, icici, {"icici_mixed": {d1}}).set_index("date")
    assert t.loc[d1, "reason"] == "NO_KITE_FOLDER+ICICI_MIXED_SERIES"
    # the net in load_minute_best: a loaded day holding a timestamp twice becomes NONE
    orig = st.load_minute_icici

    def doubled(*a, **k):
        m = orig(*a, **k)
        return pd.concat([m, m.iloc[:1]], ignore_index=True)
    monkeypatch.setattr(st, "load_minute_icici", doubled)
    m = mc.load_minute_best("DEADA", DAYS[6], DAYS[7], folder=done_copy)
    assert not m["ts"].duplicated().any() and DAYS[6] not in set(m["ts"].dt.date)
    days = m.attrs["days"].set_index("date")
    assert days.loc[DAYS[6], "source"] == "NONE" and days.loc[DAYS[6], "reason"] == mc.ICICI_MIXED

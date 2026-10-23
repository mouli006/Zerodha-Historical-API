"""Finished days only (docs/dev/equity_minute_finished_days.md): update's target day, the safety net with its deferral
cap, and equity_minute.py repair, on test_equity_minute_download.py's synthetic world and fake Kite. No network;
.env is never read (config.load_env stubbed)."""
import hashlib
import os
from datetime import date, datetime, time as dtime, timedelta

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest

import config
import equity_minute as cli
from backfill.equity import minute_download as md
from backfill.equity import minute_plan as mp
from backfill.equity import minute_repair as mr
from backfill.equity import minute_store as ms
from backfill.equity import security_master as sm
from backfill.locks import RunLock
from test_equity_minute_download import (DAYS, FAST, MORNING_AFTER, N, FakeKite, days_of, done_copy,  # noqa: F401
                                         downloaded, extend, minute_bars, kite_day, no_network, planned, q, sid,
                                         world)

IST = mp.IST


def at(d, h, m=0):
    return datetime.combine(d, dtime(h, m), tzinfo=IST)


def morning_after(i):
    return at(DAYS[i] + timedelta(days=1), 2)


class EdgeKite(FakeKite):
    """FakeKite with unfinished day candles: provisional (volume = the summed minute volume, close = the last minute
    close) or none on the given (symbol, day index); exact: the minute volumes sum to the day volume (an illiquid
    stock without pre-open or closing trades)."""

    def __init__(self, provisional=(), no_candle=(), exact=(), **kw):
        super().__init__(**kw)
        self.provisional, self.no_candle, self.exact = set(provisional), set(no_candle), set(exact)

    def historical_data(self, token, start, end, interval, continuous=False, oi=False):
        out = super().historical_data(token, start, end, interval, continuous, oi)
        sym = self.by_token[token]
        res = []
        for c in out:
            d = c["date"].date()
            i = DAYS.index(d) if d in DAYS else None
            if interval == "day":
                if (sym, i) in self.no_candle:
                    continue
                if (sym, i) in self.provisional:
                    bars = minute_bars(sym, i, d)
                    c = {**c, "close": bars[-1]["close"], "volume": sum(b["volume"] for b in bars)}
            elif (sym, i) in self.exact:
                kv = kite_day(sym, i)["volume"]
                k = [b for b in out if b["date"].date() == d].index(c)
                c = {**c, "volume": kv - 2 * (kv // 3) if k == 0 else kv // 3}
            res.append(c)
        return res


def year_file(f, sym):
    return os.path.join(ms.stock_dir(f, sid(f, sym)), "2015.parquet")


def minute_dates(f, sym):
    t = pq.read_table(year_file(f, sym)).to_pandas()
    return set(pd.to_datetime(t["ts"]).dt.tz_convert(ms.TZ).dt.date)


def stock_row(f, sym):
    return md.read_progress(f)[1][sid(f, sym)]


def tree(f, sub="equity_1min"):
    out = {}
    for root, _, files in os.walk(os.path.join(f, sub)):
        for name in files:
            if name == ".lock" or name.endswith(("-wal", "-shm")):
                continue
            p = os.path.join(root, name)
            with open(p, "rb") as fh:
                out[os.path.relpath(p, f)] = hashlib.sha256(fh.read()).hexdigest()
    return out


# ---- Part A1: the target day ----

def test_target_day_rule():
    days = [d for d in DAYS[:N]]
    d = DAYS[40]                                      # a Wednesday in the store
    assert md.target_day(days, at(d, 22))[0] == DAYS[39]                       # evening on D -> D-1
    assert md.target_day(days, at(d + timedelta(days=1), 0, 30))[0] == DAYS[39]   # 00:30 on D+1 -> D-1
    assert md.target_day(days, at(d + timedelta(days=1), 1))[0] == d           # 01:00 on D+1 -> D
    assert md.target_day(days, datetime.combine(d + timedelta(days=1), dtime(1)))[0] == d   # naive = IST
    fri = next(x for x in days if x.weekday() == 4)
    mon = fri + timedelta(days=3)
    t, why, in_store = md.target_day(days, at(mon, 9))
    assert t == fri and in_store and "bhavcopy in the store" in why                # Monday morning -> Friday


def test_target_day_holidays_and_days_without_bhavcopy():
    days = [x for x in DAYS[:N] if x != DAYS[40]]     # DAYS[40] is a holiday: not in the store
    assert md.target_day(days, at(DAYS[41], 2))[0] == DAYS[39]
    last = DAYS[N - 1]                                 # the newest bhavcopy day
    nxt = DAYS[N]                                      # the next weekday: no bhavcopy yet
    t, why, in_store = md.target_day(days, at(nxt + timedelta(days=1), 2))
    assert t == nxt and not in_store and "update would refuse" in why and "equity_update.py" in why
    t, _, in_store = md.target_day(days, at(nxt + timedelta(days=1), 2), holidays={nxt})
    assert t == last and in_store                      # cm_bhav found it a holiday: skipped
    sat = next(x for x in (last + timedelta(days=k) for k in range(1, 8)) if x.weekday() == 5)
    assert md.target_day(days + [sat], at(sat + timedelta(days=1), 2))[0] == sat   # a weekend session in the store
    assert md.target_day(days, at(sat + timedelta(days=1), 2), holidays={nxt, nxt + timedelta(days=1)})[0] != sat


def test_bhav_holidays_read_from_days_csv(tmp_path):
    f = str(tmp_path)
    assert md.bhav_holidays(f) == set()
    os.makedirs(mp.store_dir(f))
    pd.DataFrame({"date": ["2026-10-20", "2026-10-21"], "status": ["holiday", "ok"]}).to_csv(
        os.path.join(mp.store_dir(f), "days.csv"), index=False)
    assert md.bhav_holidays(f) == {date(2026, 10, 20)}


def test_status_shows_the_target_day(world):
    st = md.status(world, now=at(DAYS[N - 1], 20), log=q)
    assert st["target"] == DAYS[N - 2]
    assert f"Target day for update now: {DAYS[N - 2]} {DAYS[N - 2]:%a} (latest trading day finished" in st["report"]
    st = md.status(world, now=morning_after(N), log=q)
    assert st["target"] == DAYS[N] and "update would refuse" in st["report"]


def test_evening_update_stores_only_the_previous_day(done_copy):
    f = done_copy
    extend(f)                                          # the bhavcopy of DAYS[N] is in the store
    kite = FakeKite(last_day=N)
    res = md.update(f, kite=kite, speed=FAST, now=at(DAYS[N], 20), log=q)
    assert res["stats"]["target"] == str(DAYS[N - 1]) and res["stats"]["updated"] == 0
    assert days_of(f, "BIG").index[-1] == DAYS[N - 1]
    assert all(c[2] <= DAYS[N - 1] for c in kite.calls if c[0] != "instruments")


def test_a_run_crossing_midnight_keeps_its_target(done_copy, monkeypatch):
    f = done_copy
    extend(f)
    clock = iter([at(DAYS[N], 23, 59)])

    def now_ist():
        return next(clock, at(DAYS[N] + timedelta(days=1), 1, 30))   # after the first call: past 01:00

    monkeypatch.setattr(md, "now_ist", now_ist)
    kite = FakeKite(last_day=N)
    res = md.update(f, kite=kite, speed=FAST, log=q)
    assert res["stats"]["target"] == str(DAYS[N - 1])
    assert days_of(f, "BIG").index[-1] == DAYS[N - 1]
    assert not [c for c in kite.calls if c[0] != "instruments" and c[2] >= DAYS[N]]


# ---- Part A2: the safety net ----

@pytest.mark.parametrize("kw, reason", [(dict(no_candle={("BIG", N)}), "no day candle"),
                                        (dict(provisional={("BIG", N)}), "= summed minute volume")])
def test_unfinished_day_is_deferred_then_stored(done_copy, kw, reason):
    f = done_copy
    extend(f)
    before = days_of(f, "BIG")
    logs = []
    res = md.update(f, kite=EdgeKite(last_day=N, **kw), speed=FAST, now=MORNING_AFTER,
                    log=lambda *a, **k: logs.append(" ".join(map(str, a))))
    st = res["stats"]
    assert st["deferred_days"] == 1 and st["deferred_stocks"] == 1 and st["failed_stocks"] == 0
    assert st["deferred"][0][:2] == ("BIG", DAYS[N]) and reason in st["deferred"][0][2]
    assert any("Deferred (not stored, the next update tries again): 1" in x for x in logs)
    assert any("1 deferred days" in x for x in logs)
    pd.testing.assert_frame_equal(days_of(f, "BIG"), before)                       # nothing stored
    assert stock_row(f, "BIG")["last_day"] == str(DAYS[N - 1])                      # last_day unchanged
    assert DAYS[N] not in minute_dates(f, "BIG")                                    # no minute bars of T
    assert days_of(f, "MID").index[-1] == DAYS[N]                                   # the others stored
    res = md.update(f, kite=FakeKite(last_day=N), speed=FAST, now=MORNING_AFTER + timedelta(hours=1), log=q)
    assert res["stats"]["deferred_days"] == 0
    big = days_of(f, "BIG")
    assert big.index[-1] == DAYS[N] and big.loc[DAYS[N], "check"] == "OK"
    assert stock_row(f, "BIG")["last_day"] == str(DAYS[N]) and DAYS[N] in minute_dates(f, "BIG")
    pd.testing.assert_frame_equal(big.iloc[:-1], before)


def test_illiquid_day_with_equal_volumes_is_stored(done_copy):
    f = done_copy
    extend(f)
    res = md.update(f, kite=EdgeKite(last_day=N, exact={("BIG", N)}), speed=FAST, now=MORNING_AFTER, log=q)
    assert res["stats"]["deferred_days"] == 0
    row = days_of(f, "BIG").loc[DAYS[N]]
    assert row["kite_volume"] == row["minute_volume"] == row["bhav_volume"] and row["check"] == "OK"


def test_unfinished_unit():
    inputs = pd.DataFrame({"date": DAYS[:4], "minute_rows": [3, 3, 3, 0], "kite_close": [np.nan, 10, 10, np.nan],
                           "kite_volume": [np.nan, 300, 300, np.nan], "minute_volume": [300, 300, 300, 0]})
    bhav = pd.DataFrame({"date": DAYS[:4], "volume": [310, 310, 300, 310]})
    out = md.unfinished(inputs, bhav)
    assert set(out) == {DAYS[0], DAYS[1]}               # no candle; candle = minutes != bhav; illiquid kept
    assert md.unfinished(inputs, None) == {}            # no bhavcopy row: never unfinished


def rollback(f, sym, i):
    """Make sym's stored history end at DAYS[i] (as if the update had not run since)."""
    s, path = sid(f, sym), ms.stock_dir(f, sid(f, sym))
    days = ms.read_days(path)
    ms.write_days(days[days["date"] <= DAYS[i]], os.path.join(path, ms.DAYS_FILE))
    t = pq.read_table(year_file(f, sym)).to_pandas()
    ms.write_minutes(t[pd.to_datetime(t["ts"]).dt.tz_convert(ms.TZ).dt.date <= DAYS[i]], year_file(f, sym))
    db = md.Progress(f)
    db.write("UPDATE stocks SET last_day=? WHERE security_id=?", (str(DAYS[i]), s))
    db.close()


def test_deferral_cap_stores_an_old_unfinished_day_labelled(done_copy):
    f = done_copy
    rollback(f, "BIG", N - 5)
    bad = N - 4                                        # BIG's candle of DAYS[N-4] never looks final
    for age, t in enumerate(range(N - 4, N)):          # target DAYS[N-4] .. DAYS[N-1]: age 0 .. 3, deferred
        res = md.update(f, kite=EdgeKite(provisional={("BIG", bad)}), speed=FAST, now=morning_after(t), log=q)
        st = res["stats"]
        assert st["target"] == str(DAYS[t]) and st["deferred_stocks"] == 1, age
        assert st["deferred_days"] == age + 1 and st["unconfirmed_days"] == 0
        assert stock_row(f, "BIG")["last_day"] == str(DAYS[N - 5])
    extend(f)
    logs = []
    res = md.update(f, kite=EdgeKite(provisional={("BIG", bad)}, last_day=N), speed=FAST, now=MORNING_AFTER,
                    log=lambda *a, **k: logs.append(" ".join(map(str, a))))
    st = res["stats"]                                  # target DAYS[N]: DAYS[N-4] is 4 trading days old
    assert st["deferred_days"] == 0 and st["unconfirmed_days"] == 1
    assert st["unconfirmed"][0][:2] == ("BIG", DAYS[bad])
    assert any("Stored unconfirmed (more than 3 trading days old): 1" in x for x in logs)
    assert any("1 stored unconfirmed" in x for x in logs)
    big = days_of(f, "BIG")
    assert "CANDLE_UNCONFIRMED" in big.loc[DAYS[bad], "check"]
    assert list(big.index[-5:]) == DAYS[N - 4:N + 1] and stock_row(f, "BIG")["last_day"] == str(DAYS[N])
    summary = md.rebuild_checks(f, {})
    assert summary["checks"]["CANDLE_UNCONFIRMED"] == 1
    chk = md.checks(f, log=q)                          # checks keeps the label when it re-derives
    big_id = sid(f, "BIG")
    assert chk["after"].get("CANDLE_UNCONFIRMED") == 1
    assert big_id not in {x[0] for x in chk["changed"]} and not [x for x in chk["refused"] if big_id in x]
    md.checks(f, apply=True, log=q)                    # (b) checks --apply keeps the stored label
    assert "CANDLE_UNCONFIRMED" in days_of(f, "BIG").loc[DAYS[bad], "check"]
    assert md.rebuild_checks(f, {})["checks"]["CANDLE_UNCONFIRMED"] == 1


def test_derive_never_adds_candle_unconfirmed(done_copy):
    f = done_copy                                      # (a) volume = minute sum != bhavcopy on every day
    s = sid(f, "BIG")
    x = ms.read_days(ms.stock_dir(f, s))[md.INPUT_COLUMNS].copy()
    x["kite_volume"] = x["minute_volume"].astype("float64")
    calc = md.Calc(f, mp.Inputs(f, bhav=False))
    out = calc.derive(s, x)
    assert len(md.unfinished(x, calc.bhav_of(s))) > 50
    assert not out["check"].str.contains("CANDLE_UNCONFIRMED").any()


def test_add_check():
    assert md.add_check("OK", "CANDLE_UNCONFIRMED") == "CANDLE_UNCONFIRMED"
    assert md.add_check("SCALE_STEP|VOLUME_DIFF", "CANDLE_UNCONFIRMED") == "VOLUME_DIFF|SCALE_STEP|CANDLE_UNCONFIRMED"
    assert md.add_check("CANDLE_UNCONFIRMED", "CANDLE_UNCONFIRMED") == "CANDLE_UNCONFIRMED"


def test_build_defers_an_unfinished_last_day(world):
    res = md.download(world, kite=EdgeKite(no_candle={("BIG", N - 1)}), symbols=["BIG"], speed=FAST, workers=1,
                      now=morning_after(N - 1), log=q)
    assert res["stats"]["built"] == 1 and res["stats"]["deferred_days"] == 1
    big = days_of(world, "BIG")
    assert big.index[-1] == DAYS[N - 2] and stock_row(world, "BIG")["last_day"] == str(DAYS[N - 2])
    assert DAYS[N - 1] not in minute_dates(world, "BIG") and DAYS[N - 2] in minute_dates(world, "BIG")
    assert "DAY_EMPTY" in big.loc[DAYS[70], "check"]   # an old day without a candle stays (out of the window)


# ---- Part B: repair ----

UPDATE_RUN = "20150423-000500-abcd"


def make_rows(f, specs):
    """Store rows as the old update did: specs = [(symbol, day index, kind, fetched_at)], kind "prov" (Kite's
    provisional candle: close = last minute close, volume = summed minutes; fetched_at and the update run), "empty"
    (no candle; fetched_at and the update run) or "old_empty" (no candle, still the download's run and time). Each
    stock is re-derived whole (a consistent stored file)."""
    calc = md.Calc(f, mp.Inputs(f, bhav=False))
    by = {}
    for sym, i, kind, fetched in specs:
        by.setdefault(sym, []).append((DAYS[i], kind, fetched))
    for sym, rows in by.items():
        s, path = sid(f, sym), ms.stock_dir(f, sid(f, sym))
        x = ms.read_days(path)[md.INPUT_COLUMNS].copy()
        for c in md.FIELDS:
            x[f"kite_{c}"] = x[f"kite_{c}"].astype("float64")
        for d, kind, fetched in rows:
            i = int(np.flatnonzero(x["date"] == d)[0])
            if kind == "prov":
                x.at[i, "kite_close"] = x.at[i, "minute_close"]
                x.at[i, "kite_volume"] = x.at[i, "minute_volume"]
            else:
                for c in md.FIELDS:
                    x.at[i, f"kite_{c}"] = np.nan
            if kind != "old_empty":
                x.at[i, "fetched_at"] = fetched
                x.at[i, "run_id"] = UPDATE_RUN
        ms.write_days(calc.derive(s, x), os.path.join(path, ms.DAYS_FILE))


def update_run(f):
    db = md.Progress(f)
    db.start_run(UPDATE_RUN, "update")
    db.end_run(UPDATE_RUN, 0, 0, "done")
    db.close()


def fabricate(f, ptwo=True):
    """BIG and PTWO provisional on DAYS[N-1] (fetched the same evening), MID fetched at 00:00:00 the next day
    (ARIHANTSUP: only the 01:00 rule finds it), GEN1 a DAY_EMPTY of an update run at 00:05. PTWO: its DAYS[N-2] and
    DAYS[N-3] have no candle (old, fetched by the download) and DAYS[N-2] borrows DAYS[N-1]'s conversion: repairing
    DAYS[N-1] would change it."""
    update_run(f)
    d, nxt = DAYS[N - 1], DAYS[N - 1] + timedelta(days=1)
    specs = [("BIG", N - 1, "prov", f"{d} 20:05:00"), ("MID", N - 1, "prov", f"{nxt} 00:00:00"),
             ("GEN1", N - 1, "empty", f"{nxt} 00:05:00")]
    if ptwo:
        specs += [("PTWO", N - 1, "prov", f"{d} 20:06:00"), ("PTWO", N - 2, "old_empty", ""),
                  ("PTWO", N - 3, "old_empty", "")]
    make_rows(f, specs)


@pytest.fixture
def broken(done_copy):
    fabricate(done_copy)
    return done_copy


@pytest.fixture
def clean(done_copy):
    """broken without PTWO: every affected stock can be repaired."""
    fabricate(done_copy, ptwo=False)
    return done_copy


def rp(f, kite, **kw):
    kw.setdefault("speed", FAST)
    kw.setdefault("log", q)
    return mr.repair(f, kite=kite, **kw)


def test_repair_selection(broken):
    f = broken
    affected, counts = mr.find_affected(f)
    d = DAYS[N - 1]
    assert affected == {sid(f, "BIG"): {d: mr.PROVISIONAL}, sid(f, "MID"): {d: mr.PROVISIONAL},
                        sid(f, "PTWO"): {d: mr.PROVISIONAL}, sid(f, "GEN1"): {d: mr.DAY_EMPTY}}
    assert counts == {"provisional": 3, "day_empty": 1, "date_rule": 2, "stocks": 4}   # MID: only the 01:00 rule
    assert "DAY_EMPTY" in days_of(f, "EMPTYCO").loc[DAYS[35], "check"]      # historical DAY_EMPTY: not selected
    assert "DAY_EMPTY" in days_of(f, "BIG").loc[DAYS[70], "check"]          # nor the session without candles


def test_repair_dry_run_writes_nothing(broken):
    f = broken
    before = tree(f)
    kite = FakeKite()
    res = rp(f, kite)
    assert tree(f) == before
    assert {s for s, _, _ in res["repaired"]} == {"BIG", "MID", "GEN1"} and res["requests"] == 1 + 4
    assert [s for s, _, _ in res["refused"]] == ["PTWO"]
    assert "rows not repaired would change (" in res["refused"][0][2] and str(DAYS[N - 2]) in res["refused"][0][2]
    assert res["refused"][0][2].startswith("earlier row borrows a repaired date's conversion")      # PTWO's N-2
    assert res["borrowing"] == [sid(f, "PTWO")] and res["drift"] == []
    assert res["before"]["DAY_EMPTY"] == 1 and res["after"].get("DAY_EMPTY", 0) == 0 and res["after"]["OK"] == 3
    assert res["ratio"]["0.1-0.5%"] == 2 and sum(res["ratio"].values()) == 3
    text = res["report"]
    assert "Repaired (dry run: would repair): 3 rows in 3 stocks" in text and "PTWO" in text
    assert "(1 with a row borrowing a repaired date's conversion, 0 with a later row changed)" in text
    assert [c for c in kite.calls if c[0] == "instruments"] == [("instruments", "NSE")]
    assert sorted(c[1] for c in kite.calls if c[0] != "instruments") == [DAYS[N - 1]] * 4


def test_repair_skips_new_token_and_still_provisional(broken):
    f = broken
    kite = EdgeKite(new_tokens={"BIG": 999}, provisional={("MID", N - 1)})
    res = rp(f, kite)
    skipped = {s: why for s, _, why in res["skipped"]}
    assert list(skipped) == ["BIG"] and "token 101 -> 999: left to update / retoken" in skipped["BIG"]
    assert [(s, d) for s, _, d, _ in res["skipped_dates"]] == [("MID", DAYS[N - 1])]
    assert "still not final" in res["skipped_dates"][0][3]
    assert [s for s, _, _ in res["repaired"]] == ["GEN1"] and res["stocks"] == 4
    assert "Skipped stocks: 1" in res["report"] and "Skipped dates (the new candle is not final either): 1" in         res["report"]
    assert mr.find_affected(f)[0] == mr.find_affected(f)[0] and len(mr.find_affected(f)[0]) == 4   # nothing written


def test_repair_apply_changes_only_the_repaired_dates(broken):
    f = broken
    before = {sym: days_of(f, sym) for sym in ("BIG", "MID", "GEN1", "PTWO")}
    files = tree(f)
    res = rp(f, FakeKite(), apply=True)
    d = DAYS[N - 1]
    after = tree(f)
    changed = {k for k in after if after[k] != files.get(k)}
    for sym in ("BIG", "MID", "GEN1"):
        new, old = days_of(f, sym), before[sym]
        pd.testing.assert_frame_equal(new.drop(index=d), old.drop(index=d), check_exact=True)   # (c)
        assert new.loc[d, "check"] == "OK" and new.loc[d, "kite_close"] == kite_day(sym, N - 1)["close"]
        assert new.loc[d, "run_id"] != UPDATE_RUN
        assert os.path.join("equity_1min", "data", sid(f, sym), ms.DAYS_FILE) in changed
    pd.testing.assert_frame_equal(days_of(f, "PTWO"), before["PTWO"])            # refused: not written
    assert not [k for k in changed if k.endswith("2015.parquet")]                 # minute files untouched
    backup = res["backup"]
    assert os.path.basename(backup).startswith("repair_")
    for sym in ("BIG", "MID", "GEN1"):
        with open(os.path.join(backup, sid(f, sym), ms.DAYS_FILE), "rb") as fh:
            assert hashlib.sha256(fh.read()).hexdigest() == \
                files[os.path.join("equity_1min", "data", sid(f, sym), ms.DAYS_FILE)]
    rep = pd.read_csv(os.path.join(backup, "repair.csv"), dtype=str, keep_default_na=False)
    assert sorted(rep["result"]) == ["REFUSED", "REPAIRED", "REPAIRED", "REPAIRED"]
    runs = md.read_progress(f)[2]
    assert runs[-1]["command"] == "repair" and runs[-1]["result"] == "done"
    assert not [n for n in after if n.endswith(".tmp")]
    again, counts = mr.find_affected(f)                # a second run finds nothing (but the refused stock)
    assert list(again) == [sid(f, "PTWO")]
    kite = FakeKite()
    res2 = rp(f, kite, apply=True)
    assert res2["repaired"] == [] and res2["refused"][0][0] == "PTWO"


def test_repair_second_run_finds_nothing(clean):
    f = clean
    res = rp(f, FakeKite(), apply=True)
    assert len(res["repaired"]) == 3 and not res["refused"] and not res["skipped"]
    assert mr.find_affected(f)[0] == {}
    kite = FakeKite()
    res = rp(f, kite, apply=True)
    assert res["stocks"] == 0 and "Nothing to repair" in res["report"] and kite.calls == []


def test_repair_limit(clean):
    res = rp(clean, FakeKite(), limit=2)
    assert res["stocks"] == 2 and len(res["repaired"]) == 2 and res["requests"] == 3


def test_repair_nothing_to_do(done_copy):
    kite = FakeKite()
    res = rp(done_copy, kite)
    assert res["stocks"] == 0 and "Nothing to repair" in res["report"] and kite.calls == []


def test_repair_error_restores_every_file(broken, monkeypatch):
    f = broken
    before = tree(f, os.path.join("equity_1min", "data"))
    calls = []

    def bad_replace(tmp, path):
        calls.append(path)
        if len(calls) == 3:
            raise OSError("disk gone")
        os.replace(tmp, path)

    monkeypatch.setattr(mr, "_replace", bad_replace)
    with pytest.raises(md.ApplyFailed, match="restored"):
        rp(f, FakeKite(), apply=True)
    assert len(calls) == 3
    assert tree(f, os.path.join("equity_1min", "data")) == before
    assert md.read_progress(f)[2][-1]["result"] == "failed"


@pytest.mark.parametrize("apply", [False, True])
def test_repair_refusals(broken, monkeypatch, tmp_path, capsys, apply):
    f = broken
    flag = ["--apply"] if apply else []
    monkeypatch.setattr(mp, "kite_client", lambda: kite)
    kite = FakeKite()
    lock = RunLock(mp.lock_path(f)).acquire("test")
    try:
        with pytest.raises(mp.LockBusy):
            rp(f, kite, apply=apply)
        assert cli.main(["repair", "--folder", f] + flag) == 2
    finally:
        lock.release()
    fno = tmp_path / "fno"
    monkeypatch.setattr(config, "load_env", lambda: {"KITE_DATA_DIR": str(fno)})
    lock = RunLock(str(fno / "NIFTY_FnO_1min" / ".lock")).acquire("test")
    try:
        with pytest.raises(mp.FnoBusy):
            rp(f, kite, apply=apply)
        assert cli.main(["repair", "--folder", f] + flag) == 2
    finally:
        lock.release()
    monkeypatch.setattr(config, "load_env", lambda: {})
    lock = RunLock(os.path.join(sm.out_dir(f), ".lock")).acquire("test")
    try:
        with pytest.raises(mp.StoreBusy):
            rp(f, kite, apply=apply)
        assert cli.main(["repair", "--folder", f] + flag) == 2
    finally:
        lock.release()
    assert kite.calls == []
    capsys.readouterr()


def test_repair_cli_exit_codes(clean, monkeypatch, capsys):
    f = clean
    monkeypatch.setattr(mp, "kite_client", lambda: FakeKite())
    assert cli.main(["repair", "--folder", f]) == 0                          # GEN1, BIG, MID: all repaired
    assert "would repair): 3 rows in 3 stocks" in capsys.readouterr().out
    fabricate(f)                                                             # + PTWO, refused
    assert cli.main(["repair", "--folder", f]) == 1
    monkeypatch.setattr(mp, "kite_client", lambda: (_ for _ in ()).throw(mp.SignInNeeded("no token")))
    assert cli.main(["repair", "--folder", f]) == 2


def test_repair_refuses_when_a_later_row_depends_on_the_repaired_day(done_copy):
    f = done_copy                                      # (e) MID's DAYS[N-1] borrows DAYS[N-2]'s conversion
    update_run(f)
    make_rows(f, [("MID", N - 2, "prov", f"{DAYS[N - 2]} 20:00:00"), ("MID", N - 1, "old_empty", ""),
                  ("BIG", N - 1, "prov", f"{DAYS[N - 1]} 20:00:00")])
    assert days_of(f, "MID").loc[DAYS[N - 1], "conversion_from"] == str(DAYS[N - 2])
    mid = os.path.join("equity_1min", "data", sid(f, "MID"), ms.DAYS_FILE)
    before = tree(f)
    res = rp(f, FakeKite(), apply=True)
    assert [s for s, _, _ in res["refused"]] == ["MID"]
    why = res["refused"][0][2]
    assert "rows not repaired would change (" in why and why.endswith(f") on {DAYS[N - 1]}")   # only the later row
    assert why.startswith("a row borrows a repaired date's conversion") and res["borrowing"] == [sid(f, "MID")]
    assert tree(f)[mid] == before[mid] and not os.path.isdir(os.path.join(res["backup"], sid(f, "MID")))
    assert [s for s, _, _ in res["repaired"]] == ["BIG"]


def test_repair_keeps_preexisting_differences(clean):
    f = clean                                          # (d) BIG's stored rows differ from a fresh derive
    path = ms.stock_dir(f, sid(f, "BIG"))
    stored = ms.read_days(path)
    stored.loc[10, "note"] = "written by an older rule"
    stored.loc[11, "vol_ratio"] = 1.2345
    ms.write_days(stored, os.path.join(path, ms.DAYS_FILE))
    before = ms.read_days(path)
    res = rp(f, FakeKite(), apply=True)
    assert "BIG" in [x for x, _, _ in res["repaired"]] and not res["refused"]
    assert [(x, cols) for x, _, cols in res["preexisting"]] == [("BIG", ["vol_ratio", "note"])]
    assert ("Pre-existing re-derive differences (not fixed; those rows stay as stored): 1 stocks; columns: "
            "vol_ratio (1), note (1)") in res["report"]
    after = ms.read_days(path)
    keep = after["date"] != DAYS[N - 1]
    pd.testing.assert_frame_equal(after[keep].reset_index(drop=True), before[keep].reset_index(drop=True),
                                  check_exact=True)
    assert after.loc[10, "note"] == "written by an older rule" and after.loc[11, "vol_ratio"] == 1.2345


def test_repaired_rows_match_what_update_stores(done_copy):
    """(1) BIG updated day by day with final candles (T = N-1, then N) is the reference; the same world with those
    two rows stored provisional, repaired, gives the same rows (but fetched_at / run_id)."""
    f = done_copy
    extend(f)
    rollback(f, "BIG", N - 2)
    md.update(f, kite=FakeKite(last_day=N), speed=FAST, now=morning_after(N - 1), log=q)
    md.update(f, kite=FakeKite(last_day=N), speed=FAST, now=MORNING_AFTER, log=q)
    ref = days_of(f, "BIG")
    assert list(ref.index[-2:]) == [DAYS[N - 1], DAYS[N]]
    s, path = sid(f, "BIG"), ms.stock_dir(f, sid(f, "BIG"))
    calc = md.Calc(f, mp.Inputs(f, bhav=False))
    stored = ms.read_days(path)
    x = stored[md.INPUT_COLUMNS].copy()
    for c in md.FIELDS:
        x[f"kite_{c}"] = x[f"kite_{c}"].astype("float64")
    for d in (DAYS[N - 1], DAYS[N]):                   # as the old update stored them: provisional, that evening
        i = int(np.flatnonzero(x["date"] == d)[0])
        x.at[i, "kite_close"] = x.at[i, "minute_close"]
        x.at[i, "kite_volume"] = x.at[i, "minute_volume"]
        x.at[i, "fetched_at"] = f"{d} 20:00:00"
    ms.write_days(mr.splice(stored, mr.repaired_rows(calc, s, x, [DAYS[N - 1], DAYS[N]], calc.bhav_of(s))),
                  os.path.join(path, ms.DAYS_FILE))
    assert days_of(f, "BIG").loc[DAYS[N], "kite_close"] != ref.loc[DAYS[N], "kite_close"]
    res = rp(f, FakeKite(last_day=N), apply=True)
    assert [x for x, _, _ in res["repaired"]] == ["BIG"] and not res["refused"]
    got = days_of(f, "BIG")
    cols = [c for c in ref.columns if c not in ("fetched_at", "run_id")]
    pd.testing.assert_frame_equal(got[cols], ref[cols], check_exact=True)


def shifting_derive(monkeypatch, f, sym, repaired, rows):
    """Wrap Calc.derive: when the inputs carry Kite's final candle on DAYS[repaired] for sym, minute_volume_scale and
    note change on rows (a mask function of the dates): the hindsight drift of levels() on a whole-history derive."""
    orig = md.Calc.derive
    target, final = sid(f, sym), kite_day(sym, repaired)["close"]

    def derive(self, s, inputs, bhav=False):
        out = orig(self, s, inputs, bhav)
        k = inputs.loc[inputs["date"] == DAYS[repaired], "kite_close"]
        if s == target and len(k) and k.iloc[0] == final:
            mask = rows(out["date"])
            out.loc[mask, "minute_volume_scale"] = 1.5
            out.loc[mask, "note"] = "shifted by hindsight"
        return out

    monkeypatch.setattr(md.Calc, "derive", derive)


def test_repair_writes_a_stock_with_drift_on_earlier_rows_only(clean, monkeypatch):
    f = clean                                          # BIG: earlier rows shift, no row borrows N-1
    shifting_derive(monkeypatch, f, "BIG", N - 1, lambda d: (d >= DAYS[N - 12]) & (d < DAYS[N - 1]))
    path = ms.stock_dir(f, sid(f, "BIG"))
    before = ms.read_days(path)
    res = rp(f, FakeKite(), apply=True)
    assert "BIG" in [x for x, _, _ in res["repaired"]] and not res["refused"] and res["borrowing"] == []
    assert [(x, cols) for x, _, cols in res["drift"]] == [("BIG", ["minute_volume_scale", "note"])]
    text = res["report"]
    assert "Fresh-derive drift on earlier rows (not written): 1 stocks; columns: minute_volume_scale (1), note (1)" \
        in text and "  their repaired rows: 1; checks before -> after:" in text and "  their ratio change: " in text
    after = ms.read_days(path)
    keep = after["date"] != DAYS[N - 1]
    pd.testing.assert_frame_equal(after[keep].reset_index(drop=True), before[keep].reset_index(drop=True),
                                  check_exact=True)                      # earlier rows exactly as stored
    assert not after["note"].str.contains("hindsight").any()
    assert after.loc[~keep, "kite_close"].iloc[0] == kite_day("BIG", N - 1)["close"]


def test_repair_refuses_a_later_row_change_without_borrowing(done_copy, monkeypatch):
    f = done_copy                                      # MID repaired on N-2; its N-1 row would change
    update_run(f)
    make_rows(f, [("MID", N - 2, "prov", f"{DAYS[N - 2]} 20:00:00")])
    shifting_derive(monkeypatch, f, "MID", N - 2, lambda d: d == DAYS[N - 1])
    mid = os.path.join("equity_1min", "data", sid(f, "MID"), ms.DAYS_FILE)
    before = tree(f)
    res = rp(f, FakeKite(), apply=True)
    assert [x for x, _, _ in res["refused"]] == ["MID"] and res["borrowing"] == [] and res["drift"] == []
    why = res["refused"][0][2]
    assert why == f"a later row would change: rows not repaired would change (minute_volume_scale, note) on {DAYS[N - 1]}"
    assert "(0 with a row borrowing a repaired date's conversion, 1 with a later row changed)" in res["report"]
    assert tree(f)[mid] == before[mid]


def test_repair_reports_the_stored_token_of_a_stock_not_matched(clean):
    f = clean

    class Renamed(FakeKite):
        def instruments(self, exchange):
            out = super().instruments(exchange)
            return [{**i, "tradingsymbol": "BIGNEW"} if i["tradingsymbol"] == "BIG" else i for i in out]

    res = rp(f, Renamed())
    why = {x: w for x, _, w in res["skipped"]}["BIG"]
    assert why.startswith("NO_TOKEN in Kite's list") and "stored token 101 is now listed as BIGNEW (BIG LIMITED, NSE)" in why

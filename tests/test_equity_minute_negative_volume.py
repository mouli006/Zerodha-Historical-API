"""Minute bars with a negative volume (docs/dev/equity_minute_negative_volume.md): counted as 0 in the day aggregates
(NEGATIVE_VOLUME, neg_volume_bars, a warning line), the stored bars unchanged, and equity_minute.py
fix-negative-volume for the rows stored before. test_equity_minute_download.py's synthetic world and fake Kite; no
network, .env never read (config.load_env stubbed)."""
import os
from datetime import datetime, time as dtime

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import equity_minute as cli
from backfill.equity import minute_download as md
from backfill.equity import minute_negvol as mn
from backfill.equity import minute_plan as mp
from backfill.equity import minute_repair as mr
from backfill.equity import minute_store as ms
from backfill.equity import security_master as sm
from backfill.locks import RunLock
from test_equity_minute_download import (DAYS, FAST, MORNING_AFTER, N, FakeKite, days_of, done_copy,  # noqa: F401
                                         downloaded, extend, no_network, planned, q, sid, world)
from test_equity_minute_finished_days import tree

INT64_MIN = int(np.iinfo(np.int64).min)
U64 = 2 ** 64                    # Kite sends the garbage as an unsigned 64-bit value: 2^63 -> INT64_MIN, 2^64 - 5 -> -5


class NegKite(FakeKite):
    """FakeKite whose minute bar k of (symbol, day index) carries the given volume (as Kite sends it)."""

    def __init__(self, neg=None, **kw):
        super().__init__(**kw)
        self.neg = neg or {}

    def historical_data(self, token, start, end, interval, continuous=False, oi=False):
        out = super().historical_data(token, start, end, interval, continuous, oi)
        if interval != "minute":
            return out
        sym, seen, res = self.by_token[token], {}, []
        for c in out:
            d = c["date"].date()
            k = seen[d] = seen.get(d, -1) + 1
            v = self.neg.get((sym, DAYS.index(d) if d in DAYS else None), {}).get(k)
            res.append({**c, "volume": v} if v is not None else c)
        return res


def logger(logs):
    return lambda *a, **k: logs.append(" ".join(map(str, a)))


def bars_of(f, sym, i):
    t = pq.read_table(os.path.join(ms.stock_dir(f, sid(f, sym)), "2015.parquet")).to_pandas()
    return t[t["ts"].dt.date == DAYS[i]].sort_values("ts").reset_index(drop=True)


# ---- Part B: the day aggregates ----

def test_minute_aggregates_count_negative_bars_without_overflow():
    def bar(d, h, v):
        return {"ts": pd.Timestamp(datetime.combine(DAYS[d], dtime(h, 0))).tz_localize(ms.TZ), "open": 10.0,
                "high": 11.0, "low": 9.0, "close": 10.0, "volume": v}
    m = pd.DataFrame([bar(40, 9, 100), bar(40, 10, INT64_MIN), bar(40, 11, -5), bar(40, 12, 50),
                      bar(41, 9, INT64_MIN), bar(41, 10, INT64_MIN), bar(41, 11, 7), bar(42, 9, 3)])
    m["volume"] = m["volume"].astype("int64")
    agg = md.minute_aggregates(m.assign(run_id="r", fetched_at="t"))
    assert (agg[DAYS[40]]["minute_volume"], agg[DAYS[40]]["neg_volume_bars"]) == (150, 2)
    assert (agg[DAYS[41]]["minute_volume"], agg[DAYS[41]]["neg_volume_bars"]) == (7, 2)   # two INT64_MIN: no overflow
    assert (agg[DAYS[42]]["minute_volume"], agg[DAYS[42]]["neg_volume_bars"]) == (3, 0)
    assert agg[DAYS[40]]["minute_rows"] == 4
    assert m["volume"].iloc[1] == INT64_MIN                          # the frame itself is not changed


def test_build_labels_the_day_and_keeps_the_bars(world):
    f = world
    kite = NegKite(neg={("BIG", 40): {0: 2 ** 63, 2: U64 - 5}, ("BIG", 41): {0: 2 ** 63, 2: 2 ** 63},
                        ("MID", 42): {1: U64 - 3}})
    logs = []
    md.download(f, kite=kite, speed=FAST, workers=1, log=logger(logs))
    b40 = bars_of(f, "BIG", 40)
    assert list(b40["volume"].iloc[[0, 2]]) == [INT64_MIN, -5]       # stored as Kite sent them (int64)
    assert list(bars_of(f, "BIG", 41)["volume"].iloc[[0, 2]]) == [INT64_MIN, INT64_MIN]
    big, mid = days_of(f, "BIG"), days_of(f, "MID")
    r40, r41 = big.loc[DAYS[40]], big.loc[DAYS[41]]
    assert r40["minute_volume"] == b40["volume"].iloc[1] and r40["neg_volume_bars"] == 2
    assert r41["minute_volume"] == bars_of(f, "BIG", 41)["volume"].iloc[1] and r41["neg_volume_bars"] == 2
    # one bar of three left: below half the day volume -> VOLUME_DIFF on the clean total
    assert r40["check"] == "VOLUME_DIFF|NEGATIVE_VOLUME" and r41["check"] == "VOLUME_DIFF|NEGATIVE_VOLUME"
    assert r40["note"] == "negative volume on 2 minute bar(s), counted as 0"
    r42 = mid.loc[DAYS[42]]
    assert r42["check"] == "NEGATIVE_VOLUME" and r42["neg_volume_bars"] == 1      # -3 on one bar: volume still fine
    assert (big.drop(index=[DAYS[40], DAYS[41]])["neg_volume_bars"] == 0).all()
    assert not big.drop(index=[DAYS[40], DAYS[41]])["check"].str.contains("NEGATIVE").any()
    warn = [x for x in logs if "WARNING" in x]
    assert len(warn) == 3 and f"WARNING BIG {DAYS[40]}: 2 minute bar(s) with a negative volume" in warn[0] + warn[1]
    m = ms.load_minute("BIG", DAYS[40], DAYS[40], prices="kite", folder=f)
    assert m.attrs["clean"]["volume_set_zero"] == 2 and m["volume"].min() == 0
    summary = md.rebuild_checks(f, {})
    assert summary["checks"]["NEGATIVE_VOLUME"] == 3
    res = md.checks(f, log=q)                                       # derived from a stored column: checks keeps it
    assert not res["refused"] and not res["changed"]


def test_update_new_day_with_a_negative_bar(done_copy):
    f = done_copy
    extend(f)
    before = days_of(f, "BIG")
    kite = NegKite(last_day=N, neg={("BIG", N): {1: U64 - 7}})
    logs = []
    res = md.update(f, kite=kite, speed=FAST, workers=1, now=MORNING_AFTER, log=logger(logs))
    big = days_of(f, "BIG")
    pd.testing.assert_frame_equal(big.iloc[:-1], before)                         # stored days unchanged
    row = big.loc[DAYS[N]]
    assert row["check"] == "NEGATIVE_VOLUME" and row["neg_volume_bars"] == 1
    assert row["minute_volume"] == bars_of(f, "BIG", N)["volume"].clip(lower=0).sum()
    assert list(bars_of(f, "BIG", N)["volume"])[1] == -7
    assert res["stats"]["negative_volume_days"] == 1
    assert [x for x in logs if "WARNING" in x] == [
        f"  WARNING BIG {DAYS[N]}: 1 minute bar(s) with a negative volume (stored unchanged, counted as 0; "
        "NEGATIVE_VOLUME)"]
    assert any(x.startswith("Days with minute bars of a negative volume") and x.endswith(": 1") for x in logs)
    assert not days_of(f, "MID")["check"].str.contains("NEGATIVE").any()


def strip_column(f, sym):
    """days.parquet as stored before the neg_volume_bars column existed."""
    p = os.path.join(ms.stock_dir(f, sid(f, sym)), ms.DAYS_FILE)
    t = pq.read_table(p)
    pq.write_table(t.drop(["neg_volume_bars"]), p, compression="zstd")
    return p


def test_old_days_file_without_the_column(done_copy):
    f = done_copy
    p = strip_column(f, "BIG")
    days = ms.read_days(os.path.dirname(p))
    assert list(days.columns) == md.DAY_COLUMNS and (days["neg_volume_bars"] == 0).all()
    assert days["neg_volume_bars"].dtype == "int64"
    res = md.checks(f, log=q)
    assert not res["refused"] and not res["changed"]
    assert mr.find_affected(f)[1]["provisional"] >= 0                           # repair's selection reads it
    raw = pq.read_table(p).to_pandas()
    assert "neg_volume_bars" not in raw                                         # reading never writes
    extend(f)
    md.update(f, kite=FakeKite(last_day=N), speed=FAST, workers=1, now=MORNING_AFTER, log=q)
    after = pq.read_table(p).to_pandas()
    assert list(after.columns) == md.DAY_COLUMNS and (after["neg_volume_bars"] == 0).all()


def test_read_days_leaves_other_day_files_alone(tmp_path):
    path = tmp_path / "S000001"
    path.mkdir()
    df = pd.DataFrame({"date": [DAYS[0]], "icici_close": [1.0], "minute_volume": [5.0], "check": ["OK"]})
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), str(path / ms.DAYS_FILE))
    assert list(ms.read_days(str(path)).columns) == ["date", "icici_close", "minute_volume", "check"]


# ---- Part C: fix-negative-volume ----

def set_volumes(f, sym, changes):
    """Write volumes into the stored bars of a stock: {(day index, bar k): volume} (the test's own synthetic data)."""
    p = os.path.join(ms.stock_dir(f, sid(f, sym)), "2015.parquet")
    t = pq.read_table(p).to_pandas()
    for (i, k), v in changes.items():
        ix = t.index[t["ts"].dt.date == DAYS[i]][k]
        t.loc[ix, "volume"] = v
    t["volume"] = t["volume"].astype("int64")
    ms.write_minutes(t, p)


def make_old(f, sym, wrapped, clean_days=()):
    """days.parquet of a stock as the full download stored it before 21 Oct: no neg_volume_bars column, and on the
    `wrapped` day indexes minute_volume = the plain int64 sum of every bar (negatives in, wrapping); on clean_days
    the sum with negatives as 0 (a later build), still without the label."""
    s = sid(f, sym)
    path = ms.stock_dir(f, s)
    stored = ms.read_days(path)
    t = pq.read_table(os.path.join(path, "2015.parquet")).to_pandas()
    inputs = stored[md.INPUT_COLUMNS].copy()
    inputs["minute_volume"] = inputs["minute_volume"].astype("float64")
    pos = {d: i for i, d in enumerate(inputs["date"])}
    clean = {}
    for i in wrapped:
        v = t.loc[t["ts"].dt.date == DAYS[i], "volume"].to_numpy("int64")
        with np.errstate(over="ignore"):
            inputs.at[pos[DAYS[i]], "minute_volume"] = float(np.add.reduce(v, dtype="int64"))
        clean[DAYS[i]] = int(np.where(v < 0, 0, v).sum())
    for i in clean_days:
        v = t.loc[t["ts"].dt.date == DAYS[i], "volume"].to_numpy("int64")
        inputs.at[pos[DAYS[i]], "minute_volume"] = float(np.where(v < 0, 0, v).sum())
    inputs["neg_volume_bars"] = 0
    calc = md.Calc(f, mp.Inputs(f, bhav=False), write=False)
    old = calc.derive(s, inputs).drop(columns="neg_volume_bars")
    ms.write_days(old, os.path.join(path, ms.DAYS_FILE))
    return clean


@pytest.fixture
def broken(done_copy):
    """BIG: day 40 INT64_MIN + -5, day 41 one INT64_MIN (VOLUME_DIFF only from it), day 44 two INT64_MIN (the old sum
    wraps back to the third bar); MID: day 42 -3 (old sum), day 43 -9 already summed clean but not labelled."""
    f = done_copy
    set_volumes(f, "BIG", {(40, 0): INT64_MIN, (40, 2): -5, (41, 1): INT64_MIN, (44, 0): INT64_MIN,
                           (44, 1): INT64_MIN})
    set_volumes(f, "MID", {(42, 1): -3, (43, 0): -9})
    make_old(f, "BIG", [40, 41, 44])
    make_old(f, "MID", [42], clean_days=[43])
    return f


def data_tree(f):
    return tree(f, os.path.join("equity_1min", "data"))


def fix(f, **kw):
    kw.setdefault("log", q)
    return mn.fix(f, **kw)


def test_fix_dry_run_writes_nothing(broken):
    f = broken
    before = tree(f)
    logs = []
    res = fix(f, log=logger(logs))
    assert tree(f) == before
    assert res["counts"] == {"bars": 7, "int64_min": 4, "garbage": 0, "small": 3, "days": 5, "stocks": 2}
    assert len(res["rows"]) == 5 and res["changed_stocks"] == 2 and res["backup"] is None
    rows = {(x["symbol"], x["date"]): x for x in res["rows"]}
    r41 = rows[("BIG", str(DAYS[41]))]
    assert r41["old_check"] == "VOLUME_DIFF" and r41["new_check"] == "NEGATIVE_VOLUME"
    assert r41["old_minute_volume"] < 0 and r41["new_minute_volume"] > 0
    assert rows[("MID", str(DAYS[43]))]["old_minute_volume"] == rows[("MID", str(DAYS[43]))]["new_minute_volume"]
    assert res["before"].get("NEGATIVE_VOLUME", 0) == 0 and res["after"]["NEGATIVE_VOLUME"] == 5
    assert res["store_after"]["NEGATIVE_VOLUME"] == 5
    assert res["store_after"]["VOLUME_DIFF"] == res["store_before"]["VOLUME_DIFF"] - 1
    assert not res["drift"] and not res["mismatch"] and not res["no_row"]
    text = "\n".join(logs)
    assert "Minute bars with a negative volume: 7 on 5 stock-days in 2 stocks (4 INT64_MIN, 0 other garbage" in text
    assert f"BIG ({sid(f, 'BIG')}) {DAYS[41]}: 1 bar(s) (1 INT64_MIN)" in text and "dry run, nothing written" in text


def test_fix_apply_changes_only_the_affected_rows(broken):
    f = broken
    old = {s: ms.read_days(ms.stock_dir(f, sid(f, s))) for s in ("BIG", "MID", "PTWO")}
    raw_old = {s: open(os.path.join(ms.stock_dir(f, sid(f, s)), ms.DAYS_FILE), "rb").read() for s in ("BIG", "MID")}
    minutes = {k: v for k, v in data_tree(f).items() if not k.endswith(ms.DAYS_FILE)}
    ptwo = data_tree(f)
    res = fix(f, apply=True)
    assert len(res["rows"]) == 5
    after = data_tree(f)
    assert {k: v for k, v in after.items() if not k.endswith(ms.DAYS_FILE)} == minutes     # minute files untouched
    other = os.path.join(sid(f, "PTWO"), ms.DAYS_FILE)
    assert [v for k, v in after.items() if k.endswith(other)] == [v for k, v in ptwo.items() if k.endswith(other)]
    fixed = {"BIG": [40, 41, 44], "MID": [42, 43]}
    for s, idx in fixed.items():
        new = ms.read_days(ms.stock_dir(f, sid(f, s)))
        keep = ~new["date"].isin([DAYS[i] for i in idx])
        pd.testing.assert_frame_equal(new[keep], old[s][keep])                     # every other row as stored
        cols = [c for c in md.DAY_COLUMNS if c not in mn.FIXED]
        pd.testing.assert_frame_equal(new.loc[~keep, cols], old[s].loc[~keep, cols])
        assert (new.loc[~keep, "neg_volume_bars"] > 0).all()
        assert new.loc[~keep, "check"].str.contains("NEGATIVE_VOLUME").all()
        backup = os.path.join(res["backup"], sid(f, s), ms.DAYS_FILE)
        assert open(backup, "rb").read() == raw_old[s]
    big = ms.read_days(ms.stock_dir(f, sid(f, "BIG")))
    calc = md.Calc(f, mp.Inputs(f, bhav=False), write=False)
    pd.testing.assert_frame_equal(big, calc.derive(sid(f, "BIG"), big[md.INPUT_COLUMNS]))   # = a fresh derive
    assert big.set_index("date").loc[DAYS[44], "minute_volume"] == bars_of(f, "BIG", 44)["volume"].iloc[2]
    assert os.path.basename(res["backup"]).startswith("negvol_")
    csv = pd.read_csv(os.path.join(res["backup"], "negvol.csv"))
    assert list(csv.columns) == mn.NEGVOL_COLUMNS and len(csv) == 5
    summary = md.rebuild_checks(f, {})
    assert summary["checks"]["NEGATIVE_VOLUME"] == 5
    again = fix(f)                                                               # re-runnable: nothing left
    assert again["rows"] == [] and again["counts"]["days"] == 5
    assert not md.checks(f, log=q)["refused"]


def test_fix_error_restores_every_file(broken, monkeypatch):
    f = broken
    before = data_tree(f)
    calls = []

    def flaky(tmp, path):
        calls.append(path)
        if len(calls) == 2:
            raise OSError("disk went away")
        os.replace(tmp, path)
    monkeypatch.setattr(mn, "_replace", flaky)
    with pytest.raises(md.ApplyFailed, match="restored from"):
        fix(f, apply=True)
    assert data_tree(f) == before
    assert not [k for k in before if k.endswith(".tmp")]
    monkeypatch.setattr(mn, "_replace", lambda tmp, path: (_ for _ in ()).throw(OSError("still broken")))
    assert cli.main(["fix-negative-volume", "--folder", f, "--apply"]) == 1
    assert data_tree(f) == before


@pytest.mark.parametrize("apply", [False, True])
def test_fix_refusals(broken, capsys, apply):
    f = broken
    flag = ["--apply"] if apply else []
    before = data_tree(f)
    lock = RunLock(mp.lock_path(f)).acquire("equity_minute.py update")
    try:
        with pytest.raises(mp.LockBusy):
            fix(f, apply=apply)
        assert cli.main(["fix-negative-volume", "--folder", f] + flag) == 2
    finally:
        lock.release()
    lock = RunLock(os.path.join(sm.out_dir(f), ".lock")).acquire("equity_update.py")
    try:
        with pytest.raises(mp.StoreBusy):
            fix(f, apply=apply)
        assert cli.main(["fix-negative-volume", "--folder", f] + flag) == 2
    finally:
        lock.release()
    assert data_tree(f) == before
    capsys.readouterr()


def test_fix_drift_is_listed_not_written(broken, monkeypatch):
    f = broken
    real = md.Calc.derive

    def shifted(self, s, inputs, bhav=False):                     # a level that moves with the fixed volume
        d = real(self, s, inputs, bhav)
        if s == sid(f, "BIG"):
            d.loc[d.index[:5], "minute_volume_scale"] = 1.0 + inputs["minute_volume"].clip(lower=0).sum() / 1e12
        return d
    monkeypatch.setattr(md.Calc, "derive", shifted)
    old = ms.read_days(ms.stock_dir(f, sid(f, "BIG")))
    res = fix(f, apply=True)
    drift = {s: (cols, when) for s, _, cols, when in res["drift"]}
    assert drift["BIG"][0] == ["minute_volume_scale"] and drift["BIG"][1] == list(old["date"][:5])
    new = ms.read_days(ms.stock_dir(f, sid(f, "BIG")))
    assert (new["minute_volume_scale"] == old["minute_volume_scale"]).all()        # not written
    assert "Fresh-derive drift (not written" in res["report"]


def test_fix_cli(broken, capsys):
    f = broken
    assert cli.main(["fix-negative-volume", "--folder", f]) == 0
    out = capsys.readouterr().out
    assert "Rows to fix: 5 stock-days in 2 stocks (dry run, nothing written)" in out
    assert cli.main(["fix-negative-volume", "--folder", f, "--apply"]) == 0
    assert "Backup:" in capsys.readouterr().out
    assert cli.main(["fix-negative-volume", "--folder", f]) == 0
    assert "Rows to fix: 0 stock-days" in capsys.readouterr().out


def test_fix_nothing_found(done_copy):
    res = fix(done_copy, apply=True)
    assert res["counts"]["bars"] == 0 and res["rows"] == [] and res["backup"] is None
    assert not os.path.isdir(os.path.join(mp.out_dir(done_copy), "archive")) or \
        not [n for n in os.listdir(os.path.join(mp.out_dir(done_copy), "archive")) if n.startswith("negvol_")]

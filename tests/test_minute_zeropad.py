"""equity_minute.py fix-zero-padding (docs/dev/equity_minute_zero_padding.md): the days.parquet rows whose aggregates
still count Kite's all-zero padding bars, fixed from the stored bars with the current code; the stored bars, the
conversion columns, minute_volume and neg_volume_bars never written. test_equity_minute_download.py's synthetic world
and fake Kite; no network, .env never read (config.load_env stubbed)."""
import os
import re
import shutil
from datetime import datetime, time as dtime

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest

import config
import equity_minute as cli
from backfill import fetch
from backfill.equity import minute_download as md
from backfill.equity import minute_negvol as mn
from backfill.equity import minute_plan as mp
from backfill.equity import minute_repair as mr
from backfill.equity import minute_store as ms
from backfill.equity import minute_zeropad as mz
from backfill.equity import security_master as sm
from backfill.locks import RunLock
from test_equity_minute_download import (DAYS, FAST, MORNING_AFTER, N, FakeKite, FastStop, done_copy,  # noqa: F401
                                         downloaded, extend, no_network, planned, q, sid)
from test_equity_minute_finished_days import tree

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# (symbol, day index): the times of the all-zero bars Kite adds, or "all" (every bar of the day all-zero)
PAD = {("BIG", 40): [dtime(9, 14), dtime(13, 0)], ("BIG", 45): [dtime(13, 0)], ("PTWO", 41): [dtime(15, 30)],
       ("MID", 42): "all", ("GEN2", 44): [dtime(13, 0)], ("GEN3", 45): [dtime(10, 0), dtime(10, 1)]}
FRESH = ("BIG", "PTWO", "MID")            # stocks whose fixed days.parquet must equal the fresh download's


def zero_bar(d, t):
    return {"date": datetime.combine(d, t).replace(tzinfo=mp.IST), "open": 0.0, "high": 0.0, "low": 0.0,
            "close": 0.0, "volume": 0}


class ZeroKite(FakeKite):
    """FakeKite that pads chosen days with all-zero minute bars (PAD)."""

    def historical_data(self, token, start, end, interval, continuous=False, oi=False):
        out = super().historical_data(token, start, end, interval, continuous, oi)
        if interval != "minute":
            return out
        sym, res, seen = self.by_token[token], [], set()
        for c in out:
            d = c["date"].date()
            spec = PAD.get((sym, DAYS.index(d) if d in DAYS else None))
            if spec == "all":
                res.append(zero_bar(d, c["date"].time()))
                continue
            res.append(c)
            if spec and d not in seen:
                seen.add(d)
                res += [zero_bar(d, t) for t in spec]
        return sorted(res, key=lambda c: c["date"])


@pytest.fixture(scope="module")
def padded(planned, tmp_path_factory):
    """planned + a full download from ZeroKite with the current code (the zero bars stored, not counted), and the
    fresh days.parquet frames of every stock."""
    f = str(tmp_path_factory.mktemp("zeropad") / "data")
    shutil.copytree(planned, f)
    mpatch = pytest.MonkeyPatch()
    mpatch.setattr(config, "load_env", lambda: {})
    mpatch.setattr(fetch, "STOP", FastStop())
    md.download(f, kite=ZeroKite(), speed=FAST, workers=1, log=q)
    mpatch.undo()
    fresh = {s: ms.read_days(ms.stock_dir(f, sid(f, s))) for s in ("BIG", "PTWO", "MID", "GEN1", "GEN2", "GEN3")}
    return f, fresh


def bars_path(f, sym):
    return os.path.join(ms.stock_dir(f, sid(f, sym)), "2015.parquet")


def edit_bar(f, sym, i, k, **values):
    """Change bar k of day i in the stored bars (the test's own synthetic data)."""
    p = bars_path(f, sym)
    t = pq.read_table(p).to_pandas()
    ix = t.index[t["ts"].dt.date == DAYS[i]][k]
    for c, v in values.items():
        t.loc[ix, c] = v
    t["volume"] = t["volume"].astype("int64")
    ms.write_minutes(t, p)


def make_old(f, sym, idx, unconfirmed=()):
    """days.parquet of a stock as the full download stored it before b52ca35: on the `idx` days the aggregates of
    every stored bar (zero bars in; a negative volume as 0, as fix-negative-volume left them); unconfirmed days get
    CANDLE_UNCONFIRMED."""
    s = sid(f, sym)
    path = ms.stock_dir(f, s)
    stored = ms.read_days(path)
    t = pq.read_table(os.path.join(path, "2015.parquet")).to_pandas().sort_values("ts")
    inputs = stored[md.INPUT_COLUMNS].copy()
    pos = {d: i for i, d in enumerate(inputs["date"])}
    for i in idx:
        b = t[t["ts"].dt.date == DAYS[i]]
        v = b["volume"].to_numpy("int64")
        vals = {"minute_rows": len(b), "first_bar": b["ts"].iloc[0].strftime("%H:%M"),
                "last_bar": b["ts"].iloc[-1].strftime("%H:%M"), "minute_open": b["open"].iloc[0],
                "minute_high": b["high"].max(), "minute_low": b["low"].min(), "minute_close": b["close"].iloc[-1],
                "minute_volume": float(np.where(v < 0, 0, v).sum()), "neg_volume_bars": int((v < 0).sum())}
        for c, x in vals.items():
            inputs.at[pos[DAYS[i]], c] = x
    old = md.Calc(f, mp.Inputs(f, bhav=False), write=False).derive(s, inputs)
    marks = {DAYS[i] for i in unconfirmed}
    old["check"] = [md.add_check(c, md.UNCONFIRMED) if d in marks else c for d, c in zip(old["date"], old["check"])]
    ms.write_days(old, os.path.join(path, ms.DAYS_FILE))
    return old


@pytest.fixture
def broken(padded, tmp_path):
    """padded, with the days.parquet rows of the PAD days built the old way. Also: GEN1 day 43 has a bar with low 0
    and real other fields (OTHER); GEN2 day 44 a bar of negative volume besides its zero bar; GEN3 day 45 is
    CANDLE_UNCONFIRMED; EMPTYCO day 40 keeps its real HIGHLOW_DIFF (no zero bar)."""
    f = str(tmp_path / "data")
    shutil.copytree(padded[0], f)
    edit_bar(f, "GEN1", 43, 1, low=0.0)
    edit_bar(f, "GEN2", 44, 0, volume=-5)
    make_old(f, "BIG", [40, 45])
    make_old(f, "PTWO", [41])
    make_old(f, "MID", [42])
    make_old(f, "GEN1", [43])
    make_old(f, "GEN2", [44])
    make_old(f, "GEN3", [45], unconfirmed=[45])
    return f


def days(f, sym):
    return ms.read_days(ms.stock_dir(f, sid(f, sym)))


def row(f, sym, i):
    d = days(f, sym)
    return d[d["date"] == DAYS[i]].iloc[0]


def fix(f, **kw):
    kw.setdefault("log", q)
    return mz.fix(f, **kw)


def data_tree(f):
    return tree(f, os.path.join("equity_1min", "data"))


def outside_report(f):
    return {k: v for k, v in tree(f).items() if not k.startswith(os.path.join("equity_1min", "zeropad"))}


# ---- the old rows and the classification ----

def test_old_rows_count_the_zero_bars(broken):
    f = broken
    r = row(f, "BIG", 40)
    assert r["minute_low"] == 0 and r["minute_open"] == 0 and r["minute_rows"] == 5 and r["first_bar"] == "09:14"
    assert r["check"] == "HIGHLOW_DIFF"
    m = row(f, "MID", 42)
    assert m["minute_rows"] == 3 and m["minute_volume"] == 0 and m["check"] == "HIGHLOW_DIFF|CLOSE_DIFF|VOLUME_DIFF"
    assert row(f, "GEN2", 44)["check"] == "HIGHLOW_DIFF|NEGATIVE_VOLUME"
    assert row(f, "GEN3", 45)["check"] == "HIGHLOW_DIFF|CANDLE_UNCONFIRMED"
    assert row(f, "EMPTYCO", 40)["check"] == "HIGHLOW_DIFF" and row(f, "EMPTYCO", 40)["minute_low"] > 0


def test_classify():
    def bars(*rows):
        return pd.DataFrame([{"ts": pd.Timestamp("2015-03-02 09:15", tz=ms.TZ), "open": o, "high": h, "low": lo,
                              "close": c, "volume": v} for o, h, lo, c, v in rows])
    assert mz.classify(bars((1, 2, 1, 2, 5), (0, 0, 0, 0, 0)))[:3] == ("PADDING", 1, 1)
    assert mz.classify(bars((0, 0, 0, 0, 0), (0, 0, 0, 0, 0)))[:3] == ("PADDING", 2, 2)
    kind, _, _, why = mz.classify(bars((1, 2, 1, 2, 5), (0, 95.85, 0, 95.85, 0)))
    assert kind == "OTHER" and "non-zero field" in why and "h=95.85" in why
    assert mz.classify(bars((0, 0, 0, 0, 7)))[0] == "OTHER"                  # zero prices, volume 7: not padding
    assert mz.classify(bars((1, 2, 1, 2, 5)))[0] == "OTHER"
    assert mz.classify(bars())[0] == "OTHER"


# ---- Tests 1-6: the fixed rows ----

def test_fixed_rows_equal_a_fresh_download(broken, padded):
    f, fresh = broken, padded[1]
    res = fix(f, apply=True)
    assert {(x["symbol"], x["date"]) for x in res["rows"]} == {
        ("BIG", str(DAYS[40])), ("BIG", str(DAYS[45])), ("PTWO", str(DAYS[41])), ("MID", str(DAYS[42])),
        ("GEN2", str(DAYS[44])), ("GEN3", str(DAYS[45]))}
    for s in FRESH:
        new, want = days(f, s), fresh[s]
        cols = [c for c in md.DAY_COLUMNS if c != "minute_volume"]
        pd.testing.assert_frame_equal(new[cols], want[cols])
        mv = new["minute_volume"].to_numpy(float)
        wv = want["minute_volume"].to_numpy(float)
        only = (new["date"] == DAYS[42]).to_numpy() if s == "MID" else np.zeros(len(new), bool)
        assert np.array_equal(mv[~only], wv[~only], equal_nan=True)
        if s == "MID":                     # the one listed exception: 0 kept where a fresh download stores none
            assert mv[only][0] == 0 and np.isnan(wv[only][0])
    m = row(f, "MID", 42)
    assert m["minute_rows"] == 0 and m["first_bar"] == "" and np.isnan(m["minute_low"]) and m["check"] == "MINUTE_EMPTY"


def test_zero_bar_highlow_diff_becomes_ok(broken):
    f = broken
    res = fix(f, apply=True)
    r = row(f, "BIG", 40)
    assert r["check"] == "OK" and r["minute_rows"] == 3 and r["first_bar"] == "09:15" and r["minute_low"] > 0
    assert row(f, "PTWO", 41)["check"] == "OK" and row(f, "PTWO", 41)["last_bar"] == "15:29"
    assert res["before"]["HIGHLOW_DIFF"] == 6 and res["after"].get("HIGHLOW_DIFF", 0) == 0
    assert res["store_after"]["HIGHLOW_DIFF"] == res["store_before"]["HIGHLOW_DIFF"] - 6


def test_real_highlow_diff_untouched(broken):
    f = broken
    old = days(f, "EMPTYCO")
    fix(f, apply=True)
    pd.testing.assert_frame_equal(days(f, "EMPTYCO"), old)
    assert row(f, "EMPTYCO", 40)["check"] == "HIGHLOW_DIFF"


def test_other_row_reported_not_changed(broken):
    f = broken
    old = days(f, "GEN1")
    res = fix(f, apply=True)
    assert [(x["symbol"], x["date"]) for x in res["other"]] == [("GEN1", str(DAYS[43]))]
    assert "non-zero field" in res["other"][0]["reason"]
    pd.testing.assert_frame_equal(days(f, "GEN1"), old)
    assert "8. OTHER rows (not changed): 1" in res["report"]


def test_negative_volume_kept(broken):
    f = broken
    old = row(f, "GEN2", 44)
    fix(f, apply=True)
    new = row(f, "GEN2", 44)
    assert new["check"] == "NEGATIVE_VOLUME" and new["neg_volume_bars"] == 1 == old["neg_volume_bars"]
    assert new["minute_volume"] == old["minute_volume"] and new["note"] == old["note"] == md.negative_note(1)
    assert new["minute_low"] > 0


def test_candle_unconfirmed_kept(broken):
    f = broken
    fix(f, apply=True)
    assert row(f, "GEN3", 45)["check"] == "CANDLE_UNCONFIRMED"
    res = md.checks(f, log=q)                                   # checks agrees: nothing to change, nothing refused
    assert not res["changed"] and not res["refused"]


# ---- Tests 7-11: what is written ----

def test_apply_changes_only_the_fixed_columns(broken):
    f = broken
    old = {s: days(f, s) for s in ("BIG", "PTWO", "MID", "GEN2", "GEN3")}
    minutes = {k: v for k, v in data_tree(f).items() if not k.endswith(ms.DAYS_FILE)}
    others = {k: v for k, v in data_tree(f).items()
              if k.endswith(ms.DAYS_FILE) and not any(sid(f, s) in k for s in old)}
    res = fix(f, apply=True)
    after = data_tree(f)
    assert {k: v for k, v in after.items() if not k.endswith(ms.DAYS_FILE)} == minutes     # bars never written
    assert {k: v for k, v in after.items() if k in others} == others                      # other stocks untouched
    fixed = {}
    for x in res["rows"]:
        fixed.setdefault(x["symbol"], []).append(x["date"])
    for s, o in old.items():
        new = days(f, s)
        keep = ~new["date"].astype(str).isin(fixed[s])
        pd.testing.assert_frame_equal(new[keep], o[keep])                                 # every other row
        cols = [c for c in md.DAY_COLUMNS if c not in mz.FIXED]                           # (b), (d), volume
        pd.testing.assert_frame_equal(new.loc[~keep, cols], o.loc[~keep, cols])
        assert list(new.dtypes) == list(o.dtypes)
    assert res["years_written"] == 0 and res["years_read"] == 6
    assert "minute_volume, neg_volume_bars and every conversion column" in res["report"]


def test_drift_is_listed_held_and_not_written(broken, monkeypatch):
    f = broken
    real = md.Calc.derive
    big = sid(f, "BIG")
    moved = [DAYS[i] for i in range(37, 41)]

    def shifted(self, s, inputs, bhav=False):                 # BIG's level moves when day 40 loses its zero bars
        d = real(self, s, inputs, bhav)
        if s == big and inputs.loc[inputs["date"] == DAYS[40], "minute_low"].iat[0] > 0:
            d.loc[d["date"].isin(moved), "minute_scale"] = 1.02
        return d
    monkeypatch.setattr(md.Calc, "derive", shifted)
    old = days(f, "BIG")
    res = fix(f, apply=True)
    assert [(x["symbol"], x["date"], x["reason"]) for x in res["held"]] == [("BIG", str(DAYS[40]), "SCALE_DRIFT")]
    assert ("BIG", str(DAYS[45])) in {(x["symbol"], x["date"]) for x in res["rows"]}       # the rest is fixed
    new = days(f, "BIG")
    pd.testing.assert_series_equal(new["minute_scale"], old["minute_scale"])               # not written
    pd.testing.assert_series_equal(new.iloc[old.index[old["date"] == DAYS[40]][0]], old.iloc[
        old.index[old["date"] == DAYS[40]][0]])                                            # held row unchanged
    [x] = res["drift"]
    assert (x["security_id"], x["from_date"], x["to_date"], x["rows"]) == (big, str(DAYS[37]), str(DAYS[40]), 4)
    assert (x["old_minute_scale"], x["new_minute_scale"]) == (1.0, 1.02) and x["columns"] == "minute_scale"
    assert x["price_change_pct"] == pytest.approx(2.0) and x["held_padding_rows_stock"] == 1
    assert x["padding_rows"] == 1 and x["other_rows_changed_stock"] == 3 and x["affects_loaded_prices"] == "Y"
    csv = pd.read_csv(os.path.join(res["report_dir"], "scale_drift.csv"))
    assert list(csv.columns) == mz.DRIFT_COLUMNS and len(csv) == 1
    assert "ALL AFFECT LOADED PRICES" in res["report"] and "the rows that cause it are held" in res["report"]
    assert not res["refused_after"]


def test_dry_run_writes_only_its_report(broken):
    f = broken
    before = tree(f)
    logs = []
    res = fix(f, log=lambda *a, **k: logs.append(" ".join(map(str, a))))
    assert outside_report(f) == before
    assert len(res["rows"]) == 6 and res["backup"] is None
    assert sorted(os.listdir(res["report_dir"])) == ["held_rows.csv", "other_rows.csv", "report.txt", "rows.csv",
                                                     "scale_drift.csv"]
    assert os.path.basename(res["report_dir"]).startswith("dryrun_")
    text = "\n".join(logs)
    assert "DRY RUN, nothing written to the store" in text and "to fix: 6 rows in 5 stocks" in text
    assert "Days of only zero bars: 1 fixed" in text and "HIGHLOW_DIFF|CLOSE_DIFF|VOLUME_DIFF -> MINUTE_EMPTY: 1" in text
    assert "Minute year files read 6, written 0" in text
    assert "(+1 rows, +1 days)" in text                       # MID's empty day: one more MINUTE_EMPTY_DAYS row


def test_apply_backup_and_failure_restores_everything(broken, monkeypatch):
    f = broken
    raw = {s: open(os.path.join(ms.stock_dir(f, sid(f, s)), ms.DAYS_FILE), "rb").read() for s in ("BIG", "MID")}
    before = data_tree(f)
    calls = []

    def flaky(tmp, path):
        calls.append(path)
        if len(calls) == 2:
            raise OSError("disk went away")
        os.replace(tmp, path)
    monkeypatch.setattr(mz, "_replace", flaky)
    with pytest.raises(md.ApplyFailed, match="restored from"):
        fix(f, apply=True)
    assert data_tree(f) == before
    monkeypatch.setattr(mz, "_replace", lambda tmp, path: (_ for _ in ()).throw(OSError("still broken")))
    assert cli.main(["fix-zero-padding", "--folder", f, "--apply"]) == 1
    assert data_tree(f) == before
    monkeypatch.undo()
    res = fix(f, apply=True)
    assert os.path.basename(res["backup"]).startswith("zeropad_")
    assert os.path.dirname(res["backup"]) == os.path.join(md.out_dir(f), "archive")
    for s, b in raw.items():
        assert open(os.path.join(res["backup"], sid(f, s), ms.DAYS_FILE), "rb").read() == b
    csv = pd.read_csv(os.path.join(res["backup"], "zeropad.csv"), keep_default_na=False)
    assert list(csv.columns) == mz.ROW_COLUMNS and len(csv) == 6
    big = csv[(csv["symbol"] == "BIG") & (csv["date"] == str(DAYS[40]))].iloc[0]
    assert (big["old_check"], big["new_check"], big["old_minute_rows"], big["new_minute_rows"]) == \
        ("HIGHLOW_DIFF", "OK", 5, 3)
    assert md.rebuild_checks(f, {})["checks"].get("HIGHLOW_DIFF", 0) == res["store_after"].get("HIGHLOW_DIFF", 0)


def test_second_run_finds_nothing(broken):
    f = broken
    fix(f, apply=True)
    again = fix(f, apply=True)
    assert again["rows"] == [] and again["held"] == [] and again["backup"] is None
    assert len(again["other"]) == 1                            # OTHER is reported again, never changed


# ---- Tests 12-13 ----

@pytest.mark.parametrize("apply", [False, True])
def test_refusals(broken, capsys, monkeypatch, tmp_path, apply):
    f = broken
    flag = ["--apply"] if apply else []
    before = tree(f)
    lock = RunLock(mp.lock_path(f)).acquire("equity_minute.py update")
    try:
        with pytest.raises(mp.LockBusy):
            fix(f, apply=apply)
        assert cli.main(["fix-zero-padding", "--folder", f] + flag) == 2
    finally:
        lock.release()
    lock = RunLock(os.path.join(sm.out_dir(f), ".lock")).acquire("equity_update.py")
    try:
        with pytest.raises(mp.StoreBusy):
            fix(f, apply=apply)
        assert cli.main(["fix-zero-padding", "--folder", f] + flag) == 2
    finally:
        lock.release()
    fno = tmp_path / "fno"
    monkeypatch.setattr(config, "load_env", lambda: {"KITE_DATA_DIR": str(fno)})
    lock = RunLock(str(fno / "NIFTY_FnO_1min" / ".lock")).acquire("nifty_daily.py")
    try:
        with pytest.raises(mp.FnoBusy):
            fix(f, apply=apply)
        assert cli.main(["fix-zero-padding", "--folder", f] + flag) == 2
    finally:
        lock.release()
    assert tree(f) == before
    capsys.readouterr()


def test_old_days_file_without_neg_volume_bars(broken):
    f = broken
    p = os.path.join(ms.stock_dir(f, sid(f, "BIG")), ms.DAYS_FILE)
    pq.write_table(pq.read_table(p).drop(["neg_volume_bars"]), p, compression="zstd")
    res = fix(f, apply=True)
    assert ("BIG", str(DAYS[40])) in {(x["symbol"], x["date"]) for x in res["rows"]}
    new = pq.read_table(p).to_pandas()
    assert list(new.columns) == md.DAY_COLUMNS and (new["neg_volume_bars"] == 0).all()
    assert row(f, "BIG", 40)["check"] == "OK"


# ---- Test 14: a kept minute_volume 0 acts as none ----

def as_nan(f, sym, i):
    """The stock's days.parquet with minute_volume NaN on day i (as a fresh download stores a day of no minutes)."""
    p = os.path.join(ms.stock_dir(f, sid(f, sym)), ms.DAYS_FILE)
    d = ms.read_days(os.path.dirname(p))
    d["minute_volume"] = d["minute_volume"].astype("float64")
    d.loc[d["date"] == DAYS[i], "minute_volume"] = np.nan
    ms.write_days(d, p)


def test_zero_volume_acts_as_none(broken, tmp_path):
    f = broken
    fix(f, apply=True)
    g = str(tmp_path / "nan")
    shutil.copytree(f, g)
    as_nan(g, "MID", 42)
    s = sid(f, "MID")
    zero, none = days(f, "MID"), days(g, "MID")
    assert row(f, "MID", 42)["minute_volume"] == 0 and np.isnan(row(g, "MID", 42)["minute_volume"])
    calc = md.Calc(f, mp.Inputs(f, bhav=False), write=False)
    a, b = calc.derive(s, zero[md.INPUT_COLUMNS]), calc.derive(s, none[md.INPUT_COLUMNS])
    cols = [c for c in md.DAY_COLUMNS if c != "minute_volume"]
    pd.testing.assert_frame_equal(a[cols], b[cols])                         # checks, vol_ratio, scales: the same
    num = a.select_dtypes("number").to_numpy(float)
    assert not np.isinf(num).any()
    bhav = calc.bhav_of(s)
    assert md.unfinished(zero[md.INPUT_COLUMNS], bhav) == md.unfinished(none[md.INPUT_COLUMNS], bhav)
    for prices in ms.PRICES:
        for clean in (True, False):
            x = ms.load_minute("MID", DAYS[40], DAYS[44], prices=prices, folder=f, clean=clean)
            y = ms.load_minute("MID", DAYS[40], DAYS[44], prices=prices, folder=g, clean=clean)
            pd.testing.assert_frame_equal(x, y)
            assert x.attrs == y.attrs
    for h in (f, g):
        res = md.checks(h, log=q)
        assert not res["changed"] and not res["refused"]
        assert mn.fix(h, log=q)["rows"] == []                                 # fix-negative-volume: nothing
    assert mr.find_affected(f) == mr.find_affected(g)
    for h in (f, g):                                                         # update: stored rows unchanged
        before = days(h, "MID")
        extend(h)
        md.update(h, kite=FakeKite(last_day=N), speed=FAST, workers=1, now=MORNING_AFTER, log=q)
        after = days(h, "MID")
        pd.testing.assert_frame_equal(after.iloc[:len(before)], before)
        assert after["date"].iat[-1] == DAYS[N]
    new = [c for c in cols if c not in ("fetched_at", "run_id")]              # the two runs' own stamps
    pd.testing.assert_frame_equal(days(f, "MID").iloc[-1:][new].reset_index(drop=True),
                                  days(g, "MID").iloc[-1:][new].reset_index(drop=True))


# ---- Test 15: minute_scale_measured is written, never read ----

def test_minute_scale_measured_is_never_read():
    assert "minute_scale_measured" not in md.INPUT_COLUMNS            # derive (update, repair, negvol, checks) input
    for name in os.listdir(os.path.join(ROOT, "backfill", "equity")):
        if not re.fullmatch(r"(minute_.*|icici_.*)\.py", name) or name == "minute_zeropad.py":
            continue
        with open(os.path.join(ROOT, "backfill", "equity", name), encoding="utf-8") as fh:
            uses = [ln.strip() for ln in fh if "minute_scale_measured" in ln]
        if name == "minute_download.py":       # DAY_COLUMNS and the one assignment in Calc.derive
            assert len(uses) == 2 and uses[1].startswith('d["minute_scale_measured"] =')
        else:
            assert uses == [], name


def test_minute_scale_measured_value_changes_nothing(done_copy, tmp_path):
    f = done_copy
    g = str(tmp_path / "garbage")
    shutil.copytree(f, g)
    p = os.path.join(ms.stock_dir(g, sid(g, "BIG")), ms.DAYS_FILE)
    d = ms.read_days(os.path.dirname(p))
    d["minute_scale_measured"] = 123.0
    ms.write_days(d, p)
    for prices in ms.PRICES:
        pd.testing.assert_frame_equal(ms.load_minute("BIG", prices=prices, folder=f),
                                      ms.load_minute("BIG", prices=prices, folder=g))
    calc = md.Calc(f, mp.Inputs(f, bhav=False), write=False)
    pd.testing.assert_frame_equal(calc.derive(sid(f, "BIG"), days(f, "BIG")[md.INPUT_COLUMNS]),
                                  calc.derive(sid(g, "BIG"), days(g, "BIG")[md.INPUT_COLUMNS]))
    for h in (f, g):
        extend(h)
        md.update(h, kite=FakeKite(last_day=N), speed=FAST, workers=1, now=MORNING_AFTER, log=q)
    a, b = days(f, "BIG"), days(g, "BIG")
    cols = [c for c in md.DAY_COLUMNS if c not in ("fetched_at", "run_id")]   # the two runs' own stamps
    pd.testing.assert_frame_equal(a.iloc[-1:][cols].reset_index(drop=True), b.iloc[-1:][cols].reset_index(drop=True))


def test_load_minute_unchanged_after_apply(broken):
    f = broken
    before = {(s, p): ms.load_minute(s, prices=p, folder=f) for s in ("BIG", "PTWO", "MID", "GEN2", "GEN3")
              for p in ms.PRICES}
    res = fix(f, apply=True)
    assert not res["held"]
    for (s, p), x in before.items():
        y = ms.load_minute(s, prices=p, folder=f)
        pd.testing.assert_frame_equal(x, y)
        assert x.attrs == y.attrs


# ---- Test 16: CLI ----

def test_cli(broken, capsys):
    f = broken
    assert cli.main(["fix-zero-padding", "--folder", f]) == 0
    out = capsys.readouterr().out
    assert "DRY RUN" in out and "to fix: 6 rows in 5 stocks" in out
    assert cli.main(["fix-zero-padding", "--folder", f, "--apply"]) == 0
    assert "Backup:" in capsys.readouterr().out
    assert cli.main(["fix-zero-padding", "--folder", f]) == 0
    assert "to fix: 0 rows in 0 stocks" in capsys.readouterr().out


def test_store_without_padding(done_copy):
    res = fix(done_copy, apply=True)
    assert res["rows"] == [] and res["other"] == [] and res["backup"] is None and res["years_read"] == 0
    archive = os.path.join(mp.out_dir(done_copy), "archive")
    assert not os.path.isdir(archive) or not [n for n in os.listdir(archive) if n.startswith("zeropad_")]

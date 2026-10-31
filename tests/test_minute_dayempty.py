"""P75: Calc.derive's DAY_EMPTY rule (a bhavcopy check, borrowing only from an OK same-factor day) and
equity_minute.py fix-dayempty (backfill/equity/minute_dayempty.py), on test_equity_minute_download.py's synthetic world
and fake Kite. No network; .env is never read (config.load_env stubbed)."""
import os
import shutil
from datetime import date

import numpy as np
import pandas as pd
import pytest

import equity_minute as cli
from backfill.equity import minute_dayempty as mde
from backfill.equity import minute_download as md
from backfill.equity import minute_plan as mp
from backfill.equity import minute_rescale as mrs
from backfill.equity import minute_store as ms
from backfill.equity import minute_zeropad as mz
from backfill.locks import RunLock
from test_equity_minute_download import (DAYS, EXTRA, FAST, MORNING_AFTER, N, SESSION, SPLIT_DAY, STEP_DAY,  # noqa: F401
                                         downloaded, extend, no_network, planned, q, sid)
from test_equity_minute_finished_days import EdgeKite, rollback, tree

CONV = ["ratio", "kite_multiplier", "kite_difference", "vol_ratio", "kind"]
PRICES = ["kite_open", "kite_high", "kite_low", "kite_close"]
FRIDAY = date(2015, 2, 27)                      # the Friday before the EXTRA Saturday session


@pytest.fixture
def done(downloaded, tmp_path):
    f = str(tmp_path / "done")
    shutil.copytree(downloaded[0], f)
    return f


def calc_of(f):
    return md.Calc(f, mp.Inputs(f, bhav=False), write=False)


def path_of(f, sym):
    return ms.stock_dir(f, sid(f, sym))


def stored(f, sym):
    return ms.read_days(path_of(f, sym))


def write(f, sym, df):
    ms.write_days(df, os.path.join(path_of(f, sym), ms.DAYS_FILE))


def at(df, day):
    return int(df.index[df["date"] == (DAYS[day] if isinstance(day, int) else day)][0])


def scaled(inputs, i, k):
    x = inputs.copy()
    for c in ("minute_open", "minute_high", "minute_low", "minute_close"):
        x.at[i, c] = x.at[i, c] * k
    return x


def corrupt(inputs, i, k=30.0):
    """Kite's day candle of row i on another scale (TIL 2018-11-06: close 2.80 for 300): a bad source day."""
    x = inputs.copy()
    for c in PRICES:
        x.at[i, c] = round(x.at[i, c] / k, 2)
    return x


def level(row, d):
    """The loaded level (median of loaded high / low / close / bhavcopy) of row i of frame d with its conversion."""
    agg = {f"minute_{k}": d.at[row, f"minute_{k}"] for k in ("high", "low", "close")}
    return mrs.loaded_level(agg, d.iloc[row], d.iloc[row])["level"]


def old_pick(d, i):
    """The day the rule before P75 borrowed from: nearest day with an own conversion, the same minute_scale and
    factor first, else any (crossing a factor change), whatever its check."""
    own = (d["kite_close"].notna() & d["bhav_close"].notna() & d["ratio"].notna()).to_numpy().copy()
    own[i] = False
    src = np.flatnonzero(own)
    scale, fac = d["minute_scale"].to_numpy(float), d["our_factor"].to_numpy(float)
    dates = np.array([x.toordinal() for x in d["date"]])
    same = src[(scale[src] == scale[i]) & (fac[src] == fac[i])]
    pool = same if len(same) else src
    return int(pool[np.argmin(np.abs(dates[pool] - dates[i]))])


def oldify(d):
    """A derived frame's DAY_EMPTY rows as the rule before P75 stored them (old borrow, no bhavcopy check)."""
    out = d.copy()
    for i in np.flatnonzero(out["check"].str.contains("DAY_EMPTY").to_numpy()):
        j = old_pick(d, i)
        for c in CONV:
            out.at[i, c] = d.at[j, c]
        out.at[i, "conversion_from"] = md.iso(d.at[j, "date"])
        keep = set(out.at[i, "check"].split("|")) - mde.LABELS
        out.at[i, "check"] = "|".join(c for c in md.CHECKS if c in keep)
        parts = [p for p in str(out.at[i, "note"]).split("; ") if p and p != md.NO_OK_SOURCE]
        out.at[i, "note"] = "; ".join(f"converted as {md.iso(d.at[j, 'date'])}" if p.startswith("converted as ")
                                      else p for p in parts)
    return out


# ---- Calc.derive: the bhavcopy check of a DAY_EMPTY row ----

def test_dayempty_row_checked_against_the_bhavcopy(done):
    calc, s = calc_of(done), stored(done, "BIG")
    i, inp, big = at(s, SESSION), s[md.INPUT_COLUMNS], sid(done, "BIG")
    assert calc.derive(big, inp).at[i, "check"] == "DAY_EMPTY"                      # Kite minutes = the bhavcopy
    # tolerance PRICE_REL x price + HALF_TICK (BIG's high ~1177: 0.61): +0.04% stays, +0.1% is off
    assert calc.derive(big, scaled(inp, i, 1.0004)).at[i, "check"] == "DAY_EMPTY"
    assert calc.derive(big, scaled(inp, i, 1.001)).at[i, "check"] == "DAY_EMPTY|HIGHLOW_DIFF"
    far = calc.derive(big, scaled(inp, i, 1.2))
    assert far.at[i, "check"] == "DAY_EMPTY|HIGHLOW_DIFF|CLOSE_DIFF"
    assert far.at[i, "note"] == f"converted as {far.at[i, 'conversion_from']}"
    low = calc.derive(big, scaled(inp, i, 0.8))
    assert low.at[i, "check"] == "DAY_EMPTY|HIGHLOW_DIFF|CLOSE_DIFF"
    auction = scaled(inp, i, 1.2)
    auction.at[i, "last_bar"] = "15:14"
    assert calc.derive(big, auction).at[i, "check"] == "DAY_EMPTY|HIGHLOW_DIFF|CLOSING_AUCTION"
    # every other row as before
    pd.testing.assert_frame_equal(far.drop(index=i), calc.derive(big, inp).drop(index=i))


def test_dayempty_never_borrows_from_a_bad_day(done):
    """TIL 2018-11-07: its nearest day (11-06) has a bad Kite candle (HIGHLOW_DIFF, ratio ~30)."""
    calc, s = calc_of(done), stored(done, "BIG")
    i, big = at(s, SESSION), sid(done, "BIG")
    j0 = at(s, date.fromisoformat(s.at[i, "conversion_from"]))
    bad = corrupt(s[md.INPUT_COLUMNS], j0)
    d = calc.derive(big, bad)
    assert "HIGHLOW_DIFF" in d.at[j0, "check"] and d.at[j0, "ratio"] == pytest.approx(30, rel=0.01)
    assert old_pick(d, i) == j0 and level(i, oldify(d)) == pytest.approx(30, rel=0.01)   # the old rule: 30x
    j1 = at(d, date.fromisoformat(d.at[i, "conversion_from"]))
    assert j1 != j0 and d.at[j1, "check"] == "OK" and d.at[i, "check"] == "DAY_EMPTY"
    assert [d.at[i, c] for c in CONV] == [d.at[j1, c] for c in CONV]
    assert level(i, d) == pytest.approx(1.0, abs=0.001)


def test_scale_step_day_is_not_a_source(done):
    """SCALECO's minute scale steps at STEP_DAY without a corporate action (SCALE_STEP): the DAY_EMPTY day after it
    borrows from the next OK day instead."""
    calc, s = calc_of(done), stored(done, "SCALECO")
    x = s[md.INPUT_COLUMNS].copy()
    i = at(x, STEP_DAY + 1)
    for c in PRICES + ["kite_volume"]:
        x.at[i, c] = np.nan
    d = calc.derive(sid(done, "SCALECO"), x)
    st = at(d, STEP_DAY)
    assert "SCALE_STEP" in d.at[st, "check"] and d.at[i, "check"] == "DAY_EMPTY"
    assert d.at[i, "minute_scale"] == d.at[st, "minute_scale"]
    assert d.at[i, "conversion_from"] == str(DAYS[STEP_DAY + 2])


def test_dayempty_source_rule():
    dates = np.array([0, 10, 20, 21, 30, 40])
    src = np.array([0, 1, 3, 4, 5])
    good = np.array([0, 4, 5])
    scale = np.array([1.0, 1.0, 1.0, 2.0, 2.0, 1.0])
    factor = np.array([1.0, 1.0, 1.0, 1.0, 1.0, 5.0])
    pick = md.dayempty_source
    assert pick(2, good, src, scale, factor, dates) == 0           # same scale first (30 is nearer, other scale)
    scale2 = scale.copy()
    scale2[0] = 3.0
    assert pick(2, good, src, scale2, factor, dates) == 4          # no OK day on its scale: any scale, same factor
    assert pick(2, np.array([5]), src, scale, factor, dates) == 1  # no OK day of its factor: src, same scale first
    assert pick(2, np.array([5]), np.array([5]), scale, factor, dates) is None   # never across a factor change


def split_case(f, last, bad=()):
    """SPLITCO's inputs up to DAYS[last]: DAYS[SPLIT_DAY] (the first day of the new factor) without its day candle,
    the days in bad with a bad candle."""
    s = stored(f, "SPLITCO")
    x = s.loc[s["date"] <= DAYS[last], md.INPUT_COLUMNS].reset_index(drop=True)
    i = at(x, SPLIT_DAY)
    for c in PRICES + ["kite_volume"]:
        x.at[i, c] = np.nan
    for b in bad:
        x = corrupt(x, at(x, b))
    return i, calc_of(f).derive(sid(f, "SPLITCO"), x)


def test_dayempty_never_crosses_a_factor_change(done):
    i, d = split_case(done, SPLIT_DAY + 2, bad=[SPLIT_DAY + 1])
    assert d.at[i, "our_factor"] != d.at[i - 1, "our_factor"]
    assert d.at[i, "check"] == "DAY_EMPTY" and d.at[i, "conversion_from"] == str(DAYS[SPLIT_DAY + 2])
    assert old_pick(d, i) == at(d, SPLIT_DAY + 1)                    # the old rule took the bad day
    assert level(i, d) == pytest.approx(1.0, abs=0.001)


def test_no_ok_source_day(done):
    i, d = split_case(done, SPLIT_DAY + 1, bad=[SPLIT_DAY + 1])     # the only day of its factor is bad
    assert d.at[i, "conversion_from"] == str(DAYS[SPLIT_DAY + 1])
    assert md.NO_OK_SOURCE in d.at[i, "note"].split("; ")
    assert d.at[i, "note"] == f"{md.NO_OK_SOURCE}; converted as {DAYS[SPLIT_DAY + 1]}"
    assert "HIGHLOW_DIFF" in d.at[i, "check"]                        # and it loads 30x: labelled
    i, d = split_case(done, SPLIT_DAY)                              # no day of its factor at all
    assert old_pick(d, i) == at(d, SPLIT_DAY - 1)                    # the old rule crossed the split
    assert d.at[i, "conversion_from"] == "" and np.isnan(d.at[i, "ratio"])
    assert d.at[i, "note"] == md.NO_OK_SOURCE and d.at[i, "check"] == "DAY_EMPTY"
    m = d.iloc[[i]].assign(open=1.0, high=1.0, low=1.0, close=1.0, volume=1).reset_index(drop=True)
    assert np.isnan(ms.convert(m)["close"][0])                       # loads as NaN, never at another factor


def test_extra_session_borrowing_unchanged(done):
    """EXTRA_SESSION (and every borrower that is not DAY_EMPTY) borrows as before, even from a bad day."""
    calc, s = calc_of(done), stored(done, "BIG")
    e, fr, big = at(s, EXTRA), at(s, FRIDAY), sid(done, "BIG")
    d = calc.derive(big, corrupt(s[md.INPUT_COLUMNS], fr))
    assert "HIGHLOW_DIFF" in d.at[fr, "check"] and d.at[e, "check"] == "EXTRA_SESSION"
    assert d.at[e, "conversion_from"] == str(FRIDAY) == md.iso(d.at[old_pick(d, e), "date"])
    assert [d.at[e, c] for c in CONV] == [d.at[fr, c] for c in CONV]
    assert d.at[e, "note"] == f"converted as {FRIDAY}"


# ---- fix-dayempty ----

def old_world(f):
    """The store as the rule before P75 stored it, plus the cases the fix must not write:
    BIG     the SESSION day's nearest day has a bad candle (borrow changes); its EXTRA row and a weekday row hold values
            a fresh derive would change (P80-like): never written;
    GEN1    the SESSION day's minutes 20% above the bhavcopy, stored CANDLE_UNCONFIRMED (gets labels, keeps the label);
    GEN2    the same, but minute_low 0 (stale aggregates): held;
    GEN3    the same, but a stored our_factor that a fresh derive changes: held (PRE_EXISTING)."""
    calc = calc_of(f)
    s = stored(f, "BIG")
    i = at(s, SESSION)
    j0 = at(s, date.fromisoformat(s.at[i, "conversion_from"]))
    d = oldify(calc.derive(sid(f, "BIG"), corrupt(s[md.INPUT_COLUMNS], j0)))
    d.at[at(d, EXTRA), "ratio"] = 9.99
    d.at[at(d, 30), "kite_multiplier"] = 9.99
    write(f, "BIG", d)
    for sym in ("GEN1", "GEN2", "GEN3"):
        s = stored(f, sym)
        i = at(s, SESSION)
        x = scaled(s[md.INPUT_COLUMNS], i, 1.2)
        if sym == "GEN2":
            x.at[i, "minute_low"] = 0.0
        d = oldify(calc.derive(sid(f, sym), x))
        if sym == "GEN1":
            d.at[i, "check"] = md.add_check(d.at[i, "check"], md.UNCONFIRMED)
        if sym == "GEN3":
            d.at[i, "our_factor"] = 2.0
        write(f, sym, d)
    return f


def fix(f, **kw):
    kw.setdefault("log", q)
    return mde.fix(f, **kw)


def data_tree(f):
    return tree(f, os.path.join("equity_1min", "data"))


def same_rows(a, b, rows=None):
    """Rows (all when None) of two frames hold the same values in every column."""
    rows = range(len(a)) if rows is None else rows
    return all(mz.same(a.at[i, c], b.at[i, c]) for i in rows for c in a.columns)


def test_fix_dry_run(done):
    f = old_world(done)
    before = data_tree(f)
    res = fix(f)
    assert data_tree(f) == before                                           # nothing written
    assert [(x["symbol"], x["new_check"]) for x in res["labels"]] == \
        [("GEN1", "DAY_EMPTY|HIGHLOW_DIFF|CLOSE_DIFF|CANDLE_UNCONFIRMED")]
    [b] = res["borrow"]
    assert b["symbol"] == "BIG" and b["old_level"] == pytest.approx(30, rel=0.01)
    assert b["new_level"] == pytest.approx(1.0, abs=0.001) and b["result"] == "BETTER"
    held = {(x["symbol"], x["reason"]) for x in res["info"]}
    assert held == {("GEN2", "STALE_AGGREGATES"), ("GEN3", "PRE_EXISTING")}
    assert "our_factor" in [x for x in res["info"] if x["symbol"] == "GEN3"][0]["detail"]
    cols = {(c["symbol"], c["column"]) for c in res["cells"]}
    assert cols == {("GEN1", "check")} | {("BIG", c) for c in ("note", "conversion_from", "ratio", "kite_difference")}
    assert {c["date"] for c in res["cells"]} == {str(DAYS[SESSION])}
    out = res["report_dir"]
    assert os.path.basename(out).startswith("dryrun_") and os.path.dirname(out).endswith("dayempty")
    for name in ("report.txt", "labels.csv", "borrow.csv", "cells.csv", "no_source.csv", "info.csv",
                 "check_files.csv"):
        assert os.path.isfile(os.path.join(out, name))
    last = res["report"].strip().splitlines()[-4:]
    assert last[:3] == ["labels 1", "borrow 1", f"cells {len(res['cells'])}"] and last[3].startswith("peak memory")
    cf = {x["file"]: x for x in res["check_files"]}
    assert cf["minute_check.csv"]["rows_changed"] == 2
    assert cf["minute_check.csv"]["rows_before"] == cf["minute_check.csv"]["rows_after"]
    lab = {x["label"]: x for x in res["check_labels"]}
    assert lab["HIGHLOW_DIFF"]["after"] == lab["HIGHLOW_DIFF"]["before"] + 1
    assert lab["CLOSE_DIFF"]["after"] == lab["CLOSE_DIFF"]["before"] + 1


def test_fix_apply_writes_only_the_allowed_cells(done):
    f = old_world(done)
    old = {s: stored(f, s) for s in ("BIG", "GEN1", "GEN2", "GEN3", "MID")}
    before = data_tree(f)
    res = fix(f, apply=True, expect_labels=1, expect_borrow=1)
    after = data_tree(f)
    changed = {k for k in before if before[k] != after.get(k)}
    assert changed == {os.path.relpath(os.path.join(path_of(f, s), ms.DAYS_FILE), f) for s in ("BIG", "GEN1")}
    for s in ("BIG", "GEN1"):
        new, o = stored(f, s), old[s]
        i = at(o, SESSION)
        assert same_rows(o, new, [k for k in range(len(o)) if k != i])     # every other row as stored
        owned = ["check", "note"] + mde.BORROW_CELLS
        assert all(mz.same(o.at[i, c], new.at[i, c]) for c in o.columns if c not in owned)
    big = stored(f, "BIG")
    assert big.at[at(big, EXTRA), "ratio"] == 9.99 and big.at[at(big, 30), "kite_multiplier"] == 9.99
    i = at(big, SESSION)
    assert big.at[i, "conversion_from"] != old["BIG"].at[i, "conversion_from"] and big.at[i, "check"] == "DAY_EMPTY"
    assert level(i, big) == pytest.approx(1.0, abs=0.001)
    g1 = stored(f, "GEN1")
    assert g1.at[at(g1, SESSION), "check"] == "DAY_EMPTY|HIGHLOW_DIFF|CLOSE_DIFF|CANDLE_UNCONFIRMED"
    for s in ("GEN2", "GEN3"):
        pd.testing.assert_frame_equal(stored(f, s), old[s])
    # backup + manifest (sha256 of the backup = the file before)
    b = res["backup"]
    assert os.path.basename(b).startswith("dayempty_") and os.path.dirname(b).endswith("archive")
    man = pd.read_csv(os.path.join(b, "manifest.csv"), dtype=str)
    assert sorted(man["symbol"]) == ["BIG", "GEN1"]
    for x in man.itertuples():
        assert mde.sha256(x.backup) == x.sha256 == before[os.path.relpath(x.source, f)]
    assert len(pd.read_csv(os.path.join(b, "cells.csv"))) == len(res["cells"])
    chk = pd.read_csv(os.path.join(mp.out_dir(f), "check", "minute_check.csv"), dtype=str)
    assert "HIGHLOW_DIFF" in chk.loc[(chk["symbol"] == "GEN1") & (chk["date"] == str(DAYS[SESSION])), "check"].iat[0]
    assert os.path.basename(res["report_dir"]).startswith("apply_")
    again = fix(f)                                                          # a second dry run finds nothing
    assert (len(again["labels"]), len(again["borrow"]), len(again["cells"])) == (0, 0, 0)
    assert {(x["symbol"], x["reason"]) for x in again["info"]} == {("GEN2", "STALE_AGGREGATES"),
                                                                   ("GEN3", "PRE_EXISTING")}


def test_fix_apply_needs_the_counts(done, capsys):
    f = old_world(done)
    before = tree(f)
    with pytest.raises(mde.CountMismatch, match="expect-labels 2"):
        fix(f, apply=True, expect_labels=2, expect_borrow=1)
    with pytest.raises(mde.CountMismatch):
        fix(f, apply=True, expect_labels=None, expect_borrow=1)
    assert tree(f) == before                                                # nothing written, not even a report
    assert cli.main(["fix-dayempty", "--apply", "--expect-labels", "1", "--expect-borrow", "0",
                     "--folder", f]) == 1
    assert data_tree(f) == {k: v for k, v in before.items() if k in data_tree(f)}
    with pytest.raises(SystemExit) as e:
        cli.main(["fix-dayempty", "--apply", "--folder", f])
    assert e.value.code == 2
    assert cli.main(["fix-dayempty", "--expect-labels", "5", "--expect-borrow", "1", "--folder", f]) == 1
    assert cli.main(["fix-dayempty", "--folder", f]) == 0
    assert "labels 1" in capsys.readouterr().out


def test_fix_apply_error_restores_every_file(done, monkeypatch):
    f = old_world(done)
    before = data_tree(f)
    calls = []

    def fail(tmp, path):
        calls.append(path)
        if len(calls) == 2:
            raise OSError("disk full")
        os.replace(tmp, path)
    monkeypatch.setattr(mz, "_replace", fail)
    with pytest.raises(md.ApplyFailed, match="restored"):
        fix(f, apply=True, expect_labels=1, expect_borrow=1)
    assert data_tree(f) == before
    assert not [p for p in data_tree(f) if p.endswith(".tmp")]


def test_fix_refusals(done):
    f = old_world(done)
    before = data_tree(f)
    lock = RunLock(mp.lock_path(f)).acquire("test")
    try:
        with pytest.raises(mp.LockBusy):
            fix(f)
        assert cli.main(["fix-dayempty", "--folder", f]) == 2
    finally:
        lock.release()
    os.remove(os.path.join(md.cache_dir(f), "cache_info.json"))             # a stale bhavcopy cache
    with pytest.raises(md.CacheStale):
        fix(f)
    assert cli.main(["fix-dayempty", "--folder", f]) == 1
    assert cli.main(["fix-dayempty", "--apply", "--expect-labels", "1", "--expect-borrow", "1",
                     "--folder", f]) == 1
    assert data_tree(f) == before


class BadCandleKite(EdgeKite):
    """EdgeKite with BIG's day candle of `bad` on another scale (a HIGHLOW_DIFF day)."""

    def __init__(self, bad, **kw):
        super().__init__(**kw)
        self.bad = bad

    def historical_data(self, token, start, end, interval, continuous=False, oi=False):
        out = super().historical_data(token, start, end, interval, continuous, oi)
        if interval != "day" or self.by_token[token] != "BIG":
            return out
        return [{**c, **{k: round(c[k] / 30, 2) for k in ("open", "high", "low", "close")}}
                if c["date"].date() == DAYS[self.bad] else c for c in out]


def test_update_stores_a_new_dayempty_day_with_the_same_rule(done):
    f = done
    rollback(f, "BIG", N - 5)
    extend(f)
    empty, bad = N - 4, N - 3                       # no candle; the next day's candle is bad
    md.update(f, kite=BadCandleKite(bad, no_candle={("BIG", empty)}, last_day=N), speed=FAST, now=MORNING_AFTER,
              log=q)
    big = stored(f, "BIG")
    i, b = at(big, empty), at(big, bad)
    assert "DAY_EMPTY" in big.at[i, "check"] and "HIGHLOW_DIFF" in big.at[b, "check"]
    assert big.at[i, "conversion_from"] == str(DAYS[N - 5]) and big.at[at(big, N - 5), "check"] == "OK"
    fresh = calc_of(f).derive(sid(f, "BIG"), big[md.INPUT_COLUMNS])
    fresh["check"] = [md.add_check(c, md.UNCONFIRMED) if md.UNCONFIRMED in o else c    # set by update, not derived
                      for c, o in zip(fresh["check"], big["check"])]
    pd.testing.assert_frame_equal(fresh, big[md.DAY_COLUMNS])               # update = derive
    res = fix(f)                                                            # and fix-dayempty has nothing to do
    assert (len(res["labels"]), len(res["borrow"]), len(res["cells"])) == (0, 0, 0)

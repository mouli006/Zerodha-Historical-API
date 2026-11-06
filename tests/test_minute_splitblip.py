"""equity_minute.py fix-split-blip (backfill/equity/minute_splitblip.py): the day before a split whose minute bars Kite
left unadjusted while its day candle is adjusted, stored at minute_scale 1.0 before guard (a), fixed. Synthetic stores
in tmp_path (test_equity_minute_download.py's world and fake Kite), no network, .env stubbed."""
import os
import shutil

import numpy as np
import pandas as pd
import pytest

import config
import equity_minute as cli
from backfill import fetch
from backfill.equity import minute_download as md
from backfill.equity import minute_plan as mp
from backfill.equity import minute_splitblip as msb
from backfill.equity import minute_store as ms
from backfill.locks import RunLock
from test_equity_minute_download import (DAYS, FAST, SPLIT_DAY, FakeKite, FastStop, done_copy,  # noqa: F401
                                         downloaded, no_network, planned, q, raw, sid)
from test_equity_minute_finished_days import tree

PRE = SPLIT_DAY - 1                       # SPLITCO's day before the ex-date (x5 split)
# (symbol, day index): minute prices x mult, volume / mult (bars on another scale than the day candle that day)
MULT = {("SPLITCO", PRE): 5.0,            # JSWDULUX-like: the day candle adjusted, the minute bars not
        ("GEN1", 30): 2.0,                # a one-day break with no adjustment on the next day
        ("GEN2", 20): 3.0}                # a P175-like break, no matching factor


class BlipKite(FakeKite):
    """FakeKite whose minute bars of the MULT days are not on the day candle's scale."""

    def historical_data(self, token, start, end, interval, continuous=False, oi=False):
        out = super().historical_data(token, start, end, interval, continuous, oi)
        if interval != "minute":
            return out
        sym, res = self.by_token[token], []
        for c in out:
            d = c["date"].date()
            m = MULT.get((sym, DAYS.index(d) if d in DAYS else None))
            if m:
                c = {**c, **{k: round(c[k] * m, 2) for k in ("open", "high", "low", "close")},
                     "volume": int(c["volume"] / m)}
            res.append(c)
        return res


def no_guard(sid_, dates, *a, **k):
    n = len(dates)
    return np.zeros(n, bool), np.full(n, np.nan), np.full(n, np.nan)


@pytest.fixture(scope="module")
def blipped(planned, tmp_path_factory):
    """planned + a BlipKite download derived the old way (guard (a) off): the blips stored at minute_scale 1.0."""
    f = str(tmp_path_factory.mktemp("splitblip") / "data")
    shutil.copytree(planned, f)
    mpatch = pytest.MonkeyPatch()
    mpatch.setattr(config, "load_env", lambda: {})
    mpatch.setattr(fetch, "STOP", FastStop())
    mpatch.setattr(md, "split_blip_rows", no_guard)
    md.download(f, kite=BlipKite(), speed=FAST, workers=1, log=q)
    mpatch.undo()
    return f


@pytest.fixture
def world(blipped, tmp_path):
    f = str(tmp_path / "data")
    shutil.copytree(blipped, f)
    return f


def days(f, sym):
    return ms.read_days(ms.stock_dir(f, sid(f, sym)))


def at(df, i):
    return df.index[df["date"] == DAYS[i]][0]


def fix(f, **kw):
    kw.setdefault("log", q)
    return msb.fix(f, **kw)


def data_tree(f):
    return tree(f, os.path.join("equity_1min", "data"))


def fresh(f, sym):
    calc = md.Calc(f, mp.Inputs(f, bhav=False), write=False)
    s = days(f, sym)
    return calc.derive(sid(f, sym), s[md.INPUT_COLUMNS])


# ---- the world ----

def test_world_has_the_blips(world):
    s = days(world, "SPLITCO")
    i = at(s, PRE)
    assert s.at[i, "minute_scale"] == 1.0 and s.at[i, "minute_volume_scale"] == 1.0
    assert s.at[i, "minute_scale_measured"] == pytest.approx(0.2, rel=1e-3)
    assert s.at[i, "ratio"] == pytest.approx(5.0, rel=1e-3)
    assert {"HIGHLOW_DIFF", "CLOSE_DIFF"} <= set(s.at[i, "check"].split("|"))
    g1 = days(world, "GEN1")
    assert g1.at[at(g1, 30), "minute_scale"] == 1.0 and "HIGHLOW_DIFF" in g1.at[at(g1, 30), "check"]


def test_measure_equals_the_derive(world):
    for sym in ("SPLITCO", "GEN1", "EMPTYCO"):
        s = days(world, sym)
        assert np.array_equal(np.round(msb.measure(s), 6), fresh(world, sym)["minute_scale_measured"].to_numpy(float),
                              equal_nan=True), sym


# ---- selection ----

def test_dry_run_selects_the_split_blip_and_writes_only_its_report(world):
    before = tree(world)
    res = fix(world)
    assert [(x["symbol"], x["date"], x["result"]) for x in res["fixed"]] == [("SPLITCO", str(DAYS[PRE]), "FIX")]
    assert res["held"] == [] and res["rows_changed"] == 2
    x = res["fixed"][0]
    assert x["F"] == pytest.approx(5.0) and x["C"] == pytest.approx(5.0)
    assert x["close_x_last_before"] == pytest.approx(5.0, rel=0.01)
    assert x["close_x_last_after"] == pytest.approx(1.0, abs=0.01)
    assert x["volume_x_bhav_before"] == pytest.approx(0.2, rel=0.03)
    assert x["volume_x_bhav_after"] == pytest.approx(1.0, abs=0.03)
    after = tree(world)
    new = {k for k in after if after[k] != before.get(k)}
    assert new and all(k.startswith(os.path.join("equity_1min", "splitblip", "dryrun_")) for k in new)
    for name in ("report.txt", "rows.csv", "cells.csv", "held.csv", "hashes.csv"):
        assert os.path.isfile(os.path.join(res["report_dir"], name))
    text = res["report"]
    assert "Selected 1 rows in 1 stocks" in text and "Rows changed 2 (1 fixed rows + 1 next-row notes)" in text
    assert "hash-identical" in text and "DRY RUN" in text


def test_not_selected(world, done_copy):
    res = fix(world)
    dates = {(x["symbol"], x["date"]) for x in res["rows"]}
    assert ("GEN1", str(DAYS[30])) not in dates and ("GEN2", str(DAYS[20])) not in dates
    g1 = fresh(world, "GEN1")                                      # no adjustment next day: still a blip
    assert g1.at[at(g1, 30), "minute_scale"] == 1.0
    assert fix(done_copy)["rows"] == []           # day and minutes adjusted together (GNRL / KMEW): nothing


# ---- apply ----

def test_apply_fixes_the_row_like_a_fresh_derive(world):
    old = days(world, "SPLITCO")
    files = data_tree(world)
    res = fix(world, apply=True, expect=1)
    new, ref = days(world, "SPLITCO"), fresh(world, "SPLITCO")
    i = at(new, PRE)
    assert new.at[i, "minute_scale"] == pytest.approx(0.2, rel=1e-3) and new.at[i, "check"] == "OK"
    assert new.at[i, "minute_volume_scale"] == pytest.approx(5.0, rel=0.03)
    for c in msb.WRITTEN:
        assert msb.same(new.at[i, c], ref.at[i, c]), c
    assert msb.same(new.at[i + 1, "note"], ref.at[i + 1, "note"]) and "corporate action" in new.at[i + 1, "note"]
    for c in md.DAY_COLUMNS:                                       # nothing else moved
        for k in range(len(old)):
            if (k == i and c in msb.WRITTEN) or (k == i + 1 and c in msb.NEXT):
                continue
            assert msb.same(old.at[k, c], new.at[k, c]), (k, c)
    after = data_tree(world)
    changed = {k for k in after if after[k] != files.get(k)}
    assert changed == {os.path.join("equity_1min", "data", sid(world, "SPLITCO"), ms.DAYS_FILE)}
    m = ms.load_minute("SPLITCO", DAYS[PRE], DAYS[PRE], folder=world)
    assert m["close"].iloc[-1] == pytest.approx(raw("SPLITCO", PRE)["close"], rel=0.01)
    assert m["volume"].sum() == pytest.approx(raw("SPLITCO", PRE)["volume"], rel=0.03)
    b = res["backup"]
    assert os.path.basename(b).startswith("fix_split_blip_")
    pd.testing.assert_frame_equal(ms.read_days(os.path.join(b, sid(world, "SPLITCO"))), old)
    assert len(pd.read_csv(os.path.join(b, "rows_before.csv"))) == 2
    assert len(pd.read_csv(os.path.join(b, "cells.csv"))) == len(msb.WRITTEN) + len(msb.NEXT)
    assert fix(world)["rows"] == []                                # a second dry run finds 0


def test_expect_mismatch_writes_nothing(world, capsys):
    before = data_tree(world)
    assert cli.main(["fix-split-blip", "--folder", world, "--apply", "--expect", "2"]) == 1
    assert "not --expect 2: nothing written" in capsys.readouterr().err
    assert data_tree(world) == before
    with pytest.raises(SystemExit) as e:
        cli.main(["fix-split-blip", "--folder", world, "--apply"])
    assert e.value.code == 2
    with pytest.raises(msb.CountMismatch):
        fix(world, apply=True)
    assert data_tree(world) == before
    assert not os.path.isdir(os.path.join(md.out_dir(world), "archive")) or \
        not [n for n in os.listdir(os.path.join(md.out_dir(world), "archive")) if n.startswith("fix_split_blip_")]


def test_failure_in_the_middle_restores_everything(world, monkeypatch):
    before = data_tree(world)

    def broken(tmp, path):
        raise OSError("disk went away")
    monkeypatch.setattr(msb, "_replace", broken)
    with pytest.raises(md.ApplyFailed, match="restored from"):
        fix(world, apply=True, expect=1)
    assert data_tree(world) == before
    assert not [k for k in tree(world) if k.endswith(".tmp")]
    [b] = [n for n in os.listdir(os.path.join(md.out_dir(world), "archive")) if n.startswith("fix_split_blip_")]
    assert os.path.isfile(os.path.join(md.out_dir(world), "archive", b, sid(world, "SPLITCO"), ms.DAYS_FILE))


def test_guard_breach_writes_nothing(world, monkeypatch):
    before = data_tree(world)
    real = msb.Stock.skips
    monkeypatch.setattr(msb.Stock, "skips", lambda self: {k: v - {"note"} for k, v in real(self).items()})
    with pytest.raises(md.ApplyFailed, match="guard failed"):
        fix(world, apply=True, expect=1)
    assert data_tree(world) == before


@pytest.mark.parametrize("apply", [False, True])
def test_lock_busy_refuses(world, capsys, apply):
    args = ["fix-split-blip", "--folder", world] + (["--apply", "--expect", "1"] if apply else [])
    before = tree(world)
    lock = RunLock(mp.lock_path(world)).acquire("equity_minute.py update")
    try:
        assert cli.main(args) == 2
        assert "Not run" in capsys.readouterr().err
    finally:
        lock.release()
    assert {k: v for k, v in tree(world).items() if "download_progress" not in k} == \
        {k: v for k, v in before.items() if "download_progress" not in k}


def test_cli(world, capsys):
    assert cli.main(["fix-split-blip", "--folder", world]) == 0
    assert "Selected 1 rows in 1 stocks" in capsys.readouterr().out
    assert cli.main(["fix-split-blip", "--folder", world, "--apply", "--expect", "1"]) == 0
    assert cli.main(["fix-split-blip", "--folder", world, "--apply", "--expect", "0"]) == 0
    assert "Selected 0 rows" in capsys.readouterr().out

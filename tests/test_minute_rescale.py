"""equity_minute.py fix-rescale (backfill/equity/minute_rescale.py): held padded rows loaded at the wrong minute_scale
level, fixed; and the P81 report fix of fix-zero-padding. Synthetic stores in tmp_path, no network, .env stubbed."""
import os
import shutil
from datetime import date, datetime, time as dtime

import numpy as np
import pandas as pd
import pytest

import config
import equity_minute as cli
from backfill import fetch
from backfill.equity import minute_download as md
from backfill.equity import minute_plan as mp
from backfill.equity import minute_rescale as mrs
from backfill.equity import minute_store as ms
from backfill.equity import minute_zeropad as mz
from backfill.locks import RunLock
from test_equity_minute_download import (DAYS, EXTRA, FAST, FakeKite, FastStop, no_network, planned, q,  # noqa: F401
                                         raw, sid)
from test_equity_minute_finished_days import tree
from test_minute_zeropad import make_old

# (symbol): minute prices x mult on days [a, b) (minute_scale 1 / mult there)
MULT = {"GEN1": (45, 60, 2.0), "GEN2": (30, 45, 5.0)}
# (symbol, day index): the times of the all-zero bars, or "all"
PAD = {("GEN1", 45): [dtime(9, 14), dtime(13, 0)],      # BALKRISIND-like: first day of a x0.5 period
       ("GEN2", 45): [dtime(9, 14)],                    # JMA-like: first day after a x5 period
       ("BIG", 50): [dtime(13, 0)],                     # no scale change: stored and fresh both right
       ("GEN3", 50): [dtime(13, 0)],                    # COUNCODOS-like with a patched derive
       ("MID", 42): "all"}                              # only zero bars: no reference
OLD = {"GEN1": [45], "GEN2": [45], "BIG": [50], "GEN3": [50], "MID": [42]}
FRIDAY = DAYS.index(date(2015, 2, 27))                  # the Saturday session trades like Friday


def zero_bar(d, t):
    return {"date": datetime.combine(d, t).replace(tzinfo=mp.IST), "open": 0.0, "high": 0.0, "low": 0.0,
            "close": 0.0, "volume": 0}


class RescaleKite(FakeKite):
    """FakeKite whose minute bars of MULT stocks are on another scale for a period, padded on the PAD days."""

    def historical_data(self, token, start, end, interval, continuous=False, oi=False):
        out = super().historical_data(token, start, end, interval, continuous, oi)
        if interval != "minute":
            return out
        sym, res, seen = self.by_token[token], [], set()
        for c in out:
            d = c["date"].date()
            i = DAYS.index(d) if d in DAYS else FRIDAY
            a, b, m = MULT.get(sym, (0, 0, 1.0))
            if a <= i < b:
                c = {**c, **{k: round(c[k] * m, 2) for k in ("open", "high", "low", "close")}}
            spec = PAD.get((sym, i if d in DAYS else None))
            if spec == "all":
                res.append(zero_bar(d, c["date"].time()))
                continue
            res.append(c)
            if spec and d not in seen:
                seen.add(d)
                res += [zero_bar(d, t) for t in spec]
        return sorted(res, key=lambda c: c["date"])


@pytest.fixture(scope="module")
def rescaled(planned, tmp_path_factory):
    """planned + a RescaleKite download with the current code, then the PAD rows built the old way (zero bars in)."""
    f = str(tmp_path_factory.mktemp("rescale") / "data")
    shutil.copytree(planned, f)
    mpatch = pytest.MonkeyPatch()
    mpatch.setattr(config, "load_env", lambda: {})
    mpatch.setattr(fetch, "STOP", FastStop())
    md.download(f, kite=RescaleKite(), speed=FAST, workers=1, log=q)
    fresh = {s: days(f, s) for s in OLD}
    for s, idx in OLD.items():
        make_old(f, s, idx)
    mpatch.undo()
    return f, fresh


@pytest.fixture
def world(rescaled, tmp_path):
    f = str(tmp_path / "data")
    shutil.copytree(rescaled[0], f)
    return f


def days(f, sym):
    return ms.read_days(ms.stock_dir(f, sid(f, sym)))


def at(df, i):
    return df.index[df["date"] == DAYS[i]][0]


def fix(f, **kw):
    kw.setdefault("log", q)
    return mrs.fix(f, **kw)


def data_tree(f):
    return tree(f, os.path.join("equity_1min", "data"))


def outside_report(f):
    return {k: v for k, v in tree(f).items() if not k.startswith(os.path.join("equity_1min", "zeropad"))}


def result(res, sym, i):
    [x] = [x for x in res["candidates"] if x["symbol"] == sym and x["date"] == str(DAYS[i])]
    return x


def cells_outside(old, new, targets):
    """(date, column) cells of new that differ from old outside the fixed rows' WRITTEN and next rows' NEXT."""
    out = []
    for c in md.DAY_COLUMNS:
        for k in range(len(old)):
            if (k in targets and c in mrs.WRITTEN) or (k - 1 in targets and c in mrs.NEXT):
                continue
            if not mz.same(old.at[k, c], new.at[k, c]):
                out.append((old.at[k, "date"], c))
    return out


# ---- the world ----

def test_old_rows_carry_the_wrong_level(world, rescaled):
    g1, g2 = days(world, "GEN1"), days(world, "GEN2")
    assert g1.at[at(g1, 45), "minute_low"] == 0 and np.isnan(g1.at[at(g1, 45), "minute_scale_measured"])
    assert g1.at[at(g1, 45), "minute_scale"] == 1.0 and g1.at[at(g1, 46), "minute_scale"] == 0.5
    assert g2.at[at(g2, 45), "minute_scale"] == pytest.approx(0.2) and g2.at[at(g2, 46), "minute_scale"] == 1.0
    assert "SCALE_STEP" in g1.at[at(g1, 46), "check"] and "SCALE_STEP" in g2.at[at(g2, 46), "check"]
    fresh = rescaled[1]
    assert fresh["GEN1"].at[at(fresh["GEN1"], 45), "minute_scale"] == 0.5
    assert fresh["GEN2"].at[at(fresh["GEN2"], 45), "minute_scale"] == 1.0


# ---- the selection ----

def test_padded_first_day_of_a_scale_period_fixed(world, rescaled):
    res = fix(world)
    assert [(x["symbol"], x["date"]) for x in res["fixed"]] == [("GEN1", str(DAYS[45])), ("GEN2", str(DAYS[45]))]
    g1, g2 = result(res, "GEN1", 45), result(res, "GEN2", 45)
    assert g1["old_level"] == pytest.approx(2.0, rel=0.01) and g1["new_level"] == pytest.approx(1.0, abs=0.01)
    assert g2["old_level"] == pytest.approx(0.2, rel=0.01) and g2["new_level"] == pytest.approx(1.0, abs=0.01)
    assert (g1["old_minute_scale"], g1["new_minute_scale"]) == (1.0, 0.5)
    assert g2["new_minute_scale"] == 1.0
    assert "GEN1" in res["report"] and "(level 2.00x) ->" in res["report"] and "(level 1.00x)" in res["report"]
    assert "(level 0.20x) ->" in res["report"]


def test_fixed_rows_equal_the_fresh_download(world, rescaled):
    fix(world, apply=True, expect=2)
    for s in ("GEN1", "GEN2"):
        new, fresh = days(world, s), rescaled[1][s]
        for i in (45, 46):
            for c in mrs.WRITTEN:
                assert mz.same(new.at[at(new, i), c], fresh.at[at(fresh, i), c]), (s, i, c)


def test_next_row_check_and_note(world):
    old1, old2 = days(world, "GEN1"), days(world, "GEN2")
    res = fix(world, apply=True, expect=2)
    g1, g2 = days(world, "GEN1"), days(world, "GEN2")
    assert g1.at[at(g1, 46), "check"] == "OK" and g1.at[at(g1, 46), "note"] == ""
    assert g1.at[at(g1, 45), "note"].startswith("minute_scale 1 -> 0.5") and "SCALE_STEP" in g1.at[at(g1, 45), "check"]
    assert old2.at[at(old2, 46), "check"] == "SCALE_STEP" and g2.at[at(g2, 46), "check"] == "OK"
    assert g2.at[at(g2, 46), "note"] == "" and g2.at[at(g2, 45), "note"].startswith("minute_scale 0.2 -> 1")
    for old, new in ((old1, g1), (old2, g2)):                               # the guard: nothing else moved
        assert cells_outside(old, new, {at(old, 45)}) == []
    roles = pd.DataFrame(res["cells"])
    assert set(roles.loc[roles["role"] == "next", "column"]) == set(mrs.NEXT)
    assert set(roles.loc[roles["role"] == "target", "column"]) == set(mrs.WRITTEN)


def test_others_stay_held(world, monkeypatch):
    real = md.Calc.derive
    g3 = sid(world, "GEN3")

    def wrong(self, s, inputs, bhav=False):                     # COUNCODOS-like: the fresh derive is off
        d = real(self, s, inputs, bhav)
        if s == g3 and inputs.loc[inputs["date"] == DAYS[50], "minute_low"].iat[0] > 0:
            d.loc[d["date"] == DAYS[50], "minute_scale"] = 1.05
        return d
    monkeypatch.setattr(md.Calc, "derive", wrong)
    before = data_tree(world)
    res = fix(world, apply=True, expect=2)
    assert result(res, "BIG", 50)["result"] == "STORED_OK"                         # tick noise: both right
    x = result(res, "GEN3", 50)
    assert x["result"] == "STORED_OK" and x["new_level"] == pytest.approx(1.05, rel=0.01)   # stored right, fresh off
    assert result(res, "MID", 42)["result"] == "NO_REFERENCE"
    after = data_tree(world)
    changed = {k for k in after if after[k] != before.get(k)}
    assert changed == {os.path.relpath(os.path.join(ms.stock_dir(world, sid(world, s)), ms.DAYS_FILE), world)
                       for s in ("GEN1", "GEN2")}


def test_fresh_off_and_other_moves_stay_held(world, monkeypatch):
    real = md.Calc.derive
    g1, g2 = sid(world, "GEN1"), sid(world, "GEN2")

    def patched(self, s, inputs, bhav=False):
        d = real(self, s, inputs, bhav)
        if inputs.loc[inputs["date"] == DAYS[45], "minute_low"].iat[0] > 0:
            if s == g1:
                d.loc[d["date"] == DAYS[45], "minute_scale"] = 0.6         # the fresh derive is off too
            if s == g2:
                d.loc[d["date"] == DAYS[70], "minute_volume_scale"] = 1.5  # something else moves
        return d
    monkeypatch.setattr(md.Calc, "derive", patched)
    res = fix(world)
    assert res["fixed"] == [] and res["cells"] == []
    assert result(res, "GEN1", 45)["result"] == "FRESH_OFF"
    x = result(res, "GEN2", 45)
    assert x["result"] == "OTHER_MOVES" and "minute_volume_scale (other rows)" in x["detail"]


def test_special_session_bhav_rows_do_not_block(world, monkeypatch):
    """P80: a bhavcopy row added later for a weekend session stored as EXTRA_SESSION: the derive differs from the
    stored file there, the fix still runs and that row stays as stored."""
    real = md.Calc.bhav_of
    g1 = sid(world, "GEN1")

    def with_session(self, s):
        b = real(self, s)
        if s == g1:
            row = b[b["date"] == DAYS[FRIDAY]].assign(date=EXTRA)
            b = pd.concat([b, row]).sort_values("date").reset_index(drop=True)
        return b
    monkeypatch.setattr(md.Calc, "bhav_of", with_session)
    old = days(world, "GEN1")
    assert old.loc[old["date"] == EXTRA, "check"].iat[0].startswith("EXTRA_SESSION")
    fix(world, apply=True, expect=2)
    new = days(world, "GEN1")
    assert cells_outside(old, new, {at(old, 45)}) == []
    assert new.loc[new["date"] == EXTRA].equals(old.loc[old["date"] == EXTRA])
    assert new.at[at(new, 45), "minute_scale"] == 0.5


def test_guard_breach_raises(world, monkeypatch):
    real = mrs.Stock.outside

    def breach(self, a, b, targets):
        out = real(self, a, b, targets)
        return out + ["minute_volume (other rows)"] if a is self.stored else out
    monkeypatch.setattr(mrs.Stock, "outside", breach)
    before = data_tree(world)
    with pytest.raises(md.ApplyFailed, match="guard failed"):
        fix(world, apply=True, expect=2)
    assert data_tree(world) == before


# ---- dry run, apply, refusals ----

def test_dry_run_writes_only_its_report(world):
    before = tree(world)
    res = fix(world)
    assert outside_report(world) == before and res["backup"] is None
    assert os.path.basename(res["report_dir"]).startswith("rescale_dryrun_")
    assert os.path.dirname(res["report_dir"]) == os.path.join(md.out_dir(world), "zeropad")
    assert sorted(os.listdir(res["report_dir"])) == ["cells.csv", "held_rows.csv", "levels.csv", "report.txt"]
    cells = pd.read_csv(os.path.join(res["report_dir"], "cells.csv"), dtype=str, keep_default_na=False)
    assert list(cells.columns) == mrs.CELL_COLUMNS and len(cells) == 2 * (len(mrs.WRITTEN) + len(mrs.NEXT))
    lv = pd.read_csv(os.path.join(res["report_dir"], "levels.csv"))
    assert list(lv.columns) == mrs.LEVEL_COLUMNS and len(lv) == 2
    assert "DRY RUN, nothing written to the store" in res["report"] and "to fix: 2 rows in 2 stocks" in res["report"]
    assert "written 0" in res["report"]


def test_apply_backup_second_run_and_minutes_untouched(world):
    def year_files(s):
        t = tree(world, os.path.relpath(ms.stock_dir(world, sid(world, s)), world))
        return {k: v for k, v in t.items() if k.endswith("2015.parquet")}
    bars = {s: year_files(s) for s in ("GEN1", "GEN2")}
    assert all(bars.values())
    raw_days = {s: open(os.path.join(ms.stock_dir(world, sid(world, s)), ms.DAYS_FILE), "rb").read()
                for s in ("GEN1", "GEN2")}
    res = fix(world, apply=True, expect=2)
    assert os.path.basename(res["backup"]).startswith("zeropad_rescale_")
    assert os.path.dirname(res["backup"]) == os.path.join(md.out_dir(world), "archive")
    for s, b in raw_days.items():
        assert open(os.path.join(res["backup"], sid(world, s), ms.DAYS_FILE), "rb").read() == b
        assert year_files(s) == bars[s]                                             # minute bars never written
    csv = pd.read_csv(os.path.join(res["backup"], "rescale.csv"), dtype=str, keep_default_na=False)
    assert list(csv.columns) == mrs.CELL_COLUMNS
    assert os.path.basename(res["report_dir"]).startswith("rescale_apply_")
    again = fix(world)
    assert again["fixed"] == [] and again["cells"] == []
    assert {(x["symbol"], x["date"]) for x in again["candidates"]} == \
        {("BIG", str(DAYS[50])), ("GEN3", str(DAYS[50])), ("MID", str(DAYS[42]))}
    d = DAYS[45]
    m = ms.load_minute("GEN1", d, d, folder=world)
    assert m["high"].max() == pytest.approx(raw("GEN1", 45)["high"], rel=0.01)       # loads at the bhavcopy level
    m = ms.load_minute("GEN2", d, d, folder=world)
    assert m["high"].max() == pytest.approx(raw("GEN2", 45)["high"], rel=0.01)


def test_after_fix_zero_padding(world):
    """The real order: fix-zero-padding holds the two rows (SCALE_DRIFT), fix-rescale then fixes exactly those."""
    z = mz.fix(world, apply=True, log=q)
    assert {(x["symbol"], x["date"], x["reason"]) for x in z["held"]} == \
        {("GEN1", str(DAYS[45]), "SCALE_DRIFT"), ("GEN2", str(DAYS[45]), "SCALE_DRIFT")}
    res = fix(world, apply=True, expect=2)
    assert [(x["symbol"], x["date"]) for x in res["candidates"]] == [("GEN1", str(DAYS[45])), ("GEN2", str(DAYS[45]))]
    assert fix(world)["candidates"] == []


def test_expect_mismatch_refuses(world, capsys):
    before = data_tree(world)
    assert cli.main(["fix-rescale", "--folder", world, "--apply", "--expect", "3"]) == 1
    assert "not --expect 3: nothing written" in capsys.readouterr().err
    assert data_tree(world) == before
    with pytest.raises(mrs.CountMismatch):
        fix(world, apply=True)
    with pytest.raises(SystemExit) as e:
        cli.main(["fix-rescale", "--folder", world, "--apply"])
    assert e.value.code == 2
    with pytest.raises(mrs.CountMismatch, match="not --expect 1"):
        fix(world, expect=1)                                       # the dry run says so too (and writes its report)
    assert data_tree(world) == before


def test_apply_failure_restores_everything(world, monkeypatch):
    before = data_tree(world)
    calls = []

    def flaky(tmp, path):
        calls.append(path)
        if len(calls) == 2:
            raise OSError("disk went away")
        os.replace(tmp, path)
    monkeypatch.setattr(mz, "_replace", flaky)
    with pytest.raises(md.ApplyFailed, match="restored from"):
        fix(world, apply=True, expect=2)
    assert data_tree(world) == before
    monkeypatch.setattr(mz, "_replace", lambda tmp, path: (_ for _ in ()).throw(OSError("still broken")))
    assert cli.main(["fix-rescale", "--folder", world, "--apply", "--expect", "2"]) == 1
    assert data_tree(world) == before


@pytest.mark.parametrize("apply", [False, True])
def test_refusals(world, capsys, monkeypatch, apply):
    args = ["fix-rescale", "--folder", world] + (["--apply", "--expect", "2"] if apply else [])
    before = data_tree(world)
    lock = RunLock(mp.lock_path(world)).acquire("equity_minute.py update")
    try:
        assert cli.main(args) == 2
        assert "Not run" in capsys.readouterr().err
    finally:
        lock.release()

    def busy(*a, **k):
        raise mp.StoreBusy("an input build is running")
    monkeypatch.setattr(mp, "Inputs", busy)
    assert cli.main(args) == 2
    assert data_tree(world) == before


def test_cli(world, capsys):
    assert cli.main(["fix-rescale", "--folder", world]) == 0
    assert "to fix: 2 rows in 2 stocks" in capsys.readouterr().out
    assert cli.main(["fix-rescale", "--folder", world, "--apply", "--expect", "2"]) == 0
    assert cli.main(["fix-rescale", "--folder", world, "--expect", "0"]) == 0


# ---- P81 ----

def test_zeropad_refused_after_on_a_stock_without_fixes(world, monkeypatch):
    """P81: a stock whose PADDING rows are all held still gets its `checks` refusal reported."""
    real = md.Calc.bhav_of
    g1 = sid(world, "GEN1")

    def with_session(self, s):                         # a pre-existing difference (as P80)
        b = real(self, s)
        if s == g1:
            b = pd.concat([b, b[b["date"] == DAYS[FRIDAY]].assign(date=EXTRA)]).sort_values("date")
        return b.reset_index(drop=True)
    monkeypatch.setattr(md.Calc, "bhav_of", with_session)
    res = mz.fix(world, log=q)
    assert ("GEN1", str(DAYS[45])) in {(x["symbol"], x["date"]) for x in res["held"]}
    assert g1 not in {x["security_id"] for x in res["rows"]}
    assert g1 in res["refused_before"] and g1 in res["refused_after"]

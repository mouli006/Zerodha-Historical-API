"""The two P173 guards (docs/dev/equity_minute_splitblip.md): (a) levels() / volume_levels() keep a one-day break on
the day before a stored split / bonus (md.split_blip_rows); (b) repair holds a row stored OK that its new candle would
give HIGHLOW_DIFF / CLOSE_DIFF, and writes its log file. Synthetic data in tmp_path, no network, .env stubbed."""
import os
import shutil
from datetime import date

import numpy as np
import pandas as pd
import pytest

import config
from backfill import fetch
from backfill.equity import minute_download as md
from backfill.equity import minute_repair as mr
from backfill.equity import minute_store as ms
from test_equity_minute_download import (DAYS, FAST, N, FakeKite, FastStop, days_of, done_copy,  # noqa: F401
                                         downloaded, no_network, planned, q, sid)
from test_equity_minute_finished_days import clean, rp, tree  # noqa: F401
from test_minute_splitblip import PRE, BlipKite

# ---- levels() / volume_levels() ----


def series(n=40, at=None, value=0.1):
    v = np.ones(n) * 1.0002
    if at is not None:
        v[at] = value
    return v


def mask(n, *pos):
    m = np.zeros(n, bool)
    m[list(pos)] = True
    return m


def test_levels_keep_a_one_day_break_before_an_ex_date():
    v = series(at=20)
    assert md.levels(v, md.SCALE_STEP)[20] == pytest.approx(1.0002)                  # without keep: a blip
    out = md.levels(v, md.SCALE_STEP, keep=mask(40, 20))
    assert out[20] == 0.1 and np.allclose(np.delete(out, 20), 1.0002)
    assert np.array_equal(md.levels(v, md.SCALE_STEP, keep=mask(40, 7)), md.levels(v, md.SCALE_STEP))  # not there


def test_levels_without_keep_unchanged():
    rng = np.random.default_rng(3)
    v = np.concatenate([np.full(30, 1.0), np.full(20, 1.00945), np.full(25, 0.2)]) * (1 + rng.normal(0, 2e-4, 75))
    v[[5, 40]] = np.nan
    base = md.levels(v, md.SCALE_STEP)
    assert np.array_equal(md.levels(v, md.SCALE_STEP, keep=None), base)
    assert np.array_equal(md.levels(v, md.SCALE_STEP, keep=np.zeros(75, bool)), base)
    assert np.array_equal(md.levels(v, md.SCALE_STEP, keep=mask(75, 10, 35, 60)), base)   # multi-day levels: no move
    assert md.levels(np.full(5, np.nan), md.SCALE_STEP, keep=mask(5, 2)) is None


def test_volume_levels_keep():
    vr = series(at=20, value=10.05) * 1.005
    assert md.volume_levels(vr)[20] == 1.0
    out = md.volume_levels(vr, keep=mask(40, 20))
    assert out[20] == pytest.approx(10.05 / 1.0002, rel=1e-4) and np.all(np.delete(out, 20) == 1.0)
    assert np.array_equal(md.volume_levels(vr, keep=None), md.volume_levels(vr))


# ---- split_blip_rows ----

D = [date(2015, 3, d) for d in (2, 3, 4, 5, 6, 9, 10, 11, 12, 13)]
CAL = D + [date(2015, 3, 16), date(2015, 3, 17)]
# S1: ex-dates 03-05 (x2) and 03-11 (x2.5): cumulative 5 / 2.5 / 1
PERIODS = {"S1": ([date(2015, 1, 1), D[3], D[7]], [D[2], D[6], date(2026, 11, 6)], [5.0, 2.5, 1.0]),
           "S2": ([date(2015, 1, 1), CAL[10]], [D[9], date(2026, 11, 6)], [4.0, 1.0])}
LATE = "2026-10-20 01:00:00"


def run(sid_, i, measured, ratio, fetched=LATE, dates=D):
    n = len(dates)
    m, r, f = np.ones(n), np.ones(n), np.array([LATE] * n, dtype=object)
    m[i], r[i], f[i] = measured, ratio, fetched
    return md.split_blip_rows(sid_, dates, m, r, f, PERIODS, CAL)


def test_split_blip_ratio_equals_c_with_a_later_split():
    keep, F, C = run("S1", 2, 0.5, 5.0)                    # AJANTPHARM-like: ratio = C = 5, F = 2
    assert keep[2] and F[2] == pytest.approx(2.0) and C[2] == pytest.approx(5.0) and keep.sum() == 1
    keep, F, C = run("S1", 6, 0.4, 2.5)                    # JSWDULUX-like: C = F = 2.5
    assert keep[6] and F[6] == pytest.approx(2.5) and C[6] == pytest.approx(2.5)


def test_split_blip_ratio_equals_f_only():
    keep, F, C = run("S1", 2, 0.5, 2.0)                    # MENONBE-like: Kite never applied the later one
    assert keep[2] and C[2] == pytest.approx(5.0)


def test_split_blip_not_kept():
    assert not run("S1", 2, 0.5, 3.3)[0].any()             # ratio neither C nor F
    assert not run("S1", 2, 1.0, 5.0)[0].any()             # day and minutes adjusted together (GNRL)
    assert not run("S1", 1, 0.5, 5.0)[0].any()             # no adjustment on the next day
    keep, F, C = run("S1", 2, 0.5, 2.0, fetched="2015-03-04 20:00:00")   # fetched on the day: C = 1, ratio = F
    assert keep[2] and C[2] == 1.0
    assert not run("S1", 2, 0.5, 1.6, fetched="2015-03-04 20:00:00")[0].any()   # ratio neither 1 nor 2
    keep, F, C = run("S1", 2, 0.5, 2.0, fetched="")        # no fetch time: C unknown, F still checked
    assert keep[2] and np.isnan(C[2])
    gap = D[:3] + D[4:]                                    # the stock has no row on the ex-date: never
    assert not run("S1", 2, 0.5, 5.0, dates=gap)[0].any()
    assert not run("S9", 2, 0.5, 5.0)[0].any()             # no factors


def test_split_blip_last_row_uses_the_market_calendar():
    keep, F, _ = run("S2", 9, 0.25, 4.0)                   # the repair / update case: the ex-date is not stored yet
    assert keep[9] and F[9] == pytest.approx(4.0)
    assert not run("S2", 8, 0.25, 4.0)[0].any()


# ---- guard (a) in a download ----

@pytest.fixture(scope="module")
def guarded(planned, tmp_path_factory):
    f = str(tmp_path_factory.mktemp("splitblip_guarded") / "data")
    shutil.copytree(planned, f)
    mpatch = pytest.MonkeyPatch()
    mpatch.setattr(config, "load_env", lambda: {})
    mpatch.setattr(fetch, "STOP", FastStop())
    md.download(f, kite=BlipKite(), speed=FAST, workers=1, log=q)
    mpatch.undo()
    return f


def test_download_keeps_the_break_before_the_ex_date(guarded):
    s = days_of(guarded, "SPLITCO")
    row = s.loc[DAYS[PRE]]
    assert row["minute_scale"] == pytest.approx(0.2, rel=1e-3) and row["check"] == "OK"
    assert row["minute_volume_scale"] == pytest.approx(5.0, rel=0.03)
    assert "corporate action" in row["note"] and s.loc[DAYS[PRE + 1], "minute_scale"] == 1.0
    g1 = days_of(guarded, "GEN1")                          # the same break without an ex-date: still a blip
    assert g1.loc[DAYS[30], "minute_scale"] == 1.0 and "HIGHLOW_DIFF" in g1.loc[DAYS[30], "check"]


# ---- guard (b) in repair ----

class FlagKite(FakeKite):
    """FakeKite whose final day candle of BIG on DAYS[N-1] has a high under the minute high (HIGHLOW_DIFF) and of MID
    three times the volume (VOLUME_DIFF)."""

    def historical_data(self, token, start, end, interval, continuous=False, oi=False):
        out = super().historical_data(token, start, end, interval, continuous, oi)
        sym = self.by_token[token]
        if interval != "day":
            return out
        res = []
        for c in out:
            if c["date"].date() == DAYS[N - 1] and sym == "BIG":
                c = {**c, "high": round(c["high"] * 0.95, 2)}
            if c["date"].date() == DAYS[N - 1] and sym == "MID":
                c = {**c, "volume": c["volume"] * 3}
            res.append(c)
        return res


def logs(f):
    p = os.path.join(f, mr.LOG_DIR)
    return sorted(os.listdir(p)) if os.path.isdir(p) else []


def test_repair_holds_ok_to_price_flag(clean):
    f = clean
    d = DAYS[N - 1]
    before = {s: days_of(f, s) for s in ("BIG", "MID")}
    assert before["BIG"].loc[d, "check"] == "OK" and before["MID"].loc[d, "check"] == "OK"
    files = tree(f)
    res = rp(f, FlagKite())                                # dry run
    assert tree(f) == files                                # nothing in equity_1min
    assert [(s, x, o, n) for s, _, x, o, n in res["held"]] == [("BIG", d, "OK", "HIGHLOW_DIFF")]
    assert {s for s, _, _ in res["repaired"]} == {"MID", "GEN1"}
    assert res["after"].get("VOLUME_DIFF") == 1                         # OK -> VOLUME_DIFF: repaired as before
    assert "Held (stored OK, the new candle gives HIGHLOW_DIFF / CLOSE_DIFF" in res["report"]
    assert "BIG" in res["report"] and "OK -> HIGHLOW_DIFF" in res["report"]
    [log] = logs(f)
    assert log.startswith("repair_") and log.endswith("_dryrun.log")
    with open(os.path.join(f, mr.LOG_DIR, log), encoding="utf-8") as fh:
        text = fh.read()
    assert "Rows found (dry run, nothing written)" in text and "OK -> HIGHLOW_DIFF" in text

    res = rp(f, FlagKite(), apply=True)
    pd.testing.assert_frame_equal(days_of(f, "BIG"), before["BIG"])    # held: the old row kept, file unchanged
    assert days_of(f, "MID").loc[d, "check"] == "VOLUME_DIFF"
    rep = pd.read_csv(os.path.join(res["backup"], "repair.csv"), dtype=str, keep_default_na=False)
    held = rep[rep["result"] == "HELD"]
    assert list(held["symbol"]) == ["BIG"] and list(held["new_check"]) == ["HIGHLOW_DIFF"]
    assert "the old row kept" in held["note"].iat[0]
    assert len(logs(f)) == 2 and [n for n in logs(f) if not n.endswith("_dryrun.log")]
    assert sid(f, "BIG") in mr.find_affected(f)[0]                    # still provisional: a later run tries again
    res = rp(f, FakeKite(), apply=True)                                # OK -> OK: replaced as before
    assert [s for s, _, _ in res["repaired"]] == ["BIG"] and res["held"] == []
    assert days_of(f, "BIG").loc[d, "check"] == "OK"


def test_repair_log_written_when_nothing_to_do(done_copy):
    res = rp(done_copy, FakeKite())
    assert "Nothing to repair" in res["report"]
    [log] = logs(done_copy)
    with open(os.path.join(done_copy, mr.LOG_DIR, log), encoding="utf-8") as fh:
        assert "Nothing to repair." in fh.read()

"""retoken for the stocks the probe found no day candles for (backfill/equity/minute_download.py: probe_empty,
reprobe, apply_recovered): PROBE_RECOVERED / NO_KITE_DATA / REVIEW, NO_TOKEN listings left out, the dry run writing
nothing, an apply that changes only that stock's plan rows (built in a temp folder) and restores every file on an
error. The phase 2 synthetic world and fake Kite of tests/test_equity_minute_download.py; no network, .env never
read."""
import os
import shutil
import sqlite3
from datetime import datetime

import pandas as pd
import pyarrow.parquet as pq
import pytest

import config
from backfill import fetch
from backfill.equity import minute_download as md
from backfill.equity import minute_plan as mp
from test_equity_minute_download import DAYS, FAST, N, FastStop, FakeKite, build_inputs, no_network, q  # noqa: F401
from test_equity_minute_fixes import FixKite, results, tree_hashes

EMPTY_TOKEN = 110                      # GEN1: the probe got no day candles for it
NOW = datetime.combine(DAYS[N - 1], datetime.min.time()).replace(hour=15, tzinfo=mp.IST)   # the last bhavcopy day
FINISHED = DAYS[N - 2]                 # ... so the price check uses the day before


@pytest.fixture(scope="module")
def empty_world(tmp_path_factory):
    """Phase 1 with GEN3 gone from Kite's list (ACTIVE, NO_TOKEN) and no day candles for GEN1's token."""
    f = str(tmp_path_factory.mktemp("probe_empty") / "data")
    mpatch = pytest.MonkeyPatch()
    mpatch.setattr(config, "load_env", lambda: {})
    mpatch.setattr(fetch, "STOP", FastStop())
    build_inputs(f, N - 1)
    kite = FixKite(gone={"GEN3"}, no_day_tokens={EMPTY_TOKEN})
    mp.tokens(f, kite=kite, now=NOW, log=q)
    mp.probe(f, kite=kite, speed=FAST, now=NOW, log=q)
    mpatch.undo()
    return f


@pytest.fixture
def ew(empty_world, tmp_path):
    f = str(tmp_path / "data")
    shutil.copytree(empty_world, f)
    return f


def sid_of(f, sym):
    plan = pd.read_csv(md.plan_path(f), dtype=str, keep_default_na=False)
    return plan.loc[plan["symbols"].str.contains(sym), "security_id"].iat[0]


def plan_row(f, sid):
    plan = pd.read_csv(md.plan_path(f), dtype=str, keep_default_na=False)
    return plan[plan["security_id"] == sid].iloc[0]


def plan_hashes(f):
    return tree_hashes(mp.plan_dir(f))


def csv_rows(path, sid=None, keep=False):
    """The file's lines (header kept) of every other stock (keep=False) or of `sid` only."""
    with open(path, encoding="utf-8") as fh:
        lines = fh.read().splitlines()
    head, body = lines[0], lines[1:]
    col = head.split(",").index("security_id")
    return [head] + [ln for ln in body if (ln.split(",")[col] == sid) == keep]


def test_world_has_one_probe_empty_stock(empty_world):
    f = empty_world
    gen1, gen3 = sid_of(f, "GEN1"), sid_of(f, "GEN3")
    r = plan_row(f, gen1)
    assert (r["download_minute"], r["eod_first_day"], r["minute_first_day"]) == ("N", "", "")
    assert plan_row(f, gen3)["match_status"] == "NO_TOKEN"
    gaps = pd.read_csv(os.path.join(mp.plan_dir(f), "kite_gaps.csv"), dtype=str, keep_default_na=False)
    assert ((gaps["security_id"] == gen1) & (gaps["reason"] == "NO_KITE_DATA")).any()
    found = md.probe_empty(f, mp.Inputs(f, bhav=False))
    assert found == {gen1: EMPTY_TOKEN}                 # GEN3 (NO_TOKEN) is not a candidate


def test_recovered_dry_run_writes_nothing(ew):
    before = tree_hashes(ew)
    logs = []
    res = md.retoken(ew, kite=FixKite(gone={"GEN3"}), speed=FAST, now=NOW, log=lambda *a, **k: logs.append(a[0]))
    r = results(res)["GEN1"]
    assert r["result"] == md.PROBE_RECOVERED
    assert r["price_check"].startswith("OK 0.00%") and str(FINISHED) in r["price_check"]
    assert r["old_token"] == r["new_token"] == str(EMPTY_TOKEN)
    assert "GEN3" not in results(res)                   # NO_TOKEN listing: not a plan stock, not probe-empty
    assert any("PROBE_RECOVERED" in line for line in logs)
    assert tree_hashes(ew) == before


def test_recovered_apply_makes_it_downloadable(ew):
    gen1 = sid_of(ew, "GEN1")
    pdir = mp.plan_dir(ew)
    before = {n: open(os.path.join(pdir, n), "rb").read() for n in
              ("kite_gaps.csv", "index_coverage.csv", "build_report.txt", "build_info.json")}
    others = {n: csv_rows(os.path.join(pdir, n), gen1) for n in ("plan.csv", "kite_adjustment.csv", "eod_mismatch.csv")}
    kvb_old = pq.read_table(os.path.join(pdir, "kite_vs_bhav.parquet")).to_pandas()
    assert not (kvb_old["security_id"] == gen1).any()

    res = md.retoken(ew, kite=FixKite(gone={"GEN3"}), apply=True, speed=FAST, now=NOW, log=q)
    assert results(res)["GEN1"]["result"] == md.PROBE_RECOVERED

    r = plan_row(ew, gen1)
    assert r["download_minute"] == "Y" and r["download_eod"] == "Y"
    assert r["eod_first_day"] == str(DAYS[0]) and r["minute_first_day"] == str(mp.KITE_MINUTE_FROM)
    assert r["match_status"] == "MATCHED"
    # every other stock's rows byte-identical; the files phase 1's gaps writes not touched
    for n, rows in others.items():
        assert csv_rows(os.path.join(pdir, n), gen1) == rows
    for n, data in before.items():
        assert open(os.path.join(pdir, n), "rb").read() == data
    kvb = pq.read_table(os.path.join(pdir, "kite_vs_bhav.parquet")).to_pandas()
    pd.testing.assert_frame_equal(kvb[kvb["security_id"] != gen1].reset_index(drop=True), kvb_old)
    assert (kvb["security_id"] == gen1).sum() > 0
    # the probe's own outputs, and a backup of every target
    assert len(pq.read_table(os.path.join(pdir, "kite_day", f"{gen1}.parquet"))) > 0
    p = mp.read_probes(ew)[gen1]
    assert p["eod_first_day"] == str(DAYS[0]) and p["price_status"] == "MATCHED" and p["price_day"] == str(FINISHED)
    backups = [d for d in os.listdir(os.path.join(md.out_dir(ew), "archive")) if d.startswith("plan_retoken_")]
    assert len(backups) == 1
    assert os.path.isfile(os.path.join(md.out_dir(ew), "archive", backups[0], "plan.csv"))
    assert not [n for n in os.listdir(pdir) if n.endswith(".tmp")]
    # the progress db is not touched for it; download now picks it up
    assert gen1 not in md.read_progress(ew)[1]
    assert gen1 in md.read_plan(ew)
    md.download(ew, kite=FixKite(gone={"GEN3"}), symbols=["GEN1"], speed=FAST, workers=1, log=q)
    assert md.read_progress(ew)[1][gen1]["status"] == "DONE"
    # the next gaps run drops its NO_KITE_DATA gap row
    mp.gaps(ew, now=NOW, log=q)
    gaps = pd.read_csv(os.path.join(pdir, "kite_gaps.csv"), dtype=str, keep_default_na=False)
    assert not ((gaps["security_id"] == gen1) & (gaps["reason"] == "NO_KITE_DATA")).any()


def test_still_empty_stays_no_kite_data(ew):
    before = tree_hashes(ew)
    kite = FixKite(gone={"GEN3"}, no_day_tokens={EMPTY_TOKEN})
    res = md.retoken(ew, kite=kite, speed=FAST, now=NOW, log=q)
    r = results(res)["GEN1"]
    assert r["result"] == md.NO_KITE and "stays N" in r["action"]
    assert tree_hashes(ew) == before
    plan_before = plan_hashes(ew)
    md.retoken(ew, kite=kite, apply=True, speed=FAST, now=NOW, log=q)
    assert plan_hashes(ew) == plan_before                 # no plan file written
    gen1 = sid_of(ew, "GEN1")
    assert gen1 not in md.read_progress(ew)[1]           # not marked NO_KITE_DATA in the progress db
    assert not os.path.isdir(os.path.join(md.out_dir(ew), "archive"))


def test_price_check_fails_review_not_applied(ew):
    kite = FixKite(gone={"GEN3"}, div={EMPTY_TOKEN: 1.5})
    res = md.retoken(ew, kite=kite, speed=FAST, now=NOW, log=q)
    r = results(res)["GEN1"]
    assert r["result"] == "REVIEW" and r["price_check"].startswith("OFF") and res["review"] >= 1
    plan_before = plan_hashes(ew)
    md.retoken(ew, kite=kite, apply=True, speed=FAST, now=NOW, log=q)
    assert plan_hashes(ew) == plan_before
    assert plan_row(ew, sid_of(ew, "GEN1"))["download_minute"] == "N"


def test_apply_builds_outside_the_data_folder(ew, monkeypatch):
    before = plan_hashes(ew)
    seen = []
    real = md._replace

    def spy(tmp, path):
        if not seen:
            seen.append({k: v for k, v in plan_hashes(ew).items() if not k.endswith(".tmp")})
        real(tmp, path)
    monkeypatch.setattr(md, "_replace", spy)
    md.retoken(ew, kite=FixKite(gone={"GEN3"}), apply=True, speed=FAST, now=NOW, log=q)
    assert seen and seen[0] == before                     # nothing in the plan folder changed before the replace


def test_error_mid_apply_restores_every_file(ew, monkeypatch):
    before = plan_hashes(ew)
    calls = []
    real = md._replace

    def flaky(tmp, path):
        calls.append(path)
        if len(calls) == 3:
            raise OSError("disk went away")
        real(tmp, path)
    monkeypatch.setattr(md, "_replace", flaky)
    with pytest.raises(md.ApplyFailed, match="restored from"):
        md.retoken(ew, kite=FixKite(gone={"GEN3"}), apply=True, speed=FAST, now=NOW, log=q)
    assert len(calls) == 3                                # two targets were already replaced
    assert plan_hashes(ew) == before                      # ... and are back; no .tmp left
    assert os.listdir(os.path.join(md.out_dir(ew), "archive"))
    assert not os.path.isdir(md.update_dir(ew)) or not [n for n in os.listdir(md.update_dir(ew))
                                                        if n.startswith("retoken_")]
    db = sqlite3.connect(os.path.join(mp.plan_dir(ew), "probe_progress.sqlite"))
    try:
        eod = db.execute("SELECT eod_first_day FROM probes WHERE security_id = ?", (sid_of(ew, "GEN1"),)).fetchone()
    finally:
        db.close()
    assert eod == ("",)

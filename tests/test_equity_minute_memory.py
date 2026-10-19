"""Memory of the phase 2 download (backfill/equity/minute_download.py): the bhavcopy is read per stock from
bhav_cache (never whole), builds and the check rebuild go stock by stock, so peak memory does not grow with the
number of stocks; runs left 'running' by a killed process become ABORTED. Synthetic worlds in tmp_path, a fake Kite,
no network, .env never read."""
import gc
import os
import shutil
import sqlite3
import time
import tracemalloc
from datetime import date, datetime, time as dtime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import config
import equity_minute as cli
from backfill import fetch
from backfill.equity import adjustments as adj
from backfill.equity import exits as ex
from backfill.equity import index_membership as im
from backfill.equity import minute_download as md
from backfill.equity import minute_plan as mp
from backfill.equity import security_master as sm
from backfill.locks import RunLock
from test_equity_minute_download import (CA_HEADER, COLUMNS, FAST, HEADERS, FakeKite, FastStop, dl, no_network,  # noqa: F401
                                         planned, q, weekdays, world)

D = 400                                   # trading days: 2015-01-01 .. mid 2016 (two year files, 7 minute chunks)
MDAYS = weekdays(date(2015, 1, 1), D)
MNOW = datetime(2016, 7, 30, 10, 0, tzinfo=mp.IST)


def plain(n):
    """n plain stocks; every 10th day has a day candle but no minutes (a MINUTE_EMPTY problem row)."""
    return {f"PLN{k:03d}": {"isin": f"INE{k:03d}B01011", "token": 1000 + k, "level": 100.0 + k} for k in range(n)}


def bar(s, i):
    c = round(s["level"] + i / 10, 2)
    return {"open": round(c * 0.99, 2), "high": round(c * 1.02, 2), "low": round(c * 0.98, 2), "close": c,
            "volume": 10_000 + i}


def build_world(folder, stocks):
    store = os.path.join(folder, "NSE_CM_bhav")
    os.makedirs(store, exist_ok=True)
    rows = [{"date": d, "symbol": sym, "series": "EQ", "isin": s["isin"], **bar(s, i), "last": bar(s, i)["close"],
             "prev_close": bar(s, i)["close"], "turnover": 1e7 * (1 + s["level"]), "trades": 1, "fin_id": "1",
             "format": "old"} for sym, s in stocks.items() for i, d in enumerate(MDAYS)]
    df = pd.DataFrame(rows, columns=COLUMNS)
    old = time.time() - 600
    for y, g in df.groupby(df["date"].map(lambda d: d.year)):
        p = os.path.join(store, f"{y}.parquet")
        pq.write_table(pa.Table.from_pandas(g, preserve_index=False), p)
        os.utime(p, (old, old))
    sm.build(folder, log=q)
    ca = os.path.join(folder, "adjustments", "input")
    os.makedirs(ca, exist_ok=True)
    pd.DataFrame(columns=CA_HEADER).to_csv(os.path.join(ca, "ca.csv"), index=False)
    adj.build(folder, log=q)
    ref = os.path.join(store, "reference")
    os.makedirs(ref, exist_ok=True)
    for name, cols in HEADERS.items():
        pd.DataFrame(columns=cols).to_csv(os.path.join(ref, name), index=False, lineterminator="\n")
    ex.build(folder, log=q)
    out = im.out_dir(folder)
    os.makedirs(out, exist_ok=True)
    first = list(stocks)[:2]
    mem = pd.DataFrame([{"index": "NIFTY 50", "security_id": sm.security_id_for(s, MDAYS[5], folder), "symbol": s,
                         "isin": stocks[s]["isin"], "from_date": MDAYS[0], "to_date": None, "source": "test"}
                        for s in first], columns=im.MEMBERSHIP_COLUMNS)
    pq.write_table(pa.Table.from_pandas(mem, preserve_index=False), os.path.join(out, "nifty_membership.parquet"))
    pq.write_table(pa.Table.from_pandas(pd.DataFrame(columns=im.AMFI_COLUMNS).astype(str), preserve_index=False),
                   os.path.join(out, "amfi.parquet"))
    cov = {i: {"first": None, "last": None, "why": "no current list"} for i in im.INDEXES}
    cov["NIFTY 50"] = {"first": str(MDAYS[0]), "last": str(MDAYS[-1]), "reliable_from": str(MDAYS[0])}
    mp.write_json(os.path.join(out, "build_info.json"), {"coverage": cov})


class PlainKite:
    """Kite for plain(): day candles = the bhavcopy, three minute bars per day (none on every 10th day)."""

    def __init__(self, stocks):
        self.by_token = {s["token"]: (sym, s) for sym, s in stocks.items()}
        self.requests = 0

    def instruments(self, exchange):
        return [FakeKite.inst(sym, s["token"]) for sym, s in self.by_token.values()]

    def historical_data(self, token, start, end, interval, continuous=False, oi=False):
        self.requests += 1
        sym, s = self.by_token[token]
        out = []
        for i, d in enumerate(MDAYS):
            if not start.date() <= d <= end.date():
                continue
            b = bar(s, i)
            if interval == "day":
                out.append({"date": datetime.combine(d, dtime()).replace(tzinfo=mp.IST), **b})
            elif d >= mp.KITE_MINUTE_FROM and i % 10:
                v = int(b["volume"] * 0.33)
                for t, o, h, lo, c in ((dtime(9, 15), b["open"], b["high"], b["open"], b["close"]),
                                       (dtime(12, 0), b["close"], b["close"], b["low"], b["close"]),
                                       (dtime(15, 29), b["close"], b["close"], b["close"], b["close"])):
                    out.append({"date": datetime.combine(d, t).replace(tzinfo=mp.IST), "open": o, "high": h,
                                "low": lo, "close": c, "volume": v})
        return out


def plain_world(folder, n):
    stocks = plain(n)
    build_world(folder, stocks)
    kite = PlainKite(stocks)
    mp.tokens(folder, kite=kite, now=MNOW, log=q)
    mp.probe(folder, kite=kite, speed=FAST, now=MNOW, log=q)
    return kite


def peak_of_download(folder, kite):
    """tracemalloc peak (bytes) of a full download with two workers, including the check rebuild."""
    gc.collect()
    tracemalloc.start()
    try:
        res = md.download(folder, kite=kite, speed=FAST, workers=2, log=q)
        peak = tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()
    return res, peak


# ---- memory ----

def test_peak_memory_does_not_grow_with_stocks(tmp_path, monkeypatch):
    peaks = {}
    for n in (10, 40):
        f = str(tmp_path / f"w{n}")
        kite = plain_world(f, n)
        res, peaks[n] = peak_of_download(f, kite)
        st = res["stats"]
        assert st["built"] == n and st["failed_chunks"] == 0 and not st["stopped"]
        summary = pd.read_json(os.path.join(md.check_dir(f), "summary.json"), typ="series")
        assert summary["stocks"] == n
        empty = summary["checks"]["MINUTE_EMPTY"]
        assert empty > 0 and empty % n == 0                                 # every stock lacks the same days
        assert len(pd.read_csv(os.path.join(md.check_dir(f), "minute_check.csv"))) >= empty
    # 4x the stocks (and 4x the bhavcopy rows and problem rows): the peak stays within 15% (+0.5 MB of noise);
    # loading the whole bhavcopy and keeping every problem row grows it by far more
    assert peaks[40] <= peaks[10] * 1.15 + 500_000, peaks


def test_disk_used_skips_files_removed_meanwhile(tmp_path, monkeypatch):
    f = str(tmp_path)
    for sub, name in (("data", "a.parquet"), ("staging", "gone.parquet")):
        os.makedirs(os.path.join(md.out_dir(f), sub, "S000001"))
        with open(os.path.join(md.out_dir(f), sub, "S000001", name), "wb") as fh:
            fh.write(b"x" * 10)
    real = os.path.getsize

    def size(p):
        if p.endswith("gone.parquet"):                    # the other worker removed the staging after its build
            raise FileNotFoundError(p)
        return real(p)
    monkeypatch.setattr(os.path, "getsize", size)
    assert md.disk_used(f) == 10


def test_memory_helper():
    now, peak = md.memory()
    assert 0 < now <= peak


# ---- the bhavcopy cache ----

def test_cache_rows_equal_read_bhav(world):
    inp = mp.Inputs(world)
    logs = []
    paths = md.bhav_cache(world, inp, log=lambda *a, **k: logs.append(" ".join(map(str, a))))
    assert [os.path.basename(p) for p in paths] == ["2015.parquet"] and "1 year file(s) rebuilt (2015)" in logs[0]
    cols = ["date", "symbol", "series", "open", "high", "low", "close", "volume", "turnover"]
    for sid, g in inp.bhav.groupby("security_id"):
        got = md.read_cached_bhav(paths, sid)
        pd.testing.assert_frame_equal(got[cols].reset_index(drop=True), g[cols].reset_index(drop=True),
                                      check_dtype=False)
    assert md.read_cached_bhav(paths, "S999999") is None
    assert md.turnover_medians(world, inp) == pytest.approx(inp.bhav.groupby("security_id")["turnover"].median()
                                                            .to_dict())
    logs.clear()
    md.bhav_cache(world, inp, log=lambda *a, **k: logs.append(a[0]))
    assert logs == []                                                        # up to date: nothing rebuilt
    store = os.path.join(world, "NSE_CM_bhav", "2015.parquet")
    os.utime(store, None)                                                    # the year file changed
    md.bhav_cache(world, inp, log=lambda *a, **k: logs.append(a[0]))
    assert logs == ["Bhavcopy cache: 1 year file(s) rebuilt (2015)."]


def test_cache_rebuilds_only_changed_years(tmp_path):
    f = str(tmp_path / "w")
    build_world(f, plain(3))
    inp = mp.Inputs(f, bhav=False)
    assert inp.bhav is None and mp.Inputs(f).bhav is not None               # phase 1's default is unchanged
    logs = []
    md.bhav_cache(f, inp, log=logs.append)
    assert logs == ["Bhavcopy cache: 2 year file(s) rebuilt (2015, 2016)."]
    os.utime(os.path.join(f, "NSE_CM_bhav", "2016.parquet"), None)
    logs.clear()
    md.bhav_cache(f, inp, log=logs.append)
    assert logs == ["Bhavcopy cache: 1 year file(s) rebuilt (2016)."]


def test_dry_run_writes_no_cache(world):
    md.download(world, dry_run=True, log=q)
    assert not os.path.exists(md.cache_dir(world))


# ---- ABORTED runs ----

def test_running_row_without_process_marked_aborted(world, capsys):
    db = md.Progress(world)
    db.start_run("killed-run", "download")
    db.close()
    assert "no live process: the next download / update marks it ABORTED" in md.status(world, log=q)["report"]
    lock = RunLock(mp.lock_path(world)).acquire("test")
    try:
        assert "(running now," in md.status(world, log=q)["report"]
    finally:
        lock.release()
    md.download(world, dry_run=True, log=q)                                 # no lock: nothing marked
    assert md.read_progress(world)[2][0]["result"] == "running"
    logs = []
    dl(world, FakeKite(), symbols=["GEN1"], log=lambda *a, **k: logs.append(" ".join(map(str, a))))
    assert "Marked ABORTED (left 'running' by a stopped process): killed-run" in logs
    runs = {r["run_id"]: r for r in md.read_progress(world)[2]}
    assert runs["killed-run"]["result"] == "ABORTED" and runs["killed-run"]["ended"] == ""
    last = [r for r in runs.values() if r["run_id"] != "killed-run"][0]
    assert last["result"] == "done" and last["peak_mb"] > 0
    st = md.status(world, log=q)
    assert st["aborted"] == 1 and "ABORTED runs (process killed, e.g. low memory): 1 (killed-run)" in st["report"]
    assert "peak memory" in st["report"]
    assert cli.main(["status", "--folder", world]) == 0
    assert "ABORTED runs" in capsys.readouterr().out


def test_update_marks_aborted_too(world):
    db = md.Progress(world)
    db.start_run("killed-update", "update")
    db.close()
    with pytest.raises(md.NoBhavToday):                                      # refused after the lock was taken
        md.update(world, kite=FakeKite(), now=datetime(2030, 1, 1, tzinfo=mp.IST), log=q)
    assert md.read_progress(world)[2][0]["result"] == "ABORTED"


def test_old_progress_file_gets_peak_column(world):
    os.makedirs(md.out_dir(world), exist_ok=True)
    path = os.path.join(md.out_dir(world), "download_progress.sqlite")
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE runs (run_id TEXT PRIMARY KEY, command TEXT, started TEXT, ended TEXT, "
                "requests INTEGER, n429 INTEGER, result TEXT)")
    con.execute("INSERT INTO runs VALUES ('r1','download','2026-10-19 02:01:33','',0,0,'running')")
    con.commit()
    con.close()
    assert "peak_mb" not in md.read_progress(world)[2][0]                   # status / dry run never alter it
    assert md.mark_aborted(world, log=q) == ["r1"]
    assert md.read_progress(world)[2][0]["peak_mb"] is None


def test_stale_plan_rebuild_logs_memory(world):
    old = time.time() - 3600
    os.utime(md.plan_path(world), (old, old))
    logs = []
    dl(world, FakeKite(), symbols=["GEN1"], log=lambda *a, **k: logs.append(" ".join(map(str, a))))
    assert any("rebuilt offline (gaps)" in x for x in logs)
    assert any(x.startswith("  memory:") and "after freeing" in x for x in logs)
    assert md.plan_stale(world) is None

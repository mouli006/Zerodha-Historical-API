"""backfill/equity/minute_download.py, minute_store.py, phase 1's gaps additions and equity_minute.py's download /
update / status on a synthetic world in tmp_path: a bhavcopy store, the master, factors, exit list and index files
built with the real builders, phase 1's tokens + probe (plan.csv), and a fake KiteConnect with day and 1-minute
candles. No network; .env is never read (config.load_env stubbed); every input is only read."""
import hashlib
import json
import os
import shutil
import threading
import time
from datetime import date, datetime, time as dtime, timedelta

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests
from kiteconnect import exceptions as kex

import config
import equity_minute as cli
from backfill import fetch
from backfill.equity import adjustments as adj
from backfill.equity import exits as ex
from backfill.equity import index_membership as im
from backfill.equity import minute_download as md
from backfill.equity import minute_plan as mp
from backfill.equity import minute_store as ms
from backfill.equity import security_master as sm
from backfill.job import STOP
from backfill.locks import RunLock

COLUMNS = ["date", "symbol", "series", "isin", "open", "high", "low", "close", "last", "prev_close", "volume",
           "turnover", "trades", "fin_id", "format"]


def weekdays(first, n):
    out, d = [], first
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


N = 80                       # trading days in the base world; the update test adds day N
DAYS = weekdays(date(2015, 1, 1), N + 1)
FEB2 = DAYS.index(date(2015, 2, 2))
SPLIT_DAY = 50
STEP_DAY = 50                # SCALECO's minute scale and VOLCO's minute volume step here
NO_MINUTES = 60              # Kite has day candles but no minutes for any stock (market-wide)
SESSION = 70                 # Kite has no day candle for any stock; minutes for all but GEN4
EXTRA = date(2015, 2, 28)    # a Saturday session Kite has and the bhavcopy store lacks
NOW = datetime(2015, 4, 25, 10, 0, tzinfo=mp.IST)
FAST = 1e6
FIRST_CHUNK = (date(2015, 2, 2), date(2015, 4, 2))


def r(a, b):
    return list(range(a, b))


STOCKS = {
    "BIG": dict(isin="INE101A01011", level=lambda i: 1000 + i, token=101, tv=5.0),
    "MID": dict(isin="INE102A01011", level=lambda i: 500 + i, token=102, tv=2.0),
    "PTWO": dict(isin="INE103A01011", level=lambda i: 300 + i, token=103, tv=3.0),
    "SCALECO": dict(isin="INE104A01011", level=lambda i: 200 + i, token=104, scale=1.01),
    "VOLCO": dict(isin="INE105A01011", level=lambda i: 150 + i, token=105, volume=0.5),
    "ADDCO": dict(isin="INE106A01011", level=lambda i: 100 + i, token=106, kite="add"),
    "SPLITCO": dict(isin="INE107A01011", level=lambda i: 5 * (100 + i) if i < SPLIT_DAY else 100 + i, token=107,
                    kite="split"),
    "EMPTYCO": dict(isin="INE108A01011", level=lambda i: 80 + i, token=108, no_min={30, 31, 36}, no_day={35, 36},
                    bad_high={40}, bad_close={42}, bad_vol={45}),
    "NEWNM": dict(isin="INE109A01011", level=lambda i: 90 + i, token=109,
                  symbol=lambda i: "OLDNM" if i < 30 else "NEWNM"),
    "GEN1": dict(isin="INE110A01011", level=lambda i: 40.0 + i / 10, token=110),
    "GEN2": dict(isin="INE111A01011", level=lambda i: 41.0 + i / 10, token=111),
    "GEN3": dict(isin="INE112A01011", level=lambda i: 42.0 + i / 10, token=112),
    "GEN4": dict(isin="INE113A01011", level=lambda i: 43.0 + i / 10, token=113, no_min={SESSION}),
    "GONE": dict(isin="INE114A01011", level=lambda i: 200.0, token=None, tv=50.0, last=10),
    "NEWIPO": dict(isin="INE115A01011", level=lambda i: 60.0, token=120, first=N),   # lists on the update day
}
HEADERS = {   # the real headers of the reference lists (as in tests/test_equity_minute_plan.py)
    "mergers.csv": ["circular_no", "circular_date", "department", "old_symbol", "old_company", "scheme_type",
                    "exit_expected", "new_company", "new_symbol", "swap_ratio", "new_per_old", "suspended_from",
                    "last_trading_date", "record_date", "reason_text", "corrected_by", "pdf_url", "source_file",
                    "needs_review", "review_note"],
    "manual_mergers.csv": ["old_symbol", "new_company", "new_symbol", "swap_ratio", "scheme_type", "exit_expected",
                           "note"],
    "voluntary_delisting.csv": ["list", "symbol", "isin", "company", "board", "delisting_date", "delisting_type",
                                "initial_notice_date", "final_notice_date", "exit_price", "source_file",
                                "needs_review", "review_note"],
    "delisting_circulars.csv": ["circular_no", "circular_date", "department", "subject", "delisting_type", "symbol",
                                "company", "effective_date", "pdf_url", "source_file", "needs_review", "review_note"],
    "compulsory_delisting.csv": ["order_date", "company", "symbol", "symbol_source", "pdf_url", "source_file",
                                 "needs_review", "review_note"],
    "suspensions.csv": ["circular_no", "circular_date", "department", "status", "symbol", "company", "effective_date",
                        "reason", "exit_expected", "reason_text", "cancels_circular", "corrected_by", "subject",
                        "pdf_url", "source_file", "needs_review", "review_note"],
}
CA_HEADER = ["SYMBOL", "COMPANY NAME", "SERIES", "PURPOSE", "FACE VALUE", "EX-DATE", "RECORD DATE",
             "BOOK CLOSURE START DATE", "BOOK CLOSURE END DATE"]


class FastStop:
    """backfill.fetch.STOP for tests: the real event, but its retry back-off returns at once."""

    def is_set(self):
        return STOP.is_set()

    def wait(self, _):
        return STOP.is_set()


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def refuse(*a, **k):
        raise AssertionError("a test tried to reach the network")
    monkeypatch.setattr(requests.Session, "request", refuse)
    monkeypatch.setattr(config, "load_env", lambda: {})
    monkeypatch.setattr(fetch, "STOP", FastStop())
    STOP.clear()
    yield
    STOP.clear()


def idx_of(sym, last_day):
    s = STOCKS[sym]
    return r(s.get("first", 0), min(s.get("last", last_day), last_day) + 1)


def raw(sym, i):
    c = round(float(STOCKS[sym]["level"](i)), 2)
    return {"open": round(c * 0.99, 2), "high": round(c * 1.1, 2), "low": round(c * 0.9, 2), "close": c,
            "volume": 1000 + i}


def store_rows(last_day):
    rows = []
    for sym, s in STOCKS.items():
        for i in idx_of(sym, last_day):
            b = raw(sym, i)
            rows.append({"date": DAYS[i], "symbol": s.get("symbol", lambda _: sym)(i), "series": "EQ",
                         "isin": s["isin"], **b, "last": b["close"], "prev_close": b["close"],
                         "turnover": s.get("tv", 1.0) * 1e7, "trades": 1, "fin_id": "1", "format": "old"})
    for i in r(0, last_day + 1):
        rows.append({"date": DAYS[i], "symbol": "FILLER", "series": "N1", "isin": "INE999Z01011", "open": 1.0,
                     "high": 1.0, "low": 1.0, "close": 1.0, "last": 1.0, "prev_close": 1.0, "volume": 1,
                     "turnover": 1.0, "trades": 1, "fin_id": "1", "format": "old"})
    return rows


def write_store(folder, last_day):
    store = os.path.join(folder, "NSE_CM_bhav")
    os.makedirs(store, exist_ok=True)
    path = os.path.join(store, "2015.parquet")
    pq.write_table(pa.Table.from_pandas(pd.DataFrame(store_rows(last_day), columns=COLUMNS), preserve_index=False),
                   path)
    old = time.time() - 600
    os.utime(path, (old, old))


def write_refs(folder):
    ref = os.path.join(folder, "NSE_CM_bhav", "reference")
    os.makedirs(ref, exist_ok=True)
    rows = {"mergers.csv": [{"circular_no": "C1", "old_symbol": "GONE", "old_company": "Gone Limited",
                             "scheme_type": "MERGER", "exit_expected": "Y", "new_company": "Big Limited",
                             "new_symbol": "BIG", "swap_ratio": "1 for 1", "last_trading_date": str(DAYS[10])}]}
    for name, cols in HEADERS.items():
        pd.DataFrame([{c: x.get(c, "") for c in cols} for x in rows.get(name, [])], columns=cols) \
            .to_csv(os.path.join(ref, name), index=False, lineterminator="\n")


def write_ca(folder):
    path = os.path.join(folder, "adjustments", "input")
    os.makedirs(path, exist_ok=True)
    pd.DataFrame([["SPLITCO", "SPLITCO LTD", "EQ", "Face Value Split From Rs 10 To Rs 2", "10",
                   f"{DAYS[SPLIT_DAY]:%d-%b-%Y}", "-", "-", "-"],
                  ["VOLCO", "VOLCO LTD", "EQ", "Dividend - Rs 5 Per Share", "10",          # explains VOLCO's step
                   f"{DAYS[STEP_DAY + 1]:%d-%b-%Y}", "-", "-", "-"]], columns=CA_HEADER) \
        .to_csv(os.path.join(path, "ca.csv"), index=False)


def write_index(folder):
    out = im.out_dir(folder)
    os.makedirs(out, exist_ok=True)
    sid = {s: sm.security_id_for(s, DAYS[5], folder) for s in ("BIG", "MID", "PTWO", "GONE")}
    rows = [("NIFTY 50", "BIG", DAYS[0], None), ("NIFTY 50", "MID", DAYS[0], None),
            ("NIFTY 50", "GONE", DAYS[0], DAYS[10]), ("NIFTY 500", "BIG", DAYS[0], None),
            ("NIFTY 500", "MID", DAYS[0], None), ("NIFTY 500", "PTWO", DAYS[0], None)]
    mem = pd.DataFrame([{"index": i, "security_id": sid[s], "symbol": s, "isin": STOCKS[s]["isin"], "from_date": a,
                         "to_date": b, "source": "test"} for i, s, a, b in rows], columns=im.MEMBERSHIP_COLUMNS)
    pq.write_table(pa.Table.from_pandas(mem, preserve_index=False), os.path.join(out, "nifty_membership.parquet"))
    pq.write_table(pa.Table.from_pandas(pd.DataFrame(columns=im.AMFI_COLUMNS).astype(str), preserve_index=False),
                   os.path.join(out, "amfi.parquet"))
    cov = {i: {"first": None, "last": None, "why": "no current list"} for i in im.INDEXES}
    cov["NIFTY 50"] = {"first": str(DAYS[0]), "last": str(DAYS[N - 1]), "reliable_from": str(DAYS[0])}
    cov["NIFTY 500"] = {"first": str(DAYS[0]), "last": str(DAYS[N - 1]), "reliable_from": str(DAYS[0])}
    with open(os.path.join(out, "build_info.json"), "w", encoding="utf-8") as f:
        json.dump({"coverage": cov}, f)


def build_inputs(folder, last_day):
    write_store(folder, last_day)
    sm.build(folder, log=lambda *_: None)
    write_ca(folder)
    adj.build(folder, log=lambda *_: None)
    write_refs(folder)
    ex.build(folder, log=lambda *_: None)
    write_index(folder)


# ---- the fake Kite ----

def kite_day(sym, i, div=1.0):
    """Kite's day candle: our factor for the split, ADDCO additive before day 40, a whole-history divisor."""
    s, b = STOCKS[sym], raw(sym, i)
    f = 5.0 if s.get("kite") == "split" and i < SPLIT_DAY else 1.0
    k = {x: b[x] / f for x in ("open", "high", "low", "close")}
    if s.get("kite") == "add" and i < 40:
        k = {x: v - 4.6 for x, v in k.items()}
    k = {x: round(v / div, 2) for x, v in k.items()}
    return {**k, "volume": int(b["volume"] * f * div)}


def minute_bars(sym, i, d, div=1.0):
    """Three bars per day from the day candle: the day's high and low, a last close that is not the day close,
    volume 0.99 of the day's; SCALECO on its own scale before STEP_DAY, VOLCO at half volume, EMPTYCO's bad days."""
    s, k = STOCKS[sym], kite_day(sym, i, div)
    sc = s.get("scale", 1.0) if i < STEP_DAY else 1.0
    o, h, l, c = (k[x] / sc for x in ("open", "high", "low", "close"))
    v = int(k["volume"] * 0.33 * (s.get("volume", 1.0) if i < STEP_DAY else 1.0))
    last = min(c * 1.002, h)
    bars = [(dtime(9, 15), o, max(o, c), l, c), (dtime(12, 0), c, h, c, c),
            (dtime(15, 29), c, max(c, last), min(c, last), last)]
    if i in s.get("bad_high", ()):
        bars[1] = (dtime(12, 0), c, h * 1.05, c, c)
    if i in s.get("bad_close", ()):
        top = c * 0.95
        bars = [(t, l, top, l, top) for t, *_ in bars]
    if i in s.get("bad_vol", ()):
        v *= 3
    return [{"date": datetime.combine(d, t).replace(tzinfo=mp.IST), "open": round(bo, 2), "high": round(bh, 2),
             "low": round(bl, 2), "close": round(bc, 2), "volume": v} for t, bo, bh, bl, bc in bars]


class FakeKite:
    def __init__(self, auth_after=None, stop_after=None, fail_minute=(), n429=0, last_day=N - 1, new_tokens=None):
        self.calls, self.lock = [], threading.Lock()
        self.auth_after, self.stop_after, self.fail_minute, self.n429 = auth_after, stop_after, set(fail_minute), n429
        self.last_day, self.div = last_day, {}
        self.new_tokens = new_tokens or {}
        self.by_token = {s["token"]: sym for sym, s in STOCKS.items() if s["token"]}
        self.by_token.update({t: sym for sym, t in self.new_tokens.items()})

    def instruments(self, exchange):
        with self.lock:
            self.calls.append(("instruments", exchange))
        out = []
        for sym, s in STOCKS.items():
            if s["token"] and s.get("first", 0) <= self.last_day:
                out.append(self.inst(sym, self.new_tokens.get(sym, s["token"])))
        return out

    @staticmethod
    def inst(ts, token):
        return {"instrument_token": token, "exchange_token": str(token // 256), "tradingsymbol": ts,
                "name": f"{ts} LIMITED", "last_price": 0.0, "expiry": "", "strike": 0.0, "tick_size": 0.05,
                "lot_size": 1, "instrument_type": "EQ", "segment": "NSE", "exchange": "NSE"}

    def kite_days(self, sym):
        s = STOCKS[sym]
        out = [(i, DAYS[i]) for i in idx_of(sym, self.last_day)]
        if s.get("first", 0) == 0 and EXTRA > DAYS[s.get("first", 0)]:
            out.append((None, EXTRA))
        return sorted(out, key=lambda x: x[1])

    def historical_data(self, token, start, end, interval, continuous=False, oi=False):
        with self.lock:
            n = sum(1 for c in self.calls if c[0] != "instruments")
            if self.auth_after is not None and n >= self.auth_after:
                raise kex.TokenException("Incorrect `api_key` or `access_token`.")
            if self.stop_after is not None and n >= self.stop_after:
                raise KeyboardInterrupt()
            self.calls.append((token, start.date(), end.date(), interval))
            if interval == "minute" and self.n429 > 0:
                self.n429 -= 1
                raise kex.KiteException("Too many requests", code=429)
        time.sleep(0.001)
        if interval == "minute" and (token, start.date()) in self.fail_minute:
            raise kex.KiteException("something broke", code=500)
        sym = self.by_token[token]
        s, div = STOCKS[sym], self.div.get(token, 1.0)
        out = []
        for i, d in self.kite_days(sym):
            if not start.date() <= d <= end.date():
                continue
            j = i if i is not None else DAYS.index(date(2015, 2, 27))      # the Saturday trades like Friday
            if interval == "day":
                if i in s.get("no_day", ()) or i == SESSION:
                    continue
                out.append({"date": datetime.combine(d, dtime()).replace(tzinfo=mp.IST), **kite_day(sym, j, div)})
            else:
                if d < mp.KITE_MINUTE_FROM or i in s.get("no_min", ()) or i == NO_MINUTES:
                    continue
                out += minute_bars(sym, j, d, div)
        return out

    def minute_calls(self):
        return [c for c in self.calls if c[0] != "instruments" and c[3] == "minute"]


# ---- fixtures ----

def q(*a, **k):
    pass


@pytest.fixture(scope="module")
def planned(tmp_path_factory):
    """The world after phase 1 (tokens + probe): plan.csv, no minute data yet."""
    f = str(tmp_path_factory.mktemp("min_dl") / "data")
    mpatch = pytest.MonkeyPatch()
    mpatch.setattr(config, "load_env", lambda: {})
    build_inputs(f, N - 1)
    kite = FakeKite()
    mp.tokens(f, kite=kite, now=NOW, log=q)
    mp.probe(f, kite=kite, speed=FAST, now=NOW, log=q)
    mpatch.undo()
    return f


@pytest.fixture
def world(planned, tmp_path):
    f = str(tmp_path / "data")
    shutil.copytree(planned, f)
    return f


@pytest.fixture(scope="module")
def downloaded(planned, tmp_path_factory):
    """planned + a full download with two workers (read-only for the tests that use it)."""
    f = str(tmp_path_factory.mktemp("min_dl_done") / "data")
    shutil.copytree(planned, f)
    mpatch = pytest.MonkeyPatch()
    mpatch.setattr(config, "load_env", lambda: {})
    mpatch.setattr(fetch, "STOP", FastStop())
    kite = FakeKite()
    logs = []
    res = md.download(f, kite=kite, speed=FAST, workers=2, log=lambda *a, **k: logs.append(" ".join(map(str, a))))
    mpatch.undo()
    return f, res, kite, logs


@pytest.fixture
def done_copy(downloaded, tmp_path):
    f = str(tmp_path / "done")
    shutil.copytree(downloaded[0], f)
    return f


def sid(f, sym):
    return sm.lookup(sym, f)[0]


def days_of(f, sym):
    return ms.read_days(ms.stock_dir(f, sid(f, sym))).set_index("date")


def hashes(f, subs=("NSE_CM_bhav", "security_master", "adjustments", "exits", "index_membership",
                    os.path.join("equity_1min", "plan"))):
    out = {}
    for sub in subs:
        for root, _, files in os.walk(os.path.join(f, sub)):
            for name in files:
                p = os.path.join(root, name)
                if name == ".lock":
                    continue
                with open(p, "rb") as fh:
                    out[p] = hashlib.sha256(fh.read()).hexdigest()
    return out


def dl(f, kite, **kw):
    kw.setdefault("speed", FAST)
    kw.setdefault("workers", 1)
    kw.setdefault("log", q)
    return md.download(f, kite=kite, **kw)


PLANNED = ["BIG", "MID", "PTWO", "SCALECO", "VOLCO", "ADDCO", "SPLITCO", "EMPTYCO", "NEWNM", "GEN1", "GEN2", "GEN3",
           "GEN4"]


# ---- Part 1: work list ----

def test_work_list_order_and_filters(world):
    inp = mp.Inputs(world)
    items = md.work_list(world, inp)
    syms = [i["symbol"] for i in items]
    assert sorted(syms) == sorted(PLANNED)                                    # exited GONE is never downloaded
    assert syms[:3] == ["BIG", "MID", "PTWO"]                                 # priority 1 by turnover, then 2
    assert [i["priority"] for i in items[:3]] == [1, 1, 2] and {i["priority"] for i in items[3:]} == {3}
    rest = [i["security_id"] for i in items[3:]]
    assert rest == sorted(rest)                                               # equal turnover: by id
    assert [i["symbol"] for i in md.work_list(world, inp, priority=2)] == ["PTWO"]
    assert [i["symbol"] for i in md.work_list(world, inp, symbols=["OLDNM"])] == ["NEWNM"]   # an old symbol
    with pytest.raises(mp.MinutePlanError, match="NOPE"):
        md.work_list(world, inp, symbols=["NOPE"])
    big = items[0]
    assert big["first"] == mp.KITE_MINUTE_FROM and big["last"] == DAYS[N - 1]
    assert [(a.date(), b.date()) for a, b in big["chunks"]] == [FIRST_CHUNK, (date(2015, 4, 3), DAYS[N - 1])]


def test_dry_run_figures_write_nothing(world, monkeypatch):
    before = hashes(world)
    res = md.download(world, dry_run=True, speed=2.5, workers=2, log=q)
    t = res["estimate"]["total"]
    assert t["stocks"] == 13 and t["NOT_STARTED"] == 13 and t["DONE"] == 0
    assert t["minute_requests"] == 26 and t["day_requests"] == 13
    g = res["estimate"]["groups"]
    assert g[1]["stocks"] == 2 and g[2]["stocks"] == 1 and g[3]["stocks"] == 10
    assert res["estimate"]["hours"] == pytest.approx((26 * max(0.4, md.MINUTE_SECONDS / 2) + 13 * 0.4) / 3600)
    assert "priority 1: 2 stocks" in res["report"] and "free" in res["report"]
    assert not os.path.exists(os.path.join(mp.out_dir(world), "download_progress.sqlite"))
    assert not os.path.exists(ms.data_dir(world))
    assert hashes(world) == before
    lim = md.download(world, dry_run=True, limit=2, log=q)
    assert lim["estimate"]["total"]["stocks"] == 2
    monkeypatch.setattr(md, "free_space", lambda f: 10)
    assert "NOT ENOUGH DISK" in md.download(world, dry_run=True, log=q)["report"]


def test_refuses_without_enough_disk(world, monkeypatch, capsys):
    monkeypatch.setattr(md, "free_space", lambda f: 10)
    kite = FakeKite()
    with pytest.raises(md.NotEnoughDisk, match="Not enough disk"):
        dl(world, kite)
    assert kite.calls == []
    monkeypatch.setattr(mp, "kite_client", lambda: kite)
    assert cli.main(["download", "--folder", world]) == 2
    assert "Not enough disk" in capsys.readouterr().err


def test_no_plan_and_stale_plan(world, monkeypatch):
    os.rename(md.plan_path(world), md.plan_path(world) + ".x")
    with pytest.raises(mp.NotBuilt):
        dl(world, FakeKite())
    os.rename(md.plan_path(world) + ".x", md.plan_path(world))
    old = time.time() - 3600
    os.utime(md.plan_path(world), (old, old))                 # the master is now newer than plan.csv
    assert md.plan_stale(world) == "security master"
    assert "a real run rebuilds it" in md.download(world, dry_run=True, log=q)["report"]
    logs = []
    kite = FakeKite()
    dl(world, kite, symbols=["GEN1"], log=lambda *a, **k: logs.append(" ".join(map(str, a))))
    assert any("rebuilt offline (gaps)" in x for x in logs)
    assert md.plan_stale(world) is None
    assert not [c for c in kite.calls if c[0] == "instruments"]              # offline: no instrument list


# ---- Part 2: chunks, staging, resume ----

def test_two_workers_no_chunk_lost_or_twice(downloaded):
    f, res, kite, logs = downloaded
    st = res["stats"]
    assert st["built"] == 13 and st["failed_chunks"] == 0 and not st["stopped"]
    calls = [(c[0], c[1]) for c in kite.minute_calls()]
    assert len(calls) == len(set(calls)) == 26                                # every chunk exactly once
    day_calls = [c for c in kite.calls if c[0] != "instruments" and c[3] == "day"]
    assert len(day_calls) == 13
    chunks, stocks, runs = md.read_progress(f)
    assert len(chunks) == 26 and all(c["status"] == "DONE" for c in chunks)
    assert len(stocks) == 13 and all(s["status"] == "DONE" for s in stocks.values())
    assert runs[-1]["result"] == "done" and runs[-1]["requests"] == 39
    assert not os.listdir(md.staging_dir(f))                                  # staging removed after each build
    for sym in PLANNED:
        path = ms.stock_dir(f, sid(f, sym))
        assert sorted(os.listdir(path)) == ["2015.parquet", "days.parquet"]
    assert any("Priority 2:" in x for x in logs) and any("x 429" in x for x in logs)


def test_stored_candles_unchanged(downloaded):
    f, _, kite, _ = downloaded
    m = pq.read_table(os.path.join(ms.stock_dir(f, sid(f, "BIG")), "2015.parquet")).to_pandas()
    assert list(m.columns) == ms.MINUTE_COLUMNS
    assert str(m["ts"].dt.tz) == "Asia/Kolkata" and m["volume"].dtype == "int64"
    day = DAYS[30]
    got = m[m["ts"].dt.date == day].reset_index(drop=True)
    want = ms.minute_frame(minute_bars("BIG", 30, day))
    pd.testing.assert_frame_equal(got, want, check_dtype=False)
    assert pd.Series(m["ts"]).is_monotonic_increasing and not m["ts"].duplicated().any()


def test_stop_resume_and_build_only_when_complete(world):
    kite = FakeKite(stop_after=2)               # GEN1: day candles, first chunk, then Ctrl+C
    res = dl(world, kite, symbols=["GEN1"])
    assert res["stats"]["stopped"] and res["stats"]["built"] == 0
    g = sid(world, "GEN1")
    assert not os.path.exists(ms.stock_dir(world, g))                       # never built half
    staged = sorted(os.listdir(md.staging_dir(world, g)))
    assert staged[0].startswith("day_") and staged[1] == "m_2015-02-02.parquet" and len(staged) == 2
    chunks, _, runs = md.read_progress(world)
    assert [(c["chunk_from"], c["status"]) for c in chunks] == [("2015-02-02", "DONE")]
    assert runs[-1]["result"] == "stopped"
    kite = FakeKite()
    res = dl(world, kite, symbols=["GEN1"])
    assert res["stats"]["built"] == 1
    assert [(c[1], c[3]) for c in kite.calls] == [(mp.KITE_MINUTE_FROM, "day"), (date(2015, 4, 3), "minute")]
    assert os.path.isfile(os.path.join(ms.stock_dir(world, g), "days.parquet"))
    kite = FakeKite()
    assert dl(world, kite, symbols=["GEN1"])["stats"]["stocks"] == 0 and kite.calls == []   # DONE: skipped


def test_auth_stop_keeps_finished_and_resumes(world, capsys, monkeypatch):
    kite = FakeKite(auth_after=4)               # GEN1 complete (3 requests), then the token expires
    with pytest.raises(mp.SignInNeeded, match="finished chunks are kept"):
        dl(world, kite, symbols=["GEN1", "GEN2"])
    _, stocks, runs = md.read_progress(world)
    assert set(stocks) == {sid(world, "GEN1")} and runs[-1]["result"] == "auth"
    monkeypatch.setattr(mp, "kite_client", lambda: FakeKite(auth_after=0))
    assert cli.main(["download", "--symbols", "GEN2", "--folder", world, "--speed", "3"]) == 2
    assert "Kite sign-in needed: sign in (python app.py) and run the same command again" in capsys.readouterr().err
    kite = FakeKite()
    assert dl(world, kite, symbols=["GEN1", "GEN2"])["stats"]["built"] == 1
    assert {c[0] for c in kite.calls} == {111}


def test_failed_chunk_retried(world, monkeypatch, capsys):
    kite = FakeKite(fail_minute={(111, date(2015, 4, 3))})
    res = dl(world, kite, symbols=["GEN2", "GEN3"])
    assert res["stats"]["failed_chunks"] == 1 and res["stats"]["built"] == 1
    chunks, stocks, _ = md.read_progress(world)
    failed = [c for c in chunks if c["status"] == "FAILED"]
    assert len(failed) == 1 and "something broke" in failed[0]["error"]
    assert sid(world, "GEN2") not in stocks
    monkeypatch.setattr(mp, "kite_client", lambda: FakeKite(fail_minute={(111, date(2015, 4, 3))}))
    assert cli.main(["download", "--symbols", "GEN2", "--folder", world, "--workers", "1"]) == 1
    kite = FakeKite()
    res = dl(world, kite, symbols=["GEN2", "GEN3"])
    assert res["stats"]["built"] == 1 and [c[3] for c in kite.calls] == ["day", "minute"]
    st = md.status(world, log=q)
    assert st["failed_chunks"] == 0


def test_token_change_fetches_again(world):
    dl(world, FakeKite(stop_after=2), symbols=["GEN1"])          # first chunk DONE with token 110
    plan = pd.read_csv(md.plan_path(world), dtype=str, keep_default_na=False)
    plan.loc[plan["security_id"] == sid(world, "GEN1"), "instrument_token"] = "210"
    plan.to_csv(md.plan_path(world), index=False)
    kite = FakeKite(new_tokens={"GEN1": 210})
    dl(world, kite, symbols=["GEN1"])
    assert [c[0] for c in kite.calls] == [210, 210, 210] and len(kite.minute_calls()) == 2
    _, stocks, _ = md.read_progress(world)
    assert stocks[sid(world, "GEN1")]["token"] == "210"


def test_429_counted(world, capsys):
    kite = FakeKite(n429=2)
    res = dl(world, kite, symbols=["GEN3"])
    assert res["stats"]["n429"] == 2 and res["stats"]["built"] == 1
    st = md.status(world, log=q)
    assert st["n429"] == 2 and "429 answers: 2" in st["report"]


def test_same_scale_rule_across_two_runs(world):
    """Kite re-adjusts its whole history between two runs: each day keeps the conversion of its own run."""
    dl(world, FakeKite(stop_after=2), symbols=["GEN1"])          # run 1: day candles + chunk 1 at Kite scale 1
    kite = FakeKite()
    kite.div[110] = 2.0                                           # a new corporate action: everything / 2
    dl(world, kite, symbols=["GEN1"])                             # run 2: day candles again + chunk 2
    d = days_of(world, "GEN1")
    early, late = d.loc[DAYS[30]], d.loc[DAYS[70 - 2]]
    assert early["ratio"] == pytest.approx(1.0, abs=1e-3) and late["ratio"] == pytest.approx(2.0, abs=1e-2)
    assert early["run_id"] != late["run_id"]
    for day in (DAYS[30], DAYS[68]):
        raw_m = ms.load_minute("GEN1", day, day, prices="raw", folder=world)
        b = raw("GEN1", DAYS.index(day))
        assert raw_m["high"].max() == pytest.approx(b["high"], abs=0.03)
        assert raw_m["low"].min() == pytest.approx(b["low"], abs=0.03)


# ---- Part 3: checks ----

def test_day_checks(downloaded):
    f = downloaded[0]
    e = days_of(f, "EMPTYCO")
    assert e.loc[DAYS[30], "check"] == "MINUTE_EMPTY" and e.loc[DAYS[31], "check"] == "MINUTE_EMPTY"
    assert e.loc[DAYS[35], "check"] == "DAY_EMPTY" and e.loc[DAYS[35], "conversion_from"] != ""
    assert DAYS[36] not in e.index                                # neither a day candle nor minutes
    assert e.loc[DAYS[40], "check"] == "HIGHLOW_DIFF"
    assert e.loc[DAYS[42], "check"] == "CLOSE_DIFF"
    assert e.loc[DAYS[45], "check"] == "VOLUME_DIFF"
    assert e.loc[EXTRA, "check"] == "EXTRA_SESSION" and pd.isna(e.loc[EXTRA, "bhav_close"])
    assert e.loc[EXTRA, "conversion_from"] != "" and e.loc[EXTRA, "symbol"] == "EMPTYCO"
    b = days_of(f, "BIG")
    assert b.loc[DAYS[30], "check"] == "OK" and b.loc[DAYS[30], "minute_rows"] == 3
    assert b.loc[DAYS[30], "first_bar"] == "09:15" and b.loc[DAYS[30], "last_bar"] == "15:29"
    assert b.loc[DAYS[30], "minute_close"] != b.loc[DAYS[30], "kite_close"]      # the last minute is not the close
    assert b.loc[DAYS[NO_MINUTES], "check"] == "MINUTE_EMPTY"
    assert b.loc[DAYS[SESSION], "check"] == "DAY_EMPTY"
    assert list(b.reset_index().columns) == md.DAY_COLUMNS
    assert (b["fetched_at"] != "").all() and b["kind"].isin(mp.KINDS).all()


def test_scales_and_steps(downloaded):
    f = downloaded[0]
    s = days_of(f, "SCALECO")
    assert s.loc[DAYS[30], "minute_scale"] == pytest.approx(1.01, abs=2e-4)
    assert s.loc[DAYS[STEP_DAY], "minute_scale"] == 1.0
    assert "SCALE_STEP" in s.loc[DAYS[STEP_DAY], "check"] and "no corporate action" in s.loc[DAYS[STEP_DAY], "note"]
    assert s.loc[DAYS[30], "check"] == "OK"                       # scaled minutes match the day
    v = days_of(f, "VOLCO")
    assert v.loc[DAYS[30], "minute_volume_scale"] == pytest.approx(2.0, abs=0.01)
    assert v.loc[DAYS[STEP_DAY + 3], "minute_volume_scale"] == 1.0 and v.loc[DAYS[30], "check"] == "OK"
    assert "SCALE_STEP" not in v.loc[DAYS[STEP_DAY], "check"]     # the dividend explains it
    assert "corporate action" in v.loc[DAYS[STEP_DAY], "note"]
    assert (days_of(f, "BIG")["minute_scale"] == 1.0).all() and (days_of(f, "BIG")["minute_volume_scale"] == 1.0).all()
    a = days_of(f, "ADDCO")
    assert a.loc[DAYS[30], "kind"] == "ADDITIVE" and a.loc[DAYS[45], "kind"] == "MULTIPLICATIVE"
    assert a.loc[DAYS[30], "kite_difference"] == pytest.approx(4.6, abs=0.05)
    assert days_of(f, "SPLITCO").loc[DAYS[30], "ratio"] == pytest.approx(5.0, abs=1e-3)


def test_levels_unit():
    v = np.array([1.0] * 10 + [1.0101] * 3 + [1.0] * 10 + [2.0] * 8)
    lv = md.levels(v, 0.001)
    assert list(lv[:23]) == [1.0] * 23 and list(lv[23:]) == [2.0] * 8         # a 3-day blip stays inside
    assert md.levels(np.full(5, np.nan), 0.001) is None
    vr = np.array([1.01] * 10 + [2.02] * 10 + [1.01] * 10)
    assert list(md.volume_levels(vr)) == [1.0] * 10 + [2.0] * 10 + [1.0] * 10
    assert list(md.volume_levels(np.array([1.01, 1.3, 1.02] * 4))) == [1.0] * 12   # under 20%: unchanged


def test_check_files(downloaded):
    f = downloaded[0]
    c = os.path.join(mp.out_dir(f), "check")
    chk = pd.read_csv(os.path.join(c, "minute_check.csv"), dtype=str, keep_default_na=False)
    assert list(chk.columns) == md.CHECK_COLUMNS and "OK" not in set(chk["check"])
    e = chk[chk["security_id"] == sid(f, "EMPTYCO")].set_index("date")
    for i, name in ((30, "MINUTE_EMPTY"), (35, "DAY_EMPTY"), (40, "HIGHLOW_DIFF"), (42, "CLOSE_DIFF"),
                    (45, "VOLUME_DIFF")):
        assert e.loc[str(DAYS[i]), "check"] == name
    empty = pd.read_csv(os.path.join(c, "minute_empty_days.csv"), dtype=str, keep_default_na=False)
    market = empty[empty["security_id"] == ""]
    assert list(market["from_date"]) == [str(DAYS[NO_MINUTES])] and "13 of 13" in market["note"].iat[0]
    own = empty[empty["security_id"] == sid(f, "EMPTYCO")]
    assert list(zip(own["from_date"], own["to_date"], own["days"])) == [(str(DAYS[30]), str(DAYS[31]), "2")]
    steps = pd.read_csv(os.path.join(c, "scale_steps.csv"), dtype=str, keep_default_na=False)
    sc = steps[steps["security_id"] == sid(f, "SCALECO")]
    assert list(sc["kind"]) == ["MINUTE_SCALE"] and sc["flagged"].iat[0] == "Y" and sc["date"].iat[0] == str(DAYS[50])
    vc = steps[steps["security_id"] == sid(f, "VOLCO")]
    assert list(vc["kind"]) == ["MINUTE_VOLUME_SCALE"] and vc["flagged"].iat[0] == "N" and vc["near_action"].iat[0]
    with open(os.path.join(c, "summary.json"), encoding="utf-8") as fh:
        s = json.load(fh)
    assert s["stocks"] == 13 and s["checks"]["HIGHLOW_DIFF"] == 1 and s["checks"]["EXTRA_SESSION"] == 13
    assert s["checks_per_priority"]["1"]["DAY_EMPTY"] == 2
    assert [x["symbol"] for x in s["minute_scale_not_one"]] == ["SCALECO"]
    assert [x["symbol"] for x in s["volume_steps"]] == ["VOLCO"]
    assert s["minute_empty_market_days"] == [str(DAYS[NO_MINUTES])] and s["scale_steps_flagged"] == 1


def test_gaps_reads_minute_files(planned, downloaded, tmp_path):
    f = str(tmp_path / "data")
    shutil.copytree(downloaded[0], f)
    before = pd.read_csv(os.path.join(mp.plan_dir(planned), "kite_gaps.csv"), dtype=str, keep_default_na=False)
    mp.gaps(f, log=q)
    after = pd.read_csv(os.path.join(mp.plan_dir(f), "kite_gaps.csv"), dtype=str, keep_default_na=False)
    keep = ~before["reason"].isin(["KITE_MISSING_SESSION", "EOD_EMPTY_DAYS"])
    pd.testing.assert_frame_equal(before[keep].reset_index(drop=True),
                                  after[after["reason"].isin(before.loc[keep, "reason"])].reset_index(drop=True))
    sess = after[after["reason"] == "KITE_MISSING_SESSION"]
    assert list(sess["data_missing"]) == ["EOD", "BOTH"]                     # GEN4 has no minutes that day
    assert "12 of them" in sess["note"].iat[0] and "1 of them have no 1-minute" in sess["note"].iat[1]
    e = after[(after["reason"] == "EOD_EMPTY_DAYS") & (after["security_id"] == sid(f, "EMPTYCO"))]
    assert list(zip(e["data_missing"], e["need_from"], e["need_to"])) == \
        [("EOD", str(DAYS[35]), str(DAYS[35])), ("BOTH", str(DAYS[36]), str(DAYS[36]))]
    me = after[after["reason"] == "MINUTE_EMPTY_DAYS"]
    assert set(zip(me["data_missing"], me["security_id"], me["need_from"])) == \
        {("1MIN", "", str(DAYS[NO_MINUTES])), ("1MIN", sid(f, "EMPTYCO"), str(DAYS[30]))}
    assert "EOD" in json.load(open(os.path.join(mp.plan_dir(f), "build_info.json")))["data_missing"]


def test_gaps_unchanged_without_minute_data(world):
    path = os.path.join(mp.plan_dir(world), "kite_gaps.csv")
    with open(path, "rb") as fh:
        before = fh.read()
    assert mp.stored_minute_days(world) == {}
    mp.gaps(world, log=q)
    with open(path, "rb") as fh:
        assert fh.read() == before


# ---- Part 4: loader ----

def test_load_minute_kite_raw_ours(downloaded):
    f = downloaded[0]
    d = DAYS[30]
    k = ms.load_minute("BIG", d, d, prices="kite", folder=f)
    assert list(k.columns) == ["ts", "open", "high", "low", "close", "volume", "security_id", "symbol"]
    assert len(k) == 3 and (k["symbol"] == "BIG").all() and (k["security_id"] == sid(f, "BIG")).all()
    rw = ms.load_minute("BIG", d, d, folder=f)
    pd.testing.assert_frame_equal(rw[["open", "high", "low", "close"]], k[["open", "high", "low", "close"]])
    b = raw("BIG", 30)
    assert rw["high"].max() == b["high"] and rw["volume"].sum() == pytest.approx(0.99 * b["volume"], abs=3)
    sc_k = ms.load_minute("SCALECO", d, d, prices="kite", folder=f)
    sc = ms.load_minute("SCALECO", d, d, folder=f)
    assert sc["close"].to_numpy() == pytest.approx(sc_k["close"].to_numpy() * 1.01, rel=3e-4)
    assert sc["high"].max() == pytest.approx(raw("SCALECO", 30)["high"], abs=0.05)
    add = ms.load_minute("ADDCO", d, d, folder=f)                            # additive: raw = M x (Kite + d)
    assert add["high"].max() == pytest.approx(raw("ADDCO", 30)["high"], abs=0.05)
    assert add["low"].min() == pytest.approx(raw("ADDCO", 30)["low"], abs=0.05)
    sp_k = ms.load_minute("SPLITCO", d, d, prices="kite", folder=f)
    sp = ms.load_minute("SPLITCO", d, d, folder=f)
    ours = ms.load_minute("SPLITCO", d, d, prices="ours", folder=f)
    assert sp["high"].max() == pytest.approx(raw("SPLITCO", 30)["high"], abs=0.05)    # the bhavcopy's scale (x5)
    assert ours["close"].to_numpy() == pytest.approx(sp_k["close"].to_numpy(), rel=1e-4)   # our factor 5 = Kite's
    assert ours["volume"].sum() == pytest.approx(sp["volume"].sum() * 5)
    vol = ms.load_minute("VOLCO", d, d, folder=f)
    assert vol["volume"].sum() == pytest.approx(0.99 * raw("VOLCO", 30)["volume"], rel=0.01)  # the volume scale
    with pytest.raises(ValueError):
        ms.load_minute("BIG", d, d, prices="adjusted", folder=f)


def test_load_minute_lookup_and_errors(downloaded):
    f = downloaded[0]
    m = ms.load_minute("OLDNM", DAYS[25], DAYS[35], folder=f)                # a symbol it had before the rename
    assert set(m["security_id"]) == {sid(f, "NEWNM")}
    by_day = m.groupby(m["ts"].dt.date)["symbol"].first()
    assert by_day[DAYS[25]] == "OLDNM" and by_day[DAYS[35]] == "NEWNM"
    assert len(ms.load_minute(sid(f, "BIG"), folder=f)) > 100                 # by id, whole history
    with pytest.raises(ms.NoMinuteData, match="exited"):
        ms.load_minute("GONE", folder=f)
    with pytest.raises(ms.NoMinuteData, match="no security"):
        ms.load_minute("NOSUCH", folder=f)


# ---- Part 5: update ----

def extend(f):
    """The next trading day arrives: the bhavcopy and every input build gain DAYS[N] (NEWIPO lists that day)."""
    build_inputs(f, N)


def test_update_refuses_before_history_and_bhavcopy(world, done_copy):
    with pytest.raises(md.HistoryNotDone, match="download first"):
        md.update(world, kite=FakeKite(), speed=FAST, now=datetime.combine(DAYS[N - 1], dtime(20)), log=q)
    kite = FakeKite()
    with pytest.raises(md.NoBhavToday, match="equity_update.py"):
        md.update(done_copy, kite=kite, speed=FAST, now=datetime.combine(DAYS[N], dtime(20)), log=q)
    assert kite.calls == []
    assert cli.main(["update", "--folder", world]) == 1


def test_update_appends_new_day_new_listing_and_tokens(done_copy):
    f = done_copy
    extend(f)
    big_before = days_of(f, "BIG")
    kite = FakeKite(last_day=N, new_tokens={"GEN1": 211})
    logs = []
    res = md.update(f, kite=kite, speed=FAST, workers=2, now=datetime.combine(DAYS[N], dtime(20), tzinfo=mp.IST),
                    log=lambda *a, **k: logs.append(" ".join(map(str, a))))
    st = res["stats"]
    assert kite.calls[0] == ("instruments", "NSE")
    assert st["updated"] == 12 and st["new_days"] == 12 and st["built"] == 2      # GEN1 (new token) + NEWIPO
    assert st["new_listings"] == ["NEWIPO"] and any("rebuilt offline" in x for x in logs)
    big = days_of(f, "BIG")
    assert len(big) == len(big_before) + 1 and big.index[-1] == DAYS[N] and big.loc[DAYS[N], "check"] == "OK"
    pd.testing.assert_frame_equal(big.iloc[:-1], big_before)                      # stored days unchanged
    m = ms.load_minute("BIG", DAYS[N], DAYS[N], folder=f)
    assert len(m) == 3 and m["high"].max() == raw("BIG", N)["high"]
    ipo = days_of(f, "NEWIPO")
    assert list(ipo.index) == [DAYS[N]]
    up = os.path.join(mp.out_dir(f), "update")
    assert os.path.isfile(os.path.join(up, f"kite_instruments_NSE_{DAYS[N]}.csv"))
    toks = pd.read_csv(os.path.join(up, "tokens.csv"), dtype=str, keep_default_na=False).set_index("symbol")
    assert toks.loc["GEN1", "instrument_token"] == "211"
    assert os.listdir(os.path.join(mp.out_dir(f), "archive"))[0].startswith(sid(f, "GEN1"))
    _, stocks, _ = md.read_progress(f)
    assert stocks[sid(f, "GEN1")]["token"] == "211" and stocks[sid(f, "NEWIPO")]["status"] == "DONE"
    kite2 = FakeKite(last_day=N, new_tokens={"GEN1": 211})
    again = md.update(f, kite=kite2, speed=FAST, now=datetime.combine(DAYS[N], dtime(21), tzinfo=mp.IST), log=q)
    assert again["stats"]["up_to_date"] == 14 and again["stats"]["updated"] == 0
    assert [c for c in kite2.calls if c[0] != "instruments"] == []


# ---- Part 6: status, locks, inputs, CLI ----

def test_status_figures(world, capsys):
    dl(world, FakeKite(stop_after=2), symbols=["GEN1"])
    dl(world, FakeKite(), symbols=["GEN2"])
    st = md.status(world, log=q)
    t = st["estimate"]["total"]
    assert t["stocks"] == 13 and t["DONE"] == 1 and t["PARTLY"] == 1 and t["NOT_STARTED"] == 11
    assert t["minute_requests"] == 1 + 22 and st["failed_chunks"] == 0
    assert st["rows"] > 0 and st["staged_rows"] > 0 and "Last run: download" in st["report"]
    assert cli.main(["status", "--folder", world]) == 0
    assert "Stocks: 13" in capsys.readouterr().out


def test_locks(world, monkeypatch, tmp_path):
    lock = RunLock(mp.lock_path(world)).acquire("test")
    try:
        with pytest.raises(mp.LockBusy):
            dl(world, FakeKite(), symbols=["GEN1"])
        assert cli.main(["download", "--folder", world]) == 2
    finally:
        lock.release()
    fno = tmp_path / "fno"
    monkeypatch.setattr(config, "load_env", lambda: {"KITE_DATA_DIR": str(fno)})
    lock = RunLock(str(fno / "NIFTY_FnO_1min" / ".lock")).acquire("test")
    try:
        with pytest.raises(mp.FnoBusy):
            dl(world, FakeKite(), symbols=["GEN1"])
        with pytest.raises(mp.FnoBusy):
            md.update(world, kite=FakeKite(), speed=FAST, log=q)
    finally:
        lock.release()
    monkeypatch.setattr(config, "load_env", lambda: {})
    lock = RunLock(os.path.join(sm.out_dir(world), ".lock")).acquire("test")
    try:
        with pytest.raises(mp.StoreBusy):
            dl(world, FakeKite(), symbols=["GEN1"])
    finally:
        lock.release()


def test_inputs_unchanged(planned, downloaded):
    """Every input (bhavcopy, master, factors, exits, index, phase 1's plan folder) byte for byte as before."""
    def rel(f):
        return {os.path.relpath(p, f): h for p, h in hashes(f).items()}
    assert rel(downloaded[0]) == rel(planned) and len(rel(planned)) > 20


def test_cli(world, monkeypatch, capsys):
    assert cli.main(["download", "--dry-run", "--folder", world]) == 0
    assert "Requests left" in capsys.readouterr().out
    for bad in ("5", "0"):                                                  # 1..4 (MAX_WORKERS)
        with pytest.raises(SystemExit) as e:
            cli.main(["download", "--workers", bad, "--folder", world])
        assert e.value.code == 2
    assert cli.main(["download", "--dry-run", "--workers", "4", "--folder", world]) == 0
    with pytest.raises(SystemExit) as e:
        cli.main(["download", "--speed", "4", "--folder", world])
    assert e.value.code == 2
    with pytest.raises(SystemExit) as e:
        cli.main(["download", "--folder", "relative"])
    assert e.value.code == 2
    assert cli.main(["download", "--folder", world, "--symbols", "GEN3"]) == 2      # no token in .env
    monkeypatch.setattr(mp, "kite_client", lambda: FakeKite())
    assert cli.main(["download", "--folder", world, "--symbols", "GEN3", "--limit", "1"]) == 0
    monkeypatch.setattr(mp, "kite_client", lambda: FakeKite(stop_after=0))
    assert cli.main(["download", "--folder", world, "--symbols", "GEN4"]) == 0      # stopped by the user
    assert "resume with the same command" in capsys.readouterr().err


# ---- workers 1..4, CLOSING_AUCTION, checks ----

def test_four_workers_no_chunk_lost_or_twice(world, downloaded):
    kite = FakeKite()
    res = dl(world, kite, workers=4)
    st = res["stats"]
    assert st["built"] == 13 and st["failed_chunks"] == 0 and not st["stopped"]
    calls = [(c[0], c[1]) for c in kite.minute_calls()]
    assert len(calls) == len(set(calls)) == 26                                # every chunk exactly once
    chunks, stocks, runs = md.read_progress(world)
    assert len(chunks) == 26 and all(c["status"] == "DONE" for c in chunks)
    assert len(stocks) == 13 and all(s["status"] == "DONE" for s in stocks.values())
    assert runs[-1]["requests"] == 39 and not os.listdir(md.staging_dir(world))
    cols = [c for c in md.DAY_COLUMNS if c not in ("date", "fetched_at", "run_id")]
    for sym in PLANNED:                                                       # the same stocks as with 2 workers
        pd.testing.assert_frame_equal(days_of(world, sym)[cols], days_of(downloaded[0], sym)[cols])
        p = os.path.join(ms.stock_dir(world, sid(world, sym)), "2015.parquet")
        pd.testing.assert_frame_equal(pq.read_table(p).to_pandas(),
                                      pq.read_table(p.replace(world, downloaded[0])).to_pandas())
    with pytest.raises(ValueError):
        dl(world, kite, workers=5)


def test_shared_limiter_with_four_threads():
    lim = md.SharedLimiter(50)
    starts, lock = [], threading.Lock()

    def worker():
        for _ in range(10):
            lim.wait()
            with lock:
                starts.append(time.monotonic())
            lim.on_success()

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    starts.sort()
    assert lim.requests == 40 and len(starts) == 40
    assert min(b - a for a, b in zip(starts, starts[1:])) >= 1 / 50 - 0.003   # one cap across all workers
    lim.on_429()
    assert lim.rate == 25 and lim.n429 == 1                                   # halved for every worker


def auction_store(f, day=42, last_bar="15:14"):
    """EMPTYCO's stored days with the CLOSE_DIFF day's minutes ending at last_bar, still labelled the old way."""
    path = ms.stock_dir(f, sid(f, "EMPTYCO"))
    d = ms.read_days(path)
    i = d.index[d["date"] == DAYS[day]][0]
    assert d.at[i, "check"] == "CLOSE_DIFF"
    d.at[i, "last_bar"] = last_bar
    ms.write_days(d, os.path.join(path, ms.DAYS_FILE))
    return path


def store_hashes(f):
    return {k: v for k, v in hashes(f, ("equity_1min",)).items()}


def test_closing_auction_label(done_copy):
    calc = md.Calc(done_copy, mp.Inputs(done_copy, bhav=False), write=False)
    stored = ms.read_days(ms.stock_dir(done_copy, sid(done_copy, "EMPTYCO")))
    i = stored.index[stored["date"] == DAYS[42]][0]
    ok = stored.index[stored["date"] == DAYS[41]][0]
    for bar, want in (("15:14", "CLOSING_AUCTION"), ("15:00", "CLOSING_AUCTION"), ("15:29", "CLOSE_DIFF"),
                      ("11:43", "CLOSE_DIFF"), ("14:59", "CLOSE_DIFF")):
        inp = stored[md.INPUT_COLUMNS].copy()
        inp.loc[[i, ok], "last_bar"] = bar
        d = calc.derive(sid(done_copy, "EMPTYCO"), inp)
        assert d.at[i, "check"] == want, bar
        assert d.at[ok, "check"] == "OK"                     # the close inside the minute range: never an auction
    assert md.CHECKS.index("CLOSING_AUCTION") == md.CHECKS.index("CLOSE_DIFF") + 1


def test_checks_dry_run_then_apply(done_copy, capsys):
    path = auction_store(done_copy)
    before = store_hashes(done_copy)
    old = ms.read_days(path)
    res = md.checks(done_copy, log=q)
    assert res["changed"] == [(sid(done_copy, "EMPTYCO"), "EMPTYCO", 1)] and not res["refused"]
    assert res["after"]["CLOSING_AUCTION"] == 1 and res["after"].get("CLOSE_DIFF", 0) == res["before"]["CLOSE_DIFF"] - 1
    assert store_hashes(done_copy) == before                                  # the dry run writes nothing
    assert cli.main(["checks", "--folder", done_copy]) == 0
    assert "CLOSING_AUCTION" in capsys.readouterr().out and store_hashes(done_copy) == before
    res = md.checks(done_copy, apply=True, log=q)
    new = ms.read_days(path)
    i = new.index[new["date"] == DAYS[42]][0]
    assert new.at[i, "check"] == "CLOSING_AUCTION"
    pd.testing.assert_frame_equal(new.drop(columns="check"), old.drop(columns="check"))   # only the label changed
    assert (new["check"] != old["check"]).sum() == 1
    s = res["summary"]
    assert s["closing_auction"] == [{"security_id": sid(done_copy, "EMPTYCO"), "symbol": "EMPTYCO",
                                     "first_date": DAYS[42].isoformat(), "days": 1}]
    assert s["checks"]["CLOSING_AUCTION"] == 1
    with open(os.path.join(mp.out_dir(done_copy), "check", "summary.json"), encoding="utf-8") as fh:
        assert json.load(fh)["closing_auction"][0]["first_date"] == DAYS[42].isoformat()
    chk = pd.read_csv(os.path.join(mp.out_dir(done_copy), "check", "minute_check.csv"), dtype=str)
    assert chk.loc[chk["date"] == DAYS[42].isoformat(), "check"].tolist() == ["CLOSING_AUCTION"]
    again = md.checks(done_copy, log=q)                                       # nothing left to change
    assert again["changed"] == [] and again["before"] == again["after"]


def test_checks_refuses_when_other_columns_differ(done_copy):
    path = ms.stock_dir(done_copy, sid(done_copy, "BIG"))
    d = ms.read_days(path)
    d.loc[d.index[5], "ratio"] = 9.99                                          # not what the rules give
    ms.write_days(d, os.path.join(path, ms.DAYS_FILE))
    data = os.path.join(done_copy, "equity_1min", "data")
    before = hashes(done_copy, (os.path.relpath(data, done_copy),))
    res = md.checks(done_copy, apply=True, log=q)
    assert res["refused"] == [f"BIG ({sid(done_copy, 'BIG')})"]
    assert ms.read_days(path)["ratio"].iat[5] == 9.99                         # not written
    assert hashes(done_copy, (os.path.relpath(data, done_copy),)) == before
    assert cli.main(["checks", "--apply", "--folder", done_copy]) == 1


def test_checks_dry_run_needs_current_cache(done_copy):
    info = os.path.join(md.cache_dir(done_copy), "cache_info.json")
    os.remove(info)
    with pytest.raises(md.CacheStale):
        md.checks(done_copy, log=q)
    assert not os.path.exists(info)                                           # still nothing written
    assert cli.main(["checks", "--folder", done_copy]) == 1
    assert md.checks(done_copy, apply=True, log=q)["refused"] == [] and os.path.isfile(info)


def test_checks_lock(done_copy):
    lock = RunLock(mp.lock_path(done_copy)).acquire("test")
    try:
        with pytest.raises(mp.LockBusy):
            md.checks(done_copy, apply=True, log=q)
        md.checks(done_copy, log=q)                                           # the dry run takes no lock
    finally:
        lock.release()

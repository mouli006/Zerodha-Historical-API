"""backfill/equity/icici_plan.py (icici_minute.py plan) on a synthetic world in tmp_path: a bhavcopy store, the
master, factors, exit list and index files built with the real builders, a hand-written kite_gaps.csv, a Kite store
days.parquet for the live stocks, and a fake Breeze client (FakeBreeze, also used by tests/test_icici_download.py).
No network; .env is never read (config.load_env stubbed); the Kite store is only read."""
import hashlib
import io
import json
import os
import shutil
import time
import zipfile
from datetime import date, datetime, time as dtime, timedelta

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests

import config
import icici_minute as cli
from backfill import fetch
from backfill.equity import adjustments as adj
from backfill.equity import exits as ex
from backfill.equity import icici_client as ic
from backfill.equity import icici_plan as ip
from backfill.equity import icici_store as st
from backfill.equity import index_membership as im
from backfill.equity import minute_plan as mp
from backfill.equity import minute_store as ms
from backfill.equity import security_master as sm
from backfill.job import STOP
from backfill.locks import RunLock
from tests.test_equity_minute_download import CA_HEADER, COLUMNS, HEADERS, FastStop, weekdays

N = 130                       # dead stocks stop by day 50: more than 60 trading days before the end (EXITED)
DAYS = weekdays(date(2015, 1, 1), N)
BONUS = 30                    # DEADA: bonus 1:1 on DAYS[30]; ICICI's history is frozen-adjusted for it
MW = 3                        # the market-wide gap row covers DAYS[0..2]
NOW = datetime(2015, 9, 1, 10, 0, tzinfo=mp.IST)

STOCKS = {
    "DEADA": dict(isin="INE201A01011", last=50, code="DEAA", base=1000.0, start=5, empty={33}, no_day={20},
                  short={15}, bad_vol={25}),
    "DEADB": dict(isin="INE202A01011", last=40, code="DEAB", base=300.0, icici_isin="INE902A01011"),
    "DEADC": dict(isin="INE203A01011", last=40, code="DEAC", base=200.0, icici_isin="INE903A01011", off=1.3),
    "DEADD": dict(isin="INE204A01011", last=40, code=None, base=100.0),
    "LIVE1": dict(isin="INE205A01011", last=N - 1, code="LIVA", base=500.0, kite_missing=set(range(MW)) | {40, 41, 42}),
    "LIVE2": dict(isin="INE206A01011", last=N - 1, code="LIVB", base=400.0, kite_missing=set(range(MW))),
}


def raw(sym, i):
    """The bhavcopy row of a stock on day i (DEADA halves at its bonus)."""
    s = STOCKS[sym]
    c = s["base"] + i
    v = 10000 + 10 * i
    if sym == "DEADA" and i >= BONUS:
        c, v = (s["base"] + i) / 2, v * 2
    c = round(c, 2)
    return {"open": round(c * 0.99, 2), "high": round(c * 1.1, 2), "low": round(c * 0.9, 2), "close": c, "volume": v}


def icici_scale(sym, i):
    return 2.0 if sym == "DEADA" and i < BONUS else 1.0


def store_rows():
    rows = []
    for sym, s in STOCKS.items():
        for i in range(0, s["last"] + 1):
            b = raw(sym, i)
            rows.append({"date": DAYS[i], "symbol": sym, "series": "EQ", "isin": s["isin"], **b, "last": b["close"],
                         "prev_close": b["close"], "turnover": 1e7 * (6 - list(STOCKS).index(sym)), "trades": 1,
                         "fin_id": "1", "format": "old"})
    for i in range(N):
        rows.append({"date": DAYS[i], "symbol": "FILLER", "series": "N1", "isin": "INE999Z01011", "open": 1.0,
                     "high": 1.0, "low": 1.0, "close": 1.0, "last": 1.0, "prev_close": 1.0, "volume": 1,
                     "turnover": 1.0, "trades": 1, "fin_id": "1", "format": "old"})
    return rows


def build_world(folder):
    store = os.path.join(folder, "NSE_CM_bhav")
    os.makedirs(store, exist_ok=True)
    path = os.path.join(store, "2015.parquet")
    pq.write_table(pa.Table.from_pandas(pd.DataFrame(store_rows(), columns=COLUMNS), preserve_index=False), path)
    old = time.time() - 600
    os.utime(path, (old, old))
    sm.build(folder, log=lambda *_: None)
    ca = os.path.join(folder, "adjustments", "input")
    os.makedirs(ca, exist_ok=True)
    pd.DataFrame([["DEADA", "DEADA LTD", "EQ", "Bonus 1:1", "10", f"{DAYS[BONUS]:%d-%b-%Y}", "-", "-", "-"]],
                 columns=CA_HEADER).to_csv(os.path.join(ca, "ca.csv"), index=False)
    adj.build(folder, log=lambda *_: None)
    ref = os.path.join(folder, "NSE_CM_bhav", "reference")
    os.makedirs(ref, exist_ok=True)
    mergers = [{"circular_no": f"C{k}", "old_symbol": sym, "old_company": f"{sym} Limited", "scheme_type": "MERGER",
                "exit_expected": "Y", "new_company": "Live One Limited", "new_symbol": "LIVE1",
                "swap_ratio": "1 for 1", "last_trading_date": str(DAYS[STOCKS[sym]["last"]])}
               for k, sym in enumerate(("DEADA", "DEADB", "DEADC", "DEADD"))]
    for name, cols in HEADERS.items():
        rows = mergers if name == "mergers.csv" else []
        pd.DataFrame([{c: x.get(c, "") for c in cols} for x in rows], columns=cols) \
            .to_csv(os.path.join(ref, name), index=False, lineterminator="\n")
    ex.build(folder, log=lambda *_: None)
    out = im.out_dir(folder)
    os.makedirs(out, exist_ok=True)
    sid = {s: sm.lookup(s, folder)[0] for s in STOCKS}
    mem = [("NIFTY 50", "DEADA", DAYS[0], DAYS[50]), ("NIFTY 50", "LIVE1", DAYS[0], None),
           ("NIFTY 500", "DEADA", DAYS[0], DAYS[50]), ("NIFTY 500", "DEADB", DAYS[0], DAYS[40]),
           ("NIFTY 500", "LIVE1", DAYS[0], None), ("NIFTY 500", "LIVE2", DAYS[0], None)]
    m = pd.DataFrame([{"index": i, "security_id": sid[s], "symbol": s, "isin": STOCKS[s]["isin"], "from_date": a,
                       "to_date": b, "source": "test"} for i, s, a, b in mem], columns=im.MEMBERSHIP_COLUMNS)
    pq.write_table(pa.Table.from_pandas(m, preserve_index=False), os.path.join(out, "nifty_membership.parquet"))
    pq.write_table(pa.Table.from_pandas(pd.DataFrame(columns=im.AMFI_COLUMNS).astype(str), preserve_index=False),
                   os.path.join(out, "amfi.parquet"))
    cov = {i: {"first": None, "last": None, "why": "no current list"} for i in im.INDEXES}
    for i in ("NIFTY 50", "NIFTY 500"):
        cov[i] = {"first": str(DAYS[0]), "last": str(DAYS[N - 1]), "reliable_from": str(DAYS[0])}
    with open(os.path.join(out, "build_info.json"), "w", encoding="utf-8") as f:
        json.dump({"coverage": cov}, f)
    write_kite(folder, sid)
    return sid


def gap(data_missing, sid, sym, a, b, reason, priority, n):
    return {"data_missing": data_missing, "security_id": sid, "symbols": f"{sym} {DAYS[a]}..{DAYS[b]}" if sym else "",
            "isins": "", "company_name": "", "need_from": str(DAYS[a]), "need_to": str(DAYS[b]), "trading_days": n,
            "reason": reason, "exit_note": "", "indices": "", "median_turnover": "", "priority": priority,
            "note": ""}


def write_kite(folder, sid):
    """The Kite store's days.parquet of the live stocks (date, minute_rows) and phase 1's kite_gaps.csv."""
    for sym in ("LIVE1", "LIVE2"):
        s = STOCKS[sym]
        path = ms.stock_dir(folder, sid[sym])
        os.makedirs(path, exist_ok=True)
        df = pd.DataFrame({"date": DAYS[:N], "minute_rows": [0 if i in s["kite_missing"] else 375 for i in range(N)]})
        pq.write_table(pa.Table.from_pandas(df, preserve_index=False), os.path.join(path, ms.DAYS_FILE))
    rows = [gap("1MIN", "", "", 0, MW - 1, "KITE_MINUTE_START", 1, MW),
            gap("BOTH", sid["DEADA"], "DEADA", 0, 50, "MERGED", 1, 51),
            gap("BOTH", sid["DEADB"], "DEADB", 0, 40, "MERGED", 2, 41),
            gap("BOTH", sid["DEADC"], "DEADC", 0, 40, "MERGED", 3, 41),
            gap("BOTH", sid["DEADD"], "DEADD", 0, 40, "MERGED", 3, 41),
            gap("1MIN", sid["LIVE1"], "LIVE1", 40, 42, "MINUTE_EMPTY_DAYS", 1, 3),
            gap("BOTH", sid["LIVE1"], "LIVE1", 45, 47, "NO_KITE_DATA", 1, 3),        # stale: Kite has these minutes
            gap("EOD", sid["LIVE1"], "LIVE1", 30, 31, "EOD_EMPTY_DAYS", 1, 2)]       # EOD only: not ICICI's job
    os.makedirs(mp.plan_dir(folder), exist_ok=True)
    pd.DataFrame(rows, columns=mp.GAP_COLUMNS).to_csv(os.path.join(mp.plan_dir(folder), "kite_gaps.csv"), index=False)


# ---- the fake Breeze client ----

def master_zip(extra=()):
    rows = [{"Token": str(100 + k), "ShortName": s["code"], "Series": "EQ", "CompanyName": f"{sym} LIMITED",
             "ISINCode": s.get("icici_isin", s["isin"]), "ExchangeCode": sym}
            for k, (sym, s) in enumerate(STOCKS.items()) if s["code"]]
    rows += list(extra)
    cols = ip.MASTER_COLUMNS + ["Lotsize"]
    text = ", ".join(f'"{c}"' for c in cols) + "\n"
    for r in rows:
        text += ",".join(f'"{r.get(c, "1")}"' for c in cols) + "\n"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("NSEScripMaster.txt", text)
        z.writestr("BSEScripMaster.txt", "x\n")
    return buf.getvalue()


def icici_bars(sym, i, d, big=False):
    """ICICI's 1-minute bars of day i, on its frozen scale: a 09:07 pre-open bar, three session bars (the day's high
    and low; a last close that is not the official close) and a 15:31 bar at the official close; DEADA's short day
    starts at 12:30, its bad-volume day has 3x the volume."""
    s, b = STOCKS[sym], raw(sym, i)
    sc = icici_scale(sym, i) * s.get("off", 1.0)
    o, h, l, c = (b[x] / sc for x in ("open", "high", "low", "close"))
    vol = b["volume"] * icici_scale(sym, i) * 0.99
    if i in s.get("bad_vol", ()):
        vol *= 3
    last = min(c * 1.002, h)
    bars = [(dtime(9, 7), o, o, o, o, 0.1), (dtime(9, 15), o, max(o, c), l, c, 0.3), (dtime(12, 0), c, h, c, c, 0.3),
            (dtime(15, 29), c, max(c, last), min(c, last), last, 0.3), (dtime(15, 31), c, c, c, c, 0.0)]
    if i in s.get("short", ()):
        bars = [(dtime(12, 30), o, max(o, c), l, c, 0.3), (dtime(13, 0), c, h, c, c, 0.3)]
    if big:
        bars = [((datetime.combine(d, dtime(9, 15)) + timedelta(minutes=k)).time(), c, h, l, c, 1 / 600)
                for k in range(600)]
    return [{"close": round(bc, 2), "datetime": f"{d} {t:%H:%M:%S}", "exchange_code": "NSE", "high": round(bh, 2),
             "low": round(bl, 2), "open": round(bo, 2), "stock_code": s["code"], "volume": int(vol * f)}
            for t, bo, bh, bl, bc, f in bars]


class FakeBreeze:
    """The three calls of BreezeClient on the synthetic world. auth_after: the n-th historical call (0-based) and
    later raise IciciSignIn; rate_at: historical call numbers answering with a rate limit; fail: (code, interval,
    from) calls that fail; big: codes with 600 bars a day (a 2-day answer is cut to its last 1,000)."""

    def __init__(self, auth_after=None, rate_at=(), fail=(), big=(), reject=False, extra_master=()):
        self.calls, self.n = [], 0
        self.auth_after, self.rate_at, self.fail, self.big = auth_after, set(rate_at), set(fail), set(big)
        self.reject, self.extra_master = reject, extra_master
        self.by_code = {s["code"]: sym for sym, s in STOCKS.items() if s["code"]}

    def customer(self):
        self.calls.append(("session",))
        if self.reject:
            raise ic.IciciSignIn("ICICI rejected the session (HTTP 200: Invalid session.): " + ic.SIGN_IN)
        return ["idirect_userid"]

    def security_master(self):
        self.calls.append(("master",))
        return master_zip(self.extra_master)

    def historical(self, code, interval, start, end):
        k = self.n
        self.n += 1
        self.calls.append((code, interval, start, end))
        if self.auth_after is not None and k >= self.auth_after:
            raise ic.IciciSignIn("ICICI rejected the session (HTTP 401: Session expired): " + ic.SIGN_IN)
        if k in self.rate_at:
            raise ic.IciciRateLimit("HTTP 429: Too many requests")
        if (code, interval, start) in self.fail:
            raise ic.IciciFailed("HTTP 500: something broke")
        sym = self.by_code[code]
        s = STOCKS[sym]
        out = []
        for i, d in enumerate(DAYS):
            if not start <= d <= end or i > s["last"] or i < s.get("start", 0) or i in s.get("empty", ()):
                continue
            bars = icici_bars(sym, i, d, code in self.big)
            if interval == "1day":
                if i in s.get("no_day", ()):
                    continue
                b = raw(sym, i)
                sc = icici_scale(sym, i) * s.get("off", 1.0)
                out.append({"close": round(b["close"] / sc, 2), "datetime": f"{d} 00:00:00", "exchange_code": "NSE",
                            "high": max(x["high"] for x in bars), "low": min(x["low"] for x in bars),
                            "open": bars[0]["open"], "stock_code": code, "volume": sum(x["volume"] for x in bars)})
            else:
                out += bars
        return out[-ic.MAX_BARS:]

    def historical_calls(self, interval=None):
        return [c for c in self.calls if len(c) == 4 and (interval is None or c[1] == interval)]


# ---- fixtures ----

def q(*a, **k):
    pass


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


@pytest.fixture(scope="module")
def base(tmp_path_factory):
    f = str(tmp_path_factory.mktemp("icici") / "data")
    mpatch = pytest.MonkeyPatch()
    mpatch.setattr(config, "load_env", lambda: {})
    build_world(f)
    mpatch.undo()
    return f


@pytest.fixture
def world(base, tmp_path):
    f = str(tmp_path / "data")
    shutil.copytree(base, f)
    return f


def sid(f, sym):
    return sm.lookup(sym, f)[0]


def hashes(folder, sub):
    out = {}
    for root, _, files in os.walk(os.path.join(folder, sub)):
        for name in files:
            if name == ".lock":
                continue
            p = os.path.join(root, name)
            with open(p, "rb") as fh:
                out[p] = hashlib.sha256(fh.read()).hexdigest()
    return out


def run_plan(f, client=None, **kw):
    return ip.plan(f, client=client or FakeBreeze(), now=NOW, log=q, **kw)


# ---- tests ----

def test_plan_days_matching_and_order(world):
    kite = hashes(world, "equity_1min")
    res = run_plan(world)
    p = res["plan"].set_index("symbol")
    assert list(res["plan"]["symbol"]) == ["DEADA", "LIVE1", "DEADB", "DEADC", "DEADD"]   # P1 dead, P1 live, P2, P3
    assert p.loc["DEADA", "match_kind"] == "ISIN" and p.loc["DEADA", "icici_code"] == "DEAA"
    assert p.loc["DEADB", "match_kind"] == "SYMBOL_ONLY" and p.loc["DEADB", "icici_code"] == "DEAB"
    assert p.loc["DEADC", "match_kind"] == "SYMBOL_ONLY"
    assert p.loc["DEADD", "match_kind"] == "NOT_IN_ICICI" and p.loc["DEADD", "est_calls"] == 0
    assert p.loc["DEADA", "life"] == "DEAD" and p.loc["LIVE1", "life"] == "LIVE"
    assert p.loc["DEADA", "trading_days"] == 51 and p.loc["DEADA", "need_ranges"] == f"{DAYS[0]}..{DAYS[50]}"
    assert p.loc["DEADA", "est_minute_calls"] == 26 and p.loc["DEADA", "est_day_calls"] == 1
    # LIVE1: the market-wide days and its own empty days; the stale row and the EOD row cost nothing
    d = res["days"]
    live1 = sorted(d[d["security_id"] == sid(world, "LIVE1")]["date"])
    assert live1 == [DAYS[i] for i in (0, 1, 2, 40, 41, 42)]
    assert p.loc["LIVE1", "kite_covered_days"] == 3 and p.loc["LIVE1", "need_ranges"] == \
        f"{DAYS[0]}..{DAYS[2]}; {DAYS[40]}..{DAYS[42]}"
    assert "LIVE2" not in p.index                      # priority 2: no market-wide days by default
    rows = res["rows"].set_index("reason")
    assert rows.loc["NO_KITE_DATA", "kite_covered"] == 3 and rows.loc["NO_KITE_DATA", "icici_days"] == 0
    assert "EOD_EMPTY_DAYS" not in rows.index
    assert rows.loc["KITE_MINUTE_START", "stocks"] == 1     # LIVE1 only (ACTIVE, priority 1)
    for name in ("icici_plan.csv", "icici_plan_days.parquet", "plan_rows.csv", "plan_report.txt", "plan_info.json"):
        assert os.path.isfile(os.path.join(st.plan_dir(world), name))
    assert "Fetchable: 4 stocks" in res["report"] and "days of quota" in res["report"]
    assert hashes(world, "equity_1min") == kite                # the Kite store is only read
    back, days = ip.read_plan(world)
    assert len(back) == 5 and len(days) == len(d)


def test_market_wide_priority_2(world):
    res = run_plan(world, market_wide=2)
    p = res["plan"].set_index("symbol")
    assert p.loc["LIVE2", "trading_days"] == MW and p.loc["LIVE2", "priority"] == 2
    assert res["rows"].set_index("reason").loc["KITE_MINUTE_START", "stocks"] == 2


def test_isin_beats_symbol_and_review():
    hist = pd.DataFrame({"symbol": ["OLD", "NEW"], "isin": ["INE1", "INE2"]})
    master = pd.DataFrame([{"Token": "1", "ShortName": "AAA", "Series": "EQ", "CompanyName": "A", "ISINCode": "INE2",
                            "ExchangeCode": "ZZZ"},
                           {"Token": "2", "ShortName": "BBB", "Series": "EQ", "CompanyName": "B", "ISINCode": "INE9",
                            "ExchangeCode": "OLD"}])
    by_isin, by_sym = ip.master_index(master)
    code, _, _, kind, note = ip.match_stock(hist, by_isin, by_sym)
    assert (code, kind) == ("AAA", "ISIN") and "INE2" in note       # the old symbol's scrip BBB loses
    by_isin, by_sym = ip.master_index(master.assign(ISINCode=["INE7", "INE8"]))
    assert ip.match_stock(hist, by_isin, by_sym)[3] == "SYMBOL_ONLY"
    two = pd.concat([master, master.assign(ShortName=["CCC", "DDD"])])
    by_isin, by_sym = ip.master_index(two)
    code, _, _, kind, note = ip.match_stock(hist, by_isin, by_sym)
    assert kind == "REVIEW" and code == "" and "AAA" in note and "CCC" in note
    by_isin, by_sym = ip.master_index(master.assign(ISINCode=["X", "Y"], ExchangeCode=["P", "Q"]))
    assert ip.match_stock(hist, by_isin, by_sym)[3] == "NOT_IN_ICICI"


def test_chunk_helpers():
    cal = DAYS[:10]
    days = [cal[0], cal[1], cal[2], cal[5], cal[7], cal[8]]
    assert ip.pair_chunks(days, cal) == [(cal[0], cal[1]), (cal[2], cal[2]), (cal[5], cal[5]), (cal[7], cal[8])]
    assert ip.minute_calls(days, cal) == 4
    assert ip.day_call_ranges(cal[0], cal[9], cal) == [(cal[0], cal[9])]
    big = weekdays(date(2015, 1, 1), 2000)
    r = ip.day_call_ranges(big[0], big[-1], big)
    assert len(r) == 3 and r[0] == (big[0], big[899]) and r[1][0] == big[900] and r[-1][1] == big[-1]


def test_master_saved_and_reused(world):
    fake = FakeBreeze()
    run_plan(world, client=fake)
    ref = st.reference_dir(world)
    assert sorted(os.listdir(ref)) == [f"NSEScripMaster_{NOW.date()}.txt", f"SecurityMaster_{NOW.date()}.zip"]
    with open(os.path.join(ref, f"SecurityMaster_{NOW.date()}.zip"), "rb") as fh:
        assert fh.read() == master_zip()                  # saved as received
    run_plan(world, client=fake)
    assert fake.calls.count(("master",)) == 1             # today's copy is reused
    later = FakeBreeze()
    ip.plan(world, client=later, offline=True, now=NOW + timedelta(days=3), log=q)
    assert later.calls == []                              # offline: the newest saved copy


def test_offline_without_master_and_missing_gaps(world):
    with pytest.raises(mp.NotBuilt):
        ip.plan(world, client=FakeBreeze(), offline=True, now=NOW, log=q)
    os.remove(os.path.join(mp.plan_dir(world), "kite_gaps.csv"))
    with pytest.raises(mp.NotBuilt):
        run_plan(world)


def test_plan_refusals(world):
    lock = RunLock(st.lock_path(world)).acquire("test")
    try:
        with pytest.raises(mp.LockBusy):
            run_plan(world)
    finally:
        lock.release()
    up = RunLock(os.path.join(world, "equity_update.lock")).acquire("test")
    try:
        with pytest.raises(mp.StoreBusy):
            run_plan(world)
        assert cli.main(["plan", "--folder", world]) == 2
    finally:
        up.release()
    busy = RunLock(os.path.join(world, "exits", ".lock")).acquire("test")
    try:
        with pytest.raises(mp.StoreBusy):
            run_plan(world)
    finally:
        busy.release()


def test_cli_plan_offline_and_errors(world, capsys):
    run_plan(world)
    assert cli.main(["plan", "--offline", "--folder", world]) == 0
    assert "Written to" in capsys.readouterr().out
    with pytest.raises(SystemExit) as e:
        cli.main(["plan", "--folder", "relative"])
    assert e.value.code == 2
    other = world + "_empty"
    os.makedirs(other)
    assert cli.main(["status", "--folder", other]) == 1     # no plan yet

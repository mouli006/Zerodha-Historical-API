"""backfill/equity/minute_plan.py and equity_minute.py on a small synthetic world in tmp_path: a bhavcopy store,
the security master, factors and exit list built from it with the real builders, index membership files with the
real columns, and a fake KiteConnect (instruments, historical_data). No network; .env is never read
(config.load_env stubbed); every input is only read."""
import hashlib
import json
import os
import shutil
import sqlite3
import time
from datetime import date, datetime, time as dtime, timedelta

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests
from kiteconnect import exceptions as kex

import config
import equity_minute as cli
from backfill.equity import adjustments as adj
from backfill.equity import exits as ex
from backfill.equity import index_membership as im
from backfill.equity import minute_plan as mp
from backfill.equity import security_master as sm
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


N = 80
DAYS = weekdays(date(2015, 1, 1), N)
FEB2 = DAYS.index(date(2015, 2, 2))
SESSION = 70            # Kite has no candle on this day for any stock
SPLIT_DAY = 50
NOW = datetime(2015, 4, 25, 10, 0, tzinfo=mp.IST)


def r(a, b):
    return list(range(a, b))


# symbol: isin, trading days, price level, token (None: not in Kite), Kite behaviour, minute start (day index)
STOCKS = {
    "RELI": dict(isin="INE002A01018", idx=r(0, N), level=lambda i: 1000 + i, token=101, minute=FEB2, tv=2.0),
    "SPLITCO": dict(isin="INE070A01011", idx=r(0, N), level=lambda i: 5 * (100 + i) if i < SPLIT_DAY else 100 + i,
                    token=102, kite="split", minute=FEB2),
    "ADDCO": dict(isin="INE071A01011", idx=r(0, N), level=lambda i: 100 + i, token=103, kite="add", minute=FEB2),
    "MIXCO": dict(isin="INE072A01011", idx=r(0, N), level=lambda i: 100 + i, token=104, kite="mix", minute=FEB2),
    "LATECO": dict(isin="INE073A01011", idx=r(40, N), level=lambda i: 50 + i, token=105, kite_from=46, minute=60),
    "EMPTYCO": dict(isin="INE074A01011", idx=r(0, N), level=lambda i: 80 + i, token=106, kite="vol30",
                    kite_missing={20, 21, 22}, minute=FEB2),
    "BECO": dict(isin="INE075A01011", idx=r(0, N), level=lambda i: 30.0, token=107, minute=FEB2,
                 series=lambda i: "BE" if i >= 60 else "EQ"),
    "NOMIN": dict(isin="INE076A01011", idx=r(0, N), level=lambda i: 60.0, token=108, minute=None),
    "PRICEX": dict(isin="INE077A01011", idx=r(0, N), level=lambda i: 70.0, token=109, kite="x1.1", minute=FEB2),
    "NEWNM": dict(isin="INE078A01011", idx=r(0, N), level=lambda i: 90.0, token=110, minute=FEB2,
                  symbol=lambda i: "OLDNM" if i < 30 else "NEWNM"),
    "GEN1": dict(isin="INE081A01011", idx=r(0, N), level=lambda i: 40.0, token=111, minute=0),
    "GEN2": dict(isin="INE082A01011", idx=r(0, N), level=lambda i: 41.0, token=112, minute=FEB2),
    "GEN3": dict(isin="INE083A01011", idx=r(0, N), level=lambda i: 42.0, token=113, minute=FEB2),
    "FAILCO": dict(isin="INE084A01011", idx=r(0, N), level=lambda i: 43.0, token=199, minute=FEB2),
    "NOTOK": dict(isin="INE085A01011", idx=r(0, N), level=lambda i: 44.0, token=None),
    "TWIN": dict(isin="INE086A01011", idx=r(0, N), level=lambda i: 45.0, token=None),
    "OTHERSER": dict(isin="INE087A01011", idx=r(0, N), level=lambda i: 46.0, token=None),
    "GONE": dict(isin="INE088A01011", idx=r(0, 11), level=lambda i: 200.0, token=None, tv=50.0),
    "DORMCO": dict(isin="INE089A01011", idx=r(0, 51), level=lambda i: 47.0, token=None),
    "ETFX": dict(isin="INF090A01011", idx=r(0, N), level=lambda i: 10.0, token=130, minute=FEB2),
    "ABC-RE": dict(isin="INE091A01011", idx=r(5, 20), level=lambda i: 1.0, token=None),
}
EXTRA_INSTRUMENTS = [("TWIN", 120), ("TWIN-BE", 121), ("OTHERSER-BE", 122), ("GONE", 123), ("ABC-RE", 124),
                     ("RELI-N1", 125)]
HEADERS = {   # the real headers of the reference lists (nse_reference.py, 2026-10-17)
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


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def refuse(*a, **k):
        raise AssertionError("a test tried to reach the network")
    monkeypatch.setattr(requests.Session, "request", refuse)
    monkeypatch.setattr(config, "load_env", lambda: {})


def raw(sym, i):
    c = round(float(STOCKS[sym]["level"](i)), 2)
    return {"open": round(c * 0.99, 2), "high": round(c * 1.1, 2), "low": round(c * 0.9, 2), "close": c,
            "volume": 1000 + i}


def store_rows():
    rows = []
    for sym, s in STOCKS.items():
        for i in s["idx"]:
            b = raw(sym, i)
            rows.append({"date": DAYS[i], "symbol": s.get("symbol", lambda _: sym)(i),
                         "series": s.get("series", lambda _: "EQ")(i), "isin": s["isin"], **b, "last": b["close"],
                         "prev_close": b["close"], "turnover": s.get("tv", 1.0) * 1e7, "trades": 1, "fin_id": "1",
                         "format": "old"})
    for i in r(0, N):   # an out-of-scope row every day, like the real calendar
        rows.append({"date": DAYS[i], "symbol": "FILLER", "series": "N1", "isin": "INE999Z01011", "open": 1.0,
                     "high": 1.0, "low": 1.0, "close": 1.0, "last": 1.0, "prev_close": 1.0, "volume": 1,
                     "turnover": 1.0, "trades": 1, "fin_id": "1", "format": "old"})
    return rows


def kite_row(sym, i):
    """Kite's day candle: our factor for the split, plus each stock's own behaviour."""
    s, b = STOCKS[sym], raw(sym, i)
    f = 5.0 if sym == "SPLITCO" and i < SPLIT_DAY else 1.0
    k = {x: b[x] / f for x in ("open", "high", "low", "close")}
    vol = b["volume"] * f
    kind = s.get("kite")
    if kind == "split" and i < 30:
        k = {x: v / 1.02 for x, v in k.items()}                 # a demerger-like multiplicative step
    if kind == "add":
        c = 4.6 if i < 20 else 2.0 if i < 40 else 0.0          # two additive periods, then none
        k = {x: v - c for x, v in k.items()}
    if kind == "mix" and i < 10:
        k = {x: b[x] / (1.03 if x == "close" else 1.05) for x in k}
    if kind == "vol30" and i == 30:
        vol *= 2
    if kind == "x1.1":
        k = {x: v * 1.1 for x, v in k.items()}
    return {**{x: round(v, 2) for x, v in k.items()}, "volume": int(vol)}


def kite_days(sym):
    s = STOCKS[sym]
    return [i for i in s["idx"] if i >= s.get("kite_from", 0) and i != SESSION and i not in s.get("kite_missing", ())]


class FakeKite:
    def __init__(self, fail=(199,), auth_after=None, stop_after=None, tokens=None):
        self.calls, self.fail, self.auth_after, self.stop_after = [], set(fail), auth_after, stop_after
        self.by_token = {s["token"]: sym for sym, s in STOCKS.items() if s["token"]}
        if tokens:
            self.by_token.update(tokens)

    def instruments(self, exchange):
        self.calls.append(("instruments", exchange))
        out = []
        for sym, s in STOCKS.items():
            if s["token"]:
                ts = f"{sym}-BE" if sym == "BECO" else sym
                out.append(self.inst(ts, s["token"]))
        out += [self.inst(ts, tok) for ts, tok in EXTRA_INSTRUMENTS]
        out.append({**self.inst("NIFTY 50", 256265), "segment": "INDICES"})
        return out

    @staticmethod
    def inst(ts, token):
        return {"instrument_token": token, "exchange_token": str(token // 256), "tradingsymbol": ts,
                "name": f"{ts} LIMITED", "last_price": 0.0, "expiry": "", "strike": 0.0, "tick_size": 0.05,
                "lot_size": 1, "instrument_type": "EQ", "segment": "NSE", "exchange": "NSE"}

    def historical_data(self, token, start, end, interval, continuous=False, oi=False):
        n = sum(1 for c in self.calls if c[0] != "instruments")
        if self.auth_after is not None and n >= self.auth_after:
            raise kex.TokenException("Incorrect `api_key` or `access_token`.")
        if self.stop_after is not None and n >= self.stop_after:
            raise KeyboardInterrupt()
        self.calls.append((token, start.date(), end.date(), interval))
        if token in self.fail:
            raise kex.KiteException("something broke", code=500)
        sym = self.by_token[token]
        s = STOCKS[sym]
        out = []
        for i in kite_days(sym):
            if not start.date() <= DAYS[i] <= end.date():
                continue
            if interval == "day":
                out.append({"date": datetime.combine(DAYS[i], dtime()).replace(tzinfo=mp.IST), **kite_row(sym, i)})
            elif s.get("minute") is not None and i >= s["minute"]:
                out.append({"date": datetime.combine(DAYS[i], dtime(9, 15)).replace(tzinfo=mp.IST),
                            **kite_row(sym, i)})
        return out


def write_store(folder):
    store = os.path.join(folder, "NSE_CM_bhav")
    os.makedirs(store, exist_ok=True)
    df = pd.DataFrame(store_rows(), columns=COLUMNS)
    path = os.path.join(store, "2015.parquet")
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), path)
    old = time.time() - 600
    os.utime(path, (old, old))


def write_refs(folder):
    ref = os.path.join(folder, "NSE_CM_bhav", "reference")
    os.makedirs(ref, exist_ok=True)
    rows = {"mergers.csv": [{"circular_no": "C1", "old_symbol": "GONE", "old_company": "Gone Limited",
                             "scheme_type": "MERGER", "exit_expected": "Y", "new_company": "Reli Limited",
                             "new_symbol": "RELI", "swap_ratio": "42 for 25", "last_trading_date": str(DAYS[10])}]}
    for name, cols in HEADERS.items():
        pd.DataFrame([{c: x.get(c, "") for c in cols} for x in rows.get(name, [])], columns=cols) \
            .to_csv(os.path.join(ref, name), index=False, lineterminator="\n")


def write_ca(folder):
    path = os.path.join(folder, "adjustments", "input")
    os.makedirs(path, exist_ok=True)
    pd.DataFrame([["SPLITCO", "SPLITCO LTD", "EQ", "Face Value Split From Rs 10 To Rs 2", "10",
                   f"{DAYS[SPLIT_DAY]:%d-%b-%Y}", "-", "-", "-"]], columns=CA_HEADER) \
        .to_csv(os.path.join(path, "ca.csv"), index=False)


def write_index(folder):
    """nifty_membership.parquet, amfi.parquet and build_info.json with the real columns (im.load_built's files)."""
    out = im.out_dir(folder)
    os.makedirs(out, exist_ok=True)
    sid = {s: sm.security_id_for(s, DAYS[-1] if s != "GONE" else DAYS[5], folder) for s in
           ("RELI", "SPLITCO", "LATECO", "GONE")}
    rows = [("NIFTY 50", "RELI", DAYS[0], None), ("NIFTY 50", "GONE", DAYS[0], DAYS[10]),
            ("NIFTY 500", "RELI", DAYS[0], None), ("NIFTY 500", "SPLITCO", DAYS[0], None),
            ("NIFTY 500", "LATECO", DAYS[40], None), ("NIFTY 500", "GONE", DAYS[0], DAYS[10])]
    mem = pd.DataFrame([{"index": i, "security_id": sid[s], "symbol": s, "isin": STOCKS[s]["isin"], "from_date": a,
                         "to_date": b, "source": "test"} for i, s, a, b in rows], columns=im.MEMBERSHIP_COLUMNS)
    pq.write_table(pa.Table.from_pandas(mem, preserve_index=False), os.path.join(out, "nifty_membership.parquet"))
    pq.write_table(pa.Table.from_pandas(pd.DataFrame(columns=im.AMFI_COLUMNS).astype(str), preserve_index=False),
                   os.path.join(out, "amfi.parquet"))
    cov = {i: {"first": None, "last": None, "why": "no current list"} for i in im.INDEXES}
    cov["NIFTY 50"] = {"first": str(DAYS[0]), "last": str(DAYS[-1]), "reliable_from": str(DAYS[0])}
    cov["NIFTY 500"] = {"first": str(DAYS[0]), "last": str(DAYS[-1]), "reliable_from": str(DAYS[20])}
    with open(os.path.join(out, "build_info.json"), "w", encoding="utf-8") as f:
        json.dump({"coverage": cov}, f)


def make_world(folder):
    write_store(folder)
    sm.build(folder, log=lambda *_: None)
    write_ca(folder)
    adj.build(folder, log=lambda *_: None)
    write_refs(folder)
    ex.build(folder, log=lambda *_: None)
    write_index(folder)


@pytest.fixture(scope="module")
def base(tmp_path_factory):
    tmp = str(tmp_path_factory.mktemp("minute_base"))
    mpatch = pytest.MonkeyPatch()
    mpatch.setattr(config, "load_env", lambda: {})
    make_world(tmp)
    mpatch.undo()
    return tmp


@pytest.fixture
def world(base, tmp_path):
    f = str(tmp_path / "data")
    shutil.copytree(base, f)      # copy2: modification times are kept
    return f


FAST = 1e6   # requests per second in tests: no waiting
RealLimiter = mp.RateLimiter


def run_all(f, kite=None, **kw):
    kite = kite or FakeKite()
    mp.tokens(f, kite=kite, now=NOW, log=lambda *a, **k: None)
    return mp.probe(f, kite=kite, speed=FAST, now=NOW, log=lambda *a, **k: None, **kw), kite


@pytest.fixture(scope="module")
def done(base, tmp_path_factory):
    """tokens + probe once on a copy of the world (read-only for the tests that use it)."""
    f = str(tmp_path_factory.mktemp("minute_done") / "data")
    shutil.copytree(base, f)
    mpatch = pytest.MonkeyPatch()
    mpatch.setattr(config, "load_env", lambda: {})
    result, kite = run_all(f)
    mpatch.undo()
    return f, result, kite


def read(f, name):
    return pd.read_csv(os.path.join(mp.plan_dir(f), name), dtype=str, keep_default_na=False)


def sid_of(f, sym):
    return sm.lookup(sym, f)[0]


def gap_rows(f, sym):
    g = read(f, "kite_gaps.csv")
    return g[g["security_id"] == sid_of(f, sym)].reset_index(drop=True)


def input_hashes(f):
    out = {}
    for sub in ("NSE_CM_bhav", "security_master", "adjustments", "exits", "index_membership"):
        for root, _, files in os.walk(os.path.join(f, sub)):
            for name in files:
                p = os.path.join(root, name)
                with open(p, "rb") as fh:
                    out[p] = hashlib.sha256(fh.read()).hexdigest()
    return out


# ---- universe and matching ----

def test_universe_stocks_only_rights_never_etf_on_request(world):
    ids = {u["symbol"] for u in mp.Inputs(world).universe}
    assert "RELI" in ids and "ETFX" not in ids and "ABC-RE" not in ids
    ids = {u["symbol"] for u in mp.Inputs(world, include_etf=True).universe}
    assert "ETFX" in ids and "ABC-RE" not in ids
    u = {x["symbol"]: x for x in mp.Inputs(world).universe}
    assert u["LATECO"]["need_from"] == DAYS[40] and u["RELI"]["need_from"] == DAYS[0]
    assert u["GONE"]["status"] == "EXITED" and u["GONE"]["exit_reason"] == "MERGER"
    assert u["DORMCO"]["status"] == "DORMANT"


def test_tokens_matching(world):
    kite = FakeKite()
    res = mp.tokens(world, kite=kite, now=NOW, log=lambda *a, **k: None)
    t = read(world, "kite_tokens.csv").set_index("symbol")
    assert list(read(world, "kite_tokens.csv").columns) == mp.TOKEN_COLUMNS
    assert t.loc["RELI", "match_status"] == "MATCHED_UNCHECKED" and t.loc["RELI", "instrument_token"] == "101"
    assert t.loc["RELI", "kite_name"] == "RELI LIMITED" and t.loc["RELI", "bhav_close"] == "1079.0"
    assert t.loc["BECO", "kite_tradingsymbol"] == "BECO-BE" and t.loc["BECO", "series"] == "BE"
    assert t.loc["BECO", "match_status"] == "MATCHED_UNCHECKED"
    assert t.loc["NEWNM", "kite_tradingsymbol"] == "NEWNM"
    assert t.loc["NOTOK", "match_status"] == "NO_TOKEN"
    assert t.loc["TWIN", "match_status"] == "REVIEW" and "2 candidates" in t.loc["TWIN", "note"]
    assert t.loc["OTHERSER", "match_status"] == "REVIEW" and "only OTHERSER-BE" in t.loc["OTHERSER", "note"]
    assert "GONE" not in t.index and "DORMCO" not in t.index and "ETFX" not in t.index   # never matched
    assert kite.calls == [("instruments", "NSE")]
    saved = os.path.join(mp.plan_dir(world), "kite_instruments_NSE_2015-04-25.csv")
    assert len(pd.read_csv(saved)) == len(kite.instruments("NSE"))
    info = res["info"]
    assert info["instruments_date"] == "2015-04-25" and info["bhav_last"] == str(DAYS[-1])
    assert "weekdays before the list" in res["report"]


def test_match_tokens_unit():
    u = [{"security_id": "S1", "isin": "I1", "symbol": "ABC", "series": "BZ", "last_close": 5.0, "status": "ACTIVE"},
         {"security_id": "S2", "isin": "I2", "symbol": "BAJAJ-AUTO", "series": "EQ", "last_close": 5.0,
          "status": "ACTIVE"},
         {"security_id": "S3", "isin": "I3", "symbol": "XYZ", "series": "EQ", "last_close": 5.0, "status": "ACTIVE"}]
    inst = [FakeKite.inst("ABC-BZ", 1), FakeKite.inst("BAJAJ-AUTO", 2), FakeKite.inst("XYZ-N1", 3),
            {**FakeKite.inst("XYZ", 4), "segment": "INDICES"}]
    rows = {x["security_id"]: x for x in mp.match_tokens(u, inst)}
    assert rows["S1"]["match_status"] == "MATCHED_UNCHECKED" and rows["S1"]["instrument_token"] == 1
    assert rows["S2"]["match_status"] == "MATCHED_UNCHECKED" and rows["S2"]["instrument_token"] == 2
    assert rows["S3"]["match_status"] == "NO_TOKEN"      # a bond series and an index never match


def test_tokens_with_etf(world):
    mp.tokens(world, kite=FakeKite(), include_etf=True, now=NOW, log=lambda *a, **k: None)
    t = read(world, "kite_tokens.csv").set_index("symbol")
    assert t.loc["ETFX", "match_status"] == "MATCHED_UNCHECKED"


def test_tokens_session_expired(world):
    class Expired(FakeKite):
        def instruments(self, exchange):
            raise kex.TokenException("expired")
    with pytest.raises(mp.SignInNeeded):
        mp.tokens(world, kite=Expired(), now=NOW, log=lambda *a, **k: None)


# ---- probe ----

def probes(f):
    return mp.read_probes(f)


def test_probe_eod_and_minute_first_days(done):
    f, result, kite = done
    p = probes(f)
    reli = p[sid_of(f, "RELI")]
    assert reli["status"] == "DONE" and reli["eod_first_day"] == str(DAYS[0])
    assert reli["eod_last_day"] == str(DAYS[-1]) and json.loads(reli["eod_missing"]) == [str(DAYS[SESSION])]
    assert reli["minute_first_day"] == str(DAYS[FEB2])          # the 2015-02-02 shortcut
    assert reli["requests"] == 3                                 # 1 day chunk, first day, 2015-02-02
    assert p[sid_of(f, "GEN1")]["minute_first_day"] == str(DAYS[0]) and p[sid_of(f, "GEN1")]["requests"] == 2
    late = p[sid_of(f, "LATECO")]
    assert late["eod_first_day"] == str(DAYS[46]) and late["minute_first_day"] == str(DAYS[60])   # binary search
    assert json.loads(late["eod_missing"])[:6] == [str(DAYS[i]) for i in range(40, 46)]
    assert p[sid_of(f, "NOMIN")]["minute_first_day"] == ""
    assert json.loads(p[sid_of(f, "EMPTYCO")]["eod_missing"]) == [str(DAYS[i]) for i in (20, 21, 22, SESSION)]
    assert os.path.isfile(os.path.join(mp.plan_dir(f), "kite_day", f"{sid_of(f, 'RELI')}.parquet"))


def test_probe_price_check(done):
    f, _, _ = done
    p = probes(f)
    assert p[sid_of(f, "RELI")]["price_status"] == "MATCHED"
    assert p[sid_of(f, "PRICEX")]["price_status"] == "PRICE_MISMATCH"
    plan = read(f, "plan.csv").set_index("security_id")
    assert plan.loc[sid_of(f, "PRICEX"), "match_status"] == "PRICE_MISMATCH"
    assert plan.loc[sid_of(f, "PRICEX"), "download_eod"] == "N"


def test_probe_price_check_without_candle_and_tolerance(world):
    s = mp.Prober(FakeKite(), mp.RateLimiter(FAST))
    u = {"security_id": "SX", "need_from": DAYS[0], "need_to": DAYS[-1], "last_close": 1079.0}
    os.makedirs(mp.plan_dir(world), exist_ok=True)
    row = s.probe(101, u, [DAYS[SESSION]], mp.plan_dir(world), 0.02)    # Kite has no candle on that day
    assert row["price_status"] == "MATCHED_UNCHECKED"
    row = s.probe(109, {**u, "last_close": 70.0}, [DAYS[-1]], mp.plan_dir(world), 0.2)   # 10% off, tol 20%
    assert row["price_status"] == "MATCHED"


def test_probe_failure_is_recorded_and_others_go_on(done):
    f, result, _ = done
    p = probes(f)
    fail = p[sid_of(f, "FAILCO")]
    assert fail["status"] == "FAILED" and "something broke" in fail["error"]
    assert result["stats"]["failed"] == 1 and result["stats"]["probed"] == 13
    plan = read(f, "plan.csv").set_index("security_id")
    assert "something broke" in plan.loc[sid_of(f, "FAILCO"), "note"]
    assert result["info"]["probe_failed"] == 1


def test_probe_resume_after_stop(world):
    mp.tokens(world, kite=FakeKite(), now=NOW, log=lambda *a, **k: None)
    first = mp.probe(world, kite=FakeKite(stop_after=7), speed=FAST, log=lambda *a, **k: None)
    assert first["stats"]["stopped"] and first["stats"]["probed"] == 2
    kite = FakeKite()
    second = mp.probe(world, kite=kite, speed=FAST, log=lambda *a, **k: None)
    assert second["stats"]["skipped"] == 2 and second["stats"]["probed"] == 11
    asked = {c[0] for c in kite.calls}
    assert 101 not in asked and 102 not in asked


def test_probe_token_error_stops_cleanly_then_resumes(world):
    mp.tokens(world, kite=FakeKite(), now=NOW, log=lambda *a, **k: None)
    with pytest.raises(mp.SignInNeeded, match="run probe again"):
        mp.probe(world, kite=FakeKite(auth_after=5), speed=FAST, log=lambda *a, **k: None)
    assert len([r for r in probes(world).values() if r["status"] == "DONE"]) == 1
    assert not mp.held(mp.lock_path(world))                     # the lock is released
    again = mp.probe(world, kite=FakeKite(), speed=FAST, log=lambda *a, **k: None)
    assert again["stats"]["skipped"] == 1


def test_probe_force_and_token_change(world):
    run_all(world)
    kite = FakeKite()
    res = mp.probe(world, kite=kite, speed=FAST, log=lambda *a, **k: None)
    assert res["stats"]["probed"] == 0 and {c[0] for c in kite.calls} == {199}   # only the failed one again
    res = mp.probe(world, kite=FakeKite(), speed=FAST, force=True, log=lambda *a, **k: None)
    assert res["stats"]["probed"] == 13 and res["stats"]["skipped"] == 0
    path = os.path.join(mp.plan_dir(world), "kite_tokens.csv")
    t = pd.read_csv(path, dtype=str, keep_default_na=False)
    t.loc[t["symbol"] == "RELI", "instrument_token"] = "5101"
    t.to_csv(path, index=False)
    kite = FakeKite(tokens={5101: "RELI"})
    res = mp.probe(world, kite=kite, speed=FAST, log=lambda *a, **k: None)
    assert res["stats"]["probed"] == 1 and {c[0] for c in kite.calls} == {5101, 199}


def test_probe_refuses_while_kite_fno_runs(world, monkeypatch, tmp_path):
    fno = tmp_path / "fno"
    monkeypatch.setattr(config, "load_env", lambda: {"KITE_DATA_DIR": str(fno)})
    mp.tokens(world, kite=FakeKite(), now=NOW, log=lambda *a, **k: None)
    lock = RunLock(str(fno / "NIFTY_FnO_1min" / ".lock")).acquire("test")
    try:
        with pytest.raises(mp.FnoBusy):
            mp.probe(world, kite=FakeKite(), speed=FAST, log=lambda *a, **k: None)
    finally:
        lock.release()
    monkeypatch.setattr(config, "load_env", lambda: {"FNO_RAW_KITE": str(fno)})
    lock = RunLock(str(fno / "with_zero_volume" / "SENSEX_FnO_1min" / ".lock")).acquire("test")
    try:
        with pytest.raises(mp.FnoBusy):
            mp.probe(world, kite=FakeKite(), speed=FAST, log=lambda *a, **k: None)
    finally:
        lock.release()
    mp.probe(world, kite=FakeKite(), speed=FAST, log=lambda *a, **k: None)   # a free lock file is fine


def test_own_lock(world):
    mp.tokens(world, kite=FakeKite(), now=NOW, log=lambda *a, **k: None)
    lock = RunLock(mp.lock_path(world)).acquire("test")
    try:
        with pytest.raises(mp.LockBusy):
            mp.tokens(world, kite=FakeKite(), now=NOW, log=lambda *a, **k: None)
        with pytest.raises(mp.LockBusy):
            mp.probe(world, kite=FakeKite(), speed=FAST, log=lambda *a, **k: None)
        with pytest.raises(mp.LockBusy):
            mp.gaps(world, log=lambda *a, **k: None)
    finally:
        lock.release()


def test_probe_needs_tokens(world):
    with pytest.raises(mp.NotBuilt, match="tokens"):
        mp.probe(world, kite=FakeKite(), speed=FAST, log=lambda *a, **k: None)


# ---- kite_vs_bhav, adjustment periods, mismatches ----

def test_with_factor_equals_factor_on(done):
    f, _, _ = done
    kvb = pd.read_parquet(os.path.join(mp.plan_dir(f), "kite_vs_bhav.parquet"))
    sample = kvb.groupby("security_id").head(60)
    for row in sample.itertuples():
        assert row.our_factor == adj.factor_on(row.security_id, row.date, f)


def test_kite_vs_bhav_columns_and_values(done):
    f, _, _ = done
    kvb = pd.read_parquet(os.path.join(mp.plan_dir(f), "kite_vs_bhav.parquet"))
    assert list(kvb.columns) == ["security_id", "date"] + [f"kite_{x}" for x in mp.FIELDS] + \
        [f"bhav_{x}" for x in mp.FIELDS] + ["our_factor", "ratio", "difference", "adj_difference"]
    sp = kvb[(kvb["security_id"] == sid_of(f, "SPLITCO")) & (kvb["date"] == DAYS[40])].iloc[0]
    assert sp.our_factor == 5.0 and sp.ratio == pytest.approx(5.0, rel=1e-3)
    assert sp.difference == pytest.approx(sp.bhav_close - sp.kite_close)
    ad = kvb[(kvb["security_id"] == sid_of(f, "ADDCO")) & (kvb["date"] == DAYS[5])].iloc[0]
    assert ad.adj_difference == pytest.approx(4.6)
    assert sid_of(f, "PRICEX") not in set(kvb["security_id"])   # a price mismatch is not compared


def test_adjustment_periods_and_kinds(done):
    f, _, _ = done
    a = read(f, "kite_adjustment.csv")
    assert list(a.columns) == mp.ADJUSTMENT_COLUMNS
    sp = a[a["security_id"] == sid_of(f, "SPLITCO")]
    assert len(sp) == 1 and sp.iloc[0]["kind"] == "MULTIPLICATIVE"
    assert (sp.iloc[0]["from_date"], sp.iloc[0]["to_date"]) == (str(DAYS[0]), str(DAYS[29]))
    assert float(sp.iloc[0]["kite_vs_ours"]) == pytest.approx(1.02, abs=1e-3) and sp.iloc[0]["our_factor"] == "5"
    ad = a[a["security_id"] == sid_of(f, "ADDCO")].reset_index(drop=True)
    assert list(ad["kind"]) == ["ADDITIVE", "ADDITIVE"]                     # split at the step
    assert [(x.from_date, x.to_date) for x in ad.itertuples()] == [(str(DAYS[0]), str(DAYS[19])),
                                                                    (str(DAYS[20]), str(DAYS[39]))]
    assert [float(x) for x in ad["median_adj_difference"]] == pytest.approx([4.6, 2.0])
    mx = a[a["security_id"] == sid_of(f, "MIXCO")]
    assert len(mx) == 1 and mx.iloc[0]["kind"] == "MIXED"
    assert sid_of(f, "RELI") not in set(a["security_id"])


def test_eod_mismatch(done):
    f, _, _ = done
    m = read(f, "eod_mismatch.csv")
    assert list(m.columns) == mp.MISMATCH_COLUMNS
    mix = m[m["security_id"] == sid_of(f, "MIXCO")]
    assert len(mix) == 30 and set(mix["field"]) == {"open", "high", "low"} and set(mix["kind"]) == {"MIXED"}
    vol = m[m["field"] == "volume"]
    assert len(vol) == 1 and vol.iloc[0]["security_id"] == sid_of(f, "EMPTYCO") and vol.iloc[0]["date"] == str(DAYS[30])
    assert set(m["security_id"]) == {sid_of(f, "MIXCO"), sid_of(f, "EMPTYCO")}   # the other periods convert cleanly
    plan = read(f, "plan.csv").set_index("security_id")
    assert plan.loc[sid_of(f, "MIXCO"), "eod_mismatch_days"] == "10"
    assert plan.loc[sid_of(f, "RELI"), "eod_mismatch_days"] == "0"


def test_period_kind_unit():
    days = 10
    base = pd.DataFrame({"kite_close": [100.0 + i for i in range(days)], "our_factor": 1.0})
    for x in ("open", "high", "low"):
        base[f"bhav_{x}"] = base["kite_close"] * {"open": 0.99, "high": 1.1, "low": 0.9}[x] * 1.02
        base[f"kite_{x}"] = base["kite_close"] * {"open": 0.99, "high": 1.1, "low": 0.9}[x]
    base["ratio"] = 1.02
    base["adj_difference"] = base["kite_close"] * 0.02
    mult, add = mp.agreement(base)
    assert mp.period_kind(base, mult, add)[0] == "MULTIPLICATIVE"


# ---- sessions, plan and gaps ----

def test_market_wide_session(done):
    f, result, _ = done
    assert list(result["info"]["missing_sessions"]) == [str(DAYS[SESSION])]
    g = read(f, "kite_gaps.csv")
    s = g[g["reason"] == "KITE_MISSING_SESSION"]
    assert len(s) == 1 and s.iloc[0]["need_from"] == str(DAYS[SESSION]) and s.iloc[0]["security_id"] == ""
    assert s.iloc[0]["data_missing"] == "BOTH" and s.iloc[0]["priority"] == "1"
    plan = read(f, "plan.csv").set_index("security_id")
    assert plan.loc[sid_of(f, "RELI"), "eod_missing_days"] == "0"        # the session is not the stock's gap
    assert plan.loc[sid_of(f, "EMPTYCO"), "eod_missing_days"] == "3"


def test_kite_minute_start_row(done):
    f, _, _ = done
    g = read(f, "kite_gaps.csv")
    k = g[g["reason"] == "KITE_MINUTE_START"]
    assert len(k) == 1
    assert (k.iloc[0]["data_missing"], k.iloc[0]["need_from"], k.iloc[0]["need_to"]) == \
        ("1MIN", str(DAYS[0]), str(DAYS[FEB2 - 1]))
    assert k.iloc[0]["priority"] == "1" and k.iloc[0]["trading_days"] == str(FEB2)
    assert gap_rows(f, "RELI").empty                                       # no per-stock January row


def test_gap_reasons_and_kinds(done):
    f, _, _ = done
    def one(sym):
        rows = gap_rows(f, sym)
        return [(x.data_missing, x.reason, x.need_from, x.need_to) for x in rows.itertuples()]
    assert one("GONE") == [("BOTH", "MERGED", str(DAYS[0]), str(DAYS[10]))]
    assert one("DORMCO") == [("BOTH", "DORMANT", str(DAYS[0]), str(DAYS[50]))]
    assert one("NOTOK")[0][:2] == ("BOTH", "NO_KITE_TOKEN")
    assert one("TWIN")[0][:2] == ("BOTH", "TOKEN_REVIEW") and one("OTHERSER")[0][:2] == ("BOTH", "TOKEN_REVIEW")
    assert one("PRICEX")[0][:2] == ("BOTH", "PRICE_MISMATCH")
    assert one("LATECO") == [("BOTH", "EOD_STARTS_LATER", str(DAYS[40]), str(DAYS[45])),
                             ("1MIN", "MINUTE_STARTS_LATER", str(DAYS[46]), str(DAYS[59]))]
    assert one("EMPTYCO") == [("BOTH", "EOD_EMPTY_DAYS", str(DAYS[20]), str(DAYS[22]))]
    assert gap_rows(f, "EMPTYCO").iloc[0]["trading_days"] == "3"
    assert one("NOMIN") == [("1MIN", "MINUTE_STARTS_LATER", str(DAYS[FEB2]), str(DAYS[-1]))]
    assert one("FAILCO") == [] and one("RELI") == [] and one("SPLITCO") == []
    assert set(read(f, "kite_gaps.csv")["data_missing"]) == {"1MIN", "BOTH"}


def test_gap_details(done):
    f, _, _ = done
    gone = gap_rows(f, "GONE").iloc[0]
    assert gone["exit_note"].startswith("into RELI, 42 for 25")
    assert gone["company_name"] == "Gone Limited" and gone["priority"] == "1"
    assert gone["indices"] == f"NIFTY 50 {DAYS[0]}..{DAYS[10]}; NIFTY 500 {DAYS[0]}..{DAYS[10]} (before reliable_from)"
    assert float(gone["median_turnover"]) == 50.0 and gone["trading_days"] == "11"
    assert gone["symbols"] == f"GONE {DAYS[0]}..{DAYS[10]}" and gone["isins"] == f"INE088A01011 {DAYS[0]}..{DAYS[10]}"
    late = gap_rows(f, "LATECO")
    assert set(late["priority"]) == {"2"} and late.iloc[0]["company_name"] == "LATECO LIMITED"
    assert late.iloc[0]["indices"] == f"NIFTY 500 {DAYS[40]}..{DAYS[45]}"
    assert gap_rows(f, "NOTOK").iloc[0]["priority"] == "3"


def test_spans_over_isin_and_symbol_changes():
    rows = pd.DataFrame({"symbol": ["OLD", "OLD", "NEW"], "isin": ["I1", "I2", "I2"],
                         "from_date": [DAYS[0], DAYS[10], DAYS[20]], "to_date": [DAYS[9], DAYS[19], DAYS[30]]})
    assert mp.spans(rows, "symbol") == f"OLD {DAYS[0]}..{DAYS[19]}; NEW {DAYS[20]}..{DAYS[30]}"
    assert mp.spans(rows, "isin") == f"I1 {DAYS[0]}..{DAYS[9]}; I2 {DAYS[10]}..{DAYS[30]}"
    assert mp.spans(rows, "isin", DAYS[5], DAYS[12]) == f"I1 {DAYS[5]}..{DAYS[9]}; I2 {DAYS[10]}..{DAYS[12]}"
    assert mp.spans(rows, "symbol", DAYS[25], DAYS[40]) == f"NEW {DAYS[25]}..{DAYS[30]}"


def test_gap_sort_order(done):
    f, _, _ = done
    g = read(f, "kite_gaps.csv")
    assert list(g.columns) == mp.GAP_COLUMNS
    assert list(g["priority"]) == sorted(g["priority"])
    for _, part in g.groupby("priority"):
        t = pd.to_numeric(part["median_turnover"], errors="coerce")
        vals = list(t.dropna())
        assert vals == sorted(vals, reverse=True)
        assert t.isna().sum() == 0 or t.isna().to_numpy()[-int(t.isna().sum()):].all()   # blanks last
    assert g.iloc[0]["reason"] == "MERGED"            # 50 crore beats the session's sum


def test_plan_csv(done):
    f, _, _ = done
    plan = read(f, "plan.csv").set_index("security_id")
    assert list(read(f, "plan.csv").columns) == mp.PLAN_COLUMNS
    reli = plan.loc[sid_of(f, "RELI")]
    assert (reli["download_eod"], reli["download_minute"], reli["eod_requests"]) == ("Y", "Y", "1")
    assert reli["minute_requests"] == str(len(mp.make_chunks(DAYS[FEB2], DAYS[-1], "minute")))
    assert reli["est_minute_rows"] == str((N - FEB2 - 1) * 375)            # Kite days from 2015-02-02, no session
    nm = plan.loc[sid_of(f, "NEWNM")]
    assert nm["symbols"] == f"OLDNM {DAYS[0]}..{DAYS[29]}; NEWNM {DAYS[30]}..{DAYS[-1]}"
    assert plan.loc[sid_of(f, "NOMIN"), "download_minute"] == "N" and plan.loc[sid_of(f, "NOMIN"), "download_eod"] == "Y"
    gone = plan.loc[sid_of(f, "GONE")]
    assert (gone["status"], gone["exit_reason"], gone["download_eod"]) == ("EXITED", "MERGER", "N")
    assert plan.loc[sid_of(f, "BECO"), "match_status"] == "MATCHED"


def test_index_coverage(done):
    f, result, _ = done
    c = read(f, "index_coverage.csv")
    assert list(c.columns) == mp.COVERAGE_COLUMNS and set(c["index"]) == {"NIFTY 50", "NIFTY 500"}
    n50 = c[c["index"] == "NIFTY 50"].set_index("date")
    d5 = n50.loc[str(DAYS[5])]
    assert (d5["members"], d5["with_eod"], d5["with_minute"]) == ("2", "1", "0")
    assert d5["without_eod"] == "GONE" and d5["without_minute"] == "GONE;RELI"
    d40 = n50.loc[str(DAYS[40])]
    assert (d40["members"], d40["with_eod"], d40["with_minute"], d40["without_eod"]) == ("1", "1", "1", "")
    assert n50.loc[str(DAYS[SESSION]), "with_eod"] == "0"
    n500 = c[c["index"] == "NIFTY 500"].set_index("date")
    assert n500.loc[str(DAYS[42]), "without_eod"] == "LATECO"
    assert n500.loc[str(DAYS[50]), "without_minute"] == "LATECO" and n500.loc[str(DAYS[62]), "with_minute"] == "3"
    assert len(n50) == N


def test_report_and_build_info(done):
    f, result, _ = done
    text = open(os.path.join(mp.plan_dir(f), "build_report.txt"), encoding="utf-8").read()
    info = json.load(open(os.path.join(mp.plan_dir(f), "build_info.json"), encoding="utf-8"))
    assert "Part 0 findings" in text and "NIFTY 50: EOD complete on" in text
    assert f"NIFTY 50: 1-minute complete on" in text and "(GONE, RELI)" in text
    assert f"Market-wide missing sessions: {DAYS[SESSION]}" in text
    assert "Top 20 gaps of priority 1" in text and "can only be found by downloading" in text
    assert info["reason"]["MERGED"] == 1 and info["data_missing"]["1MIN"] == 3
    assert info["match_status"]["PRICE_MISMATCH"] == 1 and info["adjustment_periods"] == \
        {"ADDITIVE": 2, "MIXED": 1, "MULTIPLICATIVE": 1}
    assert info["eod_mismatch_rows"] == 31 and info["eod_requests"] > 0 and info["minute_requests"] > 0
    assert info["instruments_date"] == "2015-04-25" and info["est_disk_gb"]["parquet"] >= 0
    assert info["priority"]["1"] >= 3


def test_gaps_offline_rebuilds_the_same(done, tmp_path, monkeypatch):
    f, result, _ = done
    g = str(tmp_path / "copy")
    shutil.copytree(f, g)
    monkeypatch.setattr(mp, "kite_client", lambda: (_ for _ in ()).throw(AssertionError("no Kite in gaps")))
    res = mp.gaps(g, now=NOW, log=lambda *a, **k: None)
    for name in ("plan.csv", "kite_gaps.csv", "index_coverage.csv", "kite_adjustment.csv", "eod_mismatch.csv"):
        assert read(g, name).equals(read(f, name)), name


def test_gaps_needs_tokens(world):
    with pytest.raises(mp.NotBuilt):
        mp.gaps(world, log=lambda *a, **k: None)


def test_inputs_unchanged(world):
    before = input_hashes(world)
    run_all(world)
    mp.gaps(world, log=lambda *a, **k: None)
    assert input_hashes(world) == before


# ---- refusals ----

def touch(path, offset):
    t = time.time() + offset
    os.utime(path, (t, t))


@pytest.mark.parametrize("newer,older", [
    ("NSE_CM_bhav/2015.parquet", "security_master/build_info.json"),
    ("security_master/build_info.json", "adjustments/build_info.json"),
    ("adjustments/build_info.json", "exits/build_info.json"),
    ("security_master/build_info.json", "index_membership/build_info.json"),
])
def test_stale_inputs_refused(world, newer, older):
    for name in ("security_master/build_info.json", "adjustments/build_info.json", "exits/build_info.json",
                 "index_membership/build_info.json"):
        touch(os.path.join(world, name), 0)
    touch(os.path.join(world, older), 10)
    touch(os.path.join(world, newer), 100)
    if newer.startswith("security_master") and older.startswith("adjustments"):
        touch(os.path.join(world, "exits/build_info.json"), 200)
    with pytest.raises(mp.InputStale):
        mp.Inputs(world)


@pytest.mark.parametrize("missing", ["security_master/securities.parquet", "adjustments/factors.parquet",
                                     "exits/status.parquet", "index_membership/nifty_membership.parquet"])
def test_not_built_refused(world, missing):
    os.remove(os.path.join(world, missing))
    with pytest.raises(mp.NotBuilt):
        mp.Inputs(world)


@pytest.mark.parametrize("sub", ["NSE_CM_bhav", "security_master", "adjustments", "exits", "index_membership"])
def test_input_locks_refused(world, sub):
    lock = RunLock(os.path.join(world, sub, ".lock")).acquire("test")
    try:
        with pytest.raises(mp.StoreBusy):
            mp.tokens(world, kite=FakeKite(), now=NOW, log=lambda *a, **k: None)
    finally:
        lock.release()


def test_check_inputs_never_writes_locks(world):
    before = input_hashes(world)
    mp.check_inputs(world)
    assert input_hashes(world) == before


# ---- CLI ----

def test_cli_flow_and_codes(world, monkeypatch, capsys):
    monkeypatch.setattr(mp, "kite_client", lambda: FakeKite())
    monkeypatch.setattr(mp, "RateLimiter", lambda speed: RealLimiter(FAST))
    assert cli.main(["gaps", "--folder", world]) == 1                    # tokens not run yet
    assert cli.main(["tokens", "--folder", world]) == 0
    assert cli.main(["probe", "--folder", world]) == 1                   # FAILCO failed
    assert cli.main(["gaps", "--folder", world]) == 0
    assert cli.main(["show", "LATECO", "--folder", world]) == 0
    out = capsys.readouterr().out
    assert "EOD_STARTS_LATER" in out and "MATCHED" in out
    assert cli.main(["show", "OLDNM", "--folder", world]) == 0           # an old symbol finds the security
    assert cli.main(["show", "NOSUCH", "--folder", world]) == 1
    monkeypatch.setattr(mp, "kite_client", lambda: FakeKite(fail=()))
    assert cli.main(["probe", "--folder", world]) == 0


def test_cli_sign_in_needed_and_arguments(world, capsys):
    assert cli.main(["tokens", "--folder", world]) == 2                  # no token in .env (stubbed empty)
    assert "sign in" in capsys.readouterr().err.lower()
    with pytest.raises(SystemExit) as e:
        cli.main(["probe", "--speed", "4", "--folder", world])
    assert e.value.code == 2
    with pytest.raises(SystemExit) as e:
        cli.main(["gaps", "--folder", "relative/path"])
    assert e.value.code == 2


def test_cli_busy_and_stale(world, monkeypatch):
    lock = RunLock(mp.lock_path(world)).acquire("test")
    try:
        monkeypatch.setattr(mp, "kite_client", lambda: FakeKite())
        assert cli.main(["tokens", "--folder", world]) == 2
    finally:
        lock.release()
    touch(os.path.join(world, "NSE_CM_bhav", "2015.parquet"), 1000)
    assert cli.main(["tokens", "--folder", world]) == 1

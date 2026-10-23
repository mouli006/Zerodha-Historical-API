"""The fixes after the full 1-minute download (backfill/equity/minute_download.py, minute_store.py, equity_minute.py
and phase 1's gaps): error text kept in the progress db, retoken (SAME_TOKEN / NEW_TOKEN / SERIES_SWITCH /
NO_KITE_DATA / REVIEW), the NO_KITE_DATA state in download / status / update / gaps, and cleaned loading (Kite's
all-zero bars, negative volume, a high / low that does not contain open / close). The phase 2 synthetic world and fake
Kite of tests/test_equity_minute_download.py; no network, .env never read."""
import hashlib
import os
import shutil
import sqlite3
from datetime import datetime, time as dtime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from kiteconnect import exceptions as kex

import config
import equity_minute as cli
from backfill import fetch
from backfill.equity import minute_download as md
from backfill.equity import minute_plan as mp
from backfill.equity import minute_store as ms
from test_equity_minute_download import (DAYS, FAST, MORNING_AFTER, N, STOCKS, FakeKite, FastStop,  # noqa: F401
                                         build_inputs, dl, no_network, planned, q, sid, world)

PAD, ZERO = 33, 34           # BIG: two all-zero bars added on PAD; only all-zero bars on ZERO
OLD = {"GEN1": 110, "GEN2": 111, "GEN3": 112, "GEN4": 113, "NEWNM": 109}
# the first download: every old token above answers "invalid token" (as SHAHINVEST, VTMLTD, EDUCOMP, ...), VOLCO's
# first minute chunk fails (as the 502 group)
FIRST = dict(invalid=set(OLD.values()), fail_minute={(105, DAYS[DAYS.index(mp.KITE_MINUTE_FROM)])}, pad={PAD},
             zero={ZERO})
# today's Kite list: GEN1 a new token, GEN2 only as GEN2-BE, GEN3 gone, GEN4 as GEN4-BE at another price, NEWNM a new
# token with no day candles
RETOKEN = dict(new_tokens={"GEN1": 210, "GEN2": 211, "GEN4": 213, "NEWNM": 209}, be={"GEN2", "GEN4"}, gone={"GEN3"},
               invalid=set(OLD.values()), no_day_tokens={209}, div={213: 1.5})
# after retoken: the tokens it wrote answer; GEN4 and NEWNM keep their (now valid) old tokens; GEN3 still gone
AFTER = dict(new_tokens={"GEN1": 210, "GEN2": 211}, be={"GEN2"}, gone={"GEN3"})


class FixKite(FakeKite):
    """FakeKite plus: stocks gone from the list, listed as SYM-BE, old tokens answering InputException, tokens
    without day candles, per-token price divisors, and BIG's zero bars."""

    def __init__(self, gone=(), be=(), invalid=(), no_day_tokens=(), div=None, pad=(), zero=(), **kw):
        super().__init__(**kw)
        self.gone, self.be, self.invalid, self.no_day_tokens = set(gone), set(be), set(invalid), set(no_day_tokens)
        self.div.update(div or {})
        self.pad, self.zero = set(pad), set(zero)

    def instruments(self, exchange):
        out = []
        for i in super().instruments(exchange):
            if i["tradingsymbol"] in self.gone:
                continue
            if i["tradingsymbol"] in self.be:
                i = {**i, "tradingsymbol": i["tradingsymbol"] + "-BE"}
            out.append(i)
        return out

    def historical_data(self, token, start, end, interval, continuous=False, oi=False):
        if token in self.invalid or (interval == "day" and token in self.no_day_tokens):
            with self.lock:
                self.calls.append((token, start.date(), end.date(), interval))
            if token in self.invalid:
                raise kex.InputException("invalid token")
            return []
        out = super().historical_data(token, start, end, interval, continuous, oi)
        if interval == "minute" and token == STOCKS["BIG"]["token"]:
            zero_days = {DAYS[i] for i in self.zero}
            out = [c for c in out if c["date"].date() not in zero_days]
            for i in sorted(self.pad | self.zero):
                d = DAYS[i]
                if start.date() <= d <= end.date():
                    times = [dtime(9, 20), dtime(9, 21)] if i in self.pad else [dtime(9, 15), dtime(9, 16), dtime(9, 17)]
                    out += [{"date": datetime.combine(d, t).replace(tzinfo=mp.IST), "open": 0.0, "high": 0.0,
                             "low": 0.0, "close": 0.0, "volume": 0} for t in times]
            out.sort(key=lambda c: c["date"])
        return out


def logger(out):
    return lambda *a, **k: out.append(" ".join(map(str, a)))


@pytest.fixture(scope="module")
def failed(planned, tmp_path_factory):
    """planned + a download where 5 stocks' day candles fail (invalid token) and VOLCO's first chunk fails."""
    f = str(tmp_path_factory.mktemp("min_fix") / "data")
    shutil.copytree(planned, f)
    mpatch = pytest.MonkeyPatch()
    mpatch.setattr(config, "load_env", lambda: {})
    mpatch.setattr(fetch, "STOP", FastStop())
    res = md.download(f, kite=FixKite(**FIRST), speed=FAST, workers=2, log=q)
    mpatch.undo()
    return f, res


@pytest.fixture
def fcopy(failed, tmp_path):
    f = str(tmp_path / "failed")
    shutil.copytree(failed[0], f)
    return f


@pytest.fixture(scope="module")
def nk_done(failed, tmp_path_factory):
    """failed + retoken --apply + download with the checked tokens: every stock DONE but GEN3 (NO_KITE_DATA)."""
    f = str(tmp_path_factory.mktemp("min_fix_nk") / "data")
    shutil.copytree(failed[0], f)
    mpatch = pytest.MonkeyPatch()
    mpatch.setattr(config, "load_env", lambda: {})
    mpatch.setattr(fetch, "STOP", FastStop())
    md.retoken(f, kite=FixKite(**RETOKEN), apply=True, speed=FAST, log=q)
    md.download(f, kite=FixKite(**AFTER), speed=FAST, workers=2, log=q)
    mpatch.undo()
    return f


@pytest.fixture
def nkcopy(nk_done, tmp_path):
    f = str(tmp_path / "nk")
    shutil.copytree(nk_done, f)
    return f


def tree_hashes(path):
    out = {}
    for root, _, files in os.walk(path):
        for name in files:
            if name == ".lock" or name.endswith(("-wal", "-shm")):
                continue
            p = os.path.join(root, name)
            with open(p, "rb") as fh:
                out[os.path.relpath(p, path)] = hashlib.sha256(fh.read()).hexdigest()
    return out


def stocks_of(f):
    return md.read_progress(f)[1]


def results(res):
    return {r["symbol"]: r for r in res["rows"]}


# ---- Part 1: the error text ----

def test_errors_kept_and_shown(failed, capsys):
    f, res = failed
    assert res["stats"]["failed_stocks"] == 6
    errs = {(e["security_id"], e["kind"]): e for e in md.read_errors(f)}
    for sym in OLD:
        e = errs[(sid(f, sym), "DAY_CANDLES")]
        assert e["error"] == "InputException: invalid token" and e["run_id"] and e["at"]
    st = md.status(f, log=q)
    rep = st["report"]
    assert f"GEN1 ({sid(f, 'GEN1')}) DAY_CANDLES: InputException: invalid token" in rep
    assert f"VOLCO ({sid(f, 'VOLCO')}) MINUTE_CHUNK {mp.KITE_MINUTE_FROM}: " in rep and "something broke" in rep
    assert "Failed (not built yet" in rep and len(st["failures"]) == 6
    assert cli.main(["status", "--folder", f]) == 0
    assert "InputException: invalid token" in capsys.readouterr().out


def test_status_leaves_an_old_progress_file_alone(world):
    path = os.path.join(mp.out_dir(world), "download_progress.sqlite")
    os.makedirs(mp.out_dir(world), exist_ok=True)
    db = sqlite3.connect(path)
    db.execute("PRAGMA journal_mode=WAL")
    for s in md.SQL:                                            # the tables as the phase 2 code wrote them
        db.execute(s)
    db.execute("INSERT INTO stocks VALUES (?,?,?,?,?,?,?,?,?)",
               (sid(world, "BIG"), "101", "DONE", str(DAYS[0]), str(DAYS[N - 1]), 1, 3, 10, "2015-04-01 10:00:00"))
    db.commit()
    db.close()
    with open(path, "rb") as fh:
        before = hashlib.sha256(fh.read()).hexdigest()
    st = md.status(world, log=q)
    assert st["failures"] == [] and st["no_kite_data"] == []
    with open(path, "rb") as fh:
        assert hashlib.sha256(fh.read()).hexdigest() == before
    db = sqlite3.connect(path)
    assert db.execute("SELECT name FROM sqlite_master WHERE name='errors'").fetchone() is None
    assert "reason" not in [r[1] for r in db.execute("PRAGMA table_info(stocks)")]
    db.close()


# ---- Part 2: retoken ----

def test_retoken_dry_run_results_and_writes_nothing(fcopy, capsys):
    f = fcopy
    before = tree_hashes(mp.out_dir(f))
    kite = FixKite(**RETOKEN)
    logs = []
    res = md.retoken(f, kite=kite, speed=FAST, log=logger(logs))
    r = results(res)
    assert set(r) == {"VOLCO"} | set(OLD)                       # the stocks not DONE only
    assert r["VOLCO"]["result"] == "SAME_TOKEN" and r["VOLCO"]["price_check"] == ""
    assert r["GEN1"]["result"] == "NEW_TOKEN" and r["GEN1"]["new_token"] == "210"
    assert r["GEN1"]["price_check"].startswith("OK 0.00%")
    assert r["GEN2"]["result"] == "SERIES_SWITCH" and r["GEN2"]["kite_tradingsymbol"] == "GEN2-BE"
    assert r["GEN2"]["price_check"].startswith("OK")
    assert r["GEN3"]["result"] == "NO_KITE_DATA" and r["GEN3"]["new_token"] == ""
    assert r["GEN4"]["result"] == "REVIEW" and r["GEN4"]["price_check"].startswith("OFF 33.")
    assert "SERIES_SWITCH to 213: price check failed" in r["GEN4"]["note"]
    assert r["NEWNM"]["result"] == "REVIEW" and r["NEWNM"]["price_check"] == f"no Kite day candle on {DAYS[N - 1]}"
    assert res["review"] == 2 and not res["applied"]
    day_calls = [c for c in kite.calls if c[0] != "instruments"]
    assert kite.calls[0] == ("instruments", "NSE") and sorted(c[0] for c in day_calls) == [209, 210, 211, 213]
    assert all(c[3] == "day" for c in day_calls)
    assert tree_hashes(mp.out_dir(f)) == before                  # nothing written
    assert any("dry run, nothing written" in x for x in logs)


def test_retoken_cli_dry_run_exit_code(fcopy, monkeypatch, capsys):
    monkeypatch.setattr(mp, "kite_client", lambda: FixKite(**RETOKEN))
    assert cli.main(["retoken", "--folder", fcopy]) == 1          # REVIEW rows
    out = capsys.readouterr().out
    assert "GEN2-BE" in out and "REVIEW" in out and "NO_KITE_DATA" in out
    monkeypatch.setattr(mp, "kite_client", lambda: FixKite(new_tokens={"GEN1": 210, "GEN2": 211}, be={"GEN2"}))
    assert cli.main(["retoken", "--folder", fcopy]) == 0


def test_retoken_apply_then_download_uses_new_tokens(fcopy):
    f = fcopy
    up = os.path.join(mp.out_dir(f), "update")
    os.makedirs(up)
    keep = pd.DataFrame([{c: "" for c in mp.TOKEN_COLUMNS}])        # a row of an earlier update is kept
    keep.loc[0, ["security_id", "symbol", "instrument_token", "match_status"]] = ["S999999", "OTHER", "999", "MATCHED"]
    keep.to_csv(os.path.join(up, "tokens.csv"), index=False)
    data_before = tree_hashes(os.path.join(mp.out_dir(f), "data"))
    md.retoken(f, kite=FixKite(**RETOKEN), apply=True, speed=FAST, now=datetime(2015, 4, 26, 9, tzinfo=mp.IST),
               log=q)
    toks = pd.read_csv(os.path.join(up, "tokens.csv"), dtype=str, keep_default_na=False).set_index("symbol")
    assert toks.loc["GEN1", "instrument_token"] == "210" and toks.loc["GEN1", "match_status"] == "MATCHED"
    assert toks.loc["GEN2", "kite_tradingsymbol"] == "GEN2-BE" and toks.loc["GEN2", "instrument_token"] == "211"
    assert "retoken 2015-04-26: OK" in toks.loc["GEN2", "note"]
    assert toks.loc["OTHER", "instrument_token"] == "999"
    assert not {"GEN3", "GEN4", "NEWNM", "VOLCO"} & set(toks.index)           # REVIEW / no change: not written
    assert os.path.isfile(os.path.join(up, "kite_instruments_NSE_2015-04-26.csv"))
    assert os.path.isfile(os.path.join(up, "retoken_2015-04-26.csv"))
    plan = md.read_plan(f)
    assert plan[sid(f, "GEN1")]["token"] == 210 and plan[sid(f, "GEN4")]["token"] == 113
    kite = FixKite(**RETOKEN)
    res = dl(f, kite, workers=2)
    tokens = {c[0] for c in kite.calls if c[0] != "instruments"}
    assert {210, 211, 105} <= tokens and not tokens & {110, 111, 112}       # new tokens; never GEN3 or old ones
    st = stocks_of(f)
    assert st[sid(f, "GEN1")]["status"] == "DONE" and st[sid(f, "GEN1")]["token"] == "210"
    assert st[sid(f, "GEN2")]["status"] == "DONE" and st[sid(f, "VOLCO")]["status"] == "DONE"
    assert st[sid(f, "GEN3")]["status"] == "NO_KITE_DATA"
    assert res["stats"]["failed_stocks"] == 2                                # GEN4, NEWNM: still invalid tokens
    after = tree_hashes(os.path.join(mp.out_dir(f), "data"))
    assert all(after[k] == v for k, v in data_before.items())             # stored stocks unchanged, only new ones
    assert not os.path.isdir(os.path.join(mp.out_dir(f), "archive"))


# ---- Part 3: NO_KITE_DATA ----

def test_no_kite_data_state_file_and_status(nk_done, capsys):
    f = nk_done
    g = stocks_of(f)[sid(f, "GEN3")]
    assert g["status"] == "NO_KITE_DATA" and g["reason"].startswith("not in Kite's NSE list of ") and g["reason_at"]
    assert sum(s["status"] == "DONE" for s in stocks_of(f).values()) == 12
    nk = pd.read_csv(os.path.join(md.check_dir(f), "no_kite_data.csv"), dtype=str, keep_default_na=False)
    assert list(nk.columns) == md.NO_KITE_COLUMNS and list(nk["symbol"]) == ["GEN3"]
    assert nk.loc[0, "isin"] == STOCKS["GEN3"]["isin"] and nk.loc[0, "last_bhav_day"] == str(DAYS[N - 1])
    st = md.status(f, log=q)
    assert st["no_kite_data"] == ["GEN3"] and "No Kite data (NO_KITE_DATA): 1 stocks (GEN3)" in st["report"]
    assert "1 no Kite data)" in st["report"] and st["failures"] == []
    assert st["estimate"]["total"]["NO_KITE_DATA"] == 1 and st["estimate"]["total"]["minute_requests"] == 0


def test_download_skips_no_kite_data(nkcopy):
    kite = FixKite(**AFTER)
    res = dl(nkcopy, kite, symbols=["GEN3"], dry_run=True)
    assert res["estimate"]["total"]["NO_KITE_DATA"] == 1 and "Skipped, Kite has no data" in res["report"]
    res = dl(nkcopy, kite, symbols=["GEN3"])
    assert res["items"] == [] and [c for c in kite.calls if c[0] != "instruments"] == []


def test_update_refuses_only_for_other_unfinished_stocks(fcopy):
    md.retoken(fcopy, kite=FixKite(**RETOKEN), apply=True, speed=FAST, log=q)
    md.download(fcopy, kite=FixKite(**RETOKEN), speed=FAST, workers=2, log=q)     # GEN4, NEWNM still fail
    with pytest.raises(md.HistoryNotDone) as e:
        md.update(fcopy, kite=FixKite(**AFTER), speed=FAST, now=datetime.combine(DAYS[N - 1], dtime(20)), log=q)
    assert "2 stocks" in str(e.value) and "GEN3" not in str(e.value)


def test_update_runs_with_a_no_kite_data_stock(nkcopy):
    f = nkcopy
    build_inputs(f, N)
    kite = FixKite(last_day=N, **AFTER)
    logs = []
    res = md.update(f, kite=kite, speed=FAST, now=MORNING_AFTER,
                    log=logger(logs))
    assert res["stats"]["updated"] == 12 and res["stats"]["failed_stocks"] == 0
    assert 112 not in {c[0] for c in kite.calls}
    assert stocks_of(f)[sid(f, "GEN3")]["status"] == "NO_KITE_DATA"
    assert not any("Back in Kite's list" in x or "REVIEW" in x for x in logs)


def test_update_brings_back_a_stock_listed_again(nkcopy):
    f = nkcopy
    build_inputs(f, N)
    kite = FixKite(last_day=N, new_tokens={"GEN1": 210, "GEN2": 211}, be={"GEN2"})       # GEN3 listed again
    logs = []
    res = md.update(f, kite=kite, speed=FAST, now=MORNING_AFTER,
                    log=logger(logs))
    assert any(x.startswith("Back in Kite's list: GEN3") and "price OK" in x for x in logs)
    g = stocks_of(f)[sid(f, "GEN3")]
    assert g["status"] == "DONE" and g["token"] == "112" and g["last_day"] == str(DAYS[N])
    assert res["stats"]["built"] == 2                                       # GEN3 + NEWIPO
    assert [c for c in kite.calls if c[0] == 112 and c[3] == "minute"]
    nk = pd.read_csv(os.path.join(md.check_dir(f), "no_kite_data.csv"), dtype=str)
    assert nk.empty and md.status(f, log=q)["no_kite_data"] == []


@pytest.mark.parametrize("extra, why", [(dict(div={312: 1.5}), "OFF 33."),
                                         (dict(no_day_tokens={312}), "no Kite day candle on")])
def test_update_keeps_a_returning_stock_when_the_price_check_fails(nkcopy, extra, why):
    f = nkcopy
    build_inputs(f, N)
    kite = FixKite(last_day=N, new_tokens={"GEN1": 210, "GEN2": 211, "GEN3": 312}, be={"GEN2"}, **extra)
    logs = []
    res = md.update(f, kite=kite, speed=FAST, now=MORNING_AFTER,
                    log=logger(logs))
    review = [x for x in logs if x.startswith("REVIEW GEN3")]
    assert len(review) == 1 and why in review[0] and "stays NO_KITE_DATA" in review[0]
    assert stocks_of(f)[sid(f, "GEN3")]["status"] == "NO_KITE_DATA"
    assert [c[3] for c in kite.calls if c[0] == 312] == ["day"]            # the price check only
    assert res["stats"]["failed_stocks"] == 0
    nk = pd.read_csv(os.path.join(md.check_dir(f), "no_kite_data.csv"), dtype=str)
    assert list(nk["symbol"]) == ["GEN3"]


def test_retoken_apply_clears_a_stock_listed_again(nkcopy):
    f = nkcopy
    res = md.retoken(f, kite=FixKite(new_tokens={"GEN1": 210, "GEN2": 211}, be={"GEN2"}), apply=True, speed=FAST,
                     log=q)
    assert results(res)["GEN3"]["result"] == "SAME_TOKEN" and set(results(res)) == {"GEN3"}
    assert sid(f, "GEN3") not in stocks_of(f)
    assert pd.read_csv(os.path.join(md.check_dir(f), "no_kite_data.csv")).empty
    kite = FixKite(new_tokens={"GEN1": 210, "GEN2": 211}, be={"GEN2"})
    dl(f, kite)
    assert stocks_of(f)[sid(f, "GEN3")]["status"] == "DONE"


def test_gaps_adds_no_kite_data_rows(nkcopy):
    f = nkcopy
    mp.gaps(f, log=q)
    gaps = pd.read_csv(os.path.join(mp.plan_dir(f), "kite_gaps.csv"), dtype=str, keep_default_na=False)
    rows = gaps[(gaps["reason"] == "NO_KITE_DATA") & (gaps["security_id"] == sid(f, "GEN3"))]
    assert len(rows) == 1 and rows.iloc[0]["data_missing"] == "BOTH"
    assert rows.iloc[0]["need_from"] == str(DAYS[0]) and rows.iloc[0]["need_to"] == str(DAYS[N - 1])
    assert rows.iloc[0]["note"].startswith("not in Kite's NSE list of ")
    os.remove(os.path.join(md.check_dir(f), "no_kite_data.csv"))
    mp.gaps(f, log=q)
    gaps = pd.read_csv(os.path.join(mp.plan_dir(f), "kite_gaps.csv"), dtype=str, keep_default_na=False)
    assert gaps[gaps["reason"] == "NO_KITE_DATA"].empty


# ---- Part 4: clean loading and zero bars in the day checks ----

def test_zero_bars_ignored_by_the_day_checks(failed):
    f = failed[0]
    days = ms.read_days(ms.stock_dir(f, sid(f, "BIG"))).set_index("date")
    assert days.loc[DAYS[PAD], "minute_rows"] == 3 and days.loc[DAYS[PAD], "check"] == "OK"
    assert days.loc[DAYS[PAD], "first_bar"] == "09:15" and days.loc[DAYS[PAD], "minute_low"] > 0
    assert days.loc[DAYS[ZERO], "minute_rows"] == 0 and days.loc[DAYS[ZERO], "check"] == "MINUTE_EMPTY"
    stored = ms.read_minutes(ms.stock_dir(f, sid(f, "BIG")))
    by_day = stored.groupby(stored["ts"].dt.date).size()
    assert by_day[DAYS[PAD]] == 5 and by_day[DAYS[ZERO]] == 3            # the files keep every bar
    assert stocks_of(f)[sid(f, "BIG")]["minute_rows"] == len(stored)


def test_load_minute_clean_with_zero_bars(failed):
    f = failed[0]
    raw = ms.load_minute("BIG", DAYS[PAD], DAYS[ZERO], prices="kite", folder=f, clean=False)
    assert len(raw) == 8 and (raw[["open", "high", "low", "close"]] == 0).all(axis=1).sum() == 5
    assert "clean" not in raw.attrs
    c = ms.load_minute("BIG", DAYS[PAD], DAYS[ZERO], prices="kite", folder=f)
    assert len(c) == 3 and (c["close"] > 0).all()
    assert c.attrs["clean"] == {"bars": 8, "dropped_nonpositive": 5, "volume_set_zero": 0, "highlow_widened": 0}


def write_stock(f, sid_, bars):
    path = ms.stock_dir(f, sid_)
    os.makedirs(path)
    ms.write_minutes(ms.minute_frame(bars), os.path.join(path, "2015.parquet"))
    d = DAYS[30]
    days = pd.DataFrame([{"date": d, "symbol": "SYN", "kind": "MULTIPLICATIVE", "ratio": 2.0, "kite_multiplier": 1.0,
                          "kite_difference": 0.0, "vol_ratio": 1.0, "minute_scale": 1.0, "minute_volume_scale": 1.0}])
    pq.write_table(pa.Table.from_pandas(days, preserve_index=False), os.path.join(path, ms.DAYS_FILE))


def bar(t, o, h, low, c, v):
    return {"date": datetime.combine(DAYS[30], t).replace(tzinfo=mp.IST), "open": o, "high": h, "low": low,
            "close": c, "volume": v}


def test_load_minute_clean_negative_volume_and_bad_high_low(tmp_path):
    f = str(tmp_path)
    bars = [bar(dtime(9, 15), 0.0, 0.0, 0.0, 0.0, 0),              # Kite's padding
            bar(dtime(9, 16), 10.0, 10.5, 9.5, 10.2, 100),
            bar(dtime(9, 17), 10.2, 10.1, 10.0, 10.3, 50),           # high below open and close
            bar(dtime(9, 18), 10.3, 10.6, 10.4, 10.5, -7),           # low above open; negative volume
            bar(dtime(9, 19), 10.5, 10.6, -1.0, 10.4, 10)]           # a negative price
    write_stock(f, "S000001", bars)
    exact = ms.load_minute("S000001", prices="kite", folder=f, clean=False)
    assert len(exact) == 5 and list(exact["volume"]) == [0, 100, 50, -7, 10]
    assert exact.loc[2, "high"] == 10.1 and exact.loc[3, "low"] == 10.4
    k = ms.load_minute("S000001", prices="kite", folder=f)
    assert list(k["ts"].dt.strftime("%H:%M")) == ["09:16", "09:17", "09:18"]
    assert list(k["volume"]) == [100, 50, 0]
    assert k.loc[1, "high"] == 10.3 and k.loc[1, "low"] == 10.0          # widened to the close
    assert k.loc[2, "low"] == 10.3 and k.loc[2, "high"] == 10.6          # widened to the open
    assert k.attrs["clean"] == {"bars": 5, "dropped_nonpositive": 2, "volume_set_zero": 1, "highlow_widened": 2}
    r = ms.load_minute("S000001", folder=f)                             # raw: the conversion after cleaning
    assert list(r["high"]) == pytest.approx([21.0, 20.6, 21.2]) and r.attrs["clean"]["bars"] == 5


def test_minute_aggregates_skip_zero_bars():
    m = ms.minute_frame([bar(dtime(9, 15), 0.0, 0.0, 0.0, 0.0, 0), bar(dtime(9, 16), 10.0, 10.5, 9.5, 10.2, 100),
                         bar(dtime(9, 17), 10.2, 10.1, 10.0, 10.3, -5)])
    a = md.minute_aggregates(m.assign(run_id="r", fetched_at="t"))[DAYS[30]]
    assert a["minute_rows"] == 2 and a["first_bar"] == "09:16" and a["minute_low"] == 9.5
    assert a["minute_high"] == 10.5 and a["minute_volume"] == 100
    only_zero = ms.minute_frame([bar(dtime(9, 15), 0.0, 0.0, 0.0, 0.0, 0)])
    assert md.minute_aggregates(only_zero.assign(run_id="r", fetched_at="t")) == {}

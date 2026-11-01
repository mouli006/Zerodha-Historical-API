"""Kite token changes checked before and after the rebuild (backfill/equity/minute_tokencheck.py, the gate in
minute_download.Runner, equity_minute.py token-review / token-replay; docs/dev/equity_minute_tokencheck.md), on
test_equity_minute_download.py's synthetic world and fake Kite. No network; .env is never read (config.load_env
stubbed by the imported no_network fixture)."""
import hashlib
import os
import shutil
from datetime import date, datetime, time as dtime, timedelta

import pandas as pd
import pytest

import equity_minute as cli
from backfill.equity import minute_download as md
from backfill.equity import minute_plan as mp
from backfill.equity import minute_store as ms
from backfill.equity import minute_tokencheck as tc
from backfill.equity import security_master as sm
from backfill.fetch import ChunkFailed, StopJob
from backfill.locks import RunLock
from test_equity_minute_download import (DAYS, FAST, MORNING_AFTER, N, STOCKS, FakeKite, build_inputs,  # noqa: F401
                                         days_of, done_copy, downloaded, extend, kite_day, minute_bars, no_network,
                                         planned, q, sid, world)

NEW = 211          # GEN1's new token in these tests (GEN1 is stored with token 110)
WORSE = {(NEW, 30), (NEW, 31)}      # two days whose new minute bars move 1% inside the day (Kite minutes from day 22)


class TokKite(FakeKite):
    """FakeKite whose tokens can answer with another company's candles (alias: token -> symbol), with no day
    candles (empty), fail (failing), or with the minute bars of some (token, day index) changed inside the day: the
    first two bars 1% lower and the last bar unchanged (worse_days) or moved to the day close (exempt_days: the new
    day matches the bhavcopy close better); `drop`: symbols left out of the instrument list."""

    def __init__(self, alias=None, empty=(), failing=(), worse_days=(), exempt_days=(), drop=(), **kw):
        super().__init__(**kw)
        self.alias = alias or {}
        self.by_token.update(self.alias)
        self.empty, self.failing = set(empty), set(failing)
        self.worse_days, self.exempt_days, self.drop = set(worse_days), set(exempt_days), set(drop)

    def instruments(self, exchange):
        return [i for i in super().instruments(exchange) if i["tradingsymbol"] not in self.drop]

    def historical_data(self, token, start, end, interval, continuous=False, oi=False):
        if interval == "day" and token in self.empty:
            with self.lock:
                self.calls.append((token, start.date(), end.date(), interval))
            return []
        if token in self.failing:
            with self.lock:
                self.calls.append((token, start.date(), end.date(), interval))
            from kiteconnect import exceptions as kex
            raise kex.InputException("invalid token")
        out = super().historical_data(token, start, end, interval, continuous, oi)
        if interval != "minute" or token not in {t for t, _ in self.worse_days | self.exempt_days}:
            return out
        res = []
        for c in out:
            d = c["date"].date()
            i = DAYS.index(d) if d in DAYS else None
            if (token, i) in self.worse_days | self.exempt_days:
                if c["date"].time() != dtime(15, 29):
                    c = {**c, **{f: round(c[f] * 0.99, 2) for f in ("open", "high", "low", "close")}}
                elif (token, i) in self.exempt_days:
                    close = kite_day(self.by_token[token], i)["close"]
                    c = {**c, "open": close, "high": close, "low": close, "close": close}
            res.append(c)
        return res


def upd(f, kite, now=MORNING_AFTER, logs=None):
    out = logs if logs is not None else []
    res = md.update(f, kite=kite, speed=FAST, workers=1, now=now, log=lambda *a, **k: out.append(" ".join(map(str, a))))
    return res["stats"], out


def tree(path):
    out = {}
    for root, _, files in os.walk(path):
        for name in files:
            p = os.path.join(root, name)
            with open(p, "rb") as fh:
                out[os.path.relpath(p, path)] = hashlib.sha256(fh.read()).hexdigest()
    return out


def stock_files(f, sym):
    return tree(ms.stock_dir(f, sid(f, sym)))


def progress(f, sym):
    return md.read_progress(f)[1][sid(f, sym)]


def archives(f):
    p = os.path.join(mp.out_dir(f), "archive")
    return sorted(os.listdir(p)) if os.path.isdir(p) else []


def tc_dir(f):
    return tc.out_dir(f)


# ---- unit: the verdict ----

def refs_of(rows):
    return pd.DataFrame(rows, columns=["date", "open", "high", "low", "close"])


def kite_of(rows):
    return pd.DataFrame(rows, columns=["date", "open", "high", "low", "close"])


D1, D2, D3 = date(2026, 10, 27), date(2026, 10, 28), date(2026, 10, 29)


def test_rs2_stock_one_tick_passes_three_ticks_fail():
    refs = refs_of([(D1, 2.0, 2.1, 1.95, 2.0), (D2, 2.0, 2.1, 1.95, 2.0)])
    one = tc.judge(refs, kite_of([(D1, 2.0, 2.1, 1.95, 2.05), (D2, 2.0, 2.15, 1.9, 2.0)]))
    assert one["result"] == tc.PASS and one["days"] == 2 and abs(one["max_close_diff"] - 0.025) < 1e-9
    assert all(r["tol"] == pytest.approx(0.05) for r in one["rows"])          # 2 ticks of Rs 0.05 on Rs 2: 5%
    three = tc.judge(refs, kite_of([(D1, 2.0, 2.1, 1.95, 2.15), (D2, 2.0, 2.1, 1.95, 2.0)]))
    assert three["result"] == tc.FAIL and [r["ok"] for r in three["rows"]] == [False, True]
    big = refs_of([(D1, 1000, 1010, 990, 1000)])
    assert tc.judge(big, kite_of([(D1, 1000, 1010, 990, 1019)]))["result"] == tc.PASS       # 1.9% < 2%
    assert tc.judge(big, kite_of([(D1, 1000, 1010, 990, 1021)]))["result"] == tc.FAIL       # 2.1%
    assert tc.judge(big, kite_of([(D1, 1000, 1040, 990, 1000)]))["result"] == tc.FAIL       # high off 3%


def test_split_between_reference_day_and_today():
    """A 1:5 split after D1: the bhavcopy of D1 is 500, of D2 / D3 100. Kite back-adjusted (D1 at 100) passes on the
    adjusted basis, Kite not adjusted passes raw, another company fails both."""
    refs = refs_of([(D1, 500, 505, 495, 500), (D2, 100, 101, 99, 100), (D3, 100, 101, 99, 100)])
    factor = lambda d: 5.0 if d < D2 else 1.0                   # noqa: E731
    adjusted = kite_of([(D1, 100, 101, 99, 100), (D2, 100, 101, 99, 100), (D3, 100, 101, 99, 100)])
    v = tc.judge(refs, adjusted, factor, D3)
    assert v["result"] == tc.PASS and v["basis"] == "adjusted" and v["rows"][0]["k"] == 5.0
    raw = kite_of([(D1, 500, 505, 495, 500), (D2, 100, 101, 99, 100), (D3, 100, 101, 99, 100)])
    v = tc.judge(refs, raw, factor, D3)
    assert v["result"] == tc.PASS and v["basis"] == "raw"
    other = kite_of([(D1, 300, 303, 297, 300), (D2, 60, 61, 59, 60), (D3, 60, 61, 59, 60)])
    assert tc.judge(refs, other, factor, D3)["result"] == tc.FAIL
    assert tc.judge(refs, adjusted)["result"] == tc.FAIL          # without the factor the split day is off


def test_reference_days_window_and_count():
    days = [date(2026, 1, 1) + timedelta(days=i) for i in range(100)]
    bhav = pd.DataFrame({"date": days[:90], "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0})
    r = tc.reference_days(bhav, days, days[95])
    assert list(r["date"]) == days[85:90]
    assert tc.reference_days(bhav, days, days[95], n=3)["date"].tolist() == days[87:90]
    assert len(tc.reference_days(bhav[bhav["date"] < days[36]], days, days[95])) == 0     # window starts at day 36
    assert len(tc.reference_days(bhav[bhav["date"] <= days[36]], days, days[95])) == 1
    assert len(tc.reference_days(None, days, days[95])) == 0


def test_check_token_no_candles_and_failed_request():
    refs = refs_of([(D1, 10, 11, 9, 10)])
    v = tc.check_token(lambda t, a, b: pd.DataFrame(columns=["date"] + mp.FIELDS), 5, refs)
    assert v["result"] == tc.NO_CANDLES and v["days"] == 0

    def boom(t, a, b):
        raise ChunkFailed("invalid token")
    v = tc.check_token(boom, 5, refs)
    assert v["result"] == tc.NO_CANDLES and "invalid token" in v["error"]
    assert tc.check_token(boom, 5, refs_of([]))["result"] == tc.NO_REFERENCE


def test_better_rule():
    assert tc.better(100.0, (95, 99, 98), (98, 102, 100))[0]                  # inside the new range only
    assert tc.better(100.0, (95, 105, 103), (95, 105, 100.5))[0]              # both inside, new close nearer
    assert not tc.better(100.0, (95, 105, 100.5), (95, 105, 103))[0]
    assert not tc.better(float("nan"), (95, 105, 100), (95, 105, 100))[0]


def test_note_for():
    probes = {"S1": {"instrument_token": "11", "price_status": "MATCHED", "price_day": "2026-10-01"},
              "S2": {"instrument_token": "21", "price_status": "MATCHED", "price_day": "2026-10-01"}}
    checks = {("S3", "31"): {"result": "PASS", "at": "2026-11-02 21:00:00", "days": "5", "max_close_diff": "0.003"}}
    rt = {("S4", "41"): "OK 0.10%: Kite 10 vs bhavcopy 10.01 on 2026-10-20"}
    n = lambda note, sid, tok, status="MATCHED_UNCHECKED": tc.note_for(note, status, sid, tok, probes, checks, rt)  # noqa
    assert n("price checked by probe", "S1", "11") == "probe: price checked 2026-10-01"
    assert n("price checked by probe", "S2", "22") == "probe: old token 21; this token unchecked"
    sw = "SERIES_SWITCH: bhavcopy series EQ, Kite lists X-BE (31); price checked by probe"
    assert n(sw, "S3", "31") == ("SERIES_SWITCH: bhavcopy series EQ, Kite lists X-BE (31); price check new token PASS "
                                 "2026-11-02 (5 days, max 0.30%)")
    assert n("price checked by probe", "S4", "41").startswith("retoken: price check OK 0.10%")
    assert n("price checked by probe", "S5", "51") == "unchecked"
    assert n("2 candidates: A, B", "S5", "", "REVIEW") == "2 candidates: A, B"


# ---- the gate in update ----

def test_right_token_passes_and_rebuilds(done_copy):
    f = done_copy
    extend(f)
    old = stock_files(f, "GEN1")
    kite = TokKite(last_day=N, new_tokens={"GEN1": NEW})
    st, logs = upd(f, kite)
    g = sid(f, "GEN1")
    assert st["token_pass"] == 2 and st["token_refused"] == 0 and st["built"] == 2        # GEN1 + NEWIPO
    assert progress(f, "GEN1")["token"] == str(NEW) and progress(f, "GEN1")["status"] == "DONE"
    arch = [a for a in archives(f) if a.startswith(g)]
    assert len(arch) == 1
    arch_dir = os.path.join(mp.out_dir(f), "archive", arch[0])
    assert "token 110 -> 211" in open(os.path.join(arch_dir, "archived.txt")).read()
    assert {k: v for k, v in tree(arch_dir).items() if k != "archived.txt"} == old           # the old files kept
    assert days_of(f, "GEN1").index[-1] == DAYS[N]
    checks = tc.read_checks(f)
    gen1 = checks[checks["security_id"] == g]
    assert list(gen1["kind"]) == ["PRECHECK", "AFTER_DOWNLOAD"] and set(gen1["result"]) == {"PASS"}
    assert gen1.iloc[0]["days"] == "5" and os.path.isfile(gen1.iloc[0]["report"])
    assert len(tc.active(f)) == 0
    # the log names every full download (E) and the summary counts the checks
    assert any(x == f"  full download {g} GEN1: 110 -> {NEW} (TOKEN_CHANGE)" for x in logs)
    assert any(x.startswith(f"  full download {sid(f, 'NEWIPO')} NEWIPO: - -> 120 (NEW_LISTING)") for x in logs)
    assert any(x.startswith(f"  FULL {g} GEN1: 110 -> {NEW} (TOKEN_CHANGE): PASS, 5 days, max diff close 0.00%")
               for x in logs)
    assert any(x.startswith(f"  FULL {sid(f, 'NEWIPO')} NEWIPO: - -> 120 (NEW_LISTING): PASS, 1 days") for x in logs)
    assert any(x.startswith(f"  COMPARE {g} GEN1: ") and x.endswith(": replaced") for x in logs)
    assert any("token checks: 2 passed, 0 refused, 0 no reference." in x for x in logs)
    assert "Full downloads and token checks:" in logs
    # tokens.csv says what was checked (P93)
    toks = pd.read_csv(os.path.join(mp.out_dir(f), "update", "tokens.csv"), dtype=str,
                       keep_default_na=False).set_index("symbol")
    assert toks.loc["GEN1", "note"].startswith("price check new token PASS ") and \
        toks.loc["GEN1", "note"].endswith("(5 days, max 0.00%)")
    assert toks.loc["NEWIPO", "note"].startswith("price check new token PASS ")
    assert toks.loc["BIG", "note"].startswith("probe: price checked ")
    assert "price checked by probe" not in "".join(toks["note"])
    assert cli.run_result(st, "update") == 0


def test_wrong_token_refused_files_byte_identical(done_copy, capsys):
    f = done_copy
    extend(f)
    g = sid(f, "GEN1")
    old, before_days = stock_files(f, "GEN1"), progress(f, "GEN1")
    kite = TokKite(last_day=N, new_tokens={"GEN1": NEW}, alias={NEW: "BIG"})      # another company's candles
    st, logs = upd(f, kite)
    assert st["token_refused"] == 1 and st["token_pass"] == 1                        # NEWIPO passes
    assert stock_files(f, "GEN1") == old and progress(f, "GEN1") == before_days      # untouched, old token kept
    assert not [a for a in archives(f) if a.startswith(g)]
    assert [c for c in kite.calls if c[0] == NEW and c[3] == "minute"] == []         # no download at all
    r = tc.active(f, g)
    assert r["state"] == tc.PENDING and r["result"] == tc.FAIL and r["old_token"] == "110" and \
        r["new_token"] == str(NEW) and r["reason"] == tc.TOKEN_CHANGE
    ev = pd.read_csv(r["report"])
    assert len(ev) == 5 and not ev["ok"].any() and set(ev.columns) == set(tc.EVIDENCE_COLUMNS)
    assert days_of(f, "BIG").index[-1] == DAYS[N]                                    # the others are updated
    assert any("FAIL" in x and "refused, TOKEN_REVIEW" in x for x in logs)
    assert any("token checks: 1 passed, 1 refused, 0 no reference." in x for x in logs)
    assert any(x.startswith("Token reviews open: 1") for x in logs)
    assert cli.run_result(st, "update") == 1
    assert "token change(s) refused" in capsys.readouterr().err
    toks = pd.read_csv(os.path.join(mp.out_dir(f), "update", "tokens.csv"), dtype=str,
                       keep_default_na=False).set_index("symbol")
    assert toks.loc["GEN1", "note"].startswith("price check new token FAIL ")
    first = r["first_seen"]
    # the next update checks again (still refused, first_seen kept); download does not bypass the gate
    st2, _ = upd(f, TokKite(last_day=N, new_tokens={"GEN1": NEW}, alias={NEW: "BIG"}),
                 now=MORNING_AFTER + timedelta(hours=1))
    assert st2["token_refused"] == 1 and tc.active(f, g)["first_seen"] == first
    k3 = TokKite(last_day=N, new_tokens={"GEN1": NEW}, alias={NEW: "BIG"})
    res = md.download(f, kite=k3, speed=FAST, workers=1, log=q)
    assert res["stats"]["token_refused"] == 1 and [c for c in k3.calls if c[3] == "minute"] == []
    assert stock_files(f, "GEN1") == old and progress(f, "GEN1") == before_days


def test_no_reference_refused(done_copy, monkeypatch):
    f = done_copy
    extend(f)
    g = sid(f, "GEN1")
    real = md.Calc.bhav_of

    def suspended(self, s):              # GEN1 has no bhavcopy row in the last 60 trading days
        b = real(self, s)
        return b[[sm.as_date(d) < DAYS[N - 60] for d in b["date"]]] if s == g else b
    monkeypatch.setattr(md.Calc, "bhav_of", suspended)
    old = stock_files(f, "GEN1")
    kite = TokKite(last_day=N, new_tokens={"GEN1": NEW})
    st, logs = upd(f, kite)
    assert st["token_noref"] == 1 and st["token_refused"] == 0
    assert stock_files(f, "GEN1") == old and progress(f, "GEN1")["token"] == "110"
    assert tc.active(f, g)["result"] == tc.NO_REFERENCE
    assert [c for c in kite.calls if c[0] == NEW] == []                           # nothing to compare: no request
    assert any("NO_REFERENCE, 0 reference day(s)" in x for x in logs)
    assert any("token checks: 1 passed, 0 refused, 1 no reference." in x for x in logs)
    assert cli.run_result(st, "update") == 1


@pytest.mark.parametrize("how", ["empty", "failing"])
def test_no_candles_refused_without_crash(done_copy, how):
    f = done_copy
    extend(f)
    old = stock_files(f, "GEN1")
    kite = TokKite(last_day=N, new_tokens={"GEN1": NEW}, **{how: {NEW}})
    st, logs = upd(f, kite)
    assert st["token_refused"] == 1 and st["failed_stocks"] == 0
    assert stock_files(f, "GEN1") == old and progress(f, "GEN1")["token"] == "110"
    r = tc.active(f, sid(f, "GEN1"))
    assert r["result"] == tc.NO_CANDLES
    if how == "failing":
        assert "invalid token" in r["note"]
    assert days_of(f, "MID").index[-1] == DAYS[N]


def test_after_download_difference_parks_and_keeps_old(done_copy):
    f = done_copy
    extend(f)
    g = sid(f, "GEN1")
    old, prog = stock_files(f, "GEN1"), progress(f, "GEN1")
    kite = TokKite(last_day=N, new_tokens={"GEN1": NEW}, worse_days=WORSE)
    st, logs = upd(f, kite)
    assert st["token_pass"] == 2 and st["token_refused"] == 1 and st["built"] == 1          # NEWIPO only
    assert stock_files(f, "GEN1") == old and progress(f, "GEN1") == prog
    assert not [a for a in archives(f) if a.startswith(g)]
    assert not os.path.exists(ms.stock_dir(f, g) + ".building")
    r = tc.active(f, g)
    assert r["state"] == tc.PENDING and r["result"] == tc.AFTER_DOWNLOAD and os.path.isdir(r["parked"])
    assert os.path.isfile(os.path.join(r["parked"], ms.DAYS_FILE))
    cmp = pd.read_csv(r["report"])
    assert list(cmp["date"]) == [str(DAYS[i]) for i in (30, 31)] and not cmp["exempt"].any()
    assert cmp["max_diff"].min() > 0.005 and set(cmp["why"]) == {"old row matches the bhavcopy as well or better"}
    assert any("REFUSED" in x and "old files kept" in x for x in logs)
    assert not os.path.isdir(md.staging_dir(f, g))
    # a parked review waits for token-review: the next update does not download again
    k2 = TokKite(last_day=N, new_tokens={"GEN1": NEW}, worse_days=WORSE)
    st2, logs2 = upd(f, k2, now=MORNING_AFTER + timedelta(hours=1))
    assert [c for c in k2.calls if c[0] == NEW] == [] and st2["token_frozen"] == 1
    assert any("TOKEN_REVIEW pending" in x and "frozen" in x for x in logs2)
    # token-review: the dry run writes nothing; --apply installs the parked download
    parked_files = tree(r["parked"])
    before = tree(mp.out_dir(f))
    assert cli.main(["token-review", "--folder", f]) == 0
    assert cli.main(["token-review", "--accept", g, "--folder", f]) == 0
    assert tree(mp.out_dir(f)) == before
    assert cli.main(["token-review", "--accept", g, "--apply", "--folder", f]) == 0
    assert stock_files(f, "GEN1") == parked_files and progress(f, "GEN1")["token"] == str(NEW)
    arch = [a for a in archives(f) if a.startswith(g)]
    assert len(arch) == 1 and "accepted after review" in open(
        os.path.join(mp.out_dir(f), "archive", arch[0], "archived.txt")).read()
    assert tc.active(f, g) is None and tc.read_review(f).iloc[-1]["state"] == tc.BUILT
    assert "token-review --accept" in open(os.path.join(tc_dir(f), "review.log")).read()
    after = tree(mp.out_dir(f))
    res = tc.review(f, accept=g, log=q)                                         # a second dry run finds nothing
    assert res["action"] is None and tree(mp.out_dir(f)) == after


def test_after_download_exempt_days_and_cap(done_copy):
    """Days where the new bars match the bhavcopy better are exempt, at most MAX_EXEMPT per stock (listed)."""
    f = done_copy
    extend(f)
    g = sid(f, "GEN1")
    three = {(NEW, i) for i in (30, 31, 32)}
    st, logs = upd(f, TokKite(last_day=N, new_tokens={"GEN1": NEW}, exempt_days=three))
    assert st["token_refused"] == 0 and progress(f, "GEN1")["token"] == str(NEW)
    cmp = tc.read_checks(f)
    rep = pd.read_csv(cmp[(cmp["security_id"] == g) & (cmp["kind"] == "AFTER_DOWNLOAD")].iloc[0]["report"])
    assert list(rep["date"]) == [str(DAYS[i]) for i in (30, 31, 32)] and rep["exempt"].all()
    assert any("exempt 3 (" + ", ".join(str(DAYS[i]) for i in (30, 31, 32)) + ")" in x for x in logs)


def test_after_download_more_than_five_exempt_days_refused(done_copy):
    f = done_copy
    extend(f)
    g = sid(f, "GEN1")
    old = stock_files(f, "GEN1")
    six = {(NEW, i) for i in (30, 32, 34, 36, 38, 41)}      # not 5 in a row: no minute_scale level (levels())
    st, logs = upd(f, TokKite(last_day=N, new_tokens={"GEN1": NEW}, exempt_days=six))
    assert st["token_refused"] == 1 and stock_files(f, "GEN1") == old
    r = tc.active(f, g)
    assert r["result"] == tc.AFTER_DOWNLOAD and "6 exempt days, more than 5" in r["note"]


def test_stop_between_download_and_replace_then_resume(done_copy, monkeypatch):
    f = done_copy
    extend(f)
    g = sid(f, "GEN1")
    old, prog = stock_files(f, "GEN1"), progress(f, "GEN1")
    real = md.Runner.compare_replace

    def stop(self, item, tmp, final):
        raise StopJob()
    monkeypatch.setattr(md.Runner, "compare_replace", stop)
    st, _ = upd(f, TokKite(last_day=N, new_tokens={"GEN1": NEW}))
    assert st["stopped"]
    assert stock_files(f, "GEN1") == old and progress(f, "GEN1") == prog          # byte-identical, still DONE
    monkeypatch.setattr(md.Runner, "compare_replace", real)
    k2 = TokKite(last_day=N, new_tokens={"GEN1": NEW})
    st2, logs = upd(f, k2, now=MORNING_AFTER + timedelta(hours=1))
    assert progress(f, "GEN1")["token"] == str(NEW) and days_of(f, "GEN1").index[-1] == DAYS[N]
    assert [c for c in k2.calls if c[0] == NEW and c[3] == "minute"] == []           # the staged chunks resumed
    assert [c for c in k2.calls if c[0] == NEW and c[3] == "day"] == [(NEW, DAYS[N - 4], DAYS[N], "day")]  # pre-check
    assert len([a for a in archives(f) if a.startswith(g)]) == 1
    assert any(x.startswith(f"  COMPARE {g} GEN1: ") and x.endswith(": replaced") for x in logs)


def test_reject_freezes_until_kite_changes_token(done_copy):
    f = done_copy
    extend(f)
    g = sid(f, "GEN1")
    upd(f, TokKite(last_day=N, new_tokens={"GEN1": NEW}, alias={NEW: "BIG"}))
    before = tree(mp.out_dir(f))
    assert cli.main(["token-review", "--reject", g, "--folder", f]) == 0                  # dry run
    assert tree(mp.out_dir(f)) == before
    assert cli.main(["token-review", "--reject", g, "--apply", "--folder", f]) == 0
    r = tc.active(f, g)
    assert r["state"] == tc.REJECTED and r["decided_at"]
    old = stock_files(f, "GEN1")
    k = TokKite(last_day=N, new_tokens={"GEN1": NEW}, alias={NEW: "BIG"})
    st, logs = upd(f, k, now=MORNING_AFTER + timedelta(hours=1))
    assert [c for c in k.calls if c[0] in (NEW, 110)] == [] and st["token_frozen"] == 1
    assert any(f"GEN1 ({g}): REJECTED, frozen since {r['decided_at'][:10]}" in x for x in logs)
    assert stock_files(f, "GEN1") == old and cli.run_result(st, "update") == 0
    after = tree(mp.out_dir(f))
    assert tc.review(f, reject=g, log=q)["action"] is None and tree(mp.out_dir(f)) == after   # second dry run: 0
    # Kite gives another token: the mark clears and that token is checked
    k2 = TokKite(last_day=N, new_tokens={"GEN1": 212})
    st2, logs2 = upd(f, k2, now=MORNING_AFTER + timedelta(hours=2))
    assert any("token review of 211 cleared, Kite's token is now 212" in x for x in logs2)
    assert progress(f, "GEN1")["token"] == "212" and st2["token_pass"] == 1


def test_token_review_lock(done_copy):
    f = done_copy
    extend(f)
    g = sid(f, "GEN1")
    upd(f, TokKite(last_day=N, new_tokens={"GEN1": NEW}, alias={NEW: "BIG"}))
    before = tree(tc_dir(f))
    lock = RunLock(mp.lock_path(f)).acquire("test")
    try:
        assert cli.main(["token-review", "--reject", g, "--apply", "--folder", f]) == 2
        assert cli.main(["token-review", "--reject", g, "--folder", f]) == 0           # the dry run needs no lock
    finally:
        lock.release()
    assert tree(tc_dir(f)) == before and tc.active(f, g)["state"] == tc.PENDING


def test_accept_rebuilds_without_precheck_but_compares(done_copy):
    f = done_copy
    extend(f)
    g = sid(f, "GEN1")
    upd(f, TokKite(last_day=N, new_tokens={"GEN1": NEW}, alias={NEW: "BIG"}))
    assert cli.main(["token-review", "--accept", g, "--apply", "--folder", f]) == 0
    assert tc.active(f, g)["state"] == tc.ACCEPTED
    old = stock_files(f, "GEN1")
    # accepted, but the new minute history differs: the after-download comparison still refuses it
    st, logs = upd(f, TokKite(last_day=N, new_tokens={"GEN1": NEW}, worse_days=WORSE),
                   now=MORNING_AFTER + timedelta(hours=1))
    assert st["token_accepted"] == 1 and st["token_pass"] == 0 and st["token_refused"] == 1
    assert any("ACCEPTED by token-review (pre-check skipped)" in x for x in logs)
    assert stock_files(f, "GEN1") == old and tc.active(f, g)["result"] == tc.AFTER_DOWNLOAD
    pre = tc.read_checks(f)
    assert list(pre[pre["security_id"] == g]["kind"]) == ["PRECHECK", "AFTER_DOWNLOAD"]


# ---- unchanged behaviour ----

def test_same_token_no_kite_data_and_new_listings(done_copy):
    """SAME_TOKEN stocks get only their daily request (no pre-check); a NO_KITE_DATA stock missing from Kite's list
    (the P65 weekly-auction case) is skipped as before; a new listing is checked (NEW_LISTING PASS / FAIL)."""
    f = done_copy
    extend(f)
    g3 = sid(f, "GEN3")
    db = md.Progress(f)
    try:
        db.set_no_kite(g3, 112, "not in Kite's NSE list of 2015-04-24", DAYS[N - 1])
    finally:
        db.close()
    gen3 = stock_files(f, "GEN3")
    kite = TokKite(last_day=N, drop={"GEN3"}, alias={120: "BIG"})                  # NEWIPO's token is wrong
    st, logs = upd(f, kite)
    assert st["token_pass"] == 0 and st["token_refused"] == 1 and st["updated"] == 12
    day_calls = [c for c in kite.calls if c[0] != "instruments" and c[3] == "day"]
    assert sorted(c[0] for c in day_calls) == sorted(STOCKS[s]["token"] for s in
                                                       ("BIG", "MID", "PTWO", "SCALECO", "VOLCO", "ADDCO", "SPLITCO",
                                                        "EMPTYCO", "NEWNM", "GEN1", "GEN2", "GEN4", "NEWIPO"))
    assert all(c[1] == DAYS[N] for c in day_calls)                                  # daily: from the next day only
    assert stock_files(f, "GEN3") == gen3 and progress(f, "GEN3")["status"] == md.NO_KITE
    checks = tc.read_checks(f)
    assert list(checks["symbol"]) == ["NEWIPO"] and list(checks["reason"]) == [tc.NEW_LISTING]
    r = tc.active(f, sid(f, "NEWIPO"))
    assert r["reason"] == tc.NEW_LISTING and r["result"] == tc.FAIL and r["old_token"] == ""
    assert not os.path.isdir(ms.stock_dir(f, sid(f, "NEWIPO")))
    assert any("(NEW_LISTING): FAIL" in x for x in logs)


def test_gap_fill_after_freeze(planned, tmp_path):
    """A stock frozen for 3 trading days (REJECTED), then Kite lists its old token again: the mark clears and the
    daily append (Runner.append, from the day after its last stored day) stores the 3 missed days and T."""
    f = str(tmp_path / "data")
    shutil.copytree(planned, f)
    start = N - 4
    build_inputs(f, start)
    md.download(f, kite=TokKite(last_day=start), speed=FAST, workers=2, log=q)
    g = sid(f, "GEN1")
    assert days_of(f, "GEN1").index[-1] == DAYS[start]

    def day(i, **kw):
        build_inputs(f, i)
        at = datetime.combine(DAYS[i] + timedelta(days=1), dtime(2), tzinfo=mp.IST)
        return upd(f, TokKite(last_day=i, **kw), now=at)
    day(start + 1, new_tokens={"GEN1": NEW}, alias={NEW: "BIG"})                    # refused
    assert tc.review(f, reject=g, apply=True, log=q)["done"]
    for i in (start + 2, start + 3):
        st, logs = day(i, new_tokens={"GEN1": NEW}, alias={NEW: "BIG"})
        assert st["token_frozen"] == 1 and any("REJECTED, frozen since" in x for x in logs)
    assert days_of(f, "GEN1").index[-1] == DAYS[start]                              # frozen: 3 days missed
    assert days_of(f, "BIG").index[-1] == DAYS[start + 3]
    st, logs = day(N)                                                               # Kite: token 110 again
    assert any("cleared, Kite's token is back to 110; the missed days are fetched" in x for x in logs)
    gen1 = days_of(f, "GEN1")
    assert list(gen1.index[-4:]) == [DAYS[start + 1], DAYS[start + 2], DAYS[start + 3], DAYS[N]]
    assert (gen1.loc[DAYS[start + 1]:, "check"] == "OK").all() and progress(f, "GEN1")["token"] == "110"
    m = ms.load_minute("GEN1", DAYS[start + 1], DAYS[N], folder=f)
    assert sorted(set(m["ts"].dt.date)) == [DAYS[start + 1], DAYS[start + 2], DAYS[start + 3], DAYS[N]]
    assert tc.active(f, g) is None and tc.read_review(f).iloc[-1]["state"] == tc.CLEARED


# ---- token-replay ----

def test_token_replay_reads_archives_writes_nothing(done_copy, monkeypatch, capsys):
    f = done_copy
    extend(f)
    upd(f, TokKite(last_day=N, new_tokens={"GEN1": NEW, "BIG": 222}))
    stamp_day = datetime.now(mp.IST).date().isoformat()
    names = {a.split("_")[0] for a in archives(f)}
    assert names == {sid(f, "GEN1"), sid(f, "BIG")}
    before = tree(f)
    kite = TokKite(last_day=N, new_tokens={"GEN1": NEW, "BIG": 222})
    monkeypatch.setattr(mp, "kite_client", lambda: kite)
    assert cli.main(["token-replay", "--dates", stamp_day, "--folder", f]) == 0
    out = capsys.readouterr().out
    assert tree(f) == before
    assert "rows 2: PASS 2 (sum 2)" in out and "Kite day-candle requests: 2; files written: 0" in out
    inp = mp.Inputs(f, bhav=False)
    res = tc.replay(f, kite, md.Calc(f, inp, write=False), dates=(stamp_day,), controls=(("GEN1", "BIG"),), log=q)
    assert res["totals"] == {"PASS": 2, "FAIL": 1} and len(res["rows"]) == 3 and res["requests"] == 2
    assert tree(f) == before

import csv
import os
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone

import pytest
from kiteconnect import exceptions as kex

from backfill import fno
from backfill.job import JOB, STOP, job_begin
from backfill.storage import open_db

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# --- names (from the brief) ---------------------------------------------------------------

def test_name_future():
    assert fno.contract_name("FUT", date(2022, 2, 24)) == "NIFTY24FEB22"


def test_name_put_float_strike():
    assert fno.contract_name("PE", date(2026, 12, 29), 12000.0) == "NIFTY1200029DEC26PE"


def test_name_call_int_strike():
    assert fno.contract_name("CE", date(2026, 12, 29), 16000) == "NIFTY1600029DEC26CE"


def test_name_long_dated_put():
    assert fno.contract_name("PE", date(2027, 12, 28), 15000) == "NIFTY1500028DEC27PE"


def test_name_other_underlying():
    assert fno.contract_name("FUT", date(2026, 10, 29), underlying="SENSEX") == "SENSEX29OCT26"


@pytest.mark.parametrize("strike", [12000.5, None, 0, -100, float("nan")])
def test_name_bad_strike_raises(strike):
    with pytest.raises(ValueError):
        fno.contract_name("CE", date(2026, 12, 29), strike)


def test_name_bad_type_raises():
    with pytest.raises(ValueError):
        fno.contract_name("XX", date(2026, 12, 29), 12000)


def test_name_ignores_tradingsymbol():
    # Kite's weekly symbols look like NIFTY2610625000CE; the file name is built from expiry/strike
    inst = {"tradingsymbol": "NIFTY2610625000CE", "instrument_type": "CE",
            "expiry": date(2026, 10, 6), "strike": 25000.0, "name": "NIFTY"}
    name, path = fno.contract_file("D", inst, "NIFTY")
    assert name == "NIFTY2500006OCT26CE"
    assert path == os.path.join("D", "options", "NIFTY2500006OCT26CE.csv")


def test_name_from_string_expiry():
    assert fno.contract_name("FUT", "2026-10-27") == "NIFTY27OCT26"


# --- candle rows and files ------------------------------------------------------------------

def candle(ts, o=25120.5, h=25140.0, l=25110, c=25135, v=58955, oi=14046305):
    return {"date": ts, "open": o, "high": h, "low": l, "close": c, "volume": v, "oi": oi}


def test_timestamp_string_to_date_time():
    rows = fno.candle_rows("NIFTY27OCT26", [candle("2026-10-12T09:15:00+0530")])
    assert rows[0][1:3] == ["20261012", "09:15:00"]


def test_aware_utc_datetime_is_converted_to_ist():
    rows = fno.candle_rows("X", [candle(datetime(2026, 10, 12, 3, 45, tzinfo=timezone.utc))])
    assert rows[0][1:3] == ["20261012", "09:15:00"]


def test_naive_datetime_is_taken_as_ist():
    rows = fno.candle_rows("X", [candle(datetime(2026, 10, 12, 15, 59))])
    assert rows[0][1:3] == ["20261012", "15:59:00"]


def test_row_matches_brief_example():
    rows = fno.candle_rows("NIFTY27OCT26", [candle("2026-10-12T09:15:00+0530")])
    assert ",".join(rows[0]) == "NIFTY27OCT26,20261012,09:15:00,25120.5,25140,25110,25135,58955,14046305"


def test_zero_candles_writes_no_file(tmp_path):
    path = tmp_path / "options" / "NIFTY2500013OCT26CE.csv"
    assert fno.write_contract(str(path), "NIFTY2500013OCT26CE", []) == 0
    assert not path.exists()


def test_zero_volume_writes_no_file(tmp_path):
    path = tmp_path / "options" / "NIFTY2500013OCT26CE.csv"
    candles = [candle("2026-10-12T09:15:00+0530", v=0), candle("2026-10-12T09:16:00+0530", v=0)]
    assert fno.write_contract(str(path), "NIFTY2500013OCT26CE", candles) == 0
    assert not path.exists()


def test_write_contract_exact_bytes(tmp_path):
    path = tmp_path / "futures" / "NIFTY27OCT26.csv"
    candles = [candle("2026-10-12T09:15:00+0530"),
               candle("2026-10-12T15:41:00+0530", 25100.0, 25100.0, 25100.0, 25100.0, 0, 14000000)]
    assert fno.write_contract(str(path), "NIFTY27OCT26", candles) == 2
    assert path.read_bytes() == (
        b"Ticker,Date,Time,Open,High,Low,Close,Volume,OI\n"
        b"NIFTY27OCT26,20261012,09:15:00,25120.5,25140,25110,25135,58955,14046305\n"
        b"NIFTY27OCT26,20261012,15:41:00,25100,25100,25100,25100,0,14000000\n")
    assert not (tmp_path / "futures" / "NIFTY27OCT26.csv.tmp").exists()


# --- selection, buckets, ATM ----------------------------------------------------------------

def inst(sym, typ, expiry, strike=0.0, name="NIFTY", token=None):
    return {"instrument_token": token or abs(hash(sym)) % 10**7, "tradingsymbol": sym, "name": name,
            "instrument_type": typ, "expiry": expiry, "strike": strike, "segment": "NFO-OPT"}


def test_select_contracts_filters_and_orders():
    d1, d2, m1 = date(2026, 10, 13), date(2026, 10, 20), date(2026, 10, 27)
    dump = [
        inst("N3", "PE", d2, 25000), inst("N2", "CE", d1, 25100), inst("N1", "CE", d1, 25000),
        inst("N4", "PE", d1, 25000), inst("F2", "FUT", date(2026, 11, 24)), inst("F1", "FUT", m1),
        inst("OLD", "CE", date(2026, 10, 6), 25000),                 # expired before run date
        inst("BN", "FUT", m1, name="BANKNIFTY"), inst("NN", "FUT", m1, name="NIFTYNXT50"),
    ]
    got = [i["tradingsymbol"] for i in fno.select_contracts(dump, "NIFTY", date(2026, 10, 12))]
    assert got == ["F1", "F2", "N1", "N4", "N2", "N3"]


def test_bucket_labels_two_labels_on_one_expiry():
    # Monthly expiry is also the 2nd option expiry: "Next week + Current month"
    opt = [date(2026, 10, 20), date(2026, 10, 27), date(2026, 11, 3), date(2026, 11, 24)]
    fut = [date(2026, 10, 27), date(2026, 11, 24), date(2026, 12, 29)]
    assert fno.bucket_labels(opt, fut) == {
        date(2026, 10, 20): "Current week",
        date(2026, 10, 27): "Next week + Current month",
        date(2026, 11, 3): "Far",
        date(2026, 11, 24): "Next month",
    }


def test_bucket_labels_current_week_and_month():
    d = date(2026, 10, 27)
    assert fno.bucket_labels([d, date(2026, 11, 3)], [d])[d] == "Current week + Current month"


@pytest.mark.parametrize("close,atm", [(25124.9, 25100), (25125, 25150), (25174.99, 25150), (25175, 25200)])
def test_atm_strike_rounds_to_50(close, atm):
    assert fno.atm_strike(close) == atm


def test_atm_strike_step_100():
    assert fno.atm_strike(81249, 100) == 81200


# --- run date / time gate (IST) -------------------------------------------------------------

IST = fno.IST


def test_too_early_before_1540():
    assert "15:40" in fno.too_early(date(2026, 10, 12), now=datetime(2026, 10, 12, 15, 39, tzinfo=IST))
    assert fno.too_early(date(2026, 10, 12), now=datetime(2026, 10, 12, 15, 40, tzinfo=IST)) == ""


def test_too_early_only_today():
    assert fno.too_early(date(2026, 10, 9), now=datetime(2026, 10, 12, 10, 0, tzinfo=IST)) == ""


def test_too_early_on_expiry_day():
    # today is the current week expiry: refused at 15:00, allowed at 15:45
    expiry = date(2026, 10, 13)
    assert fno.too_early(expiry, now=datetime(2026, 10, 13, 15, 0, tzinfo=IST))
    assert fno.too_early(expiry, now=datetime(2026, 10, 13, 15, 45, tzinfo=IST)) == ""
    w = fno.date_window([expiry, date(2026, 10, 20)], {date(2026, 10, 6)}, expiry)
    assert w.current_expiry == w.max_date == expiry


def test_time_gate_uses_ist_not_utc():
    # 10:05 UTC is 15:35 IST: still too early; 10:15 UTC is 15:45 IST: fine
    early = datetime(2026, 10, 12, 10, 5, tzinfo=timezone.utc)
    late = datetime(2026, 10, 12, 10, 15, tzinfo=timezone.utc)
    assert fno.too_early(date(2026, 10, 12), now=early)
    assert fno.too_early(date(2026, 10, 12), now=late) == ""
    # 20:00 UTC on the 12th is already the 13th in IST
    assert fno.today_ist(datetime(2026, 10, 12, 20, 0, tzinfo=timezone.utc)) == date(2026, 10, 13)


# --- date window ------------------------------------------------------------------------------

D = lambda day: date(2026, 10, day)
LIVE = [D(13), D(20), D(27), date(2026, 11, 24)]


def test_window_from_history():
    w = fno.date_window(LIVE, {date(2026, 9, 29), D(6), D(13)}, D(11))
    assert (w.min_date, w.max_date, w.current_expiry, w.previous_expiry, w.estimated) == (D(7), D(11), D(13), D(6), False)
    assert fno.window_error(D(6), w) == ("Earliest date available is 2026-10-07. Contracts that expired on "
                                         "2026-10-06 are no longer served by Kite.")
    for day in (7, 8, 9, 11):
        assert fno.window_error(D(day), w) == ""
    assert "future" in fno.window_error(D(12), w)


def test_window_fallback_without_history():
    # current week expiry moved to Monday 19 Oct by a holiday: previous = Tuesday 13 Oct
    w = fno.date_window([D(19), D(27)], set(), D(15))
    assert (w.previous_expiry, w.min_date, w.estimated) == (D(13), D(14), True)
    assert fno.window_note(w).startswith("WARNING")


def test_window_stale_history_uses_fallback():
    # history last updated in September knows nothing about the previous week: don't trust it
    w = fno.date_window(LIVE, {date(2026, 9, 15)}, D(11))
    assert (w.previous_expiry, w.min_date, w.estimated) == (D(6), D(7), True)


def test_window_history_with_monday_expiry():
    # previous expiry was moved to Monday 5 Oct: history knows it, so 6 Oct is allowed
    w = fno.date_window(LIVE, {D(5)}, D(11))
    assert (w.previous_expiry, w.min_date, w.estimated) == (D(5), D(6), False)
    assert fno.window_note(w) == ""


def test_window_other_weekday():
    w = fno.date_window([D(15)], set(), D(12), weekday=3)   # Thursday expiries
    assert w.previous_expiry == D(8)


def expiry_rows(*days):
    return [{"name": "NIFTY", "instrument_type": "CE", "expiry": D(d)} for d in days] + \
           [{"name": "BANKNIFTY", "instrument_type": "FUT", "expiry": D(30)}]


def read_rows(path):
    with open(path, newline="", encoding="utf-8") as f:
        return [tuple(r.values()) for r in csv.DictReader(f)]


def test_expiries_seen_appended_and_deduplicated(tmp_path):
    fno.record_expiries(str(tmp_path), "NIFTY", expiry_rows(13, 20, 13), D(11))
    fno.record_expiries(str(tmp_path), "NIFTY", expiry_rows(20, 27), D(12))
    path = tmp_path / "NIFTY_FnO_1min" / fno.EXPIRIES_FILE
    assert read_rows(path) == [("2026-10-13", "2026-10-11"), ("2026-10-20", "2026-10-11"),
                               ("2026-10-27", "2026-10-12")]


def test_expiries_seeded_from_saved_dumps(tmp_path):
    day = tmp_path / "NIFTY_FnO_1min" / "2026-10-05"
    day.mkdir(parents=True)
    fno.save_instruments(str(day / "instruments_NFO.csv"), expiry_rows(6, 13))
    assert set(fno.read_expiries(str(tmp_path), "NIFTY")) == {D(6), D(13)}


def test_missing_dates(tmp_path):
    base = tmp_path / "NIFTY_FnO_1min"
    w = fno.Window(D(7), D(12), D(13), D(6), False)
    assert fno.missing_dates(str(tmp_path), "NIFTY", w, now=datetime(2026, 10, 12, 16, 0, tzinfo=IST)) == \
        [D(7), D(8), D(9), D(10), D(11), D(12)]
    assert not tmp_path.joinpath("progress.sqlite").exists()      # nothing created
    base.mkdir()
    (base / "holidays.log").write_text("2026-10-10 16:00:01 2026-10-10: no candles\n"
                                       "2026-10-11 16:00:01 2026-10-11: no candles\n", encoding="utf-8")
    db = open_db(str(tmp_path))
    for day, status in ((8, "DONE"), (9, "FAILED")):
        (base / D(day).isoformat()).mkdir()
        (base / D(day).isoformat() / "summary.csv").write_text("x")
        db.execute("INSERT INTO chunks (run, symbol, chunk_from, status) VALUES (?, 'A', 'x', ?)",
                   (fno.run_key("NIFTY", D(day)), status))
    db.commit()
    db.close()
    assert fno.missing_dates(str(tmp_path), "NIFTY", w, now=datetime(2026, 10, 12, 16, 0, tzinfo=IST)) == \
        [D(7), D(9), D(12)]
    assert fno.missing_dates(str(tmp_path), "NIFTY", w, now=datetime(2026, 10, 12, 15, 0, tzinfo=IST)) == \
        [D(7), D(9)]


TZ_SCRIPT = r"""
import time
from datetime import date, datetime, timezone
if hasattr(time, "tzset"):
    time.tzset()
from backfill import fno
rows = fno.candle_rows("X", [
    {"date": "2026-10-12T09:15:00+0530", "open": 1, "high": 1, "low": 1, "close": 1, "volume": 5, "oi": 1},
    {"date": datetime(2026, 10, 12, 4, 0, tzinfo=timezone.utc), "open": 1, "high": 1, "low": 1, "close": 1, "volume": 5, "oi": 1},
    {"date": datetime.fromisoformat("2026-10-12T15:29:00+05:30"), "open": 1, "high": 1, "low": 1, "close": 1, "volume": 5, "oi": 1},
])
assert [r[1:3] for r in rows] == [["20261012", "09:15:00"], ["20261012", "09:30:00"], ["20261012", "15:29:00"]], rows
assert fno.now_ist().utcoffset().total_seconds() == 19800
assert fno.too_early(date(2026, 10, 12), now=datetime(2026, 10, 12, 10, 5, tzinfo=timezone.utc))
print("ok")
"""


def test_runs_correctly_with_tz_utc():
    env = {**os.environ, "TZ": "UTC"}
    out = subprocess.run([sys.executable, "-c", TZ_SCRIPT], cwd=ROOT, env=env,
                         capture_output=True, text=True, timeout=60)
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "ok"


# --- cross-process lock ---------------------------------------------------------------------

def test_lock_blocks_second_holder(tmp_path):
    first = fno.acquire_lock(str(tmp_path), "NIFTY", "test one")
    try:
        with pytest.raises(fno.LockBusy) as e:
            fno.acquire_lock(str(tmp_path), "NIFTY", "test two")
        assert "test one" in str(e.value)
    finally:
        first.release()
    fno.acquire_lock(str(tmp_path), "NIFTY", "test three").release()
    assert (tmp_path / "NIFTY_FnO_1min" / ".lock").exists()


LOCK_SCRIPT = r"""
import sys
from backfill import fno
try:
    fno.acquire_lock(sys.argv[1], "NIFTY", "child").release()
    print("acquired")
except fno.LockBusy:
    print("busy")
"""


def test_lock_blocks_other_process(tmp_path):
    run = lambda: subprocess.run([sys.executable, "-c", LOCK_SCRIPT, str(tmp_path)], cwd=ROOT,
                                 capture_output=True, text=True, timeout=60).stdout.strip()
    lock = fno.acquire_lock(str(tmp_path), "NIFTY", "parent")
    try:
        assert run() == "busy"
    finally:
        lock.release()
    assert run() == "acquired"


def test_other_underlying_has_own_lock(tmp_path):
    a = fno.acquire_lock(str(tmp_path), "NIFTY", "a")
    try:
        fno.acquire_lock(str(tmp_path), "SENSEX", "b").release()
    finally:
        a.release()


# --- full run with a fake Kite ----------------------------------------------------------------

RUN_DATE = date(2026, 10, 12)
W1, W2, M1, M2 = date(2026, 10, 13), date(2026, 10, 20), date(2026, 10, 27), date(2026, 11, 24)


def bars(n, start_close, vol=100, first="09:15"):
    h, m = map(int, first.split(":"))
    t0 = datetime(2026, 10, 12, h, m, tzinfo=IST)
    return [candle(t0 + timedelta(minutes=k), start_close, start_close + 5, start_close - 5,
                   start_close + k, vol, 1000 + k) for k in range(n)]


def make_dump():
    return [
        inst("NIFTY26OCTFUT", "FUT", M1, token=1),
        inst("NIFTY26NOVFUT", "FUT", M2, token=2),
        inst("NIFTY2610625100CE", "CE", W1, 25100.0, token=11),
        inst("NIFTY2610625100PE", "PE", W1, 25100.0, token=12),
        inst("NIFTY2610625150CE", "CE", W1, 25150.0, token=13),   # no trades
        inst("NIFTY2610625150PE", "PE", W1, 25150.0, token=14),   # zero candles
        inst("NIFTY2610625125CE", "CE", W1, 25125.5, token=15),   # bad strike
        inst("NIFTY26O2025100CE", "CE", W2, 25100.0, token=21),
        inst("NIFTY26OCT25100PE", "PE", M1, 25100.0, token=31),   # kite fails this one
        inst("NIFTY2610625100XX", "CE", date(2026, 10, 9), 25100.0, token=99),  # expired
        inst("BANKNIFTY26OCTFUT", "FUT", M1, name="BANKNIFTY", token=500),
    ]


class FakeKite:
    calls = []
    profile_error = None
    holiday = False
    fail_tokens = {31}
    candles = {
        1: bars(3, 25120.0, first="09:14") + bars(2, 25140.0, first="15:35"),  # 09:14 pre-open, 15:35+ post-close
        2: bars(4, 25300.0),
        11: bars(5, 120.0),
        12: bars(3, 95.0),
        13: bars(2, 80.0, vol=0),
        14: [],
        21: bars(2, 150.0),
    }

    def __init__(self, api_key):
        self.api_key = api_key

    def set_access_token(self, token):
        self.token = token

    def profile(self):
        if FakeKite.profile_error:
            raise FakeKite.profile_error
        return {"user_id": "AB1234"}

    def instruments(self, exchange):
        assert exchange == "NFO"
        return make_dump()

    def historical_data(self, token, start, end, interval, continuous=False, oi=False):
        FakeKite.calls.append((token, start, end, interval, continuous, oi))
        if FakeKite.holiday or start.weekday() >= 5:   # holidays and weekends have no candles
            return []
        if token in FakeKite.fail_tokens:
            raise kex.InputException("invalid token", code=400)
        return FakeKite.candles.get(token, [])


def set_now(monkeypatch, day, hhmm=(16, 0)):
    monkeypatch.setattr(fno, "now_ist", lambda: datetime(day.year, day.month, day.day, *hhmm, tzinfo=IST))


@pytest.fixture
def fake_kite(monkeypatch):
    FakeKite.calls, FakeKite.profile_error, FakeKite.holiday = [], None, False
    FakeKite.fail_tokens = {31}
    monkeypatch.setattr(fno, "KiteConnect", FakeKite)
    set_now(monkeypatch, RUN_DATE)
    STOP.clear()
    return FakeKite


ACCESS = "SECRET-ACCESS-TOKEN"
PENDING_COLS = [""] * len(fno.BHAV_SUMMARY_COLUMNS) + ["OFF"]   # bhavcopy columns blank; Keep_zero_volume


def run(tmp_path, day=RUN_DATE, log=lambda m: None):
    assert job_begin(**fno.job_fields("NIFTY", day, str(tmp_path), 1000))
    return fno.run_daily("key", ACCESS, day, str(tmp_path), 1000, log=log)


def read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.reader(f))


def test_run_daily_full(tmp_path, fake_kite):
    assert run(tmp_path) == "finished_failures"
    day = tmp_path / "NIFTY_FnO_1min" / "2026-10-12"

    assert sorted(os.listdir(day / "futures")) == ["NIFTY24NOV26.csv", "NIFTY27OCT26.csv"]
    assert sorted(os.listdir(day / "options")) == ["NIFTY2510013OCT26CE.csv", "NIFTY2510013OCT26PE.csv",
                                                   "NIFTY2510020OCT26CE.csv"]
    assert (day / "instruments_NFO.csv").exists()

    # window 09:00-16:00 with OI, continuous off, 3 tries max; pre-open and post-close rows kept
    tok, start, end, interval, continuous, oi = fake_kite.calls[0]
    assert (tok, interval, continuous, oi) == (1, "minute", False, True)
    assert (start, end) == (datetime(2026, 10, 12, 9, 0), datetime(2026, 10, 12, 16, 0))
    fut = read_csv(day / "futures" / "NIFTY27OCT26.csv")
    assert fut[0] == fno.HEADER
    assert [r[2] for r in fut[1:]] == ["09:14:00", "09:15:00", "09:16:00", "15:35:00", "15:36:00"]
    assert fut[1][0] == "NIFTY27OCT26" and fut[1][1] == "20261012"
    assert not any(t == 99 or t == 500 for t, *_ in fake_kite.calls)
    assert not any(t == 15 for t, *_ in fake_kite.calls)      # bad strike: no request made
    # the failing contract was tried in pass 1 and once more in the retry pass
    assert sum(1 for t, *_ in fake_kite.calls if t == 31) == 2

    summary = read_csv(day / "summary.csv")
    assert summary[0] == fno.SUMMARY_COLUMNS
    # ATM from the 09:15 close of the near future (25121, the 09:14 pre-open bar is skipped) -> 25100.
    # ATM bars: "" when that contract is not listed, 0 when it is listed but has no file.
    # Listed_strikes / Listed_contracts from the dump; Traded_strikes / CE / PE from files on disk.
    # 13 Oct lists strikes 25100, 25125.5 (bad), 25150 (5 contracts); only 25100 traded.
    # the six bhavcopy columns stay blank until a check has run
    assert summary[1] == ["2026-10-13", "Current week", "3", "1", "5", "1", "1", "25100", "5", "3"] + PENDING_COLS
    assert summary[2] == ["2026-10-20", "Next week", "1", "1", "1", "1", "0", "25100", "2", ""] + PENDING_COLS
    # nothing traded on the 27 Oct expiry (its only option failed): the row is still there
    assert summary[3] == ["2026-10-27", "Current month", "1", "0", "1", "0", "0", "25100", "", "0"] + PENDING_COLS
    # one row per future expiry
    assert [r[:2] for r in summary[4:]] == [["2026-10-27", "Futures"], ["2026-11-24", "Futures"]]

    log = (day / "run.log").read_text(encoding="utf-8")
    assert "PARTIAL" not in log
    assert "Bhavcopy: pending" in log and "Bhavcopy: pending" in JOB["message"]
    assert "NIFTY2610625125CE" in log and "token 15" in log         # naming failure, run went on
    assert "NIFTY26OCT25100PE" in log and "token 31" in log
    assert ACCESS not in log
    assert JOB["kind"] == "fno" and JOB["state"] == "finished" and JOB["failed"] == 2
    assert JOB["empty"] == 1 and JOB["zero_skipped"] == 1 and JOB["zero_kept"] == 0
    assert "No candles (no file): 1" in log and "Zero-volume candles: kept 0 / skipped 1" in log
    assert "Keep zero-volume contracts: OFF" in log
    assert [r["Future"] for r in JOB["fno_futures"]] == ["NIFTY27OCT26", "NIFTY24NOV26"]
    f1 = JOB["fno_futures"][0]
    # O/H/L/C over 09:15..15:30 only; volume over every row; last OI from the last row
    assert (f1["Bars"], f1["Open"], f1["High"], f1["Low"], f1["Close"], f1["Volume"], f1["Last_OI"]) ==         (5, "25120", "25125", "25115", "25122", "500", "1001")


def test_run_daily_resume_skips_everything(tmp_path, fake_kite):
    run(tmp_path)
    fake_kite.calls.clear()
    fake_kite.fail_tokens = set()
    assert run(tmp_path) == "finished_failures"     # bad strike still fails, no requests for done ones
    assert [t for t, *_ in fake_kite.calls] == [31]
    fake_kite.calls.clear()
    assert run(tmp_path) == "finished_failures"
    assert fake_kite.calls == []


def test_run_daily_holiday_deletes_day_folder(tmp_path, fake_kite):
    fake_kite.holiday = True
    assert run(tmp_path) == "holiday"
    base = tmp_path / "NIFTY_FnO_1min"
    assert not (base / "2026-10-12").exists()
    assert "2026-10-12" in (base / "holidays.log").read_text(encoding="utf-8")
    assert len(fake_kite.calls) == 1


def test_run_daily_holiday_keeps_folder_with_files(tmp_path, fake_kite):
    day = tmp_path / "NIFTY_FnO_1min" / "2026-10-12" / "options"
    day.mkdir(parents=True)
    (day / "NIFTY2510013OCT26CE.csv").write_text("x")
    fake_kite.holiday = True
    assert run(tmp_path) == "holiday"
    assert (day / "NIFTY2510013OCT26CE.csv").exists()


def test_run_daily_invalid_token(tmp_path, fake_kite):
    fake_kite.profile_error = kex.TokenException("Incorrect `api_key` or `access_token`.", code=403)
    assert run(tmp_path) == "auth_failed"
    assert fake_kite.calls == []
    assert not (tmp_path / "NIFTY_FnO_1min").exists()
    assert JOB["state"] == "stopped"


def no_partial_anywhere(folder):
    for root, _, names in os.walk(folder):
        for n in names:
            assert "PARTIAL" not in open(os.path.join(root, n), encoding="utf-8", errors="ignore").read(), n


def test_run_daily_past_date_inside_window(tmp_path, fake_kite, monkeypatch):
    # run on 13 Oct for 12 Oct: complete, the dump is named after the day it was fetched
    set_now(monkeypatch, D(13))
    assert run(tmp_path) == "finished_failures"
    day = tmp_path / "NIFTY_FnO_1min" / "2026-10-12"
    assert (day / "instruments_NFO_fetched_2026-10-13.csv").exists()
    assert not (day / "instruments_NFO.csv").exists()
    assert "NIFTY2510013OCT26CE.csv" in os.listdir(day / "options")
    no_partial_anywhere(tmp_path)


def test_run_daily_outside_window(tmp_path, fake_kite):
    # the dump's history has the 9 Oct expiry, so 9 Oct and earlier are refused
    assert run(tmp_path, day=D(9)) == "outside_window"
    assert "Earliest date available is 2026-10-10" in JOB["message"] and JOB["state"] == "stopped"
    assert not (tmp_path / "NIFTY_FnO_1min" / "2026-10-09").exists()
    assert fake_kite.calls == []


def test_run_daily_future_date(tmp_path, fake_kite):
    assert run(tmp_path, day=D(13)) == "outside_window"


def test_run_daily_too_early(tmp_path, fake_kite, monkeypatch):
    set_now(monkeypatch, RUN_DATE, (15, 0))
    assert run(tmp_path) == "too_early"
    assert not (tmp_path / "NIFTY_FnO_1min").exists()


def test_run_daily_estimated_window_logs_warning(tmp_path, fake_kite, monkeypatch):
    # no history before the current expiry: min date from the Tuesday fallback, with a WARNING
    monkeypatch.setattr(FakeKite, "instruments", lambda self, ex: [i for i in make_dump() if i["expiry"] >= RUN_DATE])
    lines = []
    assert run(tmp_path, log=lines.append) == "finished_failures"
    assert any("WARNING" in l and "2026-10-07" in l for l in lines)
    assert "WARNING" in (tmp_path / "NIFTY_FnO_1min" / "2026-10-12" / "run.log").read_text(encoding="utf-8")
    seen = read_rows(tmp_path / "NIFTY_FnO_1min" / fno.EXPIRIES_FILE)
    assert [r[0] for r in seen] == ["2026-10-13", "2026-10-20", "2026-10-27", "2026-11-24"]


def test_run_missing(tmp_path, fake_kite):
    # window 10 Oct (after the 9 Oct expiry in the dump) to 12 Oct; the weekend ends at the holiday check
    assert job_begin(**fno.job_fields("NIFTY", None, str(tmp_path), 1000))
    assert fno.run_missing("key", ACCESS, str(tmp_path), 1000, log=lambda m: None) == "finished_failures"
    base = tmp_path / "NIFTY_FnO_1min"
    assert sorted(d for d in os.listdir(base) if d[0].isdigit()) == ["2026-10-12"]
    assert {"2026-10-10", "2026-10-11"} <= {l.split()[2].rstrip(":") for l in open(base / "holidays.log")}
    assert JOB["state"] == "finished" and "2026-10-12" in JOB["message"]
    # again: weekend days are known holidays and 12 Oct only retries its failed contract
    # (which still fails: pass 1 plus the retry pass)
    fake_kite.calls.clear()
    assert job_begin(**fno.job_fields("NIFTY", None, str(tmp_path), 1000))
    fno.run_missing("key", ACCESS, str(tmp_path), 1000, log=lambda m: None)
    assert [(t, start.date()) for t, start, *_ in fake_kite.calls] == [(31, RUN_DATE), (31, RUN_DATE)]


def test_run_missing_nothing_left(tmp_path, fake_kite, monkeypatch):
    fake_kite.fail_tokens = set()
    dump = [i for i in make_dump() if i["instrument_token"] != 15]   # no bad strike
    monkeypatch.setattr(FakeKite, "instruments", lambda self, ex: dump)
    assert job_begin(**fno.job_fields("NIFTY", None, str(tmp_path), 1000))
    assert fno.run_missing("key", ACCESS, str(tmp_path), 1000, log=lambda m: None) == "finished"
    fake_kite.calls.clear()
    assert job_begin(**fno.job_fields("NIFTY", None, str(tmp_path), 1000))
    assert fno.run_missing("key", ACCESS, str(tmp_path), 1000, log=lambda m: None) == "finished"
    assert fake_kite.calls == [] and JOB["message"].startswith("Nothing missing")



# --- summary counts -----------------------------------------------------------------------------

def test_summary_listed_vs_traded(tmp_path):
    e = D(27)
    contracts = [inst("NIFTY26OCTFUT", "FUT", e, token=1)] +         [inst(f"O{k}{t}", t, e, float(k), token=k + (t == "PE")) for k in (100, 150, 200) for t in ("CE", "PE")]
    files = {}
    for c in contracts:
        name, path = fno.contract_file(str(tmp_path), c, "NIFTY")
        files[c["tradingsymbol"]] = (name, path)
    fno.write_contract(files["NIFTY26OCTFUT"][1], "F", [candle("2026-10-12T09:15:00+0530", c=125)])
    fno.write_contract(files["O100CE"][1], "A", [candle("2026-10-12T09:15:00+0530")])
    fno.write_contract(files["O150PE"][1], "B", [candle("2026-10-12T09:15:00+0530")])
    rows, _ = fno.build_summary(contracts, files, 50)
    r = rows[0]
    assert (r["Listed_strikes"], r["Traded_strikes"], r["Listed_contracts"], r["CE_files"], r["PE_files"]) == (3, 2, 6, 1, 1)
    assert list(r) == fno.SUMMARY_COLUMNS


def test_summary_counts_files_from_earlier_run(tmp_path, fake_kite):
    # a previous partial run already saved the 27 Oct put (which fails in this run): it is counted
    path = tmp_path / "NIFTY_FnO_1min" / "2026-10-12" / "options" / "NIFTY2510027OCT26PE.csv"
    fno.write_contract(str(path), "NIFTY2510027OCT26PE", bars(4, 90.0))
    assert run(tmp_path) == "finished_failures", JOB["message"]
    assert 31 not in [t for t, *_ in fake_kite.calls]          # skipped: its file already exists
    summary = read_csv(tmp_path / "NIFTY_FnO_1min" / "2026-10-12" / "summary.csv")
    assert summary[3] == ["2026-10-27", "Current month", "1", "1", "1", "0", "1", "25100", "", "4"] + PENDING_COLS
    assert JOB["fno_summary"][2]["Traded_strikes"] == 1
    log = (tmp_path / "NIFTY_FnO_1min" / "2026-10-12" / "run.log").read_text(encoding="utf-8")
    assert "Listed_strikes  Traded_strikes  Listed_contracts" in log


# --- zero-volume candles ("keep zero-volume contracts") and --refetch-no-trade ------------------

ZERO_CE = "NIFTY2515013OCT26CE"        # token 13: candles, all with volume 0
NO_CANDLES_PE = "NIFTY2515013OCT26PE"  # token 14: no candles


def day_dir(tmp_path):
    return tmp_path / "NIFTY_FnO_1min" / "2026-10-12"


def reasons(tmp_path):
    db = open_db(str(tmp_path))
    try:
        return dict(db.execute("SELECT symbol, reason FROM chunks WHERE run = ?", (fno.run_key("NIFTY", RUN_DATE),)))
    finally:
        db.close()


def run_keep(tmp_path, keep_zero):
    assert job_begin(**fno.job_fields("NIFTY", RUN_DATE, str(tmp_path), 1000, keep_zero))
    return fno.run_daily("key", ACCESS, RUN_DATE, str(tmp_path), 1000, log=lambda m: None, keep_zero=keep_zero)


def test_zero_volume_off_skips_and_lists(tmp_path, fake_kite):
    run_keep(tmp_path, False)
    day = day_dir(tmp_path)
    assert not (day / "options" / f"{ZERO_CE}.csv").exists()
    assert read_csv(day / fno.ZERO_VOLUME_FILE) == [fno.ZERO_VOLUME_COLUMNS, [ZERO_CE, "2", "81", "1001", "N"]]
    assert JOB["zero_skipped"] == 1 and JOB["zero_kept"] == 0 and JOB["empty"] == 1
    assert reasons(tmp_path)["NIFTY2610625150CE"] == fno.ZERO_VOLUME
    assert reasons(tmp_path)["NIFTY2610625150PE"] == fno.NO_CANDLES
    assert read_csv(day / "summary.csv")[1][-1] == "OFF"


def test_zero_volume_on_keeps_rows_unchanged(tmp_path, fake_kite):
    run_keep(tmp_path, True)
    day = day_dir(tmp_path)
    rows = read_csv(day / "options" / f"{ZERO_CE}.csv")
    assert [r[7] for r in rows[1:]] == ["0", "0"] and [r[6] for r in rows[1:]] == ["80", "81"]
    assert read_csv(day / fno.ZERO_VOLUME_FILE)[1] == [ZERO_CE, "2", "81", "1001", "Y"]
    assert JOB["zero_kept"] == 1 and JOB["zero_skipped"] == 0
    assert reasons(tmp_path)["NIFTY2610625150CE"] is None
    log = (day / "run.log").read_text(encoding="utf-8")
    assert "Keep zero-volume contracts: ON" in log and "Zero-volume candles: kept 1 / skipped 0" in log
    assert read_csv(day / "summary.csv")[1][-1] == "ON"


@pytest.mark.parametrize("keep_zero", [False, True])
def test_no_candles_never_written(tmp_path, fake_kite, keep_zero):
    run_keep(tmp_path, keep_zero)
    assert not (day_dir(tmp_path) / "options" / f"{NO_CANDLES_PE}.csv").exists()
    assert JOB["empty"] == 1
    assert "No candles (no file): 1" in (day_dir(tmp_path) / "run.log").read_text(encoding="utf-8")


def test_keep_zero_rerun_requests_only_zero_volume(tmp_path, fake_kite):
    fake_kite.fail_tokens = set()
    run_keep(tmp_path, False)
    fake_kite.calls.clear()
    run_keep(tmp_path, True)   # ZERO_VOLUME contracts are requested again, NO_CANDLES ones are not
    assert [t for t, *_ in fake_kite.calls] == [13]
    assert (day_dir(tmp_path) / "options" / f"{ZERO_CE}.csv").exists()


def test_refetch_no_trade_requests_only_contracts_without_files(tmp_path, fake_kite):
    fake_kite.fail_tokens = set()
    run_keep(tmp_path, False)
    db = open_db(str(tmp_path))
    db.execute("UPDATE chunks SET reason = NULL")   # a day saved before reasons were stored
    db.commit()
    db.close()
    fake_kite.calls.clear()
    assert job_begin(**fno.job_fields("NIFTY", RUN_DATE, str(tmp_path), 1000, True))
    status, line = fno.refetch_no_trade("key", ACCESS, RUN_DATE, str(tmp_path), 1000, log=lambda m: None)
    assert status == "finished_failures" and line == ""   # the bad strike still fails; no bhavcopy saved
    # every contract without a file (13 zero volume, 14 and 31 no candles), none of those with one
    assert sorted(t for t, *_ in fake_kite.calls) == [13, 14, 31]
    assert (day_dir(tmp_path) / "options" / f"{ZERO_CE}.csv").exists()
    assert not (day_dir(tmp_path) / "options" / f"{NO_CANDLES_PE}.csv").exists()
    assert "re-checking contracts without a file" in (day_dir(tmp_path) / "run.log").read_text(encoding="utf-8")


def test_refetch_no_trade_needs_a_saved_day(tmp_path, fake_kite):
    assert job_begin(**fno.job_fields("NIFTY", RUN_DATE, str(tmp_path), 1000, True))
    assert fno.refetch_no_trade("key", ACCESS, RUN_DATE, str(tmp_path), 1000, log=lambda m: None)[0] == "not_downloaded"
    assert fake_kite.calls == [] and not (tmp_path / "NIFTY_FnO_1min").exists()


def tree(path):
    return sorted(os.path.relpath(os.path.join(d, f), path) for d, _, fs in os.walk(path) for f in fs)


def test_inspect_writes_nothing(tmp_path, fake_kite):
    run_keep(tmp_path, False)
    before = {p: (tmp_path / p).read_bytes() for p in tree(tmp_path)}
    lines = []
    status, rows = fno.inspect_contracts("key", ACCESS, RUN_DATE, str(tmp_path),
                                         [ZERO_CE, NO_CANDLES_PE, "NIFTY99OCT26"], log=lines.append)
    assert status == ""
    assert [(r["Contract"], r["Kite_candles"], r["Kite_volume"], r["Result"]) for r in rows] == [
        (ZERO_CE, 2, "0", "ZERO_VOLUME_CANDLES"), (NO_CANDLES_PE, 0, "0", "NO_CANDLES"),
        ("NIFTY99OCT26", "", "", "NOT_IN_LIVE_LIST")]
    assert any("Candles: 2, total volume: 0" in line for line in lines)
    assert {p: (tmp_path / p).read_bytes() for p in tree(tmp_path)} == before


def test_cli_inspect_writes_nothing(tmp_path, fake_kite, monkeypatch, capsys):
    import nifty_daily
    monkeypatch.setattr(nifty_daily, "load_env", lambda: {"KITE_API_KEY": "k", "KITE_ACCESS_TOKEN": "t"})
    folder = tmp_path / "data"
    assert nifty_daily.main(["--inspect", ZERO_CE, "2026-10-12", "--folder", str(folder)]) == 0
    assert "Candles: 2, total volume: 0" in capsys.readouterr().out
    assert not folder.exists()

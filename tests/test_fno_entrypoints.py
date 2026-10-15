"""The dashboard routes and the CLI around backfill/fno.py. .env is never read or written here."""
import time
from datetime import date, datetime

import pytest

import dashboard
import nifty_daily
from app import app
from backfill import fno
from backfill.job import JOB, JOB_LOCK
from conftest import day_of

AFTER = datetime(2026, 10, 12, 16, 0, tzinfo=fno.IST)
BEFORE = datetime(2026, 10, 12, 15, 0, tzinfo=fno.IST)
# previous expiry 9 Oct, current week expiry 13 Oct, today 12 Oct
WINDOW = fno.Window(date(2026, 10, 10), date(2026, 10, 12), date(2026, 10, 13), date(2026, 10, 9), False)


@pytest.fixture
def ist_now(monkeypatch):
    now = {"t": AFTER}
    monkeypatch.setattr(fno, "now_ist", lambda: now["t"])
    return now


@pytest.fixture
def client(monkeypatch, ist_now):
    monkeypatch.setattr(dashboard, "load_env", lambda: {"KITE_ACCESS_TOKEN": "x"})
    monkeypatch.setattr(dashboard, "credentials", lambda: ("key", ""))
    monkeypatch.setattr(dashboard, "save_env", lambda updates: None)
    monkeypatch.setattr(dashboard, "weekly_expiry_weekday", lambda underlying="NIFTY": 1)
    monkeypatch.setattr(dashboard, "keep_zero_volume", lambda: False)
    monkeypatch.setattr(dashboard, "fno_window", lambda folder, underlying: (WINDOW, ""))
    with JOB_LOCK:
        JOB["state"] = "idle"
    c = app.test_client()
    with c.session_transaction() as s:
        s["access_token"] = "x"
        s["user"] = {"user_id": "AB1234", "user_name": "Demo User", "email": "", "broker": "ZERODHA", "login_time": ""}
    return c


def post(c, folder, **form):
    form = {"folder": str(folder), "speed": "2.5", "date": "2026-10-12", **form}
    r = c.post("/download/fno-start", data={k: v for k, v in form.items() if v is not None})
    assert r.status_code == 302
    return r.headers["Location"]


def wait_for(cond):
    for _ in range(200):
        if cond():
            return True
        time.sleep(0.02)
    return False


def test_success_page_has_fno_card(client):
    html = client.get("/success").get_data(as_text=True)
    assert "/download/fno-start" in html and "/download/fno-window" in html and "FnoCard" in html and "DayRangePicker" in html


# --- /download/fno-window ---------------------------------------------------------------------

def test_window_endpoint(client, tmp_path):
    j = client.get("/download/fno-window", query_string={"folder": str(tmp_path)}).get_json()
    assert j == {"min_date": "2026-10-10", "max_date": "2026-10-12", "current_expiry": "2026-10-13",
                 "previous_expiry": "2026-10-09", "estimated": False, "today_is_expiry": False,
                 "missing": ["2026-10-10", "2026-10-11", "2026-10-12"],
                 "days": {"count": 0, "first": "", "last": "", "with": 0, "no": 0, "old": 0}}


def test_window_endpoint_today_is_expiry(client, tmp_path, ist_now, monkeypatch):
    ist_now["t"] = datetime(2026, 10, 13, 15, 0, tzinfo=fno.IST)
    w = WINDOW._replace(max_date=date(2026, 10, 13))
    monkeypatch.setattr(dashboard, "fno_window", lambda folder, underlying: (w, ""))
    j = client.get("/download/fno-window", query_string={"folder": str(tmp_path)}).get_json()
    assert j["today_is_expiry"] is True
    assert "2026-10-13" not in j["missing"]     # before 15:40


def test_window_endpoint_error(client, tmp_path, monkeypatch):
    monkeypatch.setattr(dashboard, "fno_window", lambda folder, underlying: (None, "Kite session expired. Sign in again."))
    assert client.get("/download/fno-window", query_string={"folder": str(tmp_path)}).get_json() == \
        {"error": "Kite session expired. Sign in again."}


def test_window_endpoint_creates_nothing(client, tmp_path):
    folder = tmp_path / "typed" / "so" / "far"
    client.get("/download/fno-window", query_string={"folder": str(folder)})
    assert not (tmp_path / "typed").exists()


def test_window_endpoint_relative_folder(client):
    assert "full path" in client.get("/download/fno-window", query_string={"folder": "rel"}).get_json()["error"]


def test_fno_window_fetches_instruments_once_a_day(monkeypatch, ist_now, tmp_path):
    calls = []

    class Kite:
        def __init__(self, api_key): pass
        def set_access_token(self, t): pass
        def instruments(self, exchange):
            calls.append(exchange)
            return [{"name": "NIFTY", "instrument_type": "FUT", "expiry": date(2026, 10, 27)},
                    {"name": "NIFTY", "instrument_type": "CE", "expiry": date(2026, 10, 13)}]

    monkeypatch.setattr(dashboard, "KiteConnect", Kite)
    monkeypatch.setattr(dashboard, "credentials", lambda: ("key", ""))
    monkeypatch.setattr(dashboard, "load_env", lambda: {"KITE_ACCESS_TOKEN": "x"})
    monkeypatch.setattr(dashboard, "weekly_expiry_weekday", lambda underlying="NIFTY": 1)
    monkeypatch.setattr(dashboard, "LIVE", {})
    w, err = dashboard.fno_window(str(tmp_path), "NIFTY")
    dashboard.fno_window(str(tmp_path), "NIFTY")
    assert err == "" and calls == ["NFO"]
    assert (w.current_expiry, w.previous_expiry, w.min_date, w.estimated) == \
        (date(2026, 10, 13), date(2026, 10, 6), date(2026, 10, 7), True)


# --- /download/fno-start ----------------------------------------------------------------------

def test_fno_start_too_early(client, ist_now, tmp_path):
    ist_now["t"] = BEFORE
    assert "Too+early" in post(client, tmp_path)


def test_fno_start_needs_date(client, tmp_path):
    assert "Choose+the+date" in post(client, tmp_path, date=None)
    assert "Choose+the+date" in post(client, tmp_path, date="")


def test_fno_start_before_window(client, tmp_path):
    loc = post(client, tmp_path, date="2026-10-09")
    assert "Earliest+date+available+is+2026-10-10" in loc and "expired+on+2026-10-09" in loc


def test_fno_start_future_date(client, tmp_path):
    assert "future" in post(client, tmp_path, date="2026-10-13")


def test_fno_start_window_error(client, tmp_path, monkeypatch):
    monkeypatch.setattr(dashboard, "fno_window", lambda folder, underlying: (None, "Kite session expired. Sign in again."))
    assert "session+expired" in post(client, tmp_path)


def test_fno_start_bad_folder(client):
    assert "full+path" in post(client, "relative/folder")


def test_fno_start_bad_speed(client, tmp_path):
    assert "Speed" in post(client, tmp_path, speed="5")


def test_fno_start_while_running(client, tmp_path):
    with JOB_LOCK:
        JOB["state"] = "running"
    try:
        assert "already+running" in post(client, tmp_path)
    finally:
        with JOB_LOCK:
            JOB["state"] = "idle"
    fno.acquire_lock(str(tmp_path), "NIFTY", "x").release()   # the lock was given back


def test_fno_start_lock_held_by_cli(client, tmp_path):
    lock = fno.acquire_lock(str(tmp_path), "NIFTY", "nifty_daily.py")
    try:
        assert "nifty_daily.py" in post(client, tmp_path)
        assert JOB["state"] == "idle"
    finally:
        lock.release()


def lock_free(folder):
    try:
        fno.acquire_lock(str(folder), "NIFTY", "x").release()
        return True
    except fno.LockBusy:
        return False


@pytest.mark.parametrize("picked", [date(2026, 10, 12), date(2026, 10, 10)])
def test_fno_start_runs_picked_date(client, tmp_path, monkeypatch, picked):
    seen = {}

    def fake_run(api_key, token, run_date, folder, speed, underlying, weekday, keep_zero=False, trim_to_bhav=None):
        seen.update(run_date=run_date, folder=folder, speed=speed, underlying=underlying, weekday=weekday)
        with JOB_LOCK:
            JOB["state"] = "finished"

    monkeypatch.setattr(fno, "run_daily", fake_run)
    assert "error" not in post(client, tmp_path, date=picked.isoformat())
    assert wait_for(lambda: seen)
    assert seen == {"run_date": picked, "folder": str(tmp_path), "speed": 2.5, "underlying": "NIFTY", "weekday": 1}
    assert JOB["kind"] == "fno" and JOB["fno_date"] == picked.isoformat()
    assert wait_for(lambda: lock_free(tmp_path)), "lock not released"


def test_fno_start_missing(client, tmp_path, monkeypatch):
    seen = {}

    def fake_missing(api_key, token, folder, speed, underlying, weekday, keep_zero=False, trim_to_bhav=None):
        seen.update(folder=folder, weekday=weekday)
        with JOB_LOCK:
            JOB["state"] = "finished"

    monkeypatch.setattr(fno, "run_missing", fake_missing)
    assert "error" not in post(client, tmp_path, date=None, missing="1")
    assert wait_for(lambda: seen) and seen == {"folder": str(tmp_path), "weekday": 1}
    assert JOB["fno_date"] == "" and JOB["run"] == "fno:NIFTY:NFO:missing"
    assert wait_for(lambda: lock_free(tmp_path)), "lock not released"


def test_fno_start_missing_nothing_left(client, tmp_path, monkeypatch):
    monkeypatch.setattr(fno, "missing_dates", lambda *a, **kw: [])
    assert "Nothing+missing" in post(client, tmp_path, date=None, missing="1")


# --- date range (date_from / date_to) ----------------------------------------------------------

def test_parse_pick():
    assert fno.parse_pick("2026-10-12") == (date(2026, 10, 12), None)
    assert fno.parse_pick("", "2026-10-12", "2026-10-12") == (date(2026, 10, 12), None)
    assert fno.parse_pick("2026-10-01", "2026-10-10", "2026-10-12") == \
        (None, [date(2026, 10, 10), date(2026, 10, 11), date(2026, 10, 12)])
    for args in [("",), ("", "2026-10-12", ""), ("", "2026-10-12", "2026-10-10")]:
        with pytest.raises(ValueError):
            fno.parse_pick(*args)


def test_fno_start_runs_range(client, tmp_path, monkeypatch):
    seen = {}

    def fake_missing(api_key, token, folder, speed, underlying, weekday, keep_zero=False, dates=None, trim_to_bhav=None):
        seen.update(folder=folder, dates=dates, keep_zero=keep_zero)
        with JOB_LOCK:
            JOB["state"] = "finished"

    monkeypatch.setattr(fno, "run_missing", fake_missing)
    assert "error" not in post(client, tmp_path, date=None, date_from="2026-10-10", date_to="2026-10-12", keep_zero="1")
    assert wait_for(lambda: seen)
    assert seen == {"folder": str(tmp_path), "keep_zero": True,
                    "dates": [date(2026, 10, 10), date(2026, 10, 11), date(2026, 10, 12)]}
    assert JOB["fno_date"] == "" and JOB["run"] == "fno:NIFTY:NFO:2026-10-10_to_2026-10-12"
    assert wait_for(lambda: lock_free(tmp_path)), "lock not released"


def test_fno_start_same_day_range_is_one_day(client, tmp_path, monkeypatch):
    seen = {}
    monkeypatch.setattr(fno, "run_daily", lambda api_key, token, run_date, *a, **kw: seen.update(run_date=run_date))
    monkeypatch.setattr(fno, "run_missing", lambda *a, **kw: seen.update(missing=True))
    post(client, tmp_path, date=None, date_from="2026-10-11", date_to="2026-10-11")
    assert wait_for(lambda: seen) and seen == {"run_date": date(2026, 10, 11)}
    with JOB_LOCK:
        JOB["state"] = "idle"


@pytest.mark.parametrize("form,msg", [
    ({"date_from": "2026-10-12", "date_to": "2026-10-10"}, "before+the+first"),
    ({"date_from": "2026-10-10", "date_to": ""}, "first+and+last+day"),
    ({"date_from": "2026-10-09", "date_to": "2026-10-12"}, "Earliest+date+available"),
    ({"date_from": "2026-10-10", "date_to": "2026-10-13"}, "future"),
    ({"date_from": "2026-10-10", "date_to": "2026-10-12", "refetch": "1", "keep_zero": "1"}, "single+date"),
])
def test_fno_start_range_refused(client, tmp_path, monkeypatch, form, msg):
    monkeypatch.setattr(fno, "run_missing", lambda *a, **kw: pytest.fail("must not start"))
    assert msg in post(client, tmp_path, date=None, **form)


def test_fno_start_range_ending_today_too_early(client, tmp_path, ist_now):
    ist_now["t"] = BEFORE
    assert "Too+early" in post(client, tmp_path, date=None, date_from="2026-10-10", date_to="2026-10-12")


# --- CLI --------------------------------------------------------------------------------------

@pytest.fixture
def cli(monkeypatch, ist_now):
    calls = []
    monkeypatch.setattr(nifty_daily, "load_env", lambda: {"KITE_API_KEY": "k", "KITE_ACCESS_TOKEN": "t"})
    monkeypatch.setattr(nifty_daily, "weekly_expiry_weekday", lambda underlying="NIFTY": 1)
    monkeypatch.setattr(nifty_daily, "keep_zero_volume", lambda: False)
    return calls


def set_status(monkeypatch, status, calls=None):
    def fake(name):
        def run(*a, **kw):
            if calls is not None:
                calls.append(name)
            with JOB_LOCK:
                JOB["state"] = "finished"
            return status
        return run
    monkeypatch.setattr(nifty_daily, "run_daily", fake("daily"))
    monkeypatch.setattr(nifty_daily, "run_missing", fake("missing"))


@pytest.mark.parametrize("status,code", [("finished", 0), ("holiday", 0), ("finished_failures", 1),
                                         ("stopped", 1), ("error", 1), ("too_early", 1),
                                         ("auth_failed", 2), ("outside_window", 3)])
def test_cli_exit_codes(cli, monkeypatch, tmp_path, status, code):
    set_status(monkeypatch, status)
    assert nifty_daily.main(["2026-10-12", "--folder", str(tmp_path)]) == code


def test_cli_missing(cli, monkeypatch, tmp_path):
    set_status(monkeypatch, "finished", cli)
    assert nifty_daily.main(["--missing", "--folder", str(tmp_path)]) == 0
    assert cli == ["missing"]


@pytest.mark.parametrize("argv", [[], ["2026-10-12", "--missing"], ["12-10-2026"]])
def test_cli_needs_one_date_or_missing(cli, monkeypatch, tmp_path, argv):
    set_status(monkeypatch, "finished", cli)
    with pytest.raises(SystemExit) as e:
        nifty_daily.main(argv + ["--folder", str(tmp_path)])
    assert e.value.code == 1 and cli == []


def test_cli_missing_token(cli, monkeypatch, tmp_path):
    monkeypatch.setattr(nifty_daily, "load_env", lambda: {"KITE_API_KEY": "k"})
    assert nifty_daily.main(["2026-10-12", "--folder", str(tmp_path)]) == 2


def test_cli_lock_busy(cli, monkeypatch, tmp_path):
    set_status(monkeypatch, "finished")
    lock = fno.acquire_lock(str(tmp_path), "NIFTY", "dashboard")
    try:
        assert nifty_daily.main(["2026-10-12", "--folder", str(tmp_path)]) == 1
    finally:
        lock.release()




# --- keep zero-volume contracts and re-checking no-trade contracts -------------------------------

def test_success_page_has_keep_zero_switch(client):
    html = client.get("/success").get_data(as_text=True)
    assert "Keep contracts with candles but zero volume" in html and '"keep_zero_volume": false' in html


def test_fno_start_passes_keep_zero(client, tmp_path, monkeypatch):
    seen = {}

    def fake_run(*a, keep_zero=False, **kw):
        seen["keep_zero"] = keep_zero
        with JOB_LOCK:
            JOB["state"] = "finished"

    monkeypatch.setattr(fno, "run_daily", fake_run)
    assert "error" not in post(client, tmp_path, keep_zero="1")
    assert wait_for(lambda: seen) and seen == {"keep_zero": True} and JOB["keep_zero"] is True
    assert wait_for(lambda: lock_free(tmp_path)), "lock not released"


def test_fno_refetch_needs_keep_zero(client, tmp_path):
    assert "Turn+on" in post(client, tmp_path, refetch="1", keep_zero="0")


def test_fno_refetch_needs_saved_day(client, tmp_path):
    assert "not+been+downloaded" in post(client, tmp_path, refetch="1", keep_zero="1")


def test_fno_refetch_runs(client, tmp_path, monkeypatch):
    day = day_of(tmp_path / "NIFTY_FnO_1min", "2026-10-12")
    day.mkdir(parents=True)
    (day / "summary.csv").write_text("Expiry\n")
    seen = {}

    def fake_refetch(api_key, token, run_date, folder, speed, underlying, weekday, trim_to_bhav=None):
        seen.update(run_date=run_date, folder=folder)
        with JOB_LOCK:
            JOB["state"] = "finished"
        return "finished", ""

    monkeypatch.setattr(fno, "refetch_no_trade", fake_refetch)
    assert "error" not in post(client, tmp_path, refetch="1", keep_zero="1")
    assert wait_for(lambda: seen) and seen == {"run_date": date(2026, 10, 12), "folder": str(tmp_path)}
    assert wait_for(lambda: lock_free(tmp_path)), "lock not released"


def test_cli_keep_zero_flags(cli, monkeypatch, tmp_path):
    seen = []

    def fake_run(*a, keep_zero=False, **kw):
        seen.append(keep_zero)
        with JOB_LOCK:
            JOB["state"] = "finished"
        return "finished"

    monkeypatch.setattr(nifty_daily, "run_daily", fake_run)
    assert nifty_daily.main(["2026-10-12", "--folder", str(tmp_path)]) == 0                       # config default
    assert nifty_daily.main(["2026-10-12", "--folder", str(tmp_path), "--keep-zero-volume"]) == 0
    monkeypatch.setattr(nifty_daily, "keep_zero_volume", lambda: True)
    assert nifty_daily.main(["2026-10-12", "--folder", str(tmp_path), "--no-keep-zero-volume"]) == 0
    assert seen == [False, True, False]


def test_cli_refetch_no_trade(cli, monkeypatch, tmp_path):
    seen = []

    def fake_refetch(api_key, token, run_date, folder, speed, underlying, weekday, trim_to_bhav=None):
        seen.append((run_date, JOB["keep_zero"]))
        with JOB_LOCK:
            JOB["state"] = "finished"
        return "finished", "Bhavcopy check: OK"

    monkeypatch.setattr(nifty_daily, "refetch_no_trade", fake_refetch)
    assert nifty_daily.main(["--refetch-no-trade", "2026-10-07", "--folder", str(tmp_path)]) == 0
    assert seen == [(date(2026, 10, 7), True)]
    with pytest.raises(SystemExit) as e:
        nifty_daily.main(["--refetch-no-trade", "2026-10-07", "--no-keep-zero-volume", "--folder", str(tmp_path)])
    assert e.value.code == 1

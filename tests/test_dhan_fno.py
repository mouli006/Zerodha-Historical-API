"""The Dhan F&O page (dhan.py) and fno.run_daily on Dhan (source="dhan"). No network, no real .env."""
import os
import sqlite3
import time
from datetime import date, datetime
from urllib.parse import parse_qs, urlparse

import pytest

import dashboard
import dhan
from app import app
from backfill import bhav, dhan_api, fno
from backfill.job import JOB, JOB_LOCK, STOP, job_begin
from conftest import day_of

IST = fno.IST
TOKEN = "SECRET-DHAN-TOKEN"
RUN_DATE = date(2026, 10, 12)
CSV = """SECURITY_ID,INSTRUMENT,UNDERLYING_SYMBOL,SYMBOL_NAME,LOT_SIZE,SM_EXPIRY_DATE,STRIKE_PRICE,OPTION_TYPE
35001,FUTIDX,NIFTY,NIFTY-Oct2026-FUT,75,2026-10-27,-0.01,XX
40001,OPTIDX,NIFTY,NIFTY-Oct2026-25100-CE,75,2026-10-13,25100,CE
40002,OPTIDX,NIFTY,NIFTY-Oct2026-25100-PE,75,2026-10-13,25100,PE
60001,OPTIDX,BANKNIFTY,BANKNIFTY-Oct2026-56000-CE,35,2026-10-27,56000,CE
"""


def arrays(n, close, vol=100):
    t0 = int(datetime(2026, 10, 12, 9, 15, tzinfo=IST).timestamp())
    return {"open": [close] * n, "high": [close + 5] * n, "low": [close - 5] * n, "close": [close + k for k in range(n)],
            "volume": [vol] * n, "timestamp": [t0 + 60 * k for k in range(n)], "open_interest": [1000 + k for k in range(n)]}


@pytest.fixture(autouse=True)
def dhan_trim(trim_on):
    """These Dhan runs keep the trim setting ON (its default; conftest turns it OFF for other tests)."""


@pytest.fixture(autouse=True)
def no_nse(monkeypatch):
    """No NSE calls here: by default the bhavcopy is "not published", so Dhan runs keep the full list."""
    calls = []

    def fetch(run_date, log=print, session=None, budget=None, **kw):
        calls.append((run_date, budget))
        raise bhav.BhavNotAvailable("NSE has no F&O bhavcopy for this day yet (404: test).")
    monkeypatch.setattr(bhav, "fetch_bhav", fetch)
    return calls


class Resp:
    def __init__(self, status, data=None, text=""):
        self.status_code, self._data, self.text = status, data, text

    def json(self):
        return self._data


@pytest.fixture
def dhan_http(monkeypatch):
    requested = []
    replies = {"35001": Resp(200, arrays(3, 25120.0)), "40001": Resp(200, arrays(2, 120.0)),
               "40002": Resp(400, {"errorCode": "DH-907", "errorMessage": "No data present"})}

    def get(url, headers=None, timeout=None):
        assert url == dhan_api.INSTRUMENTS_URL
        return Resp(200, text=CSV)

    def post(url, json=None, headers=None, timeout=None):
        requested.append(json["securityId"])
        return replies[json["securityId"]]

    monkeypatch.setattr(dhan_api.requests, "get", get)
    monkeypatch.setattr(dhan_api.requests, "post", post)
    monkeypatch.setattr(fno, "now_ist", lambda: datetime(2026, 10, 12, 16, 0, tzinfo=IST))
    STOP.clear()
    with JOB_LOCK:
        JOB["state"] = "idle"
    return requested


def test_run_daily_on_dhan(tmp_path, dhan_http):
    root = str(tmp_path)
    assert job_begin(**fno.job_fields("NIFTY", RUN_DATE, root, 1000, source="dhan"))
    assert JOB["source"] == "dhan" and JOB["out_dirs"] == [os.path.join("NIFTY_FnO_1min_Dhan", "2026-10-12")]
    st = fno.run_daily("", TOKEN, RUN_DATE, root, 1000, kite=dhan_api.DhanClient(TOKEN), user="1000000001",
                       log=lambda m: None, source="dhan")
    assert st == "finished"
    day = day_of(tmp_path / "NIFTY_FnO_1min_Dhan", "2026-10-12")
    assert day.name == "2026-10-12_exp-2026-10-13_noZeroVol_dl-2026-10-12_1600"  # expiry from Dhan's list
    assert JOB["out_dirs"] == [os.path.join("NIFTY_FnO_1min_Dhan", day.name)]
    assert os.listdir(day / "futures") == ["NIFTY27OCT26.csv"]
    assert os.listdir(day / "options") == ["NIFTY2510013OCT26CE.csv"]          # the PE had no data
    assert (day / "instruments_NSE_FNO.csv").exists() and (day / "summary.csv").exists()
    assert not (tmp_path / "NIFTY_FnO_1min").exists()                          # Kite folder untouched
    assert "60001" not in dhan_http                                            # other underlyings skipped
    rows = (day / "futures" / "NIFTY27OCT26.csv").read_text(encoding="utf-8").splitlines()
    assert rows[0] == ",".join(fno.HEADER)
    assert rows[1] == "NIFTY27OCT26,20261012,09:15:00,25120,25125,25115,25120,100,1000"
    db = sqlite3.connect(tmp_path / "progress.sqlite")
    runs = {r for (r,) in db.execute("SELECT DISTINCT run FROM chunks")}
    db.close()
    assert runs == {"fno-dhan:NIFTY:NSE_FNO:2026-10-12"}
    log = (day / "run.log").read_text(encoding="utf-8")
    assert "Dhan user 1000000001" in log and TOKEN not in log
    assert JOB["empty"] == 1 and JOB["state"] == "finished"
    # Dhan's missing-days list reads the Dhan folder and keys
    w = fno.Window(date(2026, 10, 10), RUN_DATE, date(2026, 10, 13), date(2026, 10, 9), False)
    assert RUN_DATE not in fno.missing_dates(root, "NIFTY", w, source="dhan")
    assert RUN_DATE in fno.missing_dates(root, "NIFTY", w)


# --- routes -------------------------------------------------------------------------------------

WINDOW = fno.Window(date(2026, 10, 10), RUN_DATE, date(2026, 10, 13), date(2026, 10, 9), False)


class FakeClient:
    plan = "Active"

    def __init__(self, token):
        self.token = token

    def profile(self):
        return {"user_id": "1000000001", "data_plan": FakeClient.plan}


@pytest.fixture
def client(monkeypatch, tmp_path):
    env = {"DHAN_CLIENT_ID": "1000000001", "DHAN_ACCESS_TOKEN": TOKEN, "DHAN_TOKEN_EXPIRY": "2099-01-01T09:00:00"}
    calls = {"run": [], "env": env, "weekday": []}
    monkeypatch.setattr(dhan, "load_env", lambda: dict(env))
    monkeypatch.setattr(dashboard, "load_env", lambda: dict(env))   # the /fno/* routes check the Dhan token too
    monkeypatch.setattr(dhan, "save_env", lambda updates: None)
    monkeypatch.setattr(dhan, "dhan_credentials", lambda: (env.get("DHAN_CLIENT_ID", ""), env.get("DHAN_ACCESS_TOKEN", "")))
    monkeypatch.setattr(dhan, "keep_zero_volume", lambda: False)
    monkeypatch.setattr(dhan, "data_dir", lambda: str(tmp_path))
    monkeypatch.setattr(dhan, "weekly_expiry_weekday", lambda underlying: calls["weekday"].append(underlying) or 1)
    monkeypatch.setattr(dhan, "fno_window", lambda folder, underlying, token: (WINDOW, ""))
    monkeypatch.setattr(dhan, "DhanClient", FakeClient)
    monkeypatch.setattr(fno, "now_ist", lambda: datetime(2026, 10, 12, 16, 0, tzinfo=IST))
    monkeypatch.setattr(fno, "run_daily", lambda *a, **k: calls["run"].append(("daily", a, k)) or "finished")
    monkeypatch.setattr(fno, "run_missing", lambda *a, **k: calls["run"].append(("missing", a, k)) or "finished")
    FakeClient.plan = "Active"
    with JOB_LOCK:
        JOB["state"] = "idle"
    yield app.test_client(), calls
    with JOB_LOCK:   # the stubbed runs never finish the job
        JOB["state"] = "idle"


def error_of(r):
    assert r.status_code == 302
    return parse_qs(urlparse(r.headers["Location"]).query).get("error", [""])[0]


def wait_for(cond):
    for _ in range(200):
        if cond():
            return True
        time.sleep(0.02)
    return False


def test_dashboard_needs_dhan(client):
    c, calls = client
    calls["env"].pop("DHAN_ACCESS_TOKEN")
    r = c.get("/dhan/dashboard")
    assert r.headers["Location"].startswith("/dhan?") and "Connect Dhan" in error_of(r)


def test_dashboard_renders_dhan_mode(client):
    c, _ = client
    html = c.get("/dhan/dashboard").get_data(as_text=True)
    assert '"mode": "dhan"' in html and "/dhan/fno-start" in html and "/dhan/fno-window" in html
    assert '"maxSpeed": 5.0' in html and TOKEN not in html
    # a visible Log out, and the bhavcopy check like the Kite page
    assert "/dhan/logout" in html and ">Log out<" in html and "/fno/bhav-status" in html and "BhavCard" in html
    # the F&O card is the Kite card: same title and texts, broker name from boot.broker
    assert "F&O daily</h2>" in html and "\"1-minute\" : \"daily\"" not in html and "${B.name} can report a single trade" in html


def test_window_route(client, tmp_path):
    c, _ = client
    j = c.get("/dhan/fno-window", query_string={"folder": str(tmp_path)}).get_json()
    assert j["min_date"] == "2026-10-10" and j["max_date"] == "2026-10-12" and j["current_expiry"] == "2026-10-13"
    assert j["missing"] == ["2026-10-10", "2026-10-11", "2026-10-12"]   # the weekend is listed until a run logs it
    assert "error" in c.get("/dhan/fno-window", query_string={"folder": "relative"}).get_json()


@pytest.mark.parametrize("form,msg", [
    ({"date": ""}, "Choose the date"),
    ({"speed": "6"}, "between 0 and 5"),
    ({"folder": "relative/path"}, "full path"),
    ({"date": "2026-10-08"}, "Dhan"),                         # before the window: Kite wording replaced
])
def test_start_is_refused(client, tmp_path, form, msg):
    c, calls = client
    data = {"date": "2026-10-12", "speed": "4", "folder": str(tmp_path), **form}
    assert msg in error_of(c.post("/dhan/fno-start", data=data))
    assert calls["run"] == []


def test_start_refused_without_data_plan(client, tmp_path):
    c, calls = client
    FakeClient.plan = "Deactive"
    assert "Data API" in error_of(c.post("/dhan/fno-start", data={"date": "2026-10-12", "folder": str(tmp_path)}))
    assert calls["run"] == []


def test_start_runs_on_dhan(client, tmp_path):
    c, calls = client
    r = c.post("/dhan/fno-start", data={"date": "2026-10-12", "speed": "4.5", "folder": str(tmp_path), "keep_zero": "1"})
    assert r.headers["Location"] == "/dhan/dashboard"
    assert wait_for(lambda: calls["run"])
    kind, args, kw = calls["run"][0]
    assert kind == "daily" and args[2] == RUN_DATE and args[4] == 4.5
    assert kw["source"] == "dhan" and isinstance(kw["kite"], FakeClient) and kw["keep_zero"] is True
    assert kw["weekday"] == 1 and calls["weekday"] == ["NIFTY"]   # per underlying, as on Kite
    assert JOB["source"] == "dhan" and JOB["run"] == "fno-dhan:NIFTY:NSE_FNO:2026-10-12"
    # the lock was released when the (stubbed) run ended
    assert wait_for(lambda: _lock_free(str(tmp_path)))


def test_start_missing_days(client, tmp_path):
    c, calls = client
    c.post("/dhan/fno-start", data={"missing": "1", "folder": str(tmp_path)})
    assert wait_for(lambda: calls["run"])
    kind, _, kw = calls["run"][0]
    assert kind == "missing" and kw["source"] == "dhan" and kw["user"] == "1000000001"


def test_start_range_on_dhan(client, tmp_path):
    c, calls = client
    assert error_of(c.post("/dhan/fno-start", data={"date_from": "2026-10-11", "date_to": "2026-10-12",
                                                    "folder": str(tmp_path)})) == ""
    assert wait_for(lambda: calls["run"])
    kind, _, kw = calls["run"][0]
    assert kind == "missing" and kw["source"] == "dhan" and kw["dates"] == [date(2026, 10, 11), RUN_DATE]
    assert JOB["run"] == "fno-dhan:NIFTY:NSE_FNO:2026-10-11_to_2026-10-12"
    assert "Dhan" in error_of(c.post("/dhan/fno-start", data={"date_from": "2026-10-08", "date_to": "2026-10-12",
                                                              "folder": str(tmp_path)}))


def _lock_free(root):
    try:
        fno.acquire_lock(root, "NIFTY", "test", "dhan").release()
        return True
    except fno.LockBusy:
        return False


def test_weekly_and_monthly_with_the_same_dhan_symbol_get_their_own_files(tmp_path, monkeypatch):
    """Dhan's SYMBOL_NAME is the same for every expiry of a month; each contract must still be saved
    under its own expiry (2026-10-07: weeklies were written into the monthly file)."""
    csv_text = """SECURITY_ID,INSTRUMENT,UNDERLYING_SYMBOL,SYMBOL_NAME,LOT_SIZE,SM_EXPIRY_DATE,STRIKE_PRICE,OPTION_TYPE
35001,FUTIDX,NIFTY,NIFTY-Oct2026-FUT,75,2026-10-27,-0.01,XX
40001,OPTIDX,NIFTY,NIFTY-Oct2026-25000-CE,75,2026-10-13,25000,CE
40011,OPTIDX,NIFTY,NIFTY-Oct2026-25000-CE,75,2026-10-19,25000,CE
40021,OPTIDX,NIFTY,NIFTY-Oct2026-25000-CE,75,2026-10-27,25000,CE
"""
    closes = {"35001": 25120.0, "40001": 13.0, "40011": 19.0, "40021": 27.0}
    monkeypatch.setattr(dhan_api.requests, "get", lambda url, headers=None, timeout=None: Resp(200, text=csv_text))
    monkeypatch.setattr(dhan_api.requests, "post", lambda url, json=None, headers=None, timeout=None:
                        Resp(200, arrays(2, closes[json["securityId"]])))
    monkeypatch.setattr(fno, "now_ist", lambda: datetime(2026, 10, 12, 16, 0, tzinfo=IST))
    STOP.clear()
    with JOB_LOCK:
        JOB["state"] = "idle"
    root = str(tmp_path)
    assert job_begin(**fno.job_fields("NIFTY", RUN_DATE, root, 1000, source="dhan"))
    assert fno.run_daily("", TOKEN, RUN_DATE, root, 1000, kite=dhan_api.DhanClient(TOKEN), log=lambda m: None,
                         source="dhan") == "finished"
    opt = day_of(tmp_path / "NIFTY_FnO_1min_Dhan", "2026-10-12") / "options"
    assert sorted(os.listdir(opt)) == ["NIFTY2500013OCT26CE.csv", "NIFTY2500019OCT26CE.csv", "NIFTY2500027OCT26CE.csv"]
    for name, close in (("13OCT26", "13"), ("19OCT26", "19"), ("27OCT26", "27")):
        rows = (opt / f"NIFTY25000{name}CE.csv").read_text(encoding="utf-8").splitlines()
        assert rows[1].split(",")[0] == f"NIFTY25000{name}CE" and rows[1].split(",")[6] == close
    db = sqlite3.connect(tmp_path / "progress.sqlite")
    rows = db.execute("SELECT symbol, status FROM chunks WHERE run = ?", ("fno-dhan:NIFTY:NSE_FNO:2026-10-12",)).fetchall()
    db.close()
    assert len(rows) == 4 and all(st == "DONE" for _, st in rows)



# --- trimming Dhan's list to the contracts in NSE's bhavcopy --------------------------------------

TRIM_CSV = """SECURITY_ID,INSTRUMENT,UNDERLYING_SYMBOL,SYMBOL_NAME,LOT_SIZE,SM_EXPIRY_DATE,STRIKE_PRICE,OPTION_TYPE
35001,FUTIDX,NIFTY,NIFTY-Oct2026-FUT,75,2026-10-27,-0.01,XX
40001,OPTIDX,NIFTY,NIFTY-Oct2026-25000-CE,75,2026-10-13,25000,CE
40002,OPTIDX,NIFTY,NIFTY-Oct2026-25000-PE,75,2026-10-13,25000,PE
49999,OPTIDX,NIFTY,NIFTY-Oct2026-17000-CE,75,2026-10-13,17000,CE
"""


def bhav_rows():
    from test_bhav import row
    # the PE did not trade that day: it is still listed, so it must still be downloaded
    return [row("IDF", "2026-10-27", vol=3, last=25120, oi=1000), row("IDO", "2026-10-13", "25000.00", "CE", vol=2, last=10),
            row("IDO", "2026-10-13", "25000.00", "PE", vol=0)]


def trim_run(tmp_path, monkeypatch):
    requested = []
    monkeypatch.setattr(dhan_api.requests, "get", lambda url, headers=None, timeout=None: Resp(200, text=TRIM_CSV))

    def post(url, json=None, headers=None, timeout=None):
        requested.append(json["securityId"])
        return Resp(200, arrays(2, 100.0))
    monkeypatch.setattr(dhan_api.requests, "post", post)
    monkeypatch.setattr(fno, "now_ist", lambda: datetime(2026, 10, 12, 16, 0, tzinfo=IST))
    STOP.clear()
    with JOB_LOCK:
        JOB["state"] = "idle"
    root = str(tmp_path)
    assert job_begin(**fno.job_fields("NIFTY", RUN_DATE, root, 1000, source="dhan"))
    st = fno.run_daily("", TOKEN, RUN_DATE, root, 1000, kite=dhan_api.DhanClient(TOKEN), log=lambda m: None, source="dhan")
    day = day_of(tmp_path / "NIFTY_FnO_1min_Dhan", "2026-10-12")
    return st, requested, day, (day / "run.log").read_text(encoding="utf-8")


def test_trim_keeps_every_bhavcopy_contract_traded_or_not(tmp_path, monkeypatch, no_nse):
    from test_bhav import zip_bytes
    monkeypatch.setattr(bhav, "fetch_bhav", lambda run_date, log=print, budget=None, **kw:
                        no_nse.append((run_date, budget)) or (zip_bytes(bhav_rows()), "https://x/BhavCopy_NSE_FO_test.csv.zip"))
    st, requested, day, log = trim_run(tmp_path, monkeypatch)
    assert st == "finished" and no_nse == [(RUN_DATE, bhav.TRIM_BUDGET)]
    assert sorted(requested) == ["35001", "40001", "40002"]          # the zero-trade PE is kept, 17000 CE is not
    assert "Trimmed to 3 contracts listed in NSE's bhavcopy for 2026-10-12 (Dhan listed 4; 1 not listed by NSE" in log
    assert (day / "bhavcopy" / "nifty_bhav.csv").exists() and (day / "bhavcopy" / "BhavCopy_NSE_FO_test.csv.zip").exists()


def test_no_bhavcopy_downloads_the_full_list(tmp_path, monkeypatch, no_nse):
    st, requested, _, log = trim_run(tmp_path, monkeypatch)
    assert st == "finished" and len(requested) == 4
    assert "Bhavcopy for 2026-10-12 not available (NSE has no F&O bhavcopy" in log and "downloading all 4 contracts Dhan lists" in log


def test_bhavcopy_timeout_downloads_the_full_list(tmp_path, monkeypatch):
    def slow(run_date, log=print, budget=None, **kw):
        raise bhav.BhavTimeout(f"bhavcopy fetch timed out after {budget} s")
    monkeypatch.setattr(bhav, "fetch_bhav", slow)
    st, requested, _, log = trim_run(tmp_path, monkeypatch)
    assert st == "finished" and len(requested) == 4 and "(bhavcopy fetch timed out)" in log


def test_saved_bhavcopy_is_used_without_network(tmp_path, monkeypatch, no_nse):
    day = day_of(tmp_path / "NIFTY_FnO_1min_Dhan", "2026-10-12") / "bhavcopy"
    day.mkdir(parents=True)
    fno.write_csv(str(day / "nifty_bhav.csv"), bhav.BHAV_HEADER, bhav_rows())
    st, requested, _, log = trim_run(tmp_path, monkeypatch)
    assert no_nse == [] and sorted(requested) == ["35001", "40001", "40002"] and "Trimmed to 3" in log


def test_dhan_underlyings(client, tmp_path):
    """NIFTY and SENSEX on Dhan; anything else is refused."""
    c, calls = client
    data = {"date": "2026-10-12", "folder": str(tmp_path), "underlying": "BANKEX"}
    assert "Unknown underlying BANKEX" in error_of(c.post("/dhan/fno-start", data=data))
    assert calls["run"] == []
    j = c.get("/fno/bhav-status", query_string={"folder": str(tmp_path), "underlying": "BANKEX", "source": "dhan"}).get_json()
    assert "Unknown underlying BANKEX" in j["error"]
    j = c.get("/fno/bhav-status", query_string={"folder": str(tmp_path), "underlying": "SENSEX", "source": "dhan"}).get_json()
    assert j["days"] == []


# --- "Download only contracts in the bhavcopy" on the Dhan page -----------------------------------

@pytest.mark.parametrize("form,expected", [({"trim_bhav": "1"}, True), ({"trim_bhav": "0"}, False), ({}, True)])
@pytest.mark.parametrize("mode", ["date", "range", "missing", "refetch"])
def test_dhan_start_passes_trim(client, tmp_path, monkeypatch, form, expected, mode):
    c, calls = client   # the trim setting is ON here (dhan_trim)
    monkeypatch.setattr(fno, "refetch_no_trade", lambda *a, **k: calls["run"].append(("refetch", a, k)) or ("finished", ""))
    extra = {"date": {"date": "2026-10-12"}, "range": {"date_from": "2026-10-11", "date_to": "2026-10-12"},
             "missing": {"missing": "1"}, "refetch": {"date": "2026-10-12", "refetch": "1", "keep_zero": "1"}}[mode]
    if mode == "refetch":
        d = tmp_path / "NIFTY_FnO_1min_Dhan" / "2026-10-12"
        d.mkdir(parents=True)
        (d / "summary.csv").write_text("Expiry\n")
    assert error_of(c.post("/dhan/fno-start", data={"folder": str(tmp_path), **extra, **form})) == ""
    assert wait_for(lambda: calls["run"])
    _, _, kw = calls["run"][0]
    assert kw["trim_to_bhav"] is expected and JOB["trim_to_bhav"] is expected


def test_dhan_page_has_trim_switch(client, monkeypatch):
    c, _ = client
    html = c.get("/dhan/dashboard").get_data(as_text=True)
    assert "Download only contracts in the bhavcopy" in html and '"trim_to_bhav": true' in html
    monkeypatch.setattr(dhan, "trim_to_bhav", lambda: False)
    assert '"trim_to_bhav": false' in c.get("/dhan/dashboard").get_data(as_text=True)

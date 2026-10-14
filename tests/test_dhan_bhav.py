"""The NSE bhavcopy check on Dhan files (<UNDERLYING>_FnO_1min_Dhan): backfill/bhav.py and bhav_export.py
with source="dhan", the dashboard routes with ?source=dhan, and re-downloading one Dhan contract.
No network, no real .env."""
import csv
import io
import json
import os
from datetime import date, datetime

import pytest

import dashboard
from app import app
from backfill import bhav, bhav_export, dhan_api, fno
from backfill.job import JOB, JOB_LOCK
from test_bhav import DAY, csv_bytes, kite_file, read_csv, row

IST = fno.IST
RUN_DATE = date(2026, 10, 12)
DHAN_ENV = {"DHAN_CLIENT_ID": "1000000001", "DHAN_ACCESS_TOKEN": "SECRET-DHAN-TOKEN", "DHAN_TOKEN_EXPIRY": "2099-01-01T09:00:00"}


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    class Blocked:
        def __init__(self, *a, **kw):
            raise AssertionError("network used")
    monkeypatch.setattr(bhav.requests, "Session", Blocked)
    monkeypatch.setattr(dhan_api.requests, "get", Blocked)
    monkeypatch.setattr(dhan_api.requests, "post", Blocked)
    monkeypatch.setattr(fno, "now_ist", lambda: datetime(2026, 10, 12, 16, 0, tzinfo=IST))


def dhan_day(root):
    """A saved Dhan day: the near future and one call; the bhavcopy also has a traded put (missing)."""
    day = root / "NIFTY_FnO_1min_Dhan" / DAY
    kite_file(str(day), "NIFTY27OCT26", [22600.0], vol=150)
    kite_file(str(day), "NIFTY2500013OCT26CE", [10.0], vol=100)
    (day / "summary.csv").write_text("Expiry\n", encoding="utf-8")
    return day


def bhav_file(tmp_path):
    rows = [row("IDF", "2026-10-27", vol=3, last=22600, oi=1000), row("IDO", "2026-10-13", "25000.00", "CE", vol=2, last=10, oi=1000),
            row("IDO", "2026-10-13", "25000.00", "PE", vol=4, last=12, oi=1000)]
    src = tmp_path / "manual.csv"
    src.write_bytes(csv_bytes(rows))
    return src


def test_run_bhav_on_dhan_files(tmp_path):
    root = tmp_path / "data"
    dhan_day(root)
    status, line, _ = bhav.run_bhav(RUN_DATE, str(root), bhav_file=str(bhav_file(tmp_path)), log=lambda m: None, source="dhan")
    out = root / "NIFTY_FnO_1min_Dhan" / DAY / "bhavcopy"
    assert status == "diffs" and "1 missing" in line
    assert json.loads((out / "bhav_check.json").read_text())["counts"]["missing"] == 1
    assert {r[0] for r in read_csv(out / "bhav_compare.csv")[1:]} == {"NIFTY27OCT26", "NIFTY2500013OCT26CE", "NIFTY2500013OCT26PE"}
    assert not (root / "NIFTY_FnO_1min").exists()                       # Kite folder untouched
    assert [d["date"] for d in bhav.list_days(str(root), source="dhan")] == [DAY]
    assert bhav.list_days(str(root)) == []
    assert bhav.unchecked_days(str(root), source="dhan") == []
    rows, _ = bhav.diff_rows(str(root), "NIFTY", RUN_DATE, source="dhan")
    assert [r["Contract"] for r in rows] == ["NIFTY2500013OCT26PE"]


def test_exports_say_dhan(tmp_path):
    root = tmp_path / "data"
    dhan_day(root)
    bhav.run_bhav(RUN_DATE, str(root), bhav_file=str(bhav_file(tmp_path)), log=lambda m: None, source="dhan")
    name, data, _ = bhav_export.export(str(root), "NIFTY", "all", RUN_DATE, source="dhan")
    assert name == f"bhavcopy_check_{DAY}_all_contracts_dhan.csv"
    rows = list(csv.reader(io.StringIO(data.decode())))
    assert "Dhan_volume" in rows[0] and "Dhan_last_bar_time" in rows[0] and not any("Kite" in c for c in rows[0])
    assert any("MISSING_IN_DHAN" in r for r in rows[1:])
    name, data, _ = bhav_export.export(str(root), "NIFTY", "days", source="dhan", today=RUN_DATE)
    assert name.endswith("_dhan.csv") and "Dhan_files" in data.decode().splitlines()[0]
    name, data, _ = bhav_export.export(str(root), "NIFTY", "xlsx", RUN_DATE, source="dhan")
    assert name == f"bhavcopy_check_{DAY}_dhan.xlsx" and data[:2] == b"PK"
    with pytest.raises(bhav.BhavError):   # the Kite folder has no check
        bhav_export.export(str(root), "NIFTY", "all", RUN_DATE)
    saved = (root / "NIFTY_FnO_1min_Dhan" / DAY / "bhavcopy" / bhav.DAYS_SUMMARY_FILE).read_bytes()
    assert saved == bhav_export.export(str(root), "NIFTY", "days", source="dhan")[1]   # one checked day
    lines = saved.decode().splitlines()
    assert "Zero_volume_in_dhan" in lines[0] and len(lines) == 2 and lines[1].startswith(DAY)


def test_kite_export_names_unchanged():
    cols, rows = bhav_export.relabel(["Kite_volume"], [{"Kite_volume": "1", "Status": "MISSING_IN_KITE"}])
    assert cols == ["Kite_volume"] and rows[0]["Status"] == "MISSING_IN_KITE"


# --- routes -------------------------------------------------------------------------------------

@pytest.fixture
def client(monkeypatch):
    env = dict(DHAN_ENV)
    monkeypatch.setattr(dashboard, "load_env", lambda: dict(env))
    monkeypatch.setattr(dashboard, "save_env", lambda updates: None)
    with JOB_LOCK:
        JOB["state"] = "idle"
    return app.test_client(), env   # no Kite session: only the Dhan token signs these requests in


def test_routes_need_the_dhan_token(client, tmp_path):
    c, env = client
    q = {"folder": str(tmp_path), "source": "dhan"}
    assert c.get("/fno/bhav-status", query_string=q).get_json() == {"days": [], "banners": {"check": [], "minor": [], "pending_old": []}}
    assert c.get("/fno/bhav-status", query_string={"folder": str(tmp_path)}).status_code == 403    # Kite files need Kite
    assert "Unknown data source" in c.get("/fno/bhav-status", query_string={**q, "source": "x"}).get_json()["error"]
    env.pop("DHAN_ACCESS_TOKEN")
    assert c.get("/fno/bhav-status", query_string=q).status_code == 403


def test_route_status_diff_export_on_dhan(client, tmp_path):
    c, _ = client
    root = tmp_path / "data"
    dhan_day(root)
    bhav.run_bhav(RUN_DATE, str(root), bhav_file=str(bhav_file(tmp_path)), log=lambda m: None, source="dhan")
    q = {"folder": str(root), "source": "dhan"}
    days = c.get("/fno/bhav-status", query_string=q).get_json()["days"]
    assert [d["date"] for d in days] == [DAY] and days[0]["status"] == "diffs"
    diff = c.get("/fno/bhav-diff", query_string={**q, "date": DAY}).get_json()
    assert [r["Contract"] for r in diff["rows"]] == ["NIFTY2500013OCT26PE"]
    r = c.get("/fno/bhav-export", query_string={**q, "date": DAY, "scope": "xlsx"})
    assert r.status_code == 200 and f"bhavcopy_check_{DAY}_dhan.xlsx" in r.headers["Content-Disposition"]


def test_route_check_on_dhan(client, tmp_path, monkeypatch):
    c, _ = client
    seen = []
    monkeypatch.setattr(bhav, "run_bhav", lambda d, folder, underlying, bhav_file=None, log=print, source="kite":
                        seen.append((d, source)) or ("ok", "Bhavcopy check: OK", None))
    j = c.post("/fno/bhav-check", data={"folder": str(tmp_path), "date": DAY, "source": "dhan"}).get_json()
    assert j == {"results": [{"date": DAY, "status": "ok", "line": "Bhavcopy check: OK"}]}
    assert seen == [(RUN_DATE, "dhan")]
    # the Dhan lock was taken and released
    fno.acquire_lock(str(tmp_path), "NIFTY", "test", "dhan").release()


def test_route_redownload_on_dhan(client, tmp_path, monkeypatch):
    c, _ = client
    seen = {}

    def fake(*a, **kw):
        seen.update(kw, token=a[1])
        return True, "NIFTY2500013OCT26PE: saved 2 bars."
    monkeypatch.setattr(fno, "redownload_contract", fake)
    monkeypatch.setattr(bhav, "check_saved", lambda day_dir, *a, **kw: seen.update(day_dir=day_dir) or ("ok", "Bhavcopy check: OK", None))
    monkeypatch.setattr(dhan_api.DhanClient, "profile", lambda self: {"user_id": "1000000001", "data_plan": "Active"})
    j = c.post("/fno/redownload", data={"folder": str(tmp_path), "date": DAY, "contract": "NIFTY2500013OCT26PE",
                                       "source": "dhan"}).get_json()
    assert j["saved"] is True and j["line"] == "Bhavcopy check: OK"
    assert seen["source"] == "dhan" and isinstance(seen["kite"], dhan_api.DhanClient) and seen["token"] == DHAN_ENV["DHAN_ACCESS_TOKEN"]
    assert seen["day_dir"] == os.path.join(str(tmp_path), "NIFTY_FnO_1min_Dhan", DAY)


def test_route_redownload_on_dhan_checks_token_and_plan(client, tmp_path, monkeypatch):
    """The same checks as a Dhan download: inactive Data API plan, refused token, expired token."""
    from kiteconnect import exceptions as kex
    c, env = client
    called = []
    monkeypatch.setattr(fno, "redownload_contract", lambda *a, **kw: called.append(1) or (True, ""))
    post = lambda: c.post("/fno/redownload", data={"folder": str(tmp_path), "date": DAY, "contract": "NIFTY2500013OCT26PE",
                                                   "source": "dhan"}).get_json()
    monkeypatch.setattr(dhan_api.DhanClient, "profile", lambda self: {"data_plan": "Inactive"})
    assert "Data API subscription is Inactive" in post()["error"]

    def refused(self):
        raise kex.TokenException("Invalid token")
    monkeypatch.setattr(dhan_api.DhanClient, "profile", refused)
    assert "invalid or expired" in post()["error"]
    env["DHAN_TOKEN_EXPIRY"] = "2026-01-01T09:00:00"
    r = c.post("/fno/redownload", data={"folder": str(tmp_path), "date": DAY, "contract": "X", "source": "dhan"})
    assert r.status_code == 403
    assert not called


# --- re-download with a Dhan client -------------------------------------------------------------

class FakeDhan:
    def __init__(self):
        self.learned, self.asked = {}, []

    def learn(self, instruments):
        self.learned = {i["instrument_token"]: i["dhan_instrument"] for i in instruments}

    def historical_data(self, token, start, end, interval, continuous=False, oi=False):
        self.asked.append(token)
        t0 = datetime(2026, 10, 12, 9, 15, tzinfo=IST)
        return [{"date": t0, "open": 12, "high": 13, "low": 11, "close": 12.5, "volume": 75, "oi": 300}]


def test_redownload_contract_on_dhan(tmp_path):
    root = tmp_path / "data"
    day = dhan_day(root)
    fno.save_instruments(str(day / "instruments_NSE_FNO.csv"), [
        {"instrument_token": "35001", "tradingsymbol": "NIFTY-Oct2026-FUT", "name": "NIFTY", "expiry": "2026-10-27",
         "strike": 0.0, "instrument_type": "FUT", "dhan_instrument": "FUTIDX"},
        {"instrument_token": "40002", "tradingsymbol": "NIFTY-Oct2026-25000-PE", "name": "NIFTY", "expiry": "2026-10-13",
         "strike": 25000.0, "instrument_type": "PE", "dhan_instrument": "OPTIDX"}])
    client = FakeDhan()
    saved, msg = fno.redownload_contract("", "tok", RUN_DATE, str(root), "NIFTY2500013OCT26PE", kite=client,
                                         source="dhan", speed=1000, log=lambda m: None)
    assert saved and "saved 1 bars" in msg
    assert client.asked == ["40002"] and client.learned["35001"] == "FUTIDX"
    assert (day / "options" / "NIFTY2500013OCT26PE.csv").exists()
    assert not (root / "NIFTY_FnO_1min").exists()



def test_fetch_bhav_respects_the_time_budget():
    """A slow NSE: every request uses its whole timeout on a fake clock; with a 30 s budget it gives up in time."""
    now = [0.0]
    timeouts = []

    class Slow:
        headers = {}

        def get(self, url, timeout=None):
            timeouts.append(timeout)
            now[0] += timeout
            raise bhav.requests.Timeout("slow")
    with pytest.raises(bhav.BhavTimeout):
        bhav.fetch_bhav(RUN_DATE, log=lambda m: None, session=Slow(), budget=30,
                        clock=lambda: now[0], sleep=lambda s: now.__setitem__(0, now[0] + s))
    assert now[0] <= 30 and timeouts[0] == 5


def test_fetch_bhav_without_budget_is_unchanged():
    class Gone:
        headers = {}

        def get(self, url, timeout=None):
            return type("R", (), {"status_code": 404, "ok": False, "content": b""})()
    with pytest.raises(bhav.BhavNotAvailable):
        bhav.fetch_bhav(RUN_DATE, log=lambda m: None, session=Gone())

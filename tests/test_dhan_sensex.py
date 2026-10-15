"""SENSEX F&O on Dhan (BSE_FNO): the same job, trim, BSE bhavcopy check, holiday check, routes and CLIs as
Dhan NIFTY and Kite SENSEX. No network, no real .env."""
import json
import os
import sqlite3
from datetime import date, datetime

import pytest

import nifty_bhav
import nifty_daily
from backfill import bhav, bhav_export, dhan_api, fno
from backfill.job import JOB, JOB_LOCK, STOP, job_begin
from test_dhan_fno import TOKEN, Resp, arrays, client, error_of, wait_for  # noqa: F401  (fixtures)
from test_dhan_parity import FakeDhan, dcli  # noqa: F401  (fixtures)
from test_sensex import bse_csv
from conftest import day_of

IST = fno.IST
RUN_DATE = date(2026, 10, 12)
DAY = RUN_DATE.isoformat()
# Dhan's BSE_FNO list: the SENSEX future, the 74700 CE/PE (in BSE's bhavcopy), a 75000 CE that is not, and BANKEX
BSE_CSV = """SECURITY_ID,INSTRUMENT,UNDERLYING_SYMBOL,SYMBOL_NAME,LOT_SIZE,SM_EXPIRY_DATE,STRIKE_PRICE,OPTION_TYPE
1101,FUTIDX,SENSEX,SENSEX-Oct2026-FUT,20,2026-10-29,-0.01,XX
1102,OPTIDX,SENSEX,SENSEX-Oct2026-74700-CE,20,2026-10-15,74700,CE
1103,OPTIDX,SENSEX,SENSEX-Oct2026-74700-PE,20,2026-10-15,74700,PE
1104,OPTIDX,SENSEX,SENSEX-Oct2026-75000-CE,20,2026-10-15,75000,CE
2001,FUTIDX,BANKEX,BANKEX-Oct2026-FUT,30,2026-10-29,-0.01,XX
"""
NO_DATA = Resp(400, {"errorCode": "DH-907", "errorMessage": "No data present"})


def dhan_day(root):
    return day_of(root / "SENSEX_FnO_1min_Dhan", DAY)


@pytest.fixture
def bse_http(monkeypatch):
    """Dhan's BSE_FNO list and candles; replies[id] answers /charts/intraday. Records the URLs and bodies."""
    seen = {"urls": [], "bodies": [], "replies": {"1101": Resp(200, arrays(5, 72910.0, vol=20)),
                                                  "1102": Resp(200, arrays(3, 20.0, vol=20)), "1103": NO_DATA,
                                                  "1104": Resp(200, arrays(2, 5.0)), "51": Resp(200, arrays(5, 74710.0))}}

    def get(url, headers=None, timeout=None):
        seen["urls"].append(url)
        return Resp(200, text=BSE_CSV)

    def post(url, json=None, headers=None, timeout=None):
        seen["bodies"].append(json)
        return seen["replies"].get(json["securityId"], NO_DATA)

    monkeypatch.setattr(dhan_api.requests, "get", get)
    monkeypatch.setattr(dhan_api.requests, "post", post)
    monkeypatch.setattr(fno, "now_ist", lambda: datetime(2026, 10, 12, 16, 0, tzinfo=IST))
    STOP.clear()
    with JOB_LOCK:
        JOB["state"] = "idle"
    return seen


@pytest.fixture
def bse(monkeypatch):
    """BSE answers with calls["data"] (None: not published)."""
    calls = {"data": None, "fetched": []}

    def fetch(run_date, log=print, budget=None, underlying="NIFTY", **kw):
        calls["fetched"].append((run_date, underlying, budget))
        if calls["data"] is None:
            raise bhav.BhavNotAvailable(f"BSE has no F&O bhavcopy for {run_date} yet (no file: test).")
        return calls["data"], bhav.bhav_url(run_date, underlying)
    monkeypatch.setattr(bhav, "fetch_bhav", fetch)
    return calls


def run_dhan_sensex(root):
    assert job_begin(**fno.job_fields("SENSEX", RUN_DATE, str(root), 1000, source="dhan"))
    return fno.run_daily("", TOKEN, RUN_DATE, str(root), 1000, "SENSEX", kite=dhan_api.DhanClient(TOKEN),
                         user="1000000001", log=lambda m: None, source="dhan", weekday=3)


def asked(seen):
    return [b["securityId"] for b in seen["bodies"]]


# --- the client ---------------------------------------------------------------------------------

def test_client_reads_bse_fno(bse_http):
    c = dhan_api.DhanClient(TOKEN)
    rows = c.instruments("BFO")
    assert bse_http["urls"] == ["https://api.dhan.co/v2/instrument/BSE_FNO"]
    assert {r["exchange"] for r in rows} == {"BSE_FNO"} and rows[0]["name"] == "SENSEX"
    c.historical_data("1102", datetime(2026, 10, 12, 9), datetime(2026, 10, 12, 16), "minute", oi=True)
    assert bse_http["bodies"][-1]["exchangeSegment"] == "BSE_FNO" and bse_http["bodies"][-1]["instrument"] == "OPTIDX"
    c.historical_data(c.spot_token("SENSEX"), datetime(2026, 10, 12, 9), datetime(2026, 10, 12, 16), "minute")
    assert bse_http["bodies"][-1] | {"fromDate": "", "toDate": ""} == {
        "securityId": "51", "exchangeSegment": "IDX_I", "instrument": "INDEX", "interval": "1", "oi": False,
        "fromDate": "", "toDate": ""}
    assert dhan_api.DhanClient(TOKEN).instruments() and bse_http["urls"][-1].endswith("/NSE_FNO")   # default NIFTY
    with pytest.raises(dhan_api.DhanError):
        c.instruments("MCX")


def test_exchange_of_dhan():
    assert fno.exchange_of("SENSEX", "dhan") == "BSE_FNO" and fno.exchange_of("NIFTY", "dhan") == "NSE_FNO"
    assert fno.run_key("SENSEX", RUN_DATE, "dhan") == "fno-dhan:SENSEX:BSE_FNO:2026-10-12"
    assert fno.exchange_of("SENSEX") == "BFO"


# --- the daily job: trim to BSE's bhavcopy, then the check --------------------------------------

def test_dhan_sensex_run_trims_to_bse_bhavcopy(tmp_path, bse_http, bse, monkeypatch):
    monkeypatch.setattr(fno, "AUTO_BHAV", True)
    bse["data"] = bse_csv()
    assert run_dhan_sensex(tmp_path) == "finished"
    day = dhan_day(tmp_path)
    assert os.listdir(day / "futures") == ["SENSEX29OCT26.csv"]
    assert os.listdir(day / "options") == ["SENSEX7470015OCT26CE.csv"]          # PE: no data, 75000 CE: trimmed
    assert (day / "instruments_BSE_FNO.csv").exists() and (day / "summary.csv").exists()
    assert sorted(asked(bse_http)) == ["1101", "1102", "1103"]                 # 1104 not in BSE's bhavcopy, no BANKEX
    assert {b["exchangeSegment"] for b in bse_http["bodies"]} == {"BSE_FNO"}
    assert bse_http["urls"] == ["https://api.dhan.co/v2/instrument/BSE_FNO"]
    assert bse["fetched"] == [(RUN_DATE, "SENSEX", bhav.TRIM_BUDGET)]          # the check reuses the trim's download
    log = (day / "run.log").read_text(encoding="utf-8")
    assert "Trimmed to 3 contracts listed in BSE's bhavcopy" in log and TOKEN not in log
    db = sqlite3.connect(tmp_path / "progress.sqlite")
    assert {r for (r,) in db.execute("SELECT DISTINCT run FROM chunks")} == {"fno-dhan:SENSEX:BSE_FNO:2026-10-12"}
    db.close()
    assert not (tmp_path / "SENSEX_FnO_1min").exists() and not (tmp_path / "NIFTY_FnO_1min_Dhan").exists()

    out = day / "bhavcopy"
    assert (out / "BhavCopy_BSE_FO_0_0_0_20261012_F_0000.CSV").exists() and (out / "sensex_bhav.csv").exists()
    check = json.loads((out / "bhav_check.json").read_text())
    assert check["counts"]["missing"] == 1                                      # the PE that Dhan had no data for
    assert bhav.list_days(str(tmp_path), "SENSEX", "dhan")[0]["date"] == DAY
    assert bhav.list_days(str(tmp_path), "SENSEX") == []                       # Kite's SENSEX folder is separate
    name, body, _ = bhav_export.export(str(tmp_path), "SENSEX", "all", RUN_DATE, source="dhan")
    assert name == "bhavcopy_check_2026-10-12_all_contracts_sensex_dhan.csv" and b"Dhan" in body


def test_dhan_sensex_without_bhavcopy_keeps_full_list(tmp_path, bse_http, bse):
    assert run_dhan_sensex(tmp_path) == "finished"
    assert sorted(asked(bse_http)) == ["1101", "1102", "1103", "1104"]
    assert "Bhavcopy for 2026-10-12 not available" in (dhan_day(tmp_path) / "run.log").read_text(encoding="utf-8")


# --- holiday check: future empty -> ask the spot (IDX_I 51) -------------------------------------

def test_dhan_sensex_untraded_future_is_not_a_holiday(tmp_path, bse_http, bse):
    bse_http["replies"]["1101"] = NO_DATA
    assert run_dhan_sensex(tmp_path) == "finished"
    root = tmp_path / "SENSEX_FnO_1min_Dhan"
    assert not (root / "holidays.log").exists() and not os.listdir(dhan_day(tmp_path) / "futures")
    assert asked(bse_http)[:2] == ["1101", "51"] and bse_http["bodies"][1]["exchangeSegment"] == "IDX_I"
    assert "SENSEX29OCT26 has no candles but SENSEX spot traded" in \
        (dhan_day(tmp_path) / "run.log").read_text(encoding="utf-8")


def test_dhan_sensex_holiday(tmp_path, bse_http, bse):
    bse_http["replies"] = {}
    assert run_dhan_sensex(tmp_path) == "holiday"
    root = tmp_path / "SENSEX_FnO_1min_Dhan"
    assert not dhan_day(tmp_path).exists() and DAY in (root / "holidays.log").read_text(encoding="utf-8")
    assert asked(bse_http) == ["1101", "51"]


# --- re-download --------------------------------------------------------------------------------

def test_redownload_on_dhan_sensex_sends_bse_fno(tmp_path, bse_http):
    day = dhan_day(tmp_path)
    day.mkdir(parents=True)
    fno.save_instruments(str(day / "instruments_BSE_FNO.csv"), dhan_api.parse_instruments(BSE_CSV, "BSE_FNO"))
    saved, msg = fno.redownload_contract("", TOKEN, RUN_DATE, str(tmp_path), "SENSEX7470015OCT26CE", "SENSEX",
                                         speed=1000, log=lambda m: None, kite=dhan_api.DhanClient(TOKEN), source="dhan")
    assert saved, msg
    assert bse_http["bodies"][-1]["securityId"] == "1102" and bse_http["bodies"][-1]["exchangeSegment"] == "BSE_FNO"
    assert (day / "options" / "SENSEX7470015OCT26CE.csv").exists()


# --- the Dhan page, routes and CLIs -------------------------------------------------------------

def test_dhan_page_offers_sensex(client):
    c, _ = client
    html = c.get("/dhan/dashboard").get_data(as_text=True)
    assert '"name": "SENSEX"' in html and '"exchange": "BSE_FNO"' in html and '"bhav": "BSE"' in html


def test_dhan_fno_start_sensex(client, tmp_path):
    c, calls = client
    r = c.post("/dhan/fno-start", data={"date": DAY, "folder": str(tmp_path), "underlying": "SENSEX", "speed": "4"})
    assert r.headers["Location"] == "/dhan/dashboard", error_of(r)
    assert wait_for(lambda: calls["run"])
    _, args, kw = calls["run"][0]
    assert args[5] == "SENSEX" and kw["source"] == "dhan"
    assert JOB["run"] == "fno-dhan:SENSEX:BSE_FNO:2026-10-12" and JOB["fno_exchange"] == "BSE_FNO"


def test_dhan_window_reads_bse_fno(bse_http):
    import dhan
    dhan.LIVE.clear()
    w, err = dhan.fno_window("C:\\nowhere", "SENSEX", TOKEN)
    assert not err and w.current_expiry == date(2026, 10, 15)
    assert bse_http["urls"] == ["https://api.dhan.co/v2/instrument/BSE_FNO"]
    dhan.LIVE.clear()


def test_cli_dhan_sensex(dcli, tmp_path):
    env, calls = dcli
    assert nifty_daily.main([DAY, "--folder", str(tmp_path), "--source", "dhan", "--underlying", "SENSEX"]) == 0
    name, args, kw = calls[0]
    assert name == "daily" and args[5] == "SENSEX" and kw["source"] == "dhan"


def test_bhav_cli_dhan_sensex(tmp_path, monkeypatch):
    seen = []
    monkeypatch.setattr(nifty_bhav, "run_bhav", lambda d, folder, underlying, bhav_file, log=print, source="kite":
                        seen.append((d, underlying, source)) or ("ok", "", None))
    (dhan_day(tmp_path)).mkdir(parents=True)
    (dhan_day(tmp_path) / "summary.csv").write_text("x")
    assert nifty_bhav.main(["--missing", "--folder", str(tmp_path), "--source", "dhan", "--underlying", "SENSEX"]) == 0
    assert seen == [(RUN_DATE, "SENSEX", "dhan")]

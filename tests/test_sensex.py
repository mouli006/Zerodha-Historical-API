"""SENSEX F&O on Kite's BFO with BSE's F&O bhavcopy: the same job and check as NIFTY / NSE.
No network: BSE answers from fake sessions or a stubbed fetch_bhav."""
import csv
import json
import os
from datetime import date, datetime

import pytest

import config
import nifty_daily
from backfill import bhav, bhav_export, fno, fno_loader
from backfill.job import JOB, JOB_LOCK, job_begin
from backfill.storage import open_db
from test_bhav import FakeResponse, FakeSession, csv_bytes, row
from test_fno import ACCESS, RUN_DATE, FakeKite, bars, inst, read_csv
from test_fno_entrypoints import client, ist_now, post, wait_for  # noqa: F401  (fixtures)
from conftest import day_of

DAY = RUN_DATE.isoformat()
W1, M1 = date(2026, 10, 15), date(2026, 10, 29)   # SENSEX expires on Thursdays


def bse_row(*a, **kw):
    """A BSE UDiFF row: Src BSE, SENSEX, lot 20, volume and OI both in shares."""
    r = row(*a, sym=kw.pop("sym", "SENSEX"), lot=kw.pop("lot", 20), **kw)
    r["Src"] = "BSE"
    return r


def bse_csv():
    return csv_bytes([
        bse_row("IDF", "2026-10-29", vol=100, oi=1000, last=72914),          # Kite: 5 bars x 20 = 100 shares
        bse_row("IDO", "2026-10-15", "74700.00", "CE", vol=60, oi=1002, last=22),
        bse_row("IDO", "2026-10-15", "74700.00", "PE", vol=40, oi=500, last=9),   # traded, no Kite file
        bse_row("IDF", "2026-10-29", sym="BANKEX", vol=9),
    ])


class SensexKite(FakeKite):
    candles = {1: bars(5, 72910.0, vol=20), 11: bars(3, 20.0, vol=20)}
    fail_tokens = set()

    def instruments(self, exchange):
        assert exchange == "BFO"
        return [inst("SENSEX26OCTFUT", "FUT", M1, name="SENSEX", token=1),
                inst("SENSEX26O1574700CE", "CE", W1, 74700.0, name="SENSEX", token=11),
                inst("SENSEX26O1574700PE", "PE", W1, 74700.0, name="SENSEX", token=12),
                inst("BANKEX26OCTFUT", "FUT", M1, name="BANKEX", token=500)]

    def historical_data(self, token, start, end, interval, continuous=False, oi=False):
        SensexKite.calls.append((token, start, end, interval, continuous, oi))
        return SensexKite.candles.get(token, [])


@pytest.fixture
def sensex_kite(monkeypatch):
    SensexKite.calls, SensexKite.profile_error, SensexKite.holiday = [], None, False
    monkeypatch.setattr(fno, "KiteConnect", SensexKite)
    monkeypatch.setattr(fno, "now_ist", lambda: datetime(2026, 10, 12, 16, 0, tzinfo=fno.IST))
    fno.STOP.clear()
    return SensexKite


@pytest.fixture
def bse(monkeypatch):
    """Kite runs fetch the bhavcopy; BSE answers with calls["data"] (None: not published)."""
    monkeypatch.setattr(fno, "AUTO_BHAV", True)
    calls = {"data": None, "fetched": []}

    def fetch(run_date, log=print, budget=None, underlying="NIFTY", **kw):
        calls["fetched"].append((run_date, underlying))
        if calls["data"] is None:
            raise bhav.BhavNotAvailable(f"BSE has no F&O bhavcopy for {run_date} yet (no file: test).")
        return calls["data"], bhav.bhav_url(run_date, underlying)
    monkeypatch.setattr(bhav, "fetch_bhav", fetch)
    return calls


def run_sensex(data):
    assert job_begin(**fno.job_fields("SENSEX", RUN_DATE, str(data), 1000))
    return fno.run_daily("key", ACCESS, RUN_DATE, str(data), 1000, "SENSEX", log=lambda m: None, weekday=3)


def test_sensex_is_registered():
    assert fno.UNDERLYINGS["SENSEX"] == ("BFO", 100)
    assert fno.exchange_name("SENSEX") == "BSE" and fno.exchange_name("NIFTY") == "NSE"
    assert bhav.bhav_url(RUN_DATE, "SENSEX") == \
        "https://www.bseindia.com/download/Bhavcopy/Derivative/BhavCopy_BSE_FO_0_0_0_20261012_F_0000.CSV"
    assert bhav.bhav_url(RUN_DATE) == bhav.BHAV_URL.format(d="20261012")


def test_weekly_expiry_weekday_per_underlying(monkeypatch):
    env = {}
    monkeypatch.setattr(config, "load_env", lambda: env)
    assert (config.weekly_expiry_weekday(), config.weekly_expiry_weekday("SENSEX")) == (1, 3)   # TUE, THU
    env.update(WEEKLY_EXPIRY_WEEKDAY="WED")                   # the older key is NIFTY's only
    assert (config.weekly_expiry_weekday(), config.weekly_expiry_weekday("SENSEX")) == (2, 3)
    env.update(WEEKLY_EXPIRY_WEEKDAY_SENSEX="friday", WEEKLY_EXPIRY_WEEKDAY_NIFTY="MON")
    assert (config.weekly_expiry_weekday(), config.weekly_expiry_weekday("SENSEX")) == (0, 4)


# --- getting BSE's file -------------------------------------------------------------------------

def test_fetch_bse_csv():
    s = FakeSession([FakeResponse(200, b"\xef\xbb\xbf" + bse_csv())])
    data, url = bhav.fetch_bhav(RUN_DATE, log=lambda m: None, session=s, sleep=lambda x: None, underlying="SENSEX")
    assert url == bhav.bhav_url(RUN_DATE, "SENSEX") and s.urls == [url]       # no NSE home page
    assert s.headers["Referer"] == "https://www.bseindia.com/"
    _, rows = bhav.read_bhav(data)
    assert [i["name"] for i in bhav.parse_bhav(rows, "SENSEX")] == \
        ["SENSEX29OCT26", "SENSEX7470015OCT26CE", "SENSEX7470015OCT26PE"]


def test_fetch_bse_page_is_not_available():
    """BSE answers a day it has not published with its web page (200), not a 404."""
    s = FakeSession([FakeResponse(200, b"<!doctype html><html><body>BSE</body></html>")])
    with pytest.raises(bhav.BhavNotAvailable, match="BSE has no F&O bhavcopy for 2026-10-12"):
        bhav.fetch_bhav(RUN_DATE, log=lambda m: None, session=s, sleep=lambda x: None, underlying="SENSEX")
    with pytest.raises(bhav.BhavNotAvailable, match="BSE"):
        bhav.fetch_bhav(RUN_DATE, log=lambda m: None, session=FakeSession([FakeResponse(404)]),
                        sleep=lambda x: None, underlying="SENSEX")


def test_fetch_bse_other_answers_are_retried():
    s = FakeSession([FakeResponse(500), FakeResponse(200, b"garbage"), FakeResponse(200, bse_csv())])
    data, _ = bhav.fetch_bhav(RUN_DATE, log=lambda m: None, session=s, sleep=lambda x: None, underlying="SENSEX")
    assert data == bse_csv()


# --- the daily job and its check ----------------------------------------------------------------

def test_sensex_run_daily_with_bse_bhavcopy(tmp_path, sensex_kite, bse):
    bse["data"] = bse_csv()
    data = tmp_path / "data"
    assert run_sensex(data) == "finished"
    day = day_of(data / "SENSEX_FnO_1min", DAY)
    assert os.listdir(day / "futures") == ["SENSEX29OCT26.csv"]
    assert os.listdir(day / "options") == ["SENSEX7470015OCT26CE.csv"]          # PE: no candles, no file
    assert (day / "instruments_BFO.csv").exists() and (day / "summary.csv").exists()
    assert read_csv(day / "futures" / "SENSEX29OCT26.csv")[1][0] == "SENSEX29OCT26"
    assert {t for t, *_ in sensex_kite.calls} == {1, 11, 12}                    # BANKEX left out
    with open_db(str(data)) as db:
        assert {r[0] for r in db.execute("SELECT DISTINCT run FROM chunks")} == {"fno:SENSEX:BFO:2026-10-12"}
    assert not (data / "NIFTY_FnO_1min").exists()

    # BSE's bhavcopy: the original CSV and sensex_bhav.csv in <day>/bhavcopy/, then the check
    assert bse["fetched"] == [(RUN_DATE, "SENSEX")]
    out = day / "bhavcopy"
    assert (out / "BhavCopy_BSE_FO_0_0_0_20261012_F_0000.CSV").exists()
    assert len(read_csv(out / "sensex_bhav.csv")) == 4                         # header + 3 SENSEX rows
    check = json.loads((out / "bhav_check.json").read_text())
    assert check["units"] == {"volume": 1, "oi": 1}                             # BSE: shares, as Kite
    assert check["source"].endswith("BhavCopy_BSE_FO_0_0_0_20261012_F_0000.CSV")
    assert check["counts"]["missing"] == 1 and check["counts"]["volume_diffs"] == 0
    cmp = {r["Contract"]: r["Status"] for r in bhav.compare_rows(str(data), "SENSEX", RUN_DATE)[0]}
    assert cmp == {"SENSEX29OCT26": "OK", "SENSEX7470015OCT26CE": "OK", "SENSEX7470015OCT26PE": "MISSING_IN_KITE"}
    with open(out / bhav.SUMMARY_FILE, newline="", encoding="utf-8") as f:
        summary = {(r["Expiry"], r["Type"]): r for r in csv.DictReader(f)}
    assert summary[("ALL", "")]["Missing"] == "1" and summary[("ALL", "")]["Volume_unit"] == "1"
    assert summary[("2026-10-15", "OPT")]["Kite_files"] == "1"
    assert (out / bhav.DAYS_SUMMARY_FILE).exists()
    assert bhav.list_days(str(data), "SENSEX")[0]["status"] == "diffs"
    assert bhav.list_days(str(data)) == []                                      # NIFTY's folder is separate


def test_sensex_bhavcopy_not_published(tmp_path, sensex_kite, bse):
    data = tmp_path / "data"
    assert run_sensex(data) == "finished"
    root = data / "SENSEX_FnO_1min"
    assert "2026-10-12: BSE has no F&O bhavcopy" in (root / "bhavcopy.log").read_text(encoding="utf-8")
    assert "Bhavcopy: pending (BSE has no F&O bhavcopy" in JOB["message"]
    assert bhav.list_days(str(data), "SENSEX")[0]["status"] == "pending"


def test_sensex_exports_and_eod(tmp_path, sensex_kite, bse):
    bse["data"] = bse_csv()
    data = tmp_path / "data"
    run_sensex(data)
    name, body, _ = bhav_export.export(str(data), "SENSEX", "all", RUN_DATE)
    assert name == "bhavcopy_check_2026-10-12_all_contracts_sensex.csv" and b"SENSEX7470015OCT26PE" in body
    assert bhav_export.export(str(data), "SENSEX", "xlsx", RUN_DATE)[0] == "bhavcopy_check_2026-10-12_sensex.xlsx"
    eod = fno_loader.load_eod(str(data), DAY, "SENSEX").set_index("Contract")
    assert eod.loc["SENSEX29OCT26", "Volume_contracts"] == 5                    # 100 shares / lot 20
    assert eod.loc["SENSEX29OCT26", "Lot"] == 20


# --- dashboard and CLI --------------------------------------------------------------------------

def test_fno_start_sensex(client, tmp_path, monkeypatch):
    seen = {}

    def fake_run(api_key, token, run_date, folder, speed, underlying, weekday, keep_zero=False, trim_to_bhav=None, derive_to=None):
        seen.update(underlying=underlying, weekday=weekday)
        with JOB_LOCK:
            JOB["state"] = "finished"

    monkeypatch.setattr(fno, "run_daily", fake_run)
    monkeypatch.setattr(__import__("dashboard"), "weekly_expiry_weekday", lambda underlying="NIFTY": {"SENSEX": 3}.get(underlying, 1))
    assert "error" not in post(client, tmp_path, underlying="SENSEX")
    assert wait_for(lambda: seen) and seen == {"underlying": "SENSEX", "weekday": 3}
    assert JOB["run"] == "fno:SENSEX:BFO:2026-10-12" and JOB["fno_exchange"] == "BFO"


def test_success_page_offers_sensex(client):
    html = client.get("/success").get_data(as_text=True)
    assert '"name": "SENSEX"' in html and '"bhav": "BSE"' in html and "fno-underlying" in html


def test_bhav_status_sensex(client, tmp_path):
    j = client.get("/fno/bhav-status", query_string={"folder": str(tmp_path), "underlying": "SENSEX"}).get_json()
    assert j["days"] == []
    j = client.get("/fno/bhav-status", query_string={"folder": str(tmp_path), "underlying": "BANKEX"}).get_json()
    assert "Unknown underlying" in j["error"]


def test_cli_sensex(tmp_path, monkeypatch):
    seen = {}
    monkeypatch.setattr(nifty_daily, "load_env", lambda: {"KITE_API_KEY": "key", "KITE_ACCESS_TOKEN": ACCESS})
    monkeypatch.setattr(fno, "now_ist", lambda: datetime(2026, 10, 12, 16, 0, tzinfo=fno.IST))
    monkeypatch.setattr(nifty_daily, "keep_zero_volume", lambda: False)
    monkeypatch.setattr(nifty_daily, "weekly_expiry_weekday", lambda underlying="NIFTY": {"SENSEX": 3}.get(underlying, 1))

    def fake_run(api_key, token, run_date, folder, speed, underlying, weekday, keep_zero=False, trim_to_bhav=None, derive_to=None):
        seen.update(underlying=underlying, weekday=weekday)
        with JOB_LOCK:
            JOB["state"] = "finished"
        return "finished"
    monkeypatch.setattr(nifty_daily, "run_daily", fake_run)
    assert nifty_daily.main([DAY, "--folder", str(tmp_path), "--underlying", "SENSEX"]) == 0
    assert seen == {"underlying": "SENSEX", "weekday": 3}


# --- holiday check: a thin future is confirmed against the spot index -------------------------

def test_sensex_untraded_future_is_not_a_holiday(tmp_path, sensex_kite, monkeypatch):
    """BSE's SENSEX future can go a whole day without a trade; the spot (token 265) shows the market was open."""
    monkeypatch.setattr(SensexKite, "candles", {11: bars(3, 20.0, vol=20), 265: bars(5, 74710.0)})
    data = tmp_path / "data"
    assert run_sensex(data) == "finished"
    day = day_of(data / "SENSEX_FnO_1min", DAY)
    assert not os.listdir(day / "futures") and os.listdir(day / "options") == ["SENSEX7470015OCT26CE.csv"]
    assert not (data / "SENSEX_FnO_1min" / "holidays.log").exists()
    assert "SENSEX29OCT26 has no candles but SENSEX spot traded" in (day / "run.log").read_text(encoding="utf-8")
    with open_db(str(data)) as db:
        assert db.execute("SELECT status, reason FROM chunks WHERE symbol = 'SENSEX26OCTFUT'").fetchone() == \
            ("DONE", fno.NO_CANDLES)


def test_sensex_holiday_when_spot_is_empty_too(tmp_path, sensex_kite, monkeypatch):
    monkeypatch.setattr(SensexKite, "candles", {})
    data = tmp_path / "data"
    assert run_sensex(data) == "holiday"
    root = data / "SENSEX_FnO_1min"
    assert fno.day_folders(str(root)) == {} and DAY in (root / "holidays.log").read_text(encoding="utf-8")
    assert [t for t, *_ in sensex_kite.calls] == [1, 265]


def test_traded_future_skips_the_spot(tmp_path, sensex_kite):
    assert run_sensex(tmp_path / "data") == "finished"
    assert 265 not in {t for t, *_ in sensex_kite.calls}

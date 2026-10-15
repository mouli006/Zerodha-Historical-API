"""The Dhan NIFTY F&O section has every feature of the Kite NSE F&O section: the bhavcopy check after each
day, Re-check no-trade contracts, the CLIs (nifty_daily.py / nifty_bhav.py --source dhan, incl. --inspect)
and the backtest loaders. No network, no real .env."""
import os
from datetime import date, datetime

import pytest
from kiteconnect import exceptions as kex

import nifty_bhav
import nifty_daily
from backfill import bhav, dhan_api, fno, fno_loader
from backfill.job import JOB, JOB_LOCK, job_begin
from test_bhav import zip_bytes
from test_dhan_fno import (RUN_DATE, TOKEN, TRIM_CSV, Resp, arrays, bhav_rows, client, error_of,  # noqa: F401
                           no_nse, trim_run, wait_for)
from conftest import day_of

DAY = RUN_DATE.isoformat()


@pytest.fixture(autouse=True)
def dhan_trim(trim_on):
    """These Dhan runs keep the trim setting ON (its default; conftest turns it OFF for other tests)."""


def dhan_day(root):
    return day_of(root / "NIFTY_FnO_1min_Dhan", DAY)


# --- the bhavcopy check after every Dhan day ----------------------------------------------------

def test_dhan_run_checks_the_bhavcopy(tmp_path, monkeypatch, no_nse):
    monkeypatch.setattr(fno, "AUTO_BHAV", True)
    monkeypatch.setattr(bhav, "fetch_bhav", lambda run_date, log=print, budget=None, **kw:
                        no_nse.append((run_date, budget)) or (zip_bytes(bhav_rows()), "https://x/BhavCopy_NSE_FO_test.csv.zip"))
    st, _, day, log = trim_run(tmp_path, monkeypatch)
    assert st == "finished"
    assert no_nse == [(RUN_DATE, bhav.TRIM_BUDGET)]          # the trim's download is reused by the check
    out = day / "bhavcopy"
    for name in ["bhav_compare.csv", "bhav_check.json", bhav.SUMMARY_FILE, bhav.DAYS_SUMMARY_FILE]:
        assert (out / name).exists(), name
    assert "Bhavcopy check:" in log and "Bhavcopy check:" in JOB["message"]
    days = bhav.list_days(str(tmp_path), source="dhan")
    assert [d["date"] for d in days] == [DAY] and days[0]["status"] in ("ok", "diffs")
    assert bhav.list_days(str(tmp_path)) == []               # nothing in the Kite folder


def test_dhan_run_bhavcopy_not_published(tmp_path, monkeypatch, no_nse):
    monkeypatch.setattr(fno, "AUTO_BHAV", True)
    st, requested, day, log = trim_run(tmp_path, monkeypatch)
    assert st == "finished" and len(requested) == 4
    assert [b for _, b in no_nse] == [bhav.TRIM_BUDGET, bhav.CHECK_BUDGET]
    assert "2026-10-12: NSE has no F&O bhavcopy" in (day.parent / "bhavcopy.log").read_text(encoding="utf-8")
    assert "Bhavcopy for 2026-10-12 not saved" in log and "Bhavcopy: pending" in JOB["message"]
    assert bhav.list_days(str(tmp_path), source="dhan")[0]["status"] == "pending"


# --- Re-check no-trade contracts on Dhan --------------------------------------------------------

def zero_volume_http(monkeypatch):
    """Dhan answers where the PE comes back with candles whose volume is all 0; returns the requested ids."""
    replies = {"35001": Resp(200, arrays(3, 25120.0)), "40001": Resp(200, arrays(2, 120.0)),
               "40002": Resp(200, arrays(2, 50.0, vol=0)), "49999": Resp(200, arrays(1, 5.0))}
    requested = []
    monkeypatch.setattr(dhan_api.requests, "get", lambda url, headers=None, timeout=None: Resp(200, text=TRIM_CSV))
    monkeypatch.setattr(dhan_api.requests, "post", lambda url, json=None, headers=None, timeout=None:
                        requested.append(json["securityId"]) or replies[json["securityId"]])
    monkeypatch.setattr(fno, "now_ist", lambda: datetime(2026, 10, 12, 16, 0, tzinfo=fno.IST))
    with JOB_LOCK:
        JOB["state"] = "idle"
    return requested


def test_refetch_no_trade_on_dhan(tmp_path, monkeypatch):
    zero_volume_http(monkeypatch)
    assert job_begin(**fno.job_fields("NIFTY", RUN_DATE, str(tmp_path), 1000, False, source="dhan"))
    st = fno.run_daily("", TOKEN, RUN_DATE, str(tmp_path), 1000, kite=dhan_api.DhanClient(TOKEN), log=lambda m: None,
                       source="dhan")                          # keep zero-volume OFF: no file for the PE
    day = dhan_day(tmp_path)
    assert st == "finished" and not (day / "options" / "NIFTY2500013OCT26PE.csv").exists()
    requested = zero_volume_http(monkeypatch)
    assert job_begin(**fno.job_fields("NIFTY", RUN_DATE, str(tmp_path), 1000, True, source="dhan"))
    st, _ = fno.refetch_no_trade("", TOKEN, RUN_DATE, str(tmp_path), 1000, log=lambda m: None,
                                 kite=dhan_api.DhanClient(TOKEN), user="1000000001", source="dhan")
    assert st == "finished" and requested == ["40002"]        # only the contract without a file
    assert not day.exists() and "noZeroVol" in day.name      # an ON run renames the day to withZeroVol
    day = dhan_day(tmp_path)
    assert "withZeroVol" in day.name
    assert (day / "options" / "NIFTY2500013OCT26PE.csv").exists()
    assert not (tmp_path / "NIFTY_FnO_1min").exists()


def test_refetch_no_trade_on_dhan_needs_a_saved_day(tmp_path):
    with JOB_LOCK:
        JOB["state"] = "idle"
    assert job_begin(**fno.job_fields("NIFTY", RUN_DATE, str(tmp_path), 1000, True, source="dhan"))
    st, line = fno.refetch_no_trade("", TOKEN, RUN_DATE, str(tmp_path), 1000, log=lambda m: None,
                                    kite=object(), source="dhan")
    assert (st, line) == ("not_downloaded", "")


def test_dhan_route_refetch(client, tmp_path, monkeypatch):
    c, _ = client
    seen = []
    monkeypatch.setattr(fno, "refetch_no_trade", lambda *a, **k: seen.append((a, k)) or ("finished", ""))
    form = {"date": DAY, "folder": str(tmp_path), "keep_zero": "1", "refetch": "1"}
    assert "has not been downloaded" in error_of(c.post("/dhan/fno-start", data=form))
    dhan_day(tmp_path).mkdir(parents=True)
    (dhan_day(tmp_path) / "summary.csv").write_text("x")
    assert "Keep contracts with candles but zero volume" in error_of(c.post("/dhan/fno-start", data={**form, "keep_zero": "0"}))
    assert "works on one day" in error_of(c.post("/dhan/fno-start", data={**form, "date": "", "date_from": "2026-10-10",
                                                                           "date_to": DAY}))
    assert seen == []
    r = c.post("/dhan/fno-start", data=form)
    assert r.headers["Location"] == "/dhan/dashboard"
    assert wait_for(lambda: seen)
    args, kw = seen[0]
    assert args[2] == RUN_DATE and kw["source"] == "dhan" and kw["kite"] is not None
    assert JOB["source"] == "dhan" and JOB["keep_zero"] is True


def test_dhan_page_offers_the_same_buttons(client):
    c, _ = client
    html = c.get("/dhan/dashboard").get_data(as_text=True)
    assert "Re-check no-trade contracts for" in html and "--source dhan" in html


# --- nifty_daily.py --source dhan ---------------------------------------------------------------

class FakeDhan:
    profile_error = None
    plan = "Active"

    def __init__(self, token):
        self.token = token

    def profile(self):
        if FakeDhan.profile_error:
            raise FakeDhan.profile_error
        return {"user_id": "1000000001", "data_plan": FakeDhan.plan}


@pytest.fixture
def dcli(monkeypatch, tmp_path):
    env = {"DHAN_ACCESS_TOKEN": TOKEN, "DHAN_TOKEN_EXPIRY": "2099-01-01T09:00:00"}
    calls = []
    FakeDhan.profile_error, FakeDhan.plan = None, "Active"
    monkeypatch.setattr(nifty_daily, "load_env", lambda: env)
    monkeypatch.setattr(nifty_daily, "DhanClient", FakeDhan)
    monkeypatch.setattr(nifty_daily, "keep_zero_volume", lambda: False)
    monkeypatch.setattr(nifty_daily, "weekly_expiry_weekday", lambda underlying="NIFTY": 1)

    def fake(name):
        def run(*a, **kw):
            calls.append((name, a, kw))
            with JOB_LOCK:
                JOB["state"] = "finished"
            return ("finished", "") if name == "refetch" else "finished"
        return run
    monkeypatch.setattr(nifty_daily, "run_daily", fake("daily"))
    monkeypatch.setattr(nifty_daily, "run_missing", fake("missing"))
    monkeypatch.setattr(nifty_daily, "refetch_no_trade", fake("refetch"))
    with JOB_LOCK:
        JOB["state"] = "idle"
    return env, calls


@pytest.mark.parametrize("argv,name", [([DAY], "daily"), (["--missing"], "missing"), (["--refetch-no-trade", DAY], "refetch")])
def test_cli_dhan_runs(dcli, tmp_path, argv, name):
    env, calls = dcli
    assert nifty_daily.main(argv + ["--folder", str(tmp_path), "--source", "dhan", "--speed", "5"]) == 0
    kind, args, kw = calls[0]
    assert kind == name and kw["source"] == "dhan" and isinstance(kw["kite"], FakeDhan) and kw["user"] == "1000000001"
    assert args[1] == TOKEN and JOB["source"] == "dhan"
    fno.acquire_lock(str(tmp_path), "NIFTY", "x", "dhan").release()   # the Dhan lock was given back


def test_cli_dhan_refusals(dcli, tmp_path, capsys):
    env, calls = dcli
    base = [DAY, "--folder", str(tmp_path), "--source", "dhan"]
    FakeDhan.plan = "Inactive"
    assert nifty_daily.main(base) == 1 and "Data API subscription is Inactive" in capsys.readouterr().err
    FakeDhan.plan, FakeDhan.profile_error = "Active", kex.TokenException("Invalid token")
    assert nifty_daily.main(base) == 2
    FakeDhan.profile_error = None
    env["DHAN_TOKEN_EXPIRY"] = "2020-01-01T09:00:00"
    assert nifty_daily.main(base) == 2 and "expired" in capsys.readouterr().err
    env.pop("DHAN_ACCESS_TOKEN")
    assert nifty_daily.main(base) == 2 and "DHAN_ACCESS_TOKEN is missing" in capsys.readouterr().err
    for extra in (["--underlying", "BANKEX"], ["--speed", "5.5"]):
        with pytest.raises(SystemExit) as e:
            nifty_daily.main(base + extra)
        assert e.value.code == 1
    assert calls == []
    with pytest.raises(SystemExit):
        nifty_daily.main([DAY, "--folder", str(tmp_path), "--speed", "4"])   # Kite stays at most 3


def test_cli_dhan_inspect_missing(dcli, tmp_path, monkeypatch, capsys):
    out = dhan_day(tmp_path) / "bhavcopy"
    out.mkdir(parents=True)
    (out / "bhav_compare.csv").write_text("Contract,Status\nNIFTY2500013OCT26PE,MISSING_IN_KITE\nNIFTY27OCT26,OK\n")
    seen = {}

    def fake(api_key, token, run_date, folder, names, underlying, speed, verbose=True, **kw):
        seen.update(names=names, **kw)
        return "", [{"Contract": names[0], "Kite_candles": 2, "Kite_volume": 0, "Kite_last_close": 50,
                     "Bhav_volume": 1, "Bhav_last": 50, "Result": "ZERO_VOLUME_CANDLES"}]
    monkeypatch.setattr(nifty_daily, "inspect_contracts", fake)
    assert nifty_daily.main(["--inspect-missing", DAY, "--folder", str(tmp_path), "--source", "dhan"]) == 0
    assert seen["names"] == ["NIFTY2500013OCT26PE"] and seen["source"] == "dhan" and isinstance(seen["kite"], FakeDhan)
    text = capsys.readouterr().out
    assert "1 MISSING_IN_DHAN contracts" in text and "Dhan_candles" in text and "Kite" not in text


# --- nifty_bhav.py --source dhan ----------------------------------------------------------------

def test_bhav_cli_dhan(tmp_path, monkeypatch, capsys):
    seen = []
    monkeypatch.setattr(nifty_bhav, "run_bhav", lambda d, folder, underlying, bhav_file, log=print, source="kite":
                        seen.append((d, source)) or ("ok", "", None))
    for d in ("2026-10-09", "2026-10-12"):
        (day_of(tmp_path / "NIFTY_FnO_1min_Dhan", d)).mkdir(parents=True)
        (day_of(tmp_path / "NIFTY_FnO_1min_Dhan", d) / "summary.csv").write_text("x")
    assert nifty_bhav.main(["--missing", "--folder", str(tmp_path), "--source", "dhan"]) == 0
    assert seen == [(date(2026, 10, 9), "dhan"), (RUN_DATE, "dhan")]
    assert nifty_bhav.main(["--missing", "--folder", str(tmp_path)]) == 0     # Kite folder: nothing saved
    assert len(seen) == 2

    monkeypatch.setattr(nifty_bhav.bhav_export, "export", lambda folder, underlying, scope, run_date, source="kite":
                        (f"{scope}_{source}.csv", b"x", "text/csv"))
    assert nifty_bhav.main(["--export", DAY, "--csv", "--folder", str(tmp_path), "--source", "dhan"]) == 0
    assert (dhan_day(tmp_path) / "bhavcopy" / "exports" / "all_dhan.csv").exists()
    assert nifty_bhav.main(["--export-days", "--folder", str(tmp_path), "--source", "dhan"]) == 0
    assert (tmp_path / "NIFTY_FnO_1min_Dhan" / "days_dhan.csv").exists()
    with pytest.raises(SystemExit):
        nifty_bhav.main([DAY, "--folder", str(tmp_path), "--source", "dhan", "--underlying", "BANKEX"])


# --- backtest loaders ---------------------------------------------------------------------------

def test_loaders_read_the_dhan_folder(tmp_path, monkeypatch):
    st, _, day, _ = trim_run(tmp_path, monkeypatch)
    df = fno_loader.load_day(str(tmp_path), DAY, source="dhan")
    assert set(df["Ticker"]) == {"NIFTY27OCT26", "NIFTY2500013OCT26CE", "NIFTY2500013OCT26PE", "NIFTY1700013OCT26CE"}
    assert len(fno_loader.load_contract(str(tmp_path), "NIFTY27OCT26", source="dhan")) == 2
    with pytest.raises(FileNotFoundError):
        fno_loader.load_day(str(tmp_path), DAY)                              # not in the Kite folder
    with pytest.raises(FileNotFoundError, match="--source dhan"):
        fno_loader.load_eod(str(tmp_path), DAY, source="dhan")
    (day / "bhavcopy").mkdir()
    fno.write_csv(str(day / "bhavcopy" / "nifty_bhav.csv"), bhav.BHAV_HEADER, bhav_rows())
    eod = fno_loader.load_eod(str(tmp_path), DAY, source="dhan").set_index("Contract")
    assert eod.loc["NIFTY27OCT26", "Last"] == 25120 and eod.loc["NIFTY27OCT26", "Volume_contracts"] == 3


@pytest.mark.parametrize("argv,name", [([DAY], "daily"), (["--missing"], "missing"), (["--refetch-no-trade", DAY], "refetch")])
@pytest.mark.parametrize("flag,setting,expected", [([], True, True), ([], False, False),
                                                   (["--no-trim-to-bhav"], True, False), (["--trim-to-bhav"], False, True)])
def test_cli_dhan_trim_flags(dcli, tmp_path, monkeypatch, argv, name, flag, setting, expected):
    env, calls = dcli
    monkeypatch.setattr(nifty_daily, "trim_to_bhav", lambda: setting)
    assert nifty_daily.main(argv + ["--folder", str(tmp_path), "--source", "dhan"] + flag) == 0
    kind, _, kw = calls[0]
    assert kind == name and kw["trim_to_bhav"] is expected and JOB["trim_to_bhav"] is expected

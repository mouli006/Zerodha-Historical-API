"""Kite F&O runs trimmed to the day's bhavcopy, like Dhan ("Download only contracts in the bhavcopy",
config.trim_to_bhav / .env TRIM_TO_BHAV, the dashboard's trim_bhav switch, nifty_daily.py --trim-to-bhav):
ON keeps only the bhavcopy's contracts, ON without a bhavcopy falls back to the full list (FALLBACK), OFF
is the full list. No network (bhav.fetch_bhav is stubbed), no real .env."""
import csv
import os
from datetime import date

import pytest

import config
import nifty_daily
from backfill import bhav, fno, fno_days
from backfill.job import JOB, JOB_LOCK, job_begin
from conftest import day_of
from test_bhav import row, zip_bytes
from test_fno import ACCESS, RUN_DATE, W1, M1, M2, fake_kite, set_now  # noqa: F401  (fixture)
from test_fno_entrypoints import client, ist_now, lock_free, post, wait_for  # noqa: F401  (fixtures)

DAY = RUN_DATE.isoformat()
ROOT = "NIFTY_FnO_1min"
# In the bhavcopy: both futures, the 13 Oct 25100 CE/PE and 25150 CE (traded or not). Not in it: 25150 PE
# (token 14), the 20 Oct 25100 CE (21) and the 27 Oct 25100 PE (31): strikes Kite lists today, not listed that day.
def listed(day=DAY):
    return [row("IDF", M1.isoformat(), vol=3, last=25124, oi=1000, day=day), row("IDF", M2.isoformat(), vol=1, last=25303, oi=1003, day=day),
            row("IDO", W1.isoformat(), "25100.00", "CE", vol=2, last=124, day=day),
            row("IDO", W1.isoformat(), "25100.00", "PE", vol=1, last=97, day=day),
            row("IDO", W1.isoformat(), "25150.00", "CE", vol=0, day=day)]


ALL_TOKENS = {1, 2, 11, 12, 13, 14, 21, 31}   # every contract Kite is asked for without the trim (15: naming error)
TRIMMED_TOKENS = {1, 2, 11, 12, 13}
# summary.csv's Listed_contracts (options; the bad strike 15 is counted): trimmed 11-13 + 15, full also 14, 21, 31
LISTED_TRIMMED, LISTED_FULL = 4, 7


@pytest.fixture
def nse(monkeypatch):
    """bhav.fetch_bhav stubbed: answers listed(run_date), or raises calls["error"]; every call is recorded."""
    calls = {"fetched": [], "error": None}

    def fetch(run_date, log=print, budget=None, **kw):
        calls["fetched"].append((run_date, budget))
        if calls["error"]:
            raise calls["error"]
        day = run_date.isoformat()
        return zip_bytes(listed(day), day), f"https://x/BhavCopy_NSE_FO_0_0_0_{day.replace('-', '')}_F_0000.csv.zip"
    monkeypatch.setattr(bhav, "fetch_bhav", fetch)
    return calls


def run(tmp_path, trim=None, day=RUN_DATE):
    with JOB_LOCK:
        JOB["state"] = "idle"
    assert job_begin(**fno.job_fields("NIFTY", day, str(tmp_path), 1000, trim_to_bhav=bool(trim)))
    return fno.run_daily("key", ACCESS, day, str(tmp_path), 1000, log=lambda m: None, trim_to_bhav=trim)


def requested(kite):
    return {c[0] for c in kite.calls}


def day_dir(tmp_path, day=DAY):
    return day_of(tmp_path / ROOT, day)


def summary(tmp_path):
    with open(day_dir(tmp_path) / "summary.csv", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def run_log(tmp_path):
    return (day_dir(tmp_path) / "run.log").read_text(encoding="utf-8")


def listed_total(rows):
    return sum(int(r["Listed_contracts"] or 0) for r in rows)


def files(tmp_path):
    d = day_dir(tmp_path)
    return sorted(os.listdir(d / "futures")) + sorted(os.listdir(d / "options"))


# --- the run ------------------------------------------------------------------------------------

def test_trim_on_requests_only_bhavcopy_contracts(tmp_path, fake_kite, nse):
    assert run(tmp_path, trim=True) == "finished_failures"   # 15: naming error; 31 (Kite fails it) not requested
    assert requested(fake_kite) == TRIMMED_TOKENS
    assert nse["fetched"] == [(RUN_DATE, bhav.TRIM_BUDGET)]
    assert files(tmp_path) == ["NIFTY24NOV26.csv", "NIFTY27OCT26.csv", "NIFTY2510013OCT26CE.csv", "NIFTY2510013OCT26PE.csv"]
    log = run_log(tmp_path)
    assert "Contracts: trimmed to bhavcopy (kept 6 of 9)" in log
    assert "Trimmed to 6 contracts listed in NSE's bhavcopy for 2026-10-12 (Kite listed 9; 3 not listed by NSE" in log
    rows = summary(tmp_path)
    assert {r["Trim_to_bhav"] for r in rows} == {"ON"} and list(rows[0])[-1] == "Trim_to_bhav"
    assert listed_total(rows) == LISTED_TRIMMED                            # the contracts requested, not all of Kite's list
    assert not [r for r in rows if r["Expiry"] == "2026-10-20"]   # no 20 Oct contract was listed that day
    assert (day_dir(tmp_path) / "bhavcopy" / "nifty_bhav.csv").exists()
    assert "Bhavcopy not available" not in JOB["message"]


@pytest.mark.parametrize("error,reason", [
    (bhav.BhavNotAvailable("NSE has no F&O bhavcopy for this day yet (404: test)."), "NSE has no F&O bhavcopy"),
    (bhav.BhavTimeout("bhavcopy fetch timed out after 30 s"), "bhavcopy fetch timed out"),
    (RuntimeError("boom"), "RuntimeError: boom"),
])
def test_trim_on_without_bhavcopy_uses_the_full_list(tmp_path, fake_kite, nse, error, reason):
    nse["error"] = error
    assert run(tmp_path, trim=True) == "finished_failures"   # never refused; 31 fails as without the trim
    assert requested(fake_kite) == ALL_TOKENS
    log = run_log(tmp_path)
    assert f"Contracts: full list (bhavcopy not available: {reason}" in log
    assert f"Bhavcopy not available ({reason}" in log and "): full contract list used" in log
    assert f"Bhavcopy not available ({reason}" in JOB["message"] and "full contract list used." in JOB["message"]
    rows = summary(tmp_path)
    assert {r["Trim_to_bhav"] for r in rows} == {"FALLBACK"} and listed_total(rows) == LISTED_FULL


def test_trim_off_uses_the_full_list(tmp_path, fake_kite, nse):
    assert run(tmp_path, trim=False) == "finished_failures"
    assert requested(fake_kite) == ALL_TOKENS and nse["fetched"] == []
    assert "Contracts: full list (trim OFF)" in run_log(tmp_path)
    assert {r["Trim_to_bhav"] for r in summary(tmp_path)} == {"OFF"}


def test_trim_none_uses_the_setting(tmp_path, fake_kite, nse, trim_on):
    assert run(tmp_path, trim=None) == "finished_failures" and requested(fake_kite) == TRIMMED_TOKENS


def test_trim_none_with_setting_off(tmp_path, fake_kite, nse):   # conftest: the setting is OFF
    assert run(tmp_path, trim=None) == "finished_failures" and requested(fake_kite) == ALL_TOKENS


def test_cli_prints_the_fallback(tmp_path, fake_kite, nse, capsys):
    nse["error"] = bhav.BhavNotAvailable("not published (404: test)")
    assert job_begin(**fno.job_fields("NIFTY", RUN_DATE, str(tmp_path), 1000, trim_to_bhav=True))
    fno.run_daily("key", ACCESS, RUN_DATE, str(tmp_path), 1000, trim_to_bhav=True)   # log=print, as the CLI
    assert "Bhavcopy not available (not published (404: test)): full contract list used" in capsys.readouterr().out


def test_bhavcopy_check_reuses_the_trim_download(tmp_path, fake_kite, nse, monkeypatch):
    monkeypatch.setattr(fno, "AUTO_BHAV", True)
    assert run(tmp_path, trim=True) == "finished_failures"
    assert nse["fetched"] == [(RUN_DATE, bhav.TRIM_BUDGET)]           # one download, no second for the check
    assert (day_dir(tmp_path) / "bhavcopy" / "bhav_check.json").exists()
    rows = summary(tmp_path)                                          # rewritten by the check (rewrite_summary)
    assert {r["Trim_to_bhav"] for r in rows} == {"ON"} and listed_total(rows) == LISTED_TRIMMED
    assert rows[0]["Kite_files"] != ""


def test_rewrite_summary_off_counts_the_full_dump(tmp_path, fake_kite, nse, monkeypatch):
    monkeypatch.setattr(fno, "AUTO_BHAV", True)
    assert run(tmp_path, trim=False) == "finished_failures"   # OFF: the check after it still fetches the bhavcopy
    rows = summary(tmp_path)
    assert {r["Trim_to_bhav"] for r in rows} == {"OFF"} and listed_total(rows) == LISTED_FULL


def test_full_day_rerun_with_trim_keeps_its_files(tmp_path, fake_kite, nse):
    assert run(tmp_path, trim=False) == "finished_failures"
    before = files(tmp_path)
    assert "NIFTY2510020OCT26CE.csv" in before                      # not in the bhavcopy
    fake_kite.calls.clear()
    assert run(tmp_path, trim=True) == "finished_failures"
    assert files(tmp_path) == before                                  # nothing deleted
    assert requested(fake_kite) == set()                              # every listed contract was already saved
    assert {r["Trim_to_bhav"] for r in summary(tmp_path)} == {"ON"}
    assert "Contracts: full list (trim OFF)" in run_log(tmp_path) and "Contracts: trimmed to bhavcopy" in run_log(tmp_path)


def test_dhan_trim_off_uses_dhans_full_list(tmp_path, monkeypatch):
    from test_dhan_fno import no_nse, trim_run   # noqa: F401
    calls = []
    monkeypatch.setattr(bhav, "fetch_bhav", lambda *a, **k: calls.append(a) or (_ for _ in ()).throw(AssertionError("NSE")))
    st, req, day, log = trim_run(tmp_path, monkeypatch)                # trim setting OFF (conftest)
    assert st == "finished" and len(req) == 4 and calls == []
    assert "Contracts: full list (trim OFF)" in log and "Trimmed" not in log


# --- run_missing ----------------------------------------------------------------------------------

@pytest.mark.parametrize("trim", [True, False])
def test_run_missing_passes_the_setting_to_every_day(tmp_path, fake_kite, nse, monkeypatch, trim):
    seen = []
    real = fno.run_daily
    monkeypatch.setattr(fno, "run_daily", lambda *a, **kw: seen.append((a[2], kw["trim_to_bhav"])) or real(*a, **kw))
    days = [date(2026, 10, 9), RUN_DATE]
    window = fno.Window(date(2026, 10, 7), RUN_DATE, W1, date(2026, 10, 6), False)
    monkeypatch.setattr(fno, "load_window", lambda *a, **k: (fake_kite("k").instruments("NFO"), window))
    assert job_begin(**fno.job_fields("NIFTY", None, str(tmp_path), 1000, label="x", trim_to_bhav=trim))
    fno.run_missing("key", ACCESS, str(tmp_path), 1000, log=lambda m: None, dates=days, trim_to_bhav=trim)
    assert seen == [(d, trim) for d in days]
    for d in days:
        assert {r["Trim_to_bhav"] for r in csv.DictReader(open(day_of(tmp_path / ROOT, d) / "summary.csv",
                                                                  encoding="utf-8"))} == {"ON" if trim else "OFF"}


def test_run_missing_none_uses_the_setting(tmp_path, monkeypatch, trim_on):
    seen = []
    monkeypatch.setattr(fno, "connect", lambda *a, **k: (object(), "u", ""))
    window = fno.Window(date(2026, 10, 7), RUN_DATE, W1, date(2026, 10, 6), False)
    monkeypatch.setattr(fno, "load_window", lambda *a, **k: ([], window))
    monkeypatch.setattr(fno, "run_daily", lambda *a, **kw: seen.append(kw["trim_to_bhav"]) or "finished")
    fno.run_missing("key", ACCESS, str(tmp_path), 1000, log=lambda m: None, dates=[RUN_DATE])
    assert seen == [True]


def test_refetch_no_trade_passes_the_setting(tmp_path, monkeypatch):
    d = tmp_path / ROOT / DAY
    d.mkdir(parents=True)
    (d / "summary.csv").write_text("Expiry\n")
    seen = []
    monkeypatch.setattr(fno, "run_daily", lambda *a, **kw: seen.append(kw["trim_to_bhav"]) or "finished")
    fno.refetch_no_trade("key", ACCESS, RUN_DATE, str(tmp_path), 1000, log=lambda m: None, trim_to_bhav=True)
    assert seen == [True]


# --- the day count --------------------------------------------------------------------------------

def test_day_index_and_marker_show_trim(tmp_path, fake_kite, nse):
    assert run(tmp_path, trim=True) == "finished_failures"
    root = tmp_path / ROOT
    (root / "2026-10-09").mkdir()                                      # an old-style day without summary.csv
    marker = fno_days.write_day_count(str(root))
    rows = fno_days.read_day_index(str(root))
    assert fno_days.INDEX_COLUMNS[-1] == "Trim_to_bhav"
    assert [(r["Day"], r["Trim_to_bhav"]) for r in rows] == [("2026-10-09", ""), (DAY, "ON")]
    text = open(marker, encoding="utf-8").read().splitlines()
    assert text[2].split()[-1] == "Trim_to_bhav" and text[-1].split()[-1] == "ON"
    assert "_trim" not in os.path.basename(str(day_dir(tmp_path))).lower()   # not in the folder name


# --- the setting ----------------------------------------------------------------------------------

@pytest.mark.parametrize("value,expected", [("", True), ("ON", True), ("off", False), ("0", False), ("yes", True),
                                            ("junk", True)])
def test_config_trim_to_bhav(monkeypatch, value, expected):
    monkeypatch.undo()   # the real config.trim_to_bhav, with a stubbed .env
    monkeypatch.setattr(config, "load_env", lambda: {"TRIM_TO_BHAV": value} if value else {})
    assert config.TRIM_TO_BHAV is True and config.trim_to_bhav() is expected


def test_env_example_lists_the_key():
    with open(os.path.join(os.path.dirname(config.__file__), ".env.example"), encoding="utf-8") as f:
        assert "\nTRIM_TO_BHAV=\n" in f.read()


# --- the dashboard --------------------------------------------------------------------------------

def test_success_page_has_trim_switch(client, monkeypatch):
    html = client.get("/success").get_data(as_text=True)
    assert "Download only contracts in the bhavcopy" in html and '"trim_to_bhav": false' in html
    import dashboard
    monkeypatch.setattr(dashboard, "trim_to_bhav", lambda: True)
    assert '"trim_to_bhav": true' in client.get("/success").get_data(as_text=True)


@pytest.mark.parametrize("form,expected", [({"trim_bhav": "1"}, True), ({"trim_bhav": "0"}, False), ({}, False)])
@pytest.mark.parametrize("mode", ["date", "range", "missing", "refetch"])
def test_fno_start_passes_trim(client, tmp_path, monkeypatch, form, expected, mode):
    seen = {}

    def fake(*a, trim_to_bhav=None, **kw):
        seen["trim"] = trim_to_bhav
        with JOB_LOCK:
            JOB["state"] = "finished"
        return ("finished", "") if mode == "refetch" else "finished"
    for name in ("run_daily", "run_missing", "refetch_no_trade"):
        monkeypatch.setattr(fno, name, fake)
    extra = {"date": {}, "range": {"date": None, "date_from": "2026-10-10", "date_to": "2026-10-12"},
             "missing": {"date": None, "missing": "1"}, "refetch": {"refetch": "1", "keep_zero": "1"}}[mode]
    if mode == "refetch":
        d = tmp_path / ROOT / DAY
        d.mkdir(parents=True)
        (d / "summary.csv").write_text("Expiry\n")
    assert "error" not in post(client, tmp_path, **form, **extra)
    assert wait_for(lambda: seen) and seen == {"trim": expected} and JOB["trim_to_bhav"] is expected
    assert wait_for(lambda: lock_free(tmp_path)), "lock not released"


def test_fno_start_trim_absent_uses_the_setting(client, tmp_path, monkeypatch):
    import dashboard
    monkeypatch.setattr(dashboard, "trim_to_bhav", lambda: True)
    seen = {}

    def fake(*a, trim_to_bhav=None, **kw):
        seen["trim"] = trim_to_bhav
        with JOB_LOCK:
            JOB["state"] = "finished"
    monkeypatch.setattr(fno, "run_daily", fake)
    assert "error" not in post(client, tmp_path)
    assert wait_for(lambda: seen) and seen == {"trim": True}
    assert wait_for(lambda: lock_free(tmp_path)), "lock not released"


# --- the CLI (Kite; Dhan in test_dhan_parity.py) -------------------------------------------------

@pytest.fixture
def kcli(monkeypatch, tmp_path, ist_now):
    calls = []
    monkeypatch.setattr(nifty_daily, "load_env", lambda: {"KITE_API_KEY": "k", "KITE_ACCESS_TOKEN": "t"})
    monkeypatch.setattr(nifty_daily, "weekly_expiry_weekday", lambda underlying="NIFTY": 1)
    monkeypatch.setattr(nifty_daily, "keep_zero_volume", lambda: False)

    def fake(name):
        def go(*a, **kw):
            calls.append((name, kw["trim_to_bhav"], JOB["trim_to_bhav"]))
            with JOB_LOCK:
                JOB["state"] = "finished"
            return ("finished", "") if name == "refetch" else "finished"
        return go
    monkeypatch.setattr(nifty_daily, "run_daily", fake("daily"))
    monkeypatch.setattr(nifty_daily, "run_missing", fake("missing"))
    monkeypatch.setattr(nifty_daily, "refetch_no_trade", fake("refetch"))
    with JOB_LOCK:
        JOB["state"] = "idle"
    return calls


@pytest.mark.parametrize("argv,name", [([DAY], "daily"), (["--missing"], "missing"), (["--refetch-no-trade", DAY], "refetch")])
@pytest.mark.parametrize("flag,setting,expected", [([], False, False), ([], True, True),
                                                   (["--trim-to-bhav"], False, True), (["--no-trim-to-bhav"], True, False)])
def test_cli_trim_flags(kcli, tmp_path, monkeypatch, argv, name, flag, setting, expected):
    monkeypatch.setattr(nifty_daily, "trim_to_bhav", lambda: setting)
    assert nifty_daily.main(argv + ["--folder", str(tmp_path)] + flag) == 0
    assert kcli == [(name, expected, expected)]

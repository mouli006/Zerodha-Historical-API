"""fno_redownload.py and backfill/fno_redownload.py: re-download a checked day's MISSING_IN_KITE contracts with the
real fno.redownload_contract and a fake Dhan / Kite client. A synthetic Dhan day in tmp_path (with_zero_volume store
under a stubbed FNO_RAW_DHAN), IST clock patched; no network, no real .env."""
import json
import os
import sqlite3
from datetime import date, datetime

import pytest
from kiteconnect import exceptions as kex

import fno_redownload as cli
from backfill import bhav, dhan_api, fno
from backfill import fno_redownload as fr
from backfill.job import JOB, JOB_LOCK
from test_bhav import kite_file, row

IST = fno.IST
RUN_DATE = date(2026, 10, 12)
DAY = RUN_DATE.isoformat()
NOW = datetime(2026, 10, 20, 16, 0, tzinfo=IST)   # 13OCT26 has expired, 27OCT26 is live
FOLDER_NAME = f"{DAY}_exp-2026-10-13_withZeroVol_dl-2026-10-12_2356"
EXPIRED = "NIFTY2500013OCT26CE"
LIVE = ["NIFTY2500027OCT26PE", "NIFTY2510027OCT26CE", "NIFTY2510027OCT26PE"]
TOKENS = {"40002": LIVE[0], "40003": LIVE[1], "40004": LIVE[2]}


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    class Blocked:
        def __init__(self, *a, **kw):
            raise AssertionError("network used")
    monkeypatch.setattr(bhav.requests, "Session", Blocked)
    monkeypatch.setattr(dhan_api.requests, "get", Blocked)
    monkeypatch.setattr(dhan_api.requests, "post", Blocked)
    monkeypatch.setattr(fno, "now_ist", lambda: NOW)
    with JOB_LOCK:
        JOB["state"] = "idle"


def inst(token, expiry, kind, strike):
    return {"instrument_token": token, "tradingsymbol": f"NIFTY-{expiry}-{strike}-{kind}", "name": "NIFTY",
            "expiry": expiry, "strike": strike, "instrument_type": kind,
            "dhan_instrument": "FUTIDX" if kind == "FUT" else "OPTIDX"}


@pytest.fixture
def world(tmp_path):
    """FNO_RAW_DHAN = tmp/FNO; a checked day in its with_zero_volume store: the future has a file, one expired and
    three live options are MISSING_IN_KITE. A without_zero_volume store with one file, never to be touched."""
    base = tmp_path / "FNO"
    folder = base / "with_zero_volume"
    day = folder / "NIFTY_FnO_1min_Dhan" / FOLDER_NAME
    kite_file(str(day), "NIFTY27OCT26", [22600.0], vol=150)
    (day / "summary.csv").write_text("Expiry\n", encoding="utf-8")
    (day / "run.log").write_text("", encoding="utf-8")
    fno.save_instruments(str(day / "instruments_NSE_FNO.csv"), [
        inst("35001", "2026-10-27", "FUT", 0.0), inst("40001", "2026-10-13", "CE", 25000.0),
        inst("40002", "2026-10-27", "PE", 25000.0), inst("40003", "2026-10-27", "CE", 25100.0),
        inst("40004", "2026-10-27", "PE", 25100.0)])
    rows = [row("IDF", "2026-10-27", vol=3, last=22600, oi=1000),
            row("IDO", "2026-10-13", "25000.00", "CE", vol=2, last=10, oi=1000),
            row("IDO", "2026-10-27", "25000.00", "PE", vol=4, last=12, oi=1000),
            row("IDO", "2026-10-27", "25100.00", "CE", vol=4, last=12, oi=1000),
            row("IDO", "2026-10-27", "25100.00", "PE", vol=4, last=12, oi=1000)]
    (day / "bhavcopy").mkdir()
    fno.write_csv(str(day / "bhavcopy" / "nifty_bhav.csv"), bhav.BHAV_HEADER, rows)
    bhav.check_saved(str(day), RUN_DATE, log=lambda m: None)
    without = base / "without_zero_volume" / "NIFTY_FnO_1min_Dhan" / DAY
    without.mkdir(parents=True)
    (without / "keep.txt").write_text("x", encoding="utf-8")
    env = {"FNO_RAW_DHAN": str(base), "DHAN_ACCESS_TOKEN": "SECRET-DHAN-TOKEN", "DHAN_TOKEN_EXPIRY": "2099-01-01T09:00:00"}
    return {"base": base, "folder": str(folder), "day": day, "env": env, "without": base / "without_zero_volume"}


class FakeDhan:
    """Candles for every token; tokens in fail raise a Kite-style error, in auth a refused token. profile() raises
    while token_bad is set."""

    def __init__(self, fail=(), auth=(), interrupt=()):
        self.asked, self.fail, self.auth, self.interrupt, self.token_bad = [], set(fail), set(auth), set(interrupt), False

    def learn(self, instruments):
        pass

    def historical_data(self, token, start, end, interval, continuous=False, oi=False):
        self.asked.append(token)
        if token in self.interrupt:
            raise KeyboardInterrupt
        if token in self.auth:
            self.token_bad = True
            raise kex.TokenException("Invalid token")
        if token in self.fail:
            raise kex.GeneralException("server error", code=500)
        t0 = datetime(2026, 10, 12, 9, 15, tzinfo=IST)
        return [{"date": t0, "open": 12, "high": 13, "low": 11, "close": 12, "volume": 200, "oi": 1000}]

    def profile(self):
        if self.token_bad:
            raise kex.TokenException("Invalid token")
        return {"user_id": "1000000001", "data_plan": "Active"}


def tree(path):
    out = {}
    for f, _, names in os.walk(path):
        for n in names:
            if n != ".lock":
                p = os.path.join(f, n)
                out[os.path.relpath(p, path)] = (os.path.getsize(p), os.stat(p).st_mtime_ns)
    return out


def go(w, client=None, lines=None, **kw):
    lines = [] if lines is None else lines
    connect = (lambda: (client, "", "tok", "")) if client is not None else (lambda: pytest.fail("signed in"))
    return fr.run(RUN_DATE, "dhan", "NIFTY", w["folder"], w["env"], log=lines.append, connect=connect,
                  speed=1000, **kw)


def missing_now(w):
    return fno.missing_in_kite(str(w["day"]))


# --- dry run --------------------------------------------------------------------------------------

def test_dry_run_lists_and_writes_nothing(world):
    before, lines = tree(world["base"]), []
    assert go(world, lines=lines) == 0
    text = "\n".join(lines)
    assert "MISSING_IN_KITE in bhav_compare.csv: 4" in text
    assert "Live (to fetch): 3, expired (skipped): 1" in text
    assert "Speed: 1000 requests/s" in text
    assert os.path.join(str(world["base"]), "archive", "2026-10-20_1600_redownload_NIFTY_dhan_2026-10-12") in text
    assert "2026-10-13  0    1   0   1      0      1" in text and "2026-10-27  0    1   2   3      3      0" in text
    assert tree(world["base"]) == before and not (world["base"] / "archive").exists()


def test_default_speed_is_2_5_for_dhan():
    assert fr.SPEED == 2.5


# --- refusals --------------------------------------------------------------------------------------

def test_refuses_without_a_check(world):
    os.remove(world["day"] / "bhavcopy" / "bhav_compare.csv")
    lines = []
    assert go(world, lines=lines, apply=True) == 1 and "run the check first" in lines[-1]


def test_refuses_without_missing(world):
    for n in [EXPIRED] + LIVE:
        kite_file(str(world["day"]), n, [12.0], vol=200)
    bhav.check_saved(str(world["day"]), RUN_DATE, log=lambda m: None)
    lines = []
    assert go(world, lines=lines, apply=True) == 1 and "no MISSING_IN_KITE contracts" in lines[-1]


def test_refuses_unknown_only_and_store_mismatch(world):
    lines = []
    assert go(world, lines=lines, only=["NIFTY99999OCT26CE"]) == 1 and "NIFTY99999OCT26CE" in lines[-1]
    assert go(world, lines=lines, keep_zero=False) == 1 and "with_zero_volume store" in lines[-1]


def test_refuses_busy_lock_and_running_job(world):
    client, lines = FakeDhan(), []
    lock = fno.acquire_lock(world["folder"], "NIFTY", "test", "dhan")
    try:
        assert go(world, client, lines, apply=True) == 1 and "in progress" in lines[-1]
    finally:
        lock.release()
    with JOB_LOCK:
        JOB["state"] = "running"
    try:
        assert go(world, client, lines, apply=True) == 1 and "job is running" in lines[-1]
    finally:
        with JOB_LOCK:
            JOB["state"] = "idle"
    assert client.asked == [] and not (world["base"] / "archive").exists()


def test_backup_failure_refuses(world, monkeypatch):
    def bad(src, dst):
        raise OSError("disk full")
    monkeypatch.setattr(fr, "copy_checked", bad)
    client, lines = FakeDhan(), []
    assert go(world, client, lines, apply=True) == 1
    assert "backup failed (disk full)" in lines[-1] and client.asked == []


def test_token_refused_at_start(world):
    lines = []
    code = fr.run(RUN_DATE, "dhan", "NIFTY", world["folder"], world["env"], apply=True, speed=1000, log=lines.append,
                  connect=lambda: (None, "", "", "Dhan refused the access token."))
    assert code == 2 and "refused the access token" in lines[-1]
    assert missing_now(world) == [EXPIRED] + LIVE


# --- apply -----------------------------------------------------------------------------------------

def test_apply_saves_backs_up_and_checks_once(world, monkeypatch):
    before_without = tree(world["without"])
    original_files = tree(world["day"])
    calls = []
    real = bhav.check_saved
    monkeypatch.setattr(bhav, "check_saved", lambda *a, **kw: calls.append(a) or real(*a, **kw))
    client, lines = FakeDhan(), []
    assert go(world, client, lines, apply=True) == 0
    assert sorted(client.asked) == sorted(TOKENS) and len(calls) == 1
    for n in LIVE:
        assert (world["day"] / "options" / f"{n}.csv").exists()
    assert not (world["day"] / "options" / f"{EXPIRED}.csv").exists()
    assert missing_now(world) == [EXPIRED]
    assert ("Re-download summary: saved 3, still no candles 0, skipped expired 1, failed 0; Missing 4 -> 1; "
            "severity major") in lines[-1]
    log = (world["day"] / "run.log").read_text(encoding="utf-8")
    assert log.count("=== Re-download of") == 3 and "=== fno_redownload.py: 3 of 4" in log and "Missing 4 -> 1" in log
    db = sqlite3.connect(os.path.join(world["folder"], "progress.sqlite"))
    rows = db.execute("SELECT symbol, status FROM chunks WHERE run = ?", (fno.run_key("NIFTY", RUN_DATE, "dhan"),)).fetchall()
    db.close()
    assert len(rows) == 3 and {s for _, s in rows} == {"DONE"}
    # the backup: the day as it was, every file in the manifest with its SHA-256
    archive = world["base"] / "archive" / "2026-10-20_1600_redownload_NIFTY_dhan_2026-10-12"
    copy = archive / FOLDER_NAME
    man = json.loads((archive / fr.MANIFEST).read_text(encoding="utf-8"))
    assert {f["path"] for f in man["files"]} == {p.replace(os.sep, "/") for p in original_files}
    assert all(fr.sha256(str(copy / f["path"])) == f["sha256"] for f in man["files"])
    assert not (copy / "options" / f"{LIVE[0]}.csv").exists()
    assert tree(world["without"]) == before_without


def test_limit_only_and_rerun_continues(world, monkeypatch):
    client = FakeDhan()
    assert go(world, client, limit=1, apply=True) == 0 and client.asked == ["40002"]
    assert missing_now(world) == [EXPIRED] + LIVE[1:]
    lines = []
    assert go(world, lines=lines, apply=True) == 1 and "already exists" in lines[-1]   # same minute: archive taken
    client2 = FakeDhan()
    monkeypatch.setattr(fno, "now_ist", lambda: datetime(2026, 10, 20, 16, 1, tzinfo=IST))   # a new archive
    assert go(world, client2, only=[LIVE[2]], apply=True) == 0 and client2.asked == ["40004"]
    assert missing_now(world) == [EXPIRED, LIVE[1]]


def test_files_already_on_disk_are_not_fetched(world):
    kite_file(str(world["day"]), LIVE[0], [12.0], vol=200)   # saved after the check (e.g. a stop before it)
    lines, client = [], FakeDhan()
    assert go(world, client, lines, apply=True) == 0
    assert "already have a file (skipped): 1" in "\n".join(lines) and "40002" not in client.asked


def test_token_failure_mid_run_stops_and_rerun_continues(world, monkeypatch):
    client, lines = FakeDhan(auth={"40003"}), []
    assert go(world, client, lines, apply=True) == 2
    text = "\n".join(lines)
    assert "Stopped early after 2 of 3" in text and "refused the access token" in text
    assert (world["day"] / "options" / f"{LIVE[0]}.csv").exists()
    assert missing_now(world) == [EXPIRED] + LIVE[1:]                  # the check ran
    assert "not tried 1" in lines[-1] and "Missing 4 -> 3" in lines[-1]
    monkeypatch.setattr(fno, "now_ist", lambda: datetime(2026, 10, 20, 16, 5, tzinfo=IST))
    client2 = FakeDhan()
    assert go(world, client2, apply=True) == 0 and client2.asked == ["40003", "40004"]
    assert missing_now(world) == [EXPIRED]


def test_network_failures_in_a_row_stop(world):
    client, lines = FakeDhan(fail=set(TOKENS)), []
    assert go(world, client, lines, apply=True) == 2
    assert "3 contracts in a row could not be fetched" in "\n".join(lines)
    assert "failed 3" in lines[-1] and missing_now(world) == [EXPIRED] + LIVE


def test_one_failure_does_not_stop(world):
    client, lines = FakeDhan(fail={"40002"}), []
    assert go(world, client, lines, apply=True) == 0
    assert "saved 2" in lines[-1] and "failed 1" in lines[-1] and missing_now(world) == [EXPIRED, LIVE[0]]


def test_ctrl_c_stops_and_checks(world):
    client, lines = FakeDhan(interrupt={"40003"}), []
    assert go(world, client, lines, apply=True) == 2
    assert "Interrupted" in "\n".join(lines) and missing_now(world) == [EXPIRED] + LIVE[1:]


def test_no_candles_counted(world, monkeypatch):
    client, lines = FakeDhan(), []
    monkeypatch.setattr(client, "historical_data", lambda *a, **kw: [])
    assert go(world, client, lines, apply=True) == 0
    assert "still no candles 3" in lines[-1] and "Missing 4 -> 4" in lines[-1]


def test_outcome():
    assert fr.outcome("X", True, "X: saved 3 bars") == "saved"
    assert fr.outcome("X", False, "X: Dhan returned no candles for 2026-10-12, so no file was written.") == "no_candles"
    assert fr.outcome("X", False, "X expired on 2026-10-13; Dhan no longer serves it.") == "expired"
    assert fr.outcome("X", False, "X: gave up after 3 tries") == "fetch_failed"
    assert fr.outcome("X", False, "X is not in the instrument list saved for 2026-10-12.") == "failed"


# --- CLI -------------------------------------------------------------------------------------------

@pytest.fixture
def cli_env(world, monkeypatch):
    monkeypatch.setattr(cli, "load_env", lambda: dict(world["env"]))
    client = FakeDhan()
    monkeypatch.setattr(cli, "sign_in", lambda source: ("", "tok", {"kite": client, "user": "1", "source": "dhan"}, 0))
    return world, client


def test_cli_dry_run_and_apply(cli_env, capsys):
    w, client = cli_env
    assert cli.main([DAY, "--source", "dhan", "--underlying", "NIFTY"]) == 0
    out = capsys.readouterr().out
    assert "Live (to fetch): 3" in out and "Speed: 2.5 requests/s" in out and "Dry run" in out and client.asked == []
    assert cli.main([DAY, "--source", "dhan", "--underlying", "NIFTY", "--only", LIVE[0].lower(), "--apply"]) == 0
    assert client.asked == ["40002"] and "Speed: 2.5 requests/s" in capsys.readouterr().out


def test_cli_errors(cli_env, monkeypatch):
    with pytest.raises(SystemExit) as e:
        cli.main(["2026-13-01", "--source", "dhan", "--underlying", "NIFTY"])
    assert e.value.code == 1
    with pytest.raises(SystemExit) as e:
        cli.main([DAY, "--source", "dhan", "--underlying", "NIFTY", "--limit", "0"])
    assert e.value.code == 1
    with pytest.raises(SystemExit) as e:
        cli.main([DAY, "--underlying", "NIFTY"])
    assert e.value.code == 1
    monkeypatch.setattr(cli, "sign_in", lambda source: ("", "", {}, 2))
    assert cli.main([DAY, "--source", "dhan", "--underlying", "NIFTY", "--apply"]) == 2
    assert cli.main([DAY, "--source", "kite", "--underlying", "NIFTY"]) == 1   # no Kite day in the store

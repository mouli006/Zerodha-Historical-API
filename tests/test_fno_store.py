"""F&O raw folders and the backtest store: the FNO_RAW_* / FNO_BACKTEST_DIR settings, fno_store.py move-raw
(backfill/fno_store.py), publish (backfill/fno_publish.py) and the loader (backfill/fno_backtest.py).
tmp_path only, .env stubbed, no network."""
import csv
import hashlib
import json
import os
import sqlite3
from datetime import date, datetime, timedelta

import pandas as pd
import pytest

import config
import dashboard
import dhan
import fno_store as cli
import nifty_bhav
import nifty_daily
from backfill import bhav, fno, fno_backtest, fno_days, fno_loader, fno_publish
from backfill import fno_store as store
from backfill.job import JOB, JOB_LOCK, job_begin
from test_bhav import row

IST = fno.IST
D1, D2, D3 = "2026-10-07", "2026-10-08", "2026-10-09"
W1, W2, M1 = date(2026, 10, 13), date(2026, 10, 20), date(2026, 10, 27)


# --- fake raw days -----------------------------------------------------------------------------------

def bars(day, closes, vol=10, oi=None):
    t0 = datetime.fromisoformat(day + "T09:15:00").replace(tzinfo=IST)
    oi = oi or [1000 + k for k in range(len(closes))]
    return [{"date": t0 + timedelta(minutes=k), "open": c, "high": c + 1, "low": c - 1, "close": c,
             "volume": vol, "oi": oi[k]} for k, c in enumerate(closes)]


def make_day(raw, day, underlying="NIFTY", source="kite", form="new", keep_zero=True, check="ok", bhav_copy=True,
             summary=True, trim="ON", step=50, counts=None):
    """A saved day: one future, a traded CE (OI with a gap), a zero-volume PE, summary.csv, the instrument list,
    the bhavcopy and bhav_check.json. form: "new" (exp/zero/dl name) or "old" (<YYYY-MM-DD>)."""
    root = fno.fno_root(raw, underlying, source)
    base = 25000 if underlying == "NIFTY" else 75000
    name = day if form == "old" else fno.day_folder_name(day, W1, keep_zero, datetime(2026, 10, 14, 9, 5, tzinfo=IST))
    d = os.path.join(root, name)
    fut = fno.contract_name("FUT", M1, None, underlying)
    ce = fno.contract_name("CE", W1, base, underlying)
    pe = fno.contract_name("PE", W1, base + step, underlying)
    fno.write_contract(os.path.join(d, "futures", fut + ".csv"), fut, bars(day, [25100.0, 25101.5, 25102.0]), True)
    fno.write_contract(os.path.join(d, "options", ce + ".csv"), ce, bars(day, [120.0, 121.0, 122.0], oi=[500, 0, 520]), True)
    fno.write_contract(os.path.join(d, "options", pe + ".csv"), pe, bars(day, [80.0, 80.0], vol=0), True)
    fno.write_csv(os.path.join(d, f"instruments_{fno.exchange_of(underlying, source)}.csv"),
                  ["instrument_type", "expiry", "strike", "name", "lot_size"],
                  [{"instrument_type": "FUT", "expiry": M1, "strike": 0, "name": underlying, "lot_size": 65},
                   {"instrument_type": "CE", "expiry": W1, "strike": base, "name": underlying, "lot_size": 65}])
    if summary:
        fno.write_csv(os.path.join(d, "summary.csv"), fno.SUMMARY_COLUMNS,
                      [{**fno.blank_row(), "Expiry": W1.isoformat(), "Bucket": "Current week",
                        "Keep_zero_volume": fno.on_off(keep_zero), "Trim_to_bhav": trim}])
    if bhav_copy:
        out = os.path.join(d, "bhavcopy")
        os.makedirs(out, exist_ok=True)
        fno.write_csv(os.path.join(out, f"{underlying.lower()}_bhav.csv"), bhav.BHAV_HEADER,
                      [row("IDF", M1.isoformat(), vol=3, last=25102, close=25102, settle=25103, oi=9000, sym=underlying,
                           lot=65, day=day),
                       row("IDO", W1.isoformat(), f"{base}.00", "CE", vol=2, last=122, close=122, settle=122, oi=520,
                           sym=underlying, lot=65, day=day),
                       row("IDO", W2.isoformat(), f"{base}.00", "CE", vol=0, sym=underlying, lot=65, day=day)])
        if check:
            with open(os.path.join(out, "bhav_check.json"), "w", encoding="utf-8") as f:
                json.dump({"status": "ok" if check == "ok" else "diffs", "severity": "" if check == "ok" else check,
                           "line": "", "counts": counts or {}}, f)
    os.makedirs(root, exist_ok=True)
    return d


def tree(folder):
    """{relative path: sha256} of every file under folder."""
    out = {}
    for f, _, names in os.walk(folder):
        for n in names:
            p = os.path.join(f, n)
            with open(p, "rb") as fh:
                out[os.path.relpath(p, folder)] = hashlib.sha256(fh.read()).hexdigest()
    return out


@pytest.fixture
def logs():
    lines = []
    return lines, lines.append


# --- Part 1: settings ------------------------------------------------------------------------------

def test_fno_raw_dir_falls_back_to_kite_data_dir():
    assert config.fno_raw_dir("kite", {}) == config.DEFAULT_DATA_DIR
    assert config.fno_raw_dir("dhan", {"KITE_DATA_DIR": "D:/old"}) == "D:/old"
    env = {"KITE_DATA_DIR": "D:/old", "FNO_RAW_KITE": "D:/fno/raw/kite", "FNO_RAW_DHAN": "D:/fno/raw/dhan"}
    assert config.fno_raw_dir("kite", env) == "D:/fno/raw/kite" and config.fno_raw_dir("dhan", env) == "D:/fno/raw/dhan"
    assert config.fno_backtest_dir({}) == "" and config.fno_backtest_dir({"FNO_BACKTEST_DIR": "D:/b"}) == "D:/b"


def test_env_example_has_the_keys():
    with open(os.path.join(os.path.dirname(config.__file__), ".env.example"), encoding="utf-8") as f:
        text = f.read()
    assert all(f"\n{k}=\n" in text for k in ("FNO_RAW_KITE", "FNO_RAW_DHAN", "FNO_BACKTEST_DIR"))


def kite_client(monkeypatch, env):
    from test_fno_entrypoints import WINDOW
    monkeypatch.setattr(dashboard, "load_env", lambda: dict(env))
    monkeypatch.setattr(dashboard, "data_dir", lambda: env.get("KITE_DATA_DIR", "C:/data"))
    monkeypatch.setattr(dashboard, "credentials", lambda: ("key", ""))
    monkeypatch.setattr(dashboard, "keep_zero_volume", lambda: False)
    monkeypatch.setattr(dashboard, "fno_window", lambda folder, underlying: (WINDOW, ""))
    saved = []
    monkeypatch.setattr(dashboard, "save_env", lambda updates: saved.append(updates))
    from app import app
    with JOB_LOCK:
        JOB.update(state="idle", folder="")
    c = app.test_client()
    with c.session_transaction() as s:
        s["access_token"] = "x"
        s["user"] = {"user_id": "AB1234", "user_name": "Demo", "email": "", "broker": "ZERODHA", "login_time": ""}
    return c, saved


def boot_of(html):
    start = html.index('<script id="boot" type="application/json">') + len('<script id="boot" type="application/json">')
    return json.loads(html[start:html.index("</script>", start)])


@pytest.mark.parametrize("env,folder", [({"KITE_DATA_DIR": "C:/old"}, "C:/old"),
                                        ({"KITE_DATA_DIR": "C:/old", "FNO_RAW_KITE": "D:/fno/raw/kite"}, "D:/fno/raw/kite")])
def test_kite_card_start_folder(monkeypatch, env, folder):
    c, _ = kite_client(monkeypatch, env)
    boot = boot_of(c.get("/success").get_data(as_text=True))
    assert boot["fno"]["folder"] == folder and boot["form"]["folder"] == "C:/old"   # the planner keeps its folder


@pytest.mark.parametrize("raw,writes", [(None, True), ("set", False)])
def test_kite_fno_start_writes_kite_data_dir_only_without_raw_key(monkeypatch, tmp_path, raw, writes):
    from test_fno_entrypoints import AFTER, wait_for
    env = {"KITE_ACCESS_TOKEN": "x", **({"FNO_RAW_KITE": str(tmp_path)} if raw else {})}
    c, saved = kite_client(monkeypatch, env)
    monkeypatch.setattr(fno, "now_ist", lambda: AFTER)
    seen = []

    def fake(*a, **k):
        seen.append(1)
        with JOB_LOCK:
            JOB["state"] = "finished"
    monkeypatch.setattr(fno, "run_daily", fake)
    c.post("/download/fno-start", data={"folder": str(tmp_path), "date": "2026-10-12", "speed": "2"})
    assert wait_for(lambda: seen)
    assert (saved == [{"KITE_DATA_DIR": str(tmp_path)}]) is writes


@pytest.mark.parametrize("env,expected", [({}, "C:/data"), ({"FNO_RAW_DHAN": "D:/fno/raw/dhan"}, "D:/fno/raw/dhan")])
def test_dhan_card_start_folder(monkeypatch, env, expected):
    from test_dhan_fno import FakeClient
    full = {"DHAN_CLIENT_ID": "1000000001", "DHAN_ACCESS_TOKEN": "t", "DHAN_TOKEN_EXPIRY": "2099-01-01T09:00:00", **env}
    monkeypatch.setattr(dhan, "load_env", lambda: dict(full))
    monkeypatch.setattr(dhan, "keep_zero_volume", lambda: False)
    monkeypatch.setattr(dhan, "data_dir", lambda: "C:/data")
    monkeypatch.setattr(dhan, "DhanClient", FakeClient)
    with JOB_LOCK:
        JOB.update(state="idle", kind="backfill", source="", folder="")
    from app import app
    html = app.test_client().get("/dhan/dashboard").get_data(as_text=True)
    assert boot_of(html)["form"]["folder"] == expected


def test_cli_default_folder_per_source(monkeypatch, tmp_path):
    k, d = tmp_path / "k", tmp_path / "d"
    k.mkdir()
    d.mkdir()
    monkeypatch.setattr(nifty_daily, "load_env", lambda: {"FNO_RAW_KITE": str(k), "FNO_RAW_DHAN": str(d)})
    assert nifty_daily.default_folder("kite") == str(k) and nifty_daily.default_folder("dhan") == str(d)
    monkeypatch.setattr(nifty_daily, "load_env", lambda: {"KITE_DATA_DIR": str(k)})
    assert nifty_daily.default_folder("dhan") == str(k)   # not set: today's KITE_DATA_DIR


def test_cli_update_day_count_uses_each_sources_folder(monkeypatch, tmp_path):
    make_day(str(tmp_path / "k"), D1)
    make_day(str(tmp_path / "d"), D1, source="dhan")
    monkeypatch.setattr(nifty_daily, "load_env", lambda: {"FNO_RAW_KITE": str(tmp_path / "k"), "FNO_RAW_DHAN": str(tmp_path / "d")})
    assert nifty_daily.main(["--update-day-count"]) == 0
    assert os.path.exists(tmp_path / "k" / "NIFTY_FnO_1min" / fno_days.INDEX_FILE)
    assert os.path.exists(tmp_path / "d" / "NIFTY_FnO_1min_Dhan" / fno_days.INDEX_FILE)


@pytest.mark.parametrize("env,expected", [({"FNO_RAW_DHAN": "dhanraw", "KITE_DATA_DIR": "old"}, "dhanraw"),
                                          ({"KITE_DATA_DIR": "old"}, "old")])
def test_nifty_bhav_default_folder(monkeypatch, tmp_path, env, expected):
    monkeypatch.setattr(nifty_bhav, "load_env", lambda: {k: str(tmp_path / v) for k, v in env.items()})
    seen = []

    def export(folder, *a, **k):
        seen.append(folder)
        raise bhav.BhavError("nothing checked")
    monkeypatch.setattr(nifty_bhav.bhav_export, "export", export)
    assert nifty_bhav.main(["--export-days", "--source", "dhan"]) == 1
    assert seen == [str(tmp_path / expected)]


# --- Part 2: move-raw ------------------------------------------------------------------------------

def test_move_dry_run_changes_nothing(tmp_path, logs):
    old, to = tmp_path / "old", tmp_path / "raw"
    make_day(str(old), D1)
    fno.acquire_lock(str(old), "NIFTY", "an earlier run").release()   # real roots have a .lock: the dry run locks it
    before = tree(old)
    lines, log = logs
    assert store.move_raw([str(old)], "kite", str(to), log=log) == 0
    strip = lambda t: {k: v for k, v in t.items() if not k.endswith(".lock")}   # only the lock's owner text changes
    assert strip(tree(old)) == strip(before) and set(tree(old)) == set(before) and not to.exists()
    assert any(" -> " in line and D1 in line for line in lines) and lines[-1].startswith("Dry run: nothing moved")


@pytest.mark.parametrize("source", ["kite", "dhan"])
def test_move_apply_byte_for_byte(tmp_path, logs, source):
    old, to = tmp_path / "old", tmp_path / "raw"
    days = [make_day(str(old), D1, "NIFTY", source), make_day(str(old), D2, "NIFTY", source, form="old"),
            make_day(str(old), D1, "SENSEX", source, step=100)]
    sums = {os.path.basename(os.path.dirname(d)) + "/" + os.path.basename(d): tree(d) for d in days}
    (old / "NSE_equity").mkdir()
    lines, log = logs
    assert store.move_raw([str(old)], source, str(to), apply=True, log=log) == 0
    for key, files in sums.items():
        assert tree(to / key) == files and not (old / key).exists()
    assert (old / "NSE_equity").exists()                                   # other content untouched
    assert os.path.isdir(fno.fno_root(str(old), "NIFTY", source))         # the old root stays
    assert any("left in place (not deleted)" in line for line in lines)
    marker = [n for n in os.listdir(fno.fno_root(str(to), "NIFTY", source)) if n.startswith("_DAYS-")]
    assert marker == [f"_DAYS-2_{D1}_to_{D2}.txt"]


def test_move_duplicates_are_listed_not_moved(tmp_path, logs):
    a, b, to = tmp_path / "a", tmp_path / "b", tmp_path / "raw"
    make_day(str(to), D1, keep_zero=False)                     # target: noZeroVol
    make_day(str(to), D2, keep_zero=True)
    incoming_more = make_day(str(a), D1, keep_zero=True)      # incoming withZeroVol: more data, still not moved
    make_day(str(a), D2, keep_zero=True)
    make_day(str(a), D3)
    target_before = tree(to)
    lines, log = logs
    assert store.move_raw([str(a)], "kite", str(to), apply=True, log=log) == 0
    assert os.path.exists(incoming_more)
    text = "\n".join(lines)
    assert "incoming has more data" in text and "already in the target as" in text
    assert "2 duplicates not moved (1 where the incoming folder has more data)" in text
    root = to / "NIFTY_FnO_1min"
    for rel, h in target_before.items():                       # nothing replaced
        if not rel.endswith((".lock", ".txt", fno_days.INDEX_FILE)):
            assert tree(to)[rel] == h
    assert sorted(fno.day_folders(str(root))) == [date(2026, 10, 7), date(2026, 10, 8), date(2026, 10, 9)]


def test_move_repeated_from_sees_earlier_moves(tmp_path, logs):
    a, b, to = tmp_path / "with", tmp_path / "without", tmp_path / "raw"
    make_day(str(a), D1, source="dhan", keep_zero=True)
    make_day(str(b), D1, source="dhan", keep_zero=False)
    lines, log = logs
    assert store.move_raw([str(a), str(b)], "dhan", str(to), log=log) == 0
    text = "\n".join(lines)
    assert "1 day folders to move, 1 duplicates not moved" in text and "DUPLICATE " + os.path.basename(
        fno.find_day_dir(fno.fno_root(str(b), "NIFTY", "dhan"), D1)) in text


def write_progress(folder, run, rows):
    db = sqlite3.connect(os.path.join(folder, "progress.sqlite"))
    db.execute("CREATE TABLE chunks (run TEXT, symbol TEXT, token INTEGER, chunk_from TEXT, chunk_to TEXT, status TEXT, "
               "tries INTEGER DEFAULT 0, error TEXT, updated TEXT, reason TEXT, PRIMARY KEY (run, symbol, chunk_from))")
    db.executemany("INSERT INTO chunks (run, symbol, token, chunk_from, status, reason) VALUES (?, ?, ?, ?, ?, ?)",
                   [(run, s, t, "x", st, r) for s, t, st, r in rows])
    db.execute("INSERT INTO chunks (run, symbol, token, chunk_from, status) VALUES ('day:2026-10-01:csv', 'TCS', 1, 'x', 'DONE')")
    db.commit()
    db.close()


def test_move_merges_expiries_holidays_progress(tmp_path, logs):
    old, to = tmp_path / "old", tmp_path / "raw"
    make_day(str(old), D1)
    src_root, dst_root = fno.fno_root(str(old), "NIFTY"), fno.fno_root(str(to), "NIFTY")
    os.makedirs(dst_root)
    fno.write_csv(os.path.join(src_root, fno.EXPIRIES_FILE), ["expiry", "first_seen"],
                  [{"expiry": "2026-10-13", "first_seen": "2026-10-05"}, {"expiry": "2026-10-20", "first_seen": "2026-10-07"}])
    fno.write_csv(os.path.join(dst_root, fno.EXPIRIES_FILE), ["expiry", "first_seen"],
                  [{"expiry": "2026-10-13", "first_seen": "2026-10-09"}, {"expiry": "2026-10-27", "first_seen": "2026-10-01"}])
    with open(os.path.join(src_root, "holidays.log"), "w", encoding="utf-8") as f:
        f.write("A holiday\nB holiday\n")
    with open(os.path.join(dst_root, "holidays.log"), "w", encoding="utf-8") as f:
        f.write("B holiday\n")
    write_progress(str(old), fno.run_key("NIFTY", D1), [("S1", 13, "DONE", fno.ZERO_VOLUME), ("S2", 14, "DONE", "NO_CANDLES")])
    lines, log = logs
    assert store.move_raw([str(old)], "kite", str(to), apply=True, log=log) == 0
    with open(os.path.join(dst_root, fno.EXPIRIES_FILE), newline="", encoding="utf-8") as f:
        assert [(r["expiry"], r["first_seen"]) for r in csv.DictReader(f)] == [
            ("2026-10-13", "2026-10-05"), ("2026-10-20", "2026-10-07"), ("2026-10-27", "2026-10-01")]
    with open(os.path.join(dst_root, "holidays.log"), encoding="utf-8") as f:
        assert f.read().splitlines() == ["B holiday", "A holiday"]
    db = sqlite3.connect(to / "progress.sqlite")
    rows = db.execute("SELECT run, symbol, status, reason FROM chunks ORDER BY symbol").fetchall()
    db.close()
    assert rows == [(fno.run_key("NIFTY", D1), "S1", "DONE", fno.ZERO_VOLUME), (fno.run_key("NIFTY", D1), "S2", "DONE", "NO_CANDLES")]
    assert any("+2 progress.sqlite rows" in line for line in lines)


def test_moved_day_resumes_without_rerequesting(tmp_path, monkeypatch):
    from test_fno import ACCESS, RUN_DATE, FakeKite, set_now
    FakeKite.calls, FakeKite.profile_error, FakeKite.holiday, FakeKite.fail_tokens = [], None, False, {31}
    monkeypatch.setattr(fno, "KiteConnect", FakeKite)
    set_now(monkeypatch, RUN_DATE)
    old, to = tmp_path / "old", tmp_path / "raw"

    def run(folder):
        with JOB_LOCK:
            JOB["state"] = "idle"
        assert job_begin(**fno.job_fields("NIFTY", RUN_DATE, str(folder), 1000))
        return fno.run_daily("key", ACCESS, RUN_DATE, str(folder), 1000, log=lambda m: None)
    run(old)
    assert {13, 14} <= {c[0] for c in FakeKite.calls}       # zero-volume and no-candles: DONE without a file
    assert store.move_raw([str(old)], "kite", str(to), apply=True, log=lambda m: None) == 0
    FakeKite.calls.clear()
    run(to)
    assert not {11, 12, 13, 14} & {c[0] for c in FakeKite.calls}


def test_move_lock_busy(tmp_path, logs, monkeypatch):
    old, to = tmp_path / "old", tmp_path / "raw"
    make_day(str(old), D1)
    real = fno.RunLock.acquire

    def busy(self, owner):
        if os.path.dirname(self.path) == fno.fno_root(str(old), "NIFTY"):
            raise fno.LockBusy("Another NIFTY_FnO_1min run is in progress (dashboard).")
        return real(self, owner)
    monkeypatch.setattr(fno.RunLock, "acquire", busy)
    lines, log = logs
    assert store.move_raw([str(old)], "kite", str(to), apply=True, log=log) == 1
    assert lines[-1].startswith("Not run: Another") and fno.day_folders(fno.fno_root(str(old), "NIFTY"))


def test_move_error_midway_lists_what_was_done(tmp_path, logs, monkeypatch):
    old, to = tmp_path / "old", tmp_path / "raw"
    make_day(str(old), D1)
    make_day(str(old), D2)
    real, calls = store.move_day, []

    def flaky(src, dst):
        calls.append(src)
        if len(calls) == 2:
            raise OSError("disk full")
        real(src, dst)
    monkeypatch.setattr(store, "move_day", flaky)
    lines, log = logs
    assert store.move_raw([str(old)], "kite", str(to), apply=True, log=log) == 1
    text = "\n".join(lines)
    assert "Stopped: OSError: disk full" in text and "Moved before the error (1):" in text and D1 in text.split("Moved before")[1]
    assert len(fno.day_folders(fno.fno_root(str(to), "NIFTY"))) == 1   # its day count is still written
    assert os.path.exists(os.path.join(fno.fno_root(str(to), "NIFTY"), f"_DAYS-1_{D1}_to_{D1}.txt"))


def test_move_cross_drive_copies_verifies_deletes(tmp_path, logs, monkeypatch):
    old, to = tmp_path / "old", tmp_path / "raw"
    d = make_day(str(old), D1)
    files = tree(d)
    monkeypatch.setattr(store, "same_device", lambda a, b: False)
    assert store.move_raw([str(old)], "kite", str(to), apply=True, log=logs[1]) == 0
    moved = os.path.join(fno.fno_root(str(to), "NIFTY"), os.path.basename(d))
    assert tree(moved) == files and not os.path.exists(d) and not os.path.exists(moved + ".part")


def test_move_cross_drive_size_mismatch_keeps_source(tmp_path, logs, monkeypatch):
    old, to = tmp_path / "old", tmp_path / "raw"
    d = make_day(str(old), D1)
    files = tree(d)
    monkeypatch.setattr(store, "same_device", lambda a, b: False)
    real = store.shutil.copytree

    def bad_copy(src, dst, *a, **kw):
        out = real(src, dst, *a, **kw)
        if dst.endswith(".part"):
            with open(os.path.join(dst, "summary.csv"), "a", encoding="utf-8") as f:
                f.write("extra\n")
        return out
    monkeypatch.setattr(store.shutil, "copytree", bad_copy)
    lines, log = logs
    assert store.move_raw([str(old)], "kite", str(to), apply=True, log=log) == 1
    assert tree(d) == files and "does not match" in "\n".join(lines)
    assert fno.day_folders(fno.fno_root(str(to), "NIFTY")) == {}


@pytest.mark.parametrize("args,msg", [([], "give --to"), (["same"], "the same folder")])
def test_move_refusals(tmp_path, logs, args, msg):
    old = tmp_path / "old"
    make_day(str(old), D1)
    to = str(old) if args == ["same"] else ""
    lines, log = logs
    assert store.move_raw([str(old)], "kite", to, log=log) == 1 and msg in lines[-1]


def test_cli_move_raw_uses_env_to(tmp_path, monkeypatch, capsys):
    old = tmp_path / "old"
    make_day(str(old), D1, source="dhan")
    monkeypatch.setattr(cli, "load_env", lambda: {"FNO_RAW_DHAN": str(tmp_path / "raw")})
    assert cli.main(["move-raw", "--from", str(old), "--source", "dhan"]) == 0
    assert str(tmp_path / "raw") in capsys.readouterr().out
    monkeypatch.setattr(cli, "load_env", lambda: {})
    assert cli.main(["move-raw", "--from", str(old), "--source", "dhan"]) == 1
    with pytest.raises(SystemExit) as e:
        cli.main(["move-raw", "--source", "dhan"])
    assert e.value.code == 1


# --- Part 3: publish -------------------------------------------------------------------------------

def pub(tmp_path, kite=True, dhan=True, **kw):
    raw = {"kite": str(tmp_path / "kite") if kite else None, "dhan": str(tmp_path / "dhan") if dhan else None}
    lines = []
    code = fno_publish.publish(raw, str(tmp_path / "bt"), kw.pop("underlyings", ["NIFTY"]), log=lines.append, **kw)
    return code, lines


def days_csv(tmp_path, u="NIFTY"):
    with open(tmp_path / "bt" / u / "days.csv", newline="", encoding="utf-8") as f:
        return {r["date"]: r for r in csv.DictReader(f)}


def test_publish_minute_file(tmp_path):
    make_day(str(tmp_path / "kite"), D1)
    code, _ = pub(tmp_path, dhan=False)
    assert code == 0
    df = pd.read_parquet(tmp_path / "bt" / "NIFTY" / "minute" / "kite" / "2026" / f"{D1}.parquet")
    assert list(df.columns) == ["contract", "type", "expiry", "strike", "dte", "datetime", "open", "high", "low", "close",
                                "volume", "oi", "oi_filled", "zero_volume"]
    assert list(df["contract"].unique()) == ["NIFTY2500013OCT26CE", "NIFTY2505013OCT26PE", "NIFTY27OCT26"]   # sorted
    fut = df[df["contract"] == "NIFTY27OCT26"]
    assert fut["type"].iloc[0] == "FUT" and pd.isna(fut["strike"]).all() and (fut["dte"] == 20).all()
    assert fut["expiry"].iloc[0] == M1 and list(fut["close"]) == [25100.0, 25101.5, 25102.0]
    assert str(df["datetime"].dtype) == "datetime64[ns]" and df["datetime"].iloc[0] == pd.Timestamp(f"{D1} 09:15")
    assert str(df["volume"].dtype) == "int64" and str(df["oi"].dtype) == "float64"
    ce = df[df["contract"] == "NIFTY2500013OCT26CE"]
    assert list(ce["oi"]) == [500.0, 500.0, 520.0] and list(ce["oi_filled"]) == [False, True, False]
    raw = fno_loader.read_contract_csv(os.path.join(fno.find_day_dir(fno.fno_root(str(tmp_path / "kite"), "NIFTY"), D1),
                                                    "options", "NIFTY2500013OCT26CE.csv"))
    assert list(raw["OI"]) == list(ce["oi"]) and list(raw["OI_filled"]) == list(ce["oi_filled"])   # as fill_oi does
    assert ce["strike"].iloc[0] == 25000.0 and (ce["dte"] == 6).all()
    assert df.groupby("contract")["zero_volume"].first().to_dict() == {
        "NIFTY2500013OCT26CE": False, "NIFTY2505013OCT26PE": True, "NIFTY27OCT26": False}


def test_publish_both_sources_all_name_forms_and_incomplete(tmp_path):
    make_day(str(tmp_path / "kite"), D1, form="old")
    make_day(str(tmp_path / "kite"), D2, summary=False)                # incomplete
    make_day(str(tmp_path / "dhan"), D1, source="dhan", keep_zero=False)
    make_day(str(tmp_path / "dhan"), D2, source="dhan")
    code, lines = pub(tmp_path)
    assert code == 0
    bt = tmp_path / "bt" / "NIFTY" / "minute"
    assert sorted(os.listdir(bt / "kite" / "2026")) == [f"{D1}.parquet"]
    assert sorted(os.listdir(bt / "dhan" / "2026")) == [f"{D1}.parquet", f"{D2}.parquet"]
    assert any("NIFTY kite: published 1" in line and f"skipped incomplete 1 ({D2})" in line for line in lines)
    rows = days_csv(tmp_path)
    assert rows[D1]["kite_zero_volume"] == "" and rows[D1]["dhan_zero_volume"] == "no"
    assert rows[D1]["expiry"] == W1.isoformat() and rows[D1]["dte"] == "6"   # the old name: expiry from the data
    assert rows[D2]["kite_available"] == "False" and rows[D2]["dhan_available"] == "True" and rows[D2]["best_source"] == "dhan"
    assert rows[D1]["kite_trim"] == "ON" and rows[D1]["kite_contracts"] == "3"


def test_publish_sensex(tmp_path):
    make_day(str(tmp_path / "kite"), D1, "SENSEX", step=100)
    code, _ = pub(tmp_path, dhan=False, underlyings=["SENSEX"])
    df = pd.read_parquet(tmp_path / "bt" / "SENSEX" / "minute" / "kite" / "2026" / f"{D1}.parquet")
    assert code == 0 and set(df["contract"]) == {"SENSEX27OCT26", "SENSEX7500013OCT26CE", "SENSEX7510013OCT26PE"}
    eod = pd.read_parquet(tmp_path / "bt" / "SENSEX" / "eod" / "2026.parquet")
    assert set(eod["contract"]) == {"SENSEX27OCT26", "SENSEX7500013OCT26CE", "SENSEX7500020OCT26CE"}


def test_publish_eod_from_kite_else_dhan(tmp_path):
    make_day(str(tmp_path / "kite"), D1)
    make_day(str(tmp_path / "dhan"), D1, source="dhan")
    make_day(str(tmp_path / "kite"), D2, bhav_copy=False)
    make_day(str(tmp_path / "dhan"), D2, source="dhan")
    make_day(str(tmp_path / "kite"), D3, bhav_copy=False)
    assert pub(tmp_path)[0] == 0
    eod = pd.read_parquet(tmp_path / "bt" / "NIFTY" / "eod" / "2026.parquet")
    assert sorted(eod["date"].dt.strftime("%Y-%m-%d").unique()) == [D1, D2]
    assert list(eod.columns[:5]) == ["date", "contract", "type", "expiry", "strike"]
    assert set(fno_loader.EOD_COLUMNS) <= set(eod.columns)
    man = json.load(open(tmp_path / "bt" / "NIFTY" / fno_publish.MANIFEST, encoding="utf-8"))
    assert man["days"][D1]["eod_source"] == "kite" and man["days"][D2]["eod_source"] == "dhan"
    rows = days_csv(tmp_path)
    assert rows[D3]["eod_available"] == "False" and rows[D1]["eod_available"] == "True"
    fut = eod[(eod["date"] == pd.Timestamp(D1)) & (eod["contract"] == "NIFTY27OCT26")].iloc[0]
    assert fut["Settle"] == 25103 and fut["Lot"] == 65


def test_publish_contracts_and_expiries(tmp_path):
    make_day(str(tmp_path / "kite"), D1)
    make_day(str(tmp_path / "dhan"), D2, source="dhan")
    fno.write_csv(os.path.join(fno.fno_root(str(tmp_path / "kite"), "NIFTY"), fno.EXPIRIES_FILE), ["expiry", "first_seen"],
                  [{"expiry": "2026-10-06", "first_seen": "2026-10-01"}, {"expiry": "2026-11-24", "first_seen": "2026-10-01"}])
    assert pub(tmp_path)[0] == 0
    c = fno_backtest.load_contracts("NIFTY", root=str(tmp_path / "bt"))
    assert list(c.columns) == fno_publish.CONTRACT_COLUMNS
    fut = c[c["contract"] == "NIFTY27OCT26"].iloc[0]
    assert fut["days_with_data"] == 2 and fut["first_date"] == date(2026, 10, 7) and fut["last_date"] == date(2026, 10, 8)
    assert fut["lot"] == 65
    ex = {r["expiry"]: (r["weekday"], r["kind"]) for _, r in fno_backtest.load_expiries("NIFTY", str(tmp_path / "bt")).iterrows()}
    assert ex == {"2026-10-06": ("TUE", "weekly"), "2026-10-13": ("TUE", "weekly"), "2026-10-20": ("TUE", "weekly"),
                  "2026-10-27": ("TUE", "monthly"), "2026-11-24": ("TUE", "monthly")}


@pytest.mark.parametrize("kite,dhan,prefer,best", [
    ("ok", "ok", "kite", "kite"), ("ok", "ok", "dhan", "dhan"), ("major", "ok", "kite", "dhan"),
    ("minor", "major", "kite", "kite"), (None, "minor", "kite", "dhan"), (None, "major", "kite", "kite"),
    ("minor", None, "dhan", "kite"), ("absent", "major", "kite", "dhan"), (None, None, "dhan", "dhan"),
])
def test_best_source(tmp_path, kite, dhan, prefer, best):
    if kite != "absent":
        make_day(str(tmp_path / "kite"), D1, check=kite)
    make_day(str(tmp_path / "dhan"), D1, source="dhan", check=dhan)
    assert pub(tmp_path, prefer=prefer)[0] == 0
    r = days_csv(tmp_path)[D1]
    assert r["best_source"] == best
    assert r["kite_check"] == ({"ok": "OK", None: "unchecked", "absent": ""}.get(kite, kite))


def test_best_source_rule():
    assert fno_publish.best_source({}) == ("", "")
    assert fno_publish.best_source({"kite": "unchecked", "dhan": "minor"}) == ("dhan", "dhan minor, kite unchecked")
    assert fno_publish.best_source({"kite": "unchecked", "dhan": "major"}) == ("kite", "dhan major, kite unchecked")
    assert fno_publish.best_source({"dhan": "major"}) == ("dhan", "only dhan (major)")


def counts(missing=0, red=0, **other):
    return {"missing": missing, "red_volume_diffs": red, "volume_diffs": red, **other}


@pytest.mark.parametrize("kite,dhan,prefer,best,reason", [
    (counts(missing=2, red=1), counts(missing=1, red=50), "kite", "dhan", "both major; dhan fewer missing contracts 1 vs 2"),
    (counts(red=99, oi_diffs=1), counts(red=32, oi_diffs=500), "kite", "dhan", "both major; dhan fewer volume diffs 32 vs 99"),
    (counts(red=5, oi_diffs=10), counts(red=5, oi_diffs=3), "kite", "dhan", "both major; dhan fewer total diffs 8 vs 15"),
    (counts(red=5, oi_diffs=3), counts(red=5, oi_diffs=3), "dhan", "dhan", "both major, same counts; --prefer dhan"),
    (counts(red=5), counts(red=7), "dhan", "kite", "both major; kite fewer volume diffs 5 vs 7"),
])
def test_best_source_tie_break_on_counts(tmp_path, kite, dhan, prefer, best, reason):
    make_day(str(tmp_path / "kite"), D1, check="major", counts=kite)
    make_day(str(tmp_path / "dhan"), D1, source="dhan", check="major", counts=dhan)
    assert pub(tmp_path, prefer=prefer)[0] == 0
    r = days_csv(tmp_path)[D1]
    assert (r["best_source"], r["best_reason"]) == (best, reason)


def test_best_source_tie_break_old_counts_and_unchecked(tmp_path):
    # a check saved before red_volume_diffs existed uses volume_diffs
    make_day(str(tmp_path / "kite"), D1, check="major", counts={"missing": 0, "volume_diffs": 9})
    make_day(str(tmp_path / "dhan"), D1, source="dhan", check="major", counts={"missing": 0, "volume_diffs": 4})
    make_day(str(tmp_path / "kite"), D2, check=None)
    make_day(str(tmp_path / "dhan"), D2, source="dhan", check=None)
    assert pub(tmp_path)[0] == 0
    rows = days_csv(tmp_path)
    assert (rows[D1]["best_source"], rows[D1]["best_reason"]) == ("dhan", "both major; dhan fewer volume diffs 4 vs 9")
    assert (rows[D2]["best_source"], rows[D2]["best_reason"]) == ("kite", "both unchecked, no counts; --prefer kite")


def test_best_reason_column_and_different_labels(tmp_path):
    make_day(str(tmp_path / "kite"), D1, check="minor")
    make_day(str(tmp_path / "dhan"), D1, source="dhan", check="major", counts=counts(red=1))
    assert pub(tmp_path)[0] == 0
    r = days_csv(tmp_path)[D1]
    assert list(r)[fno_publish.DAYS_COLUMNS.index("best_reason")] == "best_reason"
    assert (r["best_source"], r["best_reason"]) == ("kite", "dhan major, kite minor")


def test_publish_incremental_force_and_changed(tmp_path):
    k = make_day(str(tmp_path / "kite"), D1)
    make_day(str(tmp_path / "kite"), D2)
    assert pub(tmp_path, dhan=False)[0] == 0
    target = tmp_path / "bt" / "NIFTY" / "minute" / "kite" / "2026" / f"{D1}.parquet"
    mtime = os.path.getmtime(target)
    code, lines = pub(tmp_path, dhan=False)
    assert code == 0 and any("published 0, skipped unchanged 2" in line for line in lines) and os.path.getmtime(target) == mtime
    fut = os.path.join(k, "futures", "NIFTY27OCT26.csv")
    fno.write_contract(fut, "NIFTY27OCT26", bars(D1, [1.0, 2.0, 3.0, 4.0]), True)   # the raw day changed
    code, lines = pub(tmp_path, dhan=False)
    assert any("published 1, skipped unchanged 1" in line for line in lines)
    assert len(pd.read_parquet(target, filters=[("contract", "==", "NIFTY27OCT26")])) == 4
    code, lines = pub(tmp_path, dhan=False, force=True)
    assert any("published 2, skipped unchanged 0" in line for line in lines)


def test_publish_date_range(tmp_path):
    make_day(str(tmp_path / "kite"), D1)
    make_day(str(tmp_path / "kite"), D2)
    assert pub(tmp_path, dhan=False, date_from=date(2026, 10, 8))[0] == 0
    assert sorted(days_csv(tmp_path)) == [D2]


def test_publish_raw_missing_kept_then_pruned(tmp_path):
    k = make_day(str(tmp_path / "kite"), D1)
    make_day(str(tmp_path / "kite"), D2)
    assert pub(tmp_path, dhan=False)[0] == 0
    import shutil
    shutil.rmtree(k)
    code, lines = pub(tmp_path, dhan=False)
    target = tmp_path / "bt" / "NIFTY" / "minute" / "kite" / "2026" / f"{D1}.parquet"
    assert target.exists() and days_csv(tmp_path)[D1]["raw_missing"] == "kite"
    assert any("raw folder gone for 1 published days" in line for line in lines)
    code, lines = pub(tmp_path, dhan=False, prune=True)
    assert not target.exists() and D1 not in days_csv(tmp_path) and any("pruned 1" in line for line in lines)


def test_publish_never_writes_raw(tmp_path):
    make_day(str(tmp_path / "kite"), D1)
    make_day(str(tmp_path / "dhan"), D1, source="dhan")
    before = {k: v for k, v in tree(tmp_path / "kite").items()} | {"d/" + k: v for k, v in tree(tmp_path / "dhan").items()}
    assert pub(tmp_path)[0] == 0
    assert pub(tmp_path, force=True)[0] == 0
    after = {k: v for k, v in tree(tmp_path / "kite").items()} | {"d/" + k: v for k, v in tree(tmp_path / "dhan").items()}
    strip = lambda t: {k: v for k, v in t.items() if not k.endswith(".lock")}
    assert strip(after) == strip(before)


def test_publish_lock_busy_skips_root(tmp_path, monkeypatch):
    make_day(str(tmp_path / "kite"), D1)
    make_day(str(tmp_path / "dhan"), D1, source="dhan")
    real = fno.acquire_lock

    def lock(data_root, underlying, owner, source="kite"):
        if source == "kite":
            raise fno.LockBusy("Another NIFTY_FnO_1min run is in progress (dashboard).")
        return real(data_root, underlying, owner, source)
    monkeypatch.setattr(fno, "acquire_lock", lock)
    code, lines = pub(tmp_path)
    assert code == 1 and any("NIFTY kite: skipped, Another" in line for line in lines)
    r = days_csv(tmp_path)[D1]
    assert r["dhan_available"] == "True" and r["kite_available"] == "False"


def test_publish_refusals(tmp_path):
    lines = []
    assert fno_publish.publish({"kite": None, "dhan": None}, str(tmp_path / "bt"), ["NIFTY"], log=lines.append) == 1
    assert fno_publish.publish({"kite": str(tmp_path)}, "", ["NIFTY"], log=lines.append) == 1
    assert "--raw-kite" in lines[0] and "FNO_BACKTEST_DIR" in lines[1]


def test_cli_publish_uses_env(tmp_path, monkeypatch):
    make_day(str(tmp_path / "kite"), D1)
    monkeypatch.setattr(cli, "load_env", lambda: {"FNO_RAW_KITE": str(tmp_path / "kite"), "FNO_BACKTEST_DIR": str(tmp_path / "bt")})
    assert cli.main(["publish", "--underlying", "NIFTY"]) == 0
    assert (tmp_path / "bt" / "NIFTY" / "days.csv").exists()


# --- Part 4: the loader ------------------------------------------------------------------------------

@pytest.fixture
def published(tmp_path):
    make_day(str(tmp_path / "kite"), D1, check="major")
    make_day(str(tmp_path / "dhan"), D1, source="dhan", check="ok")
    make_day(str(tmp_path / "kite"), D3)
    assert pub(tmp_path)[0] == 0
    return str(tmp_path / "bt")


def test_load_day_best_and_forced(published, tmp_path):
    best = fno_backtest.load_fno_day("NIFTY", D1, root=published)
    dhan_day = fno_backtest.load_fno_day("NIFTY", D1, source="dhan", root=published)
    assert best.equals(dhan_day)   # Dhan's check is OK, Kite's major
    assert len(fno_backtest.load_fno_day("NIFTY", date(2026, 10, 7), source="kite", root=published)) == 8


def test_load_day_filters(published):
    df = fno_backtest.load_fno_day("NIFTY", D1, root=published, expiry="2026-10-13", types=["CE"], strikes=[25000],
                                   columns=["contract", "close", "oi"])
    assert list(df.columns) == ["contract", "close", "oi"] and set(df["contract"]) == {"NIFTY2500013OCT26CE"}
    nz = fno_backtest.load_fno_day("NIFTY", D1, root=published, drop_zero_volume=True)
    assert "NIFTY2505013OCT26PE" not in set(nz["contract"])
    fut = fno_backtest.load_fno_day("NIFTY", D1, root=published, types="FUT", expiry=M1)
    assert set(fut["contract"]) == {"NIFTY27OCT26"}


def test_load_range_with_gaps(published):
    df, skipped = fno_backtest.load_fno_range("NIFTY", D1, D3, source="dhan", root=published)
    assert skipped == [D3] and set(df["datetime"].dt.strftime("%Y-%m-%d")) == {D1}
    df, skipped = fno_backtest.load_fno_range("NIFTY", D1, D3, root=published, types=["FUT"])
    assert skipped == [] and list(df["datetime"].dt.strftime("%Y-%m-%d").unique()) == [D1, D3]


def test_load_errors(published, monkeypatch):
    with pytest.raises(FileNotFoundError, match="not published"):
        fno_backtest.load_fno_day("NIFTY", D2, root=published)
    with pytest.raises(FileNotFoundError, match="not published for dhan"):
        fno_backtest.load_fno_day("NIFTY", D3, source="dhan", root=published)
    with pytest.raises(ValueError, match="source must be"):
        fno_backtest.load_fno_day("NIFTY", D1, source="nse", root=published)
    monkeypatch.setattr(config, "load_env", lambda: {})
    with pytest.raises(ValueError, match="FNO_BACKTEST_DIR"):
        fno_backtest.load_fno_day("NIFTY", D1)
    monkeypatch.setattr(config, "load_env", lambda: {"FNO_BACKTEST_DIR": published})
    assert len(fno_backtest.load_fno_day("NIFTY", D1)) > 0                # root from .env


def test_load_eod_days(published):
    eod = fno_backtest.load_eod("NIFTY", D1, D2, root=published)
    assert set(eod["date"].dt.strftime("%Y-%m-%d")) == {D1}
    days = fno_backtest.load_days("NIFTY", root=published)
    assert list(days["date"]) == [D1, D3] and days["eod_available"].tolist() == [True, True]


def test_move_holidays_ignore_the_logged_at_time(tmp_path, logs):
    a, b, to = tmp_path / "a", tmp_path / "b", tmp_path / "raw"
    make_day(str(a), D1, source="dhan")
    make_day(str(b), D2, source="dhan")
    msg = "2026-10-10: no candles for NIFTY-Oct2026-FUT#48704 (token 48704) between 09:00 and 16:00 IST, so the market was closed."
    for folder, at, extra in ((a, "2026-10-13 20:11:36", []), (b, "2026-10-13 21:50:03", ["2026-10-13 21:50:04 2026-10-11: closed."])):
        with open(os.path.join(fno.fno_root(str(folder), "NIFTY", "dhan"), "holidays.log"), "w", encoding="utf-8") as f:
            f.write("\n".join([f"{at} {msg}"] + extra) + "\n")
    lines, log = logs
    assert store.move_raw([str(a), str(b)], "dhan", str(to), apply=True, log=log) == 0
    with open(os.path.join(fno.fno_root(str(to), "NIFTY", "dhan"), "holidays.log"), encoding="utf-8") as f:
        assert f.read().splitlines() == [f"2026-10-13 20:11:36 {msg}", "2026-10-13 21:50:04 2026-10-11: closed."]
    assert sum("+1 holidays.log lines" in line for line in lines) == 2
    assert fno.holidays_logged(str(to), "NIFTY", "dhan") == {date(2026, 10, 10), date(2026, 10, 11)}

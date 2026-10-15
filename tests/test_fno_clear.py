"""fno_store.py clear / restore (backfill/fno_clear.py): archive one underlying's F&O data in every store, raw and
backtest, leaving the other underlying, equity rows, expiries_seen.csv, holidays.log and .lock alone.
tmp_path only, .env stubbed, prompts stubbed, no network."""
import json
import os
import sqlite3
from datetime import date, datetime

import pytest

import fno_store as cli
from backfill import fno, fno_clear, fno_days
from backfill.job import JOB, JOB_LOCK
from test_fno_store import make_day
from test_fno_store import tree as full_tree
from test_sensex import SensexKite, run_sensex, sensex_kite  # noqa: F401  (fixture)

IST = fno.IST
TODAY = date(2026, 10, 16)
LOST, OK = ["2026-10-09", "2026-10-15"], ["2026-10-16"]   # window today: 16 Oct (previous expiry 15 Oct)
STORES = ("with_zero_volume", "without_zero_volume")


@pytest.fixture
def data(tmp_path, monkeypatch):
    """D:\\Data\\fno-like tree: SENSEX and NIFTY days in all four stores, old-layout roots, progress rows of
    SENSEX, NIFTY and equity in every save folder, and published backtest folders."""
    home = tmp_path / "fno"
    env = {"FNO_RAW_KITE": str(home / "raw" / "kite"), "FNO_RAW_DHAN": str(home / "raw" / "dhan"),
           "FNO_BACKTEST_DIR": str(home / "backtest")}
    monkeypatch.setattr(fno, "today_ist", lambda now=None: TODAY)
    monkeypatch.setattr(fno, "now_ist", lambda: datetime(2026, 10, 16, 10, 30, tzinfo=IST))
    monkeypatch.setattr(fno_clear, "weekly_expiry_weekday", lambda u="NIFTY": 3)
    for source in ("kite", "dhan"):
        base = home / "raw" / source
        for store in STORES:
            save = base / store
            for day in LOST + OK:
                make_day(str(save), day, "SENSEX", source, keep_zero=store == STORES[0], step=100)
            make_day(str(save), "2026-10-14", "NIFTY", source, keep_zero=store == STORES[0])
            add_rows(save, source)
        old = fno.fno_root(str(base), "SENSEX", source)            # the emptied old-layout root
        os.makedirs(old)
        fno_days.write_day_count(old)
        add_rows(base, source)
        for save in [base / s for s in STORES] + [base]:
            root = fno.fno_root(str(save), "SENSEX", source)
            os.makedirs(root, exist_ok=True)
            fno.write_csv(os.path.join(root, fno.EXPIRIES_FILE), ["expiry", "first_seen"],
                          [{"expiry": e, "first_seen": "2026-10-01"} for e in ("2026-10-08", "2026-10-15", "2026-10-22", "2026-10-29")])
            with open(os.path.join(root, "holidays.log"), "w", encoding="utf-8") as f:
                f.write("2026-10-11 10:00:00 2026-10-10: closed.\n")
            with open(os.path.join(root, "bhavcopy.log"), "w", encoding="utf-8") as f:
                f.write("x\n")
            fno.RunLock(os.path.join(root, ".lock")).acquire("t").release()
            if save != base:
                fno_days.write_day_count(root)
    for store in STORES:
        for u in ("SENSEX", "NIFTY"):
            os.makedirs(home / "backtest" / store / u)
            (home / "backtest" / store / u / "days.csv").write_text("date\n")
    os.makedirs(home / "backtest" / "SENSEX")
    (home / "backtest" / "SENSEX" / "publish_manifest.json").write_text("{}")
    return home, env


def tree(folder):
    """test_fno_store.tree without .lock files (taking a lock writes its owner into it)."""
    return {k: v for k, v in full_tree(folder).items() if os.path.basename(k) != ".lock"}


def add_rows(save, source):
    db = fno_clear.open_db(str(save))
    pre = "fno" if source == "kite" else "fno-dhan"
    exch = {"kite": ("BFO", "NFO"), "dhan": ("BSE_FNO", "NSE_FNO")}[source]
    db.executemany("INSERT INTO chunks (run, symbol, token, chunk_from, chunk_to, status, reason) VALUES (?, ?, 1, 'a', 'b', 'DONE', ?)",
                   [(f"{pre}:SENSEX:{exch[0]}:2026-10-09", "S1", "NO_CANDLES"),
                    (f"{pre}:SENSEX:{exch[0]}:2026-10-15", "S2", None),
                    (f"{pre}:SENSEX:{exch[0]}:missing", "S3", None),                 # a range / missing-days key
                    (f"{pre}:NIFTY:{exch[1]}:2026-10-14", "N1", None),
                    ("minute:2026-01-01:2026-02-01:csv", "INFY", None)])
    db.commit()
    db.close()


def runs(save):
    db = sqlite3.connect(str(save / "progress.sqlite"))
    try:
        return sorted(r[0] for r in db.execute("SELECT run FROM chunks"))
    finally:
        db.close()


def do(env, *, sources=("dhan", "kite"), variants=("with", "without"), apply=False, ask=lambda q: "n",
       interactive=lambda: False, **kw):
    lines = []
    code = fno_clear.clear("SENSEX" if "underlying" not in kw else kw.pop("underlying"), list(sources), list(variants),
                           env, apply=apply, log=lines.append, ask=ask, interactive=interactive, **kw)
    return code, "\n".join(lines)


ARCHIVE = "2026-10-16_1030_clear_SENSEX"


def sensex_days(home):
    out = {}
    for source in ("kite", "dhan"):
        for store in STORES:
            root = fno.fno_root(str(home / "raw" / source / store), "SENSEX", source)
            out[(source, store)] = sorted(d.isoformat() for d in fno.day_folders(root))
    return out


# --- dry run --------------------------------------------------------------------------------------------

def test_dry_run_changes_nothing_and_lists_everything(data):
    home, env = data
    before = tree(str(home))
    code, out = do(env)
    assert code == 0 and tree(str(home)) == before and not (home / "archive").exists()
    assert out.count("archive 2026-10-") == 4 * 3                     # 3 SENSEX days in each of the four stores
    assert "keep .lock, expiries_seen.csv, holidays.log" in out
    assert "archive bhavcopy.log" in out and "archive days_index.csv" in out and "archive _DAYS-0.txt" in out
    assert out.count("archive 3 progress rows") == 6                   # 4 stores + 2 bases
    assert os.path.join("backtest", "SENSEX") in out and "NIFTY_FnO_1min" not in out
    assert os.path.join(str(home), "archive", ARCHIVE) in out
    assert "Can be downloaded again: 2026-10-16." in out
    assert "Cannot be downloaded again (outside the window): 2026-10-09, 2026-10-15 - they will exist only in the archive." in out
    assert out.endswith("Dry run: nothing changed. Run again with --apply to archive.")


# --- apply ----------------------------------------------------------------------------------------------

def test_apply_archives_sensex_and_leaves_nifty(data):
    home, env = data
    nifty = {s: tree(str(home / "raw" / s.split("/")[0] / s.split("/")[1] / fno.fno_folder("NIFTY", s.split("/")[0])))
             for s in [f"{src}/{st}" for src in ("kite", "dhan") for st in STORES]}
    bt_nifty = tree(str(home / "backtest" / "with_zero_volume" / "NIFTY"))
    code, out = do(env, apply=True, yes=True)
    assert code == 0, out
    arch = home / "archive" / ARCHIVE
    assert all(not v for v in sensex_days(home).values())
    for source in ("kite", "dhan"):
        for store in STORES:
            root = fno.fno_root(str(home / "raw" / source / store), "SENSEX", source)
            assert sorted(os.listdir(root)) == sorted([".lock", "_DAYS-0.txt", fno.EXPIRIES_FILE, "days_index.csv", "holidays.log"])
            moved = os.path.join(str(arch), "raw", source, store, fno.fno_folder("SENSEX", source))
            assert len([n for n in os.listdir(moved) if fno.parse_day_folder(n)]) == 3
            assert os.path.exists(os.path.join(moved, "bhavcopy.log"))
            assert runs(home / "raw" / source / store) == sorted([f"{'fno' if source == 'kite' else 'fno-dhan'}:NIFTY:"
                                                                  f"{'NFO' if source == 'kite' else 'NSE_FNO'}:2026-10-14",
                                                                  "minute:2026-01-01:2026-02-01:csv"])
            db = sqlite3.connect(os.path.join(str(arch), "raw", source, store, "progress_sensex.sqlite"))
            assert db.execute("SELECT COUNT(*) FROM chunks").fetchone()[0] == 3
            assert db.execute("SELECT reason FROM chunks WHERE symbol = 'S1'").fetchone()[0] == "NO_CANDLES"
            db.close()
        assert len(runs(home / "raw" / source)) == 2                       # the base: NIFTY and equity only
        assert (arch / "raw" / source / fno.fno_folder("SENSEX", source) / "_DAYS-0.txt").exists()
    assert {s: tree(str(home / "raw" / s.split("/")[0] / s.split("/")[1] / fno.fno_folder("NIFTY", s.split("/")[0])))
            for s in nifty} == nifty
    assert not (home / "backtest" / "SENSEX").exists() and not (home / "backtest" / "with_zero_volume" / "SENSEX").exists()
    assert (arch / "backtest" / "SENSEX" / "publish_manifest.json").exists()
    assert (arch / "backtest" / "without_zero_volume" / "SENSEX" / "days.csv").exists()
    assert tree(str(home / "backtest" / "with_zero_volume" / "NIFTY")) == bt_nifty
    text = (arch / "CLEARED.txt").read_text()
    assert "restore --from" in text and "2026-10-09" in text and "Cannot be downloaded again" in text
    man = json.loads((arch / fno_clear.MANIFEST).read_text())
    assert len(man["items"]) == 4 * 6 + 2 * 3 + 3   # stores: 3 days, marker, index, bhavcopy.log; old roots: 3; backtest 3 and len(man["dbs"]) == 6


def test_fresh_run_after_clear_requests_every_contract(tmp_path, sensex_kite, monkeypatch):
    base = tmp_path / "raw"
    save = base / "without_zero_volume"
    assert run_sensex(save) == "finished"
    SensexKite.calls = []
    assert run_sensex(save) == "finished"
    assert {c[0] for c in SensexKite.calls} == set()                   # saved: token 12 (no candles) skipped too
    env = {"FNO_RAW_KITE": str(base)}
    monkeypatch.setattr(fno_clear, "weekly_expiry_weekday", lambda u="NIFTY": 3)
    code, out = do(env, sources=["kite"], apply=True, yes=True, archive_to=str(tmp_path / "arch"))
    assert code == 0, out
    with JOB_LOCK:
        JOB["state"] = "idle"
    SensexKite.calls = []
    assert run_sensex(save) == "finished"
    assert {c[0] for c in SensexKite.calls} == {1, 11, 12}


# --- refusals and scope ---------------------------------------------------------------------------------

def test_underlying_is_required(data, monkeypatch, capsys):
    home, env = data
    before = tree(str(home))
    assert do(env, underlying=None)[0] == 1
    monkeypatch.setattr(cli, "load_env", lambda: env)
    with pytest.raises(SystemExit) as e:
        cli.main(["clear"])
    assert e.value.code == 1 and tree(str(home)) == before


def test_scope_flags(data):
    home, env = data
    code, _ = do(env, sources=["kite"], variants=["with"], apply=True, yes=True, backtest=False)
    assert code == 0
    days = sensex_days(home)
    assert days[("kite", "with_zero_volume")] == [] and days[("kite", "without_zero_volume")] == LOST + OK
    assert days[("dhan", "with_zero_volume")] == LOST + OK
    assert (home / "raw" / "kite" / "SENSEX_FnO_1min" / "_DAYS-0.txt").exists()     # old root: --variant all only
    assert len(runs(home / "raw" / "kite")) == 5 and len(runs(home / "raw" / "kite" / "without_zero_volume")) == 5
    assert len(runs(home / "raw" / "kite" / "with_zero_volume")) == 2
    assert (home / "backtest" / "with_zero_volume" / "SENSEX").exists()


def test_cli_scope_and_default_archive(data, monkeypatch, capsys):
    home, env = data
    monkeypatch.setattr(cli, "load_env", lambda: env)
    assert cli.main(["clear", "--underlying", "SENSEX", "--source", "dhan", "--variant", "without",
                     "--apply", "--yes"]) == 0
    days = sensex_days(home)
    assert days[("dhan", "without_zero_volume")] == [] and days[("dhan", "with_zero_volume")] == LOST + OK
    assert not (home / "backtest" / "without_zero_volume" / "SENSEX").exists()
    assert (home / "backtest" / "with_zero_volume" / "SENSEX").exists() and (home / "backtest" / "SENSEX").exists()
    assert "python fno_store.py clear --underlying SENSEX" in (home / "archive" / ARCHIVE / "CLEARED.txt").read_text()


def test_delete_rules(data):
    home, env = data
    before = tree(str(home))
    assert do(env, delete=True)[0] == 1                                          # needs --apply
    code, out = do(env, apply=True, delete=True, yes=True)                       # not interactive
    assert code == 1 and "interactive terminal" in out
    code, out = do(env, apply=True, delete=True, yes=True, interactive=lambda: True, ask=lambda q: "delete sensex")
    assert code == 1 and "not confirmed" in out and tree(str(home)) == before
    asked = []
    code, out = do(env, apply=True, delete=True, interactive=lambda: True,
                   ask=lambda q: asked.append(q) or ("y" if "[y/N]" in q else "DELETE SENSEX"))
    assert code == 0, out
    assert len(asked) == 2 and "DELETE SENSEX" in asked[1]
    assert all(not v for v in sensex_days(home).values()) and not (home / "archive").exists()
    assert len(runs(home / "raw" / "dhan" / "with_zero_volume")) == 2


def test_lock_busy_or_job_running(data):
    home, env = data
    before = tree(str(home))
    lock = fno.acquire_lock(str(home / "raw" / "dhan" / "with_zero_volume"), "SENSEX", "dashboard", "dhan")
    try:
        code, out = do(env, apply=True, yes=True)
    finally:
        lock.release()
    assert code == 1 and "Not run: Another SENSEX_FnO_1min_Dhan run" in out
    with JOB_LOCK:
        JOB["state"] = "running"
    try:
        code, out = do(env, apply=True, yes=True)
    finally:
        with JOB_LOCK:
            JOB["state"] = "idle"
    assert code == 1 and "download job is running" in out
    assert tree(str(home)) == before


def test_window_confirmation(data):
    home, env = data
    before = tree(str(home))
    code, out = do(env, apply=True)
    assert code == 1 and "confirm with --yes" in out
    code, out = do(env, apply=True, interactive=lambda: True, ask=lambda q: "n")
    assert code == 1 and "not confirmed" in out and tree(str(home)) == before
    assert do(env, apply=True, interactive=lambda: True, ask=lambda q: "y")[0] == 0


def test_window_split_by_today(data, monkeypatch):
    home, env = data
    monkeypatch.setattr(fno, "today_ist", lambda now=None: date(2026, 10, 15))   # window 9 to 15 Oct
    code, out = do(env, sources=["kite"], variants=["with"])
    assert "Can be downloaded again: 2026-10-09, 2026-10-15." in out
    assert "Cannot be downloaded again (outside the window): 2026-10-16" in out


def test_error_midway_lists_moved_and_restore_works(data, monkeypatch):
    home, env = data
    before = sensex_days(home)
    real, calls = fno_clear.move_item, []

    def flaky(src, dst):
        calls.append(src)
        if len(calls) == 4:
            raise OSError("folder open in Explorer")
        real(src, dst)
    monkeypatch.setattr(fno_clear, "move_item", flaky)
    code, out = do(env, apply=True, yes=True)
    assert code == 1 and "Stopped: OSError: folder open in Explorer" in out and "Moved before the error (3)" in out
    assert len(runs(home / "raw" / "dhan" / "with_zero_volume")) == 5             # rows not removed
    monkeypatch.setattr(fno_clear, "move_item", real)
    lines = []
    assert fno_clear.restore(str(home / "archive" / ARCHIVE), apply=True, log=lines.append) == 0, lines
    assert sensex_days(home) == before


# --- restore ------------------------------------------------------------------------------------------------

def test_restore_round_trip(data):
    home, env = data
    before_days, before_rows = sensex_days(home), runs(home / "raw" / "kite" / "with_zero_volume")
    assert do(env, apply=True, yes=True)[0] == 0
    arch = str(home / "archive" / ARCHIVE)
    snapshot = tree(str(home / "raw"))
    lines = []
    assert fno_clear.restore(arch, log=lines.append) == 0 and tree(str(home / "raw")) == snapshot
    assert lines[-1].startswith("Dry run")
    assert fno_clear.restore(arch, apply=True, log=lines.append) == 0
    assert sensex_days(home) == before_days and runs(home / "raw" / "kite" / "with_zero_volume") == before_rows
    assert (home / "backtest" / "SENSEX" / "publish_manifest.json").exists()
    root = fno.fno_root(str(home / "raw" / "kite" / "with_zero_volume"), "SENSEX")
    assert [n for n in os.listdir(root) if n.startswith("_DAYS-")] == ["_DAYS-3_2026-10-09_to_2026-10-16.txt"]


def test_restore_refuses_paths_that_exist_again(data):
    home, env = data
    assert do(env, apply=True, yes=True)[0] == 0
    arch = str(home / "archive" / ARCHIVE)
    man = json.loads(open(os.path.join(arch, fno_clear.MANIFEST), encoding="utf-8").read())
    fresh = next(i["src"] for i in man["items"] if "2026-10-16" in i["src"] and "kite" in i["src"])
    os.makedirs(fresh)                                                           # a fresh download of 16 Oct
    db = fno_clear.open_db(str(home / "raw" / "kite" / "with_zero_volume"))
    db.execute("INSERT INTO chunks (run, symbol, chunk_from, status) VALUES ('fno:SENSEX:BFO:2026-10-16', 'X', 'a', 'DONE')")
    db.execute("INSERT INTO chunks (run, symbol, chunk_from, status) VALUES ('fno:SENSEX:BFO:2026-10-15', 'X', 'a', 'DONE')")
    db.commit()
    db.close()
    lines = []
    assert fno_clear.restore(arch, apply=True, log=lines.append) == 1
    out = "\n".join(lines)
    assert f"REFUSED {fresh}: the path exists again" in out
    assert "REFUSED rows of fno:SENSEX:BFO:2026-10-15" in out and "other rows for it now" in out
    assert os.listdir(fresh) == []                                               # not overwritten
    assert sensex_days(home)[("dhan", "with_zero_volume")] == LOST + OK          # the rest came back


def test_cli_restore(data, monkeypatch, capsys):
    home, env = data
    monkeypatch.setattr(cli, "load_env", lambda: env)
    assert do(env, apply=True, yes=True)[0] == 0
    assert cli.main(["restore", "--from", str(home / "archive" / ARCHIVE)]) == 0
    assert "Dry run" in capsys.readouterr().out
    assert cli.main(["restore", "--from", str(home / "nope")]) == 1

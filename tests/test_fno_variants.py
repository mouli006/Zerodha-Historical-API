"""The with / without zero-volume F&O stores: which folder a run writes to (config.fno_target, routes, CLIs), the
without copy derived after a keep zero-volume ON run (backfill/fno_derive.py), fno_store.py derive-without /
compare-variants / split-stores / move-raw --variant, and publish / the loader per variant.
tmp_path only, .env stubbed, no network."""
import csv
import filecmp
import os
import sqlite3
from datetime import datetime

import pandas as pd
import pytest

import config
import dashboard
import dhan
import fno_store as cli
import nifty_daily
import test_fno
from backfill import bhav, fno, fno_backtest, fno_days, fno_derive, fno_publish
from backfill import fno_store as store
from backfill.job import JOB, JOB_LOCK, job_begin
from test_bhav import row
from test_fno import ACCESS, RUN_DATE, W1, FakeKite, fake_kite, set_now  # noqa: F401
from test_fno_entrypoints import WINDOW, client, ist_now, lock_free, post, wait_for  # noqa: F401
from test_fno_store import D1, D2, D3, boot_of, make_day, tree

IST = fno.IST
DAY = RUN_DATE.isoformat()
WITH = "2026-10-12_exp-2026-10-13_withZeroVol_dl-2026-10-12_1600"
WITHOUT = "2026-10-12_exp-2026-10-13_noZeroVol_dl-2026-10-12_1600"
ZERO = "NIFTY2515013OCT26CE"          # token 13 of the fake dump: candles, all with volume 0
ZERO_SYM = "NIFTY2610625150CE"


@pytest.fixture
def clean_dump(monkeypatch, fake_kite):
    """The fake dump without the contracts that always fail (bad strike, token 31), so a run is "finished"."""
    full = test_fno.make_dump
    monkeypatch.setattr(test_fno, "make_dump", lambda: [i for i in full() if i["instrument_token"] not in (15, 31)])
    return fake_kite


def run(data, keep_zero, derive_to=None, refetch=False):
    assert job_begin(**fno.job_fields("NIFTY", RUN_DATE, str(data), 1000, keep_zero))
    if refetch:
        return fno.refetch_no_trade("key", ACCESS, RUN_DATE, str(data), 1000, log=lambda m: None,
                                    derive_to=derive_to and str(derive_to))[0]
    return fno.run_daily("key", ACCESS, RUN_DATE, str(data), 1000, log=lambda m: None, keep_zero=keep_zero,
                         derive_to=derive_to and str(derive_to))


def root(data, source="kite"):
    return data / fno.fno_folder("NIFTY", source)


def names(day_dir):
    return sorted(fno_derive.contract_files(str(day_dir)))


def read_rows(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def chunk_rows(data, run=None):
    db = sqlite3.connect(str(data / "progress.sqlite"))
    try:
        return {r[0]: (r[1], r[2]) for r in db.execute(
            "SELECT symbol, status, reason FROM chunks WHERE run = ?", (run or fno.run_key("NIFTY", RUN_DATE),))}
    finally:
        db.close()


# --- which folder: config ------------------------------------------------------------------------

def test_fno_target_resolves_only_the_base(tmp_path, monkeypatch):
    base = str(tmp_path / "raw")
    env = {"FNO_RAW_KITE": base}
    w, wo = os.path.join(base, "with_zero_volume"), os.path.join(base, "without_zero_volume")
    assert config.fno_target(base, "kite", True, True, env) == (w, wo)
    assert config.fno_target(base + os.sep, "kite", True, False, env) == (w, None)
    assert config.fno_target(base, "kite", False, True, env) == (wo, None)
    assert config.fno_target(str(tmp_path / "other"), "kite", True, True, env) == (str(tmp_path / "other"), None)
    assert config.fno_target(base, "dhan", True, True, env) == (base, None)     # FNO_RAW_DHAN not set
    assert config.fno_target(base, "kite", True, None, env) == (w, None)        # derive_off: the setting is OFF
    assert config.store_variant(w) == "with" and config.store_variant(wo) == "without"
    assert config.store_variant(base) is None
    assert config.store_base(w, "kite", env) == base and config.store_base(str(tmp_path), "kite", env) == str(tmp_path)


def test_default_cli_folder(monkeypatch):
    monkeypatch.setattr(config, "keep_zero_volume", lambda: True)
    env = {"FNO_RAW_DHAN": "D:/raw/dhan", "KITE_DATA_DIR": "D:/old"}
    assert config.default_fno_folder("dhan", None, env) == os.path.join("D:/raw/dhan", "with_zero_volume")
    assert config.default_fno_folder("dhan", False, env) == os.path.join("D:/raw/dhan", "without_zero_volume")
    assert config.default_fno_folder("kite", True, env) == "D:/old"   # not split: as before


def test_derive_setting(monkeypatch):
    monkeypatch.undo()   # the real config.derive_without, with a stubbed .env
    for value, want in (("OFF", False), ("on", True), ("", config.DERIVE_WITHOUT_ZERO_VOLUME)):
        monkeypatch.setattr(config, "load_env", lambda v=value: {"DERIVE_WITHOUT_ZERO_VOLUME": v})
        assert config.derive_without() is want


# --- runs in the stores ----------------------------------------------------------------------------

def test_run_refuses_the_other_store(tmp_path, clean_dump):
    data = tmp_path / "with_zero_volume"
    assert run(data, keep_zero=False) == "error"
    assert not root(data).exists()
    assert "without_zero_volume store" in JOB["message"]


def test_no_rename_inside_a_store(tmp_path, clean_dump):
    """A noZeroVol day in the with store (e.g. moved there by hand) keeps its name after an ON run."""
    assert run(tmp_path / "plain", keep_zero=False) == "finished"
    data = tmp_path / "with_zero_volume"
    data.mkdir()
    os.rename(root(tmp_path / "plain"), root(data))
    assert run(data, keep_zero=True) == "finished"
    assert fno.day_folders(str(root(data))) == {RUN_DATE: WITHOUT}


def test_auto_derive_after_an_on_run(tmp_path, clean_dump):
    w, wo = tmp_path / "with_zero_volume", tmp_path / "without_zero_volume"
    assert run(w, keep_zero=True, derive_to=wo) == "finished"
    a, b = root(w) / WITH, root(wo) / WITHOUT
    assert ZERO in names(a) and names(b) == [n for n in names(a) if n != ZERO]
    assert (b / fno_derive.DERIVED_FILE).read_text() == WITH + "\n"
    assert {r["Keep_zero_volume"] for r in read_rows(b / "summary.csv")} == {"OFF"}
    assert [(r["Contract"], r["Kept"]) for r in read_rows(b / fno.ZERO_VOLUME_FILE)] == [(ZERO, "N")]
    assert [(r["Contract"], r["Kept"]) for r in read_rows(a / fno.ZERO_VOLUME_FILE)] == [(ZERO, "Y")]   # with unchanged
    assert chunk_rows(wo)[ZERO_SYM] == ("DONE", fno.ZERO_VOLUME)
    assert chunk_rows(wo)["NIFTY26OCTFUT"][0] == "DONE"
    with open(b / "run.log", encoding="utf-8") as f:
        text = f.read()
    assert f"Derived from {WITH}" in text and "removed 1 zero-volume contracts" in text
    assert fno_days.read_day_index(str(root(wo)))[0]["Folder"] == WITHOUT
    assert "Without-zero-volume copy saved" in JOB["message"]
    with open(a / "run.log", encoding="utf-8") as f:
        assert "Without-zero-volume copy saved" in f.read()
    assert not any(n.endswith(".part") for n in os.listdir(root(wo)))


def test_no_derive_without_derive_to(tmp_path, clean_dump):
    assert run(tmp_path / "with_zero_volume", keep_zero=True) == "finished"
    assert not (tmp_path / "without_zero_volume").exists()


def test_no_derive_after_failures(tmp_path, fake_kite):
    w, wo = tmp_path / "with_zero_volume", tmp_path / "without_zero_volume"
    assert run(w, keep_zero=True, derive_to=wo) == "finished_failures"
    assert not root(wo).exists() and "some contracts failed" in JOB["message"]


def test_existing_without_day_is_compared_not_overwritten(tmp_path, clean_dump):
    w, wo = tmp_path / "with_zero_volume", tmp_path / "without_zero_volume"
    assert run(wo, keep_zero=False) == "finished"            # a real OFF download
    before = tree(str(root(wo) / WITHOUT))
    assert run(w, keep_zero=True, derive_to=wo) == "finished"
    assert tree(str(root(wo) / WITHOUT)) == before
    assert f"Without copy already present ({WITHOUT}): identical" in JOB["message"]
    # a differing copy is reported, still not touched
    fut = root(wo) / WITHOUT / "futures" / "NIFTY27OCT26.csv"
    fut.write_text(fut.read_text() + "NIFTY27OCT26,20261012,15:59:00,1,1,1,1,1,1\n")
    line = fno_derive.after_run(str(root(w) / WITH), str(wo), "NIFTY")
    assert "differs: 1 files" in line and "NIFTY27OCT26" in line


def test_derive_skipped_while_the_without_root_is_busy(tmp_path, clean_dump):
    w, wo = tmp_path / "with_zero_volume", tmp_path / "without_zero_volume"
    assert run(w, keep_zero=True) == "finished"
    lock = fno.acquire_lock(str(wo), "NIFTY", "other")
    try:
        line = fno_derive.after_run(str(root(w) / WITH), str(wo), "NIFTY")
    finally:
        lock.release()
    assert "not made" in line and not fno.day_folders(str(root(wo)))


def test_refetch_derives(tmp_path, clean_dump):
    w, wo = tmp_path / "with_zero_volume", tmp_path / "without_zero_volume"
    assert run(w, keep_zero=True) == "finished"
    assert run(w, keep_zero=True, derive_to=wo, refetch=True) == "finished"
    assert fno.day_folders(str(root(wo))) == {RUN_DATE: WITHOUT}


def test_redownload_derives_or_compares(tmp_path, clean_dump, monkeypatch):
    monkeypatch.setattr(fno, "today_ist", lambda: RUN_DATE)
    w, wo = tmp_path / "with_zero_volume", tmp_path / "without_zero_volume"
    assert run(w, keep_zero=True) == "finished"
    saved, msg = fno.redownload_contract("key", ACCESS, RUN_DATE, str(w), ZERO, speed=1000, log=lambda m: None,
                                         keep_zero=True, derive_to=str(wo))
    assert saved and "Without-zero-volume copy saved" in msg
    saved, msg = fno.redownload_contract("key", ACCESS, RUN_DATE, str(w), ZERO, speed=1000, log=lambda m: None,
                                         keep_zero=True, derive_to=str(wo))
    assert "already present" in msg and "identical" in msg
    assert fno.redownload_contract("key", ACCESS, RUN_DATE, str(wo), ZERO, keep_zero=True)[1].startswith(str(wo))


# --- the equivalence rule ---------------------------------------------------------------------------

def test_derived_on_day_equals_the_off_download(tmp_path, clean_dump):
    """The same day downloaded with ON and with OFF: the ON day minus its zero-volume files is the OFF day,
    file for file and byte for byte (zero_volume_contracts.csv too)."""
    on, off, out = tmp_path / "on", tmp_path / "off", tmp_path / "derived"
    assert run(on, keep_zero=True) == "finished"
    assert run(off, keep_zero=False) == "finished"
    derived, removed = fno_derive.derive_day(str(root(on) / WITH), str(out), "NIFTY")
    real = root(off) / WITHOUT
    assert removed == 1 and os.path.basename(derived) == WITHOUT
    assert fno_derive.compare_days(derived, str(real)) == ([], [], [])
    assert names(real) and names(derived) == names(real)
    assert filecmp.cmp(os.path.join(derived, fno.ZERO_VOLUME_FILE), real / fno.ZERO_VOLUME_FILE, shallow=False)
    assert chunk_rows(out)[ZERO_SYM] == chunk_rows(off)[ZERO_SYM] == ("DONE", fno.ZERO_VOLUME)
    s_d, s_r = read_rows(os.path.join(derived, "summary.csv")), read_rows(real / "summary.csv")
    assert s_d == s_r


def test_derived_copy_gets_its_own_bhavcopy_check(tmp_path, clean_dump):
    on, out = tmp_path / "on", tmp_path / "out"
    assert run(on, keep_zero=True) == "finished"
    day = root(on) / WITH
    (day / "bhavcopy").mkdir()
    fno.write_csv(str(day / "bhavcopy" / "nifty_bhav.csv"), bhav.BHAV_HEADER,
                  [row("IDF", "2026-10-27", vol=3, last=25104, oi=1004),
                   row("IDO", W1.isoformat(), "25150.00", "CE", vol=2, last=81, oi=1001)])
    bhav.check_saved(str(day), RUN_DATE, log=lambda m: None)
    (day / "bhavcopy" / "exports").mkdir()
    (day / "bhavcopy" / "exports" / "old.xlsx").write_text("x")
    status = {r["Contract"]: r["Status"] for r in read_rows(day / "bhavcopy" / "bhav_compare.csv")}
    assert "ZERO_VOLUME_IN_KITE" in status[ZERO]
    derived, _ = fno_derive.derive_day(str(day), str(out), "NIFTY")
    status = {r["Contract"]: r["Status"] for r in read_rows(os.path.join(derived, "bhavcopy", "bhav_compare.csv"))}
    assert "MISSING_IN_KITE" in status[ZERO]
    assert not os.path.exists(os.path.join(derived, "bhavcopy", "exports"))
    assert os.path.exists(os.path.join(derived, "bhavcopy", bhav.DAYS_SUMMARY_FILE))
    assert {r["Keep_zero_volume"] for r in read_rows(os.path.join(derived, "summary.csv"))} == {"OFF"}


# --- derive-without -----------------------------------------------------------------------------------

@pytest.fixture
def with_day(tmp_path, clean_dump):
    w = tmp_path / "raw" / "with_zero_volume"
    assert run(w, keep_zero=True) == "finished"
    return w


def derive_cli(tmp_path, *extra):
    lines = []
    w, wo = tmp_path / "raw" / "with_zero_volume", tmp_path / "raw" / "without_zero_volume"
    code = fno_derive.derive_without(str(w), str(wo), "kite", ["NIFTY", "SENSEX"], apply="--apply" in extra,
                                     log=lines.append)
    return code, "\n".join(lines)


def test_derive_without_dry_run_writes_nothing(tmp_path, with_day):
    before = tree(str(tmp_path))
    code, out = derive_cli(tmp_path)
    assert code == 0 and f"would derive {WITH} -> {WITHOUT}: remove 1 zero-volume files of 6" in out
    assert tree(str(tmp_path)) == before and not (tmp_path / "raw" / "without_zero_volume").exists()
    code, out = derive_cli(tmp_path, "--apply")
    assert code == 0 and "derived" in out and fno.day_folders(str(root(tmp_path / "raw" / "without_zero_volume")))
    code, out = derive_cli(tmp_path, "--apply")
    assert code == 0 and "already in the without store" in out


def test_derive_without_mismatch_writes_nothing(tmp_path, with_day):
    fno.write_csv(str(root(with_day) / WITH / fno.ZERO_VOLUME_FILE), fno.ZERO_VOLUME_COLUMNS, [])
    code, out = derive_cli(tmp_path, "--apply")
    assert code == 1 and "ERROR" in out and ZERO in out
    assert not fno.day_folders(str(root(tmp_path / "raw" / "without_zero_volume")))


def test_derive_part_is_removed_on_error(tmp_path, with_day, monkeypatch):
    wo_root = root(tmp_path / "raw" / "without_zero_volume")
    stale = wo_root / (WITHOUT + ".part")
    stale.mkdir(parents=True)
    (stale / "junk.txt").write_text("x")

    def boom(*a, **k):
        raise OSError("disk full")
    monkeypatch.setattr(fno, "rewrite_summary", boom)
    code, out = derive_cli(tmp_path, "--apply")
    assert code == 1 and "FAILED OSError: disk full" in out
    assert not any(n.endswith(".part") or n.startswith("2026") for n in os.listdir(wo_root))
    assert not os.path.exists(tmp_path / "raw" / "without_zero_volume" / "progress.sqlite")


def test_derive_without_skips_old_and_no_zero_days(tmp_path, with_day):
    make_day(str(with_day), D1, form="old")
    make_day(str(with_day), D2, keep_zero=False)
    code, out = derive_cli(tmp_path)
    assert code == 0 and "old-style name" in out and "is noZeroVol" in out


def test_derive_without_lock(tmp_path, with_day):
    lock = fno.acquire_lock(str(with_day), "NIFTY", "dashboard")
    try:
        code, out = derive_cli(tmp_path, "--apply")
    finally:
        lock.release()
    assert code == 1 and "Not run: Another NIFTY_FnO_1min run" in out


def test_derive_without_lists_days_only_in_without(tmp_path, with_day):
    make_day(str(tmp_path / "raw" / "without_zero_volume"), D1, keep_zero=False)
    code, out = derive_cli(tmp_path)
    assert "only in the without store" in out and "can't be derived" in out


# --- compare-variants ---------------------------------------------------------------------------------

def test_compare_variants(tmp_path, clean_dump):
    w, wo = tmp_path / "with_zero_volume", tmp_path / "without_zero_volume"
    assert run(w, keep_zero=True) == "finished"
    assert run(wo, keep_zero=False) == "finished"
    before = tree(str(tmp_path))
    lines = []
    assert fno_derive.compare_variants(str(w), str(wo), "kite", ["NIFTY"], log=lines.append) == 0
    assert "NIFTY 2026-10-12: identical" in lines[0] and tree(str(tmp_path)) == before
    (root(wo) / WITHOUT / "options" / "NIFTY2510013OCT26PE.csv").unlink()
    lines = []
    assert fno_derive.compare_variants(str(w), str(wo), "kite", ["NIFTY"], log=lines.append) == 1
    assert "only in derived 1: NIFTY2510013OCT26PE" in lines[0]


def test_cli_compare_and_derive_use_env(tmp_path, clean_dump, monkeypatch, capsys):
    base = tmp_path / "raw"
    assert run(base / "with_zero_volume", keep_zero=True) == "finished"
    monkeypatch.setattr(cli, "load_env", lambda: {"FNO_RAW_KITE": str(base)})
    assert cli.main(["derive-without", "--source", "kite", "--underlying", "NIFTY"]) == 0
    assert "would derive" in capsys.readouterr().out
    assert cli.main(["derive-without", "--source", "kite", "--apply"]) == 0
    assert cli.main(["compare-variants", "--source", "kite"]) == 0
    assert "identical" in capsys.readouterr().out
    monkeypatch.setattr(cli, "load_env", lambda: {})
    assert cli.main(["compare-variants", "--source", "dhan"]) == 1


# --- split-stores and move-raw --variant ----------------------------------------------------------------

def add_progress(folder, day, source="kite"):
    db = store.open_db(str(folder))
    db.execute("INSERT INTO chunks (run, symbol, token, chunk_from, chunk_to, status) VALUES (?, 'S', 1, 'a', 'b', 'DONE')",
               (fno.run_key("NIFTY", day, source),))
    db.commit()
    db.close()


def test_split_stores(tmp_path):
    base = tmp_path / "kite"
    make_day(str(base), D1)                                  # withZeroVol
    make_day(str(base), D2, keep_zero=False)                 # noZeroVol
    make_day(str(base), D3, form="old")                      # old-style, summary.csv says ON
    make_day(str(base), "2026-10-12", form="old", summary=False)   # old-style, nothing to tell
    r = root(base)
    fno.write_csv(str(r / fno.EXPIRIES_FILE), ["expiry", "first_seen"], [{"expiry": "2026-10-13", "first_seen": "2026-10-01"}])
    (r / "holidays.log").write_text("2026-10-11 10:00:00 2026-10-10: closed.\n")
    add_progress(base, D1)
    add_progress(base, D2)
    env = {"FNO_RAW_KITE": str(base)}
    before, lines = tree(str(tmp_path)), []
    assert store.split_stores(["kite", "dhan"], env, log=lines.append) == 1     # FNO_RAW_DHAN not set
    out = "\n".join(lines)
    assert tree(str(tmp_path)) == before and "Dry run" in out
    assert "SKIPPED" in out and "2026-10-12" in out and f"NIFTY {D2}: only noZeroVol" in out
    lines = []
    assert store.split_stores(["kite"], env, apply=True, log=lines.append) == 0
    w, wo = root(base / "with_zero_volume"), root(base / "without_zero_volume")
    assert sorted(d.isoformat() for d in fno.day_folders(str(w))) == [D1, D3]
    assert [d.isoformat() for d in fno.day_folders(str(wo))] == [D2]
    assert [d.isoformat() for d in fno.day_folders(str(r))] == ["2026-10-12"]   # left in place
    for s in (w, wo):
        assert "2026-10-13" in store.read_expiries_file(str(s)) and "closed" in (s / "holidays.log").read_text()
    assert chunk_rows(base / "with_zero_volume", fno.run_key("NIFTY", D1)) == {"S": ("DONE", None)}
    assert chunk_rows(base / "without_zero_volume", fno.run_key("NIFTY", D2)) == {"S": ("DONE", None)}
    assert fno_days.read_day_index(str(w)) and fno_days.read_day_index(str(wo))


def test_split_stores_lists_duplicates(tmp_path):
    base = tmp_path / "kite"
    make_day(str(base), D1)
    have = make_day(str(base / "with_zero_volume"), D1)
    lines = []
    assert store.split_stores(["kite"], {"FNO_RAW_KITE": str(base)}, apply=True, log=lines.append) == 0
    assert any("DUPLICATE" in x for x in lines)
    assert fno.day_folders(str(root(base))) and os.path.isdir(have)


def test_move_raw_into_a_store_takes_its_setting_only(tmp_path, monkeypatch, capsys):
    old, base = tmp_path / "old", tmp_path / "dhan"
    make_day(str(old), D1, source="dhan", keep_zero=False)
    make_day(str(old), D2, source="dhan", keep_zero=True)
    monkeypatch.setattr(cli, "load_env", lambda: {"FNO_RAW_DHAN": str(base)})
    assert cli.main(["move-raw", "--from", str(old), "--source", "dhan", "--variant", "without", "--apply"]) == 0
    out = capsys.readouterr().out
    assert "REFUSED" in out and "is withZeroVol" in out
    assert [d.isoformat() for d in fno.day_folders(str(root(base / "without_zero_volume", "dhan")))] == [D1]
    assert [d.isoformat() for d in fno.day_folders(str(root(old, "dhan")))] == [D2]
    monkeypatch.setattr(cli, "load_env", lambda: {})
    assert cli.main(["move-raw", "--from", str(old), "--source", "dhan", "--variant", "with"]) == 1


# --- publish and the loader ---------------------------------------------------------------------------

def test_publish_per_variant_and_old_layout(tmp_path):
    raw, bt = tmp_path / "raw", tmp_path / "bt"
    make_day(str(raw / "with_zero_volume"), D1)
    d = make_day(str(raw / "without_zero_volume"), D1, keep_zero=False)
    with open(os.path.join(d, fno_derive.DERIVED_FILE), "w") as f:
        f.write("x\n")
    (bt / "NIFTY").mkdir(parents=True)
    (bt / "NIFTY" / fno_publish.MANIFEST).write_text("{}")
    lines = []
    assert fno_publish.publish_variants({"kite": str(raw), "dhan": None}, str(bt), ["with", "without"], ["NIFTY"],
                                        log=lines.append) == 0
    assert any("Old layout" in x and "left in place" in x for x in lines) and (bt / "NIFTY").exists()
    w = fno_backtest.load_days("NIFTY", root=str(bt))
    wo = fno_backtest.load_days("NIFTY", root=str(bt), variant="without")
    assert w["kite_derived"].tolist() == [False] and wo["kite_derived"].tolist() == [True]
    assert w["kite_zero_volume"].tolist() == ["with"] and wo["kite_zero_volume"].tolist() == ["no"]
    day_w = fno_backtest.load_fno_day("NIFTY", D1, root=str(bt))
    day_wo = fno_backtest.load_fno_day("NIFTY", D1, root=str(bt), variant="without")
    assert len(day_w) and len(day_wo)
    assert len(fno_backtest.load_eod("NIFTY", D1, D1, root=str(bt), variant="without"))
    with pytest.raises(ValueError, match="variant must be"):
        fno_backtest.load_days("NIFTY", root=str(bt), variant="both")
    lines = []
    assert fno_publish.publish_variants({"kite": str(raw)}, str(bt), ["without"], ["NIFTY"], prune_old_layout=True,
                                        log=lines.append) == 0
    assert not (bt / "NIFTY").exists() and (bt / "with_zero_volume" / "NIFTY").exists()


def test_cli_publish_variant(tmp_path, monkeypatch):
    make_day(str(tmp_path / "raw" / "without_zero_volume"), D1, keep_zero=False)
    monkeypatch.setattr(cli, "load_env", lambda: {"FNO_RAW_KITE": str(tmp_path / "raw"), "FNO_BACKTEST_DIR": str(tmp_path / "bt")})
    assert cli.main(["publish", "--variant", "without", "--underlying", "NIFTY"]) == 0
    assert (tmp_path / "bt" / "without_zero_volume" / "NIFTY" / "days.csv").exists()
    assert not (tmp_path / "bt" / "with_zero_volume").exists()


# --- dashboard and CLI -----------------------------------------------------------------------------------

def test_dashboard_start_resolves_the_store(client, tmp_path, monkeypatch):
    base = tmp_path / "raw"
    monkeypatch.setattr(dashboard, "load_env", lambda: {"KITE_ACCESS_TOKEN": "x", "FNO_RAW_KITE": str(base)})
    seen = []

    def fake_run(*a, keep_zero=False, derive_to=None, **kw):
        seen.append((a[3], keep_zero, derive_to, JOB["derive_without"]))
        with JOB_LOCK:
            JOB["state"] = "finished"
    monkeypatch.setattr(fno, "run_daily", fake_run)
    w, wo = str(base / "with_zero_volume"), str(base / "without_zero_volume")
    for form, want in ((dict(keep_zero="1", derive="1"), (w, True, wo, True)),
                       (dict(keep_zero="1", derive="0"), (w, True, None, False)),
                       (dict(keep_zero="0", derive="1"), (wo, False, None, False))):
        seen.clear()
        assert "error" not in post(client, base, **form)
        assert wait_for(lambda: seen) and seen[0] == want
        assert wait_for(lambda: lock_free(want[0])), "lock not released"
    seen.clear()
    assert "error" not in post(client, tmp_path / "mine", keep_zero="1", derive="1")   # any other folder: as it is
    assert wait_for(lambda: seen) and seen[0] == (str(tmp_path / "mine"), True, None, False)
    assert wait_for(lambda: lock_free(tmp_path / "mine"))
    assert "store" in post(client, base / "with_zero_volume", keep_zero="0")


def test_success_page_boot_has_the_stores(client, tmp_path, monkeypatch):
    monkeypatch.setattr(dashboard, "load_env", lambda: {"KITE_ACCESS_TOKEN": "x", "FNO_RAW_KITE": str(tmp_path)})
    html = client.get("/success").get_data(as_text=True)
    b = boot_of(html)["fno"]
    assert b["base"] == str(tmp_path) and b["variants"] == config.VARIANTS and b["derive_without"] is False
    assert "Also save a without-zero-volume copy" in html


def test_dhan_page_shows_the_base_for_a_store_folder(monkeypatch):
    from test_dhan_fno import FakeClient
    env = {"DHAN_CLIENT_ID": "1000000001", "DHAN_ACCESS_TOKEN": "t", "DHAN_TOKEN_EXPIRY": "2099-01-01T09:00:00",
           "FNO_RAW_DHAN": os.path.normpath("D:/fno/raw/dhan")}
    monkeypatch.setattr(dhan, "load_env", lambda: dict(env))
    monkeypatch.setattr(dhan, "keep_zero_volume", lambda: False)
    monkeypatch.setattr(dhan, "DhanClient", FakeClient)
    with JOB_LOCK:
        JOB.update(state="finished", kind="fno", source="dhan",
                   folder=os.path.join(env["FNO_RAW_DHAN"], "with_zero_volume"))
    from app import app
    boot = boot_of(app.test_client().get("/dhan/dashboard").get_data(as_text=True))
    assert boot["form"]["folder"] == env["FNO_RAW_DHAN"] and boot["fno"]["base"] == env["FNO_RAW_DHAN"]
    with JOB_LOCK:
        JOB.update(state="idle", kind="backfill", source="", folder="")


@pytest.fixture
def cli_env(monkeypatch, tmp_path, ist_now):
    env = {"KITE_API_KEY": "k", "KITE_ACCESS_TOKEN": "t", "FNO_RAW_KITE": str(tmp_path / "raw")}
    monkeypatch.setattr(nifty_daily, "load_env", lambda: dict(env))
    monkeypatch.setattr(nifty_daily, "weekly_expiry_weekday", lambda underlying="NIFTY": 1)
    monkeypatch.setattr(nifty_daily, "keep_zero_volume", lambda: False)
    seen = []

    def fake_run(*a, keep_zero=False, derive_to=None, **kw):
        seen.append((a[3], keep_zero, derive_to))
        with JOB_LOCK:
            JOB["state"] = "finished"
        return "finished"
    monkeypatch.setattr(nifty_daily, "run_daily", fake_run)
    return tmp_path / "raw", seen


def test_cli_store_from_the_setting(cli_env, monkeypatch, tmp_path):
    base, seen = cli_env
    w, wo = str(base / "with_zero_volume"), str(base / "without_zero_volume")
    assert nifty_daily.main(["2026-10-12"]) == 0
    assert nifty_daily.main(["2026-10-12", "--keep-zero-volume"]) == 0
    assert nifty_daily.main(["2026-10-12", "--keep-zero-volume", "--derive-without"]) == 0
    monkeypatch.setattr(nifty_daily, "derive_without", lambda: True)
    assert nifty_daily.main(["2026-10-12", "--keep-zero-volume"]) == 0
    assert nifty_daily.main(["2026-10-12", "--keep-zero-volume", "--no-derive-without"]) == 0
    assert nifty_daily.main(["2026-10-12", "--keep-zero-volume", "--folder", str(tmp_path / "mine")]) == 0
    assert seen == [(wo, False, None), (w, True, None), (w, True, wo), (w, True, wo), (w, True, None),
                    (str(tmp_path / "mine"), True, None)]
    assert nifty_daily.main(["2026-10-12", "--folder", w]) == 1   # OFF into the with store


def test_cli_rename_folders_covers_both_stores(cli_env, capsys):
    base, _ = cli_env
    make_day(str(base / "with_zero_volume"), D1, form="old")
    make_day(str(base / "without_zero_volume"), D2, form="old", keep_zero=False)
    assert nifty_daily.main(["--rename-folders", "--source", "kite"]) == 0
    out = capsys.readouterr().out
    assert f"{D1} -> " in out and f"{D2} -> " in out

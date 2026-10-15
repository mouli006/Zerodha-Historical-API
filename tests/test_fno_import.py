"""fno_store.py import (backfill/fno_import.py): replace one underlying's live F&O data with a prepared copy
("Correct data": kite* / dhan* folders with "with zero volume" / "without zero volume" save folders inside).
tmp_path only, .env stubbed, prompts stubbed, publish stubbed unless a test runs the real one, no network."""
import csv
import os
import shutil
import sqlite3

import pytest

import fno_store as cli
from backfill import fno, fno_clear, fno_days, fno_import
from backfill.job import JOB, JOB_LOCK
from test_fno_clear import data, runs, sensex_days, tree  # noqa: F401  (fixture)
from test_fno_store import D1, D2, M1, W1, W2, make_day
from test_fno_store import tree as full_tree
from test_sensex import RUN_DATE, SensexKite, bars, run_sensex, sensex_kite  # noqa: F401  (fixture)

STORES = {"with": "with zero volume", "without": "without_zero_volume"}   # both spellings of the source folders
ARCHIVE = "2026-10-16_1030_import_NIFTY"
SYMS = {"FUT": ("NIFTY26OCTFUT", 1), "CE": ("NIFTY26O1325000CE", 2), "PE": ("NIFTY26O1325050PE", 3),
        "CE2": ("NIFTY26O1325100CE", 4)}


def src_root(src, source, variant, underlying="NIFTY"):
    return fno.fno_root(os.path.join(str(src), f"{source} nifty", STORES[variant]), underlying, source)


def src_day(src, source, variant, day, keep_zero=None, run_log=True):
    """An old-style source day: make_day's files, a full instrument list (CE2 has no file: no candles), run.log
    and zero_volume_contracts.csv; the without day has no file for the zero-volume PE (Kept N)."""
    keep_zero = variant == "with" if keep_zero is None else keep_zero
    save = os.path.dirname(src_root(src, source, variant))
    d = make_day(save, day, "NIFTY", source, form="old", keep_zero=keep_zero)
    pe = fno.contract_name("PE", W1, 25050)
    if not keep_zero:
        os.remove(os.path.join(d, "options", pe + ".csv"))
    fno.write_csv(os.path.join(d, fno.ZERO_VOLUME_FILE), fno.ZERO_VOLUME_COLUMNS,
                  [{"Contract": pe, "Candles": 2, "Last_close": 80, "Last_OI": 1001, "Kept": "Y" if keep_zero else "N"}])
    for n in os.listdir(d):
        if n.startswith("instruments_"):
            os.remove(os.path.join(d, n))
    rows = [(SYMS["FUT"], "FUT", M1, 0), (SYMS["CE"], "CE", W1, 25000), (SYMS["PE"], "PE", W1, 25050),
            (SYMS["CE2"], "CE", W1, 25100)]
    fno.write_csv(os.path.join(d, f"instruments_{fno.exchange_of('NIFTY', source)}_fetched_2026-10-14.csv"),
                  ["instrument_token", "tradingsymbol", "name", "expiry", "strike", "instrument_type", "lot_size"],
                  [{"instrument_token": tok, "tradingsymbol": sym, "name": "NIFTY", "expiry": exp.isoformat(),
                    "strike": strike, "instrument_type": t, "lot_size": 65} for (sym, tok), t, exp, strike in rows])
    if run_log:
        with open(os.path.join(d, "run.log"), "w", encoding="utf-8") as f:
            f.write(f"2026-10-14 09:05:00 === NIFTY F&O 1-minute run for {day} (NFO), started 2026-10-14 09:05:00 IST\n"
                    f"2026-10-14 09:05:00 Keep zero-volume contracts: {fno.on_off(keep_zero)}\n"
                    "2026-10-14 09:06:00 Failed: 0\n2026-10-14 09:06:00 === Ended 2026-10-14 09:06:00 IST\n")
    return d


def target(day, variant, source="kite"):
    return f"{day}_exp-{W1}_{'withZeroVol' if variant == 'with' else 'noZeroVol'}_dl-2026-10-14_0905"


@pytest.fixture
def setup(data, tmp_path, monkeypatch):
    """data (test_fno_clear: SENSEX + NIFTY 14 Oct live, rows, backtest) plus a source tree: Kite D1, D2 and Dhan
    D1 in both stores; live copies of Kite D1 (with: the same, without: changed) to compare against."""
    home, env = data
    monkeypatch.setattr(fno_import, "weekly_expiry_weekday", lambda u="NIFTY": 1)
    src = tmp_path / "Correct data"
    for source, days in (("kite", (D1, D2)), ("dhan", (D1,))):
        for variant in STORES:
            for day in days:
                src_day(src, source, variant, day)
            root = src_root(src, source, variant)
            fno.write_csv(os.path.join(root, fno.EXPIRIES_FILE), ["expiry", "first_seen"],
                          [{"expiry": e.isoformat(), "first_seen": "2026-10-07"} for e in (W1, W2, M1)])
            with open(os.path.join(root, "holidays.log"), "w", encoding="utf-8") as f:
                f.write("2026-10-14 09:00:00 2026-10-11: no candles, so the market was closed.\n")
                if source == "kite":
                    f.write("2026-10-16 09:00:00 2026-10-15: no candles, so the market was closed.\n")
            with open(os.path.join(root, ".lock"), "w", encoding="utf-8") as f:
                f.write("pid 1 user\n")
    os.makedirs(src / "kite nifty" / STORES["with"] / "SENSEX_FnO_1min" / D1)   # another underlying: ignored
    os.makedirs(src / "notes")                                                  # not recognised
    (src / "kite nifty" / "readme.txt").write_text("x")
    live = {(s, v): fno.fno_root(str(home / "raw" / s / f"{v}_zero_volume"), "NIFTY", s)
            for s in ("kite", "dhan") for v in STORES}
    shutil.copytree(os.path.join(src_root(src, "kite", "with"), D1), os.path.join(live[("kite", "with")], target(D1, "with")))
    changed = os.path.join(live[("kite", "without")], target(D1, "without"))
    shutil.copytree(os.path.join(src_root(src, "kite", "without"), D1), changed)
    with open(os.path.join(changed, "summary.csv"), "a", encoding="utf-8") as f:
        f.write("x\n")
    fno.write_csv(os.path.join(live[("kite", "with")], fno.EXPIRIES_FILE), ["expiry", "first_seen"],
                  [{"expiry": "2026-09-29", "first_seen": "2026-09-20"}, {"expiry": W1.isoformat(), "first_seen": "2026-10-01"}])
    with open(os.path.join(live[("kite", "with")], "holidays.log"), "w", encoding="utf-8") as f:
        f.write("2026-10-11 10:00:00 2026-10-10: closed.\n"
                "2026-10-13 23:00:00 2026-10-11: no candles, so the market was closed.\n")   # logged at another time
    return home, env, src, live


def do(env, src, *, apply=False, yes=True, underlying="NIFTY", ask=lambda q: "n", interactive=lambda: False,
       publish="stub"):
    lines, calls = [], []
    pub = (lambda e, u, log: calls.append(u) or 0) if publish == "stub" else None
    code = fno_import.import_data(str(src), underlying, env, apply=apply, yes=yes, command="python fno_store.py import",
                                  log=lines.append, ask=ask, interactive=interactive, publish=pub)
    return code, "\n".join(lines), calls


def nifty_days(home, source, variant):
    return sorted(n for n in os.listdir(fno.fno_root(str(home / "raw" / source / f"{variant}_zero_volume"), "NIFTY", source))
                  if fno.parse_day_folder(n))


# --- mapping ----------------------------------------------------------------------------------------------------

def test_folder_names_map_to_stores(tmp_path):
    for n, v in (("with zero volume", "with"), ("With_Zero_Volume", "with"), ("without zero volume", "without"),
                 ("without_zero_volume (fixed)", "without"), ("zero volume", None), ("others", None)):
        assert fno_import.variant_of_name(n) == v, n
    for n, s in (("kite nifty", "kite"), ("KITE", "kite"), ("Dhan NIFTY", "dhan"), ("kite and dhan", None), ("nse", None)):
        assert fno_import.source_of_name(n) == s, n


def test_find_stores_ignores_the_rest(setup):
    home, env, src, live = setup
    stores, notes, errors = fno_import.find_stores(str(src), "NIFTY")
    assert sorted(stores) == [("dhan", "with"), ("dhan", "without"), ("kite", "with"), ("kite", "without")]
    assert stores[("kite", "without")] == src_root(src, "kite", "without") and not errors
    text = "\n".join(notes)
    assert f"not recognised, ignored: {src / 'notes'}" in text
    assert f"not recognised, ignored: {src / 'kite nifty' / 'readme.txt'}" in text
    assert "ignored (not NIFTY_FnO_1min)" in text and "SENSEX_FnO_1min" in text
    assert fno_import.find_stores(str(src), "SENSEX")[0] == {("kite", "with"): str(src / "kite nifty" / STORES["with"] / "SENSEX_FnO_1min")}


# --- dry run ------------------------------------------------------------------------------------------------------

def test_dry_run_changes_nothing_and_reports(setup):
    home, env, src, live = setup
    src_before, live_before = full_tree(str(src)), tree(str(home))
    code, out, calls = do(env, src)
    assert code == 0, out
    assert full_tree(str(src)) == src_before and tree(str(home)) == live_before and not calls
    assert not (home / "archive").exists()
    assert f"{D1}: complete; withZeroVol; 1 futures, 2 options; OK" in out
    assert f"-> {target(D1, 'with')}  (exp: summary.csv; zero volume: summary.csv; dl: run.log)" in out
    assert "vs fno now: same" in out
    assert "vs fno now: different: 1 files differ (contents differ 1: summary.csv)" in out
    assert out.count("vs fno now: new") == 4                                       # Kite D2 x2, Dhan D1 x2
    assert out.count("ONLY IN FNO: 2026-10-14_") == 4                              # the live NIFTY day of each store
    assert f"Kite {D1}: identical (1 zero-volume files removed)" in out            # the variant check
    assert "== Archive before the import: NIFTY F&O data" in out and ARCHIVE in out
    assert "SENSEX_FnO_1min" not in out.split("== Archive")[1]                     # only NIFTY is archived
    assert "Totals: 6 days in 4 stores: 1 same, 1 different, 4 new; 4 only in fno; 0 errors." in out
    assert out.endswith("Dry run: nothing changed. Run again with --apply to import.")


def test_variant_check_reports_differences(setup):
    home, env, src, live = setup
    ce = os.path.join(src_root(src, "dhan", "without"), D1, "options", fno.contract_name("CE", W1, 25000) + ".csv")
    with open(ce, "a", encoding="utf-8", newline="\n") as f:
        f.write("x\n")
    code, out, _ = do(env, src)
    assert code == 0                                                                # information only
    assert f"Dhan {D1}: differs: 1 files (contents differ 1: NIFTY1325000CE" in out.replace(fno.contract_name("CE", W1, 25000), "NIFTY1325000CE")


def test_store_errors_refuse_apply(setup):
    home, env, src, live = setup
    src_day(src, "kite", "with", "2026-10-09", keep_zero=False)                     # a noZeroVol day in the with store
    before = tree(str(home))
    code, out, _ = do(env, src)
    assert code == 1 and "is noZeroVol but sits in the with_zero_volume store" in out
    assert out.endswith("Dry run: nothing changed. --apply refuses while errors are listed.")
    code, out, calls = do(env, src, apply=True)
    assert code == 1 and "Not run: fix the errors above first." in out and tree(str(home)) == before and not calls


def test_duplicate_and_unnamed_days_are_errors(setup):
    home, env, src, live = setup
    root = src_root(src, "dhan", "with")
    shutil.copytree(os.path.join(root, D1), os.path.join(root, target(D1, "with")))      # 7 Oct twice
    d = src_day(src, "kite", "without", "2026-10-09", run_log=False)
    os.remove(os.path.join(d, "summary.csv"))
    os.remove(os.path.join(d, fno.ZERO_VOLUME_FILE))
    code, out, _ = do(env, src)
    assert code == 1
    errors = out.split("ERRORS (3):")[1]
    assert f"More than one folder for the same trading day in {root}" in errors
    assert "can't be named: zero-volume setting unknown" in out and "zero-volume setting unknown" in out
    assert "2026-10-09: INCOMPLETE (no summary.csv)" in out


# --- apply ----------------------------------------------------------------------------------------------------------

def test_apply_archives_copies_and_writes_rows(setup):
    home, env, src, live = setup
    src_before, sensex_before = full_tree(str(src)), sensex_days(home)
    sensex_rows = [r for r in runs(home / "raw" / "kite" / "with_zero_volume") if "SENSEX" in r]
    code, out, calls = do(env, src, apply=True)
    assert code == 0, out
    assert full_tree(str(src)) == src_before                                       # the master copy is only read
    assert sensex_days(home) == sensex_before                                      # SENSEX untouched
    assert [r for r in runs(home / "raw" / "kite" / "with_zero_volume") if "SENSEX" in r] == sensex_rows
    arch = home / "archive" / ARCHIVE
    for s in ("kite", "dhan"):
        for v in STORES:
            moved = arch / "raw" / s / f"{v}_zero_volume" / fno.fno_folder("NIFTY", s)
            assert [n for n in os.listdir(moved) if n.startswith("2026-10-14_")]   # the old NIFTY day is archived
    assert nifty_days(home, "kite", "with") == [target(D1, "with"), target(D2, "with")]
    assert nifty_days(home, "dhan", "without") == [target(D1, "without")]
    for (s, v), root in live.items():
        for day in ((D1, D2) if s == "kite" else (D1,)):
            assert full_tree(os.path.join(root, target(day, v))) == full_tree(os.path.join(src_root(src, s, v), day))
        assert not [n for n in os.listdir(root) if n.endswith(".part")]
    root = live[("kite", "with")]
    with open(os.path.join(root, fno.EXPIRIES_FILE), encoding="utf-8") as f:
        exp = {r["expiry"]: r["first_seen"] for r in csv.DictReader(f)}
    assert exp == {"2026-09-29": "2026-09-20", W1.isoformat(): "2026-10-01", W2.isoformat(): "2026-10-07",
                   M1.isoformat(): "2026-10-07"}                                   # union, earlier first_seen kept
    hol = open(os.path.join(root, "holidays.log"), encoding="utf-8").read()
    assert hol.count("2026-10-10: closed") == 1 and hol.count("2026-10-11: no candles") == 1 and "2026-10-15: no candles" in hol
    assert os.path.exists(os.path.join(root, f"_DAYS-2_{D1}_to_{D2}.txt"))
    db = sqlite3.connect(os.path.join(os.path.dirname(root), "progress.sqlite"))
    got = dict(db.execute("SELECT symbol, coalesce(reason, '') FROM chunks WHERE run = ?", (f"fno:NIFTY:NFO:{D1}",)))
    db.close()
    assert got == {SYMS["FUT"][0]: "", SYMS["CE"][0]: "", SYMS["PE"][0]: ""}      # CE2 (no file): no row
    db = sqlite3.connect(str(home / "raw" / "dhan" / "without_zero_volume" / "progress.sqlite"))
    got = dict(db.execute("SELECT symbol, coalesce(reason, '') FROM chunks WHERE run = ?", (f"fno-dhan:NIFTY:NSE_FNO:{D1}",)))
    db.close()
    assert got == {SYMS["FUT"][0]: "", SYMS["CE"][0]: "", SYMS["PE"][0]: fno.ZERO_VOLUME}
    assert "1 contracts of the saved list without a row (a re-run requests them again)" in out
    assert calls == ["NIFTY"]
    report = (arch / fno_import.REPORT).read_text()
    assert "Copied days (6):" in report and "[same]" in report and "[new]" in report
    assert f'restore --from "{arch}" --apply' in report and "clear --underlying NIFTY --apply --yes" in report
    assert (arch / fno_clear.CLEARED).exists()
    assert "Kite: missing 2026-10-14. Download with:" in out
    assert "  python nifty_daily.py --missing --keep-zero-volume" in out
    assert "Dhan: missing 2026-10-14, 2026-10-15." in out
    assert "  python nifty_daily.py --missing --source dhan --keep-zero-volume" in out


def test_apply_publishes_nifty_for_both_stores(setup):
    home, env, src, live = setup
    code, out, _ = do(env, src, apply=True, publish=None)
    assert code == 0, out
    for v in STORES:
        days = (home / "backtest" / f"{v}_zero_volume" / "NIFTY" / "days.csv").read_text()
        assert D1 in days and D2 in days and "2026-10-14" not in days
        assert f"backtest" in out and "days.csv:" in out
    assert (home / "backtest" / "with_zero_volume" / "SENSEX").exists()           # SENSEX left as it was


def test_window_confirmation(setup):
    home, env, src, live = setup
    before = tree(str(home))
    code, out, _ = do(env, src, apply=True, yes=False)
    assert code == 1 and "confirm with --yes" in out and tree(str(home)) == before
    assert "2026-10-14" not in out.split("Cannot be downloaded again")[0].split("Can be downloaded again:")[0][-40:]
    assert do(env, src, apply=True, yes=False, interactive=lambda: True, ask=lambda q: "y")[0] == 0


def test_lock_busy_or_job_running(setup):
    home, env, src, live = setup
    before = tree(str(home))
    lock = fno.acquire_lock(str(home / "raw" / "dhan" / "without_zero_volume"), "NIFTY", "dashboard", "dhan")
    try:
        code, out, _ = do(env, src, apply=True)
    finally:
        lock.release()
    assert code == 1 and "Not run: Another NIFTY_FnO_1min_Dhan run" in out
    with JOB_LOCK:
        JOB["state"] = "running"
    try:
        code, out, _ = do(env, src, apply=True)
    finally:
        with JOB_LOCK:
            JOB["state"] = "idle"
    assert code == 1 and "download job is running" in out and tree(str(home)) == before


def test_copy_error_stops_and_removes_part(setup, monkeypatch):
    home, env, src, live = setup
    src_before = full_tree(str(src))
    monkeypatch.setattr(fno_import, "size_of", lambda p: os.path.getsize(p) + (1 if D2 in p and p.count(".part") else 0))
    code, out, calls = do(env, src, apply=True)
    assert code == 1 and "Stopped: OSError: copy of" in out and "does not match (size)" in out
    assert "Copied 1 of 6 days:" in out and not calls                               # Kite with D1, then D2 failed
    assert nifty_days(home, "kite", "with") == [target(D1, "with")]
    assert not [n for n in os.listdir(live[("kite", "with")]) if n.endswith(".part")]
    assert full_tree(str(src)) == src_before
    report = (home / "archive" / ARCHIVE / fno_import.REPORT).read_text()
    assert "STOPPED by an error" in report and "Copied days (1):" in report


def test_restore_after_import(setup):
    home, env, src, live = setup
    days = lambda: {k: fno.day_folders(r) for k, r in live.items()}   # noqa: E731  (history files stay merged)
    before = days()
    rows_before = runs(home / "raw" / "kite" / "with_zero_volume")
    assert do(env, src, apply=True)[0] == 0
    lines = []
    assert fno_clear.clear("NIFTY", ["dhan", "kite"], ["with", "without"], env, apply=True, yes=True,
                           log=lines.append) == 0, lines                          # the imported days go first
    assert fno_clear.restore(str(home / "archive" / ARCHIVE), apply=True, log=lines.append) == 0, lines
    assert days() == before
    assert runs(home / "raw" / "kite" / "with_zero_volume") == rows_before


def test_fresh_run_after_import_skips_saved_contracts(tmp_path, sensex_kite, monkeypatch):
    """SENSEX day saved with OFF (11: zero volume, 12: no candles), imported into an empty without store: a run of
    the same day afterwards asks only for the contract without a row (12)."""
    monkeypatch.setattr(SensexKite, "candles", {1: bars(5, 72910.0, vol=20), 11: bars(3, 20.0, vol=0)})
    scratch = tmp_path / "scratch"
    assert run_sensex(scratch) == "finished"
    with JOB_LOCK:
        JOB["state"] = "idle"
    day = fno.find_day_dir(fno.fno_root(str(scratch), "SENSEX"), RUN_DATE)
    src = tmp_path / "Correct data"
    root = fno.fno_root(str(src / "kite sensex" / "without_zero_volume"), "SENSEX")
    shutil.copytree(day, os.path.join(root, RUN_DATE.isoformat()))                 # old-style name
    shutil.copy(os.path.join(fno.fno_root(str(scratch), "SENSEX"), fno.EXPIRIES_FILE), root)
    home = tmp_path / "fno"
    env = {"FNO_RAW_KITE": str(home / "raw" / "kite"), "FNO_RAW_DHAN": str(home / "raw" / "dhan"),
           "FNO_BACKTEST_DIR": str(home / "backtest")}
    code, out, _ = do(env, src, apply=True, underlying="SENSEX")
    assert code == 0, out
    save = home / "raw" / "kite" / "without_zero_volume"
    assert len(fno.day_folders(fno.fno_root(str(save), "SENSEX"))) == 1
    with JOB_LOCK:
        JOB["state"] = "idle"
    SensexKite.calls = []
    assert run_sensex(save) == "finished"
    assert {c[0] for c in SensexKite.calls} == {12}


# --- CLI --------------------------------------------------------------------------------------------------------------

def test_cli(setup, monkeypatch, capsys):
    home, env, src, live = setup
    monkeypatch.setattr(cli, "load_env", lambda: env)
    before = tree(str(home))
    assert cli.main(["import", "--from", str(src), "--underlying", "NIFTY"]) == 0
    assert "Dry run: nothing changed" in capsys.readouterr().out and tree(str(home)) == before
    with pytest.raises(SystemExit) as e:
        cli.main(["import", "--from", str(src)])
    assert e.value.code == 1
    assert cli.main(["import", "--from", str(home / "nope"), "--underlying", "NIFTY"]) == 1

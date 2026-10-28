"""Check all four, "Mark reviewed" and "Bhavcopy not published" (backfill/bhav_review.py, nifty_bhav.py --missing-all /
--review / --unreview / --no-bhav / --undo-no-bhav / --reviewed-list, the dashboard routes). Synthetic F&O days in
tmp_path; the bhavcopy fetch is stubbed, .env stubbed, no network."""
import json
import os
from datetime import date, datetime

import pytest

import dashboard
import nifty_bhav
from app import app
from backfill import bhav, bhav_review, fno, fno_clear, fno_publish
from backfill.job import JOB, JOB_LOCK
from test_bhav import kite_file, row, zip_bytes
from test_fno_store import make_day, tree

IST = fno.IST
NOW = datetime(2026, 10, 22, 18, 0, tzinfo=IST)
BASE = {"missing": 0, "extra": 0, "zero_volume": 0, "volume_diffs": 0, "oi_diffs": 0, "price_diffs": 0, "bad_names": 0,
        "bhav_traded": 10, "kite_files": 10, "liquid_missing": 0, "liquid_diffs": 0, "liquid_volume_diffs": 0,
        "far_problems": 0, "red_volume_diffs": 0}
RED = {**BASE, "volume_diffs": 2, "red_volume_diffs": 2, "liquid_diffs": 2}
AMBER = {**BASE, "oi_diffs": 3, "liquid_diffs": 3}
MISSING = {**BASE, "missing": 1, "liquid_missing": 1}
NOT_PUBLISHED = "NSE has no F&O bhavcopy for {d} yet (404: not published, or no trading)."


@pytest.fixture(autouse=True)
def setup(monkeypatch):
    class Blocked:
        def __init__(self, *a, **kw):
            raise AssertionError("network used")
    monkeypatch.setattr(bhav.requests, "Session", Blocked)
    monkeypatch.setattr(fno, "now_ist", lambda: NOW)


def day(folder, d, status="diffs", counts=None, underlying="NIFTY", source="kite", created=None):
    """A saved day (summary.csv) with a stored check (status None: pending). created: the folder's dl time."""
    root = fno.fno_root(str(folder), underlying, source)
    old = fno.find_day_dir(root, d) if os.path.isdir(root) else None
    path = old or os.path.join(root, fno.day_folder_name(d, d, True, created or datetime(2026, 10, 1, 9, 5, tzinfo=IST)))
    os.makedirs(path, exist_ok=True)
    with open(os.path.join(path, "summary.csv"), "w") as f:
        f.write("x")
    if status:
        os.makedirs(os.path.join(path, "bhavcopy"), exist_ok=True)
        with open(os.path.join(path, "bhavcopy", "bhav_check.json"), "w", encoding="utf-8") as f:
            json.dump({"status": status, "line": "Bhavcopy check: CHECK" if status == "diffs" else "Bhavcopy check: OK",
                       "counts": counts or (RED if status == "diffs" else BASE), "checked_at": "2026-10-20 20:00:00 IST"}, f)
    return path


def log_404(folder, d, underlying="NIFTY", source="kite"):
    root = fno.fno_root(str(folder), underlying, source)
    bhav.log_line(os.path.join(root, "bhavcopy.log"), f"{d}: {NOT_PUBLISHED.format(d=d)}")


def status(folder, underlying="NIFTY", source="kite"):
    days = bhav.list_days(str(folder), underlying, source)
    return bhav_review.banners(days, fno.today_ist(), bhav_review.load_events(str(folder), underlying, source))


def env_of(tmp_path):
    return {"FNO_RAW_KITE": str(tmp_path / "kite"), "FNO_RAW_DHAN": str(tmp_path / "dhan")}


def store(tmp_path, source="kite", variant="with"):
    return str(tmp_path / source / {"with": "with_zero_volume", "without": "without_zero_volume"}[variant])


# --- severity() is not changed (D19) ------------------------------------------------------------

@pytest.mark.parametrize("counts, st, expected", [
    ({**BASE, "extra": 2, "oi_diffs": 3, "price_diffs": 1, "liquid_diffs": 6}, "diffs", "minor"),
    ({**BASE, "zero_volume": 1, "volume_diffs": 1, "liquid_volume_diffs": 1}, "diffs", "minor"),
    ({**BASE, "missing": 1, "far_problems": 1}, "diffs", "major"),
    ({**BASE, "volume_diffs": 1, "red_volume_diffs": 1}, "diffs", "major"),
    ({"missing": 0, "volume_diffs": 1}, "diffs", "major"),
    ({"missing": 2}, "diffs", "major"),
    (BASE, "ok", ""),
])
def test_severity_unchanged(counts, st, expected):
    assert bhav.severity(counts, st) == expected


# --- the sidecar ---------------------------------------------------------------------------------

def test_sidecar_path_and_atomic_write(tmp_path, monkeypatch):
    day(tmp_path, "2026-10-19")
    path = bhav_review.sidecar_path(str(tmp_path), "NIFTY")
    assert path == os.path.join(str(tmp_path), "NIFTY_FnO_1min", "bhav_review.json")
    assert bhav_review.review(str(tmp_path), "NIFTY", "kite", date(2026, 10, 19), "first")[1] == ""
    before = open(path, encoding="utf-8").read()

    def broken(src, dst):
        raise OSError("disk full")
    monkeypatch.setattr(bhav_review.os, "replace", broken)
    with pytest.raises(OSError):
        bhav_review.unreview(str(tmp_path), "NIFTY", "kite", date(2026, 10, 19))
    assert open(path, encoding="utf-8").read() == before                       # the old file, whole
    assert sorted(os.listdir(os.path.dirname(path))) == ["2026-10-19_exp-2026-10-19_withZeroVol_dl-2026-10-01_0905",
                                                         "bhav_review.json"]   # no temp file left


# --- review ----------------------------------------------------------------------------------------

def test_review_hides_red_and_amber_days(tmp_path):
    day(tmp_path, "2026-10-19", counts=RED)
    day(tmp_path, "2026-10-20", counts=AMBER)
    day(tmp_path, "2026-10-21", "ok")
    assert status(tmp_path) == {"check": ["2026-10-19"], "minor": ["2026-10-20"], "pending_old": []}
    msg, err = bhav_review.review(str(tmp_path), "NIFTY", "kite", date(2026, 10, 19), "Kite volume, accepted (P5)")
    assert err == "" and "reviewed" in msg
    assert bhav_review.review(str(tmp_path), "NIFTY", "kite", date(2026, 10, 20))[1] == ""
    assert status(tmp_path) == {"check": [], "minor": [], "pending_old": []}
    m = bhav_review.marks(str(tmp_path), "NIFTY", "kite", fno.today_ist())
    assert m["reviewed"] == ["2026-10-20", "2026-10-19"]
    assert m["days"]["2026-10-19"]["review"] == "active" and m["days"]["2026-10-19"]["review_note"] == "Kite volume, accepted (P5)"
    # the check itself is untouched
    days = bhav.list_days(str(tmp_path))
    assert [(d["date"], d["status"], d["severity"]) for d in days] == [
        ("2026-10-21", "ok", ""), ("2026-10-20", "diffs", "minor"), ("2026-10-19", "diffs", "major")]
    assert bhav_review.review(str(tmp_path), "NIFTY", "kite", date(2026, 10, 19))[1].endswith("already reviewed with these counts.")


def test_review_refused_with_missing_contracts(tmp_path):
    day(tmp_path, "2026-10-19", counts=MISSING)
    day(tmp_path, "2026-10-20", None)
    day(tmp_path, "2026-10-21", "ok")
    _, err = bhav_review.review(str(tmp_path), "NIFTY", "kite", date(2026, 10, 19))
    assert "1 contract is missing in Kite's files" in err and "B10" in err and "re-download" in err
    assert "not checked against the bhavcopy yet" in bhav_review.review(str(tmp_path), "NIFTY", "kite", date(2026, 10, 20))[1]
    assert "nothing to review" in bhav_review.review(str(tmp_path), "NIFTY", "kite", date(2026, 10, 21))[1]
    assert "not saved" in bhav_review.review(str(tmp_path), "NIFTY", "kite", date(2026, 10, 9))[1]
    assert not os.path.exists(bhav_review.sidecar_path(str(tmp_path), "NIFTY"))
    assert status(tmp_path)["check"] == ["2026-10-19"]
    assert "B10" in bhav_review.marks(str(tmp_path), "NIFTY", "kite", fno.today_ist())["days"]["2026-10-19"]["review_why"]


def real_day(tmp_path, pe_volume=100):
    """2026-10-12 checked for real (bhav_file, check_saved): the CE has a red volume difference."""
    bhav_rows = [row("IDF", "2026-10-27", vol=2, last=100, lot=50), row("IDO", "2026-10-13", "25000.00", "CE", vol=2, last=10),
                 row("IDO", "2026-10-13", "25000.00", "PE", vol=2, last=10)]
    root = fno.fno_root(str(tmp_path), "NIFTY")
    d = os.path.join(root, "2026-10-12")
    kite_file(d, "NIFTY27OCT26", [100], vol=100)
    kite_file(d, "NIFTY2500013OCT26CE", [10], vol=1000)
    kite_file(d, "NIFTY2500013OCT26PE", [10], vol=pe_volume)
    path = tmp_path / "bhav.zip"
    path.write_bytes(zip_bytes(bhav_rows))
    assert bhav.run_bhav(date(2026, 10, 12), str(tmp_path), bhav_file=str(path), log=lambda m: None)[0] == "diffs"
    with open(os.path.join(d, "summary.csv"), "w") as f:
        f.write("x")
    return d


def recheck(d):
    """check_saved as after a re-download (summary.csv set aside: the synthetic one has no instrument list)."""
    os.rename(os.path.join(d, "summary.csv"), os.path.join(d, "summary.keep"))
    try:
        bhav.check_saved(d, date(2026, 10, 12), "NIFTY", log=lambda m: None)
    finally:
        os.rename(os.path.join(d, "summary.keep"), os.path.join(d, "summary.csv"))


def test_review_void_when_counts_change(tmp_path):
    d = real_day(tmp_path)
    assert status(tmp_path)["check"] == ["2026-10-12"]
    assert bhav_review.review(str(tmp_path), "NIFTY", "kite", date(2026, 10, 12))[1] == ""
    assert status(tmp_path)["check"] == []
    recheck(d)                                                     # same files: same counts, the review holds
    assert status(tmp_path)["check"] == []
    kite_file(d, "NIFTY2500013OCT26PE", [10], vol=1000)            # one more volume difference
    recheck(d)
    assert status(tmp_path)["check"] == ["2026-10-12"]             # red again
    m = bhav_review.marks(str(tmp_path), "NIFTY", "kite", fno.today_ist())
    assert m["days"]["2026-10-12"]["review"] == "void" and m["reviewed"] == []
    assert bhav_review.review(str(tmp_path), "NIFTY", "kite", date(2026, 10, 12))[1] == ""   # reviewed again: new counts
    assert status(tmp_path)["check"] == []


def test_unreview_keeps_history(tmp_path):
    day(tmp_path, "2026-10-19")
    assert "no review to undo" in bhav_review.unreview(str(tmp_path), "NIFTY", "kite", date(2026, 10, 19))[1]
    bhav_review.review(str(tmp_path), "NIFTY", "kite", date(2026, 10, 19), "a")
    msg, err = bhav_review.unreview(str(tmp_path), "NIFTY", "kite", date(2026, 10, 19), "b")
    assert err == "" and "undone" in msg
    assert status(tmp_path)["check"] == ["2026-10-19"]
    events = bhav_review.load_events(str(tmp_path), "NIFTY", "kite")
    assert [(e["kind"], e["note"]) for e in events] == [("review", "a"), ("unreview", "b")]
    assert events[0]["at"] == "2026-10-22 18:00:00 IST" and events[0]["fingerprint"] == RED
    assert events[0]["folder"] == "2026-10-19_exp-2026-10-19_withZeroVol_dl-2026-10-01_0905"


def test_review_lapses_on_a_new_day_folder(tmp_path):
    d = day(tmp_path, "2026-10-19")
    bhav_review.review(str(tmp_path), "NIFTY", "kite", date(2026, 10, 19))
    os.rename(d, d.replace("dl-2026-10-01_0905", "dl-2026-10-21_2010"))   # as a clear + fresh download leaves it
    assert status(tmp_path)["check"] == ["2026-10-19"]
    assert bhav_review.marks(str(tmp_path), "NIFTY", "kite", fno.today_ist())["days"]["2026-10-19"]["review"] == "superseded"


# --- bhavcopy not published ---------------------------------------------------------------------

def four_stores(tmp_path, d="2026-10-12"):
    """The day pending in all four stores of NIFTY; the last fetch a 404 in three of them (not in Dhan's without)."""
    for source in ("kite", "dhan"):
        for variant in ("with", "without"):
            day(store(tmp_path, source, variant), d, None, source=source)
            if (source, variant) != ("dhan", "without"):
                log_404(store(tmp_path, source, variant), d, source=source)


def test_no_bhav_after_404_in_every_store(tmp_path):
    four_stores(tmp_path)
    roots = bhav_review.store_roots("NIFTY", env_of(tmp_path))
    assert roots == [("kite", store(tmp_path, "kite", "with")), ("kite", store(tmp_path, "kite", "without")),
                     ("dhan", store(tmp_path, "dhan", "with")), ("dhan", store(tmp_path, "dhan", "without"))]
    lines = []
    assert bhav_review.no_bhav("NIFTY", date(2026, 10, 12), roots, fno.today_ist(), "Muhurat", log=lines.append) == 3
    assert [x.split()[0] for x in lines] == ["marked", "marked", "marked", "skipped"]
    assert "no bhavcopy fetch logged" in lines[3]
    ev = bhav_review.load_events(store(tmp_path, "kite"), "NIFTY", "kite")
    assert ev[0]["kind"] == "no_bhav" and ev[0]["note"] == "Muhurat" and "has no F&O bhavcopy for 2026-10-12" in ev[0]["fetch_line"]
    assert status(store(tmp_path, "kite")) == {"check": [], "minor": [], "pending_old": []}
    assert bhav_review.marks(store(tmp_path, "kite"), "NIFTY", "kite", fno.today_ist())["no_bhav"] == ["2026-10-12"]
    assert status(store(tmp_path, "dhan", "without"), source="dhan")["pending_old"] == ["2026-10-12"]
    # still unchecked: no check result invented
    assert bhav.list_days(store(tmp_path, "kite"))[0]["status"] == "pending"
    assert bhav.unchecked_days(store(tmp_path, "kite")) == [date(2026, 10, 12)]
    lines.clear()
    assert bhav_review.no_bhav("NIFTY", date(2026, 10, 12), roots, fno.today_ist(), log=lines.append) == 0
    assert sum("already marked" in x for x in lines) == 3
    lines.clear()
    assert bhav_review.no_bhav("NIFTY", date(2026, 10, 12), roots, fno.today_ist(), undo=True, log=lines.append) == 3
    assert status(store(tmp_path, "kite"))["pending_old"] == ["2026-10-12"]
    assert [e["kind"] for e in bhav_review.load_events(store(tmp_path, "kite"), "NIFTY", "kite")] == ["no_bhav", "undo_no_bhav"]


def test_no_bhav_refusals(tmp_path, monkeypatch):
    folder = str(tmp_path)
    roots = [("kite", folder)]
    day(folder, "2026-10-22", None)                     # today
    log_404(folder, "2026-10-22")
    day(folder, "2026-10-20", "diffs")                  # checked
    log_404(folder, "2026-10-20")
    day(folder, "2026-10-19", None)                     # nothing logged
    day(folder, "2026-10-16", None)                     # a 404, then a failed fetch (logged by run_bhav now)
    log_404(folder, "2026-10-16")

    def fail(*a, **kw):
        raise bhav.BhavError("Could not download the bhavcopy (HTTP 503).")
    monkeypatch.setattr(bhav, "fetch_bhav", fail)
    with pytest.raises(bhav.BhavError):
        bhav.run_bhav(date(2026, 10, 16), folder, log=lambda m: None)
    for d, why in [("2026-10-22", "not a past day"), ("2026-10-20", "already checked"), ("2026-10-19", "no bhavcopy fetch logged"),
                   ("2026-10-16", "not a 404: "), ("2026-10-09", "not saved")]:
        lines = []
        assert bhav_review.no_bhav("NIFTY", date.fromisoformat(d), roots, fno.today_ist(), log=lines.append) == 0, d
        assert why in lines[0], (d, lines)
    assert "2026-10-16: Could not download the bhavcopy (HTTP 503)." in bhav_review.fetch_lines(fno.fno_root(folder, "NIFTY"))["2026-10-16"]
    assert not os.path.exists(bhav_review.sidecar_path(folder, "NIFTY"))


def test_run_bhav_logs_a_404_then_marks(tmp_path, monkeypatch):
    folder = str(tmp_path)
    day(folder, "2026-10-12", None)

    def gone(run_date, *a, **kw):
        raise bhav.BhavNotAvailable(NOT_PUBLISHED.format(d=run_date))
    monkeypatch.setattr(bhav, "fetch_bhav", gone)
    assert bhav.run_bhav(date(2026, 10, 12), folder, log=lambda m: None)[0] == "not_available"
    assert bhav_review.no_bhav("NIFTY", date(2026, 10, 12), [("kite", folder)], fno.today_ist(), log=lambda m: None) == 1


def marked_day(tmp_path, d="2026-10-12"):
    folder = str(tmp_path)
    day(folder, d, None)
    log_404(folder, d)
    assert bhav_review.no_bhav("NIFTY", date.fromisoformat(d), [("kite", folder)], fno.today_ist(), log=lambda m: None) == 1
    return folder


@pytest.fixture
def cli_runs(monkeypatch):
    calls, results = [], {}

    def fake(d, folder, underlying, bhav_file=None, log=print, source="kite"):
        calls.append((underlying, source, d))
        return results.get((underlying, source, d), "ok"), "", None
    monkeypatch.setattr(nifty_bhav, "run_bhav", fake)
    return calls, results


def test_missing_skips_marked_days(tmp_path, cli_runs, capsys):
    calls, _ = cli_runs
    folder = marked_day(tmp_path)
    day(folder, "2026-10-13", None)
    assert bhav_review.pending_days(folder) == ([date(2026, 10, 13)], [date(2026, 10, 12)])
    assert nifty_bhav.main(["--missing", "--folder", folder]) == 0
    assert calls == [("NIFTY", "kite", date(2026, 10, 13))]
    assert "Left out" in capsys.readouterr().out
    calls.clear()
    day(folder, "2026-10-13", "ok")
    assert nifty_bhav.main(["--missing", "--folder", folder]) == 0 and calls == []


def test_bhav_file_on_a_marked_day(tmp_path, capsys):
    folder = marked_day(tmp_path)
    d = fno.find_day_dir(fno.fno_root(folder, "NIFTY"), "2026-10-12")
    kite_file(d, "NIFTY27OCT26", [100], vol=100)
    path = tmp_path / "bhav.zip"
    path.write_bytes(zip_bytes([row("IDF", "2026-10-27", vol=2, last=100, lot=50, oi=1000)]))
    assert nifty_bhav.main(["2026-10-12", "--folder", folder, "--bhav-file", str(path)]) == 0
    assert bhav.list_days(folder)[0]["status"] == "ok"
    m = bhav_review.marks(folder, "NIFTY", "kite", fno.today_ist())
    assert m["days"]["2026-10-12"]["no_bhav"] == "superseded" and m["no_bhav"] == []
    assert bhav_review.pending_days(folder) == ([], [])


def test_undo_no_bhav_asks_again(tmp_path, cli_runs):
    calls, _ = cli_runs
    folder = marked_day(tmp_path)
    assert bhav_review.no_bhav("NIFTY", date(2026, 10, 12), [("kite", folder)], fno.today_ist(), undo=True, log=lambda m: None) == 1
    assert nifty_bhav.main(["--missing", "--folder", folder]) == 0 and calls == [("NIFTY", "kite", date(2026, 10, 12))]


# --- check all four ------------------------------------------------------------------------------

@pytest.fixture
def four(tmp_path, monkeypatch):
    """One pending day in each of the four with-zero-volume stores, .env stubbed."""
    env = env_of(tmp_path)
    monkeypatch.setattr(nifty_bhav, "load_env", lambda: env)
    monkeypatch.setattr(dashboard, "load_env", lambda: env)
    for u, s in bhav_review.SETS:
        day(store(tmp_path, s), "2026-10-12", None, underlying=u, source=s)
    return env


def test_missing_all_order_table_and_exit_codes(tmp_path, four, cli_runs, capsys):
    calls, results = cli_runs
    assert nifty_bhav.main(["--missing-all", "--keep-zero-volume"]) == 0
    assert calls == [("NIFTY", "kite", date(2026, 10, 12)), ("NIFTY", "dhan", date(2026, 10, 12)),
                     ("SENSEX", "kite", date(2026, 10, 12)), ("SENSEX", "dhan", date(2026, 10, 12))]
    out = capsys.readouterr().out
    table = out[out.index("Underlying"):].splitlines()
    assert table[0].split() == ["Underlying", "Source", "Checked", "Still", "pending", "No-bhav", "marked", "Failed", "Red", "days"]
    assert [line.split() for line in table[1:]] == [[u, s, "1", "0", "0", "0", "0"] for u, s in bhav_review.SETS]
    results[("SENSEX", "dhan", date(2026, 10, 12))] = "not_available"
    assert nifty_bhav.main(["--missing-all", "--keep-zero-volume"]) == 4
    results[("NIFTY", "kite", date(2026, 10, 12))] = "diffs"
    assert nifty_bhav.main(["--missing-all", "--keep-zero-volume"]) == 1


def test_missing_all_goes_on_after_a_failing_set(tmp_path, four, cli_runs, capsys):
    calls, _ = cli_runs
    lock = fno.acquire_lock(store(tmp_path, "dhan"), "NIFTY", "nifty_daily.py", "dhan")
    try:
        assert nifty_bhav.main(["--missing-all", "--keep-zero-volume"]) == 1
    finally:
        lock.release()
    assert [c[:2] for c in calls] == [("NIFTY", "kite"), ("SENSEX", "kite"), ("SENSEX", "dhan")]
    out = capsys.readouterr().out
    assert [line.split()[:6] for line in out[out.index("Underlying"):].splitlines()[1:]] == [
        ["NIFTY", "kite", "1", "0", "0", "0"], ["NIFTY", "dhan", "0", "0", "0", "1"],
        ["SENSEX", "kite", "1", "0", "0", "0"], ["SENSEX", "dhan", "1", "0", "0", "0"]]
    calls.clear()
    four["FNO_RAW_DHAN"] = "relative\\folder"                     # a bad folder: both Dhan sets fail
    assert nifty_bhav.main(["--missing-all", "--keep-zero-volume"]) == 1
    assert [c[:2] for c in calls] == [("NIFTY", "kite"), ("SENSEX", "kite")]


def test_missing_all_counts_marked_failed_and_red(tmp_path, four, monkeypatch, capsys):
    marked = store(tmp_path, "kite")
    log_404(marked, "2026-10-12")
    bhav_review.no_bhav("NIFTY", date(2026, 10, 12), [("kite", marked)], fno.today_ist(), log=lambda m: None)
    day(marked, "2026-10-13", None)

    def run(d, folder, underlying, bhav_file=None, log=print, source="kite"):
        if source == "dhan" and underlying == "SENSEX":
            raise bhav.BhavError("blocked")
        day(folder, d.isoformat(), "diffs", RED if underlying == "SENSEX" else AMBER, underlying, source)
        return "diffs", "", None
    monkeypatch.setattr(nifty_bhav, "run_bhav", run)
    assert nifty_bhav.main(["--missing-all", "--keep-zero-volume"]) == 1
    out = capsys.readouterr().out
    assert [line.split() for line in out[out.index("Underlying"):].splitlines()[1:]] == [
        ["NIFTY", "kite", "1", "0", "1", "0", "0"], ["NIFTY", "dhan", "1", "0", "0", "0", "0"],
        ["SENSEX", "kite", "1", "0", "0", "0", "1"], ["SENSEX", "dhan", "0", "0", "0", "1", "0"]]


@pytest.mark.parametrize("argv", [["--missing-all", "--folder", "C:\\x"], ["--missing-all", "--underlying", "NIFTY"],
                                  ["--missing-all", "--source", "dhan"], ["--no-bhav", "2026-10-12"],
                                  ["--review", "2026-10-12", "--underlying", "NIFTY"], ["--missing", "--note", "x"],
                                  ["--no-bhav", "2026-10-12", "--underlying", "NIFTY", "--source", "kite"]])
def test_cli_refuses_bad_combinations(argv, cli_runs):
    with pytest.raises(SystemExit) as e:
        nifty_bhav.main(argv)
    assert e.value.code == 1 and cli_runs[0] == []


def test_cli_review_no_bhav_and_list(tmp_path, monkeypatch, capsys):
    env = env_of(tmp_path)
    monkeypatch.setattr(nifty_bhav, "load_env", lambda: env)
    assert nifty_bhav.main(["--reviewed-list"]) == 0 and "No review or no-bhav records." in capsys.readouterr().out
    kite = store(tmp_path, "kite")
    day(kite, "2026-10-19", counts=MISSING)
    day(kite, "2026-10-20", counts=RED)
    day(kite, "2026-10-12", None)
    log_404(kite, "2026-10-12")
    base = ["--underlying", "NIFTY", "--source", "kite", "--folder", kite]
    assert nifty_bhav.main(["--review", "2026-10-19"] + base) == 1
    assert "B10" in capsys.readouterr().err
    assert nifty_bhav.main(["--review", "2026-10-20", "--note", "ok by eye"] + base) == 0
    assert nifty_bhav.main(["--no-bhav", "2026-10-12", "--underlying", "NIFTY", "--note", "Muhurat"]) == 0
    out = capsys.readouterr().out
    assert "marked Kite" in out and out.count("skipped") == 3
    assert nifty_bhav.main(["--no-bhav", "2026-10-13", "--underlying", "NIFTY"]) == 1
    capsys.readouterr()
    assert nifty_bhav.main(["--reviewed-list"]) == 0
    lines = capsys.readouterr().out.splitlines()
    assert [line.split()[:5] for line in lines] == [["2026-10-20", "NIFTY", "kite", "review", "active"],
                                                    ["2026-10-12", "NIFTY", "kite", "no_bhav", "active"]]
    assert "note: ok by eye" in lines[0] and "note: Muhurat" in lines[1]
    assert nifty_bhav.main(["--unreview", "2026-10-20"] + base) == 0
    assert nifty_bhav.main(["--undo-no-bhav", "2026-10-12", "--underlying", "NIFTY"]) == 0
    capsys.readouterr()
    nifty_bhav.main(["--reviewed-list"])
    states = [line.split()[3:5] for line in capsys.readouterr().out.splitlines()]
    assert states == [["review", "replaced"], ["no_bhav", "replaced"], ["unreview", "undone"], ["undo_no_bhav", "undone"]]


def test_cli_review_respects_the_lock(tmp_path):
    day(tmp_path, "2026-10-20")
    lock = fno.acquire_lock(str(tmp_path), "NIFTY", "nifty_daily.py")
    try:
        assert nifty_bhav.main(["--review", "2026-10-20", "--underlying", "NIFTY", "--source", "kite", "--folder", str(tmp_path)]) == 1
    finally:
        lock.release()
    assert not os.path.exists(bhav_review.sidecar_path(str(tmp_path), "NIFTY"))


# --- dashboard ------------------------------------------------------------------------------------

@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(dashboard, "save_env", lambda updates: None)
    with JOB_LOCK:
        JOB["state"] = "idle"
    c = app.test_client()
    with c.session_transaction() as s:
        s["access_token"] = "x"
        s["user"] = {"user_id": "AB1234", "user_name": "Demo User", "email": "", "broker": "", "login_time": ""}
    return c


def test_routes_review_and_marks(client, tmp_path, monkeypatch):
    monkeypatch.setattr(dashboard, "load_env", lambda: env_of(tmp_path))
    day(tmp_path, "2026-10-19", counts=MISSING)
    day(tmp_path, "2026-10-20", counts=RED)
    q = {"folder": str(tmp_path)}
    assert client.get("/fno/bhav-status", query_string=q).get_json()["banners"]["check"] == ["2026-10-20", "2026-10-19"]
    j = client.post("/fno/bhav-review", data={**q, "date": "2026-10-19"}).get_json()
    assert "B10" in j["error"]
    j = client.post("/fno/bhav-review", data={**q, "date": "2026-10-20", "note": " fine "}).get_json()
    assert "reviewed" in j["message"]
    assert client.get("/fno/bhav-status", query_string=q).get_json()["banners"]["check"] == ["2026-10-19"]
    m = client.get("/fno/bhav-marks", query_string=q).get_json()
    assert m["reviewed"] == ["2026-10-20"] and m["days"]["2026-10-20"]["review_note"] == "fine"
    assert "undone" in client.post("/fno/bhav-review", data={**q, "date": "2026-10-20", "action": "unreview"}).get_json()["message"]
    assert client.get("/fno/bhav-status", query_string=q).get_json()["banners"]["check"] == ["2026-10-20", "2026-10-19"]
    with JOB_LOCK:
        JOB["state"] = "running"
    try:
        assert "running" in client.post("/fno/bhav-review", data={**q, "date": "2026-10-20"}).get_json()["error"]
    finally:
        with JOB_LOCK:
            JOB["state"] = "idle"


def test_routes_no_bhav_and_missing(client, tmp_path, monkeypatch):
    monkeypatch.setattr(dashboard, "load_env", lambda: env_of(tmp_path))
    folder = str(tmp_path / "card")
    day(folder, "2026-10-12", None)
    day(folder, "2026-10-13", None)
    q = {"folder": folder}
    m = client.get("/fno/bhav-marks", query_string=q).get_json()
    assert "no bhavcopy fetch logged" in m["days"]["2026-10-12"]["no_bhav_why"]
    assert "no store qualifies" in client.post("/fno/bhav-no-bhav", data={**q, "date": "2026-10-12"}).get_json()["error"]
    log_404(folder, "2026-10-12")
    assert client.get("/fno/bhav-marks", query_string=q).get_json()["days"]["2026-10-12"]["no_bhav_why"] == ""
    j = client.post("/fno/bhav-no-bhav", data={**q, "date": "2026-10-12"}).get_json()
    assert j["message"] == "2026-10-12: marked in 1 store."
    assert client.get("/fno/bhav-status", query_string=q).get_json()["banners"]["pending_old"] == ["2026-10-13"]
    assert client.get("/fno/bhav-marks", query_string=q).get_json()["no_bhav"] == ["2026-10-12"]
    calls = []

    def fake(d, folder, underlying, bhav_file=None, log=print, source="kite"):
        calls.append(d)
        return "not_available", bhav.PENDING, None
    monkeypatch.setattr(bhav, "run_bhav", fake)
    assert len(client.post("/fno/bhav-check", data={**q, "missing": "1"}).get_json()["results"]) == 1
    assert calls == [date(2026, 10, 13)]
    day(folder, "2026-10-13", "ok")
    j = client.post("/fno/bhav-check", data={**q, "missing": "1"}).get_json()
    assert j["results"] == [] and "marked" in j["message"]
    assert "unmarked" in client.post("/fno/bhav-no-bhav", data={**q, "date": "2026-10-12", "action": "undo"}).get_json()["message"]


def test_route_check_all(client, tmp_path, four, monkeypatch):
    calls = []

    def fake(d, folder, underlying, bhav_file=None, log=print, source="kite"):
        calls.append((underlying, source))
        return "not_available" if source == "dhan" else "ok", "", None
    monkeypatch.setattr(bhav, "run_bhav", fake)
    lock = fno.acquire_lock(store(tmp_path, "kite"), "SENSEX", "nifty_daily.py")
    try:
        assert "nifty_daily.py" in client.post("/fno/bhav-check-all", data={"keep_zero": "1"}).get_json()["error"]
    finally:
        lock.release()
    assert calls == []
    with JOB_LOCK:
        JOB["state"] = "running"
    try:
        assert "running" in client.post("/fno/bhav-check-all", data={"keep_zero": "1"}).get_json()["error"]
    finally:
        with JOB_LOCK:
            JOB["state"] = "idle"
    j = client.post("/fno/bhav-check-all", data={"keep_zero": "1"}).get_json()
    assert calls == bhav_review.SETS
    assert [(r["underlying"], r["source"], r["checked"], r["still_pending"]) for r in j["rows"]] == [
        ("NIFTY", "kite", 1, 0), ("NIFTY", "dhan", 0, 1), ("SENSEX", "kite", 1, 0), ("SENSEX", "dhan", 0, 1)]
    assert j["code"] == 4
    for u, s in bhav_review.SETS:                                        # every lock released
        fno.acquire_lock(store(tmp_path, s), u, "t", s).release()


def test_dashboard_page_has_the_new_controls(client, monkeypatch):
    monkeypatch.setattr(dashboard, "load_env", lambda: {})
    html = client.get("/success").get_data(as_text=True)
    for s in ("Check all pending (all four)", "/fno/bhav-check-all", "/fno/bhav-marks", "/fno/bhav-review",
              "/fno/bhav-no-bhav", "Mark reviewed", "Bhavcopy not published", "No bhavcopy (marked"):
        assert s in html, s


# --- publish and the store tools leave the sidecar and its effect alone ------------------------------

def test_publish_identical_with_and_without_records(tmp_path):
    for name in ("a", "b"):
        raw = tmp_path / name / "kite"
        make_day(str(raw), "2026-10-07")
        make_day(str(raw), "2026-10-08", check="major", counts=RED)
        make_day(str(raw), "2026-10-09", check=None)
    raw_b = str(tmp_path / "b" / "kite")
    bhav_review.review(raw_b, "NIFTY", "kite", date(2026, 10, 8), "x")
    log_404(raw_b, "2026-10-09")
    assert bhav_review.no_bhav("NIFTY", date(2026, 10, 9), [("kite", raw_b)], fno.today_ist(), log=lambda m: None) == 1
    for name in ("a", "b"):
        assert fno_publish.publish({"kite": str(tmp_path / name / "kite"), "dhan": None}, str(tmp_path / name / "bt"),
                                   ["NIFTY"], log=lambda m: None) == 0
    manifest = os.path.join("NIFTY", "publish_manifest.json")
    a, b = tree(tmp_path / "a" / "bt"), tree(tmp_path / "b" / "bt")
    assert len(a) > 3 and {k: v for k, v in a.items() if k != manifest} == {k: v for k, v in b.items() if k != manifest}

    def man(name):   # the same except the raw files' mtimes (written a moment apart)
        data = json.loads((tmp_path / name / "bt" / manifest).read_text(encoding="utf-8"))
        for entry in data["days"].values():
            for src in entry.values() if isinstance(entry, dict) else ():
                if isinstance(src, dict):
                    src.pop("newest_mtime", None)
        return data
    assert man("a") == man("b")


def test_clear_and_restore_keep_the_sidecar(tmp_path, monkeypatch):
    home = tmp_path / "fno"
    env = {"FNO_RAW_KITE": str(home / "raw" / "kite"), "FNO_RAW_DHAN": str(home / "raw" / "dhan")}
    monkeypatch.setattr(fno_clear, "weekly_expiry_weekday", lambda u="NIFTY": 3)
    save = str(home / "raw" / "kite" / "with_zero_volume")
    make_day(save, "2026-10-21", check="major", counts=RED)
    make_day(save, "2026-10-20", check=None)
    bhav_review.review(save, "NIFTY", "kite", date(2026, 10, 21), "x")
    log_404(save, "2026-10-20")
    assert bhav_review.no_bhav("NIFTY", date(2026, 10, 20), [("kite", save)], fno.today_ist(), log=lambda m: None) == 1
    path = bhav_review.sidecar_path(save, "NIFTY")
    before = open(path, "rb").read()
    lines = []
    assert fno_clear.clear("NIFTY", ["kite"], ["with"], env, backtest=False, apply=True, yes=True, log=lines.append,
                           ask=lambda q: "n", interactive=lambda: False) == 0, lines
    assert open(path, "rb").read() == before and fno.day_folders(fno.fno_root(save, "NIFTY")) == {}
    archive = next(home.rglob(fno_clear.MANIFEST)).parent        # <common folder of the .env bases>\archive\<run>
    assert archive.parent.name == "archive"
    man = json.loads((archive / fno_clear.MANIFEST).read_text(encoding="utf-8"))
    assert not any(i["src"].endswith(bhav_review.SIDECAR) for i in man["items"])
    assert bhav_review.marks(save, "NIFTY", "kite", fno.today_ist()) == {"reviewed": [], "no_bhav": [], "days": {}}
    assert fno_clear.restore(str(archive), apply=True, log=lines.append) == 0, lines
    assert open(path, "rb").read() == before
    m = bhav_review.marks(save, "NIFTY", "kite", fno.today_ist())       # the same folders back: the records apply again
    assert m["reviewed"] == ["2026-10-21"] and m["no_bhav"] == ["2026-10-20"]

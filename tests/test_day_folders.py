"""Day folder names <day>_exp-<expiry>_<withZeroVol|noZeroVol>_dl-<YYYY-MM-DD>_<HHMM>, the day-count marker
(_DAYS-*.txt, days_index.csv), readers with all name forms mixed, and nifty_daily.py --rename-folders /
--update-day-count. tmp_path only, no network, no real .env."""
import csv
import os
import shutil
from datetime import date, datetime, timezone

import pytest

import nifty_daily
from backfill import bhav, bhav_export, fno, fno_days, fno_loader, fno_rename
from backfill.job import job_begin
from backfill.storage import open_db
from test_bhav import client, day_bhav  # noqa: F401  (fixture)
from test_fno import ACCESS, RUN_DATE, W1, FakeKite, fake_kite, make_dump, set_now  # noqa: F401

IST = fno.IST
DAY = RUN_DATE.isoformat()
OFF = "2026-10-12_exp-2026-10-13_noZeroVol_dl-2026-10-12_1600"    # a run at 16:00 IST on 12 Oct, keep zero OFF
ON = "2026-10-12_exp-2026-10-13_withZeroVol_dl-2026-10-12_1600"   # the same with keep zero-volume ON


def root_of(data, underlying="NIFTY", source="kite"):
    return data / fno.fno_folder(underlying, source)


def run(data, keep_zero=False, refetch=False, day=RUN_DATE):
    assert job_begin(**fno.job_fields("NIFTY", day, str(data), 1000, keep_zero))
    if refetch:
        return fno.refetch_no_trade("key", ACCESS, day, str(data), 1000, log=lambda m: None)[0]
    return fno.run_daily("key", ACCESS, day, str(data), 1000, log=lambda m: None, keep_zero=keep_zero)


def markers(root):
    return sorted(n for n in os.listdir(root) if n.startswith("_DAYS-"))


def index_rows(root):
    with open(root / fno_days.INDEX_FILE, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


# --- helpers ------------------------------------------------------------------------------------

def test_day_folder_name_and_parse():
    created = datetime(2026, 10, 13, 23, 19, 42, tzinfo=IST)
    assert fno.day_folder_name(RUN_DATE, W1, True, created) == "2026-10-12_exp-2026-10-13_withZeroVol_dl-2026-10-13_2319"
    assert fno.day_folder_name(RUN_DATE, W1, False, created) == "2026-10-12_exp-2026-10-13_noZeroVol_dl-2026-10-13_2319"
    assert fno.parse_day_folder("2026-10-12_exp-2026-10-13_withZeroVol_dl-2026-10-13_2319") == \
        (RUN_DATE, W1, True, datetime(2026, 10, 13, 23, 19, tzinfo=IST))
    assert fno.parse_day_folder("2026-10-12_exp-2026-10-13_noZeroVol_dl-2026-10-14_0905")[2] is False
    # expiry day: exp is the day itself; SENSEX uses its own (Thursday) expiry
    assert fno.day_folder_name(W1, W1, True, datetime(2026, 10, 13, 18, 12, tzinfo=IST)) == \
        "2026-10-13_exp-2026-10-13_withZeroVol_dl-2026-10-13_1812"
    assert fno.day_folder_name(date(2026, 10, 15), date(2026, 10, 15), True, datetime(2026, 10, 15, 18, 12, tzinfo=IST)) == \
        "2026-10-15_exp-2026-10-15_withZeroVol_dl-2026-10-15_1812"
    # a UTC time is written as IST; no colons in the name
    utc = datetime(2026, 10, 13, 17, 49, tzinfo=timezone.utc)
    assert fno.day_folder_name(RUN_DATE, W1, False, utc).endswith("_dl-2026-10-13_2319")
    assert fno.parse_day_folder("2026-10-12") == (RUN_DATE, None, None, None)        # old style: the day only
    for junk in ("bhavcopy.log", "2026-10-12_copy", "2026-13-01", "2026-10-12_exp-2026-10-13",
                 "2026-10-12_exp-2026-10-13_dl-2026-10-13_2319", "2026-10-12_exp-2026-10-13_maybeZeroVol_dl-2026-10-13_2319",
                 "x2026-10-12", "_DAYS-5_2026-10-07_to_2026-10-13.txt"):
        assert fno.parse_day_folder(junk) is None


def test_find_day_dir(tmp_path):
    root = tmp_path / "NIFTY_FnO_1min"
    assert fno.find_day_dir(str(root), RUN_DATE) is None and fno.day_folders(str(root)) == {}   # no root at all
    (root / "2026-10-09").mkdir(parents=True)
    (root / OFF).mkdir()
    (root / "2026-10-13_exp-2026-10-13_withZeroVol_dl-2026-10-13_1600.txt").write_text("not a folder")
    assert fno.find_day_dir(str(root), date(2026, 10, 9)) == str(root / "2026-10-09")    # old name
    assert fno.find_day_dir(str(root), RUN_DATE) == str(root / OFF)                      # new name
    assert fno.find_day_dir(str(root), date(2026, 10, 13)) is None                       # files don't count
    assert fno.day_folders(str(root)) == {date(2026, 10, 9): "2026-10-09", RUN_DATE: OFF}
    (root / ON).mkdir()
    with pytest.raises(fno.DayFolderError) as e:
        fno.find_day_dir(str(root), date(2026, 10, 9))                                   # any lookup refuses
    assert OFF in str(e.value) and ON in str(e.value)


def test_nearest_option_expiry():
    dump = make_dump()
    assert fno.nearest_option_expiry(dump, "NIFTY", RUN_DATE) == W1
    assert fno.nearest_option_expiry(dump, "NIFTY", W1) == W1                            # expiry day: itself
    assert fno.nearest_option_expiry(dump, "NIFTY", date(2026, 10, 14)) == date(2026, 10, 20)
    assert fno.nearest_option_expiry([i for i in dump if i["instrument_type"] == "FUT"], "NIFTY", RUN_DATE) is None


def test_mark_with_zero_volume(tmp_path):
    (tmp_path / OFF / "options").mkdir(parents=True)
    (tmp_path / OFF / "options" / "a.csv").write_text("x")
    assert fno.mark_with_zero_volume(str(tmp_path / OFF)) == str(tmp_path / ON)
    assert (tmp_path / ON / "options" / "a.csv").read_text() == "x" and not (tmp_path / OFF).exists()
    assert fno.mark_with_zero_volume(str(tmp_path / ON)) == str(tmp_path / ON)            # already with
    (tmp_path / "2026-10-09").mkdir()
    assert fno.mark_with_zero_volume(str(tmp_path / "2026-10-09")) == str(tmp_path / "2026-10-09")   # old: unchanged


# --- runs ----------------------------------------------------------------------------------------

def test_run_without_option_expiry_creates_nothing(tmp_path, fake_kite, monkeypatch):
    monkeypatch.setattr(FakeKite, "instruments", lambda self, e: [i for i in make_dump() if i["instrument_type"] == "FUT"])
    assert run(tmp_path) == "error"
    assert "No NIFTY option expiry on or after 2026-10-12" in fno.JOB["message"]
    assert fno.day_folders(str(root_of(tmp_path))) == {} and fake_kite.calls == []


@pytest.mark.parametrize("keep_zero, name", [(False, OFF), (True, ON)])
def test_fresh_run_gets_the_new_name(tmp_path, fake_kite, keep_zero, name):
    assert run(tmp_path, keep_zero).startswith("finished")
    root = root_of(tmp_path)
    assert fno.day_folders(str(root)) == {RUN_DATE: name}
    assert fno.JOB["out_dirs"] == [os.path.join("NIFTY_FnO_1min", name)]
    # the day count
    assert markers(root) == ["_DAYS-1_2026-10-12_to_2026-10-12.txt"]
    rows = index_rows(root)
    assert [(r["Day"], r["Folder"], r["Expiry"], r["Zero_volume"], r["Downloaded"]) for r in rows] == \
        [(DAY, name, "2026-10-13", "With" if keep_zero else "No", "2026-10-12 16:00")]
    assert rows[0]["Futures_files"] == "2" and int(rows[0]["Option_files"]) >= 3
    text = (root / markers(root)[0]).read_text(encoding="utf-8").splitlines()
    assert text[0] == (f"1 day (withZeroVol {int(keep_zero)}, noZeroVol {int(not keep_zero)}), 2026-10-12 to 2026-10-12, "
                       f"updated 2026-10-12 16:00 IST")
    assert any(name in line for line in text[2:])


def test_resume_and_redownload_reuse_the_folder(tmp_path, fake_kite, monkeypatch):
    assert run(tmp_path) == "finished_failures"
    root = root_of(tmp_path)
    (root / "notes.txt").write_text("mine")
    # the next evening: the same folder, never renamed; the run's start and setting are in run.log
    set_now(monkeypatch, date(2026, 10, 13), (23, 19))
    assert run(tmp_path) == "finished_failures"
    assert fno.day_folders(str(root)) == {RUN_DATE: OFF}
    log = (root / OFF / "run.log").read_text(encoding="utf-8")
    assert "started 2026-10-12 16:00:00 IST" in log and "started 2026-10-13 23:19:00 IST" in log
    assert log.count("Keep zero-volume contracts: OFF") == 2
    os.remove(root / OFF / "options" / "NIFTY2510013OCT26CE.csv")
    saved, _ = fno.redownload_contract("key", ACCESS, RUN_DATE, str(tmp_path), "NIFTY2510013OCT26CE", speed=1000,
                                       log=lambda m: None)
    assert saved and (root / OFF / "options" / "NIFTY2510013OCT26CE.csv").exists()
    assert fno.day_folders(str(root)) == {RUN_DATE: OFF}
    assert "Re-download of NIFTY2510013OCT26CE for 2026-10-12 (Kite), started 2026-10-13 23:19:00 IST, " \
           "keep zero-volume contracts OFF" in (root / OFF / "run.log").read_text(encoding="utf-8")
    assert (root / "notes.txt").read_text() == "mine"
    # progress stays keyed by date
    db = open_db(str(tmp_path))
    assert db.execute("SELECT COUNT(*) FROM chunks WHERE run = 'fno:NIFTY:NFO:2026-10-12'").fetchone()[0] > 0
    db.close()


def test_off_day_then_on_run_becomes_with_zero_volume(tmp_path, fake_kite, monkeypatch):
    assert run(tmp_path) == "finished_failures"
    root = root_of(tmp_path)
    assert not (root / OFF / "options" / "NIFTY2515013OCT26CE.csv").exists()            # zero-volume: no file
    set_now(monkeypatch, date(2026, 10, 13), (9, 5))
    assert run(tmp_path, keep_zero=True) == "finished_failures"
    assert fno.day_folders(str(root)) == {RUN_DATE: ON}                                  # dl is kept
    assert (root / ON / "options" / "NIFTY2515013OCT26CE.csv").exists()
    assert (root / ON / "futures" / "NIFTY27OCT26.csv").exists() and (root / ON / "summary.csv").exists()
    log = (root / ON / "run.log").read_text(encoding="utf-8")
    assert "Keep zero-volume contracts: OFF" in log and "Keep zero-volume contracts: ON" in log
    assert fno.JOB["out_dirs"] == [os.path.join("NIFTY_FnO_1min", ON)]
    assert markers(root) == ["_DAYS-1_2026-10-12_to_2026-10-12.txt"] and index_rows(root)[0]["Zero_volume"] == "With"
    # an OFF run on a withZeroVol day never renames it back
    assert run(tmp_path) == "finished_failures"
    assert fno.day_folders(str(root)) == {RUN_DATE: ON}


def test_refetch_renames_to_with_zero_volume(tmp_path, fake_kite):
    assert run(tmp_path) == "finished_failures"
    assert run(tmp_path, refetch=True) == "finished_failures"
    assert fno.day_folders(str(root_of(tmp_path))) == {RUN_DATE: ON}
    assert (root_of(tmp_path) / ON / "options" / "NIFTY2515013OCT26CE.csv").exists()


def test_redownload_with_keep_zero_renames(tmp_path, fake_kite):
    assert run(tmp_path) == "finished_failures"
    saved, _ = fno.redownload_contract("key", ACCESS, RUN_DATE, str(tmp_path), "NIFTY2515013OCT26CE", speed=1000,
                                       log=lambda m: None, keep_zero=True)
    root = root_of(tmp_path)
    assert saved and fno.day_folders(str(root)) == {RUN_DATE: ON}
    assert (root / ON / "options" / "NIFTY2515013OCT26CE.csv").exists()
    assert index_rows(root)[0]["Folder"] == ON


def test_run_reuses_an_old_style_folder(tmp_path, fake_kite):
    (root_of(tmp_path) / DAY).mkdir(parents=True)
    assert run(tmp_path, keep_zero=True) == "finished_failures"
    assert fno.day_folders(str(root_of(tmp_path))) == {RUN_DATE: DAY}                   # kept as it is
    assert (root_of(tmp_path) / DAY / "summary.csv").exists()


def test_run_refuses_duplicate_folders(tmp_path, fake_kite):
    (root_of(tmp_path) / DAY).mkdir(parents=True)
    (root_of(tmp_path) / OFF).mkdir()
    assert run(tmp_path) == "error"
    assert "More than one folder" in fno.JOB["message"] and OFF in fno.JOB["message"]
    assert fake_kite.calls == [] and os.listdir(root_of(tmp_path) / OFF) == []


def test_holiday_run_leaves_no_folder_and_counts(tmp_path, fake_kite):
    root = root_of(tmp_path)
    (root / "2026-10-09").mkdir(parents=True)
    fake_kite.holiday = True
    assert run(tmp_path) == "holiday"
    assert fno.day_folders(str(root)) == {date(2026, 10, 9): "2026-10-09"}
    assert markers(root) == ["_DAYS-1_2026-10-09_to_2026-10-09.txt"]
    assert "old-style 1" in (root / markers(root)[0]).read_text(encoding="utf-8")


def test_dhan_and_kite_roots_get_their_own_names(tmp_path, fake_kite):
    run(tmp_path)
    (root_of(tmp_path, source="dhan") / "2026-10-12_exp-2026-10-13_withZeroVol_dl-2026-10-13_0905").mkdir(parents=True)
    assert fno.day_dir(str(tmp_path), "NIFTY", RUN_DATE) == str(root_of(tmp_path) / OFF)
    assert fno.day_dir(str(tmp_path), "NIFTY", RUN_DATE, "dhan").endswith("withZeroVol_dl-2026-10-13_0905")
    assert fno.day_dir(str(tmp_path), "SENSEX", RUN_DATE) is None


def test_run_bhav_needs_the_day_folder(tmp_path):
    with pytest.raises(bhav.BhavError, match="not been downloaded"):
        bhav.run_bhav(RUN_DATE, str(tmp_path), bhav_file=day_bhav(tmp_path / "b.zip"), log=lambda m: None)
    assert not root_of(tmp_path).exists() or fno.day_folders(str(root_of(tmp_path))) == {}


# --- day count -------------------------------------------------------------------------------------

def test_write_day_count_replaces_only_markers(tmp_path):
    root = tmp_path / "NIFTY_FnO_1min"
    root.mkdir()
    assert os.path.basename(fno_days.write_day_count(str(root))) == "_DAYS-0.txt"
    assert index_rows(root) == [] and "0 days" in (root / "_DAYS-0.txt").read_text(encoding="utf-8")
    keep = ["_DAYS-notes.txt", "_DAYS-3.csv", "DAYS-2.txt", "expiries_seen.csv"]
    for n in keep:
        (root / n).write_text("x")
    (root / "_DAYS-9_2026-01-01_to_2026-01-09.txt").write_text("old")
    (root / "2026-10-07").mkdir()
    (root / ON / "futures").mkdir(parents=True)
    (root / ON / "futures" / "NIFTY27OCT26.csv").write_text("x")
    (root / "2026-10-13_exp-2026-10-13_noZeroVol_dl-2026-10-13_2319").mkdir()
    fno_days.write_day_count(str(root))
    assert markers(root) == ["_DAYS-3.csv", "_DAYS-3_2026-10-07_to_2026-10-13.txt", "_DAYS-notes.txt"]
    assert all((root / n).exists() for n in keep)
    first = (root / "_DAYS-3_2026-10-07_to_2026-10-13.txt").read_text(encoding="utf-8").splitlines()[0]
    assert first.startswith("3 days (withZeroVol 1, noZeroVol 1, old-style 1), 2026-10-07 to 2026-10-13, updated ")
    assert [(r["Day"], r["Zero_volume"], r["Futures_files"]) for r in index_rows(root)] == \
        [("2026-10-07", "", "0"), (DAY, "With", "1"), ("2026-10-13", "No", "0")]
    assert fno_days.day_summary(str(root)) == {"count": 3, "first": "2026-10-07", "last": "2026-10-13",
                                               "with": 1, "no": 1, "old": 1}


def test_update_day_count_cli(tmp_path, capsys):
    (root_of(tmp_path) / "2026-10-07").mkdir(parents=True)
    (root_of(tmp_path, "SENSEX", "dhan") / "2026-10-15_exp-2026-10-15_withZeroVol_dl-2026-10-15_1812").mkdir(parents=True)
    assert nifty_daily.main(["--update-day-count", "--folder", str(tmp_path)]) == 0
    assert markers(root_of(tmp_path)) == ["_DAYS-1_2026-10-07_to_2026-10-07.txt"]
    assert markers(root_of(tmp_path, "SENSEX", "dhan")) == ["_DAYS-1_2026-10-15_to_2026-10-15.txt"]
    lock = fno.acquire_lock(str(tmp_path), "NIFTY", "test download")
    try:
        assert nifty_daily.main(["--update-day-count", "--folder", str(tmp_path), "--underlying", "NIFTY"]) == 1
    finally:
        lock.release()
    assert "Not run:" in capsys.readouterr().out


# --- readers with all name forms mixed ---------------------------------------------------------------

@pytest.fixture
def mixed(tmp_path, fake_kite):
    """12 Oct downloaded (OFF) and checked under the new name; a copy of it as old-style 2026-10-09 and as
    withZeroVol 2026-10-08."""
    data = tmp_path / "data"
    run(data)
    bhav.run_bhav(RUN_DATE, str(data), bhav_file=day_bhav(tmp_path / "b.zip"), log=lambda m: None)
    root = root_of(data)
    shutil.copytree(root / OFF, root / "2026-10-09")
    shutil.copytree(root / OFF, root / "2026-10-08_exp-2026-10-13_withZeroVol_dl-2026-10-08_2000")
    return data, root


def test_bhav_views_read_all_names(mixed):
    data, root = mixed
    days = bhav.list_days(str(data))
    assert [(d["date"], d["expiry"], d["zero_volume"], d["downloaded"], d["status"]) for d in days] == \
        [(DAY, "2026-10-13", "no", "2026-10-12 16:00", "diffs"), ("2026-10-09", "", "", "", "diffs"),
         ("2026-10-08", "2026-10-13", "with", "2026-10-08 20:00", "diffs")]
    os.remove(root / "2026-10-09" / "bhavcopy" / "bhav_check.json")
    assert bhav.unchecked_days(str(data)) == [date(2026, 10, 9)]
    assert bhav.read_check(str(data), "NIFTY", RUN_DATE)["status"] == "diffs"
    assert bhav.diff_rows(str(data), "NIFTY", RUN_DATE)[0]
    assert bhav.bhav_dir(str(data), "NIFTY", date(2026, 10, 1)) is None


def test_missing_dates_with_new_names(mixed):
    data, _ = mixed
    db = open_db(str(data))
    db.execute("UPDATE chunks SET status = 'DONE'")                                      # every contract done
    db.commit()
    db.close()
    window = fno.Window(date(2026, 10, 10), RUN_DATE, W1, date(2026, 10, 9), False)
    assert fno.missing_dates(str(data), "NIFTY", window) == [date(2026, 10, 10), date(2026, 10, 11)]


def test_exports_read_all_names(mixed):
    data, root = mixed
    name, body, _ = bhav_export.export(str(data), "NIFTY", "days")
    assert [line.split(",")[0] for line in body.decode("utf-8").splitlines()[1:]] == [DAY, "2026-10-09", "2026-10-08"]
    for d in (RUN_DATE, date(2026, 10, 9), date(2026, 10, 8)):
        assert bhav_export.export(str(data), "NIFTY", "xlsx", d)[1][:2] == b"PK"


def test_loader_reads_all_names(mixed):
    data, root = mixed
    assert [os.path.basename(p) for p in fno_loader.day_dirs(str(data))] == \
        ["2026-10-08_exp-2026-10-13_withZeroVol_dl-2026-10-08_2000", "2026-10-09", OFF]
    assert len(fno_loader.load_day(str(data), DAY)) == len(fno_loader.load_day(str(data), "2026-10-09")) > 0
    assert not fno_loader.load_eod(str(data), DAY).empty
    os.remove(root / OFF / "futures" / "NIFTY27OCT26.csv")                                # only the copies left
    assert len(fno_loader.load_contract(str(data), "NIFTY27OCT26")) == 5                   # same bars, deduplicated


def test_dashboard_lists_all_names_and_reports_duplicates(mixed, client):
    data, root = mixed
    j = client.get("/fno/bhav-status", query_string={"folder": str(data)}).get_json()
    assert [(d["date"], d["zero_volume"]) for d in j["days"]] == [(DAY, "no"), ("2026-10-09", ""), ("2026-10-08", "with")]
    assert markers(root) == ["_DAYS-1_2026-10-12_to_2026-10-12.txt"]                  # the list never rewrites it
    (root / DAY).mkdir()
    j = client.get("/fno/bhav-status", query_string={"folder": str(data)}).get_json()
    assert "More than one folder" in j["error"] and OFF in j["error"]


def test_window_route_shows_the_day_count(tmp_path, monkeypatch, client):
    import dashboard
    w = fno.Window(date(2026, 10, 10), RUN_DATE, W1, date(2026, 10, 9), False)
    monkeypatch.setattr(dashboard, "fno_window", lambda folder, underlying: (w, ""))
    root = root_of(tmp_path)
    (root / "2026-10-07").mkdir(parents=True)
    (root / ON).mkdir()
    j = client.get("/download/fno-window", query_string={"folder": str(tmp_path)}).get_json()
    assert j["days"] == {"count": 2, "first": "2026-10-07", "last": DAY, "with": 1, "no": 0, "old": 1}
    assert not markers(root) and not (root / fno_days.INDEX_FILE).exists()                # read only


# --- migration ---------------------------------------------------------------------------------------

SUMMARY = ("Expiry,Bucket,Listed_strikes,Keep_zero_volume\n2026-10-07,Current week,10,{z}\n"
           "2026-10-20,Next week,10,{z}\n2026-10-27,Current month + Far,10,{z}\n")


def old_days(data):
    """NIFTY Kite:
    07 Oct: summary.csv (exp, Keep_zero_volume ON) and run.log -> withZeroVol from summary.csv
    08 Oct: run.log with an OFF and an ON run, no summary -> exp from expiries_seen.csv, withZeroVol from run.log
    09 Oct: only zero_volume_contracts.csv with a kept row -> withZeroVol from it, dl from the folder
    11 Oct: summary.csv says OFF -> noZeroVol
    12 Oct: run.log with OFF runs only -> noZeroVol from run.log
    13 Oct: nothing tells the zero-volume setting -> skipped
    10 Oct: already renamed. SENSEX Dhan 15 Oct: no expiry anywhere -> skipped."""
    root = root_of(data)
    for d in ("2026-10-07", "2026-10-08", "2026-10-09", "2026-10-11", "2026-10-12", "2026-10-13"):
        (root / d / "futures").mkdir(parents=True)
        (root / d / "futures" / "NIFTY27OCT26.csv").write_text("x")
    (root / "2026-10-07" / "summary.csv").write_text(SUMMARY.format(z="ON"))
    (root / "2026-10-07" / "run.log").write_text(
        "2026-10-08 09:05:01 === NIFTY F&O 1-minute run for 2026-10-07 (NFO), started 2026-10-08 09:05:01 IST, Kite\n"
        "2026-10-08 09:05:01 Keep zero-volume contracts: ON\n"
        "2026-10-09 20:00:00 === NIFTY F&O 1-minute run for 2026-10-07 (NFO), started 2026-10-09 20:00:00 IST, Kite\n")
    (root / "2026-10-08" / "run.log").write_text(
        "x started 2026-10-08 23:19:59 IST\nx Keep zero-volume contracts: OFF\n"
        "x started 2026-10-09 10:00:00 IST\nx Keep zero-volume contracts: ON\n")
    (root / "2026-10-09" / fno.ZERO_VOLUME_FILE).write_text("Contract,Candles,Last_close,Last_OI,Kept\nA,2,1,1,N\nB,2,1,1,Y\n")
    (root / "2026-10-11" / "summary.csv").write_text(SUMMARY.format(z="OFF").replace("2026-10-07,Current", "2026-10-13,Current"))
    (root / "2026-10-12" / "run.log").write_text(
        "x started 2026-10-13 23:07:00 IST\nx Keep zero-volume contracts: OFF\nx Keep zero-volume contracts: OFF\n")
    (root / "2026-10-10_exp-2026-10-13_noZeroVol_dl-2026-10-10_1600").mkdir()
    (root / fno.EXPIRIES_FILE).write_text("expiry,first_seen\n2026-10-07,2026-10-01\n2026-10-13,2026-10-01\n")
    (root_of(data, "SENSEX", "dhan") / "2026-10-15").mkdir(parents=True)


def cli(data, *extra):
    return nifty_daily.main(["--rename-folders", "--folder", str(data), *extra])


def test_rename_dry_run_changes_nothing(tmp_path, capsys):
    old_days(tmp_path)
    before = sorted(str(p) for p in tmp_path.rglob("*"))
    assert cli(tmp_path) == 0
    out = capsys.readouterr().out
    assert sorted(str(p) for p in tmp_path.rglob("*") if p.name != ".lock") == before        # no marker either
    assert ("2026-10-07 -> 2026-10-07_exp-2026-10-07_withZeroVol_dl-2026-10-08_0905  "
            "(exp: summary.csv; zero volume: summary.csv; dl: run.log)") in out
    assert ("2026-10-08 -> 2026-10-08_exp-2026-10-13_withZeroVol_dl-2026-10-08_2319  "
            "(exp: expiries_seen.csv; zero volume: run.log; dl: run.log)") in out
    assert ("2026-10-09 -> 2026-10-09_exp-2026-10-13_withZeroVol_dl-" in out
            and "(exp: expiries_seen.csv; zero volume: zero_volume_contracts.csv; dl: folder created time)" in out)
    assert "2026-10-11 -> 2026-10-11_exp-2026-10-13_noZeroVol_dl-" in out and "zero volume: summary.csv; dl: folder" in out
    assert ("2026-10-12 -> 2026-10-12_exp-2026-10-13_noZeroVol_dl-2026-10-13_2307  "
            "(exp: expiries_seen.csv; zero volume: run.log; dl: run.log)") in out
    assert "SKIPPED 2026-10-13: zero-volume setting unknown" in out
    assert "SKIPPED 2026-10-15: no expiry" in out and "already named: 1" in out
    assert "5 to rename, 2 skipped." in out and out.rstrip().endswith("Dry run: nothing renamed. Run again with --apply to rename.")


def test_rename_apply(tmp_path, capsys):
    old_days(tmp_path)
    created = datetime.fromtimestamp(os.stat(root_of(tmp_path) / "2026-10-09").st_ctime, IST)
    assert cli(tmp_path, "--apply") == 0
    root = root_of(tmp_path)
    names = sorted(fno.day_folders(str(root)).values())
    assert names[:3] == ["2026-10-07_exp-2026-10-07_withZeroVol_dl-2026-10-08_0905",
                         "2026-10-08_exp-2026-10-13_withZeroVol_dl-2026-10-08_2319",
                         f"2026-10-09_exp-2026-10-13_withZeroVol_dl-{created:%Y-%m-%d_%H%M}"]
    assert names[5:] == ["2026-10-12_exp-2026-10-13_noZeroVol_dl-2026-10-13_2307", "2026-10-13"]   # skipped one stays
    assert (root / names[0] / "futures" / "NIFTY27OCT26.csv").read_text() == "x"            # contents untouched
    assert (root_of(tmp_path, "SENSEX", "dhan") / "2026-10-15").is_dir()
    out = capsys.readouterr().out
    assert "Renamed 5 folders." in out and "Day count: _DAYS-7_2026-10-07_to_2026-10-13.txt" in out
    assert markers(root) == ["_DAYS-7_2026-10-07_to_2026-10-13.txt"] and len(index_rows(root)) == 7
    assert not any(n.startswith("_DAYS-") for n in os.listdir(root_of(tmp_path, "SENSEX", "dhan")))   # not touched
    assert cli(tmp_path, "--apply") == 0 and "0 to rename" in capsys.readouterr().out       # nothing left to do


def test_rename_one_root_only(tmp_path, capsys):
    old_days(tmp_path)
    assert cli(tmp_path, "--underlying", "SENSEX", "--source", "dhan", "--apply") == 0
    out = capsys.readouterr().out
    assert "NIFTY_FnO_1min" not in out and (root_of(tmp_path) / "2026-10-07").is_dir()


def test_rename_refuses_while_locked(tmp_path, capsys):
    old_days(tmp_path)
    lock = fno.acquire_lock(str(tmp_path), "NIFTY", "test download")
    try:
        assert cli(tmp_path) == 1                                                            # dry run too
        assert cli(tmp_path, "--apply") == 1
    finally:
        lock.release()
    assert "Not run:" in capsys.readouterr().out
    assert (root_of(tmp_path) / "2026-10-07").is_dir()
    fno.acquire_lock(str(tmp_path), "SENSEX", "test", "dhan").release()                    # other locks released


def test_rename_stops_on_first_error(tmp_path, capsys, monkeypatch):
    old_days(tmp_path)
    real, calls = os.rename, []

    def flaky(a, b):
        calls.append(a)
        if len(calls) == 2:
            raise PermissionError("in use")
        real(a, b)
    monkeypatch.setattr(fno_rename.os, "rename", flaky)
    assert cli(tmp_path, "--apply") == 1
    out = capsys.readouterr().out
    assert "Stopped: could not rename" in out and "2026-10-08" in out and "in use" in out
    assert "Renamed before the error (1):" in out and "2026-10-07_exp-2026-10-07_withZeroVol_dl-2026-10-08_0905" in out
    assert len(calls) == 2 and (root_of(tmp_path) / "2026-10-08").is_dir() and not markers(root_of(tmp_path))


def test_rename_refuses_duplicates(tmp_path, capsys):
    old_days(tmp_path)
    (root_of(tmp_path) / "2026-10-07_exp-2026-10-07_withZeroVol_dl-2026-10-08_0905").mkdir()
    assert cli(tmp_path, "--apply") == 1
    assert "More than one folder" in capsys.readouterr().out
    assert (root_of(tmp_path) / "2026-10-07").is_dir() and (root_of(tmp_path) / "2026-10-08").is_dir()   # nothing renamed


def test_rename_arguments(tmp_path, capsys):
    for argv in (["2026-10-12", "--underlying", "all"], ["2026-10-12", "--source", "all"], ["2026-10-12", "--apply"],
                 ["--update-day-count", "--apply"]):
        with pytest.raises(SystemExit) as e:
            nifty_daily.main(argv + ["--folder", str(tmp_path)])
        assert e.value.code == 1
    assert cli(tmp_path / "nope") == 1 and not (tmp_path / "nope").exists()               # never creates the folder
    assert cli(tmp_path) == 0 and "No F&O folders" in capsys.readouterr().out

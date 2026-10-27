"""backfill/equity/special_sessions.py (the special-sessions calendar) and cm_bhav.py --special [--apply] (the weekend
sessions' bhavcopy). Synthetic calendars and a fake NSE client on the cm_bhav fixtures, re-dated to weekend days.
No test touches the network or .env."""
import io
import os
import zipfile
from datetime import date, datetime

import pytest
import requests

import cm_bhav as cli
import config
from backfill import cm_bhav
from backfill.cm_bhav import IST
from backfill.equity import special_sessions as ss
from backfill.fno import RunLock
from backfill.nse_ref import fetch
from backfill.nse_ref.fetch import HOME, Throttle

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "cm_bhav")
OLD_FIX, NEW_FIX = "cm03MAR2021bhav.csv.zip", "BhavCopy_NSE_CM_0_0_0_20261014_F_0000.csv.zip"
NOW = datetime(2026, 10, 27, 20, 0, tzinfo=IST)   # a Tuesday, after 19:00
SAT_OLD = date(2021, 3, 6)      # a Saturday before the UDiFF switch: old format
SUN_NEW = date(2026, 10, 11)    # a Sunday after it: UDiFF
SAT_NONE = date(2022, 1, 15)    # a Saturday NSE has no file for (404)
SUN_NEXT = date(2026, 11, 8)    # after NOW
HEADER = ",".join(ss.COLUMNS)


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def refuse(*a, **k):
        raise AssertionError("a test tried to reach the network")
    monkeypatch.setattr(requests.Session, "request", refuse)
    monkeypatch.setattr(fetch, "launch_browser", refuse)
    monkeypatch.setattr(config, "load_env", lambda: {})


# --- helpers -----------------------------------------------------------------------------------

def line(day, kind="MUHURAT", start="18:15", end="19:15", in_bhav="no", evidence="test", note=""):
    return f"{day},{ss.WEEKDAYS[day.weekday()]},{kind},{start},{end},{in_bhav},{evidence},{note}"


def write_sessions(folder, lines, header=HEADER):
    path = ss.data_copy(str(folder))
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write("\n".join([header] + list(lines)) + "\n")
    return path


def calendar(folder, *days):
    """A data copy with these weekend sessions plus a weekday Muhurat row (never asked by --special)."""
    return write_sessions(folder, [line(date(2025, 10, 21), start="13:45", end="14:45", in_bhav="yes")]
                          + [line(d, "BUDGET_WEEKEND" if d.weekday() == 5 else "MUHURAT") for d in days])


def fixture_text(name):
    with zipfile.ZipFile(os.path.join(FIX, name)) as z:
        return z.read(z.namelist()[0]).decode()


def file_for(day, fmt):
    if fmt == "old":
        text = fixture_text(OLD_FIX).replace("03-MAR-2021", f"{day:%d}-{cm_bhav.MONTHS[day.month - 1]}-{day:%Y}")
    else:
        text = fixture_text(NEW_FIX).replace("2026-10-14", day.isoformat())
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr(zipfile.ZipInfo(cm_bhav.file_name(day, fmt)[:-4], date_time=(1980, 1, 1, 0, 0, 0)), text,
                   compress_type=zipfile.ZIP_DEFLATED)
    return buf.getvalue()


class Resp:
    def __init__(self, status, content=b""):
        self.status_code, self.content = status, content

    @property
    def ok(self):
        return 200 <= self.status_code < 300


class FakeSession:
    """URL -> Resp; anything else 404. The home page answers 403 like NSE's."""

    def __init__(self, answers=None):
        self.answers, self.calls, self.headers = dict(answers or {}), [], {}

    def get(self, url, timeout=None):
        if url == HOME:
            return Resp(403)
        self.calls.append(url)
        return self.answers.get(url, Resp(404))


class NoBrowser:
    def get(self, url):
        raise AssertionError("no browser in these tests")

    def close(self):
        pass


def client(answers=None):
    s = FakeSession(answers)
    c = cm_bhav.CmClient(session=s, throttle=Throttle(gap=0, sleep=lambda x: None), pause=lambda x: None,
                         browser=NoBrowser(), use_browser=False, log=lambda m: None)
    return c, s


def serve(*days):
    return {cm_bhav.url_for(d, cm_bhav.formats_for(d)[0]): Resp(200, file_for(d, cm_bhav.formats_for(d)[0]))
            for d in days}


def store(tmp_path, *parts):
    return os.path.join(tmp_path, "NSE_CM_bhav", *parts)


def tree(root):
    """{relative path: bytes} of every file under root."""
    out = {}
    for base, _, files in os.walk(root):
        for name in files:
            p = os.path.join(base, name)
            with open(p, "rb") as f:
                out[os.path.relpath(p, root)] = f.read()
    return out


def weekday_store(tmp_path):
    """A store with ordinary weekdays in 2020, 2021 and 2026 (as cm_bhav.py <date> leaves it)."""
    days = [date(2020, 3, 3), date(2021, 3, 3), date(2026, 10, 14)]
    c, _ = client(serve(*days))
    cm_bhav.run(str(tmp_path), days, client=c, now=NOW, log=lambda m: None)


def by_day(rows):
    return {r["day"]: r for r in rows}


# --- the calendar --------------------------------------------------------------------------------

def test_repo_copy_is_used_without_a_data_copy_and_holds_the_19_dates_plus_the_next(tmp_path):
    assert ss.path_of(str(tmp_path)) == ss.REPO_COPY
    df = ss.load_special_sessions(str(tmp_path))
    assert len(df) == 20 and list(df.columns) == ss.COLUMNS and list(df["date"]) == sorted(df["date"])
    weekend = ss.weekend_dates(str(tmp_path))
    assert len(weekend) == 12 and SUN_NEXT in weekend   # the 11 sessions found, plus 2026-11-08
    assert set(df.loc[df["in_bhavcopy"] == "no", "date"]) == set(weekend)
    assert df.loc[df["date"] == SUN_NEXT, ["start", "end"]].values.tolist() == [["", ""]]


@pytest.mark.parametrize("day, kind", [(date(2015, 11, 11), "MUHURAT"), (date(2020, 2, 1), "BUDGET_WEEKEND"),
                                       (date(2024, 1, 20), "SPECIAL_SATURDAY"), (date(2017, 7, 10), "OUTAGE"),
                                       (date(2025, 10, 21), "MUHURAT"), (date(2026, 10, 27), None)])
def test_session_kind_for_each_kind_and_an_ordinary_day(tmp_path, day, kind):
    assert ss.session_kind(day, str(tmp_path)) == kind
    assert ss.session_kind(day.isoformat(), str(tmp_path)) == kind
    assert ss.is_special(day, str(tmp_path)) is (kind is not None)


def test_data_copy_wins_over_the_repo_copy(tmp_path):
    path = write_sessions(tmp_path, [line(date(2026, 10, 25), "SPECIAL_SATURDAY")])
    assert ss.path_of(str(tmp_path)) == path
    assert ss.session_kind(date(2026, 10, 25), str(tmp_path)) == "SPECIAL_SATURDAY"
    assert ss.session_kind(date(2015, 11, 11), str(tmp_path)) is None   # only in the repo copy
    assert ss.weekend_dates(str(tmp_path)) == [date(2026, 10, 25)]


@pytest.mark.parametrize("lines, header, message", [
    ([line(SUN_NEW)], HEADER.replace(",note", ""), "missing column(s) note"),
    ([line(SUN_NEW, "DIWALI")], HEADER, "unknown kind 'DIWALI'"),
    ([line(SUN_NEW).replace(",Sun,", ",Sat,")], HEADER, "2026-10-11 is a Sun, not 'Sat'"),
    ([line(SUN_NEW), line(SUN_NEW, "BUDGET_WEEKEND")], HEADER, "listed twice: 2026-10-11"),
    ([line(SUN_NEW, start="6pm")], HEADER, "bad start '6pm'"),
    ([line(SUN_NEW, end="24:00")], HEADER, "bad end '24:00'"),
    ([line(SUN_NEW, in_bhav="maybe")], HEADER, "in_bhavcopy must be yes or no"),
    (["2026-13-01,Sun,MUHURAT,,,no,x,"], HEADER, "bad date '2026-13-01'"),
])
def test_a_bad_file_is_refused_with_its_name(tmp_path, lines, header, message):
    path = write_sessions(tmp_path, lines, header)
    with pytest.raises(ss.SpecialSessionsError) as e:
        ss.load_special_sessions(str(tmp_path))
    assert message in str(e.value) and path in str(e.value)


# --- the dry run ---------------------------------------------------------------------------------

def test_dry_run_lists_found_not_found_and_future_and_writes_nothing(tmp_path, monkeypatch):
    weekday_store(tmp_path)
    calendar(tmp_path, SAT_OLD, SUN_NEW, SAT_NONE, SUN_NEXT)
    made = []
    real = cm_bhav.tempfile.mkdtemp
    monkeypatch.setattr(cm_bhav.tempfile, "mkdtemp", lambda **kw: made.append(real(**kw)) or made[-1])
    before = tree(tmp_path)
    c, s = client(serve(SAT_OLD, SUN_NEW))
    res = cm_bhav.check_special(str(tmp_path), client=c, now=NOW, log=lambda m: None)
    assert tree(tmp_path) == before                       # nothing written, the lock file included
    assert made and not os.path.exists(made[0])           # the temporary folder is gone
    rows = by_day(res["rows"])
    assert list(rows) == [SAT_OLD, SAT_NONE, SUN_NEW, SUN_NEXT]   # weekend rows only, by date
    assert rows[SAT_OLD]["result"] == "found" and rows[SAT_OLD]["format"] == "old" and rows[SAT_OLD]["rows"] == 18
    assert rows[SUN_NEW]["result"] == "found" and rows[SUN_NEW]["format"] == "udiff" and rows[SUN_NEW]["main"] > 0
    assert rows[SAT_NONE]["result"] == "not found" and rows[SAT_NONE]["note"].startswith("404 for ")
    assert rows[SUN_NEXT]["status"] == "future" and "result" not in rows[SUN_NEXT]
    assert s.calls == [cm_bhav.url_for(SAT_OLD, "old"), cm_bhav.url_for(SAT_NONE, "old"),
                       cm_bhav.url_for(SUN_NEW, "udiff")]
    assert res["requests"] == 4                           # the home page first, as in every run
    assert res["raw"] == [cm_bhav.raw_path(store(tmp_path), SAT_OLD, "old"),
                          cm_bhav.raw_path(store(tmp_path), SUN_NEW, "udiff")]
    assert res["years"] == [(2021, "touched"), (2026, "touched")]


def test_dry_run_write_set_marks_already_stale_years(tmp_path):
    weekday_store(tmp_path)
    os.remove(store(tmp_path, "2020.parquet"))           # the db has an ok 2020 day the parquet lacks
    calendar(tmp_path, SAT_OLD)
    c, _ = client(serve(SAT_OLD))
    res = cm_bhav.check_special(str(tmp_path), client=c, now=NOW, log=lambda m: None)
    assert res["years"] == [(2020, "stale"), (2021, "touched")]
    c, _ = client(serve(SAT_OLD))
    cm_bhav.run(str(tmp_path), [SAT_OLD], client=c, now=NOW, log=lambda m: None, special=True)
    assert os.path.exists(store(tmp_path, "2020.parquet"))   # the apply rebuilds exactly that set


def test_dry_run_on_an_empty_folder_creates_nothing(tmp_path):
    calendar(tmp_path, SUN_NEW)
    before = tree(tmp_path)
    c, _ = client(serve(SUN_NEW))
    res = cm_bhav.check_special(str(tmp_path), client=c, now=NOW, log=lambda m: None)
    assert tree(tmp_path) == before and not os.path.exists(store(tmp_path, "cm_bhav_progress.sqlite"))
    assert res["years"] == [(2026, "touched")]


def test_a_zip_saved_in_raw_by_hand_is_read_without_a_request(tmp_path):
    calendar(tmp_path, SUN_NEW)
    path = cm_bhav.raw_path(store(tmp_path), SUN_NEW, "udiff")
    os.makedirs(os.path.dirname(path))
    with open(path, "wb") as f:
        f.write(file_for(SUN_NEW, "udiff"))
    c, s = client()
    res = cm_bhav.check_special(str(tmp_path), client=c, now=NOW, log=lambda m: None)
    r = res["rows"][0]
    assert r["status"] == "in raw/" and r["result"] == "found" and s.calls == [] and res["raw"] == []
    res = cm_bhav.run(str(tmp_path), [SUN_NEW], client=c, now=NOW, log=lambda m: None, special=True)
    assert res["counts"]["ok"] == 1 and s.calls == []


def test_a_weekend_session_today_counts_from_19_00(tmp_path):
    calendar(tmp_path, SUN_NEW)
    early = datetime(2026, 10, 11, 18, 0, tzinfo=IST)
    assert cm_bhav.special_plan(str(tmp_path), early)[0]["status"] == "future"
    assert cm_bhav.special_plan(str(tmp_path), early.replace(hour=19))[0]["status"] == "to ask"


def test_dry_run_bad_file_and_failed_download(tmp_path):
    calendar(tmp_path, SAT_OLD, SUN_NEW)
    answers = {cm_bhav.url_for(SAT_OLD, "old"): Resp(200, file_for(date(2021, 3, 5), "old")),   # wrong date inside
               cm_bhav.url_for(SUN_NEW, "udiff"): Resp(200, b"<html>blocked</html>")}
    c, _ = client(answers)
    rows = by_day(cm_bhav.check_special(str(tmp_path), client=c, now=NOW, log=lambda m: None)["rows"])
    assert rows[SAT_OLD]["result"] == "bad file" and "2021-03-05" in rows[SAT_OLD]["note"]
    assert rows[SUN_NEW]["result"] == "failed"


# --- the apply -----------------------------------------------------------------------------------

def test_apply_writes_only_the_found_days_and_a_second_dry_run_has_nothing_to_write(tmp_path):
    weekday_store(tmp_path)
    calendar(tmp_path, SAT_OLD, SUN_NEW, SAT_NONE, SUN_NEXT)
    y2020 = open(store(tmp_path, "2020.parquet"), "rb").read()
    c, s = client(serve(SAT_OLD, SUN_NEW))
    days = [r["day"] for r in cm_bhav.special_plan(str(tmp_path), NOW) if r["status"] == "to ask"]
    assert days == [SAT_OLD, SAT_NONE, SUN_NEW]
    res = cm_bhav.run(str(tmp_path), days, client=c, now=NOW, log=lambda m: None, special=True)
    assert res["counts"]["ok"] == 2 and res["counts"]["not_found"] == 1 and res["counts"]["holiday"] == 0
    assert res["years"] == [2021, 2026] and res["failures"] == []
    assert open(store(tmp_path, "2020.parquet"), "rb").read() == y2020
    assert os.path.exists(cm_bhav.raw_path(store(tmp_path), SAT_OLD, "old"))
    assert os.path.exists(cm_bhav.raw_path(store(tmp_path), SUN_NEW, "udiff"))
    assert not os.path.exists(cm_bhav.raw_path(store(tmp_path), SAT_NONE, "old"))
    con = cm_bhav.open_db(store(tmp_path))
    try:
        assert cm_bhav.get_day(con, SAT_OLD)["status"] == "ok" and cm_bhav.get_day(con, SUN_NEW)["status"] == "ok"
        assert cm_bhav.get_day(con, SAT_NONE) is None and cm_bhav.get_day(con, SUN_NEXT) is None
    finally:
        con.close()
    assert SAT_OLD.isoformat() in cm_bhav.parquet_dates(store(tmp_path), 2021)
    assert SUN_NEW.isoformat() in cm_bhav.parquet_dates(store(tmp_path), 2026)
    assert not os.path.exists(store(tmp_path, "holidays.log"))
    leftovers = [p for p in tree(tmp_path) if p.endswith((".part", ".tmp"))]
    assert leftovers == []
    c, s = client(serve(SAT_OLD, SUN_NEW))
    again = cm_bhav.check_special(str(tmp_path), client=c, now=NOW, log=lambda m: None)
    assert [r["status"] for r in again["rows"]] == ["in store", "to ask", "in store", "future"]
    assert again["raw"] == [] and again["years"] == [] and s.calls == [cm_bhav.url_for(SAT_NONE, "old")]


def test_second_dry_run_after_everything_was_found_makes_no_request(tmp_path):
    calendar(tmp_path, SAT_OLD, SUN_NEW)
    c, _ = client(serve(SAT_OLD, SUN_NEW))
    assert cli.main(["--special", "--apply", "--folder", str(tmp_path)], now=NOW, client=c) == 0
    c, s = client(serve(SAT_OLD, SUN_NEW))
    res = cm_bhav.check_special(str(tmp_path), client=c, now=NOW, log=lambda m: None)
    assert res["requests"] == 0 and s.calls == [] and res["raw"] == [] and res["years"] == []


def test_apply_failed_download_records_nothing_and_exits_1(tmp_path, capsys):
    calendar(tmp_path, SUN_NEW)
    c, _ = client({cm_bhav.url_for(SUN_NEW, "udiff"): Resp(200, b"<html>blocked</html>")})
    assert cli.main(["--special", "--apply", "--folder", str(tmp_path)], now=NOW, client=c) == 1
    con = cm_bhav.open_db(store(tmp_path))
    try:
        assert cm_bhav.get_day(con, SUN_NEW) is None
    finally:
        con.close()
    assert "1 download(s) failed" in capsys.readouterr().err


# --- the CLI -------------------------------------------------------------------------------------

def test_cli_dry_run_prints_the_list_and_the_write_set(tmp_path, capsys):
    weekday_store(tmp_path)
    calendar(tmp_path, SAT_OLD, SAT_NONE, SUN_NEXT)
    before = tree(tmp_path)
    c, _ = client(serve(SAT_OLD))
    assert cli.main(["--special", "--folder", str(tmp_path)], now=NOW, client=c) == 0
    out = capsys.readouterr().out
    assert tree(tmp_path) == before
    assert "DRY RUN: nothing is saved" in out
    assert "2021-03-06 Sat BUDGET_WEEKEND   found (old): 18 rows" in out
    assert "2022-01-15 Sat BUDGET_WEEKEND   not found: 404 for" in out
    assert "2026-11-08 Sun MUHURAT          future, not asked" in out
    assert "found 1, not found 1, future 1; 3 request(s)" in out
    assert "raw/ zips: 1" in out and "parquet years rebuilt: 2021 (touched)" in out


def test_cli_dry_run_exits_1_on_a_bad_file(tmp_path):
    calendar(tmp_path, SAT_OLD)
    c, _ = client({cm_bhav.url_for(SAT_OLD, "old"): Resp(200, file_for(date(2021, 3, 5), "old"))})
    assert cli.main(["--special", "--folder", str(tmp_path)], now=NOW, client=c) == 1


def test_cli_apply_prints_not_found_and_exits_0(tmp_path, capsys):
    calendar(tmp_path, SAT_OLD, SAT_NONE)
    c, _ = client(serve(SAT_OLD))
    assert cli.main(["--special", "--apply", "--folder", str(tmp_path)], now=NOW, client=c) == 0
    out = capsys.readouterr().out
    assert "This run: ok 1, holidays 0, failed 0, pending 0" in out
    assert "Special sessions: not found at NSE 1 (nothing recorded for them)" in out


@pytest.mark.parametrize("argv", [["--apply"], ["--special", "2026-10-14"], ["--special", "--missing"],
                                  ["--special", "--offline"], ["--special", "--force"],
                                  ["--special", "--from", "2026-10-12", "--to", "2026-10-14"], ["--missing", "--apply"]])
def test_cli_special_argument_errors(tmp_path, argv):
    with pytest.raises(SystemExit) as e:
        cli.main(argv + ["--folder", str(tmp_path)], now=NOW)
    assert e.value.code == 2


def test_cli_bad_calendar_exits_2(tmp_path, capsys):
    write_sessions(tmp_path, [line(SUN_NEW, "DIWALI")])
    for argv in (["--special"], ["--special", "--apply"]):
        assert cli.main(argv + ["--folder", str(tmp_path)], now=NOW, client=client()[0]) == 2
        assert "unknown kind 'DIWALI'" in capsys.readouterr().err


def test_busy_lock_refuses_dry_run_and_apply(tmp_path, capsys):
    calendar(tmp_path, SUN_NEW)
    os.makedirs(store(tmp_path), exist_ok=True)
    lock = RunLock(store(tmp_path, ".lock")).acquire("test")
    try:
        with pytest.raises(cm_bhav.LockBusy):
            cm_bhav.check_special(str(tmp_path), client=client()[0], now=NOW, log=lambda m: None)
        for argv in (["--special"], ["--special", "--apply"]):
            assert cli.main(argv + ["--folder", str(tmp_path)], now=NOW, client=client()[0]) == 2
            assert "Another NSE_CM_bhav run is in progress" in capsys.readouterr().err
    finally:
        lock.release()


# --- weekdays unchanged ----------------------------------------------------------------------------

def test_weekdays_are_unchanged_without_special(tmp_path):
    with pytest.raises(SystemExit) as e:   # a weekend date given by hand is still refused
        cli.main([SUN_NEW.isoformat(), "--folder", str(tmp_path)], now=NOW)
    assert e.value.code == 2
    assert all(d.weekday() < 5 for d in cm_bhav.weekdays(date(2026, 10, 1), date(2026, 10, 31)))
    c, s = client(serve(SUN_NEW))
    res = cm_bhav.run(str(tmp_path), [SUN_NEW], client=c, now=NOW, log=lambda m: None)
    assert s.calls == [] and res["counts"] == dict.fromkeys(cm_bhav.STATUSES + ("skipped",), 0)

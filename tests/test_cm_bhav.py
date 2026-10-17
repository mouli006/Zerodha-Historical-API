"""backfill/cm_bhav.py and cm_bhav.py: the NSE equity bhavcopy store. Fixtures are cut from real files
(cm03MAR2021bhav.csv and BhavCopy_NSE_CM_0_0_0_20261014_F_0000.csv: the header plus 18 / 20 rows of mixed series).
No test touches the network or .env."""
import csv
import io
import os
import sqlite3
import zipfile
from datetime import date, datetime

import pyarrow.parquet as pq
import pytest
import requests

import cm_bhav as cli
import config
import nse_reference
from backfill import cm_bhav
from backfill.cm_bhav import IST, BadFile, parse_zip
from backfill.fno import RunLock
from backfill.nse_ref import fetch
from backfill.nse_ref.fetch import HOME, Throttle

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "cm_bhav")
OLD_FIX, OLD_DAY = "cm03MAR2021bhav.csv.zip", date(2021, 3, 3)
NEW_FIX, NEW_DAY = "BhavCopy_NSE_CM_0_0_0_20261014_F_0000.csv.zip", date(2026, 10, 14)
NOW = datetime(2026, 10, 16, 20, 0, tzinfo=IST)   # a Friday, after 19:00


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def refuse(*a, **k):
        raise AssertionError("a test tried to reach the network")
    monkeypatch.setattr(requests.Session, "request", refuse)
    monkeypatch.setattr(fetch, "launch_browser", refuse)
    monkeypatch.setattr(config, "load_env", lambda: {})


def fixture_text(name):
    with zipfile.ZipFile(os.path.join(FIX, name)) as z:
        return z.read(z.namelist()[0]).decode()


def zip_bytes(inner, text):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr(inner, text)
    return buf.getvalue()


def old_text(day):
    return fixture_text(OLD_FIX).replace("03-MAR-2021", f"{day:%d}-{cm_bhav.MONTHS[day.month - 1]}-{day:%Y}")


def new_text(day):
    return fixture_text(NEW_FIX).replace("2026-10-14", day.isoformat())


def file_for(day, fmt, text=None):
    """Zip bytes of the fixture of fmt, dated day (or text)."""
    if text is None:
        text = old_text(day) if fmt == "old" else new_text(day)
    return zip_bytes(cm_bhav.file_name(day, fmt)[:-4], text)


class Resp:
    def __init__(self, status, content=b""):
        self.status_code, self.content = status, content

    @property
    def ok(self):
        return 200 <= self.status_code < 300


class FakeSession:
    """URL -> Resp (or an exception to raise); anything else 404. The home page answers 403 like NSE's."""

    def __init__(self, answers=None):
        self.answers, self.calls, self.headers = dict(answers or {}), [], {}

    def get(self, url, timeout=None):
        if url == HOME:
            return Resp(403)
        self.calls.append(url)
        a = self.answers.get(url, Resp(404))
        if isinstance(a, BaseException):
            raise a
        return a


class FakeBrowser:
    def __init__(self, answers=None):
        self.answers, self.calls, self.dead = dict(answers or {}), [], ""

    def get(self, url):
        self.calls.append(url)
        return self.answers.get(url, (404, b""))

    def close(self):
        pass


def client(answers=None, browser=None):
    s = FakeSession(answers)
    c = cm_bhav.CmClient(session=s, throttle=Throttle(gap=0, sleep=lambda x: None), pause=lambda x: None,
                         browser=browser or FakeBrowser(), use_browser=browser is not None, log=lambda m: None)
    return c, s


def serve(*days, fmt=None):
    """Answers serving the fixture for each day in its (first) format."""
    out = {}
    for d in days:
        f = fmt or cm_bhav.formats_for(d)[0]
        out[cm_bhav.url_for(d, f)] = Resp(200, file_for(d, f))
    return out


def run(tmp_path, days, answers=None, browser=None, now=NOW, **kw):
    c, s = client(answers, browser)
    res = cm_bhav.run(str(tmp_path), days, client=c, now=now, log=lambda m: None, **kw)
    return res, s


def db_rows(tmp_path):
    con = sqlite3.connect(os.path.join(tmp_path, "NSE_CM_bhav", "cm_bhav_progress.sqlite"))
    try:
        return {r[0]: r[1:] for r in con.execute("SELECT date, status, format, note FROM days")}
    finally:
        con.close()


def store(tmp_path, *parts):
    return os.path.join(tmp_path, "NSE_CM_bhav", *parts)


# --- sources ---------------------------------------------------------------------------------

def test_urls_and_file_names_per_date():
    assert cm_bhav.url_for(date(2015, 1, 2), "old") == \
        "https://nsearchives.nseindia.com/content/historical/EQUITIES/2015/JAN/cm02JAN2015bhav.csv.zip"
    assert cm_bhav.file_name(OLD_DAY, "old") == OLD_FIX
    assert cm_bhav.url_for(NEW_DAY, "udiff") == f"https://nsearchives.nseindia.com/content/cm/{NEW_FIX}"
    assert cm_bhav.formats_for(date(2015, 1, 2)) == ["old"]
    assert cm_bhav.formats_for(date(2024, 6, 28)) == ["old"]
    assert cm_bhav.formats_for(date(2024, 7, 5)) == ["udiff", "old"]
    assert cm_bhav.formats_for(date(2024, 7, 12)) == ["udiff", "old"]
    assert cm_bhav.formats_for(date(2024, 7, 15)) == ["udiff"]
    assert cm_bhav.parse_file_name(OLD_FIX) == (OLD_DAY, "old")
    assert cm_bhav.parse_file_name(NEW_FIX) == (NEW_DAY, "udiff")
    assert cm_bhav.parse_file_name("notes.txt") is None


# --- parsing ---------------------------------------------------------------------------------

def test_both_formats_parse_into_the_same_columns():
    old = parse_zip(os.path.join(FIX, OLD_FIX), OLD_DAY, "old")
    new = parse_zip(os.path.join(FIX, NEW_FIX), NEW_DAY, "udiff")
    assert list(old.columns) == list(new.columns) == cm_bhav.COLUMNS
    assert list(old.dtypes) == list(new.dtypes)
    assert len(old) == 18 and len(new) == 20
    # every series is kept
    assert {"EQ", "BE", "BZ", "SM", "GB", "GS", "N1"} <= set(old["series"])
    assert {"EQ", "BE", "BZ", "SM", "ST", "GB"} <= set(new["series"])
    r = old[old.symbol == "20MICRONS"].iloc[0].to_dict()
    assert r == {"date": OLD_DAY, "symbol": "20MICRONS", "series": "EQ", "isin": "INE144J01027", "open": 37.4,
                 "high": 39.2, "low": 36.95, "close": 38.15, "last": 38.2, "prev_close": 36.95, "volume": 216918,
                 "turnover": 8277912.6, "trades": 1427, "fin_id": "", "format": "old"}
    r = new[new.symbol == "20MICRONS"].iloc[0].to_dict()
    assert r["close"] == 198.43 and r["last"] == 198.0 and r["volume"] == 61040 and r["trades"] == 2195
    assert r["fin_id"] == "16921" and r["format"] == "udiff" and r["date"] == NEW_DAY
    assert cm_bhav.eq_be_bz(old) == 13


def test_old_format_trailing_empty_column(tmp_path):
    text = fixture_text(OLD_FIX)
    assert all(line.endswith(",") for line in text.splitlines())
    # without the trailing comma the columns are still recognised
    p = tmp_path / OLD_FIX
    p.write_bytes(zip_bytes("x.csv", "\n".join(line[:-1] for line in text.splitlines()) + "\n"))
    assert len(parse_zip(str(p), OLD_DAY, "old")) == 18


@pytest.mark.parametrize("stamp", ["03-Mar-21", "03-mar-21", "03-MAR-21"])
def test_old_format_two_digit_year(tmp_path, stamp):
    p = tmp_path / OLD_FIX
    p.write_bytes(zip_bytes("x.csv", fixture_text(OLD_FIX).replace("03-MAR-2021", stamp)))
    df = parse_zip(str(p), OLD_DAY, "old")
    assert len(df) == 18 and set(df["date"]) == {OLD_DAY}
    # the date must still be the requested day
    with pytest.raises(BadFile, match="expected 2021-03-04"):
        parse_zip(str(p), date(2021, 3, 4), "old")


def test_old_format_bad_timestamp(tmp_path):
    p = tmp_path / OLD_FIX
    p.write_bytes(zip_bytes("x.csv", fixture_text(OLD_FIX).replace("03-MAR-2021", "03-Mar-1x", 1)))
    with pytest.raises(BadFile, match="bad TIMESTAMP '03-Mar-1x'"):
        parse_zip(str(p), OLD_DAY, "old")


def test_turnover_is_rounded_to_2_decimals(tmp_path):
    text = fixture_text(NEW_FIX).replace(",12175822.04,", ",12175822.040000001,")
    assert "12175822.040000001" in text
    p = tmp_path / NEW_FIX
    p.write_bytes(zip_bytes("x.csv", text))
    df = parse_zip(str(p), NEW_DAY, "udiff")
    assert df[df.symbol == "20MICRONS"].iloc[0]["turnover"] == 12175822.04


def _edit_new(text, what):
    lines = text.splitlines()
    if what == "unknown":
        lines[0] = lines[0].replace("LastPric", "LastPrice")
    elif what == "missing":
        lines = [",".join(line.split(",")[:-1]) for line in lines]
    elif what == "date":
        lines[3] = lines[3].replace("2026-10-14", "2026-10-13", 1)
    elif what == "duplicate":
        lines.append(lines[2])
    elif what == "zero":
        f = lines[2].split(",")
        f[17] = "0"   # ClsPric
        lines[2] = ",".join(f)
    elif what == "inverted":
        f = lines[2].split(",")
        f[15], f[16] = f[16], f[15]   # HghPric <-> LwPric
        lines[2] = ",".join(f)
    elif what == "segment":
        lines[2] = lines[2].replace(",CM,", ",FO,", 1)
    return "\n".join(lines) + "\n"


@pytest.mark.parametrize("what,message", [
    ("unknown", "unknown columns ['LastPrice']"), ("missing", "missing columns ['Rsvd4']"),
    ("date", "dates ['2026-10-13', '2026-10-14']"), ("duplicate", "duplicate symbol/series rows: 20MICRONS EQ"),
    ("zero", "open/high/low/close <= 0: 20MICRONS EQ"), ("inverted", "high < low: 20MICRONS EQ"),
    ("segment", "Sgmt must be CM, found ['FO']")])
def test_validation_failures_name_the_file(tmp_path, what, message):
    p = tmp_path / NEW_FIX
    p.write_bytes(zip_bytes("x.csv", _edit_new(fixture_text(NEW_FIX), what)))
    with pytest.raises(BadFile) as e:
        parse_zip(str(p), NEW_DAY, "udiff")
    assert str(e.value).startswith(NEW_FIX + ":") and message in str(e.value)


def test_wrong_requested_date_and_not_a_zip(tmp_path):
    with pytest.raises(BadFile, match="expected 2021-03-04"):
        parse_zip(os.path.join(FIX, OLD_FIX), date(2021, 3, 4), "old")
    p = tmp_path / OLD_FIX
    p.write_bytes(b"<html>blocked</html>")
    with pytest.raises(BadFile, match="not a valid zip"):
        parse_zip(str(p), OLD_DAY, "old")


# --- runs ------------------------------------------------------------------------------------

def test_range_ok_and_a_bad_file_fails_alone(tmp_path):
    days = [date(2026, 10, 12), date(2026, 10, 13), date(2026, 10, 14)]
    answers = serve(*days)
    answers[cm_bhav.url_for(days[1], "udiff")] = Resp(200, file_for(days[1], "udiff",
                                                                     _edit_new(new_text(days[1]), "zero")))
    res, s = run(tmp_path, days + [date(2026, 10, 10)], answers)   # a Saturday is ignored
    rows = db_rows(tmp_path)
    assert rows["2026-10-12"][:2] == ("ok", "udiff") and rows["2026-10-14"][0] == "ok"
    assert rows["2026-10-13"][0] == "failed" and "<= 0: 20MICRONS EQ" in rows["2026-10-13"][2]
    assert "2026-10-10" not in rows
    assert res["counts"]["ok"] == 2 and res["counts"]["failed"] == 1
    assert res["failures"][0][0] == days[1] and res["failures"][0][2] == store(
        tmp_path, "raw", "2026", cm_bhav.file_name(days[1], "udiff"))
    # the raw zip is kept as downloaded
    assert open(store(tmp_path, "raw", "2026", NEW_FIX), "rb").read() == file_for(days[2], "udiff")
    with open(store(tmp_path, "days.csv"), newline="") as f:
        assert f.readline() == "date,status,format,rows,eq_be_bz_rows,file,downloaded_at,note\n"
    with open(store(tmp_path, "days.csv"), newline="") as f:
        dc = {r["date"]: r for r in csv.DictReader(f)}
    assert dc["2026-10-12"]["rows"] == "20" and dc["2026-10-12"]["eq_be_bz_rows"] == "13"
    assert dc["2026-10-12"]["file"] == "raw/2026/BhavCopy_NSE_CM_0_0_0_20261012_F_0000.csv.zip"
    assert dc["2026-10-13"]["status"] == "failed" and dc["2026-10-12"]["downloaded_at"]
    t = pq.read_table(store(tmp_path, "2026.parquet"))
    assert t.schema == cm_bhav.SCHEMA and t.num_rows == 40
    assert sorted({d.isoformat() for d in t.column("date").to_pylist()}) == ["2026-10-12", "2026-10-14"]
    assert os.path.exists(store(tmp_path, "cm_bhav.log"))


def test_404_is_a_holiday_logged_and_not_asked_again(tmp_path):
    day = date(2026, 10, 2)
    res, s = run(tmp_path, [day])
    assert db_rows(tmp_path)["2026-10-02"][0] == "holiday" and res["counts"]["holiday"] == 1
    log = open(store(tmp_path, "holidays.log"), encoding="utf-8").read()
    assert log.startswith("2026-10-02 Fri holiday: 404 for " + cm_bhav.url_for(day, "udiff"))
    res, s = run(tmp_path, [day])
    assert s.calls == [] and res["counts"]["skipped"] == 1
    res, s = run(tmp_path, [day], force=True)   # --force asks again, logs once
    assert s.calls == [cm_bhav.url_for(day, "udiff")]
    assert open(store(tmp_path, "holidays.log"), encoding="utf-8").read() == log


def test_html_page_instead_of_the_zip_is_failed_not_a_holiday(tmp_path):
    day = date(2026, 10, 15)
    url = cm_bhav.url_for(day, "udiff")
    page = Resp(200, b"<!DOCTYPE html><html>Access denied</html>")
    res, s = run(tmp_path, [day], {url: page})
    assert db_rows(tmp_path)["2026-10-15"][0] == "failed"
    path = store(tmp_path, "raw", "2026", cm_bhav.file_name(day, "udiff"))
    assert res["failures"] == [(day, url, path, "NSE blocked the script (a page instead of the zip); "
                                                "browser fallback is off")]
    assert not os.path.exists(path) and not os.path.exists(store(tmp_path, "holidays.log"))
    # with the browser fallback, a page there is a failure too
    browser = FakeBrowser({url: (200, b"<html>still a page</html>")})
    res, s = run(tmp_path, [day], {url: page}, browser=browser)
    assert browser.calls == [url] and res["failures"][0][3] == "browser: a page instead of the zip"
    # retried next run; the browser can fetch it
    browser = FakeBrowser({url: (200, file_for(day, "udiff"))})
    res, s = run(tmp_path, [day], {url: Resp(403)}, browser=browser)
    assert db_rows(tmp_path)["2026-10-15"][0] == "ok" and res["failures"] == []


def test_failed_download_lists_the_drop_in_path_and_uses_a_file_saved_there(tmp_path, capsys):
    day = date(2026, 10, 15)
    c, s = client({cm_bhav.url_for(day, "udiff"): requests.ConnectionError("reset")})
    assert cli.main(["2026-10-15", "--folder", str(tmp_path), "--no-browser"], now=NOW, client=c) == 1
    err = capsys.readouterr().err
    path = store(tmp_path, "raw", "2026", "BhavCopy_NSE_CM_0_0_0_20261015_F_0000.csv.zip")
    assert cm_bhav.url_for(day, "udiff") in err and f"-> {path}" in err
    assert len(s.calls) == cm_bhav.TRIES   # 3 tries
    with open(path, "wb") as f:   # saved by hand
        f.write(file_for(day, "udiff"))
    c, s = client()
    assert cli.main(["2026-10-15", "--folder", str(tmp_path)], now=NOW, client=c) == 0
    assert s.calls == [] and db_rows(tmp_path)["2026-10-15"][0] == "ok"


def test_switch_window_falls_back_to_the_old_file(tmp_path):
    fri, mon = date(2024, 7, 5), date(2024, 7, 8)
    answers = {**serve(fri, fmt="old"), **serve(mon, fmt="udiff")}
    res, s = run(tmp_path, [fri, mon], answers)
    assert s.calls == [cm_bhav.url_for(fri, "udiff"), cm_bhav.url_for(fri, "old"), cm_bhav.url_for(mon, "udiff")]
    rows = db_rows(tmp_path)
    assert rows["2024-07-05"][:2] == ("ok", "old") and rows["2024-07-08"][:2] == ("ok", "udiff")
    assert "Format switch: first UDiFF day 2024-07-08, last old-format day 2024-07-05" in res["report"]
    t = pq.read_table(store(tmp_path, "2024.parquet")).to_pandas()
    assert set(t["format"]) == {"old", "udiff"} and len(t) == 38
    # both 404 in the window: a holiday
    run(tmp_path, [date(2024, 7, 9)])
    assert db_rows(tmp_path)["2024-07-09"][0] == "holiday"


def test_never_downloads_twice(tmp_path):
    day = date(2026, 10, 14)
    run(tmp_path, [day], serve(day))
    res, s = run(tmp_path, [day], serve(day))
    assert s.calls == [] and res["counts"]["skipped"] == 1
    os.remove(store(tmp_path, "cm_bhav_progress.sqlite"))   # even without progress, the zip in raw/ is used
    res, s = run(tmp_path, [day], serve(day))
    assert s.calls == [] and res["counts"]["ok"] == 1 and res["requests"] == 0


def test_resume_after_a_stop(tmp_path):
    days = [date(2026, 10, 12), date(2026, 10, 13), date(2026, 10, 14)]
    answers = serve(*days)
    answers[cm_bhav.url_for(days[2], "udiff")] = KeyboardInterrupt()
    with pytest.raises(KeyboardInterrupt):
        run(tmp_path, days, answers)
    rows = db_rows(tmp_path)
    assert rows["2026-10-12"][0] == rows["2026-10-13"][0] == "ok" and "2026-10-14" not in rows
    assert pq.read_table(store(tmp_path, "2026.parquet")).num_rows == 40   # what was done is saved
    res, s = run(tmp_path, days, serve(*days))   # the lock was released; only the last day is asked for
    assert s.calls == [cm_bhav.url_for(days[2], "udiff")] and res["counts"]["skipped"] == 2
    assert pq.read_table(store(tmp_path, "2026.parquet")).num_rows == 60


def test_today_before_19_is_pending_and_a_404_today_too(tmp_path):
    today = date(2026, 10, 16)
    res, s = run(tmp_path, [today], now=datetime(2026, 10, 16, 18, 59, tzinfo=IST))
    assert s.calls == [] and db_rows(tmp_path)["2026-10-16"][0] == "pending" and res["counts"]["pending"] == 1
    res, s = run(tmp_path, [today])   # 19:00 or later, but NSE has no file yet
    assert len(s.calls) == 1 and db_rows(tmp_path)["2026-10-16"][:3] == ("pending", "", "404: not yet published")
    assert not os.path.exists(store(tmp_path, "holidays.log"))
    res, s = run(tmp_path, [today], serve(today))
    assert db_rows(tmp_path)["2026-10-16"][0] == "ok"


def test_parquet_rebuilt_per_year_only_where_needed(tmp_path):
    res, _ = run(tmp_path, [OLD_DAY, NEW_DAY], {**serve(OLD_DAY), **serve(NEW_DAY)})
    assert res["years"] == [2021, 2026]
    before = os.path.getmtime(store(tmp_path, "2021.parquet"))
    res, _ = run(tmp_path, [date(2026, 10, 15)], serve(date(2026, 10, 15)))
    assert res["years"] == [2026]
    assert os.path.getmtime(store(tmp_path, "2021.parquet")) == before
    t = pq.read_table(store(tmp_path, "2026.parquet")).to_pandas()
    assert len(t) == 40 and list(t.columns) == cm_bhav.COLUMNS
    assert t[["date", "symbol", "series"]].equals(t.sort_values(["date", "symbol", "series"])[["date", "symbol", "series"]])
    # a parquet removed by hand is rebuilt on the next run, with nothing to download
    os.remove(store(tmp_path, "2021.parquet"))
    res, s = run(tmp_path, [])
    assert res["years"] == [2021] and s.calls == [] and pq.read_table(store(tmp_path, "2021.parquet")).num_rows == 18


def test_offline_rebuilds_from_raw_only(tmp_path):
    os.makedirs(store(tmp_path, "raw", "2021"))
    os.makedirs(store(tmp_path, "raw", "2026"))
    with open(store(tmp_path, "raw", "2021", OLD_FIX), "wb") as f:
        f.write(open(os.path.join(FIX, OLD_FIX), "rb").read())
    with open(store(tmp_path, "raw", "2026", NEW_FIX), "wb") as f:
        f.write(open(os.path.join(FIX, NEW_FIX), "rb").read())
    bad = cm_bhav.file_name(date(2026, 10, 15), "udiff")
    with open(store(tmp_path, "raw", "2026", bad), "wb") as f:
        f.write(file_for(date(2026, 10, 15), "udiff", _edit_new(new_text(date(2026, 10, 15)), "duplicate")))
    res = cm_bhav.run(str(tmp_path), offline=True, now=NOW, log=lambda m: None)
    assert res["requests"] == 0 and res["counts"]["ok"] == 2 and res["counts"]["failed"] == 1
    rows = db_rows(tmp_path)
    assert rows["2021-03-03"][:2] == ("ok", "old") and rows["2026-10-15"][0] == "failed"
    assert pq.read_table(store(tmp_path, "2021.parquet")).num_rows == 18
    assert pq.read_table(store(tmp_path, "2026.parquet")).num_rows == 20


def test_cli_offline_and_argument_errors(tmp_path, capsys):
    os.makedirs(store(tmp_path, "raw", "2021"))
    with open(store(tmp_path, "raw", "2021", OLD_FIX), "wb") as f:
        f.write(open(os.path.join(FIX, OLD_FIX), "rb").read())
    assert cli.main(["--offline", "--folder", str(tmp_path)], now=NOW) == 0
    assert "2021" in capsys.readouterr().out
    for argv in (["2026-10-17"], ["2026-10-19"], [], ["2026-10-14", "--missing"], ["--offline", "2026-10-14"],
                 ["--from", "2026-10-14"], ["--from", "2026-10-15", "--to", "2026-10-14"],
                 ["--from", "2026-10-14", "--to", "2026-10-19"], ["2026-13-01"], ["--missing", "--sleep", "0"]):
        with pytest.raises(SystemExit) as e:
            cli.main(argv + ["--folder", str(tmp_path)], now=NOW)
        assert e.value.code == 2, argv


def test_cli_missing_covers_weekdays_from_2015(tmp_path, monkeypatch):
    seen = {}

    def fake_run(folder, days, **kw):
        seen["days"] = days
        return {"counts": dict.fromkeys(cm_bhav.STATUSES + ("skipped",), 0), "failures": [], "report": "",
                "flagged": {}, "store": folder, "years": [], "requests": 0}
    monkeypatch.setattr(cm_bhav, "run", fake_run)
    assert cli.main(["--missing", "--folder", str(tmp_path)], now=NOW) == 0
    assert seen["days"][0] == date(2015, 1, 1) and seen["days"][-1] == date(2026, 10, 16)
    assert all(d.weekday() < 5 for d in seen["days"]) and len(seen["days"]) == 3077


def test_cli_default_folder_is_equity_data_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "load_env", lambda: {"EQUITY_DATA_DIR": str(tmp_path / "eq"),
                                                      "KITE_DATA_DIR": str(tmp_path / "kite")})
    c, _ = client(serve(NEW_DAY))
    assert cli.main(["2026-10-14"], now=NOW, client=c) == 0
    assert os.path.exists(tmp_path / "eq" / "NSE_CM_bhav" / "2026.parquet")
    assert not os.path.exists(tmp_path / "kite")


def test_lock_refuses_a_second_run(tmp_path, capsys):
    os.makedirs(store(tmp_path))
    lock = RunLock(store(tmp_path, ".lock")).acquire("test")
    try:
        with pytest.raises(cm_bhav.LockBusy):
            cm_bhav.run(str(tmp_path), [NEW_DAY], client=client()[0], now=NOW, log=lambda m: None)
        assert cli.main(["2026-10-14", "--folder", str(tmp_path)], now=NOW, client=client()[0]) == 2
        assert "Another NSE_CM_bhav run is in progress" in capsys.readouterr().err
    finally:
        lock.release()
    c, _ = client(serve(NEW_DAY))
    assert cli.main(["2026-10-14", "--folder", str(tmp_path)], now=NOW, client=c) == 0


def test_report_counts_and_flags_years_with_too_many_holidays(tmp_path):
    days = [date(2026, 10, 12), date(2026, 10, 13)]
    run(tmp_path, days, serve(*days))
    con = cm_bhav.open_db(store(tmp_path))
    try:
        many = list(cm_bhav.weekdays(date(2016, 1, 1), date(2016, 1, 27)))[:19]
        for d in many:
            cm_bhav.set_day(con, d, "holiday", note="404")
        for d in list(cm_bhav.weekdays(date(2017, 1, 2), date(2017, 1, 31)))[:18]:
            cm_bhav.set_day(con, d, "holiday", note="404")
        cm_bhav.set_day(con, date(2026, 10, 2), "holiday", note="404")
        cm_bhav.set_day(con, date(2026, 10, 9), "failed", note="x")
        text, flagged = cm_bhav.report(con)
    finally:
        con.close()
    assert list(flagged) == ["2016"] and flagged["2016"] == [d.isoformat() for d in many]
    lines = text.splitlines()
    row = next(line for line in lines if line.startswith("2026"))
    assert row.split()[:6] == ["2026", "2", "1", "1", "0", "40"] and row.split()[6:9] == ["13", "13", "13"]
    assert next(line for line in lines if line.startswith("2016")).endswith("<- check holidays")
    assert "check holidays" not in next(line for line in lines if line.startswith("2017"))
    assert any(line.startswith("2016: 19 weekday holidays") and "2016-01-01" in line for line in lines)


def test_nse_reference_default_folder_follows_equity_data_dir(monkeypatch):
    monkeypatch.setattr(config, "load_env", lambda: {"EQUITY_DATA_DIR": r"D:\eq", "KITE_DATA_DIR": r"D:\kite"})
    assert nse_reference.data_dir() == r"D:\eq"
    monkeypatch.setattr(config, "load_env", lambda: {"KITE_DATA_DIR": r"D:\kite"})
    assert nse_reference.data_dir() == r"D:\kite"
    monkeypatch.setattr(config, "load_env", lambda: {})
    assert nse_reference.data_dir() == config.DEFAULT_DATA_DIR


def _blank_last(text, symbol, series):
    lines = text.splitlines()
    for i, line in enumerate(lines):
        f = line.split(",")
        if len(f) > 18 and f[7] == symbol and f[8] == series:
            f[18] = ""   # LastPric
            lines[i] = ",".join(f)
    return "\n".join(lines) + "\n"


def test_blank_last_price_allowed_only_outside_eq_be_bz(tmp_path):
    # July 2024: NSE left LastPric blank on one debt row a day (975IHFL28 NT, IBHFZC25B AT)
    day = date(2026, 10, 12)
    text = _blank_last(new_text(day), "SGBJUN28", "GB")
    res, s = run(tmp_path, [day], {cm_bhav.url_for(day, "udiff"): Resp(200, file_for(day, "udiff", text))})
    status, fmt, note = db_rows(tmp_path)["2026-10-12"]
    assert status == "ok" and note == "1 row(s) with a blank last price (series GB), stored as null"
    t = pq.read_table(store(tmp_path, "2026.parquet")).to_pandas()
    assert t[t.symbol == "SGBJUN28"]["last"].isna().all() and t["last"].notna().sum() == 19
    p = tmp_path / NEW_FIX
    p.write_bytes(zip_bytes("x.csv", _blank_last(fixture_text(NEW_FIX), "20MICRONS", "EQ")))
    with pytest.raises(BadFile, match="20MICRONS EQ: LastPric is not a number"):
        parse_zip(str(p), NEW_DAY, "udiff")


def test_cli_lists_a_file_that_failed_the_checks_apart_from_downloads(tmp_path, capsys):
    day = date(2026, 10, 15)
    bad = file_for(day, "udiff", _edit_new(new_text(day), "inverted"))
    c, _ = client({cm_bhav.url_for(day, "udiff"): Resp(200, bad)})
    assert cli.main(["2026-10-15", "--folder", str(tmp_path)], now=NOW, client=c) == 1
    err = capsys.readouterr().err
    assert "1 file(s) failed the checks" in err and "high < low: 20MICRONS EQ" in err
    assert "download(s) failed" not in err

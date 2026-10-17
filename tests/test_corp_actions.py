"""backfill/equity/corp_actions.py and `adjustments.py fetch-actions` on saved NSE samples in
tests/fixtures/corp_actions/ (2024_sample.json: 4 rows of the real 2024 API answer; sample.csv: real rows of the
saved yearly files; blocked.html: a block page). Fake session and browser; no network, .env never read."""
import os

import pandas as pd
import pytest
import requests

import adjustments as cli
import config
from backfill.equity import adjustments as adj
from backfill.equity import corp_actions as ca
from backfill.locks import RunLock
from backfill.nse_ref import fetch
from backfill.nse_ref.fetch import HOME, NseClient, Throttle

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "corp_actions")


def fixture(name):
    with open(os.path.join(FIX, name), "rb") as f:
        return f.read()


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def refuse(*a, **k):
        raise AssertionError("a test tried to reach the network")
    monkeypatch.setattr(requests.Session, "request", refuse)
    monkeypatch.setattr(fetch, "launch_browser", refuse)
    monkeypatch.setattr(config, "load_env", lambda: {})


class Resp:
    def __init__(self, status, content=b""):
        self.status_code, self.content = status, content
        self.ok = 200 <= status < 300


class FakeSession:
    def __init__(self, answers=None):
        self.answers, self.calls, self.headers = dict(answers or {}), [], {}

    def get(self, url, timeout=None):
        if url == HOME:
            return Resp(403)
        self.calls.append(url)
        return self.answers.get(url, Resp(404))


class FakeBrowser:
    def __init__(self, answers=None, dead=""):
        self.answers, self.calls, self.dead = dict(answers or {}), [], dead

    def get(self, url):
        self.calls.append(url)
        return self.answers.get(url, (404, b""))

    def close(self):
        pass


def client(answers=None, browser=None, use_browser=True):
    return NseClient(session=FakeSession(answers), throttle=Throttle(gap=0, sleep=lambda x: None),
                     browser=browser or FakeBrowser(), use_browser=use_browser, log=lambda *_: None)


def saved(folder, year):
    return os.path.join(folder, "adjustments", "input", f"corporate_actions_{year}.csv")


def test_url_and_json_to_nse_columns():
    assert ca.url_for(2024) == ("https://www.nseindia.com/api/corporates-corporateActions?index=equities"
                                "&from_date=01-01-2024&to_date=31-12-2024")
    body, n = ca.to_csv(fixture("2024_sample.json"))
    df = pd.read_csv(pd.io.common.BytesIO(body), dtype=str, keep_default_na=False)
    assert n == 4 and list(df.columns) == ["SYMBOL", "COMPANY NAME", "SERIES", "PURPOSE", "FACE VALUE", "EX-DATE",
                                           "RECORD DATE", "BOOK CLOSURE START DATE", "BOOK CLOSURE END DATE", "ISIN"]
    rel = df[df["SYMBOL"] == "RELIANCE"].iloc[0]
    assert (rel["PURPOSE"], rel["EX-DATE"], rel["ISIN"], rel["SERIES"]) == ("Bonus 1:1", "28-Oct-2024", "INE002A01018",
                                                                            "EQ")
    assert ca.to_csv(b"[]") == (b"SYMBOL,COMPANY NAME,SERIES,PURPOSE,FACE VALUE,EX-DATE,RECORD DATE,BOOK CLOSURE START "
                                b"DATE,BOOK CLOSURE END DATE,ISIN\n", 0)
    for bad, msg in ((b"<html>", "not JSON"), (b'{"data": []}', "keys data"), (b'[{"symbol": "X"}]', "rows lack")):
        with pytest.raises(fetch.FetchError, match=msg):
            ca.to_csv(bad)


def test_saved_file_is_read_by_the_build_parser(tmp_path):
    f = str(tmp_path)
    c = client({ca.url_for(2024): Resp(200, fixture("2024_sample.json"))})
    assert ca.fetch_years(f, 2024, 2024, c, today_year=2026, log=lambda *_: None)[0][:3] == (2024, "saved", 4)
    rows = adj.read_corp_actions(f)
    assert set(rows["symbol"]) == {"RELIANCE", "DRREDDY", "GPIL", "MITTAL"}
    assert rows.set_index("symbol").loc["DRREDDY", "isin"] == "INE089A01023"
    assert not os.path.exists(saved(f, 2024) + ".part")


def test_real_rows_parse_to_the_expected_factors():
    df = pd.read_csv(os.path.join(FIX, "sample.csv"), dtype=str, keep_default_na=False)
    got, factor = {}, {}
    for r in df.itertuples():                       # BAJFINANCE 2025: bonus and split on two rows of one day
        actions, unparsed = adj.parse_purpose(r.PURPOSE)
        got[(r.SYMBOL, r._6, r.PURPOSE)] = (actions, unparsed)
        for _, f in actions:
            factor[(r.SYMBOL, r._6)] = round(factor.get((r.SYMBOL, r._6), 1) * f, 6)
    assert factor[("TATASTEEL", "28-Jul-2022")] == 10
    assert factor[("RELIANCE", "07-Sep-2017")] == 2 and factor[("RELIANCE", "28-Oct-2024")] == 2
    assert factor[("BAJFINANCE", "08-Sep-2016")] == 10                       # bonus 1:1 + split 10 -> 2
    assert factor[("BAJFINANCE", "16-Jun-2025")] == 10                       # bonus 4:1 x split 2 -> 1
    assert factor[("MINDACORP", "05-Jan-2015")] == 10
    assert factor[("NTPC", "19-Mar-2019")] == 1.2
    assert ("NTPC", "20-Mar-2015") not in factor and ("TVSMOTOR", "25-Aug-2025") not in factor   # not equity
    assert ("RELIANCE", "08-May-2015") not in factor                         # dividend
    assert not any(u for _, u in got.values())


def test_skip_past_years_force_and_current_year(tmp_path):
    f = str(tmp_path)
    answers = {ca.url_for(y): Resp(200, fixture("2024_sample.json")) for y in (2024, 2025, 2026)}
    c = client(answers)
    res = ca.fetch_years(f, 2024, 2026, c, today_year=2026, log=lambda *_: None)
    assert [r[1] for r in res] == ["saved", "saved", "saved"]
    c = client(answers)
    res = ca.fetch_years(f, 2024, 2026, c, today_year=2026, log=lambda *_: None)
    assert [r[1] for r in res] == ["skipped", "skipped", "saved"]            # the current year is always refreshed
    assert c.session.calls == [ca.url_for(2026)]
    c = client(answers)
    res = ca.fetch_years(f, 2024, 2025, c, force=True, today_year=2026, log=lambda *_: None)
    assert [r[1] for r in res] == ["saved", "saved"]


def test_block_falls_back_to_the_browser(tmp_path):
    f = str(tmp_path)
    url = ca.url_for(2023)
    browser = FakeBrowser({url: (200, fixture("2024_sample.json"))})
    c = client({url: Resp(200, fixture("blocked.html"))}, browser)
    res = ca.fetch_years(f, 2023, 2023, c, today_year=2026, log=lambda *_: None)
    assert res[0][1] == "saved" and browser.calls == [url] and os.path.isfile(saved(f, 2023))


def test_blocked_without_browser_prints_the_drop_in(tmp_path, capsys):
    f = str(tmp_path)
    url = ca.url_for(2023)
    c = client({url: Resp(403)}, use_browser=False)
    assert cli.fetch_actions(f, 2023, 2023, False, False, 2026, client=c) == 1
    err = capsys.readouterr().err
    assert ca.PAGE in err and "From 01-01-2023 To 31-12-2023" in err and saved(f, 2023) in err
    assert not os.path.exists(saved(f, 2023))
    c = client({url: Resp(403)}, FakeBrowser(dead="Playwright is not installed"))
    assert cli.fetch_actions(f, 2023, 2023, False, True, 2026, client=c) == 1
    assert "Playwright is not installed" in capsys.readouterr().err
    assert os.listdir(os.path.join(f, "adjustments", "input")) == []


def test_lock_and_cli_codes(tmp_path, monkeypatch, capsys):
    f = str(tmp_path)
    lock = RunLock(os.path.join(f, "adjustments", ".lock")).acquire("adjustments.py build")
    monkeypatch.setattr(cli, "NseClient", lambda use_browser: client())
    try:
        assert cli.main(["fetch-actions", "--folder", f, "--from", "2024", "--to", "2024"]) == 2
    finally:
        lock.release()
    monkeypatch.setattr(cli, "NseClient", lambda use_browser: client({ca.url_for(2024): Resp(200, b"[]")}))
    assert cli.main(["fetch-actions", "--folder", f, "--from", "2024", "--to", "2024"]) == 0
    assert "saved 1, skipped 0, failed 0" in capsys.readouterr().out
    year = ca.this_year()
    for argv in (["fetch-actions", "--from", "2014"], ["fetch-actions", "--to", str(year + 1)],
                 ["fetch-actions", "--from", "2020", "--to", "2019"]):
        with pytest.raises(SystemExit) as e:
            cli.main(argv)
        assert e.value.code == 2

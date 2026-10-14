"""nse_reference.py and backfill/nse_ref: parsers on saved NSE samples (tests/fixtures/nse_reference/),
the fetcher with fake sessions / browsers, the raw cache and the CLI. No test reaches NSE."""
import csv
import json
import os
import shutil
from datetime import date

import pytest
import requests

import nse_reference
from backfill.nse_ref import circulars, compulsory, fetch, pdftext, run, suspensions, voluntary
from backfill.nse_ref.common import name_key, parse_date
from backfill.nse_ref.fetch import Cache, FetchError, NotFound, NseClient, Throttle, safe_name

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "nse_reference")


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def refuse(*a, **k):
        raise AssertionError("a test tried to reach the network")
    monkeypatch.setattr(requests.Session, "request", refuse)
    monkeypatch.setattr(fetch, "launch_browser", refuse)


def read(name):
    with open(os.path.join(FIX, name), "rb") as f:
        return f.read()


def docs():
    with open(os.path.join(FIX, "docs.json"), encoding="utf-8") as f:
        return json.load(f)


def sample(name):
    """(entry, doc) of a sample circular from docs.json."""
    d = docs()[name]
    return circulars.entries(json.dumps({"data": [d["entry"]]}).encode())[0], d["doc"]


def fixture_entries():
    return circulars.entries(read("circulars_2015.json"))


def lookup():
    rows = voluntary.parse(read("delisted.xlsx"), "delisted.xlsx", "delisted")
    return {name_key(r["company"]): r["symbol"] for r in rows}


def read_csv(path):
    with open(path, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


# --- dates and names -----------------------------------------------------------------------------

@pytest.mark.parametrize("text, expected", [
    ("March 09, 2023", date(2023, 3, 9)), ("October 17,2019", date(2019, 10, 17)),
    ("December\n24, 2015", date(2015, 12, 24)), ("26- Jun-2025", date(2025, 6, 26)),
    ("09-Oct-2026", date(2026, 10, 9)), ("17th October 2019", date(2019, 10, 17)),
    ("20150407", date(2015, 4, 7)), ("2024-08-12", date(2024, 8, 12)), ("Sept 5, 2016", date(2016, 9, 5)),
    ("no date here", None), ("February 30, 2020", None)])
def test_parse_date(text, expected):
    assert parse_date(text) == expected


def test_name_key_matches_ltd_and_punctuation():
    assert name_key("Moser-Baer (I) Ltd.") == name_key("Moser Baer (India) Limited")
    assert name_key("The Sirpur Paper Mills Limited") == name_key("Sirpur Paper Mills Ltd")


# --- voluntary: the Excel lists ------------------------------------------------------------------

def test_excel_links_from_the_delisting_page():
    links = voluntary.excel_links(read("delisting_page.html").decode())
    assert links["delisted"].endswith(".xlsx") and "List_of_delisted" in links["delisted"]
    assert "Companies_proposed" in links["proposed"]


def test_delisted_list_filters_by_date_and_merges_sheets():
    rows = voluntary.parse(read("delisted.xlsx"), "d.xlsx", "delisted", since=date(2015, 1, 1))
    syms = [r["symbol"] for r in rows]
    assert "CABOTINDIA" not in syms                    # delisted in 2002
    assert "MANJUSHREE" in syms and len(syms) == len(set(syms))
    sec = next(r for r in rows if r["symbol"] == "SECURCRED")
    assert sec["delisting_date"] == "2026-10-16" and sec["delisting_type"] == "Compulsory Delisting"
    assert sec["initial_notice_date"] == "2025-08-30"   # from the second sheet
    assert sec["isin"] == "INE195Y01010" and sec["list"] == "delisted" and sec["needs_review"] == ""
    assert not any(r["symbol"] == "Notes:" for r in rows)


def test_proposed_list():
    rows = voluntary.parse(read("proposed.xlsx"), "p.xlsx", "proposed")
    assert [(r["symbol"], r["company"], r["list"]) for r in rows] == [
        ("ARCOTECH", "Arcotech Limited", "proposed"), ("ATNINTER", "ATN International Limited", "proposed")]


# --- compulsory: orders of delisting -------------------------------------------------------------

def test_orders_page_and_symbols():
    entries = compulsory.parse_page(read("orders_page.html").decode())
    assert [e[0] for e in entries] == ["09-Oct-2026", "09-Oct-2026", "26- Jun-2025", "25-Aug-2016"]
    names = {name_key("Ponni Sugars (Orissa) Limited"): "PONNISUGAR", **lookup()}
    texts = {entries[2][2]: "In the matter of the Company: S.S. Infrastructure Development Consultants Limited (SSINFRA)"}
    rows = compulsory.build(entries, date(2015, 1, 1), names, lambda u: texts.get(u), "orders.html")
    by = {r["company"]: r for r in rows}
    sec = by["SecUR Credentials Limited"]
    assert (sec["symbol"], sec["symbol_source"], sec["order_date"], sec["needs_review"]) == \
        ("SECURCRED", "delisted list", "2026-10-09", "")
    ss = by["S.S. Infrastructure Development Consultants Limited"]
    assert (ss["symbol"], ss["symbol_source"], ss["order_date"]) == ("SSINFRA", "order PDF", "2025-06-26")
    assert ss["source_file"] == safe_name(entries[2][2])
    su = by["Suumaya Industries Limited"]   # not in the list, PDF not available
    assert su["symbol"] == "" and su["needs_review"] == "Y" and "order PDF not downloaded" in su["review_note"]
    po = by["Ponni Sugars (Orrisa) Limited"]   # NSE's typo: close match, flagged
    assert po["symbol"] == "PONNISUGAR" and po["needs_review"] == "Y" and "close name" in po["review_note"]
    assert len(compulsory.build(entries, date(2017, 1, 1), names, lambda u: None)) == 3


def test_symbol_from_a_real_order_pdf_text():
    text = docs()["order_SECURCRED"]["doc"]["text"]
    assert compulsory.symbol_from_order(text, "SecUR Credentials Limited") == "SECURCRED"


# --- circular lists ------------------------------------------------------------------------------

def test_year_list_names():
    today = date(2026, 10, 14)
    url, name = circulars.year_list(2019, today)
    assert name == "circulars_2019.json" and "fromDate=01-01-2019&toDate=31-12-2019" in url
    url, name = circulars.year_list(2026, today)
    assert name == "circulars_2026_fetched_2026-10-14.json" and "toDate=14-10-2026" in url


def test_pick_keeps_equity_and_skips_the_rest():
    delist, susp, skipped = circulars.pick(fixture_entries(), date(2015, 1, 1))
    assert [e["no"] for e in delist] == ["NSE/CML/31318", "NSE/CML/42284"]
    assert [e["no"] for e in susp] == ["NSE/CML/29381", "NSE/CML/31302"]
    assert skipped == {"delisting: Dissemination Board": 1, "suspension: not equity (debt, units, funds)": 2,
                       "suspension: revocation / cancellation": 1, "suspension: member suspension": 1}
    delist, susp, _ = circulars.pick(fixture_entries(), date(2016, 1, 1))
    assert [e["no"] for e in delist] == ["NSE/CML/42284"] and susp == []


def test_pick_one_row_per_circular_number():
    e = fixture_entries()
    twice = e + [dict(e[0], dept="Emerge")]
    assert len(circulars.pick(twice, None)[1]) == 2


# --- delisting circulars (real PDFs) -------------------------------------------------------------

def entry(no):
    return next(e for e in fixture_entries() if e["no"] == no)


def test_delisting_circular_with_doubled_bold_text():
    doc = pdftext.extract(read("CML31318.pdf"))
    assert "NNNN" not in doc["text"]
    rows = circulars.parse_delisting(entry("NSE/CML/31318"), doc, {})
    assert len(rows) == 1
    r = rows[0]
    assert (r["symbol"], r["company"], r["effective_date"], r["delisting_type"], r["needs_review"]) == \
        ("ESSARPORTS", "Essar Ports Limited", "2015-12-31", "voluntary", "")   # delisted date, not the suspension
    assert r["circular_date"] == "2015-12-10" and r["source_file"] == "CML31318.pdf"


def test_delisting_circular_with_nine_companies():
    rows = circulars.parse_delisting(entry("NSE/CML/42284"), pdftext.extract(read("CML42284.pdf")), {})
    assert len(rows) == 9
    assert {r["effective_date"] for r in rows} == {"2019-10-17"}
    assert {r["delisting_type"] for r in rows} == {"liquidation"}
    moser = next(r for r in rows if r["symbol"] == "MOSERBAER")
    assert moser["company"] == "Moser-Baer (I) Limited" and moser["needs_review"] == ""


@pytest.mark.parametrize("name, sym, kind, eff", [
    ("CML64876.pdf", "JINDCOT", "compulsory", "2024-11-11"), ("CML63184.pdf", "BIRLATYRE", "ibc", "2024-08-08"),
    ("CML63342.pdf", "SBIHOMEFIN", "other", "2024-08-12")])
def test_delisting_circular_samples(name, sym, kind, eff):
    e, doc = sample(name)
    [r] = circulars.parse_delisting(e, doc, {})
    assert (r["symbol"], r["delisting_type"], r["effective_date"], r["needs_review"]) == (sym, kind, eff, "")


def test_scanned_or_missing_pdf_needs_review():
    e = entry("NSE/CML/31318")
    [r] = circulars.parse_delisting(e, {"text": "", "tables": [], "pages": 1}, {})
    assert r["symbol"] == "ESSARPORTS" and r["needs_review"] == "Y" and "scan" in r["review_note"]
    [r] = circulars.parse_delisting(e, None, {})
    assert r["needs_review"] == "Y" and "not downloaded" in r["review_note"]


# --- suspension circulars: mergers vs other ------------------------------------------------------

def test_merger_from_the_pdf_text():
    e, doc = sample("CML64338.pdf")
    m, o = suspensions.parse(e, doc, {})
    assert o == [] and len(m) == 1
    r = m[0]
    assert (r["old_symbol"], r["old_company"], r["suspended_from"], r["last_trading_date"]) == \
        ("IDFC", "IDFC Limited", "2024-10-10", "2024-10-09")
    assert r["reason_text"] == "Scheme of Amalgamation"
    assert r["needs_review"] == "Y" and r["review_note"] == "swap ratio not in NSE circular"


def test_merger_names_the_new_company_when_the_circular_does():
    m, _ = suspensions.parse(entry("NSE/CML/29381"), pdftext.extract(read("CML29381.pdf")), {})
    assert (m[0]["old_symbol"], m[0]["new_company"]) == ("INGVYSYABK", "Kotak Mahindra Bank Limited")
    assert m[0]["last_trading_date"] == "2015-04-15"


@pytest.mark.parametrize("name", ["CML55885.pdf", "CML41407.pdf", "CML60172.pdf"])
def test_more_merger_samples(name):
    m, o = suspensions.parse(*sample(name), {})
    assert len(m) == 1 and o == [] and m[0]["old_symbol"] and m[0]["last_trading_date"]


@pytest.mark.parametrize("name, sym, reason, eff, review", [
    ("CML55931.pdf", "MERCATOR", "liquidation", "2023-03-13", ""),          # reason in the text, not the table
    ("CML56130.pdf", "AMJUMBO", "not responding to the exchange", "2023-03-27", ""),
    ("CML65533.pdf", "GRASIMPP1", "partly paid call", "2024-12-13", ""),
    ("CML62460.pdf", "JPINFRATEC", "IBC / resolution plan", "", "Y"),       # 'currently suspended': no date
    ("CML39914.pdf", "", "non-compliance with listing rules", "2019-02-01", "Y")])  # no table, no symbol
def test_other_suspensions(name, sym, reason, eff, review):
    m, o = suspensions.parse(*sample(name), {})
    assert m == [] and len(o) == 1
    r = o[0]
    assert (r["symbol"], r["reason"], r["effective_date"], r["needs_review"]) == (sym, reason, eff, review)
    assert r["reason_text"] and len(r["reason_text"]) <= 206


def test_suspension_text_only_takes_the_company_from_the_text():
    [r] = suspensions.parse(*sample("CML39914.pdf"), {})[1]
    assert r["company"] == "Raj Rayon Industries Limited" and "symbol missing" in r["review_note"]
    [r] = suspensions.parse(*sample("CML39914.pdf"), {name_key("Raj Rayon Industries Ltd"): "RAJRAYON"})[1]
    assert r["symbol"] == "RAJRAYON"


def test_multi_company_suspension_gets_symbols_from_the_delisted_list():
    _, o = suspensions.parse(entry("NSE/CML/31302"), pdftext.extract(read("CML31302.pdf")), lookup())
    assert [(r["symbol"], r["company"]) for r in o] == [("BRANDHOUSE", "Brandhouse Retails Limited"),
                                                        ("KRISHNAENG", "Krishna Engineering Works Limited")]
    assert all(r["needs_review"] == "" and r["review_note"] == "symbol looked up by company name" for r in o)
    assert {r["reason"] for r in o} == {"non-compliance with listing rules"}


def text_entry(subject):
    return {"no": "NSE/CML/1", "date": date(2020, 1, 2), "dept": "Listing", "subject": subject,
            "url": "https://nsearchives.nseindia.com/content/circulars/CML1.pdf"}


def test_merger_decided_by_text_not_subject():
    text = ("Sub: Suspension of trading in equity shares - Alpha Limited (ALPHA)\nIn view of the Scheme of "
            "Amalgamation of Alpha Limited with Beta Limited (BETA), 3 equity shares of Beta Limited for every 10 "
            "equity shares of Alpha Limited, trading will be suspended w.e.f. January 10, 2020 (i.e. closing hours of "
            "trading on January 09, 2020).")
    m, o = suspensions.parse(text_entry("Suspension of trading in equity shares - Alpha Limited (ALPHA)"),
                             {"text": text, "tables": [], "pages": 1}, {})
    assert o == [] and len(m) == 1
    r = m[0]
    assert (r["old_symbol"], r["new_company"], r["new_symbol"], r["swap_ratio"], r["new_per_old"]) == \
        ("ALPHA", "Beta Limited", "BETA", "3 for 10", "0.3")
    assert (r["suspended_from"], r["last_trading_date"], r["needs_review"]) == ("2020-01-10", "2020-01-09", "")


def test_merger_with_liquidation_or_two_ratios_needs_review():
    text = ("Sub: Suspension of trading - Gamma Limited\nPursuant to the resolution plan, the Scheme of "
            "Amalgamation with Delta Limited gives 1 equity share for every 4 equity shares, or 2 equity shares "
            "for every 5 equity shares, w.e.f. May 04, 2021 (i.e. closing hours of trading on May 03, 2021).")
    m, _ = suspensions.parse(text_entry("Suspension of trading - Gamma Limited"),
                             {"text": text, "tables": [], "pages": 1}, {})
    assert m[0]["needs_review"] == "Y" and m[0]["swap_ratio"] == ""
    assert "IBC / resolution plan" in m[0]["review_note"] and "1 for 4, 2 for 5" in m[0]["review_note"]


def test_unknown_reason_needs_review():
    text = "Sub: Suspension of trading - Omega Limited (OMEGA)\nTrading will be suspended w.e.f. May 04, 2021."
    _, [r] = suspensions.parse(text_entry("Suspension of trading - Omega Limited (OMEGA)"),
                               {"text": text, "tables": [], "pages": 1}, {})
    assert r["symbol"] == "OMEGA" and r["reason"] == "" and r["needs_review"] == "Y"
    assert "reason not recognised" in r["review_note"]


def test_ratios_and_per_old():
    assert suspensions.ratios("42 fully paid-up equity shares of face value Re 1 each of HDFC Bank Limited for "
                              "every 25 fully paid up equity shares of HDFC") == ["42 for 25"]
    assert suspensions.per_old("42 for 25") == "1.68" and suspensions.per_old("1:1") == "1"
    assert suspensions.per_old("") == "" and suspensions.per_old("5 for 0") == ""


# --- manual_mergers.csv --------------------------------------------------------------------------

def test_manual_mergers_file_and_apply(tmp_path):
    path = suspensions.ensure_manual(str(tmp_path))
    with open(path, encoding="utf-8") as f:
        assert f.read() == ("old_symbol,new_company,new_symbol,swap_ratio,scheme_type,exit_expected,note\n"
                            "HDFC,HDFC Bank Limited,HDFCBANK,42 for 25,,,\n")
    suspensions.ensure_manual(str(tmp_path))   # never overwritten
    with open(path, "a", encoding="utf-8") as f:
        f.write("IDFC,IDFC First Bank Limited,IDFCFIRSTB,155 for 100,,,\n")
    m, _ = suspensions.parse(*sample("CML64338.pdf"), {})
    missing = suspensions.apply_manual(m, suspensions.read_manual(path))
    r = m[0]
    assert (r["new_company"], r["new_symbol"], r["swap_ratio"], r["new_per_old"]) == \
        ("IDFC First Bank Limited", "IDFCFIRSTB", "155 for 100", "1.55")
    assert r["needs_review"] == "" and r["review_note"].startswith("filled from manual_mergers.csv")
    assert (r["scheme_type"], r["exit_expected"]) == ("MERGER", "Y")   # no override: parsed values stay
    assert missing == ["HDFC"]


def scheme_row(reason="Scheme of Arrangement"):
    e = {"no": "NSE/CML/34627", "date": date(2017, 4, 13), "dept": "Listing",
         "subject": "Suspension of trading in equity shares - Cairn India Limited",
         "url": "https://nsearchives.nseindia.com/content/circulars/CML34627.pdf"}
    text = ("Sub: Suspension of trading in equity shares - Cairn India Limited\nIt is notified that it will be suspended "
            "from trading w.e.f April 26, 2017(i.e. closing hours of trading on April 25, 2017).")
    tab = [[["Sr. No.", "Name of the Company", "Symbol", "Series", "Reason for Suspension"],
            ["1", "Cairn India Limited", "CAIRN", "EQ", reason]]]
    m, _ = suspensions.parse(e, {"text": text, "tables": tab}, {})
    return m


def test_manual_overrides_scheme_type_and_exit_expected(tmp_path):
    m = scheme_row()
    assert (m[0]["scheme_type"], m[0]["exit_expected"], m[0]["needs_review"]) == ("OTHER", "N", "Y")
    path = tmp_path / "manual_mergers.csv"
    path.write_text("old_symbol,new_company,new_symbol,swap_ratio,scheme_type,exit_expected,note\n"
                    "CAIRN,Vedanta Limited,VEDL,,merger,y,Cairn India amalgamated into Vedanta\n", encoding="utf-8")
    suspensions.apply_manual(m, suspensions.read_manual(str(path)))
    r = m[0]
    assert (r["new_company"], r["new_symbol"], r["swap_ratio"], r["scheme_type"], r["exit_expected"]) == \
        ("Vedanta Limited", "VEDL", "", "MERGER", "Y")
    assert r["needs_review"] == "" and "note" not in r and "scheme type unclear" in r["review_note"]


@pytest.mark.parametrize("stype, exit_, ok", [("MERGER+DEMERGER", "N", True), ("CAPITAL_REDUCTION", "", True),
                                              ("SPLIT", "Y", False), ("MERGER", "maybe", False)])
def test_manual_override_values_are_checked(tmp_path, stype, exit_, ok):
    m = scheme_row()
    path = tmp_path / "manual_mergers.csv"
    path.write_text("old_symbol,new_company,new_symbol,swap_ratio,scheme_type,exit_expected,note\n"
                    f"CAIRN,Vedanta Limited,VEDL,,{stype},{exit_},\n", encoding="utf-8")
    suspensions.apply_manual(m, suspensions.read_manual(str(path)))
    r = m[0]
    assert (r["needs_review"] == "") == ok
    if ok:
        assert r["scheme_type"] == stype and r["exit_expected"] == (exit_ or "N")
    else:
        assert "not understood" in r["review_note"]


def test_old_four_column_manual_file_still_works(tmp_path):
    m = scheme_row("Scheme of Amalgamation")
    path = tmp_path / "manual_mergers.csv"
    path.write_text("old_symbol,new_company,new_symbol,swap_ratio\nCAIRN,Vedanta Limited,VEDL,1 for 1\n", encoding="utf-8")
    suspensions.apply_manual(m, suspensions.read_manual(str(path)))
    r = m[0]
    assert (r["new_symbol"], r["new_per_old"], r["scheme_type"], r["exit_expected"], r["needs_review"]) == \
        ("VEDL", "1", "MERGER", "Y", "")


# --- fetching ------------------------------------------------------------------------------------

class Resp:
    def __init__(self, status, content):
        self.status_code, self.content = status, content
        self.ok = 200 <= status < 300


class FakeSession:
    def __init__(self, answers):
        self.answers, self.calls, self.headers = answers, [], {}

    def get(self, url, timeout=None):
        self.calls.append(url)
        a = self.answers.get(url, Resp(404, b""))
        if isinstance(a, Exception):
            raise a
        return a


class FakeBrowser:
    def __init__(self, answers):
        self.answers, self.calls, self.dead = answers, [], ""

    def get(self, url):
        self.calls.append(url)
        return self.answers.get(url, (404, b""))

    def close(self):
        pass


class Clock:
    def __init__(self):
        self.t, self.slept = 100.0, []

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.slept.append(s)
        self.t += s


PDF = b"%PDF-1.4 fake"


def client(answers, browser=None, use_browser=True):
    clock = Clock()
    s = FakeSession(answers)
    c = NseClient(session=s, throttle=Throttle(2.0, clock.sleep, clock), browser=browser or FakeBrowser({}),
                  use_browser=use_browser, log=lambda *a: None)
    return c, s, clock


def test_home_page_first_and_two_seconds_apart():
    c, s, clock = client({fetch.HOME: Resp(403, b"denied"), "u1": Resp(200, PDF), "u2": Resp(200, PDF)})
    assert c.get("u1", "pdf") == PDF and c.get("u2", "pdf") == PDF
    assert s.calls == [fetch.HOME, "u1", "u2"]
    assert clock.slept == [2.0, 2.0] and c.requests == 3


def test_blocked_request_switches_to_the_browser():
    b = FakeBrowser({"u1": (200, PDF), "u2": (200, b"{}")})
    c, s, _ = client({"u1": Resp(403, b"<html>denied</html>"), "u2": Resp(200, b"{}")}, b)
    assert c.get("u1", "pdf") == PDF
    assert c.get("u2", "json") == b"{}"   # every later request goes through the browser
    assert b.calls == ["u1", "u2"] and "u2" not in s.calls


def test_page_instead_of_the_file_counts_as_blocked():
    b = FakeBrowser({"u1": (200, PDF)})
    c, _, _ = client({"u1": Resp(200, b"<html>Access Denied</html>")}, b)
    assert c.get("u1", "pdf") == PDF and b.calls == ["u1"]


def test_network_errors_then_browser_and_404():
    b = FakeBrowser({"u1": (200, PDF)})
    c, s, _ = client({"u1": requests.ConnectionError("reset"), "gone": Resp(404, b"")}, b)
    with pytest.raises(NotFound):
        c.get("gone", "pdf")
    assert b.calls == []   # a 404 is not a block
    assert c.get("u1", "pdf") == PDF and s.calls.count("u1") == fetch.TRIES


def test_no_browser_and_browser_failures():
    c, _, _ = client({"u1": Resp(403, b"")}, use_browser=False)
    with pytest.raises(FetchError, match="browser fallback is off"):
        c.get("u1", "pdf")
    c, _, _ = client({"u1": Resp(403, b"")}, FakeBrowser({"u1": (200, b"<html>")}))
    with pytest.raises(FetchError, match="a page instead"):
        c.get("u1", "pdf")


def test_browser_that_cannot_start_is_not_retried():
    starts = []

    def launch():
        starts.append(1)
        raise FetchError("no chromium")
    b = fetch.BrowserFetcher(launch)
    c, _, _ = client({"u1": Resp(403, b""), "u2": Resp(403, b"")}, b)
    for u in ("u1", "u2"):
        with pytest.raises(FetchError, match="no chromium"):
            c.get(u, "pdf")
    assert starts == [1]


class FakeClient:
    """NseClient stand-in serving fixture bytes by URL."""

    def __init__(self, answers):
        self.answers, self.calls, self.requests = answers, [], 0

    def get(self, url, kind):
        self.calls.append(url)
        self.requests += 1
        if url not in self.answers:
            raise NotFound("404 not found")
        a = self.answers[url]
        if isinstance(a, Exception):
            raise a
        return a

    def close(self):
        pass


def test_cache_never_downloads_twice_and_takes_files_put_by_hand(tmp_path):
    fc = FakeClient({"https://x/a.pdf": PDF})
    cache = Cache(str(tmp_path), fc, log=lambda *a: None)
    assert cache.get("https://x/a.pdf", "circulars", "pdf") == PDF
    assert cache.get("https://x/a.pdf", "circulars", "pdf") == PDF
    assert fc.calls == ["https://x/a.pdf"] and cache.downloads == 1
    assert not [f for f in os.listdir(tmp_path / "circulars") if f.endswith(".part")]
    (tmp_path / "circulars" / "b.pdf").write_bytes(b"%PDF by hand")
    assert cache.get("https://x/b.pdf", "circulars", "pdf") == b"%PDF by hand" and len(fc.calls) == 1
    assert cache.get("https://x/c.pdf", "circulars", "pdf", what="circular c") is None
    assert cache.not_found == [("circular c", "https://x/c.pdf")] and cache.failures == []
    fc.answers["https://x/d.pdf"] = FetchError("HTTP 503")
    assert cache.get("https://x/d.pdf", "circulars", "pdf", what="circular d") is None
    assert cache.failures == [("circular d", "https://x/d.pdf", str(tmp_path / "circulars" / "d.pdf"), "HTTP 503")]
    offline = Cache(str(tmp_path), None, offline=True)
    assert offline.get("https://x/e.pdf", "circulars", "pdf") is None and len(offline.failures) == 1


def test_safe_name():
    assert safe_name("https://nsearchives.nseindia.com/web/x/Delisting%20Order_SSINFRA_0.pdf") == \
        "Delisting_Order_SSINFRA_0.pdf"
    assert safe_name("https://www.nseindia.com/api/circulars?fromDate=01-01-2015") == "circulars"


# --- whole runs ----------------------------------------------------------------------------------

TODAY = date(2015, 12, 31)


def online_answers():
    page = read("delisting_page.html").decode()
    links = voluntary.excel_links(page)
    url15, _ = circulars.year_list(2015, TODAY)
    ans = {voluntary.PAGE_URL: page.encode(), links["delisted"]: read("delisted.xlsx"),
           links["proposed"]: read("proposed.xlsx"), compulsory.PAGE_URL: read("orders_page.html"),
           url15: read("circulars_2015.json")}
    for n in ("CML29381.pdf", "CML31302.pdf", "CML31318.pdf", "CML42284.pdf"):
        ans["https://nsearchives.nseindia.com/content/circulars/" + n] = read(n)
    return ans


def test_run_downloads_once_and_writes_every_file(tmp_path):
    fc = FakeClient(online_answers())
    s = run.collect(str(tmp_path), client=fc, log=lambda *a: None, today=TODAY)
    ref = tmp_path / "NSE_CM_bhav" / "reference"
    assert set(s["files"]) == set(run.FILES.values())
    for name in run.FILES.values():
        assert (ref / name).exists()
    assert (ref / "manual_mergers.csv").exists()
    assert (ref / "raw" / "circular_lists" / "circulars_2015_fetched_2015-12-31.json").exists()
    assert (ref / "raw" / "pages" / "orders_of_delisting_fetched_2015-12-31.html").exists()
    assert (ref / "raw" / "circulars" / "CML42284.pdf").exists()
    # order PDFs: SecUR is matched by name; the three others are not on the fake server (404)
    assert len(s["not_found"]) == 3 and s["failures"] == []
    first = s["downloads"]
    calls = len(fc.calls)
    s2 = run.collect(str(tmp_path), client=fc, log=lambda *a: None, today=TODAY)
    assert s2["downloads"] == 0 and first == 9   # 2 pages, 2 Excel lists, 1 circular list, 4 PDFs
    assert len(fc.calls) - calls == 3   # only the 404 order PDFs are asked again
    mergers = read_csv(ref / "mergers.csv")
    assert [r["old_symbol"] for r in mergers] == ["INGVYSYABK"]
    assert len(read_csv(ref / "delisting_circulars.csv")) == 10
    susp = read_csv(ref / "suspensions.csv")
    assert [r["symbol"] for r in susp] == ["BRANDHOUSE", "KRISHNAENG"]
    assert list(susp[0]) == suspensions.SUSPENSION_COLUMNS


def test_next_day_fetches_pages_again_but_not_documents(tmp_path):
    fc = FakeClient(online_answers())
    run.collect(str(tmp_path), client=fc, log=lambda *a: None, today=TODAY)
    fc.calls.clear()
    nxt = date(2016, 1, 1)
    url15, _ = circulars.year_list(2015, nxt)
    url16, _ = circulars.year_list(2016, nxt)
    fc.answers[url15] = read("circulars_2015.json")
    fc.answers[url16] = b'{"data": []}'
    s = run.collect(str(tmp_path), client=fc, log=lambda *a: None, today=nxt)
    pdfs = [u for u in fc.calls if "/content/circulars/" in u]
    assert pdfs == [] and url15 in fc.calls and url16 in fc.calls
    assert s["failures"] == []


def offline_raw(folder):
    raw = os.path.join(folder, "NSE_CM_bhav", "reference", "raw")
    links = voluntary.excel_links(read("delisting_page.html").decode())
    files = {("pages", "delisting_of_companies_fetched_2015-12-30.html"): "delisting_page.html",
             ("pages", "orders_of_delisting_fetched_2015-12-30.html"): "orders_page.html",
             ("delisting", safe_name(links["delisted"])): "delisted.xlsx",
             ("delisting", safe_name(links["proposed"])): "proposed.xlsx",
             ("circular_lists", "circulars_2015_fetched_2015-12-30.json"): "circulars_2015.json"}
    for n in ("CML29381.pdf", "CML31302.pdf", "CML31318.pdf", "CML42284.pdf"):
        files[("circulars", n)] = n
    for (sub, name), src in files.items():
        os.makedirs(os.path.join(raw, sub), exist_ok=True)
        shutil.copy(os.path.join(FIX, src), os.path.join(raw, sub, name))


def test_cli_offline_builds_from_raw_and_lists_what_to_download(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(run, "today_ist", lambda: TODAY)
    monkeypatch.setattr(nse_reference, "data_dir", lambda: str(tmp_path))
    offline_raw(str(tmp_path))
    code = nse_reference.main(["--offline"])
    out, err = capsys.readouterr()
    ref = tmp_path / "NSE_CM_bhav" / "reference"
    assert code == 1   # the order PDFs are not in raw/
    assert "Download by hand" in err and str(ref / "raw" / "orders") in err
    assert "Downloaded 0 new file(s)" in out
    vol = read_csv(ref / "voluntary_delisting.csv")
    assert {r["list"] for r in vol} == {"delisted", "proposed"}
    comp = read_csv(ref / "compulsory_delisting.csv")
    assert comp[0]["symbol"] == "SECURCRED" and comp[0]["source_file"] == "orders_of_delisting_fetched_2015-12-30.html"
    assert read_csv(ref / "mergers.csv")[0]["needs_review"] == "Y"
    # --offline twice gives the same files
    before = {n: (ref / n).read_bytes() for n in run.FILES.values()}
    nse_reference.main(["--offline"])
    assert {n: (ref / n).read_bytes() for n in run.FILES.values()} == before


def test_cli_only_and_bad_arguments(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(run, "today_ist", lambda: TODAY)
    offline_raw(str(tmp_path))
    assert nse_reference.main(["--folder", str(tmp_path), "--offline", "--only", "voluntary,suspensions"]) == 0
    ref = tmp_path / "NSE_CM_bhav" / "reference"
    assert sorted(os.listdir(ref)) == ["manual_mergers.csv", "mergers.csv", "raw", "suspensions.csv",
                                       "text_cache", "voluntary_delisting.csv"]
    with pytest.raises(SystemExit) as e:
        nse_reference.main(["--folder", str(tmp_path), "--only", "nope"])
    assert e.value.code == 1
    with pytest.raises(SystemExit) as e:
        nse_reference.main(["--folder", str(tmp_path), "--from", "2015-13-01"])
    assert e.value.code == 1
    assert nse_reference.main(["--folder", "relative/path", "--offline"]) == 1


# --- cases seen in the real 2015-2026 circulars --------------------------------------------------

@pytest.mark.parametrize("cell, symbols, note", [
    ("SB&TINTL", ["SB&TINTL"], ""), ("BLUEBIRD*", ["BLUEBIRD"], "footnote"), ("*OMNITECH", ["OMNITECH"], "footnote"),
    ("JVLAGRO #", ["JVLAGRO"], "footnote"), ("WINSOME DJ", ["WINSOMEDJ"], "wrapped"),
    ("Entegra", ["ENTEGRA"], "upper-cased"),
    ('"GUJNRECOKE" & "GUJNREDVR"', ["GUJNRECOKE", "GUJNREDVR"], "several symbols")])
def test_symbol_cells(cell, symbols, note):
    from backfill.nse_ref.common import symbols_in_cell
    got, info = symbols_in_cell(cell)
    assert got == symbols
    assert (note in "; ".join(info)) if note else info == []


def test_two_symbols_in_one_row_give_two_rows():
    doc = {"text": "Sub: Delisting of equity shares of Gujarat NRE Coke Limited\nis delisted w.e.f. May 04, 2021.",
           "tables": [[["Sr. No.", "Symbol", "Company Name"], ["1", '"GUJNRECOKE" & "GUJNREDVR"', "Gujarat NRE Coke Limited #"]]]}
    rows = circulars.parse_delisting(text_entry("Delisting of equity shares of Gujarat NRE Coke Limited"), doc, {})
    assert [(r["symbol"], r["company"]) for r in rows] == [("GUJNRECOKE", "Gujarat NRE Coke Limited"),
                                                           ("GUJNREDVR", "Gujarat NRE Coke Limited")]
    assert rows[0]["effective_date"] == "2021-05-04" and rows[0]["needs_review"] == ""


def test_subject_company_after_the_last_of():
    from backfill.nse_ref.common import company_from_subject
    assert company_from_subject("Compulsory delisting of equity shares of Vital Communication Limited") == \
        "Vital Communication Limited"


def test_stayed_delisting_and_misspelled_dissemination_board():
    text = "Sub : Delisting - Entegra Limited\nThe delisting has been put on hold till further notice."
    [r] = circulars.parse_delisting(text_entry("Delisting - Entegra Limited"), {"text": text, "tables": []}, {})
    assert r["delisting_type"] == "on hold"
    e = dict(text_entry("Removal of delisted Companies made available on Disseimination Board of NSE"), date=date(2021, 3, 5))
    assert circulars.pick([e], None)[0] == []


def test_numbered_company_list_without_a_table():
    text = ("Sub : Suspension of trading in Securities of Companies for Non-Compliance of certain\n"
            "clauses of the Listing Agreement.\nthe trading ... will be discontinued w.e.f March 31, 2015.\n"
            "Sr. No. Name of the Company\n1. Crest Animation Studios Limited.\n2. First Leasing Company of India\n"
            "Limited.\n3. Parekh Aluminex Limited.\nThis Circular shall be effective from March 31, 2015.")
    e = dict(text_entry("Suspension of trading in Securities of Companies for Non-Compliance"), date=date(2015, 3, 20))
    _, rows = suspensions.parse(e, {"text": text, "tables": []}, {name_key("Parekh Aluminex Ltd"): "PAREKHALUM"})
    assert [r["company"] for r in rows] == ["Crest Animation Studios Limited", "First Leasing Company of India Limited",
                                            "Parekh Aluminex Limited"]
    assert [r["symbol"] for r in rows] == ["", "", "PAREKHALUM"]
    assert rows[2]["needs_review"] == "" and rows[0]["needs_review"] == "Y"
    assert {r["effective_date"] for r in rows} == {"2015-03-31"}
    assert rows[2]["reason"] == "non-compliance with listing rules"   # only the subject says why


def test_company_list_in_a_sentence():
    from backfill.nse_ref.common import companies_in_text
    text = ("Sub : Suspension of trading\nIt is hereby informed that trading in securities of Bharati Shipyard Limited, "
            "JHS Svendgaard Laboratories Limited, Surya\nPharmaceutical Limited and Varun Shipping Company Limited "
            "will be suspended w.e.f. February 25, 2015")
    assert companies_in_text(text) == ["Bharati Shipyard Limited", "JHS Svendgaard Laboratories Limited",
                                       "Surya Pharmaceutical Limited", "Varun Shipping Company Limited"]


def test_learn_symbols_skips_guessed_ones():
    lookup = {}
    rows = [{"symbol": "AAA", "company": "Alpha Limited", "review_note": ""},
            {"symbol": "BBB", "company": "Beta Limited", "review_note": "symbol looked up by company name"},
            {"symbol": "CCC", "company": "Gamma Limited", "review_note": "symbol by close name match, check it"}]
    circulars.learn_symbols(rows, lookup)
    assert lookup == {name_key("Alpha Limited"): "AAA"}


# --- spot-check fixes (2026-10-15): status, cancels_circular, United Bank, debt-only circulars,
# corrigenda, scheme_type / exit_expected ---------------------------------------------------------

@pytest.mark.parametrize("subject", [
    "Suspension of trading in equity shares - United Bank of India", "Suspension of trading - United Spirits Limited",
    "Suspension of trading - United Breweries (Holdings) Limited", "Suspension of trading - Unitech Limited",
    "Suspension of trading - Unity Infraprojects Limited", "Suspension of trading - Optiemus Infracom Limited",
    "Suspension of trading - Reliance Industrial Infrastructure Limited", "Suspension of trading - Invicta Meditek Limited",
    "Suspension of trading - Governance Systems Limited"])
def test_company_names_never_trip_the_skip_rules(subject):
    assert circulars.skip_reason(subject, circulars.SUSPENSION_SKIPS) == ""
    assert circulars.skip_reason(subject.replace("Suspension of trading", "Delisting"), circulars.DELISTING_SKIPS) == ""


@pytest.mark.parametrize("subject", [
    "Suspension of trading in units - on account of redemption", "Suspension of trading in unites - on account of redemption",
    "Suspension of trading in bonds_IDFC Limited", "Suspension of trading in Commercial Papers - Tata Motors Finance Limited",
    "Suspension of trading in Exchange Traded Funds (ETFs) - Axis Asset Management Company Limited",
    "Suspension of Membership", "Cancellation of proposed suspension of Modi Rubber Limited"])
def test_non_stock_subjects_are_still_skipped(subject):
    assert circulars.skip_reason(subject, circulars.SUSPENSION_SKIPS) != ""


def dated_entry(no, day, subject):
    return {"no": no, "date": day, "dept": "Listing", "subject": subject,
            "url": f"https://nsearchives.nseindia.com/content/circulars/{no.rsplit('/', 1)[-1]}.pdf"}


def test_circular_that_only_points_to_debt_circulars_is_left_out():
    subjects = {"NSE/CML/67999": "Suspension of trading in Commercial Papers - Tata Motors Finance Limited",
                "NSE/CML/67997": "Suspension of trading in Non-Convertible Debentures (Privately Placed) - Tata Motors Finance Limited",
                "NSE/CML/68018": "Suspension of trading in Tata Motors Finance Limited pursuant to Scheme of Amalgamation"}
    text = ("Download Ref No: NSE/CML/68018 Date: May 15, 2025\nSub: Suspension of trading in Tata Motors Finance "
            "Limited pursuant to Scheme of Amalgamation\nThis is with reference to the Exchange Circulars No- NSE/CML/67999 "
            "and NSE/CML/67997 dated May 14, 2025.\nIn this regard, kindly note that the securities of Tata Motors Finance "
            "Limited as mentioned in the aforesaid Circulars have been suspended with effect from today i.e. May 15, 2025.")
    e = dated_entry("NSE/CML/68018", date(2025, 5, 15), subjects["NSE/CML/68018"])
    skipped = {}
    assert suspensions.parse(e, {"text": text, "tables": []}, {}, skipped, subjects) == ([], [])
    assert skipped == {"suspension: refers only to debt / unit circulars": 1}
    subjects["NSE/CML/67999"] = "Suspension of trading in equity shares - Tata Motors Finance Limited"
    m, _ = suspensions.parse(e, {"text": text, "tables": []}, {}, None, subjects)
    assert len(m) == 1   # one cited circular is about equity: kept


def table(symbol, company, reason):
    return [[["Sr. No.", "Name of the Company", "Symbol", "Series", "Reason for Suspension"],
             ["1", company, symbol, "EQ", reason]]]


def test_corrigendum_is_folded_into_the_original_row():
    orig = dated_entry("NSE/CML/64455", date(2024, 10, 9), "Suspension of trading in equity shares - Jubilant Industries Limited")
    corr = dated_entry("NSE/CML/64545", date(2024, 10, 14),
                       "Corrigendum to the Circular issued on Suspension of trading in equity shares - Jubilant Industries Limited")
    t1 = ("Sub: Suspension of trading in equity shares - Jubilant Industries Limited\nIt is hereby notified that the following "
          "security will be suspended from trading w.e.f. October 16, 2024 (i.e., closing hours of trading on October 15, 2024).")
    t2 = ("Sub: Corrigendum to the Circular issued on Suspension of trading in equity shares - Jubilant Industries\nLimited\n"
          "This is with reference to Exchange Circular No. NSE/CML/64455 dated October 09, 2024, in respect of Suspension. It "
          "was notified that the following security will be suspended from trading w.e.f. w.e.f. October 16, 2024 (i.e., "
          "closing hours of trading on October 15, 2024).\nHowever, the suspension date for such security shall be considered "
          "as October 28, 2024 (i.e., closing hours of trading on October 27, 2024) pursuant to announcement dated October 11, 2024.")
    tab = table("JUBLINDS", "Jubilant Industries Limited", "Scheme of Amalgamation")
    m1, _ = suspensions.parse(orig, {"text": t1, "tables": tab}, {})
    m2, _ = suspensions.parse(corr, {"text": t2, "tables": tab}, {})
    assert (m2[0]["suspended_from"], m2[0]["last_trading_date"]) == ("2024-10-28", "2024-10-27")
    rows = suspensions.merge_corrections(m1 + m2, "old_symbol", "old_company",
                                         ("suspended_from", "last_trading_date", "record_date"))
    assert len(rows) == 1
    r = rows[0]
    assert (r["circular_no"], r["corrected_by"], r["suspended_from"], r["last_trading_date"]) == \
        ("NSE/CML/64455", "NSE/CML/64545", "2024-10-28", "2024-10-27")
    assert "corrected by NSE/CML/64545" in r["review_note"]
    alone = suspensions.merge_corrections(m2, "old_symbol", "old_company", ("suspended_from",))
    assert len(alone) == 1 and "correction without its original circular" in alone[0]["review_note"]


def test_nclt_corrigendum_order_in_the_text_is_not_a_correction():
    e = dated_entry("NSE/CML/57595", date(2023, 7, 18), "Suspension of trading in equity shares - Tantia Constructions Limited")
    text = ("Sub: Suspension of trading in equity shares - Tantia Constructions Limited\nSuspension of Trading on account of "
            "Capital Reduction pursuant to NCLT Order dated February 24, 2020, read with Corrigendum order dated May 18, 2023\n"
            "This Circular shall be effective from July 21, 2023.")
    _, [r] = suspensions.parse(e, {"text": text, "tables": []}, {})
    assert r["_correction"] is False and r["reason"] == "capital reduction" and r["exit_expected"] == "N"


def test_cancelled_and_continued_suspensions():
    orig = dated_entry("NSE/CML/35297", date(2017, 7, 5), "Suspension of trading in Equity Shares in accordance with SEBI circular")
    other = dated_entry("NSE/CML/35200", date(2017, 6, 1), "Suspension of trading in equity shares - Gangotri Textiles Limited")
    canc = dated_entry("NSE/CML/35438", date(2017, 7, 24), "Proposed Suspension not to be carried out")
    cont = dated_entry("NSE/CML/60926", date(2024, 2, 29), "Continuation of suspension in trading of securities")
    t_orig = ("Sub: Suspension of trading in Equity Shares in accordance with SEBI circular\nPursuant to SEBI circular for "
              "non-compliance with LODR, it is hereby informed that trading in Equity Shares of Orbit Corporation Limited & "
              "Gangotri Textiles Limited will be suspended w.e.f. July 27, 2017.")
    t_other = ("Sub: Suspension of trading in equity shares - Gangotri Textiles Limited\nNon-compliance with Regulation 33; "
               "suspended w.e.f. June 20, 2017.")
    t_canc = ("Sub: Proposed Suspension not to be carried out\nIn continuation of our circular no. 680/2017 dated July 05, 2017 "
              "(download ref. no. NSE/CML/35297) notifying the Suspension of trading in Equity Shares w.e.f. July 27, 2017. "
              "It is proposed not to proceed with the suspension of Trading in the equity shares of Gangotri Textiles Limited "
              "on account of compliance with Regulation 33 of SEBI (LODR).\nThis circular shall be effective from July 27, 2017.")
    t_cont = ("Sub: Continuation of suspension in trading of securities\nTrading in the securities of the Company has already been "
              "suspended w.e.f. September 10, 2018 (Download Ref. No. NSE/CML/38635). The trading in Equity shares of Gitanjali "
              "Gems Limited shall continue to remain suspended on account of initiation of Liquidation Proceedings.")
    names = {name_key("Gangotri Textiles Limited"): "GANGOTRI", name_key("Orbit Corporation Limited"): "ORBITCORP",
             name_key("Gitanjali Gems Limited"): "GITANJALI"}
    rows = []
    for e, t in ((other, t_other), (orig, t_orig), (canc, t_canc), (cont, t_cont)):
        rows += suspensions.parse(e, {"text": t, "tables": []}, names)[1]
    rows = suspensions.link_cancellations(suspensions.merge_corrections(rows))
    by = {(r["circular_no"], r["symbol"]): r for r in rows}
    assert [r["symbol"] for r in rows if r["circular_no"] == "NSE/CML/35297"] == ["ORBITCORP", "GANGOTRI"]
    c = by[("NSE/CML/35438", "GANGOTRI")]
    assert (c["status"], c["cancels_circular"], c["exit_expected"]) == ("CANCELLED", "NSE/CML/35297", "N")   # the cited one
    g = by[("NSE/CML/60926", "GITANJALI")]
    assert (g["status"], g["reason"], g["exit_expected"], g["effective_date"]) == ("CONTINUED", "liquidation", "Y", "2018-09-10")
    assert "before the circular date" not in g["review_note"]
    assert by[("NSE/CML/35297", "ORBITCORP")]["status"] == "SUSPENDED"
    lone = suspensions.link_cancellations(suspensions.parse(canc, {"text": t_canc, "tables": []}, names)[1])
    assert lone[0]["needs_review"] == "Y" and "cancelled circular not found" in lone[0]["review_note"]


@pytest.mark.parametrize("reason, stype, exit_", [
    ("Scheme of Amalgamation", "MERGER", "Y"), ("Composite Scheme of Amalgamation and Arrangement", "MERGER", "Y"),
    ("Suspension of Trading on account of Scheme of Arrangement - Demerger and Capital Reduction", "DEMERGER+CAPITAL_REDUCTION", "N"),
    ("Composite Scheme of Arrangement for Demerger and Amalgamation", "MERGER+DEMERGER", "Y"),
    ("Scheme of Arrangement", "OTHER", "N")])
def test_scheme_type_and_exit_expected(reason, stype, exit_):
    e = dated_entry("NSE/CML/46933", date(2021, 1, 7), "Suspension of trading in equity shares - Ortin Laboratories Limited")
    text = ("Sub: Suspension of trading in equity shares - Ortin Laboratories Limited\nIt is notified that it will be "
            "suspended from trading w.e.f. January 12, 2021 (i.e. closing hours of trading on January 11, 2021).")
    m, _ = suspensions.parse(e, {"text": text, "tables": table("ORTINLABSS", "Ortin Laboratories Limited", reason)}, {})
    assert (m[0]["scheme_type"], m[0]["exit_expected"]) == (stype, exit_)
    assert ("scheme type unclear" in m[0]["review_note"]) == (stype == "OTHER")
    assert ("swap ratio not in NSE circular" in m[0]["review_note"]) == ("MERGER" in stype.split("+"))


def test_scheme_capital_reduction_alone():
    assert suspensions.scheme_type("Scheme of Arrangement - Capital Reduction from Rs. 10/- to Rs. 5/-") == "CAPITAL_REDUCTION"
    assert suspensions.scheme_type("reduction of share capital") == "CAPITAL_REDUCTION"
    assert suspensions.scheme_type("Scheme of Demerger") == "DEMERGER"   # 'Demerger' is not a merger


@pytest.mark.parametrize("reason_text, label, exit_", [
    ("Delisting of equity shares", "delisting", "Y"), ("initiation of Liquidation Proceedings", "liquidation", "Y"),
    ("Resolution plan approved by NCLT under IBC", "IBC / resolution plan", "Y"),
    ("Suspension on account of second and final call", "partly paid call", "N"),
    ("Capital Reduction pursuant to NCLT order", "capital reduction", "N"),
    ("non-compliance with Regulation 33", "non-compliance with listing rules", "N")])
def test_suspension_exit_expected(reason_text, label, exit_):
    e = dated_entry("NSE/CML/1", date(2020, 1, 2), "Suspension of trading in equity shares - Alpha Limited")
    _, [r] = suspensions.parse(e, {"text": "Sub: Suspension\nIt will be suspended w.e.f. January 10, 2020.",
                                   "tables": table("ALPHA", "Alpha Limited", reason_text)}, {})
    assert (r["reason"], r["exit_expected"], r["status"]) == (label, exit_, "SUSPENDED")


def test_company_sentence_joined_with_ampersand():
    from backfill.nse_ref.common import companies_in_text, split_companies
    text = ("Sub : Suspension\nIt is hereby informed that trading in Equity Shares of Orbit Corporation Limited & Gangotri "
            "Textiles Limited will be suspended w.e.f. July 27, 2017")
    assert companies_in_text(text) == ["Orbit Corporation Limited", "Gangotri Textiles Limited"]
    assert split_companies("Johnson & Johnson Limited") == ["Johnson & Johnson Limited"]


def test_cited_circulars():
    assert circulars.cited("Download Ref. No. NSE/CML/38635, NSE / CML / 67999 and NCL/CMPT/56669; NSE/CML/38635") == \
        ["NSE/CML/38635", "NSE/CML/67999", "NCL/CMPT/56669"]

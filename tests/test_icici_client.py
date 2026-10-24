"""backfill/equity/icici_client.py: the request parts built from stubbed credentials, the answers mapped to errors,
and redaction. A fake requests session; no network; .env is never read (config.load_env stubbed)."""
import base64
import json
import re
from datetime import date

import pytest
import requests

import config
from backfill.equity import icici_client as ic

KEY, TOKEN, SECRET = "test-key-1234", "test-session-5678", "test-secret-9999"
USER, SESSION_KEY = "TESTUSER", "SESSKEY42"
B64 = base64.b64encode(f"{USER}:{SESSION_KEY}".encode()).decode()


class Answer:
    def __init__(self, status=200, body=None, text=None, content=b""):
        self.status_code, self._body, self.content = status, body, content
        self.text = text if text is not None else json.dumps(body)

    def json(self):
        if self._body is None:
            raise ValueError("no json")
        return self._body


class FakeSession:
    def __init__(self, *answers):
        self.answers, self.sent = list(answers), []

    def request(self, method, url, **kw):
        self.sent.append((method, url, kw))
        a = self.answers.pop(0)
        if isinstance(a, Exception):
            raise a
        return a


CUSTOMER_OK = Answer(body={"Success": {"session_token": B64, "idirect_userid": USER}, "Status": 200, "Error": None})


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def refuse(*a, **k):
        raise AssertionError("a test tried to reach the network")
    monkeypatch.setattr(requests.Session, "request", refuse)
    monkeypatch.setattr(config, "load_env", lambda: {"BREEZE_API_KEY": KEY, "BREEZE_SESSION_TOKEN": TOKEN,
                                                     "BREEZE_API_SECRET": SECRET})


def client(*answers):
    s = FakeSession(*answers)
    return ic.BreezeClient.from_env(session=s), s


def test_customer_request_and_session_headers():
    c, s = client(CUSTOMER_OK, Answer(body={"Success": [], "Status": 200, "Error": None}))
    url, body, headers = c.customer_request()
    assert url == ic.CUSTOMER_URL and json.loads(body) == {"SessionToken": TOKEN, "AppKey": KEY}
    assert headers == {"Content-Type": "application/json"}
    with pytest.raises(ic.IciciSignIn):
        c.hist_headers()                                   # no session yet
    assert c.customer() == ["idirect_userid"]              # field names only, never values
    method, sent_url, kw = s.sent[0]
    assert (method, sent_url) == ("GET", ic.CUSTOMER_URL) and json.loads(kw["data"]) == {"SessionToken": TOKEN,
                                                                                       "AppKey": KEY}
    assert c.hist_headers() == {"Content-Type": "application/json", "apikey": KEY, "X-SessionToken": B64}
    assert c.historical("RELIND", "1minute", date(2017, 9, 6), date(2017, 9, 7)) == []
    method, sent_url, kw = s.sent[1]
    assert sent_url == ic.HIST_URL and kw["params"] == {
        "interval": "1minute", "from_date": "2017-09-06T00:00:00.000Z", "to_date": "2017-09-07T23:59:59.000Z",
        "stock_code": "RELIND", "exch_code": "NSE"}
    assert kw["headers"]["X-SessionToken"] == B64 and kw["headers"]["apikey"] == KEY
    assert SECRET not in repr(s.sent)                      # the secret is never sent
    with pytest.raises(ValueError):
        c.hist_params("X", "5minute", date(2015, 1, 1), date(2015, 1, 1))


def test_only_three_urls_in_the_module():
    with open(ic.__file__, encoding="utf-8") as f:
        src = f.read()
    assert sorted(set(re.findall(r'"(https?://[^"]+)"', src))) == sorted(ic.URLS)
    for word in ("order", "portfolio", "funds", "squareoff", "margin"):
        assert not re.search(rf"def \w*{word}", src)


def test_answers_map_to_errors():
    candles = [{"datetime": "2015-01-15 09:15:00", "open": 1, "high": 2, "low": 1, "close": 2, "volume": 5}]
    cases = [(Answer(401, {"Success": None, "Status": 401, "Error": "Unauthorized"}), ic.IciciSignIn),
             (Answer(200, {"Success": None, "Status": 500, "Error": "Session key is expired"}), ic.IciciSignIn),
             (Answer(429, None, text="Too Many Requests"), ic.IciciRateLimit),
             (Answer(200, {"Success": None, "Status": 500, "Error": "Rate limit exceeded"}), ic.IciciRateLimit),
             (Answer(500, None, text="boom"), ic.IciciFailed),
             (Answer(200, {"Success": None, "Status": 500, "Error": "No such stock code"}), ic.IciciFailed),
             (requests.exceptions.ConnectionError("down"), ic.IciciFailed)]
    for answer, err in cases:
        c, _ = client(CUSTOMER_OK, answer)
        c.customer()
        with pytest.raises(err):
            c.historical("X", "1day", date(2015, 1, 1), date(2015, 1, 2))
    c, _ = client(CUSTOMER_OK, Answer(body={"Success": candles, "Status": 200, "Error": None}),
                  Answer(body={"Success": None, "Status": 200, "Error": None}))
    c.customer()
    assert c.historical("X", "1minute", date(2015, 1, 15), date(2015, 1, 15)) == candles
    assert c.historical("X", "1minute", date(2015, 1, 16), date(2015, 1, 16)) == []


def test_session_rejected_and_redacted():
    for body in ({"Success": None, "Status": 500, "Error": "Invalid session."},
                 {"Success": None, "Status": 500, "Error": f"Resource not available for {TOKEN}"}):
        c, _ = client(Answer(200, body))
        with pytest.raises(ic.IciciSignIn) as e:
            c.customer()
        assert "log in to ICICI again" in str(e.value) and TOKEN not in str(e.value) and KEY not in str(e.value)
    c, _ = client(CUSTOMER_OK, Answer(403, {"Success": None, "Status": 403, "Error": f"bad {B64} {SESSION_KEY}"}))
    c.customer()
    with pytest.raises(ic.IciciSignIn) as e:
        c.historical("X", "1day", date(2015, 1, 1), date(2015, 1, 2))
    assert B64 not in str(e.value) and SESSION_KEY not in str(e.value) and "***" in str(e.value)
    assert c.redact(f"{KEY}/{TOKEN}") == "***/***"


def test_credentials_from_env(monkeypatch):
    assert ic.credentials() == (KEY, TOKEN)
    monkeypatch.setattr(config, "load_env", lambda: {"BREEZE_API_KEY": KEY})
    with pytest.raises(ic.IciciSignIn) as e:
        ic.BreezeClient.from_env()
    assert "BREEZE_SESSION_TOKEN" in str(e.value) and KEY not in str(e.value)


def test_security_master_download():
    c, s = client(Answer(200, None, text="", content=b"PK zip"))
    assert c.security_master() == b"PK zip"
    assert s.sent[0][1] == ic.MASTER_URL and "headers" not in s.sent[0][2]     # public: no key, no token
    c, _ = client(Answer(404, None, text="", content=b""))
    with pytest.raises(ic.IciciFailed):
        c.security_master()

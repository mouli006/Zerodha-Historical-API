"""Dhan sign-in with an access token (dhan.py). No network and no real .env: both are replaced by fakes."""
from urllib.parse import parse_qs, urlparse

import pytest

import auth
import dashboard
import dhan
from app import app
from backfill import dhan_api

TOKEN = "SECRET-TOKEN-123"
PROFILE = {"dhanClientId": "1000000001", "tokenValidity": "30/03/2099 15:37", "dataPlan": "Active"}


@pytest.fixture
def env(monkeypatch):
    store = {}

    def save(updates):
        store.update(updates)
        for k in [k for k, v in store.items() if v is None]:
            del store[k]

    monkeypatch.setattr(dhan, "load_env", lambda: dict(store))
    monkeypatch.setattr(dhan, "save_env", save)
    monkeypatch.setattr(dhan, "dhan_credentials", lambda: (store.get("DHAN_CLIENT_ID", ""), store.get("DHAN_ACCESS_TOKEN", "")))
    monkeypatch.setattr(dashboard, "load_env", lambda: dict(store))
    monkeypatch.setattr(dashboard, "keep_zero_volume", lambda: False)
    monkeypatch.setattr(auth, "credentials", lambda: ("", ""))
    monkeypatch.setattr(dhan, "keep_zero_volume", lambda: False)
    monkeypatch.setattr(dhan, "data_dir", lambda: "C:/data")
    return store


@pytest.fixture
def calls(monkeypatch):
    seen = []

    def profile(token):
        seen.append(token)
        return dict(PROFILE)

    monkeypatch.setattr(dhan, "fetch_profile", profile)
    return seen


def error_of(r):
    assert r.status_code == 302
    return parse_qs(urlparse(r.headers["Location"]).query).get("error", [""])[0]


def kite_session(c):
    with c.session_transaction() as s:
        s["access_token"] = "x"
        s["user"] = {"user_id": "AB1234", "user_name": "Demo User", "email": "", "broker": "ZERODHA", "login_time": ""}


def test_page_renders(env):
    html = app.test_client().get("/dhan").get_data(as_text=True)
    assert "/dhan/login" in html and 'name="access_token"' in html and "api_secret" not in html


def test_both_sign_in_pages_have_broker_tabs(env):
    c = app.test_client()
    for url, active in (("/", "zerodha"), ("/dhan", "dhan")):
        html = c.get(url).get_data(as_text=True)
        assert "BrokerTabs" in html and f'active="{active}"' in html
        assert '"dhan": "/dhan"' in html and '"zerodha": "/"' in html


def test_login_requires_both_fields(env, calls):
    r = app.test_client().post("/dhan/login", data={"client_id": "1000000001"})
    assert "required" in error_of(r) and calls == [] and env == {}


def test_login_checks_and_saves_token(env, calls):
    c = app.test_client()
    r = c.post("/dhan/login", data={"client_id": "1000000001", "access_token": TOKEN})
    assert r.headers["Location"] == "/dhan/dashboard?dhan=1" and calls == [TOKEN]
    assert env == {"DHAN_CLIENT_ID": "1000000001", "DHAN_ACCESS_TOKEN": TOKEN, "DHAN_TOKEN_EXPIRY": "2099-03-30T15:37:00"}
    with c.session_transaction() as s:
        assert s["dhan_user"] == {"client_id": "1000000001", "data_plan": "Active"}
    html = c.get("/dhan").get_data(as_text=True)
    assert '"connected": true' in html and "2099-03-30 15:37:00" in html and TOKEN not in html


def test_blank_token_rechecks_saved(env, calls):
    env.update({"DHAN_CLIENT_ID": "1000000001", "DHAN_ACCESS_TOKEN": TOKEN})
    app.test_client().post("/dhan/login", data={"client_id": "1000000001", "access_token": ""})
    assert calls == [TOKEN]


def test_token_for_other_client_is_refused(env, calls):
    r = app.test_client().post("/dhan/login", data={"client_id": "999", "access_token": TOKEN})
    assert "different Dhan Client ID" in error_of(r) and env == {}


def test_rejected_token_is_shown_without_token(env, monkeypatch):
    def refuse(token):
        raise dhan.DhanError("Dhan refused the token (HTTP 401): the access token is invalid or expired.")
    monkeypatch.setattr(dhan, "fetch_profile", refuse)
    r = app.test_client().post("/dhan/login", data={"client_id": "1", "access_token": TOKEN})
    assert "invalid or expired" in error_of(r) and TOKEN not in r.headers["Location"] and env == {}


def test_fetch_profile_turns_http_error_into_message(monkeypatch):
    class Resp:
        status_code = 401
        def json(self):
            return {"errorType": "Invalid_Authentication", "errorMessage": "Client ID or user generated access token is invalid or expired."}
    seen = {}
    def get(url, headers, timeout):
        seen.update(url=url, headers=headers)
        return Resp()
    monkeypatch.setattr(dhan_api.requests, "get", get)
    with pytest.raises(dhan_api.DhanError, match="invalid or expired") as e:
        dhan_api.fetch_profile(TOKEN)
    assert TOKEN not in str(e.value)
    assert seen["url"] == "https://api.dhan.co/v2/profile" and seen["headers"]["access-token"] == TOKEN


def test_parse_validity():
    assert dhan.parse_validity("30/03/2025 15:37") == "2025-03-30T15:37:00"
    assert dhan.parse_validity("2025-03-30T15:37:00") == "2025-03-30T15:37:00"
    assert dhan.parse_validity("soon") == "soon"


def test_login_opens_dhan_dashboard(env, calls):
    c = app.test_client()
    kite_session(c)
    r = c.post("/dhan/login", data={"client_id": "1000000001", "access_token": TOKEN})
    assert r.headers["Location"] == "/dhan/dashboard?dhan=1"
    html = c.get("/dhan/dashboard?dhan=1").get_data(as_text=True)
    assert '"mode": "dhan"' in html and '"dhan_connected": true' in html and TOKEN not in html
    # the Kite dashboard shows Dhan as connected, with a link to the Dhan page
    html = c.get("/success").get_data(as_text=True)
    assert '"mode": "kite"' in html and '"connected": true' in html and "/dhan/dashboard" in html and TOKEN not in html


def test_expired_token_is_not_connected(env):
    env.update({"DHAN_ACCESS_TOKEN": TOKEN, "DHAN_TOKEN_EXPIRY": "2000-01-01T09:00:00"})
    with app.test_request_context():
        assert dhan.status()["connected"] is False


def test_logout_removes_token(env):
    env.update({"DHAN_CLIENT_ID": "1", "DHAN_ACCESS_TOKEN": TOKEN, "DHAN_TOKEN_EXPIRY": "2099-01-01T09:00:00"})
    c = app.test_client()
    with c.session_transaction() as s:
        s["dhan_user"] = {"client_id": "1", "data_plan": ""}
    r = c.get("/dhan/logout")
    assert r.headers["Location"] == "/dhan"
    assert env == {"DHAN_CLIENT_ID": "1"}
    with c.session_transaction() as s:
        assert "dhan_user" not in s

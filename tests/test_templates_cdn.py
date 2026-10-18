"""The pages load React and htm from cdnjs only (base.html), pinned: /, /dhan and /success (faked Kite session).
No network and no real .env (load_env / save_env / credentials stubbed)."""
import pytest

import auth
import dashboard
import dhan
from app import app

HTM = '<script src="https://cdnjs.cloudflare.com/ajax/libs/htm/3.1.1/htm.umd.js"></script>'
REACT = "https://cdnjs.cloudflare.com/ajax/libs/react/18.3.1/umd/react.production.min.js"


@pytest.fixture
def client(monkeypatch):
    for mod in (dhan, dashboard):
        monkeypatch.setattr(mod, "load_env", lambda: {})
        monkeypatch.setattr(mod, "save_env", lambda updates: None)
        monkeypatch.setattr(mod, "keep_zero_volume", lambda: False)
        monkeypatch.setattr(mod, "data_dir", lambda: "C:/data")
    monkeypatch.setattr(dhan, "dhan_credentials", lambda: ("", ""))
    monkeypatch.setattr(auth, "credentials", lambda: ("", ""))
    monkeypatch.setattr(dashboard, "credentials", lambda: ("", ""))
    return app.test_client()


def kite_session(c):
    with c.session_transaction() as s:
        s["access_token"] = "x"
        s["user"] = {"user_id": "AB1234", "user_name": "Demo User", "email": "", "broker": "ZERODHA", "login_time": ""}


@pytest.mark.parametrize("path", ["/", "/dhan", "/success"])
def test_page_loads_htm_from_cdnjs(client, path):
    if path == "/success":
        kite_session(client)
    r = client.get(path)
    assert r.status_code == 200
    html = r.get_data(as_text=True)
    assert HTM in html and REACT in html
    assert "unpkg.com" not in html

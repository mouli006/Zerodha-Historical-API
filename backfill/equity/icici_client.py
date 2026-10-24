"""ICICI Breeze REST client for the ICICI 1-minute source (docs/dev/equity_icici_source.md). It does not use the
breeze-connect package, which downloads a file on import, writes ./logs and clashes with our config.py. It calls the
endpoints with requests and makes only these three calls:

    customerdetails      GET  CUSTOMER_URL, JSON body {"SessionToken", "AppKey"}: checks the session and returns the
                              base64 "user_id:session_key" token the historical endpoint needs
    historicalcharts v2  GET  HIST_URL, params interval / from_date / to_date / stock_code / exch_code=NSE,
                              headers apikey + X-SessionToken
    security master      GET  MASTER_URL (a public zip; NSEScripMaster.txt inside)

There are no order, portfolio or funds calls. BREEZE_API_SECRET only signs those (X-Checksum), so it is never read
or sent. BREEZE_API_KEY and BREEZE_SESSION_TOKEN (new every day) come from .env through config.load_env(); they are
never printed, logged or written: every error text goes through redact()."""
import base64
import json

import requests

import config

CUSTOMER_URL = "https://api.icicidirect.com/breezeapi/api/v1/customerdetails"
HIST_URL = "https://breezeapi.icicidirect.com/api/v2/historicalcharts"
MASTER_URL = "https://directlink.icicidirect.com/MotherAppMaster/SecurityMaster.zip"
URLS = (CUSTOMER_URL, HIST_URL, MASTER_URL)
INTERVALS = ("1minute", "1day")
MAX_BARS = 1000                 # a longer answer is cut silently to its LAST 1,000 bars (discovery, 2026-10-19)
TIMEOUT = 60
SIGN_IN = ("log in to ICICI again and put today's session token in .env as BREEZE_SESSION_TOKEN "
           "(it is new every day)")
RATE_WORDS = ("limit", "too many", "throttl", "exceed")
SESSION_WORDS = ("session", "public key", "resource not available", "unauthor")


class IciciError(Exception):
    pass


class IciciSignIn(IciciError):
    """The session (or the API key) was rejected: stop and log in again."""


class IciciRateLimit(IciciError):
    """ICICI answered with a rate-limit sign."""


class IciciFailed(IciciError):
    """Any other failure of one call (network, server error, unexpected answer)."""


def credentials(env=None):
    """(api key, session token) from .env; IciciSignIn when either is missing. The values are never shown."""
    env = config.load_env() if env is None else env
    key, token = env.get("BREEZE_API_KEY", "").strip(), env.get("BREEZE_SESSION_TOKEN", "").strip()
    if not key or not token:
        raise IciciSignIn(f"No BREEZE_API_KEY or BREEZE_SESSION_TOKEN in .env: {SIGN_IN}.")
    return key, token


def iso_range(start, end):
    """from_date / to_date of a whole-day range (dates, inclusive) as Breeze wants them."""
    return f"{start.isoformat()}T00:00:00.000Z", f"{end.isoformat()}T23:59:59.000Z"


class BreezeClient:
    """The three Breeze calls. session: a requests.Session (tests pass a fake)."""

    def __init__(self, key, token, session=None, timeout=TIMEOUT):
        self._key, self._token = key, token
        self._b64 = None
        self._secrets = [s for s in (key, token) if s]
        self.http = session or requests.Session()
        self.timeout = timeout

    @classmethod
    def from_env(cls, env=None, session=None):
        return cls(*credentials(env), session=session)

    def redact(self, text):
        text = str(text)
        for s in sorted(self._secrets, key=len, reverse=True):
            text = text.replace(s, "***")
        return text

    # ---- request parts (tested without the network) ----

    def customer_request(self):
        """(url, body, headers) of customerdetails: the session token and API key go in the JSON body."""
        body = json.dumps({"SessionToken": self._token, "AppKey": self._key}, separators=(",", ":"))
        return CUSTOMER_URL, body, {"Content-Type": "application/json"}

    def hist_headers(self):
        if not self._b64:
            raise IciciSignIn(f"No ICICI session yet (customerdetails not called): {SIGN_IN}.")
        return {"Content-Type": "application/json", "apikey": self._key, "X-SessionToken": self._b64}

    @staticmethod
    def hist_params(code, interval, start, end):
        if interval not in INTERVALS:
            raise ValueError(f"interval must be one of {', '.join(INTERVALS)}")
        a, b = iso_range(start, end)
        return {"interval": interval, "from_date": a, "to_date": b, "stock_code": code, "exch_code": "NSE"}

    # ---- calls ----

    def customer(self):
        """Check the session (customerdetails). Returns the names of the answer's fields (no values). IciciSignIn when
        ICICI rejects the session or the key."""
        url, body, headers = self.customer_request()
        try:
            r = self.http.request("GET", url, data=body, headers=headers, timeout=self.timeout)
        except requests.exceptions.RequestException as e:
            raise IciciFailed(self.redact(f"customerdetails: {type(e).__name__}: {e}"))
        try:
            j = r.json()
        except ValueError:
            j = None
        succ = j.get("Success") if isinstance(j, dict) else None
        if isinstance(succ, dict) and succ.get("session_token"):
            b64 = str(succ["session_token"])
            self._b64 = b64
            self._secrets.append(b64)
            try:
                self._secrets += [p for p in base64.b64decode(b64).decode("ascii").split(":") if len(p) > 3]
            except (ValueError, UnicodeDecodeError):
                pass
            return sorted(k for k in succ if k != "session_token")
        if r.status_code >= 500 and not isinstance(j, dict):
            raise IciciFailed(self.redact(f"customerdetails: HTTP {r.status_code}"))
        err = j.get("Error") if isinstance(j, dict) else r.text[:200]
        raise IciciSignIn(self.redact(f"ICICI rejected the session (HTTP {r.status_code}: {err}): {SIGN_IN}."))

    def historical(self, code, interval, start, end):
        """Candles (dicts: datetime, open, high, low, close, volume, ...) of one NSE stock code over whole days
        start..end. [] for an empty answer. IciciSignIn / IciciRateLimit / IciciFailed otherwise."""
        params = self.hist_params(code, interval, start, end)
        headers = self.hist_headers()
        try:
            r = self.http.request("GET", HIST_URL, params=params, headers=headers, timeout=self.timeout)
        except requests.exceptions.RequestException as e:
            raise IciciFailed(self.redact(f"{type(e).__name__}: {e}"))
        try:
            j = r.json()
        except ValueError:
            j = None
        err = j.get("Error") if isinstance(j, dict) else None
        text = str(err if err is not None else ("" if isinstance(j, dict) else r.text[:300]))
        low = text.lower()
        if r.status_code in (401, 403) or any(w in low for w in SESSION_WORDS):
            raise IciciSignIn(self.redact(f"ICICI rejected the session (HTTP {r.status_code}: {text}): {SIGN_IN}."))
        if r.status_code == 429 or any(w in low for w in RATE_WORDS):
            raise IciciRateLimit(self.redact(f"HTTP {r.status_code}: {text or 'rate limit'}"))
        if r.status_code != 200 or not isinstance(j, dict):
            raise IciciFailed(self.redact(f"HTTP {r.status_code}: {text}"))
        succ = j.get("Success")
        if succ is None or succ == "" or succ == []:
            if err:
                raise IciciFailed(self.redact(f"Status {j.get('Status')}: {text}"))
            return []
        if not isinstance(succ, list):
            raise IciciFailed(self.redact(f"unexpected answer: {str(succ)[:200]}"))
        return succ

    def security_master(self):
        """The bytes of ICICI's SecurityMaster.zip (public, no session needed)."""
        try:
            r = self.http.request("GET", MASTER_URL, timeout=self.timeout * 3)
        except requests.exceptions.RequestException as e:
            raise IciciFailed(self.redact(f"security master: {type(e).__name__}: {e}"))
        if r.status_code != 200 or not r.content:
            raise IciciFailed(f"security master: HTTP {r.status_code}")
        return r.content

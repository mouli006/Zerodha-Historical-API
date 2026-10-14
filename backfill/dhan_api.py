"""DhanHQ v2 client shaped like the parts of KiteConnect the F&O job uses, so fno.run_daily can run on Dhan.

- profile(): token check (GET /v2/profile)
- instruments(exchange): the NSE_FNO (NIFTY) or BSE_FNO (SENSEX) instrument list as Kite-style dicts
  (name, instrument_type FUT/CE/PE, expiry, strike, tradingsymbol, instrument_token)
- historical_data(token, start, end, interval, oi=...): 1-minute candles (POST /v2/charts/intraday)
  as Kite-style dicts (date in IST, open, high, low, close, volume, oi)

Dhan errors are raised as kiteconnect exceptions so fetch.fetch_chunk retries, slows down or stops
exactly as it does for Kite. No message ever contains the access token.
"""
import csv
import io
from datetime import date, datetime
from zoneinfo import ZoneInfo

import requests
from kiteconnect import exceptions as kex

API_URL = "https://api.dhan.co/v2"
PROFILE_URL = f"{API_URL}/profile"
SEGMENT = "NSE_FNO"   # the default segment (NIFTY)
INSTRUMENT_LIST_URL = f"{API_URL}/instrument/{{segment}}"
INSTRUMENTS_URL = INSTRUMENT_LIST_URL.format(segment=SEGMENT)
INTRADAY_URL = f"{API_URL}/charts/intraday"
# Kite's F&O exchange (what fno passes, UNDERLYINGS[u][0]) or a Dhan segment -> Dhan's F&O segment
SEGMENT_OF = {"NFO": "NSE_FNO", "BFO": "BSE_FNO", "NSE_FNO": "NSE_FNO", "BSE_FNO": "BSE_FNO"}
# Underlying -> Dhan security id of its spot index (segment IDX_I), asked by the holiday check
SPOT_IDS = {"NIFTY": "13", "SENSEX": "51"}
SPOT = ("INDEX", "IDX_I")
TIMEOUT = 30
IST = ZoneInfo("Asia/Kolkata")
MAX_SPEED = 5.0   # Dhan Data APIs: 5 requests per second
INSTRUMENT_COLUMNS = ["SECURITY_ID", "INSTRUMENT", "UNDERLYING_SYMBOL", "SYMBOL_NAME", "SM_EXPIRY_DATE",
                      "STRIKE_PRICE", "OPTION_TYPE"]
KINDS = {"FUTIDX": "FUT", "OPTIDX": "OPT"}
NO_DATA = ("DH-907",)


class DhanError(Exception):
    pass


def error_text(data):
    if isinstance(data, dict):
        return data.get("errorMessage") or data.get("message") or data.get("remarks") or ""
    return ""


def token_connected(env, now=None):
    """True when .env has DHAN_ACCESS_TOKEN and its DHAN_TOKEN_EXPIRY (IST, no offset) is in the future
    (or unknown). Shared by the Dhan pages and the CLIs; never returns the token."""
    expiry = env.get("DHAN_TOKEN_EXPIRY", "")
    connected = bool(env.get("DHAN_ACCESS_TOKEN"))
    if connected and expiry:
        try:
            now = now or datetime.now(IST)
            connected = datetime.fromisoformat(expiry).replace(tzinfo=None) > now.replace(tzinfo=None)
        except ValueError:
            pass
    return connected


def token_ok(env):
    """(token, "") for a saved token that has not expired, else ("", why)."""
    if not env.get("DHAN_ACCESS_TOKEN"):
        return "", "DHAN_ACCESS_TOKEN is missing from .env. Connect Dhan on the Dhan tab of the sign-in page."
    if not token_connected(env):
        return "", (f"The Dhan access token expired ({env.get('DHAN_TOKEN_EXPIRY', '').replace('T', ' ')} IST). "
                    "Paste a new one on the Dhan tab of the sign-in page.")
    return env["DHAN_ACCESS_TOKEN"], ""


def data_plan_error(profile):
    """Why historical candles can't be fetched with this profile ("" when the Data API plan is active)."""
    plan = profile.get("data_plan", "")
    if plan and plan.lower() != "active":
        return f"Your Dhan Data API subscription is {plan}. Historical candles need an active Data API plan."
    return ""


def fetch_profile(token):
    """GET /v2/profile; errors become DhanError with Dhan's message, never the token."""
    try:
        r = requests.get(PROFILE_URL, headers={"access-token": token, "Accept": "application/json"}, timeout=TIMEOUT)
    except requests.RequestException as e:
        raise DhanError(f"Could not reach Dhan ({type(e).__name__}).") from None
    try:
        data = r.json()
    except ValueError:
        data = {}
    if r.status_code != 200 or not isinstance(data, dict) or not data.get("dhanClientId"):
        msg = error_text(data)
        if r.status_code in (401, 403) and not msg:
            msg = "the access token is invalid or expired"
        raise DhanError(f"Dhan refused the token (HTTP {r.status_code}){': ' + str(msg) if msg else ''}.")
    return data


def parse_instruments(text, segment=SEGMENT):
    """Kite-style dicts for every index future and option in Dhan's NSE_FNO or BSE_FNO list (all underlyings)."""
    reader = csv.DictReader(io.StringIO(text.lstrip("﻿")))
    header = [h.strip() for h in (reader.fieldnames or [])]
    missing = [c for c in INSTRUMENT_COLUMNS if c not in header]
    if missing:
        raise DhanError(f"Dhan's instrument list has no column {', '.join(missing)}; its format may have changed.")
    out = []
    for raw in reader:
        r = {(k or "").strip(): (v or "").strip() for k, v in raw.items()}
        kind = KINDS.get(r["INSTRUMENT"])
        if not kind or not r["SM_EXPIRY_DATE"]:
            continue
        inst_type = "FUT" if kind == "FUT" else r["OPTION_TYPE"].upper()
        if inst_type not in ("FUT", "CE", "PE"):
            continue
        try:
            expiry = date.fromisoformat(r["SM_EXPIRY_DATE"][:10])
            strike = 0.0 if inst_type == "FUT" else float(r["STRIKE_PRICE"])
        except ValueError:
            continue
        # SYMBOL_NAME is the same for every expiry of a month (NIFTY-Oct2026-25000-CE for 13, 19 and 27 Oct),
        # so the security id makes it unique: it is the progress key and the contract -> file map in run_daily
        out.append({"instrument_token": r["SECURITY_ID"], "tradingsymbol": f"{r['SYMBOL_NAME']}#{r['SECURITY_ID']}",
                    "name": r["UNDERLYING_SYMBOL"], "expiry": expiry, "strike": strike, "instrument_type": inst_type,
                    "lot_size": r.get("LOT_SIZE", ""), "exchange": segment, "dhan_instrument": r["INSTRUMENT"]})
    return out


def candles_from(data):
    """Dhan's column arrays as Kite-style candle dicts, times in IST."""
    if not isinstance(data, dict) or not data.get("timestamp"):
        return []
    oi = data.get("open_interest") or []
    out = []
    for n, ts in enumerate(data["timestamp"]):
        out.append({"date": datetime.fromtimestamp(float(ts), IST),
                    **{k: data[k][n] for k in ("open", "high", "low", "close", "volume")},
                    "oi": oi[n] if n < len(oi) else None})
    return out


def raise_for(status, data):
    """Map a failed Dhan response to the kiteconnect exception fetch_chunk expects."""
    code = str(data.get("errorCode", "")) if isinstance(data, dict) else ""
    msg = error_text(data) or f"HTTP {status}"
    text = f"Dhan {code + ': ' if code else ''}{msg}"
    if status == 401 or code == "DH-901":
        raise kex.TokenException(f"{text}. Paste a new Dhan access token.", code=403)
    if code == "DH-902":
        raise kex.PermissionException(f"{text}. Is the Dhan Data API subscription active?", code=403)
    if status == 429 or code == "DH-904":
        raise kex.KiteException(text, code=429)
    if status >= 500 or code in ("DH-908", "DH-910"):
        raise kex.NetworkException(text, code=503)
    raise kex.InputException(text, code=400)


class DhanClient:
    """The KiteConnect methods fno.run_daily needs, backed by DhanHQ v2."""

    def __init__(self, access_token):
        self._token = access_token
        self._kind = {}   # security id -> (FUTIDX / OPTIDX / INDEX, segment), filled by instruments()

    def _headers(self):
        return {"access-token": self._token, "Accept": "application/json", "Content-Type": "application/json"}

    def profile(self):
        try:
            data = fetch_profile(self._token)
        except DhanError as e:
            raise kex.TokenException(str(e), code=403) from None
        return {"user_id": str(data["dhanClientId"]), "data_plan": data.get("dataPlan") or ""}

    def instruments(self, exchange=None):
        """exchange: Kite's NFO / BFO or a Dhan segment; None is NSE_FNO."""
        segment = SEGMENT_OF.get(exchange or SEGMENT)
        if segment is None:
            raise DhanError(f"Dhan has no F&O segment for {exchange}.")
        r = requests.get(INSTRUMENT_LIST_URL.format(segment=segment), headers=self._headers(), timeout=TIMEOUT)
        if r.status_code != 200:
            try:
                data = r.json()
            except ValueError:
                data = {}
            raise_for(r.status_code, data)
        rows = parse_instruments(r.text, segment)
        self._kind = {i["instrument_token"]: (i["dhan_instrument"], segment) for i in rows}
        return rows

    def learn(self, instruments):
        """Instrument type and segment per security id from saved instrument rows (instruments_<segment>.csv)."""
        for i in instruments:
            if i.get("dhan_instrument"):
                self._kind[str(i["instrument_token"])] = (i["dhan_instrument"], i.get("exchange") or SEGMENT)

    def spot_token(self, underlying):
        """The spot index's security id (IDX_I) for historical_data, or None if unknown."""
        sid = SPOT_IDS.get(underlying)
        if sid is not None:
            self._kind[sid] = SPOT
        return sid

    def historical_data(self, instrument_token, from_date, to_date, interval, continuous=False, oi=False):
        token = str(instrument_token)
        instrument, segment = self._kind.get(token, ("OPTIDX", SEGMENT))
        body = {"securityId": token, "exchangeSegment": segment, "instrument": instrument,
                "interval": "1", "oi": bool(oi),
                "fromDate": f"{from_date:%Y-%m-%d %H:%M:%S}", "toDate": f"{to_date:%Y-%m-%d %H:%M:%S}"}
        r = requests.post(INTRADAY_URL, json=body, headers=self._headers(), timeout=TIMEOUT)
        try:
            data = r.json()
        except ValueError:
            data = {}
        if r.status_code == 200 and not (isinstance(data, dict) and data.get("errorCode")):
            return candles_from(data)
        if isinstance(data, dict) and (str(data.get("errorCode", "")) in NO_DATA or "no data" in error_text(data).lower()):
            return []
        raise_for(r.status_code, data)

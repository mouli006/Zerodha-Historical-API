"""backfill/dhan_api.py: Dhan's instrument list and candles in Kite's shape, and error mapping.
No network: requests is replaced by fakes."""
from datetime import date, datetime

import pytest
from kiteconnect import exceptions as kex

from backfill import dhan_api
from backfill.dhan_api import IST, DhanClient

TOKEN = "SECRET-DHAN-TOKEN"
CSV = """EXCH_ID,SEGMENT,SECURITY_ID,ISIN,INSTRUMENT,UNDERLYING_SECURITY_ID,UNDERLYING_SYMBOL,SYMBOL_NAME,DISPLAY_NAME,INSTRUMENT_TYPE,SERIES,LOT_SIZE,SM_EXPIRY_DATE,STRIKE_PRICE,OPTION_TYPE,TICK_SIZE,EXPIRY_FLAG
NSE,D,35001,NA,FUTIDX,26000,NIFTY,NIFTY-Oct2026-FUT,NIFTY OCT FUT,FUT,NA,75.0,2026-10-27,-0.01000,XX,10.0000,M
NSE,D,40001,NA,OPTIDX,26000,NIFTY,NIFTY-Oct2026-25100-CE,NIFTY 13 OCT 25100 CALL,OP,NA,75.0,2026-10-13 14:30:00,25100.00000,CE,5.0000,W
NSE,D,40002,NA,OPTIDX,26000,NIFTY,NIFTY-Oct2026-25100-PE,NIFTY 13 OCT 25100 PUT,OP,NA,75.0,2026-10-13,25100.00000,PE,5.0000,W
NSE,D,50001,NA,FUTSTK,1333,HDFCBANK,HDFCBANK-Oct2026-FUT,HDFCBANK OCT FUT,FUT,NA,550.0,2026-10-27,-0.01000,XX,10.0000,M
NSE,D,60001,NA,OPTIDX,26009,BANKNIFTY,BANKNIFTY-Oct2026-56000-CE,BANKNIFTY OCT 56000 CALL,OP,NA,35.0,2026-10-27,56000.00000,CE,5.0000,M
"""


class Resp:
    def __init__(self, status, data=None, text=""):
        self.status_code, self._data, self.text = status, data, text

    def json(self):
        if self._data is None:
            raise ValueError("no json")
        return self._data


@pytest.fixture
def http(monkeypatch):
    seen = {"get": [], "post": [], "replies": []}

    def get(url, headers=None, timeout=None):
        seen["get"].append((url, headers))
        if url == dhan_api.INSTRUMENTS_URL:
            return Resp(200, text=CSV)
        return Resp(200, {"dhanClientId": "1000000001", "dataPlan": "Active", "tokenValidity": "13/10/2026 09:00"})

    def post(url, json=None, headers=None, timeout=None):
        seen["post"].append((url, json, headers))
        return seen["replies"].pop(0)

    monkeypatch.setattr(dhan_api.requests, "get", get)
    monkeypatch.setattr(dhan_api.requests, "post", post)
    return seen


def test_instruments_are_kite_shaped(http):
    rows = DhanClient(TOKEN).instruments("NFO")
    by_id = {r["instrument_token"]: r for r in rows}
    assert set(by_id) == {"35001", "40001", "40002", "60001"}          # stock futures are left out
    fut, ce, pe = by_id["35001"], by_id["40001"], by_id["40002"]
    assert (fut["name"], fut["instrument_type"], fut["expiry"], fut["strike"]) == ("NIFTY", "FUT", date(2026, 10, 27), 0.0)
    assert (ce["instrument_type"], ce["expiry"], ce["strike"]) == ("CE", date(2026, 10, 13), 25100.0)
    assert pe["instrument_type"] == "PE" and pe["tradingsymbol"] == "NIFTY-Oct2026-25100-PE#40002"
    assert by_id["60001"]["name"] == "BANKNIFTY"
    assert http["get"][0][1]["access-token"] == TOKEN


def test_instrument_list_without_needed_column():
    with pytest.raises(dhan_api.DhanError, match="SM_EXPIRY_DATE"):
        dhan_api.parse_instruments("SECURITY_ID,INSTRUMENT,UNDERLYING_SYMBOL,SYMBOL_NAME,STRIKE_PRICE,OPTION_TYPE\n")


def test_candles_in_ist_with_oi(http):
    c = DhanClient(TOKEN)
    c.instruments()
    t0 = int(datetime(2026, 10, 12, 9, 15, tzinfo=IST).timestamp())
    http["replies"].append(Resp(200, {"open": [100, 101], "high": [102, 103], "low": [99, 100], "close": [101, 102.5],
                                      "volume": [750, 0], "timestamp": [t0, t0 + 60], "open_interest": [1500, 2250]}))
    rows = c.historical_data("40001", datetime(2026, 10, 12, 9, 0), datetime(2026, 10, 12, 16, 0), "minute", oi=True)
    url, body, headers = http["post"][0]
    assert url == dhan_api.INTRADAY_URL and headers["access-token"] == TOKEN
    assert body == {"securityId": "40001", "exchangeSegment": "NSE_FNO", "instrument": "OPTIDX", "interval": "1",
                    "oi": True, "fromDate": "2026-10-12 09:00:00", "toDate": "2026-10-12 16:00:00"}
    assert rows[0]["date"] == datetime(2026, 10, 12, 9, 15, tzinfo=IST)
    assert (rows[1]["close"], rows[1]["volume"], rows[1]["oi"]) == (102.5, 0, 2250)


def test_futures_are_requested_as_futidx(http):
    c = DhanClient(TOKEN)
    c.instruments()
    http["replies"].append(Resp(200, {}))
    assert c.historical_data("35001", datetime(2026, 10, 12, 9), datetime(2026, 10, 12, 16), "minute") == []
    assert http["post"][0][1]["instrument"] == "FUTIDX"


@pytest.mark.parametrize("status,data,exc,code", [
    (401, {"errorCode": "DH-901", "errorMessage": "Invalid token"}, kex.TokenException, None),
    (403, {"errorCode": "DH-902", "errorMessage": "No access"}, kex.PermissionException, 403),
    (429, {"errorCode": "DH-904", "errorMessage": "Too many requests"}, kex.KiteException, 429),
    (500, {}, kex.NetworkException, None),
    (400, {"errorCode": "DH-905", "errorMessage": "Bad input"}, kex.InputException, None),
])
def test_errors_map_to_kite_exceptions(http, status, data, exc, code):
    http["replies"].append(Resp(status, data))
    with pytest.raises(exc) as e:
        DhanClient(TOKEN).historical_data("1", datetime(2026, 10, 12, 9), datetime(2026, 10, 12, 16), "minute")
    assert TOKEN not in str(e.value)
    if code:
        assert e.value.code == code


def test_no_data_is_no_candles(http):
    http["replies"].append(Resp(400, {"errorCode": "DH-907", "errorMessage": "No data present"}))
    assert DhanClient(TOKEN).historical_data("1", datetime(2026, 10, 12, 9), datetime(2026, 10, 12, 16), "minute") == []


def test_profile(http):
    assert DhanClient(TOKEN).profile() == {"user_id": "1000000001", "data_plan": "Active"}

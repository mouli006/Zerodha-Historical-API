"""The ICICI 1-minute store (backfill/equity/icici_download.py writes it) and its loader. Separate from the Kite store
(equity_1min/, never written here):

    <folder>/equity_1min_icici/data/<security_id>/<year>.parquet   ICICI's 1-minute candles unchanged: ts
                                          (Asia/Kolkata), open, high, low, close (float64), volume (int64); ICICI's
                                          09:07 pre-open and 15:30-15:32 bars included
    <folder>/equity_1min_icici/data/<security_id>/icici_day.parquet  ICICI's day candles unchanged (date, OHLCV)
    <folder>/equity_1min_icici/data/<security_id>/days.parquet       one row per day: ICICI's day candle, the
                                          bhavcopy row, the conversion (icici_scale, icici_volume_scale), the check

load_minute_icici() rebuilds raw prices from each day's stored conversion (docs/dev/equity_icici_source.md):
    raw price = ICICI minute x icici_scale;  raw volume = ICICI minute volume x icici_volume_scale
"ours" divides raw prices by our cumulative factor of the day (adjustments), volume times it; "icici" returns the
stored values unchanged."""
import os
from datetime import date

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

import config
from backfill.equity import adjustments as adj
from backfill.equity import minute_plan as mp
from backfill.equity import minute_store as ms
from backfill.equity import security_master as sm

OUT = "equity_1min_icici"
DATA = "data"
DAYS_FILE = ms.DAYS_FILE
DAY_FILE = "icici_day.parquet"
MINUTE_COLUMNS = ms.MINUTE_COLUMNS
DAY_CANDLE_COLUMNS = ["date", "open", "high", "low", "close", "volume"]
PRICES = ("raw", "ours", "icici")
SESSION_FROM, SESSION_TO = "09:15", "15:29"   # bars outside are counted (bars_outside), stored and loaded unchanged


class NoMinuteData(ms.NoMinuteData):
    pass


def out_dir(folder=None):
    return os.path.join(folder or config.equity_data_dir(), OUT)


def lock_path(folder):
    return os.path.join(out_dir(folder), ".lock")


def data_dir(folder=None):
    return os.path.join(out_dir(folder), DATA)


def stock_dir(folder, sid):
    return os.path.join(data_dir(folder), sid)


def plan_dir(folder):
    return os.path.join(out_dir(folder), "plan")


def reference_dir(folder):
    return os.path.join(out_dir(folder), "reference")


def staging_dir(folder, sid=None):
    p = os.path.join(out_dir(folder), "staging")
    return os.path.join(p, sid) if sid else p


def check_dir(folder):
    return os.path.join(out_dir(folder), "check")


def archive_dir(folder):
    return os.path.join(out_dir(folder), "archive")


def minute_frame(candles):
    """ICICI candles (dicts with datetime 'YYYY-MM-DD HH:MM:SS' in IST, open..volume) as the stored frame, values
    unchanged."""
    df = pd.DataFrame(candles, columns=["datetime", "open", "high", "low", "close", "volume"])
    ts = pd.to_datetime(df["datetime"])
    ts = ts.dt.tz_localize(ms.TZ) if ts.dt.tz is None else ts.dt.tz_convert(ms.TZ)
    out = pd.DataFrame({"ts": ts.astype(f"datetime64[us, {ms.TZ}]")})
    for c in ("open", "high", "low", "close"):
        out[c] = df[c].astype("float64")
    out["volume"] = df["volume"].astype("int64")
    return out


def day_frame(candles):
    """ICICI day candles as (date, open, high, low, close, volume), values unchanged."""
    df = pd.DataFrame(candles, columns=["datetime", "open", "high", "low", "close", "volume"])
    out = pd.DataFrame({"date": [sm.as_date(str(x)[:10]) for x in df["datetime"]]})
    for c in ("open", "high", "low", "close"):
        out[c] = df[c].astype("float64")
    out["volume"] = df["volume"].astype("int64")
    return out


def read_day_candles(path):
    p = os.path.join(path, DAY_FILE)
    if not os.path.isfile(p):
        return pd.DataFrame({c: pd.Series(dtype="float64" if c != "date" else object) for c in DAY_CANDLE_COLUMNS})
    df = pq.read_table(p).to_pandas()
    df["date"] = [sm.as_date(d) for d in df["date"]]
    return df


def outside_session(ts):
    """Boolean array: bars outside 09:15-15:29 (ICICI's 09:07 pre-open bar, its 15:30-15:32 bars, evening sessions)."""
    hm = ts.dt.strftime("%H:%M")
    return ((hm < SESSION_FROM) | (hm > SESSION_TO)).to_numpy()


def resolve(key, folder, on_date=None):
    """The security_id for an id or any symbol / ISIN the company ever had (as minute_store.resolve, but the tie
    break looks at the ICICI store)."""
    key = str(key).strip().upper()
    if ms.ID_RE.match(key):
        return key
    ids = sm.lookup(key, folder)
    if not ids:
        raise NoMinuteData(f"{key}: no security ever had this symbol or ISIN.")
    if len(ids) == 1:
        return ids[0]
    when = sm.security_id_for(key, on_date, folder) if on_date else None
    if when:
        return when
    have = [s for s in ids if os.path.isfile(os.path.join(stock_dir(folder, s), DAYS_FILE))]
    if len(have) == 1:
        return have[0]
    raise NoMinuteData(f"{key} was used by {', '.join(ids)}; pass the security_id or a start date.")


def load_minute_icici(key, start=None, end=None, prices="raw", folder=None, clean=True):
    """ICICI 1-minute candles of one company from start to end (dates, inclusive): prices "icici" (as stored), "raw"
    (each day converted with its stored icici_scale / icici_volume_scale) or "ours" (raw, adjusted with our factors).
    Columns ts, open, high, low, close, volume, security_id, symbol (that day's), source ("ICICI"). Only days with a
    days.parquet row are returned. clean=True: the bars go through minute_store.clean_bars first (attrs["clean"])."""
    if prices not in PRICES:
        raise ValueError(f"prices must be one of {', '.join(PRICES)}")
    folder = folder or config.equity_data_dir()
    a = sm.as_date(start) if start else None
    b = sm.as_date(end) if end else None
    sid = resolve(key, folder, a)
    path = stock_dir(folder, sid)
    days = ms.read_days(path)
    if days is None:
        raise NoMinuteData(f"{sid}: no ICICI minute data stored (run: python icici_minute.py download).")
    years = None
    if a or b:
        years = set(range((a or date(1900, 1, 1)).year, (b or date(2999, 1, 1)).year + 1))
    m = ms.read_minutes(path, years)
    m["date"] = m["ts"].dt.date if len(m) else pd.Series(dtype=object)
    if a:
        m = m[m["date"] >= a]
    if b:
        m = m[m["date"] <= b]
    counts = None
    if clean:
        m, counts = ms.clean_bars(m)
    m = m.merge(days[["date", "symbol", "icici_scale", "icici_volume_scale"]], on="date", how="inner") \
        .sort_values("ts", kind="stable").reset_index(drop=True)
    out = m[MINUTE_COLUMNS].copy()
    if prices != "icici":
        s = m["icici_scale"].to_numpy(float)
        for c in ("open", "high", "low", "close"):
            out[c] = m[c].to_numpy(float) * s
        out["volume"] = np.round(m["volume"].to_numpy(float) * m["icici_volume_scale"].to_numpy(float))
    if prices == "ours":
        f = mp.with_factor(pd.DataFrame({"security_id": sid, "date": m["date"], "_i": np.arange(len(m))}),
                           adj.load_factors(folder)["factors"]).sort_values("_i")["our_factor"].to_numpy(float)
        for c in ("open", "high", "low", "close"):
            out[c] = out[c] / f
        out["volume"] = out["volume"] * f
    out["security_id"] = sid
    out["symbol"] = m["symbol"].to_numpy()
    out["source"] = "ICICI"
    if clean:
        out.attrs["clean"] = counts
    return out

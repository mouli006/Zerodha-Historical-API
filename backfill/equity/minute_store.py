"""The Kite 1-minute store of phase 2 (backfill/equity/minute_download.py writes it) and its loader.

    <folder>/equity_1min/data/<security_id>/<year>.parquet   Kite's 1-minute candles unchanged: ts (Asia/Kolkata),
                                                             open, high, low, close (float64), volume (int64)
    <folder>/equity_1min/data/<security_id>/days.parquet     one row per day: Kite's day candle of the same run, the
                                                             bhavcopy row, the conversion and the check

load_minute() rebuilds raw prices from the stored conversion of each day (docs/dev/equity_minute_download.md):
    p' = Kite minute x minute_scale (Kite's minute and day histories are not always on the same scale)
    MULTIPLICATIVE: raw = p' x ratio;  ADDITIVE: raw = kite_multiplier x (p' + kite_difference)
    volume: raw = Kite minute volume x minute_volume_scale x vol_ratio
and "ours" divides raw prices by today's cumulative factor of the day (adjustments.factor_on), volume times it.
With clean=True (default) the stored bars are cleaned first (clean_bars): Kite pads some sessions with all-zero bars,
and a few bars have a negative volume or a high / low that does not contain the open / close."""
import os
import re
from datetime import date

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import config
from backfill.equity import adjustments as adj
from backfill.equity import exits as ex
from backfill.equity import minute_plan as mp
from backfill.equity import security_master as sm

TZ = "Asia/Kolkata"
DATA = "data"
DAYS_FILE = "days.parquet"
MINUTE_COLUMNS = ["ts", "open", "high", "low", "close", "volume"]
PRICES = ("raw", "ours", "kite")
ID_RE = re.compile(r"^S\d{6}$")


class NoMinuteData(mp.MinutePlanError, LookupError):
    pass


def data_dir(folder=None):
    return os.path.join(mp.out_dir(folder), DATA)


def stock_dir(folder, sid):
    return os.path.join(data_dir(folder), sid)


def year_paths(path):
    """{year: path} of the <year>.parquet files in a stock folder."""
    if not os.path.isdir(path):
        return {}
    return {int(n[:4]): os.path.join(path, n) for n in os.listdir(path) if re.fullmatch(r"\d{4}\.parquet", n)}


def minute_frame(candles):
    """Kite candles (dicts with date / open / high / low / close / volume) as the stored frame, ts in IST."""
    df = pd.DataFrame(candles, columns=["date", "open", "high", "low", "close", "volume"]).rename(columns={"date": "ts"})
    ts = pd.to_datetime(df["ts"])
    ts = ts.dt.tz_localize(TZ) if ts.dt.tz is None else ts.dt.tz_convert(TZ)
    out = pd.DataFrame({"ts": ts.astype(f"datetime64[us, {TZ}]")})
    for c in ("open", "high", "low", "close"):
        out[c] = df[c].astype("float64")
    out["volume"] = df["volume"].astype("int64")
    return out


def write_minutes(df, path):
    """One year file, atomic, zstd."""
    table = pa.Table.from_pandas(df[MINUTE_COLUMNS].reset_index(drop=True), preserve_index=False)
    sm.write_atomic(path, lambda tmp: pq.write_table(table, tmp, compression="zstd"))


def write_days(df, path):
    sm.write_atomic(path, lambda tmp: pq.write_table(pa.Table.from_pandas(df.reset_index(drop=True),
                                                                          preserve_index=False), tmp,
                                                     compression="zstd"))


def read_minutes(path, years=None):
    frames = [pq.read_table(p).to_pandas() for y, p in sorted(year_paths(path).items())
              if years is None or y in years]
    if not frames:
        return pd.DataFrame({c: pd.Series(dtype="float64") for c in MINUTE_COLUMNS})
    return pd.concat(frames, ignore_index=True)


def read_days(path):
    p = os.path.join(path, DAYS_FILE)
    if not os.path.isfile(p):
        return None
    df = pq.read_table(p).to_pandas()
    df["date"] = [sm.as_date(d) for d in df["date"]]
    return df


def resolve(key, folder, on_date=None):
    """The security_id for an id (S000123), or any symbol / ISIN the company ever had (security master lookup).
    A name used by several companies is resolved point in time (on_date, else today's holder), then by which one
    has minute data."""
    key = str(key).strip().upper()
    if ID_RE.match(key):
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


def clean_bars(df):
    """(cleaned copy, counts) of minute bars: bars with any of open / high / low / close <= 0 dropped (Kite's all-zero
    padding bars), a negative volume set to 0, and high / low widened to contain open and close. counts: bars (in),
    dropped_nonpositive, volume_set_zero, highlow_widened."""
    o, h, l, c = (df[x].to_numpy(float) for x in ("open", "high", "low", "close"))
    keep = (o > 0) & (h > 0) & (l > 0) & (c > 0)
    out = df[keep].copy()
    v = out["volume"].to_numpy()
    neg = v < 0
    if neg.any():
        out["volume"] = np.where(neg, 0, v).astype(out["volume"].dtype)
    o, h, l, c = (out[x].to_numpy(float) for x in ("open", "high", "low", "close"))
    hi, lo = np.maximum.reduce([h, o, c]), np.minimum.reduce([l, o, c])
    widened = (hi != h) | (lo != l)
    if widened.any():
        out["high"], out["low"] = hi, lo
    return out, {"bars": len(df), "dropped_nonpositive": int((~keep).sum()), "volume_set_zero": int(neg.sum()),
                 "highlow_widened": int(widened.sum())}


def load_minute(security_id_or_symbol, start=None, end=None, prices="raw", folder=None, clean=True):
    """1-minute candles of one company from start to end (dates, inclusive): prices "kite" (as stored), "raw" (each
    day converted with its own stored conversion) or "ours" (raw, then adjusted with our factors, for indicators).
    Columns ts, open, high, low, close, volume, security_id, symbol (the symbol of that day). clean=True (default):
    the stored bars go through clean_bars first and attrs["clean"] holds its counts; clean=False: the stored bars
    exactly."""
    if prices not in PRICES:
        raise ValueError(f"prices must be one of {', '.join(PRICES)}")
    folder = folder or config.equity_data_dir()
    a = sm.as_date(start) if start else None
    b = sm.as_date(end) if end else None
    sid = resolve(security_id_or_symbol, folder, a)
    path = stock_dir(folder, sid)
    days = read_days(path)
    if days is None:
        try:
            e = ex.exit_info(sid, folder)
        except ex.ExitsError:
            e = None
        if e:
            raise NoMinuteData(f"{sid} exited ({e.get('reason', '')}, last day {e.get('last_date', '')}): there is no "
                               "Kite minute data for it; use the bhavcopy (security_master.history / "
                               "adjustments.adjusted_history).")
        raise NoMinuteData(f"{sid}: no minute data downloaded (run: python equity_minute.py download).")
    years = None
    if a or b:
        lo, hi = (a or date(1900, 1, 1)).year, (b or date(2999, 1, 1)).year
        years = set(range(lo, hi + 1))
    m = read_minutes(path, years)
    m["date"] = m["ts"].dt.date if len(m) else pd.Series(dtype=object)
    if a:
        m = m[m["date"] >= a]
    if b:
        m = m[m["date"] <= b]
    counts = None
    if clean:
        m, counts = clean_bars(m)
    cols = ["date", "symbol", "kind", "ratio", "kite_multiplier", "kite_difference", "vol_ratio", "minute_scale",
            "minute_volume_scale"]
    m = m.merge(days[cols], on="date", how="inner").sort_values("ts", kind="stable").reset_index(drop=True)
    out = m[["ts", "open", "high", "low", "close", "volume"]].copy()
    if prices != "kite":
        conv = convert(m)
        for c in ("open", "high", "low", "close", "volume"):
            out[c] = conv[c]
    if prices == "ours":
        f = mp.with_factor(pd.DataFrame({"security_id": sid, "date": m["date"], "_i": np.arange(len(m))}),
                           adj.load_factors(folder)["factors"]).sort_values("_i")["our_factor"].to_numpy(float)
        for c in ("open", "high", "low", "close"):
            out[c] = out[c] / f
        out["volume"] = out["volume"] * f
    out["security_id"] = sid
    out["symbol"] = m["symbol"].to_numpy()
    if clean:
        out.attrs["clean"] = counts
    return out


def convert(m):
    """Raw OHLCV of minute rows joined with their day's conversion columns."""
    s = m["minute_scale"].to_numpy(float)
    add = (m["kind"] == "ADDITIVE").to_numpy()
    ratio, km, kd = (m[c].to_numpy(float) for c in ("ratio", "kite_multiplier", "kite_difference"))
    out = {}
    for c in ("open", "high", "low", "close"):
        p = m[c].to_numpy(float) * s
        out[c] = np.where(add, km * (p + kd), p * ratio)
    out["volume"] = np.round(m["volume"].to_numpy(float) * m["minute_volume_scale"].to_numpy(float)
                             * m["vol_ratio"].to_numpy(float))
    return out

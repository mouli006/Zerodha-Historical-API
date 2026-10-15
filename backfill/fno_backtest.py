"""Read the F&O backtest store built by fno_store.py publish (never the raw download folders).

    from backfill.fno_backtest import load_fno_day, load_fno_range, load_eod, load_days
    day = load_fno_day("NIFTY", "2026-10-12")                                   # best source of the day
    ce = load_fno_day("NIFTY", "2026-10-12", source="kite", expiry="2026-10-13", types=["CE"],
                      strikes=[25000, 25100], drop_zero_volume=True, columns=["contract", "datetime", "close", "oi"])
    df, skipped = load_fno_range("SENSEX", "2026-10-07", "2026-10-15")
    eod = load_eod("NIFTY", "2026-10-01", "2026-10-31")                          # bhavcopy close, settle, OI
    off = load_fno_day("NIFTY", "2026-10-12", variant="without")                 # contracts with volume 0 left out

variant "with" (default) reads <root>/with_zero_volume (contracts whose candles all have volume 0 kept), "without"
<root>/without_zero_volume. root defaults to .env FNO_BACKTEST_DIR (the base of both). Filters are pushed down to pyarrow, so only the needed rows and
columns are read. backfill/fno_loader.py stays the reader of the raw folders."""
import glob
import os
from datetime import date

import pandas as pd

import config

from .fno_publish import SOURCES, eod_path, minute_path


def store_root(root=None, variant="with"):
    """<root or FNO_BACKTEST_DIR>/<with_zero_volume|without_zero_volume>."""
    if variant not in config.VARIANTS:
        raise ValueError(f"variant must be with or without, not {variant!r}")
    root = root or config.fno_backtest_dir()
    if not root:
        raise ValueError("No backtest store: pass root= or set FNO_BACKTEST_DIR in .env.")
    return os.path.join(root, config.VARIANTS[variant])


def as_iso(d):
    return d if isinstance(d, str) else d.isoformat()


def as_list(v):
    return None if v is None else list(v) if isinstance(v, (list, tuple, set)) else [v]


def load_days(underlying, root=None, variant="with"):
    """days.csv: one row per published trading day (see fno_publish.DAYS_COLUMNS), date as a string."""
    base = store_root(root, variant)
    path = os.path.join(base, underlying, "days.csv")
    if not os.path.exists(path):
        raise FileNotFoundError(f"No published {underlying} days in {base}; run python fno_store.py publish first.")
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    for c in ("kite_available", "dhan_available", "eod_available", "kite_derived", "dhan_derived"):
        if c in df:
            df[c] = df[c] == "True"
    return df


def pick_source(underlying, day, source, root, variant="with"):
    if source != "best":
        if source not in SOURCES:
            raise ValueError(f"source must be best, kite or dhan, not {source!r}")
        return source
    days = load_days(underlying, root, variant)
    row = days[days["date"] == day]
    if row.empty:
        raise FileNotFoundError(f"{underlying} {day} is not published in {store_root(root, variant)}.")
    best = row.iloc[0]["best_source"]
    if not best:
        raise FileNotFoundError(f"{underlying} {day} has no published source.")
    return best


def filters_for(expiry=None, types=None, strikes=None, drop_zero_volume=False):
    f = []
    if expiry is not None:
        f.append(("expiry", "in", [date.fromisoformat(e) if isinstance(e, str) else e for e in as_list(expiry)]))
    if types is not None:
        f.append(("type", "in", [t.upper() for t in as_list(types)]))
    if strikes is not None:
        f.append(("strike", "in", [float(s) for s in as_list(strikes)]))
    if drop_zero_volume:
        f.append(("zero_volume", "==", False))
    return f or None


def load_fno_day(underlying, day, source="best", root=None, expiry=None, types=None, strikes=None,
                 drop_zero_volume=False, columns=None, variant="with"):
    """Every contract of one published day (minute bars; columns of fno_publish.MINUTE_SCHEMA). source "best"
    uses days.csv's best_source; "kite" / "dhan" force one. Raises FileNotFoundError when it is not published."""
    day = as_iso(day)
    source = pick_source(underlying, day, source, root, variant)
    base = store_root(root, variant)
    path = minute_path(os.path.join(base, underlying), source, day)
    if not os.path.exists(path):
        raise FileNotFoundError(f"{underlying} {day} is not published for {source} in {base}.")
    return pd.read_parquet(path, columns=columns, filters=filters_for(expiry, types, strikes, drop_zero_volume))


def load_fno_range(underlying, start, end, source="best", root=None, variant="with", **filters):
    """load_fno_day for every published day from start to end, in date order. Returns (frame, skipped dates):
    days in days.csv without data for the source are skipped."""
    start, end = as_iso(start), as_iso(end)
    days = load_days(underlying, root, variant)
    frames, skipped = [], []
    for day in sorted(d for d in days["date"] if start <= d <= end):
        try:
            frames.append(load_fno_day(underlying, day, source, root, variant=variant, **filters))
        except FileNotFoundError:
            skipped.append(day)
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return df, skipped


def load_eod(underlying, start, end, root=None, variant="with"):
    """The bhavcopy rows (fno_loader.EOD_COLUMNS plus date, contract, type, expiry, strike) from start to end."""
    root, start, end = store_root(root, variant), pd.Timestamp(as_iso(start)), pd.Timestamp(as_iso(end))
    frames = [pd.read_parquet(eod_path(os.path.join(root, underlying), y))
              for y in range(start.year, end.year + 1) if os.path.exists(eod_path(os.path.join(root, underlying), y))]
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    return df[(df["date"] >= start) & (df["date"] <= end)].reset_index(drop=True)


def load_contracts(underlying, root=None, variant="with"):
    return pd.read_parquet(os.path.join(store_root(root, variant), underlying, "contracts.parquet"))


def load_expiries(underlying, root=None, variant="with"):
    return pd.read_csv(os.path.join(store_root(root, variant), underlying, "expiries.csv"), dtype=str)


def published_files(underlying, source, root=None, variant="with"):
    """Paths of every published minute file of a source (for scripts that read them directly)."""
    return sorted(glob.glob(os.path.join(store_root(root, variant), underlying, "minute", source, "*", "*.parquet")))

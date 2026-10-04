import os
import sqlite3
from datetime import datetime

import pandas as pd

FILE_FORMATS = {"csv", "parquet"}
WINDOWS_RESERVED = {"CON", "PRN", "AUX", "NUL", *(f"COM{i}" for i in range(10)),
                    *(f"LPT{i}" for i in range(10))}


def run_key(interval, start, end, fmt):
    """Progress key; CSV runs are tracked separately so switching format re-downloads."""
    return f"{interval}:{start}:{end}" + ("" if fmt == "parquet" else f":{fmt}")


def log_error(data_root, run, symbol, chunk_from, msg):
    with open(os.path.join(data_root, "errors.log"), "a", encoding="utf-8") as f:
        f.write(f"{datetime.now():%Y-%m-%d %H:%M:%S} [{run}] {symbol} {chunk_from}: {msg}\n")


def open_db(data_root):
    os.makedirs(data_root, exist_ok=True)
    db = sqlite3.connect(os.path.join(data_root, "progress.sqlite"))
    db.execute("""CREATE TABLE IF NOT EXISTS chunks (
        run TEXT, symbol TEXT, token INTEGER, chunk_from TEXT, chunk_to TEXT,
        status TEXT, tries INTEGER DEFAULT 0, error TEXT, updated TEXT,
        PRIMARY KEY (run, symbol, chunk_from))""")
    return db


def read_year(path, fmt):
    return pd.read_parquet(path) if fmt == "parquet" else pd.read_csv(path, parse_dates=["date"])


def write_year(df, path, fmt, interval):
    if fmt == "parquet":
        df.to_parquet(path, index=False)
    else:
        df.to_csv(path, index=False, date_format="%Y-%m-%d" if interval == "day" else "%Y-%m-%d %H:%M:%S")


def save_candles(data_root, interval, symbol, token, candles, fmt):
    """Merge candles into data/<interval>/<SYMBOL>/<year>.<fmt> without duplicates."""
    df = pd.DataFrame(candles)[["date", "open", "high", "low", "close", "volume"]]
    df["date"] = pd.to_datetime(df["date"])
    if df["date"].dt.tz is not None:
        df["date"] = df["date"].dt.tz_localize(None)  # keep IST wall-clock time
    df["tradingsymbol"] = symbol
    df["instrument_token"] = token

    folder_name = symbol + "_" if symbol.upper() in WINDOWS_RESERVED else symbol
    folder = os.path.join(data_root, interval, folder_name)
    os.makedirs(folder, exist_ok=True)
    for year, part in df.groupby(df["date"].dt.year):
        path = os.path.join(folder, f"{year}.{fmt}")
        if os.path.exists(path):
            part = pd.concat([read_year(path, fmt), part], ignore_index=True)
        part = (part.drop_duplicates("date", keep="last")
                    .sort_values("date").reset_index(drop=True))
        tmp = path + ".tmp"
        write_year(part, tmp, fmt, interval)
        os.replace(tmp, path)

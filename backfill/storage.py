import os
import sqlite3
from datetime import datetime

import pandas as pd

FILE_FORMATS = {"csv", "parquet"}
WINDOWS_RESERVED = {"CON", "PRN", "AUX", "NUL", *(f"COM{i}" for i in range(10)),
                    *(f"LPT{i}" for i in range(10))}


def run_key(interval, start, end, fmt, index=False):
    """Progress key; CSV runs are tracked separately so switching format re-downloads,
    and index runs get ":idx" so they never mix with equity progress."""
    return (f"{interval}:{start}:{end}" + ("" if fmt == "parquet" else f":{fmt}")
            + (":idx" if index else ""))


def job_run_key(interval, start, end, fmt, indices, equity):
    """Label for a whole job: the index run (if any) then the equity run, as they are fetched."""
    keys = ([run_key(interval, start, end, fmt, index=True)] if indices else []) \
        + ([run_key(interval, start, end, fmt)] if equity else [])
    return " + ".join(keys)


INTERVAL_NAMES = {"minute": "1min", "day": "EOD"}
KIND_NAMES = {"equity": "Equity_stocks", "index": "Index_spot"}


def run_dir_name(kind, interval, start, end, started):
    """Folder for one download, e.g. Equity_stocks_1min_2025-10-05_to_2026-10-04_started_2026-10-04_1530."""
    return (f"{KIND_NAMES[kind]}_{INTERVAL_NAMES[interval]}_{start}_to_{end}"
            f"_started_{started:%Y-%m-%d_%H%M}")


def run_dir(db, data_root, run, kind, interval, start, end):
    """The output folder of a run: the one recorded when it first started, so Resume keeps
    writing into it, or a new one named with the current time."""
    row = db.execute("SELECT dir FROM runs WHERE run = ?", (run,)).fetchone()
    if row:
        name = row[0]
    else:
        name = run_dir_name(kind, interval, start, end, datetime.now())
        db.execute("INSERT INTO runs (run, dir, started) VALUES (?, ?, ?)",
                   (run, name, datetime.now().isoformat(timespec="seconds")))
        db.commit()
    return os.path.join(data_root, name)


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
    db.execute("CREATE TABLE IF NOT EXISTS runs (run TEXT PRIMARY KEY, dir TEXT, started TEXT)")
    return db


def read_year(path, fmt):
    return pd.read_parquet(path) if fmt == "parquet" else pd.read_csv(path, parse_dates=["date"])


def write_year(df, path, fmt, interval):
    if fmt == "parquet":
        df.to_parquet(path, index=False)
    else:
        df.to_csv(path, index=False, date_format="%Y-%m-%d" if interval == "day" else "%Y-%m-%d %H:%M:%S")


def save_candles(out_dir, interval, symbol, token, candles, fmt):
    """Merge candles into <run folder>/<SYMBOL>/<year>.<fmt> without duplicates."""
    df = pd.DataFrame(candles)[["date", "open", "high", "low", "close", "volume"]]
    df["date"] = pd.to_datetime(df["date"])
    if df["date"].dt.tz is not None:
        df["date"] = df["date"].dt.tz_localize(None)  # keep IST wall-clock time
    df["tradingsymbol"] = symbol
    df["instrument_token"] = token

    folder_name = symbol.replace(" ", "_")   # "NIFTY 50" -> NIFTY_50; the file keeps the real symbol
    if folder_name.upper() in WINDOWS_RESERVED:
        folder_name += "_"
    folder = os.path.join(out_dir, folder_name)
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

"""Read saved F&O days back for backtests.

    from backfill.fno_loader import load_contract, load_day, load_eod
    df = load_contract(r"D:\\market_data", "NIFTY27OCT26")    # every saved day of one contract
    day = load_day(r"D:\\market_data", "2026-10-12")          # every contract of one day
    eod = load_eod(r"D:\\market_data", "2026-10-12")          # NSE bhavcopy: official close, settlement, OI

Minute frames have the CSV columns plus a leading Datetime column (IST wall-clock time, tz-naive).
For end-of-day close, settlement price and final OI use load_eod, not the last minute bar."""
import glob
import os
import re

import pandas as pd

from .fno import HEADER, fno_root

DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def day_dirs(data_root, underlying="NIFTY"):
    """Saved day folders, oldest first."""
    root = fno_root(data_root, underlying)
    if not os.path.isdir(root):
        return []
    return [os.path.join(root, d) for d in sorted(os.listdir(root))
            if DAY_RE.match(d) and os.path.isdir(os.path.join(root, d))]


def read_contract_csv(path):
    df = pd.read_csv(path, dtype={"Ticker": str, "Date": str, "Time": str})
    df.insert(0, "Datetime", pd.to_datetime(df["Date"] + " " + df["Time"], format="%Y%m%d %H:%M:%S"))
    return df


def combine(frames):
    if not frames:
        return pd.DataFrame(columns=["Datetime", *HEADER])
    return pd.concat(frames, ignore_index=True)


def load_day(data_root, day, underlying="NIFTY", kind="all"):
    """Every contract file of one day; kind is "all", "futures" or "options"."""
    day_dir = os.path.join(fno_root(data_root, underlying), str(day))
    if not os.path.isdir(day_dir):
        raise FileNotFoundError(f"No saved {underlying} F&O day {day} in {fno_root(data_root, underlying)}")
    subs = ("futures", "options") if kind == "all" else (kind,)
    paths = sorted(p for sub in subs for p in glob.glob(os.path.join(day_dir, sub, "*.csv")))
    return combine([read_contract_csv(p) for p in paths])


def load_contract(data_root, ticker, underlying="NIFTY"):
    """One contract (e.g. NIFTY27OCT26 or NIFTY2500013OCT26CE) across every saved day, sorted by time."""
    sub = "options" if ticker.endswith(("CE", "PE")) else "futures"
    paths = [os.path.join(d, sub, ticker + ".csv") for d in day_dirs(data_root, underlying)]
    df = combine([read_contract_csv(p) for p in paths if os.path.exists(p)])
    return df.drop_duplicates("Datetime", keep="last").sort_values("Datetime").reset_index(drop=True)


EOD_COLUMNS = ["Contract", "Expiry", "Type", "Strike", "Close", "Last", "Settle", "OI", "Volume_contracts", "Lot"]


def load_eod(data_root, day, underlying="NIFTY"):
    """The day's NSE bhavcopy (saved by the bhavcopy check) with our contract names. Use it for the
    end-of-day close, settlement price and final OI in backtests: Kite's last 1-minute bar and its OI are
    intraday snapshots. OI is in shares, volume in contracts (as NSE publishes them)."""
    from .bhav import saved_items   # bhav imports kiteconnect via fno; only needed here
    day_dir = os.path.join(fno_root(data_root, underlying), str(day))
    items = saved_items(day_dir, underlying)
    if items is None:
        raise FileNotFoundError(f"No saved bhavcopy for {underlying} {day}; run python nifty_bhav.py {day} first.")
    return pd.DataFrame([[i["name"], pd.Timestamp(i["expiry"]), i["type"], i["strike"] if i["strike"] != "" else None,
                          i["close"], i["last"], i["settle"], i["oi"], i["volume"], i["lot"]]
                         for i in items if not i["error"]], columns=EOD_COLUMNS)

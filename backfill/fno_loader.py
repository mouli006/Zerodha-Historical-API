"""Read saved F&O days back for backtests.

    from backfill.fno_loader import load_contract, load_day, load_eod
    df = load_contract(r"D:\\market_data", "NIFTY27OCT26")    # every saved day of one contract
    day = load_day(r"D:\\market_data", "2026-10-12")          # every contract of one day
    eod = load_eod(r"D:\\market_data", "2026-10-12")          # NSE / BSE bhavcopy: official close, settlement, OI
    dhan = load_day(r"D:\\market_data", "2026-10-12", source="dhan")   # the Dhan files (NIFTY_FnO_1min_Dhan)

Minute frames have the CSV columns plus a leading Datetime column (IST wall-clock time, tz-naive) and a
trailing OI_filled column. A minute with OI 0 in a contract that has non-zero OI earlier or later that day
is a missing value, not a real 0: it is filled from the previous minute (OI_filled True). Zeros before the
day's first non-zero OI have no previous minute and stay NaN; a contract with OI 0 all day is left as is.
fill_oi=False gives the raw values. The saved files are never changed.
For end-of-day close, settlement price and final OI use load_eod, not the last minute bar."""
import glob
import os

import pandas as pd

from .fno import HEADER, day_folders, find_day_dir, fno_root


def day_dirs(data_root, underlying="NIFTY", source="kite"):
    """Saved day folders (old <YYYY-MM-DD> and new <YYYY-MM-DD>_exp-..._dl-... names), oldest first
    (source "dhan": the Dhan folder)."""
    root = fno_root(data_root, underlying, source)
    return [os.path.join(root, d) for _, d in sorted(day_folders(root).items())]


def fill_oi(df):
    """One file (one contract, one day): OI 0 -> NaN when the contract has non-zero OI that day, then
    filled from the previous minute; OI_filled marks the rows that got a value that way."""
    df = df.sort_values("Datetime", kind="stable").reset_index(drop=True)
    oi = pd.to_numeric(df["OI"], errors="coerce").astype(float)
    zero = oi == 0
    if zero.any() and (oi.notna() & ~zero).any():
        filled = oi.mask(zero).ffill()
        oi = oi.where(~zero, filled)
        df["OI_filled"] = zero & oi.notna()
    else:
        df["OI_filled"] = False
    df["OI"] = oi
    return df


def read_contract_csv(path, fill=True):
    df = pd.read_csv(path, dtype={"Ticker": str, "Date": str, "Time": str})
    df.insert(0, "Datetime", pd.to_datetime(df["Date"] + " " + df["Time"], format="%Y%m%d %H:%M:%S"))
    return fill_oi(df) if fill else df


def combine(frames, fill=True):
    if not frames:
        return pd.DataFrame(columns=["Datetime", *HEADER, *(["OI_filled"] if fill else [])])
    return pd.concat(frames, ignore_index=True)


def load_day(data_root, day, underlying="NIFTY", kind="all", fill_oi=True, source="kite"):
    """Every contract file of one day; kind is "all", "futures" or "options". fill_oi: see the module notes.
    source "dhan" reads <UNDERLYING>_FnO_1min_Dhan/."""
    root = fno_root(data_root, underlying, source)
    day_dir = find_day_dir(root, str(day))
    if day_dir is None:
        raise FileNotFoundError(f"No saved {underlying} F&O day {day} in {root}")
    subs = ("futures", "options") if kind == "all" else (kind,)
    paths = sorted(p for sub in subs for p in glob.glob(os.path.join(day_dir, sub, "*.csv")))
    return combine([read_contract_csv(p, fill_oi) for p in paths], fill_oi)


def load_contract(data_root, ticker, underlying="NIFTY", fill_oi=True, source="kite"):
    """One contract (e.g. NIFTY27OCT26 or NIFTY2500013OCT26CE) across every saved day, sorted by time.
    OI is filled within each day only, never from the day before."""
    sub = "options" if ticker.endswith(("CE", "PE")) else "futures"
    paths = [os.path.join(d, sub, ticker + ".csv") for d in day_dirs(data_root, underlying, source)]
    df = combine([read_contract_csv(p, fill_oi) for p in paths if os.path.exists(p)], fill_oi)
    return df.drop_duplicates("Datetime", keep="last").sort_values("Datetime").reset_index(drop=True)


EOD_COLUMNS = ["Contract", "Expiry", "Type", "Strike", "Close", "Last", "Settle", "OI", "Volume_contracts", "Lot"]


def load_eod(data_root, day, underlying="NIFTY", source="kite"):
    """The day's NSE (NIFTY) or BSE (SENSEX) bhavcopy (saved by the bhavcopy check) with our contract
    names. Use it for the end-of-day close, settlement price and final OI in backtests: Kite's last
    1-minute bar and its OI are intraday snapshots. OI is in shares, volume in contracts (BSE publishes
    volume in shares; it is divided by the lot here)."""
    from .bhav import SOURCES, saved_items   # bhav imports kiteconnect via fno; only needed here
    from .fno import exchange_name
    day_dir = find_day_dir(fno_root(data_root, underlying, source), str(day))
    items = saved_items(day_dir, underlying) if day_dir else None
    if items is None:
        flags = (f" --underlying {underlying}" if underlying != "NIFTY" else "") + (f" --source {source}" if source != "kite" else "")
        raise FileNotFoundError(f"No saved bhavcopy for {underlying} {day}; run python nifty_bhav.py {day}{flags} first.")
    shares = SOURCES[exchange_name(underlying)]["volume"] == "shares"
    return pd.DataFrame([[i["name"], pd.Timestamp(i["expiry"]), i["type"], i["strike"] if i["strike"] != "" else None,
                          i["close"], i["last"], i["settle"], i["oi"],
                          i["volume"] / i["lot"] if shares and i["lot"] else i["volume"], i["lot"]]
                         for i in items if not i["error"]], columns=EOD_COLUMNS)

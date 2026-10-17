"""Exit list and listing types from the bhavcopy, the security master, the adjustment factors and the NSE
reference lists (backfill/equity/exits.py). No network, no broker; every input is only read.

    python exits.py build [--folder PATH] [--active-days 20] [--exit-days 60] [--batch-min 20] [--start-days 20]
    python exits.py show HDFC           status, listing kind and the exit row of every security that had it
    python exits.py check               re-run the known-case checks

Per exited security: why it left (merger, delisting, liquidation, SME, relisting, ...), the acquirer and swap ratio
for a merger, the raw and adjusted last close, distress hints and a suggested treatment. The treatment is only a
suggestion: the backtest decides the value. --folder defaults to EQUITY_DATA_DIR, else KITE_DATA_DIR or ./data.
Exit codes: 0 done (build: also when a known case is not found; the report lists it), 1 not run (an input not
built or older than the bhavcopy, a reference list missing or lacking a column, not built yet), show found
nothing, or check failed, 2 bad arguments, another build running, or a bhavcopy download / security master /
adjustments build running."""
import argparse
import os
import sys

import pandas as pd

from backfill.equity import adjustments as adj
from backfill.equity import exits as ex
from backfill.equity import security_master as sm
from config import equity_data_dir


def show(key, folder):
    ids = sm.lookup(key, folder)
    if not ids:
        print(f"{key}: no security ever had this symbol or ISIN.", file=sys.stderr)
        return 1
    with pd.option_context("display.width", 250, "display.max_colwidth", 200):
        for sid in ids:
            info = ex.listing_info(sid, folder)
            print(f"=== {sid}: {info['status']}, first {info['first_date']}, last {info['last_date']} "
                  f"({info['days_since_last']} trading days ago)")
            print(f"Listing: {info['listing_kind']}" + (f" ({info['listing_note']})" if info["listing_note"] else ""))
            e = ex.exit_info(sid, folder)
            if e:
                print(pd.Series(e).drop(labels="security_id").to_string())
            print()
    return 0


def positive(text):
    v = int(text)
    if v < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return v


def main(argv=None):
    p = argparse.ArgumentParser(description="Build and query the exit list and listing types.")
    sub = p.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build", help="full rebuild of <folder>/exits/")
    b.add_argument("--active-days", type=positive, default=ex.ACTIVE_DAYS,
                   help="ACTIVE when traded within this many trading days (default 20)")
    b.add_argument("--exit-days", type=positive, default=ex.EXIT_DAYS,
                   help="EXITED when last traded more than this many trading days ago (default 60)")
    b.add_argument("--batch-min", type=positive, default=ex.BATCH_MIN,
                   help="a batch admission day has at least this many STOCK first appearances (default 20)")
    b.add_argument("--start-days", type=positive, default=ex.START_DAYS,
                   help="first seen within this many trading days of the data start: IN_DATA_FROM_START (default 20)")
    s = sub.add_parser("show", help="status, listing kind and exit of one company")
    s.add_argument("key", help="any symbol or ISIN it ever had")
    sub.add_parser("check", help="re-run the known-case checks")
    for sp in sub.choices.values():
        sp.add_argument("--folder", help="data folder, default EQUITY_DATA_DIR, else KITE_DATA_DIR or ./data")
    args = p.parse_args(argv)   # argparse errors exit with 2
    if args.cmd == "build" and args.exit_days < args.active_days:
        p.error("--exit-days must be at least --active-days")
    if args.folder and not os.path.isabs(args.folder):
        p.error("--folder must be an absolute path")
    folder = args.folder or equity_data_dir()
    try:
        if args.cmd == "build":
            result = ex.build(folder, args.active_days, args.exit_days, args.batch_min, args.start_days)
            print(result["report"], end="")
            print(f"Written to {result['out']}")
            return 0
        if args.cmd == "show":
            return show(args.key, folder)
        lines, ok = ex.check(folder)
        print("\n".join(lines))
        print(f"Check: {'ok' if ok else 'FAILED'}")
        return 0 if ok else 1
    except (ex.LockBusy, ex.StoreBusy) as e:
        print(f"Not run: {e}", file=sys.stderr)
        return 2
    except (ex.ExitsError, sm.SecurityMasterError, adj.AdjustmentError) as e:
        print(f"Not run: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())

"""Security master: one stable security_id per company from the saved NSE equity bhavcopy
(backfill/equity/security_master.py). No network, no broker; NSE_CM_bhav/ is only read.

    python security_master.py build [--folder PATH] [--series EQ,BE,BZ] [--link-gap 10]
    python security_master.py show RELIANCE        one company's full history (any symbol or ISIN it ever had)
    python security_master.py show INE002A01018
    python security_master.py check                re-run the known-case checks on the built files

--folder defaults to EQUITY_DATA_DIR, else KITE_DATA_DIR or ./data; output goes to <folder>/security_master/.
Exit codes: 0 done (build: also when a known case is not found; the report lists it), 1 not run (conflict in the
bhavcopy, no bhavcopy files, not built yet), show found nothing, or check found a known case missing, 2 bad
arguments, another build running, or a bhavcopy download (cm_bhav.py) running."""
import argparse
import os
import sys

import pandas as pd

from backfill.equity import security_master as sm
from config import equity_data_dir


def series_list(text):
    items = [s.strip().upper() for s in text.split(",") if s.strip()]
    if not items:
        raise argparse.ArgumentTypeError("give at least one series, e.g. EQ,BE,BZ")
    return tuple(items)


def show(key, folder):
    ids = sm.lookup(key, folder)
    if not ids:
        print(f"{key}: no security ever had this symbol or ISIN.", file=sys.stderr)
        return 1
    out = sm.out_dir(folder)
    m = sm.load_master(folder)
    events = pd.read_csv(os.path.join(out, "events.csv"), dtype=str, keep_default_na=False)
    review = pd.read_csv(os.path.join(out, "review.csv"), dtype=str, keep_default_na=False)
    with pd.option_context("display.width", 250, "display.max_columns", 30, "display.max_rows", 500,
                           "display.max_colwidth", 120):
        for sid in ids:
            sec = m["securities"][m["securities"]["security_id"] == sid].iloc[0]
            print(f"=== {sid}: {sec.symbols} ({sec.isins}), {sec.first_date} to {sec.last_date}, "
                  f"{sec.trading_days} trading days, series {sec.series_seen}, last close {sec.last_close:g}, "
                  f"{sec.days_since_last} trading days since last seen")
            print("Symbol history:")
            print(m["history"][m["history"]["security_id"] == sid].drop(columns="security_id").to_string(index=False))
            ev = events[events["security_id"] == sid].drop(columns="security_id")
            ev = ev.loc[:, (ev != "").any()]
            print("Events:")
            print(ev.to_string(index=False))
            rv = review[(review["security_id_old"] == sid) | (review["security_id_new"] == sid)]
            if not rv.empty:
                print("Review:")
                print(rv.to_string(index=False))
            print()
    return 0


def main(argv=None):
    p = argparse.ArgumentParser(description="Build and query the security master from <folder>/NSE_CM_bhav/.")
    sub = p.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build", help="full rebuild of <folder>/security_master/")
    b.add_argument("--series", type=series_list, default=sm.SERIES, help="series in scope (default EQ,BE,BZ)")
    b.add_argument("--link-gap", type=int, default=sm.LINK_GAP,
                   help="trading days within which a new ISIN under a stopped symbol is linked (default 10)")
    s = sub.add_parser("show", help="one company's full history")
    s.add_argument("key", help="any symbol or ISIN it ever had")
    sub.add_parser("check", help="re-run the known-case checks on the built files")
    for sp in sub.choices.values():
        sp.add_argument("--folder", help="data folder, default EQUITY_DATA_DIR, else KITE_DATA_DIR or ./data")
    args = p.parse_args(argv)   # argparse errors exit with 2
    if args.cmd == "build" and args.link_gap < 1:
        p.error("--link-gap must be at least 1")
    if args.folder and not os.path.isabs(args.folder):
        p.error("--folder must be an absolute path")
    folder = args.folder or equity_data_dir()
    try:
        if args.cmd == "build":
            result = sm.build(folder, args.series, args.link_gap)
            print(result["report"], end="")
            print(f"Written to {result['out']}")
            return 0
        if args.cmd == "show":
            return show(args.key, folder)
        lines, ok = sm.check(folder)
        print("\n".join(lines))
        print(f"Known-case checks: {'all found' if ok else 'SOME NOT FOUND'}")
        return 0 if ok else 1
    except (sm.LockBusy, sm.BhavcopyBusy) as e:
        print(f"Not run: {e}", file=sys.stderr)
        return 2
    except sm.SecurityMasterError as e:
        print(f"Not run: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())

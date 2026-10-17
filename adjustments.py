"""Split / bonus / consolidation adjustment factors from the saved NSE equity bhavcopy and the security master
(backfill/equity/adjustments.py). No network, no broker; NSE_CM_bhav/ and security_master/ are only read.

    python adjustments.py build [--folder PATH] [--tol 0.25] [--snap-tol 0.03] [--min-price 5] [--kinds STOCK]
    python adjustments.py show RELIANCE        events and the raw vs adjusted close around each ex-date
    python adjustments.py check                re-run the known-case checks and list manual errors

Events are applied only when confirmed by an ISIN change of the security (matching ratio_open) or by the NSE
corporate-actions CSV saved by hand in <folder>/adjustments/input/; manual_factors.csv there adds, removes or sets
events. --folder defaults to EQUITY_DATA_DIR, else KITE_DATA_DIR or ./data.
Exit codes: 0 done (build: also when a known case is not found; the report lists it), 1 not run (security master
not built or older than the bhavcopy, bad input CSV, not built yet), show found nothing, or check failed, 2 bad
arguments, another build running, or a bhavcopy download / security master build running."""
import argparse
import os
import sys
from datetime import timedelta

import pandas as pd

from backfill.equity import adjustments as adj
from backfill.equity import security_master as sm
from config import equity_data_dir


def kinds_list(text):
    items = tuple(s.strip().upper() for s in text.split(",") if s.strip())
    if not items or any(k not in sm.KINDS for k in items):
        raise argparse.ArgumentTypeError(f"kinds from {', '.join(sm.KINDS)}, e.g. STOCK")
    return items


def show(key, folder):
    ids = sm.lookup(key, folder)
    if not ids:
        print(f"{key}: no security ever had this symbol or ISIN.", file=sys.stderr)
        return 1
    events = adj.read_built(folder)
    factors = adj.load_factors(folder)["factors"]
    with pd.option_context("display.width", 250, "display.max_columns", 30, "display.max_rows", 500,
                           "display.max_colwidth", 80):
        for sid in ids:
            print(f"=== {sid}")
            ev = events[events["security_id"] == sid]
            if ev.empty:
                print("No events.")
            else:
                cols = ["ex_date", "symbol", "isin", "series", "close_prev_day", "open", "close", "ratio",
                        "ratio_label", "kind", "factor", "applied", "confirmed_by", "near_isin_change", "note"]
                print(ev[cols].to_string(index=False))
            print("Factors:")
            print(factors[factors["security_id"] == sid].drop(columns="security_id").to_string(index=False))
            for r in ev.itertuples():
                ex = sm.as_date(r.ex_date)
                h = adj.adjusted_history(sid, ex - timedelta(days=10), ex + timedelta(days=10), folder)
                if h.empty:
                    continue
                at = int((h["date"] < ex).sum())
                part = h.iloc[max(at - 3, 0):at + 4][["date", "symbol", "series", "close", "factor", "adj_close"]]
                print(f"Around {r.ex_date} (applied {r.applied}):")
                print(part.to_string(index=False))
            print()
    return 0


def main(argv=None):
    p = argparse.ArgumentParser(description="Build and query the split / bonus adjustment factors.")
    sub = p.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build", help="full rebuild of <folder>/adjustments/")
    b.add_argument("--tol", type=float, default=adj.TOL,
                   help="price candidate when |close(p) / open(d) - 1| is above this (default 0.25)")
    b.add_argument("--snap-tol", type=float, default=adj.SNAP_TOL,
                   help="a ratio snaps to a known split/bonus ratio within this (default 0.03)")
    b.add_argument("--min-price", type=float, default=adj.MIN_PRICE,
                   help="price candidates need the previous close at or above this (default 5)")
    b.add_argument("--kinds", type=kinds_list, default=adj.KINDS, help="instrument kinds (default STOCK)")
    s = sub.add_parser("show", help="events and adjusted closes of one company")
    s.add_argument("key", help="any symbol or ISIN it ever had")
    sub.add_parser("check", help="re-run the known-case checks and list manual errors")
    for sp in sub.choices.values():
        sp.add_argument("--folder", help="data folder, default EQUITY_DATA_DIR, else KITE_DATA_DIR or ./data")
    args = p.parse_args(argv)   # argparse errors exit with 2
    if args.cmd == "build" and not (0 < args.tol and 0 < args.snap_tol < 0.5 and args.min_price >= 0):
        p.error("--tol and --snap-tol must be positive (--snap-tol below 0.5), --min-price at least 0")
    if args.folder and not os.path.isabs(args.folder):
        p.error("--folder must be an absolute path")
    folder = args.folder or equity_data_dir()
    try:
        if args.cmd == "build":
            result = adj.build(folder, args.tol, args.snap_tol, args.min_price, args.kinds)
            print(result["report"], end="")
            print(f"Written to {result['out']}")
            return 0
        if args.cmd == "show":
            return show(args.key, folder)
        lines, ok = adj.check(folder)
        print("\n".join(lines))
        print(f"Check: {'ok' if ok else 'FAILED'}")
        return 0 if ok else 1
    except (adj.LockBusy, adj.StoreBusy) as e:
        print(f"Not run: {e}", file=sys.stderr)
        return 2
    except (adj.AdjustmentError, sm.SecurityMasterError) as e:
        print(f"Not run: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())

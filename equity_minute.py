"""Kite 1-minute and EOD plan, phase 1 (backfill/equity/minute_plan.py): the stocks to download from Kite, their
instrument tokens, from which day Kite has day and minute candles, and the vendor list of what Kite lacks.
Downloads no candles to keep.

    python equity_minute.py tokens                Kite instrument list (one request) -> kite_tokens.csv
    python equity_minute.py probe [--speed 2.5] [--force] [--price-tol 0.02]
                                                  day candles + first minute day per matched stock, then gaps
    python equity_minute.py gaps                  rebuild plan.csv, kite_gaps.csv, ... offline
    python equity_minute.py show RELIANCE         the plan, token, gaps and adjustment periods of one company

Every command takes --folder (absolute; default EQUITY_DATA_DIR, else KITE_DATA_DIR or ./data) and --include-etf.
Output: <folder>/equity_1min/plan/. Only tokens and probe use the network (Kite only; the token from .env is never
printed). Exit codes: 0 done; 1 an input not built or stale, tokens not run yet, some probes failed or the probe
stopped, show found nothing; 2 bad arguments, a lock busy, an input build or bhavcopy download running, a Kite F&O
run busy, or Kite sign-in needed."""
import argparse
import os
import sys

import pandas as pd

from backfill.equity import adjustments as adj
from backfill.equity import exits as ex
from backfill.equity import index_membership as im
from backfill.equity import minute_plan as mp
from backfill.equity import security_master as sm
from config import equity_data_dir


def speed(text):
    v = float(text)
    if not 0 < v <= mp.MAX_SPEED:
        raise argparse.ArgumentTypeError(f"must be more than 0 and at most {mp.MAX_SPEED:g}")
    return v


def fraction(text):
    v = float(text)
    if not 0 < v < 1:
        raise argparse.ArgumentTypeError("must be between 0 and 1")
    return v


def show(key, folder):
    ids = sm.lookup(key, folder)
    if not ids:
        print(f"{key}: no security ever had this symbol or ISIN.", file=sys.stderr)
        return 1
    plan = mp.read_built(folder, "plan.csv")
    gaps = mp.read_built(folder, "kite_gaps.csv")
    toks = mp.read_tokens(folder)
    periods = mp.read_built(folder, "kite_adjustment.csv")
    mism = mp.read_built(folder, "eod_mismatch.csv")
    found = 0
    with pd.option_context("display.width", 250, "display.max_colwidth", 120):
        for sid in ids:
            p = plan[plan["security_id"] == sid]
            if p.empty:
                continue
            found += 1
            print(f"=== {sid}")
            print(p.iloc[0].drop(labels="security_id").to_string())
            t = toks[toks["security_id"] == sid]
            if len(t):
                print("Token:", ", ".join(f"{k}={v}" for k, v in t.iloc[0].items() if k != "security_id" and v))
            g = gaps[gaps["security_id"] == sid]
            print(f"Gaps: {len(g)}")
            if len(g):
                print(g[["data_missing", "reason", "need_from", "need_to", "trading_days", "priority",
                         "note"]].to_string(index=False))
            a = periods[periods["security_id"] == sid]
            if len(a):
                print("Kite adjustment periods:")
                print(a.drop(columns=["security_id"]).to_string(index=False))
            m = mism[mism["security_id"] == sid]
            print(f"EOD mismatch rows: {len(m)}")
            print()
    if not found:
        print(f"{key}: not in the plan (not a STOCK, or the plan is older than the master).", file=sys.stderr)
        return 1
    return 0


def main(argv=None):
    p = argparse.ArgumentParser(description="Kite 1-minute / EOD plan: tokens, availability probe, gap list.")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("tokens", help="download Kite's NSE instrument list and match every ACTIVE stock")
    pr = sub.add_parser("probe", help="day candles and the first minute day of every matched stock, then gaps")
    pr.add_argument("--speed", type=speed, default=mp.SPEED, help="requests per second (default 2.5, at most 3)")
    pr.add_argument("--force", action="store_true", help="probe every matched stock again")
    pr.add_argument("--price-tol", type=fraction, default=mp.PRICE_TOL,
                    help="Kite close vs bhavcopy close on the last day (default 0.02)")
    sub.add_parser("gaps", help="rebuild every output offline")
    s = sub.add_parser("show", help="the plan of one company")
    s.add_argument("key", help="any symbol or ISIN it ever had")
    for sp in sub.choices.values():
        sp.add_argument("--folder", help="data folder, default EQUITY_DATA_DIR, else KITE_DATA_DIR or ./data")
        sp.add_argument("--include-etf", action="store_true", help="also ETFs (rights entitlements never)")
    args = p.parse_args(argv)   # argparse errors exit with 2
    if args.folder and not os.path.isabs(args.folder):
        p.error("--folder must be an absolute path")
    folder = args.folder or equity_data_dir()
    try:
        if args.cmd == "tokens":
            result = mp.tokens(folder, include_etf=args.include_etf)
            print(f"Written to {result['out']}")
            return 0
        if args.cmd == "probe":
            result = mp.probe(folder, speed=args.speed, force=args.force, price_tol=args.price_tol,
                              include_etf=args.include_etf)
            st = result["stats"]
            print(f"Probe: {st['probed']:,} probed, {st['skipped']:,} already done, {st['failed']:,} failed, "
                  f"{st['requests']:,} requests. Written to {result['out']}")
            if st["stopped"]:
                print(f"Stopped at {st['stopped']}; run probe again to go on.", file=sys.stderr)
                return 1
            return 1 if st["failed"] else 0
        if args.cmd == "gaps":
            result = mp.gaps(folder, include_etf=args.include_etf)
            print(f"Written to {result['out']}")
            return 0
        return show(args.key, folder)
    except mp.SignInNeeded as e:
        print(f"Kite sign-in needed: {e}", file=sys.stderr)
        return 2
    except (mp.LockBusy, mp.StoreBusy, mp.FnoBusy) as e:
        print(f"Not run: {e}", file=sys.stderr)
        return 2
    except (mp.MinutePlanError, sm.SecurityMasterError, adj.AdjustmentError, ex.ExitsError,
            im.IndexMembershipError) as e:
        print(f"Not run: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())

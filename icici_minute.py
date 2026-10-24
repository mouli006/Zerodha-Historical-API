"""ICICI Breeze 1-minute data for the stock-days Kite lacks (backfill/equity/icici_plan.py, icici_download.py,
icici_store.py; docs/dev/equity_icici_source.md). A separate source and store: equity_1min_icici/; the Kite store
equity_1min/ is only read.

    python icici_minute.py plan [--market-wide 1|2|3] [--offline]
                                     the work list from kite_gaps.csv minus the days the Kite store has, matched to
                                     ICICI stock codes (ISIN first); one download of ICICI's security master
    python icici_minute.py download [--priority 1|2|3] [--symbols A,B] [--limit N] [--max-calls 4800]
                                    [--days-per-stock N] [--speed 90]
                                     ICICI 1-minute and day candles, one stock at a time, paced (calls a minute) and
                                     capped (calls a day, counted across runs); resumable
    python icici_minute.py status    plan totals, progress, calls used today, errors, REVIEW stocks (offline)
    python icici_minute.py checks    rebuild check/icici_check.csv, summary.json, scale_steps.csv (offline)
    python icici_minute.py coverage  which kite_gaps.csv rows are filled, partly filled or still missing (offline)
    python icici_minute.py show KEY  the plan row, progress and stored days of one company (offline)

Before download: log in to ICICI (Breeze) and put today's session token in .env as BREEZE_SESSION_TOKEN (it is new
every day; BREEZE_API_KEY stays). The values are never printed. Every command takes --folder (absolute; default
EQUITY_DATA_DIR, else KITE_DATA_DIR or ./data). Exit codes: 0 done, stopped by the user or the daily cap; 1 not
built / stale input, some chunks FAILED, stopped by a repeated rate limit, show found nothing; 2 bad arguments, a lock
busy, equity_update.py or an input build running, ICICI log-in needed."""
import argparse
import os
import sys

import pandas as pd

from backfill.equity import adjustments as adj
from backfill.equity import exits as ex
from backfill.equity import icici_download as idl
from backfill.equity import icici_plan as ip
from backfill.equity import index_membership as im
from backfill.equity import minute_plan as mp
from backfill.equity import security_master as sm
from backfill.equity.icici_client import IciciError, IciciSignIn
from config import equity_data_dir


def positive(text):
    v = int(text)
    if v < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return v


def speed(text):
    v = float(text)
    if not 0 < v <= ip.MAX_SPEED:
        raise argparse.ArgumentTypeError(f"must be more than 0 and at most {ip.MAX_SPEED} calls a minute")
    return v


def download_result(res):
    s = res["stats"]
    print(f"download: {s['stocks']:,} stocks, {s['built']:,} builds, {s['calls']:,} calls this run "
          f"({s.get('calls_today', 0):,} today), {s['chunks']:,} minute chunks, {s['rows']:,} bars, "
          f"{s.get('days_stored', 0):,} days with minutes, {s['empty_days']:,} ICICI_EMPTY, "
          f"{s['before_start_days']:,} ICICI_BEFORE_START, {s['failed_chunks']:,} failed, {len(s['review'])} REVIEW, "
          f"peak memory {s['peak_mb'] / 1e3:.2f} GB")
    if s.get("failed"):
        print(f"Not run: {s['failed']}", file=sys.stderr)
        return 1
    if s["rate_stop"]:
        print(f"Stopped: {s['rate_stop']}", file=sys.stderr)
        return 1
    if s["cap"]:
        print("Daily cap reached: resume tomorrow with the same command.", file=sys.stderr)
        return 0
    if s["stopped"]:
        print("Stopped, resume with the same command.", file=sys.stderr)
        return 0
    if s["failed_chunks"]:
        print("Some chunks FAILED: run the same command again to retry them.", file=sys.stderr)
        return 1
    return 0


def show(key, folder, n):
    ids = sm.lookup(key, folder)
    if not ids:
        print(f"{key}: no security ever had this symbol or ISIN.", file=sys.stderr)
        return 1
    plan, _ = ip.read_plan(folder)
    db = idl.Progress(folder, write=False)
    stocks = db.stocks()
    starts = db.start_checks()
    db.close()
    found = 0
    cols = ["date", "symbol", "check", "minute_rows", "bars_outside", "first_bar", "last_bar", "icici_close",
            "bhav_close", "ratio", "icici_scale", "icici_volume_scale", "vol_ratio", "minute_volume", "bhav_volume",
            "conversion_from"]
    with pd.option_context("display.width", 250, "display.max_columns", 30, "display.max_colwidth", 60):
        for sid in ids:
            p = plan[plan["security_id"] == sid]
            if p.empty:
                continue
            found += 1
            print(f"=== {sid}")
            print(p.iloc[0].drop(labels="security_id").to_string())
            s = stocks.get(sid)
            if s:
                print("Progress:", ", ".join(f"{k}={v}" for k, v in s.items() if k != "security_id" and v))
            c = starts.get(sid)
            if c:
                print("Start check:", ", ".join(f"{k}={v}" for k, v in c.items() if k != "security_id" and v))
            d = idl.stored_days(folder, sid)
            if d is not None:
                counts = {}
                for c in d["check"]:
                    for x in c.split("|"):
                        counts[x] = counts.get(x, 0) + 1
                print(f"Stored days: {len(d):,} ({int((d['minute_rows'] > 0).sum()):,} with minutes); checks: " +
                      ", ".join(f"{k} {v:,}" for k, v in sorted(counts.items())))
                with_min = d[d["minute_rows"] > 0]
                print(with_min[cols].head(n).to_string(index=False))
            print()
    if not found:
        print(f"{key}: not in the ICICI plan.", file=sys.stderr)
        return 1
    return 0


def main(argv=None):
    p = argparse.ArgumentParser(description="ICICI Breeze 1-minute data for the Kite gaps.")
    sub = p.add_subparsers(dest="cmd", required=True)
    pl = sub.add_parser("plan", help="the work list (one download of ICICI's security master)")
    pl.add_argument("--market-wide", type=int, choices=(1, 2, 3), default=ip.MARKET_WIDE,
                    help="add the market-wide gap days for live stocks of priority <= N (default 1)")
    pl.add_argument("--offline", action="store_true", help="use the newest saved ICICI security master")
    dl = sub.add_parser("download", help="ICICI 1-minute candles of the plan's stocks (resumable)")
    dl.add_argument("--priority", type=int, choices=(1, 2, 3), help="only this priority")
    dl.add_argument("--symbols", help="only these companies, comma-separated (any symbol they ever had)")
    dl.add_argument("--limit", type=positive, help="at most this many stocks still to do")
    dl.add_argument("--max-calls", type=positive, default=ip.MAX_CALLS,
                    help=f"calls a day, counted across runs (default {ip.MAX_CALLS:,}; ICICI allows 5,000)")
    dl.add_argument("--days-per-stock", type=positive,
                    help="at most this many minute days per stock in this run (a trial; the first-minute-day "
                         "search is not counted)")
    dl.add_argument("--speed", type=speed, default=ip.SPEED,
                    help=f"calls a minute (default {ip.SPEED}, at most {ip.MAX_SPEED})")
    stt = sub.add_parser("status", help="progress, offline")
    stt.add_argument("--max-calls", type=positive, default=ip.MAX_CALLS, help="the daily cap to compare with")
    sub.add_parser("checks", help="rebuild the check files, offline")
    sub.add_parser("coverage", help="which kite_gaps.csv rows are filled, offline")
    sh = sub.add_parser("show", help="one company: plan, progress, stored days")
    sh.add_argument("key", help="any symbol or ISIN it ever had")
    sh.add_argument("--days", type=positive, default=5, help="stored days to print (default 5)")
    for sp in sub.choices.values():
        sp.add_argument("--folder", help="data folder, default EQUITY_DATA_DIR, else KITE_DATA_DIR or ./data")
    args = p.parse_args(argv)   # argparse errors exit with 2
    if args.folder and not os.path.isabs(args.folder):
        p.error("--folder must be an absolute path")
    folder = args.folder or equity_data_dir()
    try:
        if args.cmd == "plan":
            res = ip.plan(folder, market_wide=args.market_wide, offline=args.offline)
            print(f"Written to {res['out']}")
            return 0
        if args.cmd == "download":
            symbols = [s.strip() for s in args.symbols.split(",") if s.strip()] if args.symbols else None
            res = idl.download(folder, priority=args.priority, symbols=symbols, limit=args.limit,
                               max_calls=args.max_calls, days_per_stock=args.days_per_stock, speed=args.speed)
            return download_result(res)
        if args.cmd == "status":
            idl.status(folder, max_calls=args.max_calls)
            return 0
        if args.cmd == "checks":
            idl.checks(folder)
            return 0
        if args.cmd == "coverage":
            df = idl.coverage(folder, write=True)
            print(idl.coverage_text(df))
            return 0
        return show(args.key, folder, args.days)
    except (mp.SignInNeeded, IciciSignIn) as e:
        print(f"ICICI log-in needed: {e}", file=sys.stderr)
        return 2
    except (mp.LockBusy, mp.StoreBusy) as e:
        print(f"Not run: {e}", file=sys.stderr)
        return 2
    except (mp.MinutePlanError, sm.SecurityMasterError, adj.AdjustmentError, ex.ExitsError,
            im.IndexMembershipError, IciciError) as e:
        print(f"Not run: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())

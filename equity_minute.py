"""Kite 1-minute and EOD plan, phase 1 (backfill/equity/minute_plan.py): the stocks to download from Kite, their
instrument tokens, from which day Kite has day and minute candles, and the vendor list of what Kite lacks.
Downloads no candles to keep.

    python equity_minute.py tokens                Kite instrument list (one request) -> kite_tokens.csv
    python equity_minute.py tokens --rematch      match again from the newest saved list, offline
    python equity_minute.py probe [--speed 2.5] [--force] [--price-tol 0.02]
                                                  day candles + first minute day per matched stock, then gaps
    python equity_minute.py gaps                  rebuild plan.csv, kite_gaps.csv, ... offline
    python equity_minute.py show RELIANCE         the plan, token, gaps and adjustment periods of one company

Phase 2 (backfill/equity/minute_download.py), each with --folder:
    python equity_minute.py download [--dry-run] [--priority 1|2|3] [--symbols A,B] [--limit N] [--speed 2.5]
                                     [--workers 2]    the 1-minute history of every plan stock (resumable)
    python equity_minute.py update [--speed 2.5] [--workers 2]   the daily update, after equity_update.py: stores
                                                  up to the target day (finished from 01:00 IST the next day)
    python equity_minute.py status                progress, offline
    python equity_minute.py checks [--apply]      the check labels of the built stocks with the current rules,
                                                  offline (dry run unless --apply)
    python equity_minute.py retoken [--apply]     match the stocks not DONE again in Kite's list (one request, a
                                                  price check per new token): dry run unless --apply, which writes
                                                  update/tokens.csv and marks NO_KITE_DATA stocks
    python equity_minute.py repair [--apply] [--limit N] [--speed 2.5]   day candles stored before they were final,
                                                  fetched again (Kite requests even in the dry run; --apply writes)
--workers is 1 to 4 (default 2); all workers share one --speed limit.

Every command takes --folder (absolute; default EQUITY_DATA_DIR, else KITE_DATA_DIR or ./data) and --include-etf.
Output: <folder>/equity_1min/plan/. Only tokens (not --rematch) and probe use the network (Kite only; the token
from .env is never printed). Exit codes: 0 done; 1 an input not built or stale, tokens not run yet (or no saved list
for --rematch), some probes failed or the probe stopped, show found nothing, retoken left a stock to REVIEW; 2 bad
arguments, a lock busy, an input
build or bhavcopy download running, a Kite F&O run busy, or Kite sign-in needed."""
import argparse
import os
import sys

import pandas as pd

from backfill.equity import adjustments as adj
from backfill.equity import exits as ex
from backfill.equity import index_membership as im
from backfill.equity import minute_download as md
from backfill.equity import minute_plan as mp
from backfill.equity import minute_repair as mr
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


def positive(text):
    v = int(text)
    if v < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return v


def run_result(stats, cmd):
    """Exit code of a download / update run: 0 done or stopped by the user, 1 some chunks failed."""
    print(f"{cmd}: {stats['stocks']:,} stocks, {stats['built']:,} built, {stats['chunks']:,} chunks, "
          f"{stats['rows']:,} rows, {stats['requests']:,} requests, {stats['n429']:,} x 429, "
          f"{stats['failed_chunks']:,} failed, {stats.get('deferred_days', 0):,} deferred days, "
          f"peak memory {stats.get('peak_mb', 0) / 1e3:.2f} GB")
    if stats["stopped"]:
        print("Stopped, resume with the same command.", file=sys.stderr)
        return 0
    if stats["failed_chunks"]:
        print("Some chunks FAILED: run the same command again to retry them.", file=sys.stderr)
        return 1
    return 0


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
    tk = sub.add_parser("tokens", help="download Kite's NSE instrument list and match every ACTIVE stock")
    tk.add_argument("--rematch", action="store_true",
                    help="match again from the newest saved instrument list, offline (no Kite request)")
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
    dl = sub.add_parser("download", help="phase 2: the 1-minute history of every stock in plan.csv (resumable)")
    dl.add_argument("--dry-run", action="store_true", help="show the work list figures only")
    dl.add_argument("--priority", type=int, choices=(1, 2, 3), help="only this group (1 NIFTY 50, 2 NIFTY 500, 3 rest)")
    dl.add_argument("--symbols", help="only these companies, comma-separated (any symbol they ever had)")
    dl.add_argument("--limit", type=positive, help="at most this many stocks still to download")
    up = sub.add_parser("update", help="phase 2: the evening update (after equity_update.py)")
    st = sub.add_parser("status", help="phase 2: download progress, offline")
    ck = sub.add_parser("checks", help="phase 2: the check labels of the built stocks again, offline")
    ck.add_argument("--apply", action="store_true", help="rewrite the changed labels (default: dry run)")
    ck.add_argument("--folder", help="data folder, default EQUITY_DATA_DIR, else KITE_DATA_DIR or ./data")
    rt = sub.add_parser("retoken", help="phase 2: match the stocks not DONE again in Kite's list (dry run)")
    rt.add_argument("--apply", action="store_true", help="write the checked tokens and NO_KITE_DATA states")
    rt.add_argument("--speed", type=speed, default=md.SPEED, help="requests per second (default 2.5, at most 3)")
    rt.add_argument("--folder", help="data folder, default EQUITY_DATA_DIR, else KITE_DATA_DIR or ./data")
    rp = sub.add_parser("repair", help="phase 2: fetch again the day candles stored before they were final (dry run)")
    rp.add_argument("--apply", action="store_true", help="write the repaired days.parquet files (backup first)")
    rp.add_argument("--limit", type=positive, help="at most this many stocks (a small trial)")
    rp.add_argument("--speed", type=speed, default=md.SPEED, help="requests per second (default 2.5, at most 3)")
    rp.add_argument("--folder", help="data folder, default EQUITY_DATA_DIR, else KITE_DATA_DIR or ./data")
    for sp in (dl, up, st):
        sp.add_argument("--speed", type=speed, default=md.SPEED, help="requests per second (default 2.5, at most 3)")
        sp.add_argument("--workers", type=int, choices=range(1, md.MAX_WORKERS + 1), default=md.WORKERS,
                        metavar="1-4", help="parallel workers sharing the --speed limit (default 2)")
        sp.add_argument("--folder", help="data folder, default EQUITY_DATA_DIR, else KITE_DATA_DIR or ./data")
    args = p.parse_args(argv)   # argparse errors exit with 2
    if args.folder and not os.path.isabs(args.folder):
        p.error("--folder must be an absolute path")
    folder = args.folder or equity_data_dir()
    try:
        if args.cmd == "tokens":
            result = mp.tokens(folder, include_etf=args.include_etf, rematch=args.rematch)
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
        if args.cmd == "download":
            symbols = [s.strip() for s in args.symbols.split(",") if s.strip()] if args.symbols else None
            result = md.download(folder, priority=args.priority, symbols=symbols, limit=args.limit, speed=args.speed,
                                 workers=args.workers, dry_run=args.dry_run)
            if result["dry_run"]:
                return 0
            return run_result(result["stats"], "download")
        if args.cmd == "update":
            return run_result(md.update(folder, speed=args.speed, workers=args.workers)["stats"], "update")
        if args.cmd == "status":
            md.status(folder, speed=args.speed, workers=args.workers)
            return 0
        if args.cmd == "checks":
            return 1 if md.checks(folder, apply=args.apply)["refused"] else 0
        if args.cmd == "repair":
            return 1 if mr.repair(folder, apply=args.apply, limit=args.limit, speed=args.speed)["refused"] else 0
        if args.cmd == "retoken":
            return 1 if md.retoken(folder, apply=args.apply, speed=args.speed)["review"] else 0
        return show(args.key, folder)
    except mp.SignInNeeded as e:
        print(f"Kite sign-in needed: {e}", file=sys.stderr)
        return 2
    except (mp.LockBusy, mp.StoreBusy, mp.FnoBusy, md.NotEnoughDisk) as e:
        print(f"Not run: {e}", file=sys.stderr)
        return 2
    except (mp.MinutePlanError, sm.SecurityMasterError, adj.AdjustmentError, ex.ExitsError,
            im.IndexMembershipError) as e:
        print(f"Not run: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())

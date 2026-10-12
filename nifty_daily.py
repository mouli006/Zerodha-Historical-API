"""NIFTY F&O 1-minute download. Run it by hand; a date for today works after 15:40 IST.

    python nifty_daily.py YYYY-MM-DD [--folder PATH] [--speed 2.5] [--underlying NIFTY]
    python nifty_daily.py --missing  [--folder PATH] [--speed 2.5] [--underlying NIFTY]
    python nifty_daily.py --refetch-no-trade YYYY-MM-DD [--folder PATH] [--speed 2.5]
    python nifty_daily.py --inspect CONTRACT YYYY-MM-DD    (e.g. NIFTY2415013OCT26PE 2026-10-07)
    python nifty_daily.py --inspect-missing YYYY-MM-DD

--inspect prints Kite's raw candles for one contract (and the saved bhavcopy row); --inspect-missing
does it for every MISSING_IN_KITE contract of the day's bhav_compare.csv and prints a table. Neither
writes anything to the data folder.

Allowed dates run from the day after the previous expiry to today: on those days every contract
is still live on Kite, so the download is complete. --missing downloads every such date without a
complete folder. --keep-zero-volume / --no-keep-zero-volume (default KEEP_ZERO_VOLUME in .env, else
config.py) keeps or skips contracts whose Kite candles all have volume 0 (Kite can report a single trade
that way). --refetch-no-trade requests every contract of a saved day that has no file again with the
setting ON, then checks the day against its saved bhavcopy again. Reads KITE_API_KEY and KITE_ACCESS_TOKEN from .env (sign in on the dashboard first).
Exit codes: 0 finished or market holiday, 1 failed contracts or not run, 2 token missing or
invalid, 3 date outside the allowed window."""
import argparse
import os
import sys
from datetime import date

from backfill.fno import (INSPECT_COLUMNS, UNDERLYINGS, LockBusy, acquire_lock, fno_root, inspect_contracts,
                          job_fields, missing_in_kite, refetch_no_trade, run_daily, run_missing, table)
from backfill.job import job_begin
from backfill.storage import check_folder
from config import data_dir, keep_zero_volume, load_env, weekly_expiry_weekday

EXIT = {"finished": 0, "holiday": 0, "auth_failed": 2, "outside_window": 3}


class Parser(argparse.ArgumentParser):
    def error(self, message):   # argparse exits with 2, which is kept for token problems
        self.print_usage(sys.stderr)
        print(f"error: {message}", file=sys.stderr)
        sys.exit(1)


def main(argv=None):
    p = Parser(description="Download 1-minute candles with OI for every NIFTY F&O contract of a day.")
    which = p.add_mutually_exclusive_group(required=True)
    which.add_argument("date", nargs="?", help="YYYY-MM-DD, from the day after the previous expiry to today")
    which.add_argument("--missing", action="store_true", help="every allowed date without a complete folder")
    which.add_argument("--refetch-no-trade", metavar="DATE",
                       help="request every contract of a saved day without a file again (keep zero-volume ON)")
    which.add_argument("--inspect", nargs=2, metavar=("CONTRACT", "DATE"),
                       help="print Kite's raw candles for one contract on a date; writes nothing")
    which.add_argument("--inspect-missing", metavar="DATE",
                       help="--inspect every MISSING_IN_KITE contract of the day's bhavcopy check; writes nothing")
    p.add_argument("--folder", help="save folder, default the dashboard's (KITE_DATA_DIR) or ./data")
    p.add_argument("--speed", type=float, default=2.5, help="requests per second, at most 3 (default 2.5)")
    p.add_argument("--underlying", default="NIFTY", choices=sorted(UNDERLYINGS))
    p.add_argument("--keep-zero-volume", action=argparse.BooleanOptionalAction, default=None,
                   help="keep contracts whose candles all have volume 0 (default from .env KEEP_ZERO_VOLUME / config.py)")
    args = p.parse_args(argv)

    run_date = None
    day = args.date or args.refetch_no_trade or (args.inspect[1] if args.inspect else args.inspect_missing)
    if day:
        try:
            run_date = date.fromisoformat(day)
        except ValueError:
            p.error(f"invalid date {day!r}, use YYYY-MM-DD")
    if not 0 < args.speed <= 3:
        p.error("--speed must be above 0 and at most 3")
    if args.inspect or args.inspect_missing:
        return inspect(args, run_date)
    keep_zero = keep_zero_volume() if args.keep_zero_volume is None else args.keep_zero_volume
    if args.refetch_no_trade:
        if args.keep_zero_volume is False:
            p.error("--refetch-no-trade keeps zero-volume contracts; drop --no-keep-zero-volume")
        keep_zero = True
    folder, err = check_folder(args.folder or data_dir())
    if err:
        print(f"Not run: {err}", file=sys.stderr)
        return 1

    env = load_env()
    api_key, token = env.get("KITE_API_KEY", ""), env.get("KITE_ACCESS_TOKEN", "")
    if not api_key or not token:
        print("KITE_API_KEY or KITE_ACCESS_TOKEN is missing from .env. Sign in on the dashboard.", file=sys.stderr)
        return 2

    try:
        lock = acquire_lock(folder, args.underlying, "nifty_daily.py")
    except LockBusy as e:
        print(f"Not run: {e}", file=sys.stderr)
        return 1
    try:
        job_begin(**job_fields(args.underlying, run_date, folder, args.speed, keep_zero))
        if args.missing:
            status = run_missing(api_key, token, folder, args.speed, args.underlying, weekday=weekly_expiry_weekday(),
                                 keep_zero=keep_zero)
        elif args.refetch_no_trade:
            status, _ = refetch_no_trade(api_key, token, run_date, folder, args.speed, args.underlying,
                                         weekday=weekly_expiry_weekday())
        else:
            status = run_daily(api_key, token, run_date, folder, args.speed, args.underlying,
                               weekday=weekly_expiry_weekday(), keep_zero=keep_zero)
    except KeyboardInterrupt:
        print("Interrupted. Finished contracts are saved; run again to continue.", file=sys.stderr)
        return 1
    finally:
        lock.release()
    return EXIT.get(status, 1)   # run_daily / run_missing have printed why


def inspect(args, run_date):
    """--inspect / --inspect-missing: no folder check, no lock, nothing written."""
    folder = os.path.normpath(os.path.expanduser((args.folder or data_dir()).strip()))
    if args.inspect:
        names = [args.inspect[0].strip().upper()]
    else:
        names = missing_in_kite(os.path.join(fno_root(folder, args.underlying), run_date.isoformat()))
        if names is None:
            print(f"Not run: {run_date} has no bhav_compare.csv in {folder}; run the bhavcopy check first.",
                  file=sys.stderr)
            return 1
        if not names:
            print(f"{run_date}: no MISSING_IN_KITE contracts.")
            return 0
        print(f"{run_date}: {len(names)} MISSING_IN_KITE contracts")
    env = load_env()
    api_key, token = env.get("KITE_API_KEY", ""), env.get("KITE_ACCESS_TOKEN", "")
    if not api_key or not token:
        print("KITE_API_KEY or KITE_ACCESS_TOKEN is missing from .env. Sign in on the dashboard.", file=sys.stderr)
        return 2
    status, rows = inspect_contracts(api_key, token, run_date, folder, names, args.underlying, args.speed,
                                     verbose=bool(args.inspect))
    if status:
        return EXIT.get(status, 1)
    if args.inspect_missing:
        print(table(INSPECT_COLUMNS, rows))
        results = {}
        for r in rows:
            results[r["Result"]] = results.get(r["Result"], 0) + 1
        print("Results: " + ", ".join(f"{k} {v}" for k, v in sorted(results.items())))
    return 0


if __name__ == "__main__":
    sys.exit(main())

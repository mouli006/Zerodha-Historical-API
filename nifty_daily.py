"""NIFTY (NFO) or SENSEX (BFO, --underlying SENSEX) F&O 1-minute download. Run it by hand; a date for
today works after 15:40 IST. After each day the NSE (NIFTY) or BSE (SENSEX) bhavcopy is fetched and checked.

    python nifty_daily.py YYYY-MM-DD [--folder PATH] [--speed 2.5] [--underlying NIFTY|SENSEX]
    python nifty_daily.py --missing  [--folder PATH] [--speed 2.5] [--underlying NIFTY|SENSEX]
    python nifty_daily.py --refetch-no-trade YYYY-MM-DD [--folder PATH] [--speed 2.5] [--underlying ...]
    python nifty_daily.py ... --source dhan    (from Dhan into <UNDERLYING>_FnO_1min_Dhan/, speed up to 5)
    python nifty_daily.py --inspect CONTRACT YYYY-MM-DD    (e.g. NIFTY2415013OCT26PE 2026-10-07)
    python nifty_daily.py --inspect-missing YYYY-MM-DD
    python nifty_daily.py --rename-folders [--underlying NIFTY|SENSEX|all] [--source kite|dhan|all] [--apply]
    python nifty_daily.py --update-day-count [--underlying NIFTY|SENSEX|all] [--source kite|dhan|all]

--inspect prints Kite's raw candles for one contract (and the saved bhavcopy row); --inspect-missing
does it for every MISSING_IN_KITE contract of the day's bhav_compare.csv and prints a table. Neither
writes anything to the data folder.

--rename-folders renames old <YYYY-MM-DD> day folders to
<YYYY-MM-DD>_exp-<expiry>_<withZeroVol|noZeroVol>_dl-<YYYY-MM-DD>_<HHMM> (default every underlying and source).
Without --apply it only prints old -> new; it takes the download lock. --update-day-count rewrites each root's
_DAYS-<n>_<first>_to_<last>.txt and days_index.csv.

Allowed dates run from the day after the previous expiry to today: on those days every contract
is still live on Kite, so the download is complete. --missing downloads every such date without a
complete folder. --keep-zero-volume / --no-keep-zero-volume (default KEEP_ZERO_VOLUME in .env, else
config.py) keeps or skips contracts whose Kite candles all have volume 0 (Kite can report a single trade
that way). --trim-to-bhav / --no-trim-to-bhav (default TRIM_TO_BHAV in .env, else config.py, ON) downloads only
the contracts in the day's NSE/BSE bhavcopy, traded or not; while it is not published (or can't be fetched in 30 s)
the full list is used and the run says why. --refetch-no-trade requests every contract of a saved day that has no file again with the
setting ON, then checks the day against its saved bhavcopy again. Reads KITE_API_KEY and KITE_ACCESS_TOKEN from .env
(sign in on the dashboard first); --source dhan reads DHAN_ACCESS_TOKEN (connect on the Dhan tab) and needs an
active Dhan Data API plan. Every mode works on both sources.
Exit codes: 0 finished or market holiday, 1 failed contracts or not run, 2 token missing or
invalid, 3 date outside the allowed window."""
import argparse
import os
import sys
from datetime import date

from kiteconnect import exceptions as kex

from backfill.bhav_export import relabel
from backfill.dhan_api import MAX_SPEED as DHAN_MAX_SPEED
from backfill.dhan_api import DhanClient, data_plan_error, token_ok
from backfill.fno import (BROKER, FOLDERS, INSPECT_COLUMNS, UNDERLYINGS, DayFolderError, LockBusy, acquire_lock,
                          day_dir, inspect_contracts, job_fields, missing_in_kite, refetch_no_trade, run_daily,
                          run_missing, table)
from backfill.fno_rename import rename_day_folders, update_day_counts
from backfill.job import job_begin
from backfill.storage import check_folder
from config import fno_raw_dir, keep_zero_volume, load_env, trim_to_bhav, weekly_expiry_weekday

EXIT = {"finished": 0, "holiday": 0, "auth_failed": 2, "outside_window": 3}


class Parser(argparse.ArgumentParser):
    def error(self, message):   # argparse exits with 2, which is kept for token problems
        self.print_usage(sys.stderr)
        print(f"error: {message}", file=sys.stderr)
        sys.exit(1)


def main(argv=None):
    p = Parser(description="Download 1-minute candles with OI for every NIFTY (NFO) or SENSEX (BFO) F&O contract of a day.")
    which = p.add_mutually_exclusive_group(required=True)
    which.add_argument("date", nargs="?", help="YYYY-MM-DD, from the day after the previous expiry to today")
    which.add_argument("--missing", action="store_true", help="every allowed date without a complete folder")
    which.add_argument("--refetch-no-trade", metavar="DATE",
                       help="request every contract of a saved day without a file again (keep zero-volume ON)")
    which.add_argument("--inspect", nargs=2, metavar=("CONTRACT", "DATE"),
                       help="print Kite's raw candles for one contract on a date; writes nothing")
    which.add_argument("--inspect-missing", metavar="DATE",
                       help="--inspect every MISSING_IN_KITE contract of the day's bhavcopy check; writes nothing")
    which.add_argument("--rename-folders", action="store_true",
                       help="rename old <YYYY-MM-DD> day folders to the new name (dry run without --apply)")
    which.add_argument("--update-day-count", action="store_true",
                       help="rewrite each F&O root's _DAYS-*.txt marker and days_index.csv")
    p.add_argument("--apply", action="store_true", help="with --rename-folders: rename (default: only print)")
    p.add_argument("--folder", help="save folder, default the dashboard's (KITE_DATA_DIR) or ./data")
    p.add_argument("--speed", type=float, default=2.5, help="requests per second, at most 3 (default 2.5)")
    p.add_argument("--underlying", choices=sorted(UNDERLYINGS) + ["all"],
                   help="NIFTY (NSE, default) or SENSEX (BSE); all only with --rename-folders / --update-day-count (their default)")
    p.add_argument("--source", choices=sorted(FOLDERS) + ["all"],
                   help="kite (default) or dhan: from your Dhan account into <UNDERLYING>_FnO_1min_Dhan/; "
                        "all only with --rename-folders / --update-day-count (their default)")
    p.add_argument("--keep-zero-volume", action=argparse.BooleanOptionalAction, default=None,
                   help="keep contracts whose candles all have volume 0 (default from .env KEEP_ZERO_VOLUME / config.py)")
    p.add_argument("--trim-to-bhav", action=argparse.BooleanOptionalAction, default=None,
                   help="download only the contracts in the day's NSE/BSE bhavcopy, full list while it is not published "
                        "(default from .env TRIM_TO_BHAV / config.py)")
    args = p.parse_args(argv)
    if args.rename_folders or args.update_day_count:
        if args.apply and not args.rename_folders:
            p.error("--apply goes with --rename-folders")
        return rename(args)
    if args.apply:
        p.error("--apply goes with --rename-folders")
    if "all" in (args.underlying, args.source):
        p.error("--underlying all / --source all go with --rename-folders or --update-day-count only")
    args.underlying, args.source = args.underlying or "NIFTY", args.source or "kite"

    run_date = None
    day = args.date or args.refetch_no_trade or (args.inspect[1] if args.inspect else args.inspect_missing)
    if day:
        try:
            run_date = date.fromisoformat(day)
        except ValueError:
            p.error(f"invalid date {day!r}, use YYYY-MM-DD")
    top = DHAN_MAX_SPEED if args.source == "dhan" else 3
    if not 0 < args.speed <= top:
        p.error(f"--speed must be above 0 and at most {top:g}")
    if args.inspect or args.inspect_missing:
        return inspect(args, run_date)
    keep_zero = keep_zero_volume() if args.keep_zero_volume is None else args.keep_zero_volume
    if args.refetch_no_trade:
        if args.keep_zero_volume is False:
            p.error("--refetch-no-trade keeps zero-volume contracts; drop --no-keep-zero-volume")
        keep_zero = True
    trim = trim_to_bhav() if args.trim_to_bhav is None else args.trim_to_bhav
    folder, err = check_folder(args.folder or default_folder(args.source))
    if err:
        print(f"Not run: {err}", file=sys.stderr)
        return 1

    api_key, token, extra, code = sign_in(args.source)
    if code:
        return code

    try:
        lock = acquire_lock(folder, args.underlying, "nifty_daily.py", args.source)
    except LockBusy as e:
        print(f"Not run: {e}", file=sys.stderr)
        return 1
    weekday = weekly_expiry_weekday(args.underlying)
    try:
        job_begin(**job_fields(args.underlying, run_date, folder, args.speed, keep_zero, args.source,
                               trim_to_bhav=trim))
        if args.missing:
            status = run_missing(api_key, token, folder, args.speed, args.underlying, weekday=weekday,
                                 keep_zero=keep_zero, trim_to_bhav=trim, **extra)
        elif args.refetch_no_trade:
            status, _ = refetch_no_trade(api_key, token, run_date, folder, args.speed, args.underlying,
                                         weekday=weekday, trim_to_bhav=trim, **extra)
        else:
            status = run_daily(api_key, token, run_date, folder, args.speed, args.underlying,
                               weekday=weekday, keep_zero=keep_zero, trim_to_bhav=trim, **extra)
    except KeyboardInterrupt:
        print("Interrupted. Finished contracts are saved; run again to continue.", file=sys.stderr)
        return 1
    finally:
        lock.release()
    return EXIT.get(status, 1)   # run_daily / run_missing have printed why


def sign_in(source):
    """(api_key, token, run kwargs, 0) from .env, or a non-zero exit code after printing why.
    Kite: the token is checked by the job itself. Dhan: the token must not have expired, Dhan must
    accept it and the Data API plan must be active (as on the Dhan page); the kwargs carry the client."""
    env = load_env()
    if source == "kite":
        api_key, token = env.get("KITE_API_KEY", ""), env.get("KITE_ACCESS_TOKEN", "")
        if not api_key or not token:
            print("KITE_API_KEY or KITE_ACCESS_TOKEN is missing from .env. Sign in on the dashboard.", file=sys.stderr)
            return "", "", {}, 2
        return api_key, token, {}, 0
    token, err = token_ok(env)
    if err:
        print(err, file=sys.stderr)
        return "", "", {}, 2
    client = DhanClient(token)
    try:
        profile = client.profile()
    except (kex.TokenException, kex.PermissionException) as e:
        print(f"Dhan refused the access token ({e}). Paste a new one on the Dhan tab of the sign-in page.", file=sys.stderr)
        return "", "", {}, 2
    except Exception as e:
        print(f"Could not reach Dhan to check the token: {type(e).__name__}: {e}", file=sys.stderr)
        return "", "", {}, 1
    err = data_plan_error(profile)
    if err:
        print(f"Not run: {err}", file=sys.stderr)
        return "", "", {}, 1
    return "", token, {"kite": client, "user": profile.get("user_id", ""), "source": "dhan"}, 0


def inspect(args, run_date):
    """--inspect / --inspect-missing: no folder check, no lock, nothing written."""
    folder = os.path.normpath(os.path.expanduser((args.folder or default_folder(args.source)).strip()))
    if args.inspect:
        names = [args.inspect[0].strip().upper()]
    else:
        try:
            saved = day_dir(folder, args.underlying, run_date, args.source)
        except DayFolderError as e:
            print(f"Not run: {e}", file=sys.stderr)
            return 1
        names = missing_in_kite(saved) if saved else None
        if names is None:
            print(f"Not run: {run_date} has no bhav_compare.csv in {folder}; run the bhavcopy check first.",
                  file=sys.stderr)
            return 1
        missing = f"MISSING_IN_{BROKER[args.source].upper()}"   # bhav_compare.csv says MISSING_IN_KITE for both
        if not names:
            print(f"{run_date}: no {missing} contracts.")
            return 0
        print(f"{run_date}: {len(names)} {missing} contracts")
    api_key, token, extra, code = sign_in(args.source)
    if code:
        return code
    extra.pop("user", None)
    status, rows = inspect_contracts(api_key, token, run_date, folder, names, args.underlying, args.speed,
                                     verbose=bool(args.inspect), **extra)
    if status:
        return EXIT.get(status, 1)
    if args.inspect_missing:
        print(table(*relabel(INSPECT_COLUMNS, rows, args.source)))
        results = {}
        for r in rows:
            results[r["Result"]] = results.get(r["Result"], 0) + 1
        print("Results: " + ", ".join(f"{k} {v}" for k, v in sorted(results.items())))
    return 0


def default_folder(source):
    """The default --folder of a source: .env FNO_RAW_KITE / FNO_RAW_DHAN, else KITE_DATA_DIR, else ./data."""
    return fno_raw_dir(source, load_env())


def rename(args):
    """--rename-folders (old -> new for every old-style day folder; renames only with --apply) and
    --update-day-count. Without --folder each source uses its own default folder."""
    underlyings = sorted(UNDERLYINGS) if args.underlying in (None, "all") else [args.underlying]
    sources = sorted(FOLDERS) if args.source in (None, "all") else [args.source]
    groups = {}   # folder -> sources in it
    for s in sources:
        folder = os.path.normpath(os.path.expanduser((args.folder or default_folder(s)).strip()))
        groups.setdefault(folder, []).append(s)
    code = 0
    for folder, srcs in groups.items():
        if not os.path.isdir(folder):
            print(f"Not run: {folder} is not a folder.", file=sys.stderr)
            code = 1
        elif args.update_day_count:
            code = max(code, update_day_counts(folder, underlyings, srcs))
        else:
            code = max(code, rename_day_folders(folder, underlyings, srcs, apply=args.apply))
    return code


if __name__ == "__main__":
    sys.exit(main())

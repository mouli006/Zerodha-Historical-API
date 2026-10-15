"""Check saved F&O days against the exchange's F&O bhavcopy: NIFTY against NSE's, SENSEX
(--underlying SENSEX) against BSE's. Run it by hand, e.g. in the evening after the exchange publishes
the file, or any later day (both keep an archive).

    python nifty_bhav.py YYYY-MM-DD [--folder PATH] [--bhav-file PATH] [--underlying NIFTY|SENSEX]
    python nifty_bhav.py --missing  [--folder PATH] [--underlying NIFTY|SENSEX]
    python nifty_bhav.py --export YYYY-MM-DD [--csv] [--folder PATH] [--underlying NIFTY|SENSEX]
    python nifty_bhav.py --export-days [--folder PATH] [--underlying NIFTY|SENSEX]
    python nifty_bhav.py ... --source dhan    (the Dhan files in <UNDERLYING>_FnO_1min_Dhan/)

--bhav-file takes a zip or CSV downloaded by hand (when the exchange blocks the script). --missing checks every
saved Kite day that has no bhavcopy check yet. --export saves a checked day's full report (Excel; --csv:
every contract as CSV) into <day>/bhavcopy/exports/, --export-days the all-days summary CSV into
<UNDERLYING>_FnO_1min/; both read the saved check only and print the path. Exit codes: 0 OK, 1 differences found or not run,
4 bhavcopy not available yet (404, or BSE's page instead of the file)."""
import argparse
import os
import sys
from datetime import date

from backfill import bhav_export
from backfill.bhav import BhavError, bhav_dir, run_bhav, unchecked_days
from backfill.fno import UNDERLYINGS, DayFolderError, LockBusy, acquire_lock, fno_root
from backfill.storage import check_folder
import config
from config import load_env

EXIT = {"ok": 0, "diffs": 1, "not_available": 4}


class Parser(argparse.ArgumentParser):
    def error(self, message):   # keep exit codes meaningful: argparse would exit with 2
        self.print_usage(sys.stderr)
        print(f"error: {message}", file=sys.stderr)
        sys.exit(1)


def main(argv=None):
    p = Parser(description="Compare saved NIFTY / SENSEX F&O days with the NSE / BSE F&O bhavcopy.")
    which = p.add_mutually_exclusive_group(required=True)
    which.add_argument("date", nargs="?", help="YYYY-MM-DD of a saved F&O day")
    which.add_argument("--missing", action="store_true", help="every saved day without a bhavcopy check")
    which.add_argument("--export", metavar="DATE", help="save a checked day's full report (Excel) into bhavcopy/exports/")
    which.add_argument("--export-days", action="store_true", help="save the all-days summary CSV")
    p.add_argument("--csv", action="store_true", help="with --export: every contract as CSV instead of Excel")
    p.add_argument("--folder", help="save folder, default the dashboard's (KITE_DATA_DIR) or ./data")
    p.add_argument("--bhav-file", help="a bhavcopy zip or CSV downloaded by hand (needs a date)")
    p.add_argument("--underlying", default="NIFTY", choices=sorted(UNDERLYINGS),
                   help="NIFTY (NSE bhavcopy, default) or SENSEX (BSE bhavcopy)")
    p.add_argument("--source", default="kite", choices=["kite", "dhan"],
                   help="whose saved files to check: kite (default) or dhan (<UNDERLYING>_FnO_1min_Dhan/)")
    p.add_argument("--keep-zero-volume", action=argparse.BooleanOptionalAction, default=None,
                   help="without --folder: check the with_zero_volume (on) or without_zero_volume (off) store "
                        "(default from .env KEEP_ZERO_VOLUME / config.py)")
    args = p.parse_args(argv)
    if args.bhav_file and (args.missing or args.export or args.export_days):
        p.error("--bhav-file needs a date, not --missing or an export")
    if args.csv and not args.export:
        p.error("--csv goes with --export DATE")
    if args.export or args.export_days:
        return export(p, args)
    try:
        dates = [date.fromisoformat(args.date)] if args.date else None
    except ValueError:
        p.error(f"invalid date {args.date!r}, use YYYY-MM-DD")
    folder, err = check_folder(args.folder or default_folder(args))
    if err:
        print(f"Not run: {err}", file=sys.stderr)
        return 1
    if dates is None:
        try:
            dates = unchecked_days(folder, args.underlying, args.source)
        except DayFolderError as e:
            print(f"Not run: {e}", file=sys.stderr)
            return 1
        if not dates:
            print("Nothing to check: every saved day has a bhavcopy check.")
            return 0

    try:
        lock = acquire_lock(folder, args.underlying, "nifty_bhav.py", args.source)   # the same lock as the download
    except LockBusy as e:
        print(f"Not run: {e}", file=sys.stderr)
        return 1
    codes = []
    try:
        for d in dates:
            try:
                status, _, _ = run_bhav(d, folder, args.underlying, args.bhav_file, source=args.source)
                codes.append(EXIT[status])
            except (BhavError, DayFolderError, OSError) as e:
                print(f"[bhav:{args.underlying}:{d}] Not checked: {e}", file=sys.stderr)
                codes.append(1)
    finally:
        lock.release()
    # worst result: differences or errors (1) over not available (4) over OK (0)
    return 1 if 1 in codes else 4 if 4 in codes else 0


def export(p, args):
    """--export / --export-days: read the saved check, write the file, print its path. No lock needed."""
    folder = os.path.normpath(os.path.expanduser((args.folder or default_folder(args)).strip()))
    run_date = None
    if args.export:
        try:
            run_date = date.fromisoformat(args.export)
        except ValueError:
            p.error(f"invalid date {args.export!r}, use YYYY-MM-DD")
    scope = "days" if args.export_days else "all" if args.csv else "xlsx"
    try:
        name, data, _ = bhav_export.export(folder, args.underlying, scope, run_date, source=args.source)
    except (BhavError, DayFolderError) as e:
        print(f"Not exported: {e}", file=sys.stderr)
        return 1
    out = (fno_root(folder, args.underlying, args.source) if args.export_days
           else os.path.join(bhav_dir(folder, args.underlying, run_date, args.source), "exports"))
    os.makedirs(out, exist_ok=True)
    path = os.path.join(out, name)
    with open(path, "wb") as f:
        f.write(data)
    print(path)
    return 0


def default_folder(args):
    """FNO_RAW_KITE / FNO_RAW_DHAN's store of the zero-volume setting (--keep-zero-volume, else the setting),
    else KITE_DATA_DIR, else ./data."""
    return config.default_fno_folder(args.source, args.keep_zero_volume, load_env())


if __name__ == "__main__":
    sys.exit(main())

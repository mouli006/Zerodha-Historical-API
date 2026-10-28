"""Check saved F&O days against the exchange's F&O bhavcopy: NIFTY against NSE's, SENSEX
(--underlying SENSEX) against BSE's. Run it by hand, e.g. in the evening after the exchange publishes
the file, or any later day (both keep an archive).

    python nifty_bhav.py YYYY-MM-DD [--folder PATH] [--bhav-file PATH] [--underlying NIFTY|SENSEX]
    python nifty_bhav.py --missing  [--folder PATH] [--underlying NIFTY|SENSEX]
    python nifty_bhav.py --export YYYY-MM-DD [--csv] [--folder PATH] [--underlying NIFTY|SENSEX]
    python nifty_bhav.py --export-days [--folder PATH] [--underlying NIFTY|SENSEX]
    python nifty_bhav.py ... --source dhan    (the Dhan files in <UNDERLYING>_FnO_1min_Dhan/)
    python nifty_bhav.py --missing-all [--keep-zero-volume | --no-keep-zero-volume]
    python nifty_bhav.py --review YYYY-MM-DD --underlying NIFTY|SENSEX --source kite|dhan [--note "..."]
    python nifty_bhav.py --unreview YYYY-MM-DD --underlying NIFTY|SENSEX --source kite|dhan
    python nifty_bhav.py --no-bhav YYYY-MM-DD --underlying NIFTY|SENSEX [--note "..."]   (and --undo-no-bhav)
    python nifty_bhav.py --reviewed-list

--bhav-file takes a zip or CSV downloaded by hand (when the exchange blocks the script). --missing checks every
saved Kite day that has no bhavcopy check yet. --export saves a checked day's full report (Excel; --csv:
every contract as CSV) into <day>/bhavcopy/exports/, --export-days the all-days summary CSV into
<UNDERLYING>_FnO_1min/; both read the saved check only and print the path. Exit codes: 0 OK, 1 differences found or not run,
4 bhavcopy not available yet (404, or BSE's page instead of the file).

--missing-all runs --missing for NIFTY kite, NIFTY dhan, SENSEX kite, SENSEX dhan (in that order, on the default
folders; one failing set does not stop the others), prints one table and exits with the worst code (1 over 4 over 0).
--review / --unreview record that a checked day's differences were looked at (refused with missing contracts): the
day leaves the red / amber banner while a re-check gives the same counts. --no-bhav marks a past pending day whose
last bhavcopy fetch was a 404 "bhavcopy not published" in every Kite / Dhan store of the underlying: --missing and
the dashboard stop asking for it; the day stays unchecked. --reviewed-list prints every record (reads only).
These exit 0 when done, 1 when refused. See docs/dev/fno_check_all_and_reviewed.md."""
import argparse
import os
import sys
from datetime import date

from backfill import bhav_export, bhav_review
from backfill.bhav import BhavError, bhav_dir, run_bhav
from backfill.fno import UNDERLYINGS, DayFolderError, LockBusy, acquire_lock, fno_root, today_ist
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
    which.add_argument("--missing-all", action="store_true",
                       help="--missing for NIFTY kite, NIFTY dhan, SENSEX kite, SENSEX dhan, with one summary table")
    which.add_argument("--review", metavar="DATE", help="mark a checked day's differences reviewed")
    which.add_argument("--unreview", metavar="DATE", help="undo a review")
    which.add_argument("--no-bhav", metavar="DATE", help='mark a past pending day whose bhavcopy fetch was a 404 "not published"')
    which.add_argument("--undo-no-bhav", metavar="DATE", help="undo --no-bhav")
    which.add_argument("--reviewed-list", action="store_true", help="print every review and no-bhav record")
    p.add_argument("--note", help="with --review / --unreview / --no-bhav / --undo-no-bhav: a note kept with the record")
    p.add_argument("--csv", action="store_true", help="with --export: every contract as CSV instead of Excel")
    p.add_argument("--folder", help="save folder, default the dashboard's (KITE_DATA_DIR) or ./data")
    p.add_argument("--bhav-file", help="a bhavcopy zip or CSV downloaded by hand (needs a date)")
    p.add_argument("--underlying", choices=sorted(UNDERLYINGS),
                   help="NIFTY (NSE bhavcopy, default) or SENSEX (BSE bhavcopy)")
    p.add_argument("--source", choices=["kite", "dhan"],
                   help="whose saved files to check: kite (default) or dhan (<UNDERLYING>_FnO_1min_Dhan/)")
    p.add_argument("--keep-zero-volume", action=argparse.BooleanOptionalAction, default=None,
                   help="without --folder: check the with_zero_volume (on) or without_zero_volume (off) store "
                        "(default from .env KEEP_ZERO_VOLUME / config.py)")
    args = p.parse_args(argv)
    marks = args.review or args.unreview or args.no_bhav or args.undo_no_bhav
    if args.bhav_file and not args.date:
        p.error("--bhav-file needs a date, not --missing or an export")
    if args.note is not None and not marks:
        p.error("--note goes with --review, --unreview, --no-bhav or --undo-no-bhav")
    every = [f for f, on in (("--missing-all", args.missing_all), ("--reviewed-list", args.reviewed_list),
                             ("--no-bhav", args.no_bhav), ("--undo-no-bhav", args.undo_no_bhav)) if on]
    if every:
        if args.folder:
            p.error(f"{every[0]} works on the default folders of every store, not --folder")
        if args.source:
            p.error(f"{every[0]} covers Kite and Dhan; leave out --source")
        if (args.missing_all or args.reviewed_list) and args.underlying:
            p.error(f"{every[0]} covers NIFTY and SENSEX; leave out --underlying")
        if (args.no_bhav or args.undo_no_bhav) and not args.underlying:
            p.error(f"{every[0]} needs --underlying NIFTY|SENSEX")
    if (args.review or args.unreview) and not (args.underlying and args.source):
        p.error("--review / --unreview need --underlying NIFTY|SENSEX and --source kite|dhan")
    args.underlying, args.source = args.underlying or "NIFTY", args.source or "kite"
    if args.missing_all:
        return missing_all(args)
    if args.reviewed_list:
        return reviewed_list()
    if marks:
        return mark(p, args)
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
            dates, marked = bhav_review.pending_days(folder, args.underlying, args.source)
        except DayFolderError as e:
            print(f"Not run: {e}", file=sys.stderr)
            return 1
        if marked:
            print('Left out (marked "bhavcopy not published"; --undo-no-bhav to ask again): '
                  + ", ".join(d.isoformat() for d in marked))
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


def missing_all(args):
    """--missing-all: the four sets in bhav_review.SETS order, one summary table, the worst exit code."""
    rows = []
    for underlying, source, folder in bhav_review.default_sets(load_env(), args.keep_zero_volume):
        folder, err = check_folder(folder)
        print(f"== {underlying} {source}: {fno_root(folder, underlying, source)}")
        if err:
            print(f"Not run: {err}", file=sys.stderr)
            rows.append({"underlying": underlying, "source": source, "folder": folder, "checked": 0, "still_pending": 0,
                         "no_bhav_marked": 0, "failed": 1, "red": 0, "code": 1, "error": err})
            continue
        rows += bhav_review.check_all([(underlying, source, folder)], run=run_bhav)
    print()
    for line in bhav_review.table(rows):
        print(line)
    return bhav_review.worst([r["code"] for r in rows])


def mark(p, args):
    """--review / --unreview (one store, its lock) and --no-bhav / --undo-no-bhav (every store of the underlying)."""
    raw = args.review or args.unreview or args.no_bhav or args.undo_no_bhav
    try:
        day = date.fromisoformat(raw)
    except ValueError:
        p.error(f"invalid date {raw!r}, use YYYY-MM-DD")
    note = args.note or ""
    if args.no_bhav or args.undo_no_bhav:
        undo = bool(args.undo_no_bhav)
        print(f'{"Undo " if undo else ""}"bhavcopy not published" for {args.underlying} {day}:')
        roots = bhav_review.store_roots(args.underlying, load_env())
        if not bhav_review.no_bhav(args.underlying, day, roots, today_ist(), note, undo=undo):
            print(f"Not {'unmarked' if undo else 'marked'}: no store qualifies (see above).", file=sys.stderr)
            return 1
        return 0
    folder = os.path.normpath(os.path.expanduser((args.folder or default_folder(args)).strip()))
    try:
        lock = acquire_lock(folder, args.underlying, "nifty_bhav.py", args.source)
    except LockBusy as e:
        print(f"Not run: {e}", file=sys.stderr)
        return 1
    try:
        do = bhav_review.unreview if args.unreview else bhav_review.review
        message, err = do(folder, args.underlying, args.source, day, note)
    except (DayFolderError, OSError) as e:
        message, err = "", f"Not run: {e}"
    finally:
        lock.release()
    if err:
        print(err, file=sys.stderr)
        return 1
    print(message)
    return 0


def reviewed_list():
    """--reviewed-list: every record of every store root (reads only, no lock)."""
    env = load_env()
    try:
        recs = bhav_review.records({u: bhav_review.store_roots(u, env) for u in sorted(UNDERLYINGS)})
    except DayFolderError as e:
        print(f"Not run: {e}", file=sys.stderr)
        return 1
    if not recs:
        print("No review or no-bhav records.")
        return 0
    for r in recs:
        print(f"{r['date']}  {r['underlying']:<6} {r['source']:<4} {r['kind']:<12} {r['state']:<10} {r['at']}  {r['root']}"
              + (f"  note: {r['note']}" if r["note"] else ""))
    return 0


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

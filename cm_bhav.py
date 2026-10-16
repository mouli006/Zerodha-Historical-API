"""NSE equity (cash market) bhavcopy, 2015 -> today, into <folder>/NSE_CM_bhav/ (backfill/cm_bhav.py).
No broker: the files come from NSE's public archive.

    python cm_bhav.py YYYY-MM-DD                       one day
    python cm_bhav.py --from YYYY-MM-DD --to YYYY-MM-DD   a range (weekdays)
    python cm_bhav.py --missing                        every weekday from 2015-01-01 to today not yet ok / holiday
    python cm_bhav.py --offline                        no downloads: re-read every zip in raw/ and rebuild the parquet
    options: --folder PATH (default EQUITY_DATA_DIR, else KITE_DATA_DIR or ./data), --sleep 1.5 (seconds between
             requests), --force (download again, ask again for holidays), --no-browser (no headless Chrome fallback)

Exit codes: 0 all done (today before 19:00 IST is pending, not a failure), 1 some days failed (each is listed with
its URL and the raw/ path to save it under by hand), 2 bad arguments or another cm_bhav.py run holds the lock."""
import argparse
import sys
from datetime import date

from backfill import cm_bhav
from backfill.storage import check_folder
from config import equity_data_dir


def parse_day(text):
    try:
        return date.fromisoformat(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"invalid date {text!r}, use YYYY-MM-DD") from None


def main(argv=None, now=None, client=None):
    p = argparse.ArgumentParser(description="Download NSE's equity (CM) bhavcopy into <folder>/NSE_CM_bhav/.")
    p.add_argument("date", nargs="?", type=parse_day, help="one day, YYYY-MM-DD")
    p.add_argument("--from", dest="since", type=parse_day, help="first day of a range")
    p.add_argument("--to", dest="until", type=parse_day, help="last day of a range")
    p.add_argument("--missing", action="store_true", help="every weekday from 2015-01-01 to today not done yet")
    p.add_argument("--offline", action="store_true", help="no downloads: rebuild the parquet from raw/ only")
    p.add_argument("--force", action="store_true", help="download again (holidays asked again)")
    p.add_argument("--folder", help="data folder, default EQUITY_DATA_DIR, else KITE_DATA_DIR or ./data")
    p.add_argument("--sleep", type=float, default=cm_bhav.SLEEP, help="seconds between requests (default 1.5)")
    p.add_argument("--no-browser", action="store_true", help="never fall back to headless Chrome")
    args = p.parse_args(argv)   # argparse errors exit with 2

    now = now or cm_bhav.now_ist()
    today = cm_bhav.today_ist(now)
    modes = sum([args.date is not None, args.since is not None or args.until is not None, args.missing])
    if args.offline and (modes or args.force):
        p.error("--offline runs alone (it re-reads every zip in raw/)")
    if not args.offline and modes != 1:
        p.error("give one of: a date, --from and --to, --missing (or --offline)")
    if args.sleep <= 0:
        p.error("--sleep must be more than 0")
    days = []
    if args.date:
        if args.date.weekday() >= 5:
            p.error(f"{args.date} is a {args.date:%A}; NSE has no bhavcopy on weekends")
        if args.date > today:
            p.error(f"{args.date} is in the future")
        days = [args.date]
    elif args.since or args.until:
        if not (args.since and args.until):
            p.error("--from and --to go together")
        if args.since > args.until:
            p.error("--from is after --to")
        if args.until > today:
            p.error(f"--to {args.until} is in the future")
        days = list(cm_bhav.weekdays(args.since, args.until))
    elif args.missing:
        days = list(cm_bhav.weekdays(cm_bhav.START, today))

    folder, err = check_folder(args.folder or equity_data_dir())
    if err:
        print(f"Not run: {err}", file=sys.stderr)
        return 2
    try:
        s = cm_bhav.run(folder, days, force=args.force, offline=args.offline, sleep=args.sleep,
                        use_browser=not args.no_browser, client=client, now=now)
    except cm_bhav.LockBusy as e:
        print(f"Not run: {e}", file=sys.stderr)
        return 2

    c = s["counts"]
    print()
    print(f"Saved in {s['store']}")
    print(f"This run: ok {c['ok']}, holidays {c['holiday']}, failed {c['failed']}, pending {c['pending']}, "
          f"already done {c['skipped']}; {s['requests']} request(s)")
    if s["years"]:
        print("Parquet rebuilt: " + ", ".join(str(y) for y in s["years"]))
    print()
    print(s["report"])
    if s["failures"]:
        print(f"\n{len(s['failures'])} download(s) failed. Download in a browser, save under the name shown, "
              "and run again:", file=sys.stderr)
        for day, url, path, error in s["failures"]:
            print(f"  {day}: {url}\n      -> {path}   ({error})", file=sys.stderr)
    return 1 if c["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())

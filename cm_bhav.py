"""NSE equity (cash market) bhavcopy, 2015 -> today, into <folder>/NSE_CM_bhav/ (backfill/cm_bhav.py).
No broker: the files come from NSE's public archive.

    python cm_bhav.py YYYY-MM-DD                       one day
    python cm_bhav.py --from YYYY-MM-DD --to YYYY-MM-DD   a range (weekdays)
    python cm_bhav.py --missing                        every weekday from 2015-01-01 to today not yet ok / holiday
    python cm_bhav.py --offline                        no downloads: re-read every zip in raw/ and rebuild the parquet
    python cm_bhav.py --special [--apply]              the weekend sessions of manual_special_sessions.csv: the dry run
                                                       asks NSE and lists what --apply would write (nothing saved)
    options: --folder PATH (default EQUITY_DATA_DIR, else KITE_DATA_DIR or ./data), --sleep 1.5 (seconds between
             requests), --force (download again, ask again for holidays), --no-browser (no headless Chrome fallback)

Exit codes: 0 all done (today before 19:00 IST is pending, not a failure; a special session NSE has no file for is
not a failure), 1 some days failed (each is listed with its URL and the raw/ path to save it under by hand), 2 bad
arguments, a bad special-sessions file or another cm_bhav.py run holds the lock."""
import argparse
import os
import sys
from datetime import date

from backfill import cm_bhav
from backfill.equity import special_sessions
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
    p.add_argument("--special", action="store_true",
                   help="weekend sessions of manual_special_sessions.csv: dry run (asks NSE, saves nothing)")
    p.add_argument("--apply", action="store_true", help="with --special: save the files found and rebuild those years")
    p.add_argument("--force", action="store_true", help="download again (holidays asked again)")
    p.add_argument("--folder", help="data folder, default EQUITY_DATA_DIR, else KITE_DATA_DIR or ./data")
    p.add_argument("--sleep", type=float, default=cm_bhav.SLEEP, help="seconds between requests (default 1.5)")
    p.add_argument("--no-browser", action="store_true", help="never fall back to headless Chrome")
    args = p.parse_args(argv)   # argparse errors exit with 2

    now = now or cm_bhav.now_ist()
    today = cm_bhav.today_ist(now)
    modes = sum([args.date is not None, args.since is not None or args.until is not None, args.missing])
    if args.apply and not args.special:
        p.error("--apply goes with --special")
    if args.special and (modes or args.offline or args.force):
        p.error("--special runs alone (with --apply, --folder, --sleep, --no-browser)")
    if args.offline and (modes or args.force):
        p.error("--offline runs alone (it re-reads every zip in raw/)")
    if not args.offline and not args.special and modes != 1:
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
    special = False
    if args.special:
        try:
            if not args.apply:
                return special_dry_run(folder, args, client, now)
            plan = cm_bhav.special_plan(folder, now)
        except special_sessions.SpecialSessionsError as e:
            print(f"Not run: {e}", file=sys.stderr)
            return 2
        except cm_bhav.LockBusy as e:
            print(f"Not run: {e}", file=sys.stderr)
            return 2
        days, special = [r["day"] for r in plan if r["status"] in ("to ask", "in raw/")], True
        for r in plan:
            if r["status"] in ("in store", "future"):
                print(f"{r['day']} {r['day']:%a} {r['kind']}: {r['status']}, not asked")
    try:
        s = cm_bhav.run(folder, days, force=args.force, offline=args.offline, sleep=args.sleep,
                        use_browser=not args.no_browser, client=client, now=now, special=special)
    except cm_bhav.LockBusy as e:
        print(f"Not run: {e}", file=sys.stderr)
        return 2

    c = s["counts"]
    print()
    print(f"Saved in {s['store']}")
    print(f"This run: ok {c['ok']}, holidays {c['holiday']}, failed {c['failed']}, pending {c['pending']}, "
          f"already done {c['skipped']}; {s['requests']} request(s)")
    if special:
        print(f"Special sessions: not found at NSE {c['not_found']} (nothing recorded for them)")
    if s["years"]:
        print("Parquet rebuilt: " + ", ".join(str(y) for y in s["years"]))
    print()
    print(s["report"])
    bad = [f for f in s["failures"] if os.path.exists(f[2])]   # downloaded, but failed the checks
    missing = [f for f in s["failures"] if not os.path.exists(f[2])]
    if missing:
        print(f"\n{len(missing)} download(s) failed. Download in a browser, save under the name shown, "
              "and run again:", file=sys.stderr)
        for day, url, path, error in missing:
            print(f"  {day}: {url}\n      -> {path}   ({error})", file=sys.stderr)
    if bad:
        print(f"\n{len(bad)} file(s) failed the checks (the zip is in raw\\; nothing to download):", file=sys.stderr)
        for day, url, path, error in bad:
            print(f"  {day}: {error}", file=sys.stderr)
    return 1 if c["failed"] else 0


def special_dry_run(folder, args, client, now):
    """--special without --apply: what NSE has for each weekend session and what --apply would write."""
    s = cm_bhav.check_special(folder, client=client, now=now, sleep=args.sleep, use_browser=not args.no_browser)
    print(f"Special sessions (weekend) from {special_sessions.path_of(folder)}; store {s['store']}")
    print("DRY RUN: nothing is saved. Run with --apply to save the files found.")
    print()
    for r in s["rows"]:
        head = f"{r['day']} {r['day']:%a} {r['kind']:<16}"
        if r["status"] in ("in store", "future"):
            print(f"{head} {r['status']}, not asked")
        elif r.get("result") == "found":
            src = "saved in raw/, no request" if r["status"] == "in raw/" else r["url"]
            print(f"{head} found ({r['format']}): {r['rows']:,} rows, {r['main']:,} EQ/BE/BZ, "
                  f"{r['trades']:,} EQ trades  <- {src}")
        else:
            print(f"{head} {r['result']}: {r['note']}")
    counts = {}
    for r in s["rows"]:
        key = r.get("result") or r["status"]
        counts[key] = counts.get(key, 0) + 1
    print()
    print("Weekend sessions: " + ", ".join(f"{k} {v}" for k, v in counts.items()) + f"; {s['requests']} request(s)")
    print("An --apply would write:")
    print(f"  raw/ zips: {len(s['raw'])}")
    for path in s["raw"]:
        print(f"    {path}")
    print("  parquet years rebuilt: " + (", ".join(f"{y} ({m})" for y, m in s["years"]) or "none"))
    print("  plus a db row per day asked that has a file, days.csv and cm_bhav.log")
    failed = sum(1 for r in s["rows"] if r.get("result") in ("failed", "bad file"))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

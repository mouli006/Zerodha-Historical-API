"""NSE delisting, suspension and merger reference data, 2015 to today, into
<data folder>/NSE_CM_bhav/reference/ (data folder = the dashboard's KITE_DATA_DIR or ./data).

    python nse_reference.py [--folder PATH] [--from YYYY-MM-DD] [--only PART,...] [--offline] [--no-browser]

Writes voluntary_delisting.csv (NSE's Excel lists of delisted / proposed companies), compulsory_delisting.csv
(orders of delisting), delisting_circulars.csv (Listing circulars about delisting), mergers.csv and
suspensions.csv (equity suspension circulars, split by the reason in the PDF). Every downloaded file is
kept in reference/raw/ and never downloaded twice; pages and this year's circular list at most once a
day. Requests are 2 s apart; when NSE blocks the script, headless Chrome (Playwright) takes over.
--offline rebuilds the CSVs from raw/ only. reference/manual_mergers.csv (old_symbol, new_company,
new_symbol, swap_ratio) fills what the circulars lack. Rows the parser is unsure about have needs_review=Y.
Exit codes: 0 done, 1 something could not be downloaded (the files to save by hand are listed) or not run.
Links that NSE itself serves as 404 are listed but do not change the exit code."""
import argparse
import sys
from datetime import date

from backfill.nse_ref.run import PARTS, START, collect
from backfill.storage import check_folder
from config import data_dir


class Parser(argparse.ArgumentParser):
    def error(self, message):   # argparse would exit with 2; keep 1 for every "not run"
        self.print_usage(sys.stderr)
        print(f"error: {message}", file=sys.stderr)
        sys.exit(1)


def main(argv=None):
    p = Parser(description="Download NSE delisting, suspension and merger information into "
                           "<data folder>/NSE_CM_bhav/reference/.")
    p.add_argument("--folder", help="data folder, default the dashboard's (KITE_DATA_DIR) or ./data")
    p.add_argument("--from", dest="since", default=START.isoformat(), help="first date, default 2015-01-01")
    p.add_argument("--only", help=f"comma-separated parts: {','.join(PARTS)} (default all)")
    p.add_argument("--offline", action="store_true", help="no downloads: rebuild the CSVs from reference/raw/")
    p.add_argument("--no-browser", action="store_true", help="never fall back to headless Chrome")
    args = p.parse_args(argv)
    try:
        since = date.fromisoformat(args.since)
    except ValueError:
        p.error(f"invalid date {args.since!r}, use YYYY-MM-DD")
    parts = PARTS
    if args.only:
        parts = [x.strip() for x in args.only.split(",") if x.strip()]
        bad = [x for x in parts if x not in PARTS]
        if bad or not parts:
            p.error(f"unknown part(s) {', '.join(bad) or '(none)'}; choose from {', '.join(PARTS)}")
    folder, err = check_folder(args.folder or data_dir())
    if err:
        print(f"Not run: {err}", file=sys.stderr)
        return 1

    s = collect(folder, since, parts, offline=args.offline, use_browser=not args.no_browser)

    print()
    print(f"Saved in {s['reference']}")
    for name, (n, review) in s["files"].items():
        print(f"  {name:28} {n:6,} rows   {review:5,} needs_review")
    if s["skipped"]:
        print("Circulars skipped by subject: " + ", ".join(f"{k} {v}" for k, v in sorted(s["skipped"].items())))
    print(f"Downloaded {s['downloads']} new file(s), {s['requests']} request(s).")
    for note in s["notes"]:
        print(f"Note: {note}")
    if s["not_found"]:
        print(f"{len(s['not_found'])} link(s) on NSE's pages answer 404 (broken there; their rows have needs_review=Y):")
        for what, url in s["not_found"]:
            print(f"  {what}: {url}")
    for sym in s["unmatched_manual"]:
        print(f"Note: manual_mergers.csv: {sym} matches no merger suspension circular")
    if s["failures"]:
        print(f"\n{len(s['failures'])} file(s) could not be downloaded. Download by hand, save under the "
              "name shown, and run again:", file=sys.stderr)
        for what, url, path, err in s["failures"]:
            print(f"  {what}: {url}\n      -> {path}   ({err})", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

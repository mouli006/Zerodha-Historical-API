"""Index membership over time: NIFTY indices and the AMFI large / mid / small cap lists
(backfill/equity/index_membership.py). Only `fetch` uses the network (AMFI, niftyindices, web.archive.org); the build
and the queries are offline.

    python index_membership.py fetch [--amfi] [--nifty] [--changes] [--snapshots] [--force] [--no-browser]
    python index_membership.py build [--amfi-lag 5]
    python index_membership.py members "NIFTY 50" 2023-07-13 [--allow-unreliable]
    python index_membership.py show HDFC          NIFTY periods and AMFI categories of every security that had it
    python index_membership.py check              re-run the known-case checks

fetch with no part flag fetches all four; files already saved are not downloaded again unless --force (the current
constituent lists and listing pages are fetched on every run). --folder defaults to EQUITY_DATA_DIR, else
KITE_DATA_DIR or ./data. Exit codes: 0 done (build: also when a known case is not found; the report lists it), 1 not
run (no security master, master older than the bhavcopy, not built yet), some downloads failed, a date outside the
coverage, show found nothing, or check failed, 2 bad arguments, another index_membership run, or a bhavcopy download
/ security master build running."""
import argparse
import os
import sys
from datetime import date

import pandas as pd

from backfill.equity import index_fetch as fe
from backfill.equity import index_membership as im
from backfill.equity import security_master as sm
from config import equity_data_dir


def run_fetch(args, folder):
    parts = [p for p in fe.PARTS if getattr(args, p)] or list(fe.PARTS)
    client = fe.new_client(use_browser=not args.no_browser)
    try:
        res = fe.fetch(folder, parts, client=client, force=args.force)
    finally:
        client.close()
    print()
    for part in parts:
        print(f"{part}: saved {res.count('saved', part)}, refreshed {res.count('refreshed', part)}, skipped "
              f"{res.count('skipped', part)}, not found {res.count('not_found', part)}, failed {res.count('failed', part)}")
    if res.failed:
        print("\nNot downloaded (save these by hand; the build reads them the same way):")
        for part, what, _, detail in res.failed:
            print(f"  {part} {what}: {detail}")
        return 1
    return 0


def show(key, folder):
    ids = sm.lookup(key, folder)
    if not ids:
        print(f"{key}: no security ever had this symbol or ISIN.", file=sys.stderr)
        return 1
    b = im.load_built(folder)
    m, a = b["membership"], b["amfi"]
    with pd.option_context("display.width", 200, "display.max_rows", 500):
        for sid in ids:
            print(f"=== {sid}")
            mine = m[m["security_id"] == sid]
            if mine.empty:
                print("No NIFTY index periods.")
            else:
                print(mine.drop(columns=["security_id"]).to_string(index=False))
            cats = a[a["security_id"] == sid][["period", "valid_from", "valid_to", "rank", "category", "symbol"]]
            print("AMFI: " + ("none" if cats.empty else ""))
            if not cats.empty:
                print(cats.to_string(index=False))
            print()
    return 0


def positive(text):
    v = int(text)
    if v < 0:
        raise argparse.ArgumentTypeError("must be 0 or more")
    return v


def main(argv=None):
    p = argparse.ArgumentParser(description="NIFTY index membership and AMFI categories over time.")
    sub = p.add_subparsers(dest="cmd", required=True)
    f = sub.add_parser("fetch", help="download the source files (network)")
    for part in fe.PARTS:
        f.add_argument(f"--{part}", action="store_true", help=f"fetch the {part} part only (combinable)")
    f.add_argument("--force", action="store_true", help="download saved files again")
    f.add_argument("--no-browser", action="store_true", help="no headless Chromium fallback when a site blocks")
    b = sub.add_parser("build", help="full rebuild of <folder>/index_membership/ (offline)")
    b.add_argument("--amfi-lag", type=positive, default=im.AMFI_LAG,
                   help="AMFI lists valid this many trading days after the month starts (default 5)")
    mm = sub.add_parser("members", help="members of an index on a date")
    mm.add_argument("index", help='e.g. "NIFTY 50", NIFTY_MIDCAP_150')
    mm.add_argument("date", type=date.fromisoformat, help="YYYY-MM-DD")
    mm.add_argument("--allow-unreliable", action="store_true",
                    help="answer for a date before the index's reliable_from date too")
    s = sub.add_parser("show", help="NIFTY periods and AMFI categories of one company")
    s.add_argument("key", help="any symbol or ISIN it ever had")
    sub.add_parser("check", help="re-run the known-case checks")
    for sp in sub.choices.values():
        sp.add_argument("--folder", help="data folder, default EQUITY_DATA_DIR, else KITE_DATA_DIR or ./data")
    args = p.parse_args(argv)   # argparse errors exit with 2
    if args.folder and not os.path.isabs(args.folder):
        p.error("--folder must be an absolute path")
    if args.cmd == "members":
        try:
            args.index = im.index_name(args.index)
        except KeyError as e:
            p.error(str(e).strip("'\""))
    folder = args.folder or equity_data_dir()
    try:
        if args.cmd == "fetch":
            return run_fetch(args, folder)
        if args.cmd == "build":
            result = im.build(folder, args.amfi_lag)
            print(result["report"], end="")
            print(f"Written to {result['out']}")
            return 0
        if args.cmd == "members":
            ids = im.members(args.index, args.date, folder, allow_unreliable=args.allow_unreliable)
            rel = im.coverage(args.index, folder).reliable_from
            if rel is None or args.date < rel:
                print(f"Note: before {args.index} is reliable ({rel or 'never'}); shown because of --allow-unreliable")
            master = im.Master(folder)
            print(f"{args.index} on {args.date}: {len(ids)} members")
            for sid in ids:
                print(f"  {sid}  {master.name_on(sid, args.date)[0]}")
            return 0
        if args.cmd == "show":
            return show(args.key, folder)
        lines, ok = im.check(folder)
        print("\n".join(lines))
        print(f"Check: {'ok' if ok else 'FAILED'}")
        return 0 if ok else 1
    except (im.LockBusy, im.StoreBusy) as e:
        print(f"Not run: {e}", file=sys.stderr)
        return 2
    except (im.IndexMembershipError, sm.SecurityMasterError) as e:
        print(f"Not run: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())

"""F&O folders for backtesting: the raw folders per broker and zero-volume setting, and the backtest store.

    python fno_store.py move-raw --from "<old save folder>" [--from ...] --source kite|dhan
                                 [--to PATH | --variant with|without] [--apply]
    python fno_store.py split-stores [--source kite|dhan|all] [--apply]
    python fno_store.py derive-without --source kite|dhan [--underlying NIFTY|SENSEX|all] [--from D] [--to D]
                                       [--with PATH] [--without PATH] [--apply]
    python fno_store.py compare-variants --source kite|dhan [--underlying ...] [--from D] [--to D]
                                         [--with PATH] [--without PATH]
    python fno_store.py clear --underlying NIFTY|SENSEX [--source kite|dhan|all] [--variant with|without|all]
                              [--no-backtest] [--archive-to PATH] [--apply] [--delete] [--yes]
    python fno_store.py restore --from "<archive folder>" [--apply]
    python fno_store.py import --from "<prepared folder>" --underlying NIFTY|SENSEX [--apply] [--yes]
    python fno_store.py publish [--raw-kite PATH] [--raw-dhan PATH] [--out PATH] [--variant with|without|both]
                                [--underlying NIFTY|SENSEX|all] [--from YYYY-MM-DD] [--to YYYY-MM-DD]
                                [--prefer kite|dhan] [--force] [--prune] [--prune-old-layout]

Layout: FNO_RAW_KITE / FNO_RAW_DHAN (.env) are base folders holding two save folders, with_zero_volume (downloads
with keep zero-volume ON) and without_zero_volume (OFF); FNO_BACKTEST_DIR holds the published copy of each.

move-raw (backfill/fno_store.py) moves the day folders of every <UNDERLYING>_FnO_1min[_Dhan] root in the old save
folder(s) into the same root under --to (--variant: the source's with / without store; into a store only days of
its setting move). split-stores moves the base folder's own roots into the two stores. derive-without
(backfill/fno_derive.py) writes the without copy of every with day the without store lacks (the with day minus
its zero-volume files); compare-variants derives into a temporary folder and compares with real without
downloads. All but compare-variants are dry runs without --apply; a day the target already has is never moved,
replaced or deleted, only listed.

clear (backfill/fno_clear.py) moves one underlying's day folders, day counts, progress rows and backtest folders into
<home>/archive/<time>_clear_<U>/ so its next downloads start clean (expiries_seen.csv, holidays.log and .lock stay);
it says which cleared days can no longer be downloaded. restore puts an archive back. Both are dry runs without
--apply; clear --apply --delete deletes instead (typed confirmation).

import (backfill/fno_import.py) replaces one underlying's live data with a prepared copy (kite*/dhan* folders holding
"with zero volume" / "without zero volume" save folders): it checks and compares every day, archives the live data
like clear, copies the days in (checked), writes their progress rows and publishes. Dry run without --apply.

publish (backfill/fno_publish.py) builds <out>/<with_zero_volume|without_zero_volume>/<UNDERLYING>/ from the raw
stores: one Parquet file per day and source, the EOD bhavcopy, contracts.parquet, expiries.csv, days.csv. Only
new or changed days are rebuilt (--force: all); raw is never written. Read the store with backfill/fno_backtest.py.
Run by hand; no cron.
Exit codes: 0 done (or dry run), 1 not run, stopped by an error, a busy folder, a failed day or (compare-variants)
a difference."""
import argparse
import sys
from datetime import date

from backfill.fno import UNDERLYINGS
from backfill.fno_clear import clear, restore
from backfill.fno_derive import compare_variants, derive_without
from backfill.fno_import import import_data
from backfill.fno_publish import publish_variants
from backfill.fno_store import move_raw, split_stores
from config import FNO_RAW_KEYS, VARIANTS, fno_backtest_dir, fno_raw_dir, load_env


class Parser(argparse.ArgumentParser):
    def error(self, message):   # argparse exits 2; keep 1 for "not run"
        self.print_usage(sys.stderr)
        print(f"{self.prog}: error: {message}", file=sys.stderr)
        sys.exit(1)


def day(text):
    try:
        return date.fromisoformat(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"invalid date {text!r}, use YYYY-MM-DD")


def store_args(p, apply=True):
    p.add_argument("--source", choices=sorted(FNO_RAW_KEYS), required=True)
    p.add_argument("--underlying", choices=sorted(UNDERLYINGS) + ["all"], default="all")
    p.add_argument("--from", dest="date_from", type=day)
    p.add_argument("--to", dest="date_to", type=day)
    p.add_argument("--with", dest="with_folder", metavar="PATH",
                   help="the with-zero-volume save folder (default <FNO_RAW_<SOURCE>>/with_zero_volume)")
    p.add_argument("--without", dest="without_folder", metavar="PATH",
                   help="the without-zero-volume save folder (default <FNO_RAW_<SOURCE>>/without_zero_volume)")
    if apply:
        p.add_argument("--apply", action="store_true", help="write (default: only print)")


def main(argv=None):
    p = Parser(description="F&O raw folders and backtest store.")
    sub = p.add_subparsers(dest="cmd", required=True, parser_class=Parser)
    m = sub.add_parser("move-raw", help="move day folders from old save folders into the raw folder (dry run)")
    m.add_argument("--from", dest="froms", action="append", required=True, metavar="PATH",
                   help="old save folder holding <UNDERLYING>_FnO_1min[_Dhan]; repeat for several, in order")
    m.add_argument("--source", choices=sorted(FNO_RAW_KEYS), required=True)
    where = m.add_mutually_exclusive_group()
    where.add_argument("--to", help="target save folder (default .env FNO_RAW_KITE / FNO_RAW_DHAN)")
    where.add_argument("--variant", choices=sorted(VARIANTS),
                       help="target: the source's with_zero_volume / without_zero_volume store under FNO_RAW_<SOURCE>")
    m.add_argument("--apply", action="store_true", help="move (default: only print)")
    s = sub.add_parser("split-stores", help="move the raw roots into with_zero_volume / without_zero_volume (dry run)")
    s.add_argument("--source", choices=sorted(FNO_RAW_KEYS) + ["all"], default="all")
    s.add_argument("--apply", action="store_true", help="move (default: only print)")
    d = sub.add_parser("derive-without", help="write the without-zero-volume copy of with days (dry run)")
    store_args(d)
    c = sub.add_parser("compare-variants", help="derive into a temporary folder and compare with real without days")
    store_args(c, apply=False)
    k = sub.add_parser("clear", help="archive one underlying's F&O data so it can be downloaded fresh (dry run)")
    k.add_argument("--underlying", choices=sorted(UNDERLYINGS), required=True, help="no default: name it")
    k.add_argument("--source", choices=sorted(FNO_RAW_KEYS) + ["all"], default="all")
    k.add_argument("--variant", choices=sorted(VARIANTS) + ["all"], default="all")
    k.add_argument("--no-backtest", action="store_true", help="leave the published backtest folders")
    k.add_argument("--archive-to", help="archive folder (default <common folder of the .env bases>/archive/<time>_clear_<U>)")
    k.add_argument("--apply", action="store_true", help="move into the archive (default: only print)")
    k.add_argument("--delete", action="store_true", help="with --apply: delete instead of archiving (typed confirmation)")
    k.add_argument("--yes", action="store_true", help="don't ask when cleared days can't be downloaded again")
    r = sub.add_parser("restore", help="put a clear's archive back (dry run)")
    r.add_argument("--from", dest="archive", required=True, metavar="PATH", help="the archive folder clear wrote")
    r.add_argument("--apply", action="store_true", help="restore (default: only print)")
    i = sub.add_parser("import", help="replace one underlying's data with a prepared copy (dry run)")
    i.add_argument("--from", dest="src", required=True, metavar="PATH",
                   help="folder holding kite* / dhan* folders with 'with zero volume' / 'without zero volume' inside")
    i.add_argument("--underlying", choices=sorted(UNDERLYINGS), required=True, help="no default: name it")
    i.add_argument("--apply", action="store_true", help="archive the live data and copy (default: only print)")
    i.add_argument("--yes", action="store_true", help="don't ask when archived days can't be downloaded again")
    b = sub.add_parser("publish", help="build the backtest store from the raw folders")
    b.add_argument("--raw-kite", help="Kite raw base folder (default .env FNO_RAW_KITE)")
    b.add_argument("--raw-dhan", help="Dhan raw base folder (default .env FNO_RAW_DHAN)")
    b.add_argument("--out", help="backtest base folder (default .env FNO_BACKTEST_DIR)")
    b.add_argument("--variant", choices=sorted(VARIANTS) + ["both"], default="both")
    b.add_argument("--underlying", choices=sorted(UNDERLYINGS) + ["all"], default="all")
    b.add_argument("--from", dest="date_from", type=day)
    b.add_argument("--to", dest="date_to", type=day)
    b.add_argument("--prefer", choices=["kite", "dhan"], default="kite", help="best_source on a tie (default kite)")
    b.add_argument("--force", action="store_true", help="rebuild every day, changed or not")
    b.add_argument("--prune", action="store_true", help="remove published days whose raw folder is gone")
    b.add_argument("--prune-old-layout", action="store_true",
                   help="remove <out>/NIFTY and <out>/SENSEX published before the with / without split")
    args = p.parse_args(argv)
    env = load_env()
    if args.cmd == "move-raw":
        base = env.get(FNO_RAW_KEYS[args.source], "")
        if args.variant:
            to = fno_raw_dir(args.source, args.variant, env) if base else ""
        else:
            to = args.to or base
        return move_raw(args.froms, args.source, to, apply=args.apply)
    if args.cmd == "clear":
        sources = sorted(FNO_RAW_KEYS) if args.source == "all" else [args.source]
        variants = sorted(VARIANTS) if args.variant == "all" else [args.variant]
        return clear(args.underlying, sources, variants, env, backtest=not args.no_backtest, archive_to=args.archive_to,
                     apply=args.apply, delete=args.delete, yes=args.yes, command=" ".join(["python fno_store.py", *(argv or sys.argv[1:])]))
    if args.cmd == "import":
        return import_data(args.src, args.underlying, env, apply=args.apply, yes=args.yes,
                           command=" ".join(["python fno_store.py", *(argv or sys.argv[1:])]))
    if args.cmd == "restore":
        return restore(args.archive, apply=args.apply)
    if args.cmd == "split-stores":
        return split_stores(sorted(FNO_RAW_KEYS) if args.source == "all" else [args.source], env, apply=args.apply)
    if args.cmd in ("derive-without", "compare-variants"):
        base = env.get(FNO_RAW_KEYS[args.source], "")
        folders = [args.with_folder or (fno_raw_dir(args.source, "with", env) if base else ""),
                   args.without_folder or (fno_raw_dir(args.source, "without", env) if base else "")]
        underlyings = sorted(UNDERLYINGS) if args.underlying == "all" else [args.underlying]
        if args.cmd == "derive-without":
            return derive_without(*folders, args.source, underlyings, args.date_from, args.date_to, apply=args.apply)
        if not all(folders):
            print("Not run: give --with and --without, or set FNO_RAW_KITE / FNO_RAW_DHAN in .env.")
            return 1
        return compare_variants(*folders, args.source, underlyings, args.date_from, args.date_to)
    raw = {"kite": args.raw_kite or env.get(FNO_RAW_KEYS["kite"]) or None,
           "dhan": args.raw_dhan or env.get(FNO_RAW_KEYS["dhan"]) or None}
    underlyings = sorted(UNDERLYINGS) if args.underlying == "all" else [args.underlying]
    variants = sorted(VARIANTS) if args.variant == "both" else [args.variant]
    return publish_variants(raw, args.out or fno_backtest_dir(env), variants, underlyings, args.date_from,
                            args.date_to, args.prefer, args.force, args.prune, args.prune_old_layout)


if __name__ == "__main__":
    sys.exit(main())

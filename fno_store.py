"""F&O folders for backtesting: move old downloads into one raw folder per broker, and publish the backtest store.

    python fno_store.py move-raw --from "<old save folder>" [--from ...] --source kite|dhan [--to PATH] [--apply]
    python fno_store.py publish [--raw-kite PATH] [--raw-dhan PATH] [--out PATH] [--underlying NIFTY|SENSEX|all]
                                [--from YYYY-MM-DD] [--to YYYY-MM-DD] [--prefer kite|dhan] [--force] [--prune]

move-raw (backfill/fno_store.py) moves the day folders of every <UNDERLYING>_FnO_1min[_Dhan] root in the old save
folder(s) into the same root under --to (default .env FNO_RAW_KITE / FNO_RAW_DHAN). It is a dry run without
--apply; a day the target already has is never moved or replaced, only listed. Several --from are processed in
order, so a later folder's days that an earlier one already brings are listed as duplicates.

publish (backfill/fno_publish.py) builds <out>/<UNDERLYING>/ (default .env FNO_BACKTEST_DIR) from the raw folders
(default FNO_RAW_KITE / FNO_RAW_DHAN): one Parquet file per day and source, the EOD bhavcopy, contracts.parquet,
expiries.csv, days.csv. Only new or changed days are rebuilt (--force: all); raw is never written.
Read the store with backfill/fno_backtest.py. Run by hand; no cron.
Exit codes: 0 done (or dry run), 1 not run, stopped by an error, a busy folder or a failed day."""
import argparse
import sys
from datetime import date

from backfill.fno import UNDERLYINGS
from backfill.fno_publish import publish
from backfill.fno_store import move_raw
from config import FNO_RAW_KEYS, fno_backtest_dir, load_env


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


def main(argv=None):
    p = Parser(description="F&O raw folders and backtest store.")
    sub = p.add_subparsers(dest="cmd", required=True, parser_class=Parser)
    m = sub.add_parser("move-raw", help="move day folders from old save folders into the raw folder (dry run)")
    m.add_argument("--from", dest="froms", action="append", required=True, metavar="PATH",
                   help="old save folder holding <UNDERLYING>_FnO_1min[_Dhan]; repeat for several, in order")
    m.add_argument("--source", choices=sorted(FNO_RAW_KEYS), required=True)
    m.add_argument("--to", help="raw folder of the source (default .env FNO_RAW_KITE / FNO_RAW_DHAN)")
    m.add_argument("--apply", action="store_true", help="move (default: only print)")
    b = sub.add_parser("publish", help="build the backtest store from the raw folders")
    b.add_argument("--raw-kite", help="Kite raw folder (default .env FNO_RAW_KITE)")
    b.add_argument("--raw-dhan", help="Dhan raw folder (default .env FNO_RAW_DHAN)")
    b.add_argument("--out", help="backtest folder (default .env FNO_BACKTEST_DIR)")
    b.add_argument("--underlying", choices=sorted(UNDERLYINGS) + ["all"], default="all")
    b.add_argument("--from", dest="date_from", type=day)
    b.add_argument("--to", dest="date_to", type=day)
    b.add_argument("--prefer", choices=["kite", "dhan"], default="kite", help="best_source on a tie (default kite)")
    b.add_argument("--force", action="store_true", help="rebuild every day, changed or not")
    b.add_argument("--prune", action="store_true", help="remove published days whose raw folder is gone")
    args = p.parse_args(argv)
    env = load_env()
    if args.cmd == "move-raw":
        return move_raw(args.froms, args.source, args.to or env.get(FNO_RAW_KEYS[args.source], ""), apply=args.apply)
    raw = {"kite": args.raw_kite or env.get(FNO_RAW_KEYS["kite"]) or None,
           "dhan": args.raw_dhan or env.get(FNO_RAW_KEYS["dhan"]) or None}
    underlyings = sorted(UNDERLYINGS) if args.underlying == "all" else [args.underlying]
    return publish(raw, args.out or fno_backtest_dir(env), underlyings, args.date_from, args.date_to,
                   args.prefer, args.force, args.prune)


if __name__ == "__main__":
    sys.exit(main())

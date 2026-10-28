"""Re-download the MISSING_IN_KITE contracts of one saved F&O day (e.g. a Dhan day whose contracts came back empty
at download time), one by one with the dashboard's Re-download route (fno.redownload_contract), then check the day
against its saved bhavcopy once. Run it by hand.

    python fno_redownload.py YYYY-MM-DD --source dhan|kite --underlying NIFTY|SENSEX
                             [--keep-zero-volume | --no-keep-zero-volume] [--limit N] [--only SYMBOL ...] [--apply]

The missing list is the day's bhavcopy/bhav_compare.csv (run the check first). Without --apply it only lists the
contracts by expiry and type (expired ones are skipped: the broker no longer serves them), the backup folder and a
time estimate; nothing is fetched or written. --apply: backup copy of the day folder under <home>/archive/ (SHA-256
manifest, checked), token check, the contracts at 2.5 requests/s, the bhavcopy check once, a summary. Keep
zero-volume contracts is ON by default (the with_zero_volume store); the without_zero_volume and backtest stores
are never touched. Reads the folder and tokens from .env (sign in on the dashboard / Dhan tab first).
Exit codes: 0 done or dry run, 1 refused or error, 2 stopped early (token or network; run again to continue)."""
import argparse
import sys
from datetime import date

from backfill import fno
from backfill import fno_redownload as fr
from config import default_fno_folder, load_env
from nifty_daily import sign_in


class Parser(argparse.ArgumentParser):
    def error(self, message):   # argparse exits with 2, which is kept for a stopped run
        self.print_usage(sys.stderr)
        print(f"error: {message}", file=sys.stderr)
        sys.exit(fr.EXIT_REFUSED)


def main(argv=None):
    p = Parser(description="Re-download the MISSING_IN_KITE contracts of one saved F&O day (dry run without --apply).")
    p.add_argument("date", help="YYYY-MM-DD, a saved and checked day")
    p.add_argument("--source", required=True, choices=sorted(fno.FOLDERS))
    p.add_argument("--underlying", required=True, choices=sorted(fno.UNDERLYINGS))
    p.add_argument("--keep-zero-volume", action=argparse.BooleanOptionalAction, default=True,
                   help="keep contracts whose candles all have volume 0 (default ON: the with_zero_volume store)")
    p.add_argument("--limit", type=int, help="fetch at most N contracts (for a first small test)")
    p.add_argument("--only", nargs="+", metavar="SYMBOL", help="only these missing contracts, e.g. NIFTY2260023NOV26CE")
    p.add_argument("--apply", action="store_true", help="back up the day and re-download (default: only list)")
    args = p.parse_args(argv)
    try:
        run_date = date.fromisoformat(args.date)
    except ValueError:
        p.error(f"invalid date {args.date!r}, use YYYY-MM-DD")
    if args.limit is not None and args.limit < 1:
        p.error("--limit must be at least 1")
    only = [s.strip().upper() for s in args.only] if args.only else None
    env = load_env()
    folder = default_fno_folder(args.source, args.keep_zero_volume, env)

    def connect():
        api_key, token, extra, code = sign_in(args.source)   # Dhan: token, profile and Data API plan checked
        if code:
            return None, "", "", "the access token check failed (see above)."
        client = extra.get("kite")
        if client is None:   # Kite: kite.profile() once
            client, msg, status = fno.connect(api_key, token, lambda m: None, report=False)
            if status:
                return None, "", "", msg
        return client, api_key, token, ""

    try:
        return fr.run(run_date, args.source, args.underlying, folder, env, keep_zero=args.keep_zero_volume,
                      only=only, limit=args.limit, apply=args.apply, connect=connect)
    except KeyboardInterrupt:
        print("Interrupted. Saved files stay; run the same command again to continue.", file=sys.stderr)
        return fr.EXIT_STOPPED


if __name__ == "__main__":
    sys.exit(main())

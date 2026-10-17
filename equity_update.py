"""The daily equity update in one command (backfill/equity/update.py): runs the existing equity CLIs in order.

    python equity_update.py [--folder PATH] [--offline] [--with-reference] [--force] [--from-step NAME] [--dry-run]

Steps: reference (nse_reference.py, only with --with-reference), bhavcopy (cm_bhav.py --missing), master
(security_master.py build + check), actions (adjustments.py fetch-actions), adjustments (adjustments.py build +
check), exits (exits.py build + check). --offline skips the three network steps (NSE only, never a broker). A
build whose build_info.json is newer than all its inputs is skipped as "up to date" (--force runs it). A
download that answers 1 (some days / years / files failed) is a WARNING and the update goes on; a build or check
that fails, or any step answering 2, stops it. Every step's output goes to <folder>/equity_update.log.
--folder defaults to EQUITY_DATA_DIR, else KITE_DATA_DIR or ./data. Run by hand; no cron.
Exit codes: 0 all OK (warnings allowed, listed in the summary), 1 a build or check failed (or a step crashed),
2 bad arguments, another update running, or a step answered 2 (its lock busy)."""
import argparse
import os
import sys

from backfill.equity import update


def main(argv=None, runner=None, now=None):
    p = argparse.ArgumentParser(description="Run the daily equity update (bhavcopy, security master, adjustments, "
                                            "exits) in order.")
    p.add_argument("--folder", help="data folder, default EQUITY_DATA_DIR, else KITE_DATA_DIR or ./data")
    p.add_argument("--offline", action="store_true", help="no network: skip reference, bhavcopy and actions")
    p.add_argument("--with-reference", action="store_true", help="also refresh the NSE reference lists first")
    p.add_argument("--force", action="store_true", help="run every build, even when up to date")
    p.add_argument("--from-step", choices=update.STEP_NAMES, help="start at this step")
    p.add_argument("--dry-run", action="store_true", help="print what would run; run nothing")
    args = p.parse_args(argv)   # argparse errors exit with 2
    if args.folder and not os.path.isabs(args.folder):
        p.error("--folder must be an absolute path")
    return update.run(args.folder, args.offline, args.with_reference, args.force, args.from_step, args.dry_run,
                      runner=runner or update.run_step, now=now,
                      argv=sys.argv[1:] if argv is None else list(argv))


if __name__ == "__main__":
    sys.exit(main())

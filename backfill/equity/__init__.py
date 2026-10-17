"""Equity modules: the security master, split/bonus factors, exits and index membership.

New equity code goes here. It never imports F&O modules (backfill.fno*, backfill.bhav*, backfill.dhan_api,
dhan, nifty_*, fno_store; tests/test_equity_imports.py checks it) and takes its lock from backfill.locks.
cm_bhav.py and nse_ref/ stay where they are for now."""

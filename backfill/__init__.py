"""Bulk backfill: candles for every NSE EQ stock over a chosen range (max 1 year).

Progress is tracked per (run, symbol, chunk) in SQLite, so a run can be stopped
and resumed; data is merged into <folder>/<interval>/<SYMBOL>/<year>.<csv|parquet>.

- job.py          in-memory job state shared with the dashboard
- fetch.py        request chunking, rate limiting and retries
- instruments.py  which NSE instruments count as equity
- storage.py      candle files, progress database and error log
- runner.py       the background download loop
"""

# Re-download a day's missing contracts (`fno_redownload.py`)

`backfill/fno_redownload.py` (logic), `fno_redownload.py` (CLI), `tests/test_fno_redownload.py`.

## Why
A broker can return empty candles for live contracts at download time and serve them later (NIFTY Dhan 2026-10-22:
464 MISSING_IN_KITE contracts, all live, empty at 23:56, served on 28 Oct). The day is then outside the download
window, so `nifty_daily.py` refuses it. The dashboard's single-contract Re-download works, but it is one click per
contract. This command loops that same route over the day's missing list.

## Command
```
python fno_redownload.py DATE --source dhan|kite --underlying NIFTY|SENSEX
       [--keep-zero-volume (default) | --no-keep-zero-volume] [--limit N] [--only SYMBOL ...] [--apply]
```
- Folder: `config.default_fno_folder(source, keep_zero)`, so with the default ON it uses the with_zero_volume store
  under `FNO_RAW_<SOURCE>`. `fno.store_mismatch` is checked.
- Missing list: `fno.day_dir` (the same lookup as the check), then `fno.missing_in_kite` (the MISSING_IN_KITE rows
  of `bhavcopy/bhav_compare.csv`). It refuses when there is no check yet, or 0 missing. `--only` names must be in
  that list (case-insensitive on the CLI).
- Left out: contracts that already have a file on disk (`bhav.disk_files`, e.g. saved before an early stop), and
  expired contracts (expiry from the name, `< fno.today_ist()`, the same rule as `redownload_contract`).
  `--limit N` keeps the first N live ones.

## Dry run (default)
No network and no writes. It prints a table per expiry (FUT / CE / PE / Total / Fetch / Expired), live and expired
totals, the backup folder it would use, the speed and the time estimate (`live / speed`).

## `--apply`, in order
1. Refuse if `JOB` is running (same process). Then take `fno.acquire_lock(folder, underlying, "fno_redownload.py",
   source)` and hold it to the end. Busy means refused (exit 1).
2. Backup: `<fno_clear.home_of(env)>/archive/<YYYY-MM-DD_HHMM IST>_redownload_<U>_<source>_<date>/<day folder>/`
   via `fno_import.copy_checked` (via `.part`, sizes + CSV SHA-256). Then `manifest.json` next to it (every file's
   path, size and SHA-256), with each copy re-hashed and compared. Any failure, no home, or an existing folder means
   refused, and nothing is fetched. Only the day folder is copied (`progress.sqlite` is not).
3. Token, once: Dhan uses `nifty_daily.sign_in` (`token_ok`, `profile()`, `data_plan_error`). Kite uses
   `fno.connect(report=False)`. A failure stops with exit 2.
4. Loop: a `RateLimiter(SPEED)` wait, then `fno.redownload_contract(..., kite=client, keep_zero, derive_to=None)`.
   That gives the same expiry rule, file layout, progress.sqlite row and `=== Re-download of` run.log line.
   `SPEED` = 2.5 requests/s for both sources (`job.DEFAULT_SPEED`), never above the dashboard's Dhan default.
   Each result is classified by `outcome()` from the returned message: saved / no_candles / expired /
   fetch_failed (`"<contract>: <error>"`, i.e. fetch_chunk raised) / failed. After a fetch failure it re-checks the
   token (`profile()`). If the token is refused, the broker is unreachable, or `MAX_FAILS_IN_A_ROW` (3) fetch failures
   come in a row, the run stops (exit 2). Ctrl+C stops the same way. A progress line is printed every 25.
5. `bhav.check_saved` once, also after an early stop when at least one contract was tried. bhav_compare.csv then
   lists only what is still missing, so a re-run continues.
6. Summary (console and day run.log): saved, still no candles, skipped expired, failed, not tried, Missing
   before -> after, and the new severity (`bhav.day_severity`). A `=== fno_redownload.py` line is also written to
   run.log at the start.

Never touched: the without_zero_volume store (`derive_to=None`; refresh with `fno_store.py clear` + `derive-without`)
and the backtest store (publish later). Nothing is deleted.

## Exit codes
0 done or dry run; 1 refused or error (incl. a failed bhavcopy check); 2 stopped early (token / network / Ctrl+C;
run again to continue).

## Tests
`tests/test_fno_redownload.py` builds a synthetic Dhan day (with store under a stubbed `FNO_RAW_DHAN`, a checked
bhavcopy with 1 expired + 3 live missing options, a without store) and uses the real `redownload_contract` with a
`FakeDhan` client. `fno.now_ist` is patched (IST) and requests are blocked. Covered: dry run writes nothing; the
refusals (no check, 0 missing, unknown `--only`, store mismatch, lock, job, backup failure, archive exists, token at
start); apply (files, progress rows, run.log, backup + manifest hashes, one check, Missing 4 -> 1, without store
unchanged); `--limit` / `--only` and re-runs; files on disk skipped; token failure mid-run then a re-run; 3 failures in
a row; one failure; Ctrl+C; no candles; `outcome`; the CLI (dry run, apply, speed shown, argument errors, exit codes).

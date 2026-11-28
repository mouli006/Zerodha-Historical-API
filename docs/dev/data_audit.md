# Data audit (#249, D212, point 101)

Goal: before the backtest engine, every missing day in every store has a reason. The gate is **UNEXPLAINED = 0 and
WAITING = 0**: only PERMANENT reasons left.
- Code: `backfill/equity/audit.py` (read only; writes only its own new output folder; takes no lock).
- Tests: `tests/test_audit.py` (25; a synthetic world in `tmp_path`, the per-stock equity tables hand-made).
- Discovery and decisions: `discovery-249-data-audit.md` in the handover folder (reasons option A, NIFTY 8 + 9 Nov
  both sources, Mouli's changes 1-3 of 20 Nov).

## Command
```
python -m backfill.equity.audit run --folder F [--fno-folder B] [--fno-raw-kite K] [--fno-raw-dhan D] [--out DIR]
                                   [--stores equity,eod,master,fno,icici] [--stocks SID,...] [--dhan-rolling R]
```
- `--fno-folder` defaults to `.env` FNO_BACKTEST_DIR (`config.fno_backtest_dir()`), `--fno-raw-*` to FNO_RAW_KITE /
  FNO_RAW_DHAN (`config.fno_raw_dir`). A worktree has no `.env`: the backtest default is then empty (refused) and the
  raw default `<project>\data` (refused by the folder check). Pass all three on the command line.
- Every path must be absolute and exist. With `fno`: each of the 4 `days.csv`
  (`<B>\{with,without}_zero_volume\{NIFTY,SENSEX}\days.csv`) and each raw store folder
  (`<K>\{with,without}_zero_volume\{NIFTY,SENSEX}_FnO_1min`, Dhan `..._FnO_1min_Dhan`) must exist, else exit 2 naming
  the path (so the F&O lock check can never be silently empty).
- Every resolved path is printed in `report.txt` and written to `run_info.json`.

## Exit codes and refusals
- 0 = CLEAN, 1 = GAPS (UNEXPLAINED or WAITING > 0), 2 = refused / error / **stores changed during the run**.
- Refused (exit 2, nothing written): a writer lock held (`minute_combined.busy`: Kite store, ICICI store,
  `equity_update.lock`, bhavcopy store; with `fno` the 8 F&O run locks `<raw store>\.lock`); `minute_plan.check_inputs`
  (an input build running, not built or stale: master vs newest bhavcopy year, adjustments vs master, exits vs
  adjustments, index vs master; it does **not** look at the ICICI `bhav_cache`, which only `icici_minute.py plan` /
  `checks` need, handover 4.E E4); a path missing or not absolute; `--out` exists; with `icici`, a non-empty
  `equity_1min_icici\download_progress.sqlite-wal` (the db is read with `immutable=1`, which skips the WAL: a run
  ended without a clean finish; never delete the -wal file; the next normal ICICI run merges it). Any other error
  also ends with exit 2. The Kite `download_progress.sqlite` is never opened, only snapshotted.
- Stores changed: a snapshot before and after the run of every lock (held + mtime) and of the 4 F&O `days.csv`, both
  `download_progress.sqlite` and their `-wal` files, every F&O `bhav_compare.csv` the contract check reads (#253), the master `build_info.json`, `NSE_CM_bhav\days.csv`, the ICICI plan's `icici_plan_days.parquet` and the newest bhavcopy year file
  (mtime + size). Any difference or a lock held at the end: the folder is still written, `report.txt` and
  `summary.csv` start with "RESULT NOT VALID: stores changed during the run, run again", exit 2.

## Reasons (first match wins)
| Store | Expected days | Order |
|---|---|---|
| equity 1-minute | every NONE traded day of every STOCK (`minute_combined.stock_sources`: traded = bhavcopy row EQ / BE / BZ with volume) | manual LOST / P65_RETRY (a P65 row past `clear_by` -> UNEXPLAINED) -> NOT_FINISHED / UPDATE_PENDING -> ICICI_EMPTY / ICICI_BEFORE_START -> VENDOR (NOT_IN_ICICI on a day of the ICICI plan's day list `equity_1min_icici/plan/icici_plan_days.parquet`, ICICI_MIXED_SERIES; NOT_IN_ICICI on any other day -> UNEXPLAINED `<kite>+NOT_IN_ICICI_AFTER_PLAN`; the list's path and rows are in the Paths block and run_info.json, an unreadable list is reported and leaves every NOT_IN_ICICI day UNEXPLAINED) -> WAITING_ICICI (ICICI_NOT_YET_FETCHED) -> UNEXPLAINED (evidence = the combined reason, e.g. NOT_IN_ICICI_PLAN = P178) |
| EOD | every weekday 2015-01-01 -> today (IST) plus the special-session weekend days, minus `days.csv` holiday rows | holiday row -> HOLIDAY (listed); `ok` row with its raw file = present; else manual LOST -> NOT_FINISHED / UPDATE_PENDING -> UNEXPLAINED |
| master | `build_info.json` last_date vs the newest `ok` bhavcopy day | older -> UNEXPLAINED; kinds counted, ETF / RIGHTS = NOT_DOWNLOADED_KIND (one count line) |
| F&O | each `days.csv` x kite / dhan, the EOD expected days from the set's first row -> today | `<src>_available` True: present, but an empty / `unchecked` `<src>_check` -> UNEXPLAINED; missing row / not available: manual LOST -> NOT_FINISHED / UPDATE_PENDING -> UNEXPLAINED |
| F&O contracts (#253, store `fno_contract`) | every raw day folder of the 8 raw stores, its `bhavcopy\bhav_compare.csv` (written by the bhavcopy check / "Check all"; the audit never compares again: `backfill/equity/` may not import `backfill.bhav`) | no compare file, or one listing another number of files than the folder holds (stale); a contract `MISSING_IN_KITE` in the day's `best_source` store (backtest `days.csv`, per variant), one row per date / underlying / variant / source (decision: option A, Mouli 22 Nov): manual `fno_contract` row (key underlying, date) -> NOT_FINISHED / UPDATE_PENDING (#264; `newest` = that variant / underlying's newest backtest `days.csv` row) -> UNEXPLAINED; missing in the other source = listed only |
| ICICI | progress db (`mode=ro&immutable=1`, own connection: `icici_download.Progress` opens without `immutable=1`) | FAILED chunk / REVIEW stock -> UNEXPLAINED |

Why this order (approved option A): the equity base is traded days, so HOLIDAY / NO_TRADE / listing reasons cannot
occur there; DAY_EMPTY days are loaded from Kite, so they are a **flag**, not a gap; the manual file first, so a named
LOST day never shows as WAITING; VENDOR is derived from the stores because the newest saved `vendor_days.csv`
(7 Nov, 278 stocks / 161,956 days) is the uncorrected list (corrected 305 / 162,508 exists only as totals).

**NOT_FINISHED vs UPDATE_PENDING** (both WAITING): NOT_FINISHED = now is before the day's D+1 01:00 IST
(`minute_download.finished_at`); UPDATE_PENDING = finished but later than the store's newest day (equity: the newest
Kite-stored day; EOD: the newest `days.csv` row; F&O: the set's newest row). A weekday without a holiday row is never
HOLIDAY until `equity_update.py` marks it. So a weekday run before the evening `equity_update.py` always shows today as
NOT_FINISHED; a weekend run after the Friday routine is the clean check.

## Manual known gaps
`reference/manual_known_gaps.csv` (repo copy) and `NSE_CM_bhav\reference\manual_known_gaps.csv` (data copy, the one
the audit reads; copy it there after review, like the other manual files). Columns: `store, security_id, symbol,
underlying, from_date, to_date, reason (LOST | P65_RETRY), problem_id, clear_by, note`. `store` = equity (key
security_id), fno (key underlying, both sources and both variants), fno_contract (key underlying: the day's best-source missing contracts, #253), eod (underlying NSE_CM) or prices (key security_id: a known price day of the #251 check, reason KNOWN there; no rows before the first real run). Missing data copy:
`report.txt` says so and those days stay UNEXPLAINED. Rows that match no gap are listed as "unused manual row" (info).
Start rows (20 Nov): ELPROINTL / DRAGARWQ 2026-11-17 LOST P204; 11 P65 stocks 2026-11-16 P65_RETRY clear_by
2026-11-22 (each checked: a 16 Nov bhavcopy row; RAJVIR left out, last traded 2026-10-26); NIFTY 2026-11-08..09 LOST
P188 both sources. COMPINFO (P215) is not added.

## Output (`<folder>\analysis\audit_<IST date>[_b, _c ...]\`, each file atomic)
| file | content |
|---|---|
| `report.txt` | paths, F&O lock-file count, counts per store / reason / kind, flags (DAY_EMPTY, NO_BHAV_ROW, frozen, vendor cross-check, EOD / master / F&O / ICICI counts, unused manual rows), WAITING per reason with when it clears, the full UNEXPLAINED list, verdict, run time, peak memory |
| `summary.csv` | store, reason, kind, days |
| `gaps.csv` | store, security_id, symbol, underlying, source, variant (with / without for F&O, else empty; #253), date, reason, kind, evidence, clears_when |
| `fno_missing_contracts.csv` | (with `fno`) every contract a raw store lacks: date, underlying, variant, source, contract, expiry, bucket, type, strike, bhav_volume, bhav_oi, status, cause (ZERO_VOLUME_DROPPED = a without store lacks it and the same source's with store has it at volume 0; NOT_DOWNLOADED; BAD_NAME = a bhavcopy row that could not be named, never a gap), other_source_has (yes / no / empty = no compare file that day), best_source, day_folder |
| `fno_contracts_summary.csv` | per store-day: bhav_traded, traded_files, extra_files, files, missing, zero_volume_dropped, not_downloaded, bad_names, other_source_has, best_source, adds_up (traded_files + missing = bhav_traded) |
| `fno_price_flags.csv` | (with `fno`, #265; header only when nothing is flagged) one row per flagged contract and store-day: date, underlying, variant, source, contract, expiry, bucket, type, strike, bhav_volume, store_volume, bhav_last, store_last, store_last_regular (the bar before 15:30), last_bar, bhav_close, bhav_settle, diff, diff_pct, other_source_flagged (yes / no / empty), best_source, day_folder |
| `fno_dhan_rolling.csv` | (with `fno`, #254; header only when nothing is found) one row per day and contract of every Dhan rolling expiry folder, covered or traded: date, underlying, expiry, contract (the #253 name), type, strike, coverage (full = every minute of that day / partly / none = traded, not covered), minutes, bars, first, last, labels, bhav_volume (contracts, from the folder's `bhav_compare.csv`), lost_row (yes = an fno LOST gap on that underlying and day), folder |
| `price_days.csv` | (with `equity`, #251; header only when nothing is listed) one row per listed loaded day: security_id, symbol, date, source, list (CLOSE / HIGHLOW), loaded_close / high / low, bhav_close / last / high / low, close_off_pct, hl_off_pct, reason, problem_id, evidence |
| `price_summary.csv` | (#251) list, source, reason, days, stocks |
| `unexplained.csv` / `waiting.csv` | the rows of those kinds |
| `changes.csv` | FILLED / NEW / REASON vs the newest older `audit_*` **folder** (a `audit_*.log` file is ignored), keyed by store, security_id, underlying, source, variant, date (an older gaps.csv without `variant` reads it as empty). A test run written with `--out` to a name not starting `audit_` is never the baseline |
| `run_info.json` | start / end, git commit, stores, every path, input mtimes / sizes / held, verdict, counts, peak memory, exit code |

## F&O fresh days (#264)
A raw day folder without (or with a stale) `bhav_compare.csv`, and a best-source missing contract, go through the
same order as the F&O days: manual `fno_contract` row -> `time_reason` -> UNEXPLAINED. Before D+1 01:00 IST the day is
WAITING NOT_FINISHED (the evening download / Check all has not run yet); after that, a day newer than the backtest
`days.csv` is WAITING UPDATE_PENDING ("after the next F&O download + check + publish"); a finished, published day stays
UNEXPLAINED. A best-source gap only exists on a day that is in `days.csv` (its best source comes from there), so it can
only be NOT_FINISHED, never UPDATE_PENDING. There is no F&O clock of its own: the equity D+1 01:00 IST rule is reused,
as `audit_fno` does. Tests 18-20 of `tests/test_audit_fno_contracts.py`.

## F&O price flags (#265, a flag only)
Is the stored last price of each contract-day near NSE / BSE's LAST? `audit_fno_contracts.price_flags` reads the same
`bhav_compare.csv` files as the #253 check (CSV only, no import of `backfill.bhav`, so own constants).
- **Rule (D235):** flag when `|stored last − Bhav_last| > max(5 % × Bhav_last, ₹0.20 = 4 ticks)`
  (`PRICE_PCT = 0.05`, `PRICE_FLOOR = 0.20`). Stored last = the `Kite_last` column (close of the day's last bar, also on
  the Dhan stores). `Bhav_close` / `Bhav_settle` / the 15:29 bar are shown, never the flag: F&O close and settlement are
  not the last trade (discovery 26 Nov: stored last vs LAST agreed within 0.5 % on 98 % of compared rows).
- **Skipped rows:** BAD_NAME, no stored file, Bhav_volume 0, Bhav_last 0 / empty, no stored last. A compare file
  without the price columns (old) is counted as "no price columns", never an error; a missing one is already a #253 gap.
- **Flag only (D234):** never in `gaps.csv`; reason, kind, VERDICT and exit code never change. After a week of daily
  counts Mouli looks again at "best source only".
- `report.txt` (Flags): one line "F&O price flags (#265, flag only; …): N rows on M store-days; in the best source K;
  in both sources J (contract-days flagged in kite and dhan); compared C; no price columns F files", then one line per
  store. `run_info.json` `fno_prices`: limit_pct, limit_ticks, compared, flags, best_flags, both, no_price_columns.
  The compare files are read before the stores-changed snapshot is taken, so a change during the run still gives exit 2.
- Tests 11-17 of `tests/test_audit_fno_contracts.py`.

## Dhan rolling (#254, a flag only)
NIFTY 8 + 9 Nov 2026 are fno LOST in both sources (P188); on 11 Nov (#219) Dhan's expired-options endpoint gave the
9-Nov-expiry ATM ± 10 strikes, saved outside the backtest store in `<R>\<UNDERLYING>\exp-YYYY-MM-DD\`
(`by_strike.parquet`, `bhav_compare.csv`, `bhav\BhavCopy_NSE_FO_*_YYYYMMDD_F_0000.csv.zip`; written by the one-off
scripts in that folder, not the repo). Module `backfill/equity/audit_dhan_rolling.py` (pandas / pyarrow / zipfile only;
its `contract_name` is a copy of `backfill/fno.py`'s, a test compares them). Tests: `tests/test_audit_dhan_rolling.py` (8).
- `R` = `--dhan-rolling` (absolute), default `<parent of the F&O backtest folder>\dhan_rolling` (`D:\Data\fno\dhan_rolling`).
- `report.txt` (Flags): per day and expiry folder: contracts (CE / PE), bars, minutes first-last, strike range; the
  bhavcopy's traded contracts of that expiry covered full / partly / none; all traded NIFTY options + futures of the day
  (from the zip); `-` when a file is missing. Info lines: a rolling day with no fno LOST row, folder not found, NOT read.
- Each fno LOST gap of that underlying and day gets ` | Dhan rolling exp …: N contracts, M bars (traded of that expiry:
  full a, partly b of t); fno_dhan_rolling.csv` added to its evidence. Reason, kind, clears_when, `summary.csv`,
  `changes.csv`, the verdict and the **exit code never change**; a missing folder, an unreadable file or a file changed
  during the run is a report line only (the rolling files have their own before / after list in `run_info.json`
  `dhan_rolling`, never in "stores changed").

## Prices (#251, a flag only)
Is every **loaded** stock-day at the right price? Module `backfill/equity/audit_prices.py` (pandas / pyarrow / numpy;
no F&O imports). Tests: `tests/test_audit_prices.py` (18). Runs with `equity`, inside the equity pass (no second
`stock_sources` pass; only the listed rows are kept).
- **Days checked:** the traded days the combined loader takes from KITE or ICICI (never NONE, so never an ICICI mixed
  day, P162). Day values from the days.parquet files, not the bars: `minute_close / high / low` converted like the
  loaders (Kite: `minute_store.convert`, i.e. × `minute_scale`, then × `ratio` or ADDITIVE `kite_multiplier` × (p +
  `kite_difference`); ICICI: × `icici_scale`). A weekly bar pass with `load_minute_best` is a separate, later command.
- **NSE side:** close / high / low from days.parquet (`bhav_*`); LAST from the bhavcopy year files (EQ / BE / BZ, volume
  > 0, the highest-volume row per stock-day). The trading calendar = the year files' dates.
- **Measures:** close_off = min(|close / NSE close − 1|, |close / NSE LAST − 1|) (NSE close is a 30-minute average);
  hl_off = max(|high / NSE high − 1|, |low / NSE low − 1|).
- **Lists:** CLOSE = close_off > 5 %; HIGHLOW = hl_off > 20 % with close_off ≤ 5 %; one report line counts close_off > 2 %.
- **Reasons** (first match wins; ratios tried = close / close, close / LAST, high / high, low / low; a target within 2 %
  of 1.0 is never used):

| # | reason | problem | rule |
|---|---|---|---|
| 1 | KNOWN | manual | a `prices` row of `manual_known_gaps.csv` (problem_id from the row) |
| 2 | SPLIT_BLIP | P173 / P175 | Kite: an applied event (`adjustments/events.csv`, applied = Y) 1-10 trading days after the day; a ratio within 2 % of F, 1 / F, or F × the neighbour event's factor (or its inverse) |
| 3 | NOT_ADJUSTED | P179 | Kite: a ratio within 2 % of the product (or inverse) of a contiguous run of later applied factors |
| 4 | PENNY_TICK | P180 | NSE close < ₹1, or (Kite) minute close × `minute_scale` < ₹0.50 |
| 5 | DIVIDEND_CUT | P205 | Kite, MULTIPLICATIVE, stored `ratio` ≠ 1 (by > 0.001) and an NSE dividend ex-date (`adjustments/input/corporate_actions_*.csv`, PURPOSE "Dividend", ISIN → owner) 1-10 trading days after the day |
| 6 | JUNK_DAY_CANDLE | P182 | Kite: day close × `kite_multiplier` more than 20 % off the NSE close (TIL 6 Nov 2017: 2.80 against ~148) |
| 7 | STRAY_BARS | P183 | Kite: day close × `kite_multiplier` within 2 % of NSE close or LAST, so the minutes are wrong (PIIND) |
| 8 | ICICI_SCALE | P165 / P224 | ICICI: a ratio within 2 % of 0.1 / 0.2 / 0.25 / 0.5 / 2 / 4 / 5 / 10, or of a later split / bonus factor (rules 2 / 3 on the ICICI day); the evidence names the matching later factor and its ex-dates (BRITANNIA 2015, half price) |
| 9 | ICICI_SHORT_DAY | — | ICICI: label SHORT_DAY |
| 10 | UNEXPLAINED | — | anything else |

  SPLIT_BLIP and NOT_ADJUSTED are Kite rules; an ICICI day at such a factor is ICICI_SCALE (decision of 22 Nov).
  ZERO_VOLUME_FILLER (P185) needs the bars: it waits for the weekly bar pass.
- **STRAY_BARS is wide on purpose:** as code it catches every Kite day whose day candle is right and whose minutes are
  not (415 CLOSE days in 171 stocks on 22 Nov, not only PIIND's 19). **Q1 option 3 (UNEXPLAINED counts) may only be
  decided after the top STRAY_BARS stocks (UMESLTD, TFL, WSI …) are reviewed.**
- **Flag only:** a #251 row never enters `gaps.csv`; it never changes a reason, a kind, the VERDICT or the exit code.
  `report.txt` (Flags): loaded days checked, the 2 % line, CLOSE and HIGHLOW per source × reason, the UNEXPLAINED
  (CLOSE) top 10 stocks, note lines. `run_info.json` `prices`: counts per list / source / reason, notes.
- **Errors are notes:** no year file → "price check not run"; an unreadable days.parquet → "<sid>: price check failed"
  (first 20 named); no events.csv / corporate actions → the rules that need them never match. The exit code is the
  same as without the check.
- **Inputs in "stores changed":** `adjustments/events.csv` and the newest `corporate_actions_*.csv` join the snapshot;
  the days.parquet files are covered by the Kite / ICICI locks and progress dbs, the newest year file was already in.
- First discovery (22 Nov, `f583e4d`, read only): loaded days 4,900,218; CLOSE 1,059 days / 271 stocks (Kite 734,
  ICICI 325); HIGHLOW 717; 2 % line 4,344; run time of the pass ~2 min standalone.

**May run together:** with nothing that writes what it reads (6.3): not with a Kite update / fix, an ICICI download,
`equity_update.py`, an F&O download or publish. Memory: the equity pass is `minute_combined`'s (990 MB peak on all
3,116 stocks, 3 Nov).

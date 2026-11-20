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
                                   [--stores equity,eod,master,fno,icici] [--stocks SID,...]
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
  `checks` need, handover 4.E E4); a path missing or not absolute; `--out` exists.
- Stores changed: a snapshot before and after the run of every lock (held + mtime) and of the 4 F&O `days.csv`, both
  `download_progress.sqlite`, the master `build_info.json`, `NSE_CM_bhav\days.csv` and the newest bhavcopy year file
  (mtime + size). Any difference or a lock held at the end: the folder is still written, `report.txt` and
  `summary.csv` start with "RESULT NOT VALID: stores changed during the run, run again", exit 2.

## Reasons (first match wins)
| Store | Expected days | Order |
|---|---|---|
| equity 1-minute | every NONE traded day of every STOCK (`minute_combined.stock_sources`: traded = bhavcopy row EQ / BE / BZ with volume) | manual LOST / P65_RETRY (a P65 row past `clear_by` -> UNEXPLAINED) -> NOT_FINISHED / UPDATE_PENDING -> ICICI_EMPTY / ICICI_BEFORE_START -> VENDOR (NOT_IN_ICICI, ICICI_MIXED_SERIES) -> WAITING_ICICI (ICICI_NOT_YET_FETCHED) -> UNEXPLAINED (evidence = the combined reason, e.g. NOT_IN_ICICI_PLAN = P178) |
| EOD | every weekday 2015-01-01 -> today (IST) plus the special-session weekend days, minus `days.csv` holiday rows | holiday row -> HOLIDAY (listed); `ok` row with its raw file = present; else manual LOST -> NOT_FINISHED / UPDATE_PENDING -> UNEXPLAINED |
| master | `build_info.json` last_date vs the newest `ok` bhavcopy day | older -> UNEXPLAINED; kinds counted, ETF / RIGHTS = NOT_DOWNLOADED_KIND (one count line) |
| F&O | each `days.csv` x kite / dhan, the EOD expected days from the set's first row -> today | `<src>_available` True: present, but an empty / `unchecked` `<src>_check` -> UNEXPLAINED; missing row / not available: manual LOST -> NOT_FINISHED / UPDATE_PENDING -> UNEXPLAINED |
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
security_id), fno (key underlying, both sources and both variants) or eod (underlying NSE_CM). Missing data copy:
`report.txt` says so and those days stay UNEXPLAINED. Rows that match no gap are listed as "unused manual row" (info).
Start rows (20 Nov): ELPROINTL / DRAGARWQ 2026-11-17 LOST P204; 11 P65 stocks 2026-11-16 P65_RETRY clear_by
2026-11-22 (each checked: a 16 Nov bhavcopy row; RAJVIR left out, last traded 2026-10-26); NIFTY 2026-11-08..09 LOST
P188 both sources. COMPINFO (P215) is not added.

## Output (`<folder>\analysis\audit_<IST date>[_b, _c ...]\`, each file atomic)
| file | content |
|---|---|
| `report.txt` | paths, F&O lock-file count, counts per store / reason / kind, flags (DAY_EMPTY, NO_BHAV_ROW, frozen, vendor cross-check, EOD / master / F&O / ICICI counts, unused manual rows), WAITING per reason with when it clears, the full UNEXPLAINED list, verdict, run time, peak memory |
| `summary.csv` | store, reason, kind, days |
| `gaps.csv` | store, security_id, symbol, underlying, source, date, reason, kind, evidence, clears_when |
| `unexplained.csv` / `waiting.csv` | the rows of those kinds |
| `changes.csv` | FILLED / NEW / REASON vs the newest older `audit_*` **folder** (a `audit_*.log` file is ignored) |
| `run_info.json` | start / end, git commit, stores, every path, input mtimes / sizes / held, verdict, counts, peak memory, exit code |

**May run together:** with nothing that writes what it reads (6.3): not with a Kite update / fix, an ICICI download,
`equity_update.py`, an F&O download or publish. Memory: the equity pass is `minute_combined`'s (990 MB peak on all
3,116 stocks, 3 Nov).

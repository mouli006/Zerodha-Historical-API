# Special sessions (Muhurat, budget weekends, special Saturdays, outages)

NSE sometimes trades outside the normal weekday 09:15-15:30 session: the Diwali Muhurat session (an evening, once an
afternoon, sometimes on a Sunday), Union Budget weekends, special Saturdays, and days that opened late after an outage.
This calendar is kept by hand, and `cm_bhav.py --special` fetches the weekend ones into the CM bhavcopy store.
- Code: `backfill/equity/special_sessions.py` (the calendar, read only) and `cm_bhav.py --special [--apply]`
  (`backfill/cm_bhav.py`: `special_plan`, `check_special`, `run(special=True)`).
- Nothing else reads the calendar yet. No minute row or label was changed (see "Later use").

## Why (found 27 Oct from the data, read only)
- `cm_bhav.weekdays()` and `run()` asked NSE for Mon-Fri only, so the 11 weekend sessions are missing from the
  bhavcopy. Kite has full minutes for them, labelled `EXTRA_SESSION` (no bhavcopy row).
- **How they were found:**
  - days whose bhavcopy EQ trades were below 40% of the median of the previous 20 trading days (8 days);
  - days on which most Kite stocks start at 15:30 or later (10 days);
  - Kite days that are not in the bhavcopy calendar (11 days, 1,068-2,222 stocks each).
- **Weekday Muhurat days** are in the bhavcopy, and Kite labels them mostly `OK`.
  - 2017-10-19 and 2018-11-07 have no Kite day candle (`DAY_EMPTY`), so `kite_gaps.csv` lists them as
    `KITE_MISSING_SESSION`.
  - ICICI has no minutes on 2015-11-11, and labels the 2025-10-21 afternoon session `SHORT_DAY`. Its SHORT_DAY rule
    exempts only evening sessions from 16:00.
- **2017-07-10** is not a special session but an outage: trading began at 12:30, and Kite shows `VOLUME_DIFF` on 916
  stocks.

## The file `manual_special_sessions.csv`
- **Data copy:** `<EQUITY_DATA_DIR>\NSE_CM_bhav\reference\manual_special_sessions.csv`, next to `manual_mergers.csv`.
  It wins when it exists.
- **Repo backup:** `reference/manual_special_sessions.csv`, which `.gitignore` lets through with `!reference/*.csv`.
  The loader uses it when there is no data copy.
- **Keeping the copies:** after editing the data copy, copy it to the repo and commit. The data copy was not created
  with this change; it is placed when the first `--apply` is approved.

| column | meaning |
|---|---|
| `date`, `weekday` | YYYY-MM-DD and Mon..Sun. They must agree. |
| `kind` | `MUHURAT`, `BUDGET_WEEKEND`, `SPECIAL_SATURDAY` or `OUTAGE` |
| `start`, `end` | HH:MM IST: the first Kite bar and the last bar + 1 minute. Blank until known (2026-11-08). |
| `in_bhavcopy` | `yes` / `no`, as found on 27 Oct. It is information only, so `--special` does not read it. |
| `evidence` | one line: bhavcopy trades vs. the 20-day median, Kite span, median bars, stock count |
| `note` | free text |

Rows (20):
- **11 weekend sessions:**
  - BUDGET_WEEKEND: 2015-02-28, 2020-02-01, 2025-02-01, 2026-02-01;
  - MUHURAT: 2016-10-30, 2019-10-27, 2020-11-14, 2023-11-12;
  - SPECIAL_SATURDAY: 2024-01-20 (full day), 2024-03-02 and 2024-05-18 (09:15-12:30, 105 bars, so with a break).
- **7 weekday Muhurat sessions:** 2015-11-11, 2017-10-19, 2018-11-07, 2021-11-04, 2022-10-24 and 2024-11-01
  (evenings), and 2025-10-21 (13:45-14:45).
- **1 outage:** 2017-07-10.
- **2026-11-08:** a Muhurat Sunday, times to be announced by NSE.

## Loader (`backfill/equity/special_sessions.py`)
- `load_special_sessions(folder=None)` returns every row sorted by date (`date` as `datetime.date`, the rest as text).
  It raises `SpecialSessionsError`, naming the file and line, on:
  - a missing file or column;
  - a bad date, or a weekday that does not match the date;
  - an unknown kind;
  - a time that is not HH:MM;
  - `in_bhavcopy` other than yes / no;
  - a date listed twice.
- `session_kind(day, folder=None)` returns the kind or `None` (`day` is a date or `YYYY-MM-DD`).
  `is_special(day, folder=None)` returns a bool.
- `weekend_dates(folder=None)` returns the Saturday / Sunday rows (what `--special` asks for).
- `path_of(folder)` returns the file in use; `data_copy(folder)` returns the data-copy path.
- It imports only `config` and pandas (no F&O module; `tests/test_equity_imports.py`).

## `cm_bhav.py --special [--apply]`
- **Arguments:** it runs alone, with `--folder`, `--sleep` and `--no-browser`. `--apply` without `--special`,
  or `--special` with a date, `--from/--to`, `--missing`, `--offline` or `--force`, is an argparse error (exit 2).
  A bad calendar or a busy lock gives exit 2.
- **`special_plan(folder, now)`** gives each weekend row one status (read only; the db is opened `mode=ro`):

  | status | meaning |
  |---|---|
  | `in store` | already ok in the db |
  | `future` | after today, or today before 19:00 IST without a saved zip |
  | `in raw/` | a zip was saved there by hand: it is read, with no request |
  | `to ask` | everything else |

- **Dry run (the default), `check_special`:**
  - **Refuses** (`LockBusy`) while a run holds `NSE_CM_bhav\.lock`. It checks with `locks.held`, which only reads,
    so the lock file is not rewritten.
  - **Asks NSE** for each `to ask` date in its `formats_for()` order, holding the answer in memory.
  - **A found file** is written to a `tempfile.mkdtemp()` folder outside the store and checked with `parse_zip`. The
    folder is removed afterwards. It is shown as `found (format): rows, EQ/BE/BZ rows, EQ trades <- URL`, or as
    `bad file: <BadFile text>`.
  - **No file:** `not found: 404 for <url>` on a 404, `failed: <error>` on another error.
  - **Write set:** what an `--apply` would write:
    - the raw/ zips;
    - the parquet years it rebuilds, each marked `touched` (a found day in it) and / or `stale` (`stale_years()` on
      the db: the parquet already differs from the db; `run()` rebuilds those too);
    - a db row per day asked that has a file, `days.csv` and `cm_bhav.log`.
  - **Writes nothing** in the store (no db, `days.csv`, log or lock). Exit 0, or 1 when a file is bad or a download
    failed.
- **`--apply`:**
  - **What runs:** `run(folder, days, special=True)` on the `to ask` and `in raw/` days. Only those weekend dates
    pass the weekday filter.
  - **A file found** goes the normal way: an atomic `write_bytes` into `raw\<YYYY>\`, `parse_zip`, `set_day` ok, the
    touched and stale years' parquet rebuilt, then `days.csv`.
  - **No file** (a 404, before 19:00, or a failed download) records **nothing**: no db row, no `holidays.log`
    line. It is counted `not_found` (printed "not found at NSE") or `failed` (exit 1, listed with the path to save it
    under by hand).
  - **A bad zip** is recorded `failed` as on a weekday, and the zip stays in raw/.
  - **Under the same lock** as every `cm_bhav.py` run.
- **A second dry run** after an apply shows the found days `in store`: no request for them, no raw/ zip and no year
  to rebuild.
- **Every other mode is unchanged:** a weekend date given by hand is refused, and `--from/--to` / `--missing` list
  weekdays only. `--offline` still reads every zip in raw/, weekend ones included.

## A Sunday session (2026-11-08): what each tool does (read on 27 Oct, unchanged here)
| tool | behaviour |
|---|---|
| `equity_update.py` | Its bhavcopy step is `cm_bhav.py --missing` (weekdays only), so the Sunday is never asked. It is not a holiday or a failure: it never appears in `days.csv`. The later steps work from the parquet and never see it. Run `cm_bhav.py --special --apply` after adding the date (with times) to the file. |
| `cm_bhav.py 2026-11-08` | refused (exit 2, "is a Sunday"); `--from/--to` skip it |
| `equity_minute.py update` | `target_day()` counts a weekend day only once its bhavcopy is in the store. A run before Tue 01:00 targets Fri 11-06. The run after Tue 01:00 targets Mon 11-09, and `Runner.append` fetches Sat..Mon, so Kite's Sunday bars are stored, labelled `EXTRA_SESSION`. |
| `icici_minute.py` | The work list holds bhavcopy days only, so the Sunday is never asked. |
| F&O `nifty_daily.py` / dashboard | `missing_dates` walks every calendar day, and a weekend ends only at the holiday check (no candles for the nearest future and none for the spot). On a Muhurat Sunday the future has candles, so the day is downloaded and saved like a weekday. The bhavcopy check asks NSE/BSE; a 404 gives `not_available` (PENDING, `bhavcopy.log`), not a failure. |

## Later use (not built)
- Checks and loaders should read the calendar only through `special_sessions` (`session_kind` / `is_special`):
  - Kite / ICICI labels: an evening or afternoon session instead of `SHORT_DAY`, and `EXTRA_SESSION` dropped once
    the bhavcopy has the day;
  - `target_day`, and the weekday bars filter.
- **Before an `--apply` on the real store**, the weekend days become new input for:
  - the master, adjustments, exits and index membership (via `equity_update.py`);
  - the Kite / ICICI minute inputs: the bhavcopy cache is rebuilt, and the trading-day list gains weekend days;
  - a weekend Kite day candle that is missing could become a `KITE_MISSING_SESSION` row;
  - the ICICI plan could add weekend days for dead stocks.

  Estimate these read-only first. Stored minute labels are not rewritten by this change.

## Tests (`tests/test_special_sessions.py`)
- **The calendar:**
  - the repo copy is used without a data copy (20 rows, 12 weekend dates incl. 2026-11-08), and the data copy wins;
  - `session_kind` for each kind, a date string, an ordinary day giving `None`, and `is_special`;
  - each validation error, naming the file.
- **The dry run** (a fake `CmClient` session on the cm_bhav fixtures re-dated to a Saturday before the UDiFF switch
  and a Sunday after it):
  - found (old / UDiFF), not found, future and weekday rows not asked;
  - the store byte-identical (the lock included) and the temp folder removed;
  - the request list and the write set (touched; stale when a year parquet is missing);
  - an empty folder left empty;
  - a zip in raw/ read without a request;
  - today before / after 19:00;
  - a bad file and a failed download.
- **The apply:**
  - only the found days are in raw/, the db and their years' parquet;
  - other years are byte-identical;
  - a not-found day leaves no db row, `holidays.log` or file;
  - no `.part` / `.tmp` left;
  - the second dry run asks only the not-found date and has nothing to write; with everything found, it makes
    0 requests;
  - a failed download records nothing (exit 1).
- **The CLI:** the printed list and write set, exit codes (bad file 1, argument errors 2, bad calendar 2, busy lock 2
  for both modes), and the not-found line after an apply.
- **Unchanged weekday behaviour:** a weekend date by hand is refused, `weekdays()` gives Mon-Fri only, and `run()`
  without `special` asks nothing for a weekend day.

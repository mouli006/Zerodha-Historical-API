# Combined Kite + ICICI 1-minute loader (#31, P74)

One loader over the two minute stores: the Kite store `equity_1min/` and the ICICI store `equity_1min_icici/`. Each
stock-day comes from **exactly one source**, never mixed.
- Code: `backfill/equity/minute_combined.py`, which is read only: it writes no store file and takes no lock.
- Tests: `tests/test_minute_combined.py`.
- Discovery: 2 Nov, on main `3a7519b`. The build: 3 Nov.

## Rule (decisions D-a .. D-g, approved 2 Nov)
"Has minutes" means the source's `days.parquet` row has `minute_rows > 0`. In both stores this agrees 100% with the
labels: no MINUTE_EMPTY, ICICI_EMPTY or ICICI_BEFORE_START row has bars.

| | case | source | reason / flags |
|---|---|---|---|
| D-a | Kite has minutes on a traded day, whatever its label (DAY_EMPTY, SHORT_DAY, HIGHLOW_DIFF, a stale EXTRA_SESSION ...) | KITE | reason `KITE`; the labels without OK in `day_flags` |
| D-a | Kite has minutes, but no conversion is stored (`minute_scale`, and `ratio`, or for ADDITIVE `kite_multiplier` + `kite_difference`; 0 rows on 2 Nov) | NONE | `KITE_NO_CONVERSION` |
| D-b | Kite has no minutes (MINUTE_EMPTY, no row, no folder) and ICICI has minutes | ICICI | why not Kite: `KITE_MINUTE_EMPTY` / `NO_KITE_ROW` / `NO_KITE_FOLDER` / `KITE_NO_MINUTES`; ICICI's labels (DAY_EMPTY, SHORT_DAY, HIGHLOW_DIFF, CLOSE_DIFF, SCALE_UNSURE ...) in `day_flags` |
| D-c | Kite has a short day and ICICI a full day | KITE | `SHORT_DAY` flag; ICICI for short days is #32 |
| E6 | both sources have minutes (0 days on 2 Nov: ICICI's plan skips every Kite day) | KITE | |
| D-d | minutes but no bhavcopy trade that day | the source that would be used | `traded=False`, reason `NO_BHAV_ROW`; not loaded unless `include_no_trade=True` |
| D-e | bars outside 09:15-15:29 | | dropped on normal days; kept on a day of `manual_special_sessions.csv` (any kind); `regular_hours=False` keeps every bar |
| D-f | neither source has minutes | NONE | `<kite part>+<icici part>` |
| D-g | frozen stock: the active Kite token review row (`equity_1min/tokencheck/token_review.csv`) is PENDING or REJECTED | as stored | `frozen=True` on every day (ACCEPTED is not frozen; a missing file means no frozen stock) |

**Trading day:** a bhavcopy row of one of the stock's ISINs, series EQ / BE / BZ, with volume > 0 (the vendor-list
definition), read through `security_master.history` (one stock) or the year files (the report). This rule replaces
the stale Kite EXTRA_SESSION labels: 18,108 EXTRA_SESSION rows with minutes are traded days, such as weekend sessions
whose bhavcopy came later.

**NONE reasons:**
- **Kite part:**
  - `NO_KITE_FOLDER`;
  - `NO_KITE_ROW`;
  - `KITE_MINUTE_EMPTY`;
  - `KITE_NO_MINUTES`: a row with 0 bars and no MINUTE_EMPTY, such as an old EXTRA_SESSION row.
- **ICICI part, from the ICICI row:**
  - `ICICI_EMPTY`;
  - `ICICI_BEFORE_START`;
  - `ICICI_NO_MINUTES`: another label without bars.
  - `ICICI_MIXED_SERIES` (P162, 5 Nov): ICICI has minutes, but the day holds two series (label `MIXED_SERIES`, or its
    stored bars checked by `icici_guard.dup_days`, so the day is refused before the label is written). Never loaded.
- **The net in `load_minute_best`:** a loaded day that still holds a timestamp twice is dropped and becomes NONE
  (`ICICI_MIXED_SERIES` for an ICICI day, `DUPLICATE_TIMESTAMPS` otherwise). The loader never returns a timestamp
  twice. Revised ICICI copies (`DUPLICATE_MINUTE`) load the last copy (`load_minute_icici`).
- **ICICI part without an ICICI row, from the plan (`icici_plan.read_plan`, read only):**
  - `NOT_IN_ICICI` (the plan's match kind);
  - `ICICI_REVIEW` (match REVIEW);
  - `ICICI_NOT_YET_FETCHED` (a planned day with no row yet);
  - `NOT_IN_ICICI_PLAN` (a day the plan never asks for: market-wide rows above the plan's priority, or not in
    `kite_gaps.csv`; also every day when no plan is built).

## API
- **`day_sources(key, start=None, end=None, folder=None)`** returns one row per day (traded, or not traded with
  minutes) with these columns:
  - `date, traded, source, reason, day_flags`;
  - `kite_label, icici_label, kite_bars, icici_bars, kite_volume, icici_volume` (the volumes are the rows'
    `minute_volume`);
  - `session_kind` (`""` on a normal day) and `frozen`.

  The key is a security_id or any symbol / ISIN the stock ever had (`minute_store.resolve`). `attrs["security_id"]`
  holds the id.
- **`load_minute_best(key, start=None, end=None, prices="raw"|"ours", clean=True, regular_hours=True,
  include_no_trade=False, folder=None)`** returns the columns `ts, open, high, low, close, volume, security_id, symbol,
  source, day_flags`, in `ts` order.
  - It calls `minute_store.load_minute` for the KITE days and `icici_store.load_minute_icici` for the ICICI days (one
    call each over the range), then keeps each source's own days.
  - The conversion, `prices` and `clean` are the existing loaders'; nothing is re-implemented.
  - `attrs["days"]` holds the day table and `attrs["clean"]` the clean counts per source.
  - It raises `NoMinuteData` when no day has bars.
- **`decide(traded, kite, icici, ctx)`** is the rule as a pure function on plain frames (the tests use it).
  `traded_days(rows)` is the trading-day rule.
- **What is read:** only the `days.parquet` columns `date, minute_rows, check, minute_volume` and Kite's conversion
  columns, and `ts, close` of the ICICI bars of the days ICICI would serve (`icici_mixed_days`). The stored
  `our_factor` is never read.
- **Examples:**
  - `day_sources("TIL", "2018-11-07", "2018-11-07")`: KITE, flag DAY_EMPTY;
  - `load_minute_best("RELIANCE", "2016-01-01", "2016-01-01")`: ICICI (Kite MINUTE_EMPTY).

**Do not load while a store is written.** `equity_minute.py update` / `download`, any fix-* / repair / retoken /
token-review `--apply`, and `icici_minute.py download` / `checks` write a stock's year files first and its
`days.parquet` last, not as one transaction. A reader in between can pair new bars with old day rows. The loaders
take no lock and check none, the same as the two existing loaders.

## Report
`python -m backfill.equity.minute_combined report [--folder F] [--out DIR] [--stocks SID_OR_SYMBOL,...]`
- **Refuses (exit 2):**
  - the output folder exists (default `<folder>/analysis/combined_<IST date>/`, always a new folder);
  - `held()` on `equity_1min/.lock`, `equity_1min_icici/.lock`, `equity_update.lock` or the bhavcopy store lock;
  - `minute_plan.check_inputs` fails (an input build running, not built or stale).
- **Never takes a lock,** so it never blocks the 01:00 update. If a lock is held at the end, or a lock file's mtime
  changed during the run, `summary.csv` starts with `# stores changed during the run: re-run` and the exit code is 1.
- **How it reads:** every STOCK security (or `--stocks`), one at a time, `days.parquet`, plus `ts, close` of the
  ICICI year files holding days Kite has no minutes for (the mixed-series check). The bhavcopy year files are read
  once (date, isin, series, volume).
- **Files** (each atomic, only in the new folder):

  | file | content |
  |---|---|
  | `summary.csv` | year x source x reason: stocks, days (traded days) |
  | `by_stock.csv` | traded_days, kite, icici, none, both, kite_day_empty, no_bhav_row, frozen, first / last day |
  | `by_date.csv` | per date: kite, icici, none, no_bhav_row |
  | `overlap.csv` | traded days both sources have |
  | `none_days.csv` | every NONE traded day with its reason and both labels |
  | `no_bhav_row.csv` | every NO_BHAV_ROW day with weekday, bars and `minute_volume` of both sides (P104 zero-volume days vs KOVAI-like days with volume) |
  | `report.txt` | the printed totals |
- **Printed:** totals with OK / FAIL sum checks (traded = KITE + ICICI + NONE; summary = by_stock = by_date; NONE
  reasons; file rows), DAY_EMPTY days loaded from Kite, NO_BHAV_ROW days, frozen stocks, peak memory.
- **Exit codes:** 0 done; 1 a sum check failed or the stores changed during the run; 2 refused.

**May run together:**
- The report and the loaders may run with F&O jobs and pytest.
- They may not run with a Kite update / download / fix-*, an ICICI download / plan / checks, or `equity_update.py`.

## Known limits and open points
- **2021-02-24 (NSE outage, trading extended to 17:00)** is not in `manual_special_sessions.csv`, so its bars after
  15:29 are trimmed with the default `regular_hours=True`. The calendar is not changed by this task.
- **KOVAI (S000322):** 434 days with Kite minutes and real volume (for example 3.44 M on 2017-11-03), but no NSE
  bhavcopy row from 2015-01-30 to 2021-08-04. They are NO_BHAV_ROW here. Whether the Kite token is wrong is a separate
  open point, with no handling here.
- **NO_BHAV_ROW (discovery):** 1,763 Kite days in 167 stocks (1,328 with zero volume, 48 with one bar: the P104
  pattern).

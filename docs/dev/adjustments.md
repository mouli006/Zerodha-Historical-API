# Split / bonus adjustment factors

Cumulative split, bonus and consolidation factors per security (`backfill/equity/adjustments.py`, CLI
`adjustments.py`), built only from the saved bhavcopy parquet (`NSE_CM_bhav\<YYYY>.parquet`) and the security master
(`security_master\`). Both are only read. No network, no broker. Bhavcopy prices are never changed: adjusted prices
are computed when loading. Dividends are not adjusted.

## What the data says (measured 2026-10-17, reported in every `build_report.txt`)
- Inside one security, `prev_close(d)` equals the close of the security's previous trading day on 99.62% of
  5,107,912 STOCK security-days.
- **NSE's prev_close is NOT adjusted on an ex-date.** On every known split and bonus, close(p) / prev_close(d) =
  1.0, and only the price moves (close(p) / open(d) = TATASTEEL 9.78, IRCTC 5.06, RELIANCE 2.00, ...). So events
  come from the price, never from prev_close.
- Nearly all of the 19,330 prev_close mismatches fall on 11 dates that follow a session missing from the store:
  Budget Saturdays, Muhurat sessions and the 2024 DR Saturdays (cm_bhav downloads weekdays only). A date where
  more than `MISSING_SESSION_SHARE` (50%) of at least `MISSING_SESSION_MIN` (10) stocks mismatch is listed once in
  the report, with the weekend days between as the likely session. Those mismatches never become events or
  review rows.
- An ISIN change is not always the ex-date (TATASTEEL, IRCTC, HDFCBANK 2019: the old ISIN already traded at the
  split price), and a bonus keeps the ISIN. Detection therefore walks every row of the security across its ISINs
  and series (`load_master()["isin_owner"]`), sorted by date. p is the security's previous row, and the gap is
  counted in the store's trading days.

## Rules
- **Price candidate:** |close(p) / open(d) − 1| > `--tol` (0.25; price bands are at most 20%) and close(p) ≥
  `--min-price` (₹5; tick noise on penny stocks).
- **Snapping:** the ratio (then close(p) / close(d)) snaps to the nearest `RATIOS` value within `--snap-tol` (3%;
  the open is not exact, e.g. 9.76 for 10). `RATIOS` holds splits 2, 2.5, 4, 5, 10, consolidations 1/2, 1/5,
  1/10, bonuses (a+b)/b for a, b in 1..10, and the combinations split 1:2 + bonus 4:1 and split 1:5 + bonus 1:1
  (both 10).
- **Kinds:** `SPLIT_OR_BONUS` (snapped > 1; the price cannot tell a split from a bonus), `CONSOLIDATION`
  (snapped < 1), `OTHER` (not snapped). The table is dense between 1.1 and 11, so a snap alone proves little.
- **Only confirmed events are applied:**
  - (a) `isin_change`: an ISIN_CHANGE of the security in `security_master\events.csv` within `ISIN_WINDOW` (10)
    trading days whose `ratio_open` is within snap-tol of the snapped value. Factor = the snapped value.
  - (b) `corp_action`: the NSE corporate-actions CSV (below). Rows of one security and ex-date are combined, and
    their factors multiplied (a bonus 4:1 and a split 2→1 make 10). The security's rows within `CA_WINDOW` (3)
    trading days of the ex-date are searched for a ratio or close move within `--ca-tol` (`CA_TOL` 0.10; the
    open can be several % off, e.g. MINDACORP 9.53 for 10) of the factor **and** closer to the factor than to 1
    (otherwise a 1:10 bonus would match any quiet day). Day choice: the exact date first, then the closest
    match, then the nearest day. This search ignores `--tol`, so a clear 1:10 bonus is found.
    - Factor, `kind` and `ratio_label` come from the corporate action.
    - No move -> review `ca_no_move` (the suspect case). Six small bonuses (1:10, 1:5) are there because their
      move cannot be told from noise.
  - Unconfirmed candidates -> `applied=N`, review `price_only`, with the suggested ratio.
- **Corporate-actions input:** every `*.csv` in `adjustments\input\` (from `fetch-actions`, or NSE's own CSV
  saved by hand), duplicates dropped; an empty or missing folder is fine. Headers are matched loosely (letters only, upper case): SYMBOL, PURPOSE and EX-DATE are
  required, ISIN and SERIES are optional. A missing column -> `CorpActionError`, exit 1. PURPOSE is parsed by
  `parse_purpose()`:
  - `BONUS a:b` -> (a+b)/b.
  - `SPLIT` / `SUB-DIVISION ... FROM RS x ... TO RS y` -> x/y.
  - `CONSOLIDATION ... FROM RS x TO RS y` -> x/y.
  - Each part (separated by `/`, not `/-`) is parsed on its own. A split's FROM is optional ("Face Value Split
    Rs 10 To Rs 1").
  - Dividends, other actions and bonuses of debentures / NCRPS / NCDs / preference shares (NTPC 2015,
    BRITANNIA, TVSMOTOR 2025) are ignored.
  - A part naming bonus / split / consolidation that cannot be parsed, or an unreadable date -> `ca_unparsed`
    (one real case: MONNETISPA's capital reduction + consolidation).
  - Ex-dates after the last bhavcopy day are skipped and counted ("after the data").
  - Rows of another series, or of another instrument kind, are skipped and counted.
  - The security is found by ISIN, else by `security_id_for(symbol, ex_date)` (then a week earlier). Not found ->
    `ca_unknown`.
- **Manual overrides** `adjustments\manual_factors.csv` (`security_id, ex_date, action, factor, note`; `#` lines are
  comments): created once with `MANUAL_TEMPLATE` and never overwritten.
  - ADD: a new applied event (source manual). An existing event on that date is an error ("use SET").
  - REMOVE: the event stays in events.csv with `applied=N` and the note "removed by manual".
  - SET: new factor, applied.
  - Errors go to review `manual_error` and make `check` exit 1.
- **Cumulative factors:** per security, the applied events in date order. A period runs from the ex-date to the
  security's trading day before the next ex-date. factor = product of all later factors, and the latest period
  is 1. Every security of the master has at least one row.
- **Locks / freshness:** the build refuses (`StoreBusy`, exit 2) while `NSE_CM_bhav\.lock` or
  `security_master\.lock` is held (`locks.held`). It refuses (`MasterStale`, exit 1) when
  `security_master\build_info.json` is older than the newest bhavcopy year file, and (`NotBuilt`, exit 1) without
  a master. It takes its own `RunLock(adjustments\.lock)` (busy -> `LockBusy`, exit 2).

## Files `<EQUITY_DATA_DIR>\adjustments\` (atomic, rebuilt in full)
- `events.csv` (`EVENT_COLUMNS`): candidates and confirmed events, with ratio_prev_close (information),
  near_isin_change / isin_change_distance, and ca_ex_date / ca_purpose.
- `factors.parquet/.csv` (`FACTOR_COLUMNS`).
- `review.csv` (`REVIEW_KINDS`).
- `links.csv`: ISIN changes with no event within 10 days, and applied events of factor ≥ 2 with no ISIN change
  (information).
- `build_report.txt`: Part 1 numbers, missing sessions, counts per kind and per confirmation, links, known cases,
  and the review count with 10 examples.
- `build_info.json`.

## Loaders
- `factor_on(security_id, on_date)`: the factor of the period holding the date (before the first -> the first,
  after the last -> 1; unknown id -> `KeyError`).
- `adjusted_history(security_id, start, end)`: `sm.history()` plus `factor`, `adj_open/high/low/close` (price /
  factor), `adj_prev_close` (prev_close / the factor of the **day before**, since NSE's prev_close is
  unadjusted on ex-dates) and `adj_volume` (volume × factor). Raw columns are kept.
- **Point in time:** back-adjusted prices use factors known only later. Use them for continuous price series
  (returns, indicators). Use raw prices for anything like "the price on that day" (order sizes, price filters,
  circuit levels).

## Known cases (`KNOWN_ADJUSTMENTS`, report and `check`)
TATASTEEL 2022-07, IRCTC 2021-10, DRREDDY 2024-10, NAUKRI 2025-05, KOTAKBANK 2026-01, BAJFINANCE 2016-09 and
2025-06, HDFCBANK 2019-09 and 2025, RELIANCE 2017-09 and 2024-10. Each line shows the event, whether it is
applied, and the close before / factor next to the open and close on the ex-date.
- Without the corporate-actions CSV, only DRREDDY, NAUKRI, KOTAKBANK 2026 and BAJFINANCE 2025 are confirmed (by
  their ISIN change). With the CSV, all are confirmed (2026-10-17).
- BAJFINANCE 2016-09 is 10: NSE lists "Bonus 1:1 / Face Value Split From Rs 10 To Rs 2".
- First full build (2026-10-17): 942 price candidates; 167 confirmed by an ISIN change, 803 by the CSV (165 by
  both); 805 applied in 608 securities; 179 price_only and 20 ca_no_move in review.
- A miss is reported, never fixed by changing a rule. `build` exits 0, and `check` exits 1.

## CLI (`adjustments.py`)
- `build [--folder] [--tol 0.25] [--snap-tol 0.03] [--ca-tol 0.10] [--min-price 5] [--kinds STOCK]`
- `show SYMBOL|ISIN`
- `check`
- `fetch-actions [--from 2015] [--to <this year>] [--force] [--no-browser]`: the only command that uses the
  network.
- Exit codes: 0 done; 1 not built / stale master / bad input CSV / show found nothing / check failed /
  fetch-actions missed a year; 2 argparse, lock busy.

## NSE corporate actions (`fetch-actions`, `backfill/equity/corp_actions.py`)
- **Source:** the JSON API behind the page `https://www.nseindia.com/companies-listing/corporate-filings-actions`
  (Equity tab), checked 2026-10-17:
  `https://www.nseindia.com/api/corporates-corporateActions?index=equities&from_date=01-01-YYYY&to_date=31-12-YYYY`.
  - It returns a list of rows with keys `symbol, comp, series, subject, faceVal, exDate, recDate, bcStartDate,
    bcEndDate, isin` (plus `ind, ndStartDate, ndEndDate, caBroadcastDate`, unused).
  - Every series is included (EQ, GS, IV, RR, ...).
  - `&csv=true` gives the same rows as NSE's CSV, but without the ISIN, so the JSON is used.
- **Files:** `to_csv()` writes `adjustments\input\corporate_actions_<YYYY>.csv` with NSE's CSV column names
  (`SYMBOL, COMPANY NAME, SERIES, PURPOSE, FACE VALUE, EX-DATE, RECORD DATE, BOOK CLOSURE START DATE, BOOK CLOSURE
  END DATE`) plus `ISIN`. An unexpected shape -> `FetchError` naming the keys.
- **Client:** `backfill.nse_ref.fetch.NseClient` (home page cookies, `GAP` 2 s between requests, headless Chromium
  after a block; `--no-browser` turns that off). One request per calendar year; each file is written atomically.
- **Skip / force:** a past year already saved is skipped unless `--force`; the current year (IST) is always
  fetched again.
- **Lock:** takes `adjustments\.lock`, so it never runs alongside a build.
- **Failures:** a failed year is listed with the page, the From / To dates to choose and the file to save NSE's
  "Download (.csv)" as (the build reads that CSV too); exit 1.
- **First run (2026-10-17):** 2015..2026, 1,854 to 2,692 rows a year, no block.

## Tests
`tests/test_adjustments.py`: a synthetic store in tmp_path and a master built with `sm.build`. Covers:
- the split before the ISIN change (price_only, then corp_action on the ex-date);
- (a) by ISIN change;
- a bonus without an ISIN change;
- split + bonus = 10;
- a consolidation;
- a 1:10 bonus below `--tol`;
- price_only / OTHER;
- `ca_no_move`;
- min price;
- missing-session days;
- gaps;
- EQ -> BE;
- manual ADD / REMOVE / SET and its errors, and the file never overwritten;
- two cumulative events, `factor_on`, `adjusted_history`;
- purpose and date parsing, input errors, the empty folder;
- the locks, the stale master, inputs byte for byte unchanged;
- known cases and the CLI;
- `--ca-tol` (an open 6% off still matches), debenture bonuses and future ex-dates skipped.

`tests/test_corp_actions.py`: fixtures in `tests/fixtures/corp_actions/`: `2024_sample.json` (4 rows of the real
answer), `sample.csv` (real rows: TATASTEEL, RELIANCE, BAJFINANCE, MINDACORP, NTPC, TVSMOTOR, a dividend, a GS row)
and `blocked.html`. Fake session / browser and an autouse guard against any request or browser launch. Covers:
- the URL and the JSON -> CSV conversion (and the bad shapes);
- the saved file read by the build;
- the real purposes parsed to their factors;
- skip / force / current year;
- the browser fallback;
- the drop-in message when blocked;
- the lock;
- the CLI codes.

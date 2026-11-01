# Kite 1-minute store: token changes checked before and after the rebuild (P43, P93)

When Kite's instrument list gives a built (DONE) stock a new token, the next `update` (or `download`) downloads its
whole history again on the new token. Before this task the stored files were moved to `archive/` first and nothing
checked the new token's prices. Now:
- a **pre-check** compares the new token's day candles on the stock's latest bhavcopy days with the bhavcopy before
  anything is downloaded;
- an **after-download comparison** compares the new history with the stored one before the old files are replaced.

A refused change leaves the stored files byte-identical, keeps the old token in the progress db, and lists the stock
for review (`equity_minute.py token-review`). The `update/tokens.csv` note says what was really checked.
- Code: `backfill/equity/minute_tokencheck.py` (check, comparison, review files, notes, `review`, `replay`). The gate
  is `Runner.token_gate` / `Runner.compare_replace`, called from `Runner.history` / `Runner.build`, plus
  `clear_back` and `update` (`backfill/equity/minute_download.py`). CLI `equity_minute.py token-review` /
  `token-replay`.
- Tests: `tests/test_minute_tokencheck.py`.
- The download and update themselves are in docs/dev/equity_minute_download.md. Where that doc says a DONE stock
  whose token changed "is moved to archive/ and fetched again", this doc supersedes it: the stock is moved only after
  both checks pass.

## Evidence (discovery 2026-11-01, read-only; `D:\Data\NSE_equity\analysis\tokencheck_discovery_2026-11-01\`)
- **Where it happened.**
  - `update` put every item that was not DONE with the same token into its full list.
  - `Runner.history` then archived `data/<sid>/` and reset the progress rows before any request.
  - `download` reached the same code through `stock_state`. It took its tokens from `update/tokens.csv`, so a token
    refused only in `update` would have been rebuilt, unchecked, by the next `download`. The gate therefore sits in
    `Runner.history`.
  - Phase 1 `tokens` / `probe --force` writing a new plan.csv token reaches the same gate.
  - `retoken` and `back_in_list` handle only stocks without stored data, and they already price-check.
- **18 token changes on 30-31 Oct 2026.** P94 (EQ -> X-BE): NSIL, KICL, TVSHLTD, WILLAMAGOR, MAHSCOOTER, PILANIINVS,
  GFLLIMITED, SILINV, HILINFRA, KAMAHOLD, ELCIDIN. P109 (X-BE -> X): VENUSREM, ORIENTLTD, GANESHBE, SICALLOG, RMDRIP,
  SYSTMTXC, PIONRINV.
  - Every one has 5 bhavcopy days before its switch date. The 10 P94 stocks without a 30 Oct row traded on 23-29 Oct.
- **Replay** (18 Kite day-candle requests, `replay.py` / `replay_output.txt`):
  - All 18 new tokens equal the bhavcopy exactly on every reference day (0.000% on close, high and low), so all 18
    PASS.
  - The two negative controls FAIL: MAHSCOOTER's reference days against PIONRINV's token are 99.6% off (3 days), and
    VENUSREM's against GFLLIMITED's are 96.4% off (4 days).
  - No split or bonus fell in the windows, so every row was compared on the raw basis.
- **After-download cost** (`disc6.py`):
  - The day level reads only `days.parquet`: 0.03-0.08 s and at most 1.2 MB.
  - Every minute bar of MAHSCOOTER (1,018,639 bars, 2,908 days) takes 0.3 s with a 49 MB peak for one side at once;
    year by year it is far less.
  - Disk: one extra copy of the stock (MAHSCOOTER 15.5 MB), which `data/<sid>.building/` already was.
- **Compare bars, not day closes** (`disc6_bars.py`). Three of the 18 move the day's last close by more than 0.5%:
  MAHSCOOTER 2016-10-30 (Muhurat), ORIENTLTD and GFLLIMITED 2021-02-24 (NSE outage).
  - In all three, every bar present in both histories is identical.
  - Kite's new history just has more bars: 60 instead of 46 to 19:29 on the Muhurat day, and up to 16:57 / 16:59
    instead of 11:39 / 11:41 on the outage day.
  - A day-close rule would wrongly refuse GFLLIMITED, so the comparison is per common minute bar.

## The pre-check (`check_token`, `judge`)
- **When.**
  - The stock is DONE and its work-list token (from `update/tokens.csv` or plan.csv) differs from the progress db
    token (`TOKEN_CHANGE`).
  - Or it is a new listing (`update/new_listings.csv`, no progress row; `NEW_LISTING`). A new listing listed after
    the target day still returns `NOT_YET` first, unchanged.
- **Reference days.** The stock's latest **5** bhavcopy days (cache master series) on or before the run's last day T,
  within the last **60** trading days up to T. 1-4 days are used as they are. 0 days gives **NO_REFERENCE**, with no
  request made.
- **Request.** One day-candle request for the new token, from the first to the last reference day, through the run's
  shared limiter.
- **Per day:** Kite's close, high and low are each within `max(2%, 2 ticks of Rs 0.05)` of the bhavcopy. On a Rs 2
  stock that is 5%, so one tick (P83) passes.
- **Split / bonus.** Let `k = factor_on(ref) / factor_on(T)` (`adjustments`). Kite's old candles may or may not be
  back-adjusted (P46), so the bhavcopy is compared both raw and divided by k. The basis with the smaller close
  difference is kept and reported (`raw` / `adjusted`). Real factors are far apart, so this cannot turn another
  company into a PASS.
- **Results:**
  - **PASS**: at least one day was compared and every day is within tolerance.
  - **FAIL**: any compared day is outside the tolerance.
  - **NO_CANDLES**: no candle on any reference day, or the request failed (Kite's error text goes into the review's
    note).
  - **NO_REFERENCE**: no reference day (see above).
  - `AuthStop` / `StopJob` stop the run as before.
- **PASS:** the download goes on. The old token's `chunks` / `day_fetches` rows are dropped
  (`Progress.drop_other_token`), but the new token's rows stay for a resume. The `stocks` row stays DONE with the old
  token, and `data/<sid>/` stays in place.
- **Anything else** is refused:
  - nothing is downloaded or touched;
  - the stock becomes a PENDING review;
  - the evidence goes to `tokencheck/<run_id>/<sid>_precheck.csv`, one row per day: bhav and Kite OHLC, k, the
    differences, the tolerance and ok;
  - the stock gets no daily append in this run.

## The after-download comparison (`compare_replace`, `compare_builds`)
- **Runs** in `Runner.build`, after the new `data/<sid>.building/` is complete, when the stock is DONE with another
  token and `data/<sid>/` exists. This also applies to an ACCEPTED review.
- **Per year, per side:** the bars go through `clean_bars`, and each bar's loaded close uses its own side's day
  conversion (`ms.convert`, as `load_minute`). Only bars with the same `ts` on both sides are compared. Only one year
  of one stock is in memory at a time.
- A day with a common bar whose loaded close moves more than **0.5%** is a bad day.
- **Exemption.** A bad day is exempt when the new day matches the bhavcopy close better: the close lies inside the new
  loaded day range and not the old one, or the new loaded close is nearer to it. A day without a bhavcopy close is
  never exempt.
- **Verdict:**
  - Passed when no bad day is left and at most **5** days are exempt.
  - Then `archive()` moves the old files to `archive/<sid>_<date>/` ("token A -> B", as before) and the new build
    replaces them.
- **Refused** (a bad day, or more than 5 exempt days):
  - The new build is **parked** in `tokencheck/<run_id>/<sid>_new/`, and the old files and old token stay.
  - The stock becomes a PENDING review with result `AFTER_DOWNLOAD`, holding the parked path and the reason.
  - Bars only on one side and the day-row counts (P95 drops) are reported, never refused.
- **Evidence:** `tokencheck/<run_id>/<sid>_compare.csv` has one row per bad day: bars, bad bars, max diff, old / new
  loaded close and range, the bhav close, exempt and why. Every exempted day is in it and in the log line.
- **What it does not catch.** The comparison uses loaded prices so that a history Kite has re-adjusted since the build
  (a split) still passes. A day's conversion is measured against the bhavcopy, so a different company with the same
  intraday shape would also load to the bhavcopy. The pre-check is what catches a wrong company. This comparison
  catches a history whose minutes differ.
- **A stop** between the download and the replace (Ctrl+C, sign-in) leaves `data/<sid>/` and the progress row
  unchanged. The next run passes the pre-check again (one request), finds the staged chunks of the new token done, and
  builds, compares and replaces.

## Review states (`tokencheck/token_review.csv`)
- **Columns:** security_id, symbol, reason, old_token, new_token, result, max_close_diff, max_hl_diff, first_seen,
  last_checked, report, parked, state, decided_at, note.
- One **active** row per stock (PENDING / ACCEPTED / REJECTED). Closed rows stay as history: CLEARED, PASSED, BUILT.
- **PENDING** (FAIL / NO_CANDLES / NO_REFERENCE):
  - checked again by every `update` / `download` (one request; `first_seen` kept);
  - a PASS closes it (PASSED) and rebuilds the stock;
  - no daily append meanwhile.
- **PENDING with result AFTER_DOWNLOAD:** frozen until `token-review`. There is no new download, the log says
  "TOKEN_REVIEW pending ... frozen".
- **ACCEPTED** (`token-review --accept`, not parked): the next run skips only the pre-check. The comparison still
  runs, then BUILT, or PENDING AFTER_DOWNLOAD again.
- **REJECTED** (`token-review --reject`):
  - the old files are frozen, with no daily append (Kite answers "invalid token" for a token missing from its list,
    P65) and no request;
  - every `update` logs "`<symbol> (<sid>): REJECTED, frozen since <date>`".
- **Clearing:**
  - Kite lists another token: the row is CLEARED and that token gets a fresh pre-check (`token_gate`).
  - Kite lists the stored token again (P100): the row is CLEARED (`clear_back`) and the stock is back on the daily
    path. `Runner.append` fetches every day from the day after its last stored day to T, so the days missed while
    frozen are filled.
- `tokencheck/checks.csv` holds every check made: at, run_id, security_id, symbol, kind PRECHECK / AFTER_DOWNLOAD,
  reason, old / new token, result, days, max diffs, basis, report.

## `update` log and exit code
- **Before the run:** one line per full download, `  full download <sid> <symbol>: <old> -> <new> (TOKEN_CHANGE |
  NEW_LISTING | RESUME)`.
- **During the run:**
  - `  FULL <sid> <symbol>: <old> -> <new> (<reason>): <PASS|FAIL|NO_CANDLES|NO_REFERENCE>, N days, max diff close
    x% high/low y% (<basis>)`;
  - `  COMPARE <sid> ...: N common bars, max loaded close diff, bars only new / old, days old -> new, exempt k (dates):
    replaced | REFUSED (...)`.
- **After the run:**
  - the same lines again, sorted, under "Full downloads and token checks:";
  - the summary line ends with "token checks: P passed, F refused, N no reference" (F includes after-download
    refusals);
  - "Token reviews open: n" when there are open reviews.
- **Exit code** (`run_result`):
  - 1 when this run refused a token change (FAIL, NO_CANDLES, NO_REFERENCE or AFTER_DOWNLOAD). It prints "token
    change(s) refused (TOKEN_REVIEW)", as retoken's REVIEW does.
  - Failed chunks also give 1 and print their own line, so neither hides the other.
  - A REJECTED (frozen) stock is not counted.

## `tokens.csv` note (P93)
`match_tokens` (phase 1, unchanged) writes "price checked by probe" on every row. `update` now rewrites the note from
the match's own text, before the run and again after it, with the first of these that applies:
1. this tool's latest pre-check of that token: `price check new token PASS 2026-11-02 (5 days, max 0.30%)`, or FAIL /
   NO_CANDLES / NO_REFERENCE;
2. phase 1's probe, when it probed this very token: `probe: price checked <day>` (or `probe: not price checked`);
3. `retoken`'s price check from `update/retoken_<date>.csv`: `retoken: price check OK ...`;
4. a probe of an older token: `probe: old token <t>; this token unchecked`, as for the 18 switches of 30-31 Oct;
5. `unchecked`, as for the new listings JPL and PARASHLTH.

The SERIES_SWITCH text stays in front. Rows that are not matched keep their note.

## `equity_minute.py token-review [--accept SID | --reject SID] [--apply]`
- **Without an option:** lists every open review with its evidence file (first rows). Offline, writes nothing.
- **`--accept SID`:**
  - a parked download is installed offline: the stored files go to `archive/` ("accepted after review"), the parked
    folder moves into `data/<sid>/`, and the progress row becomes DONE with the new token (BUILT);
  - otherwise the review becomes ACCEPTED, and the next `update` / `download` rebuilds the stock (with the
    comparison);
  - `token-review` never calls Kite.
- **`--reject SID`:** REJECTED (see above).
- **Dry run (default):** says what `--apply` would do and writes nothing, without the lock.
- **`--apply`:** takes `equity_1min/.lock` (exit 2 while it is held), writes the decision and appends it to
  `tokencheck/review.log`. A second dry run finds nothing to do.

## `equity_minute.py token-replay [--dates 2026-10-30,2026-10-31] [--speed 2.5]`
- The pre-check on archived token changes, without writing anything (needs a Kite sign-in and a current bhavcopy
  cache, else exit 1).
- **Input:** every `archive/<sid>_<date>[_n]/` of the dates whose `archived.txt` says "token A -> B".
- **Reference days:** those before the switch date.
- **Negative controls:** MAHSCOOTER's reference days against PIONRINV's new token, and VENUSREM's against
  GFLLIMITED's.
- **Requests:** one day-candle request per new token, over every window that uses it. The controls reuse those
  candles.
- **Output:** one table (sid, symbol, old / new token, switch, days, max close and high/low diff, basis, result), the
  totals (they add up to the rows) and the request count.

## Unchanged
- SAME_TOKEN stocks: the daily request from the day after the last stored day, with no pre-check.
- NO_KITE_DATA stocks (the P65 weekly-auction stocks missing from Kite's list): skipped as before.
- `retoken` and `back_in_list`: never a stock with stored data.
- `minute_plan.py` (phase 1, `match_tokens`): not changed.

# ICICI Breeze 1-minute source (phase 3)

ICICI's 1-minute candles for the stock-days Kite lacks. This covers dead stocks (exited / dormant), old gaps of live
stocks, January 2015, 2016-01-01 and the Muhurat sessions 2017-10-19 / 2018-11-07.
- Code: `backfill/equity/icici_client.py` (the three Breeze calls), `icici_plan.py` (work list),
  `icici_download.py` (download, conversion, checks, status, coverage) and `icici_store.py` (layout, loader). CLI:
  `icici_minute.py`.
- It is a separate source with its own store and lock (`<folder>/equity_1min_icici/`). The Kite store
  `equity_1min/` is only read, and no Kite module was changed.
- It is not part of `equity_update.py` (it needs an ICICI log-in) and has no dashboard UI.

## Endpoints and auth (no `breeze-connect`)
The `breeze-connect` package downloads a file on import, writes `./logs` and clashes with our `config.py`, so it is
not used. `requests` calls only these three endpoints (a test checks that no other URL is in the module):

| call | request |
|---|---|
| session | `GET https://api.icicidirect.com/breezeapi/api/v1/customerdetails`, JSON body `{"SessionToken": <BREEZE_SESSION_TOKEN>, "AppKey": <BREEZE_API_KEY>}`. Answer `Success.session_token` = base64("user_id:session_key"). Errors: `Invalid session.`, `Resource not available.`, `Public Key does not exist.` |
| candles | `GET https://breezeapi.icicidirect.com/api/v2/historicalcharts`, params `interval` (`1minute` / `1day`), `from_date` / `to_date` (`YYYY-MM-DDT00:00:00.000Z` .. `T23:59:59.000Z`), `stock_code` (ICICI's ShortName), `exch_code=NSE`; headers `apikey` and `X-SessionToken` (the base64 value) |
| master | `GET https://directlink.icicidirect.com/MotherAppMaster/SecurityMaster.zip` (public), which holds `NSEScripMaster.txt` |

- **Never orders.** There are no order, portfolio or funds calls. `BREEZE_API_SECRET` only signs those (X-Checksum),
  so it is never read or sent.
- **Credentials:** `BREEZE_API_KEY` and `BREEZE_SESSION_TOKEN` come from `.env` via `config.load_env()`. They are
  never printed, logged or written: every error text goes through `redact()`, which also covers the base64 session
  value.
- **Daily step:** log in to ICICI (Breeze), copy the day's session token into `.env` as `BREEZE_SESSION_TOKEN`, then
  run `download`. A rejected session stops the run cleanly (exit 2, "log in to ICICI again ..."). What was fetched is
  built; nothing is half-written.

## Limits
- At most **1,000 candles per call**: a longer range silently keeps only the last 1,000 (discovery, 19 Oct). A day
  has 362-378 bars, so a minute call covers 1-2 trading days. An answer of 1,000 bars is treated as truncated: a
  2-day chunk is split and fetched again; a single day of 1,000 bars is FAILED "truncated".
- ICICI documents **100 calls a minute and 5,000 a day** (one FAQ says 75 a minute). The discovery saw no rate limit
  in 89 calls at 1.2 s, nor in 14 calls on 24 Oct.
- Our defaults: `--speed 90` calls a minute (at most 100; `backfill.fetch.RateLimiter`) and `--max-calls 4800` a
  day. The cap counts every call (session, day, minute) in `calls` (IST date) across runs, and the run stops cleanly
  before the call that would exceed it (exit 0, "daily cap reached ... resume tomorrow").
- A call takes 0.16-0.57 s.

## Work list (`plan`)
1. **Master:** one download of `SecurityMaster.zip`, saved as received under
   `equity_1min_icici/reference/SecurityMaster_<IST date>.zip`, plus `NSEScripMaster_<date>.txt`. Today's copy is
   reused. `--offline` uses the newest saved copy.
2. **Rows:** `kite_gaps.csv` rows with `data_missing` `1MIN` or `BOTH` (`EOD` rows are not ICICI's job).
   - A row with a security: its bhavcopy days inside `need_from..need_to`, read from our cache
     `equity_1min_icici/bhav_cache` (`minute_download.bhav_cache(out=...)`; the Kite cache is not touched).
   - A market-wide row (blank security: `KITE_MINUTE_START` Jan 2015, the 2016-01-01 `MINUTE_EMPTY_DAYS`, the
     `KITE_MISSING_SESSION` Muhurat days): every ACTIVE stock whose priority during the row is <= `--market-wide`
     (default 1; NIFTY 50 = 1, NIFTY 500 = 2, the kite_gaps rule) and that traded on those days.
   - A day in several rows keeps the one with the lowest priority.
3. **Kite-covered days removed:** a day leaves the list when `equity_1min/data/<sid>/days.parquet` has
   `minute_rows > 0` for it. This removes stale labels (ex-NO_KITE_DATA, new listings shown as NO_KITE_TOKEN) and
   any later Kite fix, so they never cost a call. The report counts them.
4. **Match:** every ISIN the security ever had (master history) against `ISINCode`. One ShortName gives `ISIN`.
   Otherwise every symbol against `ExchangeCode`; one ShortName gives `SYMBOL_ONLY` (price checked before anything is
   stored). Several ShortNames give `REVIEW`; none gives `NOT_IN_ICICI`. REVIEW and NOT_IN_ICICI are never fetched.
5. **Outputs** (`equity_1min_icici/plan/`, atomic):
   - `icici_plan.csv`: security_id, symbol, company_name, icici_code, icici_series, match_kind, match_note,
     priority, life DEAD/LIVE, status, reasons, need_ranges, need_from, need_to, trading_days, kite_covered_days,
     est_day_calls, est_minute_calls, est_calls;
   - `icici_plan_days.parquet`: security_id, date, row, reason, priority. This is the download's day list;
   - `plan_rows.csv`: per gap row, its days, the Kite-covered days and the ICICI days (for coverage);
   - `plan_report.txt`: stocks / days / calls per priority x life x match, and quota days at 4,800;
   - `plan_info.json`.
6. **Dry run** (`plan --dry-run`, 24 Oct): the same refusals and lock, the newest saved master (no network), and
   no plan file written (only the derived bhavcopy cache may be refreshed, as by any plan). `plan_preview` compares
   the new list with the current plan and the progress db (read only) and prints:
   - the stocks new to the plan (per priority x life x match, with calls) and the market-wide rows;
   - per stock already in the plan, what happens to each day it gains. A day is either already stored or in a
     fetched chunk (no call), inferred ICICI_BEFORE_START (before the stored first minute day, start check already
     recorded: no call), before the start with a start check now pending (3-15 calls), or fetched (minute pairs,
     plus day calls over spans no day chunk covers);
   - days removed and priority changes;
   - the progress kept (chunks, start checks, stocks per status, stored days, each PARTIAL stock with its days
     left and its position before / after);
   - the stored days the download would fetch again (must be 0);
   - points to watch: new days inferred without a sample after a recorded start check, a DONE "no ICICI minutes"
     stock that gains days, and a DONE stock whose new days need no call (build would not rewrite its
     `days.parquet`).
7. **Order:** priority 1, 2, 3; DEAD before LIVE; trading days, largest first; then security_id.
8. **Estimate:** day calls of up to 900 trading days, plus minute calls in pairs of consecutive needed trading days.
   It is an upper bound for most stocks: a stock whose ICICI minutes start later costs up to about 12 search calls
   and skips the days before.

## Download (`download`)
- **Refuses** (exit 2): `equity_1min_icici/.lock` busy (one plan / download / checks at a time),
  `<folder>/equity_update.lock` held (an `equity_update.py` run), an input lock (`minute_plan.check_inputs`), an ICICI
  session rejected or no credentials. `NotBuilt` (exit 1) without a plan.
- **Kite jobs:** it may run while Kite jobs run. It is a separate API, lock and folder; it only reads the Kite
  `days.parquet` files (in `plan`), and every Kite write is an atomic replace. Its memory stays well under 0.5 GB.
- **Session:** one `customerdetails` call at the start of each run (counted in the quota).
- **One stock at a time**, in plan order (`--priority`, `--symbols`, `--limit`). Only finished needed days are
  fetched: a day counts from (day + 1) 01:00 IST (`minute_download.finished_at`). Per stock:
  1. **Day candles** (1day) over the needed span, in calls of up to 900 trading days, staged.
  2. **First minute day:** the same binary search as Kite's probe, over the needed days (at most about 12 calls, not
     counted in `--days-per-stock`). HDFC's minutes start on 2023-02-01 although its day candles go back to 2015.
     - Days before the first minute day that were not probed are `ICICI_BEFORE_START`: inferred, not fetched.
     - Probed days without bars are `ICICI_EMPTY`: fetched, empty answer.
     - When even the last needed day is empty, the first minute day is `NONE`.
     - An ICICI day without minutes on the search path can make the search land late. The start check catches this
       (below).
  2b. **Start check** (24 Oct; `start_check`, `sample_days`). It runs when the search gave a first minute day D
     after the first needed day, or NONE, and the progress db has no `start_checks` row for the stock.
     - **Samples:** 3 single days (`START_SAMPLES`). Each is the middle of the largest run of unfetched needed days
       before D (NONE: over the whole range), between boundaries (-1, D, and every day already fetched; ties: the
       earlier run). Each pick becomes a boundary. Fewer unfetched days: all of them.
     - **Calls:** normal single-day chunks, counted in the quota, not counted in `--days-per-stock`.
     - **No sample with minutes:** `KEPT`. D (or NONE) stays, and the ICICI_BEFORE_START rows note "start check
       <date>: samples a, b, c without minutes".
     - **A sample with minutes:** search again over the needed days up to the earliest such sample, save the new
       D, then a new round on it (at most `START_ROUNDS` = 5; each moves D earlier). Result `MOVED`.
     - **Record:** `start_checks` (first_before, first_after, samples JSON [{date, bars, round}], result, rounds,
       checked_at, run_id) is written once and never repeated.
     - **A failed sample or re-search call:** no row is written. The next run checks again and reuses the samples
       already fetched.
     - **Stocks searched by an earlier run** (a pending check: a stored D, no row) are checked in a pass at the start
       of `download`, in plan order, before any minute call of the run. A DONE stock with a pending check is put
       back on the work list for it.
     - The NONE case runs before the SYMBOL_ONLY "no minutes, nothing stored" shortcut.
  3. **SYMBOL_ONLY price check** on the first day with minutes, before anything is stored. ICICI's day candle (else
     its minute aggregates) must be within 2% of the bhavcopy close, high and low at an explainable scale. The
     allowed scales are 1.0, our cumulative factor that day, and that factor over each later factor level of the
     stock (`allowed_scales`). A check measured on the same day would always pass. Fail: `REVIEW`, the staging goes
     to `archive/<sid>_<time>/`, and nothing is stored.
  4. **Minute chunks:** two needed days per call only when they are consecutive trading days, else one.
     `--days-per-stock N` stops a stock after N minute days in this run (a trial).
- **Staging:** each chunk goes to `staging/<sid>/m_<from>.parquet` or `day_<from>.parquet` and is committed to the
  progress db.
- **Build** (after each stock, and also at a stop):
  - the staged chunks are merged into `data/<sid>/<year>.parquet` (a re-fetched day replaces its bars) and into
    `icici_day.parquet`;
  - `days.parquet` is derived again for the whole stock and written last (each file atomic);
  - then the chunks are marked built and the staging is removed;
  - a chunk fetched before (day + 1) 01:00 IST of one of its days is not stored; its row is dropped, so it is
    fetched again (the finished-day rule, a second net).
- **Stops:**
  - the cap gives exit 0;
  - Ctrl+C / `StopJob` gives exit 0, "resume with the same command";
  - a rejected session gives exit 2;
  - a rate-limit answer backs off 60 s and retries, and a second one in a row stops the run (exit 1);
  - another failure retries 3 times, then the chunk is FAILED with ICICI's redacted text in `errors`, and the run
    goes on (exit 1; the next run retries it);
  - a start-of-run `customerdetails` call that fails 3 times (timeout, network, server) ends the run like the other
    stops. The run row gets result `failed: session` and `ended`, the check and coverage files are rebuilt, and the
    CLI prints "Not run: <error>" and exits 1.
- **Timeouts:** each request has `timeout=60` (the master download 180 s). A timeout is `IciciFailed`, so a call
  is tried at most 3 times (waits 2 s, 4 s), and every try counts in the quota.
- **Resume:** a restart skips every DONE / EMPTY chunk and builds chunks a killed run left staged. A run left
  `running` by a killed process becomes `ABORTED`.
- **Progress db** `download_progress.sqlite` (WAL):
  - `calls` (at, ist_date, run_id, kind, security_id, result, seconds, bars);
  - `chunks` (security_id, interval, chunk_from, chunk_to, code, bars, day_bars JSON, status DONE / EMPTY / FAILED,
    built, fetched_at, run_id, seconds, error);
  - `stocks` (code, match_kind, status DONE / PARTIAL / REVIEW, first_minute_day, price_check, reason, days,
    minute_rows, built_at);
  - `runs` (calls, result, peak_mb);
  - `errors`.
- **Memory:** one stock at a time. A build reads one stock's minutes (at most about 1 million rows).

## Store layout
`<folder>/equity_1min_icici/data/<security_id>/` holds:
- **`<year>.parquet`:** ICICI's 1-minute candles **unchanged**: `ts` (Asia/Kolkata), open, high, low, close
  (float64), volume (int64), zstd. They include ICICI's 09:07 pre-open bar and its 15:30-15:32 bars, and the evening
  bars of the Muhurat sessions.
- **`icici_day.parquet`:** ICICI's day candles unchanged (date, OHLCV).
- **`days.parquet`:** one row per resolved day (fetched with or without bars, inferred before the start, or an extra
  session with bars). Its columns, with the Kite names where the meaning is the same:
  - `date, symbol, series`;
  - `icici_open .. icici_volume` (ICICI's day candle) and `bhav_open .. bhav_volume`;
  - `our_factor`;
  - `ratio` = bhav close / ICICI close and `vol_ratio` = bhav volume / ICICI day volume (both information only);
  - `icici_scale`, `icici_scale_measured`, `icici_volume_scale`, `kind` (MULTIPLICATIVE);
  - `minute_rows, bars_outside, first_bar, last_bar, minute_open .. minute_volume, session_high, session_low`;
  - `check, conversion_from, note, icici_code, match_kind, fetched_at, run_id`.
- **Aggregates and `bars_outside`:** the minute aggregates use `minute_store.clean_bars`, over all bars. `bars_outside`
  counts the stored bars outside 09:15-15:29 (normally 2-3 a day from about 2023: 09:07, 15:30-15:32). They are kept
  and loaded unchanged; whether a backtest should drop them is left for later.
- **`session_high` / `session_low`** (24 Oct): the high / low of the clean bars inside 09:15-15:29. A day with no bar
  there (a Muhurat evening session) uses all its bars. HIGHLOW_DIFF and CLOSE_DIFF compare these, so ICICI's 09:07
  pre-open print no longer flags a day. A `days.parquet` written before has no such columns: the checks then use
  `minute_high` / `minute_low`.

## Conversion to the bhavcopy (decided 24 Oct; evidence from 14 read-only calls that day)
ICICI's prices are **partly adjusted**: adjusted for the actions up to a freeze around 2017-2018, never for later ones.

| finding | evidence |
|---|---|
| ICICI's 1day candle is on the same frozen scale as its minutes | RELIANCE 2017-09-06: day high/low 826.25/811.40 = minute high/low; bhav 1652.50/1622.80 (x2.000) |
| Its day close is the official close on ICICI's scale; the last minute close is not | RELIANCE 2017-09-06: day close 822.70 = 1645.40 / 2; last minute close 822.00 |
| The day candle is built from ICICI's minutes (volume = summed minutes; a short day stays short) | IDFC 2015-10-06 (minutes from 12:30): day high/low = minute high/low, bhav range wider |
| bhav / ICICI-day high and low give exact, clean levels; the close ratio is noisy (+-0.3%) | RELIANCE 2.000 until 2017-09-06, then 1.000; DHFL 2.0 until 2015-09-08; IDFC 2.5 until 2015-09-16; CAIRN, TATAMTRDVR 1.0 throughout |
| Later actions are not applied | RELIANCE 2023 demerger and 2024-10-28 bonus: minute = bhav |
| Volume moves inversely | volume ratio 0.50 at price x2, 0.40 at x2.5, about 1.00-1.009 otherwise |
| 1day calls are cheap | 743 days (3 years) per call: about 3 calls per stock |

Rules (`derive`, a pure function; tested on a known example):
- **Price level:**
  - per day, `icici_scale_measured` = mean(bhav_high / icici_high, bhav_low / icici_low), measured where the two agree
    within 0.1% plus two half ticks;
  - `icici_scale` = `minute_download.levels(measured, 0.001)` over all ICICI day candles of the stock: a new level
    where the centred 11-day median moves by more than 0.1% for 5 measured days, snapped to 1.0 within 0.05%.
- **Volume level:**
  - `icici_volume_scale` = `levels(vol_ratio, 0.2)` (20% steps, 5 days) on days that are not SHORT_DAY, snapped to
    1.0 within 5%;
  - per-day `vol_ratio` is information only, so a short day stays short;
  - a level more than 5% from 1 / icici_scale is noted.
- **Raw values:** raw price = ICICI minute x `icici_scale`; raw volume = ICICI minute volume x
  `icici_volume_scale`.
- **No ICICI day candle:** the day takes the **previous** day-candle day's levels (the next one only when there is
  no previous), and `conversion_from` records that day. If the previous and next levels differ, the day gets
  `SCALE_UNSURE`. With no day candle at all, the scale is 1.0 (noted).

## Checks (per day, `|`-joined; tolerance 0.05% + half a 0.05 tick x scale)
ICICI's day candle is built from its own minutes, so the scaled minutes are checked against the **bhavcopy**.

| label | meaning |
|---|---|
| `OK` | nothing found |
| `ICICI_BEFORE_START` | before the first minute day, inferred, not fetched |
| `ICICI_EMPTY` | fetched, a bhavcopy row, no minutes |
| `EXTRA_SESSION` | minutes, no bhavcopy row |
| `DAY_EMPTY` | minutes, no ICICI day candle (converted as the previous day) |
| `SCALE_UNSURE` | no day candle, and the day lies between two different levels |
| `SHORT_DAY` | a daytime session whose first bar is after 10:00 or whose last bar is before 15:00 (an evening session from 16:00 is not short) |
| `HIGHLOW_DIFF` | scaled `session_high` (09:15-15:29 bars) above the bhav high, or `session_low` below the bhav low |
| `CLOSE_DIFF` | the bhav close outside the scaled `session_low .. session_high` |
| `CLOSING_AUCTION` | the same, with the last bar in 15:00..15:28 (Kite's rule; the official close is the bhavcopy close) |
| `VOLUME_DIFF` | minute volume x level above bhav x 1.01 + 1 or below bhav x 0.5 |
| `SCALE_STEP` | a level step with no corporate action within 5 trading days (`minute_download.corporate_actions`) |

`REVIEW` is per stock (SYMBOL_ONLY price check failed, or several ICICI codes).

`checks` (offline, takes the lock) rebuilds `check/icici_check.csv` (problem days), `summary.json` (counts per label
and priority, total `bars_outside`, stocks with a scale) and `scale_steps.csv` from every `days.parquet`. A download
rebuilds them at its end.

`checks --rederive [--apply]` (24 Oct) derives every stored stock's `days.parquet` again from its stored bars, chunks,
start check, our bhavcopy cache, the factors and the corporate actions. It is offline (no ICICI call), takes the
lock and refuses while `equity_update.py` runs.
- **Dry run** (the default): writes nothing; a stale bhavcopy cache stops it (`CacheStale`, exit 1). It prints the
  label counts before / after (total and per priority) and every day whose label changes. Shown apart: any change
  other than HIGHLOW_DIFF / CLOSE_DIFF removed (a new label, another column, a day added or dropped).
- **`--apply`:**
  1. Copy every `days.parquet` and `check/` to `archive/rederive_<time>/`.
  2. Write the new files, each atomic. This is all-or-nothing: on an error, every file already written is put back
     from the copy.
  3. Rebuild the check files.

First dry run on the store (24 Oct, 57 stocks): HIGHLOW_DIFF 19 -> 0, OK 9,188 -> 9,205, CLOSE_DIFF 2 -> 4. The two
new CLOSE_DIFFs are 2021-02-24 (IDFC, JBCHEPHARM), NSE's outage day. ICICI has no bars from about 11:40, then one
bar after 17:00, so the bhav close from the late session lies outside the 09:15-15:29 range.

## Status and coverage
- **`status`** (offline, writes nothing) shows:
  - the plan totals;
  - per priority: stocks done / partly / not started / REVIEW, NOT_IN_ICICI, days stored, `ICICI_EMPTY` and
    `ICICI_BEFORE_START`;
  - calls used today and left under the cap, and the calls still needed with the quota days;
  - chunks and FAILED chunks with their error;
  - the start checks: done (kept, of which still NONE; moved, of which from NONE), sample calls, pending, and each
    MOVED stock;
  - REVIEW stocks, errors, ABORTED runs, and the last run with its peak memory.
- **`coverage`** (`check/coverage.csv`, also printed after a download): per `kite_gaps.csv` 1MIN / BOTH row:
  - columns: days, Kite-covered, ICICI needed / stored / empty / before start, to do, still missing;
  - state `FILLED` (every ICICI day has minutes), `PARTLY`, `MISSING`, `NOT_IN_ICICI`, `REVIEW`, or `KITE` (the Kite
    store already had every day).

## Loader
`icici_store.load_minute_icici(key, start=None, end=None, prices="raw"|"ours"|"icici", folder=None, clean=True)`:
- the key is an id or any symbol / ISIN ever used (`sm.lookup`; a name used by several companies is resolved at
  `start`);
- `raw` = ICICI x icici_scale, volume x icici_volume_scale; `ours` = raw / our factor of the day, volume x it;
  `icici` = the stored values unchanged;
- returns the columns `ts, open, high, low, close, volume, security_id, symbol, source` ("ICICI");
- `clean=True` runs `clean_bars` first (`attrs["clean"]`);
- `NoMinuteData` when nothing is stored.

`minute_store.load_minute` (Kite) is unchanged.

**Proposed combined loader (not built):** per stock-day, take Kite when Kite's `days.parquet` row has `minute_rows >
0` and no MINUTE_EMPTY / DAY_EMPTY. Otherwise take ICICI when its row has minutes and no HIGHLOW_DIFF / CLOSE_DIFF
(or the caller accepts them). Otherwise nothing. Both sources are converted to raw (or ours) first, then concatenated
with a `source` column. For a day both sources have, Kite wins (its conversion is measured against a same-run day
candle).

## CLI and exit codes
`plan [--market-wide 1|2|3] [--offline] [--dry-run]`, `download [--priority N] [--symbols A,B] [--limit N] [--max-calls 4800]
[--days-per-stock N] [--speed 90]`, `status [--max-calls N]`, `checks [--rederive [--apply]]`, `coverage`, `show KEY [--days N]`, each with
`--folder` (absolute).

| exit | meaning |
|---|---|
| 0 | done, stopped by the user, or the daily cap |
| 1 | not built / stale input, FAILED chunks, stopped by a repeated rate limit, `show` found nothing |
| 2 | arguments, a lock busy, `equity_update.py` or an input build running, ICICI log-in needed |

## Tests
- **`tests/test_icici_client.py`** (a fake `requests` session): the customerdetails body and both header sets from
  stubbed credentials, the secret never sent, only three URLs, every answer mapped to its error, and redaction
  (token, key, base64 value).
- **`tests/test_icici_plan.py`:** a synthetic world built with the real builders; `FakeBreeze` (the three calls, on
  each stock's raw prices and ICICI's frozen scale); a hand-written `kite_gaps.csv`; Kite `days.parquet` files for
  the live stocks. It covers:
  - Kite-covered days removed (a stale row);
  - an EOD row skipped;
  - ISIN beats symbol; SYMBOL_ONLY, NOT_IN_ICICI and REVIEW;
  - market-wide days for priority 1 / 2;
  - order, estimates and chunk helpers;
  - the master saved, reused and offline;
  - refusals, and the Kite store unchanged.
- **`tests/test_icici_download.py`** (the same world). It covers:
  - candles stored unchanged (09:07 / 15:31 bars) and `bars_outside`;
  - x2 then x1 at a bonus (explained step), with the volume level;
  - DAY_EMPTY with the previous scale, SHORT_DAY, VOLUME_DIFF;
  - BEFORE_START vs EMPTY in the days, status and coverage;
  - SYMBOL_ONLY pass and REVIEW (archived, nothing stored);
  - the loader raw / ours / icici;
  - 1,000-bar split and a truncated single day;
  - cap and resume with no call repeated, across runs and days;
  - pacing (90 a minute = 1.5 a second, one wait per call);
  - auth mid-run, at start and without credentials (exit 2);
  - rate limit once (continues) and twice (stops, exit 1);
  - a FAILED chunk retried next run;
  - the finished-day rule on both nets;
  - `--days-per-stock`;
  - locks, `equity_update.lock` and ABORTED runs;
  - `derive` on a hand-built example (levels, SCALE_UNSURE, next-day fallback, SCALE_STEP) and the allowed scales;
  - the start check:
    - a sample with minutes searches again (13 -> 5, 2 rounds);
    - no minutes keeps the start, and the check is not repeated;
    - a stock searched before the change is checked first in the next run;
    - NONE moved to the real start, and NONE kept (noted);
    - a failed sample records nothing;
    - the status line;
  - a session timeout at the start leaves no `running` row (exit 1);
  - the 09:15-15:29 range:
    - `aggregates` without the pre-open / 15:31 bars, and an evening session on all its bars;
    - a 09:07 bar above the bhav high or below the low gives OK;
    - a 12:00 bar above the bhav high stays HIGHLOW_DIFF;
    - a bhav close only inside the pre-open bar gives CLOSE_DIFF;
  - `checks --rederive`:
    - the dry run leaves every file byte-identical;
    - `--apply` makes the archive backup, keeps the bars unchanged and rewrites the labels, and a second run
      finds no change;
    - an error part-way restores every file;
    - refusals on the lock and `equity_update.lock`, and `--apply` without `--rederive`;
  - `plan --dry-run`:
    - plan, data, reference and the progress db are byte-identical afterwards;
    - LIVE2 is added at priority 2;
    - re-added stored days show as "stored", with nothing fetched again;
    - the partly done stock is shown with its position;
    - a busy lock gives exit 2.

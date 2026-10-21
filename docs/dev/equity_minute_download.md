# Kite 1-minute history download (phase 2)

Downloads Kite's 1-minute candles for every stock in phase 1's `plan.csv`, each from its own `minute_first_day`,
over several evenings (resumable). Each day is checked against Kite's day candle fetched in the same run, the data is
stored by `security_id`, and a loader rebuilds raw or adjusted prices. A small daily `update` follows. Code:
`backfill/equity/minute_download.py` (work list, download, build, checks, update, status) and
`backfill/equity/minute_store.py` (file layout, `load_minute`, `clean_bars`); CLI `equity_minute.py download | update |
status | checks | retoken`.
Not part of `equity_update.py` (it needs a Kite sign-in); no dashboard UI.

## Part 0 findings (Kite, 2026-10-19, 214 requests; also `PART0_FINDINGS`)
- **Speed:** a minute request takes 0.73 s on average (median 0.79), a day request 0.12 s. One request at a time
  reaches only ~1.3-1.5 req/s, so two workers are needed to reach the 2.5 req/s limit. 150 minute requests at 2.5:
  no 429. Full history: ~116,000 minute + ~5,300 day requests, ~13.5 h with 2 workers (~26 h with 1).
- **Size:** RELIANCE 373 rows per Kite day, ENERGYDEV 267, IPOs ~372. zstd parquet 17.7 / 12.5 bytes per row
  (1.66 / 0.85 MB per stock-year). All stocks: ~20-27 GB.
- **Candle times:** 09:15..15:29 (375 bars); IPO listing day from 10:00 (330 bars); weekend sessions (Budget days,
  2024 DR drills 09:15..12:29), Muhurat (60 evening bars), outages (2017-07-10 from 12:30, 2021-02-24 to 11:43). F&O
  stocks end at 15:14 (360 bars) from August 2026 (the closing auction); ENERGYDEV still ends at 15:29.
- **Minute vs day:** the last minute close is not the day close (NSE's close is the average of the last 30 minutes;
  RELIANCE matches on 20 of 2,900 days), so the update keeps the day request. Summed minute volume is 0.5-1.5% below
  the day's (median), down to 15-27% on some days (pre-open and closing-session trades). High / low are mostly
  exact; the day range can be wider. The day candle equals the bhavcopy (close, volume) on recent days.
- **Scale:** Kite's minute and day histories are not always on the same adjustment scale. RELIANCE day / minute
  high and low = 1.00945 until 2020-05-13, then 1.0 from the rights ex-date 2020-05-14.
- **Minute-only days** (minutes and a bhavcopy row, no Kite day candle): Muhurat 2017-10-19 / 2018-11-07 and ENERGYDEV
  2018-12-03..2019-01-08. **Day-only:** 2016-01-01 for RELIANCE and ENERGYDEV.
- **Drift:** 20 stocks' day candles were unchanged against phase 1's 18 Oct `kite_day` files, but no ex-date fell
  between the two fetches. Kite re-adjusts its history after an action, so the same-run rule below stays.

## Decisions (user, 2026-10-19)
- Kite's candles are stored unchanged, plus a per-day conversion measured against a Kite day candle **fetched in the
  same run**. Every stored day records its `run_id` and `fetched_at`.
- **minute_scale** per day = Kite day / minute from high and low, measured where both agree (within 0.1% plus two
  half ticks). Levels come from `levels()`: a centred median of 11 measured days. A new level starts where it moves
  by more than 0.1% and lasts 5 measured days (a shorter move is a blip). A level within 0.05% of 1 is 1.0.
- **minute_volume_scale** is 1.0 by default. It changes only where day volume / minute volume steps by more than 20%
  for 5+ days; that period gets its median ratio / the normal ratio of the nearest unadjusted period
  (`volume_levels()`).
- A scale step of either kind with no corporate action within ±5 trading days is `SCALE_STEP`. Actions are the applied
  events in `adjustments/events.csv` plus every row of the corporate-actions CSVs (rights, dividends, demergers, ...).
- `--workers 1..4` (default 2; `MAX_WORKERS`): workers take whole stocks from one queue, so never the same stock.
  One locked `SharedLimiter` (start-to-start spacing across all workers, so `--speed` (≤3) caps them together; halves
  on 429, counts requests and 429s). sqlite in WAL mode, every statement under one lock. Priority 1 measured ~1.5 s
  per minute request (not Part 0's 0.79), so 2 workers reached only ~1.1 req/s; more workers only shorten the wait
  for Kite's answers, never the spacing.
- `download` / `update` raise `ValueError` for other worker counts (the CLI exits 2).
- `plan.csv` older than the security master or adjustments `build_info.json` (after an evening `equity_update.py`)
  is rebuilt by `download` / `update` themselves, with phase 1's offline gaps step (`mp.build_outputs`, no Kite
  request), and they say so.
- `update` keeps its own files under `update/` and never rewrites phase 1's plan folder.

## Work list (`download --dry-run`)
- `plan.csv` rows with `download_minute = Y`, plus `update/new_listings.csv`. Tokens come from `update/tokens.csv`
  when the update has matched them since. Without `plan.csv`: `NotBuilt` (1). The input locks and staleness are
  checked by `minute_plan.Inputs`.
- Order: priority 1 (ever in NIFTY 50), 2 (ever in NIFTY 500), 3; within a group, median bhavcopy turnover, largest
  first, then id. Options: `--priority`, `--symbols` (any symbol the company had), `--limit` (stocks still to do),
  `--speed` (≤3), `--workers`.
- Range `minute_first_day .. latest bhavcopy day`, 60-day chunks (`make_chunks`).
- Estimate: per request `max(1/speed, latency/workers)`. The latency is Part 0's until 200 requests are timed in the
  db. Disk uses `BYTES_PER_DAY` per priority (6,600 / 5,000 / 3,300; measured from DONE stocks once 3 exist), and
  shows free space. A real run refuses below estimate x 1.2 (`NotEnoughDisk`, exit 2). The dry run writes nothing.

## Download
- Refuses: `equity_1min/.lock` busy (shared with phase 1), a Kite F&O run (`mp.check_fno`), an input lock.
- `download_progress.sqlite`: `chunks` (security_id, token, chunk_from, chunk_to, rows, status, fetched_at, run_id,
  seconds, error), `day_fetches`, `stocks` (token, status DONE / NO_KITE_DATA, first/last day, days, minute_rows,
  bytes, built_at, reason, reason_at), `runs` (command, started, ended, requests, n429, result, peak_mb), `errors`
  (security_id, token, run_id, kind `DAY_CANDLES` / `UPDATE`, chunk_from, error, at: the text of a failed day-candle
  request or update, which before went only to the console; a failed minute chunk keeps it in `chunks.error`).
  `peak_mb`, `errors` and `reason` / `reason_at` are added to older files by the next real run or `retoken --apply`,
  never by `status` or a dry run.
- **ABORTED:** once `download` / `update` hold `.lock` (an OS lock, gone with its process), every `runs` row still
  `running` belonged to a killed process: it becomes `ABORTED` (`ended` stays empty) and the run logs it. A dry run
  marks nothing.
- Per stock: the first time a run fetches any of its chunks, it fetches the stock's day candles for the whole range
  and stages them as `staging/<sid>/day_<run_id>.parquet`. Then each chunk not DONE with this token (and with its
  staged files) goes to `staging/<sid>/m_<from>.parquet` and is committed. Each day takes its Kite day candle from
  the run of the chunk that holds it. So a stock fetched over two evenings, with a Kite re-adjustment in between,
  converts each day on its own scale.
- When all chunks are DONE, the build writes `data/<sid>.building/` and renames it to `data/<sid>/`:
  `<year>.parquet` (`ts` Asia/Kolkata, OHLC float64, volume int64, zstd, Kite's values unchanged) and
  `days.parquet` (`DAY_COLUMNS`). Then it marks the stock DONE and removes the staging. A DONE stock whose token
  changed is moved to `archive/<sid>_<date>/` and fetched again.
- Stops: `AuthStop` stops both workers (`SignInNeeded`, exit 2, finished chunks kept). Ctrl+C / `StopJob` ends the
  in-flight chunk, then exit 0 "stopped, resume with the same command". `ChunkFailed` marks the chunk FAILED and the
  run goes on (exit 1; a re-run retries it).
- A progress line every 10 stocks and at a priority change: stocks, chunks, rows, req/s, 429s, ETA, disk, memory
  (current and peak). `disk_used` skips a file the other worker removed meanwhile (a built stock's staging).

## Failed stocks, `retoken` and NO_KITE_DATA (2026-10-21)
The full download left 21 stocks not DONE: a 502 on one chunk (token unchanged), a new Kite token, Kite listing only
`SYMBOL-BE`, and 14 weekly-auction BE/BZ stocks gone from Kite's list (their old tokens answer `InputException:
invalid token`).
- **`retoken [--apply]`**: every plan stock whose `stocks` row is not DONE (NO_KITE_DATA included) is matched again
  in one `kite.instruments("NSE")` list with phase 1's `match_tokens` (exact symbol, then SERIES_SWITCH):
  `SAME_TOKEN`, `NEW_TOKEN`, `SERIES_SWITCH`, `NO_KITE_DATA` (not in the list) or `REVIEW` (several candidates, not
  ACTIVE, a failed price check; never guessed). `NEW_TOKEN` / `SERIES_SWITCH` need `price_check`: one day-candle
  request (10 days to the stock's last bhavcopy day); Kite's close that day within 2% (`mp.PRICE_TOL`) of the
  bhavcopy close. No candle that day or a failed request is REVIEW too.
- The dry run (default) prints the table (symbol, old / new token, Kite tradingsymbol, result, price check, note, what
  `--apply` would do) and writes nothing. `--apply` (takes `.lock`) makes the same requests, then saves
  `update/kite_instruments_NSE_<date>.csv`, merges the checked tokens into `update/tokens.csv` (`MATCHED`, other rows
  kept; `read_plan` and `update` use them), marks NO_KITE_DATA stocks, clears the state of NO_KITE_DATA stocks found
  again, and writes `check/no_kite_data.csv` and `update/retoken_<date>.csv`. Phase 1's plan folder is not touched.
  DONE stocks are never candidates; a DONE stock whose token changes still goes to `archive/` (download / update).
- **NO_KITE_DATA** (`stocks.status`, `reason` "not in Kite's NSE list of <date>", `reason_at`): `download` skips it
  (also with `--symbols`; the work list says so), `estimate` counts it apart, `status` shows it on its own line, and
  `update` does not wait for it (it still refuses for any other stock not DONE). `check/no_kite_data.csv`
  (security_id, symbol, isin, last_bhav_day, reason) feeds phase 1's gaps (below).
- **Back in the list:** when `update`'s token match finds a NO_KITE_DATA stock again, it gets the same price check
  (`back_in_list`). Passed: the `stocks` row is dropped (its chunks stay) and it gets a full download in that update.
  Failed or no candle: it stays NO_KITE_DATA and the log says `REVIEW <symbol> ... stays NO_KITE_DATA` with the
  reason. `retoken --apply` also clears a stock found again (SAME_TOKEN / NEW_TOKEN / SERIES_SWITCH).
- **status** lists unresolved failures with their error text (a FAILED chunk, or a stock's newest `errors` row newer
  than its `built_at`; NO_KITE_DATA stocks left out), at most 30 lines.

## Memory
The first real `--priority 1` run (2026-10-19) was killed by Windows for low memory (5.9 GB machine) after 63
chunks: phase 1's `Inputs` loaded the whole bhavcopy (5.1M rows, 0.76 GB as pandas, 1.66 GB process peak while
reading the 12 year files), `Calc` grouped it again, and a build held all of a stock's minutes. Now:
- `Inputs(bhav=False)`: no bhavcopy rows. `turnover_medians()` (work-list order) streams the year files, two
  columns at a time, and writes nothing (dry run).
- `bhav_cache/<year>.parquet` (+ `cache_info.json`): the universe's master-series rows with security_id, sorted by
  security_id and date, 20,000-row groups; built by `Calc` under the lock, one year at a time; a year is rebuilt
  when its store file's size / mtime changed, all years when the master's build_info or the universe changed.
  `read_cached_bhav(paths, sid)` reads one stock (row-group filter). The rows equal `mp.read_bhav`'s.
- The build reads staged chunks one at a time (`minute_aggregates` per chunk; a chunk never splits a day), holding
  at most one year of one stock for its year file; frames are freed after each stock.
- `rebuild_checks` goes stock by stock: problem rows are appended to `minute_check.csv.tmp` (numeric columns as
  float64) and renamed at the end; only per-date counts, steps and MINUTE_EMPTY dates are kept, and the
  `minute_empty_days` runs re-read only the `date` column of the stocks that have any.
- A stale `plan.csv` still needs the whole bhavcopy (phase 1's gaps step): `ensure_plan` loads a full `Inputs` for
  it only, frees it, and logs memory before / peak / after (measured: 0.41 GB after freeing, so it stays in
  process).
- `memory()`: the working set via `GetProcessMemoryInfo` (ctypes, no psutil), `ru_maxrss` elsewhere. `runs.peak_mb`
  and the CLI's last line give each run's peak.

## days.parquet and the conversion
- Kite-vs-bhav rows (days with a day candle and a bhavcopy row) go through phase 1's functions: `with_factor`, ratio,
  `kite_multiplier`, `kite_difference`, `adjustments_and_mismatches` (the day `kind`), `half_units`.
  `vol_ratio` = bhav volume / Kite day volume.
- Days without their own conversion (DAY_EMPTY, EXTRA_SESSION) take the nearest day's, preferring the same
  minute_scale and our factor (`conversion_from`). The symbol and series fill from the neighbours.
- `load_minute` rebuilds raw prices: p' = Kite minute x minute_scale. MULTIPLICATIVE raw = p' x ratio; ADDITIVE raw =
  kite_multiplier x (p' + kite_difference). Volume = Kite minute volume x minute_volume_scale x vol_ratio.

## Checks (per day; tolerance 0.05% + half of Kite's rounding unit)
`OK`, `CLOSE_DIFF` (the day close outside the scaled minute low..high), `CLOSING_AUCTION` (the same, on a day whose
last minute bar is in 15:00..15:28), `HIGHLOW_DIFF` (the scaled minute high above
the day high, or the low below the day low; a wider day range is normal), `VOLUME_DIFF` (scaled minute volume >
day x 1.01 + 1 or < day x 0.5), `MINUTE_EMPTY` (a day candle and a bhavcopy row, no minutes), `DAY_EMPTY` (minutes
and a bhavcopy row, no day candle), `EXTRA_SESSION` (no bhavcopy row), `SCALE_STEP`. A day can carry several,
`|`-joined. Files in `check/` are rebuilt from every `days.parquet` at the end of a run:
- `minute_check.csv`: the problem days;
- `minute_empty_days.csv`: runs of MINUTE_EMPTY days per stock. A date that more than half of 10+ downloaded stocks
  with a day candle lack minutes for becomes one market-wide row;
- `scale_steps.csv`: every scale step, its near action and whether it is flagged;
- `summary.json`: counts per check and priority, stocks with minute_scale ≠ 1, volume steps, and `closing_auction`
  (per stock its first CLOSING_AUCTION date and count).

**CLOSING_AUCTION.** From 2026-08-03 NSE's F&O stocks trade continuously only to 15:14 and close in an auction:
Kite's minutes end at 15:14 and the official close (Kite's day candle = the bhavcopy close) is often outside the
minute range. On those days **the last minute bar is not the close; backtests must take the close from the
bhavcopy** (or Kite's day candle). The rule needs no F&O list (equity code imports no F&O module): the 15:14 end
itself marks the stock. The window starts at 15:00 so that days cut short for other reasons stay CLOSE_DIFF: the
2021-02-24 NSE outage (minutes to 11:43, 18 priority 1 days) and half-day sessions (12:29, 14:44; EXTRA_SESSION).
Priority 1 (80 stocks): 77 stocks end at 15:14 from 2026-08-03 (ACC, SAMMAANCAP, ZEEL still end at 15:29); 517 of
589 CLOSE_DIFF days became CLOSING_AUCTION.

**`checks [--apply]`** (offline) re-derives every built stock's days from its stored `days.parquet` input columns
with `Calc.derive` (as `update` does) and relabels them with the current rules. Only `check` may change: a stock where
any other column would differ (e.g. a newer bhavcopy) is listed and never written (exit 1). The dry run (default)
prints the counts per check before -> after and writes nothing; it needs a current bhavcopy cache (`CacheStale`,
exit 1, otherwise). `--apply` takes `.lock`, rewrites the changed `days.parquet` atomically, rebuilds `check/` and
prints each stock's first CLOSING_AUCTION date.

## Phase 1's gaps (the approved change in `minute_plan.py`)
`build_outputs` calls `minute_gap_rows()`:
- days with stored minutes (`data/<sid>/days.parquet`, `minute_rows > 0`) leave the 1-minute part of
  `KITE_MISSING_SESSION` and `EOD_EMPTY_DAYS` rows. Where every day has minutes the row becomes `EOD` (a new
  `data_missing` value); where only some do, it splits into `EOD` and `BOTH` rows;
- `check/minute_empty_days.csv` adds `1MIN / MINUTE_EMPTY_DAYS` rows;
- `check/no_kite_data.csv` (retoken) adds a `BOTH / NO_KITE_DATA` row per stock over its `need_from..need_to`, so they
  are on the vendor list.
With no phase 2 files, the output is byte for byte phase 1's.

## Update (`update`, every evening after `python equity_update.py`)
- Refuses: a plan stock not DONE and not NO_KITE_DATA (`HistoryNotDone`, exit 1); the newest bhavcopy day before today IST (`NoBhavToday`,
  exit 1); the same locks and F&O check as download.
- One `kite.instruments("NSE")` request, matched with `mp.match_tokens`. It writes
  `update/kite_instruments_NSE_<date>.csv` and `update/tokens.csv`. A matched security absent from phase 1's
  `kite_tokens.csv` is a new listing (`update/new_listings.csv`, first minute day = its first bhavcopy day). New
  listings and changed tokens get a full history. NO_KITE_DATA stocks matched again get the price check first (see
  above); the others are skipped.
- Every other DONE stock: day and minute candles from the day after its last stored day to today. The new days are
  derived with the stored days as neighbours, and the stored rows are kept as they are. The year file and
  `days.parquet` are rewritten atomically (days last).
- Report: stocks updated, new days, problem checks, new listings, exits since the last stored day
  (`ex.exits_between`; exited stocks simply get no more days).

## Status (`status`, offline)
Per priority DONE / partly / not started, minute and day requests left, hours, disk, chunks DONE / FAILED, rows
built and staged, disk used, 429s, unresolved failures with their error text, NO_KITE_DATA stocks, ABORTED runs and
the last run (with its peak memory; a `running` row prints
"running now" while the lock is held, else that the next download / update marks it ABORTED).

## Loader
`minute_store.load_minute(security_id_or_symbol, start=None, end=None, prices="raw"|"ours"|"kite", folder=None,
clean=True)`:
- the key is an id or any symbol / ISIN ever used (`sm.lookup`; a name shared by several companies is resolved at
  `start`);
- returns `ts, open, high, low, close, volume, security_id, symbol` (that day's symbol);
- `ours` = raw / today's cumulative factor of the day (`mp.with_factor` on `adjustments.load_factors`), volume x it;
- a stock without data raises `NoMinuteData`: "exited ...; use the bhavcopy" for exited stocks;
- `clean=True` (default): the stored Kite bars go through `clean_bars` before the conversion. Bars with any of
  open / high / low / close <= 0 are dropped (Kite pads some sessions with all-zero bars: 1,060,588 bars in 898
  stock-years in the final report), a negative volume becomes 0 (192 bars), and high / low are widened to contain
  open / close (372 bars). `attrs["clean"]` = {bars, dropped_nonpositive, volume_set_zero, highlow_widened}.
  `clean=False` returns the stored bars exactly (no attrs). The stored files are never rewritten.
- The day checks use the same cleaned bars: `minute_aggregates` (new builds and `update`'s new days) runs on
  `clean_bars`, so zero bars do not count in `minute_rows`, `first_bar`, OHLC or volume, and a day of only zero bars
  is MINUTE_EMPTY. Days already stored keep their aggregates (`checks` re-derives from them). `stocks.minute_rows`
  stays the number of stored bars.

## CLI and exit codes
`download [--dry-run] [--priority N] [--symbols A,B] [--limit N] [--speed 2.5] [--workers 1-4]`, `update`, `status`,
`checks [--apply]`, `retoken [--apply] [--speed 2.5]`, each with `--folder`. Exit codes: 0 done / stopped by the user /
dry run; 1 FAILED chunks, not built, stale input, history not done, no bhavcopy for today, retoken left a stock to
REVIEW; 2 arguments, a lock busy, a Kite F&O run, Kite sign-in needed, not enough
disk.

## Tests
`tests/test_equity_minute_download.py` uses its own synthetic world (store, `sm/adj/ex.build`, index files, phase 1's
`tokens` + `probe` on a fake Kite giving `plan.csv`). The `FakeKite` has 3 minute bars per day, a minute scale
(SCALECO), a volume step explained by a dividend (VOLCO), an additive stock, a split, a rename, an exited stock,
missing minute days, a market-wide day without minutes, a session without day candles, a weekend session, bad
high / close / volume days, a token error, a failing chunk, Ctrl+C, 429s, a whole-history re-adjustment between two
runs and the update day with a new listing and a changed token. `backfill.fetch.STOP` is replaced by `FastStop` (no
back-off wait). Also: 4 workers (every chunk once, the same files as 2 workers, 5 refused), `SharedLimiter` from 4
threads (one spacing, halved on 429), the CLOSING_AUCTION window (15:14 / 15:00 yes; 15:29, 14:59, 11:43 no; a close
inside the range stays OK), `checks` dry run (writes nothing) / apply (only `check` changes, summary first date) /
refusal when another column differs / stale cache / lock.
`tests/test_equity_minute_fixes.py` (the same world; `FixKite` drops stocks from the list, lists them as `SYM-BE`,
answers old tokens with `InputException`, has tokens without day candles, shifted prices and BIG's zero bars): error
text in `errors` and `status`, an old progress file not changed by `status`; retoken's dry run (every result, nothing
written, CLI exit codes), `--apply` then `download` with the new tokens (stored stocks unchanged); NO_KITE_DATA in the
db, `no_kite_data.csv`, `status`, skipped by `download`, `update` refusing only for other stocks, running with it,
bringing it back (price ok) or keeping it (price off / no candle), `retoken --apply` clearing it, `gaps` rows; zero bars
in the day checks, `load_minute` clean / not clean, `clean_bars` counts.
`tests/test_equity_minute_memory.py`: worlds of 10 and 40 plain stocks over two years (`PlainKite`), the
tracemalloc peak of a two-worker download plus check rebuild must not grow with the stocks (40 ≤ 10 x 1.15 +
0.5 MB; the old code grew 2.97 -> 7.43 MB and fails it, the new one 2.76 -> 3.05 MB); the cache rows equal
`read_bhav`, only changed years rebuild, the dry run writes no cache; ABORTED marking by download and update (not
a dry run), status text, an old progress file without `peak_mb`; the stale-plan memory line; `disk_used` with a
file removed meanwhile.

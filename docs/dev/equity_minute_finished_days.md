# Kite 1-minute store: finished days only, and `repair`

`equity_minute.py update` stores a day only once Kite's day candle for it is final, and `equity_minute.py repair`
fetches again the day candles stored before this rule (the 1-minute bars are never fetched again). Code:
`backfill/equity/minute_download.py` (`target_day`, `bhav_holidays`, `unfinished`, `Runner.hold_back`, `update`,
`status`) and `backfill/equity/minute_repair.py` (`find_affected`, `repair`); tests
`tests/test_equity_minute_finished_days.py`. The rest of the download is in docs/dev/equity_minute_download.md.

## Evidence (real data, 22-23 Oct 2026)
- A day candle fetched on its own trading day is **provisional**: close = the last minute close, volume = the summed
  minute volume. The stored ratio (bhavcopy close / Kite close) is then wrong, and `load_minute(prices="ours")`
  (and "raw") scale that day's minutes by it.
- Fetched again the next day, all 60 tested candles (21 and 22 Oct, 30 stocks) matched the bhavcopy exactly (close
  within 0.01%, volume exact). Every day fetched on a later day already matched (~4,800 rows).
- **Minute bars fetched the same evening are final** (5 of 5 stocks identical bar for bar): the repair never fetches
  minutes again.
- Around midnight (21/22 Oct, 00:01-00:09:30 IST) Kite returned **no day candle** for the previous day: 383 stocks were
  stored as DAY_EMPTY 21 Oct rows (minutes present), 308 got nothing. From 00:09:30 the candles were final (one night's
  evidence only). A candle fetched at 00:00:00 (ARIHANTSUP) was still provisional.
- In the store on 23 Oct: 4,089 provisional rows (21 + 22 Oct) and 383 DAY_EMPTY 21 Oct rows in 2,592 stocks (382
  have both). Every row has `fetched_at` and `run_id`. `update` never revisits stored rows.
- COMFINTE's stored token answered "invalid token" on 23 Oct (worked 22 Oct 22:08): out of scope, the repair lists it
  as skipped (its request fails).
- A Kite day request takes ~0.12 s; Kite allows ~2.5 requests per second.

## The rule: the target day T
- A day D is **finished** from (D + 1) 01:00 IST (`FINISHED_AT`). `target_day(days, now, holidays)` gives T = the latest
  trading day D with now >= (D + 1) 01:00 IST, with the reason (one line) and whether its bhavcopy is in the store.
  A naive `now` is IST.
- **Trading days** are the bhavcopy dates (`Inputs.days`, the exit list's calendar of every date in the store). After
  the newest of them a weekday counts too, unless `NSE_CM_bhav/days.csv` marks it `holiday` (cm_bhav's real 404);
  a Saturday or Sunday counts only once its bhavcopy is in the store. Before the newest bhavcopy day a date missing
  from the store is no trading day (weekend, holiday).
  - An evening run on D stores up to D-1; a run after 01:00 the next morning stores up to D. A Monday-morning run
    stores up to Friday.
  - A holiday not yet checked by cm_bhav counts as T without a bhavcopy: `update` refuses (run `equity_update.py`,
    which logs the holiday), then targets the day before.
  - Special sessions (Budget Saturday, Muhurat) count once their bhavcopy is in the store.
- `update` fixes T **once** at the start: a run that crosses midnight (or 01:00) keeps it. "No bhavcopy for T"
  (`NoBhavToday`, exit 1, before any Kite request) replaces the old "no bhavcopy for today". Day and minute candles are
  fetched from `last_day + 1` to T; a stock with `last_day >= T` is up to date.
- The full histories (`download`, and new listings / token changes inside `update`) end at T at the latest
  (`work_list(target=...)`); a new listing first trading after T waits for the next run.
- `status` prints `Target day for update now: <T> <weekday> (<why>)`.

## The safety net (`unfinished`, `Runner.hold_back`)
- A day (with a bhavcopy row) is **unfinished** when it has minutes but no day candle, or when the candle volume equals
  the summed minute volume while not equal to the bhavcopy volume. An illiquid stock where all three are equal is
  stored.
- `age` = trading days from the day to T (T = 0). An unfinished day with age <= 3 (`DEFER_DAYS`) is **deferred**:
  neither it nor any later day is stored (no `days.parquet` row, no minute bars), `last_day` stays, and the next
  `update` fetches it again. Deferred days are counted (`deferred_days` / `deferred_stocks`), never as failed.
- **Cap:** an unfinished day older than 3 trading days is stored anyway, so no stock is deferred forever. The volume
  case gets the check **`CANDLE_UNCONFIRMED`**, set by `update` on the row it stores (`add_check`, in `CHECKS` order);
  without a candle it is `DAY_EMPTY` as before. Counted as `unconfirmed_days`.
- **The label is sticky, never derived.** `Calc.derive` does not compute it, and `checks` keeps it where it is stored.
  A read-only scan of the real store (23 Oct) found 49,567 days with candle volume = summed minute volume != bhavcopy:
  9,072 / 14,595 / 11,152 / 9,659 in 2015-2018 (Kite's final old candles, the bhavcopy on another volume scale,
  median difference 300%), 988 in 2019-2025, and 4,101 in 2026 (4,089 of them the provisional rows). Derived, the label
  would have relabelled ~45,000 final old days; the rule is only good for days just fetched.
- In `update` every new day is checked; in a full-history build only the days with age <= 3 at its end (older days
  are finished: the Muhurat and ENERGYDEV 2018 DAY_EMPTY days stay). A build cuts deferred days from its temporary
  year file and `days.parquet` before the rename; when nothing is left it builds nothing (staging kept).
- Run summary: `Update to T <weekday> (target day: why)`, then `Target day T: updated N stocks (X new days, P with a
  problem check), D deferred days (S stocks), U stored unconfirmed, ...` and the lists (at most 20 lines each). The CLI's
  last line shows the deferred days.

## `repair [--apply] [--limit N] [--speed 2.5]`
1. **Selection** (`find_affected`, stored data only: every `days.parquet`, the progress db's `runs` and
   `day_fetches`):
   - PROVISIONAL: a day candle fetched before (date + 1) 01:00 IST, the same rule as T. The fetch time is the row's
     `fetched_at`, or its run's `day_fetches.fetched_at` when earlier (a build's row carries the minute chunk's time;
     its day candles may have come before midnight). The report also gives the count of the old date rule (fetch date
     on or before the row date) and how many the 01:00 rule adds (ARIHANTSUP, fetched 00:00:00).
   - DAY_EMPTY: a `DAY_EMPTY` row of an `update` run fetched before (date + 1) 01:00 IST (the midnight window).
     Historical DAY_EMPTY days (fetched years later by `download`) and normal rows are never selected.
2. **Tokens:** one `kite.instruments("NSE")`, matched with `mp.match_tokens`. A stock whose token changed or that is
   not matched is skipped and listed (left to `update` / `retoken`); a stock not DONE too. For a stock not matched by
   symbol the reason also says where its stored token is in that list ("now listed as <tradingsymbol> (<name>,
   <segment>)" or "not in today's list"; no extra request).
3. **Per stock:** one day request over its affected dates. A date whose new candle is still unfinished (same check as
   the safety net) is skipped and listed; a failed request (COMFINTE) skips the stock with Kite's error text; a sign-in
   error stops (exit 2).
4. Only the day inputs of the repaired dates change (`kite_*`, `fetched_at` = the repair's fetch time, `run_id` = the
   repair run). **Each repaired row is derived as `update` stores a new day** (`repaired_rows`): for each repaired
   date d in order, `Calc.derive` on the inputs up to d only (the stored days as neighbours, nothing later yet), with
   the new candles in; row d is taken. A test checks the rows equal those of a day-by-day update with final candles
   (all columns but `fetched_at` / `run_id`).
5. **Only the repaired rows are written** (`splice`): the stored file with those rows replaced; every other row stays
   exactly as stored. Minute files are never written.
6. **Guard:** two fresh derives of the whole stock, without (`base`) and with (`trial`) the new candles, are compared
   on every row not being repaired, split at the first repaired date:
   - a row that **borrows a repaired date's conversion** (`conversion_from`, stored or in `trial`), earlier or later:
     its stored values came from the provisional candle. Refused: "earlier row borrows a repaired date's conversion:
     ..." (or "a row borrows ..."), counted apart (`borrowing`);
   - a **later row** (after a repaired date) that would change, e.g. a 23 Oct row stored before the repair and derived
     with the provisional 22 Oct as a neighbour. Refused: "a later row would change: ...".
   Refused stocks are listed with the columns and dates and not written (exit 1).
   - **Drift:** only rows before the first repaired date differ, none borrows. That is hindsight of a whole-history
     derive (`levels()` re-measured with later days; real data 23 Oct: mostly `minute_volume_scale` / `note`), the
     same effect as the pre-existing differences. The stock is repaired (only the repaired rows are written, the
     earlier rows stay as stored) and listed: `Fresh-derive drift on earlier rows (not written): N stocks; columns:
     ...`, with its own checks before -> after and ratio buckets for comparison with the rest.
   **Pre-existing differences** (`base` != the stored file: an older rule, a newer bhavcopy, ...) never block a stock
   and are not fixed; the report gives one line: `Pre-existing re-derive differences (not fixed; those rows stay as
   stored): N stocks; columns: <column> (<stocks>), ...`.
7. **Dry run** (default) writes nothing but makes the Kite requests (the counts after need the new candles). It prints
   the rows found (provisional with both rule counts / DAY_EMPTY), stocks, requests and minutes needed, the checks of
   the repaired rows before -> after, the ratio changes (new / old - 1: <=0.1%, 0.1-0.5%, 0.5-2%, >2%), and every
   skipped stock, skipped date and refused stock with its reason. A bhavcopy cache older than the store is built in a
   temporary folder (removed after). `--limit N`: the first N stocks (by security_id), for a trial.
8. **Refusals (dry run and `--apply`, exit 2):** `.lock` held (both take it for the whole run, so no `update` /
   `download` shares Kite's limit), a Kite F&O run (`check_fno`), a bhavcopy download or input build (`StoreBusy`).
9. **`--apply`:** marks runs left `running` ABORTED, records a `repair` run; per stock copies `days.parquet` to
   `equity_1min/archive/repair_<YYYY-MM-DD_HHMMSS>/<security_id>/days.parquet` and writes the new one as
   `days.parquet.tmp`; at the end replaces them all. Any error removes the `.tmp` files, restores every repaired file
   from the backup and raises `ApplyFailed` (exit 1; the run is `failed`). Then `repair.csv` (every row: REPAIRED /
   SKIPPED / REFUSED, old / new check and ratio, old `fetched_at`) goes into the backup folder and `check/` is rebuilt.
10. **Re-runnable:** repaired rows have a `fetched_at` after their date and a run that is not `update`, so a second
   run finds only what it skipped or refused. A killed run leaves at most `.tmp` files (overwritten next time).
11. Exit codes: 0 done / dry run (skips listed), 1 a stock refused or `ApplyFailed`, 2 a lock, a Kite F&O run, an input
    build, or Kite sign-in needed.

**Estimate (23 Oct):** 1 + 2,592 = 2,593 requests, 0.4 s each at 2.5 req/s (~17 min), plus ~0.25 s per stock to
read, derive and compare: about 25 minutes per run (the dry run and `--apply` each).

## Tests
`tests/test_equity_minute_finished_days.py` (test_equity_minute_download.py's world; `EdgeKite` gives provisional /
missing candles and minute volumes equal to the day's): the target day (22:00 on D, 00:30 / 01:00 on D+1, Monday,
holidays both ways, a weekend session, no bhavcopy for T, `days.csv`, status, an evening update, a run crossing
midnight); the safety net (no candle and provisional deferred then stored, illiquid stored, the build cut, the cap at
ages 0-3 / 4 with `CANDLE_UNCONFIRMED` kept by `checks` and `checks --apply`, `derive` never adding it,
`add_check`); repair (selection incl. the 00:00:00 row and both counts, dry run writes nothing, new token / still
provisional skipped, a borrowed conversion refused for an older and for a later row (nothing written for it), a
later row changing without borrowing refused, drift on earlier rows only repaired with those rows exactly as stored
(a wrapped `derive`), the refusal counts, the stored token of a stock not matched, apply
writes only the repaired rows (the others exactly as stored) and backs up, a pre-existing difference repaired with
its rows kept and reported, the repaired rows equal a day-by-day update's, an error on the third replace restores every file, a second run finds nothing,
`--limit`, refusals by lock / F&O / input build for the dry run and apply, CLI exit codes).

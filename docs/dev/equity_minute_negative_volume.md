# Kite 1-minute store: bars with a negative volume

Kite sends a few minute bars whose volume is garbage. Since this change they count as 0 in the day aggregates: the day
gets the check `NEGATIVE_VOLUME` and the column `neg_volume_bars`, and `download` / `update` print a warning line.
`equity_minute.py fix-negative-volume` fixes the `days.parquet` rows stored before this change. **The stored minute
bars are never changed.**
Code: `backfill/equity/minute_download.py` (`minute_aggregates`, `Calc.derive`, `Runner.warn_negative`),
`backfill/equity/minute_store.py` (`read_days`), `backfill/equity/minute_negvol.py` (`fix`). Tests:
`tests/test_equity_minute_negative_volume.py`. The rest of the download is in docs/dev/equity_minute_download.md.

## Evidence (read-only, 24 Oct 2026)
- **The whole store** (2,625 stocks, 20,242 year files, 1.48 bn bars) has **200 bars with a negative volume, on 193
  stock-days in 90 stocks**. By year: 2015 1, 2016 3, 2023 175, 2024 21.
  - 31 are INT64_MIN.
  - 5 are other huge garbage values (-5.4e18 .. -8.2e18).
  - 164 are small (-1 .. -102,000).
  - 7 days have two or more such bars. BHANDARI 2023-12-13 has two garbage bars, and their int64 sum wrapped to
    +2.65e18.
- No bar with a negative volume has a price <= 0.
- Nothing else holds garbage: no INT64_MAX, no volume > 1e11, no negative, NaN, inf or > 1e7 price.
- **The cause is Kite.** A re-fetch of 5 such days (KSHITIJPOL 2023-11-23, GVPTECH 2023-09-27, BHANDARI 2023-12-13,
  ANKITMETAL 2023-10-12, WSI 2023-12-12) gave every other bar identically. The bad bars come back as **unsigned
  64-bit** values: 2^63, 1.05e19, 2^64 - 2000, 2^64 - 999. `minute_frame`'s int64 cast turns them into INT64_MIN,
  -7.9e18, -2000 and -999. So Kite keeps a negative or garbage volume, and a re-fetch cannot recover the real volume.
- **The stored rows:**
  - The full download (19-20 Oct) built 185 of the 193 rows before `minute_aggregates` cleaned the bars (21 Oct).
    Their `minute_volume` is the plain int64 sum, negatives included and wrapping. 130 of the 193 days were
    VOLUME_DIFF.
  - The other 8 rows (rebuilt on 24 Oct) already had the clean sum, but no label.
- `load_minute(clean=True)` has always read these bars as volume 0. Only that bar's volume is missing.
- **Where the summed minute volume matters:**
  - VOLUME_DIFF of its own row.
  - `kite_volume / minute_volume` feeds `volume_levels()`: a centred median of 11 days, so `minute_volume_scale` and
    its notes / SCALE_STEP labels may also move on neighbouring days, in a period whose level is not 1.0.
  - `unfinished()`, on new days only.
  - `vol_ratio`, the loader, phase 1's gaps and ICICI do not use it.
- **ICICI** (report only, nothing changed): `equity_1min_icici` has 16 bars with a negative volume (down to
  -5,215,749) in 4 stocks. `icici_download.aggregates` runs on `ms.clean_bars`, so its day sums are right, but it has no
  label or count either.
- **Not handled here** (a later task): 18,566 old rows have `minute_low <= 0`. Their aggregates still include Kite's
  zero padding bars (built before 21 Oct).

## The rule (future runs)
- `minute_aggregates` runs on `ms.clean_bars`: a negative volume counts as 0, so the sum cannot overflow.
  `neg_volume_bars` = the number of the day's kept bars (price > 0) whose stored volume is < 0. It is an input
  column (`INPUT_COLUMNS`) and a day column (`DAY_COLUMNS`, after `minute_volume`).
- `Calc.derive`:
  - Sets `NEGATIVE_VOLUME` (in `CHECKS` after VOLUME_DIFF) where `neg_volume_bars > 0`.
  - Adds the note `negative volume on N minute bar(s), counted as 0` (last).
  - Computes VOLUME_DIFF on the clean sum.
  - The label is derived from a stored column, so `checks` keeps it.
- `Runner.build` (the days it stores) and `Runner.append` (the new days) log
  `  WARNING <symbol> <date>: N minute bar(s) with a negative volume (stored unchanged, counted as 0; NEGATIVE_VOLUME)`.
  `update`'s summary adds `Days with minute bars of a negative volume ...: N` when N > 0 (`stats["negative_volume_days"]`).
- `ms.read_days` gives a Kite file stored before the column (it has `kite_close`, no `neg_volume_bars`) the column with
  0, so `update` / `checks` / `repair` work without rewriting it. Other day files, such as ICICI's, are returned as
  stored.
- `check/minute_check.csv` keeps its columns: the count is in the note.
- **Loader:** `load_minute` is unchanged; it already reads a negative volume as 0. A day labelled `NEGATIVE_VOLUME`
  is missing the volume of `neg_volume_bars` bars.

## `equity_minute.py fix-negative-volume [--apply]`
1. **Refusals (exit 2):**
   - `mp.Inputs` raises StoreBusy while a bhavcopy download or input build runs (`equity_update.py`).
   - Then `equity_1min/.lock` (`update` / `download` / `repair` / `checks --apply` / `retoken --apply` hold it). Both
     the dry run and `--apply` take it.
2. **Scan** (one stock at a time): each year file's `volume` column is read first. Only a file with a negative value
   is read whole, and only the days with such a bar are kept.
3. **Per day:** `minute_aggregates` on its stored bars gives the new `minute_volume` and `neg_volume_bars`. Stored
   `minute_rows` / high / low that differ from the bars are reported (`mismatch`, not changed). The row changes only
   in:
   - `minute_volume` and `neg_volume_bars`;
   - `check`: the stored labels, with VOLUME_DIFF computed again from the new volume and the row's **stored**
     `minute_volume_scale`, plus NEGATIVE_VOLUME, in `CHECKS` order. Sticky labels (CANDLE_UNCONFIRMED) stay;
   - `note`: the stored note plus the negative-volume note.
   A day already right in all four is skipped, so a second run finds nothing. The stored dtype of `minute_volume` is
   kept.
4. **Guard:** the whole stock is derived without (`base`) and with (`trial`) the fixed inputs.
   - Rows outside the fixed ones where they differ, and columns other than the four on the fixed rows, are listed as
     **drift and not written** (for example a `minute_volume_scale` level that moves).
   - Stored values that a fresh derive no longer gives are listed on one line (pre-existing, not fixed).
5. **Dry run** (default) writes nothing. A stale bhavcopy cache is built in a temporary folder, as in `repair`. It
   prints:
   - the bar counts (INT64_MIN / other garbage below -1e12 / small);
   - every fixed row: bars, old -> new `minute_volume`, old -> new check;
   - the checks of these rows and of the whole store, before -> after;
   - drift, pre-existing differences, mismatches and peak memory.
6. **`--apply`:**
   - Each changed `days.parquet` is copied to `equity_1min/archive/negvol_<YYYY-MM-DD_HHMMSS>/<security_id>/` and the
     new one is written as `days.parquet.tmp`. All are replaced at the end.
   - Any error removes the `.tmp` files, restores every file from the backup and raises `ApplyFailed` (exit 1).
   - Then `negvol.csv` (every fixed row: old / new volume and check) goes into the backup, and `check/` is rebuilt.
   - Minute year files are never written.
7. Exit codes: 0 done or dry run, 1 `ApplyFailed`, 2 a lock or an input build.

## Tests
`tests/test_equity_minute_negative_volume.py` uses test_equity_minute_download.py's world. `NegKite` sends Kite's
unsigned garbage values on chosen bars. The tests cover:
- `minute_aggregates`: INT64_MIN + -5 on one day, and two INT64_MIN on another (no overflow);
- a build: the bars are stored as int64 exactly, the label / count / note are set, VOLUME_DIFF is computed on the
  clean sum, the warnings are printed, the loader reads 0, `summary.json` counts the label, and `checks` keeps it;
- `update` of a new day with a negative bar: the warning, the summary line, the label, and the stored rows unchanged;
- an old `days.parquet` without the column (read as 0 and never written by reading; `checks`, repair's selection and
  `update` work, and `update` writes the column) and a non-Kite day file left alone;
- fix:
  - the dry run writes nothing (every file hash);
  - apply changes only the four columns of the fixed rows (every other row, and the minute files, exactly as stored);
    the fixed stock equals a fresh derive, the backup and `negvol.csv` are written, and a second run finds nothing;
  - an error on the second replace restores every file (also through the CLI, exit 1);
  - it refuses on `.lock` and on an input build, for both the dry run and apply;
  - drift is listed and not written;
  - CLI output and exit codes, and a store without such bars.

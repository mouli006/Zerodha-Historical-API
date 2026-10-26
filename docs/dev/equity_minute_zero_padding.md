# Kite 1-minute store: rows built with all-zero padding bars

Kite pads some old sessions with **all-zero bars** (open = high = low = close = 0, volume 0). Since `b52ca35`,
`minute_aggregates` runs on `ms.clean_bars`, which drops them, so new rows and `load_minute(clean=True)` were never
wrong. But the rows the 19-20 Oct 2026 full download built **before** that commit still have day aggregates that count
the zero bars (`minute_low <= 0`). `equity_minute.py fix-zero-padding` fixes those `days.parquet` rows. **The stored
minute bars are never changed, and neither are the conversion columns, `minute_volume` or `neg_volume_bars`.**
- Code: `backfill/equity/minute_zeropad.py` (`fix`, `Stock`, `classify`).
- Tests: `tests/test_minute_zeropad.py`.
- It follows `fix-negative-volume` (docs/dev/equity_minute_negative_volume.md, which listed this as "not handled
  here").
- The download itself is in docs/dev/equity_minute_download.md.

## Evidence (read-only, 26 Oct 2026)
- **18,566 rows have `minute_low <= 0`** (2,625 stocks scanned):
  - **PADDING: 18,525 rows in 854 stocks, all in 2015.**
    - Every bar of these days with a price <= 0 is all-zero.
    - Stored `minute_rows` counted every bar.
    - Labels: HIGHLOW_DIFF 17,698, EXTRA_SESSION 827, VOLUME_DIFF 387, CLOSE_DIFF 351, SCALE_STEP 1.
    - Zero bars per day: median 13, from 1 to 375.
    - On **770 days every bar is zero**.
    - `minute_aggregates` on the stored bars changes `minute_rows` / `minute_low` (17,755 rows), `minute_open` /
      `first_bar` (16,091) and `minute_close` / `last_bar` (473).
    - It never changes `minute_high`, `minute_volume` or `neg_volume_bars`, and no PADDING day has NEGATIVE_VOLUME.
  - **OTHER: 41 rows** (40 in 2018, mostly 2018-12-13/14, and BSOFT 2019-07-05).
    - Each has a bar with low 0 but a real high or close (e.g. `o=0 h=95.85 l=0 c=95.85 v=0`).
    - `clean_bars` drops these bars too, so they are not padding. They are reported, never changed.
- Known case: **JISLDVREQS (S000003) 2015-05-15** has 375 bars, 5 of them all-zero: `minute_low` 0 -> 35.5,
  `minute_rows` 375 -> 370, HIGHLOW_DIFF.
- The whole store has 30,811 HIGHLOW_DIFF days; most of them are this artefact.
- A sample of 40 PADDING stocks: 0.44 s per stock for two derives, peak 357 MB.
  - `minute_scale_measured` changes on 38 of them.
  - `minute_scale` (a loaded-price column) changes on 1.

## Columns of `days.parquet`
| group | columns | the fix |
|---|---|---|
| (a) minute aggregates | `minute_rows, first_bar, last_bar, minute_open, minute_high, minute_low, minute_close` + `minute_scale_measured` | written on fixed rows |
| (a) volume | `minute_volume, neg_volume_bars` | never written (checked unchanged) |
| (b) conversion | `our_factor, ratio, kite_multiplier, kite_difference, kind, vol_ratio, minute_scale, minute_volume_scale, conversion_from` | never written; changes listed as drift |
| (c) labels | `check, note` | written on fixed rows |
| (d) source | `date, symbol, series, kite_*, bhav_*, fetched_at, run_id` | never written |

`load_minute` reads `kind, ratio, kite_multiplier, kite_difference, vol_ratio, minute_scale, minute_volume_scale`
(`minute_store.py`), so drift in these affects loaded prices.

`minute_scale_measured` counts as group (a), by the user's decision:
- It is the row's own measured day / minute ratio and is only written, at `minute_download.py` `Calc.derive`.
- Nothing reads it: it is not in `INPUT_COLUMNS`, which are what `update`, `repair`, `checks` and `fix-negative-volume`
  derive from, and not in the loader columns.
- Tests prove it: a source scan of `minute_*.py` / `icici_*.py`, and a garbage value that changes no derive, update or
  loader output.
- Writing it keeps `checks` from refusing every fixed stock.

## The rule
1. **Scan**, one stock at a time:
   - Read only `date, minute_low, minute_rows, kite_close, bhav_close, check` of each `days.parquet`.
   - For a stock with candidates (`minute_low <= 0`), read only the needed `<year>.parquet` files and classify each
     day (`classify`).
   - **PADDING**: every bar with a price <= 0 is all-zero (volume 0 too).
   - **OTHER**: anything else, for example a non-zero field, no such bar, or no bars at all.
2. **Per PADDING stock** (`Stock`):
   1. **New aggregates.** `md.minute_aggregates` on the day's stored bars gives the new group (a). A day of only zero
      bars gets what `inputs_from` gives a day without minutes (`minute_rows` 0, `first_bar` / `last_bar` "",
      prices NaN). **`minute_volume` stays at the stored 0**, where a fresh download stores none.
   2. **Volume check.** If the fresh `minute_volume` / `neg_volume_bars` would differ, the row is held as
      VOLUME_CHANGE.
   3. **Base and trial.** `base` is `Calc.derive` of the stored inputs. `trial1` derives with every PADDING row fixed:
      this is a fresh download's view, and it feeds `scale_drift.csv`.
   4. **Hold loop.** A PADDING row is held when, in the trial:
      - PRE_EXISTING: its stored conversion (or the row before's scales) already differs from `base`;
      - SCALE_DRIFT: its own conversion columns, or the `minute_scale` / `minute_volume_scale` of the row before
        (SCALE_STEP depends on them), would change;
      - LABEL_NOT_OWNED: a label the fix does not own (EXTRA_SESSION, NEGATIVE_VOLUME, SCALE_STEP) would change, or
        any note part except "converted as ..." would change.

      Held rows go back to their stored inputs and the stock is derived again. The loop repeats until no fixed row
      moves. Rows are only ever added to the held set, so it ends.
   5. **Fixed rows.** They take group (a), `minute_scale_measured`, `check` and `note` from the last trial (exactly
      the current rules). CANDLE_UNCONFIRMED is put back where it was stored, as `checks` does. Stored dtypes are
      kept.
   6. **Guard.** Every other row, and every other column of the fixed rows, must equal the stored values, or the run
      raises (apply restores).
3. The last trial is what `checks` / `update` will see afterwards:
   - For a fixed stock without drift, `checks` changes nothing and refuses nothing.
   - The report lists the fixed stocks `checks` would still refuse, and compares that list with the drift list.
4. **0 instead of none** on a day of only zero bars acts the same everywhere. Tested:
   - `Calc.derive`: checks, `vol_ratio`, both scales, no inf; `vr` needs `minute_volume > 0`, and VOLUME_DIFF needs
     minutes;
   - `unfinished`, which needs `minute_rows > 0`;
   - `load_minute`, which never reads `minute_volume`;
   - `checks`, `update` (stored rows unchanged, the new day the same), `repair`'s `find_affected` and
     `fix-negative-volume`.

## `equity_minute.py fix-zero-padding [--apply]`
1. **Refusals (exit 2), for both the dry run and `--apply`:**
   - `mp.Inputs` raises StoreBusy (`equity_update.py`, an input build);
   - `mp.check_fno` finds a held Kite F&O lock;
   - `equity_1min/.lock` is held (`update` / `download` / `repair` / `checks --apply` / `retoken --apply` /
     `fix-negative-volume`).

   The ICICI store has its own lock and may run.
2. **Dry run (default):**
   - Writes only `equity_1min/zeropad/dryrun_<YYYY-MM-DD_HHMMSS>/`: `report.txt`, `rows.csv` (old / new of every
     fixed column), `held_rows.csv`, `other_rows.csv`, `scale_drift.csv`.
   - A stale bhavcopy cache is built in a temporary folder.
   - It takes about 10-15 minutes on the full store. Run it in your own PowerShell:
     `python -u equity_minute.py fix-zero-padding 2>&1 | Tee-Object -FilePath <log>`.
3. **`--apply`:**
   - Each changed `days.parquet` is copied to `equity_1min/archive/zeropad_<time>/<security_id>/`, and the new one is
     written as `days.parquet.tmp`. All are replaced at the end.
   - Any error removes the `.tmp` files, restores every file from the backup and raises `ApplyFailed` (exit 1).
   - Then `zeropad.csv` (= `rows.csv`) goes into the backup, `check/` is rebuilt, and the report goes to
     `equity_1min/zeropad/apply_<time>/`.
   - A second run finds 0 rows to fix. Held and OTHER rows are listed again.
4. **The report:**
   1. PADDING / OTHER rows by year and stocks, with fixed / held per reason.
   2. Labels before -> after for the whole store and for the fixed rows.
   3. JISLDVREQS 2015-05-15 in full.
   4. 10 random fixed rows (seed 0).
   5. The columns changed, with counts, and a confirmation that volume and conversion are unchanged.
   6. Scale drift:
      - spans with old -> new scales and the % change of loaded close and volume (`ms.convert`, old vs new
        conversion);
      - the held PADDING rows and other changed rows per stock;
      - the stocks `checks` would still refuse after the fix, compared with the drift list, with each difference
        explained;
      - the stocks whose newest row would take a new level on the next `update`.
   7. Days of only zero bars:
      - labels before -> after;
      - the days that leave the "Kite has minutes" set (`mp.stored_minute_days`, `icici_plan.kite_minute_days`);
      - an **estimate** of the `minute_empty_days.csv` rows / days before -> after. It uses `rebuild_checks`'
        market-wide rule. These become `gaps`' 1MIN MINUTE_EMPTY_DAYS rows in `kite_gaps.csv`; `gaps` is not run.
   8. The OTHER rows, with their reason.
   9. Year files read / written (0), peak memory and seconds.
5. Exit codes: 0 done or dry run, 1 `ApplyFailed`, 2 a lock, an input build or an F&O run.

## Not handled here
- Conversion drift (`scale_drift.csv`) is never written. A scale fix, if needed, is a separate task after review.
- OTHER rows (bars with a price of 0 and real other fields) are reported only.

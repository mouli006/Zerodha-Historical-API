# Kite 1-minute store: held padded rows at the wrong minute_scale level

`equity_minute.py fix-rescale` fixes the few PADDING rows that `fix-zero-padding` held
(docs/dev/equity_minute_zero_padding.md) and that **load at the wrong price level**. A padded day (`minute_low` 0,
Kite's all-zero bars counted) is never measured (`minute_scale_measured` NaN), so `levels()` in `Calc.derive` gives it
its neighbour's level. On the first day of a real minute / day scale period, or the first day after one, that level is
wrong. fix-zero-padding holds such rows as SCALE_DRIFT because their `minute_scale` would move.
- Code: `backfill/equity/minute_rescale.py` (`fix`, `Stock`, `loaded_level`).
- Tests: `tests/test_minute_rescale.py`.
- It reuses `md.minute_aggregates`, `Calc.derive`, and from `minute_zeropad` `AGGREGATES` / `FIXED` / `SCAN_COLUMNS`
  / `CONVERSION` / `LOADED`, `classify`, `fmt` and `_replace`. mz's backup / `.tmp` / restore steps are inline in
  `mz.fix`, so the module repeats them (same steps, same `ApplyFailed`).

## Evidence (read-only, 28 Oct 2026)
- Of the 86 rows the 26 Oct zero-padding apply held, **2 are proven wrong**:
  - **BALKRISIND 2015-03-24** loads 2.00x the bhavcopy (stored `minute_scale` 1.0, fresh 0.5). It is the first day of a
    real scale period (BE window 03-24 -> 04-09, scheme of arrangement on 03-24); the padded day kept the level
    before.
  - **JMA 2015-03-30** loads 0.20x (stored 0.199998, fresh 1.0). It is the first EQ day after the BE window; the padded
    day took the BE level.
- 73 rows: stored and fresh both within 1% (tick noise on Rs 1-5 stocks). 7 COUNCODOS rows: stored right, fresh wrong.
  4 SHAHALLOYS rows: no reference. **All 84 stay held.**
- `checks` refuses these stocks for an unrelated reason (P80: weekend special-session bhavcopy rows added on 27 Oct
  fill `bhav_*` on rows stored as EXTRA_SESSION). The selection compares derive against derive, so it does not depend
  on `checks` passing, and those rows are never written.

## The rule
1. **Scan** as fix-zero-padding: `minute_low <= 0` rows, the needed year files only, `classify`. Only PADDING rows go
   on (OTHER rows are counted, never changed). After the zero-padding apply these are exactly the held rows.
2. **Per candidate row** (`Stock.select`), `base` = `Calc.derive` of the stored inputs, `trial` = derive with only this
   row's aggregates replaced by `minute_aggregates` of its bars (zero bars dropped):
   - **Level** (`loaded_level`): the row's clean aggregates (high / low / close) converted with `ms.convert` (the
     loader's code) divided by the bhavcopy high / low / close; the level is their median. "ours" divides by the
     factor and so does bhavcopy / factor: the factor cancels. Before = the stored conversion, after = the trial's.
   - Held, in this order:
     | reason | when |
     |---|---|
     | NO_REFERENCE | only zero bars (no minute prices) or no bhavcopy high / low / close |
     | VOLUME_CHANGE | the fresh `minute_volume` / `neg_volume_bars` differ (never written) |
     | STORED_OK | the stored level is within 1% |
     | FRESH_OFF | the fresh level is more than 1% off |
     | PRE_EXISTING | the row's stored conversion, or the next row's check / note, differ from `base` |
     | OTHER_MOVES | `trial` differs from `base` anywhere but the cells below |
   - Else the row is fixed. All fixed rows of a stock are derived together once more: still no other move and still
     within 1%, or the run raises (nothing written).
3. **Cells written**:
   - the fixed row: `minute_rows, first_bar, last_bar, minute_open, minute_high, minute_low, minute_close,
     minute_scale_measured, minute_scale, check, note` (CANDLE_UNCONFIRMED put back, stored dtypes kept);
   - the next row: `check, note` only (the scale step note moves: BALKRISIND 03-25 loses its note, JMA 03-31
     SCALE_STEP -> OK, note cleared).
   - Never written: `ratio, kind, vol_ratio, minute_volume, minute_volume_scale, neg_volume_bars, kite_*, our_factor,
     conversion_from`, and the minute bar files.
4. **Guard** (`Stock.new_frame`): every written cell equals the trial derive, every other cell equals the stored file,
   or `ApplyFailed` (nothing written).
5. A second run finds 0: the fixed rows have `minute_low > 0` and leave the scan.

## `equity_minute.py fix-rescale [--apply --expect N]`
1. Refusals (exit 2) as fix-zero-padding: `mp.Inputs` StoreBusy, `mp.check_fno`, `equity_1min/.lock` held.
2. **Dry run (default)** writes only `equity_1min/zeropad/rescale_dryrun_<YYYY-MM-DD_HHMMSS>/`:
   - `report.txt`: candidates, to fix, held per reason; each fixed row with its levels before -> after and every
     changed cell; every held row with its reason and levels;
   - `cells.csv`: every written cell (`fixed_row`, `date`, `role` target / next, `column`, `old`, `new`, `changed`);
   - `levels.csv` (the fixed rows) and `held_rows.csv`: loaded high / low / close / level vs the bhavcopy, before and
     after, and both `minute_scale`s.
3. **`--apply` needs `--expect N`** (argparse error, exit 2, without it). The whole scan runs in memory first; a count
   other than N raises `CountMismatch` (exit 1) before anything is written. With `--expect` a dry run also reports and
   exits 1 on a mismatch.
4. `--apply`: each changed `days.parquet` is copied to `equity_1min/archive/zeropad_rescale_<time>/<security_id>/`, the
   new one written as `.tmp`, all replaced at the end. Any error removes the `.tmp` files, restores every file and
   raises `ApplyFailed` (exit 1). Then `rescale.csv` (= `cells.csv`) goes into the backup, `check/` is rebuilt
   (`md.rebuild_checks`) and the report goes to `equity_1min/zeropad/rescale_apply_<time>/`.
5. Exit codes: 0 done or dry run, 1 `ApplyFailed` / `CountMismatch`, 2 bad arguments, a lock, an input build or an
   F&O run.

## Also in this change: P81
`minute_zeropad.one_stock` returned before computing `refused_after` when a stock had no fixed rows, so a second
fix-zero-padding run reported "0 refused" wrongly. It now computes it for every PADDING stock (the stored frame when
nothing is fixed). Nothing else in that file changed.

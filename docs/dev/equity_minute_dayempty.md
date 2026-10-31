# Kite 1-minute store: DAY_EMPTY rows (P75, decision D54 = option A)

A **DAY_EMPTY** row is a day with Kite minutes and a bhavcopy row but no Kite day candle: the Muhurat sessions
2017-10-19 / 2018-11-07, the EOD gap spans of P50, and two weekend sessions (2015-02-28, 2016-10-30). Such a day has
no conversion of its own, so `Calc.derive` (`backfill/equity/minute_download.py`) borrows one from another day.
`equity_minute.py fix-dayempty` brings the stored rows to the current rule.
- Code: `Calc.derive` with `dayempty_source` / `dayempty_checks` / `NO_OK_SOURCE` (`minute_download.py`);
  `backfill/equity/minute_dayempty.py` (`fix`, `Stock`, `CountMismatch`).
- Tests: `tests/test_minute_dayempty.py`.
- The download, the conversion and the other checks are in docs/dev/equity_minute_download.md.

## Evidence (read-only, 30-31 Oct 2026)
- The rule before P75 had three gaps:
  1. The source day's own check was ignored. **TIL 2018-11-07** borrowed from 2018-11-06, whose Kite candle is bad
     (close 2.80, ratio ~108, HIGHLOW_DIFF), so it loaded 29.4x the bhavcopy (close 9,154.93 vs 307.90).
  2. `pool = same if len(same) else src` could borrow across a factor change (no such row in the store).
  3. A DAY_EMPTY row was never checked: `both = has_min & has_day` gated HIGHLOW_DIFF / CLOSE_DIFF.
- The store has **6,388 DAY_EMPTY rows** (EQ 2,517, BE 3,652, BZ 219; 1,355 stocks): 2,563 Muhurat, 3,806 span,
  19 weekend.
- Loaded error with the old borrow, worst of high / low: ≤1% 5,787; 1-2% 233; 2-5% 241; >5% 127. A conversion
  measured from the bhavcopy would improve only 172 rows (option B, rejected).
- 30 Oct discovery: **335 rows (96 stocks) have bad minute data** (no multiplier fits; GTL 2019-01-30..02-08). They are labelled, not
  "fixed".
- Under the new rule (full store, derive of the stored inputs):
  - 634 rows get HIGHLOW_DIFF / CLOSE_DIFF / CLOSING_AUCTION;
  - 67 rows (12 stocks) get a new source. Every old source was a bad day: HIGHLOW_DIFF 60, SCALE_STEP 7.
    - 6 load better (TIL 29.39x → 1.0004x; INTEGRA 2019-01-18 0.967 → 1.000).
    - 51 are unchanged within rounding.
    - 10 load worse, and all beyond tolerance are labelled: VIJIFIN 8 rows (≤1% → 1.8-2.8%, HIGHLOW_DIFF and some
      CLOSE_DIFF), SHREERAMA 2015-02-28 (1.0053 → 1.0067, labelled) and VBL 2017-10-19 (1.0001 → 0.9994, within
      tolerance).
  - Rows without an OK same-factor source: 0. Rows borrowing across a minute_scale level: 0.
- Verification A (`Calc.derive` on main `baa03ac` against the branch, every column of every row of 2,632 stocks):
  977 rows differ.
  - **All of them derive as DAY_EMPTY**: 685 stored DAY_EMPTY + 292 P80 rows (stored EXTRA_SESSION on 2015-02-28 /
    2016-10-30, with a bhavcopy row added later).
  - **0 other rows**: EXTRA_SESSION borrowing and every row with its own candle are unchanged.
- Peak memory 271 MB for two derives of every stock (one stock at a time), about 15 minutes.

## The rule (`Calc.derive`)
1. The checks of days with a day candle (HIGHLOW_DIFF, CLOSE_DIFF / CLOSING_AUCTION, VOLUME_DIFF, SCALE_STEP) run
   first: they read no borrowed column, so their values are unchanged.
2. **Borrowing.** The same target rows as before (`~own | isnan(ratio)`).
   - Rows that are not DAY_EMPTY (EXTRA_SESSION, ...) keep the old code exactly: nearest day with an own
     conversion, the same minute_scale and factor first, else any. Changing EXTRA_SESSION is a later task.
   - A DAY_EMPTY row (`dayempty_source`) borrows:
     1. the nearest **OK** day of the **same `our_factor`**, the same `minute_scale` first, else any scale. OK means
        minutes, a Kite day candle and a bhavcopy row, and no HIGHLOW_DIFF, CLOSE_DIFF (CLOSING_AUCTION is fine) or
        SCALE_STEP;
     2. else the nearest day of the same factor with an own conversion, any check (same scale first), with the note
        `no OK source day`;
     3. else nothing (conversion NaN, `conversion_from` "", the same note): it loads as NaN. It never crosses a factor
        change.
   - Ties go to the earlier day (argmin), as before.
3. **The DAY_EMPTY check** (`dayempty_checks`):
   - the day's `minute_high / low / close` are loaded with the borrowed conversion (`ms.convert`, as `load_minute`)
     and compared with the bhavcopy high / low / close;
   - the tolerance is the normal days' rule in rupees, `PRICE_REL x price + HALF_TICK` (0.05% + 0.025). Scaling it by
     the multiplier gave ~400 labels instead of the 30 Oct discovery's 631, so it was not used;
   - **HIGHLOW_DIFF** when the loaded high is above the bhav high + tol or the loaded low is below the bhav low − tol;
   - **CLOSE_DIFF** when the bhav close is outside [loaded low − tol, loaded high + tol], or **CLOSING_AUCTION** when
     the last bar is in [15:00, 15:29), as on normal days;
   - the labels come after DAY_EMPTY in `CHECKS` order; the note keeps `converted as <date>`.
4. Every command that derives uses the rule: `update` (a new DAY_EMPTY day; stored rows are never re-derived),
   `checks`, `repair`, `fix-negative-volume`, `fix-zero-padding`, `fix-rescale`.

## `equity_minute.py fix-dayempty [--apply --expect-labels N --expect-borrow M]`
1. **Refusals:**
   - exit 2: `mp.Inputs` StoreBusy, `mp.check_fno`, `equity_1min/.lock` held;
   - exit 1: a stale bhavcopy cache (`CacheStale`; no temporary cache), counts other than the expected ones
     (`CountMismatch`), a failed apply (`ApplyFailed`, files restored);
   - `--apply` without both `--expect-*`: argparse error, exit 2.
2. **Per stock** with a stored DAY_EMPTY row, one at a time: `read_days`, then one `Calc.derive` of the stored inputs.
   Only rows **stored** as DAY_EMPTY can be written. Each is:
   - **held STALE_AGGREGATES** when `minute_low <= 0` (the aggregates still count Kite's zero bars; the 6 rows of
     2018-12-13/14). A HIGHLOW_DIFF there would be an artefact;
   - **held NOT_DAY_EMPTY** when the derive no longer says DAY_EMPTY;
   - **held PRE_EXISTING** when the stored value already differs from the derive in any cell the fix does not own, or
     in the parts it does not own of check / note, or (source unchanged) in the conversion, or (source changed) in the
     new source day's conversion. These are the 7 rows found: AKZOINDIA x2 `our_factor`; NMDC, TATAINVEST x2,
     BALKRISHNA, CUPID `kite_multiplier` / `kite_difference`;
   - else written. **Cells:** `check` (CANDLE_UNCONFIRMED put back) and `note`; where `conversion_from` changes, also
     `ratio, kite_multiplier, kite_difference, vol_ratio, kind, conversion_from`. Only changed cells are written and
     listed.
3. **Guard (`Stock.new_frame`):**
   - every written cell equals the derive, and every other cell of every row equals the stored file, or
     `ApplyFailed` (nothing written);
   - the P80 rows (stored EXTRA_SESSION) are never written: weekday conversion cells a fresh derive would change, the
     minute bars, `kite_*`, `bhav_*`, aggregates, volume, `minute_scale`, `our_factor`;
   - a MINUTE_EMPTY label never changes (asserted), so `minute_empty_days.csv` stays as it is.
4. **Dry run (default)** writes only `equity_1min/dayempty/dryrun_<YYYY-MM-DD_HHMMSS>/`:
   - `report.txt`: the counts, the label rows, every source change with the old / new loaded high / low / close (level)
     vs the bhavcopy, BETTER / SAME / WORSE and the final label, no-source rows, held rows with the reason, cells per
     column, and the check/ files an apply would change;
   - `labels.csv`, `borrow.csv`, `cells.csv` (every cell an apply would write), `no_source.csv`;
   - `info.csv`: held rows with the reason, and MINUTE_VOLUME_0 rows (report only);
   - `check_files.csv` / `check_labels.csv`: what `rebuild_checks` would change after an apply, from the stored labels
     in memory. It covers `minute_check.csv` rows on disk / before / after / changed, the `summary.json` label counts
     before → after, and `minute_empty_days.csv` / `scale_steps.csv` (unchanged);
   - the last lines are `labels N`, `borrow M`, `cells K`, `peak memory X GB, S s`.
5. **`--apply`:**
   1. The whole scan runs in memory first; counts other than `--expect-labels` / `--expect-borrow` raise before
      anything is written.
   2. Each changed `days.parquet` is copied to `equity_1min/archive/dayempty_<time>/<security_id>/`, and its sha256 is
      checked against the source.
   3. `manifest.csv` (security_id, symbol, source, backup, bytes, sha256) and `cells.csv` are written into the backup.
   4. The new files are written as `.tmp` and all replaced at the end. Any error removes the `.tmp` files, restores
      every file and raises `ApplyFailed`.
   5. `check/` is rebuilt (`md.rebuild_checks`) and the report goes to `equity_1min/dayempty/apply_<time>/`.
6. A second dry run after an apply reports `labels 0`, `borrow 0`, `cells 0` (the held rows are listed again).

## Expected on the real store (31 Oct 2026, all holds applied)
- **labels 625**: 634 − 3 PRE_EXISTING rows that would be labelled (NMDC 2017-10-19, TATAINVEST 2018-11-07, CUPID
  2018-11-07) − 6 STALE_AGGREGATES. That is HIGHLOW_DIFF 536, CLOSE_DIFF 132, CLOSING_AUCTION 12; span 460,
  Muhurat 161, weekend 4.
- **borrow 67** (none held).
- **rows 676** (625 + 67 − 16 with both), **cells 813**: check 625, conversion_from 67, note 67, ratio 16,
  kite_difference 16, vol_ratio 22.
- Report only: 6 STALE_AGGREGATES rows, 7 PRE_EXISTING rows, 162 rows with minute volume 0.

## Not handled here
- EXTRA_SESSION borrowing (the same bad-source problem: under the same rule 510 weekday EXTRA_SESSION rows in 88
  stocks would take another source): a later task.
- The P80 rows themselves (a weekend bhavcopy row added after they were stored): `checks` still refuses those stocks.

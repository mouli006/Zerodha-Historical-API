# Kite 1-minute store: the day before a split / bonus (split blip, P173)

On the day before a split / bonus ex-date, Kite's day candle can already be adjusted while that day's minute bars are
not. `Calc.derive` measures the break correctly (`minute_scale_measured` about 1/F). But `levels()` used to treat
one measured day as a blip (a level must last 5 days, and a single outlier never moves the 11-day centred median), so it stored
`minute_scale` 1.0 and `minute_volume_scale` 1.0. The loader computes price = minute x minute_scale x ratio and
volume = minute x minute_volume_scale x vol_ratio, so such a day loaded **F x too high in price and F x too low in volume**
(`prices="kite"` was right).
- Code: `backfill/equity/minute_splitblip.py` (`scan`, `Stock`, `fix`); guard (a) `md.split_blip_rows` and the `keep`
  argument of `levels()` / `volume_levels()` in `backfill/equity/minute_download.py`; guard (b) in
  `backfill/equity/minute_repair.py`.
- Tests: `tests/test_minute_splitblip.py` (the command), `tests/test_minute_splitblip_guards.py` (both guards).

## Evidence (read only, 6 Nov 2026; `analysis/p173_check_2026-11-06`, `analysis/p173_discovery_2026-11-06`)
- JSWDULUX 2026-10-21 (ex 10-22, x10) and AZADIND 2026-10-22 (ex 10-23, x5) were stored OK by `update`. `repair` run
  `20261023-145622-c33f` fetched only their day candles again, which Kite had adjusted by then, and kept the
  unadjusted minute bars: ratio 9.9993 / 4.9988, measured 0.100002 / 0.200012, stored 1.0. They loaded
  10.02x / 4.96x the bhavcopy close and 0.099 / 0.199 of its volume.
- 28 rows of 2015 have the same signature. The full `download` builds of 19-20 Oct (runs `7862`, `569d`, `9599`) stored them; their
  whole-history `levels()` made the blip. In 12 of them Kite's ratio also holds a later split (ratio = C, not F:
  AJANTPHARM, INSECTICID, APCOTEXIND, RELAXO, PERSISTENT, BEL, RUBYMILLS, VIVIDHA, HIKAL, KANSAINER, AARTIDRUGS,
  SHILPAMED). FCL has 2 rows. MENONBE has ratio = F only: Kite never applied its 2016 bonus 1:5.
- In total: **30 rows / 29 stocks**. A fresh derive with guard (a) changes exactly these 30 rows (`minute_scale`,
  `minute_volume_scale`, `check`, `note`) and their next rows (`note` only). All 30 become OK, close / bhavcopy LAST
  0.9988-1.0016, volume / bhavcopy 0.968-0.998.
- Not selected (4 of 5 conditions): SHIVAMAUTO 2015-10-07 and ALANKIT 2015-09-22 (ratio neither C nor F),
  MOTHERSUMI 2015-07-22 (ratio off), MAITHANALL 2015-07-14 (not measured). GISOLUTION, BEARDSELL, FEDERALBNK and ADI
  (P175), the other P174 rows, and GNRL / KMEW / CONCORDBIO (fetched on the ex-date, day and minutes adjusted together:
  measured 1.0, OK) are not selected either.

## The rule (`md.split_blip_rows`, used by guard (a) and the selection)
For row i of a stock:
- **F** = factor(day) / factor(next day). It is defined only when a factor period of the stock starts on the next day: the next
  stored row, or for the stock's last row the next market day. F != 1.
- **C** = factor(day) / factor(fetch date): the adjustments with an ex-date after the day and on or before the row's
  `fetched_at` date. It is 1 when fetched on or before the day, and NaN without a fetch time.
- keep: measured within 1% of 1/F, and ratio within 1% of C **or** of F.
Factors come from the factors table (`adj.periods_of(inp.factors)`, cached on the `Calc`), **never the stored
`our_factor` column** (it is not refreshed: AZADIND's stored rows still say 1).

## Guard (a): `levels()` / `volume_levels()`
`levels(values, rel_tol, ..., keep=None)` runs as before. Then, at each `keep` position whose measured value is more than
`rel_tol` off the level found, the measured value becomes a one-day level of its own. `volume_levels(ratio, keep=None)`
passes `keep` on, so the kept day's volume scale is (day / minute volume) / the nearest normal level (e.g. GRANULES
10.0155). It is **not snapped to F**: it then equals a fresh derive and loads like its neighbours (minutes miss the
pre-open / closing session volume, about 0.5-1.5%). With `keep=None` or no kept position the output is identical, so
no other row of the store changes.
`Calc.derive` builds `keep` for every derive: `download` builds, `update` (the new last row uses the market
calendar), `repair`'s day-by-day derive, `checks`, and the other `fix-*` commands.
**Known gap:** a pre-ex day stored **before its adjustment exists** (adjustments are built from the ex-date's
bhavcopy, so `update` usually stores the day before it) is not caught by guard (a). After every `equity_update.py`
that raises the adjustments count, run `python equity_minute.py fix-split-blip` (dry run) after the next morning's
`update`.

## Guard (b): `repair`
- A repaired date whose stored check is OK (CANDLE_UNCONFIRMED aside) and whose new candle gives HIGHLOW_DIFF or
  CLOSE_DIFF is **HELD**. It is not repaired and the old row is kept. It is listed ("Held (stored OK, the new candle gives ...)"),
  and on `--apply` written to `repair.csv` with result `HELD` and the old / new check. The other dates of the stock go on. The
  borrow / later-row guard and `splice` run on them only.
- Other new labels (CLOSING_AUCTION, VOLUME_DIFF, ...) are repaired as before. On 23 Oct this rule would have held 3
  of the 50 OK -> flagged rows (JSWDULUX, AZADIND, VIJIFIN 10-22 HIGHLOW_DIFF) and repaired the 25 CLOSING_AUCTION and
  22 VOLUME_DIFF rows as they were.
- A held row stays provisional, so the next `repair` tries it again.
- Every line `repair` prints also goes to `<data folder>/repair_logs/repair_<YYYY-MM-DD_HHMMSS>[_dryrun].log` (next to
  `equity_update.log`, outside `equity_1min/`; the only file a dry run writes).

## `equity_minute.py fix-split-blip [--apply --expect N]`
1. Refusals (exit 2): `equity_1min/.lock` held (dry run and apply both take it), an input build (`StoreBusy`), a Kite
   F&O run. `--apply` without `--expect`: argparse error (exit 2).
2. **Selection** from the stored columns only: `split_blip_rows` keep, stored `minute_scale` 1.0, check with both
   CLOSE_DIFF and HIGHLOW_DIFF.
3. **Per stock** one `Calc.derive` of the stored inputs (with guard (a); the dry run builds a temporary bhavcopy cache
   when the cache is stale, as `repair`). A selected row is fixed when all of these hold, else HELD with the reason:
   | reason | when |
   |---|---|
   | TRIAL_LEVEL | the derive does not give the measured value (rounded) as `minute_scale` |
   | TRIAL_CHECK | the derive's check is not OK |
   | NO_BHAV | no bhavcopy row for the day |
   | LOADED_OFF | loaded close / bhavcopy LAST more than 1% off |
   | VOLUME_OFF | loaded volume / bhavcopy volume more than 5% off |
   | PRE_EXISTING | ratio / vol_ratio / kind / kite_multiplier / kite_difference differ from the stored row, or the scales of the row before / after, or the notes beyond their scale step text |
   The measured scale is measured again from the stored `kite_*` / `minute_*` columns (`measure`, as `Calc.derive`);
   the stored `minute_scale_measured` column is never read (`test_minute_scale_measured_is_never_read`).
4. **Cells written** (D137, D140): the row's `minute_scale`, `minute_volume_scale`, `check` (CANDLE_UNCONFIRMED put
   back), `note`, and the next row's `note`. Never: `ratio`, `vol_ratio`, `kind`, `kite_*`, the minute aggregates,
   `minute_scale_measured`, `our_factor`, `conversion_from`, `bhav_*`, `fetched_at`, `run_id`, the minute bar files.
5. **Guard:** every written cell equals the derive. A sha256 over every other cell of the file (all other rows, and the
   unwritten cells of the two rows) must be equal before and after, both in memory and on the written `.tmp` file, else
   `ApplyFailed` and nothing is written.
6. **Dry run** (default) writes only `equity_1min/splitblip/dryrun_<time>/`: `report.txt`, `rows.csv` (per row: F, C,
   scales, checks, close / LAST, close / CLOSE, volume / bhavcopy before and after), `cells.csv`, `held.csv`,
   `hashes.csv`. Printed: `Selected n rows in s stocks (ratio ~ C: a, ~ F only: b)`, `To fix n, held h`,
   `Rows changed m (n fixed rows + k next-row notes), cells c`, `Unchanged rows: u (hash-identical in the f files
   written; g files not written)`, then one line per row.
7. **`--apply --expect N`** (N = the dry run's "To fix"; another count raises `CountMismatch`, exit 1, before anything is written).
   First the backup: `equity_1min/archive/fix_split_blip_<time>/<security_id>/days.parquet`, `rows_before.csv` (every
   row it changes, all columns), `cells.csv`. Then a `.tmp` per file, its hash checked, all replaced at the end. Any error
   removes the `.tmp` files, restores every file and raises `ApplyFailed` (exit 1). Then `check/` is rebuilt and the
   report goes to `equity_1min/splitblip/apply_<time>/`.
8. A second dry run selects 0: the fixed rows have `minute_scale` != 1.0 and check OK.
9. Exit codes: 0 done / dry run, 1 `ApplyFailed` / `CountMismatch`, 2 arguments / lock / input build / F&O run.

Expected on the real store (6 Nov 2026): selected 30, to fix 30, held 0, rows changed 60 (30 + 30 next-row notes).

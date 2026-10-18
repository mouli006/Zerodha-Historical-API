# Kite 1-minute / EOD plan (phase 1)

Which NSE stocks to download from Kite (1-minute and day candles since 2015), their Kite instrument tokens, from
which day Kite really has data, and one vendor shopping list of every stock / period Kite lacks
(`backfill/equity/minute_plan.py`, CLI `equity_minute.py`). No candles are downloaded to keep: the download itself is
a later task. Not part of `equity_update.py` (it needs a Kite sign-in); no dashboard UI.

## Part 0 findings (Kite, 2026-10-18, about 130 requests)
These decide the rules below; they are also printed in every `build_report.txt` (`PART0_FINDINGS`).
- **Instrument list:** columns `instrument_token, exchange_token, tradingsymbol, name, last_price, expiry, strike,
  tick_size, lot_size, instrument_type, segment, exchange`. No ISIN. 10,375 rows (NSE 10,239 + INDICES 136); 3,057
  without a series suffix, 229 `-BE`, 39 `-BZ`. **`last_price` is 0.0 on every row.**
- **One instrument per stock:** today's token has candles from before a rename (HEG -> HEGAM, VISASTEEL ->
  VISACHROME), before an ISIN change, and for every EQ and BE period (GSS, BHAGERIA, RELINFRA, VISASTEEL). The old
  symbol and the other series are not in the list.
- **Start:** day candles from 2015-01-01; 1-minute candles from **2015-02-02** for every stock (January 2015
  windows return nothing). For listings after 2015, master first date = Kite first day = Kite first minute day. In
  a 60-day window per stock, no day had a day candle without minute candles or the reverse.
- **Missing sessions:** Kite has no candles for the weekday Muhurat sessions 2017-10-19 and 2018-11-07. Kite has
  weekend sessions the bhavcopy store lacks (2019-10-27, 2020-02-01, 2020-11-14, 2023-11-12, 2024-01-20, ...).
- **Adjustment:** Kite adjusts for splits and bonuses, volume too (raw x factor, exact: KOTAKBANK 2026, RELIANCE
  2024). It also applies its **own** adjustments: demergers (RELIANCE 4.9% on 2023-07-20; ITC 2019 is 17.5% under
  our factors, prices / 1.2125 but volume x 1.1562), dividend-sized steps of about 2% (TATASTEEL 2024-06-21 and
  2025-06-06, INFY 2026-06-10), and **additive** ones (TATASTEEL before 2024: Kite = raw / 10 - about 4.6, so O/H/L
  give different multipliers). HDFCBANK 2019: prices match our x4, volume only x2. Kite's prices therefore cannot
  be turned back into raw prices with our factors alone.
- **EOD vs bhavcopy:** on recent days without later actions (RELIANCE, HDFCBANK, ITC, GSS, RELINFRA-BE, BHAGERIA),
  O/H/L/C and volume are identical. In split / bonus-only periods prices are within 0.05 (Kite rounds adjusted
  prices to 0.01 - 0.05).
- HDFC (merged 2023), MINDTREE and LTIM are not in the list.
- **Series switches** (real run, 2026-10-18): 8 ACTIVE stocks whose bhavcopy series is BE while Kite lists only the
  plain symbol (MTARTECH, HEXAGON, AMANTA, TRANSWORLD, PPAP, UMAEXPORTS, SMARTLINK), or EQ while Kite lists only
  `-BE` (GUJRAFFIA): one instrument per stock, so they are matched (`SERIES_SWITCH`) and price checked.
- **INFIBEAM -> CCAVENUE** (S001680, ISIN INE483S01020; INFIBEAM 2016-04-04..2026-02-02, CCAVENUE 2026-02-03..
  2026-10-12, one BL row 2026-03-23): Kite's list has only `CCAVENUE` (token 4159745, exchange_token 16249, name
  AVENUESAI, tick 0.01), no INFIBEAM and no -BE / -BZ row; its day requests for 2016-04-04..2026-10-12 returned no
  candles (`NO_KITE_DATA`). The bhavcopy has no row for that ISIN, nor any AVENUE* / CCAV* / INFIB* symbol, on
  2026-10-13..16, and the master has no AVENUESAI.
- **Kite's conversion** (real run, every probed stock vs the bhavcopy): raw = M x (Kite + B). M is Kite's own
  multiplier (our factor, times small dividend multipliers: GNFC 1.073, OFSS 1.062, BANKBARODA 1.068, HINDPETRO
  1.618 where our factor is 1.5), B an additive amount (OFSS about 225, GNFC 43, HINDPETRO 19.5, VEDL 10.6,
  TATASTEEL 3.6); both stay the same for years and step at an action. B = 0 for most stocks (purely
  multiplicative). Kite rounds adjusted prices to 0.05, and some stocks to 0.5, 1 or 5 (TATAINVEST, GODFRYPHLP,
  MCX, BAJFINANCE, ABBOTINDIA), not tied to the factor. Volume uses yet another Kite factor (SKFINDIA x2.13 where
  the price ratio is 2.185, VEDL x1.91 vs 2.62), steady between actions.

## Decisions (user, 2026-10-18)
- The price check uses the probe's day candle, not `last_price`.
- The EOD check writes `kite_adjustment.csv` (additive periods), `eod_mismatch.csv` (real data differences only)
  and `kite_vs_bhav.parquet` (every compared day, with its kind). Every tolerance allows half of Kite's rounding unit
  per Kite price on top of the percentage; each day converts with its own ratio, so only additive periods need
  finding, and a period splits only on a lasting step (user, 2026-10-18, second round).
- `SERIES_SWITCH`: the only candidate is the same symbol in another series -> matched, the price check decides.
- Market-wide missing sessions and the January 2015 minute gap are one gap row each, not one per stock.
- The later minute store keeps Kite's candles unchanged plus, per security-day, the ratio and differences from
  `kite_vs_bhav.parquet` and the day's `kind`: raw = Kite x ratio on MULTIPLICATIVE days, raw = kite_multiplier x
  (Kite + kite_difference) on ADDITIVE days (the same as our_factor x (Kite + adj_difference) when Kite's multiplier
  is ours). Built in the download task.
- DORMANT securities are not matched (only ACTIVE ones can have a token); their gap row says `DORMANT`.

## Part 1: universe and inputs (`Inputs`)
- The master's securities with `instrument_kind` STOCK (`--include-etf` adds ETF; RIGHTS never), with first / last
  date, last symbol / series / ISIN / close, status and exit reason from the exit list. `need_from` = max(first_date,
  2015-01-01), `need_to` = last_date.
- `check_inputs()`: `StoreBusy` (exit 2) while `NSE_CM_bhav`, `security_master`, `adjustments`, `exits` or
  `index_membership` `.lock` is held (`locks.held`, read only); `NotBuilt` (1) when a build is missing; `InputStale`
  (1) when the master is older than the newest bhavcopy year file, the adjustments older than the master, the exit
  list older than the adjustments, or the index membership older than the master.
- Everything is loaded once: the bhavcopy rows of the universe in the master's series (one row per security and
  day), the trading days (the exit list's `calendar.csv`), factors, membership periods and coverage, company names
  from the reference lists (`company` / `old_company` columns, by symbol).

## Part 2: `tokens` (one `kite.instruments("NSE")` request)
- Saved as received: `plan\kite_instruments_NSE_<IST date>.csv`.
- Rows of segment NSE are grouped by base symbol (`series_suffix()`: a 2-character suffix is a series; only BE / BZ
  are kept). For each ACTIVE security the expected name is the symbol (EQ) or `SYMBOL-BE` / `SYMBOL-BZ`. One
  candidate with that name -> `MATCHED_UNCHECKED`, `match_rule` `EXACT`; one candidate with the same symbol in
  another series -> `MATCHED_UNCHECKED`, `match_rule` `SERIES_SWITCH` (note "SERIES_SWITCH: bhavcopy series BE, Kite
  lists X (token)"; the probe's price check decides MATCHED / PRICE_MISMATCH); none -> `NO_TOKEN`; two or more ->
  `REVIEW` with the candidates in `note`. Never guessed. EXITED and DORMANT securities get no row.
- `kite_tokens.csv` (`TOKEN_COLUMNS`), `tokens_info.json` (list date, latest bhavcopy day, weekdays between, counts
  per match status and rule; a rename in between shows up as NO_TOKEN). A `kite_tokens.csv` from before
  `match_rule` existed reads as EXACT for its matches.
- `tokens --rematch`: matches again from the newest saved `kite_instruments_NSE_<date>.csv`, offline (`NotBuilt`,
  exit 1, without one). The tokens keep their list date, so a plain `probe` afterwards probes only the new matches
  (the others have a DONE row with the same token).

## Part 3: `probe` (Kite day and minute candles)
- Refuses: the inputs (Part 1); no `kite_tokens.csv` (`NotBuilt`, exit 1); a Kite F&O run: `held()` on every
  `<root>\*_FnO_1min\.lock` for root in `config.fno_raw_dir("kite")` and its with / without stores (read only, no
  F&O import; `FnoBusy`, exit 2); its own lock busy (exit 2).
- Kite from `config.load_env()` (`KITE_API_KEY`, `KITE_ACCESS_TOKEN`; never printed). `RateLimiter(--speed)`
  (default 2.5, at most 3), `fetch_chunk()` with its retries.
- Per MATCHED / MATCHED_UNCHECKED security, skipped when `probe_progress.sqlite` has a DONE row with the same token,
  `need_from` and `need_to` (`--force` probes all again):
  1. day candles `need_from..need_to` in 2,000-day chunks, saved as `plan\kite_day\<security_id>.parquet`;
     `eod_first_day`, `eod_last_day`, every bhavcopy day without a Kite day candle (JSON in sqlite);
  2. the price check: Kite's day close vs the bhavcopy close on the security's last bhavcopy day, within
     `--price-tol` (default 0.02) -> `MATCHED`, else `PRICE_MISMATCH`; no Kite candle that day -> stays
     `MATCHED_UNCHECKED`;
  3. the first minute day: one single-day minute request on `eod_first_day`; if empty and that day is before
     2015-02-02, the first Kite day on or after 2015-02-02; then a binary search over the security's Kite days
     (at most about 12 requests); none even on the last day -> blank.
- Each row is committed at once. `ChunkFailed` -> FAILED with the error (probed again next run; exit 1).
  `AuthStop` (token error, 403) -> `SignInNeeded`: "sign in and run probe again", exit 2. Ctrl+C stops after the
  last commit (exit 1). Then the outputs are rebuilt (Part 4).
- 1-minute days missing inside the range are not looked for: only the full download can find them, and the
  download task adds them to the gap list.

## Part 4: outputs (`gaps`, offline; also at the end of `probe`)
Every file is rebuilt in full and written atomically under `<folder>\equity_1min\plan\`.
- **States:** the token row, the probe row (only a DONE row with today's token counts), the price result.
  "Usable" = probed and MATCHED / MATCHED_UNCHECKED.
- **Market-wide sessions:** a date at least `SESSION_MIN` (10) usable securities traded inside their Kite range,
  more than `SESSION_SHARE` (50%) of them without a Kite day candle. It becomes one `KITE_MISSING_SESSION` gap row
  and is dropped from each stock's missing days (index coverage still counts it as missing).
- **kite_vs_bhav.parquet:** every Kite day of the usable securities joined with that day's bhavcopy row:
  `kite_open..kite_volume, bhav_open..bhav_volume, our_factor` (`factor_on`, vectorised in `with_factor`),
  `ratio` = bhav close / Kite close, `difference` = bhav close - Kite close, `adj_difference` = bhav close /
  our_factor - Kite close, `kite_multiplier` (M, below), `kite_difference` = bhav close / M - Kite close, `kind`
  (MULTIPLICATIVE / ADDITIVE).
- **Rounding unit** (`half_units`): 0.05, or the largest of `UNITS` (0.1 .. 10) all four Kite prices sit on for
  `UNIT_SHARE` (80%) of the stock's days in a year with one of our factors and on the day itself. Each Kite price
  in a comparison adds half a unit (`HALF_TICK` 0.025 for 0.05) to the 0.05% (`PRICE_REL`).
- **Kite's multiplier M** (`kite_multiplier`): bhav high-low / Kite high-low on days whose Kite range is at least 2%
  of the close and 20 units, the median of the 63 measured days around each day within one of our factors (follows a
  step at an action), our factor where nothing was measured.
- **Fits per O/H/L field:** multiplicative |Kite - bhav / ratio| <= 0.05% Kite + half unit x (1 + Kite / Kite close);
  additive |Kite - (bhav / M - kite_difference)| <= 0.05% Kite + 2 half units. An additive day fits additively but
  not its own ratio.
- **kite_adjustment.csv (ADDITIVE periods only),** per security and our factor: a period starts at an additive day
  and ends after `STEP_DAYS` (3) days that do not fit additively (days fitting both, flat candles, are neutral). It
  splits only on a lasting step: kite_difference moves by more than 0.3% of the close + 2 half units and the next 3
  days are additive at a level of their own; a shorter move is a blip and stays inside. Fewer than `ADD_MIN_DAYS` (3)
  additive days -> dropped. Columns: from / to, days, additive_days, kite_vs_ours, median_ratio, kite_multiplier,
  median_kite_difference, median_adj_difference, our_factor, kind.
- **eod_mismatch.csv:** a field that does not fit that day's own conversion (ratio, or additive inside a period), and
  a volume off raw x our_factor, raw x ratio and raw x Kite's volume factor (median of Kite volume / raw volume over
  the 11 days around it, `VOLUME_WINDOW`) by more than 1% + 1 share. Information only.
- **plan.csv** (`PLAN_COLUMNS`): symbols and ISINs with date ranges, status, exit reason, match status (after the
  price check), token, need range, `eod_first_day`, `eod_missing_days` (sessions excluded), `eod_mismatch_days`,
  `minute_first_day`, `download_eod` / `download_minute` (Y when usable with candles), `eod_requests` (2,000-day
  chunks from `eod_first_day`), `minute_requests` (60-day chunks from `minute_first_day`), `est_minute_rows` (Kite
  days from then x 375), `note` (not probed yet / the probe error).
- **kite_gaps.csv** (`GAP_COLUMNS`), sorted by priority, then median turnover descending (blank last):
  - EXITED -> one `BOTH` row over the need range, reason from the exit list (`EXIT_REASONS`: MERGER -> MERGED,
    VOLUNTARY / COMPULSORY_DELISTING -> DELISTED, UNKNOWN -> UNKNOWN_EXIT; LIQUIDATION, SUSPENDED, MOVED_TO_SME,
    RELISTED unchanged); DORMANT -> `DORMANT`; `NO_KITE_TOKEN`; `TOKEN_REVIEW`; `PRICE_MISMATCH`; `NO_KITE_DATA`
    (the token returned no day candles).
  - Matched: `BOTH` / `EOD_STARTS_LATER` for the bhavcopy days before Kite's first day when more than `LATER_DAYS`
    (5) (fewer go with the empty days); `BOTH` / `EOD_EMPTY_DAYS` per run of consecutive bhavcopy days without a Kite
    day candle (minute data is never there without a day candle, Part 0); `1MIN` / `MINUTE_STARTS_LATER` for the
    Kite days from max(eod_first, 2015-02-02) to the day before the first minute day when more than 5, or to the end
    when no minute day was found.
  - One `BOTH` / `KITE_MISSING_SESSION` row per session date and one `1MIN` / `KITE_MINUTE_START` row 2015-01-01 ..
    2015-01-30 (security blank, priority 1).
  - `symbols` / `isins`: the security's periods clipped to the gap. `company_name`: Kite's name, else the reference
    lists. `exit_note`: "into ACQUIRER, swap ratio" plus the exit list's note. `indices`: membership periods
    overlapping the gap, "(before reliable_from)" where so. `trading_days` and `median_turnover` (Rs crore per day)
    from the bhavcopy rows in the gap. `priority`: 1 in NIFTY 50 during the gap, 2 in NIFTY 500, else 3.
- **index_coverage.csv:** per covered index and trading day: members, members with Kite EOD (inside the Kite range
  and not missing), with 1-minute (also from the first minute day), and the symbols without each.
- **build_report.txt / build_info.json:** counts per status, match status, match rule, reason, data_missing and
  priority; the Part 0 block; sessions; days where Kite's factor differs from ours by more than 0.5% (information);
  additive periods; mismatch rows, security-days, per field, and the top 10 securities by mismatch days;
  coverage runs (all for NIFTY 50, 5 per
  other index); the later download's requests, hours at 2.5 requests/s, rows and disk (15 bytes per minute row in
  parquet, 45 in CSV: assumptions); the top 20 gaps of priority 1 and 2.

## CLI
`tokens [--rematch] | probe [--speed 2.5] [--force] [--price-tol 0.02] | gaps | show SYMBOL|ISIN`, each with `--folder`
(absolute) and `--include-etf`. Exit codes: 0 done; 1 not built / stale / tokens not run / some probes failed or the
probe stopped / show found nothing; 2 argparse, a lock busy, an input build or bhavcopy download running, a Kite
F&O run, Kite sign-in needed.

## Tests
`tests/test_equity_minute_plan.py`: a synthetic store from 2015-01-01 (80 weekdays, an N1 filler row every day),
`sm.build`, `adj.build` (a corporate-actions CSV confirms one split), `ex.build` (reference lists with the real
headers, one merger), index membership files with the real columns, and a `FakeKite` whose candles follow each
stock's raw prices: our factor, a multiplicative step, two additive periods (with a blip), bad O/H/L days, a volume
error, a late start, missing days, no minute data, a price mismatch, three series switches (one 10% off), a failing
token, a session missing for everyone. Hand-built frames test the EOD check: a rounded low-price stock, a coarse
Kite unit, a small dividend step, an additive period with a blip and a lasting step, Kite's own multiplier, a too
short additive run, real O/H/L and volume errors and whole-share rounding. Covers the universe, matching
(`SERIES_SWITCH`, `--rematch` offline, probing only the new match), the price check, probe (first days, binary
search, resume after a stop, token error, `--force`,
token change, the F&O lock, its own lock), the outputs, sessions, every gap reason, spans, priority and sort, index
coverage, the report, `gaps` offline, inputs byte for byte unchanged, every refusal and the CLI codes.

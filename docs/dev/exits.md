# Exit list and listing types

Who left, when and why; who arrived, and how (`backfill/equity/exits.py`, CLI `exits.py`). No network, no broker.
Every input is only read: the bhavcopy parquet (`NSE_CM_bhav\<YYYY>.parquet`, **every series**), the security
master (`security_master\`), the adjustment factors (`adjustments\`) and the NSE reference lists
(`NSE_CM_bhav\reference\`, from `nse_reference.py`).

## Why
A backtest holding a stock must know what happened when it stopped trading: shares of another company (merger),
cash at about the last price (voluntary delisting), or most of the money gone (liquidation, insolvency). And a
stock that "first appears" on NSE is not always an IPO: on 2026-04-20 NSE admitted 88 companies and on 2026-08-17
126 (most likely already listed on BSE), and companies move up from the SME platform.

## Part 1: status
- The calendar is every date in the store, whatever the series. `days_since_last` comes from `securities.parquet`
  (counted in those days).
- `ACTIVE`: `days_since_last < --active-days` (`ACTIVE_DAYS` 20), i.e. traded within the last 20 trading days.
- `DORMANT`: from 20 up to `--exit-days` (`EXIT_DAYS` 60).
- `EXITED`: more than 60.
- A security with GAP events that came back is judged by its final last date only. Its gaps are listed in
  `status.csv` `listing_note` ("gaps: a to b (n days)").

## Part 2: why each EXITED security left
**Keys:** a security is matched by the symbols it used in its last `NEAR_LAST` (60) trading days (plus its last
symbol) and by its ISINs (where the list has an ISIN: `voluntary_delisting.csv`). Distances are in trading days
from the last date. A date past the data counts weekdays on. Every match records its file, row (1 = the first
data row), date and distance.

**Reference headers** are read from the files and printed in the report. The columns used are in
`REFERENCE_FILES`. A missing list or column -> `ReferenceListError` (exit 1); `manual_mergers.csv` may be
missing.

| List | Rows used | Date | Window | Reason |
|---|---|---|---|---|
| `mergers.csv` | `exit_expected` not N | `last_trading_date`, else `suspended_from`, else `record_date` | ±10 (`MERGER_WINDOW`) | MERGER |
| `manual_mergers.csv` | fills blank `new_symbol` / `new_company` / `swap_ratio` of a `mergers.csv` row; a symbol with no `mergers.csv` row matches the latest exited security with that symbol (no date, note "manual, no date") | – | – | MERGER |
| `voluntary_delisting.csv` | list `delisted`, board Main Board (SME / ITP / proposed skipped and counted) | `delisting_date` | Voluntary: 0..60; Liquidation and Compulsory: any later day | by `delisting_type` |
| `delisting_circulars.csv` | not `withdrawal` | `effective_date`, else `circular_date` | voluntary, other / on hold: 0..60 (`DELIST_WINDOW`); compulsory, liquidation / ibc: any later day | voluntary, compulsory, liquidation / ibc -> LIQUIDATION; other / on hold: note only |
| `compulsory_delisting.csv` | all | `order_date` | any later day (orders come years later) | COMPULSORY_DELISTING |
| `suspensions.csv` | SUSPENDED / CONTINUED; CANCELLED rows and the rows they cancel (`cancels_circular`) dropped | `effective_date`, else `circular_date` | SUSPENDED: 0..10 (`SUSPEND_WINDOW`); LIQUIDATION: any later day | LIQUIDATION when `reason` (else `reason_text` if `reason` is blank) matches `LIQUIDATION_RX` (liquidation / winding up / IBC / NCLT / insolvency); else SUSPENDED |

The classified `reason` decides for suspensions, because the free text of a capital-reduction circular also
names the NCLT (SUMEETINDS 2024).

**Any later day** (`ANY_LATER`): every LIQUIDATION match, whatever the list, and every COMPULSORY_DELISTING match
(compulsory orders, compulsory circulars, the compulsory rows of the voluntary list). Liquidation and compulsory
delistings often take effect years after trading stopped.

**Relisting cap:** a reference date on or after the first date of a later security with one of the exited
security's near symbols (its `relistings()` entry, the same one that gives RELISTED) never matches the old
security. The unmatched reason says "after relisting as S... on d".

**From the data:**
- `MOVED_TO_SME`: one of its ISINs trades later in SM / ST.
- Any other later series of its ISINs goes in the note.
- `RELISTED`: the old side of a master `symbol_reused?` review row, or a later security (first date after this
  one's last date) holding one of its near symbols; the earliest one becomes `relisted_as`.
- `RIGHTS_ENDED` / `ETF_ENDED`: by `instrument_kind`.

**Priority** (`REASONS`): MERGER, VOLUNTARY_DELISTING, COMPULSORY_DELISTING, LIQUIDATION, MOVED_TO_SME,
RELISTED, SUSPENDED, RIGHTS_ENDED, ETF_ENDED, UNKNOWN.
- One exception (`pick_with_why`): when VOLUNTARY_DELISTING would win and a LIQUIDATION match is dated on or
  before the voluntary delisting date + `LIQUIDATION_AFTER_VOLUNTARY` (60) trading days, LIQUIDATION wins: the
  delisting was part of the insolvency (e.g. NATNLSTEEL, EDL, SANGHVIFOR). A liquidation dated later leaves
  VOLUNTARY_DELISTING (shareholders were paid out first).
  - The other match stays in the note.
  - The conflict review row says which was chosen and why ("chose X: ...").
- Within one reason, the smallest |distance| wins.
- Every other match is listed in `note` ("also ...").
- Matches from two of the `CONFLICT_GROUP` groups (merger / voluntary / write-down / SME / relisted), or two
  mergers naming different acquirers, -> review `conflict`.

**Acquirer** (MERGER):
- `new_symbol` (else `new_company` when it is a known symbol) is resolved with `security_id_for` on the last
  date, then a week earlier, then the next trading day. Failing that, the single security that ever had the
  symbol (it was named by a later or earlier symbol; noted).
- `acquirer_symbol` is the symbol as named; "acquirer now X" in the note when it was renamed since (MINDTREE ->
  LTI, now LTM).
- `swap_ratio` is the list's "A for B" text (A new per B old); the note carries "A/B new per old".
- Not found -> review `unresolved_acquirer`.
- Merger facts live only in `mergers.csv` and `manual_mergers.csv`. To add one, edit `manual_mergers.csv`, copy
  it to the repo backup `reference/manual_mergers.csv`, run `python nse_reference.py --offline`, then build.
  Rows added 2026-10-17: MINDTREE -> LTI 73 for 100, GSKCONS -> HINDUNILVR 4.39 for 1, IDFC -> IDFCFIRSTB 155 for
  100, SHRIRAMCIT -> SRTRANSFIN 1.55 for 1, TATASTLBSL -> TATASTEEL (ratio blank, not verified yet).

## Part 3: what a backtest should do (facts + a suggestion)
- `last_close` is raw. `last_close_adj` = `last_close / factor_on(security_id, last_date)`; with no factors (ETF,
  RIGHTS) the factor is 1, noted. Since no factor comes after an exited security's last day, the two are normally
  equal.
- Distress hints over the last `DISTRESS_WINDOW` (60) store trading days up to the last date, from the
  security's rows in the master's series:
  - `distress_price_change` (%, on adjusted closes, so a split is not a fall);
  - `distress_be_bz_days`;
  - `distress_turnover_ratio`: median daily turnover in the window / median over the `YEAR_DAYS` (250) trading
    days before it; blank without a year before.
  - A hint is any of: change ≤ `DISTRESS_DROP` (−50 %), BE/BZ days ≥ `DISTRESS_BE_BZ` (30), ratio ≤
    `DISTRESS_TURNOVER` (0.25).
- `suggested_treatment` (`treatment()`):
  - `CONVERT`: MERGER with a resolved acquirer and a parsable swap ratio.
  - `CASH_AT_LAST`: MERGER without a ratio (with or without an acquirer), VOLUNTARY_DELISTING, MOVED_TO_SME,
    RELISTED.
  - `WRITE_DOWN`: LIQUIDATION, COMPULSORY_DELISTING, UNKNOWN with a distress hint.
  - `NOT_APPLICABLE`: RIGHTS_ENDED, ETF_ENDED (a rights entitlement ends by design; an ETF is not a company).
  - `REVIEW`: anything else (incl. a MERGER with a ratio but no acquirer, SUSPENDED).
- **This is only a suggestion. The backtest decides the actual value of an exited holding.**

## Part 4: listing kinds (first appearance), in this order
1. `IN_DATA_FROM_START`: first seen within the first `--start-days` (`START_DAYS` 20) trading days of the data.
   Many illiquid stocks simply did not trade on 2015-01-01 (33 first seen on 2015-01-02); the report lists the
   day-by-day counts of the window.
2. `SME_MIGRATION`: one of its ISINs traded earlier in SM / ST (ANNAPURNA, SWARAJ 2026).
3. `RELISTING`: the `relisted_as` of an exit, or the new side of a master `symbol_reused?` row.
4. `BATCH_ADMISSION`: a STOCK first seen on a day after the start window with at least `--batch-min`
   (`BATCH_MIN` 20) STOCK first appearances. The count includes SME migrants and relistings that day.
5. `NEW_LISTING`: everything else (IPOs and other first listings; also ETFs / rights on a batch day).

A BATCH_ADMISSION or SME_MIGRATION start date is **not an IPO date**: strategies such as "IPO base" must skip them
(or use the real listing date from elsewhere).

## Files `<EQUITY_DATA_DIR>\exits\` (atomic, rebuilt in full)
- `exits.csv/.parquet` (`EXIT_COLUMNS`), `status.csv/.parquet` (`STATUS_COLUMNS`).
- `listings.csv` (`kind, date, count, examples`): one row per batch day and per SME-migration date.
- `review.csv` (`kind, security_id, date, symbol, details`; `REVIEW_KINDS`):
  - `unknown_large`: the `REVIEW_TOP` (50) UNKNOWN exits with the largest median turnover over their last year.
  - `conflict`, `unresolved_acquirer`.
  - `unmatched_reference`: list rows that fit no exited security, with why: no exited security with that
    symbol / ISIN, or a date outside the window (with the distance). Suspensions only when the circular expected
    an exit (`exit_expected` Y): a suspension that ends with trading resuming is normal.
  - Rows whose symbol (and ISIN) never appears in the bhavcopy, in any series, are not in review: they are only
    counted per file in the report.
- `calendar.csv` (the trading days, for `status_on`), `build_info.json`, `build_report.txt`:
  - the reference headers and the rows not considered;
  - counts per status / reason / treatment / listing kind;
  - the start window per day, the batch days, the SME migrations;
  - the known cases;
  - review counts with 10 examples.

## Locks / freshness
- `StoreBusy` (exit 2) while `NSE_CM_bhav\.lock`, `security_master\.lock` or `adjustments\.lock` is held
  (`locks.held`).
- `NotBuilt` (exit 1) without the master or the factors.
- `InputStale` (exit 1) when either `build_info.json` is older than the newest year file, or the adjustments are
  older than the master.
- Own `RunLock(exits\.lock)` (busy -> `LockBusy`, exit 2).

## Loaders (cached until the files change; `NotBuilt` when missing)
- `exit_info(security_id)`: the exits row as a dict, None if not exited.
- `exits_between(start, end, kinds=("STOCK",))`: exits with a last date in [start, end] (`kinds=None`: all).
- `status_on(security_id, on_date)`, point in time:
  - None before the first date.
  - While it trades: ACTIVE.
  - Inside a GAP (from the master's events) or after the last date: by the trading days since its last trade
    before that date (the build's thresholds).
  - After the data -> ValueError; unknown id -> KeyError.
- `listing_info(security_id)`: the status row plus `first_date`.

## Known cases (`KNOWN_*`, report and `check`)
- HDFC 2023-07-12 MERGER -> HDFCBANK 42 for 25, CONVERT.
- MINDTREE 2022-11 MERGER -> LTI (or LTIM / now LTM).
- SHRIRAMCIT, TATASTLBSL, GSKCONS, CAIRN, IDFC: MERGER with an acquirer.
- One real voluntary (`voluntary_delisting.csv`) and one compulsory (`compulsory_delisting.csv`) exit after 2015,
  picked from the build and named.
- ANNAPURNA, SWARAJ: SME_MIGRATION.
- 2026-04-20 (~88) and 2026-08-17 (~126): batch days.
- RELIANCE, HDFCBANK: ACTIVE.
- VERTOZ, SUMEETINDS: RELISTED.

A miss is reported, never fixed by changing a rule: ask the user. `build` exits 0 with a miss, `check` exits 1.

## CLI (`exits.py`)
- `build [--folder] [--active-days 20] [--exit-days 60] [--batch-min 20] [--start-days 20]`
- `show SYMBOL|ISIN`
- `check`
- Exit codes: 0 done; 1 not built / stale / reference list problem / show found nothing / check failed; 2
  argparse, a lock busy.

## Tests
`tests/test_exits.py`: a synthetic store (`write_store` with an N1 filler row on every day, SM rows), `sm.build`
and `adj.build` on it (a corporate-actions CSV confirms one split), synthetic reference lists with the real
headers, `config.load_env` stubbed.

Covers:
- the thresholds and their options; a gap then a return;
- merger with a ratio and an acquirer renamed later; without a ratio (filled from manual); manual-only; acquirer
  named by its later symbol; unresolved; exit_expected N;
- voluntary by ISIN; compulsory years later; liquidation from the reason and from the text; capital reduction
  with NCLT text; suspension; a cancelled suspension; priority and conflict;
- liquidation (all three lists) and compulsory circulars far later; voluntary and ordinary suspensions keep their
  windows; no match on or after a relisting; liquidation over voluntary within 60 trading days of it (and the
  boundary), voluntary kept when the liquidation comes later;
- MOVED_TO_SME; RELISTED / RELISTING; RIGHTS (incl. `-RE1`) / ETF with NOT_APPLICABLE;
- unmatched reference rows, and rows naming a symbol that never trades (counted only);
- distress hints, adjusted closes (a split is not distress), `last_close_adj` with a patched factor; every
  treatment;
- the listing kinds, start window, batch day and `--batch-min`;
- the loaders and `status_on`;
- known checks (patched);
- the report;
- inputs byte for byte unchanged, the three input locks, the own lock, stale inputs, not built, reference
  errors;
- the CLI codes.

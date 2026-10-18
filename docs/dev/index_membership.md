# Index membership (NIFTY indices) and AMFI categories

Point-in-time membership of eight NIFTY indices and the AMFI large / mid / small cap lists
(`backfill/equity/index_fetch.py`, `index_notices.py`, `index_membership.py`; CLI `index_membership.py`). Only
`fetch` uses the network. The build and the loaders are offline. Not part of `equity_update.py` yet.

## Sources (checked 2026-10-17)

| Source | Where | Format | Range |
|---|---|---|---|
| AMFI categorisation | `https://www.amfiindia.com/otherdata/categorisation-of-stocks` (the page embeds `{"<year>":[{"period","pdfUrl","excelUrl"}]}`; the old `/research-information/...` URL is a 404) | one xlsx per half year; file names have no pattern (`Avg. Market Capitalization ... -Jul-Dec 2017.xlsx`, `...sixmonthsended31Dec2022.xlsx`, `uploads/Average_Market_Capitalization_30_Jun2024_<hash>.xlsx`, `portal.amfiindia.com/spages/...30Jun2026.xlsx`) | 18 half years, H2-2017 to H1-2026 |
| NIFTY current lists | `https://www.niftyindices.com/IndexConstituent/<file>` (`fe.INDEX_FILES`) | CSV `Company Name, Industry, Symbol, Series, ISIN Code` | today |
| NIFTY changes | `https://www.niftyindices.com/press-release` (one page listing every notice) | PDF `/Press_Release/ind_prsDDMMYYYY[_n].pdf` (the name carries the announcement date) | 1998 on; fetched from 2014-10-01 (`CHANGES_FROM`) |
| Snapshots (a check only) | Wayback CDX for `niftyindices.com/IndexConstituent/<file>` and `www1.nseindia.com/content/indices/<file>` | the same CSV | about 2 to 15 captures per index, NIFTY 50 from 2016 |
| Methodology | `https://www.niftyindices.com/Methodology/Method_NIFTY_Equity_Indices.pdf` | | 100 = 50 + Next 50; 200 = 100 + Midcap 100; Smallcap 250 = 500 - 100 - Midcap 150 |

AMFI files always have the category column (100 Large Cap and 150 Mid Cap in every half year). They have no
reliable publication date: HTTP Last-Modified is 2025-08-19 on every old file (a re-upload). It is kept in
`input\amfi\sources.csv` for information only.

## fetch (`index_fetch.py`)
- `fetch [--amfi] [--nifty] [--changes] [--snapshots] [--force] [--no-browser]`. No part flag means all four.
- Files go to `<folder>\index_membership\input\` and are saved unchanged.
- The client is `SiteClient`, a `backfill.nse_ref.fetch.NseClient` that warms each site's own home page:
  - `GAP` 2 s between requests;
  - headless Chromium once a site blocks (Wayback answers 429 after a while).
- Fetched once (`--force` downloads again):
  - the AMFI xlsx (`safe_name` of the URL);
  - every notice PDF since `CHANGES_FROM` whose title passes `wanted_notice`. `KEEP_TITLE` minus `SKIP_TITLE`
    (fixed income, launches, IPO / SME only, ...), and anything "corporate" is always kept: 254 PDFs on
    2026-10-18;
  - each distinct Wayback capture (`<INDEX>_<UTC timestamp>.csv`).
- Fetched on every run, as a copy dated with today's IST date:
  - the two listing pages (`categorisation-of-stocks_<date>.html`, `press-release_<date>.html`); when one fails,
    the newest saved copy is used;
  - the current lists (`nifty_current\<INDEX>_<date>.csv`; the header must contain Symbol and ISIN Code);
  - the CDX answers.
- A failed file prints the page, the URL and the exact path to save it under by hand. The build reads hand-saved
  files the same way.
- Own `RunLock(index_membership\.lock)`.
- Exit codes: 0, 1 some failed, 2 arguments or lock.

## Notice parsing (`index_notices.py`)
`extract_stream(pdf)` gives the text lines and tables (pdfplumber `find_tables`) in reading order. Lines inside a
table are dropped. The build caches it in `index_membership\text_cache\<file>.json`, keyed by file size and
`STREAM_VERSION`.

`parse_notice(items, announced, title, file)` reads it as follows:
- **Effective date:** "become effective from DATE", else "effective from DATE":
  - in the introduction, the text before the first heading; else in the whole text; else the title's "w.e.f.".
  - Several dates means several sections. "A. ... effective from DATE" lists its indices (`section_dates`). An
    index of ours left without a date → `unresolved` (review `not_parsed`).
- **Headings:** `1) Nifty 500`, `(1) CNX Nifty Index`, `a) Nifty 50` are mapped through `ALIASES`, and anything
  else is ignored (Equal Weight, sectoral, strategy).
  - CNX Nifty / Nifty 50 → NIFTY 50; CNX Nifty Junior → NEXT 50; CNX 100 / 200 / 500 → 100 / 200 / 500.
  - **NIFTY MIDCAP 100** is the old CNX Midcap: Nifty Midcap 100, then Nifty Free Float Midcap 100 (2016-04 to
    2018-03), then Nifty Midcap 100 again (ind_prs05012018.pdf). "Nifty Full Midcap 100" was another index, ceased
    2018-04-02, and is not mapped.
- **Rows:** "being excluded" / "being included" set OUT / IN.
  - The stock tables under them give the symbols (the Symbol column; a table cut by a page break without its
    header uses the last column). Footnote marks are stripped.
  - A table row that pdfplumber left as text ("3 Cairn India Ltd. CAIRN") is read with `TEXT_ROW`.
- **"Index Name" tables:** one stock entering or leaving the listed indices.
  - Applied as IN / OUT when the text above names it: "(Symbol: TATAMTRDVR) shall be excluded from the following
    indices" (ind_prs23082024_1.pdf), and the notice is not a demerger.
  - A demerger's spun-off company (JIOFIN 2023, ...) is never applied (review `not_applied`; your decision,
    2026-10-17).
- **Revision tables** "Index Name | Security Name | Symbol | Remarks" (`is_revision_table`; 3 notices since 2015:
  ind_prs19032024, ind_prs23092024, ind_prs25092024):
  - The index name is carried down blank cells (also across a page break).
  - Remarks map through `REMARKS`: Inclusion → IN, Exclusion → OUT, Inclusion revoked → REVOKE_IN, Exclusion
    revoked → REVOKE_OUT. Any other remark on one of our indices → `odd_rows` (review `not_parsed`).
  - When a notice has several dates, "all indices listed above ... effective from DATE" (`LISTED_ABOVE`) gives
    the date of the indices without their own section date (ind_prs25092024: Midcap Select alone was postponed).
- `replaces`: the text says an earlier list "stands replaced" (ind_prs15092021.pdf).
- `no_text`: less than `NO_TEXT` characters, an image-only PDF (ind_prs23082021.pdf, the Sept 2021 review).

## build (`index_membership.py`)
`build [--folder] [--amfi-lag 5]`.

**Refusals:**
- `StoreBusy` (exit 2) while `NSE_CM_bhav\.lock` or `security_master\.lock` is held (`locks.held`);
- `NotBuilt` (1) without the master;
- `InputStale` (1) when the master's `build_info.json` is older than the newest year file;
- own lock busy → `LockBusy` (2).

**Calendar:** every date in the bhavcopy (any series).

**AMFI**
- `read_amfi` finds the header row (ISIN + Company name) and maps columns by name. The period end comes from the
  title ("six months ended 31 December 2018"), else the file name.
- Category comes from the column. Without one, it is derived from the rank (1-100 LARGE, 101-250 MID, 251+ SMALL);
  `category_from` in the report says which.
- The H2-2017 file stops numbering after rank 3452: the ranks continue in row order.
- `valid_from` = the first trading day on or after the 1st of the month after the half year, plus `--amfi-lag`
  trading days. `valid_to` = the trading day before the next list's `valid_from`; the latest stays open.
- `security_id`: the ISIN through the master's `isin_owner` (every ISIN a company ever had, so an old ISIN from
  before a split still maps). Else `security_id_for(NSE symbol, valid_from)`, then on the period end.
- Unmatched rows are counted per period. Only ranks 1-500 go to review (`amfi_unmatched`); lower ranks are mostly
  BSE-only companies.
- Two rows of one list on one security → `amfi_duplicate`.
- Not 100 / 150 → `amfi_count`.

**Changes** (`assemble_changes`)
- Parsed rows, deduplicated per file.
- **Revoked:** a REVOKE_IN / REVOKE_OUT row cancels the earlier-announced parsed IN / OUT of the same index, symbol
  and effective date. The cancelled row gets status `revoked`; the revoke row gets status `revocation` (never
  walked). A revocation matching nothing → review `conflict`. (IREDA's inclusion, March 2024; Vodafone Idea's
  exclusion and Central Bank's moves, Sept 2024.)
- **Superseded:** the same index + symbol + action announced again before the first one's effective date → the
  earlier row is `superseded` (Yes Bank 2020: 18 Feb for 27 Mar, then 16 Mar for 19 Mar).
- A `replaces` notice supersedes the earlier same-index, same-date rows of the indices it lists.
- `input\manual_changes.csv` (`MANUAL_COLUMNS`; created once with comments and an example, never overwritten;
  `#` lines ignored):
  - IN / OUT rows are added.
  - IGNORE drops the parsed rows of that index, date and symbol; with symbol and isin blank, every parsed row of
    that index and date.
  - A manual row contradicting a parsed one (same index, date and symbol, other action) wins → review `conflict`.
  - The same as a parsed row → `duplicate`.
  - A bad row or an IGNORE matching nothing → `conflict`.
- **Mapping:** the ISIN when given, else the symbol on the announcement date, the trading day before the effective
  date, the effective date, else the only security that ever had it. Else `unmapped` (review `unmapped_change`).

**Membership** (`walk`, per index)
- Start from the newest `nifty_current\<INDEX>_<date>.csv`, mapped by ISIN then by symbol on the list date
  (`unmapped_current`: REITs / InvITs are not in the equity master, nor are placeholder rows like DUMMYHEG).
- Changes effective after the list date are `pending`.
- Walk the effective dates back from the list date to the coverage start, undoing each date:
  - members before d = members on d - IN + OUT;
  - an IN whose security is not a member, or an OUT whose security still is, is `skipped` (review
    `undo_mismatch`).
- Periods: `from_date` is the effective date of the IN (or the coverage start); `to_date` is the trading day before
  the OUT's effective date (open while in the current list).
- `symbol` / `isin` are the master's on `to_date`. `source` is "<start> -> <end>" (file, `manual_changes.csv`,
  `coverage start`, `current list <date>`).
- Changes effective on or before the coverage start are `before coverage`.

**Coverage**
- From the data start (2015-01-01): NIFTY 50, Next 50, 100, 200, 500, Midcap 100.
- From 2016-04-01: Midcap 150 and Smallcap 250 (first published then, ind_prs22022016_1.pdf; `INDEX_START`).
- To the last trading day on or before the current list's date.
- An index without a current list is not covered.

**Checks** (reported; nothing is changed to make them pass)
- **Size:** members per trading day = `SIZES`. Days off are grouped into ranges (`count_mismatch`), with the share
  of exact days.
- **Composites** (`COMPOSITES`):
  - NIFTY 100 = NIFTY 50 + NEXT 50;
  - NIFTY 200 = NIFTY 100 + MIDCAP 100, from 2018-04-02 (before that it used the Full Midcap 100);
  - NIFTY 500 = NIFTY 100 + MIDCAP 150 + SMALLCAP 250, from 2016-04-01.
  - The share of days where the parts are disjoint and their union is the whole, with the first ranges off.
- **Not trading:** a member without a bhavcopy row (master series) for more than `NOT_TRADING_DAYS` (20) trading
  days while in the index → `member_not_trading`.
- **Snapshots:** the rebuilt members on the capture's trading day (UTC + 5:30, on or before) vs the archived list.
  A difference within `SNAPSHOT_TIMING` (5) trading days of an applied change of that stock in that index is
  `timing`; any other is `snapshot_mismatch`. Your decision, 2026-10-17: copies may be a few days old or new.

**Output** `<folder>\index_membership\` (atomic, rebuilt in full):
- `nifty_membership.parquet/.csv` (`MEMBERSHIP_COLUMNS`);
- `nifty_changes.csv` (`CHANGE_COLUMNS`, `status`: applied / superseded / ignored / duplicate / unmapped / skipped
  / pending / before coverage);
- `amfi.parquet/.csv` (`AMFI_COLUMNS`);
- `review.csv` (`REVIEW_COLUMNS`, `REVIEW_KINDS`);
- `calendar.csv`, `build_report.txt`;
- `build_info.json` (parameters, inputs, coverage, AMFI periods, reviews per year, size / composite results).

## reliable_from (per index)
**Expected deviations** (`EXPECTED_RULES`, also in the report and build_info):
- **Additional share class:** each member whose symbol ends in `DVR` (`ADDITIONAL_CLASS`; the Tata Motors DVR)
  adds 1 to the expected size.
- **REIT / InvIT:** each current-list row whose company matches `REIT_RX` (not in the equity master, so never a
  member here) takes 1 from the expected size. It counts from the effective date of its (unmapped) IN row in that
  index, else from the coverage start.
- Nothing else is expected; e.g. a DUMMYHEG-style placeholder day counts as off.

**The rule** (`reliable_from()`):
- The size check reports the raw share (count = size) and the adjusted share (count = expected size).
  `count_mismatch` review rows are the days off the expected size.
- `reliable_from` = the earliest trading day d of the coverage that comes after the latest compared snapshot with
  a real mismatch (timing differences allowed), and with the index at its expected size on at least
  `RELIABLE_SHARE` (98%) of the trading days from d to the coverage end. None if there is no such day.
- Stored in build_info `coverage[index]` (`reliable_from`, `reliable_why`, `snapshots_after`). The report lists it
  per index with the rules.

## Loaders (cached until the files change; `NotBuilt` when missing)
- `members(index, on_date, allow_unreliable=False)` → sorted security_ids. A date before `reliable_from` (or any
  date when it is None) → `OutOfCoverage`, unless `allow_unreliable=True`. A date outside first..last always
  raises. CLI: `members INDEX DATE --allow-unreliable` (prints a note).
- `is_member(security_id, index, on_date, allow_unreliable=False)`.
- `coverage(index)` → `Coverage(first, last, reliable_from)` (a namedtuple).
- `amfi_category(security_id, on_date)` → (LARGE / MID / SMALL or None, period).
- `amfi_list(on_date, category=None)` → the rows of the list valid then, by rank.
- A date outside the coverage (AMFI: before the first `valid_from` or after the last bhavcopy day) →
  `OutOfCoverage` (a `ValueError`). An unknown index → `KeyError` (`index_name` accepts "nifty50", "NIFTY_50").

## CLI
- `fetch | build | members INDEX DATE | show SYMBOL|ISIN | check`.
- `--folder` must be absolute.
- Exit codes: 0; 1 not built / stale / some downloads failed / out of coverage / show found nothing / check
  failed; 2 argparse, lock busy, store busy.

## Known cases (`KNOWN_*`, report and `check`; a miss is reported, never fixed by a rule)
- RELIANCE in NIFTY 50 for the whole coverage, and LARGE in every AMFI half year.
- HDFC in NIFTY 50 until 2023-07-12, and LTIM in from 2023-07-13 (ind_prs04072023.pdf).
- 18 AMFI half years, each 100 / 150.
- A March and a September review notice for every year, with whether NIFTY 50 changed.
- One NIFTY 50 inclusion of a stock not in NIFTY 100 the day before, or AMFI MID then.

## Limits (real data, 2026-10-18)
- **Real size deviations:**
  - The Tata Motors DVR was an additional security: NIFTY 50 had 51 from 2016-04-01 to 2017-09-28, and NIFTY 100
    / Next 50 / 200 / 500 one more while it was in them (exit 2024-08-30).
  - REITs / InvITs entered NIFTY 500 / Midcap 150 / Smallcap 250 on 2026-09-30, and are not in the equity master.
- **2020-03-27:** the replacements announced for that date (18 Feb, 12 Mar, 19 Mar 2020) were declared "null and
  void" (ind_prs13052020.pdf), except NIFTY 50, and redone for 2020-06-26. The parser does not read that, so
  `manual_changes.csv` has 7 IGNORE rows (every index but NIFTY 50, 2020-03-27, blank symbol; added 2026-10-18).
- **Backup:** `reference/manual_index_changes.csv` in the repo is the backup of the data folder's
  `manual_changes.csv` (force-added: `*.csv` is gitignored). The build reads only the data copy. After editing it,
  copy it to the repo and commit.
- **ind_prs23082021.pdf** (the Sept 2021 review) is an image-only PDF (no NIFTY 50 section).
  - Its Next 50 / 100 / 200 / Midcap 100 rows were read by eye into
    `<folder>\index_membership\input\draft_rows_2021-09.csv`, which the build never reads. They are to be checked,
    then copied to `manual_changes.csv`.
  - Its 500 / Midcap 150 / Smallcap 250 lists were replaced by ind_prs15092021.pdf (parsed), so they are not
    drafted.
- **ind_prs21022018.pdf**, the April 2018 one-time Midcap 100 recomposition, sits under a sentence heading ("11)
  IISL had announced revision of criteria ...") with the index name on a bullet line. It is the only notice like
  that since 2015, so it isn't parsed: its 92 rows are drafted in `input\draft_rows_2018-04.csv`.
- A missed change makes every earlier date of that index wrong for the stocks involved. The snapshots, the size
  check and `undo_mismatch` show where; fix it with manual rows.

## Tests
`tests/test_index_membership.py` uses a synthetic store (an N1 filler row on every day), `sm.build` on it, and
notices given as stream items (`im.notice_stream` patched). It also uses saved real samples in
`tests/fixtures/index_membership/`:
- two notice PDFs, `notices.json` (the streams of eight more), 40 AMFI rows with and without the category column,
  and 10 rows of the NIFTY 50 list.
- No network: `requests` and the browser are refused. `config.load_env` is stubbed.

Covers:
- AMFI parsing, the period from the title or file name, rank continuation;
- the real 2015 (CNX names), 2023 (HDFC → LTIM), 2016 multi-date, 2020 Yes Bank, 2016 restructure, lettered-heading,
  demerger, DVR, "stands replaced" and image notices;
- the text-row fallback, the aliases, the backward walk over two reviews and an ad hoc change;
- the point-in-time loaders and out-of-range errors;
- AMFI valid_from / valid_to / lag, ISIN mapping across a split, symbol mapping, count and duplicate reviews;
- superseded rows, a replaced list, undo mismatches;
- manual IN / OUT / IGNORE (with and without a symbol), conflicts, duplicates, bad rows, the manual file kept;
- pending / unmapped / not applied;
- size, composite, not-trading and snapshot checks;
- the report and build_info, the known checks (patched);
- inputs unchanged, both input locks, the own lock, the stale master, not built;
- CLI exit codes;
- fetch: saved / skipped / forced, current lists dated each run, the blocked message, the saved page fallback, the
  title filter, the lock, `SiteClient` warming.

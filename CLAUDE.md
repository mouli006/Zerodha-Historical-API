# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.
It is kept short on purpose: the detail lives in `docs/dev/` (see the index at the end).

## What this app is
A local Flask app ("Kite Backfill", one machine) that:
- downloads 1-minute F&O candles with OI for every live NIFTY and SENSEX future and option from Kite (NFO/BFO) or Dhan, saved in the Accelpix layout, and checks each day against the NSE/BSE F&O bhavcopy;
- keeps an F&O raw store (with / without zero-volume contracts) and publishes it to a parquet backtest store;
- keeps an NSE equity (CM) bhavcopy store since 2015 and NSE delisting / suspension / merger reference lists (no broker login);
- bulk-downloads Kite historical candles for NSE equity and indices, with a dashboard showing every job live.

## Golden rules
- Tests never touch the network (Kite, Dhan, NSE, BSE) and never read or write the real `.env`.
- All dates and times are IST (`ZoneInfo("Asia/Kolkata")`), never local time. Never default a run date to today.
- Write files atomically (temp file, then replace); a stopped run must never leave a half file.
- One lock per data root (`.lock`, OS lock, dropped when the process dies); never PID files.
- No cron, scheduled tasks or alerts: every job and check is started by hand.
- Anything that moves or deletes data: dry run first, show it, wait for the user's OK, then `--apply`.
- Never delete user data: archive instead (`fno_store.py clear` moves to an archive; duplicates are never replaced).
- Work on a branch; never push unless asked. Never stage `.env` or any value from it; never print or log a token or secret.
- F&O and equity code stay separate: new equity code goes in `backfill/equity/`, takes its lock from `backfill/locks.py` and never imports F&O modules (`tests/test_equity_imports.py`).
- No behaviour change without tests; tests are never removed or weakened.
- Keep CLAUDE.md short: new feature detail goes in `docs/dev/`, CLAUDE.md gets at most one line per feature.
- Keep `README.md` (the user-facing guide) in sync when features change.

## Commands
- Install: `pip install -r requirements.txt` (pinned), then `playwright install chromium` (fallback browser for `nse_reference.py`).
- Run: `python app.py` -> http://127.0.0.1:5000. Debug is off: restart after any `.py` or template change (a restart stops a running download; Resume is safe).
- Tests: `pytest` (`pytest.ini` sets `pythonpath = .`). No linter.
- `nifty_daily.py`: the daily F&O download (one date, `--missing`, `--refetch-no-trade`, inspect, `--rename-folders`, `--update-day-count`). See docs/dev/fno_download.md.
- `nifty_bhav.py`: the F&O bhavcopy check and its exports. See docs/dev/fno_bhavcopy_check.md.
- `fno_store.py`: the F&O stores (move-raw, split-stores, derive-without, compare-variants, publish, clear, restore, import). See docs/dev/fno_store.md.
- `cm_bhav.py`: the NSE equity bhavcopy store. See docs/dev/equity_bhavcopy.md.
- `cm_bhav.py --special [--apply]`: the weekend sessions of `manual_special_sessions.csv` (dry run asks NSE, writes nothing). See docs/dev/special_sessions.md.
- `nse_reference.py`: the NSE delisting / suspension / merger reference. See docs/dev/nse_reference.md.
- `security_master.py`: build / show / check the security master (one `security_id` per company). See docs/dev/security_master.md.
- `adjustments.py`: build / show / check split / bonus factors (price move confirmed by ISIN change or NSE CSV), fetch-actions (NSE corporate actions). See docs/dev/adjustments.md.
- `exits.py`: build / show / check the exit list (status, exit reason, suggested treatment) and listing kinds. See docs/dev/exits.md.
- `equity_update.py`: the daily equity update, runs cm_bhav / security_master / adjustments / exits / index_membership (and nse_reference, index fetch) in order. See docs/dev/equity_update.md.
- `index_membership.py`: fetch (network) / build / members / show / check NIFTY index membership and AMFI categories, point in time. See docs/dev/index_membership.md.
- `equity_minute.py`: Kite 1-minute / EOD plan: tokens / probe (Kite network) / gaps / show -> the vendor gap list. See docs/dev/equity_minute_plan.md.
- `equity_minute.py download / update / status / checks`: phase 2, the Kite 1-minute history (2 workers, resumable) and evening update; `load_minute` loader. See docs/dev/equity_minute_download.md.
- `equity_minute.py update` stores only finished days (target day T = finished by 01:00 IST next day; unfinished days deferred, capped); `repair [--apply]` re-fetches day candles stored before that. See docs/dev/equity_minute_finished_days.md.
- `equity_minute.py retoken [--apply]`: re-match failed stocks (price-checked tokens, NO_KITE_DATA), re-probe probe-empty stocks (PROBE_RECOVERED); `load_minute(clean=True)` drops zero bars. See docs/dev/equity_minute_download.md.
- `equity_minute.py fix-negative-volume [--apply]`: Kite's garbage minute volumes count as 0 (`NEGATIVE_VOLUME`, `neg_volume_bars`); fixes the days.parquet rows stored before (bars never changed). See docs/dev/equity_minute_negative_volume.md.
- `equity_minute.py fix-zero-padding [--apply]`: re-aggregates `days.parquet` rows that still count Kite's all-zero padding bars (offline; conversion columns, volume and bars never written; drift held and listed). See docs/dev/equity_minute_zero_padding.md.
- `icici_minute.py plan / download / status / checks / coverage / show`: ICICI Breeze 1-minute data for the Kite gaps (own store `equity_1min_icici/`; `.env` BREEZE_API_KEY + daily BREEZE_SESSION_TOKEN; never orders). See docs/dev/equity_icici_source.md.
- Never run a CLI against real Kite or Dhan unless the user asks.

## Code map
Shared
- `config.py`: paths, `.env` helpers (`load_env`, `save_env`, `credentials`, ...) and the setting defaults.
- `backfill/job.py`: in-memory job state (`JOB`, `job_begin`, `STOP`).
- `backfill/fetch.py`: request chunks, `RateLimiter`, `fetch_chunk` with retries.
- `backfill/locks.py`: `RunLock` / `LockBusy` for all non-F&O code (re-exported from `fno.py`).

F&O
- `backfill/fno.py`: the daily job (`run_daily`, `run_missing`, window, day folders, lock).
- `backfill/dhan_api.py`: `DhanClient`, a Kite-shaped DhanHQ v2 client.
- `backfill/bhav.py` / `bhav_export.py`: the bhavcopy check and its CSV/Excel exports.
- `backfill/fno_days.py` / `fno_rename.py`: day-count marker, `days_index.csv`, folder renames.
- `backfill/fno_loader.py`: reads raw F&O days back.
- `backfill/fno_store.py` / `fno_derive.py` / `fno_clear.py` / `fno_import.py`: move-raw / split-stores, without-zero-volume copies, clear/restore, import.
- `backfill/fno_publish.py` / `fno_backtest.py`: raw -> backtest store, and its reader.
- CLIs: `nifty_daily.py`, `nifty_bhav.py`, `fno_store.py`.

Equity
- `backfill/cm_bhav.py` (+ `cm_bhav.py`): NSE equity bhavcopy store.
- `backfill/nse_ref/` (+ `nse_reference.py`): NSE reference lists.
- `backfill/equity/`: home for new equity modules (security master, split/bonus factors, exits, index membership).
- `backfill/equity/special_sessions.py`: the special-sessions calendar (`load_special_sessions`, `session_kind`, `is_special`).
- `backfill/equity/security_master.py` (+ `security_master.py`): security master from the bhavcopy parquet, and its loaders.
- `backfill/equity/adjustments.py` / `corp_actions.py` (+ `adjustments.py`): split / bonus factors, `factor_on`, `adjusted_history`; NSE corporate-actions download.
- `backfill/equity/exits.py` (+ `exits.py`): exit list, listing kinds, `exit_info`, `exits_between`, `status_on`, `listing_info`.
- `backfill/equity/update.py` (+ `equity_update.py`): runs the equity CLIs as subprocesses (`run_step`), up-to-date skips, log.
- `backfill/equity/index_fetch.py` / `index_notices.py` / `index_membership.py` (+ `index_membership.py`): downloads, notice / AMFI parsers, build, `members`, `amfi_category`.
- `backfill/equity/minute_plan.py` (+ `equity_minute.py`): Kite tokens, availability probe, adjustment periods, `kite_gaps.csv`.
- `backfill/equity/minute_download.py` / `minute_store.py`: 1-minute download, checks, update, status; store layout and `load_minute`.
- `backfill/equity/minute_zeropad.py` (+ `equity_minute.py fix-zero-padding`): rows built with Kite's zero padding bars, fixed.
- `backfill/equity/icici_client.py` / `icici_plan.py` / `icici_download.py` / `icici_store.py` (+ `icici_minute.py`): Breeze REST calls, work list, download + conversion + checks, `load_minute_icici`.
- `backfill/instruments.py`, `storage.py`, `runner.py`: the Kite bulk candle download.

Web
- `app.py`: creates the app and registers the blueprints.
- `auth.py`: Kite sign-in (`/`, `/login`, `/callback`, `/logout`).
- `dhan.py`: Dhan sign-in and the Dhan F&O page.
- `dashboard.py`: `/success`, `/download/*`, `/fno/*`, `/browse-folder`.
- `templates/`: React 18 + htm from CDNs, no build step (`base.html`, `login.html`, `dhan_login.html`, `success.html`).

## Data folders and `.env` keys
`.env` is gitignored and read/written only by `config.load_env()` / `save_env()` (no python-dotenv). `.env.example` lists the keys. Names only:
- Folders: `KITE_DATA_DIR` (bulk download and default F&O folder), `FNO_RAW_KITE`, `FNO_RAW_DHAN` (bases of the with / without zero-volume raw stores), `FNO_BACKTEST_DIR` (backtest store), `EQUITY_DATA_DIR` (equity bhavcopy + NSE reference; else `KITE_DATA_DIR`, else `./data`).
- Kite: `KITE_API_KEY`, `KITE_API_SECRET`, `KITE_ACCESS_TOKEN`. Dhan: `DHAN_CLIENT_ID`, `DHAN_ACCESS_TOKEN`, `DHAN_TOKEN_EXPIRY`. Flask: `FLASK_SECRET_KEY` (generated on first import).
- Settings (ON/OFF unless noted): `KEEP_ZERO_VOLUME`, `TRIM_TO_BHAV`, `DERIVE_WITHOUT_ZERO_VOLUME`, `WEEKLY_EXPIRY_WEEKDAY`, `WEEKLY_EXPIRY_WEEKDAY_<UNDERLYING>` (MON..SUN).

## Docs index
| Before working on | Read |
|---|---|
| code layout, templates/React, config, repository rules | docs/dev/architecture.md |
| tests, fakes, conftest fixtures, adding a test | docs/dev/testing.md |
| Kite sign-in, dashboard planner/live card, job state | docs/dev/dashboard.md |
| Kite bulk equity/index candle download | docs/dev/kite_bulk_download.md |
| F&O daily download (window, expiries, holidays, trim, zero volume, day folders, markers, lock) | docs/dev/fno_download.md |
| F&O bhavcopy check (statuses, severity, routes, exports, BhavCard) | docs/dev/fno_bhavcopy_check.md |
| Dhan sign-in, Dhan F&O page, `DhanClient` | docs/dev/dhan.md |
| F&O stores: with/without, derive, move-raw, publish, backtest, clear/restore, import | docs/dev/fno_store.md |
| NSE equity bhavcopy store (`cm_bhav`) | docs/dev/equity_bhavcopy.md |
| special sessions (Muhurat, budget weekends, `cm_bhav.py --special`, what each tool does on a Sunday) | docs/dev/special_sessions.md |
| NSE delisting / suspension / merger reference | docs/dev/nse_reference.md |
| security master (linking rules, events, review, loaders) | docs/dev/security_master.md |
| split / bonus factors (detection, confirmation, manual overrides, loaders) | docs/dev/adjustments.md |
| exit list and listing kinds (reasons, priority, treatments, loaders) | docs/dev/exits.md |
| daily equity update (steps, up to date, warnings vs. failures, log) | docs/dev/equity_update.md |
| index membership and AMFI categories (sources, notice parsing, walk, checks, loaders, limits) | docs/dev/index_membership.md |
| Kite 1-minute / EOD plan (Part 0 findings, tokens, probe, adjustment periods, gap list) | docs/dev/equity_minute_plan.md |
| Kite 1-minute download (Part 0 findings, scales, checks, workers, update, loader) | docs/dev/equity_minute_download.md |
| 1-minute finished days (target day, safety net, deferral cap, `repair`) | docs/dev/equity_minute_finished_days.md |
| Kite 1-minute bars with a negative volume (evidence, NEGATIVE_VOLUME, `fix-negative-volume`) | docs/dev/equity_minute_negative_volume.md |
| Kite 1-minute rows built with all-zero padding bars (evidence, PADDING / OTHER, hold rules, `fix-zero-padding`) | docs/dev/equity_minute_zero_padding.md |
| ICICI Breeze 1-minute source (endpoints, limits, work list, conversion evidence, checks, loader) | docs/dev/equity_icici_source.md |

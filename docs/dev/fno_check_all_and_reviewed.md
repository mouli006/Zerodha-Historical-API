# Check all four, reviewed days, bhavcopy not published

`backfill/bhav_review.py`, used by `nifty_bhav.py` and `dashboard.py`. It adds three things to the F&O bhavcopy check (docs/dev/fno_bhavcopy_check.md) without changing a check result.

Why: Kite has accepted VOLUME_DIFFs almost every day (P5), so the red banner showed on nearly every day and a real problem looked the same (P52). An F&O bhavcopy that is never published (404) left its day pending for ever, asked on every run (P61). The next likely case is the Muhurat session on Sun 8 Nov 2026.

## What stays the same
- `severity()` (D19), `list_days()`, `bhav.banners()`, `unchecked_days()` and `check_saved()` are unchanged. No write goes inside a day folder (A2). `summary.csv`, `bhav_check.json`, `days_index.csv` and the backtest store are never touched. `publish` and `best_source` behave as before (`publish` reads `list_days`, which ignores the sidecar).
- The one change in `bhav.py`: `run_bhav` also appends a `BhavError` from a download (fetch failed, wrong day, no rows of the underlying) to `<root>/bhavcopy.log` before it re-raises. A 404 was already logged there, so the log's last line for a day is now its last fetch. `--bhav-file` errors are not logged.

## Sidecar `<F&O root>/bhav_review.json`
- There is one file per root (`fno.fno_root(save folder, underlying, source)`, next to `bhavcopy.log` and `.lock`), for example `<FNO_RAW_KITE>\with_zero_volume\NIFTY_FnO_1min\bhav_review.json`. It is one per root because the lock, the checks and `bhavcopy.log` are all per root, and the with / without stores are checked on their own.
- Format: JSON `{"version": 1, "underlying", "source", "events": [...]}`, events oldest first, only ever appended (an undo is a new event).
  - `review`: `date`, `folder`, `at` (IST), `note`, `severity`, `checked_at`, `fingerprint` (the check's whole `counts` dict at review time);
  - `unreview`: `date`, `folder`, `at`, `note`;
  - `no_bhav`: `date`, `folder`, `at`, `note`, `fetch_line` (the `bhavcopy.log` line that allowed it);
  - `undo_no_bhav`: `date`, `folder`, `at`, `note`.
- Each write is atomic (`append_event`: `tempfile.mkstemp` in the same folder, then `os.replace`) and made by the holder of the root's F&O lock. A failed write leaves the old file whole and no temp file behind. The tool never deletes the sidecar.
- The current state is the last event of a date per family (`review_state` / `no_bhav_state`):
  - review: `active` (same folder, status `diffs`, counts == fingerprint), `void` (counts changed or no longer diffs), `superseded` (another day folder, or the day is gone), `undone`;
  - no-bhav: `active` (same folder, still pending), `superseded` (checked since, for example with `--bhav-file`, or another folder or gone), `undone`.
  - Both kinds apply only to the day folder they were made on. A clear + re-download or an import creates a new `dl-` name, so the mark lapses.
- Store tools (all leave the file alone; none deletes or overwrites it):
  - `fno_store clear` keeps it in place, since it is not a day folder, marker or `CLEAR_FILES` entry, also with `--delete`;
  - `restore` never sees it, since it is not in the manifest;
  - `import` keeps it, and ignores one in the import source;
  - `move-raw` / `split-stores` move day folders only, so the file stays in the old root and moved days show red / pending in their new root;
  - `derive-without` ignores it, and the without store keeps its own.

## Check all four
- `nifty_bhav.py --missing-all [--keep-zero-volume | --no-keep-zero-volume]`:
  - it runs `bhav_review.SETS` in this fixed order: NIFTY kite, NIFTY dhan, SENSEX kite, SENSEX dhan, on `config.default_fno_folder(source, keep_zero)` (`default_sets`);
  - per set it uses `check_set`: `pending_days` (marked days left out), the set's lock, `run_bhav` per day;
  - a failing set (bad folder from `check_folder`, `LockBusy`, `DayFolderError`) is `failed` with code 1, and the next set still runs;
  - it prints `table(rows)`: Underlying, Source, Checked, Still pending (404 this run), No-bhav marked, Failed (days or the set), Red days (checked this run, `day_severity` major);
  - the exit code is `worst(codes)`: 1 (differences, errors, not run) over 4 (not available) over 0. That is the same meaning as `--missing`, and a marked day no longer gives a 4;
  - it refuses `--folder`, `--source`, `--underlying` and `--bhav-file` (`--underlying` / `--source` now default to None and resolve to NIFTY / kite for the other modes).
- `POST /fno/bhav-check-all` (`keep_zero=1|0`):
  - needs a Kite session or a connected Dhan, and is refused while a download job runs;
  - it takes **all four locks first** and refuses (releasing them) if any is busy;
  - then it runs `check_all` with those locks (synchronous, like Check all pending) and returns `{rows, code}`.
  - UI: "Check all pending (all four)" above the grid on both pages. It toasts one line per set and reloads the list. The per-card button stays.

## Mark reviewed
- `review(data_root, underlying, source, day, note)` / `unreview(...)` return `(message, error)`; the caller holds the lock.
- Refused for:
  - a day that is not saved, pending, or OK;
  - an already active review;
  - **Missing > 0**: "N contracts are missing in Kite's files: re-download them while the contract is still live (B10)". Missing contracts must be fixed, not hidden.
- Red and amber days can be reviewed. `unreview` needs a `review` as the last review event.
- `bhav_review.banners(days, today, events)` = `bhav.banners` without the actively reviewed `check` / `minor` days and the actively marked `pending_old` days. With no records it gives the same result.
- CLI: `--review DATE --underlying U --source S [--note] [--folder] [--keep-zero-volume]` and `--unreview`. Both need `--underlying` and `--source`, take the root's lock, and exit 0 when written, 1 when refused (the reason goes to stderr).
- Routes:
  - `GET /fno/bhav-status` keeps its shape, with the banners filtered as above;
  - `GET /fno/bhav-marks` (reads only) returns `{reviewed, no_bhav, days: {date: {review, review_note, review_why (diffs days), no_bhav, no_bhav_note, no_bhav_why (pending days)}}}`;
  - `POST /fno/bhav-review` (`action=review|unreview`, `note`) goes through `locked_fno`.
- UI:
  - the diff panel has a review bar: a note field + "Mark reviewed", "Unreview" when reviewed, the refusal reason instead of the button, and "An earlier review no longer applies" when void;
  - a grey `reviewed` pill in the status cell;
  - a grey "Reviewed ..." banner.

## Bhavcopy not published
- `no_bhav(underlying, day, roots, today, note, undo)` goes over every root of `store_roots(underlying, env, also)`:
  - Kite and Dhan, each the with and without store when `FNO_RAW_*` is set (else the one folder), plus `also` (the dashboard's folder), with duplicates dropped;
  - each root takes its own lock.
- A root qualifies (`no_bhav_why`) when:
  - the day is a past day (< today IST: today's file may still come);
  - it is saved and still pending;
  - it is not marked yet;
  - its last `bhavcopy.log` line contains `has no F&O bhavcopy for`, the `BhavNotAvailable` wording for NSE's 404 and for BSE's page.
- Other roots are logged as skipped with the reason. The return value is the number of roots changed (CLI exit 0 if > 0, else 1). A derived without-store day has no log line of its own, so it is skipped: run `--missing --no-keep-zero-volume` there first. `undo` adds `undo_no_bhav` where the last event is `no_bhav`.
- Effect:
  - `pending_days()` leaves the day out of `--missing`, `--missing-all`, `/fno/bhav-check missing=1` and the two buttons;
  - the yellow banner drops it and a grey "No bhavcopy (marked)" banner lists it.
  - The day stays unchecked. No result is invented, and `publish` handles it as before (unchecked).
  - `DATE --bhav-file PATH` and "Check now" still check it; the mark is then `superseded`.
- Route: `POST /fno/bhav-no-bhav` (`action=mark|undo`, `note`). It is refused while a job runs. UI: on a pending row, "Bhavcopy not published" (disabled, with the reason as the tooltip, unless allowed), or a grey `no bhavcopy` pill + Undo.

## Listing
`nifty_bhav.py --reviewed-list` reads only and takes no lock. It prints every event of every root of both underlyings (date, underlying, source, kind, state, at, root, note). The state is the current one on a date's latest event, `replaced` on earlier ones.

## Tests
`tests/test_bhav_review.py`: synthetic days in `tmp_path`, `fetch_bhav` stubbed, `load_env` stubbed, network blocked. It covers:
- `severity()` unchanged;
- the sidecar path and the atomic write;
- review: red and amber hidden, refused with Missing > 0, void after a real `check_saved` with other counts, kept with the same counts, unreview with history, lapsed on a new folder name;
- no-bhav: across four stores with a skip, refusals (today, checked, nothing logged, a later failed fetch logged by `run_bhav`), skipped by `--missing`, `--bhav-file` superseding it, undo;
- `--missing-all`: order, table, exit codes, continue after a busy lock or bad folder, marked / failed / red counts;
- CLI argument refusals, CLI review / no-bhav / list, the lock;
- the routes (review, marks, no-bhav, `missing=1`, check-all with a busy lock, a running job and all locks released) and the page;
- publish identical with and without records (raw-file mtimes in the manifest aside);
- clear + restore keeping the sidecar byte for byte, outside the manifest.

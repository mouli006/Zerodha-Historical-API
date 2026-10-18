# Daily equity update (`equity_update.py`)

One command for the equity routine (`backfill/equity/update.py`, CLI `equity_update.py`). It only runs the
existing CLIs, in order, each as a subprocess with the same Python (`sys.executable`, no shell, cwd = repo root,
`PYTHONIOENCODING=utf-8`). It has no rules or data logic of its own: every decision is the step's. It never
touches F&O data or F&O commands, and no broker (the network steps go to NSE only).

```
python equity_update.py [--folder PATH] [--offline] [--with-reference] [--force] [--from-step NAME] [--dry-run]
```

## Steps

| # | Name | Commands | Network | Skipped when |
|---|---|---|---|---|
| 1 | `reference` | `nse_reference.py` | yes | always unless `--with-reference`; `--offline` |
| 2 | `bhavcopy` | `cm_bhav.py --missing` | yes | `--offline` |
| 3 | `master` | `security_master.py build`, `security_master.py check` | no | up to date |
| 4 | `actions` | `adjustments.py fetch-actions` | yes | `--offline` |
| 5 | `adjustments` | `adjustments.py build`, `adjustments.py check` | no | up to date |
| 6 | `exits` | `exits.py build`, `exits.py check` | no | up to date |
| 7 | `index_fetch` | `index_membership.py fetch` | yes (AMFI, niftyindices, web.archive.org) | always unless `--with-reference`; `--offline` |
| 8 | `index` | `index_membership.py build`, `index_membership.py check` | no | up to date |

`--folder` (absolute, like the other CLIs) is passed to every command when given. Lock, log and freshness use it,
else `config.equity_data_dir()`.

## Options
- `--offline`: skip 1, 2, 4 and 7 (even with `--with-reference`) and rebuild from what is saved.
- `--with-reference`: also run the monthly downloads: step 1 first and step 7 (`WITH_REFERENCE`).
- `--force`: run every build even when up to date.
- `--from-step NAME`: start there. Earlier steps show SKIPPED "before --from-step". An unknown name is an
  argparse error (2).
- `--dry-run`: print each step, its commands, and whether it would run, be skipped or is up to date now. It runs
  nothing, takes no lock and writes no log.

## Up to date (steps 3, 5, 6, 8)
A build is skipped, together with its check, when its `build_info.json` is at least as new (mtime) as every input
file that exists (`inputs_of()`). It is evaluated when the step is reached, so a master rebuilt in this run makes
the adjustments, exits and index stale. A missing `build_info.json` always runs.

| Build | Output | Inputs compared |
|---|---|---|
| master | `security_master\build_info.json` | every `NSE_CM_bhav\<YYYY>.parquet` |
| adjustments | `adjustments\build_info.json` | `security_master\build_info.json`, every `adjustments\input\*.csv`, `adjustments\manual_factors.csv`, every year file |
| exits | `exits\build_info.json` | `security_master\build_info.json`, `adjustments\build_info.json`, the six lists in `NSE_CM_bhav\reference\` (`exits.REFERENCE_FILES`, incl. `manual_mergers.csv`), every year file |
| index | `index_membership\build_info.json` | `security_master\build_info.json`, every year file, every file under `index_membership\input\` at any depth (`amfi\`, `nifty_current\`, `nifty_changes\`, `snapshots\`, `manual_changes.csv`, the draft files) |

These are the comparisons the builds themselves use for their stale refusals, plus their input folders.
- `fetch-actions` always downloads the current year again and rewrites `corporate_actions_<year>.csv`, so an
  online run normally rebuilds adjustments and exits.
- `--with-reference` rewrites the reference lists, so exits rebuild too.
- `index_fetch` saves dated copies of the current constituent lists (and the listing pages, the Wayback indexes) on
  every run, so a `--with-reference` run always rebuilds the index membership. Without it, `index` rebuilds only
  after a new master build, a new bhavcopy year file, or an edit under `index_membership\input\` (e.g.
  `manual_changes.csv`).

## Warnings vs. failures
The exit codes are the CLIs' own (checked 2026-10-19):
- `nse_reference.py`:
  - 0 done; 404 links are only listed.
  - 1 a download failed (files to save by hand are listed), or argparse / folder errors. It never exits 2.
- `cm_bhav.py`:
  - 0 done; today pending is fine.
  - 1 some days failed.
  - 2 arguments, folder or lock.
- `security_master.py`, `adjustments.py` (build / check / fetch-actions) and `exits.py`:
  - 0 done.
  - 1 not run / stale / bad input / check failed / a year not fetched.
  - 2 arguments or a lock busy.
- `index_membership.py`:
  - fetch: 0 done; 1 some files failed (each printed with the path to save it by hand); 2 arguments or its lock.
  - build / check: 0 done; 1 not built / stale master / check failed; 2 arguments, its lock or a bhavcopy /
    master lock busy.

What the update does with them:
- **Downloads** (`reference`, `bhavcopy`, `actions`, `index_fetch`):
  - 0 -> OK.
  - 1 -> **WARNING**: some days, years or files failed (e.g. a bhavcopy day that failed its checks, a blocked
    corporate-action year, nse_reference's broken Cora Hub PDF link). The step's last 20 output lines are shown,
    and the update goes on: the builds work on what is saved.
  - An output containing `Traceback (most recent call last)` is a crash, not "some items failed": FAILED.
- **Builds and checks** (`master`, `adjustments`, `exits`, `index`): any non-zero exit is FAILED and stops the update at
  that step. The later steps show "not run".
- **Exit 2** from any step (a lock busy, arguments) stops the update.

## Lock
`<folder>\equity_update.lock` (`backfill.locks.RunLock`): one update at a time; busy -> exit 2. The steps keep
their own locks (a running `cm_bhav.py` or build makes its step answer 2).

## Before 19:00 IST
When the bhavcopy step will run before `cm_bhav.READY_AT` (19:00 IST), a note says today's bhavcopy may not be
published yet. cm_bhav marks it pending; this is only a hint.

## Log and screen
- **Log:** `<folder>\equity_update.log`, appended, UTF-8.
  - Per run: `===== equity_update <IST time> argv: ...`.
  - Per command: `----- <step>: python <script> <args> (<IST time>)`, its full output (stderr merged), then
    `----- exit N, T s`.
  - Skipped steps: `----- <step>: SKIPPED (...)` / `up to date (...)`.
  - At the end: the summary and `===== exit N`.
- **Screen:**
  - One line per step (`[3/6] master ... OK (12 s)`), with the last 20 output lines on a warning or failure.
  - Then the summary `step | result | time | key numbers`. Result is OK, WARNING, SKIPPED, up to date, FAILED
    or not run.
  - A list of the warnings, then the log path.
- **Key numbers** (`NUMBERS`): the last line of the step's output that matches; "-" in place of a number when
  none matches. Two other kinds of spec: `("sum", regex, format)` adds each group over every matching line;
  `("count", [regex, ...], format)` counts the lines matching each regex.

  | Step | Line | Shown |
  |---|---|---|
  | reference | `Downloaded N new file(s), ...` | `downloads N` |
  | bhavcopy | `This run: ok A, holidays B, failed C, pending D, ...` | `ok A, holiday B, failed C, pending D` |
  | master | `  securities: N` | `securities N` |
  | actions | `saved A, skipped B, failed C (years ...)` | `saved A, skipped B, failed C` |
  | adjustments | `  applied: N (...` | `applied N` |
  | exits | `  EXITED: N`, `Review rows: N` | `exited N; review N` |
  | index_fetch | `<part>: saved A, refreshed B, skipped C, not found D, failed E` (part = amfi, nifty, changes, snapshots), summed (`FETCH_PART`) | `saved A, failed E` |
  | index | the report's "Reliable from" lines `  <index> <date>    <why>` (a date then 3+ spaces, `RELIABLE`) and `  <index> not reliable  <why>` (`UNRELIABLE`), counted; `Review rows: N` | `reliable A of B; review N` |

  Steps with a check add `check ok` when the last `Check: ok` / `Known-case checks: all found` line says so,
  else `check -`.

## Exit codes
- 0: all OK; warnings are allowed and listed.
- 1: a build or check failed, or a step crashed.
- 2: bad arguments, another update running, or a step answered 2.

## Daily routine
- Every evening after 19:00 IST: `python equity_update.py`.
- Monthly (or when new delistings / mergers / index changes are expected): `python equity_update.py
  --with-reference` (the NSE reference lists and the index membership sources).
- No network: `python equity_update.py --offline`.
- After fixing a failed step: `python equity_update.py --from-step <step>`.
- Run by hand; no cron.

## Tests
`tests/test_equity_update.py` uses a `FakeRun` in place of `subprocess.run`: no real process. It records the
commands, answers set codes and outputs, and touches `build_info.json` on a successful build. Fake inputs and
outputs have set mtimes in `tmp_path`, and `config.load_env` is stubbed.

Covers:
- the order of steps, `--with-reference`, `--offline`, `--folder` on every command;
- `index_fetch` / `index`: the order, skipped without `--with-reference` and offline, a fetch warning (the build
  still runs), a fetch crash and exit 2, build and check failures, each input kind making `index` stale
  (incl. a new file in a sub-folder and a fetch in the same run), `--from-step index_fetch` / `index`, the key
  numbers and their "-";
- the `run_step` argv (same Python, no shell, repo cwd);
- download warnings, including the last 20 lines;
- a crash, build and check failures, exit 2 from any kind of step;
- up to date and each input kind making a build stale, including the chain, the missing `build_info.json` and
  `--force`;
- `--from-step` (and an invalid name), `--dry-run`, the own lock;
- the relative folder, the 19:00 note;
- the log content and appending, the summary and its key numbers ("-" when not parsed).

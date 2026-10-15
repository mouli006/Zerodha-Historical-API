"""Rename old-style F&O day folders (<YYYY-MM-DD>) to
<YYYY-MM-DD>_exp-<expiry>_<withZeroVol|noZeroVol>_dl-<YYYY-MM-DD>_<HHMM> (python nifty_daily.py --rename-folders).
Renames folders only; nothing inside them is touched.

exp: the "Current week" row of the day's summary.csv, else the nearest expiry on or after the day in
expiries_seen.csv. Zero volume: withZeroVol if summary.csv's Keep_zero_volume is ON, run.log shows any run
with it ON, or zero_volume_contracts.csv has a kept row; noZeroVol if summary.csv says OFF or run.log shows
only OFF runs. dl: the first run's start time in run.log, else the folder's creation time (IST). A folder
whose expiry or zero-volume setting can't be told is skipped and listed."""
import csv
import os
import re
from datetime import datetime

from . import fno, fno_days

STARTED_RE = re.compile(r"started (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) IST")
KEEP_ZERO_RE = re.compile(r"keep zero-volume contracts:? (ON|OFF)\b", re.IGNORECASE)


def read_rows(path):
    if not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def summary_expiry(day_dir):
    """Expiry of the "Current week" row of summary.csv, or None."""
    for r in read_rows(os.path.join(day_dir, "summary.csv")):
        if "Current week" in (r.get("Bucket") or "") and r.get("Expiry"):
            try:
                return fno.as_date(r["Expiry"])
            except ValueError:
                return None
    return None


def run_log_lines(day_dir):
    path = os.path.join(day_dir, "run.log")
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8", errors="replace") as f:
        return f.readlines()


def first_start(lines):
    """The first run's start time (IST) in run.log's lines, or None."""
    for line in lines:
        m = STARTED_RE.search(line)
        if m:
            return datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S").replace(tzinfo=fno.IST)
    return None


def zero_volume_setting(day_dir, lines):
    """(True/False/None, where it came from): see the module notes."""
    saved = {(r.get("Keep_zero_volume") or "").strip().upper() for r in read_rows(os.path.join(day_dir, "summary.csv"))}
    runs = {m.group(1).upper() for line in lines for m in [KEEP_ZERO_RE.search(line)] if m}
    kept = any((r.get("Kept") or "").strip().upper() == "Y"
               for r in read_rows(os.path.join(day_dir, fno.ZERO_VOLUME_FILE)))
    if "ON" in saved:
        return True, "summary.csv"
    if "ON" in runs:
        return True, "run.log"
    if kept:
        return True, fno.ZERO_VOLUME_FILE
    if "OFF" in saved:
        return False, "summary.csv"
    if runs == {"OFF"}:
        return False, "run.log"
    return None, ""


def plan_root(data_root, underlying, source):
    """(moves [(old path, new path, how)], skipped [(name, why)], already named [name]) for one root.
    Raises DayFolderError on duplicate days."""
    root = fno.fno_root(data_root, underlying, source)
    expiries = None
    moves, skipped, named = [], [], []
    for day, name in sorted(fno.day_folders(root).items()):
        if fno.parse_day_folder(name)[1] is not None:
            named.append(name)
            continue
        path = os.path.join(root, name)
        exp, exp_from = summary_expiry(path), "summary.csv"
        if exp is None:
            if expiries is None:
                expiries = sorted(fno.read_expiries(data_root, underlying, source))
            exp, exp_from = next((e for e in expiries if e >= day), None), fno.EXPIRIES_FILE
        if exp is None:
            skipped.append((name, f"no expiry: no Current week row in summary.csv and nothing on or after {day} "
                                  f"in {fno.EXPIRIES_FILE}"))
            continue
        lines = run_log_lines(path)
        zero, zero_from = zero_volume_setting(path, lines)
        if zero is None:
            skipped.append((name, "zero-volume setting unknown (no Keep_zero_volume in summary.csv, no setting in "
                                  f"run.log, no kept row in {fno.ZERO_VOLUME_FILE})"))
            continue
        dl, dl_from = first_start(lines), "run.log"
        if dl is None:
            dl, dl_from = datetime.fromtimestamp(os.stat(path).st_ctime, fno.IST), "folder created time"
        moves.append((path, os.path.join(root, fno.day_folder_name(day, exp, zero, dl)),
                      f"exp: {exp_from}; zero volume: {zero_from}; dl: {dl_from}"))
    return moves, skipped, named


def rename_day_folders(data_root, underlyings, sources, apply=False, log=print):
    """Print old -> new for every old-style day folder in the roots; rename them with apply, then rewrite
    each touched root's day count. Takes each root's download lock first (dry run too).
    Returns the exit code: 0 done (or dry run), 1 not run or stopped by an error."""
    roots = [(u, s) for u in underlyings for s in sources if os.path.isdir(fno.fno_root(data_root, u, s))]
    if not roots:
        log(f"No F&O folders in {data_root}.")
        return 0
    locks = []
    try:
        for u, s in roots:
            try:
                locks.append(fno.acquire_lock(data_root, u, "nifty_daily.py --rename-folders", s))
            except fno.LockBusy as e:
                log(f"Not run: {e}")
                return 1
        plans = []
        for u, s in roots:
            try:
                plans.append((fno.fno_root(data_root, u, s), *plan_root(data_root, u, s)))
            except fno.DayFolderError as e:
                log(f"Not run: {e}")
                return 1
        for root, moves, skipped, named in plans:
            log(f"{root}:")
            for old, new, how in moves:
                log(f"  {os.path.basename(old)} -> {os.path.basename(new)}  ({how})")
            for name, why in skipped:
                log(f"  SKIPPED {name}: {why}")
            if named:
                log(f"  already named: {len(named)}")
            if not (moves or skipped or named):
                log("  no day folders")
        n_move = sum(len(p[1]) for p in plans)
        log(f"{n_move} to rename, {sum(len(p[2]) for p in plans)} skipped.")
        if not apply:
            log("Dry run: nothing renamed. Run again with --apply to rename.")
            return 0
        done = []
        for root, moves, _, _ in plans:
            for old, new, _ in moves:
                try:
                    os.rename(old, new)
                except OSError as e:
                    log(f"Stopped: could not rename {old} -> {os.path.basename(new)}: {e}")
                    log(f"Renamed before the error ({len(done)}):" + "".join(f"\n  {o} -> {os.path.basename(n)}"
                                                                              for o, n in done))
                    return 1
                done.append((old, new))
            if moves:
                log(f"Day count: {os.path.basename(fno_days.write_day_count(root))}")
        log(f"Renamed {len(done)} folders.")
        return 0
    finally:
        for lock in locks:
            lock.release()


def update_day_counts(data_root, underlyings, sources, log=print):
    """nifty_daily.py --update-day-count: rewrite the marker and days_index.csv of every existing root,
    each under its lock. Returns the exit code."""
    roots = [(u, s) for u in underlyings for s in sources if os.path.isdir(fno.fno_root(data_root, u, s))]
    if not roots:
        log(f"No F&O folders in {data_root}.")
        return 0
    for u, s in roots:
        try:
            lock = fno.acquire_lock(data_root, u, "nifty_daily.py --update-day-count", s)
        except fno.LockBusy as e:
            log(f"Not run: {e}")
            return 1
        try:
            root = fno.fno_root(data_root, u, s)
            log(os.path.join(root, os.path.basename(fno_days.write_day_count(root))))
        except fno.DayFolderError as e:
            log(f"Not run: {e}")
            return 1
        finally:
            lock.release()
    return 0

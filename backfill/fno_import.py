"""Replace one underlying's F&O data in the live stores with a prepared copy (python fno_store.py import).

    python fno_store.py import --from "D:\\Data\\Correct data" --underlying NIFTY [--apply] [--yes]

Mapping inside --from: a folder whose name contains "kite" (any case) is Kite, "dhan" Dhan; inside it a folder whose
name starts with "with zero volume" / "with_zero_volume" is the with store, "without zero volume" /
"without_zero_volume" the without store; in each, only the <UNDERLYING>_FnO_1min[_Dhan] root of --underlying is used.
Everything else is listed as not recognised and ignored. The source is only read (no lock is taken there either:
taking a .lock rewrites its owner text).

The dry run (default) lists every source day (complete?, zero-volume part, file counts, bhavcopy check), checks the
stores (a with store holds only keep zero-volume ON days, a without store only OFF days; duplicate days and old-style
names that can't be named are errors), derives each with day's without copy in a temporary folder and compares it
with the source's without day (information only), compares every day with the live store (same / different / new /
only in fno), shows what the archive step would move (fno_clear, as `clear --underlying <U>`) and the days still
missing in the download window. Exit 1 when an error was listed; --apply then refuses.

--apply (all live roots of the underlying locked for the whole run): archive the live data with the clear logic into
<home>\\archive\\<YYYY-MM-DD_HHMM IST>_import_<U>, copy every day in (<name>.part, every size and the SHA-256 of every
CSV checked, renamed to the final name; old-style names get the --rename-folders name), merge expiries_seen.csv and
holidays.log, write DONE progress rows (ZERO_VOLUME for listed zero-volume contracts without a file), rewrite the day
counts, publish the underlying for both stores (--force) and write import_report.txt into the archive. The first error
stops it. Run by hand; no cron."""
import csv
import filecmp
import glob
import hashlib
import json
import os
import shutil
import sqlite3
import sys
from datetime import date, datetime

from config import FNO_RAW_KEYS, VARIANTS, fno_backtest_dir, fno_raw_dir, weekly_expiry_weekday

from . import bhav, fno, fno_clear, fno_days, fno_derive, fno_rename
from .fno_store import holiday_key, merge_expiries, read_expiries_file, read_lines, take_locks
from .job import JOB, JOB_LOCK
from .storage import open_db

REPORT = "import_report.txt"
SOURCES = ("kite", "dhan")
LIST_LIMIT = 20


def variant_of_name(name):
    """"with" / "without" for a store folder's name in the source tree, else None."""
    n = name.lower().replace("_", " ")
    if n.startswith("without zero volume"):
        return "without"
    if n.startswith("with zero volume"):
        return "with"
    return None


def source_of_name(name):
    hits = [s for s in SOURCES if s in name.lower()]
    return hits[0] if len(hits) == 1 else None


def find_stores(src, underlying):
    """({(source, variant): root}, notes, errors): the underlying's roots in the source tree (see the module notes)."""
    stores, notes, errors = {}, [], []
    for n in sorted(os.listdir(src)):
        path = os.path.join(src, n)
        source = source_of_name(n) if os.path.isdir(path) else None
        if source is None:
            notes.append(f"not recognised, ignored: {path}")
            continue
        for m in sorted(os.listdir(path)):
            sub = os.path.join(path, m)
            variant = variant_of_name(m) if os.path.isdir(sub) else None
            if variant is None:
                notes.append(f"not recognised, ignored: {sub}")
                continue
            root = fno.fno_root(sub, underlying, source)
            for other in sorted(os.listdir(sub)):
                if other != os.path.basename(root):
                    notes.append(f"ignored (not {fno.fno_folder(underlying, source)}): {os.path.join(sub, other)}")
            if not os.path.isdir(root):
                notes.append(f"no {fno.fno_folder(underlying, source)} in {sub}")
                continue
            if (source, variant) in stores:
                errors.append(f"two folders for the {fno.BROKER[source]} {VARIANTS[variant]} store: "
                              f"{stores[(source, variant)]} and {root}")
                continue
            stores[(source, variant)] = root
    return stores, notes, errors


# --- inventory ------------------------------------------------------------------------------------------------

def completeness(day_dir):
    """(complete?, why not): summary.csv written and run.log's last run ended with Failed: 0."""
    if not os.path.exists(os.path.join(day_dir, "summary.csv")):
        return False, "no summary.csv"
    lines = fno_rename.run_log_lines(day_dir)
    starts = [i for i, line in enumerate(lines) if "F&O 1-minute run for" in line]
    if not starts:
        return False, "no run in run.log"
    block = lines[starts[-1]:]
    if not any("=== Ended" in line for line in block):
        return False, "last run in run.log did not end"
    failed = [line.split("Failed:", 1)[1].strip() for line in block if "Failed:" in line]
    if not failed or failed[-1] != "0":
        return False, f"last run in run.log: Failed: {failed[-1] if failed else '?'}"
    return True, ""


def check_label(day_dir):
    path = os.path.join(day_dir, "bhavcopy", "bhav_check.json")
    if not os.path.exists(path):
        return "no check"
    try:
        with open(path, encoding="utf-8") as f:
            j = json.load(f)
    except (OSError, ValueError):
        return "check unreadable"
    return "OK" if j.get("status") == "ok" else f"check: {j.get('severity') or j.get('status') or '?'}"


def csv_files(folder):
    return len(glob.glob(os.path.join(folder, "*.csv")))


def inventory(root, source, variant, underlying):
    """(days {date: info}, errors) of one source store."""
    errors = []
    try:
        folders = fno.day_folders(root)
    except fno.DayFolderError as e:
        return {}, [f"{root}: {e}"]
    hist = []

    def expiries():
        if not hist:
            hist.append(sorted(date.fromisoformat(e) for e in read_expiries_file(root)))
        return hist[0]
    days = {}
    for day, name in sorted(folders.items()):
        path = os.path.join(root, name)
        parsed = fno.parse_day_folder(name)
        if parsed[1] is None:
            target, how = fno_rename.new_name(path, day, expiries)
        else:
            target, how = name, "name kept"
        zero = parsed[2] if parsed[2] is not None else fno_rename.zero_volume_setting(path, fno_rename.run_log_lines(path))[0]
        complete, why = completeness(path)
        info = {"day": day, "name": name, "path": path, "target": target, "how": how, "zero": zero,
                "complete": complete, "why": why, "futures": csv_files(os.path.join(path, "futures")),
                "options": csv_files(os.path.join(path, "options")), "check": check_label(path)}
        if target is None:
            errors.append(f"{path}: can't be named: {how}")
        want = variant == "with"
        if zero is None:
            errors.append(f"{path}: zero-volume setting unknown")
        elif zero != want:
            errors.append(f"{path}: is {fno.ZERO_PARTS[zero]} but sits in the {VARIANTS[variant]} store")
        days[day] = info
    return days, errors


def tree_files(folder):
    out = set()
    for f, _, names in os.walk(folder):
        for n in names:
            out.add(os.path.relpath(os.path.join(f, n), folder))
    return out


def compare_trees(a, b):
    """(only in a, only in b, in both but different) relative file paths of folders a and b."""
    fa, fb = tree_files(a), tree_files(b)
    diff = sorted(p for p in fa & fb if not filecmp.cmp(os.path.join(a, p), os.path.join(b, p), shallow=False))
    return sorted(fa - fb), sorted(fb - fa), diff


def short(names):
    return ", ".join(names[:LIST_LIMIT]) + (" ..." if len(names) > LIST_LIMIT else "")


def compare_live(info, live_path):
    if live_path is None:
        return "new"
    only_a, only_b, diff = compare_trees(info["path"], live_path)
    n = len(only_a) + len(only_b) + len(diff)
    if not n:
        return "same"
    parts = [f"{label} {len(x)}: {short(x)}" for label, x in
             (("only in Correct data", only_a), ("only in fno", only_b), ("contents differ", diff)) if x]
    return f"different: {n} files differ ({'; '.join(parts)})"


# --- the missing-days report ------------------------------------------------------------------------------------

def window_of(underlying, roots, today):
    history = set()
    for r in roots:
        history.update(date.fromisoformat(e) for e in read_expiries_file(r))
    live = sorted(e for e in history if e >= today)
    if not live:
        return None
    return fno.date_window(live, history, today, weekly_expiry_weekday(underlying))


def holidays_of(roots, underlying):
    """{source: logged holidays} of every (root, source) in roots (per source, as --missing skips them)."""
    days = {s: set() for s in SOURCES}
    for r, source in roots:
        if os.path.isdir(r):
            days[source] |= fno.holidays_logged(os.path.dirname(r), underlying, source)
    return days


def missing_report(underlying, window, holidays, have, log, now=None):
    """have: {(source, variant): set of complete days}, holidays: {source: days}. Prints the days of the window that
    neither store of a source has."""
    if window is None:
        log("No live expiry in the saved expiry history: the download window is unknown, so no missing days are listed.")
        return
    log(f"{underlying} download window today ({fno.today_ist()}, IST): {window.min_date} to {window.max_date}"
        + (" (start estimated)" if window.estimated else "") + ".")
    every = [d for d in fno.range_dates(window.min_date, window.max_date) if not fno.too_early(d, now)]
    extra = "" if underlying == "NIFTY" else f" --underlying {underlying}"
    for source in SOURCES:
        cand = [d for d in every if d not in holidays.get(source, set())]
        w, wo = have.get((source, "with"), set()), have.get((source, "without"), set())
        miss = [d for d in cand if d not in w and d not in wo]
        only_without = [d for d in cand if d in w and d not in wo]
        label = fno.BROKER[source]
        if miss:
            log(f"{label}: missing {', '.join(map(str, miss))}. Download with:")
            log(f"  python nifty_daily.py --missing{extra}{' --source dhan' if source == 'dhan' else ''} --keep-zero-volume")
            log("  (the without-zero-volume copy is made after each day when that setting is on)")
        else:
            log(f"{label}: no day of the window is missing.")
        if only_without:
            log(f"{label}: the without store lacks {', '.join(map(str, only_without))}: "
                f"python fno_store.py derive-without --source {source} --underlying {underlying} --apply")
    log("Nothing is downloaded by this command.")


# --- apply steps --------------------------------------------------------------------------------------------------

size_of = os.path.getsize   # tests patch it to simulate a bad copy


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def copy_checked(src, dst):
    """Copy folder src to dst via dst.part: every file's size, and every CSV's SHA-256, checked, then renamed.
    Raises OSError (the .part is removed); src is only read."""
    if os.path.exists(dst):
        raise OSError(f"{dst} already exists")
    part = dst + ".part"
    if os.path.exists(part):
        raise OSError(f"{part} exists from an earlier attempt; remove it first")
    try:
        shutil.copytree(src, part)
        for rel in sorted(tree_files(src)):
            a, b = os.path.join(src, rel), os.path.join(part, rel)
            if not os.path.exists(b) or size_of(a) != size_of(b):
                raise OSError(f"copy of {a} does not match (size)")
            if rel.lower().endswith(".csv") and sha256(a) != sha256(b):
                raise OSError(f"copy of {a} does not match (SHA-256)")
        os.rename(part, dst)
    except BaseException:
        shutil.rmtree(part, ignore_errors=True)
        raise


def merge_history(src_root, dst_root):
    """expiries_seen.csv (union, earlier first_seen wins) and holidays.log (holidays not listed yet) of src_root into
    dst_root. Returns (expiries added or changed, holiday lines added)."""
    have = read_expiries_file(dst_root)
    changed = merge_expiries(have, read_expiries_file(src_root))
    if changed:
        fno.write_csv(os.path.join(dst_root, fno.EXPIRIES_FILE), ["expiry", "first_seen"],
                      [{"expiry": e, "first_seen": have[e]} for e in sorted(have)])
    path = os.path.join(dst_root, "holidays.log")
    lines = read_lines(path)
    keys = {holiday_key(x) for x in lines}
    new = []
    for line in read_lines(os.path.join(src_root, "holidays.log")):
        if holiday_key(line) not in keys:
            keys.add(holiday_key(line))
            new.append(line)
    if new:
        with open(path + ".tmp", "w", encoding="utf-8", newline="\n") as f:
            f.write("\n".join(lines + new) + "\n")
        os.replace(path + ".tmp", path)
    return len(changed), len(new)


def zero_listed(day_dir):
    path = os.path.join(day_dir, fno.ZERO_VOLUME_FILE)
    if not os.path.exists(path):
        return set()
    with open(path, newline="", encoding="utf-8") as f:
        return {r["Contract"] for r in csv.DictReader(f) if r.get("Contract")}


def write_rows(save, day_dir, underlying, source, run_date):
    """DONE rows for the day's contracts with a file, DONE + ZERO_VOLUME for listed zero-volume contracts without one.
    Returns (with a file, zero volume, contracts of the saved list without a row)."""
    symbols = fno_derive.symbols_of(day_dir, underlying, run_date)
    files = fno_derive.contract_files(day_dir)
    zero = zero_listed(day_dir)
    run = fno.run_key(underlying, run_date, source)
    start = str(datetime.combine(run_date, fno.WINDOW[0]))
    end = str(datetime.combine(run_date, fno.WINDOW[1]))
    now = datetime.now().isoformat(timespec="seconds")
    rows, n_file, n_zero, n_none = [], 0, 0, 0
    for name, c in sorted(symbols.items()):
        if name in files:
            reason, n_file = None, n_file + 1
        elif name in zero:
            reason, n_zero = fno.ZERO_VOLUME, n_zero + 1
        else:
            n_none += 1
            continue
        rows.append((run, c["tradingsymbol"], c.get("instrument_token"), start, end, now, reason))
    db = open_db(save)
    try:
        db.execute("DELETE FROM chunks WHERE run = ?", (run,))
        db.executemany("INSERT INTO chunks (run, symbol, token, chunk_from, chunk_to, status, tries, updated, reason) "
                       "VALUES (?, ?, ?, ?, ?, 'DONE', 0, ?, ?)", rows)
        db.commit()
    finally:
        db.close()
    return n_file, n_zero, n_none


# --- the command ----------------------------------------------------------------------------------------------------

def import_data(src, underlying, env, apply=False, yes=False, command="", log=print, ask=input, interactive=None,
                publish=None):
    """See the module notes. Returns the exit code: 0 done or a clean dry run, 1 errors listed, not run or stopped."""
    interactive = interactive if interactive is not None else (lambda: sys.stdin.isatty())
    if underlying not in fno.UNDERLYINGS:
        log("Not run: give --underlying (NIFTY or SENSEX).")
        return 1
    if not src or not os.path.isdir(src):
        log(f"Not run: {src} is not a folder.")
        return 1
    src = os.path.normpath(src)
    missing = [k for k in (*FNO_RAW_KEYS.values(), "FNO_BACKTEST_DIR") if not env.get(k)]
    if missing:
        log(f"Not run: {', '.join(missing)} not set in .env.")
        return 1
    home = fno_clear.home_of(env)
    if not home:
        log("Not run: the .env bases share no common folder for the archive.")
        return 1
    if any(os.path.normcase(os.path.normpath(env[k])).startswith(os.path.normcase(src) + os.sep)
           or os.path.normcase(src).startswith(os.path.normcase(os.path.normpath(env[k])) + os.sep)
           for k in FNO_RAW_KEYS.values()):
        log("Not run: --from must be outside the live stores (and they outside it).")
        return 1
    with JOB_LOCK:
        if JOB["state"] == "running":
            log("Not run: a download job is running. Stop it or wait for it to finish.")
            return 1
    archive = os.path.join(home, "archive", f"{fno.now_ist():%Y-%m-%d_%H%M}_import_{underlying}")
    if os.path.exists(archive):
        log(f"Not run: the archive folder {archive} already exists.")
        return 1
    live = {(s, v): fno.fno_root(fno_raw_dir(s, v, env), underlying, s) for s in SOURCES for v in VARIANTS}
    old = [fno.fno_root(os.path.normpath(env[FNO_RAW_KEYS[s]]), underlying, s) for s in SOURCES]
    owner = f"fno_store.py import {underlying}"
    try:
        locks = take_locks([r for r in [*live.values(), *old] if os.path.isdir(r)], apply, owner)
    except fno.LockBusy as e:
        log(f"Not run: {e}")
        return 1
    try:
        return run_import(src, underlying, env, home, archive, live, old, locks, apply, yes, command, log, ask,
                          interactive, publish, owner)
    finally:
        for lock in locks:
            lock.release()


def run_import(src, underlying, env, home, archive, live, old, locks, apply, yes, command, log, ask, interactive,
               publish, owner):
    log(f"== Import {underlying} F&O data from {src}")
    stores, notes, errors = find_stores(src, underlying)
    for n in notes:
        log(f"  {n}")
    if not stores:
        log(f"No {underlying} store found in {src}.")
        return 1
    inv, live_days = {}, {}
    for key in sorted(stores, key=lambda k: (SOURCES.index(k[0]), k[1])):
        source, variant = key
        days, errs = inventory(stores[key], source, variant, underlying)
        inv[key], errors = days, errors + errs
        try:
            have = fno.day_folders(live[key])
        except fno.DayFolderError as e:
            errors.append(f"{live[key]}: {e}")
            have = {}
        live_days[key] = {d: os.path.join(live[key], n) for d, n in have.items()}

    results = {}
    for key, days in inv.items():
        source, variant = key
        log("")
        log(f"{fno.BROKER[source]} {VARIANTS[variant]}: {stores[key]}  ->  {live[key]}")
        for day, i in days.items():
            state = "complete" if i["complete"] else f"INCOMPLETE ({i['why']})"
            zero = {True: "withZeroVol", False: "noZeroVol", None: "?"}[i["zero"]]
            log(f"  {i['name']}: {state}; {zero}; {i['futures']} futures, {i['options']} options; {i['check']}")
            if i["target"] != i["name"]:
                log(f"      -> {i['target'] or 'CAN NOT BE NAMED'}  ({i['how']})")
            results[(key, day)] = compare_live(i, live_days[key].get(day))
            log(f"      vs fno now: {results[(key, day)]}")
        for day in sorted(set(live_days[key]) - set(days)):
            log(f"  ONLY IN FNO: {os.path.basename(live_days[key][day])} - will be archived and not replaced")

    log("")
    log("Variant check (the without copy derived from each with day, compared with the without day; information only):")
    n_var = 0
    for source in SOURCES:
        w, wo = inv.get((source, "with"), {}), inv.get((source, "without"), {})
        for day in sorted(set(w) & set(wo)):
            name = w[day]["target"]
            if not name or fno.parse_day_folder(name)[2] is not True:
                log(f"  {fno.BROKER[source]} {day}: not compared (the with day is not a withZeroVol day)")
                continue
            try:
                line, removed = fno_derive.compare_day(w[day]["path"], wo[day]["path"], underlying, source, name=name)
            except (fno_derive.DeriveError, OSError, bhav.BhavError) as e:
                log(f"  {fno.BROKER[source]} {day}: not compared: {e}")
                continue
            n_var += 1
            log(f"  {fno.BROKER[source]} {day}: {line} ({removed} zero-volume files removed)")
    if not n_var:
        log("  no day has both a with and a without copy")

    if errors:
        log("")
        log(f"ERRORS ({len(errors)}):")
        for e in errors:
            log(f"  {e}")

    log("")
    scope = fno_clear.plan_clear(underlying, list(SOURCES), list(VARIANTS), True, env)
    incoming = {d for days in inv.values() for d in days}
    n_days = sum(len(d) for d in inv.values())
    if not apply or errors:
        fno_clear.run_clear(scope, underlying, env, home, archive, False, False, yes, command, log, ask, interactive,
                            label="Archive before the import:", also=incoming, dry_tail=False)
        log("")
        log("--apply would: archive the above, copy every day listed (via <name>.part, sizes and CSV hashes checked; "
            "old-style names renamed), merge expiries_seen.csv and holidays.log, write DONE progress rows "
            f"(ZERO_VOLUME for listed zero-volume contracts without a file), rewrite the day counts, publish "
            f"{underlying} for both stores (--force) and write {REPORT} into the archive.")
        totals = {k: sum(1 for r in results.values() if r.split(":")[0] == k) for k in ("same", "different", "new")}
        only = sum(len(set(live_days[k]) - set(inv[k])) for k in inv) + sum(
            len(v) for k, v in live_days.items() if k not in inv)
        log(f"Totals: {n_days} days in {len(inv)} stores: {totals['same']} same, {totals['different']} different, "
            f"{totals['new']} new; {only} only in fno; {len(errors)} errors.")
        log("")
        log("Still missing after the import:")
        have = {k: {d for d, i in days.items() if i["complete"]} for k, days in inv.items()}
        missing_report(underlying, window_of(underlying, [*stores.values(), *live.values(), *old], fno.today_ist()),
                       holidays_of([(r, k[0]) for k, r in [*stores.items(), *live.items()]], underlying), have, log)
        if errors:
            log("Not run: fix the errors above first." if apply else "Dry run: nothing changed. --apply refuses while errors are listed.")
            return 1
        log("Dry run: nothing changed. Run again with --apply to import.")
        return 0

    # --- apply ---
    code = fno_clear.run_clear(scope, underlying, env, home, archive, True, False, yes, command, log, ask, interactive,
                               label="Archive before the import:", also=incoming)
    if code:
        log("Not run: the archive step did not finish; nothing was copied.")
        return 1
    os.makedirs(archive, exist_ok=True)   # also when there was nothing to archive, for the report
    for root in live.values():
        if not os.path.isdir(root):
            os.makedirs(root)
            locks.append(fno.RunLock(os.path.join(root, ".lock")).acquire(owner))
    copied, rows, history = [], [], []
    error = None
    try:
        for key, days in inv.items():
            source, variant = key
            save = os.path.dirname(live[key])
            for day, i in days.items():
                dst = os.path.join(live[key], i["target"])
                copy_checked(i["path"], dst)
                copied.append((key, i, dst))
                rows.append((key, day, *write_rows(save, dst, underlying, source, day)))
            history.append((key, *merge_history(stores[key], live[key])))
    except (OSError, sqlite3.Error, ValueError) as e:
        error = f"{type(e).__name__}: {e}"
    finally:
        fno_clear.rewrite_counts(list(live.values()), log)
    log("")
    log(f"Copied {len(copied)} of {sum(len(d) for d in inv.values())} days:")
    for key, i, dst in copied:
        log(f"  {i['path']} -> {dst}")
    for key, day, n_file, n_zero, n_none in rows:
        log(f"  {fno.BROKER[key[0]]} {VARIANTS[key[1]]} {day}: progress rows {n_file:,} with a file, {n_zero:,} zero "
            f"volume; {n_none:,} contracts of the saved list without a row (a re-run requests them again)")
    for key, n_exp, n_hol in history:
        log(f"  {live[key]}: {n_exp} expiries and {n_hol} holidays merged")
    pub_code = None
    for lock in locks:   # publish takes each raw root's lock while it reads
        lock.release()
    locks.clear()
    if error is None:
        log("")
        publish = publish or default_publish
        pub_code = publish(env, underlying, log)
        for v in VARIANTS.values():
            path = os.path.join(fno_backtest_dir(env), v, underlying, "days.csv")
            log(f"{path}:")
            for line in read_lines(path) or ["(missing)"]:
                log(f"  {line}")
        log("")
        log("Still missing:")
        window = window_of(underlying, [*live.values(), *old], fno.today_ist())
        have = {}
        if window is not None:   # fno.missing_dates: summary.csv and no non-DONE progress row
            every = set(fno.range_dates(window.min_date, window.max_date))
            have = {k: every - set(fno.missing_dates(fno_raw_dir(k[0], k[1], env), underlying, window, source=k[0]))
                    for k in live}
        missing_report(underlying, window, holidays_of([(r, k[0]) for k, r in live.items()], underlying), have, log)
    write_report(archive, command, copied, results, error, pub_code)
    if error:
        log(f"Stopped: {error}")
        log(f"The archive is complete ({archive}); see {REPORT} for what was copied.")
        return 1
    log(f"Imported {len(copied)} days. Archive and report: {archive}")
    return 0 if not pub_code else 1


def default_publish(env, underlying, log):
    from .fno_publish import publish_variants
    raw = {s: env.get(FNO_RAW_KEYS[s]) or None for s in SOURCES}
    return publish_variants(raw, fno_backtest_dir(env), list(VARIANTS), [underlying], force=True, log=log)


def write_report(archive, command, copied, results, error, pub_code):
    man = {}
    path = os.path.join(archive, fno_clear.MANIFEST)
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            man = json.load(f)
    lines = [f"Import of {man.get('underlying', '')} F&O data on {fno.now_ist():%Y-%m-%d %H:%M:%S} IST",
             f"Command: {command}", "", f"Copied days ({len(copied)}):"]
    for key, i, dst in copied:
        lines.append(f"  {fno.BROKER[key[0]]} {VARIANTS[key[1]]}: {i['path']} -> {dst}  [{results.get((key, i['day']), '')}]")
    if error:
        lines += ["", f"STOPPED by an error: {error}"]
    elif pub_code is not None:
        lines += ["", "Publish: " + ("done" if not pub_code else "finished with problems (see the console output)")]
    lines += ["", f"Archived items ({len(man.get('items', []))}):"]
    lines += [f"  {i['src']} -> {i['dst']}" for i in man.get("items", [])]
    lines += [f"  {d['live']}: {d['rows']:,} progress rows -> {d['archive']}" for d in man.get("dbs", [])]
    u = man.get("underlying", "")
    lines += ["", "To bring the old data back (the imported days and their progress rows must go first; the source "
                  "folder still holds them):",
              f"  python fno_store.py clear --underlying {u} --apply --yes",
              f'  python fno_store.py restore --from "{archive}" --apply',
              f"  python fno_store.py publish --underlying {u} --force"]
    p = os.path.join(archive, REPORT)
    with open(p + ".tmp", "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(lines) + "\n")
    os.replace(p + ".tmp", p)

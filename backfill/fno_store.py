"""Move F&O day folders from old save folders into one raw folder per source (python fno_store.py move-raw).

    <from>/NIFTY_FnO_1min/<day folder>  ->  <to>/NIFTY_FnO_1min/<day folder>    (same for SENSEX, and _Dhan)

Day folders keep their names and contents byte for byte: os.rename on the same drive, else a copy whose
file sizes are checked before the source is deleted. A day the target already has (any name form) is never
moved, replaced or deleted; it is listed ("incoming has more data" when the target is noZeroVol and the
incoming folder withZeroVol). Per root, expiries_seen.csv (union, earliest first_seen) and holidays.log
(lines for holidays it doesn't list yet; the leading "logged at" time is ignored) are merged, and the progress.sqlite rows of every moved day (run key fno:/fno-dhan:) are copied
into <to>/progress.sqlite, so a resume skips contracts that were DONE without a file. Dry run by default."""
import csv
import os
import shutil
import sqlite3

import config

from . import fno, fno_days, fno_rename
from .storage import open_db


def same_device(a, b):
    """True when paths a and b (or their nearest existing parents) are on the same drive (os.rename works)."""
    def dev(p):
        while not os.path.exists(p):
            parent = os.path.dirname(p)
            if parent == p:
                break
            p = parent
        return os.stat(p).st_dev
    return dev(a) == dev(b)


def read_expiries_file(root):
    """{expiry ISO: first_seen} of root/expiries_seen.csv (the file only; nothing is seeded)."""
    path = os.path.join(root, fno.EXPIRIES_FILE)
    if not os.path.exists(path):
        return {}
    with open(path, newline="", encoding="utf-8") as f:
        return {r["expiry"]: r["first_seen"] for r in csv.DictReader(f) if r.get("expiry")}


def merge_expiries(target, incoming):
    """target updated with incoming (union; the earlier first_seen wins). Returns the expiries added or changed."""
    changed = []
    for e, seen in incoming.items():
        if e not in target or (seen and (not target[e] or seen < target[e])):
            target[e] = seen
            changed.append(e)
    return changed


def holiday_key(line):
    """A holidays.log line without its leading "YYYY-MM-DD HH:MM:SS" (when it was logged), so the same holiday
    logged by two runs counts once."""
    parts = line.split(" ", 2)
    return parts[2] if len(parts) == 3 and parts[0][:2] == "20" and ":" in parts[1] else line


def read_lines(path):
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8", errors="replace") as f:
        return [line.rstrip("\n") for line in f if line.strip()]


def progress_rows(from_dir, run):
    """(columns, rows) of the chunks of one run in from_dir/progress.sqlite (read only), or ([], [])."""
    path = os.path.join(from_dir, "progress.sqlite")
    if not os.path.exists(path):
        return [], []
    db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        if not db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='chunks'").fetchone():
            return [], []
        cur = db.execute("SELECT * FROM chunks WHERE run = ?", (run,))
        return [d[0] for d in cur.description], cur.fetchall()
    finally:
        db.close()


def target_has_run(to_dir, run):
    path = os.path.join(to_dir, "progress.sqlite")
    if not os.path.exists(path):
        return 0
    db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        if not db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='chunks'").fetchone():
            return 0
        return db.execute("SELECT COUNT(*) FROM chunks WHERE run = ?", (run,)).fetchone()[0]
    finally:
        db.close()


def copy_verified(src, dst):
    """Copy folder src to dst on another drive: into dst.part, every file's size checked, then renamed to dst
    and src deleted. A mismatch removes the copy and raises OSError; src is kept."""
    part = dst + ".part"
    if os.path.exists(part):
        raise OSError(f"{part} exists from an earlier attempt; remove it first")
    shutil.copytree(src, part)
    for folder, _, names in os.walk(src):
        rel = os.path.relpath(folder, src)
        for n in names:
            a, b = os.path.join(folder, n), os.path.join(part, rel, n)
            if not os.path.exists(b) or os.path.getsize(a) != os.path.getsize(b):
                shutil.rmtree(part, ignore_errors=True)
                raise OSError(f"copy of {a} does not match (size); the source is kept")
    os.rename(part, dst)
    shutil.rmtree(src)


def move_day(src, dst):
    if os.path.exists(dst):
        raise OSError(f"{dst} already exists")
    if same_device(src, os.path.dirname(dst)):
        os.rename(src, dst)
    else:
        copy_verified(src, dst)


def plan(froms, source, to, pick=None):
    """The moves, duplicates and merges for every root of source in froms (processed in order: a later
    folder sees the earlier ones' days as already in the target). Raises DayFolderError on duplicate days.
    pick(name, path): True to move the day, None to leave it out silently (it goes elsewhere), or a reason
    to list it as refused."""
    roots, targets = [], {}
    for frm in froms:
        for u in sorted(fno.UNDERLYINGS):
            src_root = fno.fno_root(frm, u, source)
            if not os.path.isdir(src_root):
                continue
            dst_root = fno.fno_root(to, u, source)
            if dst_root not in targets:
                have = fno.day_folders(dst_root) if os.path.isdir(dst_root) else {}
                targets[dst_root] = {"days": dict(have), "expiries": read_expiries_file(dst_root),
                                     "holidays": {holiday_key(x) for x in read_lines(os.path.join(dst_root, "holidays.log"))}}
            t = targets[dst_root]
            r = {"from": frm, "to": to, "underlying": u, "src": src_root, "dst": dst_root, "moves": [], "dups": [],
                 "refused": [], "expiries": [], "holidays": [], "progress": 0}
            for day, name in sorted(fno.day_folders(src_root).items()):
                ok = True if pick is None else pick(name, os.path.join(src_root, name))
                if ok is None:
                    continue
                if ok is not True:
                    r["refused"].append((name, ok))
                    continue
                if day in t["days"]:
                    have = t["days"][day]
                    zero_have, zero_new = fno.parse_day_folder(have)[2], fno.parse_day_folder(name)[2]
                    why = (f"incoming has more data ({name} is withZeroVol, the target's {have} is noZeroVol); not "
                           "moved, nothing replaced" if zero_have is False and zero_new is True
                           else f"already in the target as {have}")
                    r["dups"].append((name, why, zero_have is False and zero_new is True))
                    continue
                t["days"][day] = name
                run = fno.run_key(u, day, source)
                cols, rows = progress_rows(frm, run)
                if rows and not target_has_run(to, run):
                    r["progress"] += len(rows)
                r["moves"].append((name, run, cols, rows))
            r["expiries"] = merge_expiries(t["expiries"], read_expiries_file(src_root))
            new = []
            for line in read_lines(os.path.join(src_root, "holidays.log")):
                if holiday_key(line) not in t["holidays"]:
                    t["holidays"].add(holiday_key(line))
                    new.append(line)
            r["holidays"] = new
            r["empty_after"] = pick is None and len(r["moves"]) == len(fno.day_folders(src_root))
            roots.append(r)
    return roots, targets


def store_pick(variant):
    """pick() for a move into the with / without store: a day of the other setting is refused; an old-style
    name is decided like nifty_daily.py --rename-folders (summary.csv, run.log, zero_volume_contracts.csv)."""
    want = variant == "with"

    def pick(name, path):
        zero = day_zero_volume(name, path)
        if zero is None:
            return "zero-volume setting unknown (old-style name, nothing in summary.csv / run.log); not moved"
        if zero != want:
            return f"is {fno.ZERO_PARTS[zero]}, this is the {config.VARIANTS[variant]} store; not moved"
        return True
    return pick


def day_zero_volume(name, path):
    """True / False / None: the day folder's zero-volume setting from its name, else as --rename-folders decides."""
    zero = fno.parse_day_folder(name)[2]
    if zero is None:
        zero = fno_rename.zero_volume_setting(path, fno_rename.run_log_lines(path))[0]
    return zero


def take_locks(roots, create, owner):
    """The .lock of every root in roots (create False: only roots that already have one, so a dry run creates
    nothing). Raises LockBusy after releasing those taken."""
    locks = []
    try:
        for root in roots:
            if not create and not os.path.exists(os.path.join(root, ".lock")):
                continue   # a root without a .lock has never been run, so it can't be busy
            locks.append(fno.RunLock(os.path.join(root, ".lock")).acquire(owner))
    except fno.LockBusy:
        for lock in locks:
            lock.release()
        raise
    return locks


def move_raw(froms, source, to, apply=False, log=print):
    """Print (dry run) or do (apply) the moves; see the module notes. Into a with / without store (by the --to
    folder's name) only days of that setting move; the others are listed. Returns the exit code: 0 done or dry
    run, 1 not run or stopped by an error."""
    if not to:
        log(f"Not run: give --to (or --variant) or set {('FNO_RAW_KITE', 'FNO_RAW_DHAN')[source == 'dhan']} in .env.")
        return 1
    to = os.path.normpath(to)
    if not os.path.isabs(to):
        log(f"Not run: --to must be a full path ({to}).")
        return 1
    froms = [os.path.normpath(f) for f in froms]
    for frm in froms:
        if not os.path.isdir(frm):
            log(f"Not run: {frm} is not a folder.")
            return 1
        if os.path.normcase(frm) == os.path.normcase(to):
            log(f"Not run: --from and --to are the same folder ({to}).")
            return 1
    srcs = [fno.fno_root(f, u, source) for f in froms for u in sorted(fno.UNDERLYINGS)]
    srcs = [s for s in srcs if os.path.isdir(s)]
    if not srcs:
        log(f"No {fno.BROKER[source]} F&O folders ({', '.join(fno.fno_folder(u, source) for u in sorted(fno.UNDERLYINGS))}) "
            f"in {', '.join(froms)}.")
        return 0
    dsts = sorted({fno.fno_root(to, u, source) for u in sorted(fno.UNDERLYINGS)
                   if any(os.path.isdir(fno.fno_root(f, u, source)) for f in froms)})
    try:
        locks = take_locks(srcs + dsts, apply, "fno_store.py move-raw")
    except fno.LockBusy as e:
        log(f"Not run: {e}")
        return 1
    try:
        variant = config.store_variant(to)
        try:
            roots, _ = plan(froms, source, to, store_pick(variant) if variant else None)
        except fno.DayFolderError as e:
            log(f"Not run: {e}")
            return 1
        return report_and_apply(roots, apply, log)
    finally:
        for lock in locks:
            lock.release()


def report_and_apply(roots, apply, log):
    n_move = n_dup = n_more = n_rows = n_refused = 0
    for r in roots:
        log(f"{r['src']} -> {r['dst']}:")
        for name, *_ in r["moves"]:
            log(f"  {os.path.join(r['src'], name)} -> {os.path.join(r['dst'], name)}")
        for name, why, more in r["dups"]:
            log(f"  DUPLICATE {name}: {why}")
        for name, why in r["refused"]:
            log(f"  REFUSED {name}: {why}")
        n_refused += len(r["refused"])
        log(f"  merge: +{len(r['expiries'])} expiries in {fno.EXPIRIES_FILE}, +{len(r['holidays'])} holidays.log lines, "
            f"+{r['progress']:,} progress.sqlite rows")
        if r["empty_after"] and r["moves"]:
            log("  the old root has no day folders left after this; it is left in place (not deleted)")
        n_move += len(r["moves"])
        n_dup += len(r["dups"])
        n_more += sum(1 for d in r["dups"] if d[2])
        n_rows += r["progress"]
    log(f"Totals: {n_move} day folders to move, {n_dup} duplicates not moved"
        + (f" ({n_more} where the incoming folder has more data)" if n_more else "")
        + (f", {n_refused} refused" if n_refused else "") + f", {n_rows:,} progress rows.")
    if not apply:
        log("Dry run: nothing moved. Run again with --apply to move.")
        return 0
    done, touched = [], []
    db = db_root = None
    try:
        for r in roots:
            os.makedirs(r["dst"], exist_ok=True)
            # merges first: they only add (expiries, holiday lines), so a later error leaves nothing inconsistent
            if r["expiries"]:
                seen = read_expiries_file(r["dst"])
                merge_expiries(seen, read_expiries_file(r["src"]))
                fno.write_csv(os.path.join(r["dst"], fno.EXPIRIES_FILE), ["expiry", "first_seen"],
                              [{"expiry": e, "first_seen": seen[e]} for e in sorted(seen)])
            if r["holidays"]:
                have = {holiday_key(x) for x in read_lines(os.path.join(r["dst"], "holidays.log"))}
                with open(os.path.join(r["dst"], "holidays.log"), "a", encoding="utf-8") as f:
                    f.writelines(line + "\n" for line in r["holidays"] if holiday_key(line) not in have)
            for name, run, cols, rows in r["moves"]:
                src, dst = os.path.join(r["src"], name), os.path.join(r["dst"], name)
                move_day(src, dst)
                done.append((src, dst))
                for root in (r["dst"], r["src"]):
                    if root not in touched:
                        touched.append(root)
                if rows:
                    if db is not None and db_root != r["to"]:
                        db.close()
                        db = None
                    if db is None:
                        db, db_root = open_db(r["to"]), r["to"]
                    have = {c[1] for c in db.execute("PRAGMA table_info(chunks)")}
                    use = [c for c in cols if c in have]
                    idx = [cols.index(c) for c in use]
                    db.executemany(f"INSERT OR IGNORE INTO chunks ({', '.join(use)}) VALUES ({', '.join('?' * len(use))})",
                                   [[row[i] for i in idx] for row in rows])
                    db.commit()
    except (OSError, sqlite3.Error, fno.DayFolderError) as e:
        log(f"Stopped: {type(e).__name__}: {e}")
        log(f"Moved before the error ({len(done)}):" + "".join(f"\n  {s} -> {d}" for s, d in done))
        return 1
    finally:
        if db is not None:
            db.close()
        # the day count of every root that gained or lost a day (the old root's too, so it doesn't list moved days)
        for root in touched:
            try:
                log(f"Day count: {os.path.join(root, os.path.basename(fno_days.write_day_count(root)))}")
            except (OSError, fno.DayFolderError) as e:
                log(f"Day count of {root} not updated: {e}")
    log(f"Moved {len(done)} day folders.")
    for src in dict.fromkeys(r["src"] for r in roots):
        if os.path.isdir(src) and not fno.day_folders(src):
            log(f"{src} has no day folders left; it is left in place (not deleted).")
    return 0


def split_stores(sources, env, apply=False, log=print):
    """python fno_store.py split-stores: move each root's day folders from FNO_RAW_<SOURCE> into its
    with_zero_volume / without_zero_volume store (by the name's withZeroVol / noZeroVol; old-style names as
    --rename-folders decides, else skipped and listed), with the merges of move-raw. The emptied roots stay.
    Dry run by default. Returns the exit code: 0 done or dry run, 1 not run or stopped by an error."""
    code = 0
    for source in sources:
        base = env.get(config.FNO_RAW_KEYS[source], "")
        if not base:
            log(f"{fno.BROKER[source]}: Not run: {config.FNO_RAW_KEYS[source]} is not set in .env.")
            code = 1
            continue
        base = os.path.normpath(base)
        srcs = [fno.fno_root(base, u, source) for u in sorted(fno.UNDERLYINGS)]
        srcs = [s for s in srcs if os.path.isdir(s)]
        log(f"== {fno.BROKER[source]}: {base}")
        if not srcs:
            log("  no F&O roots to split")
            continue
        to = {v: config.fno_raw_dir(source, v, env) for v in config.VARIANTS}
        dsts = [fno.fno_root(to[v], u, source) for v in to for u in sorted(fno.UNDERLYINGS)
                if os.path.isdir(fno.fno_root(base, u, source))]
        try:
            locks = take_locks(srcs + dsts, apply, "fno_store.py split-stores")
        except fno.LockBusy as e:
            log(f"Not run: {e}")
            code = 1
            continue
        try:
            unknown = []

            def pick_for(variant):
                def pick(name, path):
                    zero = day_zero_volume(name, path)
                    if zero is None:
                        if variant == "with":
                            unknown.append(path)
                        return None
                    return True if zero == (variant == "with") else None
                return pick
            try:
                roots = []
                for v in config.VARIANTS:
                    roots += plan([base], source, to[v], pick_for(v))[0]
            except fno.DayFolderError as e:
                log(f"Not run: {e}")
                code = 1
                continue
            for path in unknown:
                log(f"  SKIPPED {path}: zero-volume setting unknown (old-style name, nothing in summary.csv / run.log); left in place")
            for u in sorted(fno.UNDERLYINGS):
                with_days = set(fno.day_folders(fno.fno_root(to["with"], u, source)))
                without = {}
                for r in roots:
                    if r["underlying"] != u:
                        continue
                    names = [m[0] for m in r["moves"]]
                    days = {fno.parse_day_folder(n)[0]: n for n in names}
                    (without.update(days) if r["to"] == to["without"] else with_days.update(days))
                for day in sorted(set(without) - with_days):
                    log(f"  {u} {day}: only noZeroVol ({without[day]}): no with copy (can't be derived)")
            code = max(code, report_and_apply(roots, apply, log))
        finally:
            for lock in locks:
                lock.release()
    return code

"""Move F&O day folders from old save folders into one raw folder per source (python fno_store.py move-raw).

    <from>/NIFTY_FnO_1min/<day folder>  ->  <to>/NIFTY_FnO_1min/<day folder>    (same for SENSEX, and _Dhan)

Day folders keep their names and contents byte for byte: os.rename on the same drive, else a copy whose
file sizes are checked before the source is deleted. A day the target already has (any name form) is never
moved, replaced or deleted; it is listed ("incoming has more data" when the target is noZeroVol and the
incoming folder withZeroVol). Per root, expiries_seen.csv (union, earliest first_seen) and holidays.log
(new lines) are merged, and the progress.sqlite rows of every moved day (run key fno:/fno-dhan:) are copied
into <to>/progress.sqlite, so a resume skips contracts that were DONE without a file. Dry run by default."""
import csv
import os
import shutil
import sqlite3

from . import fno, fno_days
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


def plan(froms, source, to):
    """The moves, duplicates and merges for every root of source in froms (processed in order: a later
    folder sees the earlier ones' days as already in the target). Raises DayFolderError on duplicate days."""
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
                                     "holidays": set(read_lines(os.path.join(dst_root, "holidays.log")))}
            t = targets[dst_root]
            r = {"from": frm, "underlying": u, "src": src_root, "dst": dst_root, "moves": [], "dups": [],
                 "expiries": [], "holidays": [], "progress": 0}
            for day, name in sorted(fno.day_folders(src_root).items()):
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
            new = [line for line in read_lines(os.path.join(src_root, "holidays.log")) if line not in t["holidays"]]
            t["holidays"].update(new)
            r["holidays"] = new
            r["empty_after"] = len(r["moves"]) == len(fno.day_folders(src_root))
            roots.append(r)
    return roots, targets


def move_raw(froms, source, to, apply=False, log=print):
    """Print (dry run) or do (apply) the moves; see the module notes. Returns the exit code: 0 done or dry
    run, 1 not run or stopped by an error."""
    if not to:
        log(f"Not run: give --to or set {('FNO_RAW_KITE', 'FNO_RAW_DHAN')[source == 'dhan']} in .env.")
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
    locks = []
    try:
        dsts = sorted({fno.fno_root(to, u, source) for u in sorted(fno.UNDERLYINGS)
                       if any(os.path.isdir(fno.fno_root(f, u, source)) for f in froms)})
        for root in srcs + dsts:
            if not apply and not os.path.exists(os.path.join(root, ".lock")):
                continue   # a dry run creates nothing; a root without a .lock has never been run, so it can't be busy
            try:
                locks.append(fno.RunLock(os.path.join(root, ".lock")).acquire("fno_store.py move-raw"))
            except fno.LockBusy as e:
                log(f"Not run: {e}")
                return 1
        try:
            roots, _ = plan(froms, source, to)
        except fno.DayFolderError as e:
            log(f"Not run: {e}")
            return 1
        return report_and_apply(roots, to, apply, log)
    finally:
        for lock in locks:
            lock.release()


def report_and_apply(roots, to, apply, log):
    n_move = n_dup = n_more = n_rows = 0
    for r in roots:
        log(f"{r['src']} -> {r['dst']}:")
        for name, *_ in r["moves"]:
            log(f"  {os.path.join(r['src'], name)} -> {os.path.join(r['dst'], name)}")
        for name, why, more in r["dups"]:
            log(f"  DUPLICATE {name}: {why}")
        log(f"  merge: +{len(r['expiries'])} expiries in {fno.EXPIRIES_FILE}, +{len(r['holidays'])} holidays.log lines, "
            f"+{r['progress']:,} progress.sqlite rows")
        if r["empty_after"] and r["moves"]:
            log("  the old root has no day folders left after this; it is left in place (not deleted)")
        n_move += len(r["moves"])
        n_dup += len(r["dups"])
        n_more += sum(1 for d in r["dups"] if d[2])
        n_rows += r["progress"]
    log(f"Totals: {n_move} day folders to move, {n_dup} duplicates not moved"
        + (f" ({n_more} where the incoming folder has more data)" if n_more else "") + f", {n_rows:,} progress rows.")
    if not apply:
        log("Dry run: nothing moved. Run again with --apply to move.")
        return 0
    done, touched = [], []
    db = None
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
                have = set(read_lines(os.path.join(r["dst"], "holidays.log")))
                with open(os.path.join(r["dst"], "holidays.log"), "a", encoding="utf-8") as f:
                    f.writelines(line + "\n" for line in r["holidays"] if line not in have)
            for name, run, cols, rows in r["moves"]:
                src, dst = os.path.join(r["src"], name), os.path.join(r["dst"], name)
                move_day(src, dst)
                done.append((src, dst))
                for root in (r["dst"], r["src"]):
                    if root not in touched:
                        touched.append(root)
                if rows:
                    db = db or open_db(to)
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
    for r in roots:
        if os.path.isdir(r["src"]) and not fno.day_folders(r["src"]):
            log(f"{r['src']} has no day folders left; it is left in place (not deleted).")
    return 0

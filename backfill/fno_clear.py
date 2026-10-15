"""Clear one underlying's F&O data from the active stores (python fno_store.py clear), and put it back (restore).

    python fno_store.py clear --underlying SENSEX [--source kite|dhan|all] [--variant with|without|all]
                              [--no-backtest] [--archive-to PATH] [--apply] [--delete] [--yes]
    python fno_store.py restore --from "<archive folder>" [--apply]

What is cleared, for each root of the underlying in scope (<FNO_RAW_<SOURCE>>\\<with|without>_zero_volume\\<ROOT>, and
with --variant all the old-layout <FNO_RAW_<SOURCE>>\\<ROOT>): every day folder, the _DAYS-*.txt marker, days_index.csv
and bhavcopy.log. Kept in place: expiries_seen.csv (the date window needs the expiry history), holidays.log (known
holidays are skipped) and .lock. The underlying's progress.sqlite rows (run keys fno:<U>: / fno-dhan:<U>:, range keys
too) are removed from every save folder in scope: otherwise a fresh run would skip contracts that were DONE without a
file and the new download would be incomplete. Rows of other underlyings and of equity stay. Unless --no-backtest,
<FNO_BACKTEST_DIR>\\<variant>\\<U> (and the old-layout <FNO_BACKTEST_DIR>\\<U>) go too, so the next publish rebuilds them.

Nothing is deleted by default: everything moves into <home>\\archive\\<YYYY-MM-DD_HHMM IST>_clear_<U>\\ under its path
relative to <home> (the common folder of the .env bases, e.g. D:\\Data\\fno), the rows into progress_<u>.sqlite next to
it, with clear_manifest.json (read by restore) and CLEARED.txt. Days outside the download window can never be
downloaded again, so the dry run says which cleared days are lost for good. --apply --delete removes instead (typed
confirmation, interactive terminal only). Dry run by default; run by hand."""
import json
import os
import shutil
import sqlite3
import sys
from datetime import date

from config import FNO_RAW_KEYS, VARIANTS, weekly_expiry_weekday

from . import fno, fno_days
from .fno_store import read_expiries_file, same_device, take_locks
from .job import JOB, JOB_LOCK
from .storage import open_db

MANIFEST = "clear_manifest.json"
CLEARED = "CLEARED.txt"
INDEX = fno_days.INDEX_FILE
CLEAR_FILES = (INDEX, "bhavcopy.log")


def is_marker(name):
    return bool(fno_days.MARKER_RE.fullmatch(name)) or name == INDEX


def prefix(underlying, source):
    """The start of every run key of the underlying and source (fno.run_key; range keys too)."""
    return f"{'fno' if source == 'kite' else f'fno-{source}'}:{underlying}:"


def count_rows(db_path, prefixes):
    if not os.path.exists(db_path):
        return 0
    db = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        if not db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='chunks'").fetchone():
            return 0
        return sum(db.execute("SELECT COUNT(*) FROM chunks WHERE substr(run, 1, ?) = ?", (len(p), p)).fetchone()[0]
                   for p in prefixes)
    finally:
        db.close()


def home_of(env):
    """The common folder of the set .env bases (FNO_RAW_KITE, FNO_RAW_DHAN, FNO_BACKTEST_DIR), or None."""
    bases = [os.path.normpath(v) for k in (*FNO_RAW_KEYS.values(), "FNO_BACKTEST_DIR") if (v := env.get(k))]
    if not bases:
        return None
    try:
        home = os.path.commonpath(bases)
    except ValueError:   # different drives
        return None
    return None if os.path.dirname(home) == home else home   # never a drive root


def rel_to(path, home):
    path = os.path.normpath(path)
    if home and os.path.normcase(path).startswith(os.path.normcase(home) + os.sep):
        return os.path.relpath(path, home)
    drive, rest = os.path.splitdrive(path)
    return os.path.join(drive.rstrip(":") or "root", rest.lstrip("\\/"))


def plan_clear(underlying, sources, variants, backtest, env):
    """{"roots": [...], "dbs": [...], "backtest": [...]} of everything in scope (reads only)."""
    roots, dbs, bt = [], {}, []
    for source in sources:
        base = os.path.normpath(env[FNO_RAW_KEYS[source]])
        saves = [os.path.join(base, VARIANTS[v]) for v in variants]
        if len(variants) == len(VARIANTS):
            saves.append(base)   # the old-layout root and progress.sqlite of the base
        for save in saves:
            root = fno.fno_root(save, underlying, source)
            if os.path.isdir(root):
                days = fno.day_folders(root)
                items, kept = [], []
                for n in sorted(os.listdir(root)):
                    path = os.path.join(root, n)
                    parsed = fno.parse_day_folder(n)
                    if (parsed and os.path.isdir(path)) or (n.endswith(".part") and os.path.isdir(path)) \
                            or fno_days.MARKER_RE.fullmatch(n) or n in CLEAR_FILES:
                        items.append(path)
                    else:
                        kept.append(n)
                roots.append({"root": root, "source": source, "items": items, "kept": kept, "days": sorted(days)})
            db = os.path.join(save, "progress.sqlite")
            dbs.setdefault(db, set()).add(prefix(underlying, source))
    db_list = [{"live": p, "prefixes": sorted(px), "rows": count_rows(p, px)} for p, px in dbs.items()]
    if backtest and env.get("FNO_BACKTEST_DIR"):
        out = os.path.normpath(env["FNO_BACKTEST_DIR"])
        cands = [os.path.join(out, VARIANTS[v], underlying) for v in variants]
        if len(variants) == len(VARIANTS):
            cands.append(os.path.join(out, underlying))
        bt = [p for p in cands if os.path.isdir(p)]
    return {"roots": roots, "dbs": [d for d in db_list if d["rows"]], "backtest": bt}


def window_lines(underlying, roots, today=None, also=(), also_label="Correct data"):
    """(lines, lost days): which cleared days can still be downloaded, from the saved expiry history (no login).
    also: days that exist elsewhere too (fno_store.py import: the days being copied in), marked in the lost list."""
    days = sorted({d for r in roots for d in r["days"]})
    if not days:
        return [], []
    today = today or fno.today_ist()
    history = set()
    for r in roots:
        history.update(date.fromisoformat(e) for e in read_expiries_file(r["root"]))
    live = sorted(e for e in history if e >= today)
    if not live:
        lost = days
        lines = ["No live expiry in the saved expiry history, so the download window is unknown: treating every "
                 "cleared day as one that cannot be downloaded again."]
    else:
        w = fno.date_window(live, history, today, weekly_expiry_weekday(underlying))
        lost = [d for d in days if fno.window_error(d, w)]
        lines = [f"{underlying} download window today ({today}, IST): {w.min_date} to {w.max_date}"
                 + (" (start estimated)" if w.estimated else "") + "."]
    ok = [d for d in days if d not in lost]
    lines.append("Can be downloaded again: " + (", ".join(map(str, ok)) or "none") + ".")
    if lost:
        if also:
            lines.append("Cannot be downloaded again (outside the window): "
                         + ", ".join(f"{d} (also in {also_label})" if d in also else f"{d} (archive only)" for d in lost)
                         + f" - they will exist only in the archive" + (f" and in {also_label}" if any(d in also for d in lost) else "") + ".")
        else:
            lines.append("Cannot be downloaded again (outside the window): " + ", ".join(map(str, lost))
                         + " - they will exist only in the archive.")
    return lines, lost


def write_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=1)
    os.replace(tmp, path)


def write_cleared(archive, man, window):
    lines = [f"Cleared {man['underlying']} F&O data on {man['created']} IST",
             f"Command: {man['command']}", "", *window, "", f"Moved items ({len(man['items'])}):"]
    lines += [f"  {i['src']} -> {i['dst']}" for i in man["items"]]
    lines += ["", "Progress rows (removed from the live databases, kept in the archive):"]
    lines += [f"  {d['live']}: {d['rows']:,} rows -> {d['archive']}" for d in man["dbs"]]
    lines += ["", "Kept in place: expiries_seen.csv, holidays.log and .lock of every root; a fresh _DAYS-0.txt and "
                  "empty days_index.csv were written.",
              "", "To put everything back (refused for any path that exists again):",
              f'  python fno_store.py restore --from "{archive}"  (dry run; add --apply)']
    path = os.path.join(archive, CLEARED)
    with open(path + ".tmp", "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(lines) + "\n")
    os.replace(path + ".tmp", path)


def move_item(src, dst):
    """src (folder or file) to dst: a rename on the same drive, else copy, sizes checked, source removed."""
    from .fno_store import move_day   # same_device -> os.rename, else copy_verified
    if os.path.exists(dst):
        raise OSError(f"{dst} already exists")
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    if os.path.isdir(src):
        move_day(src, dst)
    elif same_device(src, os.path.dirname(dst)):
        os.rename(src, dst)
    else:
        shutil.copy2(src, dst + ".part")
        if os.path.getsize(src) != os.path.getsize(dst + ".part"):
            os.remove(dst + ".part")
            raise OSError(f"copy of {src} does not match (size); the source is kept")
        os.rename(dst + ".part", dst)
        os.remove(src)


def remove_item(path):
    shutil.rmtree(path) if os.path.isdir(path) else os.remove(path)


def copy_rows(live, archive_db, prefixes):
    """The live db's rows of the prefixes into archive_db (created with the chunks schema)."""
    src = sqlite3.connect(f"file:{live}?mode=ro", uri=True)
    try:
        cur = src.execute("SELECT * FROM chunks WHERE " + " OR ".join("substr(run, 1, ?) = ?" for _ in prefixes),
                          [x for p in prefixes for x in (len(p), p)])
        cols, rows = [d[0] for d in cur.description], cur.fetchall()
    finally:
        src.close()
    os.makedirs(os.path.dirname(archive_db), exist_ok=True)
    db = sqlite3.connect(archive_db)
    try:
        db.execute("""CREATE TABLE IF NOT EXISTS chunks (
            run TEXT, symbol TEXT, token INTEGER, chunk_from TEXT, chunk_to TEXT,
            status TEXT, tries INTEGER DEFAULT 0, error TEXT, updated TEXT, reason TEXT,
            PRIMARY KEY (run, symbol, chunk_from))""")
        have = {c[1] for c in db.execute("PRAGMA table_info(chunks)")}
        use = [c for c in cols if c in have]
        idx = [cols.index(c) for c in use]
        db.executemany(f"INSERT OR REPLACE INTO chunks ({', '.join(use)}) VALUES ({', '.join('?' * len(use))})",
                       [[r[i] for i in idx] for r in rows])
        db.commit()
    finally:
        db.close()
    return len(rows)


def delete_rows(live, prefixes):
    db = sqlite3.connect(live)
    try:
        n = 0
        for p in prefixes:
            n += db.execute("DELETE FROM chunks WHERE substr(run, 1, ?) = ?", (len(p), p)).rowcount
        db.commit()
        return n
    finally:
        db.close()


def rewrite_counts(roots, log):
    for root in roots:
        if os.path.isdir(root):
            try:
                fno_days.write_day_count(root)
            except (OSError, fno.DayFolderError) as e:
                log(f"Day count of {root} not updated: {e}")


def clear(underlying, sources, variants, env, backtest=True, archive_to=None, apply=False, delete=False, yes=False,
          command="", log=print, ask=input, interactive=None):
    """See the module notes. Returns the exit code: 0 done or dry run, 1 not run, refused or stopped by an error."""
    interactive = interactive if interactive is not None else (lambda: sys.stdin.isatty())
    if not underlying:
        log("Not run: give --underlying (there is no default, so nothing is cleared by accident).")
        return 1
    if underlying not in fno.UNDERLYINGS:
        log(f"Not run: unknown underlying {underlying}.")
        return 1
    if delete and not apply:
        log("Not run: --delete goes with --apply.")
        return 1
    if delete and not interactive():
        log("Not run: --delete asks for a typed confirmation; run it in an interactive terminal.")
        return 1
    missing = [FNO_RAW_KEYS[s] for s in sources if not env.get(FNO_RAW_KEYS[s])]
    if missing:
        log(f"Not run: {', '.join(missing)} not set in .env.")
        return 1
    with JOB_LOCK:
        if JOB["state"] == "running":
            log("Not run: a download job is running. Stop it or wait for it to finish.")
            return 1
    home = home_of(env)
    archive = None
    if not delete:
        if archive_to:
            archive = os.path.normpath(archive_to)
        elif home:
            archive = os.path.join(home, "archive", f"{fno.now_ist():%Y-%m-%d_%H%M}_clear_{underlying}")
        else:
            log("Not run: the .env bases share no common folder; give --archive-to.")
            return 1
        if os.path.exists(archive):
            log(f"Not run: the archive folder {archive} already exists.")
            return 1
    try:
        scope = plan_clear(underlying, sources, variants, backtest, env)
    except fno.DayFolderError as e:
        log(f"Not run: {e}")
        return 1
    try:
        locks = take_locks([r["root"] for r in scope["roots"]], apply, f"fno_store.py clear {underlying}")
    except fno.LockBusy as e:
        log(f"Not run: {e}")
        return 1
    try:
        return run_clear(scope, underlying, env, home, archive, apply, delete, yes, command, log, ask, interactive)
    finally:
        for lock in locks:
            lock.release()


def run_clear(scope, underlying, env, home, archive, apply, delete, yes, command, log, ask, interactive, label="Clear",
              also=(), dry_tail=True):
    target = (lambda p: os.path.join(archive, rel_to(p, home))) if archive else (lambda p: "(deleted)")
    items = [(p, "item") for r in scope["roots"] for p in r["items"]] + [(p, "backtest") for p in scope["backtest"]]
    verb = "delete" if delete else "archive"
    log(f"== {label} {underlying} F&O data ({'DELETE, no archive' if delete else f'archive: {archive}'})")
    for r in scope["roots"]:
        log(f"{r['root']}:")
        for p in r["items"]:
            log(f"  {verb} {os.path.basename(p)}" + ("" if delete else f" -> {target(p)}"))
        log(f"  keep {', '.join(r['kept']) or '(nothing)'}")
    for d in scope["dbs"]:
        log(f"{d['live']}: {verb} {d['rows']:,} progress rows ({' / '.join(d['prefixes'])}*)"
            + ("" if delete else f" -> {target(os.path.join(os.path.dirname(d['live']), f'progress_{underlying.lower()}.sqlite'))}"))
    for p in scope["backtest"]:
        log(f"backtest: {verb} {p}" + ("" if delete else f" -> {target(p)}"))
    if scope["backtest"]:
        log("  (the backtest folders hold every broker; the next publish rebuilds them from the raw data that is left)")
    window, lost = window_lines(underlying, scope["roots"], also=also)
    for line in window:
        log(line)
    n_rows = sum(d["rows"] for d in scope["dbs"])
    log(f"Totals: {len(items)} folders/files, {n_rows:,} progress rows in {len(scope['dbs'])} databases.")
    if not items and not n_rows:
        log("Nothing to clear.")
        return 0
    if not apply:
        if dry_tail:
            log("Dry run: nothing changed. Run again with --apply to " + verb + ".")
        return 0
    if lost and not yes:
        if not interactive():
            log("Not run: some cleared days cannot be downloaded again; confirm with --yes or run interactively.")
            return 1
        if ask(f"{len(lost)} cleared days cannot be downloaded again. Continue? [y/N] ").strip().lower() != "y":
            log("Not run: not confirmed.")
            return 1
    if delete:
        word = f"DELETE {underlying}"
        if ask(f"This permanently deletes everything listed above. Type {word} to continue: ").strip() != word:
            log("Not run: not confirmed.")
            return 1
    from .fno import now_ist
    man = {"underlying": underlying, "created": f"{now_ist():%Y-%m-%d %H:%M:%S}", "command": command, "home": home,
           "roots": [r["root"] for r in scope["roots"]], "items": [], "dbs": []}
    done, touched = [], [r["root"] for r in scope["roots"]]
    try:
        if not delete:
            os.makedirs(archive)
            write_json(os.path.join(archive, MANIFEST), man)
            for d in scope["dbs"]:
                arch = target(os.path.join(os.path.dirname(d["live"]), f"progress_{underlying.lower()}.sqlite"))
                rows = copy_rows(d["live"], arch, d["prefixes"])
                man["dbs"].append({"live": d["live"], "archive": arch, "prefixes": d["prefixes"], "rows": rows})
                write_json(os.path.join(archive, MANIFEST), man)
        for p, kind in items:
            if delete:
                remove_item(p)
            else:
                move_item(p, target(p))
                man["items"].append({"src": p, "dst": target(p), "kind": kind})
                write_json(os.path.join(archive, MANIFEST), man)
            done.append(p)
        for d in scope["dbs"]:
            delete_rows(d["live"], d["prefixes"])
    except (OSError, sqlite3.Error) as e:
        log(f"Stopped: {type(e).__name__}: {e}")
        log(f"{'Deleted' if delete else 'Moved'} before the error ({len(done)}):" + "".join(f"\n  {p}" for p in done))
        log("Progress rows were not removed." + ("" if delete else f" Restore with: python fno_store.py restore --from \"{archive}\""))
        return 1
    finally:
        rewrite_counts(touched, log)
        if not delete and os.path.isdir(archive):
            write_cleared(archive, man, window)
    log(f"{'Deleted' if delete else 'Archived'} {len(done)} folders/files and {n_rows:,} progress rows."
        + ("" if delete else f" Archive: {archive} (see {CLEARED})"))
    return 0


def restore(archive, apply=False, log=print):
    """Put a clear's archive back (dry run by default). Items whose original path exists again are refused and
    listed; markers are rewritten, not restored; progress rows go back per run key only where the live database has
    none. Returns 0 when everything is (or would be) restored, else 1."""
    path = os.path.join(archive or "", MANIFEST)
    if not archive or not os.path.exists(path):
        log(f"Not run: {path} not found (give the archive folder that clear wrote).")
        return 1
    with open(path, encoding="utf-8") as f:
        man = json.load(f)
    roots = [r for r in man["roots"] if os.path.isdir(r)]
    try:
        locks = take_locks(roots, apply, "fno_store.py restore")
    except fno.LockBusy as e:
        log(f"Not run: {e}")
        return 1
    code, moved = 0, 0
    try:
        log(f"== Restore {man['underlying']} from {archive}")
        todo = []
        for i in man["items"]:
            name = os.path.basename(i["src"])
            if i["kind"] == "item" and is_marker(name):
                log(f"  skip {i['src']}: day count files are rewritten, not restored")
            elif not os.path.exists(i["dst"]):
                log(f"  REFUSED {i['src']}: not in the archive ({i['dst']})")
                code = 1
            elif os.path.exists(i["src"]):
                log(f"  REFUSED {i['src']}: the path exists again (a fresh download?); not overwritten")
                code = 1
            else:
                log(f"  restore {i['dst']} -> {i['src']}")
                todo.append(i)
        rows_plan = []
        for d in man["dbs"]:
            if not os.path.exists(d["archive"]):
                log(f"  REFUSED rows for {d['live']}: {d['archive']} is missing")
                code = 1
                continue
            src = sqlite3.connect(f"file:{d['archive']}?mode=ro", uri=True)
            try:
                runs = [r[0] for r in src.execute("SELECT DISTINCT run FROM chunks ORDER BY run")]
            finally:
                src.close()
            have, same = set(), set()
            if os.path.exists(d["live"]):
                key = "SELECT symbol, chunk_from, status, reason FROM chunks WHERE run = ?"
                live = sqlite3.connect(f"file:{d['live']}?mode=ro", uri=True)
                src = sqlite3.connect(f"file:{d['archive']}?mode=ro", uri=True)
                try:
                    if live.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='chunks'").fetchone():
                        for r in runs:
                            now = set(live.execute(key, (r,)).fetchall())
                            if now:
                                # the rows were never removed (a clear stopped by an error): nothing to put back
                                (same if set(src.execute(key, (r,)).fetchall()) <= now else have).add(r)
                finally:
                    live.close()
                    src.close()
            if same:
                log(f"  {len(same)} run keys of progress rows are still in {d['live']}; nothing to put back")
            for r in sorted(have):
                log(f"  REFUSED rows of {r} in {d['live']}: the live database has other rows for it now")
                code = 1
            have |= same
            keep = [r for r in runs if r not in have]
            if keep:
                log(f"  restore {len(keep)} run keys of progress rows into {d['live']}")
                rows_plan.append((d, keep))
        if not apply:
            log("Dry run: nothing changed. Run again with --apply to restore.")
            return code
        for i in todo:
            move_item(i["dst"], i["src"])
            moved += 1
        for d, keep in rows_plan:
            src = sqlite3.connect(f"file:{d['archive']}?mode=ro", uri=True)
            try:
                cur = src.execute(f"SELECT * FROM chunks WHERE run IN ({', '.join('?' * len(keep))})", keep)
                cols, rows = [c[0] for c in cur.description], cur.fetchall()
            finally:
                src.close()
            db = open_db(os.path.dirname(d["live"]))
            try:
                have = {c[1] for c in db.execute("PRAGMA table_info(chunks)")}
                use = [c for c in cols if c in have]
                idx = [cols.index(c) for c in use]
                db.executemany(f"INSERT OR IGNORE INTO chunks ({', '.join(use)}) VALUES ({', '.join('?' * len(use))})",
                               [[r[i] for i in idx] for r in rows])
                db.commit()
            finally:
                db.close()
    except (OSError, sqlite3.Error) as e:
        log(f"Stopped: {type(e).__name__}: {e} ({moved} items restored before the error)")
        return 1
    finally:
        if apply:
            rewrite_counts(man["roots"], log)
        for lock in locks:
            lock.release()
    log(f"Restored {moved} items.")
    return code

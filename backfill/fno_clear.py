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
confirmation, interactive terminal only). Dry run by default; run by hand.

With a date filter (--date D, repeatable, or --from / --to) only those days go: their day folders (and .part
folders), the progress rows of exactly their run keys (fno.run_key; range keys stay), and in the backtest store of
each variant in scope only those days of the sources in scope (minute\\<source>\\<YYYY>\\<date>.parquet archived,
the day's publish_manifest.json entry for that source dropped and days.csv rewritten; a copy of both is kept in the
archive; EOD, contracts.parquet, expiries.csv and the other source stay, the next publish rebuilds the rest). Every
other file of a root stays; the day count (_DAYS-*.txt, days_index.csv) is rewritten."""
import json
import os
import shutil
import sqlite3
import sys
from datetime import date
from typing import Callable, NamedTuple

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


class DayFilter(NamedTuple):
    keep: Callable    # date -> bool
    label: str        # "2026-10-16" / "2026-10-14 to 2026-10-16" / "from 2026-10-14"


def day_filter(dates=(), date_from=None, date_to=None):
    """The days clear keeps to (--date, repeatable, or --from / --to), or None: the whole underlying."""
    if dates:
        dates = set(dates)
        return DayFilter(lambda d: d in dates, ", ".join(map(str, sorted(dates))))
    if date_from or date_to:
        label = (f"{date_from} to {date_to}" if date_from and date_to
                 else f"from {date_from}" if date_from else f"up to {date_to}")
        return DayFilter(lambda d: (not date_from or d >= date_from) and (not date_to or d <= date_to), label)
    return None


def run_day(run):
    """The trading day of a per-day run key (fno.run_key), else None (range / missing-days keys)."""
    try:
        return date.fromisoformat(run.rsplit(":", 1)[1])
    except (IndexError, ValueError):
        return None


def match(prefixes, runs=()):
    """(SQL condition, params): exactly the run keys runs when given (a date filter), else every key that starts
    with one of the prefixes."""
    if runs:
        return f"run IN ({', '.join('?' * len(runs))})", list(runs)
    return " OR ".join("substr(run, 1, ?) = ?" for _ in prefixes), [x for p in prefixes for x in (len(p), p)]


def has_chunks(db):
    return bool(db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='chunks'").fetchone())


def day_runs(db_path, prefixes, keep):
    """The per-day run keys of the prefixes in db_path whose day passes keep (sorted)."""
    if not os.path.exists(db_path):
        return []
    db = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        if not has_chunks(db):
            return []
        cond, params = match(prefixes)
        found = [r[0] for r in db.execute(f"SELECT DISTINCT run FROM chunks WHERE {cond}", params)]
    finally:
        db.close()
    return sorted(r for r in found if (d := run_day(r)) and keep(d))


def count_rows(db_path, prefixes, runs=()):
    if not os.path.exists(db_path):
        return 0
    db = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        if not has_chunks(db):
            return 0
        if runs:
            cond, params = match(prefixes, runs)
            return db.execute(f"SELECT COUNT(*) FROM chunks WHERE {cond}", params).fetchone()[0]
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


def in_filter(name, path, days):
    """True for a day folder (or its .part) of a day the filter keeps."""
    parsed = fno.parse_day_folder(name[:-len(".part")] if name.endswith(".part") else name)
    return bool(parsed and os.path.isdir(path) and days.keep(parsed[0]))


def plan_clear(underlying, sources, variants, backtest, env, days=None):
    """{"roots": [...], "dbs": [...], "backtest": [...], "backtest_days": [...]} of everything in scope (reads
    only). days (day_filter) limits it to those days: see the module notes."""
    roots, dbs, bt, bt_days = [], {}, [], []
    for source in sources:
        base = os.path.normpath(env[FNO_RAW_KEYS[source]])
        saves = [os.path.join(base, VARIANTS[v]) for v in variants]
        if len(variants) == len(VARIANTS):
            saves.append(base)   # the old-layout root and progress.sqlite of the base
        for save in saves:
            root = fno.fno_root(save, underlying, source)
            if os.path.isdir(root):
                found = fno.day_folders(root)
                items, kept = [], []
                for n in sorted(os.listdir(root)):
                    path = os.path.join(root, n)
                    if days:   # only the filtered days' folders; markers are rewritten, the rest stays
                        if in_filter(n, path, days):
                            items.append(path)
                        else:
                            kept.append(n)
                        continue
                    parsed = fno.parse_day_folder(n)
                    if (parsed and os.path.isdir(path)) or (n.endswith(".part") and os.path.isdir(path)) \
                            or fno_days.MARKER_RE.fullmatch(n) or n in CLEAR_FILES:
                        items.append(path)
                    else:
                        kept.append(n)
                roots.append({"root": root, "source": source, "items": items, "kept": kept,
                              "days": sorted(d for d in found if not days or days.keep(d))})
            db = os.path.join(save, "progress.sqlite")
            dbs.setdefault(db, set()).add(prefix(underlying, source))
    db_list = []
    for p, px in dbs.items():
        if days:
            runs = day_runs(p, px, days.keep)
            db_list.append({"live": p, "prefixes": sorted(px), "runs": runs, "rows": count_rows(p, px, runs) if runs else 0})
        else:
            db_list.append({"live": p, "prefixes": sorted(px), "rows": count_rows(p, px)})
    if backtest and env.get("FNO_BACKTEST_DIR"):
        out = os.path.normpath(env["FNO_BACKTEST_DIR"])
        if days:
            bt_days = published_days(out, underlying, sources, variants, days)
        else:
            cands = [os.path.join(out, VARIANTS[v], underlying) for v in variants]
            if len(variants) == len(VARIANTS):
                cands.append(os.path.join(out, underlying))
            bt = [p for p in cands if os.path.isdir(p)]
    return {"roots": roots, "dbs": [d for d in db_list if d["rows"]], "backtest": bt, "backtest_days": bt_days}


def published_days(out, underlying, sources, variants, days):
    """[{"store", "date", "source", "file" (the minute parquet or None), "entry" (in the manifest)}] of the filtered
    days of the sources in the backtest store of each variant (<out>\\<variant>\\<U>; never the old layout)."""
    from .fno_publish import load_manifest, minute_path
    found = []
    for v in variants:
        out_u = os.path.join(out, VARIANTS[v], underlying)
        if not os.path.isdir(out_u):
            continue
        man = load_manifest(out_u)["days"]
        for source in sources:
            dates = {d for d, e in man.items() if source in e}
            folder = os.path.join(out_u, "minute", source)
            if os.path.isdir(folder):
                for year in os.listdir(folder):
                    for n in os.listdir(os.path.join(folder, year)) if os.path.isdir(os.path.join(folder, year)) else []:
                        if n.endswith(".parquet"):
                            dates.add(n[:-len(".parquet")])
            for d in sorted(dates):
                try:
                    if not days.keep(date.fromisoformat(d)):
                        continue
                except ValueError:
                    continue
                path = minute_path(out_u, source, d)
                found.append({"store": out_u, "date": d, "source": source,
                              "file": path if os.path.exists(path) else None, "entry": source in man.get(d, {})})
    return found


def drop_published(out_u, pairs):
    """Remove the (date, source) pairs from out_u's publish_manifest.json and rewrite days.csv from it (the
    minute files are moved by the caller). A day left without any source goes from both."""
    from .fno_publish import MANIFEST as PUBLISHED, SOURCES, _json, load_manifest, write_atomic, write_days
    man = load_manifest(out_u)
    for day, source in pairs:
        man["days"].get(day, {}).pop(source, None)
        if day in man["days"] and not any(s in man["days"][day] for s in SOURCES):
            del man["days"][day]
    write_days(out_u, man["days"], "kite")
    write_atomic(os.path.join(out_u, PUBLISHED), lambda tmp: _json(tmp, man))


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
    days = man.get("days")
    lines = [f"Cleared {man['underlying']} F&O data" + (f" of {days}" if days else "") + f" on {man['created']} IST",
             f"Command: {man['command']}", "", *window, "", f"Moved items ({len(man['items'])}):"]
    lines += [f"  {i['src']} -> {i['dst']}" for i in man["items"]]
    lines += ["", "Progress rows (removed from the live databases, kept in the archive):"]
    lines += [f"  {d['live']}: {d['rows']:,} rows -> {d['archive']}" for d in man["dbs"]]
    if man.get("backtest_edits"):
        lines += ["", "Backtest store entries removed (publish_manifest.json and days.csv rewritten; copies before "
                      "the change kept in the archive):"]
        lines += [f"  {e['store']}: {e['date']} {e['source']} (backup {e['backup']})" for e in man["backtest_edits"]]
    if days:
        lines += ["", "Kept in place: every other day folder and file of each root (expiries_seen.csv, holidays.log, "
                      ".lock, bhavcopy.log); the day count (_DAYS-*.txt, days_index.csv) was rewritten."]
    else:
        lines += ["", "Kept in place: expiries_seen.csv, holidays.log and .lock of every root; a fresh _DAYS-0.txt and "
                      "empty days_index.csv were written."]
    lines += ["", "To put everything back (refused for any path that exists again):",
              f'  python fno_store.py restore --from "{archive}"  (dry run; add --apply)']
    if man.get("backtest_edits"):
        lines.append("  then python fno_store.py publish (adds the restored days to the backtest store again)")
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


def copy_rows(live, archive_db, prefixes, runs=()):
    """The live db's rows of the prefixes (or exactly the run keys runs) into archive_db (created with the chunks
    schema)."""
    src = sqlite3.connect(f"file:{live}?mode=ro", uri=True)
    try:
        cond, params = match(prefixes, runs)
        cur = src.execute(f"SELECT * FROM chunks WHERE {cond}", params)
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


def delete_rows(live, prefixes, runs=()):
    db = sqlite3.connect(live)
    try:
        if runs:
            cond, params = match(prefixes, runs)
            n = db.execute(f"DELETE FROM chunks WHERE {cond}", params).rowcount
        else:
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
          command="", log=print, ask=input, interactive=None, days=None):
    """See the module notes; days (day_filter) clears only those days. Returns the exit code: 0 done or dry run,
    1 not run, refused or stopped by an error."""
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
        scope = plan_clear(underlying, sources, variants, backtest, env, days)
    except fno.DayFolderError as e:
        log(f"Not run: {e}")
        return 1
    try:
        locks = take_locks([r["root"] for r in scope["roots"]], apply, f"fno_store.py clear {underlying}")
    except fno.LockBusy as e:
        log(f"Not run: {e}")
        return 1
    try:
        return run_clear(scope, underlying, env, home, archive, apply, delete, yes, command, log, ask, interactive,
                         days=days)
    finally:
        for lock in locks:
            lock.release()


def run_clear(scope, underlying, env, home, archive, apply, delete, yes, command, log, ask, interactive, label="Clear",
              also=(), dry_tail=True, days=None):
    target = (lambda p: os.path.join(archive, rel_to(p, home))) if archive else (lambda p: "(deleted)")
    bt_days = scope.get("backtest_days", [])
    items = [(p, "item") for r in scope["roots"] for p in r["items"]] + [(p, "backtest") for p in scope["backtest"]]         + [(b["file"], "backtest") for b in bt_days if b["file"]]
    edits = {}   # backtest store -> [(date, source)] to drop from its manifest and days.csv
    for b in bt_days:
        if b["entry"]:
            edits.setdefault(b["store"], []).append((b["date"], b["source"]))
    verb = "delete" if delete else "archive"
    log(f"== {label} {underlying} F&O data" + (f" of {days.label}" if days else "")
        + f" ({'DELETE, no archive' if delete else f'archive: {archive}'})")
    for r in scope["roots"]:
        log(f"{r['root']}:")
        for p in r["items"]:
            log(f"  {verb} {os.path.basename(p)}" + ("" if delete else f" -> {target(p)}"))
        if days:
            log(f"  keep everything else ({len(r['kept'])} entries); the day count (_DAYS-*.txt, days_index.csv) is rewritten")
        else:
            log(f"  keep {', '.join(r['kept']) or '(nothing)'}")
    for d in scope["dbs"]:
        keys = ", ".join(d["runs"]) if d.get("runs") else " / ".join(d["prefixes"]) + "*"
        log(f"{d['live']}: {verb} {d['rows']:,} progress rows ({keys})"
            + ("" if delete else f" -> {target(os.path.join(os.path.dirname(d['live']), f'progress_{underlying.lower()}.sqlite'))}"))
    for p in scope["backtest"]:
        log(f"backtest: {verb} {p}" + ("" if delete else f" -> {target(p)}"))
    if scope["backtest"]:
        log("  (the backtest folders hold every broker; the next publish rebuilds them from the raw data that is left)")
    for b in bt_days:
        log(f"backtest {b['store']}: {b['date']} {b['source']}: "
            + (f"{verb} {b['file']}" + ("" if delete else f" -> {target(b['file'])}") if b["file"] else "no minute file")
            + ("; publish_manifest.json entry removed, days.csv rewritten" if b["entry"] else "; not in publish_manifest.json"))
    if bt_days:
        log("  (EOD, contracts.parquet, expiries.csv and the other source's data of those days stay; "
            "the next publish rebuilds what is needed)")
    window, lost = window_lines(underlying, scope["roots"], also=also)
    for line in window:
        log(line)
    n_rows = sum(d["rows"] for d in scope["dbs"])
    n_edits = sum(map(len, edits.values()))
    log(f"Totals: {len(items)} folders/files, {n_rows:,} progress rows in {len(scope['dbs'])} databases"
        + (f", {n_edits} backtest store entries" if days else "") + ".")
    if not items and not n_rows and not n_edits:
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
    if days:
        man.update({"days": days.label, "backtest_edits": []})
    done, touched = [], [r["root"] for r in scope["roots"]]
    try:
        if not delete:
            os.makedirs(archive)
            write_json(os.path.join(archive, MANIFEST), man)
            for d in scope["dbs"]:
                arch = target(os.path.join(os.path.dirname(d["live"]), f"progress_{underlying.lower()}.sqlite"))
                rows = copy_rows(d["live"], arch, d["prefixes"], d.get("runs", ()))
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
            delete_rows(d["live"], d["prefixes"], d.get("runs", ()))
        for store, pairs in edits.items():
            backup = None
            if not delete:   # the manifest and days.csv as they were, next to the archived minute files
                backup = target(store)
                os.makedirs(backup, exist_ok=True)
                for n in ("publish_manifest.json", "days.csv"):
                    if os.path.exists(os.path.join(store, n)):
                        shutil.copy2(os.path.join(store, n), os.path.join(backup, n))
            drop_published(store, pairs)
            if not delete:
                man["backtest_edits"] += [{"store": store, "date": d, "source": s, "backup": backup} for d, s in pairs]
                write_json(os.path.join(archive, MANIFEST), man)
    except (OSError, sqlite3.Error, ValueError) as e:
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
        published = sorted({(e["date"], e["source"]) for e in man.get("backtest_edits", [])})
        if published:
            log("  backtest store entries are not put back: after the restore run python fno_store.py publish, which "
                "adds " + ", ".join(f"{d} {s}" for d, s in published) + " again from the restored raw days")
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

"""Bhavcopy check of all four sets at once, "Mark reviewed" and "Bhavcopy not published" (nifty_bhav.py --missing-all /
--review / --unreview / --no-bhav / --undo-no-bhav / --reviewed-list, and the dashboard). See
docs/dev/fno_check_all_and_reviewed.md.

The records live in <F&O root>/bhav_review.json (next to bhavcopy.log and .lock; outside the day folders and the
backtest store), one file per root, written atomically by the holder of that root's lock. Events are only ever
appended: an undo is a new event, the history stays. Nothing here changes a check: severity(), list_days(),
bhav_check.json, summary.csv, publish and best_source stay as they are. A review only hides a day from the red /
amber banner while its counts are the ones reviewed; a "not published" mark only keeps a pending day out of
--missing and the yellow banner while it is still unchecked. Both apply only to the day folder they were made on
(a re-download or import gives the day a new folder name)."""
import json
import os
import re
import tempfile

import config

from . import bhav, fno

SIDECAR = "bhav_review.json"
# --missing-all and the dashboard's "Check all pending (all four)": always this order
SETS = [("NIFTY", "kite"), ("NIFTY", "dhan"), ("SENSEX", "kite"), ("SENSEX", "dhan")]
EXIT = {"ok": 0, "diffs": 1, "not_available": 4}
REVIEW_KINDS = ("review", "unreview")
NO_BHAV_KINDS = ("no_bhav", "undo_no_bhav")
NOT_PUBLISHED = "has no F&O bhavcopy for"   # in every BhavNotAvailable message (NSE 404, BSE's page)
LOG_LINE = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} (\d{4}-\d{2}-\d{2}): (.*)")


def worst(codes):
    """Differences or errors (1) over not available (4) over OK (0), as nifty_bhav.py --missing."""
    return 1 if 1 in codes else 4 if 4 in codes else 0


def now_text():
    return f"{fno.now_ist():%Y-%m-%d %H:%M:%S} IST"


# --- the sidecar --------------------------------------------------------------------------------

def sidecar_path(data_root, underlying, source="kite"):
    return os.path.join(fno.fno_root(data_root, underlying, source), SIDECAR)


def load_events(data_root, underlying, source="kite"):
    """Every event of the root, oldest first ([] without a file). Reads only."""
    path = sidecar_path(data_root, underlying, source)
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as f:
        return json.load(f).get("events", [])


def append_event(data_root, underlying, source, event):
    """Add one event: the whole file is written to a temp file in the same folder, then replaces the old one,
    so a stopped run leaves the old file or the new one, never half a file. The caller holds the root's lock."""
    path = sidecar_path(data_root, underlying, source)
    events = load_events(data_root, underlying, source) + [event]
    folder = os.path.dirname(path)
    os.makedirs(folder, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=SIDECAR + ".", suffix=".tmp", dir=folder)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
            json.dump({"version": 1, "underlying": underlying, "source": source, "events": events}, f, indent=2)
            f.write("\n")
        os.replace(tmp, path)
    except BaseException:
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise
    return event


def last_event(events, day, kinds):
    for e in reversed(events):
        if e.get("date") == day and e.get("kind") in kinds:
            return e
    return None


def review_state(events, d):
    """State of the day's review for a list_days() entry d ({"date"} only: the day is not saved): "" (never
    reviewed), "active", "void" (counts changed since), "superseded" (another day folder now, or not saved) or "undone"."""
    e = last_event(events, d["date"], REVIEW_KINDS)
    if not e:
        return ""
    if e["kind"] == "unreview":
        return "undone"
    if e.get("folder") != d.get("folder"):
        return "superseded"
    if d["status"] != "diffs" or e.get("fingerprint") != d.get("counts"):
        return "void"
    return "active"


def no_bhav_state(events, d):
    """State of the day's "bhavcopy not published" mark: "", "active" (still pending, same folder),
    "superseded" (checked since, e.g. with --bhav-file, another folder, or not saved) or "undone"."""
    e = last_event(events, d["date"], NO_BHAV_KINDS)
    if not e:
        return ""
    if e["kind"] == "undo_no_bhav":
        return "undone"
    if e.get("folder") != d.get("folder") or d.get("status") != "pending":
        return "superseded"
    return "active"


def fetch_lines(root):
    """{date ISO: the last bhavcopy.log line about it} of an F&O root."""
    out = {}
    path = os.path.join(root, "bhavcopy.log")
    if os.path.exists(path):
        with open(path, encoding="utf-8", errors="replace") as f:
            for line in f:
                m = LOG_LINE.fullmatch(line.rstrip("\n"))
                if m:
                    out[m.group(1)] = line.rstrip("\n")
    return out


# --- what the dashboard and --missing see -------------------------------------------------------

def pending_days(data_root, underlying="NIFTY", source="kite"):
    """(days to check, days left out because marked "bhavcopy not published"): bhav.unchecked_days split by the
    marks. Reads only; raises DayFolderError like unchecked_days."""
    days = bhav.unchecked_days(data_root, underlying, source)
    if not days:
        return [], []
    events = load_events(data_root, underlying, source)
    folders = fno.day_folders(fno.fno_root(data_root, underlying, source))
    marked = [d for d in days
              if no_bhav_state(events, {"date": d.isoformat(), "folder": folders[d], "status": "pending"}) == "active"]
    return [d for d in days if d not in marked], marked


def banners(days, today, events):
    """bhav.banners() without the actively reviewed days (red and amber) and the actively marked pending days."""
    out = bhav.banners(days, today)
    by = {d["date"]: d for d in days}
    reviewed = {x for x in out["check"] + out["minor"] if review_state(events, by[x]) == "active"}
    marked = {x for x in out["pending_old"] if no_bhav_state(events, by[x]) == "active"}
    return {"check": [x for x in out["check"] if x not in reviewed],
            "minor": [x for x in out["minor"] if x not in reviewed],
            "pending_old": [x for x in out["pending_old"] if x not in marked]}


def no_bhav_why(d, today, events, lines):
    """"" when the list_days() entry d may be marked "bhavcopy not published" in its root, else why not."""
    if d is None:
        return "not saved in this store"
    if d["date"] >= today.isoformat():
        return f"{d['date']} is not a past day (today is {today}); the bhavcopy may still come"
    if d["status"] != "pending":
        return "already checked against the bhavcopy"
    if no_bhav_state(events, d) == "active":
        return "already marked"
    line = lines.get(d["date"])
    if not line:
        return "no bhavcopy fetch logged for this day in bhavcopy.log; run the check first"
    if NOT_PUBLISHED not in line:
        return f"the last logged bhavcopy fetch was not a 404: {line}"
    return ""


def marks(data_root, underlying, source, today):
    """The dashboard's view of the root's records: {"reviewed": dates hidden from the red / amber banner,
    "no_bhav": marked pending days, "days": {date: {review, review_note, no_bhav, no_bhav_note, no_bhav_why}}}.
    Reads only."""
    days = bhav.list_days(data_root, underlying, source)
    events = load_events(data_root, underlying, source)
    lines = fetch_lines(fno.fno_root(data_root, underlying, source))
    base = bhav.banners(days, today)
    out = {"reviewed": [], "no_bhav": [], "days": {}}
    for d in days:
        r, nb = review_state(events, d), no_bhav_state(events, d)
        info = {"review": r, "no_bhav": nb}
        if r:
            info["review_note"] = last_event(events, d["date"], REVIEW_KINDS).get("note", "")
        if nb:
            info["no_bhav_note"] = last_event(events, d["date"], NO_BHAV_KINDS).get("note", "")
        if d["status"] == "pending":
            info["no_bhav_why"] = no_bhav_why(d, today, events, lines)
        if d["status"] == "diffs":
            info["review_why"] = review_refusal(d, source)
        out["days"][d["date"]] = info
        if r == "active" and d["date"] in base["check"] + base["minor"]:
            out["reviewed"].append(d["date"])
        if nb == "active":
            out["no_bhav"].append(d["date"])
    return out


# --- review / unreview --------------------------------------------------------------------------

def review_refusal(d, source="kite"):
    """Why the checked day d can't be reviewed ("" when it can, apart from an active review)."""
    if d is None:
        return "not saved in this folder"
    if d["status"] == "pending":
        return "not checked against the bhavcopy yet"
    if d["status"] != "diffs":
        return "the check is OK; there is nothing to review"
    missing = int(d.get("counts", {}).get("missing", 0) or 0)
    if missing:
        return (f"{missing} {'contract is' if missing == 1 else 'contracts are'} missing in {fno.BROKER[source]}'s files: "
                "re-download them while the contract is still live (B10); a review can't hide missing contracts")
    return ""


def find_day(data_root, underlying, source, day):
    return next((d for d in bhav.list_days(data_root, underlying, source) if d["date"] == day.isoformat()), None)


def review(data_root, underlying, source, day, note=""):
    """Record a review of a checked day (red or amber, Missing 0). Returns (message, error). The caller holds the lock."""
    d = find_day(data_root, underlying, source, day)
    why = review_refusal(d, source)
    if why:
        return "", f"{underlying} {fno.BROKER[source]} {day}: not reviewed: {why}."
    events = load_events(data_root, underlying, source)
    if review_state(events, d) == "active":
        return "", f"{underlying} {fno.BROKER[source]} {day}: already reviewed with these counts."
    append_event(data_root, underlying, source, {
        "kind": "review", "date": d["date"], "folder": d["folder"], "at": now_text(), "note": note,
        "severity": d["severity"], "checked_at": d.get("checked_at", ""), "fingerprint": d["counts"]})
    return (f"{underlying} {fno.BROKER[source]} {day}: reviewed. It leaves the banner while a re-check gives the "
            f"same counts. Undo with --unreview."), ""


def unreview(data_root, underlying, source, day, note=""):
    events = load_events(data_root, underlying, source)
    e = last_event(events, day.isoformat(), REVIEW_KINDS)
    if not e or e["kind"] != "review":
        return "", f"{underlying} {fno.BROKER[source]} {day}: no review to undo."
    append_event(data_root, underlying, source, {"kind": "unreview", "date": day.isoformat(), "folder": e.get("folder", ""),
                                                 "at": now_text(), "note": note})
    return f"{underlying} {fno.BROKER[source]} {day}: review undone; the day shows in the banner again.", ""


# --- bhavcopy not published -----------------------------------------------------------------------

def store_roots(underlying, env, also=()):
    """[(source, save folder)] of every store a bhavcopy check runs in: Kite and Dhan, each the with and without
    store under FNO_RAW_<SOURCE> (else the one folder), plus also (the dashboard's folder); duplicates dropped."""
    out, seen = [], set()
    pairs = []
    for source in ("kite", "dhan"):
        if config.split_stores_on(source, env):
            pairs += [(source, config.fno_raw_dir(source, v, env)) for v in config.VARIANTS]
        else:
            pairs.append((source, config.fno_raw_dir(source, env=env)))
    for source, folder in pairs + list(also):
        key = (source, os.path.normcase(os.path.normpath(folder)))
        if key not in seen:
            seen.add(key)
            out.append((source, os.path.normpath(folder)))
    return out


def no_bhav(underlying, day, roots, today, note="", undo=False, owner="nifty_bhav.py", log=print):
    """Mark (or with undo, unmark) day "bhavcopy not published" in every root where it qualifies: saved, pending,
    a past day, and its last bhavcopy.log line is the exchange's 404 / "no file". Each root takes its own lock.
    Returns the number of roots changed; every root is logged as changed or skipped with the reason."""
    done = 0
    for source, folder in roots:
        root = fno.fno_root(folder, underlying, source)
        where = f"{fno.BROKER[source]} {root}"
        if not os.path.isdir(root):
            log(f"  skipped {where}: no such folder")
            continue
        try:
            lock = fno.acquire_lock(folder, underlying, owner, source)
        except fno.LockBusy as e:
            log(f"  skipped {where}: {e}")
            continue
        try:
            d = find_day(folder, underlying, source, day)
            events = load_events(folder, underlying, source)
            if undo:
                e = last_event(events, day.isoformat(), NO_BHAV_KINDS)
                if not e or e["kind"] != "no_bhav":
                    log(f"  skipped {where}: not marked")
                    continue
                append_event(folder, underlying, source, {"kind": "undo_no_bhav", "date": day.isoformat(),
                                                          "folder": e.get("folder", ""), "at": now_text(), "note": note})
                log(f"  unmarked {where}")
            else:
                why = no_bhav_why(d, today, events, fetch_lines(root))
                if why:
                    log(f"  skipped {where}: {why}")
                    continue
                append_event(folder, underlying, source, {"kind": "no_bhav", "date": d["date"], "folder": d["folder"],
                                                          "at": now_text(), "note": note,
                                                          "fetch_line": fetch_lines(root)[d["date"]]})
                log(f"  marked {where}")
            done += 1
        except (fno.DayFolderError, OSError) as e:
            log(f"  skipped {where}: {e}")
        finally:
            lock.release()
    return done


# --- check all four -----------------------------------------------------------------------------

def default_sets(env, keep_zero=None):
    """[(underlying, source, folder)] in SETS order, on the CLIs' default folders (config.default_fno_folder)."""
    return [(u, s, config.default_fno_folder(s, keep_zero, env)) for u, s in SETS]


def check_set(folder, underlying, source, run=None, lock=None, owner="nifty_bhav.py --missing-all", log=print):
    """Check every pending day (marked days left out) of one set. lock: already held by the caller (released by
    the caller), else taken here. Returns the set's summary row; never raises for one day or a busy lock."""
    run = run or bhav.run_bhav
    row = {"underlying": underlying, "source": source, "folder": folder, "checked": 0, "still_pending": 0,
           "no_bhav_marked": 0, "failed": 0, "red": 0, "code": 0, "error": ""}

    def fail(msg):
        row.update(error=msg, code=1, failed=row["failed"] or 1)
        log(f"[bhav:{underlying}:{source}] Not run: {msg}")
        return row
    try:
        todo, marked = pending_days(folder, underlying, source)
    except (fno.DayFolderError, OSError) as e:
        return fail(str(e))
    row["no_bhav_marked"] = len(marked)
    if not todo:
        return row
    own = lock is None
    if own:
        try:
            lock = fno.acquire_lock(folder, underlying, owner, source)
        except fno.LockBusy as e:
            return fail(str(e))
    codes = []
    try:
        for d in todo:
            try:
                status, _, _ = run(d, folder, underlying, None, log=log, source=source)
            except (bhav.BhavError, fno.DayFolderError, OSError) as e:
                log(f"[bhav:{underlying}:{d}] Not checked: {e}")
                row["failed"] += 1
                codes.append(1)
                continue
            codes.append(EXIT[status])
            if status == "not_available":
                row["still_pending"] += 1
                continue
            row["checked"] += 1
            check = bhav.read_check(folder, underlying, d, source)
            if check and bhav.day_severity(check) == "major":
                row["red"] += 1
    finally:
        if own:
            lock.release()
    row["code"] = worst(codes)
    return row


def check_all(sets, run=None, locks=None, owner="nifty_bhav.py --missing-all", log=print):
    """check_set for each (underlying, source, folder) in order; one failing set never stops the next.
    locks: {(underlying, source): held lock} (the dashboard takes all four first). Returns the rows."""
    locks = locks or {}
    return [check_set(folder, u, s, run, locks.get((u, s)), owner, log) for u, s, folder in sets]


TABLE = [("underlying", "Underlying"), ("source", "Source"), ("checked", "Checked"), ("still_pending", "Still pending"),
         ("no_bhav_marked", "No-bhav marked"), ("failed", "Failed"), ("red", "Red days")]


def table(rows):
    """The --missing-all summary as fixed-width text lines."""
    cells = [[h for _, h in TABLE]] + [[str(r[k]) for k, _ in TABLE] for r in rows]
    width = [max(len(c[i]) for c in cells) for i in range(len(TABLE))]
    return ["  ".join(c[i].ljust(width[i]) for i in range(len(TABLE))).rstrip() for c in cells]


# --- listing ------------------------------------------------------------------------------------

def records(roots_by_underlying):
    """Every event of every root: [{underlying, source, root, kind, date, at, note, state}], each root's events
    oldest first. state: the current state on the latest event of a date (active / void / superseded / undone),
    "replaced" on the earlier ones. roots_by_underlying: {underlying: [(source, folder)]}. Reads only."""
    out = []
    for underlying, roots in roots_by_underlying.items():
        for source, folder in roots:
            events = load_events(folder, underlying, source)
            if not events:
                continue
            days = {d["date"]: d for d in bhav.list_days(folder, underlying, source)}
            for e in events:
                d = days.get(e["date"], {"date": e["date"]})   # {"date"} only: the day is not saved any more
                kinds = REVIEW_KINDS if e["kind"] in REVIEW_KINDS else NO_BHAV_KINDS
                if last_event(events, e["date"], kinds) is not e:
                    state = "replaced"
                elif kinds == REVIEW_KINDS:
                    state = review_state(events, d) or "superseded"
                else:
                    state = no_bhav_state(events, d) or "superseded"
                out.append({"underlying": underlying, "source": source, "root": fno.fno_root(folder, underlying, source),
                            "kind": e["kind"], "date": e["date"], "at": e.get("at", ""), "note": e.get("note", ""),
                            "state": state})
    return out

"""The day count of an F&O root, so Explorer shows how many days are saved:

    NIFTY_FnO_1min/_DAYS-5_2026-10-07_to_2026-10-13.txt   (_DAYS-0.txt when there is no day folder)
    NIFTY_FnO_1min/days_index.csv

Both list every day folder (old-style <YYYY-MM-DD> names included) with its expiry, zero-volume setting,
download time and file counts. They are rewritten after a run that creates, removes or renames a day folder,
after nifty_daily.py --rename-folders --apply and by --update-day-count; readers never write them."""
import csv
import os
import re

from . import fno

INDEX_FILE = "days_index.csv"
INDEX_COLUMNS = ["Day", "Folder", "Expiry", "Zero_volume", "Downloaded", "Futures_files", "Option_files"]
MARKER_RE = re.compile(r"_DAYS-\d+(_\d{4}-\d{2}-\d{2}_to_\d{4}-\d{2}-\d{2})?\.txt")
ZERO_LABEL = {True: "With", False: "No", None: ""}


def csv_count(folder):
    return sum(1 for n in os.listdir(folder) if n.endswith(".csv")) if os.path.isdir(folder) else 0


def day_rows(root):
    """One row per day folder, oldest first, in INDEX_COLUMNS. Raises DayFolderError on duplicates."""
    rows = []
    for day, name in sorted(fno.day_folders(root).items()):
        _, expiry, zero, created = fno.parse_day_folder(name)
        path = os.path.join(root, name)
        rows.append({"Day": day.isoformat(), "Folder": name, "Expiry": expiry.isoformat() if expiry else "",
                     "Zero_volume": ZERO_LABEL[zero], "Downloaded": f"{created:%Y-%m-%d %H:%M}" if created else "",
                     "Futures_files": csv_count(os.path.join(path, "futures")),
                     "Option_files": csv_count(os.path.join(path, "options"))})
    return rows


def summary(rows):
    """{count, first, last, with, no, old} of day rows."""
    zero = [r["Zero_volume"] for r in rows]
    return {"count": len(rows), "first": rows[0]["Day"] if rows else "", "last": rows[-1]["Day"] if rows else "",
            "with": zero.count("With"), "no": zero.count("No"), "old": zero.count("")}


def marker_name(s):
    return f"_DAYS-{s['count']}_{s['first']}_to_{s['last']}.txt" if s["count"] else "_DAYS-0.txt"


def totals_line(s):
    parts = f"withZeroVol {s['with']}, noZeroVol {s['no']}" + (f", old-style {s['old']}" if s["old"] else "")
    span = f", {s['first']} to {s['last']}" if s["count"] else ""
    return (f"{s['count']} {'day' if s['count'] == 1 else 'days'} ({parts}){span}, "
            f"updated {fno.now_ist():%Y-%m-%d %H:%M} IST")


def write_day_count(root):
    """Rewrite days_index.csv and the _DAYS-*.txt marker of root (atomically), removing older markers only.
    Returns the marker's path. The caller holds the root's lock."""
    rows = day_rows(root)
    s = summary(rows)
    os.makedirs(root, exist_ok=True)
    fno.write_csv(os.path.join(root, INDEX_FILE), INDEX_COLUMNS, rows)
    lines = [totals_line(s), "", fno.table(INDEX_COLUMNS, rows) if rows else "(no day folders)"]
    path = os.path.join(root, marker_name(s))
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(lines) + "\n")
    os.replace(tmp, path)
    for n in os.listdir(root):
        if MARKER_RE.fullmatch(n) and n != os.path.basename(path):
            os.remove(os.path.join(root, n))
    return path


def read_day_index(root):
    """Rows of root's days_index.csv, or None when it has not been written."""
    path = os.path.join(root, INDEX_FILE)
    if not os.path.exists(path):
        return None
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def day_summary(root):
    """For the dashboard (reads only): the summary from days_index.csv, else computed from the folders."""
    rows = read_day_index(root)
    return summary(rows if rows is not None else day_rows(root))

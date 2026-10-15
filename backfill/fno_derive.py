"""The "without zero volume" copy of an F&O day, derived from its "with zero volume" download.

    <raw>/<source>/with_zero_volume/NIFTY_FnO_1min/2026-10-12_exp-2026-10-13_withZeroVol_dl-2026-10-13_2307
 -> <raw>/<source>/without_zero_volume/NIFTY_FnO_1min/2026-10-12_exp-2026-10-13_noZeroVol_dl-2026-10-13_2307

A keep zero-volume OFF run writes exactly the files of an ON run except the contracts whose candles all have
volume 0 (Kept = Y in zero_volume_contracts.csv), so the without day is the with day minus those files. The
reverse is impossible: an OFF run never fetched them. A day is derived only when the without store doesn't have
it; an existing without day (a real OFF download) is never overwritten, only compared.

The copy is built in <name>.part and renamed into place; then the day's progress.sqlite rows are copied into the
without store (zero-volume contracts DONE with reason ZERO_VOLUME, so an OFF run doesn't request them again) and
the root's day count is rewritten. The callers hold both roots' locks (after_run takes the without one itself)."""
import csv
import filecmp
import glob
import os
import shutil
import tempfile
from datetime import datetime

from . import bhav, fno, fno_days
from .storage import open_db

DERIVED_FILE = "derived_from.txt"
SKIP_DIRS = {os.path.join("bhavcopy", "exports")}   # downloads of the with day's check; the copy gets its own check


class DeriveError(RuntimeError):
    pass


def file_volume(path):
    with open(path, newline="", encoding="utf-8") as f:
        return sum(float(r.get("Volume") or 0) for r in csv.DictReader(f))


def contract_files(day_dir):
    """{contract name: path} of the day's futures/ and options/ CSVs."""
    out = {}
    for sub in ("futures", "options"):
        for path in glob.glob(os.path.join(day_dir, sub, "*.csv")):
            out[os.path.splitext(os.path.basename(path))[0]] = path
    return out


def zero_volume_files(day_dir):
    """Names of the day's contract files whose total volume is 0. Raises DeriveError when they disagree with
    zero_volume_contracts.csv (Kept = Y rows must be exactly those files)."""
    zero = {name for name, path in contract_files(day_dir).items() if file_volume(path) == 0}
    path = os.path.join(day_dir, fno.ZERO_VOLUME_FILE)
    kept = set()
    if os.path.exists(path):
        with open(path, newline="", encoding="utf-8") as f:
            kept = {r["Contract"] for r in csv.DictReader(f) if (r.get("Kept") or "").strip().upper() == "Y"}
    if zero != kept:
        parts = []
        if zero - kept:
            parts.append(f"files with volume 0 not listed as kept in {fno.ZERO_VOLUME_FILE}: " + ", ".join(sorted(zero - kept)[:20]))
        if kept - zero:
            parts.append(f"listed as kept in {fno.ZERO_VOLUME_FILE} but no file with volume 0: " + ", ".join(sorted(kept - zero)[:20]))
        raise DeriveError("; ".join(parts))
    return zero


def without_name(name):
    """The without copy's folder name (withZeroVol -> noZeroVol, same exp and dl). Raises DeriveError for
    old-style or noZeroVol names."""
    parsed = fno.parse_day_folder(name)
    if not parsed:
        raise DeriveError(f"{name} is not a day folder")
    day, expiry, zero, created = parsed
    if zero is None:
        raise DeriveError(f"{name} is an old-style name (run nifty_daily.py --rename-folders first)")
    if zero is False:
        raise DeriveError(f"{name} is noZeroVol: it has no zero-volume contracts to remove")
    return fno.day_folder_name(day, expiry, False, created)


def symbols_of(day_dir, underlying, run_date):
    """{contract name: instrument row} from the day's saved instrument list."""
    dumps = sorted(glob.glob(os.path.join(day_dir, "instruments_*.csv")))
    if not dumps:
        return {}
    with open(dumps[0], newline="", encoding="utf-8") as f:
        contracts = fno.select_contracts(list(csv.DictReader(f)), underlying, run_date)
    out = {}
    for c in contracts:
        try:
            out[fno.contract_file(day_dir, c, underlying)[0]] = c
        except ValueError:
            pass
    return out


def copy_day(src, dst, skip_names):
    """Copy folder src to dst without the contract files in skip_names and without SKIP_DIRS."""
    for folder, dirs, names in os.walk(src):
        rel = os.path.relpath(folder, src)
        dirs[:] = [d for d in dirs if os.path.normpath(os.path.join(rel, d)) not in SKIP_DIRS]
        os.makedirs(os.path.join(dst, rel), exist_ok=True)
        for n in names:
            if n.endswith(".tmp"):
                continue
            if rel in ("futures", "options") and n.endswith(".csv") and n[:-4] in skip_names:
                continue
            shutil.copy2(os.path.join(folder, n), os.path.join(dst, rel, n))


def mark_not_kept(day_dir):
    path = os.path.join(day_dir, fno.ZERO_VOLUME_FILE)
    if not os.path.exists(path):
        return
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    for r in rows:
        r["Kept"] = "N"
    fno.write_csv(path, fno.ZERO_VOLUME_COLUMNS, rows)


def summary_off(day_dir):
    """summary.csv's Keep_zero_volume set to OFF (the counts are rebuilt from disk afterwards)."""
    path = os.path.join(day_dir, "summary.csv")
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        columns, rows = list(reader.fieldnames or []), list(reader)
    if "Keep_zero_volume" not in columns:
        columns.append("Keep_zero_volume")
    for r in rows:
        r["Keep_zero_volume"] = "OFF"
    fno.write_csv(path, columns, rows)


def copy_progress(with_data_root, without_data_root, underlying, source, run_date, zero, symbols):
    """The day's chunks rows of the with store into the without store's progress.sqlite (replacing the day's
    old rows there); zero-volume contracts DONE with reason ZERO_VOLUME."""
    from .fno_store import progress_rows   # fno_store imports this module
    run = fno.run_key(underlying, run_date, source)
    cols, rows = progress_rows(with_data_root, run)
    db = open_db(without_data_root)
    try:
        db.execute("DELETE FROM chunks WHERE run = ?", (run,))
        have = {c[1] for c in db.execute("PRAGMA table_info(chunks)")}
        use = [c for c in cols if c in have]
        idx = [cols.index(c) for c in use]
        if rows:
            db.executemany(f"INSERT OR REPLACE INTO chunks ({', '.join(use)}) VALUES ({', '.join('?' * len(use))})",
                           [[row[i] for i in idx] for row in rows])
        start = str(datetime.combine(run_date, fno.WINDOW[0]))
        end = str(datetime.combine(run_date, fno.WINDOW[1]))
        now = datetime.now().isoformat(timespec="seconds")
        for name in sorted(zero):
            c = symbols.get(name)
            if c is None:
                continue
            sym = c["tradingsymbol"]
            if not db.execute("SELECT 1 FROM chunks WHERE run = ? AND symbol = ?", (run, sym)).fetchone():
                db.execute("INSERT INTO chunks (run, symbol, token, chunk_from, chunk_to, status) "
                           "VALUES (?, ?, ?, ?, ?, 'DONE')", (run, sym, c["instrument_token"], start, end))
            db.execute("UPDATE chunks SET status = 'DONE', error = NULL, reason = ?, updated = ? WHERE run = ? AND symbol = ?",
                       (fno.ZERO_VOLUME, now, run, sym))
        db.commit()
    finally:
        db.close()


def derive_day(with_day, without_data_root, underlying, source="kite", quiet=lambda m: None):
    """Write the without copy of the with day folder with_day into without_data_root's root. Returns
    (new folder path, number of zero-volume files removed). Raises DeriveError (nothing written) or OSError
    (the .part is removed). The caller holds both roots' locks."""
    name = os.path.basename(os.path.normpath(with_day))
    new_name = without_name(name)
    run_date = fno.parse_day_folder(name)[0]
    if not os.path.exists(os.path.join(with_day, "summary.csv")):
        raise DeriveError(f"{name} has no summary.csv (the download did not finish)")
    zero = zero_volume_files(with_day)
    root = fno.fno_root(without_data_root, underlying, source)
    if fno.find_day_dir(root, run_date):
        raise DeriveError(f"the without store already has {run_date}")
    final = os.path.join(root, new_name)
    part = final + ".part"
    os.makedirs(root, exist_ok=True)
    if os.path.exists(part):
        shutil.rmtree(part)   # left by an attempt that failed; never a day folder
    try:
        copy_day(with_day, part, zero)
        mark_not_kept(part)
        summary_off(part)
        bhav.log_line(os.path.join(part, "run.log"),
                      f"Derived from {name} on {fno.now_ist():%Y-%m-%d %H:%M:%S} IST: removed {len(zero)} zero-volume contracts")
        with open(os.path.join(part, DERIVED_FILE), "w", encoding="utf-8", newline="\n") as f:
            f.write(f"{name}\n")
        if bhav.saved_items(part, underlying) is not None:
            bhav.check_saved(part, run_date, underlying, log=quiet)   # also rebuilds summary.csv from disk
        else:
            fno.rewrite_summary(part, run_date, underlying)
        os.rename(part, final)
    except BaseException:
        shutil.rmtree(part, ignore_errors=True)
        raise
    bhav.write_days_summary(final, underlying)
    with_data_root = os.path.dirname(os.path.dirname(os.path.abspath(with_day)))
    copy_progress(with_data_root, without_data_root, underlying, source, run_date, zero, symbols_of(final, underlying, run_date))
    fno_days.write_day_count(root)
    return final, len(zero)


def compare_days(a, b, skip_a=()):
    """(only in a, only in b, in both but different) contract names of day folders a and b; skip_a: names of
    a left out (its zero-volume files when a is a with day)."""
    fa = {k: v for k, v in contract_files(a).items() if k not in set(skip_a)}
    fb = contract_files(b)
    diff = sorted(n for n in set(fa) & set(fb) if not filecmp.cmp(fa[n], fb[n], shallow=False))
    return sorted(set(fa) - set(fb)), sorted(set(fb) - set(fa)), diff


def diff_line(only_a, only_b, diff, a_label="with", b_label="without", limit=20):
    n = len(only_a) + len(only_b) + len(diff)
    if not n:
        return "identical"
    parts = []
    for label, names in ((f"only in {a_label}", only_a), (f"only in {b_label}", only_b), ("contents differ", diff)):
        if names:
            parts.append(f"{label} {len(names)}: " + ", ".join(names[:limit]) + (" ..." if len(names) > limit else ""))
    return f"differs: {n} files ({'; '.join(parts)})"


def after_run(with_day, without_data_root, underlying, source="kite", log=print):
    """After a keep zero-volume ON run of a day: derive its without copy, or compare it with the one the
    without store already has. Takes the without root's lock. Returns the line for run.log and the job message."""
    try:
        lock = fno.acquire_lock(without_data_root, underlying, "derive without zero volume", source)
    except fno.LockBusy as e:
        return f"Without-zero-volume copy not made: {e}"
    try:
        run_date = fno.parse_day_folder(os.path.basename(with_day))[0]
        have = fno.day_dir(without_data_root, underlying, run_date, source)
        if have:
            try:
                zero = zero_volume_files(with_day)
            except DeriveError as e:
                return f"Without copy already present ({os.path.basename(have)}); not compared: {e}"
            return (f"Without copy already present ({os.path.basename(have)}): "
                    + diff_line(*compare_days(with_day, have, zero)))
        path, removed = derive_day(with_day, without_data_root, underlying, source)
        return f"Without-zero-volume copy saved: {path} ({removed} zero-volume contracts removed)"
    except (DeriveError, OSError, fno.DayFolderError, bhav.BhavError) as e:
        return f"Without-zero-volume copy not made: {e}"
    finally:
        lock.release()


def in_range(day, d_from, d_to):
    return (not d_from or day >= d_from) and (not d_to or day <= d_to)


def derive_without(with_folder, without_folder, source, underlyings, d_from=None, d_to=None, apply=False, log=print):
    """python fno_store.py derive-without: every with day the without store lacks. Dry run by default. Returns
    the exit code: 0 done or dry run, 1 not run, a busy root or a failed day."""
    from .fno_store import take_locks   # fno_store imports this module
    if not with_folder or not without_folder:
        log("Not run: the with and without folders are not set (FNO_RAW_KITE / FNO_RAW_DHAN in .env, or --with / --without).")
        return 1
    if config_same(with_folder, without_folder):
        log("Not run: the with and without folders are the same.")
        return 1
    code = n_derive = n_done = 0
    for u in underlyings:
        src_root = fno.fno_root(with_folder, u, source)
        dst_root = fno.fno_root(without_folder, u, source)
        if not os.path.isdir(src_root):
            continue
        log(f"{src_root} -> {dst_root}:")
        try:
            locks = take_locks([src_root, dst_root], apply, "fno_store.py derive-without")
        except fno.LockBusy as e:
            log(f"  Not run: {e}")
            code = 1
            continue
        try:
            try:
                days, have = fno.day_folders(src_root), fno.day_folders(dst_root)
            except fno.DayFolderError as e:
                log(f"  Not run: {e}")
                code = 1
                continue
            for day, name in sorted(days.items()):
                if not in_range(day, d_from, d_to):
                    continue
                if day in have:
                    log(f"  {day}: already in the without store as {have[day]}")
                    continue
                path = os.path.join(src_root, name)
                try:
                    new = without_name(name)
                    if not os.path.exists(os.path.join(path, "summary.csv")):
                        raise DeriveError(f"{name} has no summary.csv (the download did not finish)")
                except DeriveError as e:
                    log(f"  {day}: SKIPPED {e}")
                    continue
                try:
                    zero = zero_volume_files(path)
                except DeriveError as e:
                    log(f"  {day}: ERROR {name}: {e}; nothing written for this day")
                    code = 1
                    continue
                n_derive += 1
                total = len(contract_files(path))
                if not apply:
                    log(f"  {day}: would derive {name} -> {new}: remove {len(zero)} zero-volume files of {total}")
                    continue
                try:
                    final, removed = derive_day(path, without_folder, u, source)
                    n_done += 1
                    log(f"  {day}: derived {os.path.basename(final)} ({removed} zero-volume files removed of {total})")
                except (DeriveError, OSError, fno.DayFolderError, bhav.BhavError) as e:
                    log(f"  {day}: FAILED {type(e).__name__}: {e}")
                    code = 1
            for day in sorted(set(have) - set(days)):
                if in_range(day, d_from, d_to):
                    log(f"  {day}: only in the without store ({have[day]}): no with copy (can't be derived)")
        finally:
            for lock in locks:
                lock.release()
    if apply:
        log(f"Derived {n_done} of {n_derive} days.")
    else:
        log(f"Dry run: {n_derive} days would be derived. Run again with --apply to write them.")
    return code


def compare_variants(with_folder, without_folder, source, underlyings, d_from=None, d_to=None, log=print):
    """python fno_store.py compare-variants: every day that has a with and a without download is derived into a
    temporary folder and compared with the real without day. Writes nothing in raw. Returns 0 when every compared
    day is identical, else 1."""
    from .fno_store import take_locks   # fno_store imports this module
    code, n = 0, 0
    for u in underlyings:
        a_root, b_root = fno.fno_root(with_folder, u, source), fno.fno_root(without_folder, u, source)
        if not os.path.isdir(a_root) or not os.path.isdir(b_root):
            log(f"{u}: needs both {a_root} and {b_root}")
            continue
        try:
            locks = take_locks([a_root, b_root], False, "fno_store.py compare-variants")
        except fno.LockBusy as e:
            log(f"{u}: Not run: {e}")
            code = 1
            continue
        try:
            a_days, b_days = fno.day_folders(a_root), fno.day_folders(b_root)
            for day in sorted(set(a_days) & set(b_days)):
                if not in_range(day, d_from, d_to):
                    continue
                a, b = os.path.join(a_root, a_days[day]), os.path.join(b_root, b_days[day])
                with tempfile.TemporaryDirectory(prefix="fno_compare_") as tmp:
                    try:
                        derived, removed = derive_day(a, tmp, u, source)
                    except (DeriveError, OSError, bhav.BhavError) as e:
                        log(f"{u} {day}: not compared: {e}")
                        code = 1
                        continue
                    line = diff_line(*compare_days(derived, b), a_label="derived", b_label="real without")
                n += 1
                log(f"{u} {day}: {line} (derived from {a_days[day]}, {removed} zero-volume files removed; "
                    f"real without {b_days[day]})")
                if line != "identical":
                    code = 1
        finally:
            for lock in locks:
                lock.release()
    log(f"Compared {n} days.")
    return code


def config_same(a, b):
    return os.path.normcase(os.path.abspath(a)) == os.path.normcase(os.path.abspath(b))

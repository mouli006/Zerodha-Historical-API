"""Re-download every MISSING_IN_KITE contract of one saved F&O day (fno_redownload.py), one by one through
fno.redownload_contract (the dashboard's Re-download button), then check the day against its saved bhavcopy once.

Dry run by default: lists the missing contracts, writes nothing and makes no network call. With apply: the root's
lock for the whole run, a verified backup of the day folder under <home>/archive/, one token check, the loop at
SPEED requests per second, bhav.check_saved once (also after an early stop, so bhav_compare.csv lists only what is
still missing and a re-run continues). The without_zero_volume store and the backtest store are never touched
(derive_to=None). See docs/dev/fno_redownload_missing.md."""
import json
import os

from kiteconnect import exceptions as kex

from . import bhav, fno
from .dhan_api import data_plan_error
from .fetch import RateLimiter
from .fno_clear import home_of
from .fno_import import copy_checked, sha256, tree_files
from .job import DEFAULT_SPEED, JOB, JOB_LOCK

SPEED = DEFAULT_SPEED     # requests per second, Kite and Dhan alike (never above the dashboard's Dhan default)
PROGRESS_EVERY = 25
MAX_FAILS_IN_A_ROW = 3    # fetch failures in a row with a good token: the network is gone, stop
MANIFEST = "manifest.json"
TYPES = ("FUT", "CE", "PE")
NO_CANDLES = ("returned no candles", "all with volume 0")   # redownload_contract's "no file" messages
EXIT_DONE, EXIT_REFUSED, EXIT_STOPPED = 0, 1, 2


class Refused(Exception):
    pass


def plan(run_date, source, underlying, folder, keep_zero=True, only=None, limit=None, today=None):
    """What a run would do, read from the day's bhav_compare.csv: a dict with day, missing (all MISSING_IN_KITE
    names), on_disk (missing names that already have a file), expired, live (to fetch, after --only / --limit),
    cut (left out by --limit). No network, nothing written. Raises Refused."""
    err = fno.store_mismatch(folder, keep_zero)
    if err:
        raise Refused(err)
    try:
        day = fno.day_dir(folder, underlying, run_date, source)
    except fno.DayFolderError as e:
        raise Refused(str(e))
    if not day:
        raise Refused(f"{run_date} has no {fno.BROKER[source]} {underlying} day folder in {folder}.")
    missing = fno.missing_in_kite(day)
    if missing is None:
        raise Refused(f"{run_date} has no bhavcopy check yet ({os.path.join(day, 'bhavcopy', 'bhav_compare.csv')} "
                      f"not found); run the check first.")
    if not missing:
        raise Refused(f"{run_date}: no MISSING_IN_KITE contracts; nothing to re-download.")
    names = missing
    if only:
        unknown = [n for n in only if n not in missing]
        if unknown:
            raise Refused(f"Not MISSING_IN_KITE on {run_date}: {', '.join(unknown)}.")
        names = [n for n in missing if n in set(only)]
    today = today or fno.today_ist()
    files = bhav.disk_files(day)
    on_disk = [n for n in names if n in files]
    expired, live = [], []
    for n in names:
        if n in files:
            continue
        parsed = bhav.parse_name(n, underlying)
        (expired if parsed and parsed[0] < today else live).append(n)
    cut = []
    if limit is not None:
        live, cut = live[:limit], live[limit:]
    return {"day": day, "missing": missing, "on_disk": on_disk, "expired": expired, "live": live, "cut": cut}


def by_expiry(p, underlying):
    """Table rows: per expiry the FUT / CE / PE counts of the contracts considered, and how many are fetched."""
    fetch, out = set(p["live"]), {}
    for n in p["live"] + p["expired"] + p["cut"]:
        parsed = bhav.parse_name(n, underlying)
        expiry, kind = (parsed[0].isoformat(), parsed[1]) if parsed else ("?", "?")
        r = out.setdefault(expiry, {"Expiry": expiry, "FUT": 0, "CE": 0, "PE": 0, "Total": 0, "Fetch": 0, "Expired": 0})
        if kind in TYPES:
            r[kind] += 1
        r["Total"] += 1
        r["Fetch"] += n in fetch
        r["Expired"] += n in p["expired"]
    return [out[k] for k in sorted(out)]


def backup_path(folder, env, underlying, source, run_date, now=None):
    """<home of the .env F&O bases>/archive/<YYYY-MM-DD_HHMM IST>_redownload_<U>_<source>_<date>, or None
    when the bases share no common folder (fno_clear.home_of)."""
    home = home_of(env)
    if not home:
        return None
    return os.path.join(home, "archive", f"{fno.to_ist(now or fno.now_ist()):%Y-%m-%d_%H%M}_redownload_"
                                         f"{underlying}_{source}_{run_date}")


def backup(day, archive):
    """Copy the day folder to <archive>/<day folder name>/ (fno_import.copy_checked, via .part) and write
    <archive>/manifest.json with every file's size and SHA-256, each checked against the copy. Raises OSError."""
    if os.path.exists(archive):
        raise OSError(f"{archive} already exists")
    dst = os.path.join(archive, os.path.basename(day))
    os.makedirs(archive)
    copy_checked(day, dst)
    files = []
    for rel in sorted(tree_files(day)):
        digest = sha256(os.path.join(day, rel))
        if not os.path.exists(os.path.join(dst, rel)) or sha256(os.path.join(dst, rel)) != digest:
            raise OSError(f"backup of {os.path.join(day, rel)} does not match (SHA-256)")
        files.append({"path": rel.replace(os.sep, "/"), "size": os.path.getsize(os.path.join(day, rel)), "sha256": digest})
    man = {"source": day, "copy": dst, "created": f"{fno.now_ist():%Y-%m-%d %H:%M:%S} IST", "files": files}
    tmp = os.path.join(archive, MANIFEST + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(man, f, indent=1)
    os.replace(tmp, os.path.join(archive, MANIFEST))
    return dst, len(files)


def token_error(client, source):
    """"" while the broker still accepts the token, else why (one profile call)."""
    try:
        profile = client.profile()
    except (kex.TokenException, kex.PermissionException) as e:
        return f"{fno.BROKER[source]} refused the access token ({e}). Sign in again, then run the same command again."
    except Exception as e:
        return f"Could not reach {fno.BROKER[source]} ({type(e).__name__}: {e}). Run the same command again later."
    return data_plan_error(profile) if source == "dhan" else ""


def outcome(contract, saved, msg):
    """saved / no_candles / expired / fetch_failed / failed from redownload_contract's result."""
    if saved:
        return "saved"
    if any(t in msg for t in NO_CANDLES):
        return "no_candles"
    if " expired on " in msg:
        return "expired"
    if msg.startswith(f"{contract}: "):   # fetch_chunk raised (token, access, network or Kite error)
        return "fetch_failed"
    return "failed"


def show_plan(p, run_date, source, underlying, folder, archive, speed, log):
    log(f"{underlying} {fno.BROKER[source]} {run_date}: {p['day']}")
    log(f"MISSING_IN_KITE in bhav_compare.csv: {len(p['missing'])}"
        + (f"; already have a file (skipped): {len(p['on_disk'])}" if p["on_disk"] else ""))
    rows = by_expiry(p, underlying)
    if rows:
        log(fno.table(["Expiry", "FUT", "CE", "PE", "Total", "Fetch", "Expired"], rows))
    log(f"Live (to fetch): {len(p['live'])}, expired (skipped): {len(p['expired'])}"
        + (f", left out by --limit: {len(p['cut'])}" if p["cut"] else ""))
    log(f"Backup of the day folder: {archive or '(none: the .env F&O bases share no common folder)'}")
    log(f"Speed: {speed:g} requests/s; estimate {fno.fmt_eta(len(p['live']) / speed)} for {len(p['live'])} contracts")


def run(run_date, source, underlying, folder, env, keep_zero=True, only=None, limit=None, apply=False, speed=SPEED,
        log=print, connect=None):
    """The whole command; returns the exit code (0 done or dry run, 1 refused or error, 2 stopped early).
    connect() -> (client, api_key, token, error) signs in once, only with apply and only after the backup."""
    try:
        p = plan(run_date, source, underlying, folder, keep_zero, only, limit)
    except Refused as e:
        log(f"Not run: {e}")
        return EXIT_REFUSED
    archive = backup_path(folder, env, underlying, source, run_date)
    show_plan(p, run_date, source, underlying, folder, archive, speed, log)
    if not apply:
        log("Dry run: nothing fetched or written. Add --apply to re-download.")
        return EXIT_DONE
    if not p["live"]:
        log("Nothing to fetch.")
        return EXIT_DONE
    if not archive:
        log("Not run: no archive folder for the backup (the .env F&O bases share no common folder).")
        return EXIT_REFUSED
    with JOB_LOCK:
        if JOB["state"] == "running":
            log("Not run: a download job is running. Stop it or wait for it to finish.")
            return EXIT_REFUSED
    try:
        lock = fno.acquire_lock(folder, underlying, "fno_redownload.py", source)
    except fno.LockBusy as e:
        log(f"Not run: {e}")
        return EXIT_REFUSED
    try:
        return apply_run(p, run_date, source, underlying, folder, archive, keep_zero, speed, log, connect)
    finally:
        lock.release()


def apply_run(p, run_date, source, underlying, folder, archive, keep_zero, speed, log, connect):
    day = p["day"]
    try:
        dst, n = backup(day, archive)
    except OSError as e:
        log(f"Not run: the backup failed ({e}); nothing was fetched.")
        return EXIT_REFUSED
    log(f"Backup: {n} files copied and checked (SHA-256) to {dst}; manifest {os.path.join(archive, MANIFEST)}")
    client, api_key, token, err = connect()
    if err:
        log(f"Not run: {err}")
        return EXIT_STOPPED
    before = len(p["missing"])
    bhav.log_line(os.path.join(day, "run.log"),
                  f"=== fno_redownload.py: {len(p['live'])} of {before} missing contracts, {speed:g} requests/s, "
                  f"keep zero-volume contracts {fno.on_off(keep_zero)}, backup {dst}")
    counts = dict.fromkeys(("saved", "no_candles", "expired", "fetch_failed", "failed"), 0)
    limiter, fails, stopped, total = RateLimiter(speed), 0, "", len(p["live"])
    try:
        for i, name in enumerate(p["live"], 1):
            limiter.wait()
            saved, msg = fno.redownload_contract(api_key, token, run_date, folder, name, underlying, speed,
                                                 log=log, keep_zero=keep_zero, kite=client, source=source,
                                                 derive_to=None)
            result = outcome(name, saved, msg)
            counts[result] += 1
            if result in ("expired", "failed", "fetch_failed"):
                log(msg)
            if result == "fetch_failed":
                fails += 1
                stopped = token_error(client, source)
                if not stopped and fails >= MAX_FAILS_IN_A_ROW:
                    stopped = f"{fails} contracts in a row could not be fetched (last: {msg}). Run the same command again later."
                if stopped:
                    break
            else:
                fails = 0
            if i % PROGRESS_EVERY == 0 or i == total:
                log(f"Progress {i}/{total}: saved {counts['saved']}, no candles {counts['no_candles']}, "
                    f"failed {counts['failed'] + counts['fetch_failed']}; "
                    f"left {fno.fmt_eta((total - i) / speed)}")
    except KeyboardInterrupt:
        stopped = "Interrupted (Ctrl+C)."
    tried = sum(counts.values())
    if stopped:
        log(f"Stopped early after {tried} of {total}: {stopped} Saved files stay; run the same command again to continue.")
    day = fno.day_dir(folder, underlying, run_date, source) or day   # a keep zero-volume ON run may rename noZeroVol
    line, sev, after = "", "", None
    if tried:
        try:
            _, line, _ = bhav.check_saved(day, run_date, underlying, log=log)
            check = bhav.read_check(folder, underlying, run_date, source)
            sev = bhav.day_severity(check) if check else ""
            after = len(fno.missing_in_kite(day) or [])
        except (bhav.BhavError, OSError) as e:
            log(f"The bhavcopy check failed: {e}")
            stopped = stopped or "check failed"
    summary = (f"Re-download summary: saved {counts['saved']}, still no candles {counts['no_candles']}, "
               f"skipped expired {len(p['expired']) + counts['expired']}, failed {counts['failed'] + counts['fetch_failed']}"
               + (f", not tried {total - tried}" if total - tried else "")
               + f"; Missing {before} -> {after if after is not None else '?'}"
               + f"; severity {(sev or 'OK') if line else '?'}")
    log(summary)
    if os.path.exists(os.path.join(day, "run.log")):
        bhav.log_line(os.path.join(day, "run.log"), summary)
    if stopped == "check failed":
        return EXIT_REFUSED
    return EXIT_STOPPED if stopped else EXIT_DONE

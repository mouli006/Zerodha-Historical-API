"""Data audit (#249, D212, point 101): one reason for every missing day in every store, read only.

python -m backfill.equity.audit run --folder F [--fno-folder B] [--fno-raw-kite K] [--fno-raw-dhan D] [--out DIR]
                                  [--stores equity,eod,master,fno,icici] [--stocks SID,...]

Reads the stores and writes only a new output folder <folder>/analysis/audit_<IST date>[_b, _c ...]. Takes no lock;
refuses (exit 2) while a writer lock of a store it reads is held. Exit 0 = CLEAN, 1 = GAPS (UNEXPLAINED or WAITING
> 0), 2 = refused, error, or the stores changed during the run (the result is not valid: run again).
Details: docs/dev/data_audit.md.
"""
import argparse
import json
import os
import pathlib
import sqlite3
import subprocess
import time
from datetime import date, datetime, timedelta

import pandas as pd

import config
from backfill.equity import icici_store as st
from backfill.equity import minute_combined as mc
from backfill.equity import minute_download as md
from backfill.equity import minute_plan as mp
from backfill.equity import minute_store as ms
from backfill.equity import security_master as sm
from backfill.equity import special_sessions as ss
from backfill.locks import held

STORES = ("equity", "eod", "master", "fno", "icici")
UNEXPLAINED = "UNEXPLAINED"
PERMANENT = ("LOST", "VENDOR", "HOLIDAY", "ICICI_EMPTY", "ICICI_BEFORE_START")
WAITING = ("WAITING_ICICI", "P65_RETRY", "NOT_FINISHED", "UPDATE_PENDING")
MANUAL_NAME = "manual_known_gaps.csv"
MANUAL_COLUMNS = ["store", "security_id", "symbol", "underlying", "from_date", "to_date", "reason", "problem_id",
                  "clear_by", "note"]
GAP_COLUMNS = ["store", "security_id", "symbol", "underlying", "source", "date", "reason", "kind", "evidence",
               "clears_when"]
UNDERLYINGS = ("NIFTY", "SENSEX")
VARIANTS = ("with", "without")
SOURCES = ("kite", "dhan")
RAW_FOLDER = {"kite": "{u}_FnO_1min", "dhan": "{u}_FnO_1min_Dhan"}
EOD_FROM = date(2015, 1, 1)
NOT_VALID = "RESULT NOT VALID: stores changed during the run, run again"
CLEARS = {"NOT_FINISHED": "after D+1 01:00 IST",
          "UPDATE_PENDING": {"equity": "after the next equity_minute.py update",
                             "eod": "after the next equity_update.py",
                             "fno": "after the next F&O download + check + publish"},
          "WAITING_ICICI": "after the ICICI download reaches the stock",
          "P65_RETRY": "after the weekend update (clear_by)"}
# the ICICI part of a combined NONE reason (minute_combined.decide) -> the audit reason
ICICI_PART = {"ICICI_EMPTY": "ICICI_EMPTY", "ICICI_BEFORE_START": "ICICI_BEFORE_START", "NOT_IN_ICICI": "VENDOR",
              "ICICI_MIXED_SERIES": "VENDOR", "ICICI_NOT_YET_FETCHED": "WAITING_ICICI"}


class Refused(Exception):
    """The audit does not run (exit 2): a lock held, a path missing, inputs busy or stale."""


def kind_of(reason):
    if reason in PERMANENT:
        return "PERMANENT"
    if reason in WAITING:
        return "WAITING"
    return UNEXPLAINED


def gap(store, day, reason, evidence="", security_id="", symbol="", underlying="", source="", clears=""):
    return {"store": store, "security_id": security_id, "symbol": symbol, "underlying": underlying, "source": source,
            "date": day, "reason": reason, "kind": kind_of(reason), "evidence": evidence, "clears_when": clears}


# ---- paths ----

def manual_path(folder):
    return os.path.join(mp.store_dir(folder), "reference", MANUAL_NAME)


def icici_db(folder):
    return os.path.join(st.out_dir(folder), "download_progress.sqlite")


def kite_db(folder):
    return os.path.join(mp.out_dir(folder), "download_progress.sqlite")


def fno_days_paths(fno_folder):
    return {(v, u): os.path.join(fno_folder, f"{v}_zero_volume", u, "days.csv") for v in VARIANTS for u in UNDERLYINGS}


def raw_folders(raw):
    """{(source, variant, underlying): the F&O raw store folder (its .lock is the run lock)}."""
    return {(s, v, u): os.path.join(raw[s], f"{v}_zero_volume", RAW_FOLDER[s].format(u=u))
            for s in SOURCES for v in VARIANTS for u in UNDERLYINGS}


def resolve_paths(folder, stores, fno_folder=None, raw_kite=None, raw_dhan=None):
    """Every path the run reads, checked (absolute, existing); Refused otherwise. Defaults from config (.env)."""
    if not folder or not os.path.isabs(folder):
        raise Refused(f"--folder must be an absolute path (got {folder!r}).")
    if not os.path.isdir(folder):
        raise Refused(f"--folder {folder} does not exist.")
    paths = {"equity folder": folder, "manual file": manual_path(folder), "ICICI progress db": icici_db(folder),
             "Kite progress db": kite_db(folder), "bhavcopy days.csv": os.path.join(mp.store_dir(folder), "days.csv"),
             "master build_info": os.path.join(sm.out_dir(folder), "build_info.json")}
    fno = {}
    if "fno" in stores:
        fno_folder = fno_folder if fno_folder is not None else config.fno_backtest_dir()
        if not fno_folder:
            raise Refused("No F&O backtest folder: .env FNO_BACKTEST_DIR is not set; pass --fno-folder.")
        raw = {"kite": raw_kite if raw_kite is not None else config.fno_raw_dir("kite"),
               "dhan": raw_dhan if raw_dhan is not None else config.fno_raw_dir("dhan")}
        for name, p in [("--fno-folder", fno_folder), ("--fno-raw-kite", raw["kite"]), ("--fno-raw-dhan", raw["dhan"])]:
            if not os.path.isabs(p):
                raise Refused(f"{name} must be an absolute path (got {p!r}).")
            if not os.path.isdir(p):
                raise Refused(f"{name} {p} does not exist.")
        for key, p in fno_days_paths(fno_folder).items():
            if not os.path.isfile(p):
                raise Refused(f"No F&O days.csv for {key[1]} ({key[0]} zero volume): {p}")
        for key, p in raw_folders(raw).items():
            if not os.path.isdir(p):
                raise Refused(f"No F&O raw store folder for {key[0]} {key[2]} ({key[1]} zero volume): {p} "
                              f"(wrong raw base? pass --fno-raw-{key[0]})")
        fno = {"backtest": fno_folder, "raw": raw}
        paths["F&O backtest folder"] = fno_folder
        paths["F&O raw kite"] = raw["kite"]
        paths["F&O raw dhan"] = raw["dhan"]
        for (v, u), p in fno_days_paths(fno_folder).items():
            paths[f"F&O days.csv {u} {v}"] = p
    return paths, fno


def fno_locks(fno):
    return [os.path.join(p, ".lock") for p in raw_folders(fno["raw"]).values()] if fno else []


def check_locks(folder, stores, fno):
    lines = list(mc.busy(folder))
    lines += [f"F&O run lock held: {p}" for p in fno_locks(fno) if held(p)]
    if lines:
        raise Refused("; ".join(lines) + "; run after it ends.")
    if "equity" in stores or "master" in stores:
        try:
            mp.check_inputs(folder)
        except mp.MinutePlanError as e:
            raise Refused(str(e)) from None


def _stat(p):
    try:
        s = os.stat(p)
        return (round(s.st_mtime, 6), s.st_size)
    except OSError:
        return None


def snapshot(folder, fno):
    """{path: (mtime, size) or None, held} of every lock and the key files the audit reads (stores-changed check)."""
    files = list(mc.lock_paths(folder).values()) + fno_locks(fno)
    files += [icici_db(folder), kite_db(folder), os.path.join(mp.store_dir(folder), "days.csv"),
              os.path.join(sm.out_dir(folder), "build_info.json")]
    years = sm.year_files(mp.store_dir(folder)) if os.path.isdir(mp.store_dir(folder)) else []
    if years:
        files.append(max(years, key=os.path.getmtime))
    if fno:
        files += list(fno_days_paths(fno["backtest"]).values())
    return {p: (_stat(p), held(p) if p.endswith(".lock") else False) for p in files}


def changed(before, after):
    out = [p for p in before if before[p] != after.get(p)]
    out += [p for p, (_, h) in after.items() if h and p not in out]
    return out


# ---- the manual file ----

def read_manual(path):
    """The manual known gaps (data-folder copy) with dates parsed; None when the file is missing."""
    if not os.path.isfile(path):
        return None
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    missing = [c for c in MANUAL_COLUMNS if c not in df.columns]
    if missing:
        raise Refused(f"{path}: missing columns {', '.join(missing)}")
    return read_manual_frame(df)


def read_manual_frame(rows):
    """The manual rows (a frame or a list of dicts with MANUAL_COLUMNS, all text) with the dates parsed."""
    df = pd.DataFrame(rows, columns=MANUAL_COLUMNS).fillna("").astype(str)
    df["from_date"] = [sm.as_date(x) for x in df["from_date"]]
    df["to_date"] = [sm.as_date(x) if x else None for x in df["to_date"]]
    df["clear_by"] = [sm.as_date(x) if x else None for x in df["clear_by"]]
    return df.reset_index(drop=True)


class Manual:
    """Lookup of the manual rows by (store, key, day); remembers which rows matched a gap."""

    def __init__(self, df):
        self.df = df
        self.used = set()

    def hit(self, store, key, day, today):
        if self.df is None:
            return None
        for i, r in enumerate(self.df.itertuples(index=False)):
            if r.store != store or key not in (r.security_id, r.underlying):
                continue
            if not (r.from_date <= day <= (r.to_date or r.from_date)):
                continue
            self.used.add(i)
            pid = f"{r.problem_id}: " if r.problem_id else ""
            if r.reason == "P65_RETRY" and r.clear_by and today > r.clear_by:
                return UNEXPLAINED, f"{pid}P65_RETRY row past clear_by {r.clear_by}", ""
            if r.reason == "P65_RETRY":
                return "P65_RETRY", f"{pid}{r.note}".strip(), f"{CLEARS['P65_RETRY']} {r.clear_by or ''}".strip()
            return r.reason, f"{pid}{r.note}".strip(), ""
        return None

    def unused(self):
        if self.df is None:
            return []
        return [r for i, r in enumerate(self.df.to_dict("records")) if i not in self.used]


def time_reason(store, day, newest, now):
    """NOT_FINISHED before the day's D+1 01:00 IST; UPDATE_PENDING when finished but later than the store's newest
    stored day; else None."""
    if now < md.finished_at(day):
        return "NOT_FINISHED", CLEARS["NOT_FINISHED"]
    if newest is None or day > newest:
        return "UPDATE_PENDING", CLEARS["UPDATE_PENDING"][store]
    return None


# ---- (a) equity 1-minute ----

def equity_tables(folder, ids, log=print):
    """(security_id, day table) per stock: minute_combined.stock_sources on the traded days (read only)."""
    traded = mc.traded_by_stock(folder, set(ids))
    ctx = mc.Context(folder)
    for n, sid in enumerate(ids, 1):
        yield sid, mc.stock_sources(folder, sid, traded.get(sid, set()), ctx)
        if n % 500 == 0:
            cur, peak = md.memory()
            log(f"  {n}/{len(ids)} stocks, memory {cur / 2**20:.0f} MB (peak {peak / 2**20:.0f})")


def classify_equity(reason, day, sid, manual, newest_kite, now):
    """(reason, evidence, clears_when) of one NONE traded day; reason = the combined '<kite>+<icici>' reason."""
    hit = manual.hit("equity", sid, day, now.date())
    if hit:
        return hit
    t = time_reason("equity", day, newest_kite, now)
    if t:
        return t[0], reason, t[1]
    icici = reason.split("+", 1)[1] if "+" in reason else ""
    r = ICICI_PART.get(icici)
    if r:
        return r, reason, CLEARS.get(r, "")
    return UNEXPLAINED, reason, ""


def audit_equity(folder, ids, symbols, manual, now, log=print):
    none_rows, flags = [], {"day_empty": 0, "no_bhav_row": 0, "frozen": 0, "stocks": 0, "traded": 0}
    newest = None
    for sid, t in equity_tables(folder, ids, log):
        if t is None or not len(t):
            continue
        flags["stocks"] += 1
        tr = t[t["traded"]]
        flags["traded"] += len(tr)
        flags["no_bhav_row"] += int((~t["traded"]).sum())
        flags["frozen"] += int(bool(t["frozen"].iat[0]))
        kite = tr[tr["source"] == mc.KITE]
        flags["day_empty"] += int(sum("DAY_EMPTY" in str(x).split("|") for x in kite["kite_label"]))
        if len(kite):
            m = max(kite["date"])
            newest = m if newest is None or m > newest else newest
        for d, r in zip(tr.loc[tr["source"] == mc.NONE, "date"], tr.loc[tr["source"] == mc.NONE, "reason"]):
            none_rows.append((sid, d, r))
    gaps = []
    for sid, d, r in none_rows:
        reason, ev, clears = classify_equity(r, d, sid, manual, newest, now)
        gaps.append(gap("equity", d, reason, ev, sid, symbols.get(sid, ""), clears=clears))
    flags["newest_kite"] = newest
    return gaps, flags


# ---- (b) EOD ----

def read_bhav_days(folder):
    p = os.path.join(mp.store_dir(folder), "days.csv")
    if not os.path.isfile(p):
        raise Refused(f"No bhavcopy days.csv: {p}")
    df = pd.read_csv(p, dtype=str, keep_default_na=False)
    df["date"] = [sm.as_date(x) for x in df["date"]]
    return df


def expected_days(start, today, holidays, special):
    """Weekdays start..today without a holiday row, plus the special-session weekend days in that range."""
    out, d = [], start
    while d <= today:
        if (d.weekday() < 5 and d not in holidays) or d in special:
            out.append(d)
        d += timedelta(days=1)
    return out


def audit_eod(folder, manual, now, special):
    df = read_bhav_days(folder)
    store = mp.store_dir(folder)
    holidays = set(df.loc[df["status"] == "holiday", "date"])
    ok = {d: f for d, s, f in zip(df["date"], df["status"], df["file"]) if s == "ok"}
    newest = max(df["date"]) if len(df) else None
    gaps = [gap("eod", d, "HOLIDAY", "days.csv holiday", underlying="NSE_CM") for d in sorted(holidays)]
    for d in expected_days(EOD_FROM, now.date(), holidays, special):
        if d in ok and os.path.isfile(os.path.join(store, ok[d])):
            continue
        ev = f"days.csv ok but no file {ok[d]}" if d in ok else "no days.csv row"
        hit = manual.hit("eod", "NSE_CM", d, now.date())
        if hit:
            gaps.append(gap("eod", d, hit[0], hit[1] or ev, underlying="NSE_CM", clears=hit[2]))
            continue
        t = time_reason("eod", d, newest, now) if d not in ok else None
        if t:
            gaps.append(gap("eod", d, t[0], ev, underlying="NSE_CM", clears=t[1]))
        else:
            gaps.append(gap("eod", d, UNEXPLAINED, ev, underlying="NSE_CM"))
    newest_ok = max(ok) if ok else None
    return gaps, {"rows": len(df), "ok": len(ok), "holiday": len(holidays), "newest": newest,
                  "newest_ok": newest_ok, "holidays": holidays}


# ---- (c) master ----

def audit_master(folder, newest_ok):
    p = os.path.join(sm.out_dir(folder), "build_info.json")
    info = json.load(open(p, encoding="utf-8")) if os.path.isfile(p) else {}
    last = sm.as_date(info["last_date"]) if info.get("last_date") else None
    gaps = []
    if newest_ok and (last is None or last < newest_ok):
        gaps.append(gap("master", newest_ok, UNEXPLAINED,
                        f"master older than bhavcopy: build_info last_date {last}, newest ok bhavcopy {newest_ok}"))
    secs = sm.load_master(folder)["securities"]
    kinds = secs["instrument_kind"].value_counts().to_dict()
    return gaps, {"last_date": last, "built_at": info.get("built_at", ""), "kinds": kinds}


# ---- (d) F&O ----

def _true(x):
    return str(x).strip().lower() in ("true", "1", "yes")


def audit_fno(fno, manual, now, holidays, special):
    gaps, info = [], {}
    for (v, u), p in fno_days_paths(fno["backtest"]).items():
        df = pd.read_csv(p, dtype=str, keep_default_na=False)
        df["date"] = [sm.as_date(x) for x in df["date"]]
        rows = {d: r for d, r in zip(df["date"], df.to_dict("records"))}
        info[f"{u} {v}"] = len(df)
        if not len(df):
            continue
        newest = max(rows)
        for d in expected_days(min(rows), now.date(), holidays, special):
            r = rows.get(d)
            for s in SOURCES:
                if r is not None and _true(r.get(f"{s}_available")):
                    chk = str(r.get(f"{s}_check", "")).strip()
                    if not chk or chk.lower() == "unchecked":
                        gaps.append(gap("fno", d, UNEXPLAINED, f"{v} zero volume: {s} day not checked",
                                        underlying=u, source=s))
                    continue
                ev = f"{v} zero volume: " + ("no days.csv row" if r is None else f"{s}_available False")
                hit = manual.hit("fno", u, d, now.date())
                if hit:
                    gaps.append(gap("fno", d, hit[0], hit[1] or ev, underlying=u, source=s, clears=hit[2]))
                    continue
                t = time_reason("fno", d, newest, now)
                if t:
                    gaps.append(gap("fno", d, t[0], ev, underlying=u, source=s, clears=t[1]))
                else:
                    gaps.append(gap("fno", d, UNEXPLAINED, ev, underlying=u, source=s))
    return gaps, info


# ---- (e) ICICI ----

def audit_icici(folder):
    p = icici_db(folder)
    if not os.path.isfile(p):
        return [], {"db": "missing", "stocks": {}}
    con = sqlite3.connect(pathlib.Path(p).as_uri() + "?mode=ro&immutable=1", uri=True)
    try:
        failed = con.execute("SELECT security_id, chunk_from, error FROM chunks WHERE status = 'FAILED'").fetchall()
        review = con.execute("SELECT security_id, reason FROM stocks WHERE status = 'REVIEW'").fetchall()
        states = dict(con.execute("SELECT COALESCE(status, ''), COUNT(*) FROM stocks GROUP BY status").fetchall())
    finally:
        con.close()
    gaps = [gap("icici", sm.as_date(c), UNEXPLAINED, f"ICICI chunk FAILED: {e or ''}".strip(), security_id=s)
            for s, c, e in failed]
    gaps += [gap("icici", None, UNEXPLAINED, f"ICICI stock REVIEW: {r or ''}".strip(), security_id=s)
             for s, r in review]
    return gaps, {"db": p, "failed": len(failed), "review": len(review), "stocks": states}


# ---- output ----

def out_folder(folder, out, now):
    if out:
        if not os.path.isabs(out):
            raise Refused(f"--out must be an absolute path (got {out!r}).")
        if os.path.exists(out):
            raise Refused(f"{out} exists (each run writes a new folder).")
        return out
    base = os.path.join(folder, "analysis", f"audit_{now:%Y-%m-%d}")
    if not os.path.exists(base):
        return base
    for c in "bcdefghijklmnopqrstuvwxyz":
        p = f"{base}_{c}"
        if not os.path.exists(p):
            return p
    raise Refused(f"{base}_b .. _z all exist.")


def previous_audit(folder, out):
    a = os.path.join(folder, "analysis")
    if not os.path.isdir(a):
        return None
    dirs = [os.path.join(a, n) for n in os.listdir(a) if n.startswith("audit_")]
    dirs = [d for d in dirs if os.path.isdir(d) and os.path.normcase(d) != os.path.normcase(out)
            and os.path.isfile(os.path.join(d, "gaps.csv"))]
    return max(dirs, key=os.path.getmtime) if dirs else None


KEY = ["store", "security_id", "underlying", "source", "date"]


def changes(gaps, prev):
    cols = KEY + ["change", "reason_before", "reason_now"]
    if prev is None:
        return pd.DataFrame(columns=cols), "first audit: no older audit folder"
    old = pd.read_csv(os.path.join(prev, "gaps.csv"), dtype=str, keep_default_na=False)
    new = gaps.astype(str).replace({"None": "", "nan": ""})
    o = {tuple(r[k] for k in KEY): r["reason"] for r in old.to_dict("records")}
    n = {tuple(r[k] for k in KEY): r["reason"] for r in new.to_dict("records")}
    rows = [dict(zip(KEY, k), change="FILLED", reason_before=o[k], reason_now="") for k in o if k not in n]
    rows += [dict(zip(KEY, k), change="NEW", reason_before="", reason_now=n[k]) for k in n if k not in o]
    rows += [dict(zip(KEY, k), change="REASON", reason_before=o[k], reason_now=n[k]) for k in n
             if k in o and o[k] != n[k]]
    return pd.DataFrame(rows, columns=cols), f"compared with {prev}"


def git_commit():
    try:
        root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        r = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, capture_output=True, text=True, timeout=10)
        return r.stdout.strip() or "unknown"
    except (OSError, subprocess.SubprocessError):
        return "unknown"


def newest_vendor_days(folder):
    a = os.path.join(folder, "analysis")
    if not os.path.isdir(a):
        return None, None
    c = [os.path.join(a, n, "vendor_days.csv") for n in os.listdir(a) if n.startswith("vendor_list_")]
    c = [p for p in c if os.path.isfile(p)]
    if not c:
        return None, None
    p = max(c, key=os.path.getmtime)
    with open(p, encoding="utf-8") as f:
        return p, max(sum(1 for _ in f) - 1, 0)


# ---- the run ----

def run(folder, stores=STORES, fno_folder=None, raw_kite=None, raw_dhan=None, out=None, stocks=None, now=None,
        log=print):
    """The audit; returns the exit code (0 CLEAN, 1 GAPS, 2 refused / error / stores changed)."""
    t0 = time.time()
    now = md.as_ist(now)
    stores = tuple(s for s in STORES if s in stores)
    try:
        paths, fno = resolve_paths(folder, stores, fno_folder, raw_kite, raw_dhan)
        out = out_folder(folder, out, now)
        check_locks(folder, stores, fno)
    except Refused as e:
        log(f"Refused: {e}")
        return 2
    paths["output folder"] = out
    before = snapshot(folder, fno)
    manual_df = read_manual(paths["manual file"])
    manual = Manual(manual_df)
    special = set(ss.load_special_sessions(folder)["date"])
    lines = []

    def say(x=""):
        log(x)
        lines.append(str(x))

    say(f"Data audit (read only), {now:%Y-%m-%d %H:%M} IST; stores: {', '.join(stores)}")
    say("")
    say("Paths:")
    for k, v in paths.items():
        say(f"  {k:<28} {v}")
    if fno:
        lf = fno_locks(fno)
        say(f"  F&O lock check: {len(lf)} folders, {sum(os.path.isfile(p) for p in lf)} lock files found")
    if manual_df is None:
        say(f"  MANUAL FILE MISSING: {paths['manual file']}: its LOST / P65_RETRY days stay UNEXPLAINED")
    say("")

    gaps, notes = [], {}
    master = sm.load_master(folder)
    secs = master["securities"]
    symbols = dict(zip(secs["security_id"], secs["last_symbol"]))
    eod_info = None
    if "eod" in stores or "fno" in stores or "master" in stores:
        g, eod_info = audit_eod(folder, manual, now, special)
        if "eod" in stores:
            gaps += g
    if "equity" in stores:
        ids = sorted({ms.resolve(k, folder) for k in stocks}) if stocks else \
            sorted(secs.loc[secs["instrument_kind"] == "STOCK", "security_id"])
        g, notes["equity"] = audit_equity(folder, ids, symbols, manual, now, log)
        gaps += g
    if "master" in stores:
        g, notes["master"] = audit_master(folder, eod_info["newest_ok"] if eod_info else None)
        gaps += g
    if "fno" in stores:
        g, notes["fno"] = audit_fno(fno, manual, now, eod_info["holidays"], special)
        gaps += g
    if "icici" in stores:
        g, notes["icici"] = audit_icici(folder)
        gaps += g

    df = pd.DataFrame(gaps, columns=GAP_COLUMNS)
    after = snapshot(folder, fno)
    moved = changed(before, after)
    n_unexp = int((df["kind"] == UNEXPLAINED).sum())
    n_wait = int((df["kind"] == "WAITING").sum())
    verdict = "CLEAN" if not (n_unexp or n_wait) else "GAPS"

    say("Counts per store and reason:")
    summary = (df.groupby(["store", "reason", "kind"]).size().rename("days").reset_index()
               if len(df) else pd.DataFrame(columns=["store", "reason", "kind", "days"]))
    for r in summary.itertuples(index=False):
        say(f"  {r.store:<7} {r.reason:<20} {r.kind:<12} {r.days:>10,}")
    say("")
    say("Flags (not gaps):")
    if "equity" in notes:
        e = notes["equity"]
        say(f"  equity: stocks {e['stocks']:,}, traded stock-days {e['traded']:,}, newest Kite day {e['newest_kite']}")
        say(f"  Kite DAY_EMPTY days loaded from Kite: {e['day_empty']:,}")
        say(f"  NO_BHAV_ROW days (minutes, no bhavcopy row; not loaded by default): {e['no_bhav_row']:,}")
        say(f"  frozen stocks (token review PENDING / REJECTED): {e['frozen']:,}")
        vp, vn = newest_vendor_days(folder)
        vendor = int((df["reason"] == "VENDOR").sum())
        say(f"  vendor cross-check (info): VENDOR days {vendor:,}; newest vendor_days.csv "
            f"{vp or 'none'} rows {vn if vn is not None else '-'}")
    if eod_info:
        say(f"  EOD days.csv rows {eod_info['rows']:,} = ok {eod_info['ok']:,} + holiday {eod_info['holiday']:,} "
            f"(+ others); newest ok {eod_info['newest_ok']}")
    if "master" in notes:
        m = notes["master"]
        k = m["kinds"]
        other = sum(v for x, v in k.items() if x != "STOCK")
        say(f"  master last_date {m['last_date']} built {m['built_at']}; kinds "
            + ", ".join(f"{x} {v:,}" for x, v in sorted(k.items()))
            + f"; NOT_DOWNLOADED_KIND (not in the minute stores): {other:,}")
    if "fno" in notes:
        say("  F&O days.csv rows: " + ", ".join(f"{x} {v}" for x, v in notes["fno"].items()))
    if "icici" in notes:
        i = notes["icici"]
        say(f"  ICICI progress: {i.get('failed', 0)} FAILED chunks, {i.get('review', 0)} REVIEW stocks; "
            f"stocks by status {i['stocks']}")
    for r in manual.unused():
        say(f"  unused manual row (matches no gap): {r['store']} {r['security_id'] or r['underlying']} "
            f"{r['symbol']} {r['from_date']}..{r['to_date'] or r['from_date']} {r['reason']} {r['problem_id']}")
    say("")
    say("WAITING per reason:")
    for r, g in df[df["kind"] == "WAITING"].groupby("reason"):
        say(f"  {r:<16} {len(g):>8,}  clears: {', '.join(sorted(set(g['clears_when']) - {''}))}")
    say("")
    say(f"UNEXPLAINED ({n_unexp:,}):")
    for r in df[df["kind"] == UNEXPLAINED].itertuples(index=False):
        say(f"  {r.store} {r.security_id or r.underlying} {r.symbol} {r.source} {r.date} {r.evidence}")
    say("")
    chg, chg_note = changes(df, previous_audit(folder, out))
    say(f"changes: {len(chg):,} rows ({chg_note})")
    cur, peak = md.memory()
    say(f"VERDICT: {verdict} (UNEXPLAINED {n_unexp:,}, WAITING {n_wait:,}); {time.time() - t0:.0f} s, "
        f"peak memory {peak / 2**20:.0f} MB")
    if moved:
        say(NOT_VALID + ": " + "; ".join(moved))

    os.makedirs(out)
    head = NOT_VALID + "\n" if moved else ""
    sm.write_text(os.path.join(out, "report.txt"), head + "\n".join(lines) + "\n")
    sm.write_text(os.path.join(out, "summary.csv"), head + summary.to_csv(index=False, lineterminator="\n"))
    sm.write_csv(df, os.path.join(out, "gaps.csv"))
    sm.write_csv(df[df["kind"] == UNEXPLAINED], os.path.join(out, "unexplained.csv"))
    sm.write_csv(df[df["kind"] == "WAITING"], os.path.join(out, "waiting.csv"))
    sm.write_csv(chg, os.path.join(out, "changes.csv"))
    code = 2 if moved else (0 if verdict == "CLEAN" else 1)
    info = {"started": f"{now:%Y-%m-%d %H:%M:%S}", "ended": f"{md.as_ist(None):%Y-%m-%d %H:%M:%S}",
            "commit": git_commit(), "stores": list(stores), "paths": paths,
            "inputs": {p: {"mtime": s[0][0] if s[0] else None, "size": s[0][1] if s[0] else None, "held": s[1]}
                       for p, s in before.items()},
            "stores_changed": moved, "verdict": verdict, "unexplained": n_unexp, "waiting": n_wait,
            "peak_memory_mb": round(peak / 2**20), "exit_code": code}
    sm.write_text(os.path.join(out, "run_info.json"), json.dumps(info, indent=2, default=str) + "\n")
    log(f"written: {out}")
    return code


def main(argv=None):
    p = argparse.ArgumentParser(prog="python -m backfill.equity.audit",
                                description="Data audit: one reason for every missing day (read only).")
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="audit the stores; writes only a new output folder")
    r.add_argument("--folder", required=True, help="equity data folder (absolute)")
    r.add_argument("--fno-folder", help="F&O backtest store (absolute; default .env FNO_BACKTEST_DIR)")
    r.add_argument("--fno-raw-kite", help="F&O raw base of Kite (absolute; default .env FNO_RAW_KITE)")
    r.add_argument("--fno-raw-dhan", help="F&O raw base of Dhan (absolute; default .env FNO_RAW_DHAN)")
    r.add_argument("--out", help="new output folder (default <folder>/analysis/audit_<IST date>[_b ...])")
    r.add_argument("--stores", default=",".join(STORES), help="comma-separated: " + ",".join(STORES))
    r.add_argument("--stocks", help="comma-separated security_ids or symbols (default: every STOCK)")
    a = p.parse_args(argv)
    stores = [x.strip() for x in a.stores.split(",") if x.strip()]
    bad = [x for x in stores if x not in STORES]
    if bad:
        p.error(f"unknown store(s): {', '.join(bad)}")
    stocks = [x.strip() for x in a.stocks.split(",") if x.strip()] if a.stocks else None
    try:
        return run(a.folder, stores, a.fno_folder, a.fno_raw_kite, a.fno_raw_dhan, a.out, stocks)
    except (Refused, mp.MinutePlanError) as e:
        print(f"Refused: {e}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

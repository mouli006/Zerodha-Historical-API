"""Build the F&O backtest store from the raw download folders (python fno_store.py publish).

    <out>/<UNDERLYING>/minute/<kite|dhan>/<YYYY>/<YYYY-MM-DD>.parquet   every contract of the day, one file per source
                      /eod/<YYYY>.parquet                              the exchange bhavcopy (Kite's copy, else Dhan's)
                      /contracts.parquet, expiries.csv, days.csv, publish_manifest.json

The store is derived data: it can be deleted and rebuilt from raw at any time, and publishing never writes
into raw (it only takes each raw root's .lock while reading it, so it never reads a day being downloaded).
A day is rebuilt only when it is new, its fingerprint (file count, total size, newest mtime of the whole day
folder) changed, or with force. A published day whose raw folder is gone is kept and marked (days.csv
raw_missing) until prune. Values are as saved in raw; OI is filled like fno_loader.fill_oi."""
import csv
import glob
import json
import math
import os
from datetime import date

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from . import bhav, fno, fno_loader
from .fno_store import read_expiries_file

SOURCES = ("kite", "dhan")
MINUTE_SCHEMA = pa.schema([("contract", pa.string()), ("type", pa.string()), ("expiry", pa.date32()),
                           ("strike", pa.float64()), ("dte", pa.int32()), ("datetime", pa.timestamp("ns")),
                           ("open", pa.float64()), ("high", pa.float64()), ("low", pa.float64()), ("close", pa.float64()),
                           ("volume", pa.int64()), ("oi", pa.float64()), ("oi_filled", pa.bool_()),
                           ("zero_volume", pa.bool_())])
EOD_KEYS = ["date", "contract", "type", "expiry", "strike"]
CONTRACT_COLUMNS = ["contract", "type", "expiry", "strike", "lot", "first_date", "last_date", "days_with_data"]
DAYS_COLUMNS = ["date", "expiry", "dte", "kite_available", "dhan_available", "kite_check", "dhan_check",
                "kite_zero_volume", "dhan_zero_volume", "kite_trim", "dhan_trim", "kite_contracts", "dhan_contracts",
                "eod_available", "best_source", "published_at", "raw_missing"]
EXPIRY_COLUMNS = ["expiry", "weekday", "kind"]
CHECK_RANK = {"OK": 0, "minor": 1, "unchecked": 2, "major": 3}
MANIFEST = "publish_manifest.json"


def write_atomic(path, write):
    """write(tmp path), then os.replace into path."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    write(tmp)
    os.replace(tmp, path)


def write_table(path, df, schema=None):
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    write_atomic(path, lambda tmp: pq.write_table(table, tmp))


def write_rows_csv(path, columns, rows):
    write_atomic(path, lambda tmp: _csv(tmp, columns, rows))


def _csv(tmp, columns, rows):
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=columns, lineterminator="\n", extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def fingerprint(day_dir):
    """{files, bytes, newest_mtime} of every file under the day folder."""
    files = size = 0
    newest = 0.0
    for folder, _, names in os.walk(day_dir):
        for n in names:
            st = os.stat(os.path.join(folder, n))
            files, size, newest = files + 1, size + st.st_size, max(newest, st.st_mtime)
    return {"files": files, "bytes": size, "newest_mtime": round(newest, 6)}


def minute_path(out_u, source, day):
    return os.path.join(out_u, "minute", source, day[:4], f"{day}.parquet")


def eod_path(out_u, year):
    return os.path.join(out_u, "eod", f"{year}.parquet")


def check_label(entry):
    """bhav.list_days entry -> OK / minor / major / unchecked."""
    if entry is None or entry["status"] == "pending":
        return "unchecked"
    return "OK" if entry["status"] == "ok" else ("minor" if entry["severity"] == "minor" else "major")


def best_source(checks, prefer="kite"):
    """checks: {source: label} of the available sources. OK > minor > unchecked > major; a tie goes to prefer."""
    if not checks:
        return ""
    order = sorted(checks, key=lambda s: (CHECK_RANK.get(checks[s], 2), s != prefer))
    return order[0]


def day_frame(day_dir, day, underlying):
    """Every contract file of the day as one frame in MINUTE_SCHEMA order, sorted by contract then datetime.
    Returns (frame, contracts that could not be named)."""
    run_date = date.fromisoformat(day)
    frames, bad = [], []
    for path in sorted(glob.glob(os.path.join(day_dir, "futures", "*.csv")) + glob.glob(os.path.join(day_dir, "options", "*.csv"))):
        name = os.path.splitext(os.path.basename(path))[0]
        parsed = bhav.parse_name(name, underlying)
        if parsed is None:
            bad.append(name)
            continue
        expiry, typ, strike = parsed
        df = fno_loader.read_contract_csv(path)
        volume = pd.to_numeric(df["Volume"], errors="coerce").fillna(0).astype("int64")
        frames.append(pd.DataFrame({
            "contract": name, "type": typ, "expiry": expiry, "strike": float(strike) if strike != "" else math.nan,
            "dte": (expiry - run_date).days, "datetime": df["Datetime"],
            "open": df["Open"].astype(float), "high": df["High"].astype(float), "low": df["Low"].astype(float),
            "close": df["Close"].astype(float), "volume": volume, "oi": df["OI"].astype(float),
            "oi_filled": df["OI_filled"].astype(bool), "zero_volume": bool(volume.sum() == 0)}))
    if not frames:
        return pd.DataFrame({f.name: pd.Series(dtype=object) for f in MINUTE_SCHEMA}), bad
    out = pd.concat(frames, ignore_index=True).sort_values(["contract", "datetime"], kind="stable")
    return out.reset_index(drop=True), bad


def instrument_lots(day_dir, underlying):
    """{our contract name: lot} from the day's saved instrument list (lot_size), where it has one."""
    lots = {}
    for path in glob.glob(os.path.join(day_dir, "instruments_*.csv")):
        with open(path, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                if r.get("name") != underlying or not r.get("lot_size"):
                    continue
                try:
                    name = fno.contract_name(r["instrument_type"], r["expiry"], r.get("strike"), underlying)
                    lots[name] = int(float(r["lot_size"]))
                except (ValueError, KeyError):
                    continue
    return lots


def eod_frame(raw_root, day, underlying, source):
    """The day's bhavcopy (fno_loader.load_eod) with date/contract/type/expiry/strike first, or None."""
    try:
        eod = fno_loader.load_eod(raw_root, day, underlying, source)
    except FileNotFoundError:
        return None
    eod.insert(0, "date", pd.Timestamp(day))
    eod.insert(1, "contract", eod["Contract"])
    eod.insert(2, "type", eod["Type"])
    eod.insert(3, "expiry", eod["Expiry"])
    eod.insert(4, "strike", pd.to_numeric(eod["Strike"], errors="coerce"))
    return eod


def load_manifest(out_u):
    path = os.path.join(out_u, MANIFEST)
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return {"days": {}, "lots": {}, "expiries_seen": []}


def publish(raw, out, underlyings, date_from=None, date_to=None, prefer="kite", force=False, prune=False, log=print):
    """raw: {"kite": save folder or None, "dhan": ...}. Returns the exit code: 0 done, 1 something was
    skipped (busy root) or failed, or not run."""
    if not out:
        log("Not run: give --out or set FNO_BACKTEST_DIR in .env.")
        return 1
    if not any(raw.values()):
        log("Not run: give --raw-kite and/or --raw-dhan, or set FNO_RAW_KITE / FNO_RAW_DHAN in .env.")
        return 1
    for s, path in raw.items():
        if path and os.path.normcase(os.path.abspath(path)) == os.path.normcase(os.path.abspath(out)):
            log(f"Not run: --out must not be the {s} raw folder.")
            return 1
    code = 0
    for u in underlyings:
        code = max(code, publish_underlying(raw, out, u, date_from, date_to, prefer, force, prune, log))
    return code


def in_range(day, date_from, date_to):
    return (not date_from or day >= str(date_from)) and (not date_to or day <= str(date_to))


def publish_underlying(raw, out, underlying, date_from, date_to, prefer, force, prune, log):
    out_u = os.path.join(out, underlying)
    man = load_manifest(out_u)
    days = man["days"]
    code = 0
    eod_changed = {}   # date -> (source, frame or None)
    stats = {}
    seen_expiries = set(man.get("expiries_seen", []))
    for source in SOURCES:
        if not raw.get(source):
            continue
        root = fno.fno_root(raw[source], underlying, source)
        st = stats[source] = {"published": 0, "unchanged": 0, "incomplete": [], "failed": [], "rows": 0, "bytes": 0,
                              "raw_missing": 0, "pruned": 0}
        if not os.path.isdir(root):
            log(f"{underlying} {source}: no folder {root}")
            st["absent"] = True
            continue
        try:
            lock = fno.acquire_lock(raw[source], underlying, "fno_store.py publish", source)
        except fno.LockBusy as e:
            log(f"{underlying} {source}: skipped, {e}")
            st["busy"] = True
            code = 1
            continue
        try:
            folders = fno.day_folders(root)
            checks = {d["date"]: d for d in bhav.list_days(raw[source], underlying, source)}
            seen_expiries.update(read_expiries_file(root))
            for day_d, name in sorted(folders.items()):
                day = day_d.isoformat()
                if not in_range(day, date_from, date_to):
                    continue
                path = os.path.join(root, name)
                if not os.path.exists(os.path.join(path, "summary.csv")):
                    st["incomplete"].append(day)
                    continue
                fp = fingerprint(path)
                prev = days.get(day, {}).get(source)
                if (not force and prev and prev.get("folder") == name and all(prev.get(k) == v for k, v in fp.items())
                        and os.path.exists(minute_path(out_u, source, day))):
                    if prev.get("raw_missing"):
                        prev["raw_missing"] = False
                    st["unchanged"] += 1
                    continue
                try:
                    frame, bad = day_frame(path, day, underlying)
                    if bad:
                        log(f"{underlying} {source} {day}: {len(bad)} files with names that can't be parsed skipped: "
                            + ", ".join(bad[:5]))
                    target = minute_path(out_u, source, day)
                    write_table(target, frame, MINUTE_SCHEMA)
                    _, expiry, zero, _ = fno.parse_day_folder(name)
                    if expiry is None:
                        opts = frame[frame["type"].isin(["CE", "PE"])]["expiry"]
                        future = [e for e in opts if e >= day_d]
                        expiry = min(future) if future else None
                    man["lots"].update({k: v for k, v in instrument_lots(path, underlying).items()})
                    eod = eod_frame(raw[source], day, underlying, source)
                    days.setdefault(day, {})[source] = {
                        "folder": name, **fp, "published_at": f"{fno.now_ist():%Y-%m-%d %H:%M:%S}",
                        "contracts": int(frame["contract"].nunique()) if len(frame) else 0, "rows": int(len(frame)),
                        "check": check_label(checks.get(day)), "zero_volume": {True: "with", False: "no"}.get(zero, ""),
                        "trim": fno.saved_trim(path), "expiry": expiry.isoformat() if expiry else "",
                        "has_eod": eod is not None, "raw_missing": False}
                    if eod is not None:
                        eod_changed.setdefault(day, {})[source] = eod
                    st["published"] += 1
                    st["rows"] += len(frame)
                    st["bytes"] += os.path.getsize(target)
                except Exception as e:   # one bad day never stops the others
                    st["failed"].append(f"{day}: {type(e).__name__}: {e}")
                    code = 1
            # published days whose raw folder is gone: kept and marked, removed only with prune
            have = {d.isoformat() for d in folders}
            for day in sorted(days):
                entry = days[day].get(source)
                if entry is None or day in have or not in_range(day, date_from, date_to):
                    continue
                if prune:
                    path = minute_path(out_u, source, day)
                    if os.path.exists(path):
                        os.remove(path)
                    del days[day][source]   # the EOD rows stay: the bhavcopy is the exchange's, not the source's
                    st["pruned"] += 1
                else:
                    entry["raw_missing"] = True
                    st["raw_missing"] += 1
        finally:
            lock.release()

    # EOD: Kite's bhavcopy, else Dhan's (the same exchange file); each touched year file is rewritten
    years = {}
    for day, by_source in eod_changed.items():
        current = days.get(day, {}).get("eod_source")
        pick = "kite" if "kite" in by_source else None
        if pick is None and "dhan" in by_source and current != "kite":
            pick = "dhan"
        if pick is not None:
            days[day]["eod_source"] = pick
            years.setdefault(day[:4], {})[day] = by_source[pick]
    for year, changed in years.items():
        path = eod_path(out_u, year)
        old = pd.read_parquet(path) if os.path.exists(path) else None
        keep = [] if old is None else [old[~old["date"].dt.strftime("%Y-%m-%d").isin(changed)]]
        frames = [f for f in keep + list(changed.values()) if len(f)]
        if frames:
            df = pd.concat(frames, ignore_index=True).sort_values(["date", "contract"], kind="stable")
            write_table(path, df.reset_index(drop=True))
    for day in [d for d, v in days.items() if not any(s in v for s in SOURCES)]:
        del days[day]   # pruned from every source

    man["expiries_seen"] = sorted(seen_expiries)
    write_contracts(out_u, days, man["lots"])
    write_expiries(out_u, seen_expiries)
    write_days(out_u, days, prefer)
    write_atomic(os.path.join(out_u, MANIFEST), lambda tmp: _json(tmp, man))
    for source, st in stats.items():
        if st.get("absent"):
            continue
        state = " (root busy, skipped)" if st.get("busy") else ""
        log(f"{underlying} {source}{state}: published {st['published']}, skipped unchanged {st['unchanged']}, "
            f"skipped incomplete {len(st['incomplete'])}"
            + (f" ({', '.join(st['incomplete'])})" if st["incomplete"] else "")
            + f", failed {len(st['failed'])}; {st['rows']:,} rows, {st['bytes'] / 1e6:,.1f} MB"
            + (f"; raw folder gone for {st['raw_missing']} published days (kept; --prune removes them)" if st["raw_missing"] else "")
            + (f"; pruned {st['pruned']}" if st["pruned"] else ""))
        for line in st["failed"]:
            log(f"  FAILED {line}")
    return code


def _json(tmp, obj):
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=1, sort_keys=True)


def write_contracts(out_u, days, lots):
    """contracts.parquet from every published minute file (contract columns only) and the EOD lots."""
    seen = {}
    for day in sorted(days):
        for source in SOURCES:
            path = minute_path(out_u, source, day)
            if source not in days[day] or not os.path.exists(path):
                continue
            df = pd.read_parquet(path, columns=["contract", "type", "expiry", "strike"]).drop_duplicates("contract")
            for r in df.itertuples(index=False):
                c = seen.setdefault(r.contract, {"contract": r.contract, "type": r.type, "expiry": r.expiry,
                                                 "strike": r.strike, "dates": set()})
                c["dates"].add(day)
    eod_lots = {}
    for path in sorted(glob.glob(os.path.join(out_u, "eod", "*.parquet"))):
        df = pd.read_parquet(path, columns=["contract", "Lot"])
        eod_lots.update({k: v for k, v in zip(df["contract"], df["Lot"]) if v == v and v})
    rows = []
    for name in sorted(seen):
        c = seen[name]
        lot = eod_lots.get(name) or lots.get(name)
        rows.append({"contract": name, "type": c["type"], "expiry": c["expiry"], "strike": c["strike"],
                     "lot": int(lot) if lot else None, "first_date": date.fromisoformat(min(c["dates"])),
                     "last_date": date.fromisoformat(max(c["dates"])), "days_with_data": len(c["dates"])})
    df = pd.DataFrame(rows, columns=CONTRACT_COLUMNS)
    df["lot"] = df["lot"].astype("Int64")
    write_table(os.path.join(out_u, "contracts.parquet"), df)


def write_expiries(out_u, seen):
    """expiries.csv: the raw expiries_seen.csv files, the bhavcopy expiries and the previous file, with the
    weekday and weekly/monthly (monthly = the last expiry of its calendar month)."""
    found = {date.fromisoformat(e) for e in seen}
    for path in glob.glob(os.path.join(out_u, "eod", "*.parquet")):
        found.update(d.date() for d in pd.read_parquet(path, columns=["expiry"])["expiry"].dropna())
    old = os.path.join(out_u, "expiries.csv")
    if os.path.exists(old):
        with open(old, newline="", encoding="utf-8") as f:
            found.update(date.fromisoformat(r["expiry"]) for r in csv.DictReader(f))
    last = {}
    for e in found:
        key = (e.year, e.month)
        last[key] = max(last.get(key, e), e)
    rows = [{"expiry": e.isoformat(), "weekday": fno_weekday(e), "kind": "monthly" if last[(e.year, e.month)] == e else "weekly"}
            for e in sorted(found)]
    write_rows_csv(old, EXPIRY_COLUMNS, rows)


def fno_weekday(d):
    return ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"][d.weekday()]


def write_days(out_u, days, prefer):
    rows = []
    for day in sorted(days):
        v = days[day]
        avail = {s: v[s] for s in SOURCES if s in v}
        expiry = next((e["expiry"] for e in avail.values() if e.get("expiry")), "")
        best = best_source({s: e["check"] for s, e in avail.items() if not e.get("raw_missing")}
                           or {s: e["check"] for s, e in avail.items()}, prefer)
        row = {"date": day, "expiry": expiry,
               "dte": (date.fromisoformat(expiry) - date.fromisoformat(day)).days if expiry else "",
               "eod_available": bool(v.get("eod_source")), "best_source": best,
               "published_at": max((e["published_at"] for e in avail.values()), default=""),
               "raw_missing": "+".join(s for s in SOURCES if s in avail and avail[s].get("raw_missing"))}
        for s in SOURCES:
            e = avail.get(s)
            row.update({f"{s}_available": e is not None, f"{s}_check": e["check"] if e else "",
                        f"{s}_zero_volume": e["zero_volume"] if e else "", f"{s}_trim": e["trim"] if e else "",
                        f"{s}_contracts": e["contracts"] if e else ""})
        rows.append(row)
    write_rows_csv(os.path.join(out_u, "days.csv"), DAYS_COLUMNS, rows)

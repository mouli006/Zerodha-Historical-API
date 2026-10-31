"""equity_minute.py fix-dayempty [--apply --expect-labels N --expect-borrow M]: the stored DAY_EMPTY rows (Kite minutes,
no Kite day candle) brought to the DAY_EMPTY rule of Calc.derive (docs/dev/equity_minute_dayempty.md).

Calc.derive now (P75, D54 option A)
    borrows a DAY_EMPTY day's conversion only from an OK day of the same factor (minutes, a day candle and a bhavcopy
    row; no HIGHLOW_DIFF / CLOSE_DIFF / SCALE_STEP), the same minute_scale first; without one, the nearest day of the
    same factor + note "no OK source day"; never across a factor change (EXTRA_SESSION and every other borrower: as
    before);
    checks a DAY_EMPTY day's loaded minutes against the bhavcopy (HIGHLOW_DIFF / CLOSE_DIFF / CLOSING_AUCTION).
The stored rows were built before. This one-off fix derives each stock that has a stored DAY_EMPTY row once and keeps
only these cells of its stored DAY_EMPTY rows: check (CANDLE_UNCONFIRMED kept) and note; where the borrow source
changes also ratio, kite_multiplier, kite_difference, vol_ratio, kind, conversion_from. Every other cell of every row
stays as stored (the guard), in particular the P80 rows (stored EXTRA_SESSION, a weekend bhavcopy row added later) and
the minute bars, kite_*, aggregates, volume and our_factor.
Held (never written, listed in info.csv): STALE_AGGREGATES (minute_low <= 0: aggregates still count Kite's zero bars),
PRE_EXISTING (a cell the fix does not own already differs from a fresh derive), NOT_DAY_EMPTY (the derive no longer
says DAY_EMPTY). Offline; the equity_1min lock; a stale bhavcopy cache refuses. Dry run (default) writes only
equity_1min/dayempty/dryrun_<time>/. --apply needs the two counts of the dry run (else CountMismatch, nothing written),
backs up every changed days.parquet (sha256 checked, manifest.csv) to equity_1min/archive/dayempty_<time>/, writes
.tmp files and replaces them all at the end (any error restores every file: ApplyFailed), then rebuilds check/."""
import hashlib
import json
import os
import shutil
import time

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

import config
from backfill.equity import minute_download as md
from backfill.equity import minute_plan as mp
from backfill.equity import minute_store as ms
from backfill.equity import minute_zeropad as mz
from backfill.equity import security_master as sm
from backfill.equity import special_sessions as ss
from backfill.equity.minute_negvol import same
from backfill.equity.minute_rescale import loaded_level
from backfill.locks import RunLock

DAY_EMPTY = "DAY_EMPTY"
NO_OK = md.NO_OK_SOURCE
LABELS = {"HIGHLOW_DIFF", "CLOSE_DIFF", "CLOSING_AUCTION"}       # the labels the new check may add
CONVERSION = ["ratio", "kite_multiplier", "kite_difference", "vol_ratio", "kind"]
LABEL_CELLS = ["check", "note"]
BORROW_CELLS = CONVERSION + ["conversion_from"]
REF = ("high", "low", "close")
PROGRESS_EVERY = 250
SHOWN = 30
CELL_COLUMNS = ["security_id", "symbol", "date", "column", "old", "new"]
LABEL_COLUMNS = ["security_id", "symbol", "series", "date", "kind_of_day", "old_check", "new_check", "conversion_from"]
BORROW_COLUMNS = ["security_id", "symbol", "series", "date", "kind_of_day", "old_from", "new_from", "old_check",
                  "new_check"] + [f"{w}_{k}" for w in ("old", "new") for k in REF + ("level",)] + \
    [f"bhav_{k}" for k in REF] + ["result"]
NO_SOURCE_COLUMNS = ["security_id", "symbol", "date", "conversion_from", "new_check", "written"]
INFO_COLUMNS = ["security_id", "symbol", "date", "reason", "detail", "check"]
MANIFEST_COLUMNS = ["security_id", "symbol", "source", "backup", "bytes", "sha256"]
CHECK_FILE_COLUMNS = ["file", "rows_on_disk", "rows_before", "rows_after", "rows_changed", "note"]


class CountMismatch(mp.MinutePlanError):
    """--expect-labels / --expect-borrow are not the counts found: nothing written."""


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def labels_of(check):
    return {x for x in str(check).split("|") if x and x != "OK"}


def note_core(note):
    """A note without the parts the fix owns ("converted as ...", "no OK source day")."""
    return [p for p in str(note).split("; ") if p and not p.startswith("converted as ") and p != md.NO_OK_SOURCE]


def kind_of_day(d, muhurat):
    if d.weekday() >= 5:
        return "weekend"
    return "muhurat" if d in muhurat else "span"


def muhurat_dates(folder):
    try:
        df = ss.load_special_sessions(folder)
    except ss.SpecialSessionsError:
        return set()
    return set(df.loc[df["kind"] == "MUHURAT", "date"])


class Stock:
    """One stock with stored DAY_EMPTY rows: the derive, the rows written / held and the new frame with its guard."""

    def __init__(self, calc, sid, stored, muhurat):
        self.sid, self.stored = sid, stored
        self.symbol = stored["symbol"].iat[-1] if len(stored) else sid
        self.new = calc.derive(sid, stored[md.INPUT_COLUMNS], bhav=calc.bhav_of(sid))
        if list(self.new["date"]) != list(stored["date"]):
            raise md.ApplyFailed(f"{sid}: days.parquet is not sorted by date; not fixed")
        self.pos = {d: i for i, d in enumerate(stored["date"])}
        self.writes = {}                     # row -> {column: new value}
        self.labels, self.borrow, self.no_source, self.info = [], [], [], []
        s, n = stored, self.new
        for i in np.flatnonzero(s["check"].astype(str).str.contains(DAY_EMPTY).to_numpy()):
            d = s["date"].iat[i]
            base = {"security_id": sid, "symbol": s["symbol"].iat[i], "date": md.iso(d), "check": s["check"].iat[i]}
            if float(s["minute_volume"].iat[i]) == 0:
                self.info.append({**base, "reason": "MINUTE_VOLUME_0", "detail": "minute volume 0 (report only)"})
            why, detail = self.hold(i)
            if NO_OK in str(n["note"].iat[i]):
                self.no_source.append({"security_id": sid, "symbol": base["symbol"], "date": base["date"],
                                       "conversion_from": n["conversion_from"].iat[i],
                                       "new_check": n["check"].iat[i], "written": "N" if why else "Y"})
            if why:
                self.info.append({**base, "reason": why, "detail": detail})
                continue
            cells = {}
            chk = n["check"].iat[i]
            if md.UNCONFIRMED in str(s["check"].iat[i]):
                chk = md.add_check(chk, md.UNCONFIRMED)          # set by update, never derived (as checks)
            moved = not same(s["conversion_from"].iat[i], n["conversion_from"].iat[i])
            for c, v in [("check", chk), ("note", n["note"].iat[i])] + \
                    ([(c, n[c].iat[i]) for c in BORROW_CELLS] if moved else []):
                if not same(s[c].iat[i], v):
                    cells[c] = v
            if not cells:
                continue
            self.writes[i] = cells
            kd = kind_of_day(d, muhurat)
            if "check" in cells:
                self.labels.append({"security_id": sid, "symbol": base["symbol"], "series": s["series"].iat[i],
                                    "date": base["date"], "kind_of_day": kd, "old_check": s["check"].iat[i],
                                    "new_check": chk, "conversion_from": n["conversion_from"].iat[i]})
            if moved:
                self.borrow.append(self.borrow_row(i, kd, chk))

    def hold(self, i):
        """(reason, detail) when stored DAY_EMPTY row i is not written, else (None, "")."""
        s, n = self.stored, self.new
        if float(s["minute_low"].iat[i]) <= 0:
            return "STALE_AGGREGATES", f"minute_low {s['minute_low'].iat[i]:g}: the aggregates still count Kite's " \
                "zero bars (report only)"
        if DAY_EMPTY not in str(n["check"].iat[i]):
            return "NOT_DAY_EMPTY", f"a fresh derive says {n['check'].iat[i]}"
        owned = set(LABEL_CELLS + BORROW_CELLS)
        pre = [c for c in md.DAY_COLUMNS if c not in owned and not same(s[c].iat[i], n[c].iat[i])]
        if labels_of(s["check"].iat[i]) - {md.UNCONFIRMED} - LABELS != labels_of(n["check"].iat[i]) - LABELS:
            pre.append("check")
        if note_core(s["note"].iat[i]) != note_core(n["note"].iat[i]):
            pre.append("note")
        if same(s["conversion_from"].iat[i], n["conversion_from"].iat[i]):
            pre += [c for c in CONVERSION if not same(s[c].iat[i], n[c].iat[i])]
        elif n["conversion_from"].iat[i]:
            j = self.pos.get(sm.as_date(n["conversion_from"].iat[i]))
            bad = [c for c in CONVERSION if j is None or not same(s[c].iat[j], n[c].iat[j])]
            if bad:
                pre.append(f"source {n['conversion_from'].iat[i]} {'/'.join(bad)}")
        if pre:
            return "PRE_EXISTING", "stored differs from a fresh derive: " + ", ".join(pre)
        return None, ""

    def borrow_row(self, i, kd, chk):
        s, n = self.stored, self.new
        agg = {f"minute_{k}": s[f"minute_{k}"].iat[i] for k in REF}
        bhav = {f"bhav_{k}": s[f"bhav_{k}"].iat[i] for k in REF}
        old = loaded_level(agg, s.iloc[i], bhav)
        new = loaded_level(agg, n.iloc[i], bhav)
        x = {"security_id": self.sid, "symbol": s["symbol"].iat[i], "series": s["series"].iat[i],
             "date": md.iso(s["date"].iat[i]), "kind_of_day": kd, "old_from": s["conversion_from"].iat[i],
             "new_from": n["conversion_from"].iat[i], "old_check": s["check"].iat[i], "new_check": chk}
        for w, lv in (("old", old), ("new", new)):
            for k in REF:
                x[f"{w}_{k}"] = round(lv[k] * float(bhav[f"bhav_{k}"]), 4) if lv else np.nan
            x[f"{w}_level"] = round(lv["level"], 6) if lv else np.nan
        for k in REF:
            x[f"bhav_{k}"] = bhav[f"bhav_{k}"]
        if old and new:
            e0, e1 = abs(old["level"] - 1), abs(new["level"] - 1)
            x["result"] = "BETTER" if e1 < e0 - 1e-9 else "WORSE" if e1 > e0 + 1e-9 else "SAME"
        else:
            x["result"] = "NO_REFERENCE"
        return x

    def new_frame(self):
        """(the days.parquet frame with the written cells, the cells.csv rows); raises ApplyFailed when a written cell
        is not the derive's or any other cell is not the stored one (the guard)."""
        s = self.stored
        out = s.copy()
        for i, cells in self.writes.items():
            for c, v in cells.items():
                out.at[i, c] = v
        for c in s.columns:                                      # keep the stored dtypes
            if out[c].dtype != s[c].dtype:
                try:
                    out[c] = out[c].astype(s[c].dtype)
                except (TypeError, ValueError):
                    pass
        bad = [f"{md.iso(s['date'].iat[i])} {c}" for i, cells in self.writes.items() for c, v in cells.items()
               if not same(out.at[i, c], v)]
        for c in s.columns:
            rows = {i for i, cells in self.writes.items() if c in cells}
            for i in np.flatnonzero(~equal(s[c], out[c])):
                if i not in rows:
                    bad.append(f"{md.iso(s['date'].iat[i])} {c} (not owned)")
        if list(out.columns) != list(s.columns) or len(out) != len(s):
            bad.append("shape")
        if bad:
            raise md.ApplyFailed(f"{self.sid}: the guard failed ({', '.join(bad[:10])}); nothing written")
        cells = [{"security_id": self.sid, "symbol": s["symbol"].iat[i], "date": md.iso(s["date"].iat[i]),
                  "column": c, "old": s[c].iat[i], "new": v}
                 for i, cs in sorted(self.writes.items()) for c, v in cs.items()]
        return out, cells



def equal(a, b):
    """Row mask: two columns hold the same value (NaN equal to NaN)."""
    try:
        x, y = a.to_numpy(float), b.to_numpy(float)
        return (x == y) | (np.isnan(x) & np.isnan(y))
    except (TypeError, ValueError):
        return np.array([str(p) == str(q) for p, q in zip(a, b)], bool)


def check_files(folder, counts_before, counts_after, nonok_before, nonok_after, changed_rows):
    """What rebuild_checks would change in equity_1min/check/ after an apply (from the stored labels in memory; nothing
    written): check_files.csv rows and the label counts of summary.json."""
    out = md.check_dir(folder)

    def disk_rows(name):
        p = os.path.join(out, name)
        if not os.path.isfile(p):
            return np.nan
        try:
            return len(pd.read_csv(p, usecols=[0]))
        except (OSError, ValueError):
            return np.nan

    try:
        with open(os.path.join(out, "summary.json"), encoding="utf-8") as f:
            disk = json.load(f).get("checks", {})
    except (OSError, ValueError):
        disk = {}
    rows = [{"file": "minute_check.csv", "rows_on_disk": disk_rows("minute_check.csv"), "rows_before": nonok_before,
             "rows_after": nonok_after, "rows_changed": changed_rows,
             "note": "one row per day whose check is not OK: the written rows' check / note change"},
            {"file": "minute_empty_days.csv", "rows_on_disk": disk_rows("minute_empty_days.csv"),
             "rows_before": np.nan, "rows_after": np.nan, "rows_changed": 0,
             "note": "unchanged: built from MINUTE_EMPTY labels, which the fix never adds or removes"},
            {"file": "scale_steps.csv", "rows_on_disk": disk_rows("scale_steps.csv"), "rows_before": np.nan,
             "rows_after": np.nan, "rows_changed": 0,
             "note": "unchanged: built from minute_scale / minute_volume_scale, which the fix never writes"},
            {"file": "summary.json", "rows_on_disk": np.nan, "rows_before": np.nan, "rows_after": np.nan,
             "rows_changed": sum(1 for k in set(counts_before) | set(counts_after)
                                 if counts_before.get(k, 0) != counts_after.get(k, 0)),
             "note": "label counts (checks) change as below; built_at is new"}]
    labels = [{"label": k, "on_disk": disk.get(k, np.nan), "before": counts_before.get(k, 0),
               "after": counts_after.get(k, 0)} for k in ["OK"] + md.CHECKS
              if counts_before.get(k, 0) or counts_after.get(k, 0) or disk.get(k, 0)]
    return rows, labels


def fix(folder=None, apply=False, expect_labels=None, expect_borrow=None, now=None, log=print):
    """See the module docstring. Returns a dict with the counts, the rows and cells and the report."""
    folder = folder or config.equity_data_dir()
    if apply and (expect_labels is None or expect_borrow is None):
        raise CountMismatch("fix-dayempty --apply needs --expect-labels N --expect-borrow M (the dry run's counts)")
    inp = mp.Inputs(folder, bhav=False)                    # refuses while a bhavcopy download / input build runs
    mp.check_fno(config.load_env())                        # ... and while a Kite F&O run holds its lock
    lock = RunLock(mp.lock_path(folder)).acquire("equity_minute.py fix-dayempty" + (" --apply" if apply else ""))
    tmps, backup, backed = [], None, []
    started = time.time()
    try:
        now = md.as_ist(now)
        stamp = f"{now:%Y-%m-%d_%H%M%S}"
        base_dir = ms.data_dir(folder)
        sids = sorted(n for n in os.listdir(base_dir) if os.path.isfile(os.path.join(base_dir, n, ms.DAYS_FILE))) \
            if os.path.isdir(base_dir) else []
        log(f"Scanning {len(sids):,} built stocks for DAY_EMPTY rows ({'apply' if apply else 'dry run, nothing written'})"
            " ...")
        calc = md.Calc(folder, inp, log if apply else md.quiet, write=False)   # a stale bhavcopy cache: CacheStale
        muhurat = muhurat_dates(folder)
        r = {"applied": apply, "expect_labels": expect_labels, "expect_borrow": expect_borrow, "stocks": len(sids),
             "day_empty": 0, "day_empty_stocks": 0, "labels": [], "borrow": [], "cells": [], "no_source": [],
             "info": [], "pending": [], "backup": None, "report_dir": None, "stamp": stamp}
        before, after = {}, {}
        nonok_before = nonok_after = changed_rows = 0
        for k, sid in enumerate(sids, 1):
            path = ms.stock_dir(folder, sid)
            chk = pq.read_table(os.path.join(path, ms.DAYS_FILE), columns=["check"]).column("check").to_pylist()
            for c in chk:
                for x in str(c).split("|"):
                    before[x] = before.get(x, 0) + 1
            nonok_before += sum(1 for c in chk if c != "OK")
            new_chk = list(chk)
            de = sum(1 for c in chk if DAY_EMPTY in str(c))
            if de:
                r["day_empty"] += de
                r["day_empty_stocks"] += 1
                st = Stock(calc, sid, ms.read_days(path), muhurat)
                for key in ("labels", "borrow", "no_source", "info"):
                    r[key] += getattr(st, key)
                if st.writes:
                    new, cells = st.new_frame()
                    r["cells"] += cells
                    r["pending"].append((sid, st.symbol, path, new))
                    new_chk = list(new["check"])
                    changed_rows += len(st.writes)
                    if any(("MINUTE_EMPTY" in str(a)) != ("MINUTE_EMPTY" in str(b)) for a, b in zip(chk, new_chk)):
                        raise md.ApplyFailed(f"{sid}: a MINUTE_EMPTY label would change; nothing written")
                del st
            for c in new_chk:
                for x in str(c).split("|"):
                    after[x] = after.get(x, 0) + 1
            nonok_after += sum(1 for c in new_chk if c != "OK")
            if k % PROGRESS_EVERY == 0:
                log(f"  {k:,} / {len(sids):,} stocks, labels {len(r['labels']):,}, borrow {len(r['borrow']):,}, "
                    f"{time.time() - started:,.0f} s")
        r["check_files"], r["check_labels"] = check_files(folder, before, after, nonok_before, nonok_after,
                                                          changed_rows)
        r["rows"] = changed_rows
        mismatch = [f"--expect-{w} {e} (found {len(r[k])})"
                    for w, k, e in (("labels", "labels", expect_labels), ("borrow", "borrow", expect_borrow))
                    if e is not None and e != len(r[k])]
        if apply and mismatch:
            raise CountMismatch(f"fix-dayempty: {', '.join(mismatch)}: nothing written")
        if apply and r["pending"]:
            backup = os.path.join(md.out_dir(folder), "archive", f"dayempty_{stamp}")
            manifest = []
            for sid, symbol, path, new in r["pending"]:
                src = os.path.join(path, ms.DAYS_FILE)
                dst = os.path.join(backup, sid, ms.DAYS_FILE)
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                shutil.copy2(src, dst)
                backed.append(sid)
                h = sha256(src)
                if sha256(dst) != h:
                    raise md.ApplyFailed(f"{sid}: the backup copy's sha256 differs from the source")
                manifest.append({"security_id": sid, "symbol": symbol, "source": src, "backup": dst,
                                 "bytes": os.path.getsize(dst), "sha256": h})
            sm.write_csv(pd.DataFrame(manifest, columns=MANIFEST_COLUMNS), os.path.join(backup, "manifest.csv"))
            sm.write_csv(pd.DataFrame(r["cells"], columns=CELL_COLUMNS), os.path.join(backup, "cells.csv"))
            for sid, _, path, new in r["pending"]:
                tmp = os.path.join(path, ms.DAYS_FILE + ".tmp")
                ms.write_days(new, tmp)
                tmps.append((tmp, os.path.join(path, ms.DAYS_FILE)))
            for tmp, p in tmps:
                mz._replace(tmp, p)
            tmps = []
            r["backup"] = backup
            prio = md.priorities(inp.membership)
            md.rebuild_checks(folder, {n: prio.get(n, 3) for n in sids})
        r["pending"] = len(r["pending"])
        r["peak_mb"] = round(md.memory()[1] / 1e6, 1)
        r["seconds"] = round(time.time() - started)
        r["report_dir"] = os.path.join(md.out_dir(folder), "dayempty", f"{'apply' if apply else 'dryrun'}_{stamp}")
        r["mismatch"] = mismatch
        r["report"] = report(r)
        write_report(r)
        log(r["report"], end="")
        if mismatch:
            raise CountMismatch(f"fix-dayempty: {', '.join(mismatch)} (report: {r['report_dir']})")
        return r
    except BaseException as e:
        if apply and backup and backed:
            for tmp, _ in tmps:
                if os.path.exists(tmp):
                    os.remove(tmp)
            for sid in backed:
                src = os.path.join(backup, sid, ms.DAYS_FILE)
                if os.path.isfile(src):
                    shutil.copy2(src, os.path.join(ms.stock_dir(folder, sid), ms.DAYS_FILE))
            if isinstance(e, Exception):
                raise md.ApplyFailed(f"fix-dayempty --apply failed ({type(e).__name__}: {e}); every days.parquet "
                                     f"restored from {backup}") from e
        raise
    finally:
        lock.release()


def by(rows, key):
    out = {}
    for x in rows:
        out[x[key]] = out.get(x[key], 0) + 1
    return ", ".join(f"{k} {v:,}" for k, v in sorted(out.items())) or "none"


def count_labels(rows):
    out = {k: sum(1 for x in rows if k in str(x["new_check"]).split("|")) for k in sorted(LABELS)}
    return ", ".join(f"{k} {v:,}" for k, v in out.items())


def lvl(x, w):
    if pd.isna(x[f"{w}_level"]):
        return "-"
    return "/".join(f"{x[f'{w}_{k}']:.4f}" for k in REF) + f" ({x[f'{w}_level']:.4f}x)"


def report(r):
    lines = [("APPLIED" if r["applied"] else "DRY RUN, nothing written to the store")
             + f": {r['stocks']:,} stocks scanned"]
    lines.append(f"1. Stored DAY_EMPTY rows: {r['day_empty']:,} in {r['day_empty_stocks']:,} stocks")
    lines.append(f"2. New labels (loaded minutes vs the bhavcopy, PRICE_REL x price + HALF_TICK): {len(r['labels']):,} "
                 f"rows ({count_labels(r['labels'])}; by day: {by(r['labels'], 'kind_of_day')})")
    for x in r["labels"][:SHOWN]:
        lines.append(f"  {x['symbol']} ({x['security_id']}) {x['date']}: {x['old_check']} -> {x['new_check']}")
    if len(r["labels"]) > SHOWN:
        lines.append(f"  ... {len(r['labels']) - SHOWN:,} more (labels.csv)")
    lines.append(f"3. New borrow source (OK day, same factor): {len(r['borrow']):,} rows in "
                 f"{len({x['security_id'] for x in r['borrow']}):,} stocks ({by(r['borrow'], 'result')}); loaded "
                 "high/low/close (level) before -> after, bhavcopy")
    for x in r["borrow"]:
        lines.append(f"  {x['symbol']} ({x['security_id']}) {x['date']}: {x['old_from']} -> {x['new_from']}; "
                     f"{lvl(x, 'old')} -> {lvl(x, 'new')}; bhav {x['bhav_high']:g}/{x['bhav_low']:g}/"
                     f"{x['bhav_close']:g}; {x['result']}; {x['new_check']}")
    lines.append(f"4. No OK source day (nearest same-factor day kept, note added): {len(r['no_source']):,}")
    for x in r["no_source"][:SHOWN]:
        lines.append(f"  {x['symbol']} ({x['security_id']}) {x['date']}: from {x['conversion_from'] or '-'}")
    held = [x for x in r["info"] if x["reason"] != "MINUTE_VOLUME_0"]
    lines.append(f"5. Held, never written: {len(held):,} ({by(held, 'reason')}); report only: minute volume 0 "
                 f"{sum(1 for x in r['info'] if x['reason'] == 'MINUTE_VOLUME_0'):,} (info.csv)")
    for x in held:
        lines.append(f"  {x['symbol']} ({x['security_id']}) {x['date']}: {x['reason']}: {x['detail']}")
    cols = {}
    for c in r["cells"]:
        cols[c["column"]] = cols.get(c["column"], 0) + 1
    lines.append(f"6. Cells written: {len(r['cells']):,} in {r['rows']:,} rows of {r['pending']:,} days.parquet files ("
                 + ", ".join(f"{k} {v:,}" for k, v in sorted(cols.items())) + ")")
    lines.append("  never written: minute bars, kite_*, bhav_*, minute aggregates / volume, minute_scale, our_factor, "
                 "every row not stored as DAY_EMPTY (guard checked per stock before anything is written)")
    lines.append("7. check/ files rebuild_checks would change after an apply (rows on disk / before / after / changed)")
    for x in r["check_files"]:
        f = lambda v: "-" if pd.isna(v) else f"{int(v):,}"   # noqa: E731
        lines.append(f"  {x['file']:<22} {f(x['rows_on_disk'])} / {f(x['rows_before'])} / {f(x['rows_after'])} / "
                     f"{f(x['rows_changed'])}: {x['note']}")
    for x in r["check_labels"]:
        if x["before"] != x["after"]:
            lines.append(f"    {x['label']:<16} {x['before']:>9,} -> {x['after']:,}")
    for w, k in (("labels", "labels"), ("borrow", "borrow")):
        e = r[f"expect_{w}"]
        if e is not None:
            lines.append(f"  --expect-{w} {e}: " + ("matches" if e == len(r[k]) else
                                                   f"DIFFERS (found {len(r[k])}), nothing written"))
    if r["backup"]:
        lines.append(f"Backup: {r['backup']} (manifest.csv with sha256, cells.csv)")
    lines.append(f"Report: {r['report_dir']}")
    lines.append(f"labels {len(r['labels'])}")
    lines.append(f"borrow {len(r['borrow'])}")
    lines.append(f"cells {len(r['cells'])}")
    lines.append(f"peak memory {r['peak_mb'] / 1e3:.2f} GB, {r['seconds']:,} s")
    return "\n".join(lines) + "\n"


def write_report(r):
    out = r["report_dir"]
    os.makedirs(out, exist_ok=True)
    sm.write_text(os.path.join(out, "report.txt"), r["report"])
    for name, key, cols in (("labels.csv", "labels", LABEL_COLUMNS), ("borrow.csv", "borrow", BORROW_COLUMNS),
                            ("cells.csv", "cells", CELL_COLUMNS), ("no_source.csv", "no_source", NO_SOURCE_COLUMNS),
                            ("info.csv", "info", INFO_COLUMNS),
                            ("check_files.csv", "check_files", CHECK_FILE_COLUMNS)):
        sm.write_csv(pd.DataFrame(r[key], columns=cols), os.path.join(out, name))
    sm.write_csv(pd.DataFrame(r["check_labels"], columns=["label", "on_disk", "before", "after"]),
                 os.path.join(out, "check_labels.csv"))

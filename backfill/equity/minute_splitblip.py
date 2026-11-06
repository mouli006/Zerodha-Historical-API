"""equity_minute.py fix-split-blip [--apply --expect N]: the day before a split / bonus ex-date whose minute bars Kite
left unadjusted while its day candle is adjusted, stored at minute_scale 1.0 (docs/dev/equity_minute_splitblip.md).

Calc.derive measures such a day correctly (about 1/F), but levels() used to treat one measured day as a blip and
stored 1.0 (minute_volume_scale too), so the day loaded F x too high in price and F x too low in volume (JSWDULUX
2026-10-21 after the 23 Oct repair, 28 rows of 2015 full downloads). Guard (a) in levels() / volume_levels()
(md.split_blip_rows) now keeps such a break; this command fixes the rows stored before it.

Selection (stored columns only), all of: stored minute_scale 1.0; check with CLOSE_DIFF and HIGHLOW_DIFF;
md.split_blip_rows: an adjustment (factors table) with ex-date = the stock's next stored trading day, factor F;
measured within 1% of 1/F; ratio within 1% of C (the adjustments with ex-date after the day and on or before the
fetch date) or of F.
Per stock one fresh Calc.derive of the stored inputs (with guard (a)); a selected row is fixed when the derive gives it
its measured level and check OK, the day loads within 1% of the bhavcopy LAST and 5% of its volume, the row's other
conversion cells and its neighbours' scales equal the stored ones, and its notes differ only by the scale step text.
Otherwise HELD with the reason.
Cells written (D137 / D140): the row's minute_scale, minute_volume_scale, check (CANDLE_UNCONFIRMED kept), note;
the next row's note. Never: ratio, vol_ratio, kind, kite_*, the minute aggregates, the measured scale,
our_factor, conversion_from, bhav_*, fetched_at, run_id, the minute bar files. Every other row: hash-checked
unchanged. Dry run (default) writes only equity_1min/splitblip/dryrun_<time>/. apply needs expect = the count found
(else CountMismatch, nothing written): backs up each changed days.parquet and the rows it changes to
equity_1min/archive/fix_split_blip_<time>/ first, writes .tmp files, checks their hashes, replaces them all at the end
(any error restores every file: ApplyFailed), then rebuilds check/. Both take equity_1min/.lock."""
import hashlib
import os
import shutil
import tempfile
import time

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

import config
from backfill.equity import adjustments as adj
from backfill.equity import minute_download as md
from backfill.equity import minute_plan as mp
from backfill.equity import minute_store as ms
from backfill.equity import security_master as sm
from backfill.locks import RunLock

PRICE_TOL = 0.01                 # the fixed day loads within 1% of the bhavcopy LAST ...
VOLUME_TOL = 0.05                # ... and within 5% of its volume (pre-open / closing session trades are not in minutes)
WRITTEN = ["minute_scale", "minute_volume_scale", "check", "note"]   # the selected row
NEXT = ["note"]                                                      # the next (ex-date) row: the scale step note
SAME = ["ratio", "vol_ratio", "kind", "kite_multiplier", "kite_difference"]
SCAN_COLUMNS = ["date", "symbol", "ratio", "minute_scale", "check", "fetched_at", "kite_high", "kite_low", "kite_close",
                "minute_high", "minute_low", "minute_rows"]
STEP_TEXT = ("minute_scale ", "minute_volume_scale ")
ROW_COLUMNS = ["security_id", "symbol", "date", "F", "C", "ratio", "measured", "result", "detail", "scale_before",
               "scale_after", "volume_scale_before", "volume_scale_after", "check_before", "check_after",
               "close_x_last_before", "close_x_last_after", "close_x_close_before", "close_x_close_after",
               "volume_x_bhav_before", "volume_x_bhav_after"]
CELL_COLUMNS = ["security_id", "symbol", "fixed_row", "date", "role", "column", "old", "new"]
HASH_COLUMNS = ["security_id", "rows", "rows_changed", "untouched_before", "untouched_after", "same"]
FIX = "FIX"


class CountMismatch(mp.MinutePlanError):
    """--expect N is not the count of rows to fix: nothing written."""


def same(a, b):
    try:
        x, y = float(a), float(b)
        return x == y or (np.isnan(x) and np.isnan(y))
    except (TypeError, ValueError):
        return str(a) == str(b)


def without_steps(note):
    """A note without its scale step parts (they move with the fix)."""
    return "; ".join(x for x in str(note).split("; ") if x and not x.startswith(STEP_TEXT))


def measure(t):
    """The measured minute scale per row from the stored day candle and minute aggregates, as Calc.derive measures it
    (Kite day / minute from high and low where both agree); the stored measured column is never read (zeropad rows
    kept an old value there)."""
    kh, kl, kc = (t[f"kite_{x}"].to_numpy(float) for x in ("high", "low", "close"))
    mh, ml = t["minute_high"].to_numpy(float), t["minute_low"].to_numpy(float)
    has = (t["minute_rows"].to_numpy(float) > 0) & ~np.isnan(kc)
    with np.errstate(divide="ignore", invalid="ignore"):
        sh, sl = kh / mh, kl / ml
        agree = np.abs(sh - sl) <= md.SCALE_AGREE + 2 * mp.HALF_TICK / kc
    return np.where(has & agree & (mh > 0) & (ml > 0), (sh + sl) / 2, np.nan)


def scan(folder, inp, sids, log):
    """{security_id: [(row index, F, C)]} of the selected rows, from the stored columns only."""
    periods = adj.periods_of(inp.factors)
    out, rows = {}, 0
    for k, sid in enumerate(sids, 1):
        t = pq.read_table(os.path.join(ms.stock_dir(folder, sid), ms.DAYS_FILE), columns=SCAN_COLUMNS).to_pandas()
        rows += len(t)
        dates = [sm.as_date(d) for d in t["date"]]
        keep, F, C = md.split_blip_rows(sid, dates, measure(t),
                                        t["ratio"].to_numpy(float), t["fetched_at"].to_numpy(object), periods, inp.days)
        chk = t["check"].astype(str)
        sel = keep & (t["minute_scale"].to_numpy(float) == 1.0) & \
            (chk.str.contains("CLOSE_DIFF") & chk.str.contains("HIGHLOW_DIFF")).to_numpy()
        if sel.any():
            out[sid] = [(int(i), float(F[i]), float(C[i])) for i in np.flatnonzero(sel)]
        if k % 500 == 0:
            log(f"  {k:,} / {len(sids):,} stocks scanned, {sum(len(v) for v in out.values()):,} rows selected")
    return out, rows


def bhav_day(folder, symbol, series, day):
    """(close, last, volume) of the bhavcopy store's row, or None."""
    path = os.path.join(mp.store_dir(folder), f"{day.year}.parquet")
    if not os.path.isfile(path):
        return None
    t = pq.read_table(path, columns=["date", "symbol", "series", "close", "last", "volume"],
                      filters=[("date", "==", day), ("symbol", "==", symbol)]).to_pandas()
    if len(t) > 1:
        t = t[t["series"] == series]
    if not len(t):
        return None
    return float(t["close"].iat[0]), float(t["last"].iat[0]), float(t["volume"].iat[0])


def loaded(row, conv):
    """(close, volume) the loader gives the day of `row` (its clean minute aggregates) with the conversion of conv."""
    m = pd.DataFrame([{**{c: conv[c] for c in ("kind", "ratio", "kite_multiplier", "kite_difference", "vol_ratio",
                                                "minute_scale", "minute_volume_scale")},
                       "open": row["minute_close"], "high": row["minute_high"], "low": row["minute_low"],
                       "close": row["minute_close"], "volume": row["minute_volume"]}])
    with np.errstate(divide="ignore", invalid="ignore"):
        c = ms.convert(m)
    return float(c["close"][0]), float(c["volume"][0])


def row_digest(df, rows, skip):
    """sha256 over the rows `rows` of df, every column but skip[row] (a set) of that row."""
    h = hashlib.sha256()
    cols = list(df.columns)
    for i in rows:
        vals = [repr(df.iat[i, j]) if c not in skip.get(i, ()) else "-" for j, c in enumerate(cols)]
        h.update(("\x1f".join(vals) + "\x1e").encode())
    return h.hexdigest()


class Stock:
    """One stock's selected rows: the fresh derive, the per-row decision, the new frame and its guard."""

    def __init__(self, calc, folder, sid, stored, picks):
        self.sid, self.stored = sid, stored
        self.symbol = stored["symbol"].iat[-1] if len(stored) else sid
        self.measured = measure(stored)
        self.trial = calc.derive(sid, stored[md.INPUT_COLUMNS], bhav=calc.bhav_of(sid))
        if list(self.trial["date"]) != list(stored["date"]):
            raise md.ApplyFailed(f"{sid}: days.parquet is not sorted by date; not fixed")
        self.rows, self.fixes = [], []
        for i, f, c in picks:
            why, detail, rec = self.decide(folder, i)
            self.rows.append({"security_id": sid, "symbol": self.symbol, "date": md.iso(stored["date"].iat[i]),
                              "F": round(f, 6), "C": round(c, 6) if not np.isnan(c) else np.nan,
                              "ratio": stored.at[i, "ratio"], "measured": round(float(self.measured[i]), 6),
                              "result": why or FIX, "detail": detail, **rec})
            if not why:
                self.fixes.append(i)

    def expected(self, i, c):
        v = self.trial.at[i, c]
        if c == "check" and md.UNCONFIRMED in str(self.stored.at[i, "check"]):
            v = md.add_check(v, md.UNCONFIRMED)          # set by update, never derived (as checks)
        return v

    def decide(self, folder, i):
        """(reason or None, detail, before / after record) of selected row i."""
        s, t = self.stored, self.trial
        rec = {"scale_before": s.at[i, "minute_scale"], "scale_after": t.at[i, "minute_scale"],
               "volume_scale_before": s.at[i, "minute_volume_scale"], "volume_scale_after": t.at[i, "minute_volume_scale"],
               "check_before": s.at[i, "check"], "check_after": self.expected(i, "check")}
        b = bhav_day(folder, s.at[i, "symbol"], s.at[i, "series"], s.at[i, "date"])
        if b:
            for w, conv in (("before", s.iloc[i]), ("after", t.iloc[i])):
                close, vol = loaded(s.iloc[i], conv)
                rec[f"close_x_last_{w}"], rec[f"close_x_close_{w}"] = close / b[1], close / b[0]
                rec[f"volume_x_bhav_{w}"] = vol / b[2] if b[2] else np.nan
        meas = self.measured[i]
        if not same(t.at[i, "minute_scale"], round(float(meas), 6)) or t.at[i, "minute_scale"] == 1.0:
            return "TRIAL_LEVEL", f"the derive gives minute_scale {t.at[i, 'minute_scale']}, measured {meas}", rec
        if t.at[i, "check"] != "OK":                      # derive never sets CANDLE_UNCONFIRMED
            return "TRIAL_CHECK", f"the derive gives check {t.at[i, 'check']}", rec
        if not b:
            return "NO_BHAV", "no bhavcopy row for the day", rec
        if abs(rec["close_x_last_after"] - 1) > PRICE_TOL:
            return "LOADED_OFF", f"loaded close / bhavcopy LAST {rec['close_x_last_after']:.4f}", rec
        if abs(rec["volume_x_bhav_after"] - 1) > VOLUME_TOL:
            return "VOLUME_OFF", f"loaded volume / bhavcopy volume {rec['volume_x_bhav_after']:.4f}", rec
        pre = [c for c in SAME if not same(s.at[i, c], t.at[i, c])]
        for j in (i - 1, i + 1):
            if 0 <= j < len(s):
                pre += [f"{c} of {md.iso(s['date'].iat[j])}" for c in ("minute_scale", "minute_volume_scale")
                        if not same(s.at[j, c], t.at[j, c])]
        if without_steps(s.at[i, "note"]) != without_steps(t.at[i, "note"]):
            pre.append("note")
        if i + 1 < len(s) and without_steps(s.at[i + 1, "note"]) != without_steps(t.at[i + 1, "note"]):
            pre.append(f"note of {md.iso(s['date'].iat[i + 1])}")
        if pre:
            return "PRE_EXISTING", "stored and derived differ in " + ", ".join(pre), rec
        return None, f"minute_scale 1 -> {t.at[i, 'minute_scale']:g}", rec

    def cells(self):
        """[(fixed row, row, role, column)] of every written cell."""
        out = []
        for i in self.fixes:
            out += [(i, i, "target", c) for c in WRITTEN]
            if i + 1 < len(self.stored) and i + 1 not in self.fixes:
                out += [(i, i + 1, "next", c) for c in NEXT]
        return out

    def skips(self):
        """{row: the columns the fix may change}."""
        out = {}
        for _, j, _, c in self.cells():
            out.setdefault(j, set()).add(c)
        return out

    def new_frame(self):
        """(new frame, cells.csv rows, hashes.csv row); raises when a written cell is not the derive's or any other
        cell is not the stored one (the guard)."""
        s = self.stored
        new = s.copy()
        cells = self.cells()
        for _, j, _, c in cells:
            new.at[j, c] = self.expected(j, c)
        for c in WRITTEN:                                        # keep the stored dtypes
            if new[c].dtype != s[c].dtype:
                try:
                    new[c] = new[c].astype(s[c].dtype)
                except (TypeError, ValueError):
                    pass
        bad = [f"{md.iso(s['date'].iat[j])} {c}" for _, j, _, c in cells if not same(new.at[j, c], self.expected(j, c))]
        skip = self.skips()
        before, after = row_digest(s, range(len(s)), skip), row_digest(new, range(len(new)), skip)
        if before != after or list(new.columns) != list(s.columns) or len(new) != len(s):
            bad.append("cells outside the fix")
        if bad:
            raise md.ApplyFailed(f"{self.sid}: the guard failed ({', '.join(bad)}); nothing written")
        out = [{"security_id": self.sid, "symbol": self.symbol, "fixed_row": md.iso(s["date"].iat[f]),
                "date": md.iso(s["date"].iat[j]), "role": role, "column": c, "old": s.at[j, c], "new": new.at[j, c]}
               for f, j, role, c in cells]
        changed = sum(1 for j in skip if any(not same(s.at[j, c], new.at[j, c]) for c in skip[j]))
        return new, out, {"security_id": self.sid, "rows": len(s), "rows_changed": changed,
                          "untouched_before": before, "untouched_after": after, "same": "Y"}


def fix(folder=None, apply=False, expect=None, now=None, log=print):
    """See the module docstring. Returns a dict with the counts, the rows, the cells and the report."""
    folder = folder or config.equity_data_dir()
    if apply and expect is None:
        raise CountMismatch("fix-split-blip --apply needs --expect N (the count of the dry run)")
    inp = mp.Inputs(folder, bhav=False)                    # refuses while a bhavcopy download / input build runs
    mp.check_fno(config.load_env())                        # ... and while a Kite F&O run holds its lock
    lock = RunLock(mp.lock_path(folder)).acquire("equity_minute.py fix-split-blip" + (" --apply" if apply else ""))
    work, tmps, backup, backed = None, [], None, []
    started = time.time()
    try:
        now = md.as_ist(now)
        stamp = f"{now:%Y-%m-%d_%H%M%S}"
        base_dir = ms.data_dir(folder)
        sids = sorted(n for n in os.listdir(base_dir) if os.path.isfile(os.path.join(base_dir, n, ms.DAYS_FILE))) \
            if os.path.isdir(base_dir) else []
        log(f"Scanning {len(sids):,} built stocks for split blips ({'apply' if apply else 'dry run, nothing written'})")
        picks, store_rows = scan(folder, inp, sids, log)
        r = {"applied": apply, "expect": expect, "stocks": len(sids), "store_rows": store_rows, "rows": [],
             "cells": [], "hashes": [], "pending": [], "backup": None, "report_dir": None}
        if picks:
            if apply:
                calc = md.Calc(folder, inp, log)
            else:
                try:
                    calc = md.Calc(folder, inp, write=False)
                except md.CacheStale:
                    work = tempfile.mkdtemp(prefix="splitblip_")
                    log("The bhavcopy cache is older than the store: a temporary one is built for the dry run.")
                    calc = md.Calc(folder, inp, write=True, cache_out=os.path.join(work, md.CACHE))
            for sid in sorted(picks):
                path = ms.stock_dir(folder, sid)
                st = Stock(calc, folder, sid, ms.read_days(path), picks[sid])
                r["rows"] += st.rows
                if st.fixes:
                    new, cells, hashes = st.new_frame()
                    r["cells"] += cells
                    r["hashes"].append(hashes)
                    r["pending"].append((sid, path, new, st))
        r["fixed"] = [x for x in r["rows"] if x["result"] == FIX]
        r["held"] = [x for x in r["rows"] if x["result"] != FIX]
        r["rows_changed"] = sum(h["rows_changed"] for h in r["hashes"])
        mismatch = expect is not None and len(r["fixed"]) != expect
        if apply and mismatch:
            raise CountMismatch(f"fix-split-blip found {len(r['fixed'])} rows to fix, not --expect {expect}: nothing "
                                "written")
        if apply and r["pending"]:
            backup = os.path.join(md.out_dir(folder), "archive", f"fix_split_blip_{stamp}")
            before_rows = []
            for sid, path, new, st in r["pending"]:              # the backup first, before any write
                dst = os.path.join(backup, sid, ms.DAYS_FILE)
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                shutil.copy2(os.path.join(path, ms.DAYS_FILE), dst)
                backed.append(sid)
                before_rows.append(st.stored.iloc[sorted(st.skips())].assign(security_id=sid))
            sm.write_csv(pd.concat(before_rows, ignore_index=True), os.path.join(backup, "rows_before.csv"))
            sm.write_csv(pd.DataFrame(r["cells"], columns=CELL_COLUMNS), os.path.join(backup, "cells.csv"))
            for sid, path, new, st in r["pending"]:
                tmp = os.path.join(path, ms.DAYS_FILE + ".tmp")
                ms.write_days(new, tmp)
                tmps.append((tmp, os.path.join(path, ms.DAYS_FILE)))
                back = pq.read_table(tmp).to_pandas()
                back["date"] = [sm.as_date(d) for d in back["date"]]
                skip = st.skips()
                if row_digest(back, range(len(back)), skip) != row_digest(st.stored, range(len(st.stored)), skip):
                    raise md.ApplyFailed(f"{sid}: the written file differs outside the fixed cells")
            for tmp, p in tmps:
                _replace(tmp, p)
            tmps = []
            r["backup"] = backup
            prio = md.priorities(inp.membership)
            md.rebuild_checks(folder, {n: prio.get(n, 3) for n in sids})
        r["seconds"] = round(time.time() - started)
        r["report_dir"] = os.path.join(md.out_dir(folder), "splitblip", f"{'apply' if apply else 'dryrun'}_{stamp}")
        r["report"] = report(r)
        write_report(r)
        log(r["report"], end="")
        if mismatch:
            raise CountMismatch(f"fix-split-blip found {len(r['fixed'])} rows to fix, not --expect {expect} "
                                f"(report: {r['report_dir']})")
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
                raise md.ApplyFailed(f"fix-split-blip --apply failed ({type(e).__name__}: {e}); every days.parquet "
                                     f"restored from {backup}") from e
        raise
    finally:
        if work:
            shutil.rmtree(work, ignore_errors=True)
        lock.release()


def _replace(tmp, path):
    os.replace(tmp, path)


def fmt(v, spec=".4f"):
    try:
        return "-" if pd.isna(v) else format(float(v), spec)
    except (TypeError, ValueError):
        return str(v)


def report(r):
    fixed, held = r["fixed"], r["held"]
    by_c = sum(1 for x in r["rows"] if not pd.isna(x["C"]) and abs(x["ratio"] / x["C"] - 1) <= md.SPLIT_BLIP_TOL)
    lines = [("APPLIED" if r["applied"] else "DRY RUN, nothing written to the store")
             + f": {r['stocks']:,} stocks, {r['store_rows']:,} rows scanned",
             f"Selected {len(r['rows']):,} rows in {len({x['security_id'] for x in r['rows']}):,} stocks (ratio ~ C: "
             f"{by_c:,}, ~ F only: {len(r['rows']) - by_c:,})",
             f"To fix {len(fixed):,}, held {len(held):,}"]
    if r["expect"] is not None:
        lines.append(f"--expect {r['expect']}: " + ("matches" if r["expect"] == len(fixed) else
                                                     f"DIFFERS (found {len(fixed)}), nothing written"))
    nxt = sum(1 for c in r["cells"] if c["role"] == "next")
    lines.append(f"Rows changed {r['rows_changed']:,} ({len(fixed):,} fixed rows + {nxt:,} next-row notes), cells "
                 f"{sum(1 for c in r['cells'] if not same(c['old'], c['new'])):,}")
    files = len(r["hashes"])
    untouched = r["store_rows"] - r["rows_changed"]
    ok = all(h["same"] == "Y" for h in r["hashes"])
    lines.append(f"Unchanged rows: {untouched:,} ({'hash-identical' if ok else 'HASH MISMATCH'} in the {files:,} files "
                 f"written; {r['stocks'] - files:,} files not written)")
    lines.append("Rows (F, C, minute_scale, volume scale, check, loaded close / bhavcopy LAST, loaded volume / "
                 "bhavcopy volume: before -> after)")
    for x in r["rows"]:
        lines.append(f"  {x['symbol']} ({x['security_id']}) {x['date']} {x['result']}: F {fmt(x['F'], 'g')} C "
                     f"{fmt(x['C'], 'g')}; scale {fmt(x['scale_before'], 'g')} -> {fmt(x['scale_after'], 'g')}, vol scale "
                     f"{fmt(x['volume_scale_before'], 'g')} -> {fmt(x['volume_scale_after'], 'g')}; {x['check_before']} -> "
                     f"{x['check_after']}; close/LAST {fmt(x.get('close_x_last_before'))} -> "
                     f"{fmt(x.get('close_x_last_after'))}; volume {fmt(x.get('volume_x_bhav_before'))} -> "
                     f"{fmt(x.get('volume_x_bhav_after'))}" + ("" if x["result"] == FIX else f" ({x['detail']})"))
    lines.append("Cells written: fixed row " + ", ".join(WRITTEN) + "; next row " + ", ".join(NEXT) + ". Never: ratio, "
                 "vol_ratio, kind, kite_*, minute aggregates, measured scale, our_factor, conversion_from, "
                 "bhav_*, fetched_at, run_id, minute bar files")
    if r["backup"]:
        lines.append(f"Backup: {r['backup']} (days.parquet per stock, rows_before.csv, cells.csv)")
    lines.append(f"Report: {r['report_dir']}")
    lines.append(f"{r['seconds']:,} s")
    return "\n".join(lines) + "\n"


def write_report(r):
    out = r["report_dir"]
    os.makedirs(out, exist_ok=True)
    sm.write_text(os.path.join(out, "report.txt"), r["report"])
    sm.write_csv(pd.DataFrame(r["rows"], columns=ROW_COLUMNS), os.path.join(out, "rows.csv"))
    sm.write_csv(pd.DataFrame(r["held"], columns=ROW_COLUMNS), os.path.join(out, "held.csv"))
    sm.write_csv(pd.DataFrame(r["cells"], columns=CELL_COLUMNS), os.path.join(out, "cells.csv"))
    sm.write_csv(pd.DataFrame(r["hashes"], columns=HASH_COLUMNS), os.path.join(out, "hashes.csv"))

"""equity_minute.py fix-rescale [--apply --expect N]: the PADDING rows fix-zero-padding held whose stored minute_scale
loads the day at the wrong level, fixed (docs/dev/equity_minute_rescale.md).

A padded day (minute_low 0, Kite's all-zero bars counted) is never measured (its measured scale NaN), so levels()
gives it the level of its neighbour. On the first day of a real minute / day scale period, or the first day after one,
that is the wrong level: BALKRISIND 2015-03-24 loads 2.00x the bhavcopy, JMA 2015-03-30 0.20x. fix-zero-padding holds
such rows (their minute_scale would move). A held PADDING row is fixed here only when
    the stored loaded level is more than 1% off the bhavcopy, and the fresh derive (the row's aggregates without the
    zero bars, Calc.derive of the stock) is within 1%;
    level = the median of loaded high / low / close (ms.convert of the row's clean aggregates) / bhavcopy high / low /
    close (prices "ours" against bhavcopy / factor: the factor cancels);
    nothing else of the fresh derive moves against the derive of the stored rows (derive against derive, so stored
    rows that differ for an unrelated reason, e.g. weekend special-session bhav rows on EXTRA_SESSION rows, do not
    block it and are never written).
Cells written: on the row zeropad's FIXED cells (aggregates, measured scale, check with CANDLE_UNCONFIRMED kept, note)
and minute_scale; on the next row only check and note. The guard: those cells equal the fresh derive, every other cell equals
the stored file. Never written: ratio, kind, vol_ratio, minute_volume, minute_volume_scale, neg_volume_bars, kite_*,
our_factor, conversion_from, and the minute bar files. Dry run (default) writes only
equity_1min/zeropad/rescale_dryrun_<time>/. apply needs expect = the count found (else CountMismatch, nothing
written): backs up each changed days.parquet to equity_1min/archive/zeropad_rescale_<time>/<security_id>/, writes .tmp
files, replaces them all at the end (any error restores every file: ApplyFailed), writes rescale.csv into the backup
and rebuilds check/. Same lock and refusals as fix-zero-padding."""
import os
import shutil
import tempfile
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
from backfill.equity.minute_negvol import same
from backfill.equity.minute_repair import different
from backfill.locks import RunLock

TOL = 0.01                                   # a loaded level within 1% of the bhavcopy is right (tick noise)
WRITTEN = mz.FIXED + ["minute_scale"]        # the fixed row's cells
NEXT = ["check", "note"]                     # the next row's cells (the scale step note moves)
REF = ("high", "low", "close")
NOT_WRITTEN = ["ratio", "kind", "vol_ratio", "minute_volume", "minute_volume_scale", "neg_volume_bars", "kite_*",
               "our_factor", "conversion_from"]
CELL_COLUMNS = ["security_id", "symbol", "fixed_row", "date", "role", "column", "old", "new", "changed"]
LEVEL_COLUMNS = ["security_id", "symbol", "date", "result", "detail", "old_minute_scale", "new_minute_scale"] + \
    [f"{w}_{k}" for w in ("old", "new") for k in REF + ("level",)]
SHOWN = 100


class CountMismatch(mp.MinutePlanError):
    """--expect N is not the count of rows found: nothing written."""


def loaded_level(agg, conv, bhav):
    """{high, low, close, level}: the loaded high / low / close (ms.convert with the conversion columns of `conv`) of
    the clean aggregates `agg` divided by the bhavcopy's, and their median; None without a reference."""
    p = {k: float(agg[f"minute_{k}"]) for k in REF}
    b = {k: float(bhav[f"bhav_{k}"]) for k in REF}
    if not all(np.isfinite(v) and v > 0 for v in list(p.values()) + list(b.values())):
        return None
    m = pd.DataFrame([{**{c: conv[c] for c in mz.LOADED}, "open": p["close"], **p, "volume": 0}])
    with np.errstate(divide="ignore", invalid="ignore"):
        c = ms.convert(m)
    out = {k: float(c[k][0]) / b[k] for k in REF}
    out["level"] = float(np.median([out[k] for k in REF]))
    return out


def unconfirmed_off(check):
    """The stored check without CANDLE_UNCONFIRMED (set by update, never derived)."""
    return "|".join(x for x in str(check).split("|") if x != md.UNCONFIRMED) or "OK"


class Stock:
    """One stock's candidate rows: the selection, the fixed rows' trial derive, the new frame and its guard."""

    def __init__(self, calc, sid, stored, padding):
        self.calc, self.sid, self.stored = calc, sid, stored
        self.symbol = stored["symbol"].iat[-1] if len(stored) else sid
        self.bhav = calc.bhav_of(sid)
        self.inputs = stored[md.INPUT_COLUMNS]
        self.base = calc.derive(sid, self.inputs, bhav=self.bhav)
        if list(self.base["date"]) != list(stored["date"]):
            raise md.ApplyFailed(f"{sid}: days.parquet is not sorted by date; not fixed")
        self.pos = {d: i for i, d in enumerate(stored["date"])}
        self.rows, self.fixes = [], {}         # rows: one record per candidate (levels.csv / held_rows.csv)
        dates = sorted(padding)
        agg = md.minute_aggregates(pd.concat([padding[d] for d in dates]).assign(run_id="", fetched_at=""))
        for d in dates:
            i = self.pos[d]
            why, detail, old, new, vals, trial = self.select(i, agg.get(d))
            self.record(i, why or "FIX", detail, old, new, trial)
            if not why:
                self.fixes[i] = vals
        self.trial = self.derive(self.fixes)
        if self.fixes:                         # the fixed rows together: still nothing else moves, still within 1%
            moved = self.moves(self.trial, self.fixes)
            if moved:
                raise md.ApplyFailed(f"{sid}: the fixed rows together would move {moved}; nothing written")
            for i, vals in self.fixes.items():
                lv = loaded_level(vals, self.trial.iloc[i], self.base.iloc[i])
                if lv is None or abs(lv["level"] - 1) > TOL:
                    raise md.ApplyFailed(f"{sid} {md.iso(stored['date'].iat[i])}: the fixed rows together leave the "
                                         "level off the bhavcopy; nothing written")

    def select(self, i, a):
        """(reason or None, detail, stored level, fresh level, new aggregates, trial) of candidate row i."""
        s, b = self.stored, self.base
        if not a:
            return "NO_REFERENCE", "only zero bars: no minute prices", None, None, None, None
        vals = {c: a[c] for c in mz.AGGREGATES}
        if not same(s.at[i, "minute_volume"], a["minute_volume"]) or \
                int(s.at[i, "neg_volume_bars"]) != int(a["neg_volume_bars"]):
            return "VOLUME_CHANGE", f"minute_volume {s.at[i, 'minute_volume']} -> {a['minute_volume']}", \
                None, None, None, None
        old = loaded_level(vals, s.iloc[i], b.iloc[i])
        if old is None:
            return "NO_REFERENCE", "no bhavcopy high / low / close", None, None, None, None
        trial = self.derive({i: vals})
        new = loaded_level(vals, trial.iloc[i], b.iloc[i])
        if abs(old["level"] - 1) <= TOL:
            return "STORED_OK", f"stored level {old['level']:.4f} within {TOL:.0%}", old, new, None, trial
        if abs(new["level"] - 1) > TOL:
            return "FRESH_OFF", f"fresh level {new['level']:.4f} not within {TOL:.0%}", old, new, None, trial
        pre = [c for c in mz.CONVERSION if not same(b.at[i, c], s.at[i, c])]
        j = i + 1
        if j < len(s):
            if unconfirmed_off(s.at[j, "check"]) != b.at[j, "check"]:
                pre.append(f"next row check {s.at[j, 'check']} (derive {b.at[j, 'check']})")
            if not same(s.at[j, "note"], b.at[j, "note"]):
                pre.append("next row note")
        if pre:
            return "PRE_EXISTING", "the stored " + ", ".join(pre) + " differ from a derive of the stored rows", \
                old, new, None, trial
        moved = self.moves(trial, {i: vals})
        if moved:
            return "OTHER_MOVES", f"the fresh derive also moves {moved}", old, new, None, trial
        return None, f"level {old['level']:.4f} -> {new['level']:.4f}", old, new, vals, trial

    def record(self, i, result, detail, old, new, trial):
        x = {"security_id": self.sid, "symbol": self.symbol, "date": md.iso(self.stored["date"].iat[i]),
             "result": result, "detail": detail, "old_minute_scale": self.stored.at[i, "minute_scale"],
             "new_minute_scale": trial.at[i, "minute_scale"] if trial is not None else np.nan}
        for w, lv in (("old", old), ("new", new)):
            for k in REF + ("level",):
                x[f"{w}_{k}"] = round(lv[k], 6) if lv else np.nan
        self.rows.append(x)

    def derive(self, fixes):
        if not fixes:
            return self.base
        t = self.inputs.copy()
        for i, vals in fixes.items():
            for c, v in vals.items():
                t.at[i, c] = v
        return self.calc.derive(self.sid, t, bhav=self.bhav)

    def masks(self, targets):
        """(target rows, next rows, the rest) as boolean masks."""
        n = len(self.stored)
        tgt, nxt = np.zeros(n, bool), np.zeros(n, bool)
        tgt[list(targets)] = True
        for i in targets:
            if i + 1 < n:
                nxt[i + 1] = True
        nxt &= ~tgt
        return tgt, nxt, ~(tgt | nxt)

    def outside(self, a, b, targets):
        """Columns where a and b differ outside the cells the fix writes ([] = none)."""
        tgt, nxt, rest = self.masks(targets)
        out = [f"{c} (other rows)" for c in different(a, b, rest)[0]]
        out += [f"{c} (fixed row)" for c in different(a, b, tgt)[0] if c not in WRITTEN]
        out += [f"{c} (next row)" for c in different(a, b, nxt)[0] if c not in NEXT]
        return out

    def moves(self, trial, targets):
        return self.outside(self.base, trial, targets)

    def expected(self, i, c):
        v = self.trial.at[i, c]
        if c == "check" and md.UNCONFIRMED in str(self.stored.at[i, "check"]):
            v = md.add_check(v, md.UNCONFIRMED)          # set by update, never derived (as checks)
        return v

    def new_frame(self):
        """(the days.parquet frame with the fixed cells, the cells.csv rows); raises when a written cell is not the
        fresh derive's or any other cell is not the stored one (the guard)."""
        s = self.stored
        new = s.copy()
        cells = []
        for i in sorted(self.fixes):
            cells += [(i, i, "target", c) for c in WRITTEN]
            if i + 1 < len(s) and i + 1 not in self.fixes:
                cells += [(i, i + 1, "next", c) for c in NEXT]
        for _, i, _, c in cells:
            new.at[i, c] = self.expected(i, c)
        for c in WRITTEN:                                        # keep the stored dtypes
            if new[c].dtype != s[c].dtype:
                try:
                    new[c] = new[c].astype(s[c].dtype)
                except (TypeError, ValueError):
                    pass
        bad = [f"{md.iso(s['date'].iat[i])} {c}" for _, i, _, c in cells
               if not same(new.at[i, c], self.expected(i, c))]
        bad += self.outside(s, new, self.fixes)
        if bad:
            raise md.ApplyFailed(f"{self.sid}: the guard failed ({', '.join(bad)}); nothing written")
        out = [{"security_id": self.sid, "symbol": self.symbol, "fixed_row": md.iso(s["date"].iat[f]),
                "date": md.iso(s["date"].iat[i]), "role": role, "column": c, "old": s.at[i, c], "new": new.at[i, c],
                "changed": "N" if same(s.at[i, c], new.at[i, c]) else "Y"} for f, i, role, c in cells]
        return new, out


def stock_bars(path, cand, r):
    """{date: the day's stored bars} of the candidate dates (only the needed year files are read)."""
    years = ms.year_paths(path)
    bars = {}
    for y in sorted({d.year for d in cand}):
        if y not in years:
            continue
        m = pq.read_table(years[y]).to_pandas()
        r["years_read"] += 1
        day = m["ts"].dt.date
        for d in cand:
            if d.year == y:
                bars[d] = m[day == d].sort_values("ts", kind="stable").reset_index(drop=True)
        del m
    return bars


def fix(folder=None, apply=False, expect=None, now=None, log=print):
    """See the module docstring. Returns a dict with the counts, the candidate rows, the written cells and the report."""
    folder = folder or config.equity_data_dir()
    if apply and expect is None:
        raise CountMismatch("fix-rescale --apply needs --expect N (the count of the dry run)")
    inp = mp.Inputs(folder, bhav=False)                    # refuses while a bhavcopy download / input build runs
    mp.check_fno(config.load_env())                        # ... and while a Kite F&O run holds its lock
    lock = RunLock(mp.lock_path(folder)).acquire("equity_minute.py fix-rescale" + (" --apply" if apply else ""))
    work, tmps, backup, backed = None, [], None, []
    started = time.time()
    try:
        now = md.as_ist(now)
        stamp = f"{now:%Y-%m-%d_%H%M%S}"
        base_dir = ms.data_dir(folder)
        sids = sorted(n for n in os.listdir(base_dir) if os.path.isfile(os.path.join(base_dir, n, ms.DAYS_FILE))) \
            if os.path.isdir(base_dir) else []
        mode = "apply" if apply else "dry run, nothing written"
        log(f"Scanning {len(sids):,} built stocks for held PADDING rows ({mode}) ...")
        if apply:
            calc = md.Calc(folder, inp, log)
            backup = os.path.join(md.out_dir(folder), "archive", f"zeropad_rescale_{stamp}")
        else:
            try:
                calc = md.Calc(folder, inp, write=False)
            except md.CacheStale:
                work = tempfile.mkdtemp(prefix="rescale_")
                log("The bhavcopy cache is older than the store: a temporary one is built for the dry run.")
                calc = md.Calc(folder, inp, write=True, cache_out=os.path.join(work, md.CACHE))
        r = {"applied": apply, "expect": expect, "stocks": len(sids), "candidates": [], "cells": [], "other": 0,
             "pending": [], "years_read": 0, "backup": None, "report_dir": None}
        for k, sid in enumerate(sids, 1):
            path = ms.stock_dir(folder, sid)
            t = pq.read_table(os.path.join(path, ms.DAYS_FILE), columns=mz.SCAN_COLUMNS).to_pandas()
            t["date"] = [sm.as_date(d) for d in t["date"]]
            cand = list(t.loc[t["minute_low"].to_numpy(float) <= 0, "date"])
            if cand:
                bars = stock_bars(path, cand, r)
                padding = {}
                for d in cand:
                    b = bars.get(d, pd.DataFrame(columns=ms.MINUTE_COLUMNS))
                    if mz.classify(b)[0] == "PADDING":
                        padding[d] = b
                    else:
                        r["other"] += 1
                if padding:
                    st = Stock(calc, sid, ms.read_days(path), padding)
                    r["candidates"] += st.rows
                    if st.fixes:
                        new, cells = st.new_frame()
                        r["cells"] += cells
                        r["pending"].append((sid, path, new))
            if k % mz.PROGRESS_EVERY == 0:
                log(f"  {k:,} / {len(sids):,} stocks, {sum(1 for x in r['candidates'] if x['result'] == 'FIX'):,} "
                    f"rows to fix, {time.time() - started:,.0f} s")
        r["fixed"] = [x for x in r["candidates"] if x["result"] == "FIX"]
        r["held"] = [x for x in r["candidates"] if x["result"] != "FIX"]
        mismatch = expect is not None and len(r["fixed"]) != expect
        if apply and not mismatch:
            for sid, path, new in r["pending"]:
                dst = os.path.join(backup, sid, ms.DAYS_FILE)
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                shutil.copy2(os.path.join(path, ms.DAYS_FILE), dst)
                backed.append(sid)
                tmp = os.path.join(path, ms.DAYS_FILE + ".tmp")
                ms.write_days(new, tmp)
                tmps.append((tmp, os.path.join(path, ms.DAYS_FILE)))
            for tmp, p in tmps:
                mz._replace(tmp, p)
            tmps = []
            if r["fixed"]:
                sm.write_csv(pd.DataFrame(r["cells"], columns=CELL_COLUMNS), os.path.join(backup, "rescale.csv"))
                r["backup"] = backup
                prio = md.priorities(inp.membership)
                md.rebuild_checks(folder, {n: prio.get(n, 3) for n in sids})
        r["peak_mb"] = round(md.memory()[1] / 1e6, 1)
        r["seconds"] = round(time.time() - started)
        if mismatch:
            text = ", ".join(f"{x['symbol']} {x['date']}" for x in r["fixed"]) or "none"
            if apply:
                raise CountMismatch(f"fix-rescale found {len(r['fixed'])} rows to fix ({text}), not --expect "
                                    f"{expect}: nothing written")
        r["report_dir"] = os.path.join(md.out_dir(folder), "zeropad",
                                       f"rescale_{'apply' if apply else 'dryrun'}_{stamp}")
        r["report"] = report(r)
        write_report(r)
        log(r["report"], end="")
        if mismatch:
            raise CountMismatch(f"fix-rescale found {len(r['fixed'])} rows to fix, not --expect {expect} "
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
                raise md.ApplyFailed(f"fix-rescale --apply failed ({type(e).__name__}: {e}); every days.parquet "
                                     f"restored from {backup}") from e
        raise
    finally:
        if work:
            shutil.rmtree(work, ignore_errors=True)
        lock.release()


def levels_text(x, w):
    if pd.isna(x[f"{w}_level"]):
        return "-"
    return "/".join(f"{x[f'{w}_{k}']:.4f}" for k in REF) + f" (level {x[f'{w}_level']:.2f}x)"


def report(r):
    lines = []
    fixed, held = r["fixed"], r["held"]
    lines.append(("APPLIED" if r["applied"] else "DRY RUN, nothing written to the store")
                 + f": {r['stocks']:,} stocks scanned")
    lines.append(f"1. Held PADDING rows (minute_low <= 0, all-zero bars): {len(r['candidates']):,} rows in "
                 f"{len({x['security_id'] for x in r['candidates']}):,} stocks (OTHER rows skipped: {r['other']:,})")
    lines.append(f"  to fix: {len(fixed):,} rows in {len({x['security_id'] for x in fixed}):,} stocks (stored level "
                 f"more than {TOL:.0%} off the bhavcopy, the fresh derive within {TOL:.0%}, nothing else moves)")
    reasons = {}
    for x in held:
        reasons[x["result"]] = reasons.get(x["result"], 0) + 1
    lines.append(f"  stay held: {len(held):,}" + (" (" + ", ".join(f"{k} {v:,}" for k, v in sorted(reasons.items()))
                                                   + ")" if held else ""))
    if r["expect"] is not None:
        lines.append(f"  --expect {r['expect']}: " + ("matches" if r["expect"] == len(fixed) else
                                                       f"DIFFERS (found {len(fixed)}), nothing written"))
    lines.append("2. Rows to fix (loaded high / low / close / bhavcopy, before -> after)")
    for x in fixed:
        lines.append(f"  {x['symbol']} ({x['security_id']}) {x['date']}: minute_scale {mz.fmt(x['old_minute_scale'])} "
                     f"-> {mz.fmt(x['new_minute_scale'])}; {levels_text(x, 'old')} -> {levels_text(x, 'new')}")
        for c in r["cells"]:
            if c["security_id"] == x["security_id"] and c["fixed_row"] == x["date"] and c["changed"] == "Y":
                lines.append(f"    {c['role']:<6} {c['date']} {c['column']:<22} {mz.fmt(c['old'])} -> "
                             f"{mz.fmt(c['new'])}")
    lines.append(f"3. Rows that stay held: {len(held):,}")
    for x in held[:SHOWN]:
        lines.append(f"  {x['symbol']} ({x['security_id']}) {x['date']}: {x['result']}: {x['detail']}; "
                     f"{levels_text(x, 'old')} -> {levels_text(x, 'new')}")
    if len(held) > SHOWN:
        lines.append(f"  ... {len(held) - SHOWN:,} more (held_rows.csv)")
    lines.append(f"4. Cells written: {sum(1 for c in r['cells'] if c['changed'] == 'Y'):,} changed of "
                 f"{len(r['cells']):,} (fixed row: {', '.join(WRITTEN)}; next row: {', '.join(NEXT)})")
    lines.append("  never written: " + ", ".join(NOT_WRITTEN) + " (guard checked per stock before anything is "
                 "written)")
    lines.append(f"5. Minute year files read {r['years_read']:,}, written 0")
    if r["backup"]:
        lines.append(f"Backup: {r['backup']} (rescale.csv lists every written cell)")
    lines.append(f"Report: {r['report_dir']}")
    lines.append(f"Peak memory {r['peak_mb'] / 1e3:.2f} GB, {r['seconds']:,} s")
    return "\n".join(lines) + "\n"


def write_report(r):
    out = r["report_dir"]
    os.makedirs(out, exist_ok=True)
    sm.write_text(os.path.join(out, "report.txt"), r["report"])
    sm.write_csv(pd.DataFrame(r["cells"], columns=CELL_COLUMNS), os.path.join(out, "cells.csv"))
    sm.write_csv(pd.DataFrame(r["fixed"], columns=LEVEL_COLUMNS), os.path.join(out, "levels.csv"))
    sm.write_csv(pd.DataFrame(r["held"], columns=LEVEL_COLUMNS), os.path.join(out, "held_rows.csv"))

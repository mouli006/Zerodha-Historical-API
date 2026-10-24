"""equity_minute.py fix-negative-volume [--apply]: the days.parquet rows of the Kite 1-minute store whose day has
minute bars with a negative volume, fixed (docs/dev/equity_minute_negative_volume.md).

Kite sends a few bars whose volume is an unsigned 64-bit garbage value (2^63, 2^64 - 2000, ...), stored as int64 they
are negative (INT64_MIN, -2000, ...). The rows built before minute_aggregates cleaned the bars (21 Oct 2026) summed
them into minute_volume (BHANDARI 2023-12-13 even wrapped to +2.65e18), so VOLUME_DIFF is wrong there. Per affected
day only these columns change, every other column and row stays as stored:
    minute_volume     the sum with a negative volume counted as 0 (minute_aggregates on the stored bars)
    neg_volume_bars   the number of such bars
    check             the stored labels, VOLUME_DIFF again from the new volume (the row's stored minute_volume_scale),
                      plus NEGATIVE_VOLUME
    note              the stored note plus "negative volume on N minute bar(s), counted as 0"
The stored minute bars are never written. A whole-stock derive with and without the fix is compared: any other
change (minute_volume_scale levels of other days, ...) is listed as drift and not written. Dry run (default) writes
nothing. apply: backs up each changed days.parquet to equity_1min/archive/negvol_<time>/<security_id>/, writes the
new ones as .tmp, replaces them all at the end (any error restores every file: ApplyFailed), writes negvol.csv into
the backup and rebuilds check/. Both take equity_1min/.lock and refuse while a bhavcopy download / input build runs."""
import os
import shutil
import tempfile

import numpy as np
import pandas as pd
import pyarrow.compute as pc
import pyarrow.parquet as pq

import config
from backfill.equity import minute_download as md
from backfill.equity import minute_plan as mp
from backfill.equity import minute_store as ms
from backfill.equity import security_master as sm
from backfill.equity.minute_repair import count_labels, different
from backfill.locks import RunLock

INT64_MIN = int(np.iinfo(np.int64).min)
GARBAGE = -10 ** 12              # a negative volume below this is garbage, above it a small negative
FIXED = ["minute_volume", "neg_volume_bars", "check", "note"]
NEGVOL_COLUMNS = ["security_id", "symbol", "date", "neg_bars", "int64_min", "garbage", "small", "old_minute_volume",
                  "new_minute_volume", "old_check", "new_check"]
PROGRESS_EVERY = 500
SHOWN = 10                       # drift / mismatch lists: at most 10 lines each


def negative_days(path):
    """{date: that day's stored bars} of the days in a stock folder with a bar of negative volume. A year file is
    read whole only when its volume column has a negative value."""
    out = {}
    for _, p in sorted(ms.year_paths(path).items()):
        v = pq.read_table(p, columns=["volume"]).column("volume")
        if not len(v) or pc.min(v).as_py() >= 0:
            continue
        t = pq.read_table(p).to_pandas()
        day = t["ts"].dt.date
        for d in sorted(set(day[t["volume"] < 0])):
            out[d] = t[day == d].reset_index(drop=True)
        del t
    return out


def kinds(volumes):
    """(INT64_MIN, other garbage, small) counts of negative volumes."""
    v = np.asarray([x for x in volumes if x < 0], dtype="int64")
    big = int((v == INT64_MIN).sum())
    garbage = int(((v < GARBAGE) & (v != INT64_MIN)).sum())
    return big, garbage, len(v) - big - garbage


def new_check(row, mv, nb):
    """The check of a stored row with minute volume mv and nb negative bars: VOLUME_DIFF again (the row's stored
    minute_volume_scale), NEGATIVE_VOLUME when nb > 0, every other stored label kept."""
    have = set(str(row["check"]).split("|")) - {"OK", "", "VOLUME_DIFF", md.NEGATIVE}
    kv = float(row["kite_volume"])
    if row["minute_rows"] > 0 and pd.notna(row["kite_close"]):
        vs = mv * float(row["minute_volume_scale"])
        if vs > kv * md.VOLUME_HIGH + 1 or vs < kv * md.VOLUME_LOW:
            have.add("VOLUME_DIFF")
    if nb > 0:
        have.add(md.NEGATIVE)
    return "|".join(c for c in md.CHECKS if c in have) or "OK"


def new_note(note, nb):
    note = "" if note is None or (isinstance(note, float) and np.isnan(note)) else str(note)
    add = md.negative_note(nb)
    if nb <= 0 or add in note:
        return note
    return f"{note}; {add}" if note else add


def same(a, b):
    try:
        return float(a) == float(b) or (np.isnan(float(a)) and np.isnan(float(b)))
    except (TypeError, ValueError):
        return str(a) == str(b)


def _replace(tmp, path):
    os.replace(tmp, path)


def fix(folder=None, apply=False, now=None, log=print):
    """See the module docstring. Returns a dict: counts, rows (one per affected day), drift, preexisting, mismatch,
    before / after (labels of the affected rows), store_before / store_after (labels of the whole store), backup."""
    folder = folder or config.equity_data_dir()
    inp = mp.Inputs(folder, bhav=False)                    # refuses while a bhavcopy download / input build runs
    lock = RunLock(mp.lock_path(folder)).acquire("equity_minute.py fix-negative-volume" + (" --apply" if apply else ""))
    work, tmps, backup, backed = None, [], None, []
    try:
        now = md.as_ist(now)
        base_dir = ms.data_dir(folder)
        sids = sorted(n for n in os.listdir(base_dir) if os.path.isfile(os.path.join(base_dir, n, ms.DAYS_FILE))) \
            if os.path.isdir(base_dir) else []
        mode = "apply" if apply else "dry run, nothing written"
        log(f"Scanning the minute bars of {len(sids):,} built stocks for a negative volume ({mode}) ...")
        found, store_before = {}, {}
        counts = {"bars": 0, "int64_min": 0, "garbage": 0, "small": 0, "days": 0, "stocks": 0}
        for k, sid in enumerate(sids, 1):
            path = ms.stock_dir(folder, sid)
            count_labels(store_before, pq.read_table(os.path.join(path, ms.DAYS_FILE),
                                                     columns=["check"]).column("check").to_pylist())
            neg = negative_days(path)
            if neg:
                found[sid] = neg
                vols = [int(x) for b in neg.values() for x in b["volume"] if x < 0]
                big, garbage, small = kinds(vols)
                counts.update(bars=counts["bars"] + len(vols), int64_min=counts["int64_min"] + big,
                              garbage=counts["garbage"] + garbage, small=counts["small"] + small,
                              days=counts["days"] + len(neg), stocks=counts["stocks"] + 1)
            if k % PROGRESS_EVERY == 0:
                log(f"  {k:,} / {len(sids):,} stocks")
        log(f"Minute bars with a negative volume: {counts['bars']:,} on {counts['days']:,} stock-days in "
            f"{counts['stocks']:,} stocks ({counts['int64_min']:,} INT64_MIN, {counts['garbage']:,} other garbage "
            f"below {GARBAGE:,}, {counts['small']:,} small)")
        result = {"applied": apply, "counts": counts, "rows": [], "drift": [], "preexisting": [], "mismatch": [],
                  "no_row": [], "before": {}, "after": {}, "store_before": store_before, "store_after": {},
                  "backup": None, "changed_stocks": 0}
        if apply:
            calc = md.Calc(folder, inp, log)
        else:
            try:
                calc = md.Calc(folder, inp, write=False)
            except md.CacheStale:
                work = tempfile.mkdtemp(prefix="negvol_")
                log("The bhavcopy cache is older than the store: a temporary one is built for the dry run.")
                calc = md.Calc(folder, inp, write=True, cache_out=os.path.join(work, md.CACHE))
        if apply:
            backup = os.path.join(md.out_dir(folder), "archive", f"negvol_{now:%Y-%m-%d_%H%M%S}")
        for sid in sorted(found):
            path = ms.stock_dir(folder, sid)
            stored = ms.read_days(path)
            symbol = stored["symbol"].iat[-1] if len(stored) else sid
            pos = {d: i for i, d in enumerate(stored["date"])}
            new_in, new = stored[md.INPUT_COLUMNS].copy(), stored.copy()
            whole = pd.api.types.is_integer_dtype(stored["minute_volume"])      # keep the stored dtype
            affected, rows = [], []
            for d, bars in sorted(found[sid].items()):
                if d not in pos:
                    result["no_row"].append((symbol, sid, d))
                    continue
                agg = md.minute_aggregates(bars.assign(run_id="", fetched_at="")).get(d)
                mv, nb = (float(agg["minute_volume"]), int(agg["neg_volume_bars"])) if agg else (0.0, 0)
                i = pos[d]
                row = stored.iloc[i]
                if agg:
                    off = [c for c in ("minute_rows", "minute_high", "minute_low") if not same(row[c], agg[c])]
                    if off:
                        result["mismatch"].append((symbol, sid, d, off))
                chk, note = new_check(row, mv, nb), new_note(row["note"], nb)
                if same(row["minute_volume"], mv) and int(row["neg_volume_bars"]) == nb and chk == row["check"] \
                        and note == row["note"]:
                    continue                                   # fixed already
                for frame in (new_in, new):
                    frame.at[i, "minute_volume"] = int(mv) if whole else mv
                    frame.at[i, "neg_volume_bars"] = nb
                new.at[i, "check"], new.at[i, "note"] = chk, note
                affected.append(d)
                big, garbage, small = kinds(bars["volume"])
                rows.append({"security_id": sid, "symbol": symbol, "date": md.iso(d), "neg_bars": big + garbage + small,
                             "int64_min": big, "garbage": garbage, "small": small,
                             "old_minute_volume": row["minute_volume"], "new_minute_volume": mv,
                             "old_check": row["check"], "new_check": chk})
            if not affected:
                continue
            # the guard: what the fix would change on a fresh derive of the whole stock, beyond the fixed columns
            bhav = calc.bhav_of(sid)
            base = calc.derive(sid, stored[md.INPUT_COLUMNS], bhav=bhav)
            trial = calc.derive(sid, new_in, bhav=bhav)
            keep = ~stored["date"].isin(affected).to_numpy()
            pre = different(stored, base, np.ones(len(stored), bool))[0]
            if pre:
                result["preexisting"].append((symbol, sid, pre))
            cols, when = different(base, trial, keep)
            inner = [c for c in different(base, trial, ~keep)[0] if c not in FIXED]
            if cols or inner:
                result["drift"].append((symbol, sid, sorted(set(cols) | set(inner)), when))
            count_labels(result["before"], stored.loc[~keep, "check"])
            count_labels(result["after"], new.loc[~keep, "check"])
            result["rows"] += rows
            result["changed_stocks"] += 1
            if apply:
                dst = os.path.join(backup, sid, ms.DAYS_FILE)
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                shutil.copy2(os.path.join(path, ms.DAYS_FILE), dst)
                backed.append(sid)
                tmp = os.path.join(path, ms.DAYS_FILE + ".tmp")
                ms.write_days(new, tmp)
                tmps.append((tmp, os.path.join(path, ms.DAYS_FILE)))
            del stored, new_in, new, base, trial
        after = dict(store_before)
        for k, v in result["before"].items():
            after[k] = after.get(k, 0) - v
        for k, v in result["after"].items():
            after[k] = after.get(k, 0) + v
        result["store_after"] = {k: v for k, v in after.items() if v}
        if apply:
            for tmp, p in tmps:
                _replace(tmp, p)
            tmps = []
            if result["rows"]:
                sm.write_csv(pd.DataFrame(result["rows"], columns=NEGVOL_COLUMNS), os.path.join(backup, "negvol.csv"))
                result["backup"] = backup
                prio = md.priorities(inp.membership)
                md.rebuild_checks(folder, {n: prio.get(n, 3) for n in sids})
        result["peak_mb"] = round(md.memory()[1] / 1e6, 1)
        result["report"] = report(result)
        log(result["report"], end="")
        return result
    except BaseException as e:
        if apply and backup:
            for tmp, _ in tmps:
                if os.path.exists(tmp):
                    os.remove(tmp)
            for sid in backed:
                src = os.path.join(backup, sid, ms.DAYS_FILE)
                if os.path.isfile(src):
                    shutil.copy2(src, os.path.join(ms.stock_dir(folder, sid), ms.DAYS_FILE))
            if isinstance(e, Exception):
                raise md.ApplyFailed(f"fix-negative-volume --apply failed ({type(e).__name__}: {e}); every "
                                     f"days.parquet restored from {backup}") from e
        raise
    finally:
        if work:
            shutil.rmtree(work, ignore_errors=True)
        lock.release()


def report(r):
    lines = []
    rows = r["rows"]
    lines.append(f"Rows to fix: {len(rows):,} stock-days in {r['changed_stocks']:,} stocks"
                 + (" (applied)" if r["applied"] else " (dry run, nothing written)"))
    for x in rows:
        kind = ", ".join(f"{n} {name}" for n, name in ((x["int64_min"], "INT64_MIN"), (x["garbage"], "garbage"),
                                                     (x["small"], "small")) if n)
        lines.append(f"  {x['symbol']} ({x['security_id']}) {x['date']}: {x['neg_bars']} bar(s) ({kind}); "
                     f"minute_volume {x['old_minute_volume']:,.0f} -> {x['new_minute_volume']:,.0f}; "
                     f"{x['old_check']} -> {x['new_check']}")
    keys = ["OK"] + md.CHECKS
    lines.append("Checks of these rows, before -> after:")
    for k in keys:
        if r["before"].get(k) or r["after"].get(k):
            lines.append(f"  {k:<18} {r['before'].get(k, 0):>7,} -> {r['after'].get(k, 0):,}")
    lines.append("Checks of the whole store, before -> after:")
    for k in keys:
        if r["store_before"].get(k) or r["store_after"].get(k):
            lines.append(f"  {k:<18} {r['store_before'].get(k, 0):>9,} -> {r['store_after'].get(k, 0):,}")
    if r["drift"]:
        lines.append(f"Fresh-derive drift (not written; the stored values stay): {len(r['drift']):,} stocks")
        for symbol, sid, cols, when in r["drift"][:SHOWN]:
            dates = ", ".join(md.iso(d) for d in when[:5]) + (" ..." if len(when) > 5 else "")
            lines.append(f"  {symbol} ({sid}): {', '.join(cols)}" + (f" on {len(when):,} other rows: {dates}"
                                                                     if when else " on the fixed rows"))
        if len(r["drift"]) > SHOWN:
            lines.append(f"  ... {len(r['drift']) - SHOWN:,} more")
    else:
        lines.append("Fresh-derive drift: none (no other row or column would change)")
    if r["preexisting"]:
        cols = {}
        for _, _, cs in r["preexisting"]:
            for c in cs:
                cols[c] = cols.get(c, 0) + 1
        lines.append(f"Pre-existing re-derive differences (not fixed; those values stay as stored): "
                     f"{len(r['preexisting']):,} stocks; columns: "
                     + ", ".join(f"{c} ({n})" for c, n in sorted(cols.items())))
    if r["mismatch"]:
        lines.append(f"Stored minute aggregates other than the volume differ from the bars: {len(r['mismatch']):,} "
                     "days (not changed)")
        for symbol, sid, d, off in r["mismatch"][:SHOWN]:
            lines.append(f"  {symbol} ({sid}) {md.iso(d)}: {', '.join(off)}")
    if r["no_row"]:
        lines.append(f"Days with negative bars but no days.parquet row (nothing to fix): {len(r['no_row']):,}")
    if r["backup"]:
        lines.append(f"Backup: {r['backup']} (negvol.csv lists every row)")
    lines.append(f"Peak memory {r['peak_mb'] / 1e3:.2f} GB")
    return "\n".join(lines) + "\n"

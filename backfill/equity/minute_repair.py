"""equity_minute.py repair: Kite day candles stored before they were final, fetched again (the 1-minute bars are not
touched). docs/dev/equity_minute_finished_days.md has the rules and the evidence (22-23 Oct 2026).

A day candle fetched before (date + 1) 01:00 IST is provisional (close = last minute close, volume = summed minute
volume), and around midnight Kite returned none (DAY_EMPTY rows). find_affected() finds those rows from the stored
days.parquet files and the progress db only; repair() fetches each stock's day candles over its affected dates once,
replaces only the day inputs (kite_*, fetched_at, run_id) of those dates and derives each repaired row as update stores
a new day (repaired_rows); only those rows are spliced into the stored file (splice). A stock where the repair would
change any other row (two fresh derives, without and with the new candles) is listed, never written.
Dry run (default) writes nothing; --apply backs up every changed days.parquet to
equity_1min/archive/repair_<time>/<security_id>/ and replaces them all at the end (any error restores them). Both
take equity_1min/.lock and refuse while a Kite F&O run or an input build runs (they make Kite requests)."""
import os
import secrets
import shutil
import tempfile

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from kiteconnect import exceptions as kex

import config
from backfill.equity import minute_download as md
from backfill.equity import minute_plan as mp
from backfill.equity import minute_store as ms
from backfill.equity import security_master as sm
from backfill.fetch import AuthStop, ChunkFailed, fetch_chunk, make_chunks
from backfill.locks import RunLock

PROVISIONAL, DAY_EMPTY = "PROVISIONAL", "DAY_EMPTY"
RATIO_BUCKETS = [(0.001, "<=0.1%"), (0.005, "0.1-0.5%"), (0.02, "0.5-2%"), (np.inf, ">2%")]
DERIVE_SECONDS = 0.25            # estimate: reading, deriving and comparing one stock (besides its request)
PROGRESS_EVERY = 100
LIST_LINES = 200                 # at most 200 skipped dates are printed (every skipped stock is)
REPAIR_COLUMNS = ["security_id", "symbol", "date", "kind", "result", "note", "old_check", "new_check", "old_ratio",
                  "new_ratio", "old_fetched_at"]


def parse_stamp(text):
    """A stored fetched_at ("YYYY-MM-DD HH:MM:SS", IST), or None."""
    try:
        return pd.Timestamp(str(text)).to_pydatetime().replace(tzinfo=mp.IST) if text else None
    except ValueError:
        return None


def day_fetch_times(folder):
    """{(security_id, run_id): fetched_at} of the progress db's day_fetches (a build's day candles of each run)."""
    if not os.path.isfile(os.path.join(md.out_dir(folder), "download_progress.sqlite")):
        return {}
    db = md.Progress(folder, write=False)
    try:
        return {(r["security_id"], r["run_id"]): r["fetched_at"]
                for r in db.rows("SELECT security_id, run_id, fetched_at FROM day_fetches")}
    finally:
        db.close()


def find_affected(folder):
    """(affected, counts). affected: {security_id: {date: kind}}, from the stored data only:
    PROVISIONAL: a day candle fetched before (date + 1) 01:00 IST (the earlier of the row's fetched_at and its run's
    day_fetches time: a build's row carries the minute chunk's time);
    DAY_EMPTY: a DAY_EMPTY row of an update run fetched before (date + 1) 01:00 IST (the midnight window).
    counts: provisional, day_empty, date_rule (the provisional rows with the fetch date on or before the row date),
    stocks."""
    _, _, runs = md.read_progress(folder)
    update_runs = {r["run_id"] for r in runs if r["command"] == "update"}
    fetches = day_fetch_times(folder)
    base = ms.data_dir(folder)
    sids = sorted(n for n in os.listdir(base) if os.path.isfile(os.path.join(base, n, ms.DAYS_FILE))) \
        if os.path.isdir(base) else []
    affected = {}
    counts = {"provisional": 0, "day_empty": 0, "date_rule": 0}
    for sid in sids:
        t = pq.read_table(os.path.join(base, sid, ms.DAYS_FILE),
                          columns=["date", "kite_close", "check", "fetched_at", "run_id"]).to_pandas()
        for d, kc, chk, fetched, run in zip(t["date"], t["kite_close"], t["check"], t["fetched_at"], t["run_id"]):
            at = parse_stamp(fetched)
            if at is None:
                continue
            d = sm.as_date(d)
            if not pd.isna(kc):
                day_at = parse_stamp(fetches.get((sid, run)))
                at = min(at, day_at) if day_at else at
                if at < md.finished_at(d):
                    affected.setdefault(sid, {})[d] = PROVISIONAL
                    counts["provisional"] += 1
                    counts["date_rule"] += at.date() <= d
            elif "DAY_EMPTY" in chk and run in update_runs and at < md.finished_at(d):
                affected.setdefault(sid, {})[d] = DAY_EMPTY
                counts["day_empty"] += 1
    counts["stocks"] = len(affected)
    return affected, counts


def bucket(old, new):
    if pd.isna(old) or pd.isna(new) or not old:
        return "no ratio before" if pd.isna(old) or not old else "no ratio after"
    x = abs(new / old - 1)
    return next(name for top, name in RATIO_BUCKETS if x <= top)


def different(a, b, keep):
    """(columns, dates): the DAY_COLUMNS (other than date) where the rows `keep` of a and b differ, and the dates of
    those rows (NaN equal to NaN, as md.same_column). ([], []) = the same."""
    a, b = a[keep].reset_index(drop=True), b[keep].reset_index(drop=True)
    cols, bad = [], np.zeros(len(a), bool)
    for c in md.DAY_COLUMNS:
        if c == "date":
            continue
        try:
            x, y = a[c].to_numpy(float), b[c].to_numpy(float)
            eq = (x == y) | (np.isnan(x) & np.isnan(y))
        except (TypeError, ValueError):
            eq = a[c].astype(str).to_numpy() == b[c].astype(str).to_numpy()
        if not eq.all():
            cols.append(c)
            bad |= ~eq
    return cols, list(a.loc[bad, "date"])


def repaired_rows(calc, sid, inputs, dates, bhav):
    """The new days.parquet rows of the repaired dates, each derived as update stores a new day: from the inputs up
    to that date only (the stored days as neighbours, nothing later yet), with the new candles in."""
    rows = []
    for d in sorted(dates):
        der = calc.derive(sid, inputs[inputs["date"] <= d], bhav=bhav)
        rows.append(der[der["date"] == d])
    return pd.concat(rows, ignore_index=True)


def splice(stored, rows):
    """stored with only the rows of rows' dates replaced (every other row as stored), in the stored dtypes where
    the values fit."""
    out = pd.concat([stored[~stored["date"].isin(set(rows["date"]))], rows[stored.columns]], ignore_index=True)
    out = out.sort_values("date", kind="stable").reset_index(drop=True)
    for c in out.columns:
        if out[c].dtype != stored[c].dtype and not out[c].isna().any():
            try:
                out[c] = out[c].astype(stored[c].dtype)
            except (TypeError, ValueError):
                pass
    return out


def count_labels(counts, labels):
    for chk in labels:
        for x in chk.split("|"):
            counts[x] = counts.get(x, 0) + 1


def _replace(tmp, path):
    os.replace(tmp, path)


def repair(folder=None, kite=None, apply=False, limit=None, speed=md.SPEED, now=None, log=print):
    """Fetch again the day candles of the rows find_affected() selects and re-derive their stocks (see the module
    docstring). Dry run (default): writes nothing, but makes the Kite requests (the counts after need the new
    candles). apply: writes the changed days.parquet files (backup first, all replaced at the end, any error restores
    them; ApplyFailed), then rebuilds check/. Both hold equity_1min/.lock."""
    folder = folder or config.equity_data_dir()
    inp = mp.Inputs(folder, bhav=False)                    # refuses while a bhavcopy download / input build runs
    mp.check_fno(config.load_env())
    lock = RunLock(mp.lock_path(folder)).acquire("equity_minute.py repair" + (" --apply" if apply else ""))
    work, db, run_id = None, None, None
    tmps, backup = [], None
    try:
        now = md.as_ist(now)
        affected, counts = find_affected(folder)
        mode = "apply" if apply else "dry run, nothing written"
        log(f"Rows found ({mode}): {counts['provisional']:,} provisional (day candle fetched before the next day "
            f"01:00 IST; the old date rule finds {counts['date_rule']:,}, the 01:00 rule adds "
            f"{counts['provisional'] - counts['date_rule']:,}), {counts['day_empty']:,} DAY_EMPTY of an update run "
            f"in the midnight window; {counts['stocks']:,} stocks")
        sids = sorted(affected)
        if limit:
            sids = sids[:limit]
        result = {"applied": apply, "counts": counts, "stocks": len(sids), "repaired": [], "skipped": [],
                  "skipped_dates": [], "refused": [], "preexisting": [], "before": {}, "after": {}, "ratio": {},
                  "backup": None, "requests": 0}
        if not sids:
            log("Nothing to repair.")
            result["report"] = "Nothing to repair."
            return result
        secs = (1 + len(sids)) * max(1 / speed, md.DAY_SECONDS) + len(sids) * DERIVE_SECONDS
        log(f"Requests needed: 1 instrument list + {len(sids):,} day requests"
            f"{f' (--limit {limit})' if limit else ''}, about {secs / 60:.0f} minutes at {speed:g} req/s")
        if apply:
            md.mark_aborted(folder, log)
            calc = md.Calc(folder, inp, log)
        else:
            try:
                calc = md.Calc(folder, inp, write=False)
            except md.CacheStale:
                work = tempfile.mkdtemp(prefix="repair_")
                log("The bhavcopy cache is older than the store: a temporary one is built for the dry run.")
                calc = md.Calc(folder, inp, write=True, cache_out=os.path.join(work, md.CACHE))
        _, stocks, _ = md.read_progress(folder)
        kite = kite or mp.kite_client()
        try:
            instruments = kite.instruments("NSE")
        except kex.TokenException as e:
            raise mp.SignInNeeded(f"Kite session expired or invalid ({type(e).__name__}). Sign in (python app.py) "
                                  "and run the same command again.")
        matched = {m["security_id"]: m for m in mp.match_tokens([inp.by_id[s] for s in sids if s in inp.by_id],
                                                                instruments)}
        limiter = md.SharedLimiter(speed)
        run_id = f"{now:%Y%m%d-%H%M%S}-{secrets.token_hex(2)}"
        if apply:
            db = md.Progress(folder)
            db.start_run(run_id, "repair")
            backup = os.path.join(md.out_dir(folder), "archive", f"repair_{now:%Y-%m-%d_%H%M%S}")
        rows = []
        for k, sid in enumerate(sids, 1):
            if k % PROGRESS_EVERY == 0:
                log(f"  {k:,} / {len(sids):,} stocks, {limiter.requests:,} requests")
            u = inp.by_id.get(sid) or {}
            symbol = u.get("symbol", sid)
            dates = sorted(affected[sid])
            st = stocks.get(sid) or {}
            m = matched.get(sid)
            why = None
            if st.get("status") != "DONE":
                why = f"not DONE ({st.get('status') or 'no progress row'})"
            elif m is None or m["match_status"] not in mp.MATCHED or not m["instrument_token"]:
                why = ("not ACTIVE: not matched" if m is None else
                       f"{m['match_status']} in Kite's list ({m['note']}): left to update / retoken")
            elif int(m["instrument_token"]) != int(st["token"]):
                why = f"token {st['token']} -> {m['instrument_token']}: left to update / retoken"
            if why:
                result["skipped"].append((symbol, sid, why))
                continue
            token = int(st["token"])
            try:
                candles = []
                for s, e in make_chunks(dates[0], dates[-1], "day"):
                    candles += fetch_chunk(kite, limiter, token, s, e, "day")
            except AuthStop as e:
                raise mp.SignInNeeded(f"Kite session expired or invalid ({e}). Sign in (python app.py) and run the "
                                      "same command again.")
            except ChunkFailed as e:
                result["skipped"].append((symbol, sid, f"day candles failed: {e}"))
                continue
            fetched = md.stamp()
            dayf = mp.day_frame(candles).set_index("date")
            path = ms.stock_dir(folder, sid)
            stored = ms.read_days(path)
            inputs = stored[md.INPUT_COLUMNS].copy()
            for f in md.FIELDS:                                    # a candle may be missing: NaN needs float
                inputs[f"kite_{f}"] = inputs[f"kite_{f}"].astype("float64")
            pos = {d: i for i, d in enumerate(inputs["date"])}
            trial = inputs.copy()
            for d in dates:
                for f in md.FIELDS:
                    trial.at[pos[d], f"kite_{f}"] = dayf.at[d, f] if d in dayf.index else np.nan
            bhav = calc.bhav_of(sid)
            bad = md.unfinished(trial.iloc[[pos[d] for d in dates]], bhav)
            for d in sorted(bad):
                result["skipped_dates"].append((symbol, sid, d, f"still not final: {bad[d]}"))
            good = [d for d in dates if d not in bad]
            if not good:
                continue
            new_in = inputs.copy()
            for d in good:
                i = pos[d]
                for f in md.FIELDS:
                    new_in.at[i, f"kite_{f}"] = trial.at[i, f"kite_{f}"]
                new_in.at[i, "fetched_at"] = fetched
                new_in.at[i, "run_id"] = run_id
            keep = ~stored["date"].isin(good).to_numpy()
            # the guard: the repair may change no other row (older or later), compared on two fresh derives
            base = calc.derive(sid, inputs, bhav=bhav)
            after = calc.derive(sid, new_in, bhav=bhav)
            pre = different(stored, base, keep)[0]
            if pre:                                            # stored rows a fresh derive no longer gives: kept
                result["preexisting"].append((symbol, sid, pre))
            cols, when = different(base, after, keep)
            if cols:
                result["refused"].append((symbol, sid, f"rows not repaired would change ({', '.join(cols)}) on "
                                                       f"{', '.join(md.iso(d) for d in when[:5])}"
                                                       f"{' ...' if len(when) > 5 else ''}"))
                continue
            new = splice(stored, repaired_rows(calc, sid, new_in, good, bhav))
            sel = ~keep
            old_r, new_r = stored.loc[sel], new.loc[sel]
            count_labels(result["before"], old_r["check"])
            count_labels(result["after"], new_r["check"])
            for (_, a), (_, b) in zip(old_r.iterrows(), new_r.iterrows()):
                name = bucket(a["ratio"], b["ratio"])
                result["ratio"][name] = result["ratio"].get(name, 0) + 1
                rows.append({"security_id": sid, "symbol": symbol, "date": md.iso(a["date"]),
                             "kind": affected[sid][a["date"]], "result": "REPAIRED", "note": "",
                             "old_check": a["check"], "new_check": b["check"], "old_ratio": a["ratio"],
                             "new_ratio": b["ratio"], "old_fetched_at": a["fetched_at"]})
            result["repaired"].append((symbol, sid, len(good)))
            if apply:
                dst = os.path.join(backup, sid, ms.DAYS_FILE)
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                shutil.copy2(os.path.join(path, ms.DAYS_FILE), dst)
                tmp = os.path.join(path, ms.DAYS_FILE + ".tmp")
                ms.write_days(new, tmp)
                tmps.append((tmp, os.path.join(path, ms.DAYS_FILE)))
            del stored, inputs, trial, new_in, new, base, after
        result["requests"] = limiter.requests + 1
        if apply:
            for tmp, p in tmps:
                _replace(tmp, p)
            tmps = []
            for symbol, sid, why in result["skipped"]:
                rows += [{"security_id": sid, "symbol": symbol, "date": md.iso(d), "kind": affected[sid][d],
                          "result": "SKIPPED", "note": why} for d in sorted(affected[sid])]
            for symbol, sid, d, why in result["skipped_dates"]:
                rows.append({"security_id": sid, "symbol": symbol, "date": md.iso(d), "kind": affected[sid][d],
                             "result": "SKIPPED", "note": why})
            for symbol, sid, why in result["refused"]:
                rows += [{"security_id": sid, "symbol": symbol, "date": md.iso(d), "kind": affected[sid][d],
                          "result": "REFUSED", "note": why} for d in sorted(affected[sid])]
            os.makedirs(backup, exist_ok=True)
            sm.write_csv(pd.DataFrame(rows, columns=REPAIR_COLUMNS), os.path.join(backup, "repair.csv"))
            result["backup"] = backup
            db.end_run(run_id, limiter.requests + 1, limiter.n429, "done", round(md.memory()[1] / 1e6, 1))
            prio = md.priorities(inp.membership)
            base = ms.data_dir(folder)
            md.rebuild_checks(folder, {n: prio.get(n, 3) for n in os.listdir(base)})
        result["report"] = report(result, apply)
        log(result["report"], end="")
        return result
    except BaseException as e:
        if apply and backup:
            for tmp, _ in tmps:
                if os.path.exists(tmp):
                    os.remove(tmp)
            for symbol, sid, _ in result["repaired"]:
                src = os.path.join(backup, sid, ms.DAYS_FILE)
                if os.path.isfile(src):
                    shutil.copy2(src, os.path.join(ms.stock_dir(folder, sid), ms.DAYS_FILE))
            if db:
                db.end_run(run_id, 0, 0, "failed")
            if isinstance(e, Exception) and not isinstance(e, mp.SignInNeeded):
                raise md.ApplyFailed(f"repair --apply failed ({type(e).__name__}: {e}); every days.parquet restored "
                                     f"from {backup}") from e
        raise
    finally:
        if db:
            db.close()
        if work:
            shutil.rmtree(work, ignore_errors=True)
        lock.release()


def report(r, apply):
    rows = sum(n for _, _, n in r["repaired"])
    lines = [f"Repaired{'' if apply else ' (dry run: would repair)'}: {rows:,} rows in {len(r['repaired']):,} stocks; "
             f"{r['requests']:,} Kite requests",
             "Checks of the repaired rows, before -> after:"]
    for k in ["OK"] + md.CHECKS:
        if r["before"].get(k) or r["after"].get(k):
            lines.append(f"  {k:<18} {r['before'].get(k, 0):>7,} -> {r['after'].get(k, 0):,}")
    names = [n for _, n in RATIO_BUCKETS] + [n for n in ("no ratio before", "no ratio after") if r["ratio"].get(n)]
    lines.append("Ratio change (bhavcopy close / Kite close, new vs old): " +
                 ", ".join(f"{n} {r['ratio'].get(n, 0):,}" for n in names))
    if r["skipped"]:
        lines.append(f"Skipped stocks: {len(r['skipped']):,}")
        lines += [f"  {s} ({sid}): {why}" for s, sid, why in r["skipped"]]
    if r["skipped_dates"]:
        lines.append(f"Skipped dates (the new candle is not final either): {len(r['skipped_dates']):,}")
        lines += [f"  {s} ({sid}) {md.iso(d)}: {why}" for s, sid, d, why in r["skipped_dates"][:LIST_LINES]]
        if len(r["skipped_dates"]) > LIST_LINES:
            lines.append(f"  ... {len(r['skipped_dates']) - LIST_LINES:,} more")
    if r["refused"]:
        lines.append(f"Not written, the repair would change rows not repaired: {len(r['refused']):,}")
        lines += [f"  {s} ({sid}): {why}" for s, sid, why in r["refused"]]
    if r["preexisting"]:
        cols = {}
        for _, _, cs in r["preexisting"]:
            for c in cs:
                cols[c] = cols.get(c, 0) + 1
        lines.append(f"Pre-existing re-derive differences (not fixed; those rows stay as stored): "
                     f"{len(r['preexisting']):,} stocks; columns: " +
                     ", ".join(f"{c} ({n:,})" for c, n in sorted(cols.items(), key=lambda x: -x[1])))
    if apply and r["backup"]:
        lines.append(f"Backup of every changed days.parquet and repair.csv: {r['backup']}")
    return "\n".join(lines) + "\n"

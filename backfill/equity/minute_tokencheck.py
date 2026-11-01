"""Kite token changes of the 1-minute store, checked before and after the rebuild (P43, P93).
docs/dev/equity_minute_tokencheck.md.

Before a full download on a new Kite token (a DONE stock whose token changed, or a new listing), the new token's day
candles on the stock's latest bhavcopy days must match the bhavcopy (check_token). After the download, before the old
files are replaced, every minute bar both histories have must load to the same close (compare_builds). A refused
token change leaves the stored files untouched and is listed for review (token_review.csv), decided by
`equity_minute.py token-review`. `token-replay` runs the pre-check on archived token changes, writing nothing.

Files (all under <folder>/equity_1min/tokencheck/):
    token_review.csv        one row per review (the active one per stock: PENDING / ACCEPTED / REJECTED)
    checks.csv              every check made (pre-check and after-download), for the tokens.csv note
    review.log              what token-review --apply did
    <run_id>/<sid>_*.csv    the evidence of one check; <run_id>/<sid>_new/ a parked download
"""
import os
import shutil
import threading
from datetime import datetime

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from backfill.equity import adjustments as adj
from backfill.equity import minute_plan as mp
from backfill.equity import minute_store as ms
from backfill.equity import security_master as sm
from backfill.fetch import ChunkFailed, RateLimiter, fetch_chunk, make_chunks
from backfill.locks import RunLock

REF_DAYS = 5             # the latest bhavcopy days of the stock compared with the new token's day candles ...
REF_WINDOW = 60          # ... within the last 60 trading days up to the target day
REL_TOL = 0.02           # close (and high / low) within max(2%, 2 ticks) of the bhavcopy
TICK = 0.05
TICKS = 2
BAR_TOL = 0.005          # after the download: a common minute bar whose loaded close moves more than 0.5% ...
MAX_EXEMPT = 5           # ... refuses, except up to 5 days where the new history matches the bhavcopy better
DAY_REL, DAY_HALF_TICK = 0.0005, 0.025     # "matches better": the bhav close inside the loaded day range (dayempty rule)

PASS, FAIL, NO_REFERENCE, NO_CANDLES = "PASS", "FAIL", "NO_REFERENCE", "NO_CANDLES"
AFTER_DOWNLOAD = "AFTER_DOWNLOAD"
TOKEN_CHANGE, NEW_LISTING = "TOKEN_CHANGE", "NEW_LISTING"
PENDING, ACCEPTED, REJECTED = "PENDING", "ACCEPTED", "REJECTED"
CLEARED, BUILT, PASSED = "CLEARED", "BUILT", "PASSED"           # closed rows (kept as history)
ACTIVE = (PENDING, ACCEPTED, REJECTED)

REVIEW_COLUMNS = ["security_id", "symbol", "reason", "old_token", "new_token", "result", "max_close_diff",
                  "max_hl_diff", "first_seen", "last_checked", "report", "parked", "state", "decided_at", "note"]
CHECK_COLUMNS = ["at", "run_id", "security_id", "symbol", "kind", "reason", "old_token", "new_token", "result",
                 "days", "max_close_diff", "max_hl_diff", "basis", "report"]
EVIDENCE_COLUMNS = ["date", "basis", "k", "bhav_open", "bhav_high", "bhav_low", "bhav_close", "kite_open", "kite_high",
                    "kite_low", "kite_close", "close_diff", "hl_diff", "tol", "ok"]
COMPARE_COLUMNS = ["date", "bars", "bad_bars", "max_diff", "old_close", "new_close", "old_low", "old_high", "new_low",
                   "new_high", "bhav_close", "exempt", "why"]
CONV = ["date", "kind", "ratio", "kite_multiplier", "kite_difference", "vol_ratio", "minute_scale",
        "minute_volume_scale"]

LOCK = threading.Lock()       # the review / check files are written by both download workers


class TokenCheckError(mp.MinutePlanError):
    pass


def now_ist():
    return datetime.now(mp.IST)


def stamp():
    return f"{now_ist():%Y-%m-%d %H:%M:%S}"


def out_dir(folder):
    return os.path.join(mp.out_dir(folder), "tokencheck")


def review_path(folder):
    return os.path.join(out_dir(folder), "token_review.csv")


def checks_path(folder):
    return os.path.join(out_dir(folder), "checks.csv")


def pct(x):
    return "" if x is None or (isinstance(x, float) and np.isnan(x)) else f"{x:.2%}"


# ---- the pre-check ----

def reference_days(bhav, days, end, n=REF_DAYS, window=REF_WINDOW):
    """The stock's latest n bhavcopy rows (date, open, high, low, close) on or before end, within the last `window`
    trading days (`days`: every trading day, sorted) up to end. An empty frame when there is none."""
    cols = ["date", "open", "high", "low", "close"]
    if bhav is None or not len(bhav):
        return pd.DataFrame(columns=cols)
    upto = [d for d in days if d <= end]
    if not upto:
        return pd.DataFrame(columns=cols)
    first = upto[-window:][0]
    b = bhav[cols].copy()
    b["date"] = [sm.as_date(d) for d in b["date"]]
    b = b[(b["date"] >= first) & (b["date"] <= end) & (b["close"] > 0)]
    return b.sort_values("date").drop_duplicates("date", keep="first").tail(n).reset_index(drop=True)


def judge(refs, kite, factor=None, today=None):
    """The verdict of a token's Kite day candles (`kite`: date + OHLC) on the reference days. factor(d): our cumulative
    factor of a day (a split / bonus between a reference day and today: Kite's old candles may be back-adjusted, so
    the bhavcopy is compared raw and divided by factor(ref) / factor(today), and the closer one is kept).
    Returns {result, days, max_close_diff, max_hl_diff, basis, rows}."""
    out = {"result": NO_REFERENCE, "days": 0, "max_close_diff": None, "max_hl_diff": None, "basis": "", "rows": []}
    if not len(refs):
        return out
    k = kite[["date", "open", "high", "low", "close"]].copy() if len(kite) else pd.DataFrame(
        columns=["date", "open", "high", "low", "close"])
    k["date"] = [sm.as_date(d) for d in k["date"]]
    j = refs.merge(k, on="date", suffixes=("_b", "_k"))
    if not len(j):
        return {**out, "result": NO_CANDLES}
    bases = [("raw", [1.0] * len(j))]
    if factor is not None and today is not None:
        ft = factor(today)
        ks = [factor(d) / ft if ft else 1.0 for d in j["date"]]
        if any(abs(x - 1.0) > 1e-12 for x in ks):
            bases.append(("adjusted", ks))
    best = None
    for basis, ks in bases:
        rows = []
        for r, kk in zip(j.itertuples(), ks):
            exp = {f: getattr(r, f"{f}_b") / kk for f in ("open", "high", "low", "close")}
            cd = abs(r.close_k / exp["close"] - 1)
            hd = max(abs(r.high_k / exp["high"] - 1), abs(r.low_k / exp["low"] - 1))
            tol = max(REL_TOL, TICKS * TICK / exp["close"])
            rows.append({"date": r.date, "basis": basis, "k": kk, **{f"bhav_{f}": v for f, v in exp.items()},
                         "kite_open": r.open_k, "kite_high": r.high_k, "kite_low": r.low_k, "kite_close": r.close_k,
                         "close_diff": cd, "hl_diff": hd, "tol": tol, "ok": cd <= tol and hd <= tol})
        mc = max(x["close_diff"] for x in rows)
        if best is None or mc < best[0]:
            best = (mc, basis, rows)
    mc, basis, rows = best
    return {"result": PASS if all(x["ok"] for x in rows) else FAIL, "days": len(rows), "max_close_diff": mc,
            "max_hl_diff": max(x["hl_diff"] for x in rows), "basis": basis, "rows": rows}


def check_token(fetch_days, token, refs, factor=None, today=None):
    """The pre-check of one token: one day-candle request (fetch_days(token, first, last) -> mp.day_frame) over the
    reference days. A failed request is NO_CANDLES (the text in "error"); AuthStop / StopJob are raised."""
    if not len(refs):
        return judge(refs, pd.DataFrame())
    try:
        kite = fetch_days(token, refs["date"].iloc[0], refs["date"].iloc[-1])
    except ChunkFailed as e:
        return {**judge(refs, pd.DataFrame()), "result": NO_CANDLES, "error": str(e)}
    return judge(refs, kite, factor, today)


def factor_fn(folder, sid):
    """factor(d) for judge: adjustments.factor_on, 1.0 for a stock without factors."""
    def f(d):
        try:
            return float(adj.factor_on(sid, d, folder))
        except Exception:
            return 1.0
    return f


def write_evidence(folder, run_id, sid, kind, rows, columns):
    path = os.path.join(out_dir(folder), run_id, f"{sid}_{kind}.csv")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    sm.write_csv(pd.DataFrame(rows, columns=columns), path)
    return path


# ---- the after-download comparison ----

def _loaded(m, days):
    """Loaded (raw) close of minute bars m (ts, OHLCV) with their day's conversion."""
    m = m.copy()
    m["ts"] = pd.to_datetime(m["ts"]).dt.tz_convert(ms.TZ)
    m["date"] = m["ts"].dt.date
    m = m.merge(days[CONV], on="date", how="inner")
    m["loaded"] = ms.convert(m)["close"]
    return m[["ts", "date", "loaded"]]


def _day_loaded(days):
    """{date: (low, high, close)} of a days.parquet frame, loaded with the day's own conversion."""
    m = days.rename(columns={"minute_open": "open", "minute_high": "high", "minute_low": "low",
                             "minute_close": "close", "minute_volume": "volume"})
    c = ms.convert(m)
    return {d: (lo, hi, cl) for d, lo, hi, cl in zip(days["date"], c["low"], c["high"], c["close"])}


def better(bc, old, new):
    """The new day matches the bhavcopy close better than the old one: inside the new loaded low..high and not the old
    one's, or a smaller distance from the loaded close. A day without a bhavcopy close is never better."""
    if bc is None or pd.isna(bc) or old is None or new is None:
        return False, "no bhavcopy close"
    tol = DAY_REL * bc + DAY_HALF_TICK
    inside_new = new[0] - tol <= bc <= new[1] + tol
    inside_old = old[0] - tol <= bc <= old[1] + tol
    if inside_new and not inside_old:
        return True, "bhav close inside the new day range only"
    if abs(new[2] - bc) < abs(old[2] - bc):
        return True, "new loaded close nearer the bhav close"
    return False, "old row matches the bhavcopy as well or better"


def compare_builds(old_dir, new_dir, max_exempt=MAX_EXEMPT, tol=BAR_TOL):
    """Compare a rebuilt stock (new_dir) with its stored files (old_dir), one year at a time: every minute bar in
    both (same ts) is loaded with its own side's day conversion (clean_bars first, as load_minute). A bar whose loaded
    close moves more than tol makes its day bad; a bad day is exempt when the new day matches the bhavcopy better
    (better()). ok: no bad day left, at most max_exempt exempt days. Bars only on one side and the day-row counts are
    reported, never refused."""
    od, nd = ms.read_days(old_dir), ms.read_days(new_dir)
    oy, ny = ms.year_paths(old_dir), ms.year_paths(new_dir)
    common = only_old = only_new = 0
    max_diff = 0.0
    bad = {}
    for y in sorted(set(oy) | set(ny)):
        o = ms.clean_bars(ms.read_minutes(old_dir, {y}))[0] if y in oy else None
        n = ms.clean_bars(ms.read_minutes(new_dir, {y}))[0] if y in ny else None
        if o is None or n is None:
            only_old += len(o) if o is not None else 0
            only_new += len(n) if n is not None else 0
            continue
        lo, ln = _loaded(o, od), _loaded(n, nd)
        j = lo.merge(ln, on="ts", suffixes=("_o", "_n"))
        common += len(j)
        only_old += len(o) - len(j)
        only_new += len(n) - len(j)
        if len(j):
            diff = (j["loaded_n"] / j["loaded_o"] - 1).abs().to_numpy(float)
            diff = np.where(np.isfinite(diff), diff, 0.0)
            max_diff = max(max_diff, float(diff.max()))
            j["diff"] = diff
            for d, g in j[j["diff"] > tol].groupby("date_o"):
                bad[d] = {"bars": int((j["date_o"] == d).sum()), "bad_bars": len(g), "max_diff": float(g["diff"].max())}
        del o, n, lo, ln, j
    old_day, new_day = _day_loaded(od), _day_loaded(nd)
    bhav = dict(zip(nd["date"], nd["bhav_close"]))
    rows, exempt = [], []
    for d in sorted(bad):
        ok, why = better(bhav.get(d), old_day.get(d), new_day.get(d))
        o, n = old_day.get(d, (np.nan,) * 3), new_day.get(d, (np.nan,) * 3)
        rows.append({"date": d, **bad[d], "old_close": o[2], "new_close": n[2], "old_low": o[0], "old_high": o[1],
                     "new_low": n[0], "new_high": n[1], "bhav_close": bhav.get(d), "exempt": ok, "why": why})
        if ok:
            exempt.append(d)
    refused = [r["date"] for r in rows if not r["exempt"]]
    ok = not refused and len(exempt) <= max_exempt
    reason = "" if ok else (f"{len(refused)} day(s) moved more than {tol:.1%}" if refused else
                            f"{len(exempt)} exempt days, more than {max_exempt}")
    return {"ok": ok, "common_bars": common, "only_old": only_old, "only_new": only_new, "max_diff": max_diff,
            "bad_days": len(rows), "exempt": exempt, "refused": refused, "rows": rows, "reason": reason,
            "old_days": len(od), "new_days": len(nd)}


def park(folder, run_id, sid, building):
    """Move a refused download (data/<sid>.building) to tokencheck/<run_id>/<sid>_new/ (never deleted)."""
    dst = os.path.join(out_dir(folder), run_id, f"{sid}_new")
    n = 1
    while os.path.exists(dst):
        n += 1
        dst = os.path.join(out_dir(folder), run_id, f"{sid}_new_{n}")
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    shutil.move(building, dst)
    return dst


# ---- the review and check files ----

def read_review(folder):
    p = review_path(folder)
    if not os.path.isfile(p):
        return pd.DataFrame(columns=REVIEW_COLUMNS)
    df = pd.read_csv(p, dtype=str, keep_default_na=False)
    for c in REVIEW_COLUMNS:
        if c not in df:
            df[c] = ""
    return df[REVIEW_COLUMNS]


def _write_review(folder, df):
    os.makedirs(out_dir(folder), exist_ok=True)
    sm.write_csv(df[REVIEW_COLUMNS], review_path(folder))


def active(folder, sid=None):
    """The active review row of a stock (dict), or None; without sid: every active row (DataFrame)."""
    df = read_review(folder)
    df = df[df["state"].isin(ACTIVE)]
    if sid is None:
        return df.reset_index(drop=True)
    df = df[df["security_id"] == sid]
    return df.iloc[-1].to_dict() if len(df) else None


def set_active(folder, row):
    """Write `row` as the stock's active review row (an older active row of the stock is replaced; first_seen kept
    while it is about the same token)."""
    with LOCK:
        df = read_review(folder)
        mask = (df["security_id"] == row["security_id"]) & df["state"].isin(ACTIVE)
        prev = df[mask]
        row = {c: "" for c in REVIEW_COLUMNS} | {k: ("" if v is None else str(v)) for k, v in row.items()}
        if len(prev) and prev.iloc[-1]["new_token"] == row["new_token"] and prev.iloc[-1]["first_seen"]:
            row["first_seen"] = prev.iloc[-1]["first_seen"]
        row["first_seen"] = row["first_seen"] or stamp()
        df = pd.concat([df[~mask], pd.DataFrame([row], columns=REVIEW_COLUMNS)], ignore_index=True)
        _write_review(folder, df)
    return row


def resolve(folder, sid, state, note=""):
    """Close the stock's active review row (state CLEARED / BUILT / PASSED) or move it to ACCEPTED / REJECTED.
    Returns the row before the change, or None when there was none."""
    with LOCK:
        df = read_review(folder)
        idx = df.index[(df["security_id"] == sid) & df["state"].isin(ACTIVE)]
        if not len(idx):
            return None
        i = idx[-1]
        before = df.loc[i].to_dict()
        df.loc[i, "state"] = state
        df.loc[i, "decided_at"] = stamp()
        if note:
            df.loc[i, "note"] = note
        _write_review(folder, df)
    return before


def add_check(folder, row):
    """Append one row to checks.csv (rewritten atomically)."""
    with LOCK:
        p = checks_path(folder)
        old = pd.read_csv(p, dtype=str, keep_default_na=False) if os.path.isfile(p) else \
            pd.DataFrame(columns=CHECK_COLUMNS)
        new = {c: "" for c in CHECK_COLUMNS} | {k: ("" if v is None else v) for k, v in row.items()}
        for c in ("max_close_diff", "max_hl_diff"):
            if new[c] != "" and not isinstance(new[c], str):
                new[c] = f"{new[c]:.6f}"
        os.makedirs(out_dir(folder), exist_ok=True)
        sm.write_csv(pd.concat([old, pd.DataFrame([new], columns=CHECK_COLUMNS).astype(str)], ignore_index=True)
                     [CHECK_COLUMNS], p)


def read_checks(folder):
    p = checks_path(folder)
    if not os.path.isfile(p):
        return pd.DataFrame(columns=CHECK_COLUMNS)
    return pd.read_csv(p, dtype=str, keep_default_na=False)


# ---- the tokens.csv note (P93) ----

PROBE_NOTE = "price checked by probe"


def _retoken_checks(folder):
    """{(security_id, token): price check text} of retoken's applied files (update/retoken_<date>.csv)."""
    out = {}
    d = os.path.join(mp.out_dir(folder), "update")
    if not os.path.isdir(d):
        return out
    for name in sorted(os.listdir(d)):
        if not (name.startswith("retoken_") and name.endswith(".csv")):
            continue
        df = pd.read_csv(os.path.join(d, name), dtype=str, keep_default_na=False)
        for r in df.itertuples():
            if getattr(r, "price_check", "").startswith("OK") and r.new_token:
                out[(r.security_id, r.new_token)] = r.price_check
    return out


def note_for(note, status, sid, token, probes, checks, retoken):
    """The tokens.csv note with what was really checked for this token (P93): this tool's latest pre-check of the
    token; else the probe when it probed this token; else retoken's price check; else the probe of an older token;
    else "unchecked". The SERIES_SWITCH text is kept."""
    if status not in mp.MATCHED or not token:
        return note
    base = note.replace(f"; {PROBE_NOTE}", "").replace(PROBE_NOTE, "").strip().rstrip(";").strip()
    c = checks.get((sid, token))
    p = probes.get(sid)
    if c is not None:
        days = f"{c['days']} days, max {pct(float(c['max_close_diff']))}" if c["max_close_diff"] else \
            f"{c['days'] or 0} days"
        truth = f"price check new token {c['result']} {c['at'][:10]} ({days})"
    elif p and str(p.get("instrument_token") or "") == token:
        truth = f"probe: price checked {p.get('price_day') or ''}".strip() if p.get("price_status") == "MATCHED" \
            else "probe: not price checked"
    elif (sid, token) in retoken:
        truth = f"retoken: price check {retoken[(sid, token)]}"
    elif p and p.get("instrument_token"):
        truth = f"probe: old token {p['instrument_token']}; this token unchecked"
    else:
        truth = "unchecked"
    return f"{base}; {truth}" if base else truth


def notes(folder, toks):
    """The note column of a token match (mp.match_tokens frame) rewritten by note_for."""
    probes = mp.read_probes(folder)
    ch = read_checks(folder)
    ch = ch[ch["kind"] == "PRECHECK"] if len(ch) else ch
    checks = {(r["security_id"], r["new_token"]): r for r in ch.to_dict("records")}       # the last row wins
    rt = _retoken_checks(folder)
    return [note_for(t.note, t.match_status, t.security_id, str(t.instrument_token or ""), probes, checks, rt)
            for t in toks.itertuples()]


# ---- token-review ----

def _log_line(folder, text):
    p = os.path.join(out_dir(folder), "review.log")
    old = open(p, encoding="utf-8").read() if os.path.isfile(p) else ""
    os.makedirs(out_dir(folder), exist_ok=True)
    sm.write_text(p, old + f"{stamp()} {text}\n")


def evidence_lines(path, limit=12):
    if not path or not os.path.isfile(path):
        return []
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    lines = [f"    {path}"]
    for r in df.head(limit).to_dict("records"):
        lines.append("      " + ", ".join(f"{k} {v}" for k, v in r.items() if v != ""))
    if len(df) > limit:
        lines.append(f"      ... {len(df) - limit} more rows")
    return lines


def review(folder, accept=None, reject=None, apply=False, log=print):
    """List the active reviews, or accept / reject one stock (dry run unless apply). Returns {"rows", "action",
    "done"}. accept: a parked download (after-download refusal) is installed offline (the old files archived, the
    progress db set to the new token); any other review becomes ACCEPTED and the next update / download rebuilds the
    stock without the pre-check (the after-download comparison still runs). reject: REJECTED, the stored files stay
    frozen (no daily append) until Kite's token for the stock changes again. apply takes equity_1min/.lock."""
    rows = active(folder)
    if not accept and not reject:
        if not len(rows):
            log("No token reviews.")
        for r in rows.to_dict("records"):
            log(f"{r['state']} {r['security_id']} {r['symbol']}: {r['reason']} {r['old_token'] or '-'} -> "
                f"{r['new_token']}, {r['result']}, max close diff {pct(float(r['max_close_diff'])) if r['max_close_diff'] else '-'}"
                f", first seen {r['first_seen']}, last checked {r['last_checked']}"
                + (f", decided {r['decided_at']}" if r["decided_at"] else "")
                + (f", parked {r['parked']}" if r["parked"] else "") + (f" ({r['note']})" if r["note"] else ""))
            for line in evidence_lines(r["report"]):
                log(line)
        return {"rows": rows, "action": None, "done": False}
    sid = (accept or reject).strip().upper()
    r = active(folder, sid)
    what = "accept" if accept else "reject"
    if r is None or (accept and r["state"] == ACCEPTED) or (reject and r["state"] == REJECTED):
        log(f"{what} {sid}: nothing to do (no {'pending' if accept else 'open'} token review for it).")
        return {"rows": rows, "action": None, "done": False}
    parked = r["parked"] if accept and r["result"] == AFTER_DOWNLOAD and r["parked"] else ""
    if accept:
        plan = (f"install the parked download {parked} (old files to archive/, progress db token {r['new_token']})"
                if parked else f"mark ACCEPTED: the next update / download rebuilds it on token {r['new_token']} "
                               f"without the pre-check (the after-download comparison still runs)")
    else:
        plan = (f"mark REJECTED: keep token {r['old_token'] or '-'} and the stored files, frozen (no daily append) "
                f"until Kite's token changes again")
    if not apply:
        log(f"Dry run, {what} {sid} {r['symbol']} ({r['result']}): would {plan}. Nothing written; run with --apply.")
        return {"rows": rows, "action": what, "done": False}
    lock = RunLock(mp.lock_path(folder)).acquire(f"equity_minute.py token-review --{what}")
    try:
        if parked:
            install(folder, r)
            resolve(folder, sid, BUILT, f"accepted; parked download installed from {parked}")
        else:
            resolve(folder, sid, ACCEPTED if accept else REJECTED,
                    "accepted by token-review" if accept else "rejected by token-review")
        text = f"token-review --{what} {sid} {r['symbol']} {r['old_token'] or '-'} -> {r['new_token']}: {plan}"
        _log_line(folder, text)
        log(f"Done: {text}")
    finally:
        lock.release()
    return {"rows": rows, "action": what, "done": True}


def install(folder, r):
    """Accept a parked download: archive the stored files, move the parked build in, set the progress db row DONE
    with the new token. The caller holds the lock."""
    from backfill.equity import minute_download as md        # minute_download imports this module
    sid, parked = r["security_id"], r["parked"]
    if not os.path.isfile(os.path.join(parked, ms.DAYS_FILE)):
        raise TokenCheckError(f"{sid}: the parked download {parked} is missing or has no {ms.DAYS_FILE}.")
    _, stocks, _ = md.read_progress(folder)
    st = stocks.get(sid)
    if not st or st["status"] != "DONE" or (r["old_token"] and st["token"] != r["old_token"]):
        raise TokenCheckError(f"{sid}: the progress db no longer has the old token {r['old_token']} DONE; not "
                              "installed.")
    final = ms.stock_dir(folder, sid)
    if os.path.isdir(final):
        md.archive(folder, sid, f"token {r['old_token']} -> {r['new_token']} (accepted after review)")
    shutil.move(parked, final)
    days = ms.read_days(final)
    rows = sum(pq.ParquetFile(p).metadata.num_rows for p in ms.year_paths(final).values())
    size = sum(os.path.getsize(os.path.join(final, n)) for n in os.listdir(final))
    db = md.Progress(folder)
    try:
        db.drop_other_token(sid, r["new_token"])
        db.save_stock(security_id=sid, token=str(r["new_token"]), status="DONE",
                      first_day=md.iso(days["date"].iat[0]) if len(days) else "",
                      last_day=md.iso(days["date"].iat[-1]) if len(days) else "", days=len(days), minute_rows=rows,
                      bytes=size, built_at=md.stamp())
    finally:
        db.close()


# ---- token-replay ----

def archived_changes(folder, dates):
    """[(security_id, switch date, old token, new token, archive folder)] of the archive folders <sid>_<date>[_n]
    whose archived.txt says "token A -> B", for the given dates."""
    base = os.path.join(mp.out_dir(folder), "archive")
    out = []
    for name in sorted(os.listdir(base)) if os.path.isdir(base) else []:
        parts = name.split("_")
        if len(parts) < 2 or not mp_id(parts[0]) or parts[1] not in dates:
            continue
        p = os.path.join(base, name, "archived.txt")
        if not os.path.isfile(p):
            continue
        text = open(p, encoding="utf-8").read()
        if "token " not in text or " -> " not in text:
            continue
        old, new = (x.split()[0] for x in text.split("token ", 1)[1].split(" -> ", 1))
        out.append((parts[0], sm.as_date(parts[1]), old, int(new), os.path.join(base, name)))
    return out


def mp_id(s):
    return bool(ms.ID_RE.match(s))


DEFAULT_DATES = ("2026-10-30", "2026-10-31")
DEFAULT_CONTROLS = (("MAHSCOOTER", "PIONRINV"), ("VENUSREM", "GFLLIMITED"))


def replay(folder, kite, calc, dates=DEFAULT_DATES, controls=DEFAULT_CONTROLS, speed=2.0, log=print):
    """The pre-check on archived token changes (read-only): for each archived change of `dates`, the new token's day
    candles on the reference days before the switch date (one request per token over every window that uses it),
    plus the negative controls (stock A's reference days against stock B's new token). Writes nothing. Returns
    {"rows", "totals", "requests"}."""
    changes = archived_changes(folder, set(dates))
    if not changes:
        raise TokenCheckError(f"No archived token changes of {', '.join(dates)} in {mp.out_dir(folder)}\\archive.")
    by = {}
    for sid, sw, old, new, _ in changes:
        upto = [d for d in calc.days if d < sw]
        refs = reference_days(calc.bhav_of(sid), calc.days, upto[-1]) if upto else reference_days(None, [], sw)
        sym = calc.inp.by_id.get(sid, {}).get("symbol", sid)
        by[sid] = {"symbol": sym, "old": old, "new": new, "refs": refs, "switch": sw}
    sym2sid = {v["symbol"]: k for k, v in by.items()}
    cases = [(sid, sid, False) for sid in sorted(by)]
    for a, b in controls:
        if a in sym2sid and b in sym2sid:
            cases.append((sym2sid[a], sym2sid[b], True))
        else:
            log(f"Control {a} vs {b}: not among the archived changes, skipped.")
    span = {}
    for ref, tok, _ in cases:
        refs = by[ref]["refs"]
        if len(refs):
            a, b = span.get(tok, (refs["date"].iloc[0], refs["date"].iloc[-1]))
            span[tok] = (min(a, refs["date"].iloc[0]), max(b, refs["date"].iloc[-1]))
    limiter = RateLimiter(speed)
    frames, errors, requests = {}, {}, 0
    for tok, (a, b) in sorted(span.items()):
        got = []
        try:
            for s, e in make_chunks(a, b, "day"):
                requests += 1
                got += fetch_chunk(kite, limiter, by[tok]["new"], s, e, "day")
            frames[tok] = mp.day_frame(got)
        except ChunkFailed as e:
            errors[tok] = str(e)
    rows = []
    for ref, tok, control in cases:
        s, t = by[ref], by[tok]
        kd = frames.get(tok, pd.DataFrame())
        v = judge(s["refs"], kd, factor_fn(folder, ref), s["switch"])
        rows.append({"sid": ref, "symbol": f"{s['symbol']} vs {t['symbol']} token" if control else s["symbol"],
                     "old_token": "" if control else s["old"], "token": t["new"],
                     "switch": s["switch"].isoformat(), "days": v["days"], "max_close_diff": pct(v["max_close_diff"]),
                     "max_hl_diff": pct(v["max_hl_diff"]), "basis": v["basis"], "result": v["result"],
                     "note": errors.get(tok, "")})
    df = pd.DataFrame(rows)
    with pd.option_context("display.width", 220, "display.max_rows", 500, "display.max_colwidth", 60):
        log(df.to_string(index=False))
    totals = df["result"].value_counts().to_dict()
    log(f"\nrows {len(df)}: " + ", ".join(f"{k} {v}" for k, v in sorted(totals.items())) +
        f" (sum {sum(totals.values())})")
    log(f"Kite day-candle requests: {requests}; files written: 0")
    return {"rows": df, "totals": totals, "requests": requests}

"""ICICI 1-minute source, part A: the work list (icici_minute.py plan). docs/dev/equity_icici_source.md has the rules.

Inputs, only read: the Kite gap list (equity_1min/plan/kite_gaps.csv), the Kite store's days.parquet files (a day
Kite already has minutes for never costs an ICICI call), the bhavcopy (through our own cache), the security master,
the exit list and the index membership (minute_plan.Inputs). Network: one download of ICICI's security master (the
public SecurityMaster.zip; reused when today's copy is saved; none with --offline). Output (every file atomic):
    equity_1min_icici/reference/SecurityMaster_<IST date>.zip, NSEScripMaster_<IST date>.txt   as received
    equity_1min_icici/plan/icici_plan.csv            one row per stock: ICICI code, match, priority, days, calls
    equity_1min_icici/plan/icici_plan_days.parquet   every stock-day to fetch (the download's day list)
    equity_1min_icici/plan/plan_rows.csv             per kite_gaps.csv row: days needed, Kite-covered (coverage)
    equity_1min_icici/plan/plan_report.txt, plan_info.json
    equity_1min_icici/bhav_cache/                    the bhavcopy rows by security_id (minute_download.bhav_cache)
equity_1min_icici/.lock: one plan / download at a time; refused while equity_update.py or an input build runs."""
import glob
import io
import math
import os
import zipfile
from bisect import bisect_left, bisect_right
from datetime import date

import pandas as pd
import pyarrow.parquet as pq

import config
from backfill.equity import icici_client as ic
from backfill.equity import icici_store as st
from backfill.equity import minute_download as md
from backfill.equity import minute_plan as mp
from backfill.equity import minute_store as ms
from backfill.equity import security_master as sm
from backfill.equity import update as eu
from backfill.locks import RunLock, held

GAP_KINDS = ("1MIN", "BOTH")
DAY_CALL_DAYS = 900              # trading days per 1day call (a call returns at most 1,000 candles)
MAX_CALLS = 4800                 # default daily cap (ICICI documents 5,000 calls a day)
SPEED = 90                       # calls per minute (ICICI documents 100)
MAX_SPEED = 100
MARKET_WIDE = 1                  # market-wide gap days are added for live stocks of priority <= 1 by default
DEAD = ("EXITED", "DORMANT")
MATCH_KINDS = ["ISIN", "SYMBOL_ONLY", "NOT_IN_ICICI", "REVIEW"]
FETCHABLE = ("ISIN", "SYMBOL_ONLY")
MASTER_COLUMNS = ["Token", "ShortName", "Series", "CompanyName", "ISINCode", "ExchangeCode"]
PLAN_COLUMNS = ["security_id", "symbol", "company_name", "icici_code", "icici_series", "match_kind", "match_note",
                "priority", "life", "status", "reasons", "need_ranges", "need_from", "need_to", "trading_days",
                "kite_covered_days", "est_day_calls", "est_minute_calls", "est_calls"]
DAY_LIST_COLUMNS = ["security_id", "date", "row", "reason", "priority"]
ROW_COLUMNS = ["row", "data_missing", "security_id", "symbols", "reason", "need_from", "need_to", "priority",
               "gap_trading_days", "stocks", "days", "kite_covered", "icici_days"]
CACHE = "bhav_cache"


class IciciPlanError(mp.MinutePlanError):
    pass


def quiet(*a, **k):
    pass


def refuse_busy(folder):
    """StoreBusy while equity_update.py runs (its OS lock <folder>/equity_update.lock is held)."""
    path = os.path.join(folder, eu.LOCK)
    if held(path):
        raise mp.StoreBusy(f"python equity_update.py holds {path}; run after it ends.")


def cache_out(folder):
    return os.path.join(st.out_dir(folder), CACHE)


def iso(d):
    return d.isoformat() if d else ""


# ---- ICICI's security master ----

def master_text(data):
    """The NSEScripMaster.txt bytes inside SecurityMaster.zip."""
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as z:
            names = [n for n in z.namelist() if os.path.basename(n).lower() == "nsescripmaster.txt"]
            if not names:
                raise IciciPlanError("ICICI's SecurityMaster.zip has no NSEScripMaster.txt.")
            return z.read(names[0])
    except zipfile.BadZipFile as e:
        raise IciciPlanError(f"ICICI's SecurityMaster.zip is not a zip file ({e}).")


def read_master(path):
    """ICICI's NSE master: one row per scrip, the columns of MASTER_COLUMNS stripped (all text)."""
    df = pd.read_csv(path, dtype=str, skipinitialspace=True, keep_default_na=False)
    df.columns = [c.strip() for c in df.columns]
    missing = [c for c in MASTER_COLUMNS if c not in df]
    if missing:
        raise IciciPlanError(f"{path}: no column {', '.join(missing)}.")
    df = df[MASTER_COLUMNS].apply(lambda c: c.str.strip())
    return df


def get_master(folder, client, today, offline=False, log=print):
    """(path of NSEScripMaster_<date>.txt, downloaded?): today's saved copy, else one download saved as received
    (zip and txt); offline: the newest saved copy."""
    ref = st.reference_dir(folder)
    saved = sorted(glob.glob(os.path.join(ref, "NSEScripMaster_*.txt")))
    txt = os.path.join(ref, f"NSEScripMaster_{today.isoformat()}.txt")
    if offline:
        if not saved:
            raise mp.NotBuilt(f"No saved ICICI security master in {ref}: run plan without --offline once.")
        return saved[-1], False
    if os.path.isfile(txt):
        return txt, False
    client = client or ic.BreezeClient.from_env()
    data = client.security_master()
    body = master_text(data)
    os.makedirs(ref, exist_ok=True)

    def put(content):
        def write(tmp):
            with open(tmp, "wb") as f:
                f.write(content)
        return write
    sm.write_atomic(os.path.join(ref, f"SecurityMaster_{today.isoformat()}.zip"), put(data))
    sm.write_atomic(txt, put(body))
    log(f"ICICI security master downloaded: {txt}")
    return txt, True


def match_stock(history, master_by_isin, master_by_symbol):
    """(code, series, company, match_kind, note) of one security: ISIN first (every ISIN it ever had), then the symbol
    (SYMBOL_ONLY: price checked before anything is stored), else NOT_IN_ICICI; several codes are REVIEW."""
    isins = sorted({i for i in history["isin"] if i})
    symbols = sorted({s for s in history["symbol"] if s})
    for kind, keys, by, field in (("ISIN", isins, master_by_isin, "ISINCode"),
                                  ("SYMBOL_ONLY", symbols, master_by_symbol, "ExchangeCode")):
        rows = [r for k in keys for r in by.get(k, [])]
        codes = sorted({r["ShortName"] for r in rows})
        if len(codes) == 1:
            series = ";".join(sorted({x["Series"] for x in rows}))
            via = ", ".join(sorted({x[field] for x in rows}))
            return codes[0], series, rows[0]["CompanyName"], kind, f"{field} {via}"
        if len(codes) > 1:
            return "", "", "", "REVIEW", f"several ICICI codes by {field}: {', '.join(codes)}"
    return "", "", "", "NOT_IN_ICICI", "no ICICI scrip with any of its ISINs or symbols"


def master_index(master):
    by_isin, by_symbol = {}, {}
    for r in master.to_dict("records"):
        if r["ISINCode"]:
            by_isin.setdefault(r["ISINCode"], []).append(r)
        if r["ExchangeCode"]:
            by_symbol.setdefault(r["ExchangeCode"], []).append(r)
    return by_isin, by_symbol


# ---- days ----

def bhav_days(paths, sids):
    """{security_id: sorted bhavcopy dates} of the given securities, from the cache (two columns only)."""
    out = {}
    if not sids:
        return out
    for p in paths:
        t = pq.read_table(p, columns=["security_id", "date"], filters=[("security_id", "in", sorted(sids))])
        for sid, d in zip(t.column("security_id").to_pylist(), t.column("date").to_pylist()):
            out.setdefault(sid, []).append(sm.as_date(d))
    return {k: sorted(set(v)) for k, v in out.items()}


def kite_minute_days(folder, sid):
    """The days the Kite store already has minutes for (equity_1min/data/<sid>/days.parquet, minute_rows > 0)."""
    path = os.path.join(ms.stock_dir(folder, sid), ms.DAYS_FILE)
    if not os.path.isfile(path):
        return set()
    t = pq.read_table(path, columns=["date", "minute_rows"]).to_pandas()
    return {sm.as_date(d) for d, n in zip(t["date"], t["minute_rows"]) if n and n > 0}


def membership_periods(membership):
    """{security_id: [(priority, from, to)]} of the NIFTY 50 (1) and NIFTY 500 (2) membership periods."""
    names = {name: p for p, name in mp.PRIORITY_INDEX}
    out = {}
    for r in membership[membership["index"].isin(names)].itertuples():
        hi = r.to_date
        hi = date.max if hi is None or str(hi) in ("", "None", "nan", "NaT") else sm.as_date(hi)
        out.setdefault(r.security_id, []).append((names[r.index], sm.as_date(r.from_date), hi))
    return out


def gap_priority(periods, sid, a, b):
    """1 in NIFTY 50 during a..b, 2 in NIFTY 500, else 3 (kite_gaps.csv's rule)."""
    return min([p for p, lo, hi in periods.get(sid, []) if lo <= b and hi >= a] + [3])


def runs_of(days, calendar):
    """Runs of consecutive trading days: [(first, last, n)]."""
    pos = {d: i for i, d in enumerate(calendar)}
    out = []
    for d in days:
        if out and pos.get(d, -9) == pos.get(out[-1][1], -99) + 1:
            out[-1] = (out[-1][0], d, out[-1][2] + 1)
        else:
            out.append((d, d, 1))
    return out


def minute_calls(days, calendar):
    """Calls of 1-2 days: two needed days share a call only when they are consecutive trading days."""
    return sum(math.ceil(n / 2) for _, _, n in runs_of(days, calendar))


def pair_chunks(days, calendar):
    """[(first, last)] chunks of 1-2 consecutive trading days covering the sorted days."""
    out = []
    for a, b, n in runs_of(days, calendar):
        i = bisect_left(calendar, a)
        run = calendar[i:i + n]
        for k in range(0, n, 2):
            out.append((run[k], run[min(k + 1, n - 1)]))
    return out


def day_call_ranges(first, last, calendar):
    """[(from, to)] 1day call ranges of at most DAY_CALL_DAYS trading days covering first..last."""
    a, b = bisect_left(calendar, first), bisect_right(calendar, last)
    span = calendar[a:b] or [first]
    return [(span[k], span[min(k + DAY_CALL_DAYS, len(span)) - 1]) for k in range(0, len(span), DAY_CALL_DAYS)]


def need_ranges(days, calendar):
    return "; ".join(f"{iso(a)}..{iso(b)}" for a, b, _ in runs_of(days, calendar))


# ---- the plan ----

def read_gaps(folder):
    path = os.path.join(mp.plan_dir(folder), "kite_gaps.csv")
    if not os.path.isfile(path):
        raise mp.NotBuilt(f"No {path}: run python equity_minute.py gaps first.")
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def build(folder, inp, master, market_wide=MARKET_WIDE, log=quiet):
    """(plan frame, day-list frame, rows frame, counts) from the gap rows, the bhavcopy days, the Kite store and the
    ICICI master. Writes only the bhavcopy cache (under the caller's lock)."""
    gaps = read_gaps(folder)
    cal = inp.days
    paths = md.bhav_cache(folder, inp, log, out=cache_out(folder))
    active = [u["security_id"] for u in inp.universe if u["status"] == "ACTIVE"]
    by_isin, by_symbol = master_index(master)
    periods = membership_periods(inp.membership)
    rows = gaps[gaps["data_missing"].isin(GAP_KINDS)]
    wanted = {r.security_id for r in rows.itertuples() if r.security_id}
    market = [r for r in rows.itertuples() if not r.security_id]
    if market:
        wanted |= set(active)
    days_of = bhav_days(paths, wanted & set(inp.by_id))
    need = {}                                     # (sid, date) -> (row, reason, priority)
    row_info, skipped = [], []
    for r in rows.itertuples():
        a, b = sm.as_date(r.need_from), sm.as_date(r.need_to)
        if r.security_id:
            if r.security_id not in inp.by_id:
                skipped.append(f"row {r.Index}: {r.security_id} is not in the universe")
                continue
            prio = {r.security_id: int(r.priority or 3)}
        else:
            prio = {sid: gap_priority(periods, sid, a, b) for sid in active}
            prio = {sid: p for sid, p in prio.items() if p <= market_wide}
        n_stocks, n_days = 0, 0
        for sid, p in sorted(prio.items()):
            ds = days_of.get(sid, [])
            ds = ds[bisect_left(ds, a):bisect_right(ds, b)]
            if not ds:
                continue
            n_stocks += 1
            n_days += len(ds)
            for d in ds:
                old = need.get((sid, d))
                if old is None or p < old[2]:
                    need[(sid, d)] = (r.Index, r.reason, p)
        row_info.append({"row": r.Index, "data_missing": r.data_missing, "security_id": r.security_id,
                         "symbols": r.symbols, "reason": r.reason, "need_from": r.need_from, "need_to": r.need_to,
                         "priority": r.priority, "gap_trading_days": r.trading_days, "stocks": n_stocks,
                         "days": n_days})
    # days the Kite store already has minutes for never cost a call
    by_sid = {}
    for (sid, d), v in need.items():
        by_sid.setdefault(sid, {})[d] = v
    covered_row, covered_sid, day_rows = {}, {}, []
    for sid in sorted(by_sid):
        kite = kite_minute_days(folder, sid)
        for d, (row, reason, p) in sorted(by_sid[sid].items()):
            if d in kite:
                covered_row[row] = covered_row.get(row, 0) + 1
                covered_sid[sid] = covered_sid.get(sid, 0) + 1
            else:
                day_rows.append({"security_id": sid, "date": d, "row": row, "reason": reason, "priority": p})
    days = pd.DataFrame(day_rows, columns=DAY_LIST_COLUMNS)
    rows_df = pd.DataFrame(row_info, columns=ROW_COLUMNS[:-2])
    rows_df["kite_covered"] = rows_df["row"].map(covered_row).fillna(0).astype(int)
    rows_df["icici_days"] = rows_df["days"] - rows_df["kite_covered"]
    plan = []
    for sid, g in days.groupby("security_id", sort=True):
        u = inp.by_id[sid]
        ds = sorted(g["date"])
        code, series, company, kind, note = match_stock(inp.history[sid], by_isin, by_symbol)
        fetch = kind in FETCHABLE
        d_calls = len(day_call_ranges(ds[0], ds[-1], cal)) if fetch else 0
        m_calls = minute_calls(ds, cal) if fetch else 0
        plan.append({"security_id": sid, "symbol": u["symbol"],
                     "company_name": company or inp.names.get(u["symbol"], ""), "icici_code": code,
                     "icici_series": series, "match_kind": kind, "match_note": note,
                     "priority": int(g["priority"].min()), "life": "DEAD" if u["status"] in DEAD else "LIVE",
                     "status": u["status"], "reasons": ";".join(sorted(set(g["reason"]))),
                     "need_ranges": need_ranges(ds, cal), "need_from": iso(ds[0]), "need_to": iso(ds[-1]),
                     "trading_days": len(ds), "kite_covered_days": covered_sid.get(sid, 0),
                     "est_day_calls": d_calls, "est_minute_calls": m_calls, "est_calls": d_calls + m_calls})
    # stocks every needed day of which the Kite store has (stale labels): listed in the counts only
    plan = pd.DataFrame(plan, columns=PLAN_COLUMNS)
    if len(plan):
        plan["_life"] = (plan["life"] != "DEAD").astype(int)
        plan = plan.sort_values(["priority", "_life", "trading_days", "security_id"],
                                ascending=[True, True, False, True], kind="stable").drop(columns="_life")
        plan = plan.reset_index(drop=True)
    fully_covered = sorted(set(by_sid) - set(plan["security_id"]))
    counts = {"gap_rows": len(rows), "market_wide_rows": len(market), "skipped": skipped,
              "kite_covered_days": int(sum(covered_row.values())), "kite_covered_stocks": fully_covered,
              "market_wide": market_wide}
    return plan, days, rows_df, counts


def report(plan, rows_df, counts, master_path, max_calls=MAX_CALLS):
    lines = [f"ICICI 1-minute work list (master {os.path.basename(master_path)})", ""]
    lines.append(f"kite_gaps.csv rows used (1MIN / BOTH): {counts['gap_rows']:,} "
                 f"({counts['market_wide_rows']} market-wide, added for live stocks of priority <= "
                 f"{counts['market_wide']})")
    lines.append(f"Stock-days the Kite store already has minutes for (removed): {counts['kite_covered_days']:,}; "
                 f"stocks with nothing left: {len(counts['kite_covered_stocks']):,}")
    for s in counts["skipped"]:
        lines.append(f"  skipped {s}")
    lines.append("")
    if plan.empty:
        lines.append("Nothing to fetch.")
        return "\n".join(lines) + "\n"
    head = f"{'priority':>8} {'life':<5} {'match':<12} {'stocks':>7} {'days':>9} {'calls':>9}"
    lines += ["Per priority, life and match kind:", head]
    g = plan.groupby(["priority", "life", "match_kind"], sort=True)
    for (p, life, kind), x in g:
        lines.append(f"{p:>8} {life:<5} {kind:<12} {len(x):>7,} {x['trading_days'].sum():>9,} "
                     f"{x['est_calls'].sum():>9,}")
    lines += ["", "Totals per priority (fetchable = ISIN + SYMBOL_ONLY):"]
    for p, x in plan.groupby("priority", sort=True):
        f = x[x["match_kind"].isin(FETCHABLE)]
        lines.append(f"  priority {p}: {len(x):,} stocks ({len(f):,} fetchable, "
                     f"{(x['match_kind'] == 'NOT_IN_ICICI').sum():,} not in ICICI, "
                     f"{(x['match_kind'] == 'REVIEW').sum():,} review), {f['trading_days'].sum():,} days, "
                     f"{f['est_calls'].sum():,} calls (~{f['est_calls'].sum() / max_calls:.1f} days of quota)")
    f = plan[plan["match_kind"].isin(FETCHABLE)]
    total = int(f["est_calls"].sum())
    lines += ["", f"Fetchable: {len(f):,} stocks, {int(f['trading_days'].sum()):,} stock-days, {total:,} calls "
              f"({int(f['est_day_calls'].sum()):,} day + {int(f['est_minute_calls'].sum()):,} minute) = about "
              f"{total / max_calls:.1f} days at {max_calls:,} calls a day.",
              "Upper bound: a stock whose ICICI minutes start later costs up to ~12 search calls and skips the "
              "days before (ICICI_BEFORE_START).", ""]
    mw = rows_df[rows_df["security_id"] == ""]
    if len(mw):
        lines.append("Market-wide rows:")
        for r in mw.itertuples():
            lines.append(f"  {r.reason} {r.need_from}..{r.need_to}: {r.stocks:,} stocks, {r.days:,} days, "
                         f"{r.kite_covered:,} Kite-covered, {r.icici_days:,} to fetch")
        lines.append("")
    top = plan.head(20)
    lines.append("First 20 in download order:")
    for r in top.itertuples():
        lines.append(f"  P{r.priority} {r.life:<4} {r.symbol:<14} {r.icici_code or '-':<8} {r.match_kind:<12} "
                     f"{r.trading_days:>6,} days {r.est_calls:>6,} calls  {r.need_from}..{r.need_to}")
    return "\n".join(lines) + "\n"


def plan(folder=None, client=None, market_wide=MARKET_WIDE, offline=False, now=None, log=print, dry_run=False,
         preview=None):
    """Build the ICICI work list (Part A). Refuses while equity_update.py or an input build runs, or the lock is
    busy (exit 2); NotBuilt when kite_gaps.csv or an input is missing (exit 1). dry_run: the saved master (no
    network), no plan file written (only the derived bhavcopy cache may be refreshed, as by any plan); preview(folder,
    result) runs under the lock and its text is returned as result["preview"]."""
    folder = folder or config.equity_data_dir()
    refuse_busy(folder)
    inp = mp.Inputs(folder, bhav=False)
    lock = RunLock(st.lock_path(folder)).acquire("icici_minute.py plan" + (" --dry-run" if dry_run else ""))
    try:
        today = md.as_ist(now).date()
        path, _ = get_master(folder, client, today, offline or dry_run, log)
        master = read_master(path)
        p, days, rows_df, counts = build(folder, inp, master, market_wide, log)
        if dry_run:
            res = {"plan": p, "days": days, "rows": rows_df, "counts": counts, "inp": inp, "master": path,
                   "market_wide": market_wide, "report": report(p, rows_df, counts, path)}
            if preview:
                res["preview"] = preview(folder, res)
            return res
        out = st.plan_dir(folder)
        os.makedirs(out, exist_ok=True)
        sm.write_csv(p, os.path.join(out, "icici_plan.csv"))
        sm.write_parquet(days, os.path.join(out, "icici_plan_days.parquet"))
        sm.write_csv(rows_df, os.path.join(out, "plan_rows.csv"))
        text = report(p, rows_df, counts, path)
        sm.write_text(os.path.join(out, "plan_report.txt"), text)
        mp.write_json(os.path.join(out, "plan_info.json"),
                      {"built_at": md.as_ist(now).strftime("%Y-%m-%d %H:%M:%S"), "master": os.path.basename(path),
                       "market_wide": market_wide, "stocks": len(p), "days": len(days),
                       "kite_covered_days": counts["kite_covered_days"],
                       "kite_covered_stocks": counts["kite_covered_stocks"]})
        log(text)
        return {"plan": p, "days": days, "rows": rows_df, "counts": counts, "out": out, "report": text}
    finally:
        lock.release()


def read_plan(folder):
    """(plan frame, day-list frame) as written by plan(); NotBuilt without them."""
    out = st.plan_dir(folder)
    pp, dp = os.path.join(out, "icici_plan.csv"), os.path.join(out, "icici_plan_days.parquet")
    if not (os.path.isfile(pp) and os.path.isfile(dp)):
        raise mp.NotBuilt(f"No ICICI plan in {out}: run python icici_minute.py plan first.")
    p = pd.read_csv(pp, dtype=str, keep_default_na=False)
    for c in ("priority", "trading_days", "kite_covered_days", "est_day_calls", "est_minute_calls", "est_calls"):
        p[c] = p[c].astype(int)
    d = pq.read_table(dp).to_pandas()
    d["date"] = [sm.as_date(x) for x in d["date"]]
    return p, d


def read_rows(folder):
    path = os.path.join(st.plan_dir(folder), "plan_rows.csv")
    if not os.path.isfile(path):
        raise mp.NotBuilt(f"No {path}: run python icici_minute.py plan first.")
    return pd.read_csv(path, dtype=str, keep_default_na=False)

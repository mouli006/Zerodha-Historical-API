"""Price check of the data audit (#251): is every LOADED stock-day at the right price? A flag only.

For every traded day the combined loader takes from Kite or ICICI (minute_combined day table, source KITE / ICICI;
never NONE, so never an ICICI mixed day), the day's loaded close / high / low are computed from the days.parquet day
values (minute_open/high/low/close converted the way the loaders convert the bars: Kite with minute_store.convert,
ICICI with icici_scale) and compared with the NSE bhavcopy:

    close_off = min(|loaded close / NSE close - 1|, |loaded close / NSE LAST - 1|)    (NSE close = 30-minute average)
    hl_off    = max(|loaded high / NSE high - 1|,   |loaded low / NSE low - 1|)

Lists: CLOSE (close_off > 5 %), HIGHLOW (hl_off > 20 % with close_off <= 5 %); one count of close_off > 2 %.
Every listed day gets one reason, the first rule that matches (REASONS). Nothing here raises: an error becomes a note
line. Only the listed rows are kept. Reads only: the bhavcopy year files (LAST, the trading calendar), the two
days.parquet files of each stock, adjustments/events.csv and adjustments/input/corporate_actions_*.csv.
Never changes a gap, a reason, a kind, the VERDICT or the exit code of the audit (Mouli Q1, 22 Nov)."""
import glob
import os
from collections import Counter

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from backfill.equity import adjustments as adj
from backfill.equity import corp_actions as ca
from backfill.equity import icici_store as st
from backfill.equity import minute_combined as mc
from backfill.equity import minute_plan as mp
from backfill.equity import minute_store as ms
from backfill.equity import security_master as sm

STORE = "prices"                       # the manual_known_gaps.csv store of known price days
CLOSE, HIGHLOW = "CLOSE", "HIGHLOW"
CLOSE_OFF, HL_OFF, SMALL_OFF = 0.05, 0.20, 0.02
TOL = 0.02                             # ratio match tolerance; a target within TOL of 1.0 is never used
BLIP_DAYS = (1, 10)                    # trading days from the day to a later ex-date (SPLIT_BLIP, DIVIDEND_CUT)
PENNY_CLOSE, PENNY_KITE = 1.0, 0.50
JUNK_OFF = 0.20
ICICI_SCALES = (0.1, 0.2, 0.25, 0.5, 2, 4, 5, 10)
REASONS = ("KNOWN", "SPLIT_BLIP", "NOT_ADJUSTED", "PENNY_TICK", "DIVIDEND_CUT", "JUNK_DAY_CANDLE", "STRAY_BARS",
           "ICICI_SCALE", "ICICI_SHORT_DAY", "UNEXPLAINED")
PROBLEM = {"SPLIT_BLIP": "P173/P175", "NOT_ADJUSTED": "P179", "PENNY_TICK": "P180", "DIVIDEND_CUT": "P205",
           "JUNK_DAY_CANDLE": "P182", "STRAY_BARS": "P183", "ICICI_SCALE": "P165/P224"}
KITE_COLUMNS = ["date", "check", "kind", "ratio", "kite_multiplier", "kite_difference", "minute_scale", "kite_close",
                "minute_open", "minute_high", "minute_low", "minute_close", "bhav_close", "bhav_high", "bhav_low"]
ICICI_COLUMNS = ["date", "check", "icici_scale", "minute_open", "minute_high", "minute_low", "minute_close",
                 "bhav_close", "bhav_high", "bhav_low"]
DAY_COLUMNS = ["security_id", "symbol", "date", "source", "list", "loaded_close", "loaded_high", "loaded_low",
               "bhav_close", "bhav_last", "bhav_high", "bhav_low", "close_off_pct", "hl_off_pct", "reason",
               "problem_id", "evidence"]
SUMMARY_COLUMNS = ["list", "source", "reason", "days", "stocks"]


# ---- inputs (read once) ----

def events_path(folder):
    return os.path.join(adj.out_dir(folder), "events.csv")


def dividend_files(folder):
    return sorted(glob.glob(os.path.join(ca.input_dir(folder), "corporate_actions_*.csv")))


def read_last(folder, owner):
    """(frame security_id / date (datetime64) / last of the traded rows, sorted; trading calendar as a sorted
    datetime64 array) from the bhavcopy year files, EQ / BE / BZ with volume > 0, the highest-volume row per
    stock-day (the minute_combined.traded_by_stock pattern)."""
    parts, cal = [], []
    files = sm.year_files(mp.store_dir(folder))
    if not files:
        raise FileNotFoundError(f"no bhavcopy year files in {mp.store_dir(folder)}")
    for f in files:
        t = pq.read_table(f, columns=["date", "isin", "series", "volume", "last"],
                          filters=[("series", "in", list(mc.SERIES)), ("volume", ">", 0)]).to_pandas()
        t["date"] = pd.to_datetime(t["date"]).to_numpy("datetime64[ns]")
        cal.append(t["date"].unique())
        t["security_id"] = t["isin"].map(owner)
        parts.append(t.loc[t["security_id"].notna(), ["security_id", "date", "volume", "last"]])
    b = pd.concat(parts, ignore_index=True)
    b = b.sort_values("volume", ascending=False, kind="stable").drop_duplicates(["security_id", "date"])
    b = b.sort_values(["security_id", "date"], kind="stable").reset_index(drop=True)[["security_id", "date", "last"]]
    return b, np.unique(np.concatenate(cal)) if cal else np.array([], "datetime64[ns]")


def read_events(folder):
    """{security_id: [(ex_date datetime64, factor)] sorted} of the applied split / bonus events (applied = Y)."""
    ev = pd.read_csv(events_path(folder), dtype=str, keep_default_na=False)
    ev = ev[(ev["applied"] == "Y") & (ev["factor"] != "")].copy()
    ev["d"] = pd.to_datetime(ev["ex_date"]).to_numpy("datetime64[ns]")
    ev["f"] = ev["factor"].astype(float)
    ev = ev.sort_values(["security_id", "d"], kind="stable")
    return {s: list(zip(g["d"].to_numpy("datetime64[ns]"), g["f"])) for s, g in ev.groupby("security_id")}


def read_dividends(folder, owner):
    """{security_id: sorted datetime64 array} of the NSE dividend ex-dates (corporate_actions_*.csv, PURPOSE
    contains 'Dividend', ISIN -> owner)."""
    files = dividend_files(folder)
    if not files:
        raise FileNotFoundError(f"no corporate_actions_*.csv in {ca.input_dir(folder)}")
    c = pd.concat([pd.read_csv(f, dtype=str, keep_default_na=False) for f in files], ignore_index=True)
    c = c[c["PURPOSE"].str.contains("dividend", case=False, na=False)].copy()
    c["security_id"] = c["ISIN"].map(owner)
    c["d"] = pd.to_datetime(c["EX-DATE"], format="%d-%b-%Y", errors="coerce")
    c = c[c["security_id"].notna() & c["d"].notna()]
    return {s: np.unique(g["d"].to_numpy("datetime64[ns]")) for s, g in c.groupby("security_id")}


# ---- the rules (pure) ----

def near(x, target, tol=TOL):
    """x within tol of target, and target not within tol of 1.0 (a 1:1250 bonus would match every day)."""
    return (target > 0 and abs(target - 1) > tol and np.isfinite(x) and x > 0 and abs(x / target - 1) <= tol)


def _fmt_d(x):
    return str(np.datetime64(x, "D"))


class Calendar:
    """Trading-day positions of the bhavcopy calendar."""

    def __init__(self, days):
        self.days = np.asarray(days, "datetime64[ns]")

    def between(self, a, b):
        """Trading days from a to b (b later: positive); None when a day is not on the calendar."""
        ia, ib = np.searchsorted(self.days, a), np.searchsorted(self.days, b)
        if ia >= len(self.days) or ib >= len(self.days) or self.days[ia] != a or self.days[ib] != b:
            return None
        return int(ib - ia)


def split_blip(day, ratios, events, cal):
    """SPLIT_BLIP evidence or None: an applied event 1-10 trading days after the day whose factor F (or 1 / F, or F x
    the neighbour event's factor, or its inverse) is within 2 % of a ratio."""
    for j, (ed, f) in enumerate(events):
        k = cal.between(day, ed)
        if k is None or not BLIP_DAYS[0] <= k <= BLIP_DAYS[1]:
            continue
        nb = [events[j + o][1] for o in (-1, 1) if 0 <= j + o < len(events)]
        for t in [f, 1 / f] + [f * x for x in nb] + [1 / (f * x) for x in nb]:
            for name, x in ratios:
                if near(x, t):
                    return f"event {_fmt_d(ed)} factor {f:g} (+{k} trading days); {name} {x:.4f} ~ {t:.4f}"
    return None


def later_run(day, ratios, events):
    """Evidence or None: a ratio within 2 % of the product (or its inverse) of a contiguous run of applied factors
    with ex-date after the day."""
    later = [(ed, f) for ed, f in events if ed > day]
    for a in range(len(later)):
        p = 1.0
        for b in range(a, len(later)):
            p *= later[b][1]
            for t in (p, 1 / p):
                for name, x in ratios:
                    if near(x, t):
                        run = ", ".join(f"{_fmt_d(ed)} x{f:g}" for ed, f in later[a:b + 1])
                        return f"later factor {p:g} ({run}); {name} {x:.4f} ~ {t:.4f}"
    return None


def classify(src, day, r, ratios, events, divs, cal, manual_hit):
    """(reason, problem_id, evidence) of one listed day; r = the day's values (dict); ratios = [(name, value)]."""
    if manual_hit:
        reason, ev, _ = manual_hit
        return "KNOWN", "", f"{reason}: {ev}".strip(": ")
    kite = src == mc.KITE
    if kite:
        e = split_blip(day, ratios, events, cal)
        if e:
            return "SPLIT_BLIP", PROBLEM["SPLIT_BLIP"], e
        e = later_run(day, ratios, events)
        if e:
            return "NOT_ADJUSTED", PROBLEM["NOT_ADJUSTED"], e
    raw = r["minute_close"] * r["minute_scale"] if kite else np.nan
    if r["bhav_close"] < PENNY_CLOSE or (kite and raw < PENNY_KITE):
        return "PENNY_TICK", PROBLEM["PENNY_TICK"], f"NSE close {r['bhav_close']:g}" + (
            f", Kite price {raw:.4f}" if kite else "")
    if kite:
        ratio, km, kc = r["ratio"], r["kite_multiplier"], r["kite_close"]
        if r["kind"] != "ADDITIVE" and np.isfinite(ratio) and abs(ratio - 1) > 1e-3:
            for x in divs:
                k = cal.between(day, x)
                if k is not None and BLIP_DAYS[0] <= k <= BLIP_DAYS[1]:
                    return "DIVIDEND_CUT", PROBLEM["DIVIDEND_CUT"], \
                        f"stored ratio {ratio:.6f}; dividend ex-date {_fmt_d(x)} (+{k} trading days)"
        day_candle = kc * km
        if np.isfinite(day_candle) and abs(day_candle / r["bhav_close"] - 1) > JUNK_OFF:
            return "JUNK_DAY_CANDLE", PROBLEM["JUNK_DAY_CANDLE"], \
                f"Kite day close {kc:g} x multiplier {km:g} = {day_candle:g} vs NSE close {r['bhav_close']:g}"
        if np.isfinite(day_candle) and min(abs(day_candle / r["bhav_close"] - 1),
                                           abs(day_candle / r["bhav_last"] - 1)) <= TOL:
            return "STRAY_BARS", PROBLEM["STRAY_BARS"], \
                f"Kite day close {kc:g} x {km:g} matches NSE; minute close {r['minute_close']:g} " \
                f"low {r['minute_low']:g} (x scale {r['minute_scale']:g})"
    else:
        for name, x in ratios:
            for t in ICICI_SCALES:
                if near(x, t):
                    e = f"icici_scale {r['icici_scale']:g}; {name} {x:.4f} ~ x{t:g}"
                    f = later_run(day, ratios, events) or split_blip(day, ratios, events, cal)
                    return "ICICI_SCALE", PROBLEM["ICICI_SCALE"], e + (f"; {f}" if f else "; no later factor")
        # Mouli A option 1 (22 Nov): an ICICI day at a split / bonus factor (not in the list above) is ICICI_SCALE too
        f = later_run(day, ratios, events) or split_blip(day, ratios, events, cal)
        if f:
            return "ICICI_SCALE", PROBLEM["ICICI_SCALE"], f"icici_scale {r['icici_scale']:g}; {f}"
        if "SHORT_DAY" in str(r["check"]).split("|"):
            return "ICICI_SHORT_DAY", "", f"label {r['check']}"
    lab = r["check"]
    return "UNEXPLAINED", "", "; ".join(f"{n} {x:.4f}" for n, x in ratios) + (f"; label {lab}" if lab else "")


def loaded_values(src, d):
    """(close, high, low) arrays of the loaded day values of days.parquet rows d."""
    if src == mc.KITE:
        m = pd.DataFrame({c: pd.to_numeric(d[f"minute_{c}"], errors="coerce").to_numpy(float)
                          for c in ("open", "high", "low", "close")})
        for c in ("ratio", "kite_multiplier", "kite_difference", "minute_scale"):
            m[c] = pd.to_numeric(d[c], errors="coerce").to_numpy(float)
        m["kind"] = d["kind"].to_numpy()
        m["volume"], m["minute_volume_scale"], m["vol_ratio"] = 0.0, 1.0, 1.0
        out = ms.convert(m)
        return out["close"], out["high"], out["low"]
    s = pd.to_numeric(d["icici_scale"], errors="coerce").to_numpy(float)
    return tuple(pd.to_numeric(d[f"minute_{c}"], errors="coerce").to_numpy(float) * s for c in ("close", "high", "low"))


# ---- the check ----

class PriceCheck:
    """Built once per audit run; .stock(sid, table) per stock (never raises); .rows / .notes / counts at the end."""

    def __init__(self, folder, manual, today, symbols=None, log=print):
        self.folder, self.manual, self.today = folder, manual, today
        self.symbols = symbols or {}
        self.notes, self.rows = [], []
        self.checked, self.small = Counter(), Counter()
        self.errors = 0
        self.ok = True
        self.bhav = self.idx = self.cal = None
        self.events, self.divs = {}, {}
        try:
            owner = sm.load_master(folder)["isin_owner"]
            self.bhav, cal = read_last(folder, owner)
            self.idx = self.bhav.groupby("security_id").indices
            self.cal = Calendar(cal)
        except Exception as e:                      # no LAST, no calendar: the check cannot run
            self.ok = False
            self.notes.append(f"price check not run: bhavcopy LAST not read ({type(e).__name__}: {e})")
            return
        try:
            self.events = read_events(folder)
        except Exception as e:
            self.notes.append(f"events.csv not read ({type(e).__name__}: {e}): SPLIT_BLIP / NOT_ADJUSTED / "
                              "the ICICI later factor never match")
        try:
            self.divs = read_dividends(folder, owner)
        except Exception as e:
            self.notes.append(f"corporate actions not read ({type(e).__name__}: {e}): DIVIDEND_CUT never matches")

    def stock(self, sid, table):
        """Check the loaded days of one stock's day table (DAY_COLUMNS of minute_combined); never raises."""
        if not self.ok or table is None or not len(table):
            return
        try:
            t = table[table["traded"].astype(bool) & table["source"].isin([mc.KITE, mc.ICICI])]
            for src, columns, path in ((mc.KITE, KITE_COLUMNS, ms.stock_dir(self.folder, sid)),
                                       (mc.ICICI, ICICI_COLUMNS, st.stock_dir(self.folder, sid))):
                want = set(t.loc[t["source"] == src, "date"])
                if want:
                    self._side(sid, src, want, mc.read_side(path, columns))
        except Exception as e:
            self.errors += 1
            if self.errors <= 20:
                self.notes.append(f"{sid}: price check failed ({type(e).__name__}: {e})")

    def _side(self, sid, src, want, d):
        if d is None:
            raise FileNotFoundError(f"{src} days.parquet missing for {len(want)} loaded days")
        d = d[d["date"].isin(want)].drop_duplicates("date", keep="last").reset_index(drop=True)
        self.checked[src] += len(d)
        if not len(d):
            return
        lc, lh, ll = loaded_values(src, d)
        bc, bh, bl = (pd.to_numeric(d[c], errors="coerce").to_numpy(float) for c in ("bhav_close", "bhav_high",
                                                                                      "bhav_low"))
        dates = pd.to_datetime(pd.Series(d["date"])).to_numpy("datetime64[ns]")
        last = np.full(len(d), np.nan)
        pos = self.idx.get(sid)
        if pos is not None:
            s = self.bhav.iloc[pos]
            last = pd.Series(s["last"].to_numpy(float), index=s["date"].to_numpy()).reindex(dates).to_numpy(float)
        with np.errstate(divide="ignore", invalid="ignore"):
            co = np.fmin(np.abs(lc / bc - 1), np.abs(lc / last - 1))
            hl = np.fmax(np.abs(lh / bh - 1), np.abs(ll / bl - 1))
        self.small[src] += int(np.sum(co > SMALL_OFF))
        lst = np.where(co > CLOSE_OFF, CLOSE, np.where((hl > HL_OFF) & ~(co > CLOSE_OFF), HIGHLOW, ""))
        for i in np.nonzero(lst != "")[0]:
            r = {c: d.at[i, c] for c in d.columns}
            for c in r:
                if c not in ("date", "check", "kind"):
                    r[c] = pd.to_numeric(r[c], errors="coerce")
            r["check"] = "" if pd.isna(r.get("check")) else str(r.get("check"))
            r["bhav_last"] = last[i]
            ratios = [("close/close", lc[i] / bc[i]), ("close/LAST", lc[i] / last[i]),
                      ("high/high", lh[i] / bh[i]), ("low/low", ll[i] / bl[i])]
            day = r["date"]
            hit = self.manual.hit(STORE, sid, day, self.today) if self.manual is not None else None
            reason, pid, ev = classify(src, dates[i], r, ratios, self.events.get(sid, []),
                                       self.divs.get(sid, np.array([], "datetime64[ns]")), self.cal, hit)
            if reason == "KNOWN":
                pid = hit_pid(self.manual, sid, day)
            self.rows.append({"security_id": sid, "symbol": self.symbols.get(sid, ""), "date": day, "source": src,
                              "list": lst[i], "loaded_close": lc[i], "loaded_high": lh[i], "loaded_low": ll[i],
                              "bhav_close": bc[i], "bhav_last": last[i], "bhav_high": bh[i], "bhav_low": bl[i],
                              "close_off_pct": round(co[i] * 100, 2), "hl_off_pct": round(hl[i] * 100, 2),
                              "reason": reason, "problem_id": pid, "evidence": ev})

    # ---- results ----

    def frame(self):
        return pd.DataFrame(self.rows, columns=DAY_COLUMNS)

    def summary(self):
        df = self.frame()
        if not len(df):
            return pd.DataFrame(columns=SUMMARY_COLUMNS)
        order = {r: i for i, r in enumerate(REASONS)}
        s = (df.groupby(["list", "source", "reason"]).agg(days=("security_id", "size"),
                                                          stocks=("security_id", "nunique")).reset_index())
        s["_o"] = s["reason"].map(order)
        return s.sort_values(["list", "source", "_o"]).drop(columns="_o").reset_index(drop=True)[SUMMARY_COLUMNS]

    def info(self):
        df = self.frame()
        per = {f"{x.list} {x.source} {x.reason}": {"days": int(x.days), "stocks": int(x.stocks)}
               for x in self.summary().itertuples(index=False)}
        return {"ran": self.ok, "checked": dict(self.checked), "checked_total": sum(self.checked.values()),
                "close_off_2pct": dict(self.small), "close_off_2pct_total": sum(self.small.values()),
                "close_days": int((df["list"] == CLOSE).sum()), "highlow_days": int((df["list"] == HIGHLOW).sum()),
                "per_list_source_reason": per, "errors": self.errors, "notes": list(self.notes)}

    def report_lines(self):
        out = ["  prices (#251, flag only: never a gap, never the exit code):"]
        if not self.ok:
            return out + [f"    note: {x}" for x in self.notes]
        df = self.frame()
        k, i = self.checked.get(mc.KITE, 0), self.checked.get(mc.ICICI, 0)
        out.append(f"    loaded days checked {k + i:,} (Kite {k:,}, ICICI {i:,})")
        out.append(f"    close off NSE close and LAST by more than 2 %: {sum(self.small.values()):,} "
                   f"(Kite {self.small.get(mc.KITE, 0):,}, ICICI {self.small.get(mc.ICICI, 0):,})")
        s = self.summary()
        for lst, text in ((CLOSE, "close off both NSE close and LAST by more than 5 %"),
                          (HIGHLOW, "high or low off by more than 20 %, close within 5 %")):
            x = df[df["list"] == lst]
            out.append(f"    {lst} ({text}): {len(x):,} days, {x['security_id'].nunique():,} stocks")
            for r in s[s["list"] == lst].itertuples(index=False):
                out.append(f"      {r.source:<5} {r.reason:<16} {r.days:>7,} days {r.stocks:>5,} stocks")
        u = df[(df["list"] == CLOSE) & (df["reason"] == "UNEXPLAINED")]
        top = u.groupby(["security_id", "symbol"]).size().sort_values(ascending=False, kind="stable").head(10)
        out.append("    UNEXPLAINED (CLOSE) top 10: " + (", ".join(f"{sym or sid} {n}" for (sid, sym), n in top.items())
                                                       or "none"))
        if self.errors > 20:
            out.append(f"    note: {self.errors - 20} more stocks failed (first 20 below)")
        out += [f"    note: {x}" for x in self.notes]
        return out


def hit_pid(manual, sid, day):
    """The problem_id of the manual prices row that matched (Manual.hit returns it only inside the evidence)."""
    df = getattr(manual, "df", None)
    if df is None:
        return ""
    for r in df.itertuples(index=False):
        if r.store == STORE and sid in (r.security_id, r.underlying) and r.from_date <= day <= (r.to_date or r.from_date):
            return r.problem_id
    return ""

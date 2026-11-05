"""The ICICI answer guard (P162 / P163 / P164; docs/dev/equity_icici_source.md "Answer guard"). Pure functions on
frames, used by the download (before staging), the build of days.parquet, and both loaders.

    minute answer   another stock code in the answer, a day with duplicated timestamps at another price level
                    (MIXED_SERIES) or more than MAX_DAY_BARS bars in 09:00-15:35 (TOO_MANY_BARS): the chunk is FAILED
    revised copies  duplicated timestamps whose copies are at the same price level (DUPLICATE_MINUTE): stored as
                    received; the last copy of the answer is the one used (aggregates and loaders)
    day answer      duplicate dates (DUPLICATE_DATES), or more than DAY_OFF_SHARE of the day candles off the bhavcopy
                    by more than DAY_OFF_TOL at the answer's own level (DAY_MISMATCH): the chunk is FAILED
Raw ICICI data is never edited: the guard only refuses answers, labels days and chooses which stored copy is read."""
import numpy as np
import pandas as pd

from backfill.equity import minute_download as md
from backfill.equity import minute_plan as mp

MIXED = "MIXED_SERIES"
DUPLICATE = "DUPLICATE_MINUTE"
TOO_MANY = "TOO_MANY_BARS"
DAY_MISMATCH = "DAY_MISMATCH"
DUPLICATE_DATES = "DUPLICATE_DATES"
GUARD_REASONS = (MIXED, TOO_MANY, DAY_MISMATCH, DUPLICATE_DATES)

MAX_DAY_BARS = 400               # bars of one day inside WINDOW (one instrument has at most 396 minutes there)
WINDOW_FROM, WINDOW_TO = "09:00", "15:35"   # ICICI's normal day: 09:07 pre-open .. 15:32; evening sessions not counted
MIXED_CLOSE_DIFF = 0.10          # duplicated timestamps whose copies' closes differ by more than 10% (median): two series
DAY_OFF_TOL = 0.02               # a day candle is off when high and low both miss the bhavcopy by more than 2% ...
DAY_OFF_SHARE = 0.10             # ... and an answer with more than 10% of such candles ...
DAY_OFF_MIN = 5                  # ... and at least 5 of them is DAY_MISMATCH


def dup_days(bars):
    """{date: (duplicated timestamps, median close difference, MIXED or DUPLICATE)} of minute bars (ts, close)."""
    if not len(bars):
        return {}
    dup = bars[bars["ts"].duplicated(keep=False)]
    if not len(dup):
        return {}
    c = dup["close"].astype(float)
    g = pd.DataFrame({"ts": dup["ts"], "hi": c, "lo": c}).groupby("ts").agg(hi=("hi", "max"), lo=("lo", "min"))
    with np.errstate(divide="ignore", invalid="ignore"):
        diff = np.where(g["lo"] > 0, g["hi"] / g["lo"] - 1, np.inf)
    g = g.assign(diff=diff, d=g.index.date)
    out = {}
    for d, x in g.groupby("d", sort=True):
        med = float(np.median(x["diff"]))
        out[d] = (len(x), med, MIXED if med > MIXED_CLOSE_DIFF else DUPLICATE)
    return out


def keep_last(bars, days=None):
    """The bars with one row per timestamp, the last copy kept (answer order), sorted by ts; days: only these dates
    are reduced (None: every date)."""
    if not len(bars):
        return bars
    dup = bars["ts"].duplicated(keep="last")
    if days is not None:
        dup &= bars["ts"].dt.date.isin(set(days))
    return bars[~dup].sort_values("ts", kind="stable")


def window_bars(bars):
    """{date: bars inside WINDOW_FROM .. WINDOW_TO}."""
    if not len(bars):
        return {}
    hm = bars["ts"].dt.strftime("%H:%M")
    inside = bars[(hm >= WINDOW_FROM) & (hm <= WINDOW_TO)]
    return {d: int(k) for d, k in inside.groupby(inside["ts"].dt.date).size().items()}


def minute_answer_problems(bars, codes, code, special=()):
    """The guard of one minute answer (bars = icici_store.minute_frame of it, codes = the answer's stock_code values,
    None where absent): the problem texts, [] when the answer is fine. A day of the special-sessions calendar is not
    counted for TOO_MANY_BARS."""
    out = []
    other = sorted({str(x).strip() for x in codes if x is not None and str(x).strip()
                    and str(x).strip().upper() != str(code).strip().upper()})
    if other:
        out.append(f"{MIXED}: answer code {', '.join(other)} != {code}")
    for d, (n, med, kind) in dup_days(bars).items():
        if kind == MIXED:
            out.append(f"{MIXED}: {n} duplicated timestamps on {d} at another price level (median close "
                       f"difference {med:.0%})")
    for d, n in window_bars(bars).items():
        if n > MAX_DAY_BARS and d not in set(special):
            out.append(f"{TOO_MANY}: {n} bars on {d} in {WINDOW_FROM}-{WINDOW_TO} (at most {MAX_DAY_BARS})")
    return out


def day_candles_off(day_candles, bhav):
    """(off, compared, levels) of ICICI day candles (date, high, low, close) against bhavcopy rows (date, high, low):
    the bhav / ICICI high and low ratios measured and levelled as derive does (minute_download.levels); a candle is
    off when its high and low both miss its level by more than DAY_OFF_TOL. levels: the distinct levels (rounded)."""
    if bhav is None or not len(bhav) or not len(day_candles):
        return 0, 0, []
    dc = day_candles.drop_duplicates("date", keep="last")
    b = bhav.drop_duplicates("date", keep="last")[["date", "high", "low"]].rename(columns={"high": "bh", "low": "bl"})
    j = dc.sort_values("date", kind="stable").merge(b, on="date", how="inner")
    j = j[(j["high"] > 0) & (j["low"] > 0) & j["bh"].notna()]
    if not len(j):
        return 0, 0, []
    ih, il, ic_ = (j[x].to_numpy(float) for x in ("high", "low", "close"))
    rh, rl = j["bh"].to_numpy(float) / ih, j["bl"].to_numpy(float) / il
    with np.errstate(divide="ignore", invalid="ignore"):
        agree = np.abs(rh - rl) <= md.SCALE_AGREE + 2 * mp.HALF_TICK / ic_
    lv = md.levels(np.where(agree, (rh + rl) / 2, np.nan), md.SCALE_STEP)
    lv = np.ones(len(j)) if lv is None else lv
    off = (np.abs(rh / lv - 1) > DAY_OFF_TOL) & (np.abs(rl / lv - 1) > DAY_OFF_TOL)
    return int(off.sum()), len(j), [round(float(x), 4) for x in pd.unique(np.round(lv, 4))]


def day_mismatch(off, compared):
    return off >= DAY_OFF_MIN and compared > 0 and off / compared > DAY_OFF_SHARE


def day_answer_problems(day_candles, bhav):
    """The guard of one day-candle answer (icici_store.day_frame of it) against the stock's bhavcopy rows."""
    out = []
    n = int(day_candles["date"].duplicated().sum()) if len(day_candles) else 0
    if n:
        out.append(f"{DUPLICATE_DATES}: {n} duplicate day-candle date(s)")
    off, k, lv = day_candles_off(day_candles, bhav)
    if day_mismatch(off, k):
        out.append(f"{DAY_MISMATCH}: {off} of {k} day candles off the bhavcopy by more than {DAY_OFF_TOL:.0%} at "
                   f"level {', '.join(f'{x:g}' for x in lv)}")
    return out

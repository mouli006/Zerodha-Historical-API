"""Data audit, Dhan rolling (#254): what the Dhan "expired options" (rolling ATM +- n) folders hold, shown next to the
fno LOST rows of the same underlying and day. A flag only: it never adds a gap, never changes a reason, a kind or the
exit code, and a missing or unreadable folder is a report line, never a refusal.

Layout (written by the one-off #219 scripts outside the repo): <root>/<UNDERLYING>/exp-YYYY-MM-DD/
  by_strike.parquet  one row per minute and label: timestamp_ist, date, option_type (CE/PE), label (ATM, ATM+1 ...),
                     strike (the real strike of that minute), open high low close volume oi iv spot
  bhav_compare.csv   per day and 9-Nov-expiry contract: coverage, bhav_volume_contracts, ... (optional)
  bhav/BhavCopy_NSE_FO_0_0_0_YYYYMMDD_F_0000.csv.zip   the NSE F&O bhavcopy of that day (optional; all-expiry totals)

Read only, pandas / pyarrow / zipfile only: backfill/equity/ never imports F&O modules (tests/test_equity_imports.py),
so contract_name below is a copy of backfill/fno.py contract_name (a test compares the two). Details:
docs/dev/data_audit.md.
"""
import io
import os
import re
import zipfile
from datetime import date

import pandas as pd
import pyarrow.parquet as pq

FOLDER_RE = re.compile(r"exp-(\d{4}-\d{2}-\d{2})")
DATA = "by_strike.parquet"
COMPARE = "bhav_compare.csv"
BHAV_DIR = "bhav"
MONTHS = ("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC")
COLUMNS = ["date", "underlying", "expiry", "contract", "type", "strike", "coverage", "minutes", "bars", "first", "last",
           "labels", "bhav_volume", "lost_row", "folder"]


def contract_name(underlying, expiry, typ, strike):
    """NIFTY{strike}{DD}{MON}{YY}{CE|PE}, as backfill/fno.py contract_name names option files."""
    e = expiry if isinstance(expiry, date) else date.fromisoformat(str(expiry)[:10])
    k = float(strike)
    if not k.is_integer() or k <= 0:
        raise ValueError(f"strike {strike!r} is not a whole positive number")
    return f"{underlying}{int(k)}{e.day:02d}{MONTHS[e.month - 1]}{e.year % 100:02d}{typ}"


def default_root(fno_folder):
    """<parent of the F&O backtest folder>/dhan_rolling (D:\\Data\\fno\\backtest -> D:\\Data\\fno\\dhan_rolling)."""
    return os.path.join(os.path.dirname(os.path.normpath(fno_folder)), "dhan_rolling") if fno_folder else None


def expiry_folders(root):
    """[(underlying, expiry date, folder)] of <root>/<U>/exp-YYYY-MM-DD/ folders that hold by_strike.parquet."""
    out = []
    for u in sorted(os.listdir(root)):
        base = os.path.join(root, u)
        if not os.path.isdir(base):
            continue
        for n in sorted(os.listdir(base)):
            m = FOLDER_RE.fullmatch(n)
            p = os.path.join(base, n)
            if m and os.path.isfile(os.path.join(p, DATA)):
                out.append((u, date.fromisoformat(m.group(1)), p))
    return out


def bhav_zips(folder):
    """{'YYYY-MM-DD': zip path} of the folder's bhav/ copies."""
    b = os.path.join(folder, BHAV_DIR)
    out = {}
    if os.path.isdir(b):
        for n in sorted(os.listdir(b)):
            m = re.search(r"_(\d{8})_F_\d+\.csv\.zip$", n)
            if m:
                s = m.group(1)
                out[f"{s[:4]}-{s[4:6]}-{s[6:]}"] = os.path.join(b, n)
    return out


def input_files(root):
    """Every file the reader reads (for the before / after stat list); [] when the root is missing."""
    if not root or not os.path.isabs(root) or not os.path.isdir(root):
        return []
    files = []
    for _, _, p in expiry_folders(root):
        files.append(os.path.join(p, DATA))
        if os.path.isfile(os.path.join(p, COMPARE)):
            files.append(os.path.join(p, COMPARE))
        files += list(bhav_zips(p).values())
    return files


def stat(files):
    out = {}
    for p in files:
        try:
            s = os.stat(p)
            out[p] = (round(s.st_mtime, 6), s.st_size)
        except OSError:
            out[p] = None
    return out


def bhav_totals(path, underlying):
    """(options traded, futures traded) of the underlying in an NSE F&O bhavcopy zip (UDiFF columns)."""
    with zipfile.ZipFile(path) as z:
        df = pd.read_csv(io.BytesIO(z.read(z.namelist()[0])), dtype=str, keep_default_na=False)
    df = df[df["TckrSymb"] == underlying]
    vol = pd.to_numeric(df["TtlTradgVol"], errors="coerce").fillna(0)
    opt = df["OptnTp"].isin(["CE", "PE"])
    return int(((vol > 0) & opt).sum()), int(((vol > 0) & ~opt).sum())


def _hhmm(t):
    return f"{t:%H:%M}"


def read_folder(u, expiry, folder, lost_days):
    """(rows, per_day) of one expiry folder; per_day = {(u, day, expiry iso): counts}."""
    df = pq.read_table(os.path.join(folder, DATA),
                       columns=["timestamp_ist", "date", "option_type", "label", "strike"]).to_pandas()
    df["date"] = df["date"].astype(str).str[:10]
    cmp = None
    if os.path.isfile(os.path.join(folder, COMPARE)):
        cmp = pd.read_csv(os.path.join(folder, COMPARE), dtype=str, keep_default_na=False)
        cmp["strike_f"] = pd.to_numeric(cmp["strike"], errors="coerce")
        cmp["vol_f"] = pd.to_numeric(cmp["bhav_volume_contracts"], errors="coerce").fillna(0)
    zips = bhav_zips(folder)
    rows, per_day = [], {}
    name = os.path.basename(folder)
    for day, g in df.groupby("date"):
        session = g["timestamp_ist"].nunique()
        covered = {}
        for (typ, k), c in g.groupby(["option_type", "strike"]):
            n = c["timestamp_ist"].nunique()
            covered[(typ, float(k))] = {"minutes": n, "bars": len(c), "first": _hhmm(c["timestamp_ist"].min()),
                                        "last": _hhmm(c["timestamp_ist"].max()),
                                        "labels": " ".join(sorted(c["label"].astype(str).unique())),
                                        "coverage": "full" if n == session else "partly"}
        bhav_vol, traded = {}, None
        if cmp is not None:
            cd = cmp[cmp["date"].str[:10] == day]
            bhav_vol = {(t, float(k)): v for t, k, v in zip(cd["option_type"], cd["strike_f"], cd["vol_f"])}
            traded = {key for key, v in bhav_vol.items() if v > 0}
        lost = "yes" if (u, day) in lost_days else "no"
        for key in sorted(set(covered) | (traded or set())):
            c = covered.get(key, {"minutes": 0, "bars": 0, "first": "", "last": "", "labels": "", "coverage": "none"})
            v = bhav_vol.get(key)
            rows.append({"date": day, "underlying": u, "expiry": expiry.isoformat(),
                         "contract": contract_name(u, expiry, key[0], key[1]), "type": key[0], "strike": f"{key[1]:g}",
                         "coverage": c["coverage"], "minutes": c["minutes"], "bars": c["bars"], "first": c["first"],
                         "last": c["last"], "labels": c["labels"], "bhav_volume": "" if v is None else f"{v:g}",
                         "lost_row": lost, "folder": name})
        cov = {"full": 0, "partly": 0, "none": 0}
        for key in traded or ():
            cov[covered[key]["coverage"] if key in covered else "none"] += 1
        all_opt = all_fut = None
        if day in zips:
            all_opt, all_fut = bhav_totals(zips[day], u)
        hh = g["timestamp_ist"]
        per_day[(u, day, expiry.isoformat())] = {
            "contracts": len(covered), "ce": sum(1 for k in covered if k[0] == "CE"),
            "pe": sum(1 for k in covered if k[0] == "PE"), "bars": len(g), "minutes": session,
            "first": _hhmm(hh.min()), "last": _hhmm(hh.max()), "strike_min": float(g["strike"].min()),
            "strike_max": float(g["strike"].max()), "traded": None if traded is None else len(traded), **cov,
            "all_options": all_opt, "all_futures": all_fut, "lost_row": lost, "folder": folder}
    return rows, per_day


def read(root, lost_days):
    """{'root', 'found', 'folders', 'rows', 'per_day', 'notes'}; lost_days = {(underlying, 'YYYY-MM-DD')} of the fno
    LOST rows. Never raises: a missing root or an unreadable folder becomes a note."""
    res = {"root": root, "found": False, "folders": [], "rows": [], "per_day": {}, "notes": []}
    if not root:
        res["notes"].append("Dhan rolling: no folder (no --dhan-rolling and no F&O folder)")
        return res
    if not os.path.isabs(root):
        res["notes"].append(f"Dhan rolling NOT read: --dhan-rolling must be an absolute path (got {root!r})")
        return res
    if not os.path.isdir(root):
        res["notes"].append(f"Dhan rolling folder not found: {root}")
        return res
    res["found"] = True
    try:
        folders = expiry_folders(root)
    except Exception as e:                       # flag only: never an error of the audit
        res["notes"].append(f"Dhan rolling NOT read: {root}: {type(e).__name__}: {e}")
        return res
    for u, expiry, folder in folders:
        res["folders"].append(folder)
        try:
            rows, per_day = read_folder(u, expiry, folder, lost_days)
        except Exception as e:
            res["notes"].append(f"Dhan rolling NOT read: {folder}: {type(e).__name__}: {e}")
            continue
        res["rows"] += rows
        res["per_day"].update(per_day)
    for (u, day, _), c in sorted(res["per_day"].items()):
        if c["lost_row"] != "yes":
            res["notes"].append(f"rolling day with no fno LOST row: {u} {day} (info)")
    return res


def evidence(c, expiry):
    traded = f"full {c['full']}, partly {c['partly']} of {c['traded']}" if c["traded"] is not None else "bhav -"
    return (f"Dhan rolling exp {expiry}: {c['contracts']} contracts, {c['bars']:,} bars (traded of that expiry: "
            f"{traded}); fno_dhan_rolling.csv")


def attach(gaps, res):
    """Adds the rolling text to the evidence of every fno LOST gap with rolling data on that underlying and day; the
    reason, kind and clears_when stay as they are. Returns the number of gaps touched."""
    n = 0
    for g in gaps:
        if g["store"] != "fno" or g["reason"] != "LOST":
            continue
        day = str(g["date"])
        texts = [evidence(c, exp) for (u, d, exp), c in sorted(res["per_day"].items())
                 if u == g["underlying"] and d == day]
        if texts:
            g["evidence"] = " | ".join([g["evidence"]] + texts) if g["evidence"] else " | ".join(texts)
            n += 1
    return n


def report_lines(res, changed=()):
    out = [f"  Dhan rolling (#254, flag only, {res['root']}): {len(res['folders'])} expiry folder"
           f"{'s' if len(res['folders']) != 1 else ''}"]
    for (u, day, exp), c in sorted(res["per_day"].items()):
        out.append(f"    {u} {day} exp {exp}: {c['contracts']} contracts (CE {c['ce']}, PE {c['pe']}), {c['bars']:,} bars, "
                   f"{c['minutes']} minutes {c['first']}-{c['last']}, strikes {c['strike_min']:g}..{c['strike_max']:g};")
        traded = (f"bhavcopy traded of this expiry {c['traded']}: full {c['full']}, partly {c['partly']}, "
                  f"none {c['none']}" if c["traded"] is not None else "bhavcopy traded of this expiry -")
        if c["all_options"] is None:
            alltr = "all traded -"
        else:
            alltr = (f"all traded {c['all_options'] + c['all_futures']:,} (options {c['all_options']:,} + futures "
                     f"{c['all_futures']:,}): rolling {c['contracts']}")
        out.append(f"      {traded}; {alltr}")
    out += [f"    {x}" for x in res["notes"]]
    if changed:
        out.append(f"    Dhan rolling files changed during the run (counts may be off): {'; '.join(changed)}")
    return out


def frame(res):
    return pd.DataFrame(res["rows"], columns=COLUMNS)


def info(res, before, after, changed):
    return {"root": res["root"], "found": res["found"], "folders": res["folders"], "rows": len(res["rows"]),
            "per_day": {f"{u} {d} exp {e}": {k: v for k, v in c.items() if k != "folder"}
                        for (u, d, e), c in sorted(res["per_day"].items())},
            "notes": res["notes"],
            "files": {p: {"mtime": s[0] if s else None, "size": s[1] if s else None} for p, s in before.items()},
            "changed": list(changed)}

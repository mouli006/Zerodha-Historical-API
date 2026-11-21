"""Data audit, F&O contracts (#253): which contracts the NSE / BSE F&O bhavcopy traded but a raw store lacks.

Read only. Reads the bhav_compare.csv that the bhavcopy check (backfill/bhav.py check_saved, "Check all") saved in
every raw day folder (<raw>/<variant>_zero_volume/<UNDERLYING>_FnO_1min[_Dhan]/<day folder>/bhavcopy/), never the
network and never the F&O code (backfill/equity/ never imports F&O modules, tests/test_equity_imports.py).

Status words of bhav_compare.csv (also on the Dhan stores, whose column is still called Kite_volume):
MISSING_IN_KITE = traded in the bhavcopy, no file in the store; ZERO_VOLUME_IN_KITE = a file with volume 0;
BAD_NAME (...) = a bhavcopy row that could not be named. Details: docs/dev/data_audit.md.
"""
import os
import re

import pandas as pd

DAY_RE = re.compile(r"(\d{4}-\d{2}-\d{2})(?:_exp-\d{4}-\d{2}-\d{2}_(?:withZeroVol|noZeroVol)_dl-\d{4}-\d{2}-\d{2}_\d{4})?")
COMPARE = ("bhavcopy", "bhav_compare.csv")
MISSING = "MISSING_IN_KITE"
ZERO = "ZERO_VOLUME_IN_KITE"
EXTRA = "EXTRA_IN_KITE"
BAD = "BAD_NAME"
ZERO_DROPPED = "ZERO_VOLUME_DROPPED"      # a without store lacks it; the same source's with store has it at volume 0
NOT_DOWNLOADED = "NOT_DOWNLOADED"         # any other missing contract
OTHER = {"kite": "dhan", "dhan": "kite"}
LIST_COLUMNS = ["date", "underlying", "variant", "source", "contract", "expiry", "bucket", "type", "strike",
                "bhav_volume", "bhav_oi", "status", "cause", "other_source_has", "best_source", "day_folder"]
SUMMARY_COLUMNS = ["date", "underlying", "variant", "source", "bhav_traded", "traded_files", "extra_files", "files",
                   "missing", "zero_volume_dropped", "not_downloaded", "bad_names", "other_source_has", "best_source",
                   "adds_up"]


class DayFolderError(Exception):
    """A raw store has two folders for one trading day."""


def day_folders(store):
    """{trading day (YYYY-MM-DD): folder name} of a raw store; DayFolderError when a day has two folders."""
    found = {}
    if not os.path.isdir(store):
        return {}
    for n in sorted(os.listdir(store)):
        m = DAY_RE.fullmatch(n)
        if m and os.path.isdir(os.path.join(store, n)):
            found.setdefault(m.group(1), []).append(n)
    dups = ["; ".join(ns) for ns in found.values() if len(ns) > 1]
    if dups:
        raise DayFolderError(f"more than one folder for one trading day in {store}: " + " | ".join(dups))
    return {day: ns[0] for day, ns in found.items()}


def store_days(raw_folders):
    """{(source, variant, underlying, day): day folder path} of every raw store (raw_folders as audit.raw_folders)."""
    out = {}
    for (s, v, u), store in raw_folders.items():
        for day, name in day_folders(store).items():
            out[(s, v, u, day)] = os.path.join(store, name)
    return out


def compare_path(day_dir):
    return os.path.join(day_dir, *COMPARE)


def contract_files(day_dir):
    """Number of contract files (.csv in futures/ and options/), as the bhavcopy check counts them."""
    n = 0
    for sub in ("futures", "options"):
        p = os.path.join(day_dir, sub)
        if os.path.isdir(p):
            n += sum(1 for e in os.scandir(p) if e.is_file() and e.name.endswith(".csv"))
    return n


def _num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return 0.0


def read_compare(path):
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def check(days, best):
    """The per-contract check.

    days: store_days(); best: {(variant, underlying, day): best_source} from the backtest days.csv.
    Returns (missing list rows, summary rows, problems, best_missing):
    problems = [(source, variant, underlying, day, evidence)] for a day folder without bhav_compare.csv or whose
    compare file lists another number of files than the folder holds (stale);
    best_missing = {(variant, underlying, day, source): [contract, ...]} missing in that day's best source."""
    frames, problems = {}, []
    for key, day_dir in sorted(days.items()):
        p = compare_path(day_dir)
        if not os.path.isfile(p):
            problems.append(key + (f"no bhav_compare.csv in {day_dir}",))
            continue
        df = read_compare(p)
        for c in ("Contract", "Status", "Bhav_volume", "Kite_volume"):
            if c not in df.columns:
                df[c] = ""
        listed = int((df["Kite_volume"] != "").sum())
        on_disk = contract_files(day_dir)
        if listed != on_disk:
            problems.append(key + (f"bhav_compare.csv stale: lists {listed} files, folder has {on_disk} ({day_dir})",))
        frames[key] = df

    def has_file(key, contract):
        df = frames.get(key)
        if df is None:
            return ""
        r = df[df["Contract"] == contract]
        return "yes" if len(r) and (r["Kite_volume"] != "").any() else "no"

    def zero_in_with(s, u, day, contract):
        df = frames.get((s, "with", u, day))
        if df is None:
            return False
        r = df[df["Contract"] == contract]
        return bool(len(r)) and any(ZERO in x for x in r["Status"])

    rows, summary, best_missing = [], [], {}
    for (s, v, u, day), df in sorted(frames.items()):
        status = df["Status"]
        bad = status.str.startswith(BAD)
        miss = status.str.contains(MISSING, regex=False) & ~bad
        has = df["Kite_volume"] != ""
        traded = (df["Bhav_volume"].map(_num) > 0) & ~bad
        b = best.get((v, u, day), "")
        other = (OTHER[s], v, u, day)
        counts = {ZERO_DROPPED: 0, NOT_DOWNLOADED: 0, BAD: 0, "other": 0}
        for r in df[miss | bad].to_dict("records"):
            if r["Status"].startswith(BAD):
                cause = BAD
            elif v == "without" and zero_in_with(s, u, day, r["Contract"]):
                cause = ZERO_DROPPED
            else:
                cause = NOT_DOWNLOADED
            o = has_file(other, r["Contract"])
            counts[cause] += 1
            counts["other"] += o == "yes"
            rows.append({"date": day, "underlying": u, "variant": v, "source": s, "contract": r["Contract"],
                         "expiry": r.get("Expiry", ""), "bucket": r.get("Bucket", ""), "type": r.get("Type", ""),
                         "strike": r.get("Strike", ""), "bhav_volume": r.get("Bhav_volume", ""),
                         "bhav_oi": r.get("Bhav_OI", ""), "status": r["Status"], "cause": cause,
                         "other_source_has": o, "best_source": b, "day_folder": os.path.basename(days[(s, v, u, day)])})
            if cause != BAD and b == s:
                best_missing.setdefault((v, u, day, s), []).append(r["Contract"])
        n_traded, n_tf, n_miss = int(traded.sum()), int((traded & has).sum()), int(miss.sum())
        summary.append({"date": day, "underlying": u, "variant": v, "source": s, "bhav_traded": n_traded,
                        "traded_files": n_tf, "extra_files": int(status.str.contains(EXTRA, regex=False).sum()),
                        "files": int(has.sum()), "missing": n_miss, "zero_volume_dropped": counts[ZERO_DROPPED],
                        "not_downloaded": counts[NOT_DOWNLOADED], "bad_names": counts[BAD],
                        "other_source_has": counts["other"], "best_source": b,
                        "adds_up": n_tf + n_miss == n_traded})
    return rows, summary, problems, best_missing


def missing_in_both(rows):
    """Contracts (date, underlying, variant, contract) missing in both sources of the same store variant."""
    by = {}
    for r in rows:
        if r["cause"] != BAD:
            by.setdefault((r["date"], r["underlying"], r["variant"], r["contract"]), set()).add(r["source"])
    return sorted(k for k, v in by.items() if v == {"kite", "dhan"})

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

# #265 price flags (D234 flag only, D235 limit): the stored last price (column Kite_last = close of the day's last
# bar, also on the Dhan stores) against the bhavcopy LAST (Bhav_last). Own constants: backfill/equity/ may not import
# backfill.bhav (tests/test_equity_imports.py).
TICK = 0.05
PRICE_PCT = 0.05                          # 5 % of Bhav_last
PRICE_FLOOR = 0.20                        # Rs 0.20 = 4 ticks, the cheap-option floor
PRICE_FIELDS = ("Bhav_last", "Kite_last")
PRICE_COLUMNS = ["date", "underlying", "variant", "source", "contract", "expiry", "bucket", "type", "strike",
                 "bhav_volume", "store_volume", "bhav_last", "store_last", "store_last_regular", "last_bar",
                 "bhav_close", "bhav_settle", "diff", "diff_pct", "other_source_flagged", "best_source", "day_folder"]


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


def _price(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def price_flags(days, best, pct=PRICE_PCT, floor=PRICE_FLOOR):
    """#265: contracts whose stored last price is off the bhavcopy LAST by more than max(pct x Bhav_last, floor).

    A flag only (D234): never a gap. Reads the same bhav_compare.csv files as check() (a missing file is already a
    problem there); a file without the price columns is counted in no_price_columns, never an error. Skipped rows:
    BAD_NAME, no stored file, Bhav_volume 0, Bhav_last 0 or empty, no stored last price.
    Returns (rows in PRICE_COLUMNS, info)."""
    compared, flagged, no_cols = {}, {}, 0
    for key, day_dir in sorted(days.items()):
        p = compare_path(day_dir)
        if not os.path.isfile(p):
            continue
        df = read_compare(p)
        if not all(c in df.columns for c in PRICE_FIELDS):
            no_cols += 1
            continue
        seen, hits = {}, []
        for r in df.to_dict("records"):
            if str(r.get("Status", "")).startswith(BAD) or r.get("Kite_volume", "") == "":
                continue
            bl, sl = _price(r["Bhav_last"]), _price(r["Kite_last"])
            if _num(r.get("Bhav_volume")) <= 0 or bl is None or bl <= 0 or sl is None:
                continue
            diff = round(abs(sl - bl), 6)
            flag = diff > round(max(pct * bl, floor), 6)
            seen[r.get("Contract", "")] = flag
            if flag:
                hits.append((r, diff, bl))
        compared[key] = seen
        flagged[key] = hits

    rows, per = [], {}
    for (s, v, u, day), hits in sorted(flagged.items()):
        c = per.setdefault((u, v, s), {"flags": 0, "compared": 0})
        c["compared"] += len(compared[(s, v, u, day)])
        c["flags"] += len(hits)
        other = compared.get((OTHER[s], v, u, day))
        for r, diff, bl in hits:
            name = r.get("Contract", "")
            o = "" if other is None or name not in other else ("yes" if other[name] else "no")
            rows.append({"date": day, "underlying": u, "variant": v, "source": s, "contract": name,
                         "expiry": r.get("Expiry", ""), "bucket": r.get("Bucket", ""), "type": r.get("Type", ""),
                         "strike": r.get("Strike", ""), "bhav_volume": r.get("Bhav_volume", ""),
                         "store_volume": r.get("Kite_volume", ""), "bhav_last": r.get("Bhav_last", ""),
                         "store_last": r.get("Kite_last", ""), "store_last_regular": r.get("Kite_last_regular", ""),
                         "last_bar": r.get("Last_bar", ""), "bhav_close": r.get("Bhav_close", ""),
                         "bhav_settle": r.get("Bhav_settle", ""), "diff": round(diff, 2),
                         "diff_pct": round(diff / bl * 100, 2), "other_source_flagged": o,
                         "best_source": best.get((v, u, day), ""), "day_folder": os.path.basename(days[(s, v, u, day)])})
    both = {(r["date"], r["underlying"], r["variant"], r["contract"]) for r in rows if r["other_source_flagged"] == "yes"}
    info = {"limit_pct": round(pct * 100, 4), "limit_ticks": round(floor / TICK),
            "compared": sum(map(len, compared.values())), "flags": len(rows),
            "store_days": sum(1 for h in flagged.values() if h),
            "best_flags": sum(1 for r in rows if r["source"] == r["best_source"]), "both": len(both),
            "no_price_columns": no_cols, "per_store": per}
    return rows, info


def missing_in_both(rows):
    """Contracts (date, underlying, variant, contract) missing in both sources of the same store variant."""
    by = {}
    for r in rows:
        if r["cause"] != BAD:
            by.setdefault((r["date"], r["underlying"], r["variant"], r["contract"]), set()).add(r["source"])
    return sorted(k for k, v in by.items() if v == {"kite", "dhan"})

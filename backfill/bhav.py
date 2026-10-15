"""NSE / BSE F&O bhavcopy (UDiFF): download it for a day and compare it with the Kite F&O files saved by
backfill/fno.py (NIFTY against NSE's, SENSEX against BSE's; fno.EXCHANGE_OF). Run after each Kite day,
or by hand (nifty_bhav.py or the dashboard); the bhavcopy stays on the exchange's archive.

Verified format (2026-10-09): one CSV in the zip, header in BHAV_HEADER below, dates as YYYY-MM-DD,
strikes as "21450.00". TtlTradgVol is in CONTRACTS, OpnIntrst in SHARES (Kite has shares for both),
so the units are detected separately for volume and OI (detect_unit).
BSE (verified 2026-10-13): a plain CSV (not a zip) with the same header, Src BSE, TckrSymb SENSEX
(BANKEX rows too), both TtlTradgVol and OpnIntrst in SHARES; a day that is not published yet comes
back as 200 with BSE's HTML page instead of a 404."""
import csv
import io
import json
import os
import re
import statistics
import time
import zipfile
from datetime import date, timedelta

import requests

from . import fno

BHAV_URL = "https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_{d}_F_0000.csv.zip"
BSE_BHAV_URL = "https://www.bseindia.com/download/Bhavcopy/Derivative/BhavCopy_BSE_FO_0_0_0_{d}_F_0000.CSV"
# Exchange -> where its F&O bhavcopy is: the URL, a home page fetched first for cookies (NSE answers
# scripts with 403 there; ignored), the Referer, whether the file is a zip and the unit of TtlTradgVol
SOURCES = {"NSE": {"url": BHAV_URL, "home": "https://www.nseindia.com/", "referer": "https://www.nseindia.com/",
                   "zipped": True, "volume": "contracts"},
           "BSE": {"url": BSE_BHAV_URL, "home": None, "referer": "https://www.bseindia.com/", "zipped": False,
                   "volume": "shares"}}
BHAV_HEADER = ["TradDt", "BizDt", "Sgmt", "Src", "FinInstrmTp", "FinInstrmId", "ISIN", "TckrSymb", "SctySrs",
               "XpryDt", "FininstrmActlXpryDt", "StrkPric", "OptnTp", "FinInstrmNm", "OpnPric", "HghPric",
               "LwPric", "ClsPric", "LastPric", "PrvsClsgPric", "UndrlygPric", "SttlmPric", "OpnIntrst",
               "ChngInOpnIntrst", "TtlTradgVol", "TtlTrfVal", "TtlNbOfTxsExctd", "SsnId", "NewBrdLotQty",
               "Rmks", "Rsvd1", "Rsvd2", "Rsvd3", "Rsvd4"]
BHAV_TYPES = {"IDF": "FUT", "IDO": None}   # index future / index option (CE or PE from OptnTp)
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                         "Chrome/126.0 Safari/537.36",
           "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
           "Accept-Language": "en-US,en;q=0.9"}
TICK = 0.05
# Volume: flagged only above 1% AND above 5 lots. OI: 3% (Kite's OI is an intraday snapshot, the
# bhavcopy's is the official end of day). Price: max(0.5%, 2 ticks) against Kite's last bar of the day.
VOLUME_TOL, VOLUME_MIN_LOTS, OI_TOL, PRICE_TOL = 0.01, 5, 0.03, 0.005
REGULAR_END = "15:30:00"   # Kite labels candles by start time: the 15:29 candle is the last regular one
LATE_BAR = "15:40:00"      # a last bar before this may miss trades the exchange counted
TRIES = 3
COMPARE_COLUMNS = ["Contract", "Expiry", "Bucket", "Type", "Strike", "Bhav_volume", "Kite_volume", "Bhav_OI", "Kite_OI",
                   "Bhav_last", "Kite_last", "Kite_last_regular", "Last_bar", "Bhav_close", "Bhav_settle",
                   "Status", "Note"]
COUNT_NAMES = [("missing", "missing"), ("extra", "extra"), ("zero_volume", "zero-volume in Kite"),
               ("volume_diffs", "volume diffs"),
               ("oi_diffs", "OI diffs"), ("price_diffs", "price diffs"), ("bad_names", "bad names")]
PENDING = "Bhavcopy: pending"
# Written next to bhav_compare.csv by every check
SUMMARY_FILE = "bhav_summary.csv"
DAYS_SUMMARY_FILE = "checked_days_summary.csv"   # the checked day's row of the "All days summary (CSV)", saved by every check
SUMMARY_FILE_COLUMNS = ["Expiry", "Bucket", "Type", "Bhav_contracts", "Bhav_traded", "Kite_files", "OK", "Missing",
                        "Extra", "Zero_volume", "Volume_diffs", "OI_diffs", "Price_diffs",
                        "Date", "Status", "Volume_unit", "OI_unit"]
SUMMARY_FLAGS = {"Missing": "MISSING_IN_KITE", "Extra": "EXTRA_IN_KITE", "Zero_volume": "ZERO_VOLUME_IN_KITE",
                 "Volume_diffs": "VOLUME_DIFF", "OI_diffs": "OI_DIFF", "Price_diffs": "PRICE_DIFF"}
CHECK_BUDGET = 60   # seconds: a Kite run waits at most this long on NSE/BSE for the day's bhavcopy
FAR = "Far"   # every other bucket (Current week, Next week, Current month, Next month) counts as liquid


class BhavNotAvailable(Exception):
    """404: not published yet, or no trading that day."""


class BhavError(Exception):
    """Download blocked or failed, or a file that is not this day's bhavcopy."""


class BhavTimeout(BhavError):
    """fetch_bhav(budget=...) ran out of time."""


TRIM_BUDGET = 30   # seconds: a run with the bhavcopy trim waits at most this long on NSE/BSE before using the full list


# --- getting the file ---------------------------------------------------------------------------

def bhav_url(run_date, underlying="NIFTY"):
    """The F&O bhavcopy URL for run_date on the underlying's exchange."""
    return SOURCES[fno.exchange_name(underlying)]["url"].format(d=f"{run_date:%Y%m%d}")


def fetch_bhav(run_date, log=print, session=None, sleep=time.sleep, budget=None, clock=time.monotonic,
               underlying="NIFTY"):
    """(zip or CSV bytes, url) from the underlying's exchange. NSE's home page often answers scripts
    with 403; that is ignored, the archive URL works without its cookies. Raises BhavNotAvailable on 404
    (or BSE's HTML page for a day it has not published), BhavError after TRIES failures.
    budget (seconds) caps everything (home page, tries, waits); BhavTimeout when it runs out."""
    exch = fno.exchange_name(underlying)
    src = SOURCES[exch]
    url = bhav_url(run_date, underlying)
    end = None if budget is None else clock() + budget

    def left(cap):
        if end is None:
            return cap
        rest = end - clock()
        if rest <= 0:
            raise BhavTimeout(f"bhavcopy fetch timed out after {budget:g} s")
        return min(cap, rest)

    s = session or requests.Session()
    s.headers.update({**HEADERS, "Referer": src["referer"]})
    if src["home"]:
        try:
            s.get(src["home"], timeout=left(15 if end is None else 5))
        except requests.RequestException:
            pass
    gone = f"{exch} has no F&O bhavcopy for {run_date} yet (404: not published, or no trading)."
    err = ""
    for attempt in range(1, TRIES + 1):
        try:
            r = s.get(url, timeout=left(30))
            if r.status_code == 404:
                raise BhavNotAvailable(gone)
            if r.ok and src["zipped"] and r.content[:2] == b"PK":
                return r.content, url
            if r.ok and not src["zipped"]:
                if r.content.lstrip().lstrip(b"\xef\xbb\xbf")[:6] == b"TradDt":
                    return r.content, url
                if r.content.lstrip()[:1] == b"<":   # BSE serves its web page for a day it has not published
                    raise BhavNotAvailable(gone.replace("404", "no file"))
            err = (f"HTTP {r.status_code}" if not r.ok
                   else f"{exch} answered with a page instead of the {'zip' if src['zipped'] else 'CSV'}")
        except requests.RequestException as e:
            err = f"{type(e).__name__}: {e}"
        log(f"Bhavcopy download try {attempt} failed: {err}")
        if attempt < TRIES:
            sleep(left(2 ** attempt))
    raise BhavError(f"Could not download the bhavcopy ({err}). Download it in a browser from {url} "
                    "and pass it with --bhav-file.")


def read_bhav(data):
    """(header, rows) from zip bytes or CSV bytes."""
    if data[:2] == b"PK":
        z = zipfile.ZipFile(io.BytesIO(data))
        names = [n for n in z.namelist() if n.lower().endswith(".csv")]
        if not names:
            raise BhavError("The zip has no CSV file.")
        data = z.read(names[0])
    reader = csv.DictReader(io.StringIO(data.decode("utf-8-sig")))
    rows = list(reader)
    missing = [c for c in ("TradDt", "FinInstrmTp", "TckrSymb", "XpryDt", "StrkPric", "OptnTp", "ClsPric",
                           "LastPric", "SttlmPric", "OpnIntrst", "TtlTradgVol", "NewBrdLotQty")
               if c not in (reader.fieldnames or [])]
    if missing:
        raise BhavError(f"Not an NSE/BSE F&O UDiFF bhavcopy: columns missing: {', '.join(missing)}")
    return reader.fieldnames, rows


def num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def parse_bhav(rows, underlying="NIFTY"):
    """The underlying's index futures and options as dicts with our contract name."""
    out = []
    for r in rows:
        if r["TckrSymb"] != underlying or r["FinInstrmTp"] not in BHAV_TYPES:
            continue
        kind = BHAV_TYPES[r["FinInstrmTp"]] or r["OptnTp"]
        item = {"expiry": fno.as_date(r["XpryDt"]), "type": kind,
                "strike": "" if kind == "FUT" else num(r["StrkPric"]),
                "volume": num(r["TtlTradgVol"]), "oi": num(r["OpnIntrst"]), "last": num(r["LastPric"]),
                "close": num(r["ClsPric"]), "settle": num(r["SttlmPric"]), "lot": num(r["NewBrdLotQty"]),
                "nse_name": r.get("FinInstrmNm", "")}
        item["traded"] = item["volume"] > 0
        try:
            item["name"], item["error"] = fno.contract_name(kind, r["XpryDt"], r["StrkPric"], underlying), ""
        except ValueError as e:
            item["name"], item["error"] = item["nse_name"] or f"{kind} {r['XpryDt']} {r['StrkPric']}", str(e)
        out.append(item)
    return out


# --- comparing ----------------------------------------------------------------------------------

def kite_stats(path):
    """Total volume, last OI, close and start time of the day's last bar, and the regular-session close
    (last bar starting before 15:30) of a saved day file."""
    bars = fno.read_bars(path) or []
    regular = [b for b in bars if b["Time"] < REGULAR_END]
    return {"volume": sum(num(b["Volume"]) for b in bars), "oi": num(bars[-1]["OI"]) if bars else 0.0,
            "last": num(bars[-1]["Close"]) if bars else None, "last_bar": bars[-1]["Time"] if bars else "",
            "last_regular": num(regular[-1]["Close"]) if regular else None}


def detect_unit(pairs, lot, default):
    """Kite/bhav multiplier: the median ratio over futures, snapped to 1 or the lot size."""
    ratios = [k / b for k, b in pairs if k > 0 and b > 0]
    if not ratios or lot <= 1:
        return default
    m = statistics.median(ratios)
    return lot if abs(m / lot - 1) < abs(m - 1) else 1


def disk_files(day_dir):
    files = {}
    for sub in ("futures", "options"):
        folder = os.path.join(day_dir, sub)
        if os.path.isdir(folder):
            for n in os.listdir(folder):
                if n.endswith(".csv"):
                    files[n[:-4]] = os.path.join(folder, n)
    return files


def off(a, b, tol):
    return abs(a - b) > tol * max(abs(b), 1)


def parse_name(name, underlying="NIFTY"):
    """(expiry, type, strike) from one of our file names, e.g. NIFTY2500013OCT26CE -> (2026-10-13, CE, 25000)
    and NIFTY27OCT26 -> (2026-10-27, FUT, ""); None if the name doesn't follow the format."""
    m = re.fullmatch(rf"{re.escape(underlying)}(\d+?)?(\d{{2}})([A-Z]{{3}})(\d{{2}})(CE|PE)?", name)
    if not m or m.group(3) not in fno.MONTHS or bool(m.group(1)) != bool(m.group(5)):
        return None
    try:
        expiry = date(2000 + int(m.group(4)), fno.MONTHS.index(m.group(3)) + 1, int(m.group(2)))
    except ValueError:
        return None
    return expiry, m.group(5) or "FUT", float(m.group(1)) if m.group(1) else ""


def bucket_namer(items, names, underlying="NIFTY"):
    """bucket(type, expiry) for the day, from the expiries in the bhavcopy and in the file names on disk.
    Options use fno.bucket_labels (Current week, Next week, Current month, Next month, two labels when
    expiries coincide); futures are Current month / Next month by expiry order; the rest are Far."""
    parsed = [x for x in (parse_name(n, underlying) for n in names) if x]
    opt = {i["expiry"] for i in items if i["type"] != "FUT"} | {e for e, t, _ in parsed if t != "FUT"}
    fut = sorted({i["expiry"] for i in items if i["type"] == "FUT"} | {e for e, t, _ in parsed if t == "FUT"})
    opt_labels = fno.bucket_labels(opt, fut)
    fut_labels = dict(zip(fut, ("Current month", "Next month")))
    return lambda kind, expiry: (fut_labels if kind == "FUT" else opt_labels).get(expiry, FAR) if expiry else FAR


def compare(bhav_items, day_dir, underlying="NIFTY"):
    """(rows in COMPARE_COLUMNS, counts, units) for every bhavcopy contract and every file on disk.
    Every row gets its expiry Bucket; counts add liquid_missing / liquid_diffs (problems outside Far)
    and far_problems. Grouping only: every difference is still a row and in the totals."""
    files = disk_files(day_dir)
    bucket = bucket_namer(bhav_items, files, underlying)
    bhav = {i["name"]: i for i in bhav_items if not i["error"]}
    stats = {n: kite_stats(p) for n, p in files.items()}
    futs = [(stats[n], b) for n, b in bhav.items() if b["type"] == "FUT" and n in stats and b["traded"]]
    lot = int(max((b["lot"] for b in bhav.values() if b["type"] == "FUT"), default=0))
    shares = SOURCES[fno.exchange_name(underlying)]["volume"] == "shares"   # BSE: no lot multiplier by default
    v_unit = detect_unit([(s["volume"], b["volume"]) for s, b in futs], lot, 1 if shares else lot or 1)
    oi_unit = detect_unit([(s["oi"], b["oi"]) for s, b in futs], lot, 1)
    counts = {k: 0 for k, _ in COUNT_NAMES}
    counts.update(bhav_traded=sum(1 for b in bhav.values() if b["traded"]), kite_files=len(files),
                  liquid_missing=0, liquid_diffs=0, liquid_volume_diffs=0, far_problems=0, red_volume_diffs=0)

    def tally(row, missing):
        if row["Status"] != "OK":
            key = "far_problems" if row["Bucket"] == FAR else "liquid_missing" if missing else "liquid_diffs"
            counts[key] += 1
            if row["Bucket"] != FAR and "VOLUME_DIFF" in row["Status"]:
                counts["liquid_volume_diffs"] += 1

    rows = []
    for i in bhav_items:
        if i["error"]:
            counts["bad_names"] += 1
            rows.append({"Contract": i["name"], "Expiry": i["expiry"].isoformat(), "Bucket": bucket(i["type"], i["expiry"]),
                         "Type": i["type"], "Strike": fno.fmt_num(i["strike"]), "Bhav_volume": fno.fmt_num(i["volume"]),
                         "Status": f"BAD_NAME ({i['error']})"})
            tally(rows[-1], False)
    for name in sorted(set(bhav) | set(files)):
        b, s = bhav.get(name), stats.get(name)
        flags, note = [], ""
        if b and b["traded"] and not s:
            flags.append("MISSING_IN_KITE")
            counts["missing"] += 1
        if s and not (b and b["traded"]):
            flags.append("EXTRA_IN_KITE")
            counts["extra"] += 1
        if b and b["traded"] and s:
            # Kite can report a contract that traded once as candles with volume 0 (a kept zero-volume
            # file): a volume difference, not a missing contract; VOLUME_DIFF too when over the limits
            if s["volume"] == 0:
                flags.append("ZERO_VOLUME_IN_KITE")
                counts["zero_volume"] += 1
            gap = abs(s["volume"] - b["volume"] * v_unit)
            if gap > VOLUME_TOL * max(b["volume"] * v_unit, 1) and gap > VOLUME_MIN_LOTS * (b["lot"] or lot or 1):
                flags.append("VOLUME_DIFF")
                counts["volume_diffs"] += 1
                if s["volume"] > 0:   # with Kite volume 0 it is the zero-volume limitation: amber
                    counts["red_volume_diffs"] += 1
            if (s["oi"] or b["oi"]) and off(s["oi"], b["oi"] * oi_unit, OI_TOL):
                flags.append("OI_DIFF")
                counts["oi_diffs"] += 1
            if b["last"] > 0 and s["last"] is not None:
                if abs(s["last"] - b["last"]) > max(PRICE_TOL * b["last"], 2 * TICK) + 1e-9:
                    flags.append("PRICE_DIFF")
                    counts["price_diffs"] += 1
                    if s["last_bar"] < LATE_BAR:
                        note = "trades after Kite's last bar"
        parsed = None if b else parse_name(name, underlying)   # files the bhavcopy doesn't list
        expiry = b["expiry"] if b else parsed[0] if parsed else None
        kind = b["type"] if b else parsed[1] if parsed else ("FUT" if not name.endswith(("CE", "PE")) else name[-2:])
        strike = b["strike"] if b else parsed[2] if parsed else ""
        rows.append({
            "Contract": name, "Expiry": expiry.isoformat() if expiry else "", "Bucket": bucket(kind, expiry),
            "Type": kind, "Strike": fno.fmt_num(strike) if strike != "" else "",
            "Bhav_volume": fno.fmt_num(b["volume"]) if b else "", "Kite_volume": fno.fmt_num(s["volume"]) if s else "",
            "Bhav_OI": fno.fmt_num(b["oi"]) if b else "", "Kite_OI": fno.fmt_num(s["oi"]) if s else "",
            "Bhav_last": fno.fmt_num(b["last"]) if b else "",
            "Kite_last": fno.fmt_num(s["last"]) if s and s["last"] is not None else "",
            "Kite_last_regular": fno.fmt_num(s["last_regular"]) if s and s["last_regular"] is not None else "",
            "Last_bar": s["last_bar"] if s else "", "Bhav_close": fno.fmt_num(b["close"]) if b else "",
            "Bhav_settle": fno.fmt_num(b["settle"]) if b else "", "Status": "+".join(flags) or "OK", "Note": note})
        tally(rows[-1], "MISSING_IN_KITE" in flags)
    return rows, counts, {"volume": v_unit, "oi": oi_unit}


def check_status(counts):
    """One line: all differences, then the liquid buckets (everything but Far) when there are any."""
    bad = [f"{counts[k]} {label}" for k, label in COUNT_NAMES if counts.get(k)]
    if not bad:
        return "Bhavcopy check: OK"
    line = f"Bhavcopy check: CHECK ({', '.join(bad)})"
    if "liquid_missing" in counts:
        line += f"; liquid buckets: {counts['liquid_missing']} missing, {counts['liquid_diffs']} diffs"
    return line


def severity(counts, status="diffs"):
    """"major" (red) only for MISSING_IN_KITE in any bucket, or a VOLUME_DIFF over the limits on a contract
    whose Kite volume is above 0. Everything else is a known Kite limitation and "minor" (amber), in any
    bucket: ZERO_VOLUME_IN_KITE, EXTRA_IN_KITE, OI_DIFF, PRICE_DIFF. "" when OK. Checks saved before
    red_volume_diffs existed use volume_diffs (zero-volume files were never written then)."""
    if status != "diffs":
        return ""
    volume = counts["red_volume_diffs"] if "red_volume_diffs" in counts else counts.get("volume_diffs", 0)
    return "major" if counts.get("missing", 0) + volume else "minor"


OLD_SEVERITY = {"liquid": "major", "far": "minor"}   # names stored by earlier versions


def bhav_summary(rows, expiry_of):
    """Summary columns per (expiry, "FUT" | "OPT") from bhav_compare rows. expiry_of maps a file name
    to its expiry for files the bhavcopy doesn't list (EXTRA_IN_KITE)."""
    out = {}
    for r in rows:
        if r["Status"].startswith("BAD_NAME"):
            continue
        expiry = r["Expiry"] or expiry_of.get(r["Contract"], "")
        key = (expiry, "FUT" if r["Type"] == "FUT" else "OPT")
        g = out.setdefault(key, {c: 0 for c in fno.BHAV_SUMMARY_COLUMNS})
        g["Bhav_traded_contracts"] += num(r["Bhav_volume"]) > 0
        g["Kite_files"] += r["Kite_volume"] != ""
        g["Missing_in_kite"] += "MISSING_IN_KITE" in r["Status"]
        g["Extra_in_kite"] += "EXTRA_IN_KITE" in r["Status"]
        g["OI_diffs"] += "OI_DIFF" in r["Status"]
        g["Price_diffs"] += "PRICE_DIFF" in r["Status"]
    return out


# --- files of a day -----------------------------------------------------------------------------

def bhav_dir(data_root, underlying, run_date, source="kite"):
    """<day folder>/bhavcopy of run_date, or None when the day has no folder."""
    day = fno.day_dir(data_root, underlying, run_date, source)
    return os.path.join(day, "bhavcopy") if day else None


def saved_items(day_dir, underlying):
    """Parsed rows of the day's saved <underlying>_bhav.csv, or None if the day has not been checked."""
    path = os.path.join(day_dir, "bhavcopy", f"{underlying.lower()}_bhav.csv")
    if not os.path.exists(path):
        return None
    with open(path, newline="", encoding="utf-8") as f:
        return parse_bhav(list(csv.DictReader(f)), underlying)


def read_check(data_root, underlying, run_date, source="kite"):
    out = bhav_dir(data_root, underlying, run_date, source)
    path = os.path.join(out, "bhav_check.json") if out else ""
    if not path or not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def unchecked_days(data_root, underlying="NIFTY", source="kite"):
    """Saved days (summary.csv present) without a bhavcopy check, oldest first."""
    root = fno.fno_root(data_root, underlying, source)
    out = []
    for day, d in sorted(fno.day_folders(root).items()):
        if (os.path.exists(os.path.join(root, d, "summary.csv"))
                and not os.path.exists(os.path.join(root, d, "bhavcopy", "bhav_check.json"))):
            out.append(day)
    return out


def log_line(path, line):
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{fno.now_ist():%Y-%m-%d %H:%M:%S} {line}\n")


def run_bhav(run_date, data_root, underlying="NIFTY", bhav_file=None, log=print, source="kite"):
    """Get the day's bhavcopy, save it and compare it with the saved files (Kite's, or Dhan's with
    source="dhan"). The caller holds the F&O lock of that source.
    Returns (status, line, summary rows or None): status "ok", "diffs" or "not_available"; raises BhavError."""
    root = fno.fno_root(data_root, underlying, source)
    echo = lambda m: log(f"[bhav:{underlying}:{run_date}] {m}")
    if bhav_file:
        with open(bhav_file, "rb") as f:
            data = f.read()
        source, original = os.path.abspath(bhav_file), os.path.basename(bhav_file)
    else:
        try:
            data, source = fetch_bhav(run_date, echo, underlying=underlying)
        except BhavNotAvailable as e:
            os.makedirs(root, exist_ok=True)
            log_line(os.path.join(root, "bhavcopy.log"), f"{run_date}: {e}")
            echo(str(e))
            return "not_available", PENDING, None
        original = os.path.basename(source)
    header, rows = read_bhav(data)
    dates = {r["TradDt"] for r in rows if r.get("TradDt")}
    if dates and dates != {run_date.isoformat()}:
        raise BhavError(f"This bhavcopy is for {', '.join(sorted(dates))}, not {run_date}.")
    mine = [r for r in rows if r["TckrSymb"] == underlying and r["FinInstrmTp"] in BHAV_TYPES]
    if not mine:
        raise BhavError(f"No {underlying} index futures or options in this bhavcopy.")

    day_dir = fno.find_day_dir(root, run_date)
    if day_dir is None:
        raise BhavError(f"{run_date} has not been downloaded in this folder yet; download it first.")
    out = os.path.join(day_dir, "bhavcopy")
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, original), "wb") as f:
        f.write(data)
    fno.write_csv(os.path.join(out, f"{underlying.lower()}_bhav.csv"), header, mine)

    return check_saved(day_dir, run_date, underlying, source, log)


def listed_names(run_date, day_dir, underlying="NIFTY", log=print, budget=TRIM_BUDGET):
    """(our names of EVERY contract of the underlying in the day's bhavcopy, traded or not, "") or
    (None, reason). Uses the day's saved <underlying>_bhav.csv, else downloads the bhavcopy (at most
    budget seconds) and saves it into <day>/bhavcopy/ as run_bhav does. Used to trim the Kite or Dhan list,
    which also has strikes NSE did not list that day, and to fetch the bhavcopy after a Kite day."""
    saved = saved_items(day_dir, underlying)
    if saved is None:
        try:
            data, src = fetch_bhav(run_date, log, budget=budget, underlying=underlying)
            header, rows = read_bhav(data)
        except BhavTimeout:
            return None, "bhavcopy fetch timed out"
        except BhavNotAvailable as e:
            return None, str(e)
        except (BhavError, requests.RequestException) as e:
            return None, f"bhavcopy download failed: {e}"
        dates = {r["TradDt"] for r in rows if r.get("TradDt")}
        if dates and dates != {run_date.isoformat()}:
            return None, f"the bhavcopy is for {', '.join(sorted(dates))}"
        mine = [r for r in rows if r["TckrSymb"] == underlying and r["FinInstrmTp"] in BHAV_TYPES]
        if not mine:
            return None, f"no {underlying} contracts in the bhavcopy"
        out = os.path.join(day_dir, "bhavcopy")
        os.makedirs(out, exist_ok=True)
        with open(os.path.join(out, os.path.basename(src)), "wb") as f:
            f.write(data)
        fno.write_csv(os.path.join(out, f"{underlying.lower()}_bhav.csv"), header, mine)
        saved = parse_bhav(mine, underlying)
    names = {i["name"] for i in saved if not i["error"]}
    return (names, "") if names else (None, f"no {underlying} contracts in the saved bhavcopy")


def summary_file_rows(cmp_rows, units, run_date, status, sev):
    """bhav_summary.csv: an ALL row (with the date, status and units), then one row per (expiry, FUT/OPT)."""
    def blank():
        return {c: 0 for c in SUMMARY_FILE_COLUMNS[3:13]}
    total, groups = blank(), {}
    for r in cmp_rows:
        kind = "FUT" if r["Type"] == "FUT" else "OPT"
        g = groups.setdefault((r["Expiry"], kind), {"Expiry": r["Expiry"], "Bucket": r.get("Bucket", ""), "Type": kind, **blank()})
        for t in (g, total):
            t["Bhav_contracts"] += r.get("Bhav_volume", "") != ""
            t["Bhav_traded"] += num(r.get("Bhav_volume")) > 0
            t["Kite_files"] += r.get("Kite_volume", "") != ""
            t["OK"] += r["Status"] == "OK"
            for col, flag in SUMMARY_FLAGS.items():
                t[col] += flag in r["Status"]
    head = {"Expiry": "ALL", "Bucket": "", "Type": "", **total, "Date": run_date.isoformat(),
            "Status": "OK" if status == "ok" else "CHECK (minor)" if sev == "minor" else "CHECK",
            "Volume_unit": fno.fmt_num(units["volume"]), "OI_unit": fno.fmt_num(units["oi"])}
    return [head] + [groups[k] for k in sorted(groups, key=lambda k: (k[0] or "9999", k[1] != "FUT"))]


def write_bhav_summary(out, cmp_rows, units, run_date, status, sev):
    """bhav_summary.csv in <day>/bhavcopy/. The differences themselves are only in bhav_compare.csv
    (the dashboard's Download menu gives them per bucket or as the Excel report)."""
    fno.write_csv(os.path.join(out, SUMMARY_FILE), SUMMARY_FILE_COLUMNS,
                  summary_file_rows(cmp_rows, units, run_date, status, sev))


def write_days_summary(day_dir, underlying="NIFTY"):
    """<day>/bhavcopy/checked_days_summary.csv: this day's row only, in the columns of the dashboard's
    "All days summary (CSV)" (which still lists every day). Nothing is written while the day has no
    summary.csv or no check. The broker comes from the folder name (check_saved's source is the bhavcopy's)."""
    from .bhav_export import DAY_COLUMNS, day_row, relabel   # bhav_export imports this module
    day_dir = os.path.abspath(day_dir)
    root = os.path.dirname(day_dir)
    source = "dhan" if os.path.basename(root).endswith(fno.FOLDERS["dhan"]) else "kite"
    data_root = os.path.dirname(root)
    rows = [day_row(data_root, underlying, d, source) for d in list_days(data_root, underlying, source)
            if d["folder"] == os.path.basename(day_dir) and d["status"] != "pending"]
    if not rows:
        return
    columns, rows = relabel(DAY_COLUMNS, rows, source)
    fno.write_csv(os.path.join(day_dir, "bhavcopy", DAYS_SUMMARY_FILE), columns, rows)


def check_saved(day_dir, run_date, underlying="NIFTY", source="", log=print):
    """Compare the day's saved <underlying>_bhav.csv with the files on disk (no network): writes
    bhav_compare.csv, bhav_summary.csv, bhav_check.json and checked_days_summary.csv, rewrites
    summary.csv and adds the line to run.log.
    Used after a download and after a contract is re-downloaded. Returns (status, line, summary rows)."""
    echo = lambda m: log(f"[bhav:{underlying}:{run_date}] {m}")
    items = saved_items(day_dir, underlying)
    if items is None:
        raise BhavError(f"{run_date} has no saved bhavcopy; run the check first.")
    out = os.path.join(day_dir, "bhavcopy")
    old = os.path.join(out, "bhav_check.json")
    if not source and os.path.exists(old):
        with open(old, encoding="utf-8") as f:
            source = json.load(f).get("source", "")
    cmp_rows, counts, units = compare(items, day_dir, underlying)
    fno.write_csv(os.path.join(out, "bhav_compare.csv"), COMPARE_COLUMNS, cmp_rows)
    line = check_status(counts)
    status = "ok" if line.endswith(": OK") else "diffs"
    write_bhav_summary(out, cmp_rows, units, run_date, status, severity(counts, status))
    with open(old, "w", encoding="utf-8") as f:
        json.dump({"date": run_date.isoformat(), "status": status, "severity": severity(counts, status),
                   "line": line, "counts": counts,
                   "units": units, "source": source,
                   "checked_at": f"{fno.now_ist():%Y-%m-%d %H:%M:%S} IST"}, f, indent=2)
    summary = None
    if os.path.exists(os.path.join(day_dir, "summary.csv")):
        summary = fno.rewrite_summary(day_dir, run_date, underlying)
    write_days_summary(day_dir, underlying)
    if os.path.exists(os.path.join(day_dir, "run.log")):
        log_line(os.path.join(day_dir, "run.log"), line)
    echo(f"{len(items):,} {underlying} contracts in the bhavcopy, {counts['bhav_traded']:,} traded; "
         f"{counts['kite_files']:,} Kite files. Units: volume x{units['volume']:g}, OI x{units['oi']:g}.")
    echo(line)
    return status, line, summary


# --- dashboard views ----------------------------------------------------------------------------

def list_days(data_root, underlying="NIFTY", source="kite"):
    """Every saved day (summary.csv present), latest first, with its bhavcopy check or pending.
    Creates nothing on disk."""
    root = fno.fno_root(data_root, underlying, source)
    out = []
    for day, d in sorted(fno.day_folders(root).items(), reverse=True):
        if not os.path.exists(os.path.join(root, d, "summary.csv")):
            continue
        path = os.path.join(root, d, "bhavcopy", "bhav_check.json")
        check = None
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                check = json.load(f)
        _, expiry, zero, created = fno.parse_day_folder(d)
        out.append({"date": day.isoformat(), "folder": d, "expiry": expiry.isoformat() if expiry else "",
                    "zero_volume": {True: "with", False: "no"}.get(zero, ""),
                    "downloaded": f"{created:%Y-%m-%d %H:%M}" if created else "",
                    "status": check["status"] if check else "pending",
                    "severity": day_severity(check) if check else "",
                    "line": check["line"] if check else PENDING,
                    "counts": check.get("counts", {}) if check else {},
                    "checked_at": check.get("checked_at", "") if check else ""})
    return out


def day_severity(check):
    """A stored check's severity under the current rules: from its counts when they have the missing and
    volume counts, otherwise its stored severity (old names mapped), otherwise major."""
    counts = check.get("counts", {})
    if ("missing" in counts and ("red_volume_diffs" in counts or "volume_diffs" in counts)) or check["status"] != "diffs":
        return severity(counts, check["status"])
    return OLD_SEVERITY.get(check.get("severity"), check.get("severity")) or "major"


RECENT_DAYS = 10   # banner: CHECK among the last 10 saved (trading) days


def banners(days, today):
    """Dates for the dashboard banners, days latest first: among the last RECENT_DAYS days, major CHECK
    days (red: missing contracts or volume diffs with Kite volume, see severity) and minor ones (amber); and pending days
    dated before yesterday (today's and yesterday's file may not be out yet)."""
    recent = [d for d in days[:RECENT_DAYS] if d["status"] == "diffs"]
    return {"check": [d["date"] for d in recent if d.get("severity", "major") != "minor"],
            "minor": [d["date"] for d in recent if d.get("severity") == "minor"],
            "pending_old": [d["date"] for d in days
                            if d["status"] == "pending" and date.fromisoformat(d["date"]) < today - timedelta(days=1)]}


def compare_rows(data_root, underlying, run_date, source="kite"):
    """(every row of the day's saved bhav_compare.csv, check). Reads only; a file saved before buckets
    existed is compared again in memory. Raises BhavError if the day has not been checked."""
    check = read_check(data_root, underlying, run_date, source)
    out = bhav_dir(data_root, underlying, run_date, source)
    path = os.path.join(out, "bhav_compare.csv") if out else ""
    if not check or not os.path.exists(path):
        raise BhavError(f"{run_date} has not been checked against the bhavcopy yet.")
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    if rows and "Bucket" not in rows[0]:   # checked before buckets existed: compare again, nothing written
        day_dir = os.path.dirname(os.path.dirname(path))
        items = saved_items(day_dir, underlying)
        if items is not None:
            rows = compare(items, day_dir, underlying)[0]
    return rows, check


def diff_rows(data_root, underlying, run_date, source="kite"):
    """(rows of bhav_compare.csv with Status != OK, check). Rows gain Bhav_volume_shares (bhav volume
    times the volume unit, comparable with Kite_volume) and Live (expiry today or later, so Kite still
    serves it). Raises BhavError if the day has not been checked."""
    rows, check = compare_rows(data_root, underlying, run_date, source)
    unit = (check.get("units") or {}).get("volume", 1)
    today = fno.today_ist()
    rows = [r for r in rows if r["Status"] != "OK"]
    for r in rows:
        r.setdefault("Bucket", FAR)
        r["Bhav_volume_shares"] = fno.fmt_num(num(r["Bhav_volume"]) * unit) if r.get("Bhav_volume") else ""
        r["Live"] = bool(r.get("Expiry")) and date.fromisoformat(r["Expiry"]) >= today
        r["Liquid"] = r["Bucket"] != FAR
    # biggest first: by bhavcopy volume; rows without one (extra files, bad names) last
    rows.sort(key=lambda r: (-num(r["Bhav_volume"]), r["Contract"]))
    return rows, check

import os
import sqlite3
import subprocess
import sys
import threading
import time
from datetime import date, datetime, timedelta

import pandas as pd
import requests
from flask import Flask, jsonify, redirect, render_template, request, session, url_for
from kiteconnect import KiteConnect
from kiteconnect import exceptions as kex

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE_DIR, ".env")


def load_env():
    env = {}
    if os.path.exists(ENV_PATH):
        with open(ENV_PATH, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    env[key.strip()] = value.strip().strip('"').strip("'")
    return env


def save_env(updates):
    """Write keys to .env; a value of None removes the key."""
    env = load_env()
    env.update(updates)
    with open(ENV_PATH, "w", encoding="utf-8") as f:
        for key, value in env.items():
            if value is not None:
                f.write(f"{key}={value}\n")


def credentials():
    env = load_env()
    return env.get("KITE_API_KEY", ""), env.get("KITE_API_SECRET", "")


app = Flask(__name__)
_env = load_env()
if "FLASK_SECRET_KEY" not in _env:
    save_env({"FLASK_SECRET_KEY": os.urandom(24).hex()})
app.secret_key = load_env()["FLASK_SECRET_KEY"]


def kite_client():
    kite = KiteConnect(api_key=credentials()[0])
    if "access_token" in session:
        kite.set_access_token(session["access_token"])
    return kite


@app.route("/")
def index():
    if "access_token" in session:
        return redirect(url_for("success"))
    api_key, api_secret = credentials()
    return render_template("login.html", error=request.args.get("error"),
                           api_key=api_key, has_secret=bool(api_secret))


@app.route("/login", methods=["POST"])
def login():
    api_key = request.form.get("api_key", "").strip()
    api_secret = request.form.get("api_secret", "").strip()
    saved_key, saved_secret = credentials()

    # Keep the saved secret if the field was left blank and the key is unchanged
    if not api_secret and api_key == saved_key:
        api_secret = saved_secret
    if not api_key or not api_secret:
        return redirect(url_for("index", error="API key and API secret are required."))

    save_env({"KITE_API_KEY": api_key, "KITE_API_SECRET": api_secret})
    # Sends the user to Zerodha's login page
    return redirect(KiteConnect(api_key=api_key).login_url())


@app.route("/callback")
def callback():
    # Kite redirects here with ?request_token=...&status=success
    if request.args.get("status") != "success" or "request_token" not in request.args:
        return redirect(url_for("index", error="Login was cancelled or failed."))
    try:
        api_key, api_secret = credentials()
        data = KiteConnect(api_key=api_key).generate_session(
            request.args["request_token"], api_secret=api_secret
        )
    except Exception as e:
        return redirect(url_for("index", error=str(e)))
    session["access_token"] = data["access_token"]
    # The bulk downloader reads the token from .env
    save_env({"KITE_ACCESS_TOKEN": data["access_token"]})
    session["user"] = {
        "user_id": data.get("user_id"),
        "user_name": data.get("user_name"),
        "email": data.get("email"),
        "broker": data.get("broker"),
        "login_time": str(data.get("login_time")),
    }
    return redirect(url_for("success"))


@app.route("/success")
def success():
    if "access_token" not in session:
        return redirect(url_for("index"))
    today = date.today()
    with JOB_LOCK:
        form = {
            "from": JOB["from"] or (today - timedelta(days=364)).isoformat(),
            "to": JOB["to"] or today.isoformat(),
            "interval": JOB["interval"] or "minute",
            "speed": JOB["speed"],
            "etfs": JOB["etfs"],
            "t2t": JOB["t2t"],
            "folder": JOB["folder"] or data_dir(),
            "format": JOB["format"] or "csv",
        }
    return render_template("success.html", user=session["user"], form=form,
                           today=today.isoformat(), error=request.args.get("error"))


@app.route("/logout")
def logout():
    try:
        kite_client().invalidate_access_token()
    except Exception:
        pass
    save_env({"KITE_ACCESS_TOKEN": None})
    session.clear()
    return redirect(url_for("index"))


# ---------------------------------------------------------------------------
# Bulk backfill: candles for every NSE EQ stock over a chosen range (max 1 year).
# Progress is tracked per (run, symbol, chunk) in SQLite, so a run can be
# stopped and resumed; data is merged into data/<interval>/<SYMBOL>/<year>.<csv|parquet>.
# ---------------------------------------------------------------------------

DEFAULT_DATA_DIR = os.path.join(BASE_DIR, "data")

# Max days per request allowed by Kite for each interval
INTERVALS = {"minute": 60, "day": 2000}
MAX_RANGE_DAYS = 365
DEFAULT_SPEED = 2.5   # requests per second, start to start
MAX_TRIES = 10
FILE_FORMATS = {"csv", "parquet"}
WINDOWS_RESERVED = {"CON", "PRN", "AUX", "NUL", *(f"COM{i}" for i in range(10)),
                    *(f"LPT{i}" for i in range(10))}

JOB_LOCK = threading.Lock()
JOB = {"state": "idle", "run": "", "interval": "", "from": "", "to": "", "speed": DEFAULT_SPEED,
       "format": "", "etfs": False, "t2t": True, "filter_info": "", "folder": "", "pass": 0, "stocks_done": 0, "stocks_total": 0, "chunks_done": 0,
       "chunks_total": 0, "eta": "", "requests": 0, "n429": 0, "net_errors": 0, "failed": 0,
       "empty": 0, "rate": DEFAULT_SPEED, "symbol": "", "chunk": "", "message": "",
       "job_no": 0, "completed": []}  # completed: [symbol, time] per stock finished this job
STOP = threading.Event()


class StopJob(Exception):
    pass


class AuthStop(Exception):
    pass


class ChunkFailed(Exception):
    pass


def job_update(**kw):
    with JOB_LOCK:
        JOB.update(kw)


def job_incr(key, n=1):
    with JOB_LOCK:
        JOB[key] += n


def data_dir():
    """Save folder chosen on the form (remembered in .env), or <project>/data."""
    return load_env().get("KITE_DATA_DIR") or DEFAULT_DATA_DIR


def log_error(data_root, run, symbol, chunk_from, msg):
    with open(os.path.join(data_root, "errors.log"), "a", encoding="utf-8") as f:
        f.write(f"{datetime.now():%Y-%m-%d %H:%M:%S} [{run}] {symbol} {chunk_from}: {msg}\n")


class RateLimiter:
    """Spaces request starts at least 1/rate seconds apart; halves the rate on a
    429 and doubles it back (up to the configured rate) after 20 clean requests."""

    def __init__(self, rate):
        self.max_rate = self.rate = rate
        self.last = None
        self.ok = 0

    def wait(self):
        if self.last is not None:
            delay = self.last + 1 / self.rate - time.monotonic()
            if delay > 0:
                time.sleep(delay)
        self.last = time.monotonic()

    def on_429(self):
        self.rate = max(0.5, self.rate / 2)
        self.ok = 0

    def on_success(self):
        self.ok += 1
        if self.ok >= 20 and self.rate < self.max_rate:
            self.rate = min(self.max_rate, self.rate * 2)
            self.ok = 0


def fetch_chunk(kite, limiter, token, start, end, interval):
    """One stock + one chunk, with retries. Raises AuthStop, StopJob or ChunkFailed."""
    for attempt in range(1, MAX_TRIES + 1):
        if STOP.is_set():
            raise StopJob()
        limiter.wait()
        job_incr("requests")
        try:
            candles = kite.historical_data(token, start, end, interval)
            limiter.on_success()
            job_update(rate=limiter.rate)
            return candles
        except kex.TokenException as e:
            raise AuthStop(f"Session expired or invalid ({e}). Sign in again and click Resume download.")
        except kex.KiteException as e:
            if e.code == 403:
                raise AuthStop(f"Access denied ({e}). Sign in again and click Resume download.")
            if e.code == 429 or "too many" in str(e).lower():
                job_incr("n429")
                limiter.on_429()
                job_update(rate=limiter.rate)
            elif isinstance(e, kex.NetworkException):
                job_incr("net_errors")
            else:
                raise ChunkFailed(f"{type(e).__name__}: {e}")
            err = e
        except requests.exceptions.RequestException as e:
            job_incr("net_errors")
            err = e
        if attempt < MAX_TRIES and STOP.wait(min(2 ** attempt, 60)):
            raise StopJob()
    raise ChunkFailed(f"gave up after {MAX_TRIES} tries: {type(err).__name__}: {err}")


def run_key(interval, start, end, fmt):
    """Progress key; CSV runs are tracked separately so switching format re-downloads."""
    return f"{interval}:{start}:{end}" + ("" if fmt == "parquet" else f":{fmt}")


def read_year(path, fmt):
    return pd.read_parquet(path) if fmt == "parquet" else pd.read_csv(path, parse_dates=["date"])


def write_year(df, path, fmt, interval):
    if fmt == "parquet":
        df.to_parquet(path, index=False)
    else:
        df.to_csv(path, index=False, date_format="%Y-%m-%d" if interval == "day" else "%Y-%m-%d %H:%M:%S")


def save_candles(data_root, interval, symbol, token, candles, fmt):
    """Merge candles into data/<interval>/<SYMBOL>/<year>.<fmt> without duplicates."""
    df = pd.DataFrame(candles)[["date", "open", "high", "low", "close", "volume"]]
    df["date"] = pd.to_datetime(df["date"])
    if df["date"].dt.tz is not None:
        df["date"] = df["date"].dt.tz_localize(None)  # keep IST wall-clock time
    df["tradingsymbol"] = symbol
    df["instrument_token"] = token

    folder_name = symbol + "_" if symbol.upper() in WINDOWS_RESERVED else symbol
    folder = os.path.join(data_root, interval, folder_name)
    os.makedirs(folder, exist_ok=True)
    for year, part in df.groupby(df["date"].dt.year):
        path = os.path.join(folder, f"{year}.{fmt}")
        if os.path.exists(path):
            part = pd.concat([read_year(path, fmt), part], ignore_index=True)
        part = (part.drop_duplicates("date", keep="last")
                    .sort_values("date").reset_index(drop=True))
        tmp = path + ".tmp"
        write_year(part, tmp, fmt, interval)
        os.replace(tmp, path)


def is_etf(inst):
    sym, name = inst["tradingsymbol"].upper(), (inst.get("name") or "").upper()
    return "ETF" in name or "ETF" in sym or sym.endswith("BEES")


T2T_SERIES = {"BE", "BZ"}
MAX_STOCKS = 3000  # more than this means the series filter broke; refuse to start


def series_suffix(inst):
    """NSE series suffix of a tradingsymbol ("BE", "GB", "N1", ...) or "" for normal
    equity. Suffixes are always 2 characters, so BAJAJ-AUTO / NAM-INDIA are plain equity."""
    _, sep, suffix = inst["tradingsymbol"].rpartition("-")
    return suffix.upper() if sep and len(suffix) == 2 else ""


def is_equity(inst, include_t2t):
    """Normal equity, plus trade-to-trade -BE / -BZ if asked. Bonds, NCDs, G-secs,
    T-bills, SGBs (-N*, -Y*, -Z*, -GB, -GS, -SG, -TB, -RR, ...) and SME are excluded."""
    suffix = series_suffix(inst)
    return not suffix or (include_t2t and suffix in T2T_SERIES)


def make_chunks(start, end, interval):
    """Split [start 00:00, end 23:59:59] into windows of Kite's max size."""
    chunks, cur = [], datetime.combine(start, datetime.min.time())
    last = datetime.combine(end, datetime.max.time()).replace(microsecond=0)
    step = timedelta(days=INTERVALS[interval])
    while cur <= last:
        chunk_end = min(cur + step - timedelta(seconds=1), last)
        chunks.append((cur, chunk_end))
        cur = chunk_end + timedelta(seconds=1)
    return chunks


def open_db(data_root):
    os.makedirs(data_root, exist_ok=True)
    db = sqlite3.connect(os.path.join(data_root, "progress.sqlite"))
    db.execute("""CREATE TABLE IF NOT EXISTS chunks (
        run TEXT, symbol TEXT, token INTEGER, chunk_from TEXT, chunk_to TEXT,
        status TEXT, tries INTEGER DEFAULT 0, error TEXT, updated TEXT,
        PRIMARY KEY (run, symbol, chunk_from))""")
    return db


def fmt_eta(seconds):
    seconds = int(seconds)
    return f"{seconds // 3600}h {seconds % 3600 // 60:02d}m"


def bulk_download(api_key, access_token, interval, start, end, speed, etfs, t2t, data_root, fmt):
    run = run_key(interval, start, end, fmt)
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    limiter = RateLimiter(speed)
    db = open_db(data_root)
    try:
        job_update(message="Loading instrument list...")
        eq = [i for i in kite.instruments("NSE")
              if i["segment"] == "NSE" and i["instrument_type"] == "EQ"]
        series_ok = [i for i in eq if is_equity(i, t2t)]
        instruments = [i for i in series_ok if etfs or not is_etf(i)]
        symbols = {i["tradingsymbol"] for i in instruments}
        n_t2t = sum(1 for i in instruments if series_suffix(i))
        filter_info = (f"{len(instruments):,} stocks (normal {len(instruments) - n_t2t:,}"
                       + (f" + -BE/-BZ {n_t2t:,}" if t2t else "") + ") | excluded "
                       f"{len(eq) - len(series_ok):,} bonds/other series"
                       + ("" if etfs else f", {len(series_ok) - len(instruments):,} ETFs"))
        job_update(filter_info=filter_info)
        print(f"[{run}] Stocks selected: {filter_info}", flush=True)
        if len(instruments) > MAX_STOCKS:
            msg = (f"Not started: {len(instruments):,} stocks selected (limit {MAX_STOCKS:,}). "
                   "The symbol filter may be broken.")
            print(f"[{run}] {msg}", flush=True)
            job_update(state="stopped", message=msg)
            return

        chunks = make_chunks(start, end, interval)
        db.executemany(
            "INSERT OR IGNORE INTO chunks (run, symbol, token, chunk_from, chunk_to, status) "
            "VALUES (?, ?, ?, ?, ?, 'PENDING')",
            [(run, i["tradingsymbol"], i["instrument_token"], str(a), str(b))
             for i in instruments for a, b in chunks])
        db.commit()

        rows = [r for r in db.execute(
            "SELECT symbol, status FROM chunks WHERE run = ?", (run,)) if r[0] in symbols]
        remaining = {}
        for sym, status in rows:
            remaining.setdefault(sym, 0)
            if status != "DONE":
                remaining[sym] += 1
        job_update(stocks_total=len(remaining),
                   stocks_done=sum(1 for n in remaining.values() if n == 0),
                   chunks_total=len(rows),
                   chunks_done=sum(1 for _, s in rows if s == "DONE"),
                   failed=sum(1 for _, s in rows if s == "FAILED"), message="")
        print(f"[{run}] {len(remaining)} stocks, {len(rows)} chunks, "
              f"{JOB['chunks_done']} already done", flush=True)

        t0, processed = time.monotonic(), 0
        pass_no = 0
        while True:
            pass_no += 1
            todo = [r for r in db.execute(
                "SELECT symbol, token, chunk_from, chunk_to, status FROM chunks "
                "WHERE run = ? AND status != 'DONE' ORDER BY symbol, chunk_from", (run,))
                if r[0] in symbols]
            if not todo:
                break
            job_update(**{"pass": pass_no})
            print(f"[{run}] pass {pass_no}: {len(todo)} chunks to fetch", flush=True)
            newly_done = 0

            for sym, token, cfrom, cto, prev_status in todo:
                if STOP.is_set():
                    raise StopJob()
                job_update(symbol=sym, chunk=cfrom[:10])
                status, error = "DONE", None
                try:
                    candles = fetch_chunk(kite, limiter, token, datetime.fromisoformat(cfrom),
                                          datetime.fromisoformat(cto), interval)
                    if candles:
                        save_candles(data_root, interval, sym, token, candles, fmt)
                    else:
                        job_incr("empty")
                except ChunkFailed as e:
                    status, error = "FAILED", str(e)
                    log_error(data_root, run, sym, cfrom, error)
                except (AuthStop, StopJob):
                    raise
                except Exception as e:
                    status, error = "FAILED", f"{type(e).__name__}: {e}"
                    log_error(data_root, run, sym, cfrom, error)

                db.execute("UPDATE chunks SET status = ?, tries = tries + 1, error = ?, updated = ? "
                           "WHERE run = ? AND symbol = ? AND chunk_from = ?",
                           (status, error, datetime.now().isoformat(timespec="seconds"),
                            run, sym, cfrom))
                db.commit()

                processed += 1
                if status == "DONE":
                    newly_done += 1
                    remaining[sym] -= 1
                    job_incr("chunks_done")
                    if remaining[sym] == 0:
                        with JOB_LOCK:
                            JOB["stocks_done"] += 1
                            JOB["completed"].append([sym, datetime.now().isoformat(timespec="seconds")])
                    if prev_status == "FAILED":
                        job_incr("failed", -1)
                elif prev_status != "FAILED":
                    job_incr("failed")

                left = JOB["chunks_total"] - JOB["chunks_done"]
                job_update(eta=fmt_eta((time.monotonic() - t0) / processed * left))
                if processed % 50 == 0:
                    print(f"[{run}] stocks {JOB['stocks_done']}/{JOB['stocks_total']} | chunks "
                          f"{JOB['chunks_done']}/{JOB['chunks_total']} | ETA {JOB['eta']} | "
                          f"429s {JOB['n429']} | failed {JOB['failed']}", flush=True)

            if newly_done == 0:
                break  # a full re-loop pass fixed nothing; stop looping

        state_msg = ("All chunks downloaded." if JOB["failed"] == 0 else
                     f"Finished with {JOB['failed']} failed chunks (see errors.log in the save folder). "
                     "Click Resume download to retry them.")
        job_update(state="finished", symbol="", chunk="", eta="", message=state_msg)
    except StopJob:
        job_update(state="stopped", message="Stopped. Click Resume download to continue.")
    except AuthStop as e:
        log_error(data_root, run, JOB["symbol"], JOB["chunk"], str(e))
        job_update(state="stopped", message=str(e))
    except Exception as e:
        log_error(data_root, run, JOB["symbol"], JOB["chunk"], f"{type(e).__name__}: {e}")
        job_update(state="stopped", message=f"Stopped: {type(e).__name__}: {e}")
    finally:
        db.close()
        with JOB_LOCK:
            print(f"[{run}] {JOB['state']}: requests {JOB['requests']}, 429s {JOB['n429']}, "
                  f"network errors {JOB['net_errors']}, failed chunks {JOB['failed']}, "
                  f"empty chunks {JOB['empty']}. {JOB['message']}", flush=True)


@app.route("/download/start", methods=["POST"])
def download_start():
    if "access_token" not in session:
        return redirect(url_for("index"))
    api_key = credentials()[0]
    access_token = load_env().get("KITE_ACCESS_TOKEN")
    if not access_token:
        return redirect(url_for("index", error="No access token in .env. Please log in again."))

    def fail(msg):
        return redirect(url_for("success", error=msg))

    interval = request.form.get("interval", "")
    if interval not in INTERVALS:
        return fail("Choose 1 minute or EOD.")
    try:
        start = date.fromisoformat(request.form.get("from", ""))
        end = date.fromisoformat(request.form.get("to", ""))
    except ValueError:
        return fail("Enter a valid start and end date.")
    try:
        speed = float(request.form.get("speed", DEFAULT_SPEED))
    except ValueError:
        speed = 0
    if not 0 < speed <= 3:
        return fail("Speed must be between 0 and 3 requests per second.")
    if end > date.today():
        return fail("End date cannot be in the future.")
    if start >= end:
        return fail("Start date must be before end date.")
    if (end - start).days > MAX_RANGE_DAYS:
        return fail("Range too long: maximum 1 year per run.")
    fmt = request.form.get("format", "csv")
    if fmt not in FILE_FORMATS:
        return fail("Choose CSV or Parquet.")
    etfs = bool(request.form.get("etfs"))
    t2t = bool(request.form.get("t2t"))
    folder = os.path.expanduser(request.form.get("folder", "").strip() or DEFAULT_DATA_DIR)
    if not os.path.isabs(folder):
        return fail(r"Save folder must be a full path, e.g. D:\market_data")
    folder = os.path.normpath(folder)
    try:
        os.makedirs(folder, exist_ok=True)
        probe = os.path.join(folder, ".write_test")
        with open(probe, "w") as f:
            f.write("ok")
        os.remove(probe)
    except OSError as e:
        return fail(f"Cannot write to folder {folder}: {e.strerror or e}")
    save_env({"KITE_DATA_DIR": folder})

    with JOB_LOCK:
        if JOB["state"] == "running":
            return redirect(url_for("success"))
        JOB.update(state="running", run=run_key(interval, start, end, fmt), interval=interval, format=fmt,
                   **{"from": start.isoformat()}, to=end.isoformat(), speed=speed, etfs=etfs, t2t=t2t, filter_info="", folder=folder,
                   stocks_done=0, stocks_total=0, chunks_done=0, chunks_total=0,
                   eta="", requests=0, n429=0, net_errors=0, failed=0, empty=0, rate=speed,
                   symbol="", chunk="", message="Starting...", completed=[])
        JOB["job_no"] += 1
        JOB["pass"] = 0
    STOP.clear()
    threading.Thread(target=bulk_download, daemon=True,
                     args=(api_key, access_token, interval, start, end, speed, etfs, t2t,
                           folder, fmt)).start()
    return redirect(url_for("success"))


BROWSE_SCRIPT = r"""
import sys, tkinter as tk
from tkinter import filedialog
root = tk.Tk()
root.withdraw()
root.attributes("-topmost", True)
path = filedialog.askdirectory(initialdir=sys.argv[1], title="Choose folder to save data")
print(path or "", end="")
"""


@app.route("/browse-folder", methods=["POST"])
def browse_folder():
    """Opens a native folder picker on this machine (the app only runs on 127.0.0.1)."""
    if "access_token" not in session:
        return jsonify({"path": "", "error": "Not logged in."}), 403
    current = request.form.get("current") or data_dir()
    try:
        out = subprocess.run([sys.executable, "-c", BROWSE_SCRIPT, current],
                             capture_output=True, text=True, timeout=300)
    except subprocess.TimeoutExpired:
        return jsonify({"path": "", "error": "Folder dialog timed out."})
    if out.returncode != 0:
        return jsonify({"path": "", "error": "Could not open folder dialog."})
    return jsonify({"path": os.path.normpath(out.stdout) if out.stdout else ""})


@app.route("/download/stop", methods=["POST"])
def download_stop():
    STOP.set()
    return redirect(url_for("success"))


@app.route("/download/status")
def download_status():
    """Job state; completed stocks are sent incrementally from ?since=<count already shown>."""
    since = request.args.get("since", 0, type=int)
    with JOB_LOCK:
        out = {k: v for k, v in JOB.items() if k != "completed"}
        out["completed_count"] = len(JOB["completed"])
        out["completed_new"] = JOB["completed"][max(since, 0):]
    return jsonify(out)


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=False)

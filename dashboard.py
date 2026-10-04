"""Dashboard page and the bulk-download endpoints it calls."""
import os
import subprocess
import sys
import threading
from datetime import date, timedelta

from flask import Blueprint, jsonify, redirect, render_template, request, session, url_for

from backfill.fetch import INTERVALS, MAX_INDEX_RANGE_DAYS, MAX_RANGE_DAYS
from backfill.instruments import INDEX_CHOICES
from backfill.job import DEFAULT_SPEED, JOB, JOB_LOCK, STOP
from backfill.runner import bulk_download
from backfill.storage import FILE_FORMATS, job_run_key
from config import DEFAULT_DATA_DIR, credentials, data_dir, load_env, save_env

bp = Blueprint("dashboard", __name__)


@bp.route("/success")
def success():
    if "access_token" not in session:
        return redirect(url_for("auth.index"))
    today = date.today()
    with JOB_LOCK:
        form = {
            "from": JOB["from"] or (today - timedelta(days=364)).isoformat(),
            "to": JOB["to"] or today.isoformat(),
            "interval": JOB["interval"] or "minute",
            "speed": JOB["speed"],
            "equity": JOB["equity"],
            "indices": JOB["indices"],
            "etfs": JOB["etfs"],
            "t2t": JOB["t2t"],
            "folder": JOB["folder"] or data_dir(),
            "format": JOB["format"] or "csv",
        }
    return render_template("success.html", user=session["user"], form=form, index_choices=INDEX_CHOICES,
                           today=today.isoformat(), error=request.args.get("error"))


@bp.route("/download/start", methods=["POST"])
def download_start():
    if "access_token" not in session:
        return redirect(url_for("auth.index"))
    api_key = credentials()[0]
    access_token = load_env().get("KITE_ACCESS_TOKEN")
    if not access_token:
        return redirect(url_for("auth.index", error="No access token in .env. Please log in again."))

    def fail(msg):
        return redirect(url_for("dashboard.success", error=msg))

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
    equity = bool(request.form.get("equity"))
    indices = [s for s in INDEX_CHOICES if s in request.form.getlist("indices")]
    if not equity and not indices:
        return fail("Choose at least one thing to download.")
    if equity and (end - start).days > MAX_RANGE_DAYS:
        return fail("Range too long: with NSE equity stocks ticked the maximum is 1 year per run.")
    if (end - start).days > MAX_INDEX_RANGE_DAYS:
        return fail("Range too long: the maximum for indices is 20 years per run.")
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
            return redirect(url_for("dashboard.success"))
        JOB.update(state="running", run=job_run_key(interval, start, end, fmt, indices, equity),
                   interval=interval, format=fmt, equity=equity, indices=indices, part="", equity_total=0, out_dirs=[],
                   **{"from": start.isoformat()}, to=end.isoformat(), speed=speed, etfs=etfs, t2t=t2t, filter_info="", folder=folder,
                   stocks_done=0, stocks_total=0, chunks_done=0, chunks_total=0,
                   eta="", requests=0, n429=0, net_errors=0, failed=0, empty=0, rate=speed,
                   symbol="", chunk="", message="Starting...", completed=[])
        JOB["job_no"] += 1
        JOB["pass"] = 0
    STOP.clear()
    threading.Thread(target=bulk_download, daemon=True,
                     args=(api_key, access_token, interval, start, end, speed, etfs, t2t,
                           indices, equity, folder, fmt)).start()
    return redirect(url_for("dashboard.success"))


BROWSE_SCRIPT = r"""
import sys, tkinter as tk
from tkinter import filedialog
root = tk.Tk()
root.withdraw()
root.attributes("-topmost", True)
path = filedialog.askdirectory(initialdir=sys.argv[1], title="Choose folder to save data")
print(path or "", end="")
"""


@bp.route("/browse-folder", methods=["POST"])
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


@bp.route("/download/stop", methods=["POST"])
def download_stop():
    STOP.set()
    return redirect(url_for("dashboard.success"))


@bp.route("/download/status")
def download_status():
    """Job state; completed stocks are sent incrementally from ?since=<count already shown>."""
    since = request.args.get("since", 0, type=int)
    with JOB_LOCK:
        out = {k: v for k, v in JOB.items() if k != "completed"}
        out["completed_count"] = len(JOB["completed"])
        out["completed_new"] = JOB["completed"][max(since, 0):]
    return jsonify(out)

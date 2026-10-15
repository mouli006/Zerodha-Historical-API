"""Dashboard page and the bulk-download endpoints it calls."""
import io
import os
import subprocess
import sys
import threading
from datetime import date, timedelta

from flask import Blueprint, jsonify, redirect, render_template, request, send_file, session, url_for
from kiteconnect import KiteConnect
from kiteconnect import exceptions as kex

import dhan
from backfill import bhav, bhav_export, fno, fno_days
from backfill.dhan_api import DhanClient, data_plan_error, token_ok
from backfill.fetch import INTERVALS, MAX_INDEX_RANGE_DAYS, MAX_RANGE_DAYS
from backfill.instruments import INDEX_CHOICES
from backfill.job import DEFAULT_SPEED, JOB, JOB_LOCK, STOP, job_begin
from backfill.runner import bulk_download
from backfill.storage import FILE_FORMATS, check_folder, job_run_key
from config import DEFAULT_DATA_DIR, credentials, data_dir, keep_zero_volume, load_env, save_env, weekly_expiry_weekday

bp = Blueprint("dashboard", __name__)


@bp.errorhandler(fno.DayFolderError)
def day_folder_error(e):
    """Two folders for one trading day: every F&O route answers with the folders to fix."""
    return jsonify({"error": str(e)})


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
    # the F&O and bhavcopy cards switch between these (exchange: Kite's; bhav: whose bhavcopy checks it)
    underlyings = [{"name": u, "exchange": exch, "bhav": fno.exchange_name(u)} for u, (exch, _) in fno.UNDERLYINGS.items()]
    fno_info = {"underlying": "NIFTY", "exchange": fno.UNDERLYINGS["NIFTY"][0], "underlyings": underlyings,
                "ready_at": fno.READY_AT.strftime("%H:%M"), "today": fno.today_ist().isoformat(),
                "keep_zero_volume": keep_zero_volume()}
    broker = {"name": "Kite", "maxSpeed": 3, "defaultSpeed": DEFAULT_SPEED,
              "urls": {"fnoWindow": url_for("dashboard.download_fno_window"), "fno": url_for("dashboard.download_fno_start"),
                       "page": url_for("dashboard.success")}}
    return render_template("success.html", mode="kite", broker=broker, kite=True,
                           user=session["user"], form=form, index_choices=INDEX_CHOICES,
                           today=today.isoformat(), fno=fno_info, dhan=dhan.status(load_env()),
                           dhan_connected=request.args.get("dhan") == "1", error=request.args.get("error"))


def read_speed():
    try:
        speed = float(request.form.get("speed", DEFAULT_SPEED))
    except ValueError:
        return 0
    return speed if 0 < speed <= 3 else 0


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
    speed = read_speed()
    if not speed:
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
    folder, err = check_folder(request.form.get("folder", "") or DEFAULT_DATA_DIR)
    if err:
        return fail(err)
    save_env({"KITE_DATA_DIR": folder})

    if not job_begin(kind="backfill", run=job_run_key(interval, start, end, fmt, indices, equity),
                     interval=interval, format=fmt, equity=equity, indices=indices, equity_total=0,
                     **{"from": start.isoformat()}, to=end.isoformat(), speed=speed, etfs=etfs, t2t=t2t,
                     folder=folder):
        return redirect(url_for("dashboard.success"))
    STOP.clear()
    threading.Thread(target=bulk_download, daemon=True,
                     args=(api_key, access_token, interval, start, end, speed, etfs, t2t,
                           indices, equity, folder, fmt)).start()
    return redirect(url_for("dashboard.success"))


LIVE = {}   # (IST date, underlying) -> live expiries; the instrument list is fetched once a day


def fno_window(folder, underlying):
    """(Window, "") for the F&O date picker, or (None, error message). Reads the expiry history
    of the save folder but writes nothing (runs record it)."""
    today = fno.today_ist()
    try:
        if (today, underlying) not in LIVE:
            kite = KiteConnect(api_key=credentials()[0])
            kite.set_access_token(load_env().get("KITE_ACCESS_TOKEN", ""))
            LIVE[(today, underlying)] = fno.live_expiries(
                kite.instruments(fno.UNDERLYINGS[underlying][0]), underlying, today)
        history = set(fno.read_expiries(folder, underlying))
        return fno.date_window(LIVE[(today, underlying)], history, today, weekly_expiry_weekday(underlying)), ""
    except (kex.TokenException, kex.PermissionException):
        return None, "Kite session expired. Sign in again."
    except Exception as e:
        return None, f"Couldn't load the {underlying} expiry list from Kite: {type(e).__name__}: {e}"


@bp.route("/download/fno-window")
def download_fno_window():
    """Allowed dates for the F&O card: previous expiry + 1 to today, plus the missing days."""
    if "access_token" not in session:
        return jsonify({"error": "Not logged in."}), 403
    underlying = request.args.get("underlying", "NIFTY")
    if underlying not in fno.UNDERLYINGS:
        return jsonify({"error": f"Unknown underlying {underlying}."})
    folder = os.path.expanduser(request.args.get("folder", "").strip() or DEFAULT_DATA_DIR)
    if not os.path.isabs(folder):
        return jsonify({"error": "Enter the save folder as a full path."})
    folder = os.path.normpath(folder)
    w, err = fno_window(folder, underlying)
    if err:
        return jsonify({"error": err})
    return jsonify({"min_date": w.min_date.isoformat(), "max_date": w.max_date.isoformat(),
                    "current_expiry": w.current_expiry.isoformat(), "previous_expiry": w.previous_expiry.isoformat(),
                    "estimated": w.estimated, "today_is_expiry": w.current_expiry == w.max_date,
                    "missing": [d.isoformat() for d in fno.missing_dates(folder, underlying, w)],
                    "days": fno_days.day_summary(fno.fno_root(folder, underlying))})


@bp.route("/download/fno-start", methods=["POST"])
def download_fno_start():
    """F&O run for one picked date, a picked range (date_from/date_to: every day in it, saved files
    skipped), or every missing date (missing=1), or a re-check of one saved
    date's contracts without a file (refetch=1, needs keep_zero=1); the same jobs as nifty_daily.py
    DATE / --missing / --refetch-no-trade DATE. keep_zero=1 keeps contracts with zero-volume candles."""
    if "access_token" not in session:
        return redirect(url_for("auth.index"))
    api_key = credentials()[0]
    access_token = load_env().get("KITE_ACCESS_TOKEN")
    if not access_token:
        return redirect(url_for("auth.index", error="No access token in .env. Please log in again."))

    def fail(msg):
        return redirect(url_for("dashboard.success", error=msg))

    underlying = request.form.get("underlying", "NIFTY")
    if underlying not in fno.UNDERLYINGS:
        return fail(f"Unknown underlying {underlying}.")
    missing = bool(request.form.get("missing"))
    refetch = bool(request.form.get("refetch")) and not missing
    keep_zero = request.form.get("keep_zero") == "1"
    if refetch and not keep_zero:
        return fail("Turn on \"Keep contracts with candles but zero volume\" to re-check no-trade contracts.")
    run_date = dates = None
    if not missing:
        try:
            run_date, dates = fno.parse_pick(request.form.get("date", ""), request.form.get("date_from", ""),
                                             request.form.get("date_to", ""))
        except ValueError as e:
            return fail(str(e))
        if dates and refetch:
            return fail("Re-check no-trade contracts works on one day; pick a single date.")
    speed = read_speed()
    if not speed:
        return fail("Speed must be between 0 and 3 requests per second.")
    folder, err = check_folder(request.form.get("folder", "") or DEFAULT_DATA_DIR)
    if err:
        return fail(err)
    w, err = fno_window(folder, underlying)
    if err:
        return fail(err)
    if missing:
        if not fno.missing_dates(folder, underlying, w):
            return fail(f"Nothing missing: every day from {w.min_date} to {w.max_date} is saved or a holiday.")
    elif dates:
        err = fno.window_error(dates[0], w) or fno.window_error(dates[-1], w) or fno.too_early(dates[-1])
        if err:
            return fail(err)
    else:
        err = fno.window_error(run_date, w) or fno.too_early(run_date)
        if err:
            return fail(err)
        saved = fno.day_dir(folder, underlying, run_date)
        if refetch and not (saved and os.path.exists(os.path.join(saved, "summary.csv"))):
            return fail(f"{run_date} has not been downloaded in this folder yet; download the day first.")
    try:
        lock = fno.acquire_lock(folder, underlying, "dashboard")
    except fno.LockBusy as e:
        return fail(str(e))
    label = f"{dates[0]}_to_{dates[-1]}" if dates else "missing"
    if not job_begin(**fno.job_fields(underlying, run_date, folder, speed, keep_zero, label=label)):
        lock.release()
        return fail("A download is already running. Stop it or wait for it to finish.")
    save_env({"KITE_DATA_DIR": folder})
    STOP.clear()

    weekday = weekly_expiry_weekday(underlying)

    def work():
        try:
            if missing:
                fno.run_missing(api_key, access_token, folder, speed, underlying, weekday=weekday,
                                keep_zero=keep_zero)
            elif dates:
                fno.run_missing(api_key, access_token, folder, speed, underlying, weekday=weekday,
                                keep_zero=keep_zero, dates=dates)
            elif refetch:
                fno.refetch_no_trade(api_key, access_token, run_date, folder, speed, underlying,
                                     weekday=weekday)
            else:
                fno.run_daily(api_key, access_token, run_date, folder, speed, underlying,
                              weekday=weekday, keep_zero=keep_zero)
        finally:
            lock.release()

    threading.Thread(target=work, daemon=True).start()
    return redirect(url_for("dashboard.success"))


def fno_args(need_date=True):
    """(folder, date or None, underlying, error) from an F&O / bhavcopy request. Nothing is created."""
    values = request.values
    underlying = values.get("underlying", "NIFTY")
    if underlying not in fno.UNDERLYINGS:
        return None, None, None, f"Unknown underlying {underlying}."
    run_date = None
    if need_date:
        try:
            run_date = date.fromisoformat(values.get("date", ""))
        except ValueError:
            return None, None, None, "Choose the date."
    folder = os.path.normpath(os.path.expanduser(values.get("folder", "").strip() or DEFAULT_DATA_DIR))
    if not os.path.isabs(folder):
        return None, None, None, "Enter the save folder as a full path."
    return folder, run_date, underlying, ""


def fno_source():
    """(source, error) from ?source= on the bhavcopy routes: "kite" (default) or "dhan" (the Dhan F&O page)."""
    source = request.values.get("source", "kite") or "kite"
    if source not in fno.FOLDERS:
        return None, f"Unknown data source {source}."
    if source == "dhan" and request.values.get("underlying", "NIFTY") not in dhan.UNDERLYINGS:
        return None, f"Unknown underlying {request.values.get('underlying')}."
    return source, ""


def signed_in(source):
    """Kite files need the Kite session; Dhan files a valid Dhan token."""
    return "access_token" in session if source == "kite" else dhan.status(load_env())["connected"]


def locked_fno(folder, underlying, owner, source="kite"):
    """(lock, error): the F&O lock, refused while a download job is running (it owns the rate limit)."""
    with JOB_LOCK:
        if JOB["state"] == "running":
            return None, "A download is running. Try again when it has finished."
    try:
        return fno.acquire_lock(folder, underlying, owner, source), ""
    except fno.LockBusy as e:
        return None, str(e)


def refresh_live_summary(folder, run_date, rows, source="kite"):
    """Show new summary rows on the live card when it is showing this day."""
    if rows is not None:
        with JOB_LOCK:
            if (JOB["kind"] == "fno" and JOB["fno_date"] == run_date.isoformat() and JOB["folder"] == folder
                    and (JOB["source"] or "kite") == source):
                JOB["fno_summary"] = rows


@bp.route("/fno/bhav-status")
def fno_bhav_status():
    """Every saved day (Kite's, or Dhan's with source=dhan) with its bhavcopy check (latest first) and the banner dates."""
    source, err = fno_source()
    if err:
        return jsonify({"error": err})
    if not signed_in(source):
        return jsonify({"error": "Not logged in."}), 403
    folder, _, underlying, err = fno_args(need_date=False)
    if err:
        return jsonify({"error": err})
    days = bhav.list_days(folder, underlying, source)
    return jsonify({"days": days, "banners": bhav.banners(days, fno.today_ist())})


@bp.route("/fno/bhav-diff")
def fno_bhav_diff():
    """The day's bhav_compare.csv rows whose Status is not OK."""
    source, err = fno_source()
    if err:
        return jsonify({"error": err})
    if not signed_in(source):
        return jsonify({"error": "Not logged in."}), 403
    folder, run_date, underlying, err = fno_args()
    if err:
        return jsonify({"error": err})
    try:
        rows, check = bhav.diff_rows(folder, underlying, run_date, source)
    except bhav.BhavError as e:
        return jsonify({"error": str(e)})
    return jsonify({"rows": rows, "units": check.get("units", {}), "line": check["line"]})


@bp.route("/fno/bhav-export")
def fno_bhav_export():
    """A download of the bhavcopy check: scope=days (every checked day), or for a date scope=view (the
    difference list for bucket=<name|liquid|all>), all (every contract) or xlsx (full report). Reads the
    saved files only; 404 when the day has no check."""
    source, err = fno_source()
    if err:
        return jsonify({"error": err}), 400
    if not signed_in(source):
        return jsonify({"error": "Not logged in."}), 403
    scope = request.args.get("scope", "")
    if scope not in ("days", "view", "all", "xlsx"):
        return jsonify({"error": "scope must be days, view, all or xlsx."}), 400
    folder, run_date, underlying, err = fno_args(need_date=scope != "days")
    if err:
        return jsonify({"error": err}), 400
    try:
        name, data, mimetype = bhav_export.export(folder, underlying, scope, run_date,
                                                  request.args.get("bucket", "all") or "all", source=source)
    except bhav.BhavError as e:
        return jsonify({"error": str(e)}), 404
    return send_file(io.BytesIO(data), mimetype=mimetype, as_attachment=True, download_name=name)


@bp.route("/fno/bhav-check", methods=["POST"])
def fno_bhav_check():
    """Bhavcopy check for one date, or every saved day without one (missing=1); nifty_bhav.py DATE /
    --missing. Runs in the request (about 1 MB per day); no Kite token needed. source=dhan checks the
    Dhan files (<UNDERLYING>_FnO_1min_Dhan)."""
    source, err = fno_source()
    if err:
        return jsonify({"error": err})
    if not signed_in(source):
        return jsonify({"error": "Not logged in."}), 403
    missing = bool(request.values.get("missing"))
    folder, run_date, underlying, err = fno_args(need_date=not missing)
    if not err:
        folder, err = check_folder(folder)
    if err:
        return jsonify({"error": err})
    dates = bhav.unchecked_days(folder, underlying, source) if missing else [run_date]
    if not dates:
        return jsonify({"results": [], "message": "Every saved day has a bhavcopy check."})
    lock, err = locked_fno(folder, underlying, "dashboard bhavcopy check", source)
    if err:
        return jsonify({"error": err})
    results = []
    try:
        for d in dates:
            try:
                status, line, rows = bhav.run_bhav(d, folder, underlying, source=source)
                refresh_live_summary(folder, d, rows, source)
            except (bhav.BhavError, OSError) as e:
                status, line = "error", str(e)
            results.append({"date": d.isoformat(), "status": status, "line": line})
    finally:
        lock.release()
    return jsonify({"results": results})


@bp.route("/fno/redownload", methods=["POST"])
def fno_redownload():
    """Fetch one missing contract for a day again (still-live contracts only; keep_zero=1 keeps it when
    its candles all have volume 0), then re-run the day's bhavcopy comparison from the saved file.
    source=dhan re-downloads a Dhan file with the Dhan token."""
    source, err = fno_source()
    if err:
        return jsonify({"error": err})
    if not signed_in(source):
        return jsonify({"error": "Not logged in."}), 403
    folder, run_date, underlying, err = fno_args()
    contract = request.values.get("contract", "").strip()
    if not err and not contract:
        err = "Which contract?"
    if err:
        return jsonify({"error": err})
    env = load_env()
    client = None
    if source == "dhan":
        # the same checks as /dhan/fno-start: token not expired, accepted by Dhan, Data API plan active
        access_token, err = token_ok(env)
        if not err:
            client = DhanClient(access_token)
            try:
                err = data_plan_error(client.profile())
            except (kex.TokenException, kex.PermissionException) as e:
                err = f"Dhan access token is invalid or expired ({e}). Paste a new one on the Dhan tab."
            except Exception as e:
                err = f"Could not reach Dhan to check the token: {type(e).__name__}: {e}"
        if err:
            return jsonify({"error": err})
    else:
        access_token = env.get("KITE_ACCESS_TOKEN")
        if not access_token:
            return jsonify({"error": "No access token in .env. Please log in again."})
    lock, err = locked_fno(folder, underlying, "dashboard re-download", source)
    if err:
        return jsonify({"error": err})
    try:
        saved, message = fno.redownload_contract(credentials()[0] if source == "kite" else "", access_token, run_date,
                                                 folder, contract, underlying, read_speed() or DEFAULT_SPEED,
                                                 keep_zero=request.values.get("keep_zero") == "1",
                                                 kite=client, source=source)
        day_dir = fno.day_dir(folder, underlying, run_date, source)
        status, line, rows = bhav.check_saved(day_dir, run_date, underlying) if saved else ("", "", None)
        refresh_live_summary(folder, run_date, rows, source)
    except (bhav.BhavError, OSError) as e:
        return jsonify({"error": str(e)})
    finally:
        lock.release()
    return jsonify({"saved": saved, "message": message, "status": status, "line": line})


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
    if "access_token" not in session and not dhan.status(load_env())["connected"]:
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

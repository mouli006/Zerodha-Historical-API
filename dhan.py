"""Dhan sign-in with an access token generated on web.dhan.co, and the Dhan F&O page after it.

The pasted token is checked with the User Profile API (https://dhanhq.co/docs/v2/authentication),
which also returns the client ID it belongs to and how long it is valid. The F&O page runs the same
daily job as the Kite dashboard (backfill/fno.py) through backfill.dhan_api.DhanClient, saving into
<save folder>/<UNDERLYING>_FnO_1min_Dhan/ (NIFTY from NSE_FNO, SENSEX from BSE_FNO).
"""
import os
import threading
from datetime import date, datetime
from zoneinfo import ZoneInfo

from flask import Blueprint, jsonify, redirect, render_template, request, session, url_for
from kiteconnect import exceptions as kex

from backfill import fno, fno_days
from backfill.dhan_api import MAX_SPEED, DhanClient, DhanError, data_plan_error, fetch_profile, token_connected
from backfill.job import JOB, JOB_LOCK, STOP, job_begin
from backfill.storage import check_folder
from config import (DEFAULT_DATA_DIR, FNO_RAW_KEYS, data_dir, dhan_credentials, keep_zero_volume, load_env, save_env,
                    trim_to_bhav, weekly_expiry_weekday)

bp = Blueprint("dhan", __name__)


@bp.errorhandler(fno.DayFolderError)
def day_folder_error(e):
    """Two folders for one trading day: the F&O routes answer with the folders to fix."""
    return jsonify({"error": str(e)})

IST = ZoneInfo("Asia/Kolkata")
SOURCE = "dhan"
DEFAULT_SPEED = 4.0
UNDERLYINGS = {"NIFTY", "SENSEX"}   # NIFTY from Dhan's NSE_FNO list, SENSEX from BSE_FNO
LIVE = {}   # (IST day, underlying) -> live expiries from Dhan's instrument list, for the date picker


def parse_validity(text):
    """Dhan's tokenValidity ("30/03/2025 15:37", IST) as ISO "2025-03-30T15:37:00"; unknown formats are kept as they are."""
    for fmt in ("%d/%m/%Y %H:%M", "%d/%m/%Y %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text.strip(), fmt).isoformat()
        except ValueError:
            pass
    return text.strip()


def status(env=None):
    """Connection state for the pages from .env (or the given env dict); never includes the token."""
    env = load_env() if env is None else env
    expiry = env.get("DHAN_TOKEN_EXPIRY", "")
    connected = token_connected(env)   # Dhan's validity is IST without an offset
    user = session.get("dhan_user") or {}
    return {"connected": connected, "expiry": expiry.replace("T", " "), "has_token": bool(env.get("DHAN_ACCESS_TOKEN")),
            "client_id": user.get("client_id") or env.get("DHAN_CLIENT_ID", ""), "data_plan": user.get("data_plan", "")}


def broker_info():
    """What the shared dashboard page needs to run its F&O card on Dhan."""
    return {"name": "Dhan", "maxSpeed": MAX_SPEED, "defaultSpeed": DEFAULT_SPEED,
            "urls": {"fnoWindow": url_for("dhan.fno_window_route"), "fno": url_for("dhan.fno_start"),
                     "page": url_for("dhan.dashboard")}}


@bp.route("/dhan")
def index():
    client_id, token = dhan_credentials()
    return render_template("dhan_login.html", error=request.args.get("error"), client_id=client_id,
                           has_token=bool(token), dhan=status(), dashboard=url_for("dhan.dashboard"))


@bp.route("/dhan/login", methods=["POST"])
def login():
    client_id = request.form.get("client_id", "").strip()
    token = request.form.get("access_token", "").strip()
    saved_id, saved_token = dhan_credentials()

    # Keep the saved token if the field was left blank and the client ID is unchanged
    if not token and client_id == saved_id:
        token = saved_token
    if not client_id or not token:
        return redirect(url_for("dhan.index", error="Client ID and access token are required."))

    try:
        profile = fetch_profile(token)
    except DhanError as e:
        return redirect(url_for("dhan.index", error=str(e)))
    if str(profile["dhanClientId"]) != client_id:
        return redirect(url_for("dhan.index", error="This access token belongs to a different Dhan Client ID."))

    expiry = parse_validity(str(profile.get("tokenValidity") or ""))
    save_env({"DHAN_CLIENT_ID": client_id, "DHAN_ACCESS_TOKEN": token, "DHAN_TOKEN_EXPIRY": expiry or None})
    session["dhan_user"] = {"client_id": client_id, "data_plan": profile.get("dataPlan") or ""}
    return redirect(url_for("dhan.dashboard", dhan=1))


@bp.route("/dhan/logout")
def logout():
    # Only forgets the token locally; it stays valid on Dhan's side until it expires or is revoked on web.dhan.co
    save_env({"DHAN_ACCESS_TOKEN": None, "DHAN_TOKEN_EXPIRY": None})
    session.pop("dhan_user", None)
    if request.args.get("next") == "dashboard" and "access_token" in session:
        return redirect(url_for("dashboard.success"))
    return redirect(url_for("dhan.index"))


# --- Dhan F&O page --------------------------------------------------------------------------

def dhan_token():
    """The saved token while it is valid, else ""."""
    env = load_env()
    return env.get("DHAN_ACCESS_TOKEN", "") if status(env)["connected"] else ""


@bp.route("/dhan/dashboard")
def dashboard():
    if not dhan_token():
        return redirect(url_for("dhan.index", error="Connect Dhan first: paste a valid access token."))
    st = status()
    with JOB_LOCK:
        mine = JOB["kind"] == "fno" and JOB["source"] == SOURCE
        speed = JOB["speed"] if mine else DEFAULT_SPEED
        last = JOB["folder"]
    # the last Dhan F&O job's folder, else .env FNO_RAW_DHAN, else the last folder used / KITE_DATA_DIR
    raw = load_env().get(FNO_RAW_KEYS[SOURCE])
    folder = (last if mine and last else "") or raw or last or data_dir()
    form = {"folder": folder, "speed": speed}
    underlyings = [{"name": u, "exchange": fno.exchange_of(u, SOURCE), "bhav": fno.exchange_name(u)}
                   for u in fno.UNDERLYINGS if u in UNDERLYINGS]
    fno_info = {"underlying": "NIFTY", "exchange": fno.exchange_of("NIFTY", SOURCE),
                "underlyings": underlyings, "ready_at": fno.READY_AT.strftime("%H:%M"),
                "today": fno.today_ist().isoformat(), "keep_zero_volume": keep_zero_volume(),
                "trim_to_bhav": trim_to_bhav()}
    user = {"user_id": st["client_id"], "user_name": "", "email": "", "broker": "DHAN", "login_time": ""}
    return render_template("success.html", mode="dhan", broker=broker_info(), user=user, form=form, index_choices=[],
                           today=date.today().isoformat(), fno=fno_info, dhan=st, kite=("access_token" in session),
                           dhan_connected=request.args.get("dhan") == "1", error=request.args.get("error"))


def fno_window(folder, underlying, token):
    """(Window, "") for the date picker from Dhan's live expiries and the Dhan folder's history, or (None, error)."""
    today = fno.today_ist()
    try:
        if (today, underlying) not in LIVE:
            LIVE[(today, underlying)] = fno.live_expiries(
                DhanClient(token).instruments(fno.UNDERLYINGS[underlying][0]), underlying, today)
        history = set(fno.read_expiries(folder, underlying, SOURCE))
        return fno.date_window(LIVE[(today, underlying)], history, today, weekly_expiry_weekday(underlying)), ""
    except (kex.TokenException, kex.PermissionException):
        return None, "Dhan access token is invalid or expired. Paste a new one on the Dhan tab."
    except Exception as e:
        return None, f"Couldn't load the {underlying} expiry list from Dhan: {type(e).__name__}: {e}"


def window_error(run_date, w):
    msg = fno.window_error(run_date, w)
    return msg.replace("Kite", "Dhan") if msg else msg


@bp.route("/dhan/fno-window")
def fno_window_route():
    """Allowed dates for the Dhan F&O card: previous expiry + 1 to today, plus the missing days."""
    token = dhan_token()
    if not token:
        return jsonify({"error": "Dhan is not connected."}), 403
    underlying = request.args.get("underlying", "NIFTY")
    if underlying not in UNDERLYINGS:
        return jsonify({"error": f"Unknown underlying {underlying}."})
    folder = os.path.expanduser(request.args.get("folder", "").strip() or DEFAULT_DATA_DIR)
    if not os.path.isabs(folder):
        return jsonify({"error": "Enter the save folder as a full path."})
    folder = os.path.normpath(folder)
    w, err = fno_window(folder, underlying, token)
    if err:
        return jsonify({"error": err})
    return jsonify({"min_date": w.min_date.isoformat(), "max_date": w.max_date.isoformat(),
                    "current_expiry": w.current_expiry.isoformat(), "previous_expiry": w.previous_expiry.isoformat(),
                    "estimated": w.estimated, "today_is_expiry": w.current_expiry == w.max_date,
                    "missing": [d.isoformat() for d in fno.missing_dates(folder, underlying, w, source=SOURCE)],
                    "days": fno_days.day_summary(fno.fno_root(folder, underlying, SOURCE))})


@bp.route("/dhan/fno-start", methods=["POST"])
def fno_start():
    """Dhan F&O run for one picked date, a picked range (date_from/date_to), or for every missing
    date (missing=1), or a re-request of every contract of a saved date without a file (refetch=1,
    needs keep_zero=1): the same jobs as the Kite card. keep_zero=1 keeps contracts whose candles all
    have volume 0; trim_bhav=1|0 downloads only the contracts in the day's bhavcopy (absent: the setting)."""
    token = dhan_token()
    if not token:
        return redirect(url_for("dhan.index", error="Dhan access token is missing or expired. Paste a new one."))

    def fail(msg):
        return redirect(url_for("dhan.dashboard", error=msg))

    underlying = request.form.get("underlying", "NIFTY")
    if underlying not in UNDERLYINGS:
        return fail(f"Unknown underlying {underlying}.")
    missing = bool(request.form.get("missing"))
    keep_zero = request.form.get("keep_zero") == "1"
    trim = {"1": True, "0": False}.get(request.form.get("trim_bhav", ""))
    if trim is None:
        trim = trim_to_bhav()
    refetch = bool(request.form.get("refetch")) and not missing
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
    try:
        speed = float(request.form.get("speed", DEFAULT_SPEED))
    except ValueError:
        speed = 0
    if not 0 < speed <= MAX_SPEED:
        return fail(f"Speed must be between 0 and {MAX_SPEED:g} requests per second.")
    folder, err = check_folder(request.form.get("folder", "") or DEFAULT_DATA_DIR)
    if err:
        return fail(err)
    client = DhanClient(token)
    try:
        profile = client.profile()
    except kex.TokenException as e:
        return redirect(url_for("dhan.index", error=str(e)))
    err = data_plan_error(profile)
    if err:
        return fail(err)
    w, err = fno_window(folder, underlying, token)
    if err:
        return fail(err)
    if missing:
        if not fno.missing_dates(folder, underlying, w, source=SOURCE):
            return fail(f"Nothing missing: every day from {w.min_date} to {w.max_date} is saved or a holiday.")
    elif dates:
        err = window_error(dates[0], w) or window_error(dates[-1], w) or fno.too_early(dates[-1])
        if err:
            return fail(err)
    else:
        err = window_error(run_date, w) or fno.too_early(run_date)
        if err:
            return fail(err)
        saved = fno.day_dir(folder, underlying, run_date, SOURCE)
        if refetch and not (saved and os.path.exists(os.path.join(saved, "summary.csv"))):
            return fail(f"{run_date} has not been downloaded in this folder yet; download the day first.")
    try:
        lock = fno.acquire_lock(folder, underlying, "dhan dashboard", SOURCE)
    except fno.LockBusy as e:
        return fail(str(e))
    label = f"{dates[0]}_to_{dates[-1]}" if dates else "missing"
    if not job_begin(**fno.job_fields(underlying, run_date, folder, speed, keep_zero, SOURCE, label, trim)):
        lock.release()
        return fail("A download is already running. Stop it or wait for it to finish.")
    if not load_env().get(FNO_RAW_KEYS[SOURCE]):   # with FNO_RAW_DHAN set, KITE_DATA_DIR stays the planner's
        save_env({"KITE_DATA_DIR": folder})
    STOP.clear()
    user = profile.get("user_id", "")

    def work():
        try:
            if missing or dates:
                fno.run_missing("", token, folder, speed, underlying, weekday=weekly_expiry_weekday(underlying),
                                keep_zero=keep_zero, kite=client, user=user, source=SOURCE, dates=dates,
                                trim_to_bhav=trim)
            elif refetch:
                fno.refetch_no_trade("", token, run_date, folder, speed, underlying, weekday=weekly_expiry_weekday(underlying),
                                     kite=client, user=user, source=SOURCE, trim_to_bhav=trim)
            else:
                fno.run_daily("", token, run_date, folder, speed, underlying, weekday=weekly_expiry_weekday(underlying),
                              kite=client, user=user, keep_zero=keep_zero, source=SOURCE, trim_to_bhav=trim)
        finally:
            lock.release()

    threading.Thread(target=work, daemon=True).start()
    return redirect(url_for("dhan.dashboard"))

"""Sign-in pages: Kite Connect login, the OAuth-style callback and logout."""
from flask import Blueprint, redirect, render_template, request, session, url_for
from kiteconnect import KiteConnect

from config import credentials, save_env

bp = Blueprint("auth", __name__)


def kite_client():
    kite = KiteConnect(api_key=credentials()[0])
    if "access_token" in session:
        kite.set_access_token(session["access_token"])
    return kite


@bp.route("/")
def index():
    if "access_token" in session:
        return redirect(url_for("dashboard.success"))
    api_key, api_secret = credentials()
    return render_template("login.html", error=request.args.get("error"),
                           api_key=api_key, has_secret=bool(api_secret))


@bp.route("/login", methods=["POST"])
def login():
    api_key = request.form.get("api_key", "").strip()
    api_secret = request.form.get("api_secret", "").strip()
    saved_key, saved_secret = credentials()

    # Keep the saved secret if the field was left blank and the key is unchanged
    if not api_secret and api_key == saved_key:
        api_secret = saved_secret
    if not api_key or not api_secret:
        return redirect(url_for("auth.index", error="API key and API secret are required."))

    save_env({"KITE_API_KEY": api_key, "KITE_API_SECRET": api_secret})
    # Sends the user to Zerodha's login page
    return redirect(KiteConnect(api_key=api_key).login_url())


@bp.route("/callback")
def callback():
    # Kite redirects here with ?request_token=...&status=success
    if request.args.get("status") != "success" or "request_token" not in request.args:
        return redirect(url_for("auth.index", error="Login was cancelled or failed."))
    try:
        api_key, api_secret = credentials()
        data = KiteConnect(api_key=api_key).generate_session(
            request.args["request_token"], api_secret=api_secret
        )
    except Exception as e:
        return redirect(url_for("auth.index", error=str(e)))
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
    return redirect(url_for("dashboard.success"))


@bp.route("/logout")
def logout():
    try:
        kite_client().invalidate_access_token()
    except Exception:
        pass
    save_env({"KITE_ACCESS_TOKEN": None})
    session.clear()
    return redirect(url_for("auth.index"))

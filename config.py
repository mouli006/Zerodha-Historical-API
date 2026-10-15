import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE_DIR, ".env")
DEFAULT_DATA_DIR = os.path.join(BASE_DIR, "data")


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


def dhan_credentials():
    """Dhan Client ID and access token from .env (read on every request, like credentials())."""
    env = load_env()
    return env.get("DHAN_CLIENT_ID", ""), env.get("DHAN_ACCESS_TOKEN", "")


def secret_key():
    """Flask session key, generated once and kept in .env so sessions survive restarts."""
    if "FLASK_SECRET_KEY" not in load_env():
        save_env({"FLASK_SECRET_KEY": os.urandom(24).hex()})
    return load_env()["FLASK_SECRET_KEY"]


def data_dir():
    """Save folder chosen on the form (remembered in .env), or <project>/data."""
    return load_env().get("KITE_DATA_DIR") or DEFAULT_DATA_DIR


# F&O folders (backfill/fno_store.py): the raw download folder per source and the backtest store. Optional;
# without FNO_RAW_<SOURCE> the F&O cards and CLIs use KITE_DATA_DIR as before.
FNO_RAW_KEYS = {"kite": "FNO_RAW_KITE", "dhan": "FNO_RAW_DHAN"}


def fno_raw_dir(source="kite", env=None):
    """The F&O save folder of a source: .env FNO_RAW_KITE / FNO_RAW_DHAN, else KITE_DATA_DIR, else <project>/data.
    env: an already loaded .env (callers pass their own load_env() so tests' stubs apply)."""
    env = load_env() if env is None else env
    return env.get(FNO_RAW_KEYS[source]) or env.get("KITE_DATA_DIR") or DEFAULT_DATA_DIR


def fno_backtest_dir(env=None):
    """.env FNO_BACKTEST_DIR (the backtest store, built by fno_store.py publish), or ""."""
    env = load_env() if env is None else env
    return env.get("FNO_BACKTEST_DIR", "")


WEEKDAYS = ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]


# Weekly expiry weekday per F&O underlying (only used when the expiry history is too short)
WEEKLY_EXPIRY_DEFAULTS = {"NIFTY": "TUE", "SENSEX": "THU"}


def weekly_expiry_weekday(underlying="NIFTY"):
    """The underlying's weekly expiry weekday as Monday = 0: .env WEEKLY_EXPIRY_WEEKDAY_<UNDERLYING>
    (MON..SUN or Monday..Sunday), for NIFTY also the older WEEKLY_EXPIRY_WEEKDAY; default
    WEEKLY_EXPIRY_DEFAULTS (NIFTY Tuesday, SENSEX Thursday)."""
    env = load_env()
    keys = [f"WEEKLY_EXPIRY_WEEKDAY_{underlying}"] + (["WEEKLY_EXPIRY_WEEKDAY"] if underlying == "NIFTY" else [])
    for key in keys:
        value = env.get(key, "").strip().upper()[:3]
        if value in WEEKDAYS:
            return WEEKDAYS.index(value)
    return WEEKDAYS.index(WEEKLY_EXPIRY_DEFAULTS.get(underlying, "TUE"))


# "Keep contracts with candles but zero volume" (backfill/fno.py): Kite can report a contract that
# traded once as candles with volume 0. False = no file for those (the old rule); .env KEEP_ZERO_VOLUME
# (ON/OFF) overrides this default.
KEEP_ZERO_VOLUME = False


def keep_zero_volume():
    value = load_env().get("KEEP_ZERO_VOLUME", "").strip().upper()
    if value in ("1", "ON", "TRUE", "YES", "Y"):
        return True
    if value in ("0", "OFF", "FALSE", "NO", "N"):
        return False
    return KEEP_ZERO_VOLUME


# "Download only contracts in the bhavcopy" (backfill/fno.py trim_contracts), for Kite and Dhan: a day's run
# requests only the contracts in that day's NSE/BSE bhavcopy (traded or not); without a bhavcopy the full
# list is used. .env TRIM_TO_BHAV (ON/OFF) overrides this default.
TRIM_TO_BHAV = True


def trim_to_bhav():
    value = load_env().get("TRIM_TO_BHAV", "").strip().upper()
    if value in ("1", "ON", "TRUE", "YES", "Y"):
        return True
    if value in ("0", "OFF", "FALSE", "NO", "N"):
        return False
    return TRIM_TO_BHAV

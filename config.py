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


def secret_key():
    """Flask session key, generated once and kept in .env so sessions survive restarts."""
    if "FLASK_SECRET_KEY" not in load_env():
        save_env({"FLASK_SECRET_KEY": os.urandom(24).hex()})
    return load_env()["FLASK_SECRET_KEY"]


def data_dir():
    """Save folder chosen on the form (remembered in .env), or <project>/data."""
    return load_env().get("KITE_DATA_DIR") or DEFAULT_DATA_DIR


WEEKDAYS = ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]


def weekly_expiry_weekday():
    """WEEKLY_EXPIRY_WEEKDAY from .env (MON..SUN or Monday..Sunday) as Monday = 0; default Tuesday."""
    value = load_env().get("WEEKLY_EXPIRY_WEEKDAY", "").strip().upper()[:3]
    return WEEKDAYS.index(value) if value in WEEKDAYS else 1


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

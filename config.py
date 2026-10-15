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


def fno_raw_dir(source="kite", variant=None, env=None):
    """The F&O save folder of a source: .env FNO_RAW_KITE / FNO_RAW_DHAN, else KITE_DATA_DIR, else <project>/data.
    variant "with" / "without": the store of that zero-volume setting under FNO_RAW_<SOURCE> (VARIANTS); without
    the key set there are no separate stores and the folder is the same as without a variant.
    env: an already loaded .env (callers pass their own load_env() so tests' stubs apply)."""
    env = load_env() if env is None else env
    base = env.get(FNO_RAW_KEYS[source])
    if base and variant:
        return os.path.join(base, VARIANTS[variant])
    return base or env.get("KITE_DATA_DIR") or DEFAULT_DATA_DIR


# "With zero volume" and "without zero volume" F&O data are kept apart: each is a normal save folder under
# FNO_RAW_<SOURCE>. A download with keep zero-volume ON goes to the first, OFF to the second; the without copy
# of a with day can be derived (backfill/fno_derive.py) because it is the with day minus the zero-volume files.
VARIANTS = {"with": "with_zero_volume", "without": "without_zero_volume"}


def variant_of(keep_zero):
    return "with" if keep_zero else "without"


def store_variant(folder):
    """"with" / "without" when folder is one of the two stores (by its name), else None."""
    name = os.path.basename(os.path.normpath(folder or ""))
    return next((v for v, n in VARIANTS.items() if n.lower() == name.lower()), None)


def same_path(a, b):
    return bool(a and b) and os.path.normcase(os.path.normpath(a)) == os.path.normcase(os.path.normpath(b))


def is_fno_base(folder, source, env=None):
    """True when folder is the .env FNO_RAW_<SOURCE> base (the folder the cards and CLIs start from)."""
    env = load_env() if env is None else env
    return same_path(folder, env.get(FNO_RAW_KEYS[source]))


def fno_store_dir(folder, source, keep_zero, env=None):
    """The folder an F&O run writes to: the base FNO_RAW_<SOURCE> becomes the store of the zero-volume setting;
    any other folder is used as it is."""
    env = load_env() if env is None else env
    if is_fno_base(folder, source, env):
        return fno_raw_dir(source, variant_of(keep_zero), env)
    return folder


def split_stores_on(source, env=None):
    """True when FNO_RAW_<SOURCE> is set, so the source's F&O data is kept in the two stores."""
    env = load_env() if env is None else env
    return bool(env.get(FNO_RAW_KEYS[source]))


def default_fno_folder(source, keep_zero=None, env=None):
    """The CLIs' default F&O folder: the store of keep_zero (None: the setting, keep_zero_volume()) under
    FNO_RAW_<SOURCE>, else KITE_DATA_DIR, else <project>/data."""
    env = load_env() if env is None else env
    if not split_stores_on(source, env):
        return fno_raw_dir(source, env=env)
    return fno_raw_dir(source, variant_of(keep_zero_volume() if keep_zero is None else keep_zero), env)


def fno_target(folder, source, keep_zero, derive=None, env=None):
    """(folder an F&O run writes to, without store to derive into or None) for a folder from a card or the CLI:
    the base FNO_RAW_<SOURCE> becomes the store of keep_zero, and an ON run there also derives the without copy
    when derive is on (None: the setting, derive_without()). Any other folder is used as it is, never derived."""
    env = load_env() if env is None else env
    if not is_fno_base(folder, source, env):
        return folder, None
    store = fno_raw_dir(source, variant_of(keep_zero), env)
    if not keep_zero or not (derive_without() if derive is None else derive):
        return store, None
    return store, fno_raw_dir(source, "without", env)


def store_base(folder, source, env=None):
    """A store folder of FNO_RAW_<SOURCE> back to the base (the Dhan page's remembered folder), else folder."""
    env = load_env() if env is None else env
    if store_variant(folder) and is_fno_base(os.path.dirname(os.path.normpath(folder)), source, env):
        return env.get(FNO_RAW_KEYS[source])
    return folder


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


def on_off_setting(key, default):
    value = load_env().get(key, "").strip().upper()
    if value in ("1", "ON", "TRUE", "YES", "Y"):
        return True
    if value in ("0", "OFF", "FALSE", "NO", "N"):
        return False
    return default


def trim_to_bhav():
    return on_off_setting("TRIM_TO_BHAV", TRIM_TO_BHAV)


# "Also save a without-zero-volume copy" (backfill/fno_derive.py): after a keep zero-volume ON run of a day into
# the with store, the day minus its zero-volume files is written into the without store (unless it has the day).
# .env DERIVE_WITHOUT_ZERO_VOLUME (ON/OFF) overrides this default.
DERIVE_WITHOUT_ZERO_VOLUME = True


def derive_without():
    return on_off_setting("DERIVE_WITHOUT_ZERO_VOLUME", DERIVE_WITHOUT_ZERO_VOLUME)

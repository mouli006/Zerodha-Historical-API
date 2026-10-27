"""The special-sessions calendar: Muhurat sessions, budget weekends, special Saturdays and outage days, kept by hand in
manual_special_sessions.csv (docs/dev/special_sessions.md). Read only; nothing here writes a file.

    <folder>/NSE_CM_bhav/reference/manual_special_sessions.csv   the data copy (wins when it exists)
    <repo>/reference/manual_special_sessions.csv                  the backup in the repo (used otherwise)

Columns: date, weekday (Mon..Sun), kind (KINDS), start, end (HH:MM IST, blank until known), in_bhavcopy (yes / no),
evidence, note. cm_bhav.py --special asks NSE for the weekend dates (weekend_dates)."""
import os
import re
from datetime import date

import pandas as pd

import config

FILE = "manual_special_sessions.csv"
COLUMNS = ["date", "weekday", "kind", "start", "end", "in_bhavcopy", "evidence", "note"]
KINDS = ("MUHURAT", "BUDGET_WEEKEND", "SPECIAL_SATURDAY", "OUTAGE")
WEEKDAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
STORE, REFERENCE = "NSE_CM_bhav", "reference"   # the data copy sits next to manual_mergers.csv
REPO_COPY = os.path.join(config.BASE_DIR, "reference", FILE)
TIME_RE = re.compile(r"([01]\d|2[0-3]):[0-5]\d")


class SpecialSessionsError(Exception):
    """A bad or missing special-sessions file; the message names the file."""


def data_copy(folder=None):
    return os.path.join(folder or config.equity_data_dir(), STORE, REFERENCE, FILE)


def path_of(folder=None):
    """The data copy when it exists, else the repo backup."""
    path = data_copy(folder)
    return path if os.path.isfile(path) else REPO_COPY


def load_special_sessions(folder=None):
    """Every row of the file, sorted by date (date as datetime.date, the rest as text). SpecialSessionsError on a
    missing file or column, a bad date / weekday / kind / time / in_bhavcopy, or a date listed twice."""
    path = path_of(folder)
    if not os.path.isfile(path):
        raise SpecialSessionsError(f"No {path}.")
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    df.columns = [c.strip().lower() for c in df.columns]
    missing = [c for c in COLUMNS if c not in df.columns]
    if missing:
        raise SpecialSessionsError(f"{path}: missing column(s) {', '.join(missing)}")
    df = df[COLUMNS].apply(lambda s: s.str.strip())
    days = []
    for i, r in enumerate(df.itertuples(index=False), 2):   # line 1 is the header
        where = f"{path} line {i}"
        try:
            d = date.fromisoformat(r.date)
        except ValueError:
            raise SpecialSessionsError(f"{where}: bad date {r.date!r} (use YYYY-MM-DD)") from None
        if r.weekday != WEEKDAYS[d.weekday()]:
            raise SpecialSessionsError(f"{where}: {d} is a {WEEKDAYS[d.weekday()]}, not {r.weekday!r}")
        if r.kind not in KINDS:
            raise SpecialSessionsError(f"{where}: unknown kind {r.kind!r} (one of {', '.join(KINDS)})")
        for name in ("start", "end"):
            value = getattr(r, name)
            if value and not TIME_RE.fullmatch(value):
                raise SpecialSessionsError(f"{where}: bad {name} {value!r} (use HH:MM or leave blank)")
        if r.in_bhavcopy not in ("yes", "no"):
            raise SpecialSessionsError(f"{where}: in_bhavcopy must be yes or no, not {r.in_bhavcopy!r}")
        days.append(d)
    df["date"] = days
    twice = sorted(df.loc[df["date"].duplicated(), "date"].unique())
    if twice:
        raise SpecialSessionsError(f"{path}: date(s) listed twice: {', '.join(map(str, twice))}")
    return df.sort_values("date", kind="stable").reset_index(drop=True)


def session_kind(day, folder=None):
    """The kind of a special session on day (a date or YYYY-MM-DD), or None."""
    day = date.fromisoformat(day) if isinstance(day, str) else day
    df = load_special_sessions(folder)
    hit = df.loc[df["date"] == day, "kind"]
    return hit.iat[0] if len(hit) else None


def is_special(day, folder=None):
    return session_kind(day, folder) is not None


def weekend_dates(folder=None):
    """The Saturday / Sunday sessions in the file, oldest first (cm_bhav.py --special asks NSE for these)."""
    return [d for d in load_special_sessions(folder)["date"] if d.weekday() >= 5]

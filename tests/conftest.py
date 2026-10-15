"""Shared test setup. No test may reach NSE: Kite runs don't fetch the bhavcopy after a day unless a
test turns fno.AUTO_BHAV on again (and stubs bhav.fetch_bhav), and the "download only contracts in the
bhavcopy" setting is OFF (never read from .env) unless a test turns it on (trim_on) with the fetch stubbed."""
import pytest

import config
import dashboard
import dhan
import nifty_daily
from backfill import fno


@pytest.fixture(autouse=True)
def no_auto_bhav(monkeypatch):
    monkeypatch.setattr(fno, "AUTO_BHAV", False)


def set_trim(monkeypatch, on):
    """The trim setting as config.trim_to_bhav() and the routes/CLI see it (they import it by name)."""
    for mod in (config, dashboard, dhan, nifty_daily):
        monkeypatch.setattr(mod, "trim_to_bhav", lambda: on)


@pytest.fixture(autouse=True)
def trim_off(monkeypatch):
    set_trim(monkeypatch, False)


@pytest.fixture
def trim_on(monkeypatch):
    set_trim(monkeypatch, True)


def set_derive(monkeypatch, on):
    """The "also save a without-zero-volume copy" setting as config.derive_without() and the routes/CLI see it."""
    for mod in (config, dashboard, dhan, nifty_daily):
        monkeypatch.setattr(mod, "derive_without", lambda: on)


@pytest.fixture(autouse=True)
def derive_off(monkeypatch):
    """Never read from .env; tests that derive turn it on (derive_on) or pass derive_to / derive=1."""
    set_derive(monkeypatch, False)


@pytest.fixture
def derive_on(monkeypatch):
    set_derive(monkeypatch, True)


def day_of(root, day):
    """The day folder of day in an F&O root (old or new name), or root/<day> when there is none yet."""
    found = fno.find_day_dir(str(root), str(day))
    return type(root)(found) if found else root / str(day)

"""Shared test setup. No test may reach NSE: Kite runs don't fetch the bhavcopy after a day unless a
test turns fno.AUTO_BHAV on again (and stubs bhav.fetch_bhav)."""
import pytest

from backfill import fno


@pytest.fixture(autouse=True)
def no_auto_bhav(monkeypatch):
    monkeypatch.setattr(fno, "AUTO_BHAV", False)


def day_of(root, day):
    """The day folder of day in an F&O root (old or new name), or root/<day> when there is none yet."""
    found = fno.find_day_dir(str(root), str(day))
    return type(root)(found) if found else root / str(day)

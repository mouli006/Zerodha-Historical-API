"""Shared test setup. No test may reach NSE: Kite runs don't fetch the bhavcopy after a day unless a
test turns fno.AUTO_BHAV on again (and stubs bhav.fetch_bhav)."""
import pytest

from backfill import fno


@pytest.fixture(autouse=True)
def no_auto_bhav(monkeypatch):
    monkeypatch.setattr(fno, "AUTO_BHAV", False)

import pytest

from backfill import fno
from backfill.fno_loader import load_contract, load_day


def bar(day, hhmm, close, vol=10):
    return {"date": f"{day}T{hhmm}:00+0530", "open": close, "high": close, "low": close,
            "close": close, "volume": vol, "oi": 100}


def save_day(root, day, contracts=("NIFTY27OCT26", "NIFTY2500013OCT26CE")):
    day_dir = root / "NIFTY_FnO_1min" / day
    for name in contracts:
        sub = "options" if name.endswith(("CE", "PE")) else "futures"
        fno.write_contract(str(day_dir / sub / f"{name}.csv"), name,
                           [bar(day, "09:15", 100.0), bar(day, "09:16", 101.5)])
    fno.write_csv(str(day_dir / "summary.csv"), fno.SUMMARY_COLUMNS, [])
    return day_dir


def test_load_day(tmp_path):
    save_day(tmp_path, "2026-10-12")
    df = load_day(str(tmp_path), "2026-10-12")
    assert sorted(df["Ticker"].unique()) == ["NIFTY2500013OCT26CE", "NIFTY27OCT26"]
    assert str(df["Datetime"].iloc[0]) == "2026-10-12 09:15:00"
    assert list(df.columns) == ["Datetime", *fno.HEADER]


def test_load_day_kind(tmp_path):
    save_day(tmp_path, "2026-10-09")
    assert list(load_day(str(tmp_path), "2026-10-09", kind="futures")["Ticker"].unique()) == ["NIFTY27OCT26"]


def test_load_day_missing(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_day(str(tmp_path), "2026-10-12")


def test_load_contract_across_days(tmp_path):
    save_day(tmp_path, "2026-10-09")
    save_day(tmp_path, "2026-10-12")
    df = load_contract(str(tmp_path), "NIFTY27OCT26")
    assert [str(t) for t in df["Datetime"]] == ["2026-10-09 09:15:00", "2026-10-09 09:16:00",
                                                "2026-10-12 09:15:00", "2026-10-12 09:16:00"]


def test_load_contract_option(tmp_path):
    save_day(tmp_path, "2026-10-12")
    df = load_contract(str(tmp_path), "NIFTY2500013OCT26CE")
    assert len(df) == 2 and df["Close"].tolist() == [100, 101.5]

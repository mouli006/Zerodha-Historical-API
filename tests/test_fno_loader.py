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
    assert list(df.columns) == ["Datetime", *fno.HEADER, "OI_filled"]


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



# --- OI 0 inside a day is a missing value, filled from the previous minute --------------------------

def save_oi(root, day, name, ois):
    """One file whose minutes 09:15, 09:16, ... carry these OI values."""
    sub = "options" if name.endswith(("CE", "PE")) else "futures"
    path = root / "NIFTY_FnO_1min" / day / sub / f"{name}.csv"
    bars = [{**bar(day, f"09:{15 + k:02d}", 100.0 + k), "oi": oi} for k, oi in enumerate(ois)]
    fno.write_contract(str(path), name, bars)
    return path


def oi_of(df):
    return [None if v != v else v for v in df["OI"].tolist()], df["OI_filled"].tolist()


def test_zero_oi_in_the_middle_is_filled(tmp_path):
    save_oi(tmp_path, "2026-10-12", "NIFTY27OCT26", [100, 0, 0, 130])
    assert oi_of(load_contract(str(tmp_path), "NIFTY27OCT26")) == ([100, 100, 100, 130], [False, True, True, False])


def test_leading_zero_oi_stays_missing(tmp_path):
    save_oi(tmp_path, "2026-10-12", "NIFTY27OCT26", [0, 0, 120, 0])
    assert oi_of(load_contract(str(tmp_path), "NIFTY27OCT26")) == ([None, None, 120, 120], [False, False, False, True])


def test_zero_oi_all_day_is_left_alone(tmp_path):
    save_oi(tmp_path, "2026-10-12", "NIFTY2500013OCT26CE", [0, 0, 0])
    assert oi_of(load_day(str(tmp_path), "2026-10-12")) == ([0, 0, 0], [False, False, False])


def test_fill_never_crosses_days(tmp_path):
    save_oi(tmp_path, "2026-10-09", "NIFTY27OCT26", [100, 110])
    save_oi(tmp_path, "2026-10-12", "NIFTY27OCT26", [0, 150])
    ois, filled = oi_of(load_contract(str(tmp_path), "NIFTY27OCT26"))
    assert ois == [100, 110, None, 150] and filled == [False, False, False, False]


def test_raw_values_and_files_unchanged(tmp_path):
    path = save_oi(tmp_path, "2026-10-12", "NIFTY27OCT26", [100, 0, 130])
    before = path.read_bytes()
    raw = load_day(str(tmp_path), "2026-10-12", fill_oi=False)
    assert raw["OI"].tolist() == [100, 0, 130] and list(raw.columns) == ["Datetime", *fno.HEADER]
    assert load_day(str(tmp_path), "2026-10-12")["OI"].tolist() == [100, 100, 130]
    assert path.read_bytes() == before

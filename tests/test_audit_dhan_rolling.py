"""Data audit, Dhan rolling (#254): backfill/equity/audit_dhan_rolling.py through audit.run on a synthetic world (no
network, no secrets file). The rolling folder is hand-made in the #219 layout: by_strike.parquet, bhav_compare.csv and a
tiny UDiFF bhavcopy zip. A flag only: reasons, kinds and the exit code never change."""
import io
import json
import os
import zipfile

import pandas as pd

from backfill import fno
from backfill.equity import audit
from backfill.equity import audit_dhan_rolling as dr
from backfill.equity import minute_plan as mp
from test_audit import WEEK, World, d, mrow

IST = mp.IST
EXP = "2026-11-18"
LOST_DAY = "2026-11-18"


def ts(day, hhmm):
    return pd.Timestamp(f"{day} {hhmm}", tz=IST)


def bars():
    """18 Nov: CE 25000 and PE 25000 every minute 09:15-09:19 (full), CE 25050 2 minutes, PE 24950 3 minutes
    (partly) = 4 contracts, 15 bars; 17 Nov: CE 25000 2 minutes (a day with no LOST row)."""
    rows = []
    mins = ["09:15", "09:16", "09:17", "09:18", "09:19"]
    for m in mins:
        rows.append((LOST_DAY, m, "CE", "ATM", 25000.0))
        rows.append((LOST_DAY, m, "PE", "ATM", 25000.0))
    rows += [(LOST_DAY, m, "CE", "ATM+1", 25050.0) for m in mins[:2]]
    rows += [(LOST_DAY, m, "PE", "ATM-1", 24950.0) for m in mins[2:]]
    rows += [("2026-11-17", m, "CE", "ATM", 25000.0) for m in mins[:2]]
    return pd.DataFrame([{"timestamp_ist": ts(day, m), "date": day, "option_type": t, "label": lb, "strike": k,
                          "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 65.0, "oi": 1.0, "iv": 1.0,
                          "spot": 25000.0} for day, m, t, lb, k in rows])


COMPARE = [  # date, type, strike, bhav volume (contracts): traded 4 = full 2 + partly 1 + none 1; PE 24950 not traded
    (LOST_DAY, "CE", 25000, 10), (LOST_DAY, "PE", 25000, 5), (LOST_DAY, "CE", 25050, 3), (LOST_DAY, "PE", 24950, 0),
    (LOST_DAY, "CE", 25100, 7)]


def bhav_zip(path):
    """UDiFF rows: NIFTY options traded 6 (one more at volume 0), 1 NIFTY future traded, 1 BANKNIFTY row."""
    rows = [("NIFTY", "CE", 25000 + 50 * i, "2026-11-18", 1) for i in range(6)]
    rows += [("NIFTY", "PE", 24000, "2026-11-25", 0), ("NIFTY", "", "", "2026-11-25", 9),
             ("BANKNIFTY", "CE", 52000, "2026-11-25", 4)]
    df = pd.DataFrame(rows, columns=["TckrSymb", "OptnTp", "StrkPric", "XpryDt", "TtlTradgVol"])
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("BhavCopy_NSE_FO_0_0_0_20261118_F_0000.csv", df.to_csv(index=False))
    with open(path, "wb") as f:
        f.write(buf.getvalue())


def rolling(root, compare=True, zip_=True):
    """<root>/NIFTY/exp-2026-11-18/ in the #219 layout; returns the folder."""
    p = os.path.join(root, "NIFTY", f"exp-{EXP}")
    os.makedirs(os.path.join(p, "bhav"), exist_ok=True)
    bars().to_parquet(os.path.join(p, dr.DATA), index=False)
    if compare:
        pd.DataFrame([{"date": x, "option_type": t, "strike": float(k), "coverage": "", "bhav_volume_contracts": float(v)}
                      for x, t, k, v in COMPARE]).to_csv(os.path.join(p, dr.COMPARE), index=False)
    if zip_:
        bhav_zip(os.path.join(p, "bhav", "BhavCopy_NSE_FO_0_0_0_20261118_F_0000.csv.zip"))
    return p


def world(tmp_path, monkeypatch, with_folder=True, unchecked=False, **kw):
    """NIFTY 18 Nov missing in both sources (manual LOST, 4 gaps); unchecked=True adds a SENSEX UNEXPLAINED day."""
    def rows(v, u):
        return [{"date": x.isoformat(), "kite_available": "True", "dhan_available": "True",
                 "kite_check": "" if (unchecked and u == "SENSEX" and v == "with" and x == d(17)) else "major",
                 "dhan_check": "major", "best_source": "dhan"} for x in WEEK if not (u == "NIFTY" and x == d(18))]
    w = World(tmp_path, monkeypatch, fno_rows=rows,
              manual=[mrow("fno", "LOST", LOST_DAY, underlying="NIFTY", pid="P188", note="both sources")])
    if with_folder:
        rolling(dr.default_root(w.fno), **kw)
    return w


def table(w):
    return pd.read_csv(os.path.join(w.out(), "fno_dhan_rolling.csv"), dtype=str, keep_default_na=False)


def run_info(w):
    with open(os.path.join(w.out(), "run_info.json"), encoding="utf-8") as f:
        return json.load(f)


def test_counts_per_day_and_the_csv(tmp_path, monkeypatch):                                     # 1
    w = world(tmp_path, monkeypatch)
    assert w.run(stores=["fno"]) == 0
    rep = w.report()
    assert "Dhan rolling (#254)" in rep and dr.default_root(w.fno) in rep
    assert ("NIFTY 2026-11-18 exp 2026-11-18: 4 contracts (CE 2, PE 2), 15 bars, 5 minutes 09:15-09:19, "
            "strikes 24950..25050;") in rep
    assert "bhavcopy traded of this expiry 4: full 2, partly 1, none 1; all traded 7 (options 6 + futures 1): rolling 4" \
        in rep
    t = table(w)
    assert list(t.columns) == dr.COLUMNS
    t18 = t[t["date"] == LOST_DAY]
    assert len(t18) == 5 and set(t18["lost_row"]) == {"yes"}
    by = dict(zip(t18["contract"], t18["coverage"]))
    assert by == {"NIFTY2500018NOV26CE": "full", "NIFTY2500018NOV26PE": "full", "NIFTY2505018NOV26CE": "partly",
                  "NIFTY2495018NOV26PE": "partly", "NIFTY2510018NOV26CE": "none"}
    ce = t18[t18["contract"] == "NIFTY2505018NOV26CE"].iloc[0]
    assert (ce["minutes"], ce["bars"], ce["first"], ce["last"], ce["labels"], ce["bhav_volume"]) == \
        ("2", "2", "09:15", "09:16", "ATM+1", "3")
    assert run_info(w)["dhan_rolling"]["rows"] == len(t)


def test_contract_name_copy_matches_the_fno_rule():                                             # 2
    for u, exp, typ, k in [("NIFTY", "2026-11-09", "CE", 25000.0), ("NIFTY", "2026-10-27", "PE", 24350),
                           ("SENSEX", "2026-11-12", "PE", "66600"), ("NIFTY", "2027-01-05", "CE", 12000.0)]:
        assert dr.contract_name(u, exp, typ, k) == fno.contract_name(typ, exp, k, u)
    assert dr.contract_name("NIFTY", "2026-11-09", "CE", 25000.0) == "NIFTY2500009NOV26CE"


def test_evidence_only_on_lost_rows_and_nothing_else_changes(tmp_path, monkeypatch):            # 3
    a = world(tmp_path / "a", monkeypatch, with_folder=False, unchecked=True)
    code_a = a.run(stores=["fno"])
    ga = a.gaps()
    b = world(tmp_path / "b", monkeypatch, unchecked=True)
    code_b = b.run(stores=["fno"])
    gb = b.gaps()
    assert code_a == code_b == 1
    cols = ["store", "underlying", "source", "variant", "date", "reason", "kind", "clears_when"]
    assert ga[cols].equals(gb[cols])
    assert open(os.path.join(a.out(), "summary.csv")).read() == open(os.path.join(b.out(), "summary.csv")).read()
    lost = gb[gb["reason"] == "LOST"]
    assert len(lost) == 4
    text = ("Dhan rolling exp 2026-11-18: 4 contracts, 15 bars (traded of that expiry: full 2, partly 1 of 4); "
            "fno_dhan_rolling.csv")
    assert all(e.startswith("P188: both sources | ") and e.endswith(text) for e in lost["evidence"])
    other = gb[gb["reason"] != "LOST"]
    assert list(other["evidence"]) == list(ga[ga["reason"] != "LOST"]["evidence"])


def test_folder_missing_is_a_flag_line(tmp_path, monkeypatch):                                  # 4
    w = world(tmp_path, monkeypatch, with_folder=False)
    assert w.run(stores=["fno"]) == 0
    assert f"Dhan rolling folder not found: {dr.default_root(w.fno)}" in w.report()
    t = table(w)
    assert list(t.columns) == dr.COLUMNS and len(t) == 0
    assert run_info(w)["dhan_rolling"]["found"] is False
    assert all("Dhan rolling" not in e for e in w.gaps()["evidence"])


def test_a_broken_parquet_is_not_read_and_the_exit_code_stays(tmp_path, monkeypatch):           # 5
    w = world(tmp_path, monkeypatch)
    p = os.path.join(dr.default_root(w.fno), "NIFTY", f"exp-{EXP}", dr.DATA)
    with open(p, "wb") as f:
        f.write(b"not a parquet file")
    assert w.run(stores=["fno"]) == 0
    assert "Dhan rolling NOT read:" in w.report()
    assert len(table(w)) == 0
    assert any("NOT read" in n for n in run_info(w)["dhan_rolling"]["notes"])


def test_a_rolling_day_without_a_lost_row_is_info_only(tmp_path, monkeypatch):                  # 6
    w = world(tmp_path, monkeypatch)
    assert w.run(stores=["fno"]) == 0
    assert "rolling day with no fno LOST row: NIFTY 2026-11-17 (info)" in w.report()
    t17 = table(w)
    t17 = t17[t17["date"] == "2026-11-17"]
    assert len(t17) == 1 and set(t17["lost_row"]) == {"no"}
    g = w.gaps()
    assert len(g) == 4 and set(g["date"]) == {LOST_DAY}


def test_a_rolling_file_changed_during_the_run_is_a_flag_not_exit_2(tmp_path, monkeypatch):     # 7
    w = world(tmp_path, monkeypatch)
    real = dr.read

    def touch_then_read(root, lost_days):
        with open(os.path.join(root, "NIFTY", f"exp-{EXP}", dr.COMPARE), "a") as f:
            f.write("\n")
        return real(root, lost_days)
    monkeypatch.setattr(dr, "read", touch_then_read)
    assert w.run(stores=["fno"]) == 0
    rep = w.report()
    assert "Dhan rolling files changed during the run (counts may be off)" in rep
    assert audit.NOT_VALID not in rep
    info = run_info(w)
    assert info["stores_changed"] == [] and info["exit_code"] == 0
    assert len(info["dhan_rolling"]["changed"]) == 1 and info["dhan_rolling"]["changed"][0].endswith(dr.COMPARE)


def test_no_compare_no_zip_and_an_explicit_folder(tmp_path, monkeypatch):                       # 8
    w = world(tmp_path, monkeypatch, with_folder=False)
    other = str(tmp_path / "elsewhere" / "rolling")
    rolling(other, compare=False, zip_=False)
    assert w.run(stores=["fno"], dhan_rolling=other) == 0
    rep = w.report()
    assert f"Dhan rolling (#254, flag only, {other}): 1 expiry folder" in rep
    assert "bhavcopy traded of this expiry -; all traded -" in rep
    lost = w.gaps()
    assert all(e.endswith("(traded of that expiry: bhav -); fno_dhan_rolling.csv") for e in lost["evidence"])
    t = table(w)
    assert len(t[t["date"] == LOST_DAY]) == 4 and set(t["bhav_volume"]) == {""}

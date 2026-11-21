"""Data audit, F&O contracts (#253): backfill/equity/audit_fno_contracts.py through audit.run on a synthetic world
(no network, no .env). Raw day folders get hand-made bhavcopy/bhav_compare.csv files in the layout the bhavcopy
check writes (backfill/bhav.py COMPARE_COLUMNS, Status words joined with '+')."""
import json
import os

import pandas as pd

from backfill.equity import audit
from backfill.equity import audit_fno_contracts as fc
from test_audit import SAT, World, mrow

DAY = "2026-11-19"
CMP_COLS = ["Contract", "Expiry", "Bucket", "Type", "Strike", "Bhav_volume", "Kite_volume", "Bhav_OI", "Kite_OI",
            "Status", "Note"]
OK3 = [("C1", 10, 650, "OK"), ("C2", 2, 130, "OK"), ("C3", 5, 325, "OK")]


def name(n):
    return f"NIFTY2{n[1:]}00019NOV26PE"


def day_folder(w, s, v, u, day=DAY, tag=None):
    zero = "withZeroVol" if v == "with" else "noZeroVol"
    folder = tag or f"{day}_exp-2026-11-23_{zero}_dl-{day}_2213"
    return os.path.join(audit.raw_folders(w.raw)[(s, v, u)], folder)


def store_day(w, s, v, rows, u="NIFTY", day=DAY, files=None, compare=True, tag=None):
    """A raw day folder: rows = [(contract, bhav volume, store volume or None, status)]; a file for every row with a
    store volume (files = another number of files on disk, to make the compare file stale)."""
    p = day_folder(w, s, v, u, day, tag)
    opt = os.path.join(p, "options")
    os.makedirs(opt, exist_ok=True)
    out = []
    for c, bv, kv, st in rows:
        nm = c if c.startswith(("NIFTY", "SENSEX", "BAD")) else name(c)
        out.append({"Contract": nm, "Expiry": "2026-11-23", "Bucket": "Current week", "Type": "PE", "Strike": "25000",
                    "Bhav_volume": str(bv), "Kite_volume": "" if kv is None else str(kv), "Bhav_OI": "100",
                    "Kite_OI": "" if kv is None else "100", "Status": st, "Note": ""})
    names = [r["Contract"] for r in out if r["Kite_volume"] != ""]
    n = len(names) if files is None else files
    for i in range(n):
        open(os.path.join(opt, (names[i] if i < len(names) else f"X{i}") + ".csv"), "w").close()
    if compare:
        os.makedirs(os.path.join(p, "bhavcopy"), exist_ok=True)
        pd.DataFrame(out, columns=CMP_COLS).to_csv(os.path.join(p, "bhavcopy", "bhav_compare.csv"), index=False)
    return p


def nov19(w, kite_with=None, kite_without=None, dhan_with=None, dhan_without=None):
    """19 Nov NIFTY in the four stores (each defaults to all three contracts present)."""
    store_day(w, "kite", "with", kite_with or OK3)
    store_day(w, "kite", "without", kite_without or OK3)
    store_day(w, "dhan", "with", dhan_with or OK3)
    store_day(w, "dhan", "without", dhan_without or OK3)


def listed(w, name_=None):
    return pd.read_csv(os.path.join(w.out(name_), "fno_missing_contracts.csv"), dtype=str, keep_default_na=False)


# kite with: C2 kept at volume 0, C3 never downloaded; kite without: C2 dropped (volume 0), C3 missing
KITE_WITH = [("C1", 10, 650, "OK"), ("C2", 2, 0, "ZERO_VOLUME_IN_KITE+VOLUME_DIFF"), ("C3", 5, None, "MISSING_IN_KITE")]
KITE_WITHOUT = [("C1", 10, 650, "OK"), ("C2", 2, None, "MISSING_IN_KITE"), ("C3", 5, None, "MISSING_IN_KITE")]


def test_missing_contracts_of_the_four_stores_are_listed(tmp_path, monkeypatch):                # 1
    w = World(tmp_path, monkeypatch)
    nov19(w, kite_with=KITE_WITH, kite_without=KITE_WITHOUT)
    assert w.run(stores=["fno"]) == 0                            # best source dhan has everything
    lst = listed(w)
    assert list(lst.columns) == fc.LIST_COLUMNS
    assert sorted(zip(lst["variant"], lst["source"], lst["contract"])) == [
        ("with", "kite", name("C3")), ("without", "kite", name("C2")), ("without", "kite", name("C3"))]
    assert set(lst["date"]) == {DAY} and set(lst["underlying"]) == {"NIFTY"} and set(lst["best_source"]) == {"dhan"}
    assert set(lst["day_folder"]) == {os.path.basename(day_folder(w, "kite", "without", "NIFTY"))
                                      } | {os.path.basename(day_folder(w, "kite", "with", "NIFTY"))}
    summ = pd.read_csv(os.path.join(w.out(), "fno_contracts_summary.csv"), dtype=str, keep_default_na=False)
    assert list(summ.columns) == fc.SUMMARY_COLUMNS and len(summ) == 4 and set(summ["adds_up"]) == {"True"}
    kw = summ[(summ["variant"] == "without") & (summ["source"] == "kite")].iloc[0]
    assert (kw["bhav_traded"], kw["traded_files"], kw["missing"]) == ("3", "1", "2")


def test_cause_zero_volume_dropped_or_not_downloaded(tmp_path, monkeypatch):                    # 2
    w = World(tmp_path, monkeypatch)
    nov19(w, kite_with=KITE_WITH, kite_without=KITE_WITHOUT)
    assert w.run(stores=["fno"]) == 0
    got = {(r["variant"], r["contract"]): r["cause"] for r in listed(w).to_dict("records")}
    assert got == {("with", name("C3")): fc.NOT_DOWNLOADED, ("without", name("C2")): fc.ZERO_DROPPED,
                   ("without", name("C3")): fc.NOT_DOWNLOADED}


def test_other_source_has_yes_no_or_unknown(tmp_path, monkeypatch):                             # 3
    w = World(tmp_path, monkeypatch)
    store_day(w, "kite", "without", KITE_WITHOUT)
    store_day(w, "dhan", "without", [("C1", 10, 650, "OK"), ("C2", 2, 130, "OK"), ("C3", 5, None, "MISSING_IN_KITE")])
    store_day(w, "kite", "with", KITE_WITH, day="2026-11-18")   # no other-source folder that day
    assert w.run(stores=["fno"]) == 1                            # dhan (best) lacks C3 on 19 Nov
    got = {(r["date"], r["source"], r["contract"]): r["other_source_has"] for r in listed(w).to_dict("records")}
    assert got == {(DAY, "kite", name("C2")): "yes", (DAY, "kite", name("C3")): "no",
                   (DAY, "dhan", name("C3")): "no", ("2026-11-18", "kite", name("C3")): ""}
    assert "missing in both sources: 1" in w.report()


def test_missing_in_the_best_source_is_unexplained(tmp_path, monkeypatch):                      # 4
    w = World(tmp_path, monkeypatch)
    nov19(w, dhan_without=[("C1", 10, 650, "OK"), ("C2", 2, None, "MISSING_IN_KITE"), ("C3", 5, 325, "OK")])
    assert w.run(stores=["fno"]) == 1
    g = w.gaps()
    assert list(zip(g["store"], g["underlying"], g["source"], g["variant"], g["date"], g["reason"])) == [
        (audit.FNO_CONTRACT, "NIFTY", "dhan", "without", DAY, "UNEXPLAINED")]
    assert "1 contract missing in the best source" in g["evidence"].iat[0] and name("C2") in g["evidence"].iat[0]
    rep = w.report()
    assert "missing in the best source: 1 contracts on 1 store-days" in rep
    info = json.load(open(os.path.join(w.out(), "run_info.json"), encoding="utf-8"))
    assert info["fno_contracts"]["best_missing"] == 1 and info["exit_code"] == 1


def test_missing_only_outside_the_best_source_is_a_flag(tmp_path, monkeypatch):                 # 5
    w = World(tmp_path, monkeypatch)
    nov19(w, kite_with=KITE_WITH, kite_without=KITE_WITHOUT)
    assert w.run(stores=["fno"]) == 0
    assert len(w.gaps()) == 0
    rep = w.report()
    assert "NIFTY  without kite missing 2 (zero-volume dropped 1, not downloaded 1; other source has 2)" in rep
    assert "4 raw store-days, bhav_compare.csv read 4, missing or stale 0" in rep
    assert "traded files + missing != bhav traded: 0" in rep


def test_manual_fno_contract_row_covers_the_best_source_gap(tmp_path, monkeypatch):             # 6
    w = World(tmp_path, monkeypatch,
              manual=[mrow(audit.FNO_CONTRACT, "LOST", DAY, underlying="NIFTY", pid="P999", note="never served")])
    nov19(w, dhan_without=[("C1", 10, 650, "OK"), ("C2", 2, None, "MISSING_IN_KITE"), ("C3", 5, 325, "OK")])
    assert w.run(stores=["fno"]) == 0
    g = w.gaps()
    assert list(zip(g["store"], g["reason"], g["kind"])) == [(audit.FNO_CONTRACT, "LOST", "PERMANENT")]
    assert "P999" in g["evidence"].iat[0] and "unused manual row" not in w.report()


def test_no_compare_file_or_a_stale_one_is_unexplained(tmp_path, monkeypatch):                  # 7
    w = World(tmp_path, monkeypatch)
    store_day(w, "kite", "with", OK3, compare=False)
    store_day(w, "dhan", "with", OK3, files=4)                    # a file written after the check
    assert w.run(stores=["fno"]) == 1
    g = w.gaps().sort_values("source")
    assert list(zip(g["store"], g["source"], g["variant"], g["reason"])) == [
        (audit.FNO_CONTRACT, "dhan", "with", "UNEXPLAINED"), (audit.FNO_CONTRACT, "kite", "with", "UNEXPLAINED")]
    ev = dict(zip(g["source"], g["evidence"]))
    assert "stale: lists 3 files, folder has 4" in ev["dhan"] and "no bhav_compare.csv" in ev["kite"]


def test_bad_name_rows_are_listed_but_never_a_gap(tmp_path, monkeypatch):                       # 8
    w = World(tmp_path, monkeypatch)
    nov19(w, dhan_without=OK3 + [("BAD 2026-11-23 0", 4, None, "BAD_NAME (strike 0 is not a whole positive number)")])
    assert w.run(stores=["fno"]) == 0
    lst = listed(w)
    assert list(zip(lst["contract"], lst["cause"])) == [("BAD 2026-11-23 0", fc.BAD)]
    assert len(w.gaps()) == 0
    summ = pd.read_csv(os.path.join(w.out(), "fno_contracts_summary.csv"), dtype=str, keep_default_na=False)
    r = summ[(summ["variant"] == "without") & (summ["source"] == "dhan")].iloc[0]
    assert (r["bad_names"], r["missing"], r["bhav_traded"], r["adds_up"]) == ("1", "0", "3", "True")


def test_variant_keeps_with_and_without_apart_and_old_gaps_csv_is_read(tmp_path, monkeypatch):  # 9
    def rows(v, u):
        return [{"date": x, "kite_available": "True", "dhan_available": "True",
                 "kite_check": "" if (u == "NIFTY" and x == DAY) else "major", "dhan_check": "major",
                 "best_source": "dhan"} for x in ("2026-11-16", "2026-11-17", "2026-11-18", DAY, "2026-11-20")]
    w = World(tmp_path, monkeypatch, fno_rows=rows)
    old = os.path.join(w.folder, "analysis", "audit_2026-11-20")
    os.makedirs(old)
    pd.DataFrame([{"store": "fno", "security_id": "", "symbol": "", "underlying": "NIFTY", "source": "kite",
                   "date": DAY, "reason": "UNEXPLAINED", "kind": "UNEXPLAINED", "evidence": "x", "clears_when": ""}]
                 ).to_csv(os.path.join(old, "gaps.csv"), index=False)        # made before the variant column
    assert w.run(stores=["fno"]) == 1
    g = w.gaps()
    assert sorted(zip(g["variant"], g["source"], g["date"])) == [("with", "kite", DAY), ("without", "kite", DAY)]
    c = pd.read_csv(os.path.join(w.out(), "changes.csv"), dtype=str, keep_default_na=False)
    assert list(c.columns) == audit.KEY + ["change", "reason_before", "reason_now"]
    assert sorted(zip(c["variant"], c["change"])) == [("", "FILLED"), ("with", "NEW"), ("without", "NEW")]


def test_two_folders_for_one_day_or_a_compare_file_changed_during_the_run(tmp_path, monkeypatch):  # 10
    w = World(tmp_path, monkeypatch)
    store_day(w, "kite", "with", OK3)
    store_day(w, "kite", "with", OK3, tag=DAY)                   # an old-style second folder for 19 Nov
    msgs = []
    assert w.run(stores=["fno"], log=msgs.append) == 2
    assert any("more than one folder for one trading day" in m for m in msgs)
    assert not os.path.exists(os.path.join(w.folder, "analysis"))

    w2 = World(tmp_path / "two", monkeypatch)
    p = store_day(w2, "dhan", "with", OK3)
    real = fc.check

    def touch_then_check(days, best):
        with open(fc.compare_path(p), "a") as f:
            f.write("\n")
        return real(days, best)
    monkeypatch.setattr(fc, "check", touch_then_check)
    assert w2.run(stores=["fno"]) == 2
    assert w2.report().splitlines()[0] == audit.NOT_VALID
    assert fc.compare_path(p) in w2.report()

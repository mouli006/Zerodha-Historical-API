"""Data audit, F&O contracts (#253): backfill/equity/audit_fno_contracts.py through audit.run on a synthetic world
(no network, no .env). Raw day folders get hand-made bhavcopy/bhav_compare.csv files in the layout the bhavcopy
check writes (backfill/bhav.py COMPARE_COLUMNS, Status words joined with '+')."""
import json
import os
from datetime import datetime

import pandas as pd

from backfill.equity import audit
from backfill.equity import audit_fno_contracts as fc
from test_audit import IST, SAT, World, mrow

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


PRICE_COLS = ["Bhav_last", "Kite_last", "Kite_last_regular", "Last_bar", "Bhav_close", "Bhav_settle"]
# the order of backfill/bhav.py COMPARE_COLUMNS (price columns between Kite_OI and Status)
CMP_COLS_PRICE = CMP_COLS[:9] + PRICE_COLS + CMP_COLS[9:]


def pr(bhav_last, store_last, regular="", last_bar="15:39:00", close="", settle=""):
    """The price fields of one compare row (#265)."""
    return dict(zip(PRICE_COLS, (str(bhav_last), str(store_last), str(regular), last_bar, str(close), str(settle))))


def store_day(w, s, v, rows, u="NIFTY", day=DAY, files=None, compare=True, tag=None):
    """A raw day folder: rows = [(contract, bhav volume, store volume or None, status[, price fields])]; a file for
    every row with a store volume (files = another number of files on disk, to make the compare file stale). When a
    row carries price fields (pr()), the compare file gets the price columns (#265; every other row "")."""
    p = day_folder(w, s, v, u, day, tag)
    opt = os.path.join(p, "options")
    os.makedirs(opt, exist_ok=True)
    out = []
    priced = any(len(r) > 4 for r in rows)
    for c, bv, kv, st, *price in rows:
        nm = c if c.startswith(("NIFTY", "SENSEX", "BAD")) else name(c)
        r = {"Contract": nm, "Expiry": "2026-11-23", "Bucket": "Current week", "Type": "PE", "Strike": "25000",
             "Bhav_volume": str(bv), "Kite_volume": "" if kv is None else str(kv), "Bhav_OI": "100",
             "Kite_OI": "" if kv is None else "100", "Status": st, "Note": ""}
        if priced:
            r.update({k: "" for k in PRICE_COLS})
            r.update(price[0] if price else {})
        out.append(r)
    names = [r["Contract"] for r in out if r["Kite_volume"] != ""]
    n = len(names) if files is None else files
    for i in range(n):
        open(os.path.join(opt, (names[i] if i < len(names) else f"X{i}") + ".csv"), "w").close()
    if compare:
        os.makedirs(os.path.join(p, "bhavcopy"), exist_ok=True)
        pd.DataFrame(out, columns=CMP_COLS_PRICE if priced else CMP_COLS).to_csv(
            os.path.join(p, "bhavcopy", "bhav_compare.csv"), index=False)
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


# ---- #265 price flags: stored last vs the bhavcopy LAST, > max(5 %, Rs 0.20), a flag only (D234, D235) ----

def flags(w, name_=None):
    return pd.read_csv(os.path.join(w.out(name_), "fno_price_flags.csv"), dtype=str, keep_default_na=False)


def run_info(w, name_=None):
    return json.load(open(os.path.join(w.out(name_), "run_info.json"), encoding="utf-8"))


STRAY = ("C1", 11048, 650, "PRICE_DIFF", pr("1.25", "3.00", "0.55", "15:39:00", "0.80", "0.80"))   # 140 % off


def test_price_flag_over_the_limit_is_listed_with_all_columns(tmp_path, monkeypatch):           # 11
    w = World(tmp_path, monkeypatch)
    nov19(w, kite_with=[STRAY, ("C2", 2, 130, "OK", pr("10", "10.05")), ("C3", 5, 325, "OK", pr("50", "50"))])
    assert w.run(stores=["fno"]) == 0
    f = flags(w)
    assert list(f.columns) == fc.PRICE_COLUMNS and len(f) == 1
    r = f.iloc[0]
    assert (r["date"], r["underlying"], r["variant"], r["source"], r["contract"]) == (DAY, "NIFTY", "with", "kite",
                                                                                      name("C1"))
    assert (r["bhav_volume"], r["store_volume"], r["bhav_last"], r["store_last"], r["store_last_regular"],
            r["last_bar"], r["bhav_close"], r["bhav_settle"]) == ("11048", "650", "1.25", "3.00", "0.55", "15:39:00",
                                                                  "0.80", "0.80")
    assert (float(r["diff"]), float(r["diff_pct"])) == (1.75, 140.0)
    assert r["best_source"] == "dhan" and r["other_source_flagged"] == ""        # the dhan file has no price columns
    assert r["day_folder"] == os.path.basename(day_folder(w, "kite", "with", "NIFTY"))


def test_small_price_differences_are_not_listed(tmp_path, monkeypatch):                         # 12
    w = World(tmp_path, monkeypatch)
    store_day(w, "kite", "with", [("C1", 1696167, 650, "PRICE_DIFF", pr("222.55", "223.80")),   # 0.56 %
                                  ("C2", 10, 130, "OK", pr("100", "104.90")),                     # 4.9 %
                                  ("C3", 10, 325, "OK", pr("100", "105.10"))])                    # 5.1 %
    assert w.run(stores=["fno"]) == 0
    assert list(flags(w)["contract"]) == [name("C3")]


def test_cheap_option_floor_of_twenty_paise(tmp_path, monkeypatch):                             # 13
    w = World(tmp_path, monkeypatch)
    store_day(w, "kite", "with", [("C1", 10, 650, "OK", pr("0.05", "0.15")),    # 200 %, but Rs 0.10 <= Rs 0.20
                                  ("C2", 10, 130, "OK", pr("0.05", "0.25")),    # Rs 0.20 = the floor, not over it
                                  ("C3", 10, 325, "OK", pr("0.05", "0.30"))])   # Rs 0.25 > Rs 0.20
    assert w.run(stores=["fno"]) == 0
    assert list(flags(w)["contract"]) == [name("C3")]


def test_rows_the_price_check_skips(tmp_path, monkeypatch):                                     # 14
    w = World(tmp_path, monkeypatch)
    store_day(w, "kite", "with", [("C1", 10, 650, "OK", pr("100", "200")),                     # flagged
                                  ("C2", 10, None, "MISSING_IN_KITE", pr("100", "")),           # no file
                                  ("C3", 10, 325, "OK", pr("0", "200")),                        # Bhav_last 0
                                  ("C4", 0, 130, "OK", pr("100", "200")),                       # Bhav_volume 0
                                  ("C5", 10, 130, "OK", pr("100", "")),                         # no stored last
                                  ("BAD 2026-11-23 0", 4, 10, "BAD_NAME (strike 0)", pr("100", "200"))])
    assert w.run(stores=["fno"]) == 0
    assert list(flags(w)["contract"]) == [name("C1")]
    p = run_info(w)["fno_prices"]
    assert (p["compared"], p["flags"], p["no_price_columns"]) == (1, 1, 0)


def test_best_source_and_other_source_flagged(tmp_path, monkeypatch):                           # 15
    w = World(tmp_path, monkeypatch)
    store_day(w, "kite", "with", [("C1", 10, 650, "OK", pr("100", "200")), ("C2", 10, 130, "OK", pr("100", "200")),
                                  ("C3", 10, 325, "OK", pr("100", "200"))])
    store_day(w, "dhan", "with", [("C1", 10, 650, "OK", pr("100", "200")), ("C2", 10, 130, "OK", pr("100", "100")),
                                  ("C3", 10, 325, "OK", pr("100", ""))])
    assert w.run(stores=["fno"]) == 0
    got = {(r["source"], r["contract"]): (r["other_source_flagged"], r["best_source"])
           for r in flags(w).to_dict("records")}
    assert got == {("kite", name("C1")): ("yes", "dhan"), ("kite", name("C2")): ("no", "dhan"),
                   ("kite", name("C3")): ("", "dhan"), ("dhan", name("C1")): ("yes", "dhan")}
    p = run_info(w)["fno_prices"]
    assert (p["flags"], p["best_flags"], p["both"], p["compared"]) == (4, 1, 1, 5)
    assert "4 rows on 2 store-days; in the best source 1; in both sources 1; compared 5" in w.report()


def test_price_flags_are_a_flag_only(tmp_path, monkeypatch):                                    # 16
    w = World(tmp_path, monkeypatch)
    nov19(w, kite_with=[STRAY] + OK3[1:], dhan_with=[STRAY] + OK3[1:])              # also in the best source dhan
    assert w.run(stores=["fno"]) == 0
    assert len(w.gaps()) == 0
    info = run_info(w)
    assert info["exit_code"] == 0 and info["verdict"] == "CLEAN" and info["unexplained"] == 0
    assert info["fno_prices"] == {"limit_pct": 5, "limit_ticks": 4, "compared": 2, "flags": 2, "best_flags": 1,
                                  "both": 1, "no_price_columns": 2}          # C2 / C3 carry no stored last
    rep = w.report()
    assert "F&O price flags (#265, flag only; last vs NSE/BSE LAST, > 5 % and > Rs 0.20 = 4 ticks): 2 rows" in rep
    assert "NIFTY  with    kite price flags 1 of 1 compared" in rep


def test_old_compare_files_without_price_columns(tmp_path, monkeypatch):                        # 17
    w = World(tmp_path, monkeypatch)
    nov19(w)
    assert w.run(stores=["fno"]) == 0
    f = flags(w)
    assert list(f.columns) == fc.PRICE_COLUMNS and len(f) == 0
    assert run_info(w)["fno_prices"]["no_price_columns"] == 4
    assert "compared 0; no price columns 4 files" in w.report()


# ---- #264 fresh days: manual file, then time_reason, then UNEXPLAINED ----

FRIDAY_EVENING = datetime(2026, 11, 20, 18, 0, tzinfo=IST)       # 20 Nov not finished until 21 Nov 01:00 IST
FRI = "2026-11-20"


def fresh_friday(w):
    """20 Nov: the kite with folder has no compare file yet; the best source dhan (without) lacks C2."""
    store_day(w, "kite", "with", OK3, day=FRI, compare=False)
    store_day(w, "dhan", "without", [("C1", 10, 650, "OK"), ("C2", 2, None, "MISSING_IN_KITE"), ("C3", 5, 325, "OK")],
              day=FRI)


def test_fresh_day_is_waiting_not_finished(tmp_path, monkeypatch):                              # 18
    w = World(tmp_path, monkeypatch)
    fresh_friday(w)
    assert w.run(now=FRIDAY_EVENING, stores=["fno"]) == 1
    g = w.gaps(f"audit_{FRI}").sort_values("source")
    assert list(zip(g["store"], g["source"], g["variant"], g["date"], g["reason"], g["kind"], g["clears_when"])) == [
        (audit.FNO_CONTRACT, "dhan", "without", FRI, "NOT_FINISHED", "WAITING", "after D+1 01:00 IST"),
        (audit.FNO_CONTRACT, "kite", "with", FRI, "NOT_FINISHED", "WAITING", "after D+1 01:00 IST")]
    assert "no bhav_compare.csv" in g["evidence"].iat[1]


def test_finished_day_newer_than_days_csv_is_update_pending(tmp_path, monkeypatch):             # 19
    def rows(v, u):
        return [{"date": x, "kite_available": "True", "dhan_available": "True", "kite_check": "major",
                 "dhan_check": "major", "best_source": "dhan"} for x in ("2026-11-16", "2026-11-17", "2026-11-18", DAY)]
    w = World(tmp_path, monkeypatch, fno_rows=rows)                   # 20 Nov not published yet
    store_day(w, "kite", "with", OK3, day=FRI, compare=False)
    store_day(w, "kite", "without", OK3, compare=False)               # 19 Nov is published: a real gap
    assert w.run(stores=["fno"]) == 1
    g = w.gaps()
    g = g[g["store"] == audit.FNO_CONTRACT].sort_values("date")
    assert list(zip(g["variant"], g["date"], g["reason"], g["clears_when"])) == [
        ("without", DAY, "UNEXPLAINED", ""),
        ("with", FRI, "UPDATE_PENDING", "after the next F&O download + check + publish")]


def test_manual_row_wins_over_the_time_reason(tmp_path, monkeypatch):                           # 20
    w = World(tmp_path, monkeypatch,
              manual=[mrow(audit.FNO_CONTRACT, "LOST", FRI, underlying="NIFTY", pid="P998", note="known")])
    fresh_friday(w)
    assert w.run(now=FRIDAY_EVENING, stores=["fno"]) == 0
    g = w.gaps(f"audit_{FRI}")
    assert list(zip(g["store"], g["reason"], g["kind"])) == [(audit.FNO_CONTRACT, "LOST", "PERMANENT")] * 2
    assert "unused manual row" not in w.report(f"audit_{FRI}")

"""The data audit (#249): backfill/equity/audit.py on a small synthetic world (no network, no .env).

The per-stock equity day tables (minute_combined, tested in test_minute_combined.py) are replaced by hand-made
tables; the master and the special-sessions calendar by small frames; everything else is real files in tmp_path."""
import json
import os
import sqlite3
from datetime import date, datetime

import pandas as pd
import pytest

import config
from backfill.equity import audit
from backfill.equity import minute_plan as mp
from backfill.equity import security_master as sm
from backfill.locks import RunLock

IST = mp.IST
SAT = datetime(2026, 11, 21, 10, 0, tzinfo=IST)           # Fri 20 Nov finished, stored
WEEK = [date(2026, 11, d) for d in (16, 17, 18, 19, 20)]  # Mon..Fri


def q(*a, **k):
    pass


def d(day):
    return date(2026, 11, day)


def kite_table(days, none=None, labels=None):
    """A minute_combined day table: KITE on days, NONE with the given combined reason on none={date: reason}."""
    none = none or {}
    labels = labels or {}
    rows = [{"date": x, "traded": True, "source": "NONE" if x in none else "KITE",
             "reason": none.get(x, "KITE"), "kite_label": labels.get(x, "OK"), "frozen": False}
            for x in sorted(set(days) | set(none))]
    return pd.DataFrame(rows)


def write_csv(path, rows, cols):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pd.DataFrame(rows, columns=cols).to_csv(path, index=False)


FNO_COLS = ["date", "kite_available", "dhan_available", "kite_check", "dhan_check", "best_source"]


class World:
    def __init__(self, root, monkeypatch, tables=None, eod_days=WEEK, holidays=(), special=(), master_last=d(20),
                 fno_rows=None, manual=True, failed=0, plan_days=None):
        self.root = root
        self.folder = str(root / "eq")
        self.fno = str(root / "fno" / "backtest")
        self.raw = {"kite": str(root / "fno" / "raw" / "kite"), "dhan": str(root / "fno" / "raw" / "dhan")}
        store = mp.store_dir(self.folder)
        rows = []
        for x in eod_days:
            f = f"raw/{x.year}/bhav_{x:%Y%m%d}.zip"
            os.makedirs(os.path.join(store, os.path.dirname(f)), exist_ok=True)
            open(os.path.join(store, f), "wb").close()
            rows.append({"date": x.isoformat(), "status": "ok", "file": f})
        rows += [{"date": x.isoformat(), "status": "holiday", "file": ""} for x in holidays]
        write_csv(os.path.join(store, "days.csv"), sorted(rows, key=lambda r: r["date"]), ["date", "status", "file"])
        os.makedirs(sm.out_dir(self.folder), exist_ok=True)
        with open(os.path.join(sm.out_dir(self.folder), "build_info.json"), "w") as f:
            json.dump({"last_date": master_last.isoformat(), "built_at": "2026-11-20 22:31:45 IST"}, f)
        if manual:
            self.manual(manual if isinstance(manual, list) else [])
        for (v, u), p in audit.fno_days_paths(self.fno).items():
            r = fno_rows(v, u) if fno_rows else [
                {"date": x.isoformat(), "kite_available": "True", "dhan_available": "True", "kite_check": "major",
                 "dhan_check": "major", "best_source": "dhan"} for x in WEEK]
            write_csv(p, r, FNO_COLS)
        for p in audit.raw_folders(self.raw).values():
            os.makedirs(p, exist_ok=True)
        db = audit.icici_db(self.folder)
        os.makedirs(os.path.dirname(db), exist_ok=True)
        con = sqlite3.connect(db)
        con.execute("CREATE TABLE chunks (security_id TEXT, interval TEXT, chunk_from TEXT, chunk_to TEXT, code TEXT,"
                    " bars INTEGER, day_bars TEXT, status TEXT, built INTEGER, fetched_at TEXT, run_id TEXT,"
                    " seconds REAL, error TEXT)")
        con.execute("CREATE TABLE stocks (security_id TEXT PRIMARY KEY, code TEXT, match_kind TEXT, status TEXT,"
                    " first_minute_day TEXT, price_check TEXT, reason TEXT, days INTEGER, minute_rows INTEGER,"
                    " built_at TEXT)")
        con.execute("INSERT INTO stocks (security_id, status) VALUES ('S9', 'DONE')")
        for i in range(failed):
            con.execute("INSERT INTO chunks (security_id, chunk_from, status, error) VALUES (?, ?, 'FAILED', 'timeout')",
                        ("S9", f"2026-11-1{i}"))
        con.commit()
        con.close()
        self.plan_days(plan_days if plan_days is not None else {"S1": [d(19)]})
        self.tables = tables if tables is not None else {"S9": kite_table(WEEK)}
        secs = pd.DataFrame({"security_id": ["S1", "S2", "S3", "S9", "E1", "R1"],
                             "last_symbol": ["ONE", "TWO", "THREE", "NINE", "ETFX", "RTS"],
                             "instrument_kind": ["STOCK"] * 4 + ["ETF", "RIGHTS"]})
        monkeypatch.setattr(audit.sm, "load_master", lambda folder=None: {"securities": secs})
        monkeypatch.setattr(audit.ss, "load_special_sessions",
                            lambda folder=None: pd.DataFrame({"date": list(special), "kind": ["MUHURAT"] * len(special)}))
        monkeypatch.setattr(audit.mp, "check_inputs", lambda folder: None)
        monkeypatch.setattr(audit, "equity_tables",
                            lambda folder, ids, log=print: ((s, self.tables[s]) for s in ids if s in self.tables))
        monkeypatch.setattr(audit, "EOD_FROM", WEEK[0])
        monkeypatch.setattr(audit, "git_commit", lambda: "test")

    def plan_days(self, by_sid):
        """The ICICI plan's day list (icici_plan_days.parquet), {security_id: [dates]}."""
        rows = [{"security_id": s, "date": x, "row": 0, "reason": "test", "priority": 1}
                for s, ds in by_sid.items() for x in ds]
        p = audit.plan_days_path(self.folder)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        pd.DataFrame(rows, columns=["security_id", "date", "row", "reason", "priority"]).to_parquet(p, index=False)

    def manual(self, rows):
        write_csv(audit.manual_path(self.folder), rows, audit.MANUAL_COLUMNS)

    def run(self, now=SAT, **kw):
        args = {"fno_folder": self.fno, "raw_kite": self.raw["kite"], "raw_dhan": self.raw["dhan"], "now": now, "log": q}
        args.update(kw)
        return audit.run(self.folder, **args)

    def out(self, name=None):
        return os.path.join(self.folder, "analysis", name or f"audit_{SAT:%Y-%m-%d}")

    def gaps(self, name=None):
        return pd.read_csv(os.path.join(self.out(name), "gaps.csv"), dtype=str, keep_default_na=False)

    def report(self, name=None):
        return open(os.path.join(self.out(name), "report.txt"), encoding="utf-8").read()


def mrow(store, reason, from_date, to_date="", sid="", underlying="", pid="P0", clear_by="", note="test"):
    return {"store": store, "security_id": sid, "symbol": "", "underlying": underlying, "from_date": from_date,
            "to_date": to_date, "reason": reason, "problem_id": pid, "clear_by": clear_by, "note": note}


# ---- equity reasons ----

def test_every_equity_reason(tmp_path, monkeypatch):                                             # 1
    tables = {"S9": kite_table(WEEK),
              "S1": kite_table([], {d(16): "NO_KITE_ROW+ICICI_NOT_YET_FETCHED", d(17): "NO_KITE_FOLDER+ICICI_EMPTY",
                                    d(18): "NO_KITE_FOLDER+ICICI_BEFORE_START", d(19): "NO_KITE_FOLDER+NOT_IN_ICICI",
                                    d(20): "NO_KITE_ROW+NOT_IN_ICICI_PLAN"}),
              "S2": kite_table([], {d(16): "NO_KITE_ROW+ICICI_NOT_YET_FETCHED", d(17): "NO_KITE_ROW+ICICI_NOT_YET_FETCHED",
                                    d(18): "NO_KITE_FOLDER+ICICI_MIXED_SERIES"})}
    w = World(tmp_path, monkeypatch, tables, manual=[mrow("equity", "LOST", "2026-11-17", sid="S2", pid="P204"),
                                                     mrow("equity", "P65_RETRY", "2026-11-16", sid="S2", pid="P65",
                                                          clear_by="2026-11-22")])
    assert w.run(stores=["equity"]) == 1
    g = w.gaps()
    got = {(r["security_id"], r["date"]): r["reason"] for r in g.to_dict("records")}
    assert got == {("S1", "2026-11-16"): "WAITING_ICICI", ("S1", "2026-11-17"): "ICICI_EMPTY",
                   ("S1", "2026-11-18"): "ICICI_BEFORE_START", ("S1", "2026-11-19"): "VENDOR",
                   ("S1", "2026-11-20"): "UNEXPLAINED", ("S2", "2026-11-16"): "P65_RETRY",
                   ("S2", "2026-11-17"): "LOST", ("S2", "2026-11-18"): "VENDOR"}
    assert set(g.loc[g["reason"] == "P65_RETRY", "kind"]) == {"WAITING"}
    assert set(g.loc[g["reason"] == "VENDOR", "kind"]) == {"PERMANENT"}


def test_manual_lost_beats_waiting_icici():                                                       # 2
    m = audit.Manual(audit.read_manual_frame(
        [mrow("equity", "LOST", "2026-11-17", sid="S2", pid="P204", note="gone")]))
    r = audit.classify_equity("NO_KITE_ROW+ICICI_NOT_YET_FETCHED", d(17), "S2", m, d(20), SAT)
    assert r[0] == "LOST" and "P204" in r[1]


def test_plain_missing_day_is_unexplained_with_the_reason_as_evidence():                          # 3
    m = audit.Manual(None)
    r = audit.classify_equity("NO_KITE_ROW+NOT_IN_ICICI_PLAN", d(18), "S1", m, d(20), SAT)
    assert r == ("UNEXPLAINED", "NO_KITE_ROW+NOT_IN_ICICI_PLAN", "")


def test_p65_row_past_clear_by_is_unexplained():                                                  # 4
    m = audit.Manual(audit.read_manual_frame(
        [mrow("equity", "P65_RETRY", "2026-11-16", sid="S2", pid="P65", clear_by="2026-11-22")]))
    late = datetime(2026, 11, 23, 10, 0, tzinfo=IST)
    r = audit.classify_equity("NO_KITE_ROW+NOT_IN_ICICI_PLAN", d(16), "S2", m, d(20), late)
    assert r[0] == "UNEXPLAINED" and "past clear_by 2026-11-22" in r[1]
    assert audit.classify_equity("X+Y", d(16), "S2", m, d(20), SAT)[0] == "P65_RETRY"


def test_missing_manual_file_is_reported_and_its_days_stay_unexplained(tmp_path, monkeypatch):   # 5
    w = World(tmp_path, monkeypatch, {"S9": kite_table(WEEK), "S2": kite_table([], {d(17): "NO_KITE_ROW+X"})},
              manual=False)
    assert w.run(stores=["equity"]) == 1
    assert "MANUAL FILE MISSING" in w.report()
    assert list(w.gaps()["reason"]) == ["UNEXPLAINED"]


def test_day_empty_is_a_flag_not_a_gap(tmp_path, monkeypatch):                                   # 6
    w = World(tmp_path, monkeypatch, {"S9": kite_table(WEEK, labels={d(18): "DAY_EMPTY"})})
    assert w.run(stores=["equity"]) == 0
    assert "Kite DAY_EMPTY days loaded from Kite: 1" in w.report()
    assert len(w.gaps()) == 0


# ---- EOD ----

def test_eod_holiday_row_is_holiday(tmp_path, monkeypatch):                                       # 7
    w = World(tmp_path, monkeypatch, eod_days=[x for x in WEEK if x != d(18)], holidays=[d(18)])
    assert w.run(stores=["eod"]) == 0
    g = w.gaps()
    assert list(zip(g["date"], g["reason"], g["kind"])) == [("2026-11-18", "HOLIDAY", "PERMANENT")]


def test_eod_missing_finished_weekday_is_unexplained(tmp_path, monkeypatch):                      # 8
    w = World(tmp_path, monkeypatch, eod_days=[x for x in WEEK if x != d(18)])
    assert w.run(stores=["eod"]) == 1
    g = w.gaps()
    assert list(zip(g["date"], g["reason"], g["evidence"])) == [("2026-11-18", "UNEXPLAINED", "no days.csv row")]


def test_eod_weekend_special_session_is_expected_a_normal_weekend_is_not(tmp_path, monkeypatch):  # 9
    sun = datetime(2026, 11, 22, 10, 0, tzinfo=IST)
    w = World(tmp_path, monkeypatch, special=[d(21)])
    assert w.run(stores=["eod"], now=sun) == 1
    g = w.gaps(f"audit_{sun:%Y-%m-%d}")
    assert list(zip(g["date"], g["reason"])) == [("2026-11-21", "UPDATE_PENDING")]       # Sat session; Sun 22 not


def test_eod_weekday_without_row_before_01_00_is_not_finished_never_holiday(tmp_path, monkeypatch):  # 10
    fri = datetime(2026, 11, 20, 15, 0, tzinfo=IST)
    w = World(tmp_path, monkeypatch, eod_days=WEEK[:4])
    assert w.run(stores=["eod"], now=fri) == 1
    g = w.gaps(f"audit_{fri:%Y-%m-%d}")
    assert list(zip(g["date"], g["reason"], g["kind"])) == [("2026-11-20", "NOT_FINISHED", "WAITING")]


def test_not_finished_vs_update_pending():                                                        # 11
    before = datetime(2026, 11, 21, 0, 30, tzinfo=IST)
    after = datetime(2026, 11, 21, 2, 0, tzinfo=IST)
    assert audit.time_reason("equity", d(20), d(19), before)[0] == "NOT_FINISHED"
    assert audit.time_reason("equity", d(20), d(19), after)[0] == "UPDATE_PENDING"
    assert audit.time_reason("equity", d(20), d(20), after) is None
    assert audit.kind_of("NOT_FINISHED") == audit.kind_of("UPDATE_PENDING") == "WAITING"


# ---- F&O, master, ICICI ----

def test_fno_missing_day_both_sources_covered_by_manual_lost(tmp_path, monkeypatch):              # 12
    def rows(v, u):
        return [{"date": x.isoformat(), "kite_available": "True", "dhan_available": "True", "kite_check": "major",
                 "dhan_check": "major", "best_source": "dhan"} for x in WEEK if not (u == "NIFTY" and x == d(18))]
    w = World(tmp_path, monkeypatch, fno_rows=rows,
              manual=[mrow("fno", "LOST", "2026-11-18", underlying="NIFTY", pid="P188", note="both sources")])
    assert w.run(stores=["fno"]) == 0
    g = w.gaps()
    assert sorted(zip(g["underlying"], g["source"], g["reason"])) == [("NIFTY", "dhan", "LOST"),
                                                                       ("NIFTY", "dhan", "LOST"),
                                                                       ("NIFTY", "kite", "LOST"),
                                                                       ("NIFTY", "kite", "LOST")]


def test_fno_unchecked_source_is_unexplained(tmp_path, monkeypatch):                              # 13
    def rows(v, u):
        return [{"date": x.isoformat(), "kite_available": "True", "dhan_available": "True",
                 "kite_check": "" if (u == "SENSEX" and v == "with" and x == d(17)) else "major",
                 "dhan_check": "major", "best_source": "dhan"} for x in WEEK]
    w = World(tmp_path, monkeypatch, fno_rows=rows)
    assert w.run(stores=["fno"]) == 1
    g = w.gaps()
    assert list(zip(g["underlying"], g["source"], g["date"], g["reason"])) == [("SENSEX", "kite", "2026-11-17",
                                                                                 "UNEXPLAINED")]
    assert "not checked" in g["evidence"].iat[0]


def test_master_older_than_bhavcopy_and_kind_counts(tmp_path, monkeypatch):                       # 14
    w = World(tmp_path, monkeypatch, master_last=d(19))
    assert w.run(stores=["master"]) == 1
    g = w.gaps()
    assert list(zip(g["store"], g["reason"])) == [("master", "UNEXPLAINED")]
    rep = w.report()
    assert "ETF 1" in rep and "RIGHTS 1" in rep and "NOT_DOWNLOADED_KIND (not in the minute stores): 2" in rep


def test_icici_failed_chunk_is_unexplained(tmp_path, monkeypatch):                                # 15
    w = World(tmp_path, monkeypatch, failed=1)
    assert w.run(stores=["icici"]) == 1
    g = w.gaps()
    assert list(zip(g["store"], g["security_id"], g["reason"])) == [("icici", "S9", "UNEXPLAINED")]
    assert "1 FAILED chunks" in w.report()


# ---- refusals ----

def test_refused_while_an_equity_writer_lock_is_held(tmp_path, monkeypatch):                      # 16
    w = World(tmp_path, monkeypatch)
    lock = RunLock(mp.lock_path(w.folder)).acquire("test")
    try:
        assert w.run() == 2
    finally:
        lock.release()
    assert not os.path.exists(os.path.join(w.folder, "analysis"))


def test_refused_while_an_fno_run_lock_is_held(tmp_path, monkeypatch):                            # 17
    w = World(tmp_path, monkeypatch)
    p = os.path.join(audit.raw_folders(w.raw)[("kite", "with", "NIFTY")], ".lock")
    lock = RunLock(p).acquire("test")
    msgs = []
    try:
        assert w.run(log=msgs.append) == 2
    finally:
        lock.release()
    assert any("F&O run lock held" in m and "NIFTY_FnO_1min" in m for m in msgs)


def test_missing_days_csv_or_no_fno_folder_is_refused(tmp_path, monkeypatch):                     # 18
    w = World(tmp_path, monkeypatch)
    p = audit.fno_days_paths(w.fno)[("without", "SENSEX")]
    os.remove(p)
    msgs = []
    assert w.run(log=msgs.append) == 2
    assert any(p in m for m in msgs)
    monkeypatch.setattr(config, "fno_backtest_dir", lambda env=None: "")
    msgs = []
    assert w.run(fno_folder=None, log=msgs.append) == 2
    assert any("FNO_BACKTEST_DIR" in m for m in msgs)


def test_raw_base_without_the_store_folders_is_refused(tmp_path, monkeypatch):                    # 19
    w = World(tmp_path, monkeypatch)
    empty = tmp_path / "data"                          # what config falls back to without .env
    empty.mkdir()
    msgs = []
    assert w.run(raw_dhan=str(empty), log=msgs.append) == 2
    assert any("NIFTY_FnO_1min_Dhan" in m for m in msgs)


def test_non_absolute_paths_are_refused(tmp_path, monkeypatch):                                   # 20
    w = World(tmp_path, monkeypatch)
    assert audit.run("eq", log=q) == 2
    assert w.run(fno_folder="fno/backtest") == 2
    assert audit.main(["run", "--folder", "relative/eq"]) == 2


def test_fno_days_csv_changed_during_the_run_is_not_valid(tmp_path, monkeypatch):                 # 21
    w = World(tmp_path, monkeypatch)
    real = audit.audit_fno

    def touch_then_audit(fno, *a):
        with open(audit.fno_days_paths(fno["backtest"])[("with", "NIFTY")], "a") as f:
            f.write("\n")
        return real(fno, *a)
    monkeypatch.setattr(audit, "audit_fno", touch_then_audit)
    assert w.run() == 2
    assert w.report().splitlines()[0] == audit.NOT_VALID
    assert open(os.path.join(w.out(), "summary.csv")).readline().strip() == audit.NOT_VALID


# ---- output ----

def tree(root):
    out = {}
    for base, _, files in os.walk(root):
        for n in files:
            p = os.path.join(base, n)
            s = os.stat(p)
            out[p] = (s.st_size, s.st_mtime_ns)
    return out


def test_writes_nothing_outside_its_new_folder(tmp_path, monkeypatch):                            # 22
    w = World(tmp_path, monkeypatch, {"S9": kite_table(WEEK), "S1": kite_table([], {d(18): "A+NOT_IN_ICICI_PLAN"})})
    before = tree(tmp_path)
    assert w.run() == 1
    after = tree(tmp_path)
    added = {p for p in after if p not in before}
    assert {k: v for k, v in after.items() if k not in added} == before
    assert added and all(p.startswith(w.out() + os.sep) for p in added)
    assert sorted(os.listdir(w.out())) == sorted(["report.txt", "summary.csv", "gaps.csv", "unexplained.csv",
                                                  "waiting.csv", "changes.csv", "run_info.json",
                                                  "fno_missing_contracts.csv", "fno_contracts_summary.csv",
                                                  "fno_dhan_rolling.csv"])


def test_second_run_same_day_gets_a_b_folder(tmp_path, monkeypatch):                              # 23
    w = World(tmp_path, monkeypatch)
    assert w.run() == 0
    assert w.run() == 0
    assert os.path.isdir(w.out()) and os.path.isdir(w.out() + "_b")


def test_changes_against_the_previous_audit_folder(tmp_path, monkeypatch):                        # 24
    w = World(tmp_path, monkeypatch, {"S9": kite_table(WEEK),
                                      "S1": kite_table([], {d(18): "A+NOT_IN_ICICI_PLAN", d(19): "A+NOT_IN_ICICI"})})
    os.makedirs(os.path.join(w.folder, "analysis"), exist_ok=True)
    open(os.path.join(w.folder, "analysis", "audit_2026-11-20.log"), "w").close()   # a Tee log, not a folder
    assert w.run() == 1
    first = pd.read_csv(os.path.join(w.out(), "changes.csv"))
    assert len(first) == 0 and "first audit" in w.report()
    w.tables["S1"] = kite_table([d(18)], {d(19): "A+NOT_IN_ICICI", d(20): "A+NOT_IN_ICICI_PLAN"})
    assert w.run() == 1
    c = pd.read_csv(os.path.join(w.out() + "_b", "changes.csv"), dtype=str, keep_default_na=False)
    assert sorted(zip(c["date"], c["change"])) == [("2026-11-18", "FILLED"), ("2026-11-20", "NEW")]


def test_exit_codes_and_every_path_in_the_report(tmp_path, monkeypatch):                          # 25
    w = World(tmp_path, monkeypatch)
    assert w.run() == 0
    rep = w.report()
    info = json.load(open(os.path.join(w.out(), "run_info.json")))
    assert info["exit_code"] == 0 and info["verdict"] == "CLEAN" and info["commit"] == "test"
    for k, p in info["paths"].items():
        assert p in rep, k
    for p in list(audit.fno_days_paths(w.fno).values()) + [w.raw["kite"], w.raw["dhan"], audit.icici_db(w.folder)]:
        assert p in info["paths"].values()
    assert "F&O lock check: 8 folders, 0 lock files found" in rep
    w.tables["S1"] = kite_table([], {d(19): "A+ICICI_NOT_YET_FETCHED"})
    assert w.run() == 1
    assert "VERDICT: GAPS" in w.report(f"audit_{SAT:%Y-%m-%d}_b")


def test_a_non_empty_icici_wal_is_refused(tmp_path, monkeypatch):                                 # 26
    w = World(tmp_path, monkeypatch)
    wal = audit.icici_db(w.folder) + "-wal"
    with open(wal, "wb") as f:
        f.write(b"x" * 4120)                     # rows a run left uncheckpointed (immutable=1 would not see them)
    msgs = []
    assert w.run(log=msgs.append) == 2
    m = " ".join(msgs)
    assert wal in m and "4,120 bytes" in m and "never delete the -wal file" in m
    assert not os.path.exists(os.path.join(w.folder, "analysis"))
    assert os.path.getsize(wal) == 4120          # left alone
    assert w.run(stores=["eod"]) == 0            # without icici the WAL does not matter


def test_any_other_error_ends_with_exit_2(tmp_path, monkeypatch, capsys):                         # 27
    w = World(tmp_path, monkeypatch)

    def no_master(folder=None):
        raise sm.NotBuilt("No security master")
    monkeypatch.setattr(audit.sm, "load_master", no_master)
    assert audit.main(["run", "--folder", w.folder, "--stores", "eod"]) == 2
    assert "NotBuilt" in capsys.readouterr().out


# ---- NOT_IN_ICICI only on the ICICI plan's day list (COMPINFO 2026-11-19) ----

def test_not_in_icici_is_vendor_only_on_a_plan_day():                                             # 28
    m = audit.Manual(None)
    assert audit.classify_equity("NO_KITE_ROW+NOT_IN_ICICI", d(16), "S1", m, d(20), SAT, {d(16)}) ==         ("VENDOR", "NO_KITE_ROW+NOT_IN_ICICI", "")
    assert audit.classify_equity("NO_KITE_ROW+NOT_IN_ICICI", d(19), "S1", m, d(20), SAT, {d(16)}) ==         ("UNEXPLAINED", "NO_KITE_ROW+NOT_IN_ICICI_AFTER_PLAN", "")
    assert audit.classify_equity("NO_KITE_FOLDER+ICICI_MIXED_SERIES", d(19), "S1", m, d(20), SAT, set())[0] == "VENDOR"


def test_day_after_the_plan_is_unexplained_and_the_day_list_is_reported(tmp_path, monkeypatch):  # 29
    tables = {"S9": kite_table(WEEK),
              "S1": kite_table([], {d(16): "NO_KITE_ROW+NOT_IN_ICICI", d(19): "NO_KITE_ROW+NOT_IN_ICICI"})}
    w = World(tmp_path, monkeypatch, tables, plan_days={"S1": [d(16)], "S2": [d(17), d(18)]})
    assert w.run(stores=["equity"]) == 1
    got = {r["date"]: (r["reason"], r["evidence"]) for r in w.gaps().to_dict("records")}
    assert got == {"2026-11-16": ("VENDOR", "NO_KITE_ROW+NOT_IN_ICICI"),
                   "2026-11-19": ("UNEXPLAINED", "NO_KITE_ROW+NOT_IN_ICICI_AFTER_PLAN")}
    rep = w.report()
    assert audit.plan_days_path(w.folder) in rep and "ICICI plan day list rows" in rep and audit.PLAN_NOT_READ not in rep
    info = json.load(open(os.path.join(w.out(), "run_info.json"), encoding="utf-8"))
    assert info["icici_plan_days"] == {"path": audit.plan_days_path(w.folder), "rows": 3, "read": True, "error": None}
    assert audit.plan_days_path(w.folder) in info["inputs"]
    with open(audit.plan_days_path(w.folder), "wb") as f:      # unreadable list: both days UNEXPLAINED, said so
        f.write(b"not a parquet file")
    assert w.run(stores=["equity"]) == 1
    g = w.gaps(f"audit_{SAT:%Y-%m-%d}_b")
    assert set(g["reason"]) == {"UNEXPLAINED"} and len(g) == 2
    assert audit.PLAN_NOT_READ in w.report(f"audit_{SAT:%Y-%m-%d}_b")
    info = json.load(open(os.path.join(w.out(f"audit_{SAT:%Y-%m-%d}_b"), "run_info.json"), encoding="utf-8"))
    assert info["icici_plan_days"]["read"] is False and info["icici_plan_days"]["rows"] is None

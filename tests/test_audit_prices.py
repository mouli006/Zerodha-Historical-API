"""Data audit, price check (#251): backfill/equity/audit_prices.py on a synthetic world (no network, no secrets file).

Real files in tmp_path: a bhavcopy year file (LAST and the trading calendar), the Kite / ICICI days.parquet of each
stock, adjustments/events.csv and one corporate_actions file. The master is a small frame. A flag only: gaps, reasons,
kinds, the VERDICT and the exit code never change."""
import json
import os
from datetime import date, timedelta

import pandas as pd
import pytest

from backfill.equity import audit
from backfill.equity import audit_prices as ap
from backfill.equity import corp_actions as ca
from backfill.equity import icici_store as st
from backfill.equity import minute_plan as mp
from backfill.equity import minute_store as ms
from test_audit import SAT, WEEK, World, d, mrow

DAY = d(16)                                                     # Mon 16 Nov 2026, the checked day
CAL = [date(2026, 11, 2) + timedelta(days=i) for i in range(40)]
CAL = [x for x in CAL if x.weekday() < 5]                       # 2 Nov .. 11 Dec, weekdays


def kite_row(day=DAY, mc=100.0, mh=None, ml=None, scale=1.0, ratio=1.0, km=1.0, kd=0.0, kind="MULTIPLICATIVE",
             kc=100.0, bc=100.0, bh=101.0, bl=99.0, check="OK"):
    return {"date": day, "check": check, "kind": kind, "ratio": ratio, "kite_multiplier": km, "kite_difference": kd,
            "minute_scale": scale, "kite_close": kc, "minute_open": mc, "minute_high": mh if mh is not None else mc,
            "minute_low": ml if ml is not None else mc, "minute_close": mc, "minute_rows": 375,
            "bhav_close": bc, "bhav_high": bh, "bhav_low": bl}


def icici_row(day=DAY, mc=100.0, mh=None, ml=None, scale=1.0, bc=100.0, bh=101.0, bl=99.0, check="OK"):
    return {"date": day, "check": check, "icici_scale": scale, "minute_open": mc,
            "minute_high": mh if mh is not None else mc, "minute_low": ml if ml is not None else mc,
            "minute_close": mc, "minute_rows": 375, "bhav_close": bc, "bhav_high": bh, "bhav_low": bl}


class Prices:
    """The #251 inputs in folder: year file, days.parquet files, events.csv, corporate actions."""

    def __init__(self, folder, monkeypatch, last=None, events=(), dividends=(), year_file=True):
        self.folder = folder
        self.sids = []
        self.last = last or {}
        self.monkeypatch = monkeypatch
        self.year_file = year_file
        self.write_events(events)
        p = os.path.join(ca.input_dir(folder), "corporate_actions_2026.csv")
        os.makedirs(os.path.dirname(p), exist_ok=True)
        pd.DataFrame([{"SYMBOL": "X", "PURPOSE": "Dividend - Rs 5 Per Share", "EX-DATE": x.strftime("%d-%b-%Y"),
                       "ISIN": f"IN{sid}"} for sid, x in dividends],
                     columns=["SYMBOL", "PURPOSE", "EX-DATE", "ISIN"]).to_csv(p, index=False)

    def write_events(self, events):
        p = ap.events_path(self.folder)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        pd.DataFrame([{"security_id": s, "ex_date": x.isoformat(), "factor": f, "applied": "Y"} for s, x, f in events],
                     columns=["security_id", "ex_date", "factor", "applied"]).to_csv(p, index=False)

    def kite(self, sid, rows):
        self._days(ms.stock_dir(self.folder, sid), rows, sid)

    def icici(self, sid, rows):
        self._days(st.stock_dir(self.folder, sid), rows, sid)

    def _days(self, path, rows, sid):
        os.makedirs(path, exist_ok=True)
        pd.DataFrame(rows).to_parquet(os.path.join(path, ms.DAYS_FILE), index=False)
        if sid not in self.sids:
            self.sids.append(sid)

    def finish(self, secs=None):
        """The year file (every stock traded on every calendar day, LAST = last.get((sid, date), 100)) and the
        master (isin IN<sid> -> sid)."""
        sids = sorted(set(self.sids) | {"S0"})
        if self.year_file:
            rows = [{"date": x.isoformat(), "isin": f"IN{s}", "series": "EQ", "volume": 100,
                     "last": self.last.get((s, x), 100.0)} for s in sids for x in CAL]
            store = mp.store_dir(self.folder)
            os.makedirs(store, exist_ok=True)
            pd.DataFrame(rows).to_parquet(os.path.join(store, "2026.parquet"), index=False)
        secs = secs if secs is not None else pd.DataFrame(
            {"security_id": sids, "last_symbol": [f"SYM{s}" for s in sids], "instrument_kind": ["STOCK"] * len(sids)})
        owner = {f"IN{s}": s for s in sids}
        self.monkeypatch.setattr(audit.sm, "load_master",
                                 lambda folder=None: {"securities": secs, "isin_owner": owner})
        return self


def table(days, source="KITE"):
    """A minute_combined day table: every day traded, from source (or {date: source})."""
    src = source if isinstance(source, dict) else {x: source for x in days}
    return pd.DataFrame([{"date": x, "traded": True, "source": src[x], "reason": src[x], "kite_label": "OK",
                          "frozen": False} for x in days])


def check(p, sid, days=(DAY,), source="KITE", manual=None):
    pc = ap.PriceCheck(p.folder, manual if manual is not None else audit.Manual(None), SAT.date(),
                       {s: f"SYM{s}" for s in p.sids}, log=lambda *a: None)
    pc.stock(sid, table(list(days), source))
    return pc


# ---- one day per reason ----

CASES = {
    "KNOWN": dict(kite=kite_row(mc=50.0)),
    "SPLIT_BLIP": dict(kite=kite_row(mc=200.0, kc=50.0, km=2.0), events=[("S1", d(18), 2.0)]),
    "NOT_ADJUSTED": dict(kite=kite_row(mc=500.0), events=[("S1", date(2026, 12, 21), 5.0)]),
    "PENNY_TICK": dict(kite=kite_row(mc=0.5, kc=0.8, bc=0.8, bh=0.81, bl=0.79)),
    "DIVIDEND_CUT": dict(kite=kite_row(mc=100.0, ratio=0.9), dividends=[("S1", d(18))]),
    "JUNK_DAY_CANDLE": dict(kite=kite_row(mc=100.0, ratio=35.7, kc=2.8)),
    "STRAY_BARS": dict(kite=kite_row(mc=10.0, kc=100.0)),
    "ICICI_SCALE": dict(icici=icici_row(mc=50.0)),
    "ICICI_SHORT_DAY": dict(icici=icici_row(mc=70.0, check="SHORT_DAY")),
    "UNEXPLAINED": dict(kite=kite_row(mc=70.0, kc=90.0)),
}


@pytest.mark.parametrize("reason", list(CASES))
def test_one_day_per_reason(tmp_path, monkeypatch, reason):                                       # 1-10
    c = CASES[reason]
    p = Prices(str(tmp_path), monkeypatch, events=c.get("events", ()), dividends=c.get("dividends", ()))
    if "kite" in c:
        p.kite("S1", [c["kite"]])
    else:
        p.icici("S1", [c["icici"]])
    p.finish()
    manual = audit.Manual(audit.read_manual_frame([mrow("prices", "SCALE_KNOWN", "2026-11-16", sid="S1",
                                                        pid="P999")])) if reason == "KNOWN" else None
    pc = check(p, "S1", source="KITE" if "kite" in c else "ICICI", manual=manual)
    df = pc.frame()
    assert list(df["reason"]) == [reason], (df.to_dict("records"), pc.notes)
    assert list(df["list"]) == ["CLOSE"]
    if reason == "KNOWN":
        assert list(df["problem_id"]) == ["P999"]
    assert pc.errors == 0


def test_dividend_brought_back_by_the_stored_ratio_is_not_listed(tmp_path, monkeypatch):          # 11
    p = Prices(str(tmp_path), monkeypatch, dividends=[("S1", d(18))])
    p.kite("S1", [kite_row(mc=102.0, ratio=0.98, kc=102.0)])                    # 102 x 0.98 = 99.96 ~ NSE 100
    pc = check(p.finish(), "S1")
    assert len(pc.frame()) == 0 and pc.checked["KITE"] == 1 and pc.small["KITE"] == 0


def test_icici_half_price_is_icici_scale_with_the_later_factor(tmp_path, monkeypatch):            # 12
    p = Prices(str(tmp_path), monkeypatch, events=[("S1", date(2026, 12, 21), 2.0)])    # BRITANNIA kind (P224)
    p.icici("S1", [icici_row(mc=50.0)])
    df = check(p.finish(), "S1", source="ICICI").frame()
    assert list(df["reason"]) == ["ICICI_SCALE"] and list(df["problem_id"]) == ["P165/P224"]
    assert "later factor 2" in df["evidence"].iat[0] and "2026-12-21" in df["evidence"].iat[0]


def test_mixed_icici_day_and_none_day_are_never_checked(tmp_path, monkeypatch):                   # 13
    p = Prices(str(tmp_path), monkeypatch)
    p.icici("S1", [icici_row(day=d(16), mc=10.0), icici_row(day=d(17), mc=10.0)])
    p.finish()
    pc = ap.PriceCheck(p.folder, audit.Manual(None), SAT.date(), log=lambda *a: None)
    pc.stock("S1", table([d(16), d(17)], {d(16): "NONE", d(17): "NONE"}))       # a mixed day is NONE in the table
    assert len(pc.frame()) == 0 and sum(pc.checked.values()) == 0


def test_close_off_against_close_but_fine_against_last_is_not_listed(tmp_path, monkeypatch):      # 14
    p = Prices(str(tmp_path), monkeypatch, last={("S1", DAY): 108.0})
    p.kite("S1", [kite_row(mc=107.0, kc=107.0, bh=110.0)])                      # 7 % off close, 0.9 % off LAST
    pc = check(p.finish(), "S1")
    assert len(pc.frame()) == 0 and pc.small["KITE"] == 0


def test_highlow_list_and_the_two_percent_line(tmp_path, monkeypatch):                           # 15
    p = Prices(str(tmp_path), monkeypatch)
    p.kite("S1", [kite_row(day=d(16), mc=100.0, mh=125.0, bh=100.0), kite_row(day=d(17), mc=103.0, kc=103.0, bh=104.0)])
    pc = check(p.finish(), "S1", days=(d(16), d(17)))
    df = pc.frame()
    assert list(df["list"]) == ["HIGHLOW"] and list(df["date"]) == [d(16)] and df["hl_off_pct"].iat[0] == 25.0
    assert pc.small["KITE"] == 1
    text = "\n".join(pc.report_lines())
    assert "more than 2 %: 1 (Kite 1, ICICI 0)" in text and "HIGHLOW" in text and "loaded days checked 2" in text


def test_manual_prices_row_known_and_unused_row_listed(tmp_path, monkeypatch):                    # 16
    p = Prices(str(tmp_path), monkeypatch)
    p.kite("S1", [kite_row(mc=50.0)])
    p.finish()
    m = audit.Manual(audit.read_manual_frame([mrow("prices", "SCALE", "2026-11-16", sid="S1", pid="P1"),
                                              mrow("prices", "SCALE", "2026-11-18", sid="S1", pid="P2")]))
    df = check(p, "S1", manual=m).frame()
    assert list(df["reason"]) == ["KNOWN"]
    assert [r["problem_id"] for r in m.unused()] == ["P2"]


def price_world(tmp_path, monkeypatch, off, manual=None):
    """The #249 World plus the #251 inputs; S1 loads from Kite on the week, 16 Nov at half price when off."""
    w = World(tmp_path, monkeypatch, {"S9": table(WEEK), "S1": table(WEEK)}, manual=manual or [])
    p = Prices(w.folder, monkeypatch)
    p.kite("S1", [kite_row(day=x, mc=50.0 if (off and x == DAY) else 100.0) for x in WEEK])
    p.kite("S9", [kite_row(day=x) for x in WEEK])
    secs = pd.DataFrame({"security_id": ["S1", "S9"], "last_symbol": ["ONE", "NINE"], "instrument_kind": ["STOCK"] * 2})
    p.finish(secs)
    return w


def test_flag_only_same_verdict_and_exit_code(tmp_path, monkeypatch):                             # 17
    codes, infos, gaps = [], [], []
    for off in (False, True):
        root = tmp_path / str(off)
        w = price_world(root, monkeypatch, off, manual=[mrow("prices", "X", "2026-11-19", sid="S1", pid="P7")])
        codes.append(w.run(stores=["equity"]))
        infos.append(json.load(open(os.path.join(w.out(), "run_info.json"))))
        gaps.append(w.gaps().to_dict("records"))
        if off:
            rep = w.report()
            days = pd.read_csv(os.path.join(w.out(), "price_days.csv"), dtype=str)
            assert list(days["reason"]) == ["STRAY_BARS"] and list(days["date"]) == ["2026-11-16"]
            assert "prices (#251, flag only" in rep and "unused manual row (matches no gap): prices S1" in rep
            assert os.path.isfile(os.path.join(w.out(), "price_summary.csv"))
    assert codes[0] == codes[1] == 0
    assert infos[0]["verdict"] == infos[1]["verdict"] == "CLEAN" and gaps[0] == gaps[1] == []
    assert infos[0]["prices"]["close_days"] == 0 and infos[1]["prices"]["close_days"] == 1
    assert infos[1]["prices"]["checked_total"] == 10


def test_broken_days_file_or_missing_year_file_is_a_note(tmp_path, monkeypatch):                  # 18
    w = price_world(tmp_path / "a", monkeypatch, True)
    with open(os.path.join(ms.stock_dir(w.folder, "S1"), ms.DAYS_FILE), "wb") as f:
        f.write(b"not a parquet file")
    assert w.run(stores=["equity"]) == 0
    assert "S1: price check failed" in w.report()
    w2 = price_world(tmp_path / "b", monkeypatch, True)
    os.remove(os.path.join(mp.store_dir(w2.folder), "2026.parquet"))
    assert w2.run(stores=["equity"]) == 0
    assert "price check not run" in w2.report()
    info = json.load(open(os.path.join(w2.out(), "run_info.json")))
    assert info["prices"]["ran"] is False and info["exit_code"] == 0

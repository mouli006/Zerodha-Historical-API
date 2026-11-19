"""Two job slots (task #247, P208): a Kite and a Dhan F&O download at the same time, each page seeing and stopping only
its own job. Fake runs held on Events, no network, the .env stubbed (conftest points config.ENV_PATH at a temp file)."""
import threading
import time
from datetime import date, datetime

import pytest

import dashboard
import dhan
from app import app
from backfill import bhav, fno, job
from backfill.fetch import RateLimiter, StopJob, fetch_chunk
from backfill.job import JOB, STOP, job_begin, job_update, slot, stop_event

IST = fno.IST
TOKEN = "SECRET-DHAN-TOKEN"
DAY = "2026-10-12"
WINDOW = fno.Window(date(2026, 10, 10), date(2026, 10, 12), date(2026, 10, 13), date(2026, 10, 9), False)
ENV = {"KITE_ACCESS_TOKEN": "x", "DHAN_CLIENT_ID": "1000000001", "DHAN_ACCESS_TOKEN": TOKEN,
       "DHAN_TOKEN_EXPIRY": "2099-01-01T09:00:00"}


class FakeDhan:
    def __init__(self, token):
        self.token = token

    def profile(self):
        return {"user_id": "1000000001", "data_plan": "Active"}


class Runs:
    """Fake fno.run_daily / run_missing: each run waits until released (or its own STOP is set) and reports the slot
    its thread is bound to."""

    def __init__(self):
        self.release = {s: threading.Event() for s in job.SLOTS}
        self.entered = {s: threading.Event() for s in job.SLOTS}
        self.seen = {}
        self.result = {}

    def __call__(self, *a, **k):
        src = k.get("source", "kite")
        self.seen[src] = job.current_slot()
        self.entered[src].set()
        while not self.release[src].wait(0.01):
            if STOP.is_set():
                job_update(state="stopped", message="Stopped.")
                self.result[src] = "stopped"
                return "stopped"
        job_update(state="finished", message="Done.")
        self.result[src] = "finished"
        return "finished"


@pytest.fixture
def world(monkeypatch, tmp_path):
    """Logged in to Kite and Dhan; Kite and Dhan F&O starts post to separate folders; workers are tracked and always
    released and joined at the end."""
    runs, workers, planner = Runs(), [], []
    real_start = job.start_worker

    def tracked(name, target, *args):
        w = real_start(name, target, *args)
        workers.append(w)
        return w

    for mod in (dashboard, dhan):
        monkeypatch.setattr(mod, "start_worker", tracked)
        monkeypatch.setattr(mod, "load_env", lambda: dict(ENV))
        monkeypatch.setattr(mod, "save_env", lambda updates: None)
        monkeypatch.setattr(mod, "keep_zero_volume", lambda: False)
    monkeypatch.setattr(dashboard, "credentials", lambda: ("key", ""))
    monkeypatch.setattr(dashboard, "weekly_expiry_weekday", lambda underlying="NIFTY": 1)
    monkeypatch.setattr(dashboard, "fno_window", lambda folder, underlying: (WINDOW, ""))
    monkeypatch.setattr(dhan, "dhan_credentials", lambda: (ENV["DHAN_CLIENT_ID"], TOKEN))
    monkeypatch.setattr(dhan, "weekly_expiry_weekday", lambda underlying: 1)
    monkeypatch.setattr(dhan, "fno_window", lambda folder, underlying, token: (WINDOW, ""))
    monkeypatch.setattr(dhan, "DhanClient", FakeDhan)
    monkeypatch.setattr(fno, "now_ist", lambda: datetime(2026, 10, 12, 16, 0, tzinfo=IST))
    monkeypatch.setattr(fno, "run_daily", runs)
    monkeypatch.setattr(fno, "run_missing", runs)

    def no_nse(run_date, log=print, **kw):
        raise bhav.BhavNotAvailable("NSE has no F&O bhavcopy for this day yet (404: test).")
    monkeypatch.setattr(bhav, "fetch_bhav", no_nse)

    def fake_bulk(*args):
        planner.append(job.current_slot())
        job_update(state="finished", message="Done.")
    monkeypatch.setattr(dashboard, "bulk_download", fake_bulk)

    c = app.test_client()
    with c.session_transaction() as s:
        s["access_token"] = "x"
        s["user"] = {"user_id": "AB1234", "user_name": "Demo", "email": "", "broker": "ZERODHA", "login_time": ""}
    folders = {"kite": tmp_path / "kite", "dhan": tmp_path / "dhan", "planner": tmp_path / "planner"}
    for f in folders.values():
        f.mkdir()
    w = {"client": c, "runs": runs, "workers": workers, "folders": folders, "planner": planner}
    yield w
    for e in runs.release.values():
        e.set()
    for t in workers:
        t.join(timeout=5)
    assert not any(t.is_alive() for t in workers)


def start_kite(w, folder=None):
    r = w["client"].post("/download/fno-start", data={"folder": str(folder or w["folders"]["kite"]), "speed": "2.5",
                                                      "date": DAY})
    assert r.status_code == 302
    return r.headers["Location"]


def start_dhan(w, folder=None):
    r = w["client"].post("/dhan/fno-start", data={"folder": str(folder or w["folders"]["dhan"]), "speed": "4",
                                                  "date": DAY})
    assert r.status_code == 302
    return r.headers["Location"]


def start_planner(w):
    r = w["client"].post("/download/start", data={"interval": "minute", "from": "2026-01-01", "to": "2026-01-05",
                                                  "speed": "2.5", "equity": "1", "format": "csv",
                                                  "folder": str(w["folders"]["planner"])})
    assert r.status_code == 302
    return r.headers["Location"]


def both_running(w):
    start_kite(w)
    start_dhan(w)
    assert w["runs"].entered["kite"].wait(5) and w["runs"].entered["dhan"].wait(5)


def wait_for(cond):
    for _ in range(250):
        if cond():
            return True
        time.sleep(0.02)
    return False


# 1. Kite and Dhan run at the same time ------------------------------------------------------------------------

def test_kite_and_dhan_run_together(world):
    both_running(world)
    assert slot("kite")["state"] == "running" and slot("dhan")["state"] == "running"
    assert slot("kite")["source"] == "kite" and slot("dhan")["source"] == "dhan"
    assert world["runs"].seen == {"kite": "kite", "dhan": "dhan"}   # each worker writes its own slot
    assert len([t for t in world["workers"] if t.is_alive()]) == 2


# 2. each page polls only its own job --------------------------------------------------------------------------

def test_status_shows_only_own_source(world):
    both_running(world)
    c = world["client"]
    dj, kj = c.get("/download/status/dhan").get_json(), c.get("/download/status/kite").get_json()
    assert dj["source"] == "dhan" and dj["folder"].endswith("dhan")
    assert kj["source"] == "kite" and kj["folder"].endswith("kite")
    assert c.get("/download/status?source=dhan").get_json()["source"] == "dhan"
    assert c.get("/download/status").get_json()["source"] == "kite"             # no source = Kite, as before
    assert c.get("/download/status/other").status_code == 400


def test_pages_get_their_own_status_and_stop_urls(world):
    html = world["client"].get("/success").get_data(as_text=True)
    assert "/download/stop/kite" in html and "/download/status/kite" in html and "/download/stop/dhan" not in html
    html = world["client"].get("/dhan/dashboard").get_data(as_text=True)
    assert "/download/stop/dhan" in html and "/download/status/dhan" in html and "/download/stop/kite" not in html


# 3. Stop on the Dhan page stops only the Dhan run -------------------------------------------------------------

def test_stop_dhan_stops_only_dhan(world):
    both_running(world)
    world["client"].post("/download/stop/dhan")
    assert stop_event("dhan").is_set() and not stop_event("kite").is_set()
    assert wait_for(lambda: world["runs"].result.get("dhan") == "stopped")
    assert slot("dhan")["state"] == "stopped" and slot("kite")["state"] == "running"
    world["runs"].release["kite"].set()
    assert wait_for(lambda: world["runs"].result.get("kite") == "finished")
    assert slot("kite")["state"] == "finished"


def test_stop_without_source_stops_kite(world):
    both_running(world)
    world["client"].post("/download/stop")
    assert stop_event("kite").is_set() and not stop_event("dhan").is_set()


# 4. a second start of the same source is refused --------------------------------------------------------------

def test_second_start_of_same_source_refused(world, tmp_path):
    both_running(world)
    other = tmp_path / "other"
    other.mkdir()
    assert "already+running" in start_kite(world, other)
    assert "already+running" in start_dhan(world, other)
    assert slot("kite")["folder"].endswith("kite") and slot("dhan")["folder"].endswith("dhan")


# 5. the planner shares the Kite slot --------------------------------------------------------------------------

def test_planner_refused_while_kite_fno_runs(world):
    start_kite(world)
    assert world["runs"].entered["kite"].wait(5)
    start_planner(world)
    assert world["planner"] == [] and slot("kite")["kind"] == "fno"


def test_planner_allowed_while_only_dhan_runs(world):
    start_dhan(world)
    assert world["runs"].entered["dhan"].wait(5)
    start_planner(world)
    assert wait_for(lambda: world["planner"] == ["kite"])
    assert slot("kite")["kind"] == "backfill" and slot("dhan")["state"] == "running"


# 6. single-card guards look at their own source ---------------------------------------------------------------

def check(w, source):
    return w["client"].post("/fno/bhav-check", data={"source": source, "folder": str(w["folders"][source]),
                                                    "date": DAY}).get_json()


def test_dhan_bhav_check_refused_only_while_dhan_runs(world):
    start_kite(world)
    assert world["runs"].entered["kite"].wait(5)
    assert "running" not in str(check(world, "dhan"))          # only Kite runs: the Dhan card may check
    assert "Kite download is running" in check(world, "kite")["error"]
    start_dhan(world)
    assert world["runs"].entered["dhan"].wait(5)
    assert "Dhan download is running" in check(world, "dhan")["error"]


def test_kite_bhav_check_allowed_while_only_dhan_runs(world):
    start_dhan(world)
    assert world["runs"].entered["dhan"].wait(5)
    assert "running" not in str(check(world, "kite"))


# 7. Check all pending and no-bhav wait for every download (D204) ----------------------------------------------

def test_check_all_and_no_bhav_refused_while_either_runs(world):
    c = world["client"]
    start_dhan(world)
    assert world["runs"].entered["dhan"].wait(5)
    assert c.post("/fno/bhav-check-all", data={"keep_zero": "1"}).get_json()["error"] == \
        "A Dhan download is running. Try again when it has finished."
    no_bhav = c.post("/fno/bhav-no-bhav", data={"source": "kite", "folder": str(world["folders"]["kite"]),
                                               "date": DAY, "action": "mark"}).get_json()
    assert "Dhan download is running" in no_bhav["error"]
    start_kite(world)
    assert world["runs"].entered["kite"].wait(5)
    assert "Kite and Dhan downloads are running" in c.post("/fno/bhav-check-all", data={"keep_zero": "1"}).get_json()["error"]


# 8. same save folder guard (D205, P209) -----------------------------------------------------------------------

def test_same_folder_refused_also_in_other_case(tmp_path):
    folder = str(tmp_path / "Shared")
    assert job_begin(**fno.job_fields("NIFTY", date(2026, 10, 12), folder, 2.5)) is True
    before = dict(slot("dhan"))
    assert job_begin(**fno.job_fields("NIFTY", date(2026, 10, 12), folder.upper(), 4, source="dhan")) is False
    assert job.last_refusal("dhan") == "same_folder:kite" and "same save folder" in job.refusal_text("dhan")
    assert slot("dhan") == before                                 # nothing changed in the other slot
    assert job_begin(**fno.job_fields("NIFTY", date(2026, 10, 12), str(tmp_path / "other"), 4, source="dhan")) is True


def test_same_folder_start_refused_on_the_page(world):
    start_kite(world)
    assert world["runs"].entered["kite"].wait(5)
    assert "same+save+folder" in start_dhan(world, world["folders"]["kite"])
    assert slot("dhan")["state"] == "idle"


# 9. thread binding and counters -------------------------------------------------------------------------------

def run_in(name, fn):
    out = {}
    t = job.start_worker(name, lambda: out.update(v=fn()))
    t.join(timeout=5)
    return out.get("v")


class FakeKite:
    def historical_data(self, *a, **k):
        return [{"date": "x"}]


def test_unbound_thread_is_kite_and_job_begin_binds():
    assert job.current_slot() == "kite"
    assert job_begin(source="dhan", folder="X") is True
    assert job.current_slot() == "dhan" and JOB["folder"] == "X" and slot("kite")["folder"] == ""


def test_throwaway_slot_touches_no_slot():
    with job.use_slot(None):
        job_update(message="scratch")
        job.job_incr("requests")
        assert JOB["requests"] == 1
    assert slot("kite")["message"] == "" and slot("dhan")["message"] == ""
    assert slot("kite")["requests"] == 0 and slot("dhan")["requests"] == 0
    assert job.current_slot() == "kite"


def test_fetch_counters_land_in_the_bound_slot():
    start, end = datetime(2026, 10, 12, 9, 15), datetime(2026, 10, 12, 15, 30)
    assert run_in("dhan", lambda: fetch_chunk(FakeKite(), RateLimiter(100), 1, start, end, "minute")) == [{"date": "x"}]
    assert slot("dhan")["requests"] == 1 and slot("kite")["requests"] == 0


# 10. an old Stop no longer breaks a re-download (D206, P210) ------------------------------------------------------

def test_old_stop_breaks_fetch_on_the_slot_but_not_on_a_throwaway_slot():
    start, end = datetime(2026, 10, 12, 9, 15), datetime(2026, 10, 12, 15, 30)
    stop_event("kite").set()                                      # left set by a stopped Kite run
    with pytest.raises(StopJob):
        fetch_chunk(FakeKite(), RateLimiter(100), 1, start, end, "minute")
    with job.use_slot(None):
        assert fetch_chunk(FakeKite(), RateLimiter(100), 1, start, end, "minute") == [{"date": "x"}]


def test_redownload_route_runs_on_a_throwaway_slot(world, monkeypatch):
    seen = {}

    def fake(*a, **k):
        seen.update(slot=job.current_slot(), stop=STOP.is_set())
        return False, "not saved"
    monkeypatch.setattr(fno, "redownload_contract", fake)
    stop_event("kite").set()                                      # an old Stop from a stopped Kite run
    j = world["client"].post("/fno/redownload", data={"folder": str(world["folders"]["kite"]), "date": DAY,
                                                     "contract": "NIFTY2510013OCT26CE"}).get_json()
    assert j["message"] == "not saved" and seen == {"slot": None, "stop": False}
    assert slot("kite")["requests"] == 0

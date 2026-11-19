"""In-memory job state: one slot per broker ("kite", "dhan"), so a Kite and a Dhan download can run at the same time.

The equity planner (kind "backfill") and the Kite F&O run share the "kite" slot (one Kite job at a time); the Dhan F&O
run has the "dhan" slot. Each slot has its own job dict and its own stop event.

JOB and STOP keep their old names and behave like the old dict / Event, but they are views of the slot the *current
thread* works for. A worker thread is bound to its slot when it starts (start_worker), job_begin binds the calling thread
(the nifty_daily.py path), and a thread that was never bound uses "kite" (the CLI tools, the equity downloads). So
backfill/fno.py, fetch.py, runner.py and the equity modules use JOB / STOP / job_update / job_incr unchanged.
"""
import copy
import threading
from collections.abc import MutableMapping
from contextlib import contextmanager

from config import same_path

DEFAULT_SPEED = 2.5   # requests per second, start to start
SLOTS = ("kite", "dhan")
BROKER = {"kite": "Kite", "dhan": "Dhan"}

JOB_LOCK = threading.Lock()
FRESH = {"state": "idle", "run": "", "interval": "", "from": "", "to": "", "speed": DEFAULT_SPEED,
         "format": "", "equity": True, "indices": [], "part": "", "equity_total": 0, "out_dirs": [],
         "etfs": False, "t2t": False, "filter_info": "", "folder": "", "pass": 0, "stocks_done": 0, "stocks_total": 0,
         "chunks_done": 0, "chunks_total": 0, "eta": "", "requests": 0, "n429": 0, "net_errors": 0, "failed": 0,
         "empty": 0, "rate": DEFAULT_SPEED, "symbol": "", "chunk": "", "message": "",
         "job_no": 0, "completed": [],  # completed: [symbol, time] per stock finished this job
         # kind: "backfill" (planner) or "fno" (daily F&O run, backfill/fno.py)
         "kind": "backfill", "fno_date": "", "fno_underlying": "", "fno_exchange": "",
         # source of an F&O job: "kite" or "dhan" (backfill/dhan_api.py)
         "source": "",
         "fno_summary": [], "fno_futures": [],
         # F&O: keep_zero = "keep zero-volume contracts" setting; zero_kept / zero_skipped count contracts
         # whose candles all have volume 0 (empty counts those without candles)
         "keep_zero": False, "zero_kept": 0, "zero_skipped": 0,
         # F&O: trim_to_bhav = "download only contracts in the bhavcopy" setting of the job
         "trim_to_bhav": False}
JOBS = {name: copy.deepcopy(FRESH) for name in SLOTS}
STOPS = {name: threading.Event() for name in SLOTS}
REFUSALS = {name: "" for name in SLOTS}   # why the last job_begin of a slot was refused ("" = it was not)
# Per-job counters, reset by job_begin()
COUNTERS = {"pass": 0, "stocks_done": 0, "stocks_total": 0, "chunks_done": 0, "chunks_total": 0,
            "eta": "", "requests": 0, "n429": 0, "net_errors": 0, "failed": 0, "empty": 0,
            "zero_kept": 0, "zero_skipped": 0,
            "symbol": "", "chunk": "", "part": "", "filter_info": "", "out_dirs": [], "source": ""}

_local = threading.local()   # .slot: "kite" / "dhan" / None (throwaway); .scratch: (dict, Event) for None


def current_slot():
    """The slot the calling thread works for: "kite", "dhan", or None (a throwaway slot, use_slot(None))."""
    return getattr(_local, "slot", "kite")


def _job():
    name = current_slot()
    return _local.scratch[0] if name is None else JOBS[name]


def _stop():
    name = current_slot()
    return _local.scratch[1] if name is None else STOPS[name]


class _JobView(MutableMapping):
    """JOB: the job dict of the current thread's slot."""

    def __getitem__(self, key):
        return _job()[key]

    def __setitem__(self, key, value):
        _job()[key] = value

    def __delitem__(self, key):
        del _job()[key]

    def __iter__(self):
        return iter(_job())

    def __len__(self):
        return len(_job())

    def __repr__(self):
        return f"JOB[{current_slot()}]={_job()!r}"


class _StopView:
    """STOP: the stop event of the current thread's slot."""

    def is_set(self):
        return _stop().is_set()

    def set(self):
        _stop().set()

    def clear(self):
        _stop().clear()

    def wait(self, timeout=None):
        return _stop().wait(timeout)


JOB = _JobView()
STOP = _StopView()


def slot(name):
    """The job dict of a slot (read it under JOB_LOCK)."""
    return JOBS[name]


def stop_event(name):
    return STOPS[name]


def bind(name):
    """Bind the calling thread to a slot ("kite" / "dhan")."""
    _local.slot = name


def unbind():
    _local.__dict__.pop("slot", None)
    _local.__dict__.pop("scratch", None)


@contextmanager
def use_slot(name):
    """Run a block on a slot; None = a throwaway slot with its own counters and its own never-set stop event
    (a single-contract re-download is not a job: it moves no job's counters and never sees an old Stop)."""
    saved = dict(_local.__dict__)
    if name is None:
        _local.scratch = (copy.deepcopy(FRESH), threading.Event())
    _local.slot = name
    try:
        yield
    finally:
        _local.__dict__.clear()
        _local.__dict__.update(saved)


def start_worker(name, target, *args):
    """Start a daemon thread bound to slot name."""
    def run():
        bind(name)
        target(*args)
    worker = threading.Thread(target=run, daemon=True)
    worker.start()
    return worker


def job_update(**kw):
    with JOB_LOCK:
        _job().update(kw)


def job_incr(key, n=1):
    with JOB_LOCK:
        _job()[key] += n


def job_begin(slot=None, **fields):
    """Mark a new job as running on its slot with fresh counters and bind the calling thread to it. The slot is
    `slot`, else "dhan" for source="dhan", else "kite". Returns exactly True or False: False (nothing changed) while
    that slot runs, or while the other slot runs a job in the same save folder (one progress.sqlite per folder);
    the reason is in refusal_text(slot)."""
    name = slot or ("dhan" if fields.get("source") == "dhan" else "kite")
    with JOB_LOCK:
        job = JOBS[name]
        if job["state"] == "running":
            REFUSALS[name] = "running"
            return False
        folder = fields.get("folder", "")
        for other in SLOTS:
            if other != name and JOBS[other]["state"] == "running" and same_path(JOBS[other]["folder"], folder):
                REFUSALS[name] = "same_folder:" + other
                return False
        REFUSALS[name] = ""
        job.update(copy.deepcopy(COUNTERS), completed=[], state="running", message="Starting...", **fields)
        job["rate"] = job["speed"]
        job["job_no"] += 1
    bind(name)
    return True


def last_refusal(name):
    return REFUSALS[name]


def refusal_text(name):
    """The message for a refused job_begin of slot name."""
    reason = REFUSALS[name]
    if reason.startswith("same_folder:"):
        other = reason.split(":", 1)[1]
        with JOB_LOCK:
            folder = JOBS[other]["folder"]
        return (f"The {BROKER[other]} download is running in the same save folder ({folder}); two downloads can't "
                f"share one folder. Wait for it to finish or choose another folder.")
    return "A download is already running. Stop it or wait for it to finish."


def running_brokers():
    """Names of the brokers whose slot runs a job, e.g. ["Kite"]."""
    with JOB_LOCK:
        return [BROKER[name] for name in SLOTS if JOBS[name]["state"] == "running"]


def running_text():
    """"A Kite download is running. ..." for the routes that wait for every download (check all, no-bhav)."""
    names = running_brokers()
    if not names:
        return ""
    if len(names) == 1:
        return f"A {names[0]} download is running. Try again when it has finished."
    return f"{' and '.join(names)} downloads are running. Try again when they have finished."


def reset_slots():
    """Tests: both slots back to a fresh idle job, both stop events cleared, the calling thread unbound."""
    with JOB_LOCK:
        for name in SLOTS:
            JOBS[name].clear()
            JOBS[name].update(copy.deepcopy(FRESH))
            STOPS[name].clear()
            REFUSALS[name] = ""
    unbind()

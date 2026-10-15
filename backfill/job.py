import copy
import threading

DEFAULT_SPEED = 2.5   # requests per second, start to start

JOB_LOCK = threading.Lock()
JOB = {"state": "idle", "run": "", "interval": "", "from": "", "to": "", "speed": DEFAULT_SPEED,
       "format": "", "equity": True, "indices": [], "part": "", "equity_total": 0, "out_dirs": [],
       "etfs": False, "t2t": False, "filter_info": "", "folder": "", "pass": 0, "stocks_done": 0, "stocks_total": 0, "chunks_done": 0,
       "chunks_total": 0, "eta": "", "requests": 0, "n429": 0, "net_errors": 0, "failed": 0,
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
STOP = threading.Event()
# Per-job counters, reset by job_begin()
COUNTERS = {"pass": 0, "stocks_done": 0, "stocks_total": 0, "chunks_done": 0, "chunks_total": 0,
            "eta": "", "requests": 0, "n429": 0, "net_errors": 0, "failed": 0, "empty": 0,
            "zero_kept": 0, "zero_skipped": 0,
            "symbol": "", "chunk": "", "part": "", "filter_info": "", "out_dirs": [], "source": ""}


def job_update(**kw):
    with JOB_LOCK:
        JOB.update(kw)


def job_incr(key, n=1):
    with JOB_LOCK:
        JOB[key] += n


def job_begin(**fields):
    """Mark a new job as running with fresh counters; False if a job is already running."""
    with JOB_LOCK:
        if JOB["state"] == "running":
            return False
        JOB.update(copy.deepcopy(COUNTERS), completed=[], state="running", message="Starting...", **fields)
        JOB["rate"] = JOB["speed"]
        JOB["job_no"] += 1
        return True

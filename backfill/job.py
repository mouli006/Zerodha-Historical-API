import threading

DEFAULT_SPEED = 2.5   # requests per second, start to start

JOB_LOCK = threading.Lock()
JOB = {"state": "idle", "run": "", "interval": "", "from": "", "to": "", "speed": DEFAULT_SPEED,
       "format": "", "etfs": False, "t2t": True, "filter_info": "", "folder": "", "pass": 0, "stocks_done": 0, "stocks_total": 0, "chunks_done": 0,
       "chunks_total": 0, "eta": "", "requests": 0, "n429": 0, "net_errors": 0, "failed": 0,
       "empty": 0, "rate": DEFAULT_SPEED, "symbol": "", "chunk": "", "message": "",
       "job_no": 0, "completed": []}  # completed: [symbol, time] per stock finished this job
STOP = threading.Event()


def job_update(**kw):
    with JOB_LOCK:
        JOB.update(kw)


def job_incr(key, n=1):
    with JOB_LOCK:
        JOB[key] += n

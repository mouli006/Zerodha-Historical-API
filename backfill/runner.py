import time
from datetime import datetime

from kiteconnect import KiteConnect

from .fetch import AuthStop, ChunkFailed, RateLimiter, StopJob, fetch_chunk, make_chunks
from .instruments import MAX_STOCKS, is_equity, is_etf, series_suffix
from .job import JOB, JOB_LOCK, STOP, job_incr, job_update
from .storage import log_error, open_db, run_key, save_candles


def fmt_eta(seconds):
    seconds = int(seconds)
    return f"{seconds // 3600}h {seconds % 3600 // 60:02d}m"


def bulk_download(api_key, access_token, interval, start, end, speed, etfs, t2t, data_root, fmt):
    run = run_key(interval, start, end, fmt)
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    limiter = RateLimiter(speed)
    db = open_db(data_root)
    try:
        job_update(message="Loading instrument list...")
        eq = [i for i in kite.instruments("NSE")
              if i["segment"] == "NSE" and i["instrument_type"] == "EQ"]
        series_ok = [i for i in eq if is_equity(i, t2t)]
        instruments = [i for i in series_ok if etfs or not is_etf(i)]
        symbols = {i["tradingsymbol"] for i in instruments}
        n_t2t = sum(1 for i in instruments if series_suffix(i))
        filter_info = (f"{len(instruments):,} stocks (normal {len(instruments) - n_t2t:,}"
                       + (f" + -BE/-BZ {n_t2t:,}" if t2t else "") + ") | excluded "
                       f"{len(eq) - len(series_ok):,} bonds/other series"
                       + ("" if etfs else f", {len(series_ok) - len(instruments):,} ETFs"))
        job_update(filter_info=filter_info)
        print(f"[{run}] Stocks selected: {filter_info}", flush=True)
        if len(instruments) > MAX_STOCKS:
            msg = (f"Not started: {len(instruments):,} stocks selected (limit {MAX_STOCKS:,}). "
                   "The symbol filter may be broken.")
            print(f"[{run}] {msg}", flush=True)
            job_update(state="stopped", message=msg)
            return

        chunks = make_chunks(start, end, interval)
        db.executemany(
            "INSERT OR IGNORE INTO chunks (run, symbol, token, chunk_from, chunk_to, status) "
            "VALUES (?, ?, ?, ?, ?, 'PENDING')",
            [(run, i["tradingsymbol"], i["instrument_token"], str(a), str(b))
             for i in instruments for a, b in chunks])
        db.commit()

        rows = [r for r in db.execute(
            "SELECT symbol, status FROM chunks WHERE run = ?", (run,)) if r[0] in symbols]
        remaining = {}
        for sym, status in rows:
            remaining.setdefault(sym, 0)
            if status != "DONE":
                remaining[sym] += 1
        job_update(stocks_total=len(remaining),
                   stocks_done=sum(1 for n in remaining.values() if n == 0),
                   chunks_total=len(rows),
                   chunks_done=sum(1 for _, s in rows if s == "DONE"),
                   failed=sum(1 for _, s in rows if s == "FAILED"), message="")
        print(f"[{run}] {len(remaining)} stocks, {len(rows)} chunks, "
              f"{JOB['chunks_done']} already done", flush=True)

        t0, processed = time.monotonic(), 0
        pass_no = 0
        while True:
            pass_no += 1
            todo = [r for r in db.execute(
                "SELECT symbol, token, chunk_from, chunk_to, status FROM chunks "
                "WHERE run = ? AND status != 'DONE' ORDER BY symbol, chunk_from", (run,))
                if r[0] in symbols]
            if not todo:
                break
            job_update(**{"pass": pass_no})
            print(f"[{run}] pass {pass_no}: {len(todo)} chunks to fetch", flush=True)
            newly_done = 0

            for sym, token, cfrom, cto, prev_status in todo:
                if STOP.is_set():
                    raise StopJob()
                job_update(symbol=sym, chunk=cfrom[:10])
                status, error = "DONE", None
                try:
                    candles = fetch_chunk(kite, limiter, token, datetime.fromisoformat(cfrom),
                                          datetime.fromisoformat(cto), interval)
                    if candles:
                        save_candles(data_root, interval, sym, token, candles, fmt)
                    else:
                        job_incr("empty")
                except ChunkFailed as e:
                    status, error = "FAILED", str(e)
                    log_error(data_root, run, sym, cfrom, error)
                except (AuthStop, StopJob):
                    raise
                except Exception as e:
                    status, error = "FAILED", f"{type(e).__name__}: {e}"
                    log_error(data_root, run, sym, cfrom, error)

                db.execute("UPDATE chunks SET status = ?, tries = tries + 1, error = ?, updated = ? "
                           "WHERE run = ? AND symbol = ? AND chunk_from = ?",
                           (status, error, datetime.now().isoformat(timespec="seconds"),
                            run, sym, cfrom))
                db.commit()

                processed += 1
                if status == "DONE":
                    newly_done += 1
                    remaining[sym] -= 1
                    job_incr("chunks_done")
                    if remaining[sym] == 0:
                        with JOB_LOCK:
                            JOB["stocks_done"] += 1
                            JOB["completed"].append([sym, datetime.now().isoformat(timespec="seconds")])
                    if prev_status == "FAILED":
                        job_incr("failed", -1)
                elif prev_status != "FAILED":
                    job_incr("failed")

                left = JOB["chunks_total"] - JOB["chunks_done"]
                job_update(eta=fmt_eta((time.monotonic() - t0) / processed * left))
                if processed % 50 == 0:
                    print(f"[{run}] stocks {JOB['stocks_done']}/{JOB['stocks_total']} | chunks "
                          f"{JOB['chunks_done']}/{JOB['chunks_total']} | ETA {JOB['eta']} | "
                          f"429s {JOB['n429']} | failed {JOB['failed']}", flush=True)

            if newly_done == 0:
                break  # a full re-loop pass fixed nothing; stop looping

        state_msg = ("All chunks downloaded." if JOB["failed"] == 0 else
                     f"Finished with {JOB['failed']} failed chunks (see errors.log in the save folder). "
                     "Click Resume download to retry them.")
        job_update(state="finished", symbol="", chunk="", eta="", message=state_msg)
    except StopJob:
        job_update(state="stopped", message="Stopped. Click Resume download to continue.")
    except AuthStop as e:
        log_error(data_root, run, JOB["symbol"], JOB["chunk"], str(e))
        job_update(state="stopped", message=str(e))
    except Exception as e:
        log_error(data_root, run, JOB["symbol"], JOB["chunk"], f"{type(e).__name__}: {e}")
        job_update(state="stopped", message=f"Stopped: {type(e).__name__}: {e}")
    finally:
        db.close()
        with JOB_LOCK:
            print(f"[{run}] {JOB['state']}: requests {JOB['requests']}, 429s {JOB['n429']}, "
                  f"network errors {JOB['net_errors']}, failed chunks {JOB['failed']}, "
                  f"empty chunks {JOB['empty']}. {JOB['message']}", flush=True)

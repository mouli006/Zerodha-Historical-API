import os
import time
from datetime import datetime

from kiteconnect import KiteConnect

from .fetch import AuthStop, ChunkFailed, RateLimiter, StopJob, fetch_chunk, make_chunks
from .instruments import MAX_STOCKS, is_equity, is_etf, is_index, series_suffix
from .job import JOB, JOB_LOCK, STOP, job_incr, job_update
from .storage import job_run_key, log_error, open_db, run_dir, run_key, save_candles


def fmt_eta(seconds):
    seconds = int(seconds)
    return f"{seconds // 3600}h {seconds % 3600 // 60:02d}m"


def select_equity(all_nse, etfs, t2t):
    """NSE equity stocks after the series / ETF filter, plus the text shown on the page."""
    eq = [i for i in all_nse if i["segment"] == "NSE" and i["instrument_type"] == "EQ"]
    series_ok = [i for i in eq if is_equity(i, t2t)]
    instruments = [i for i in series_ok if etfs or not is_etf(i)]
    n_t2t = sum(1 for i in instruments if series_suffix(i))
    info = (f"{len(instruments):,} stocks (normal {len(instruments) - n_t2t:,}"
            + (f" + -BE/-BZ {n_t2t:,}" if t2t else "") + ") | excluded "
            f"{len(eq) - len(series_ok):,} bonds/other series"
            + ("" if etfs else f", {len(series_ok) - len(instruments):,} ETFs"))
    return instruments, info


def select_indices(all_nse, indices):
    """The chosen NSE indices in the order given, plus the text shown on the page."""
    found = {i["tradingsymbol"]: i for i in all_nse if is_index(i, indices)}
    missing = [s for s in indices if s not in found]
    info = (f"{len(found)} indices ({', '.join(s for s in indices if s in found)})"
            + (f", not found: {', '.join(missing)}" if missing else ""))
    return [found[s] for s in indices if s in found], info


def bulk_download(api_key, access_token, interval, start, end, speed, etfs, t2t, indices, equity,
                  data_root, fmt):
    """One job: the selected indices first (a few requests each), then equity stocks.
    Each part has its own run key, so their progress is tracked separately."""
    label = job_run_key(interval, start, end, fmt, indices, equity)
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    limiter = RateLimiter(speed)
    db = open_db(data_root)
    run = label   # the part's run key once a part starts; used in errors.log
    try:
        job_update(message="Loading instrument list...")
        all_nse = kite.instruments("NSE")
        parts, info = [], []   # parts: (name, run key, instruments, output folder)
        if indices:
            idx, idx_info = select_indices(all_nse, indices)
            irun = run_key(interval, start, end, fmt, index=True)
            parts.append(("Indices", irun, idx,
                          run_dir(db, data_root, irun, "index", interval, start, end)))
            info.append(idx_info)
        if equity:
            stocks, stock_info = select_equity(all_nse, etfs, t2t)
            erun = run_key(interval, start, end, fmt)
            parts.append(("Equity", erun, sorted(stocks, key=lambda i: i["tradingsymbol"]),
                          run_dir(db, data_root, erun, "equity", interval, start, end)))
            info.append(stock_info)
            job_update(equity_total=len(stocks))
        filter_info = " | ".join(info)
        job_update(filter_info=filter_info, out_dirs=[os.path.basename(p[3]) for p in parts])
        print(f"[{label}] Selected: {filter_info}", flush=True)
        if equity and len(stocks) > MAX_STOCKS:
            msg = (f"Not started: {len(stocks):,} stocks selected (limit {MAX_STOCKS:,}). "
                   "The symbol filter may be broken.")
            print(f"[{label}] {msg}", flush=True)
            job_update(state="stopped", message=msg)
            return

        chunks = make_chunks(start, end, interval)
        remaining = {}   # (run, symbol) -> chunks not DONE yet
        n_rows = n_done = n_failed = 0
        for _, prun, instruments, _ in parts:
            db.executemany(
                "INSERT OR IGNORE INTO chunks (run, symbol, token, chunk_from, chunk_to, status) "
                "VALUES (?, ?, ?, ?, ?, 'PENDING')",
                [(prun, i["tradingsymbol"], i["instrument_token"], str(a), str(b))
                 for i in instruments for a, b in chunks])
            db.commit()
            symbols = {i["tradingsymbol"] for i in instruments}
            for sym, status in db.execute("SELECT symbol, status FROM chunks WHERE run = ?", (prun,)):
                if sym not in symbols:
                    continue
                remaining.setdefault((prun, sym), 0)
                n_rows += 1
                if status == "DONE":
                    n_done += 1
                else:
                    remaining[(prun, sym)] += 1
                    n_failed += status == "FAILED"
        job_update(stocks_total=len(remaining),
                   stocks_done=sum(1 for v in remaining.values() if v == 0),
                   chunks_total=n_rows, chunks_done=n_done, failed=n_failed, message="")
        print(f"[{label}] {len(remaining)} instruments, {n_rows} chunks, {n_done} already done",
              flush=True)

        t0, processed = time.monotonic(), 0
        for name, run, instruments, out_dir in parts:
            order = {i["tradingsymbol"]: n for n, i in enumerate(instruments)}
            part_total = len(order)

            def part_text(sym):
                if name == "Indices":
                    return f"Indices: {sym}"
                done = sum(1 for (r, _), v in remaining.items() if r == run and v == 0)
                return f"Equity: {done:,} / {part_total:,} stocks"

            pass_no = 0
            while True:
                pass_no += 1
                todo = sorted((r for r in db.execute(
                    "SELECT symbol, token, chunk_from, chunk_to, status FROM chunks "
                    "WHERE run = ? AND status != 'DONE'", (run,)) if r[0] in order),
                    key=lambda r: (order[r[0]], r[2]))
                if not todo:
                    break
                job_update(**{"pass": pass_no})
                print(f"[{run}] pass {pass_no}: {len(todo)} chunks to fetch", flush=True)
                newly_done = 0

                for sym, token, cfrom, cto, prev_status in todo:
                    if STOP.is_set():
                        raise StopJob()
                    job_update(symbol=sym, chunk=cfrom[:10], part=part_text(sym))
                    status, error = "DONE", None
                    try:
                        candles = fetch_chunk(kite, limiter, token, datetime.fromisoformat(cfrom),
                                              datetime.fromisoformat(cto), interval)
                        if candles:
                            save_candles(out_dir, interval, sym, token, candles, fmt)
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
                        remaining[(run, sym)] -= 1
                        job_incr("chunks_done")
                        if remaining[(run, sym)] == 0:
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
                        print(f"[{run}] {part_text(sym)} | chunks "
                              f"{JOB['chunks_done']}/{JOB['chunks_total']} | ETA {JOB['eta']} | "
                              f"429s {JOB['n429']} | failed {JOB['failed']}", flush=True)

                if newly_done == 0:
                    break  # a full re-loop pass fixed nothing; stop looping

        state_msg = ("All chunks downloaded." if JOB["failed"] == 0 else
                     f"Finished with {JOB['failed']} failed chunks (see errors.log in the save folder). "
                     "Click Resume download to retry them.")
        job_update(state="finished", symbol="", chunk="", part="", eta="", message=state_msg)
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
            print(f"[{label}] {JOB['state']}: requests {JOB['requests']}, 429s {JOB['n429']}, "
                  f"network errors {JOB['net_errors']}, failed chunks {JOB['failed']}, "
                  f"empty chunks {JOB['empty']}. {JOB['message']}", flush=True)

# Dashboard and Kite sign-in

The Kite login flow, the bulk-download dashboard (planner, live card, folder dialog) and the in-memory
job state shared by every job. F&O and bhavcopy UI details are in fno_download.md and fno_bhavcopy_check.md.

## Kite sign-in
- Login flow: `GET /` shows the sign-in page (`login.html`) for the API key and secret. `POST /login` saves them to `.env`, then redirects to `KiteConnect.login_url()`. Zerodha redirects back to `GET /callback?request_token=...&status=success`, and `generate_session()` exchanges the token for an `access_token`. That token and the user details go into the Flask session. Then `/success` shows the dashboard (`success.html`), and `/logout` calls `invalidate_access_token()` and clears the session. Errors come back as `?error=` and are shown on the page (inline on sign-in, as a toast on the dashboard).
- The Redirect URL registered in the Kite developer console must be exactly `http://127.0.0.1:5000/callback`.
- On the login form, a blank secret with an unchanged API key keeps the saved secret.

## Dashboard
- `POST /browse-folder` opens a native folder dialog by running tkinter in a subprocess (`BROWSE_SCRIPT`), which keeps Tk off Flask threads. It works because the server and the browser are on the same machine.
- Dashboard (`success.html`): a planner card (Stocks checkbox list with nested trade-to-trade / ETF switches, interval switch, date presets incl. "Since 2015", speed slider, folder with Browse, CSV / Parquet switch) whose JS mirrors the server checks and `make_chunks()` / `job_run_key()` for a live estimate and window ruler; a live card (progress ring, current part, run folders under "Saving to", throughput sparkline computed from `requests` deltas between polls, stat tiles, run details, Stop); and a scrollable list of every stock and index completed in the current job. It polls `/download/status` every 2 s. `JOB["completed"]` is sent incrementally by `/download/status?since=<n>` as `completed_new` (plus `completed_count`) and reset per job via `job_no`. Start / Stop / Browse use `fetch`; start reads `?error=` from the redirect URL and shows toasts. The start button reads "Resume download" when the form matches a partly done run in `JOB`. The tab title shows the percentage while a job runs.
- Job state lives in memory (`JOB`), so any server restart stops the job. Resume is safe. `job_begin(**fields)` in `job.py` starts every job: it refuses if one is running, resets `COUNTERS`, sets `state="running"` and bumps `job_no`. `JOB["kind"]` is `"backfill"` or `"fno"`. An F&O job leaves the planner's keys (`from`, `to`, `interval`, `format`, `equity`, `indices`) alone, so the planner form keeps its values.

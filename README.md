# Zerodha Historical API

A local Flask app that signs in to Zerodha Kite Connect and downloads historical candles (1-minute or daily) for every NSE equity stock, about 2,200 symbols, up to one year per run. Files are saved as CSV or Parquet, downloads can be stopped and resumed, and the dashboard shows live progress.

![Dashboard with a download in progress](docs/screenshots/dashboard.png)

## Requirements
- Python 3.10 or newer
- A Kite Connect app with the paid **Historical data** add-on. Without it, Kite rejects the requests and the error appears in the dashboard.
- An internet connection (the dashboard loads React and fonts from public CDNs)

## Setup
1. Install the dependencies and start the app:
   ```
   pip install -r requirements.txt
   python app.py
   ```
2. In the Kite developer console, set your app's redirect URL to exactly:
   ```
   http://127.0.0.1:5000/callback
   ```
3. Open http://127.0.0.1:5000, enter your API key and secret, and sign in with Zerodha.

   ![Sign-in page](docs/screenshots/sign-in.png)

Kite sessions expire every day, so you need to sign in again each day.

## Downloading data
1. Choose the candle size: **1 minute** or **Daily (EOD)**.
2. Pick a date range of up to one year, or use a shortcut (last 30 days, 3 months, 6 months or 1 year).
3. Set the speed, from 0.5 to 3 requests a second (Kite's limit is 3). The app slows down automatically if Kite rate-limits it.
4. Choose whether to include trade-to-trade stocks (`-BE`, `-BZ`) and ETFs.
5. Choose a save folder and a file format: **CSV** (opens in Excel) or **Parquet** (smaller, for pandas and Python).
6. Click **Start download**.

Before you start, the dashboard estimates the number of requests and the time the download will take. Kite returns at most 60 days of 1-minute candles per request, so a year of 1-minute data takes 7 requests per stock, about 15,000 in total, or roughly 1 h 45 min at full speed. Daily data takes one request per stock.

### Live progress
While a download runs, the dashboard shows the percentage done, the stock being fetched, the time left, the request speed, rate-limit hits and errors, and a list of the stocks completed so far. The browser tab title also shows the percentage.

<img src="docs/screenshots/mobile.png" alt="Live progress on a narrow screen" width="320">

### Stopping and resuming
Progress is saved after every request. Click **Stop download** at any time. To continue, start again with the same dates, file format and save folder, and click **Resume download**. Finished chunks are skipped. Requests that fail are retried automatically at the end of the run.

## Output
Files are saved as:
```
<folder>/<interval>/<SYMBOL>/<year>.csv      (or .parquet)
```
For example, `D:\market_data\minute\RELIANCE\2026.csv`. Each file has the columns `date, open, high, low, close, volume, tradingsymbol, instrument_token`. Times are in IST. A run's progress (`progress.sqlite`) and error log (`errors.log`) are kept in the same folder.

## Which stocks are included
Every NSE equity (`EQ`) instrument with no series suffix, plus `-BE` and `-BZ` trade-to-trade stocks if you include them. Bonds, NCDs, government securities, T-bills, sovereign gold bonds and SME stocks are excluded, and ETFs are excluded unless you include them. The dashboard shows the exact counts once the download starts.

## Credentials
Your API key, secret and access token are stored only in a local `.env` file, which the app creates when you sign in. It is gitignored and never committed. `.env.example` shows the format.

Screenshots show demo data, not a real account.

## Development notes
Architecture and implementation notes are in [CLAUDE.md](CLAUDE.md).

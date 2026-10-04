# Zerodha Historical API

A local Flask app that signs in to Zerodha Kite Connect and downloads historical candles (1-minute or daily) for every NSE equity stock, about 2,200 symbols, up to one year per run. Downloads are resumable, and the dashboard shows live progress and lets you browse and export the saved data as CSV or Parquet.

## Requirements
- Python 3.10+
- A Kite Connect app with the paid **Historical data** add-on

## Setup
```
pip install -r requirements.txt
python app.py
```
Open http://127.0.0.1:5000, enter your API key and secret, and sign in with Zerodha.

In the Kite developer console, set your app's redirect URL to exactly:
```
http://127.0.0.1:5000/callback
```

## Using it
1. Choose 1-minute or daily candles, a date range (one year at most), the request speed, and a save folder.
2. Click **Start download**. Progress is saved after every chunk, so you can stop at any time and click **Resume download** later with the same dates and folder.
3. In **Your data**, preview any stock and download it as CSV or Parquet, or download many stocks as a ZIP.

Files are saved as `<folder>/<interval>/<SYMBOL>/<year>.<csv|parquet>`.

Kite sessions expire every day, so you need to sign in again each day.

## Credentials
Your API key, secret and access token are stored only in a local `.env` file, which is gitignored and never committed. `.env.example` shows the format.

Architecture notes are in [CLAUDE.md](CLAUDE.md).

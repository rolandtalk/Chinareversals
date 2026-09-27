# Chinareversals

China and Hong Kong MA3 reversal scanner, adapted from [reversal_list_CursorAI](https://github.com/rolandtalk/reversal_list_CursorAI).

**Live app:** [chinareversals-production.up.railway.app](https://chinareversals-production.up.railway.app)

The app tracks 611 selections—473 mainland and 138 Hong Kong listings. This includes 306 names across the nine `鉅亨網 AI 股市贏家` categories captured on 2026-09-26, 14 user-selected innovative-drug and supply-chain companies, 48 additional A-share names from the world-leading and PCB screenshots, and 243 unique additions from the `A股申万一级行业前十大市值公司及60日涨跌幅` CSV added on 2026-09-27. Existing selections were preserved instead of duplicated. It preserves the source market, original names and stock codes, maps each listing to its market ticker, and calculates:

- latest close and RSI(14)
- days since the latest below-MA3 reversal low (DG)
- gain from that reversal low (GG)
- 1-, 3-, 5-, 20-, and 60-session returns
- interactive 3-month Close/MA3 charts
- exchange-aware TradingView company-profile links that open in the one-month view

The web app is PostgreSQL-first: page loads read the latest completed snapshot instead of waiting for Yahoo Finance. A separate Railway Cron service refreshes the universe and prices at **16:15 Asia/Taipei/Hong Kong (08:15 UTC), Monday–Friday**, after the mainland and Hong Kong cash markets close. The database contains four tables:

- `stocks` — the active stock universe and source metadata
- `scan_runs` — one record for every completed Yahoo Finance scan
- `scan_results` — the per-stock price and reversal metrics for each run
- `daily_prices` — Close and MA3 chart points cached by ticker and trading date

`GET /api/database-status` reports row counts and the latest stored run. `GET /api/history/<ticker>` returns persisted scan history for a symbol.

## Railway services

Both services use this repository and the same PostgreSQL `DATABASE_URL`:

| Service | Start command | Schedule |
| --- | --- | --- |
| Web | `gunicorn app:app --bind 0.0.0.0:$PORT --workers 1 --threads 8 --timeout 120` | always on |
| After-close cron | `python cron_scan.py` | `15 8 * * 1-5` (UTC) |

The cron process synchronizes the public Google Sheet (falling back to the bundled snapshot), downloads six months of daily prices, writes one completed scan plus chart history, and exits. Railway skips a cron execution if the prior one is still running; the entrypoint also uses a PostgreSQL advisory lock.

To run the same refresh manually:

```bash
.venv/bin/python cron_scan.py
```

## Run locally

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python app.py
```

Open <http://127.0.0.1:5000>.

## Configuration

| Variable | Purpose | Default |
| --- | --- | --- |
| `APP_BASE_URL` | Canonical production URL | local URL |
| `GITHUB_REPO_URL` | Header GitHub link | this repository |
| `GOOGLE_SHEET_ID` | Source stock-list Sheet | current CNYES capture |
| `SOURCE_SHEET_NAME` | Source tab name | `股票清單` |
| `BATCH_SIZE` | YFinance symbols per request | `45` |
| `DATABASE_URL` | PostgreSQL connection URL; Railway injects this | local SQLite file |
| `DB_POOL_SIZE` | Web PostgreSQL connection pool size | `5` |
| `DB_MAX_OVERFLOW` | Temporary connections above the pool | `5` |
| `DB_POOL_TIMEOUT` | Seconds to wait for a connection | `10` |

The cron refreshes the universe from the public Google Sheet. If it is unavailable, the scan uses the committed 611-row snapshot in `data/stock_universe.json`.

## Data note

Yahoo Finance data can be delayed, unavailable, or rate-limited. This application is for research and is not investment advice.

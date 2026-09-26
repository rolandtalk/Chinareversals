# Chinareversals

China and Hong Kong MA3 reversal scanner, adapted from [reversal_list_CursorAI](https://github.com/rolandtalk/reversal_list_CursorAI).

**Live app:** [chinareversals-production.up.railway.app](https://chinareversals-production.up.railway.app)

The app tracks 306 selections—170 Shanghai/Shenzhen and 136 Hong Kong listings—in the nine `鉅亨網 AI 股市贏家` categories captured on 2026-09-26. It preserves the source market, original names and stock codes, maps each listing to its Yahoo Finance ticker, and calculates:

- latest close and RSI(14)
- days since the latest below-MA3 reversal low (DG)
- gain from that reversal low (GG)
- 1-, 3-, 5-, and 20-session returns
- interactive 3-month Close/MA3 charts
- exchange-aware TradingView company-profile links that open in the one-month view

Each fresh scan is persisted to PostgreSQL on Railway. The database contains three tables:

- `stocks` — the active stock universe and source metadata
- `scan_runs` — one record for every completed Yahoo Finance scan
- `scan_results` — the per-stock price and reversal metrics for each run

`GET /api/database-status` reports row counts and the latest stored run. `GET /api/history/<ticker>` returns persisted scan history for a symbol.

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
| `CACHE_TTL` | Price-scan cache in seconds | `900` |
| `BATCH_SIZE` | YFinance symbols per request | `45` |
| `DATABASE_URL` | PostgreSQL connection URL; Railway injects this | local SQLite file |

If the Google Sheet is publicly readable, the app refreshes the universe from it. Otherwise it uses the committed 306-row snapshot in `data/stock_universe.json`.

## Data note

Yahoo Finance data can be delayed, unavailable, or rate-limited. This application is for research and is not investment advice.

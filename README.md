# Chinareversals

China A-share MA3 reversal scanner, adapted from [reversal_list_CursorAI](https://github.com/rolandtalk/reversal_list_CursorAI).

The app tracks the 170 Shanghai/Shenzhen selections in the nine `鉅亨網 AI 股市贏家` categories captured on 2026-09-26. It preserves the original Chinese names and stock codes, maps each listing to its Yahoo Finance ticker, and calculates:

- latest close and RSI(14)
- days since the latest below-MA3 reversal low (DG)
- gain from that reversal low (GG)
- 1-, 3-, 5-, and 20-session returns
- interactive 3-month Close/MA3 charts

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

If the Google Sheet is publicly readable, the app refreshes the universe from it. Otherwise it uses the committed 170-row snapshot in `data/china_stocks.json`.

## Data note

Yahoo Finance data can be delayed, unavailable, or rate-limited. This application is for research and is not investment advice.

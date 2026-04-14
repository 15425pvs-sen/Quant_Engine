# StratEdge Quant Application

End-to-end RSI-based quant workflow for Indian equities using `yfinance`, SQLite, heatmap strategy generation, live signal detection, FastAPI access, and a Kotlin Android client.

## Overview

This project supports the following flow:

1. Download historical stock data for Indian equities
2. Ingest the downloaded CSV data into SQLite
3. Compute and fill RSI values in each equity table
4. Build RSI heatmap strategy rows from historical data
5. Fetch live stock prices and generate BUY/SELL signals
6. Expose signal generation and signal retrieval through FastAPI
7. Review and act on pending signals in the `StratEdge` Android app

Main database:
- `quant_historic_data.db`

Historical CSV folder:
- `historic_data/`

## Database Tables

### Equity Tables

Each stock has its own table, for example:
- `TCS`
- `RELIANCE`
- `INFY`

Schema:
- `trade_date`
- `open`
- `high`
- `low`
- `close`
- `adj_close`
- `volume`
- `rsi`

### Heatmap Table

Table name:
- `rsi_heatmap_data`

Purpose:
- Stores entry/exit RSI strategy rows generated from historical data

Columns:
- `source_table`
- `entry_rsi`
- `exit_rsi`
- `avg_return_pct`
- `trades`
- `win_rate_pct`
- `min_return_pct`
- `max_return_pct`
- `first_trade_date`
- `last_trade_date`
- `generated_at`

### Live Signal Log Table

Table name:
- `rsi_live_signal_log`

Purpose:
- Stores BUY/SELL live signals generated from heatmap buckets

Columns:
- `id`
- `source_table`
- `signal_type`
- `entry_rsi`
- `exit_rsi`
- `previous_rsi`
- `current_rsi`
- `ltp`
- `signal_date`
- `signal_timestamp`
- `notes`

### Pipeline Log Tables

Tables:
- `quant_engine_runs`
- `quant_engine_steps`

Purpose:
- Store run-level and step-level status for the production pipeline

## Scripts

### 1. `download_indian_stock_history.py`

Purpose:
- Downloads historical OHLCV data for Indian stocks using `yfinance`
- Stores one CSV per stock in `historic_data/`
- If the equity table already exists in SQLite, it downloads only missing dates

Usage:

```powershell
py download_indian_stock_history.py
py download_indian_stock_history.py TCS RELIANCE INFY
```

### 2. `ingest_yfinance_to_sqlite.py`

Purpose:
- Reads CSV files from `historic_data/`
- Creates one SQLite equity table per stock if needed
- Appends only rows newer than the latest stored `trade_date`
- Initializes `rsi` as `NULL`

Notes:
- `open`, `high`, and `close` are rounded to 2 decimal places
- Date parsing is handled explicitly to avoid month/day confusion

Usage:

```powershell
py ingest_yfinance_to_sqlite.py
py ingest_yfinance_to_sqlite.py TCS RELIANCE
```

### 3. `calculate_rsi_fill_db_frozen.py`

Purpose:
- Computes RSI(14) using standard Wilder smoothing
- Updates only rows where `rsi IS NULL`
- Leaves existing RSI values unchanged

Usage:

```powershell
py calculate_rsi_fill_db_frozen.py
py calculate_rsi_fill_db_frozen.py RELIANCE
py calculate_rsi_fill_db_frozen.py TCS INFY RELIANCE
```

### 4. `fill_rsi_backward_claude_frozen.py`

Purpose:
- Direct one-table RSI backfill script
- Useful for individual-table manual operations

Usage:

```powershell
py fill_rsi_backward_claude_frozen.py RELIANCE
```

### 5. `build_rsi_heatmap_table.py`

Purpose:
- Builds RSI strategy heatmap rows for a selected equity table
- Stores rows in `rsi_heatmap_data`

Behavior:
- Uses `MIN_TRADES = 3`
- Uses `MIN_AVG_RETURN_PCT = 3.0`
- Uses stop loss filtering when `USE_STOP_LOSS = True`
- Current stop-loss threshold is `STOP_LOSS_PCT = -0.12`
- Reports the correct reason if zero rows are produced
- Distinguishes between:
  - not enough trades
  - average return threshold not met

Usage:

```powershell
py build_rsi_heatmap_table.py RELIANCE
py build_rsi_heatmap_table.py RELIANCE --force
```

### 6. `quant_engine.py`

Purpose:
- Production-style orchestrator for the whole pipeline

Pipeline order:
1. Download stock history
2. Ingest CSV data into SQLite
3. Fill RSI only for missing rows
4. Build heatmap rows only for missing or changed equity tables
5. Run `rsi_live_signal_engine.py --check-last-rsi` as the final signal-generation step

Features:
- DB preflight check
- Run logging into SQLite
- Step logging into SQLite
- Symbol-scoped execution
- Selective heatmap rebuilds
- Final logged signal step: `signals:last_rsi`

Usage:

```powershell
py quant_engine.py
py quant_engine.py RELIANCE TCS
py quant_engine.py RELIANCE --force-heatmap
```

### 7. `repair_reliance.py`

Purpose:
- Repairs only the `RELIANCE` dataset without taking a backup

What it does:
1. Drops the `RELIANCE` table
2. Deletes `RELIANCE` rows from `rsi_heatmap_data`
3. Deletes `RELIANCE` rows from `rsi_live_signal_log` if present
4. Deletes `historic_data/RELIANCE.csv`
5. Re-downloads fresh RELIANCE data
6. Re-ingests it into SQLite
7. Refills RSI
8. Rebuilds heatmap

Usage:

```powershell
py repair_reliance.py
```

### 8. `rsi_live_signal_engine.py`

Purpose:
- Reads all stocks present in `rsi_heatmap_data`
- Fetches live LTP using `yfinance`
- Uses the last stored RSI in the equity table as the previous RSI reference
- Reads only the last `RSI_PERIOD + RSI_BUFFER_ROWS` rows from the stock table
- Calculates live RSI for the current stock price
- Generates BUY/SELL signals based on heatmap entry/exit buckets
- Suppresses duplicate signals for the same `(stock, entry_rsi, exit_rsi)` bucket
- Logs signals into `rsi_live_signal_log`

Selection behavior:
- If multiple heatmap rows share the same `entry_rsi`, BUY evaluation now prefers the highest `avg_return_pct` bucket first
- Ties are further ranked by higher `trades`, then lower `exit_rsi`

Modes:
- Default mode computes a live RSI using the latest `yfinance` price
- `--check-last-rsi` mode reads the last two stored RSI rows from each equity table in `rsi_heatmap_data`
- In `--check-last-rsi` mode, the script checks for BUY/SELL crossover hits using:
  - previous RSI = second-last stored RSI
  - current RSI = last stored RSI
  - LTP = last stored close
- Running `py rsi_live_signal_engine.py --check-last-rsi` checks all equity tables represented in `rsi_heatmap_data`

Usage:

```powershell
py rsi_live_signal_engine.py
py rsi_live_signal_engine.py RELIANCE TCS
py rsi_live_signal_engine.py --dry-run
py rsi_live_signal_engine.py --check-last-rsi
py rsi_live_signal_engine.py RELIANCE INFY --check-last-rsi
py rsi_live_signal_engine.py --check-last-rsi --dry-run
```

### 9. `signal_api.py`

Purpose:
- FastAPI wrapper around the live signal engine for mobile/web clients
- Returns pending signals up to the current date for the mobile app

Endpoints:
- `GET /health`
- `POST /run-signals`
- `GET /run-signals/status`
- `GET /signals/today`
- `POST /test/signals/seed`

Security:
- `POST /run-signals` requires `X-API-Key`
- `GET /run-signals/status` requires `X-API-Key`
- `GET /signals/today` requires `X-API-Key`
- `POST /test/signals/seed` requires `X-API-Key`

## Environment Variables

### Required for API

- `QUANT_API_KEY`
- Optional: `QUANT_ALLOWED_ORIGINS`

Set for current PowerShell session:

```powershell
$env:QUANT_API_KEY="your-very-secret-key"
$env:QUANT_ALLOWED_ORIGINS="https://your-ngrok-domain.ngrok-free.dev"
```

Set permanently for the current user:

```powershell
[System.Environment]::SetEnvironmentVariable("QUANT_API_KEY", "your-very-secret-key", "User")
[System.Environment]::SetEnvironmentVariable("QUANT_ALLOWED_ORIGINS", "https://your-ngrok-domain.ngrok-free.dev", "User")
```

Check value:

```powershell
echo $env:QUANT_API_KEY
```

## Typical Data Pipeline Commands

Run the full pipeline:

```powershell
py quant_engine.py
```

Run the full pipeline for selected symbols:

```powershell
py quant_engine.py RELIANCE TCS
```

Force heatmap rebuild:

```powershell
py quant_engine.py RELIANCE --force-heatmap
```

Repair RELIANCE:

```powershell
py repair_reliance.py
```

Run live signal engine directly:

```powershell
py rsi_live_signal_engine.py
py rsi_live_signal_engine.py RELIANCE
py rsi_live_signal_engine.py --dry-run
py rsi_live_signal_engine.py --check-last-rsi
```

## FastAPI Setup

Install dependencies:

```powershell
pip install fastapi uvicorn
```

Start API:

```powershell
$env:QUANT_API_KEY="your-very-secret-key"
$env:QUANT_ALLOWED_ORIGINS="https://your-ngrok-domain.ngrok-free.dev"
uvicorn signal_api:app --host 0.0.0.0 --port 8000
```

## ngrok Setup

Expose local API publicly:

```powershell
ngrok http 8000
```

Use the generated HTTPS URL for mobile app or Postman calls.

Note:
- Free ngrok URLs may change when restarted

## API Endpoints

### `GET /health`

Example:

```text
https://your-ngrok-domain.ngrok-free.dev/health
```

Response:

```json
{
  "status": "ok"
}
```

### `POST /run-signals`

Headers:

```http
X-API-Key: your-very-secret-key
Content-Type: application/json
```

Request body:

```json
{
  "symbols": ["RELIANCE", "TCS"],
  "dry_run": false
}
```

Example URL:

```text
https://your-ngrok-domain.ngrok-free.dev/run-signals
```

### `GET /run-signals/status`

Headers:

```http
X-API-Key: your-very-secret-key
```

Example URL:

```text
https://your-ngrok-domain.ngrok-free.dev/run-signals/status
```

### `GET /signals/today`

Headers:

```http
X-API-Key: your-very-secret-key
```

Example URL:

```text
https://your-ngrok-domain.ngrok-free.dev/signals/today
```

Example response:

```json
{
  "date": "2026-04-12",
  "signals": [
    {
      "id": 42,
      "equity": "RELIANCE",
      "rsi_entry": 25,
      "rsi_exit": 75,
      "signal_type": "BUY",
      "current_rsi": 26.4,
      "ltp": 1450.25,
      "signal_timestamp": "2026-04-12T10:15:00"
    }
  ]
}
```

Behavior:
- Returns all signal rows where `signal_date <= today`
- Orders signals newest-first by `signal_timestamp DESC, id DESC`
- This allows the app to recover older missed BUY/SELL calls, not only the current day

### `POST /test/signals/seed`

Headers:

```http
X-API-Key: your-very-secret-key
Content-Type: application/json
```

Example request body:

```json
{
  "equity": "INFY",
  "rsi_entry": 25,
  "rsi_exit": 70,
  "signal_type": "BUY",
  "current_rsi": 26.4,
  "ltp": 1300.0,
  "signal_timestamp": "2026-04-12T09:15:00"
}
```

Example SELL seed body:

```json
{
  "equity": "RELIANCE",
  "rsi_entry": 25,
  "rsi_exit": 70,
  "signal_type": "SELL",
  "current_rsi": 71.2,
  "ltp": 1528.90,
  "signal_timestamp": "2026-04-10T15:45:00"
}
```
## Postman Test Examples

Base URL:

```text
https://your-ngrok-domain.ngrok-free.dev
```

### Health

```text
GET {{base_url}}/health
```

### Run Signals

```text
POST {{base_url}}/run-signals
```

Headers:
- `Content-Type: application/json`
- `X-API-Key: your-very-secret-key`

Body:

```json
{
  "symbols": ["RELIANCE", "TCS"],
  "dry_run": false
}
```

### Read Pending Signals

```text
GET {{base_url}}/signals/today
```

Header:
- `X-API-Key: your-very-secret-key`

### Signal Engine Status

```text
GET {{base_url}}/run-signals/status
```

Header:
- `X-API-Key: your-very-secret-key`

### Seed Test Signal

```text
POST {{base_url}}/test/signals/seed
```

Headers:
- `Content-Type: application/json`
- `X-API-Key: your-very-secret-key`

## Important Notes

- `rsi_live_signal_engine.py` does not modify the main equity tables
- Live signals are logged only in `rsi_live_signal_log`
- Duplicate BUY/SELL calls are suppressed per `(stock, entry_rsi, exit_rsi)` bucket
- `/signals/today` is now a pending-signal feed for the app, despite the legacy endpoint name
- The signal feed is returned in descending timestamp order
- RELIANCE previously had date-format corruption; use `repair_reliance.py` if required
- ngrok gives HTTPS access, while `X-API-Key` protects the business endpoints

## Android App

Project:
- `android-app/`

Current app identity:
- App name: `StratEdge`
- Dashboard title: `StratEdge Portfolio`

Current behavior:
- `Run Signal Engine` calls `POST /run-signals`
- The app polls `GET /run-signals/status`
- This will run the script rsi_live_signal_engine.py on all the equity &    try to get a Buy/Sell calls. If a Buy/Sell call is generated the same will be listed in the application.
- `Refresh Signals` calls `GET /signals/today`
- The app shows missed BUY/SELL calls from previous dates up to today
- Signal cards are shown newest-first
- BUY signals require explicit investment confirmation
- SELL signals are optional and show `SELL` plus `Dismiss`
- Dismissing a SELL keeps the BUY position carried forward
- The main screen is tab-based:
  - `Current Portfolio`
  - `Investments`
- The `Investments` tab shows equity-wise invested capital, open capital, units, allocation, and realized P&L
- Local portfolio state is stored on-device with Room

Clearing local Android test data:
- The visible reset button has been removed from the UI
- To clear local Room data on a device or emulator:
  - Android Settings > Apps > StratEdge > Storage & cache > Clear storage

#!/usr/bin/env python
"""
FastAPI service for running the live RSI signal engine and reading pending signals up to today.

Run with:
    uvicorn signal_api:app --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import threading
from datetime import date, datetime
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import APIKeyHeader
from pydantic import BaseModel


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "quant_historic_data.db"
SIGNAL_ENGINE_PATH = BASE_DIR / "rsi_live_signal_engine.py"
SIGNAL_LOG_TABLE = "rsi_live_signal_log"
BUY_OPEN_STATE = "OPEN"
BUY_CLOSED_STATE = "CLOSED"
SELL_CONFIRMED_STATE = "CONFIRMED"
WATCHLIST_ENGINE_PATH = BASE_DIR / "quant_engine.py"
API_KEY_HEADER_NAME = "X-API-Key"
EXPECTED_API_KEY = os.getenv("QUANT_API_KEY", "").strip()
ALLOWED_ORIGINS = [
    origin.strip()
    for origin in os.getenv("QUANT_ALLOWED_ORIGINS", "*").split(",")
    if origin.strip()
]

app = FastAPI(title="Quant Signal API", version="1.0.0")
api_key_header = APIKeyHeader(name=API_KEY_HEADER_NAME, auto_error=False)
run_signals_lock = threading.Lock()
run_signals_state: dict[str, object] = {
    "status": "idle",
    "symbols": [],
    "dry_run": False,
    "started_at": None,
    "finished_at": None,
    "output": "",
    "error": None,
}

watchlist_lock = threading.Lock()

watchlist_state: dict[str, object] = {
    "status": "idle",
    "symbols": [],
    "run_all": False,
    "started_at": None,
    "finished_at": None,
    "output": "",
    "error": None,
}
SYSTEM_TABLES = {
    "stocks_rsi_cagrs",
    "market_data",
    "sqlite_sequence",
    "sqlite_stat1",
    "sqlite_stat2",
    "sqlite_stat3",
    "sqlite_stat4",
    SIGNAL_LOG_TABLE,
    "rsi_heatmap_data",
    "quant_engine_runs",
    "quant_engine_steps",
}
EQUITY_REQUIRED_COLS = {"trade_date", "open", "high", "low", "close", "adj_close", "volume"}

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS if ALLOWED_ORIGINS else ["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


class RunSignalsRequest(BaseModel):
    symbols: list[str] = []
    dry_run: bool = False


class SeedSignalRequest(BaseModel):
    equity: str
    rsi_entry: int
    rsi_exit: int
    signal_type: str
    current_rsi: float
    ltp: float
    signal_timestamp: str | None = None
    buy_signal_id: int | None = None
    trigger_exit_rsi: int | None = None


class WatchlistRequest(BaseModel):
    symbols: str  # space-separated string


def quote_identifier(name: str) -> str:
    if not isinstance(name, str) or not name.strip():
        raise ValueError("Identifier must be a non-empty string.")
    if "\x00" in name:
        raise ValueError("Identifier contains an invalid null byte.")
    return '"' + name.replace('"', '""') + '"'


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name = ?
        """,
        (table_name,),
    ).fetchone()
    return row is not None


def get_equity_tables(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
    ).fetchall()

    equity_tables: list[str] = []
    for (name,) in rows:
        if name in SYSTEM_TABLES:
            continue

        cols = {
            row[1].lower()
            for row in conn.execute(f"PRAGMA table_info({quote_identifier(name)})")
        }
        if EQUITY_REQUIRED_COLS.issubset(cols):
            equity_tables.append(name)

    return equity_tables


def ensure_signal_log_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {quote_identifier(SIGNAL_LOG_TABLE)} (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            source_table      TEXT NOT NULL,
            signal_type       TEXT NOT NULL,
            entry_rsi         INTEGER NOT NULL,
            exit_rsi          INTEGER NOT NULL,
            previous_rsi      REAL NOT NULL,
            current_rsi       REAL NOT NULL,
            ltp               REAL NOT NULL,
            signal_date       TEXT NOT NULL,
            signal_timestamp  TEXT NOT NULL,
            notes             TEXT,
            position_state    TEXT NOT NULL DEFAULT 'OPEN',
            buy_signal_id     INTEGER,
            trigger_exit_rsi  INTEGER,
            action_timestamp  TEXT,
            closed_by_signal_id INTEGER
        )
        """
    )
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_{SIGNAL_LOG_TABLE}_lookup
        ON {quote_identifier(SIGNAL_LOG_TABLE)} (source_table, entry_rsi, exit_rsi, signal_timestamp)
        """
    )
    existing_cols = {
        row[1].lower()
        for row in conn.execute(f"PRAGMA table_info({quote_identifier(SIGNAL_LOG_TABLE)})")
    }
    if "position_state" not in existing_cols:
        conn.execute(
            f"ALTER TABLE {quote_identifier(SIGNAL_LOG_TABLE)} "
            "ADD COLUMN position_state TEXT NOT NULL DEFAULT 'OPEN'"
        )
    if "buy_signal_id" not in existing_cols:
        conn.execute(
            f"ALTER TABLE {quote_identifier(SIGNAL_LOG_TABLE)} "
            "ADD COLUMN buy_signal_id INTEGER"
        )
    if "trigger_exit_rsi" not in existing_cols:
        conn.execute(
            f"ALTER TABLE {quote_identifier(SIGNAL_LOG_TABLE)} "
            "ADD COLUMN trigger_exit_rsi INTEGER"
        )
    if "action_timestamp" not in existing_cols:
        conn.execute(
            f"ALTER TABLE {quote_identifier(SIGNAL_LOG_TABLE)} "
            "ADD COLUMN action_timestamp TEXT"
        )
    if "closed_by_signal_id" not in existing_cols:
        conn.execute(
            f"ALTER TABLE {quote_identifier(SIGNAL_LOG_TABLE)} "
            "ADD COLUMN closed_by_signal_id INTEGER"
        )
    conn.commit()


def validate_api_key(provided_api_key: str | None) -> None:
    if not EXPECTED_API_KEY:
        raise HTTPException(
            status_code=500,
            detail="Server API key is not configured. Set QUANT_API_KEY before starting the API.",
        )

    if provided_api_key != EXPECTED_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API key.")


def update_run_state(**kwargs: object) -> None:
    with run_signals_lock:
        run_signals_state.update(kwargs)


def snapshot_run_state() -> dict[str, object]:
    with run_signals_lock:
        return dict(run_signals_state)

def update_watchlist_state(**kwargs: object) -> None:
    with watchlist_lock:
        watchlist_state.update(kwargs)


def snapshot_watchlist_state() -> dict[str, object]:
    with watchlist_lock:
        return dict(watchlist_state)


def get_latest_quant_engine_run(conn: sqlite3.Connection) -> dict[str, object] | None:
    if not table_exists(conn, "quant_engine_runs"):
        return None

    row = conn.execute(
        """
        SELECT run_id, started_at, finished_at, status, symbols, force_heatmap, error_message
        FROM quant_engine_runs
        ORDER BY run_id DESC
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        return None

    return {
        "run_id": row[0],
        "started_at": row[1],
        "finished_at": row[2],
        "status": row[3],
        "symbols": row[4].split(",") if row[4] else [],
        "force_heatmap": bool(row[5]),
        "error_message": row[6],
    }


def run_signal_engine_job(symbols: list[str], dry_run: bool) -> None:
    cmd = [sys.executable, str(SIGNAL_ENGINE_PATH)]
    if symbols:
        cmd.extend(symbols)
    if dry_run:
        cmd.append("--dry-run")

    update_run_state(
        status="running",
        symbols=symbols,
        dry_run=dry_run,
        started_at=datetime.now().isoformat(timespec="seconds"),
        finished_at=None,
        output="",
        error=None,
    )

    try:
        result = subprocess.run(
            cmd,
            cwd=str(BASE_DIR),
            check=True,
            capture_output=True,
            text=True,
        )
        output = "\n".join(part for part in [result.stdout, result.stderr] if part).strip()
        update_run_state(
            status="completed",
            finished_at=datetime.now().isoformat(timespec="seconds"),
            output=output,
            error=None,
        )
    except subprocess.CalledProcessError as exc:
        detail = "\n".join(part for part in [exc.stdout, exc.stderr] if part).strip()
        update_run_state(
            status="failed",
            finished_at=datetime.now().isoformat(timespec="seconds"),
            output="",
            error=detail or "Signal engine execution failed.",
        )

def run_watchlist_job(symbols: list[str], run_all: bool = False) -> None:
    cmd = [sys.executable, str(WATCHLIST_ENGINE_PATH)]
    if not run_all:
        cmd.extend(symbols)

    update_watchlist_state(
        status="running",
        symbols=symbols,
        run_all=run_all,
        started_at=datetime.now().isoformat(timespec="seconds"),
        finished_at=None,
        output="",
        error=None,
    )

    try:
        result = subprocess.run(
            cmd,
            cwd=str(BASE_DIR),
            check=True,
            capture_output=True,
            text=True,
        )

        output = "\n".join(
            part for part in [result.stdout, result.stderr] if part
        ).strip()

        update_watchlist_state(
            status="completed",
            run_all=run_all,
            finished_at=datetime.now().isoformat(timespec="seconds"),
            output=output,
            error=None,
        )

    except subprocess.CalledProcessError as exc:
        error = "\n".join(
            part for part in [exc.stdout, exc.stderr] if part
        ).strip()

        update_watchlist_state(
            status="failed",
            run_all=run_all,
            finished_at=datetime.now().isoformat(timespec="seconds"),
            output="",
            error=error or "Execution failed",
        )


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/run-signals")
def run_signals(
    request: RunSignalsRequest,
    x_api_key: str | None = Depends(api_key_header),
) -> dict[str, object]:
    validate_api_key(x_api_key)

    symbols = [symbol.strip().upper() for symbol in request.symbols if symbol.strip()]
    current_state = snapshot_run_state()
    if current_state["status"] == "running":
        return {
            "status": "already_running",
            "symbols": current_state["symbols"],
            "dry_run": current_state["dry_run"],
            "output": "",
        }

    worker = threading.Thread(
        target=run_signal_engine_job,
        args=(symbols, request.dry_run),
        daemon=True,
    )
    worker.start()
    return {
        "status": "started",
        "symbols": symbols,
        "dry_run": request.dry_run,
        "output": "",
    }


@app.get("/run-signals/status")
def get_run_signals_status(
    x_api_key: str | None = Depends(api_key_header),
) -> dict[str, object]:
    validate_api_key(x_api_key)
    return snapshot_run_state()


@app.get("/signals/today")
def get_today_signals(
    x_api_key: str | None = Depends(api_key_header),
) -> dict[str, object]:
    validate_api_key(x_api_key)

    today = date.today().isoformat()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    try:
        ensure_signal_log_table(conn)
        if not table_exists(conn, SIGNAL_LOG_TABLE):
            return {"date": today, "signals": []}

        rows = conn.execute(
            f"""
            SELECT
                id,
                source_table,
                entry_rsi,
                exit_rsi,
                signal_type,
                current_rsi,
                ltp,
                signal_timestamp,
                position_state,
                buy_signal_id,
                trigger_exit_rsi
            FROM {quote_identifier(SIGNAL_LOG_TABLE)}
            WHERE signal_date <= ?
            ORDER BY signal_timestamp DESC, id DESC
            """,
            (today,),
        ).fetchall()
    finally:
        conn.close()

    signals = [
        {
            "id": row["id"],
            "equity": row["source_table"],
            "rsi_entry": row["entry_rsi"],
            "rsi_exit": row["exit_rsi"],
            "signal_type": row["signal_type"],
            "current_rsi": row["current_rsi"],
            "ltp": row["ltp"],
            "signal_timestamp": row["signal_timestamp"],
            "position_state": row["position_state"],
            "buy_signal_id": row["buy_signal_id"],
            "trigger_exit_rsi": row["trigger_exit_rsi"],
        }
        for row in rows
    ]

    return {
        "date": today,
        "signals": signals,
    }


@app.post("/test/signals/seed")
def seed_test_signal(
    request: SeedSignalRequest,
    x_api_key: str | None = Depends(api_key_header),
) -> dict[str, object]:
    validate_api_key(x_api_key)

    signal_type = request.signal_type.strip().upper()
    if signal_type not in {"BUY", "SELL"}:
        raise HTTPException(status_code=400, detail="signal_type must be BUY or SELL.")

    timestamp = request.signal_timestamp or f"{date.today().isoformat()}T10:00:00"
    signal_date = timestamp.split("T", 1)[0]

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        ensure_signal_log_table(conn)
        linked_buy_id: int | None = None
        if request.buy_signal_id is not None:
            linked_buy_id = int(request.buy_signal_id)
            if linked_buy_id <= 0:
                raise HTTPException(status_code=400, detail="buy_signal_id must be positive.")

        cursor = conn.execute(
            f"""
            INSERT INTO {quote_identifier(SIGNAL_LOG_TABLE)} (
                source_table,
                signal_type,
                entry_rsi,
                exit_rsi,
                previous_rsi,
                current_rsi,
                ltp,
                signal_date,
                signal_timestamp,
                notes,
                position_state,
                buy_signal_id,
                trigger_exit_rsi,
                action_timestamp,
                closed_by_signal_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                request.equity.strip().upper(),
                signal_type,
                int(request.rsi_entry),
                int(request.rsi_exit),
                float(request.current_rsi),
                float(request.current_rsi),
                float(request.ltp),
                signal_date,
                timestamp,
                "Seeded test signal for mobile app flow validation.",
                BUY_OPEN_STATE if signal_type == "BUY" else SELL_CONFIRMED_STATE,
                linked_buy_id,
                int(request.trigger_exit_rsi) if request.trigger_exit_rsi is not None else None,
                timestamp if signal_type == "SELL" else None,
                None,
            ),
        )
        inserted_id = int(cursor.lastrowid)

        linked_buy_closed = False
        if signal_type == "SELL" and linked_buy_id is not None:
            close_result = conn.execute(
                f"""
                UPDATE {quote_identifier(SIGNAL_LOG_TABLE)}
                SET position_state = ?, action_timestamp = ?, closed_by_signal_id = ?
                WHERE id = ?
                  AND signal_type = 'BUY'
                  AND COALESCE(position_state, '{BUY_OPEN_STATE}') = '{BUY_OPEN_STATE}'
                """,
                (BUY_CLOSED_STATE, timestamp, inserted_id, linked_buy_id),
            )
            linked_buy_closed = close_result.rowcount > 0

        conn.commit()
    finally:
        conn.close()

    return {
        "status": "seeded",
        "id": inserted_id,
        "equity": request.equity.strip().upper(),
        "rsi_entry": request.rsi_entry,
        "rsi_exit": request.rsi_exit,
        "signal_type": signal_type,
        "signal_timestamp": timestamp,
        "buy_signal_id": linked_buy_id,
        "trigger_exit_rsi": request.trigger_exit_rsi,
        "linked_buy_closed": linked_buy_closed if signal_type == "SELL" else None,
    }


@app.post("/add/watchlist")
def add_watchlist(
    request: WatchlistRequest,
    x_api_key: str | None = Depends(api_key_header),
) -> dict[str, object]:
    validate_api_key(x_api_key)

    raw_symbols = request.symbols.strip()
    run_all = raw_symbols.upper() == "RUN-ALL"
    if run_all:
        symbols = []
    else:
        symbols = [s.strip().upper() for s in raw_symbols.split() if s.strip()]

    if not symbols:
        if raw_symbols.upper() != "RUN-ALL":
            raise HTTPException(status_code=400, detail="No valid symbols provided.")

    current_state = snapshot_watchlist_state()
    if current_state["status"] == "running":
        return {
            "status": "already_running",
            "symbols": current_state["symbols"],
        }

    worker = threading.Thread(
        target=run_watchlist_job,
        args=(symbols, run_all),
        daemon=True,
    )
    worker.start()

    return {
        "status": "started",
        "symbols": symbols,
    }

@app.get("/add/watchlist/status")
def get_watchlist_status(
    x_api_key: str | None = Depends(api_key_header),
) -> dict[str, object]:
    validate_api_key(x_api_key)
    conn = sqlite3.connect(DB_PATH)
    try:
        state = snapshot_watchlist_state()
        state["equity_tables"] = get_equity_tables(conn)
        latest_run = get_latest_quant_engine_run(conn)
        if latest_run is not None:
            state["engine_run"] = latest_run
            if state["status"] != "running" or not state["symbols"]:
                state["status"] = latest_run["status"]
                state["started_at"] = latest_run["started_at"]
                state["finished_at"] = latest_run["finished_at"]
                state["error"] = latest_run["error_message"]
                state["output"] = latest_run["error_message"] or state["output"]
                state["symbols"] = latest_run["symbols"] or get_equity_tables(conn)
                state["run_all"] = not bool(latest_run["symbols"])
        return state
    finally:
        conn.close()

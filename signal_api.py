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
    "started_at": None,
    "finished_at": None,
    "output": "",
    "error": None,
}

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
            notes             TEXT
        )
        """
    )
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_{SIGNAL_LOG_TABLE}_lookup
        ON {quote_identifier(SIGNAL_LOG_TABLE)} (source_table, entry_rsi, exit_rsi, signal_timestamp)
        """
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

def run_watchlist_job(symbols: list[str]) -> None:
    cmd = [sys.executable, str(WATCHLIST_ENGINE_PATH)]
    cmd.extend(symbols)

    update_watchlist_state(
        status="running",
        symbols=symbols,
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
                signal_timestamp
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
    try:
        ensure_signal_log_table(conn)
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
                notes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            ),
        )
        conn.commit()
        inserted_id = int(cursor.lastrowid)
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
    }

@app.post("/add/watchlist")
def add_watchlist(
    request: WatchlistRequest,
    x_api_key: str | None = Depends(api_key_header),
) -> dict[str, object]:
    validate_api_key(x_api_key)

    symbols = [s.strip().upper() for s in request.symbols.split() if s.strip()]
    if not symbols:
        raise HTTPException(status_code=400, detail="No valid symbols provided.")

    current_state = snapshot_watchlist_state()
    if current_state["status"] == "running":
        return {
            "status": "already_running",
            "symbols": current_state["symbols"],
        }

    worker = threading.Thread(
        target=run_watchlist_job,
        args=(symbols,),
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
    return snapshot_watchlist_state()
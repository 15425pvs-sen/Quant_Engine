"""
zerodha_order_manager.py

Equity BUY/SELL order manager for Zerodha Kite Connect.

This module mirrors the order-execution features that were added for the
Upstox manager, but uses the official Zerodha Kite Connect APIs.

Implemented features:
- BUY order execution
- SELL order execution
- Single SELL support using a linked `signal_id`
- Basket SELL support using multiple BUY ids
- Basket SELL grouping by product type
- Basket SELL validation against `rsi_live_signal_log_trading`
- Live inventory checks using Kite holdings / positions
- Order confirmation prompt before submission
- Dry-run mode
- Daily BUY spend tracking

The manager is intentionally conservative. It will refuse to execute SELLs
when the referenced database rows are missing, mixed across source tables, or
mix incompatible product types.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from kiteconnect import KiteConnect

from zerodha_kite import create_kite_client


DB_PATH = Path(__file__).resolve().parent / "quant_historic_data.db"
SIGNAL_LOG_TABLE = "rsi_live_signal_log_trading"
TOKEN_FILE = Path(__file__).with_name("zerodha_tokens.json")
DAILY_USAGE_FILE = Path(__file__).resolve().parent / "zerodha_daily_usage.json"

PER_TRADE_VALUE = 4000.0
DAILY_LIMIT = 20000.0
DEFAULT_EXCHANGE = "NSE"
DEFAULT_PRODUCT = "CNC"
DEFAULT_ORDER_TYPE = "MARKET"
DEFAULT_VALIDITY = "DAY"


def _today_key() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _normalize_text(value: Any) -> str:
    return str(value or "").strip().upper()


def _safe_int(value: Any) -> int:
    try:
        return int(float(value or 0))
    except (TypeError, ValueError):
        return 0


def _parse_signal_date(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(raw[:19], fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def _signal_product_to_kite_product(product: str | None) -> str:
    normalized = _normalize_text(product)
    if normalized == "D":
        return "CNC"
    if normalized == "I":
        return "MIS"
    if normalized in {"CNC", "MIS", "NRML"}:
        return normalized
    return normalized or DEFAULT_PRODUCT


def _kite_product_to_signal_product(product: str | None) -> str | None:
    normalized = _normalize_text(product)
    if normalized == "CNC":
        return "D"
    if normalized == "MIS":
        return "I"
    if normalized:
        return normalized
    return None


def load_tokens() -> dict[str, Any]:
    if not TOKEN_FILE.exists():
        raise FileNotFoundError(
            "No Zerodha token file found. Run zerodha_auth.py first and complete the login flow."
        )

    with TOKEN_FILE.open("r", encoding="utf-8") as handle:
        data = json.load(handle)

    if not data.get("api_key") or not data.get("access_token"):
        raise ValueError(
            "The Zerodha token file is incomplete. Re-run zerodha_auth.py to generate a valid session."
        )

    return data


def _extract_numeric_margin(payload: Any) -> float | None:
    if isinstance(payload, (int, float)):
        return float(payload)
    if isinstance(payload, list):
        for item in payload:
            margin = _extract_numeric_margin(item)
            if margin is not None:
                return margin
        return None
    if not isinstance(payload, dict):
        return None

    candidate_keys = (
        "available_cash",
        "available_margin",
        "available_funds",
        "cash",
        "net",
        "equity",
        "margin",
        "available",
        "cash_available_to_trade",
        "opening_balance",
    )
    for key in candidate_keys:
        if key in payload:
            value = payload.get(key)
            if isinstance(value, (int, float)):
                return float(value)
            if isinstance(value, str):
                try:
                    return float(value)
                except ValueError:
                    pass
            nested = _extract_numeric_margin(value)
            if nested is not None:
                return nested

    for value in payload.values():
        nested = _extract_numeric_margin(value)
        if nested is not None:
            return nested
    return None


def get_today_usage() -> tuple[str, float]:
    today = _today_key()
    if not DAILY_USAGE_FILE.exists():
        return today, 0.0

    try:
        with DAILY_USAGE_FILE.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return today, 0.0

    if not isinstance(data, dict):
        return today, 0.0

    usage_date = str(data.get("date", today))
    if usage_date != today:
        return today, 0.0
    used_value = float(data.get("used_value", 0.0) or 0.0)
    return usage_date, used_value


def get_today_rejects() -> tuple[str, list[str]]:
    today = _today_key()
    if not DAILY_USAGE_FILE.exists():
        return today, []

    try:
        with DAILY_USAGE_FILE.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return today, []

    if not isinstance(data, dict):
        return today, []

    usage_date = str(data.get("date", today))
    if usage_date != today:
        return today, []

    rejects_raw = data.get("reject_list", [])
    if not isinstance(rejects_raw, list):
        rejects_raw = []
    rejects = sorted({str(item).strip().upper() for item in rejects_raw if str(item).strip()})
    return usage_date, rejects


def save_today_usage(date_str: str, used_value: float) -> None:
    payload = {
        "date": date_str,
        "used_value": round(float(used_value), 2),
    }
    _, rejects = get_today_rejects()
    payload["reject_list"] = rejects
    with DAILY_USAGE_FILE.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def add_today_reject(symbol: str) -> None:
    symbol = str(symbol).strip().upper()
    if not symbol:
        return

    today = _today_key()
    _, used_today = get_today_usage()
    _, rejects = get_today_rejects()
    rejects_set = {item.upper() for item in rejects}
    rejects_set.add(symbol)
    payload = {
        "date": today,
        "used_value": round(float(used_today), 2),
        "reject_list": sorted(rejects_set),
    }
    with DAILY_USAGE_FILE.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def is_today_rejected(symbol: str) -> bool:
    symbol = str(symbol).strip().upper()
    if not symbol:
        return False

    usage_date, rejects = get_today_rejects()
    if usage_date != _today_key():
        return False
    return symbol in {item.upper() for item in rejects}


def estimate_trade_quantity(ltp: float, per_trade_value: float) -> tuple[int, float]:
    if ltp <= 0 or per_trade_value <= 0:
        return 0, 0.0

    qty = int(per_trade_value // ltp)
    if qty <= 0:
        return 0, 0.0

    return qty, float(qty * ltp)


def validate_trade_amount(
    symbol: str,
    ltp: float,
    available_margin: float | None = None,
) -> tuple[int, float, float]:
    if ltp <= 0:
        raise ValueError("ltp must be greater than zero.")
    if PER_TRADE_VALUE <= 0:
        raise ValueError("PER_TRADE_VALUE must be greater than zero.")
    if DAILY_LIMIT <= 0:
        raise ValueError("DAILY_LIMIT must be greater than zero.")

    usage_date, used_today = get_today_usage()
    today = _today_key()
    if usage_date != today:
        used_today = 0.0

    remaining_today = max(0.0, DAILY_LIMIT - used_today)
    available_budget = max(0.0, float(available_margin)) if available_margin is not None else PER_TRADE_VALUE
    spend_budget = min(float(PER_TRADE_VALUE), remaining_today, available_budget)
    qty = int(spend_budget // ltp)
    if qty <= 0:
        raise ValueError(
            f"Order quantity came out to zero for {symbol} at LTP {ltp:.2f}. "
            f"PER_TRADE_VALUE {PER_TRADE_VALUE:.2f}, remaining {remaining_today:.2f}, "
            f"available_funds {available_budget:.2f}."
        )

    order_value = float(qty * ltp)
    return qty, order_value, used_today


def update_daily_usage(order_value: float) -> None:
    today = _today_key()
    _, used_today = get_today_usage()
    save_today_usage(today, used_today + float(order_value))


def quote_identifier(identifier: str) -> str:
    return f'"{identifier.replace("\"", "\"\"")}"'


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def ensure_signal_qty_column(conn: sqlite3.Connection) -> None:
    if not table_exists(conn, SIGNAL_LOG_TABLE):
        return
    rows = conn.execute(f"PRAGMA table_info({quote_identifier(SIGNAL_LOG_TABLE)})").fetchall()
    existing_cols = {
        str(row[1]).lower()
        for row in rows
        if len(row) > 1 and row[1] is not None
    }
    if "qty" not in existing_cols:
        conn.execute(
            f"ALTER TABLE {quote_identifier(SIGNAL_LOG_TABLE)} ADD COLUMN qty INTEGER"
        )
    if "product" not in existing_cols:
        conn.execute(
            f"ALTER TABLE {quote_identifier(SIGNAL_LOG_TABLE)} ADD COLUMN product TEXT"
        )


def fetch_signal_row(signal_id: int, db_path: Path = DB_PATH) -> Dict[str, Any] | None:
    if signal_id <= 0 or not db_path.exists():
        return None

    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            if table_exists(conn, SIGNAL_LOG_TABLE):
                ensure_signal_qty_column(conn)
                row = conn.execute(
                    f"""
                    SELECT id, source_table, signal_type, qty, product, buy_signal_id, position_state,
                           signal_date, signal_timestamp, action_timestamp, closed_by_signal_id, basket_buy_ids,
                           entry_rsi, exit_rsi, current_rsi, notes
                    FROM {quote_identifier(SIGNAL_LOG_TABLE)}
                    WHERE id = ?
                    """,
                    (int(signal_id),),
                ).fetchone()
                return dict(row) if row is not None else None
    except sqlite3.Error as exc:
        print(f"Warning: failed to fetch signal row {signal_id}: {exc}")
        return None
    return None


def fetch_signal_rows(signal_ids: list[int], db_path: Path = DB_PATH) -> list[Dict[str, Any]]:
    rows: list[Dict[str, Any]] = []
    for signal_id in signal_ids:
        row = fetch_signal_row(int(signal_id), db_path=db_path)
        if row is not None:
            rows.append(row)
    return rows


def update_signal_execution_details(
    signal_id: int,
    qty: int,
    product: str | None,
    db_path: Path = DB_PATH,
) -> bool:
    if signal_id <= 0:
        return False

    if not db_path.exists():
        print(
            f"Skipping qty update for signal_id={signal_id}: "
            f"database not found at {db_path}."
        )
        return False

    last_exc: Exception | None = None
    desired_qty = int(qty)
    desired_product = product
    for attempt in range(1, 6):
        try:
            with sqlite3.connect(db_path, timeout=30) as conn:
                conn.execute("PRAGMA busy_timeout = 30000")
                conn.execute("PRAGMA journal_mode = WAL")
                ensure_signal_qty_column(conn)
                cursor = conn.execute(
                    f"UPDATE {quote_identifier(SIGNAL_LOG_TABLE)} SET qty = ?, product = ? WHERE id = ?",
                    (desired_qty, desired_product, int(signal_id)),
                )
                conn.commit()
                if cursor.rowcount <= 0:
                    print(
                        f"Warning: execution detail update affected no rows for "
                        f"signal_id={signal_id}."
                    )
                    return False
                return True
        except sqlite3.OperationalError as exc:
            last_exc = exc
            if "locked" not in str(exc).lower() or attempt >= 5:
                break
            continue
        except sqlite3.Error as exc:
            last_exc = exc
            break

    try:
        for _ in range(10):
            with sqlite3.connect(db_path, timeout=30) as conn:
                conn.execute("PRAGMA busy_timeout = 30000")
                conn.execute("PRAGMA journal_mode = WAL")
                conn.row_factory = sqlite3.Row
                row = conn.execute(
                    f"SELECT qty, product FROM {quote_identifier(SIGNAL_LOG_TABLE)} WHERE id = ?",
                    (int(signal_id),),
                ).fetchone()
                if row is not None:
                    current_qty = row["qty"]
                    current_product = row["product"]
                    if int(current_qty or 0) == desired_qty and str(current_product or "") == str(desired_product or ""):
                        return True
            time.sleep(0.25)
    except sqlite3.Error:
        pass

    print(
        f"Warning: failed to update execution details for "
        f"signal_id={signal_id}: {last_exc}"
    )
    return False


def resolve_sell_quantity(
    sell_signal_id: int,
    db_path: Path = DB_PATH,
) -> tuple[int, int, str | None]:
    sell_row = fetch_signal_row(sell_signal_id, db_path=db_path)
    if not sell_row:
        raise ValueError(f"SELL signal row {sell_signal_id} not found in {db_path}.")

    signal_type = _normalize_text(sell_row.get("signal_type"))
    if signal_type == "BUY":
        qty = _safe_int(sell_row.get("qty"))
        if qty <= 0:
            raise ValueError(f"BUY signal row {sell_signal_id} has no valid qty.")
        product = sell_row.get("product")
        return qty, int(sell_signal_id), _signal_product_to_kite_product(product)

    if signal_type != "SELL":
        raise ValueError(f"Signal row {sell_signal_id} is not a BUY or SELL row.")

    buy_signal_id = _safe_int(sell_row.get("buy_signal_id"))
    if buy_signal_id <= 0:
        raise ValueError(f"SELL signal row {sell_signal_id} does not reference a BUY row.")

    buy_row = fetch_signal_row(buy_signal_id, db_path=db_path)
    if not buy_row:
        raise ValueError(
            f"Linked BUY signal row {buy_signal_id} not found for SELL row {sell_signal_id}."
        )

    if _normalize_text(buy_row.get("signal_type")) != "BUY":
        raise ValueError(f"Linked signal row {buy_signal_id} is not a BUY row.")

    qty = _safe_int(buy_row.get("qty"))
    if qty <= 0:
        raise ValueError(f"Linked BUY signal row {buy_signal_id} has no valid qty.")

    buy_product = _signal_product_to_kite_product(buy_row.get("product"))
    return qty, buy_signal_id, buy_product


def resolve_basket_sell_quantity(
    basket_buy_ids: list[int],
    db_path: Path = DB_PATH,
) -> tuple[int, list[int], str | None, datetime | None]:
    if not basket_buy_ids:
        raise ValueError("Basket SELL requires at least one BUY signal id.")

    buy_rows = fetch_signal_rows(basket_buy_ids, db_path=db_path)
    if len(buy_rows) != len(basket_buy_ids):
        missing = sorted(
            set(int(signal_id) for signal_id in basket_buy_ids)
            - {int(row["id"]) for row in buy_rows}
        )
        raise ValueError(f"Basket BUY signal rows not found for ids: {missing}")

    total_qty = 0
    products: set[str] = set()
    source_tables: set[str] = set()
    trade_dates: list[datetime] = []

    for row in buy_rows:
        if _normalize_text(row.get("signal_type")) != "BUY":
            raise ValueError(f"Signal row {row.get('id')} is not a BUY row.")

        qty = _safe_int(row.get("qty"))
        if qty <= 0:
            raise ValueError(f"BUY signal row {row.get('id')} has no valid qty.")

        total_qty += qty
        product = _signal_product_to_kite_product(row.get("product"))
        if product:
            products.add(product)

        source_table = _normalize_text(row.get("source_table"))
        if source_table:
            source_tables.add(source_table)

        trade_dt = _parse_signal_date(row.get("signal_date") or row.get("signal_timestamp") or row.get("action_timestamp"))
        if trade_dt is not None:
            trade_dates.append(trade_dt)

    if len(source_tables) > 1:
        raise ValueError(
            f"Basket BUY signal rows must all belong to the same source_table, found: {sorted(source_tables)}"
        )

    if len(products) > 1:
        raise ValueError(
            f"Basket BUY signal rows must all have the same product type, found: {sorted(products)}"
        )

    preferred_product = products.pop() if len(products) == 1 else None
    earliest_trade_dt = min(trade_dates) if trade_dates else None
    return total_qty, [int(signal_id) for signal_id in basket_buy_ids], preferred_product, earliest_trade_dt


def _row_matches_symbol(row: Dict[str, Any], symbol: str, exchange: str) -> bool:
    row_symbol = _normalize_text(
        row.get("tradingsymbol") or row.get("trading_symbol") or row.get("symbol")
    )
    row_exchange = _normalize_text(row.get("exchange"))
    if row_symbol != _normalize_text(symbol):
        return False
    if row_exchange and row_exchange != _normalize_text(exchange):
        return False
    return True


def _available_holdings_quantity(
    rows: list[Dict[str, Any]],
    symbol: str,
    exchange: str,
    product: str | None = None,
) -> int:
    available = 0
    product_filter = _normalize_text(product) or None
    for row in rows:
        if not isinstance(row, dict):
            continue
        if not _row_matches_symbol(row, symbol, exchange):
            continue

        row_product = _normalize_text(row.get("product")) or None
        if product_filter is not None and row_product != product_filter:
            continue

        quantity = _safe_int(row.get("quantity"))
        used = _safe_int(row.get("used_quantity"))
        available += max(0, quantity - used)
    return available


def _available_positions_quantity(
    rows: list[Dict[str, Any]],
    symbol: str,
    exchange: str,
    product: str | None = None,
) -> int:
    available = 0
    product_filter = _normalize_text(product) or None
    for row in rows:
        if not isinstance(row, dict):
            continue
        if not _row_matches_symbol(row, symbol, exchange):
            continue

        row_product = _normalize_text(row.get("product")) or None
        if product_filter is not None and row_product != product_filter:
            continue

        available += max(0, _safe_int(row.get("quantity")))
    return available


class ZerodhaOrderError(RuntimeError):
    pass


class ZerodhaOrderManager:
    def __init__(self) -> None:
        self.kite = create_kite_client()
        self.allow_live_orders = str(os.getenv("ZERODHA_ALLOW_LIVE_ORDERS", "")).strip().lower() in {"1", "true", "yes", "y", "on"}

    def get_available_margin(self) -> float | None:
        for getter in (
            lambda: self.kite.margins(),
            lambda: self.kite.margins("equity"),
        ):
            try:
                payload = getter()
            except Exception:
                continue
            margin = _extract_numeric_margin(payload)
            if margin is not None:
                return margin
        return None

    def get_positions(self) -> list[Dict[str, Any]]:
        payload = self.kite.positions()
        if isinstance(payload, dict):
            net = payload.get("net", [])
            return net if isinstance(net, list) else []
        return []

    def get_holdings(self) -> list[Dict[str, Any]]:
        payload = self.kite.holdings()
        return payload if isinstance(payload, list) else []

    def resolve_live_sell_inventory(
        self,
        symbol: str,
        exchange: str,
        required_qty: int,
        preferred_product: str | None = None,
    ) -> tuple[str, int, str]:
        if required_qty <= 0:
            raise ZerodhaOrderError("required_qty must be positive.")

        positions = self.get_positions()
        holdings = self.get_holdings()
        preferred_product = _signal_product_to_kite_product(preferred_product)

        candidates: list[tuple[str, int, str]] = []
        for product_name, rows, source in (
            ("MIS", positions, "positions"),
            ("NRML", positions, "positions"),
            ("CNC", holdings, "holdings"),
        ):
            available = (
                _available_positions_quantity(rows, symbol, exchange, product=product_name)
                if source == "positions"
                else _available_holdings_quantity(rows, symbol, exchange, product=product_name)
            )
            if available > 0:
                candidates.append((product_name, available, source))

        if preferred_product in {"CNC", "MIS", "NRML"}:
            preferred_source = "holdings" if preferred_product == "CNC" else "positions"
            preferred_available = (
                _available_holdings_quantity(holdings, symbol, exchange, product=preferred_product)
                if preferred_source == "holdings"
                else _available_positions_quantity(positions, symbol, exchange, product=preferred_product)
            )
            if preferred_available <= 0:
                raise ZerodhaOrderError(
                    f"No live {preferred_product} inventory found for {symbol} on {exchange}."
                )
            if preferred_available < required_qty:
                raise ZerodhaOrderError(
                    f"Insufficient live {preferred_product} inventory for {symbol} on {exchange}. "
                    f"Required {required_qty}, available {preferred_available}."
                )
            return preferred_product, preferred_available, preferred_source

        for product_name, available, source in candidates:
            if available >= required_qty:
                return product_name, available, source

        raise ZerodhaOrderError(
            f"No live inventory found for {symbol} on {exchange} with quantity {required_qty}."
        )

    def place_order(
        self,
        *,
        symbol: str,
        quantity: int,
        side: str,
        order_type: str = DEFAULT_ORDER_TYPE,
        product: str = DEFAULT_PRODUCT,
        exchange: str = DEFAULT_EXCHANGE,
        price: float = 0.0,
        trigger_price: float = 0.0,
        validity: str = DEFAULT_VALIDITY,
        tag: str = "PYTHON",
        amo: bool = False,
        dry_run: bool = False,
        market_protection: float | None = None,
        autoslice: bool | None = None,
    ) -> Dict[str, Any]:
        if quantity <= 0:
            raise ZerodhaOrderError("quantity must be greater than zero.")

        transaction_type = "BUY" if side.upper() == "BUY" else "SELL"
        order_type = order_type.upper()
        product = _signal_product_to_kite_product(product)
        variety = "amo" if amo else "regular"

        payload: dict[str, Any] = {
            "variety": variety,
            "exchange": exchange,
            "tradingsymbol": symbol,
            "transaction_type": transaction_type,
            "quantity": int(quantity),
            "product": product,
            "order_type": order_type,
            "validity": validity,
            "tag": tag,
        }

        if order_type in {"LIMIT", "SL", "SL-M"}:
            payload["price"] = float(price)
        if order_type in {"SL", "SL-M"} and float(trigger_price or 0.0) > 0:
            payload["trigger_price"] = float(trigger_price)
        if market_protection is not None:
            payload["market_protection"] = float(market_protection)
        if autoslice is not None:
            payload["autoslice"] = bool(autoslice)

        if dry_run or not self.allow_live_orders:
            return {
                "status": "DRY_RUN",
                "payload": payload,
            }

        try:
            order_id = self.kite.place_order(**payload)
        except Exception as exc:  # pragma: no cover - depends on broker/API state
            raise ZerodhaOrderError(f"Failed to place Zerodha order: {exc}") from exc

        return {
            "status": "SUBMITTED",
            "order_id": order_id,
            "payload": payload,
        }


def confirm_prompt(prompt: str) -> bool:
    while True:
        print(prompt, end="", flush=True)
        response = input().strip().upper()
        if response in {"Y", "N"}:
            return response == "Y"
        print("Please type Y or N.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Zerodha Kite equity order manager")
    parser.add_argument("--symbol", required=True, help="Trading symbol, e.g. RELIANCE")
    parser.add_argument("--side", required=True, choices=["BUY", "SELL"])
    parser.add_argument("--order-type", default=DEFAULT_ORDER_TYPE, choices=["MARKET", "LIMIT", "SL", "SL-M"])
    parser.add_argument("--ltp", required=True, type=float, help="Current LTP used to size the order.")
    parser.add_argument("--product", default=DEFAULT_PRODUCT, help="CNC, MIS, NRML, D, or I")
    parser.add_argument("--price", default=0.0, type=float)
    parser.add_argument("--trigger-price", default=0.0, type=float)
    parser.add_argument("--validity", default=DEFAULT_VALIDITY, choices=["DAY", "IOC", "TTL"])
    parser.add_argument("--exchange", default=DEFAULT_EXCHANGE, choices=["NSE", "BSE", "NFO", "MCX"])
    parser.add_argument("--tag", default="PYTHON")
    parser.add_argument("--signal-id", type=int, default=None, help="Optional rsi_live_signal_log_trading row id.")
    parser.add_argument("--basket-buy-ids", default="", help="Comma-separated BUY signal ids to combine into one SELL order.")
    parser.add_argument("--db-path", default=str(DB_PATH), help="Path to quant_historic_data.db used for qty resolution.")
    parser.add_argument("--live", action="store_true", help="Allow real order submission for this invocation.")
    parser.add_argument("--confirm-order", action="store_true", help="Prompt for confirmation before order placement.")
    parser.add_argument("--dry-run", action="store_true", help="Force a dry-run response.")
    parser.add_argument("--amo", action="store_true", help="Submit as an after-market order when supported.")
    parser.add_argument("--market-protection", type=float, default=None)
    parser.add_argument("--autoslice", action="store_true")

    args = parser.parse_args()

    if args.live:
        os.environ["ZERODHA_ALLOW_LIVE_ORDERS"] = "true"

    manager = ZerodhaOrderManager()
    db_path = Path(args.db_path)

    qty: int = 0
    order_value: float = 0.0
    used_today = 0.0
    linked_buy_signal_id: int | None = None
    resolved_sell_product: str | None = None
    basket_buy_ids: list[int] = []

    available_margin = manager.get_available_margin()
    if available_margin is not None:
        print(f"Available Zerodha margin: {available_margin:.2f}")
    else:
        print("Available Zerodha margin: unavailable")

    if args.side == "BUY":
        if is_today_rejected(args.symbol):
            print(f"BUY {args.symbol} skipped: symbol declined earlier today.")
            return
        try:
            qty, order_value, used_today = validate_trade_amount(
                args.symbol,
                args.ltp,
                available_margin=available_margin,
            )
            print(f"Daily used so far: {used_today:.2f}")
            print(f"Remaining today: {max(0.0, DAILY_LIMIT - used_today):.2f}")
            print(f"Calculated qty: {qty}")
            print(f"Proposed value: {order_value:.2f}")
            print(f"Configured PER_TRADE_VALUE: {PER_TRADE_VALUE:.2f}")
            print(f"Configured DAILY_LIMIT: {DAILY_LIMIT:.2f}")
        except ValueError as exc:
            print(f"BUY {args.symbol} skipped: {exc}")
            return
    else:
        basket_arg = str(args.basket_buy_ids or "").strip()
        if basket_arg:
            try:
                basket_buy_ids = [
                    int(part.strip())
                    for part in basket_arg.split(",")
                    if part.strip()
                ]
            except ValueError:
                print(f"SELL {args.symbol} skipped: invalid --basket-buy-ids value.")
                return

            try:
                qty, basket_buy_ids, linked_buy_product, _basket_trade_dt = resolve_basket_sell_quantity(
                    basket_buy_ids,
                    db_path=db_path,
                )
                linked_buy_signal_id = basket_buy_ids[0]
                linked_buy_row = fetch_signal_row(linked_buy_signal_id, db_path=db_path)
                source_table = _normalize_text((linked_buy_row or {}).get("source_table"))
                basket_buy_rows = fetch_signal_rows(basket_buy_ids, db_path=db_path)
                basket_buy_ids_text = ",".join(str(buy_id) for buy_id in basket_buy_ids)
                resolved_sell_product, live_available_qty, inventory_source = manager.resolve_live_sell_inventory(
                    args.symbol,
                    args.exchange,
                    qty,
                    preferred_product=linked_buy_product,
                )
                if live_available_qty < qty:
                    raise ZerodhaOrderError(
                        f"Insufficient live inventory for basket SELL of {args.symbol}: "
                        f"required {qty}, available {live_available_qty}."
                    )
                order_value = float(qty) * float(args.ltp)
                print(
                    f"Resolved SELL qty from BASKET buy_ids=[{basket_buy_ids_text}]: {qty}"
                )
                if source_table:
                    print(f"Basket source_table: {source_table}")
                print(f"Live Zerodha {inventory_source} quantity for {args.symbol} on {args.exchange}: {live_available_qty}")
                if linked_buy_product:
                    print(f"Basket BUY product: {linked_buy_product}")
                print(f"Resolved SELL product: {resolved_sell_product}")
                print(f"Basket BUY rows: {len(basket_buy_rows)}")
                print(f"SELL order value: {order_value:.2f}")
            except (ValueError, ZerodhaOrderError) as exc:
                print(f"SELL {args.symbol} skipped: {exc}")
                return
        else:
            if args.signal_id is None:
                print("SELL orders require --signal-id or --basket-buy-ids so the linked BUY quantity can be resolved.")
                return

            try:
                qty, linked_buy_signal_id, linked_buy_product = resolve_sell_quantity(
                    args.signal_id,
                    db_path=db_path,
                )
                linked_buy_row = fetch_signal_row(linked_buy_signal_id, db_path=db_path)
                linked_buy_dt = _parse_signal_date((linked_buy_row or {}).get("signal_timestamp"))
                if linked_buy_dt is None:
                    linked_buy_dt = _parse_signal_date((linked_buy_row or {}).get("action_timestamp"))
                if linked_buy_dt is None:
                    linked_buy_dt = _parse_signal_date((linked_buy_row or {}).get("signal_date"))
                order_value = float(qty) * float(args.ltp)
                resolved_sell_product, live_available_qty, inventory_source = manager.resolve_live_sell_inventory(
                    args.symbol,
                    args.exchange,
                    qty,
                    preferred_product=linked_buy_product,
                )
                if live_available_qty < qty:
                    raise ZerodhaOrderError(
                        f"Insufficient live inventory for SELL of {args.symbol}: "
                        f"required {qty}, available {live_available_qty}."
                    )
                print(
                    f"Resolved SELL qty from BUY signal_id={linked_buy_signal_id}: {qty}"
                )
                if linked_buy_dt is not None:
                    print(f"Linked BUY date: {linked_buy_dt.date().isoformat()}")
                print(f"Live Zerodha {inventory_source} quantity for {args.symbol} on {args.exchange}: {live_available_qty}")
                if linked_buy_product:
                    print(f"Linked BUY product: {linked_buy_product}")
                print(f"Resolved SELL product: {resolved_sell_product}")
                print(f"SELL order value: {order_value:.2f}")
            except (ValueError, ZerodhaOrderError) as exc:
                print(f"SELL {args.symbol} skipped: {exc}")
                return

    if args.confirm_order:
        if args.side == "BUY":
            prompt = (
                f"Confirm Zerodha BUY order for {args.symbol} | "
                f"qty={qty} | order_value={order_value:.2f} | "
                f"available_funds={(available_margin if available_margin is not None else 0.0):.2f} | "
                f"remaining_daily_limit={max(0.0, DAILY_LIMIT - used_today):.2f} ? [Y/N]: "
            )
        else:
            prompt = (
                f"Confirm Zerodha SELL order for {args.symbol} | "
                f"qty={qty} | order_value={order_value:.2f} | "
                f"linked_buy_signal_id={linked_buy_signal_id} | "
                f"live_inventory={resolved_sell_product or args.product} ? [Y/N]: "
            )
        try:
            if not confirm_prompt(prompt):
                print(f"Skipping {args.side} {args.symbol}: user declined confirmation.")
                if args.side == "BUY":
                    add_today_reject(args.symbol)
                return
        except EOFError:
            print(f"Skipping {args.side} {args.symbol}: confirmation input unavailable.")
            return

    product_for_order = args.product
    if args.side == "SELL":
        product_for_order = resolved_sell_product or args.product

    result = manager.place_order(
        symbol=args.symbol,
        quantity=qty,
        side=args.side,
        order_type=args.order_type,
        product=product_for_order,
        exchange=args.exchange,
        price=args.price,
        trigger_price=args.trigger_price,
        validity=args.validity,
        tag=args.tag,
        amo=args.amo,
        dry_run=args.dry_run,
        market_protection=args.market_protection,
        autoslice=args.autoslice if args.autoslice else None,
    )

    print("\nZerodha response:")
    print(result)

    if result.get("status") == "DRY_RUN":
        return

    if args.side == "BUY":
        update_daily_usage(order_value)


if __name__ == "__main__":
    main()

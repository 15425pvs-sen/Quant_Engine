"""
upstox_order_manager.py

Equity BUY/SELL order manager for Upstox.

This module:
- Loads the access token from upstox_auth.py
- Searches NSE equity instruments and resolves instrument_key
- Places BUY/SELL equity orders
- Supports MARKET, LIMIT, SL and SL-M
- Supports Delivery (D) and Intraday (I)
- Supports AMO
- Supports order slicing
- Reads order details / order history
- Cancels an order
- Provides a simple CLI for testing

IMPORTANT:
The place_buy/place_sell methods submit REAL orders when using your live
Upstox access token. Test with Upstox Sandbox before going live.

"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import requests

from upstox_auth import get_valid_access_token


BASE_URL = "https://api.upstox.com/v2"
FUNDS_URL = f"{BASE_URL}/user/get-funds-and-margin"
FUNDS_URL_V3 = "https://api.upstox.com/v3/user/get-funds-and-margin"
ORDER_URL_V3 = "https://api-hft.upstox.com/v3/order/place"
CONVERT_POSITION_URL = f"{BASE_URL}/portfolio/convert-position"
DEFAULT_TIMEOUT = 30
DB_PATH = Path(__file__).resolve().parent / "quant_historic_data.db"
SIGNAL_LOG_TABLE = "rsi_live_signal_log_trading"
DAILY_USAGE_FILE = Path(__file__).resolve().parent / "upstox_daily_usage.json"
PER_TRADE_VALUE = 4000.0
DAILY_LIMIT = 20000.0
BUY_FUNDS_BUFFER_PCT = 0.0


def _today_key() -> str:
    return datetime.now().strftime("%Y-%m-%d")


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


def _extract_available_margin(payload: Dict[str, Any]) -> float | None:
    data = payload.get("data", {})
    if not isinstance(data, dict):
        return None

    def _as_float(value: Any) -> float:
        if value is None:
            raise ValueError
        return float(value)

    direct_candidates = [
        data.get("available_margin"),
        data.get("availableFunds"),
        data.get("available_funds"),
    ]
    for value in direct_candidates:
        try:
            return _as_float(value)
        except (TypeError, ValueError):
            continue

    equity = data.get("equity")
    if isinstance(equity, dict):
        for value in (
            equity.get("available_margin"),
            equity.get("availableFunds"),
            equity.get("available_funds"),
            equity.get("total"),
        ):
            try:
                return _as_float(value)
            except (TypeError, ValueError):
                continue

    available_to_trade = data.get("available_to_trade")
    if isinstance(available_to_trade, dict):
        for value in (
            available_to_trade.get("total"),
            available_to_trade.get("available_margin"),
            available_to_trade.get("cash_available_to_trade"),
        ):
            if isinstance(value, (int, float, str)):
                try:
                    return _as_float(value)
                except (TypeError, ValueError):
                    continue

        cash = available_to_trade.get("cash")
        if isinstance(cash, dict):
            for value in (
                cash.get("available_margin"),
                cash.get("margin_available"),
                cash.get("available"),
            ):
                try:
                    return _as_float(value)
                except (TypeError, ValueError):
                    continue

        for value in (
            available_to_trade.get("available_margin"),
        ):
            try:
                return _as_float(value)
            except (TypeError, ValueError):
                continue

    return None


def ensure_signal_qty_column(conn: sqlite3.Connection) -> None:
    rows = conn.execute(f"PRAGMA table_info({SIGNAL_LOG_TABLE})").fetchall()
    existing_cols = {
        str(row[1]).lower()
        for row in rows
        if len(row) > 1 and row[1] is not None
    }
    if "qty" not in existing_cols:
        conn.execute(
            f"ALTER TABLE {SIGNAL_LOG_TABLE} ADD COLUMN qty INTEGER"
        )
    if "product" not in existing_cols:
        conn.execute(
            f"ALTER TABLE {SIGNAL_LOG_TABLE} ADD COLUMN product TEXT"
        )


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
                    f"UPDATE {SIGNAL_LOG_TABLE} SET qty = ?, product = ? WHERE id = ?",
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
                    f"SELECT qty, product FROM {SIGNAL_LOG_TABLE} WHERE id = ?",
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


def fetch_signal_row(
    signal_id: int,
    db_path: Path = DB_PATH,
) -> Dict[str, Any] | None:
    if signal_id <= 0 or not db_path.exists():
        return None

    try:
        with sqlite3.connect(db_path) as conn:
            ensure_signal_qty_column(conn)
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                f"""
                SELECT id, source_table, signal_type, qty, product, buy_signal_id, position_state,
                       signal_date, signal_timestamp, action_timestamp, closed_by_signal_id, basket_buy_ids
                FROM {SIGNAL_LOG_TABLE}
                WHERE id = ?
                """,
                (int(signal_id),),
            ).fetchone()
            return dict(row) if row is not None else None
    except sqlite3.Error as exc:
        print(f"Warning: failed to fetch signal row {signal_id}: {exc}")
        return None


def fetch_signal_rows(
    signal_ids: list[int],
    db_path: Path = DB_PATH,
) -> list[Dict[str, Any]]:
    rows: list[Dict[str, Any]] = []
    for signal_id in signal_ids:
        row = fetch_signal_row(int(signal_id), db_path=db_path)
        if row is not None:
            rows.append(row)
    return rows


def resolve_sell_quantity(
    sell_signal_id: int,
    db_path: Path = DB_PATH,
) -> tuple[int, int, str | None]:
    sell_row = fetch_signal_row(sell_signal_id, db_path=db_path)
    if not sell_row:
        raise ValueError(
            f"SELL signal row {sell_signal_id} not found in {db_path}."
        )

    signal_type = str(sell_row.get("signal_type", "")).upper()
    if signal_type == "BUY":
        qty_raw = sell_row.get("qty")
        try:
            qty = int(qty_raw or 0)
        except (TypeError, ValueError):
            qty = 0

        if qty <= 0:
            raise ValueError(
                f"BUY signal row {sell_signal_id} has no valid qty."
            )

        product = sell_row.get("product")
        return qty, int(sell_signal_id), str(product) if product else None

    if signal_type != "SELL":
        raise ValueError(
            f"Signal row {sell_signal_id} is not a BUY or SELL row."
        )

    buy_signal_id = int(sell_row.get("buy_signal_id") or 0)
    if buy_signal_id <= 0:
        raise ValueError(
            f"SELL signal row {sell_signal_id} does not reference a BUY row."
        )

    buy_row = fetch_signal_row(buy_signal_id, db_path=db_path)
    if not buy_row:
        raise ValueError(
            f"Linked BUY signal row {buy_signal_id} not found for SELL row "
            f"{sell_signal_id}."
        )

    if str(buy_row.get("signal_type", "")).upper() != "BUY":
        raise ValueError(
            f"Linked signal row {buy_signal_id} is not a BUY row."
        )

    qty_raw = buy_row.get("qty")
    try:
        qty = int(qty_raw or 0)
    except (TypeError, ValueError):
        qty = 0

    if qty <= 0:
        raise ValueError(
            f"Linked BUY signal row {buy_signal_id} has no valid qty."
        )

    buy_product = _normalize_text(buy_row.get("product")) or None
    return qty, buy_signal_id, buy_product


def resolve_basket_sell_quantity(
    basket_buy_ids: list[int],
    db_path: Path = DB_PATH,
) -> tuple[int, list[int], str | None, datetime | None]:
    if not basket_buy_ids:
        raise ValueError("Basket SELL requires at least one BUY signal id.")

    buy_rows = fetch_signal_rows(basket_buy_ids, db_path=db_path)
    if len(buy_rows) != len(basket_buy_ids):
        missing = sorted(set(int(signal_id) for signal_id in basket_buy_ids) - {int(row["id"]) for row in buy_rows})
        raise ValueError(f"Basket BUY signal rows not found for ids: {missing}")

    total_qty = 0
    products: set[str] = set()
    trade_dates: list[datetime] = []
    for row in buy_rows:
        if str(row.get("signal_type", "")).upper() != "BUY":
            raise ValueError(f"Signal row {row.get('id')} is not a BUY row.")

        qty_raw = row.get("qty")
        try:
            qty = int(qty_raw or 0)
        except (TypeError, ValueError):
            qty = 0
        if qty <= 0:
            raise ValueError(f"BUY signal row {row.get('id')} has no valid qty.")

        total_qty += qty
        product = _normalize_text(row.get("product")) or ""
        if product:
            products.add(product)

        trade_dt = _parse_signal_date(row.get("signal_date"))
        if trade_dt is not None:
            trade_dates.append(trade_dt)

    if len(products) > 1:
        raise ValueError(
            f"Basket BUY signal rows must all have the same product type, found: {sorted(products)}"
        )

    preferred_product = products.pop() if len(products) == 1 else None
    earliest_trade_dt = min(trade_dates) if trade_dates else None
    return total_qty, [int(signal_id) for signal_id in basket_buy_ids], preferred_product, earliest_trade_dt


def _normalize_text(value: Any) -> str:
    return str(value or "").strip().upper()


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


def _row_matches_symbol(
    row: Dict[str, Any],
    symbol: str,
    exchange: str,
) -> bool:
    row_symbol = _normalize_text(
        row.get("trading_symbol") or row.get("tradingsymbol")
    )
    row_exchange = _normalize_text(row.get("exchange"))
    if row_symbol != _normalize_text(symbol):
        return False
    if row_exchange and row_exchange != _normalize_text(exchange):
        return False
    return True


def _safe_int(value: Any) -> int:
    try:
        return int(float(value or 0))
    except (TypeError, ValueError):
        return 0


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
        used = _safe_int(row.get("cnc_used_quantity"))
        row_available = max(0, quantity - used)
        available += row_available
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


class UpstoxOrderError(RuntimeError):
    pass


class UpstoxOrderManager:
    def __init__(
        self,
        access_token: Optional[str] = None,
        auto_authenticate: bool = True,
    ) -> None:
        self.access_token = access_token or get_valid_access_token(
            force_login=False
        )
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            }
        )

        # Prevent accidental live orders during development unless explicitly
        # enabled by the caller.
        self.allow_live_orders = (
            os.getenv("UPSTOX_ALLOW_LIVE_ORDERS", "false").lower()
            == "true"
        )

    def _request(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        try:
            response = self.session.request(
                method,
                url,
                timeout=DEFAULT_TIMEOUT,
                **kwargs,
            )
        except requests.RequestException as exc:
            raise UpstoxOrderError(
                f"Network error while calling Upstox: {exc}"
            ) from exc

        try:
            payload = response.json()
        except ValueError:
            payload = {"raw_response": response.text}

        if response.status_code == 401:
            raise UpstoxOrderError(
                "Upstox access token is invalid or expired. "
                "Run `python upstox_auth.py` to authenticate again."
            )

        if not response.ok:
            raise UpstoxOrderError(
                f"Upstox API error HTTP {response.status_code}: {payload}"
            )

        return payload

    def search_equity(
        self,
        symbol: str,
        exchange: str = "NSE",
        records: int = 10,
    ) -> list[Dict[str, Any]]:
        """
        Search for equity instruments.

        Returns matching instruments including instrument_key.
        """
        params = {
            "query": symbol.upper(),
            "exchanges": exchange.upper(),
            "segments": "EQ",
            "instrument_types": "EQ",
            "page_number": 1,
            "records": records,
        }

        payload = self._request(
            "GET",
            f"{BASE_URL}/instruments/search",
            params=params,
        )

        return payload.get("data", [])

    def resolve_instrument(
        self,
        symbol: str,
        exchange: str = "NSE",
    ) -> Dict[str, Any]:
        """
        Resolve a stock symbol to a unique Upstox instrument.

        Example:
            resolve_instrument("RELIANCE")
        """
        symbol = symbol.strip().upper()

        results = self.search_equity(symbol, exchange=exchange)

        exact = [
            item
            for item in results
            if item.get("trading_symbol", "").upper() == symbol
            and item.get("segment") == f"{exchange.upper()}_EQ"
        ]

        if len(exact) == 1:
            return exact[0]

        if len(results) == 1:
            return results[0]

        if not results:
            raise UpstoxOrderError(
                f"No {exchange.upper()} equity instrument found for "
                f"symbol '{symbol}'."
            )

        candidates = "\n".join(
            f"  - {item.get('trading_symbol')} | "
            f"{item.get('instrument_key')} | "
            f"{item.get('name')}"
            for item in results
        )

        raise UpstoxOrderError(
            f"Could not uniquely resolve '{symbol}'. "
            f"Candidates:\n{candidates}"
        )

    def _validate_order(
        self,
        quantity: int,
        order_type: str,
        product: str,
        transaction_type: str,
        price: float,
        trigger_price: float,
    ) -> None:
        if quantity <= 0:
            raise ValueError("quantity must be greater than 0")

        if order_type not in {"MARKET", "LIMIT", "SL", "SL-M"}:
            raise ValueError(
                "order_type must be one of MARKET, LIMIT, SL, SL-M"
            )

        if product not in {"D", "I"}:
            raise ValueError("product must be 'D' (Delivery) or 'I' (Intraday)")

        if transaction_type not in {"BUY", "SELL"}:
            raise ValueError("transaction_type must be BUY or SELL")

        if order_type == "LIMIT" and price <= 0:
            raise ValueError("LIMIT orders require price > 0")

        if order_type == "SL" and (price <= 0 or trigger_price <= 0):
            raise ValueError(
                "SL orders require both price > 0 and trigger_price > 0"
            )

        if order_type == "SL-M" and trigger_price <= 0:
            raise ValueError("SL-M orders require trigger_price > 0")

        if order_type == "MARKET" and (
            price != 0 or trigger_price != 0
        ):
            raise ValueError(
                "MARKET orders should use price=0 and trigger_price=0"
            )

    def place_equity_order(
        self,
        symbol: str,
        quantity: int,
        transaction_type: str,
        order_type: str = "MARKET",
        product: str = "D",
        price: float = 0.0,
        trigger_price: float = 0.0,
        is_amo: bool = False,
        tag: str = "PYTHON",
        exchange: str = "NSE",
        slice_order: bool = True,
        disclosed_quantity: int = 0,
    ) -> Dict[str, Any]:
        """
        Place an equity BUY/SELL order.

        Parameters:
            symbol: NSE/BSE trading symbol, e.g. RELIANCE
            quantity: Number of shares
            transaction_type: BUY or SELL
            order_type: MARKET, LIMIT, SL, SL-M
            product: D=Delivery, I=Intraday
            price: Required for LIMIT/SL
            trigger_price: Required for SL/SL-M
            is_amo: True for After Market Order
            tag: User-defined order tag
            exchange: NSE or BSE
            slice_order: Enable Upstox auto-slicing
        """
        transaction_type = transaction_type.upper()
        order_type = order_type.upper()
        product = product.upper()
        exchange = exchange.upper()

        self._validate_order(
            quantity=quantity,
            order_type=order_type,
            product=product,
            transaction_type=transaction_type,
            price=price,
            trigger_price=trigger_price,
        )

        instrument = self.resolve_instrument(symbol, exchange)
        instrument_key = instrument["instrument_key"]

        payload = {
            "quantity": quantity,
            "product": product,
            "validity": "DAY",
            "price": price,
            "tag": tag[:40],
            "instrument_token": instrument_key,
            "order_type": order_type,
            "transaction_type": transaction_type,
            "disclosed_quantity": disclosed_quantity,
            "trigger_price": trigger_price,
            "is_amo": bool(is_amo),
            "slice": bool(slice_order),
            "market_protection": -1
        }

        if not self.allow_live_orders:
            print("\nLIVE ORDER BLOCKED")
            print(
                "Set UPSTOX_ALLOW_LIVE_ORDERS=true only when you are "
                "ready to submit real orders."
            )
            print("\nResolved instrument:")
            print(f"  Symbol          : {instrument.get('trading_symbol')}")
            print(f"  Instrument key  : {instrument_key}")
            print("\nOrder payload:")
            print(payload)

            return {
                "status": "DRY_RUN",
                "instrument": instrument,
                "order": payload,
            }

        return self._request(
            "POST",
            ORDER_URL_V3,
            json=payload,
            headers={"Content-Type": "application/json"},
        )

    def place_buy(self, **kwargs: Any) -> Dict[str, Any]:
        kwargs["transaction_type"] = "BUY"
        return self.place_equity_order(**kwargs)

    def place_sell(self, **kwargs: Any) -> Dict[str, Any]:
        kwargs["transaction_type"] = "SELL"
        return self.place_equity_order(**kwargs)

    def get_order_details(self, order_id: str) -> Dict[str, Any]:
        return self._request(
            "GET",
            f"{BASE_URL}/order/details",
            params={"order_id": order_id},
        )

    def get_order_history(self) -> Dict[str, Any]:
        return self._request(
            "GET",
            f"{BASE_URL}/order/retrieve-all",
        )

    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        return self._request(
            "DELETE",
            f"{BASE_URL}/order/cancel",
            params={"order_id": order_id},
        )

    def get_available_margin(self, segment: str = "SEC") -> float:
        payload = None
        errors: list[str] = []
        for url, params in (
            (FUNDS_URL_V3, None),
            (FUNDS_URL, {"segment": segment}),
        ):
            try:
                payload = self._request(
                    "GET",
                    url,
                    params=params,
                )
                break
            except UpstoxOrderError as exc:
                errors.append(str(exc))
                payload = None
                continue

        if payload is None:
            raise UpstoxOrderError(
                "Unable to fetch Upstox funds from either v3 or v2 endpoint. "
                + " | ".join(errors)
            )

        available_margin = _extract_available_margin(payload)
        if available_margin is None:
            raise UpstoxOrderError(
                f"Unable to determine available funds from Upstox response: {payload}"
            )
        return available_margin

    def ensure_buy_funds_available(
        self,
        symbol: str,
        qty: int,
        ltp: float,
        segment: str = "SEC",
        safety_buffer_pct: float = 0.0,
    ) -> tuple[float, float]:
        estimated_order_value = float(qty) * float(ltp)
        available_margin = self.get_available_margin(segment=segment)
        required_margin = estimated_order_value * (
            1.0 + max(0.0, safety_buffer_pct) / 100.0
        )
        if available_margin < required_margin:
            raise UpstoxOrderError(
                f"Insufficient Upstox funds for BUY {symbol}: "
                f"required {required_margin:.2f}, available {available_margin:.2f}."
            )
        return available_margin, estimated_order_value

    def get_positions(self) -> list[Dict[str, Any]]:
        payload = self._request(
            "GET",
            f"{BASE_URL}/portfolio/short-term-positions",
        )
        return payload.get("data", [])

    def get_holdings(self) -> list[Dict[str, Any]]:
        payload = self._request(
            "GET",
            f"{BASE_URL}/portfolio/long-term-holdings",
        )
        return payload.get("data", [])

    def convert_position(
        self,
        symbol: str,
        exchange: str,
        quantity: int,
        old_product: str,
        new_product: str,
        transaction_type: str = "BUY",
    ) -> Dict[str, Any]:
        instrument = self.resolve_instrument(symbol, exchange)
        payload = {
            "instrument_token": instrument["instrument_key"],
            "old_product": old_product.upper(),
            "new_product": new_product.upper(),
            "transaction_type": transaction_type.upper(),
            "quantity": int(quantity),
        }
        if not self.allow_live_orders:
            print("\nLIVE POSITION CONVERSION BLOCKED")
            print(
                "Set UPSTOX_ALLOW_LIVE_ORDERS=true only when you are "
                "ready to submit real position conversion requests."
            )
            print("\nResolved instrument:")
            print(f"  Symbol          : {instrument.get('trading_symbol')}")
            print(f"  Instrument key  : {instrument['instrument_key']}")
            print("\nConvert payload:")
            print(payload)
            return {
                "status": "DRY_RUN",
                "instrument": instrument,
                "conversion": payload,
            }

        return self._request(
            "PUT",
            CONVERT_POSITION_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
        )

    def resolve_live_sell_inventory(
        self,
        symbol: str,
        exchange: str,
        required_qty: int,
        preferred_product: str | None = None,
        trade_dt: datetime | None = None,
    ) -> tuple[str, int, str]:
        positions = self.get_positions()
        holdings = self.get_holdings()
        product_filter = _normalize_text(preferred_product) or None
        same_day_trade = trade_dt is not None and trade_dt.date() == datetime.now().date()

        positions_qty = _available_positions_quantity(
            positions,
            symbol,
            exchange,
            product=product_filter,
        )
        holdings_qty = _available_holdings_quantity(
            holdings,
            symbol,
            exchange,
            product=product_filter,
        )

        if product_filter == "I":
            if positions_qty >= required_qty:
                return "I", positions_qty, "positions"
            raise UpstoxOrderError(
                f"Insufficient live intraday positions for {symbol} on {exchange}. "
                f"Required {required_qty}, positions available {positions_qty}."
            )

        if product_filter == "D":
            if same_day_trade:
                if positions_qty >= required_qty:
                    return "D", positions_qty, "positions"
                raise UpstoxOrderError(
                    f"Insufficient live same-day positions for {symbol} on {exchange}. "
                    f"Required {required_qty}, positions available {positions_qty}."
                )
            if holdings_qty >= required_qty:
                return "D", holdings_qty, "holdings"
            raise UpstoxOrderError(
                f"Insufficient live delivery holdings for {symbol} on {exchange}. "
                f"Required {required_qty}, holdings available {holdings_qty}."
            )

        if same_day_trade:
            if positions_qty >= required_qty:
                return "I", positions_qty, "positions"
            raise UpstoxOrderError(
                f"Insufficient live same-day positions for {symbol} on {exchange}. "
                f"Required {required_qty}, positions available {positions_qty}."
            )

        if positions_qty >= required_qty:
            return "I", positions_qty, "positions"

        if holdings_qty >= required_qty:
            return "D", holdings_qty, "holdings"

        raise UpstoxOrderError(
            f"Insufficient live Upstox inventory for {symbol} on {exchange}. "
            f"Required {required_qty}, positions available {positions_qty}, "
            f"holdings available {holdings_qty}."
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Upstox NSE/BSE equity order manager"
    )

    parser.add_argument(
        "--symbol",
        required=True,
        help="Trading symbol, e.g. RELIANCE",
    )
    parser.add_argument(
        "--side",
        required=True,
        choices=["BUY", "SELL"],
    )
    parser.add_argument(
        "--order-type",
        default="MARKET",
        choices=["MARKET", "LIMIT", "SL", "SL-M"],
    )
    parser.add_argument(
        "--ltp",
        required=True,
        type=float,
        help="Current LTP used to size the order.",
    )
    parser.add_argument(
        "--product",
        default="D",
        choices=["D", "I"],
        help="D=Delivery, I=Intraday",
    )
    parser.add_argument(
        "--price",
        default=0.0,
        type=float,
    )
    parser.add_argument(
        "--trigger-price",
        default=0.0,
        type=float,
    )
    parser.add_argument(
        "--amo",
        action="store_true",
        help="Place as After Market Order",
    )
    parser.add_argument(
        "--exchange",
        default="NSE",
        choices=["NSE", "BSE"],
    )
    parser.add_argument(
        "--tag",
        default="PYTHON",
    )
    parser.add_argument(
        "--signal-id",
        type=int,
        default=None,
        help="Optional rsi_live_signal_log_trading row id to update with qty.",
    )
    parser.add_argument(
        "--basket-buy-ids",
        default="",
        help="Comma-separated BUY signal ids to combine into one SELL order.",
    )
    parser.add_argument(
        "--db-path",
        default=str(DB_PATH),
        help="Path to quant_historic_data.db used for qty updates.",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="Allow real order submission for this invocation",
    )
    parser.add_argument(
        "--confirm-order",
        action="store_true",
        help="Prompt for Y/N after validation checks and before placing the order.",
    )

    args = parser.parse_args()

    if args.live:
        os.environ["UPSTOX_ALLOW_LIVE_ORDERS"] = "true"

    manager = UpstoxOrderManager()
    db_path = Path(args.db_path)
    qty: int
    order_value: float
    used_today = 0.0
    linked_buy_signal_id: int | None = None
    resolved_sell_product: str | None = None
    basket_buy_ids: list[int] = []

    try:
        available_margin = manager.get_available_margin()
    except Exception as exc:
        raise UpstoxOrderError(f"Unable to read Upstox available funds: {exc}") from exc

    print(f"Available Upstox funds: {available_margin:.2f}")

    if args.side == "BUY":
        if is_today_rejected(args.symbol):
            print(f"BUY {args.symbol} skipped: symbol declined earlier today.")
            return
        if available_margin <= 0:
            print("No available funds in Upstox")
            return
        try:
            qty, order_value, used_today = validate_trade_amount(
                args.symbol,
                args.ltp,
                available_margin=available_margin,
            )
            available_margin, live_order_value = manager.ensure_buy_funds_available(
                args.symbol,
                qty,
                args.ltp,
                segment="SEC",
                safety_buffer_pct=BUY_FUNDS_BUFFER_PCT,
            )
            print(f"Daily used so far: {used_today:.2f}")
            print(f"Remaining today: {max(0.0, DAILY_LIMIT - used_today):.2f}")
            print(f"Calculated qty: {qty}")
            print(f"Proposed value: {live_order_value:.2f}")
            print(f"Available Upstox funds: {available_margin:.2f}")
            print(f"Configured PER_TRADE_VALUE: {PER_TRADE_VALUE:.2f}")
            print(f"Configured DAILY_LIMIT: {DAILY_LIMIT:.2f}")
        except ValueError as exc:
            print(f"BUY {args.symbol} skipped: {exc}")
            return
        except UpstoxOrderError as exc:
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
                qty, basket_buy_ids, linked_buy_product, basket_trade_dt = resolve_basket_sell_quantity(
                    basket_buy_ids,
                    db_path=db_path,
                )
                linked_buy_signal_id = basket_buy_ids[0]
                linked_buy_row = fetch_signal_row(linked_buy_signal_id, db_path=db_path)
                basket_buy_rows = fetch_signal_rows(basket_buy_ids, db_path=db_path)
                today_key = datetime.now().date()
                should_convert_same_day = (
                    linked_buy_product == "D"
                    and basket_trade_dt is not None
                    and basket_trade_dt.date() == today_key
                )

                if should_convert_same_day and manager.allow_live_orders:
                    positions = manager.get_positions()
                    live_same_day_qty = _available_positions_quantity(
                        positions,
                        args.symbol,
                        args.exchange,
                        product="D",
                    )
                    if live_same_day_qty <= 0:
                        raise UpstoxOrderError(
                            f"Insufficient live same-day positions for {args.symbol} on {args.exchange}. "
                            f"Required {qty}, positions available {live_same_day_qty}."
                        )
                    if live_same_day_qty < qty:
                        print(
                            f"Same-day SELL quantity {qty} exceeds live same-day positions "
                            f"{live_same_day_qty}; using available quantity."
                        )
                        qty = live_same_day_qty

                    try:
                        conversion_result = manager.convert_position(
                            args.symbol,
                            args.exchange,
                            qty,
                            old_product="D",
                            new_product="I",
                            transaction_type="BUY",
                        )
                        print(
                            f"Converted same-day delivery position to intraday for {args.symbol} "
                            f"before SELL."
                        )
                        if conversion_result.get("status") != "DRY_RUN":
                            print(f"Conversion response: {conversion_result}")
                    except UpstoxOrderError as exc:
                        print(
                            f"SELL {args.symbol} skipped: unable to convert same-day "
                            f"delivery position to intraday: {exc}"
                        )
                        return

                    resolved_sell_product = "I"
                    live_available_qty = qty
                    inventory_source = "converted"
                else:
                    resolved_sell_product, live_available_qty, inventory_source = manager.resolve_live_sell_inventory(
                        args.symbol,
                        args.exchange,
                        qty,
                        preferred_product=linked_buy_product,
                        trade_dt=basket_trade_dt,
                    )
                order_value = float(qty) * float(args.ltp)
                print(
                    f"Resolved SELL qty from BASKET buy_ids={basket_buy_ids}: {qty}"
                )
                print(
                    f"Live Upstox {inventory_source} quantity for {args.symbol} on {args.exchange}: "
                    f"{live_available_qty}"
                )
                if linked_buy_product:
                    print(f"Basket BUY product: {linked_buy_product}")
                print(f"Resolved SELL product: {resolved_sell_product}")
                print(f"SELL order value: {order_value:.2f}")
            except ValueError as exc:
                print(f"SELL {args.symbol} skipped: {exc}")
                return
            except UpstoxOrderError as exc:
                print(f"SELL {args.symbol} skipped: {exc}")
                return
        else:
            if args.signal_id is None:
                print("SELL orders require --signal-id so the linked BUY quantity can be resolved.")
                return

            try:
                qty, linked_buy_signal_id, linked_buy_product = resolve_sell_quantity(
                    args.signal_id,
                    db_path=db_path,
                )
                linked_buy_row = fetch_signal_row(linked_buy_signal_id, db_path=db_path)
                linked_buy_product = str(linked_buy_product or "").upper() or None
                linked_buy_dt = _parse_signal_date((linked_buy_row or {}).get("signal_timestamp"))
                if linked_buy_dt is None:
                    linked_buy_dt = _parse_signal_date((linked_buy_row or {}).get("action_timestamp"))
                if linked_buy_dt is None:
                    linked_buy_dt = _parse_signal_date((linked_buy_row or {}).get("signal_date"))
                today_key = datetime.now().date()
                should_convert_same_day = (
                    linked_buy_product == "D"
                    and linked_buy_dt is not None
                    and linked_buy_dt.date() == today_key
                )

                if should_convert_same_day and manager.allow_live_orders:
                    positions = manager.get_positions()
                    live_same_day_qty = _available_positions_quantity(
                        positions,
                        args.symbol,
                        args.exchange,
                        product="D",
                    )
                    if live_same_day_qty <= 0:
                        raise UpstoxOrderError(
                            f"Insufficient live same-day positions for {args.symbol} on {args.exchange}. "
                            f"Required {qty}, positions available {live_same_day_qty}."
                        )
                    if live_same_day_qty < qty:
                        print(
                            f"Same-day SELL quantity {qty} exceeds live same-day positions "
                            f"{live_same_day_qty}; using available quantity."
                        )
                        qty = live_same_day_qty

                    try:
                        conversion_result = manager.convert_position(
                            args.symbol,
                            args.exchange,
                            qty,
                            old_product="D",
                            new_product="I",
                            transaction_type="BUY",
                        )
                        print(
                            f"Converted same-day delivery position to intraday for {args.symbol} "
                            f"before SELL."
                        )
                        if conversion_result.get("status") != "DRY_RUN":
                            print(f"Conversion response: {conversion_result}")
                    except UpstoxOrderError as exc:
                        print(
                            f"SELL {args.symbol} skipped: unable to convert same-day "
                            f"delivery position to intraday: {exc}"
                        )
                        return

                    resolved_sell_product = "I"
                    live_available_qty = qty
                    inventory_source = "converted"
                else:
                    resolved_sell_product, live_available_qty, inventory_source = manager.resolve_live_sell_inventory(
                        args.symbol,
                        args.exchange,
                        qty,
                        preferred_product=linked_buy_product,
                        trade_dt=linked_buy_dt,
                    )
                order_value = float(qty) * float(args.ltp)
                print(
                    f"Resolved SELL qty from BUY signal_id={linked_buy_signal_id}: {qty}"
                )
                print(
                    f"Live Upstox {inventory_source} quantity for {args.symbol} on {args.exchange}: "
                    f"{live_available_qty}"
                )
                if linked_buy_product:
                    print(f"Linked BUY product: {linked_buy_product}")
                print(f"Resolved SELL product: {resolved_sell_product}")
                print(f"SELL order value: {order_value:.2f}")
            except ValueError as exc:
                print(f"SELL {args.symbol} skipped: {exc}")
                return
            except UpstoxOrderError as exc:
                print(f"SELL {args.symbol} skipped: {exc}")
                return

    if args.confirm_order:
        try:
            if args.side == "BUY":
                prompt = (
                    f"Confirm Upstox BUY order for {args.symbol} | "
                    f"qty={qty} | order_value={order_value:.2f} | "
                    f"available_funds={available_margin:.2f} | "
                    f"remaining_daily_limit={max(0.0, DAILY_LIMIT - used_today):.2f} ? [Y/N]: "
                )
            else:
                prompt = (
                    f"Confirm Upstox SELL order for {args.symbol} | "
                    f"qty={qty} | order_value={order_value:.2f} | "
                    f"linked_buy_signal_id={linked_buy_signal_id} | "
                    f"live_inventory={resolved_sell_product or args.product} ? [Y/N]: "
                )
            while True:
                print(prompt, end="", flush=True)
                response = input().strip().upper()
                if response in {"Y", "N"}:
                    break
                print("Please type Y or N.")
        except EOFError:
            print(
                f"Skipping {args.side} {args.symbol}: confirmation input unavailable."
            )
            return

        if response == "N":
            print(f"Skipping {args.side} {args.symbol}: user declined confirmation.")
            if args.side == "BUY":
                add_today_reject(args.symbol)
            return

    kwargs = {
        "symbol": args.symbol,
        "quantity": qty,
        "order_type": args.order_type,
        "product": args.product,
        "price": args.price,
        "trigger_price": args.trigger_price,
        "is_amo": args.amo,
        "exchange": args.exchange,
        "tag": args.tag,
    }

    if args.side == "BUY":
        result = manager.place_buy(**kwargs)
    else:
        kwargs["product"] = resolved_sell_product or args.product
        result = manager.place_sell(**kwargs)

    print("\nUpstox response:")
    print(result)

    if result.get("status") == "DRY_RUN":
        return

    order_submitted = bool(result) and result.get("status") != "DRY_RUN"

    if args.side == "BUY" and order_submitted:
        update_daily_usage(order_value)


if __name__ == "__main__":
    main()

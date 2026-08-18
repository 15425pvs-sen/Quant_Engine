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
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import requests

from upstox_auth import get_valid_access_token


BASE_URL = "https://api.upstox.com/v2"
FUNDS_URL = f"{BASE_URL}/user/get-funds-and-margin"
FUNDS_URL_V3 = "https://api.upstox.com/v3/user/get-funds-and-margin"
ORDER_URL_V3 = "https://api-hft.upstox.com/v3/order/place"
DEFAULT_TIMEOUT = 30
DB_PATH = Path(__file__).resolve().parent / "quant_historic_data.db"
SIGNAL_LOG_TABLE = "rsi_live_signal_log_trading"
DAILY_USAGE_FILE = Path(__file__).resolve().parent / "upstox_daily_usage.json"
PER_TRADE_VALUE = 1500.0
DAILY_LIMIT = 10000.0
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
    used_value = float(data.get("used_value", 0.0) or 0.0)
    return usage_date, used_value


def save_today_usage(date_str: str, used_value: float) -> None:
    payload = {
        "date": date_str,
        "used_value": round(float(used_value), 2),
    }
    with DAILY_USAGE_FILE.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def estimate_trade_quantity(ltp: float, per_trade_value: float) -> tuple[int, float]:
    if ltp <= 0 or per_trade_value <= 0:
        return 0, 0.0

    qty = int(per_trade_value // ltp)
    if qty <= 0:
        return 0, 0.0

    return qty, float(qty * ltp)


def validate_trade_amount(symbol: str, ltp: float) -> tuple[int, float, float]:
    if ltp <= 0:
        raise ValueError("ltp must be greater than zero.")
    if PER_TRADE_VALUE <= 0:
        raise ValueError("PER_TRADE_VALUE must be greater than zero.")
    if DAILY_LIMIT <= 0:
        raise ValueError("DAILY_LIMIT must be greater than zero.")

    qty, order_value = estimate_trade_quantity(ltp, PER_TRADE_VALUE)
    if qty <= 0:
        raise ValueError(
            f"Order quantity came out to zero for {symbol} at LTP {ltp:.2f} "
            f"and PER_TRADE_VALUE {PER_TRADE_VALUE:.2f}."
        )

    usage_date, used_today = get_today_usage()
    today = _today_key()
    if usage_date != today:
        used_today = 0.0

    remaining_today = max(0.0, DAILY_LIMIT - used_today)
    if order_value > remaining_today:
        raise ValueError(
            f"This trade would exceed today's remaining allocation. "
            f"Order value {order_value:.2f} > remaining {remaining_today:.2f}."
        )

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

    try:
        with sqlite3.connect(db_path) as conn:
            ensure_signal_qty_column(conn)
            cursor = conn.execute(
                f"UPDATE {SIGNAL_LOG_TABLE} SET qty = ?, product = ? WHERE id = ?",
                (int(qty), product, int(signal_id)),
            )
            conn.commit()
            if cursor.rowcount <= 0:
                print(
                    f"Warning: execution detail update affected no rows for "
                    f"signal_id={signal_id}."
                )
                return False
            return True
    except sqlite3.Error as exc:
        print(
            f"Warning: failed to update execution details for "
            f"signal_id={signal_id}: {exc}"
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
                SELECT id, source_table, signal_type, qty, product, buy_signal_id, position_state
                FROM {SIGNAL_LOG_TABLE}
                WHERE id = ?
                """,
                (int(signal_id),),
            ).fetchone()
            return dict(row) if row is not None else None
    except sqlite3.Error as exc:
        print(f"Warning: failed to fetch signal row {signal_id}: {exc}")
        return None


def resolve_sell_quantity(
    sell_signal_id: int,
    db_path: Path = DB_PATH,
) -> tuple[int, int, str | None]:
    sell_row = fetch_signal_row(sell_signal_id, db_path=db_path)
    if not sell_row:
        raise ValueError(
            f"SELL signal row {sell_signal_id} not found in {db_path}."
        )

    if str(sell_row.get("signal_type", "")).upper() != "SELL":
        raise ValueError(
            f"Signal row {sell_signal_id} is not a SELL row."
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


def _normalize_text(value: Any) -> str:
    return str(value or "").strip().upper()


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

    def resolve_live_sell_inventory(
        self,
        symbol: str,
        exchange: str,
        required_qty: int,
        preferred_product: str | None = None,
    ) -> tuple[str, int, str]:
        positions = self.get_positions()
        holdings = self.get_holdings()
        product_filter = _normalize_text(preferred_product) or None

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
            if holdings_qty >= required_qty:
                return "D", holdings_qty, "holdings"
            raise UpstoxOrderError(
                f"Insufficient live delivery holdings for {symbol} on {exchange}. "
                f"Required {required_qty}, holdings available {holdings_qty}."
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
        "--db-path",
        default=str(DB_PATH),
        help="Path to quant_historic_data.db used for qty updates.",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="Allow real order submission for this invocation",
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

    try:
        available_margin = manager.get_available_margin()
    except Exception as exc:
        raise UpstoxOrderError(f"Unable to read Upstox available funds: {exc}") from exc

    print(f"Available Upstox funds: {available_margin:.2f}")
    if available_margin < PER_TRADE_VALUE:
        print("Funds are lower than Per trade value")
        return

    if args.side == "BUY":
        try:
            qty, order_value, used_today = validate_trade_amount(args.symbol, args.ltp)
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
        if args.signal_id is None:
            print("SELL orders require --signal-id so the linked BUY quantity can be resolved.")
            return

        try:
            qty, linked_buy_signal_id, linked_buy_product = resolve_sell_quantity(
                args.signal_id,
                db_path=db_path,
            )
            resolved_sell_product, live_available_qty, inventory_source = manager.resolve_live_sell_inventory(
                args.symbol,
                args.exchange,
                qty,
                preferred_product=linked_buy_product,
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

    if args.side == "BUY":
        update_daily_usage(order_value)

    if args.signal_id is not None:
        if update_signal_execution_details(
            args.signal_id,
            qty,
            resolved_sell_product if args.side == "SELL" else args.product,
            db_path=db_path,
        ):
            print(
                f"Updated {SIGNAL_LOG_TABLE} for signal_id={args.signal_id} "
                f"with qty={qty}."
            )


if __name__ == "__main__":
    main()

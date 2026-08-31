# README_ZERODHA

This file documents the Zerodha order-execution features added in this workspace.

## What is implemented

- Live BUY order execution through Kite Connect.
- Live SELL order execution through Kite Connect.
- Single-signal SELL support using a linked `signal_id`.
- Basket SELL support using multiple open BUY ids.
- Basket SELL quantity resolution as one combined order quantity.
- Basket SELL grouping by product type so mixed-product baskets are rejected.
- Basket SELL validation against `rsi_live_signal_log_trading`.
- Basket SELL restriction to one `source_table` only.
- Live inventory checks using Kite `holdings()` and `positions()`.
- Order confirmation prompt before submission.
- Dry-run mode.
- Daily BUY spend tracking.
- AMO support.
- MARKET, LIMIT, SL, and SL-M order types.
- Optional `tag`, `validity`, `market_protection`, and `autoslice` order fields.

## Execution flow

### Single BUY

- The script evaluates the requested trade size from `ltp` and the configured per-trade value.
- It applies daily spend tracking.
- It can prompt for confirmation before submission.

### Single SELL

- The script first resolves the linked BUY row from `rsi_live_signal_log_trading`.
- It uses the BUY row quantity for the SELL order quantity.
- It checks live Zerodha inventory before submission.
- It refuses to sell if the referenced row is missing or invalid.

### Basket SELL

- Basket SELLs are built only from BUY rows listed in `--basket-buy-ids`.
- Basket SELLs must all belong to the same `source_table`.
- Basket SELLs must all use the same product type.
- Basket SELLs submit one combined SELL order for the full basket quantity.
- Basket SELLs can be used to reduce repeated AMC-style execution costs.

## Zerodha order manager features

- BUY and SELL order placement.
- MARKET, LIMIT, SL, and SL-M order types.
- Regular and AMO varieties.
- `CNC`, `MIS`, and `NRML` product support.
- Order confirmation prompt before submission.
- Live order gating through `ZERODHA_ALLOW_LIVE_ORDERS`.
- Qty resolution from:
  - single BUY signal id
  - basket BUY id list
- Live inventory checks before SELL submission.
- Signal log lookups from `rsi_live_signal_log_trading`.

## Signal log fields used

- `source_table`
- `signal_type`
- `entry_rsi`
- `exit_rsi`
- `current_rsi`
- `qty`
- `product`
- `signal_date`
- `signal_timestamp`
- `notes`
- `position_state`
- `buy_signal_id`
- `action_timestamp`
- `closed_by_signal_id`
- `basket_buy_ids`

## Files involved

- [zerodha_order_manager.py](./zerodha_order_manager.py)
- [zerodha_kite.py](./zerodha_kite.py)
- [zerodha_auth.py](./zerodha_auth.py)
- [README_UPSTOX.md](./README_UPSTOX.md)

## Notes

- Signal products `D` and `I` are normalized to Zerodha product types:
  - `D` -> `CNC`
  - `I` -> `MIS`
- Basket SELLs are intentionally strict.
- Mixed-product baskets are rejected.
- Mixed-source-table baskets are rejected.
- The manager uses Kite holdings and positions as the live inventory source of truth.

## How to run

### Single SELL from one BUY signal

```powershell
py zerodha_order_manager.py --side SELL --symbol ASHOKLEY --signal-id 141 --ltp 179.05 --live
```

### Basket SELL from multiple BUY signal ids

```powershell
py zerodha_order_manager.py --side SELL --symbol ASHOKLEY --basket-buy-ids 104,141 --ltp 179.05 --live
```

### Single BUY example

```powershell
py zerodha_order_manager.py --side BUY --symbol ICICIBANK --ltp 1416.40 --live
```

### Dry-run

```powershell
py zerodha_order_manager.py --side SELL --symbol ICICIBANK --signal-id 140 --ltp 1500.00 --dry-run
```

## Environment variables

- `ZERODHA_ALLOW_LIVE_ORDERS`:
  - Set to `true` to allow real order submission.
  - Leave unset or `false` to keep executions blocked.
- `ZERODHA_API_KEY`:
  - Used by `zerodha_auth.py` when generating the login URL.
- `ZERODHA_API_SECRET`:
  - Used by `zerodha_auth.py` when generating the session token.

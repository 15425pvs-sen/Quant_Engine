# README_UPSTOX

This file documents the Upstox order-execution features currently implemented in this workspace.

## What is implemented

- Live BUY order execution from RSI signals.
- Live SELL order execution from RSI signals.
- Single-signal SELL support using a linked `buy_signal_id`.
- Basket SELL support using multiple open BUY ids for the same `source_table`.
- Basket SELL quantity resolution as one combined order quantity.
- Basket SELL splitting by product type so `D` and `I` are handled separately.
- Basket SELL exclusion of same-day BUY rows.
- Basket SELL selection based on row-level `current_rsi > exit_rsi`.
- Configurable basket RSI threshold constant at the top of `live_rsi_tracking.py`.
- Explicit console logging when a basket SELL is taking place.
- Explicit logging of basket BUY ids, weighted average buy price, and final PnL.
- Explicit logging of BUY ids excluded from basket creation because they were bought today.

## Execution flow

### Single BUY

- The script evaluates the RSI bucket rules.
- It skips duplicates already open in the signal log.
- It applies optional protections such as:
  - hybrid confirmation
  - BUY RSI protection
  - fund / trade sizing checks in the order manager

### Single SELL

- The script first checks for open BUY rows for the same stock.
- It attempts to close matching open positions when RSI reaches the exit level.
- It skips SELLs when:
  - live LTP is not above the BUY price
  - profit percentage is below the configured minimum
  - PnL does not clear the AMC threshold

### Basket SELL

- Basket SELLs are built only from open BUY rows for the same `source_table`.
- Basket SELLs are grouped by product type.
- Basket SELLs only include BUY rows whose live RSI has crossed that row’s `exit_rsi`.
- Basket SELLs exclude rows bought on the current date.
- Basket SELLs submit one Upstox SELL order for the combined quantity.
- Basket SELLs close all included BUY ids together after the order is accepted.

## Upstox order manager features

- BUY and SELL order placement.
- MARKET, LIMIT, SL, and SL-M order types.
- Delivery (`D`) and Intraday (`I`) products.
- AMO support.
- Order confirmation prompt before submission.
- Live order gating through `UPSTOX_ALLOW_LIVE_ORDERS`.
- Qty resolution from:
  - single BUY signal id
  - basket BUY id list
- Live inventory checks before SELL submission.
- Same-day delivery-to-intraday conversion support when required.
- Signal log updates for resolved qty and product.

## Signal log fields used

- `source_table`
- `signal_type`
- `entry_rsi`
- `exit_rsi`
- `previous_rsi`
- `current_rsi`
- `ltp`
- `qty`
- `product`
- `signal_date`
- `signal_timestamp`
- `notes`
- `position_state`
- `buy_signal_id`
- `trigger_exit_rsi`
- `action_timestamp`
- `closed_by_signal_id`
- `basket_buy_ids`

## Files involved

- [live_rsi_tracking.py](./live_rsi_tracking.py)
- [upstox_order_manager.py](./upstox_order_manager.py)
- [quant_engine.py](./quant_engine.py)

## Notes

- Basket SELLs are intentionally strict.
- Mixed-product baskets are not allowed.
- Same-day BUY rows are excluded from basket formation.
- The basket threshold constant can be changed in `live_rsi_tracking.py`, but basket formation is currently driven by row-level `exit_rsi` crossings.

## Basket SELL rules

- Basket SELLs are created only within the same `source_table`.
- Basket SELLs are split by product type, so `D` and `I` positions are never mixed.
- A BUY id is eligible for basketing only if:
  - it is still open,
  - it was not bought today,
  - `current_rsi > that row's exit_rsi`.
- Basket SELLs require at least two eligible BUY ids in the same product bucket.
- Basket SELLs use one weighted-average cost across the included BUY ids.
- Basket SELLs close all included BUY ids together after the Upstox order is accepted.

### Example

If `ICICIBANK` has two open BUY rows:

- `buy_id=140` with `exit_rsi=51`
- `buy_id=141` with `exit_rsi=55`

then a live `current_rsi=56` makes both rows eligible for the same basket SELL, because `56 > 51` and `56 > 55`.

## How to run

### Live tracker

```powershell
py live_rsi_tracking.py
py live_rsi_tracking.py --hybrid
py live_rsi_tracking.py --hybrid --telegram --confirmOrder --buy-rsi-protection 1.0
py live_rsi_tracking.py --broker zerodha --hybrid --telegram --confirmOrder
```

### Signal-gap checker

```powershell
py check_rsi_sell_gap.py
py check_rsi_sell_gap.py --symbols ICICIBANK ASHOKLEY
```

### Quant engine

```powershell
py quant_engine.py
py quant_engine.py --rerun-today
py quant_engine.py TCS RELIANCE --force-heatmap
```

### Upstox order manager

Single SELL from one BUY signal:

```powershell
py upstox_order_manager.py --side SELL --symbol ASHOKLEY --signal-id 141 --ltp 179.05 --live
```

Basket SELL from multiple BUY signal ids:

```powershell
py upstox_order_manager.py --side SELL --symbol ASHOKLEY --basket-buy-ids 104,141 --ltp 179.05 --live
```

Single BUY example:

```powershell
py upstox_order_manager.py --side BUY --symbol ICICIBANK --ltp 1416.40 --live
```

## Environment variables

- `UPSTOX_ALLOW_LIVE_ORDERS`:
  - Set to `true` to allow real order submission.
  - Leave unset or `false` to keep executions blocked.
- `ORDER_EXECUTION_BROKER`:
  - Set to `upstox` or `zerodha` to choose the broker used by `live_rsi_tracking.py`.
- `UPSTOX_ACCESS_TOKEN`:
  - Optional live Upstox access token used by the order manager.
  - If omitted, the auth helper is used to obtain one.
- `TELEGRAM_BOT_TOKEN`:
  - Bot token for Telegram alerts from the tracker.
- `TELEGRAM_CHAT_ID`:
  - Chat id for Telegram alerts from the tracker.

import sqlite3
import pandas as pd
import numpy as np

DB_FILE = "quant_historic_data.db"      # SQLite database
TABLE = "BAJFINANCE"


# -----------------------------------------
# RSI Calculation
# -----------------------------------------
def calculate_rsi(df, period=14):

    delta = df['close'].diff()

    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(alpha=1/period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False).mean()

    rs = avg_gain / avg_loss

    df["RSI"] = 100 - (100 / (1 + rs))

    return df


# -----------------------------------------
# EMA
# -----------------------------------------
def calculate_ema(df):

    df["EMA20"] = df["close"].ewm(span=20).mean()

    return df


# -----------------------------------------
# Volume Average
# -----------------------------------------
def calculate_volume(df):

    df["VOL20"] = df["volume"].rolling(20).mean()

    return df


# -----------------------------------------
# ATR
# -----------------------------------------
def calculate_atr(df, period=14):

    high_low = df["high"] - df["low"]

    high_close = np.abs(df["high"] - df["close"].shift())

    low_close = np.abs(df["low"] - df["close"].shift())

    ranges = pd.concat(
        [high_low, high_close, low_close],
        axis=1
    )

    true_range = ranges.max(axis=1)

    df["ATR"] = true_range.rolling(period).mean()

    return df


# -----------------------------------------
# BUY / SELL
# -----------------------------------------
def generate_signal(df):

    signals = []

    position = False

    stoploss = 0

    for i in range(20, len(df)):

        signal = ""

        close = df.iloc[i]["close"]
        ema = df.iloc[i]["EMA20"]
        rsi = df.iloc[i]["RSI"]
        atr = df.iloc[i]["ATR"]

        prev_rsi = df.iloc[i-1]["RSI"]

        prev_high = df.iloc[i-1]["high"]

        vol = df.iloc[i]["volume"]

        avg_vol = df.iloc[i]["VOL20"]

        # BUY
        if not position:

            if (
                close > ema and
                prev_rsi < 45 and
                rsi > 45 and
                close > prev_high and
                vol > avg_vol * 1.5
            ):

                signal = "BUY"

                position = True

                stoploss = close - atr


        # SELL
        else:

            if (
                close < ema or
                (prev_rsi > 70 and rsi < prev_rsi) or
                close < stoploss
            ):

                signal = "SELL"

                position = False


            else:

                stoploss = max(stoploss,
                               close - atr)


        signals.append(signal)

    df = df.iloc[20:].copy()

    df["Signal"] = signals

    return df


# -----------------------------------------
# MAIN
# -----------------------------------------
conn = sqlite3.connect(DB_FILE)

df = pd.read_sql(
    f"SELECT * FROM {TABLE} ORDER BY trade_date",
    conn
)

conn.close()

df = calculate_rsi(df)
df = calculate_ema(df)
df = calculate_volume(df)
df = calculate_atr(df)

df = generate_signal(df)

print(df[df["Signal"] != ""][[
    "trade_date",
    "close",
    "EMA20",
    "RSI",
    "Signal"
]])
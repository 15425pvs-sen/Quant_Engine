import streamlit as st
import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

DB_NAME = "quant_historic_data.db"

# ─────────────────────────────────────────
# LOAD DATA
# ─────────────────────────────────────────
def load_data(table):
    conn = sqlite3.connect(DB_NAME)

    df = pd.read_sql(
        f"SELECT trade_date, close, rsi FROM {table} ORDER BY trade_date",
        conn
    )
    conn.close()

    # Convert date
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")

    # 🔥 FIX: Convert numeric columns properly
    for col in ["close", "rsi"]:
        df[col] = pd.to_numeric(
            df[col].astype(str)
                   .str.replace(",", "", regex=False)
                   .str.strip(),
            errors="coerce"
        )

    # Drop bad rows
    df = df.dropna(subset=["trade_date", "close", "rsi"]).reset_index(drop=True)

    return df

# ─────────────────────────────────────────
# SIMPLE TRADE SIMULATION
# ─────────────────────────────────────────
def simulate(df, entry, exit, use_sl=True, sl=-0.12):

    closes = df["close"].values
    rsis   = df["rsi"].values

    n = len(df)
    trades = []

    i = 1
    while i < n:

        if rsis[i-1] < entry <= rsis[i]:
            ep = closes[i]
            j = i + 1

            while j < n:
                ret = (closes[j] / ep - 1) * 100

                if rsis[j] >= exit or (use_sl and ret <= sl * 100):
                    trades.append(ret)
                    i = j
                    break

                j += 1
        i += 1

    return trades


# ─────────────────────────────────────────
# STRATEGY GRID
# ─────────────────────────────────────────
def build_strategy_matrix(df):

    results = []

    for entry in range(20, 51, 5):
        for exit in range(entry+5, 81, 5):

            trades = simulate(df, entry, exit)

            if len(trades) < 3:
                continue

            avg_ret = np.mean(trades)

            results.append({
                "entry": entry,
                "exit": exit,
                "avg_return": round(avg_ret, 2),
                "trades": len(trades)   # 👈 IMPORTANT
            })

    return pd.DataFrame(results)
# ─────────────────────────────────────────
# UI
# ─────────────────────────────────────────
st.title("📊 RSI Quant Dashboard")

table = st.text_input("Enter Table Name")

if table:

    df = load_data(table)

    st.write("### Data Preview", df.tail())

    entry = st.slider("RSI Entry", 10, 60, 30)
    exit  = st.slider("RSI Exit", 40, 90, 60)

    if st.button("Run Simulation"):

        trades = simulate(df, entry, exit)

        if trades:
            st.write(f"Trades: {len(trades)}")
            st.write(f"Avg Return: {np.mean(trades):.2f}%")
            st.write(f"Win Rate: {np.mean(np.array(trades)>0)*100:.2f}%")

            fig, ax = plt.subplots()
            ax.hist(trades, bins=20)
            st.pyplot(fig)

    # Strategy Heatmap
    if st.button("Build Strategy Matrix"):

        mat = build_strategy_matrix(df)

        pivot_ret = mat.pivot_table(
            index="entry",
            columns="exit",
            values="avg_return",
            aggfunc="mean"
        )

        pivot_cnt = mat.pivot_table(
            index="entry",
            columns="exit",
            values="trades",
            aggfunc="mean"
        )
        
        annot = pivot_ret.copy().astype(str)

        for i in range(pivot_ret.shape[0]):
            for j in range(pivot_ret.shape[1]):

                val = pivot_ret.iloc[i, j]
                cnt = pivot_cnt.iloc[i, j]

                if pd.notna(val):
                    annot.iloc[i, j] = f"{val:.1f}\n({int(cnt)})"
                else:
                    annot.iloc[i, j] = ""

        fig, ax = plt.subplots(figsize=(10, 6))

        sns.heatmap(
            pivot_ret,
            annot=annot,
            fmt="",
            cmap="coolwarm",
            ax=ax
        )

        st.pyplot(fig)
    
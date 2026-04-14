import sqlite3
import pandas as pd
import subprocess
import os

DB_NAME = "quant_historic_data.db"
CSV_BASE_PATH = r"D:\QuantApplication\Database_Quant\historic_data"
REFERENCE_TABLE = "market_data"

conn = sqlite3.connect(DB_NAME)
cursor = conn.cursor()


# ------------------------------------------------
# CHECK TABLE EXISTS
# ------------------------------------------------
def table_exists(table_name):

    cursor.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table' AND name=?
    """, (table_name,))

    return cursor.fetchone() is not None


# ------------------------------------------------
# CREATE TABLE
# ------------------------------------------------
def create_table_like_reference(new_table):

    cursor.execute(f"""
        CREATE TABLE {new_table} AS
        SELECT * FROM {REFERENCE_TABLE} WHERE 0
    """)

    conn.commit()

def ensure_indexes(table):

    # Unique constraint to prevent duplicate dates
    cursor.execute(f"""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_{table}_unique_date
        ON {table}(trade_date)
    """)

    # Performance index for fast sorting / filtering
    cursor.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_{table}_date
        ON {table}(trade_date)
    """)

    conn.commit()

# ------------------------------------------------
# GET TABLE COLUMNS
# ------------------------------------------------
def get_table_columns(table):

    cursor.execute(f"PRAGMA table_info({table})")

    return [row[1] for row in cursor.fetchall()]


# ------------------------------------------------
# NORMALIZE DATES
# ------------------------------------------------
def normalize_dates(df):

    df["trade_date"] = pd.to_datetime(
        df["trade_date"].str.strip(),
        format="%d-%b-%y",
        errors="coerce"
    ).dt.strftime("%Y-%m-%d")

    return df.dropna(subset=["trade_date"])


# ------------------------------------------------
# INSERT NEW ROWS
# ------------------------------------------------
def insert_new_rows(table, df):

    # Get last date in table
    last_date_query = f"""
        SELECT MAX(trade_date) as last_date
        FROM {table}
    """

    last_date_df = pd.read_sql(last_date_query, conn)

    last_date = last_date_df.iloc[0]["last_date"]

    if last_date is not None:

        last_date = pd.to_datetime(last_date).strftime("%Y-%m-%d")

        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.strftime("%Y-%m-%d")

        # Keep only rows after last DB date
        df = df[df["trade_date"] > last_date]

    if df.empty:
        return 0

    table_cols = get_table_columns(table)

    df = df[[c for c in df.columns if c in table_cols]]

    df.to_sql(table, conn, if_exists="append", index=False)

    return len(df)

# ------------------------------------------------
# MAIN PROCESS
# ------------------------------------------------
if not os.path.exists(CSV_BASE_PATH):
    raise RuntimeError(f"Folder not found: {CSV_BASE_PATH}")

csv_files = [
    f for f in os.listdir(CSV_BASE_PATH)
    if f.lower().endswith(".csv")
]

if not csv_files:
    raise RuntimeError("No CSV files found")

for csv_file in csv_files:

    equity = os.path.splitext(csv_file)[0].upper()
    csv_path = os.path.join(CSV_BASE_PATH, csv_file)

    print(f"\n📌 Processing {equity}")

    if not table_exists(equity):
        print(f"🆕 Creating table {equity}")
        create_table_like_reference(equity)
    else:
        print(f"✅ Table {equity} already exists")

    ensure_indexes(equity)
    df = pd.read_csv(csv_path, dtype=str)

    # Normalize column names
    df.columns = [
        c.strip().lower()
         .replace(" ", "_")
         .replace(".", "")
    for c in df.columns]

    column_map = {
        "date": "trade_date",
        "timestamp": "trade_date",
        "open": "open",
        "high": "high",
        "low": "low",
        "close": "close",
        "prev_close": "prev_close",
        "volume": "volume",
        "value": "value",
        "no_of_trades": "no_of_trades",
        "series": "series"
    }

    df.rename(columns=column_map, inplace=True)

    if "trade_date" not in df.columns:
        print("trade_date column missing — skipping")
        continue

    df = normalize_dates(df)

    df = df.sort_values("trade_date")

    inserted = insert_new_rows(equity, df)

    print(f"➕ Inserted {inserted} rows")

    if inserted == 0:
        print("ℹ️ No new data")
        continue

#    print("📊 Computing RSI...")

#    subprocess.run(
#        ["py", "fill_rsi_backward.py", equity],
#        check=True
#    )

print("\n✅ CSV ingestion complete")

conn.close()
import sqlite3
import pandas as pd
import numpy as np

# ──────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────
DB_NAME = "quant_historic_data.db"

# 🔹 PLACEHOLDER: Provide your table name here
TABLE_NAME = "BAJAJFINANCE"

# 🔹 Hardcoded RSI strategy
RSI_ENTRY = 50
RSI_EXIT  = 70

# Return milestones
TARGET_RETURNS = [5, 10, 15, 20, 30, 40, 50]

# Stop loss (same as original script)
STOP_LOSS_PCT = -0.50


# ──────────────────────────────────────────────────────────
# DATA LOADING (from your script)
# ──────────────────────────────────────────────────────────
def load_equity_data(conn, table):
    df = pd.read_sql(
        f"""
        SELECT trade_date, close, rsi 
        FROM {table}
        WHERE close IS NOT NULL AND rsi IS NOT NULL
        ORDER BY trade_date ASC
        """,
        conn,
    )

    if df.empty:
        print(f"No data found in table {table}")
        return df

    df["trade_date"] = pd.to_datetime(
        df["trade_date"].astype(str).str.strip(), errors="coerce"
    )

    for col in ("close", "rsi"):
        df[col] = pd.to_numeric(
            df[col].astype(str)
                   .str.replace(",", "", regex=False)
                   .str.replace(" ", "", regex=False)
                   .str.strip(),
            errors="coerce",
        )

    df = df.dropna(subset=["trade_date", "close", "rsi"]).reset_index(drop=True)

    print(f"Loaded {len(df)} rows from {table}")
    print(f"Date range: {df['trade_date'].iloc[0].date()} → {df['trade_date'].iloc[-1].date()}")

    return df


# ──────────────────────────────────────────────────────────
# CORE LOGIC: RETURN MILESTONE SIMULATION
# ──────────────────────────────────────────────────────────
def simulate_return_milestones(df, rsi_entry, rsi_exit):

    closes = df["close"].values
    rsis   = df["rsi"].values
    n      = len(df)

    results = {
        t: {"count": 0, "days": []}
        for t in TARGET_RETURNS
    }

    i = 1

    while i < n:

        # ENTRY condition (RSI crossover)
        if rsis[i - 1] < rsi_entry <= rsis[i]:

            entry_price = closes[i]
            j = i + 1

            hit_targets = {t: False for t in TARGET_RETURNS}

            while j < n:

                ret = (closes[j] / entry_price - 1) * 100
                days_taken = j - i

                # Check milestones
                for t in TARGET_RETURNS:
                    if not hit_targets[t] and ret >= t:
                        results[t]["count"] += 1
                        results[t]["days"].append(days_taken)
                        hit_targets[t] = True

                # Exit condition
                if rsis[j] >= rsi_exit or ret <= STOP_LOSS_PCT * 100:
                    i = j
                    break

                j += 1
            else:
                break

        i += 1

    return results


# ──────────────────────────────────────────────────────────
# OUTPUT FORMATTER
# ──────────────────────────────────────────────────────────
def print_milestone_summary(results):

    print("\n================ RETURN MILESTONE SUMMARY ================\n")

    for t in sorted(results.keys()):
        count = results[t]["count"]
        days  = results[t]["days"]

        avg_days = round(sum(days) / len(days), 2) if days else None
        min_days = min(days) if days else None
        max_days = max(days) if days else None

        print(f"{t}% Return:")
        print(f"   Hits       : {count}")
        print(f"   Avg Days   : {avg_days}")
        print(f"   Min Days   : {min_days}")
        print(f"   Max Days   : {max_days}")
        print(f"   All Days   : {days}")
        print()

def find_best_rsi_strategy(df):

    TARGETS = [5, 10, 15, 20]

    closes = df["close"].values
    rsis   = df["rsi"].values
    n      = len(df)

    results = {t: [] for t in TARGETS}

    # 🔹 Try multiple RSI combinations
    for entry in range(20, 51, 5):          # 20,25,30,...50
        for exit in range(entry + 5, 81, 5):  # entry+5 → 80

            i = 1

            # Track results for this combo
            combo_hits = {
                t: {"count": 0, "days": []}
                for t in TARGETS
            }

            while i < n:

                # ENTRY
                if rsis[i - 1] < entry <= rsis[i]:

                    entry_price = closes[i]
                    j = i + 1

                    hit_targets = {t: False for t in TARGETS}

                    while j < n:

                        ret = (closes[j] / entry_price - 1) * 100
                        days_taken = j - i

                        for t in TARGETS:
                            if not hit_targets[t] and ret >= t:
                                combo_hits[t]["count"] += 1
                                combo_hits[t]["days"].append(days_taken)
                                hit_targets[t] = True

                        # Exit condition
                        if rsis[j] >= exit or ret <= STOP_LOSS_PCT * 100:
                            i = j
                            break

                        j += 1
                    else:
                        break

                i += 1

            # Store valid combos
            for t in TARGETS:
                count = combo_hits[t]["count"]
                days  = combo_hits[t]["days"]

                if count >= 3:  # 🔹 minimum reliability filter
                    avg_days = sum(days) / len(days)

                    results[t].append({
                        "entry": entry,
                        "exit": exit,
                        "count": count,
                        "avg_days": round(avg_days, 2)
                    })

    return results

def print_best_strategies(results):

    print("\n================ STRATEGY BUILDER =================\n")

    for target, combos in results.items():

        if not combos:
            print(f"No valid strategies found for {target}% return\n")
            continue

        # Sort by fastest return
        combos = sorted(combos, key=lambda x: x["avg_days"])

        print(f"\n🔥 Best RSI strategies for {target}% return:\n")

        for c in combos[:5]:  # Top 5
            print(
                f"Entry={c['entry']} Exit={c['exit']} | "
                f"Avg Days={c['avg_days']} | "
                f"Hits={c['count']}"
            )

def find_best_rsi_strategy_no_stoploss(df):

    TARGETS = [5, 10, 15, 20]

    closes = df["close"].values
    rsis   = df["rsi"].values
    n      = len(df)

    results = {t: [] for t in TARGETS}

    # 🔹 Try multiple RSI combinations
    for entry in range(20, 51, 5):
        for exit in range(entry + 5, 81, 5):

            i = 1

            combo_hits = {
                t: {"count": 0, "days": []}
                for t in TARGETS
            }

            while i < n:

                # ENTRY
                if rsis[i - 1] < entry <= rsis[i]:

                    entry_price = closes[i]
                    j = i + 1

                    hit_targets = {t: False for t in TARGETS}

                    while j < n:

                        ret = (closes[j] / entry_price - 1) * 100
                        days_taken = j - i

                        # Track milestone hits
                        for t in TARGETS:
                            if not hit_targets[t] and ret >= t:
                                combo_hits[t]["count"] += 1
                                combo_hits[t]["days"].append(days_taken)
                                hit_targets[t] = True

                        # ✅ ONLY EXIT CONDITION (NO STOP LOSS)
                        if rsis[j] >= exit:
                            i = j
                            break

                        j += 1
                    else:
                        break

                i += 1

            # Store valid combos
            for t in TARGETS:
                count = combo_hits[t]["count"]
                days  = combo_hits[t]["days"]

                if count >= 3:  # reliability filter
                    avg_days = sum(days) / len(days)

                    results[t].append({
                        "entry": entry,
                        "exit": exit,
                        "count": count,
                        "avg_days": round(avg_days, 2)
                    })

    return results
    
def print_best_strategies_no_stoploss(results):

    print("\n=========== STRATEGY BUILDER (NO STOP LOSS) ===========\n")

    for target, combos in results.items():

        if not combos:
            print(f"No valid strategies found for {target}% return\n")
            continue

        combos = sorted(combos, key=lambda x: x["avg_days"])

        print(f"\n🔥 Best RSI strategies for {target}% return (No SL):\n")

        for c in combos[:5]:
            print(
                f"Entry={c['entry']} Exit={c['exit']} | "
                f"Avg Days={c['avg_days']} | "
                f"Hits={c['count']}"
            )


def auto_strategy_selector(results_sl, results_no_sl):

    final_selection = {}

    for target in [10, 20]:

        sl_list  = results_sl.get(target, [])
        nosl_list = results_no_sl.get(target, [])

        if not sl_list or not nosl_list:
            continue

        best_score = -1
        best_combo = None

        # Convert no-SL to lookup
        nosl_lookup = {
            (c["entry"], c["exit"]): c
            for c in nosl_list
        }

        for sl in sl_list:

            key = (sl["entry"], sl["exit"])

            if key not in nosl_lookup:
                continue

            nosl = nosl_lookup[key]

            sl_hits   = sl["count"]
            nosl_hits = nosl["count"]
            avg_days  = sl["avg_days"]

            if avg_days == 0:
                continue

            # Potential multiplier (upside capability)
            potential_factor = nosl_hits / sl_hits if sl_hits > 0 else 0

            # Final score
            score = (sl_hits * potential_factor) / avg_days

            if score > best_score:
                best_score = score
                best_combo = {
                    "entry": sl["entry"],
                    "exit": sl["exit"],
                    "target": target,
                    "avg_days": avg_days,
                    "sl_hits": sl_hits,
                    "nosl_hits": nosl_hits,
                    "potential_factor": round(potential_factor, 2),
                    "score": round(score, 3)
                }

        if best_combo:
            final_selection[target] = best_combo

    return final_selection


def print_auto_selected_strategies(final_selection):

    print("\n================ AUTO STRATEGY SELECTOR =================\n")

    if not final_selection:
        print("No optimal strategies found.\n")
        return

    for target, s in final_selection.items():

        print(f"🎯 BEST Strategy for {target}% Return:\n")

        print(f"   Entry RSI        : {s['entry']}")
        print(f"   Exit RSI         : {s['exit']}")
        print(f"   Avg Days         : {s['avg_days']}")
        print(f"   Hits (With SL)   : {s['sl_hits']}")
        print(f"   Hits (No SL)     : {s['nosl_hits']}")
        print(f"   Potential Factor : {s['potential_factor']}")
        print(f"   Final Score      : {s['score']}\n")
        

# ──────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────
def main():

    conn = sqlite3.connect(DB_NAME)

    try:
        df = load_equity_data(conn, TABLE_NAME)

        if df.empty:
            return

        results = simulate_return_milestones(
            df,
            rsi_entry=RSI_ENTRY,
            rsi_exit=RSI_EXIT
        )

        print_milestone_summary(results)

        # Strategy builders
        strategy_results = find_best_rsi_strategy(df)
        strategy_no_sl   = find_best_rsi_strategy_no_stoploss(df)

        print_best_strategies(strategy_results)
        print_best_strategies_no_stoploss(strategy_no_sl)

        # 🔥 Auto Selector
        auto_selected = auto_strategy_selector(strategy_results, strategy_no_sl)
        print_auto_selected_strategies(auto_selected)

    finally:
        conn.close()


# ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
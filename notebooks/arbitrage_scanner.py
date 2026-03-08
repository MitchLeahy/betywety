# Databricks notebook source
# MAGIC %md
# MAGIC # Cross-Exchange Arbitrage Scanner
# MAGIC
# MAGIC Matches CBB moneyline games across Kalshi and Polymarket, then checks
# MAGIC whether buying opposing outcomes on different exchanges yields a
# MAGIC guaranteed profit (arbitrage).
# MAGIC
# MAGIC **How it works**
# MAGIC
# MAGIC | Platform    | Structure per game                     | Price unit     |
# MAGIC |-------------|----------------------------------------|----------------|
# MAGIC | Kalshi      | 2 markets (one per team), YES/NO each  | cents (0-100)  |
# MAGIC | Polymarket  | 1 market, 2 outcomes                   | decimal (0-1)  |
# MAGIC
# MAGIC Arbitrage exists when: `cost_team_A_exchange1 + cost_team_B_exchange2 < $1.00`

# COMMAND ----------
# MAGIC %md
# MAGIC ## ADLS path

# COMMAND ----------
DATA_PATH = "abfss://kalshi-data@stkalshiogihujuict7io.dfs.core.windows.net"

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1 — Load Kalshi markets
# MAGIC
# MAGIC `yes_sub_title` directly contains the team name for each market
# MAGIC (e.g. "Purdue Fort Wayne"), so we know exactly which team the YES
# MAGIC side represents.

# COMMAND ----------
from pyspark.sql import functions as F

kalshi_markets = (
    spark.read.format("delta").load(f"{DATA_PATH}/silver/markets")
    .select(
        "ticker",
        "event_ticker",
        "title",
        "yes_sub_title",
        "status",
        F.col("yes_bid").cast("int").alias("yes_bid"),
        F.col("yes_ask").cast("int").alias("yes_ask"),
        F.col("no_bid").cast("int").alias("no_bid"),
        F.col("no_ask").cast("int").alias("no_ask"),
        F.col("last_price").cast("int").alias("last_price"),
    )
)

print(f"Kalshi total markets: {kalshi_markets.count()}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2 — Load Polymarket moneyline markets
# MAGIC
# MAGIC Filter to `sportsMarketType = 'moneyline'` (exclude spreads, totals,
# MAGIC futures).

# COMMAND ----------
pm_markets = (
    spark.read.format("delta").load(f"{DATA_PATH}/silver/polymarket/markets")
    .filter(F.col("sportsMarketType") == "moneyline")
    .select(
        "conditionId",
        "question",
        "outcomes",
        "outcomePrices",
        F.col("gameStartTime").cast("timestamp").alias("game_start"),
        "slug",
    )
)

print(f"Polymarket moneyline markets: {pm_markets.count()}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3 — Parse dates and team names
# MAGIC
# MAGIC **Kalshi** `event_ticker` encodes the date: `KXNCAAMBGAME-26MAR05...`
# MAGIC → `2026-03-05`.  `yes_sub_title` gives the team name directly.
# MAGIC
# MAGIC **Polymarket** uses `gameStartTime` for the date and `outcomes` JSON
# MAGIC array for full team names with mascots.

# COMMAND ----------
import re, json

MONTH_MAP = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}


def parse_kalshi_date(event_ticker: str) -> str:
    m = re.search(r"-(\d{2})([A-Z]{3})(\d{2})", event_ticker)
    if not m:
        return None
    yy, mon, dd = m.group(1), m.group(2), m.group(3)
    month = MONTH_MAP.get(mon)
    if not month:
        return None
    return f"20{yy}-{month:02d}-{int(dd):02d}"


def normalize(name: str) -> str:
    if not name:
        return ""
    name = name.lower().strip()
    name = name.replace("st.", "state").replace("'", "").replace("-", " ")
    name = re.sub(r"[^\w\s]", "", name)
    return re.sub(r"\s+", " ", name).strip()

# ── Kalshi: group two markets per game ────────────────────────────

kalshi_rows = kalshi_markets.collect()

kalshi_games = {}
for row in kalshi_rows:
    evt = row["event_ticker"]
    game_date = parse_kalshi_date(evt)
    if not game_date:
        continue

    team_name = row["yes_sub_title"] or ""
    norm_team = normalize(team_name)

    entry = kalshi_games.setdefault(evt, {"date": game_date, "teams": {}})
    entry["teams"][norm_team] = {
        "raw_name": team_name,
        "ticker": row["ticker"],
        "yes_bid": row["yes_bid"],
        "yes_ask": row["yes_ask"],
        "no_bid": row["no_bid"],
        "no_ask": row["no_ask"],
        "last_price": row["last_price"],
    }

kalshi_by_date = {}
for evt, info in kalshi_games.items():
    if len(info["teams"]) != 2:
        continue
    kalshi_by_date.setdefault(info["date"], []).append(info)

print(f"Kalshi unique games (2-market pairs): {sum(len(v) for v in kalshi_by_date.values())}")
print(f"Dates with games: {sorted(kalshi_by_date.keys())}")

# ── Polymarket: parse outcomes ────────────────────────────────────

pm_rows = pm_markets.collect()
print(f"Polymarket moneyline rows: {len(pm_rows)}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4 — Match games across exchanges
# MAGIC
# MAGIC For each Polymarket game, find a Kalshi game on the same date where
# MAGIC the Kalshi `yes_sub_title` (normalized) is a prefix of the Polymarket
# MAGIC outcome name (normalized, with mascot).  Both teams must match.

# COMMAND ----------

def teams_match(kalshi_norm: str, pm_full_name: str) -> bool:
    """Does the Kalshi school name match the Polymarket full team name?"""
    pm_norm = normalize(pm_full_name)
    if kalshi_norm == pm_norm:
        return True
    if pm_norm.startswith(kalshi_norm):
        return True
    if kalshi_norm.startswith(pm_norm):
        return True
    k_tok = set(kalshi_norm.split())
    p_tok = set(pm_norm.split())
    if len(k_tok) >= 2 and k_tok.issubset(p_tok):
        return True
    return False


matched = []
unmatched_pm = []

for pm_row in pm_rows:
    outcomes_raw = pm_row["outcomes"]
    prices_raw = pm_row["outcomePrices"]
    game_start = pm_row["game_start"]

    if not outcomes_raw or not prices_raw or not game_start:
        continue

    outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw
    prices = json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw

    if len(outcomes) != 2 or len(prices) != 2:
        continue

    pm_date = str(game_start.date()) if hasattr(game_start, "date") else str(game_start)[:10]
    pm_team_a = outcomes[0]
    pm_team_b = outcomes[1]
    pm_price_a = float(prices[0])
    pm_price_b = float(prices[1])

    candidates = kalshi_by_date.get(pm_date, [])
    found = False

    for k_game in candidates:
        k_teams = list(k_game["teams"].keys())
        k_team_0, k_team_1 = k_teams[0], k_teams[1]

        # Try mapping: k_team_0 ↔ pm_team_a, k_team_1 ↔ pm_team_b
        if teams_match(k_team_0, pm_team_a) and teams_match(k_team_1, pm_team_b):
            matched.append({
                "game_date": pm_date,
                "pm_question": pm_row["question"],
                "pm_team_a": pm_team_a,
                "pm_team_b": pm_team_b,
                "pm_price_a": pm_price_a,
                "pm_price_b": pm_price_b,
                "k_team_a_name": k_game["teams"][k_team_0]["raw_name"],
                "k_team_b_name": k_game["teams"][k_team_1]["raw_name"],
                "k_team_a_yes_ask": k_game["teams"][k_team_0]["yes_ask"],
                "k_team_b_yes_ask": k_game["teams"][k_team_1]["yes_ask"],
                "k_team_a_yes_bid": k_game["teams"][k_team_0]["yes_bid"],
                "k_team_b_yes_bid": k_game["teams"][k_team_1]["yes_bid"],
                "k_team_a_ticker": k_game["teams"][k_team_0]["ticker"],
                "k_team_b_ticker": k_game["teams"][k_team_1]["ticker"],
            })
            found = True
            break

        # Try reversed mapping: k_team_0 ↔ pm_team_b, k_team_1 ↔ pm_team_a
        if teams_match(k_team_0, pm_team_b) and teams_match(k_team_1, pm_team_a):
            matched.append({
                "game_date": pm_date,
                "pm_question": pm_row["question"],
                "pm_team_a": pm_team_a,
                "pm_team_b": pm_team_b,
                "pm_price_a": pm_price_a,
                "pm_price_b": pm_price_b,
                "k_team_a_name": k_game["teams"][k_team_1]["raw_name"],
                "k_team_b_name": k_game["teams"][k_team_0]["raw_name"],
                "k_team_a_yes_ask": k_game["teams"][k_team_1]["yes_ask"],
                "k_team_b_yes_ask": k_game["teams"][k_team_0]["yes_ask"],
                "k_team_a_yes_bid": k_game["teams"][k_team_1]["yes_bid"],
                "k_team_b_yes_bid": k_game["teams"][k_team_0]["yes_bid"],
                "k_team_a_ticker": k_game["teams"][k_team_1]["ticker"],
                "k_team_b_ticker": k_game["teams"][k_team_0]["ticker"],
            })
            found = True
            break

    if not found:
        unmatched_pm.append({
            "date": pm_date,
            "question": pm_row["question"],
            "pm_team_a_norm": normalize(pm_team_a),
            "pm_team_b_norm": normalize(pm_team_b),
        })

print(f"Matched games:    {len(matched)}")
print(f"Unmatched (PM):   {len(unmatched_pm)}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5 — Calculate arbitrage
# MAGIC
# MAGIC After matching, `k_team_a` maps to `pm_team_a` and `k_team_b` maps to
# MAGIC `pm_team_b`.  Kalshi's `yes_ask` is the cost (in cents) to buy "this
# MAGIC team wins".  There are two cross-exchange scenarios:
# MAGIC
# MAGIC | Scenario | Buy on Kalshi (cents) | Buy on Polymarket ($) | Total cost ($) |
# MAGIC |----------|----------------------|----------------------|---------------|
# MAGIC | 1        | Team A yes_ask       | Team B pm_price_b    | k_a/100 + pm_b |
# MAGIC | 2        | Team B yes_ask       | Team A pm_price_a    | k_b/100 + pm_a |
# MAGIC
# MAGIC Payout is always **$1.00** for the winner.  If total cost < $1, you
# MAGIC profit the difference regardless of who wins.

# COMMAND ----------
arb_rows = []

for m in matched:
    k_ask_a = m["k_team_a_yes_ask"]
    k_ask_b = m["k_team_b_yes_ask"]

    if k_ask_a is None or k_ask_b is None:
        continue

    k_price_a = k_ask_a / 100.0
    k_price_b = k_ask_b / 100.0
    pm_price_a = m["pm_price_a"]
    pm_price_b = m["pm_price_b"]

    # Scenario 1: Buy TeamA on Kalshi + TeamB on Polymarket
    cost_1 = k_price_a + pm_price_b
    profit_1 = 1.0 - cost_1

    # Scenario 2: Buy TeamB on Kalshi + TeamA on Polymarket
    cost_2 = k_price_b + pm_price_a
    profit_2 = 1.0 - cost_2

    # Also check same-exchange implied overround
    kalshi_overround = k_price_a + k_price_b  # typically > 1.0
    pm_overround = pm_price_a + pm_price_b      # typically ~1.0

    best_profit = max(profit_1, profit_2)
    best_scenario = 1 if profit_1 >= profit_2 else 2

    arb_rows.append({
        "game_date": m["game_date"],
        "game": m["pm_question"],
        "k_team_a": m["k_team_a_name"],
        "k_team_b": m["k_team_b_name"],
        "k_price_a": round(k_price_a, 2),
        "k_price_b": round(k_price_b, 2),
        "pm_team_a": m["pm_team_a"],
        "pm_team_b": m["pm_team_b"],
        "pm_price_a": pm_price_a,
        "pm_price_b": pm_price_b,
        "scenario_1_cost": round(cost_1, 4),
        "scenario_2_cost": round(cost_2, 4),
        "best_profit": round(best_profit, 4),
        "best_scenario": best_scenario,
        "is_arb": best_profit > 0,
        "kalshi_overround": round(kalshi_overround, 2),
        "pm_overround": round(pm_overround, 4),
    })

arb_rows.sort(key=lambda x: -x["best_profit"])

print(f"Games analyzed:           {len(arb_rows)}")
arb_count = sum(1 for r in arb_rows if r["is_arb"])
print(f"Arbitrage opportunities:  {arb_count}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6 — Results
# MAGIC
# MAGIC Positive `best_profit` = guaranteed profit per $1 contract (before
# MAGIC exchange fees).  Sort descending so the juiciest opportunities appear
# MAGIC first.

# COMMAND ----------
if arb_rows:
    df_arb = spark.createDataFrame(arb_rows)
    display(
        df_arb.select(
            "game_date", "game",
            "k_team_a", "k_price_a", "pm_price_a",
            "k_team_b", "k_price_b", "pm_price_b",
            "best_profit", "best_scenario", "is_arb",
            "kalshi_overround", "pm_overround",
        ).orderBy(F.col("best_profit").desc())
    )
else:
    print("No matched games to analyze.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7 — Arbitrage detail
# MAGIC
# MAGIC For each opportunity where `is_arb = true`, print the exact trade.

# COMMAND ----------
print("=" * 80)
arb_only = [r for r in arb_rows if r["is_arb"]]
if not arb_only:
    print("No arbitrage found at current prices.")
    print("\nClosest opportunities:")
    for r in arb_rows[:5]:
        print(f"  {r['game_date']} {r['game']}: spread = {-r['best_profit']:.4f}")

for r in arb_only:
    s = r["best_scenario"]
    if s == 1:
        buy_k = r["k_team_a"]
        buy_pm = r["pm_team_b"]
        cost_k = r["k_price_a"]
        cost_pm = r["pm_price_b"]
    else:
        buy_k = r["k_team_b"]
        buy_pm = r["pm_team_a"]
        cost_k = r["k_price_b"]
        cost_pm = r["pm_price_a"]

    total = cost_k + cost_pm
    print(f"\n{r['game_date']}  {r['game']}")
    print(f"  BUY on Kalshi:     {buy_k:30s}  @ ${cost_k:.2f}")
    print(f"  BUY on Polymarket: {buy_pm:30s}  @ ${cost_pm:.4f}")
    print(f"  Total cost: ${total:.4f}   Guaranteed profit: ${r['best_profit']:.4f}")
print("=" * 80)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8 — Unmatched games (diagnostics)
# MAGIC
# MAGIC Polymarket moneyline games that could **not** be joined to any Kalshi
# MAGIC game.  Review these to tune the team-name matching logic.

# COMMAND ----------
if unmatched_pm:
    print(f"{len(unmatched_pm)} unmatched Polymarket games:\n")
    df_unmatched = spark.createDataFrame(unmatched_pm)
    display(df_unmatched.orderBy("date"))
else:
    print("All Polymarket moneyline games matched!")

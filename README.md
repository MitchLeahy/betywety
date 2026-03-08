# betywety

College basketball prediction-market pipelines — Kalshi (`KXNCAAMBGAME`) and Polymarket (`tag_id=102114` NCAAB). Bronze/silver medallion architecture with REST ingestion and WebSocket streaming. Only active lines are tracked. Live prices are maintained by the streams in dedicated silver tables. An arbitrage scanner cross-matches games between exchanges to find mispriced outcomes.

## Data Flow

```mermaid
flowchart TB
    subgraph kalshi_src [Kalshi - College Basketball]
        K_REST[REST API\nseries: KXNCAAMBGAME]
        K_WS[WebSocket - PEM auth]
    end

    subgraph poly_src [Polymarket - NCAAB]
        P_REST[Gamma API\ntag_id: 102114]
        P_WS[WebSocket - no auth]
    end

    subgraph kalshi_batch [Kalshi Batch - hourly]
        K_Fetch[Fetch open events + markets]
        K_BronzeE[bronze/events]
        K_BronzeM[bronze/markets]
        K_Dedupe[Dedupe by ticker]
        K_SilverE[silver/events]
        K_SilverM[silver/markets]
    end

    subgraph kalshi_stream [Kalshi Stream - continuous]
        K_Read[Load active tickers from silver]
        K_Sub[Subscribe ticker + trade]
        K_Refresh[Refresh subs every 5 min]
        K_Buf[Buffer + flush]
        K_BronzeT[bronze/ticker_snapshots]
        K_BronzeTr[bronze/trades]
        K_LivePrices[silver/live_prices]
    end

    subgraph poly_batch [Polymarket Batch - hourly]
        P_Fetch[Fetch active events + markets]
        P_Filter[Filter: active only]
        P_BronzeE[bronze/polymarket/events]
        P_BronzeM[bronze/polymarket/markets]
        P_Dedupe[Dedupe by conditionId]
        P_SilverE[silver/polymarket/events]
        P_SilverM[silver/polymarket/markets]
    end

    subgraph poly_stream [Polymarket Stream - continuous]
        P_Read[Load active token IDs from silver]
        P_Sub[Subscribe market channel]
        P_Refresh[Refresh subs every 5 min]
        P_Buf[Buffer + flush]
        P_BronzeP[bronze/polymarket/price_updates]
        P_LivePrices[silver/polymarket/live_prices]
    end

    subgraph arb [Arbitrage Scanner - on demand]
        ARB_Read[Read silver/markets +\nsilver/polymarket/markets]
        ARB_Match[Match games by\ndate + team name]
        ARB_Calc[Compare prices\nacross exchanges]
        ARB_Out[Arbitrage\nopportunities]
    end

    K_REST --> K_Fetch --> K_BronzeE & K_BronzeM
    K_BronzeE & K_BronzeM --> K_Dedupe --> K_SilverE & K_SilverM
    K_SilverM --> K_Read
    K_WS --> K_Sub
    K_Read --> K_Sub
    K_Sub --> K_Buf --> K_BronzeT & K_BronzeTr
    K_Buf --> K_LivePrices
    K_SilverM --> K_Refresh --> K_Sub

    P_REST --> P_Fetch --> P_Filter --> P_BronzeE & P_BronzeM
    P_BronzeE & P_BronzeM --> P_Dedupe --> P_SilverE & P_SilverM
    P_SilverM --> P_Read
    P_WS --> P_Sub
    P_Read --> P_Sub
    P_Sub --> P_Buf --> P_BronzeP
    P_Buf --> P_LivePrices
    P_SilverM --> P_Refresh --> P_Sub

    K_SilverM --> ARB_Read
    P_SilverM --> ARB_Read
    ARB_Read --> ARB_Match --> ARB_Calc --> ARB_Out
```

### How it works

1. **Hourly ingestion** pulls only active CBB events/markets from each exchange's REST API into bronze, dedupes into silver
2. **Continuous streams** subscribe to active lines from silver via WebSocket, buffer messages, and append to bronze
3. **Subscription refresh** — every 5 minutes the streams re-read silver, subscribe to new markets, and unsubscribe from resolved ones (no reconnection needed)
4. **Live prices** — on each flush (~30s), the streams write the latest price per asset/ticker to a dedicated `silver/live_prices` table, merging with existing prices so no data is lost between flushes
5. **Arbitrage scanning** — reads silver markets from both exchanges, matches games by date + normalized team name, and flags opportunities where buying opposing outcomes across exchanges costs less than the $1 payout

### Filtering

| Exchange | Filter | Effect |
|----------|--------|--------|
| **Kalshi** | `series_ticker=KXNCAAMBGAME`, `status=open` | Men's College Basketball games only |
| **Polymarket** | `tag_id=102114`, `active=true`, `closed=false` | NCAAB events only, closed markets excluded |
| **Streams** | Re-read silver every 5 min | Auto-subscribe new lines, drop resolved ones |

### Systems and responsibilities

| System | Responsibility |
|--------|----------------|
| **Kalshi REST API** | CBB events and markets snapshots (public, no auth). Filtered by `KXNCAAMBGAME` series. |
| **Kalshi WebSocket** | Real-time ticker and trade data for active CBB markets. PEM auth required. |
| **Polymarket Gamma API** | NCAAB events and active markets (public, no auth). Filtered by `tag_id=102114`. |
| **Polymarket CLOB WebSocket** | Real-time price changes, best bid/ask, last trade for active token IDs. No auth. |
| **Databricks (kalshi_ingestion_pipeline)** | Fetches Kalshi CBB events/markets, writes bronze, dedupes to silver. Hourly. |
| **Databricks (kalshi_websocket_stream)** | Subscribes to active CBB tickers from silver, buffers ticker/trade to bronze, writes latest snapshot per market to `silver/live_prices` on each flush. Refreshes subs every 5 min. Continuous. |
| **Databricks (polymarket_ingestion_pipeline)** | Fetches Polymarket NCAAB events, filters active-only markets, writes bronze, dedupes to silver. Hourly. |
| **Databricks (polymarket_websocket_stream)** | Subscribes to active NCAAB token IDs from silver, buffers price updates to bronze, writes latest price per asset to `silver/polymarket/live_prices` on each flush. Refreshes subs every 5 min. Continuous. |
| **Databricks (arbitrage_scanner)** | Cross-matches Kalshi and Polymarket moneyline markets by game date + team name. Compares prices and flags arbitrage when combined cost < $1. On-demand. |
| **ADLS Gen2 (kalshi-data container)** | Stores all Delta tables: Kalshi bronze/silver and Polymarket bronze/silver (under `polymarket/` subdirs). |
| **Azure Key Vault** | Secrets: `sp-client-secret` (ADLS OAuth), `kalshi-api-key`, `kalshi-private-key` (Kalshi WebSocket auth). |
| **Databricks Secret Scope (kalshi-secrets)** | Linked to Key Vault. All notebooks read secrets via `dbutils.secrets.get()`. |
| **Service principal (sp-kalshi-databricks)** | OAuth identity for Databricks to access ADLS and Key Vault. Storage Blob Data Contributor + Key Vault Secrets User. |

### Live prices tables

| Table | Key | Updated by | Frequency |
|-------|-----|-----------|-----------|
| `silver/live_prices` | `market_ticker` | `kalshi_websocket_stream` | Every flush (~30s) |
| `silver/polymarket/live_prices` | `asset_id` | `polymarket_websocket_stream` | Every flush (~30s) |

These tables contain the most recent price snapshot for each active market/asset. On each flush, the stream merges new prices with existing ones (new data wins for matching keys) and overwrites the table. Consumers can join these to the corresponding `silver/markets` tables for enriched views.

## Quick Start

1. Deploy infrastructure: see [infrastructure/arm/README.md](infrastructure/arm/README.md)
2. Run `notebooks/kalshi_ingestion_pipeline.py` — Kalshi CBB REST fetch, bronze, silver
3. Run `notebooks/kalshi_websocket_stream.py` — Kalshi CBB ticker/trade stream + live prices
4. Run `notebooks/polymarket_ingestion_pipeline.py` — Polymarket NCAAB REST fetch, bronze, silver
5. Run `notebooks/polymarket_websocket_stream.py` — Polymarket NCAAB price stream + live prices
6. Run `notebooks/arbitrage_scanner.py` — cross-exchange arbitrage detection

## Workflows (Databricks Asset Bundles)

Jobs are defined in `databricks.yml` and `resources/jobs.yml`.

**Deploy:**
```bash
databricks bundle deploy -t dev
```

**Jobs:**
- **kalshi_ingestion** — Hourly. REST fetch CBB -> bronze -> silver.
- **kalshi_websocket_stream** — Manual start, runs continuously. Streams active CBB ticker/trade to bronze + silver/live_prices. Refreshes subs every 5 min.
- **polymarket_ingestion** — Hourly. REST fetch NCAAB (active only) -> bronze -> silver.
- **polymarket_websocket_stream** — Manual start, runs continuously. Streams active NCAAB prices to bronze + silver/polymarket/live_prices. Refreshes subs every 5 min.

**Manual run:**
```bash
databricks bundle run kalshi_ingestion -t dev
databricks bundle run kalshi_websocket_stream -t dev
databricks bundle run polymarket_ingestion -t dev
databricks bundle run polymarket_websocket_stream -t dev
```

Or trigger from the Databricks Jobs UI after deploy.

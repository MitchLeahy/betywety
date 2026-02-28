# Polymarket Pipeline Plan

Mirror the Kalshi pipeline for Polymarket: REST fetch (events, markets), bronze/silver, WebSocket streaming for live prices, and price updates into silver.

---

## Polymarket vs Kalshi

| Aspect | Kalshi | Polymarket |
|--------|--------|------------|
| REST base | `api.elections.kalshi.com/trade-api/v2` | `gamma-api.polymarket.com` |
| REST auth | Public (events/markets) | Public |
| WebSocket | `wss://api.elections.kalshi.com/trade-api/ws/v2` | `wss://ws-subscriptions-clob.polymarket.com/ws/market` |
| WebSocket auth | PEM required | **None** for market channel |
| Subscribe by | market_ticker | assets_ids (token IDs) |
| Event→Market | series_ticker filter | Events contain nested markets array |

---

## 1. REST API client (`src/polymarket/api.py`)

Add `fetch_events()` and `fetch_markets()`:

- **Events:** `GET https://gamma-api.polymarket.com/events?active=true&closed=false&limit=100&offset=...`
- **Markets:** From events (nested `markets[]`) or `GET .../markets?active=true&closed=false`
- Pagination: `limit` + `offset`
- Optional: filter by `tag_id` (e.g. sports tag) for CBB/football

Events response includes nested markets. Extract markets from events or call `/markets` separately.

**Key fields for silver:**
- Event: `id`, `slug`, `title`, `active`, `closed`, `endDate`, `volume`, `liquidity`
- Market: `id`, `conditionId`, `question`, `clobTokenIds`, `outcomePrices`, `bestBid`, `bestAsk`, `lastTradePrice`, `active`, `closed`

---

## 2. Bronze ingestion (`notebooks/polymarket_ingestion_pipeline.py`)

Same structure as Kalshi:

1. **Configure ADLS** — reuse same `KALSHI_DATA_PATH` or add `POLYMARKET_DATA_PATH` (e.g. `bronze/polymarket/events`, `bronze/polymarket/markets`)
2. **Fetch events** — `fetch_events(active=True, closed=False)` with pagination
3. **Flatten markets** — extract from events or fetch separately
4. **Write bronze** — `bronze/polymarket/events`, `bronze/polymarket/markets` with `_ingestion_ts`
5. **Silver dedupe** — by `id` (event) and `id` or `conditionId` (market)
6. **Price update** — join silver markets with latest from `bronze/polymarket/price_updates` (WebSocket)

Storage: either **shared container** (`kalshi-data`) with `polymarket/` subdirs, or separate container. Shared is simpler.

---

## 3. WebSocket stream (`notebooks/polymarket_websocket_stream.py`)

**Endpoint:** `wss://ws-subscriptions-clob.polymarket.com/ws/market`

**Auth:** None for market channel.

**Subscribe by:** `assets_ids` — token IDs. Each market has `clobTokenIds` (e.g. `"123,456"` for Yes/No). Parse and use both.

**Message types:** `book`, `price_change`, `last_trade_price`, `best_bid_ask` (with `custom_feature_enabled: true`)

**Flow:**
1. Read active markets from `silver/polymarket/markets`
2. Extract `clobTokenIds` per market, split into Yes/No token IDs
3. Connect WebSocket, send subscription: `{ "assets_ids": [...], "type": "market", "custom_feature_enabled": true }`
4. Send `PING` every 10 seconds
5. Buffer `price_change`, `last_trade_price`, `best_bid_ask` → append to `bronze/polymarket/price_updates`

**Schema for price updates:** Map `asset_id` back to `conditionId`/market via lookup. Store: `asset_id`, `condition_id`, `market_id`, `price`, `type`, `_ingestion_ts`.

---

## 4. Storage layout

| Path | Content |
|------|---------|
| `bronze/polymarket/events` | Event snapshots (append) |
| `bronze/polymarket/markets` | Market snapshots (append) |
| `bronze/polymarket/price_updates` | WebSocket price/trade messages (append) |
| `silver/polymarket/events` | Deduped events |
| `silver/polymarket/markets` | Deduped markets, optionally enriched with latest price |

---

## 5. Implementation order

| Step | Task |
|------|------|
| 1 | Add `src/polymarket/api.py` with `fetch_events`, `fetch_markets` |
| 2 | Create `notebooks/polymarket_ingestion_pipeline.py` (bronze + silver) |
| 3 | Add ticker inspection cell for `bronze/polymarket/price_updates` |
| 4 | Create `notebooks/polymarket_websocket_stream.py` |
| 5 | Add Polymarket price-update join to ingestion (silver ← price_updates) |
| 6 | Add Polymarket jobs to `resources/jobs.yml` |
| 7 | Update README with Polymarket data flow |

---

## 6. Differences to handle

1. **Token IDs:** Polymarket WebSocket uses `assets_ids` (token IDs). REST markets have `clobTokenIds` (string, comma-separated). Parse and dedupe before subscribing.
2. **No auth:** WebSocket market channel needs no PEM/API key.
3. **Heartbeat:** Send `PING` every 10 seconds (not asyncio-based ping).
4. **Schema:** Polymarket event/market schemas differ from Kalshi. Use `id`, `conditionId`, `clobTokenIds`, `outcomePrices`, etc.

---

## 7. DAB jobs

```yaml
polymarket_ingestion:
  schedule: "0 0 * * * ?"  # hourly
  notebook: polymarket_ingestion_pipeline.py

polymarket_websocket_stream:
  no schedule, manual start
  notebook: polymarket_websocket_stream.py
```


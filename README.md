# 🧠 Polymarket CTF Indexer + Analytics API

High-performance Polygon indexer and ClickHouse analytics backend for
Polymarket Conditional Token Framework (CTF).

------------------------------------------------------------------------

## 🚀 Overview

This system:

-   Indexes Polymarket CTF + Exchange contracts
-   Stores raw + derived events in ClickHouse
-   Maintains a deterministic trader ledger
-   Computes Realized / Unrealized PnL
-   Tracks win rate for resolved conditions
-   Exposes a rate-limited REST API

------------------------------------------------------------------------

# 🏗 Architecture

Polygon (WSS + HTTP) │ ▼ listener.ts │ ▼ indexer.ts │ ▼ ClickHouse │ ▼
api.ts

------------------------------------------------------------------------

# 📦 Project Structure

src/ ├── abi.ts ├── api.ts ├── config.ts ├── db.ts ├── indexer.ts └──
listener.ts

------------------------------------------------------------------------

# ⚙️ Environment Variables

Create a `.env` file:

POLYGON_WSS=wss://... POLYGON_HTTP=https://...

CLICKHOUSE_URL=http://localhost:8123 CLICKHOUSE_USERNAME=default
CLICKHOUSE_PASS=

START_BLOCK=0 REORG_SAFETY_BLOCKS=50 BACKFILL_CHUNK=50 MAX_BUFFER=1000
FLUSH_INTERVAL=1000 RPC_DELAY_MS=200 DEBUG=false

------------------------------------------------------------------------

# 🧱 ClickHouse Tables

⚠️ The tables MUST be created exactly using the following commands.

    CREATE TABLE default.conditions (`condition_id` String, `outcome_slot_count` UInt8, `payout_numerators` Array(UInt256), `resolved` UInt8 DEFAULT 0, `resolved_block` UInt64) ENGINE = SharedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') ORDER BY condition_id SETTINGS index_granularity = 8192;

    CREATE TABLE default.market_prices (`position_id` String, `last_price` UInt64, `block_number` UInt64) ENGINE = SharedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', block_number) ORDER BY position_id SETTINGS index_granularity = 8192;

    CREATE TABLE default.positions (`position_id` String, `condition_id` String, `index_set` UInt256) ENGINE = SharedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') ORDER BY position_id SETTINGS index_granularity = 8192;

    CREATE TABLE default.raw_events (`block_number` UInt64, `tx_hash` String, `log_index` UInt32, `contract` String, `event_name` String, `data` String) ENGINE = SharedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') PARTITION BY intDiv(block_number, 500000) ORDER BY (block_number, tx_hash, log_index) SETTINGS index_granularity = 8192;

    CREATE TABLE default.trader_ledger (`trader` LowCardinality(String), `position_id` String, `block_number` UInt64, `size_delta` Int256, `collateral_delta` Int256, `fee_delta` Int256, `event_type` Enum8('trade_buy' = 1, 'trade_sell' = 2, 'redemption' = 3), `tx_hash` String, `log_index` UInt32) ENGINE = SharedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') PARTITION BY intDiv(block_number, 500000) ORDER BY (trader, position_id, block_number, tx_hash, log_index) SETTINGS index_granularity = 8192;

------------------------------------------------------------------------

# 📡 Running the System

## Install

npm install

## Start Indexer

npm run ws

## Start API

npm run api

API runs at:

http://localhost:3000

------------------------------------------------------------------------

# 📊 API Endpoint

GET /trader/:address/metrics

Response:

{ "trader": "0x...", "realized_pnl": 123.45, "unrealized_pnl": -54.12,
"total_pnl": 69.33, "win_rate": 0.62 }

------------------------------------------------------------------------

# 📈 Metric Definitions

Realized PnL: sum(collateral_delta + fee_delta)

Unrealized PnL: (size × last_price) - cost

Win Rate: \# profitable resolved conditions / total resolved conditions
traded

------------------------------------------------------------------------

# 🔐 Safety Features

-   Reorg safety backfill
-   Auto WebSocket reconnect
-   Raw event storage
-   Buffered inserts
-   Rate limiting
-   30-second caching

------------------------------------------------------------------------

# 🧩 Summary

Production-ready Polymarket CTF indexer with deterministic accounting
and high-performance analytics on ClickHouse.

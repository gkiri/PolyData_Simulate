# Unified Data Schema for Polymarket 15-Minute Crypto Events

## Overview

This document describes the production-grade data schema designed for backtesting, live simulation, and replay of any trading strategy on Polymarket's 15-minute crypto prediction markets.

## Design Philosophy

1. **One File Per Event**: Each 15-minute window gets exactly ONE JSONL file containing ALL data
2. **Self-Contained Records**: Every record contains full context (window info, timestamps, metrics)
3. **Strategy-Agnostic**: Captures everything needed for ANY strategy (Gabagool, momentum, arbitrage, market making, etc.)
4. **Human Readable**: Clear naming, meaningful timestamps, derived metrics
5. **Replay-Ready**: Data is sequenced by nanosecond timestamps for accurate simulation

## Folder Structure

```
data/
├── BTC/
│   ├── date=2026-01-12/
│   │   ├── window_00_1768204800.jsonl   # 00:00-00:15 UTC
│   │   ├── window_01_1768205700.jsonl   # 00:15-00:30 UTC
│   │   ├── window_02_1768206600.jsonl   # 00:30-00:45 UTC
│   │   ├── ...
│   │   └── window_95_1768290300.jsonl   # 23:45-00:00 UTC
│   └── date=2026-01-13/
│       └── ...
├── ETH/
│   ├── date=2026-01-12/
│   │   ├── window_00_1768204800.jsonl
│   │   └── ...
│   └── ...
└── SCHEMA.md
```

### File Naming Convention

```
window_{INDEX:02d}_{START_TIMESTAMP}.jsonl
```

- **INDEX**: 0-95 (which 15-minute window of the day, 00=midnight UTC)
- **START_TIMESTAMP**: Unix timestamp when window started

### Window Index Reference

| Index | UTC Time     | Index | UTC Time     | Index | UTC Time     |
|-------|--------------|-------|--------------|-------|--------------|
| 00    | 00:00-00:15  | 32    | 08:00-08:15  | 64    | 16:00-16:15  |
| 01    | 00:15-00:30  | 33    | 08:15-08:30  | 65    | 16:15-16:30  |
| 02    | 00:30-00:45  | 34    | 08:30-08:45  | 66    | 16:30-16:45  |
| 03    | 00:45-01:00  | 35    | 08:45-09:00  | 67    | 16:45-17:00  |
| ...   | ...          | ...   | ...          | ...   | ...          |
| 31    | 07:45-08:00  | 63    | 15:45-16:00  | 95    | 23:45-00:00  |

---

## Record Types

Each JSONL file contains records of different types, all following the unified schema:

### 1. `market_meta` - Market Metadata

Written once at the start of each window with market details.

```json
{
  "schema_version": 2,
  "record_type": "market_meta",
  "recv_ts_ns": 1768250236000000000,
  "window_index": 42,
  "window_start_ts": 1768250400,
  "window_end_ts": 1768251300,
  "asset": "BTC",
  "elapsed_sec": 0.0,
  "remaining_sec": 900.0,
  "market_id": "0x2a12fd3d8235c25c062dc9652970ef9f635636c4018df0909e1a51c673e125cb",
  "condition_id": "0x123...",
  "question": "Will BTC price be above $95,000 at 3:15 PM UTC?",
  "strike_price": "95000",
  "token_id_yes": "56386988715176239099720172290594769295756680374928571099138069977179519358166",
  "token_id_no": "63670753354844494163057275310194312729998307779831462269530137743341854359908"
}
```

### 2. `snapshot` - Full State Snapshot

Periodic (every 1 second) complete state capture with order books and crypto price.

```json
{
  "schema_version": 2,
  "record_type": "snapshot",
  "recv_ts_ns": 1768250237123456789,
  "server_ts_ms": 1768250237100,
  "window_index": 42,
  "window_start_ts": 1768250400,
  "window_end_ts": 1768251300,
  "asset": "BTC",
  "elapsed_sec": 1.123,
  "remaining_sec": 898.877,
  
  "market_id": "0x2a12...",
  "token_id_yes": "5638...",
  "token_id_no": "6367...",
  
  "crypto_price_usd": "94523.45",
  "crypto_price_ts_ms": 1768250237000,
  
  "yes_bids": [
    {"price": "0.52", "size": "1250.5"},
    {"price": "0.51", "size": "890.25"},
    ...
  ],
  "yes_asks": [
    {"price": "0.53", "size": "1100.0"},
    {"price": "0.54", "size": "750.0"},
    ...
  ],
  "yes_best_bid": "0.52",
  "yes_best_ask": "0.53",
  "yes_last_trade_price": "0.52",
  
  "no_bids": [...],
  "no_asks": [...],
  "no_best_bid": "0.47",
  "no_best_ask": "0.48",
  "no_last_trade_price": "0.48",
  
  "metrics": {
    "yes_spread": "0.01",
    "no_spread": "0.01",
    "yes_mid": "0.525",
    "no_mid": "0.475",
    "pair_cost_at_ask": "1.01",
    "pair_cost_at_bid": "0.99",
    "arbitrage_spread": "-0.01",
    "yes_bid_depth_3": "2500.75",
    "yes_bid_depth_5": "4200.00",
    "yes_bid_depth_10": "8500.00",
    "yes_ask_depth_3": "2100.00",
    "yes_ask_depth_5": "3800.00",
    "yes_ask_depth_10": "7200.00",
    "no_bid_depth_3": "2300.00",
    "no_bid_depth_5": "4000.00",
    "no_bid_depth_10": "8000.00",
    "no_ask_depth_3": "2200.00",
    "no_ask_depth_5": "3900.00",
    "no_ask_depth_10": "7500.00",
    "yes_bid_ask_imbalance": "0.087",
    "no_bid_ask_imbalance": "0.025"
  }
}
```

### 3. `book_update` - Order Book Change

Real-time order book updates from WebSocket.

```json
{
  "schema_version": 2,
  "record_type": "book_update",
  "recv_ts_ns": 1768250237234567890,
  "server_ts_ms": 1768250237200,
  "window_index": 42,
  "window_start_ts": 1768250400,
  "window_end_ts": 1768251300,
  "asset": "BTC",
  "elapsed_sec": 1.234,
  "remaining_sec": 898.766,
  
  "update_type": "price_change",  // or "book", "best_bid_ask"
  "market_id": "0x2a12...",
  
  "raw": {
    "market": "0x2a12...",
    "price_changes": [
      {
        "asset_id": "5638...",
        "price": "0.52",
        "size": "1300.5",
        "side": "BUY",
        "hash": "abc123...",
        "best_bid": "0.52",
        "best_ask": "0.53"
      }
    ],
    "timestamp": "1768250237200",
    "event_type": "price_change"
  }
}
```

### 4. `trade` - Trade Execution

Trade events from the market.

```json
{
  "schema_version": 2,
  "record_type": "trade",
  "recv_ts_ns": 1768250238345678901,
  "server_ts_ms": 1768250238300,
  "window_index": 42,
  "window_start_ts": 1768250400,
  "window_end_ts": 1768251300,
  "asset": "BTC",
  "elapsed_sec": 2.345,
  "remaining_sec": 897.655,
  
  "market_id": "0x2a12...",
  "trade_asset_id": "5638...",
  "trade_price": "0.53",
  "trade_size": "25",
  "trade_side": "BUY",
  "trade_fee_bps": "1000",
  "trade_tx_hash": "0x4e8394d73cdd85d2e33dd86a160c77dbac6f1e7a521c0a79f40ec70d60aef7d6",
  
  "raw": {...}
}
```

### 5. `price_tick` - Crypto Price Update

Chainlink price updates from RTDS.

```json
{
  "schema_version": 2,
  "record_type": "price_tick",
  "recv_ts_ns": 1768250239456789012,
  "server_ts_ms": 1768250239400,
  "window_index": 42,
  "window_start_ts": 1768250400,
  "window_end_ts": 1768251300,
  "asset": "BTC",
  "elapsed_sec": 3.456,
  "remaining_sec": 896.544,
  
  "crypto_price_usd": "94528.75",
  "crypto_price_ts_ms": 1768250239000
}
```

### 6. `window_end` - End Marker

Written when window closes.

```json
{
  "schema_version": 2,
  "record_type": "window_end",
  "recv_ts_ns": 1768251300000000000,
  "window_index": 42,
  "window_start_ts": 1768250400,
  "window_end_ts": 1768251300,
  "asset": "BTC",
  "elapsed_sec": 900.0,
  "remaining_sec": 0.0
}
```

---

## Field Reference

### Core Identifiers

| Field | Type | Description |
|-------|------|-------------|
| `schema_version` | int | Schema version (currently 2) |
| `record_type` | string | Type of record (see above) |

### Timing Fields

| Field | Type | Description |
|-------|------|-------------|
| `recv_ts_ns` | int | Local receive timestamp in nanoseconds |
| `server_ts_ms` | int | Server timestamp in milliseconds |
| `window_index` | int | Which 15-minute window (0-95) |
| `window_start_ts` | int | Unix timestamp when window started |
| `window_end_ts` | int | Unix timestamp when window ends |
| `elapsed_sec` | float | Seconds since window start |
| `remaining_sec` | float | Seconds until window end |

### Asset Context

| Field | Type | Description |
|-------|------|-------------|
| `asset` | string | Asset symbol (BTC, ETH) |

### Market Metadata

| Field | Type | Description |
|-------|------|-------------|
| `market_id` | string | Polymarket market ID (hex) |
| `condition_id` | string | Condition ID for the market |
| `question` | string | Market question text |
| `strike_price` | string | Strike price for price prediction |
| `token_id_yes` | string | Token ID for YES outcome |
| `token_id_no` | string | Token ID for NO outcome |

### Crypto Price

| Field | Type | Description |
|-------|------|-------------|
| `crypto_price_usd` | string | Current crypto price in USD |
| `crypto_price_ts_ms` | int | Timestamp of price update |

### Order Book State

| Field | Type | Description |
|-------|------|-------------|
| `yes_bids` | array | YES token bid levels [{price, size}, ...] |
| `yes_asks` | array | YES token ask levels |
| `yes_best_bid` | string | Best bid price for YES |
| `yes_best_ask` | string | Best ask price for YES |
| `yes_last_trade_price` | string | Last trade price for YES |
| `no_bids` | array | NO token bid levels |
| `no_asks` | array | NO token ask levels |
| `no_best_bid` | string | Best bid price for NO |
| `no_best_ask` | string | Best ask price for NO |
| `no_last_trade_price` | string | Last trade price for NO |

### Derived Metrics

| Field | Type | Description |
|-------|------|-------------|
| `yes_spread` | string | YES token bid-ask spread |
| `no_spread` | string | NO token bid-ask spread |
| `yes_mid` | string | YES token mid price |
| `no_mid` | string | NO token mid price |
| `pair_cost_at_ask` | string | Cost to buy YES+NO at ask prices |
| `pair_cost_at_bid` | string | Value of YES+NO at bid prices |
| `arbitrage_spread` | string | 1.0 - pair_cost_at_ask (opportunity size) |
| `yes_bid_depth_3` | string | Total size at top 3 YES bid levels |
| `yes_bid_depth_5` | string | Total size at top 5 YES bid levels |
| `yes_bid_depth_10` | string | Total size at top 10 YES bid levels |
| `yes_ask_depth_3` | string | Total size at top 3 YES ask levels |
| `yes_ask_depth_5` | string | Total size at top 5 YES ask levels |
| `yes_ask_depth_10` | string | Total size at top 10 YES ask levels |
| `no_bid_depth_3` | string | Total size at top 3 NO bid levels |
| `no_bid_depth_5` | string | Total size at top 5 NO bid levels |
| `no_bid_depth_10` | string | Total size at top 10 NO bid levels |
| `no_ask_depth_3` | string | Total size at top 3 NO ask levels |
| `no_ask_depth_5` | string | Total size at top 5 NO ask levels |
| `no_ask_depth_10` | string | Total size at top 10 NO ask levels |
| `yes_bid_ask_imbalance` | string | (bid_depth - ask_depth) / (bid_depth + ask_depth) for YES |
| `no_bid_ask_imbalance` | string | (bid_depth - ask_depth) / (bid_depth + ask_depth) for NO |

### Trade Event Fields

| Field | Type | Description |
|-------|------|-------------|
| `trade_side` | string | BUY or SELL |
| `trade_price` | string | Execution price |
| `trade_size` | string | Trade size |
| `trade_fee_bps` | string | Fee rate in basis points |
| `trade_tx_hash` | string | Transaction hash |
| `trade_asset_id` | string | Which token was traded |

### Update Event Fields

| Field | Type | Description |
|-------|------|-------------|
| `update_type` | string | Type of update (price_change, book, best_bid_ask) |

### Raw Data

| Field | Type | Description |
|-------|------|-------------|
| `raw` | object | Original message for debugging/advanced analysis |

---

## Usage Examples

### Python: Reading a Window File

```python
import json
from pathlib import Path

def read_window(asset: str, date: str, window_index: int):
    """Read all records from a window file."""
    data_dir = Path("data") / asset.upper() / f"date={date}"
    
    # Find the file for this window
    for f in data_dir.glob(f"window_{window_index:02d}_*.jsonl"):
        records = []
        with open(f) as fp:
            for line in fp:
                records.append(json.loads(line))
        return records
    
    return []

# Example: Read window 42 for BTC on 2026-01-12
records = read_window("BTC", "2026-01-12", 42)
print(f"Loaded {len(records)} records")

# Filter snapshots only
snapshots = [r for r in records if r["record_type"] == "snapshot"]
print(f"Found {len(snapshots)} snapshots")
```

### Python: Replaying for Strategy Backtesting

```python
import json
from decimal import Decimal

def replay_gabagool_strategy(records):
    """
    Replay Gabagool strategy on historical data.
    
    Strategy: Buy YES+NO pairs when pair_cost_at_ask < 0.98
    """
    trades = []
    position_yes = Decimal("0")
    position_no = Decimal("0")
    locked_profit = Decimal("0")
    
    for record in records:
        if record["record_type"] != "snapshot":
            continue
            
        metrics = record.get("metrics", {})
        pair_cost = metrics.get("pair_cost_at_ask")
        
        if not pair_cost:
            continue
            
        pair_cost = Decimal(pair_cost)
        
        # Check for opportunity
        if pair_cost < Decimal("0.98"):
            # Simulate buying 10 shares of each
            trade_size = Decimal("10")
            yes_price = Decimal(record["yes_best_ask"])
            no_price = Decimal(record["no_best_ask"])
            
            position_yes += trade_size
            position_no += trade_size
            
            # Calculate locked profit
            paired = min(position_yes, position_no)
            locked_profit = paired * (Decimal("1.0") - pair_cost)
            
            trades.append({
                "elapsed_sec": record["elapsed_sec"],
                "pair_cost": str(pair_cost),
                "locked_profit": str(locked_profit),
            })
    
    return trades

# Usage
records = read_window("BTC", "2026-01-12", 42)
trades = replay_gabagool_strategy(records)
print(f"Strategy would have made {len(trades)} trades")
print(f"Final locked profit: ${trades[-1]['locked_profit'] if trades else 0}")
```

### Python: Analyzing Crypto Price Movement

```python
def analyze_price_movement(records):
    """Analyze crypto price movement during window."""
    prices = []
    
    for record in records:
        if record["record_type"] == "snapshot":
            if record.get("crypto_price_usd"):
                prices.append({
                    "elapsed_sec": record["elapsed_sec"],
                    "price": float(record["crypto_price_usd"]),
                })
    
    if not prices:
        return None
    
    start_price = prices[0]["price"]
    end_price = prices[-1]["price"]
    max_price = max(p["price"] for p in prices)
    min_price = min(p["price"] for p in prices)
    
    return {
        "start_price": start_price,
        "end_price": end_price,
        "change_pct": (end_price - start_price) / start_price * 100,
        "range_pct": (max_price - min_price) / start_price * 100,
        "high": max_price,
        "low": min_price,
    }
```

---

## Strategy Compatibility

This schema supports the following strategy types:

### 1. Gabagool/Volatility Arbitrage
- Uses: `pair_cost_at_ask`, `arbitrage_spread`, `yes/no_ask_depth_*`
- Logic: Buy YES+NO pairs when `pair_cost_at_ask < 0.98`

### 2. Momentum Trading
- Uses: `crypto_price_usd`, `yes_mid`, `no_mid`, imbalance metrics
- Logic: Trade based on crypto price momentum and market maker positioning

### 3. Market Making
- Uses: Full order book (`yes_bids`, `yes_asks`, etc.), spreads, depths
- Logic: Provide liquidity and capture bid-ask spread

### 4. Liquidity-Based
- Uses: `*_depth_*` metrics, imbalance metrics
- Logic: Trade based on liquidity imbalances

### 5. Event-Based
- Uses: `elapsed_sec`, `remaining_sec`, trade events
- Logic: Trade based on time remaining or trade flow

### 6. Cross-Asset
- Uses: Multiple asset files, `crypto_price_usd` across assets
- Logic: Trade based on BTC/ETH correlation

---

## Data Quality

### Record Frequency

| Record Type | Approximate Frequency |
|-------------|----------------------|
| `market_meta` | 1 per window |
| `snapshot` | ~1 per second (900/window) |
| `book_update` | Variable (50-500/second during active trading) |
| `trade` | Variable (0-50/second) |
| `price_tick` | ~1 per second (from Chainlink) |
| `window_end` | 1 per window |

### Typical File Size

- Active window: 10-50 MB
- Low activity window: 2-5 MB

### Data Integrity

- All records have nanosecond timestamps for ordering
- Server timestamps included when available
- Raw messages preserved for debugging
- Schema version tracked for backward compatibility

---

## Migration from Old Format

If migrating from the old multi-file format:

1. Old format: `data/raw/clob_ws/date=.../market=.../asset=.../events.jsonl`
2. New format: `data/{ASSET}/date={DATE}/window_{INDEX}_{TS}.jsonl`

The new format:
- Consolidates all data sources into one file
- Adds derived metrics
- Provides cleaner asset-based organization
- Includes window context in every record

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 2 | 2026-01-12 | Unified schema with derived metrics |
| 1 | 2026-01-11 | Initial multi-file format |


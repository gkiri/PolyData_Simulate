#!/usr/bin/env python3
"""
OPTIMIZED Unified Data Collector for Polymarket 15-Minute Crypto Events

Storage Optimization Strategies:
1. GZIP Compression - 80-90% reduction (lossless)
2. No Raw Field - Remove duplicate data from book_updates
3. Compact Mode - Store only essential updates (configurable)
4. Efficient Snapshots - Snapshots every N seconds (not every 1s)

Storage Estimates (per 15-minute window):
- Original:           80-100 MB
- GZIP only:          8-12 MB  (85% reduction)
- GZIP + No Raw:      3-5 MB   (95% reduction)
- GZIP + Compact:     1-2 MB   (98% reduction)

Usage:
    # Default (gzip + no raw) - RECOMMENDED for production
    python unified_collector_optimized.py
    
    # Maximum compression (compact mode)
    python unified_collector_optimized.py --compact
    
    # Original format (no compression, for debugging)
    python unified_collector_optimized.py --no-compress --include-raw

Author: Production-grade optimized data collection
"""

import gzip
import json
import os
import time
import threading
import signal
import sys
import argparse
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from collections import deque

import requests
from websocket import WebSocketApp

from market_discovery import discover_active_crypto_15m


# =============================================================================
# CONFIGURATION
# =============================================================================

CLOB_WS_BASE = "wss://ws-subscriptions-clob.polymarket.com"
CLOB_REST_BASE = "https://clob.polymarket.com"
RTDS_WS_URL = "wss://ws-live-data.polymarket.com"

DATA_ROOT = Path("data")

PING_INTERVAL_CLOB = 10
PING_INTERVAL_RTDS = 5
REST_BOOK_CHECKPOINT_INTERVAL = 30

ORDER_BOOK_DEPTH = 10
WINDOW_DURATION_SEC = 900
WINDOW_END_BUFFER_SEC = 5


# =============================================================================
# STORAGE CONFIGURATION
# =============================================================================

@dataclass
class StorageConfig:
    """Configuration for storage optimization."""
    
    # Compression
    use_gzip: bool = True  # Compress output files
    
    # Data reduction
    include_raw: bool = False  # Include raw field (doubles size)
    store_book_updates: bool = True  # Store individual book updates
    
    # Compact mode settings
    compact_mode: bool = False  # Ultra-compact mode
    snapshot_interval_sec: float = 1.0  # Seconds between snapshots (1-5)
    
    # Only store book updates that affect top N levels (0 = store all)
    book_update_depth_filter: int = 0
    
    # Aggregate book updates (batch N updates together)
    book_update_batch_size: int = 1  # 1 = no batching
    
    def get_file_extension(self) -> str:
        return ".jsonl.gz" if self.use_gzip else ".jsonl"
    
    def estimate_size_reduction(self) -> str:
        """Estimate size reduction from baseline."""
        baseline = 100  # MB
        
        if not self.include_raw:
            baseline *= 0.5  # Raw field is ~50% of data
        
        if self.compact_mode:
            baseline *= 0.3  # Compact mode reduces further
            
        if self.use_gzip:
            baseline *= 0.12  # GZIP achieves ~88% compression on JSON
            
        return f"~{baseline:.1f} MB per 15-min window (vs ~100 MB baseline)"


# Default: Optimized for production
DEFAULT_CONFIG = StorageConfig(
    use_gzip=True,
    include_raw=False,
    store_book_updates=True,
    compact_mode=False,
    snapshot_interval_sec=1.0,
)

# Compact: Maximum compression for long-term storage
COMPACT_CONFIG = StorageConfig(
    use_gzip=True,
    include_raw=False,
    store_book_updates=False,  # Only store snapshots and trades
    compact_mode=True,
    snapshot_interval_sec=2.0,  # Snapshot every 2 seconds
)

# Debug: Full data for troubleshooting
DEBUG_CONFIG = StorageConfig(
    use_gzip=False,
    include_raw=True,
    store_book_updates=True,
    compact_mode=False,
    snapshot_interval_sec=1.0,
)


# =============================================================================
# SCHEMA (Same as before, but with optional raw field)
# =============================================================================

SCHEMA_VERSION = 2


@dataclass
class UnifiedRecord:
    """Unified record schema - same as before but with optimizations."""
    
    schema_version: int = SCHEMA_VERSION
    record_type: str = "snapshot"
    
    recv_ts_ns: int = 0
    server_ts_ms: Optional[int] = None
    
    window_index: int = 0
    window_start_ts: int = 0
    window_end_ts: int = 0
    elapsed_sec: float = 0.0
    remaining_sec: float = 0.0
    
    asset: str = ""
    
    market_id: Optional[str] = None
    condition_id: Optional[str] = None
    question: Optional[str] = None
    strike_price: Optional[str] = None
    token_id_yes: Optional[str] = None
    token_id_no: Optional[str] = None
    
    crypto_price_usd: Optional[str] = None
    crypto_price_ts_ms: Optional[int] = None
    
    yes_bids: Optional[List[Dict]] = None
    yes_asks: Optional[List[Dict]] = None
    yes_best_bid: Optional[str] = None
    yes_best_ask: Optional[str] = None
    yes_last_trade_price: Optional[str] = None
    
    no_bids: Optional[List[Dict]] = None
    no_asks: Optional[List[Dict]] = None
    no_best_bid: Optional[str] = None
    no_best_ask: Optional[str] = None
    no_last_trade_price: Optional[str] = None
    
    metrics: Optional[Dict] = None
    
    trade_side: Optional[str] = None
    trade_price: Optional[str] = None
    trade_size: Optional[str] = None
    trade_fee_bps: Optional[str] = None
    trade_tx_hash: Optional[str] = None
    trade_asset_id: Optional[str] = None
    
    update_type: Optional[str] = None
    
    # Raw field - only included when config.include_raw is True
    raw: Optional[Dict] = None


# =============================================================================
# UTILITIES
# =============================================================================

def now_ns() -> int:
    return time.time_ns()

def now_sec() -> float:
    return time.time()

def get_window_boundaries(ts_sec: float) -> Tuple[int, int]:
    window_start = int(ts_sec // WINDOW_DURATION_SEC) * WINDOW_DURATION_SEC
    window_end = window_start + WINDOW_DURATION_SEC
    return window_start, window_end

def safe_decimal(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        return str(Decimal(str(value)))
    except:
        return None

def calculate_spread(bid: Optional[str], ask: Optional[str]) -> Optional[str]:
    if bid is None or ask is None:
        return None
    try:
        return str(Decimal(ask) - Decimal(bid))
    except:
        return None

def calculate_mid(bid: Optional[str], ask: Optional[str]) -> Optional[str]:
    if bid is None or ask is None:
        return None
    try:
        return str((Decimal(bid) + Decimal(ask)) / 2)
    except:
        return None

def calculate_imbalance(bid_depth: str, ask_depth: str) -> Optional[str]:
    try:
        bd = Decimal(bid_depth)
        ad = Decimal(ask_depth)
        total = bd + ad
        if total == 0:
            return "0"
        return str((bd - ad) / total)
    except:
        return None


# =============================================================================
# OPTIMIZED DATA FILE
# =============================================================================

class OptimizedDataFile:
    """
    Data file with compression support.
    
    Features:
    - GZIP compression (configurable)
    - Buffered writes for better performance
    - Automatic flushing
    """
    
    def __init__(self, asset: str, window_start_ts: int, config: StorageConfig):
        self.asset = asset.upper()
        self.window_start_ts = window_start_ts
        self.window_end_ts = window_start_ts + WINDOW_DURATION_SEC
        self.config = config
        
        # Calculate window index
        dt = datetime.fromtimestamp(window_start_ts, tz=timezone.utc)
        minutes_since_midnight = dt.hour * 60 + dt.minute
        self.window_index = minutes_since_midnight // 15
        
        # Build file path
        date_str = dt.strftime("%Y-%m-%d")
        ext = config.get_file_extension()
        filename = f"window_{self.window_index:02d}_{window_start_ts}{ext}"
        
        self.path = DATA_ROOT / self.asset / f"date={date_str}" / filename
        self.path.parent.mkdir(parents=True, exist_ok=True)
        
        self._file = None
        self._lock = threading.Lock()
        self._record_count = 0
        self._buffer = []
        self._buffer_size = 100  # Flush every N records
        
    def write_record(self, record: UnifiedRecord):
        """Write a record (buffered for performance)."""
        with self._lock:
            # Add window context
            record.window_index = self.window_index
            record.window_start_ts = self.window_start_ts
            record.window_end_ts = self.window_end_ts
            record.asset = self.asset
            
            # Calculate elapsed/remaining
            current_sec = record.recv_ts_ns / 1e9
            record.elapsed_sec = round(current_sec - self.window_start_ts, 3)
            record.remaining_sec = round(self.window_end_ts - current_sec, 3)
            
            # Remove raw if not configured
            if not self.config.include_raw:
                record.raw = None
            
            # Serialize
            data = self._record_to_dict(record)
            line = json.dumps(data, separators=(",", ":"), ensure_ascii=False)
            
            self._buffer.append(line)
            self._record_count += 1
            
            if len(self._buffer) >= self._buffer_size:
                self._flush_buffer()
    
    def _flush_buffer(self):
        """Flush buffer to file."""
        if not self._buffer:
            return
            
        if self._file is None:
            if self.config.use_gzip:
                self._file = gzip.open(self.path, "at", encoding="utf-8")
            else:
                self._file = open(self.path, "a", encoding="utf-8")
        
        for line in self._buffer:
            self._file.write(line + "\n")
        
        self._file.flush()
        self._buffer = []
    
    def _record_to_dict(self, record: UnifiedRecord) -> Dict:
        """Convert record to dict, removing None values."""
        data = {}
        for k, v in asdict(record).items():
            if v is not None:
                data[k] = v
        return data
    
    def close(self):
        """Close the file."""
        with self._lock:
            self._flush_buffer()
            if self._file:
                self._file.close()
                self._file = None
    
    @property
    def record_count(self) -> int:
        return self._record_count


class OptimizedFileManager:
    """Manages optimized data files."""
    
    def __init__(self, config: StorageConfig):
        self.config = config
        self._files: Dict[str, OptimizedDataFile] = {}
        self._lock = threading.Lock()
        
    def get_file(self, asset: str, ts_sec: float) -> OptimizedDataFile:
        window_start, _ = get_window_boundaries(ts_sec)
        key = f"{asset.upper()}_{window_start}"
        
        with self._lock:
            if key not in self._files:
                self._files[key] = OptimizedDataFile(asset, window_start, self.config)
            return self._files[key]
    
    def close_old_windows(self, current_ts_sec: float):
        with self._lock:
            to_close = []
            for key, wf in self._files.items():
                if wf.window_end_ts < current_ts_sec:
                    to_close.append(key)
            
            for key in to_close:
                wf = self._files.pop(key)
                end_record = UnifiedRecord(
                    record_type="window_end",
                    recv_ts_ns=now_ns(),
                )
                wf.write_record(end_record)
                wf.close()
                
                # Show compression stats
                if wf.path.exists():
                    size_mb = wf.path.stat().st_size / 1024 / 1024
                    print(f"[Closed] {wf.path.name}: {wf.record_count} records, {size_mb:.2f} MB")
                
    def close_all(self):
        with self._lock:
            for wf in self._files.values():
                wf.close()
            self._files.clear()


# =============================================================================
# ORDER BOOK STATE
# =============================================================================

class OrderBookState:
    """Maintains current order book state."""
    
    def __init__(self, token_id_yes: str, token_id_no: str):
        self.token_id_yes = token_id_yes
        self.token_id_no = token_id_no
        
        self.yes_bids: List[Dict] = []
        self.yes_asks: List[Dict] = []
        self.no_bids: List[Dict] = []
        self.no_asks: List[Dict] = []
        
        self.yes_last_trade_price: Optional[str] = None
        self.no_last_trade_price: Optional[str] = None
        
        self._lock = threading.Lock()
        
    def apply_book_snapshot(self, asset_id: str, bids: List[Dict], asks: List[Dict], 
                            last_trade_price: Optional[str] = None):
        with self._lock:
            if asset_id == self.token_id_yes:
                self.yes_bids = sorted(bids, key=lambda x: Decimal(x["price"]), reverse=True)[:ORDER_BOOK_DEPTH]
                self.yes_asks = sorted(asks, key=lambda x: Decimal(x["price"]))[:ORDER_BOOK_DEPTH]
                if last_trade_price:
                    self.yes_last_trade_price = last_trade_price
            elif asset_id == self.token_id_no:
                self.no_bids = sorted(bids, key=lambda x: Decimal(x["price"]), reverse=True)[:ORDER_BOOK_DEPTH]
                self.no_asks = sorted(asks, key=lambda x: Decimal(x["price"]))[:ORDER_BOOK_DEPTH]
                if last_trade_price:
                    self.no_last_trade_price = last_trade_price
                    
    def apply_price_change(self, asset_id: str, price: str, size: str, side: str):
        with self._lock:
            if asset_id == self.token_id_yes:
                self._update_level(self.yes_bids if side == "BUY" else self.yes_asks, price, size, side)
            elif asset_id == self.token_id_no:
                self._update_level(self.no_bids if side == "BUY" else self.no_asks, price, size, side)
                
    def _update_level(self, levels: List[Dict], price: str, size: str, side: str):
        price_dec = Decimal(price)
        size_dec = Decimal(size)
        
        for i, level in enumerate(levels):
            if Decimal(level["price"]) == price_dec:
                if size_dec == 0:
                    levels.pop(i)
                else:
                    levels[i]["size"] = size
                return
        
        if size_dec > 0:
            levels.append({"price": price, "size": size})
            is_bids = (side == "BUY")
            levels.sort(key=lambda x: Decimal(x["price"]), reverse=is_bids)
            while len(levels) > ORDER_BOOK_DEPTH:
                levels.pop()
                
    def update_last_trade_price(self, asset_id: str, price: str):
        with self._lock:
            if asset_id == self.token_id_yes:
                self.yes_last_trade_price = price
            elif asset_id == self.token_id_no:
                self.no_last_trade_price = price
                
    def get_snapshot(self) -> Dict:
        with self._lock:
            return {
                "yes_bids": list(self.yes_bids),
                "yes_asks": list(self.yes_asks),
                "no_bids": list(self.no_bids),
                "no_asks": list(self.no_asks),
                "yes_last_trade_price": self.yes_last_trade_price,
                "no_last_trade_price": self.no_last_trade_price,
            }
    
    def calculate_metrics(self) -> Dict:
        with self._lock:
            metrics = {}
            
            yes_best_bid = self.yes_bids[0]["price"] if self.yes_bids else None
            yes_best_ask = self.yes_asks[0]["price"] if self.yes_asks else None
            no_best_bid = self.no_bids[0]["price"] if self.no_bids else None
            no_best_ask = self.no_asks[0]["price"] if self.no_asks else None
            
            metrics["yes_spread"] = calculate_spread(yes_best_bid, yes_best_ask)
            metrics["no_spread"] = calculate_spread(no_best_bid, no_best_ask)
            metrics["yes_mid"] = calculate_mid(yes_best_bid, yes_best_ask)
            metrics["no_mid"] = calculate_mid(no_best_bid, no_best_ask)
            
            if yes_best_ask and no_best_ask:
                metrics["pair_cost_at_ask"] = str(Decimal(yes_best_ask) + Decimal(no_best_ask))
                metrics["arbitrage_spread"] = str(Decimal("1.0") - Decimal(metrics["pair_cost_at_ask"]))
            if yes_best_bid and no_best_bid:
                metrics["pair_cost_at_bid"] = str(Decimal(yes_best_bid) + Decimal(no_best_bid))
            
            def calc_depth(levels: List[Dict], n: int) -> str:
                return str(sum(Decimal(l["size"]) for l in levels[:n]))
            
            metrics["yes_bid_depth_5"] = calc_depth(self.yes_bids, 5)
            metrics["yes_ask_depth_5"] = calc_depth(self.yes_asks, 5)
            metrics["no_bid_depth_5"] = calc_depth(self.no_bids, 5)
            metrics["no_ask_depth_5"] = calc_depth(self.no_asks, 5)
            
            metrics["yes_bid_ask_imbalance"] = calculate_imbalance(
                metrics["yes_bid_depth_5"], metrics["yes_ask_depth_5"]
            )
            metrics["no_bid_ask_imbalance"] = calculate_imbalance(
                metrics["no_bid_depth_5"], metrics["no_ask_depth_5"]
            )
            
            return metrics


# =============================================================================
# MARKET STATE
# =============================================================================

@dataclass
class MarketState:
    asset: str
    market_id: str
    condition_id: str
    question: str
    token_id_yes: str
    token_id_no: str
    window_start_ts: int
    window_end_ts: int
    strike_price: Optional[str] = None
    
    order_book: OrderBookState = None
    crypto_price: Optional[str] = None
    crypto_price_ts_ms: Optional[int] = None
    
    def __post_init__(self):
        self.order_book = OrderBookState(self.token_id_yes, self.token_id_no)


# =============================================================================
# OPTIMIZED COLLECTOR
# =============================================================================

class OptimizedCollector:
    """Collector with storage optimizations."""
    
    def __init__(self, assets: Tuple[str, ...] = ("btc", "eth"), config: StorageConfig = None):
        self.assets = [a.upper() for a in assets]
        self.config = config or DEFAULT_CONFIG
        self.file_manager = OptimizedFileManager(self.config)
        
        self.markets: Dict[str, MarketState] = {}
        self._markets_lock = threading.Lock()
        
        self.crypto_prices: Dict[str, Tuple[str, int]] = {}
        self._prices_lock = threading.Lock()
        
        self.clob_ws: Optional[WebSocketApp] = None
        self.rtds_ws: Optional[WebSocketApp] = None
        
        self._stop = threading.Event()
        self._threads: List[threading.Thread] = []
        
        # For compact mode - track last snapshot time
        self._last_snapshot_time: Dict[str, float] = {}
        
    def update_markets(self):
        discovered = discover_active_crypto_15m(tuple(a.lower() for a in self.assets))
        
        with self._markets_lock:
            for asset in self.assets:
                info = discovered.get(asset)
                if not info:
                    continue
                    
                current = self.markets.get(asset)
                if current and current.market_id == info["market_id"]:
                    continue
                    
                from datetime import datetime
                end_time = datetime.fromisoformat(info["end_time"].replace("+00:00", "+00:00"))
                window_end_ts = int(end_time.timestamp())
                window_start_ts = window_end_ts - WINDOW_DURATION_SEC
                
                market = MarketState(
                    asset=asset,
                    market_id=info["market_id"],
                    condition_id=info["condition_id"],
                    question=info["question"],
                    token_id_yes=info["token_ids"][0],
                    token_id_no=info["token_ids"][1],
                    window_start_ts=window_start_ts,
                    window_end_ts=window_end_ts,
                )
                
                question = info["question"]
                if "$" in question:
                    import re
                    match = re.search(r'\$([0-9,]+)', question)
                    if match:
                        market.strike_price = match.group(1).replace(",", "")
                
                self.markets[asset] = market
                self._write_market_meta(market)
                
                print(f"[Market] {asset}: {info['question'][:50]}...")
                
    def _write_market_meta(self, market: MarketState):
        data_file = self.file_manager.get_file(market.asset, now_sec())
        
        record = UnifiedRecord(
            record_type="market_meta",
            recv_ts_ns=now_ns(),
            market_id=market.market_id,
            condition_id=market.condition_id,
            question=market.question,
            strike_price=market.strike_price,
            token_id_yes=market.token_id_yes,
            token_id_no=market.token_id_no,
        )
        
        data_file.write_record(record)
        
    def get_all_token_ids(self) -> List[str]:
        with self._markets_lock:
            token_ids = []
            for market in self.markets.values():
                token_ids.extend([market.token_id_yes, market.token_id_no])
            return token_ids
    
    def find_market_by_token(self, token_id: str) -> Optional[MarketState]:
        with self._markets_lock:
            for market in self.markets.values():
                if token_id in (market.token_id_yes, market.token_id_no):
                    return market
            return None
    
    def find_market_by_market_id(self, market_id: str) -> Optional[MarketState]:
        with self._markets_lock:
            for market in self.markets.values():
                if market.market_id == market_id:
                    return market
            return None
            
    def _snapshot_loop(self):
        """Write periodic snapshots."""
        while not self._stop.is_set():
            try:
                current_ts = now_sec()
                
                with self._markets_lock:
                    for asset, market in self.markets.items():
                        if current_ts >= market.window_end_ts - WINDOW_END_BUFFER_SEC:
                            continue
                        
                        # Check snapshot interval
                        last_snap = self._last_snapshot_time.get(asset, 0)
                        if current_ts - last_snap < self.config.snapshot_interval_sec:
                            continue
                        
                        self._last_snapshot_time[asset] = current_ts
                        
                        book = market.order_book.get_snapshot()
                        metrics = market.order_book.calculate_metrics()
                        
                        with self._prices_lock:
                            price_data = self.crypto_prices.get(asset.lower())
                            crypto_price = price_data[0] if price_data else None
                            crypto_ts = price_data[1] if price_data else None
                        
                        record = UnifiedRecord(
                            record_type="snapshot",
                            recv_ts_ns=now_ns(),
                            market_id=market.market_id,
                            token_id_yes=market.token_id_yes,
                            token_id_no=market.token_id_no,
                            crypto_price_usd=crypto_price,
                            crypto_price_ts_ms=crypto_ts,
                            yes_bids=book["yes_bids"],
                            yes_asks=book["yes_asks"],
                            yes_best_bid=book["yes_bids"][0]["price"] if book["yes_bids"] else None,
                            yes_best_ask=book["yes_asks"][0]["price"] if book["yes_asks"] else None,
                            yes_last_trade_price=book["yes_last_trade_price"],
                            no_bids=book["no_bids"],
                            no_asks=book["no_asks"],
                            no_best_bid=book["no_bids"][0]["price"] if book["no_bids"] else None,
                            no_best_ask=book["no_asks"][0]["price"] if book["no_asks"] else None,
                            no_last_trade_price=book["no_last_trade_price"],
                            metrics=metrics,
                        )
                        
                        data_file = self.file_manager.get_file(asset, current_ts)
                        data_file.write_record(record)
                        
            except Exception as e:
                print(f"[Snapshot] Error: {e}")
                
            time.sleep(0.5)  # Check twice per second
            
    def _rest_checkpoint_loop(self):
        while not self._stop.is_set():
            try:
                with self._markets_lock:
                    for market in self.markets.values():
                        for token_id in [market.token_id_yes, market.token_id_no]:
                            try:
                                resp = requests.get(
                                    f"{CLOB_REST_BASE}/book",
                                    params={"token_id": token_id},
                                    timeout=10
                                )
                                if resp.status_code == 200:
                                    book_data = resp.json()
                                    bids = [{"price": b["price"], "size": b["size"]} for b in book_data.get("bids", [])]
                                    asks = [{"price": a["price"], "size": a["size"]} for a in book_data.get("asks", [])]
                                    last_trade = book_data.get("last_trade_price")
                                    market.order_book.apply_book_snapshot(token_id, bids, asks, last_trade)
                            except:
                                pass
                                
            except Exception as e:
                print(f"[REST] Error: {e}")
                
            time.sleep(REST_BOOK_CHECKPOINT_INTERVAL)
            
    def _clob_on_open(self, ws):
        token_ids = self.get_all_token_ids()
        if not token_ids:
            return
            
        sub_msg = {
            "assets_ids": token_ids,
            "type": "market",
            "custom_feature_enabled": True,
        }
        ws.send(json.dumps(sub_msg))
        print(f"[CLOB WS] Subscribed to {len(token_ids)} tokens")
        
        def ping_loop():
            while not self._stop.is_set():
                try:
                    ws.send("PING")
                except:
                    break
                time.sleep(PING_INTERVAL_CLOB)
                
        threading.Thread(target=ping_loop, daemon=True).start()
        
    def _clob_on_message(self, ws, message: str):
        recv_ts_ns = now_ns()
        
        try:
            msg = json.loads(message)
        except:
            return
            
        event_type = msg.get("event_type")
        market_id = msg.get("market")
        
        market = self.find_market_by_market_id(market_id) if market_id else None
        if not market:
            asset_id = msg.get("asset_id")
            if asset_id:
                market = self.find_market_by_token(asset_id)
        
        if not market:
            return
            
        current_ts = recv_ts_ns / 1e9
        
        server_ts_ms = None
        try:
            server_ts_ms = int(msg.get("timestamp", 0))
        except:
            pass
        
        # Process by event type
        if event_type == "book":
            asset_id = msg.get("asset_id")
            bids = [{"price": b["price"], "size": b["size"]} for b in msg.get("bids", [])]
            asks = [{"price": a["price"], "size": a["size"]} for a in msg.get("asks", [])]
            
            market.order_book.apply_book_snapshot(asset_id, bids, asks)
            
            # Only store book updates if configured
            if self.config.store_book_updates:
                data_file = self.file_manager.get_file(market.asset, current_ts)
                record = UnifiedRecord(
                    record_type="book_update",
                    recv_ts_ns=recv_ts_ns,
                    server_ts_ms=server_ts_ms,
                    update_type="book",
                    market_id=market_id,
                    raw=msg if self.config.include_raw else None,
                )
                data_file.write_record(record)
            
        elif event_type == "price_change":
            for change in msg.get("price_changes", []):
                asset_id = change.get("asset_id")
                price = change.get("price")
                size = change.get("size")
                side = change.get("side")
                
                if asset_id and price and size and side:
                    market.order_book.apply_price_change(asset_id, price, size, side)
            
            # Only store if configured
            if self.config.store_book_updates:
                data_file = self.file_manager.get_file(market.asset, current_ts)
                record = UnifiedRecord(
                    record_type="book_update",
                    recv_ts_ns=recv_ts_ns,
                    server_ts_ms=server_ts_ms,
                    update_type="price_change",
                    market_id=market_id,
                    raw=msg if self.config.include_raw else None,
                )
                data_file.write_record(record)
            
        elif event_type == "last_trade_price":
            # ALWAYS store trades (they're important)
            asset_id = msg.get("asset_id")
            price = msg.get("price")
            size = msg.get("size")
            side = msg.get("side")
            fee_bps = msg.get("fee_rate_bps")
            tx_hash = msg.get("transaction_hash")
            
            if asset_id and price:
                market.order_book.update_last_trade_price(asset_id, price)
            
            data_file = self.file_manager.get_file(market.asset, current_ts)
            record = UnifiedRecord(
                record_type="trade",
                recv_ts_ns=recv_ts_ns,
                server_ts_ms=server_ts_ms,
                market_id=market_id,
                trade_asset_id=asset_id,
                trade_price=price,
                trade_size=size,
                trade_side=side,
                trade_fee_bps=str(fee_bps) if fee_bps else None,
                trade_tx_hash=tx_hash,
                raw=msg if self.config.include_raw else None,
            )
            data_file.write_record(record)
            
    def _clob_on_error(self, ws, error):
        print(f"[CLOB WS] Error: {error}")
        
    def _clob_on_close(self, ws, status_code, msg):
        print(f"[CLOB WS] Closed: {status_code}")
        
    def _run_clob_ws(self):
        url = f"{CLOB_WS_BASE}/ws/market"
        backoff = 1
        
        while not self._stop.is_set():
            try:
                self.clob_ws = WebSocketApp(
                    url,
                    on_open=self._clob_on_open,
                    on_message=self._clob_on_message,
                    on_error=self._clob_on_error,
                    on_close=self._clob_on_close,
                )
                self.clob_ws.run_forever()
            except Exception as e:
                print(f"[CLOB WS] Exception: {e}")
                
            if not self._stop.is_set():
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                
    def _rtds_on_open(self, ws):
        subs = []
        
        for asset in self.assets:
            symbol = f"{asset.lower()}/usd"
            subs.append({
                "topic": "crypto_prices_chainlink",
                "type": "*",
                "filters": json.dumps({"symbol": symbol}),
            })
            
        ws.send(json.dumps({
            "action": "subscribe",
            "subscriptions": subs,
        }))
        print(f"[RTDS WS] Subscribed to Chainlink prices")
        
        def ping_loop():
            while not self._stop.is_set():
                try:
                    ws.send("PING")
                except:
                    break
                time.sleep(PING_INTERVAL_RTDS)
                
        threading.Thread(target=ping_loop, daemon=True).start()
        
    def _rtds_on_message(self, ws, message: str):
        recv_ts_ns = now_ns()
        
        try:
            msg = json.loads(message)
        except:
            return
            
        topic = msg.get("topic", "")
        if "crypto_prices" not in topic:
            return
            
        payload = msg.get("payload", {})
        data_points = payload.get("data", [])
        symbol = payload.get("symbol", "").lower()
        
        asset = None
        for a in self.assets:
            if a.lower() in symbol:
                asset = a
                break
                
        if not asset or not data_points:
            return
            
        latest = data_points[-1] if data_points else None
        if latest:
            price = str(latest.get("value"))
            ts_ms = latest.get("timestamp")
            
            with self._prices_lock:
                self.crypto_prices[asset.lower()] = (price, ts_ms)
                
    def _rtds_on_error(self, ws, error):
        print(f"[RTDS WS] Error: {error}")
        
    def _rtds_on_close(self, ws, status_code, msg):
        print(f"[RTDS WS] Closed: {status_code}")
        
    def _run_rtds_ws(self):
        backoff = 1
        
        while not self._stop.is_set():
            try:
                self.rtds_ws = WebSocketApp(
                    RTDS_WS_URL,
                    on_open=self._rtds_on_open,
                    on_message=self._rtds_on_message,
                    on_error=self._rtds_on_error,
                    on_close=self._rtds_on_close,
                )
                self.rtds_ws.run_forever()
            except Exception as e:
                print(f"[RTDS WS] Exception: {e}")
                
            if not self._stop.is_set():
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                
    def _market_refresh_loop(self):
        while not self._stop.is_set():
            try:
                self.file_manager.close_old_windows(now_sec())
                self.update_markets()
            except Exception as e:
                print(f"[Refresh] Error: {e}")
            time.sleep(30)
            
    def start(self):
        print("=" * 60)
        print("OPTIMIZED POLYMARKET DATA COLLECTOR")
        print("=" * 60)
        print(f"Assets: {self.assets}")
        print(f"Compression: {'GZIP' if self.config.use_gzip else 'None'}")
        print(f"Include Raw: {self.config.include_raw}")
        print(f"Store Book Updates: {self.config.store_book_updates}")
        print(f"Snapshot Interval: {self.config.snapshot_interval_sec}s")
        print(f"Estimated Size: {self.config.estimate_size_reduction()}")
        print("=" * 60)
        
        self.update_markets()
        
        threads = [
            ("CLOB WS", self._run_clob_ws),
            ("RTDS WS", self._run_rtds_ws),
            ("Snapshot", self._snapshot_loop),
            ("REST", self._rest_checkpoint_loop),
            ("Refresh", self._market_refresh_loop),
        ]
        
        for name, target in threads:
            t = threading.Thread(target=target, name=name, daemon=True)
            t.start()
            self._threads.append(t)
            
        print("\n[Collector] Running. Press Ctrl+C to stop.\n")
        
    def stop(self):
        print("\n[Collector] Stopping...")
        self._stop.set()
        
        if self.clob_ws:
            try:
                self.clob_ws.close()
            except:
                pass
                
        if self.rtds_ws:
            try:
                self.rtds_ws.close()
            except:
                pass
                
        self.file_manager.close_all()
        print("[Collector] Stopped.")
        
    def run_forever(self):
        self.start()
        
        try:
            while not self._stop.is_set():
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        finally:
            self.stop()


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Optimized Polymarket Data Collector")
    parser.add_argument("--no-compress", action="store_true", help="Disable GZIP compression")
    parser.add_argument("--include-raw", action="store_true", help="Include raw field in records")
    parser.add_argument("--compact", action="store_true", help="Use compact mode (minimal storage)")
    parser.add_argument("--snapshot-interval", type=float, default=1.0, help="Seconds between snapshots")
    parser.add_argument("--no-book-updates", action="store_true", help="Don't store individual book updates")
    
    args = parser.parse_args()
    
    if args.compact:
        config = COMPACT_CONFIG
    else:
        config = StorageConfig(
            use_gzip=not args.no_compress,
            include_raw=args.include_raw,
            store_book_updates=not args.no_book_updates,
            snapshot_interval_sec=args.snapshot_interval,
        )
    
    collector = OptimizedCollector(assets=("btc", "eth"), config=config)
    
    def signal_handler(sig, frame):
        collector.stop()
        sys.exit(0)
        
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    collector.run_forever()


if __name__ == "__main__":
    main()


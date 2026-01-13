#!/usr/bin/env python3
"""
Unified Data Collector for Polymarket 15-Minute Crypto Events

Production-grade data collection system designed for strategy backtesting,
live simulation, and replay of any trading strategy.

Architecture:
- ONE JSONL file per 15-minute event per asset (BTC, ETH)
- Clear folder structure: data/{ASSET}/date={YYYY-MM-DD}/window_{INDEX:02d}_{start_ts}.jsonl
- 96 windows per day (24 hours × 4 windows/hour)
- Unified schema capturing ALL data for complete market replay

Schema Design Philosophy:
- Every record is self-contained with full context
- Timestamped to nanosecond precision for accurate sequencing  
- Includes both raw data and derived metrics
- Supports any strategy: Gabagool, momentum, arbitrage, market making, etc.

Author: Production-grade implementation for HFT backtesting
"""

import json
import os
import time
import threading
import signal
import sys
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from collections import deque

import requests
from websocket import WebSocketApp

from market_discovery import discover_active_crypto_15m, collect_asset_ids, fetch_crypto_15m_market


# =============================================================================
# CONFIGURATION
# =============================================================================

CLOB_WS_BASE = "wss://ws-subscriptions-clob.polymarket.com"
CLOB_REST_BASE = "https://clob.polymarket.com"
RTDS_WS_URL = "wss://ws-live-data.polymarket.com"
GAMMA_API_BASE = "https://gamma-api.polymarket.com"

# Data storage root
DATA_ROOT = Path("data")

# Collection parameters
PING_INTERVAL_CLOB = 10  # seconds
PING_INTERVAL_RTDS = 5   # seconds
BOOK_SNAPSHOT_INTERVAL = 1.0  # Full book snapshot every N seconds
REST_BOOK_CHECKPOINT_INTERVAL = 30  # REST book checkpoint every N seconds

# Order book depth to capture (10 levels each side is sufficient for most strategies)
ORDER_BOOK_DEPTH = 10

# 15-minute window duration
WINDOW_DURATION_SEC = 900  # 15 minutes = 900 seconds

# Buffer before window end to stop collecting (to ensure clean data)
WINDOW_END_BUFFER_SEC = 5


# =============================================================================
# UNIFIED DATA SCHEMA
# =============================================================================

SCHEMA_VERSION = 2  # Increment when schema changes

@dataclass
class OrderBookLevel:
    """Single price level in order book."""
    price: str
    size: str

@dataclass
class OrderBookSide:
    """One side of order book (bids or asks)."""
    levels: List[OrderBookLevel] = field(default_factory=list)
    
    @property
    def best_price(self) -> Optional[str]:
        return self.levels[0].price if self.levels else None
    
    @property
    def total_size(self) -> str:
        total = sum(Decimal(l.size) for l in self.levels)
        return str(total)
    
    def depth_at_levels(self, n: int) -> str:
        """Total size at top N levels."""
        total = sum(Decimal(self.levels[i].size) for i in range(min(n, len(self.levels))))
        return str(total)

@dataclass  
class AssetOrderBook:
    """Order book for one asset (YES or NO token)."""
    asset_id: str
    bids: OrderBookSide = field(default_factory=OrderBookSide)
    asks: OrderBookSide = field(default_factory=OrderBookSide)
    last_trade_price: Optional[str] = None
    book_hash: Optional[str] = None
    timestamp_ms: Optional[int] = None

@dataclass
class MarketOrderBook:
    """Complete order book for a market (both YES and NO tokens)."""
    yes_book: AssetOrderBook = field(default_factory=AssetOrderBook)
    no_book: AssetOrderBook = field(default_factory=AssetOrderBook)

@dataclass
class DerivedMetrics:
    """Calculated metrics for strategy analysis."""
    # Spreads
    yes_spread: Optional[str] = None  # yes_ask - yes_bid
    no_spread: Optional[str] = None   # no_ask - no_bid
    
    # Mid prices
    yes_mid: Optional[str] = None
    no_mid: Optional[str] = None
    
    # Combined pair metrics (for Gabagool-style strategies)
    pair_cost_at_ask: Optional[str] = None  # yes_ask + no_ask
    pair_cost_at_bid: Optional[str] = None  # yes_bid + no_bid
    arbitrage_spread: Optional[str] = None  # 1.0 - pair_cost_at_ask
    
    # Liquidity depth at various levels
    yes_bid_depth_3: Optional[str] = None
    yes_bid_depth_5: Optional[str] = None
    yes_bid_depth_10: Optional[str] = None
    yes_ask_depth_3: Optional[str] = None
    yes_ask_depth_5: Optional[str] = None
    yes_ask_depth_10: Optional[str] = None
    no_bid_depth_3: Optional[str] = None
    no_bid_depth_5: Optional[str] = None
    no_bid_depth_10: Optional[str] = None
    no_ask_depth_3: Optional[str] = None
    no_ask_depth_5: Optional[str] = None
    no_ask_depth_10: Optional[str] = None
    
    # Imbalance metrics
    yes_bid_ask_imbalance: Optional[str] = None  # (bid_depth - ask_depth) / (bid_depth + ask_depth)
    no_bid_ask_imbalance: Optional[str] = None


@dataclass
class UnifiedRecord:
    """
    UNIFIED SCHEMA: One record type for all data points.
    
    This is the master schema that captures everything needed
    for replaying any trading strategy.
    
    Record Types:
    - 'snapshot': Periodic full state capture (order books + crypto price)
    - 'book_update': Order book change from WebSocket
    - 'trade': Trade execution event
    - 'price_tick': Crypto price update from Chainlink
    - 'market_meta': Market metadata (start of window)
    - 'window_end': End of window marker
    """
    
    # === Core Identifiers ===
    schema_version: int = SCHEMA_VERSION
    record_type: str = "snapshot"  # snapshot, book_update, trade, price_tick, market_meta, window_end
    
    # === Timing ===
    recv_ts_ns: int = 0  # Local receive timestamp (nanoseconds)
    server_ts_ms: Optional[int] = None  # Server timestamp (milliseconds)
    
    # === Window Context ===
    window_index: int = 0  # 0-95 for which 15-min window of the day
    window_start_ts: int = 0  # Unix timestamp of window start
    window_end_ts: int = 0  # Unix timestamp of window end
    elapsed_sec: float = 0.0  # Seconds since window start
    remaining_sec: float = 0.0  # Seconds until window end
    
    # === Asset Context ===
    asset: str = ""  # BTC, ETH
    
    # === Market Metadata (populated for market_meta, included in snapshots) ===
    market_id: Optional[str] = None
    condition_id: Optional[str] = None
    question: Optional[str] = None
    strike_price: Optional[str] = None
    token_id_yes: Optional[str] = None
    token_id_no: Optional[str] = None
    
    # === Crypto Price (from Chainlink via RTDS) ===
    crypto_price_usd: Optional[str] = None
    crypto_price_ts_ms: Optional[int] = None
    
    # === Order Book State ===
    # YES token book (top N levels)
    yes_bids: Optional[List[Dict]] = None  # [{price, size}, ...]
    yes_asks: Optional[List[Dict]] = None
    yes_best_bid: Optional[str] = None
    yes_best_ask: Optional[str] = None
    yes_last_trade_price: Optional[str] = None
    
    # NO token book (top N levels)
    no_bids: Optional[List[Dict]] = None
    no_asks: Optional[List[Dict]] = None
    no_best_bid: Optional[str] = None
    no_best_ask: Optional[str] = None
    no_last_trade_price: Optional[str] = None
    
    # === Derived Metrics ===
    metrics: Optional[Dict] = None
    
    # === Event-Specific Data ===
    # For trade events
    trade_side: Optional[str] = None  # BUY/SELL
    trade_price: Optional[str] = None
    trade_size: Optional[str] = None
    trade_fee_bps: Optional[str] = None
    trade_tx_hash: Optional[str] = None
    trade_asset_id: Optional[str] = None
    
    # For book_update events
    update_type: Optional[str] = None  # price_change, best_bid_ask, book
    
    # === Raw Data (for debugging/advanced analysis) ===
    raw: Optional[Dict] = None


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def now_ns() -> int:
    """Current time in nanoseconds."""
    return time.time_ns()

def now_sec() -> float:
    """Current time in seconds."""
    return time.time()

def utc_date_str(ts_sec: float) -> str:
    """Get UTC date string from timestamp."""
    return datetime.fromtimestamp(ts_sec, tz=timezone.utc).strftime("%Y-%m-%d")

def calculate_window_index(ts_sec: float) -> int:
    """
    Calculate which 15-minute window (0-95) this timestamp falls into.
    Windows are aligned to UTC midnight.
    """
    dt = datetime.fromtimestamp(ts_sec, tz=timezone.utc)
    minutes_since_midnight = dt.hour * 60 + dt.minute
    return minutes_since_midnight // 15

def get_window_boundaries(ts_sec: float) -> Tuple[int, int]:
    """Get start and end timestamps for the 15-minute window containing ts_sec."""
    window_start = int(ts_sec // WINDOW_DURATION_SEC) * WINDOW_DURATION_SEC
    window_end = window_start + WINDOW_DURATION_SEC
    return window_start, window_end

def safe_decimal(value: Any) -> Optional[str]:
    """Convert value to decimal string safely."""
    if value is None:
        return None
    try:
        return str(Decimal(str(value)))
    except:
        return None

def calculate_spread(bid: Optional[str], ask: Optional[str]) -> Optional[str]:
    """Calculate spread between bid and ask."""
    if bid is None or ask is None:
        return None
    try:
        return str(Decimal(ask) - Decimal(bid))
    except:
        return None

def calculate_mid(bid: Optional[str], ask: Optional[str]) -> Optional[str]:
    """Calculate mid price."""
    if bid is None or ask is None:
        return None
    try:
        return str((Decimal(bid) + Decimal(ask)) / 2)
    except:
        return None

def calculate_imbalance(bid_depth: str, ask_depth: str) -> Optional[str]:
    """Calculate bid/ask imbalance ratio."""
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
# DATA FILE MANAGEMENT
# =============================================================================

class WindowDataFile:
    """
    Manages a single JSONL file for one 15-minute window.
    
    File naming: window_{INDEX:02d}_{start_ts}.jsonl
    Example: window_42_1768250400.jsonl (42nd window of the day, starting at that unix ts)
    """
    
    def __init__(self, asset: str, window_start_ts: int):
        self.asset = asset.upper()
        self.window_start_ts = window_start_ts
        self.window_end_ts = window_start_ts + WINDOW_DURATION_SEC
        
        # Calculate window index (0-95)
        dt = datetime.fromtimestamp(window_start_ts, tz=timezone.utc)
        minutes_since_midnight = dt.hour * 60 + dt.minute
        self.window_index = minutes_since_midnight // 15
        
        # Build file path
        date_str = dt.strftime("%Y-%m-%d")
        filename = f"window_{self.window_index:02d}_{window_start_ts}.jsonl"
        
        self.path = DATA_ROOT / self.asset / f"date={date_str}" / filename
        self.path.parent.mkdir(parents=True, exist_ok=True)
        
        self._file = None
        self._lock = threading.Lock()
        self._record_count = 0
        
    def write_record(self, record: UnifiedRecord):
        """Write a record to the file."""
        with self._lock:
            if self._file is None:
                self._file = open(self.path, "a", encoding="utf-8")
            
            # Add window context
            record.window_index = self.window_index
            record.window_start_ts = self.window_start_ts
            record.window_end_ts = self.window_end_ts
            record.asset = self.asset
            
            # Calculate elapsed/remaining time
            current_sec = record.recv_ts_ns / 1e9
            record.elapsed_sec = round(current_sec - self.window_start_ts, 3)
            record.remaining_sec = round(self.window_end_ts - current_sec, 3)
            
            # Serialize
            data = self._record_to_dict(record)
            line = json.dumps(data, separators=(",", ":"), ensure_ascii=False)
            
            self._file.write(line + "\n")
            self._file.flush()
            self._record_count += 1
            
    def _record_to_dict(self, record: UnifiedRecord) -> Dict:
        """Convert record to dictionary, removing None values."""
        data = {}
        for k, v in asdict(record).items():
            if v is not None:
                data[k] = v
        return data
    
    def close(self):
        """Close the file."""
        with self._lock:
            if self._file:
                self._file.close()
                self._file = None
                
    @property
    def record_count(self) -> int:
        return self._record_count


class DataFileManager:
    """
    Manages data files for multiple assets and windows.
    Handles window transitions automatically.
    """
    
    def __init__(self):
        self._files: Dict[str, WindowDataFile] = {}  # key: "{asset}_{window_start}"
        self._lock = threading.Lock()
        
    def get_file(self, asset: str, ts_sec: float) -> WindowDataFile:
        """Get or create the appropriate data file for this asset and timestamp."""
        window_start, _ = get_window_boundaries(ts_sec)
        key = f"{asset.upper()}_{window_start}"
        
        with self._lock:
            if key not in self._files:
                self._files[key] = WindowDataFile(asset, window_start)
            return self._files[key]
    
    def close_old_windows(self, current_ts_sec: float):
        """Close files for windows that have ended."""
        current_window_start, _ = get_window_boundaries(current_ts_sec)
        
        with self._lock:
            to_close = []
            for key, wf in self._files.items():
                if wf.window_end_ts < current_ts_sec:
                    to_close.append(key)
            
            for key in to_close:
                wf = self._files.pop(key)
                # Write window end marker
                end_record = UnifiedRecord(
                    record_type="window_end",
                    recv_ts_ns=now_ns(),
                )
                wf.write_record(end_record)
                wf.close()
                print(f"[DataFileManager] Closed {wf.path} with {wf.record_count} records")
                
    def close_all(self):
        """Close all open files."""
        with self._lock:
            for wf in self._files.values():
                wf.close()
            self._files.clear()


# =============================================================================
# ORDER BOOK STATE MANAGER
# =============================================================================

class OrderBookState:
    """
    Maintains current order book state for a market.
    Applies incremental updates and provides snapshots.
    """
    
    def __init__(self, token_id_yes: str, token_id_no: str):
        self.token_id_yes = token_id_yes
        self.token_id_no = token_id_no
        
        # Current order book state
        self.yes_bids: List[Dict] = []
        self.yes_asks: List[Dict] = []
        self.no_bids: List[Dict] = []
        self.no_asks: List[Dict] = []
        
        self.yes_last_trade_price: Optional[str] = None
        self.no_last_trade_price: Optional[str] = None
        
        self._lock = threading.Lock()
        
    def apply_book_snapshot(self, asset_id: str, bids: List[Dict], asks: List[Dict], 
                            last_trade_price: Optional[str] = None):
        """Apply a full order book snapshot."""
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
        """Apply an incremental price change update."""
        with self._lock:
            if asset_id == self.token_id_yes:
                self._update_level(self.yes_bids if side == "BUY" else self.yes_asks, price, size, side)
            elif asset_id == self.token_id_no:
                self._update_level(self.no_bids if side == "BUY" else self.no_asks, price, size, side)
                
    def _update_level(self, levels: List[Dict], price: str, size: str, side: str):
        """Update a single price level."""
        price_dec = Decimal(price)
        size_dec = Decimal(size)
        
        # Find existing level
        for i, level in enumerate(levels):
            if Decimal(level["price"]) == price_dec:
                if size_dec == 0:
                    levels.pop(i)
                else:
                    levels[i]["size"] = size
                return
        
        # Add new level
        if size_dec > 0:
            levels.append({"price": price, "size": size})
            # Re-sort
            is_bids = (side == "BUY")
            levels.sort(key=lambda x: Decimal(x["price"]), reverse=is_bids)
            # Trim to depth
            while len(levels) > ORDER_BOOK_DEPTH:
                levels.pop()
                
    def update_last_trade_price(self, asset_id: str, price: str):
        """Update last trade price."""
        with self._lock:
            if asset_id == self.token_id_yes:
                self.yes_last_trade_price = price
            elif asset_id == self.token_id_no:
                self.no_last_trade_price = price
                
    def get_snapshot(self) -> Dict:
        """Get current order book state."""
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
        """Calculate derived metrics from current state."""
        with self._lock:
            metrics = {}
            
            # Best prices
            yes_best_bid = self.yes_bids[0]["price"] if self.yes_bids else None
            yes_best_ask = self.yes_asks[0]["price"] if self.yes_asks else None
            no_best_bid = self.no_bids[0]["price"] if self.no_bids else None
            no_best_ask = self.no_asks[0]["price"] if self.no_asks else None
            
            # Spreads
            metrics["yes_spread"] = calculate_spread(yes_best_bid, yes_best_ask)
            metrics["no_spread"] = calculate_spread(no_best_bid, no_best_ask)
            
            # Mid prices
            metrics["yes_mid"] = calculate_mid(yes_best_bid, yes_best_ask)
            metrics["no_mid"] = calculate_mid(no_best_bid, no_best_ask)
            
            # Pair costs (for Gabagool strategy)
            if yes_best_ask and no_best_ask:
                metrics["pair_cost_at_ask"] = str(Decimal(yes_best_ask) + Decimal(no_best_ask))
                metrics["arbitrage_spread"] = str(Decimal("1.0") - Decimal(metrics["pair_cost_at_ask"]))
            if yes_best_bid and no_best_bid:
                metrics["pair_cost_at_bid"] = str(Decimal(yes_best_bid) + Decimal(no_best_bid))
            
            # Depth calculations
            def calc_depth(levels: List[Dict], n: int) -> str:
                return str(sum(Decimal(l["size"]) for l in levels[:n]))
            
            metrics["yes_bid_depth_3"] = calc_depth(self.yes_bids, 3)
            metrics["yes_bid_depth_5"] = calc_depth(self.yes_bids, 5)
            metrics["yes_bid_depth_10"] = calc_depth(self.yes_bids, 10)
            metrics["yes_ask_depth_3"] = calc_depth(self.yes_asks, 3)
            metrics["yes_ask_depth_5"] = calc_depth(self.yes_asks, 5)
            metrics["yes_ask_depth_10"] = calc_depth(self.yes_asks, 10)
            
            metrics["no_bid_depth_3"] = calc_depth(self.no_bids, 3)
            metrics["no_bid_depth_5"] = calc_depth(self.no_bids, 5)
            metrics["no_bid_depth_10"] = calc_depth(self.no_bids, 10)
            metrics["no_ask_depth_3"] = calc_depth(self.no_asks, 3)
            metrics["no_ask_depth_5"] = calc_depth(self.no_asks, 5)
            metrics["no_ask_depth_10"] = calc_depth(self.no_asks, 10)
            
            # Imbalance
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
    """State for a single 15-minute market."""
    asset: str  # BTC, ETH
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
# UNIFIED COLLECTOR
# =============================================================================

class UnifiedCollector:
    """
    Main collector that coordinates all data sources and writes unified records.
    
    Data Sources:
    1. CLOB WebSocket - Order book updates, trades, price changes
    2. RTDS WebSocket - Chainlink crypto prices
    3. CLOB REST - Periodic order book snapshots
    4. Gamma API - Market discovery
    """
    
    def __init__(self, assets: Tuple[str, ...] = ("btc", "eth")):
        self.assets = [a.upper() for a in assets]
        self.file_manager = DataFileManager()
        
        # Current markets by asset
        self.markets: Dict[str, MarketState] = {}
        self._markets_lock = threading.Lock()
        
        # Latest crypto prices
        self.crypto_prices: Dict[str, Tuple[str, int]] = {}  # asset -> (price, ts_ms)
        self._prices_lock = threading.Lock()
        
        # WebSocket connections
        self.clob_ws: Optional[WebSocketApp] = None
        self.rtds_ws: Optional[WebSocketApp] = None
        
        # Control
        self._stop = threading.Event()
        self._threads: List[threading.Thread] = []
        
    def discover_markets(self) -> Dict[str, Optional[Dict]]:
        """Discover active 15-minute markets for all assets."""
        return discover_active_crypto_15m(tuple(a.lower() for a in self.assets))
    
    def update_markets(self):
        """Update market state with latest discovered markets."""
        discovered = self.discover_markets()
        
        with self._markets_lock:
            for asset in self.assets:
                info = discovered.get(asset)
                if not info:
                    continue
                    
                # Check if this is a new market
                current = self.markets.get(asset)
                if current and current.market_id == info["market_id"]:
                    continue  # Same market
                    
                # Parse window boundaries
                from datetime import datetime
                end_time = datetime.fromisoformat(info["end_time"].replace("+00:00", "+00:00"))
                window_end_ts = int(end_time.timestamp())
                window_start_ts = window_end_ts - WINDOW_DURATION_SEC
                
                # Create new market state
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
                
                # Try to extract strike price from question
                question = info["question"]
                if "$" in question:
                    import re
                    match = re.search(r'\$([0-9,]+)', question)
                    if match:
                        market.strike_price = match.group(1).replace(",", "")
                
                self.markets[asset] = market
                
                # Write market metadata record
                self._write_market_meta(market)
                
                print(f"[Collector] New market for {asset}: {info['question'][:50]}...")
                
    def _write_market_meta(self, market: MarketState):
        """Write market metadata record at start of window."""
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
        """Get all token IDs for current markets."""
        with self._markets_lock:
            token_ids = []
            for market in self.markets.values():
                token_ids.extend([market.token_id_yes, market.token_id_no])
            return token_ids
    
    def find_market_by_token(self, token_id: str) -> Optional[MarketState]:
        """Find market by token ID."""
        with self._markets_lock:
            for market in self.markets.values():
                if token_id in (market.token_id_yes, market.token_id_no):
                    return market
            return None
    
    def find_market_by_market_id(self, market_id: str) -> Optional[MarketState]:
        """Find market by market ID."""
        with self._markets_lock:
            for market in self.markets.values():
                if market.market_id == market_id:
                    return market
            return None
            
    # -------------------------------------------------------------------------
    # SNAPSHOT WRITER
    # -------------------------------------------------------------------------
    
    def _snapshot_loop(self):
        """Write periodic snapshots with full state."""
        while not self._stop.is_set():
            try:
                current_ts = now_sec()
                
                with self._markets_lock:
                    for asset, market in self.markets.items():
                        # Check if window is still active
                        if current_ts >= market.window_end_ts - WINDOW_END_BUFFER_SEC:
                            continue
                            
                        # Get order book snapshot
                        book = market.order_book.get_snapshot()
                        metrics = market.order_book.calculate_metrics()
                        
                        # Get crypto price
                        with self._prices_lock:
                            price_data = self.crypto_prices.get(asset.lower())
                            crypto_price = price_data[0] if price_data else None
                            crypto_ts = price_data[1] if price_data else None
                        
                        # Create snapshot record
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
                        
                        # Write to file
                        data_file = self.file_manager.get_file(asset, current_ts)
                        data_file.write_record(record)
                        
            except Exception as e:
                print(f"[Snapshot] Error: {e}")
                
            time.sleep(BOOK_SNAPSHOT_INTERVAL)
            
    # -------------------------------------------------------------------------
    # REST BOOK FETCHER
    # -------------------------------------------------------------------------
    
    def _fetch_rest_book(self, token_id: str) -> Optional[Dict]:
        """Fetch order book from REST API."""
        try:
            resp = requests.get(
                f"{CLOB_REST_BASE}/book",
                params={"token_id": token_id},
                timeout=10
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"[REST] Error fetching book for {token_id[:20]}...: {e}")
            return None
            
    def _rest_checkpoint_loop(self):
        """Periodically fetch full order book from REST API."""
        while not self._stop.is_set():
            try:
                with self._markets_lock:
                    for market in self.markets.values():
                        for token_id in [market.token_id_yes, market.token_id_no]:
                            book_data = self._fetch_rest_book(token_id)
                            if book_data:
                                bids = [{"price": b["price"], "size": b["size"]} for b in book_data.get("bids", [])]
                                asks = [{"price": a["price"], "size": a["size"]} for a in book_data.get("asks", [])]
                                last_trade = book_data.get("last_trade_price")
                                
                                market.order_book.apply_book_snapshot(token_id, bids, asks, last_trade)
                                
            except Exception as e:
                print(f"[REST Checkpoint] Error: {e}")
                
            time.sleep(REST_BOOK_CHECKPOINT_INTERVAL)
            
    # -------------------------------------------------------------------------
    # CLOB WEBSOCKET
    # -------------------------------------------------------------------------
    
    def _clob_on_open(self, ws):
        """Handle CLOB WebSocket open."""
        token_ids = self.get_all_token_ids()
        if not token_ids:
            print("[CLOB WS] No tokens to subscribe to")
            return
            
        sub_msg = {
            "assets_ids": token_ids,
            "type": "market",
            "custom_feature_enabled": True,
        }
        ws.send(json.dumps(sub_msg))
        print(f"[CLOB WS] Subscribed to {len(token_ids)} tokens")
        
        # Start ping thread
        def ping_loop():
            while not self._stop.is_set():
                try:
                    ws.send("PING")
                except:
                    break
                time.sleep(PING_INTERVAL_CLOB)
                
        threading.Thread(target=ping_loop, daemon=True).start()
        
    def _clob_on_message(self, ws, message: str):
        """Handle CLOB WebSocket message."""
        recv_ts_ns = now_ns()
        
        try:
            msg = json.loads(message)
        except:
            return
            
        event_type = msg.get("event_type")
        market_id = msg.get("market")
        
        market = self.find_market_by_market_id(market_id) if market_id else None
        if not market:
            # Try to find by asset_id in message
            asset_id = msg.get("asset_id")
            if asset_id:
                market = self.find_market_by_token(asset_id)
        
        if not market:
            return
            
        current_ts = recv_ts_ns / 1e9
        data_file = self.file_manager.get_file(market.asset, current_ts)
        
        server_ts_ms = None
        try:
            server_ts_ms = int(msg.get("timestamp", 0))
        except:
            pass
        
        # Process by event type
        if event_type == "book":
            # Full order book update
            asset_id = msg.get("asset_id")
            bids = [{"price": b["price"], "size": b["size"]} for b in msg.get("bids", [])]
            asks = [{"price": a["price"], "size": a["size"]} for a in msg.get("asks", [])]
            
            market.order_book.apply_book_snapshot(asset_id, bids, asks)
            
            # Write book_update record
            record = UnifiedRecord(
                record_type="book_update",
                recv_ts_ns=recv_ts_ns,
                server_ts_ms=server_ts_ms,
                update_type="book",
                market_id=market_id,
                raw=msg,
            )
            data_file.write_record(record)
            
        elif event_type == "price_change":
            # Price level changes
            for change in msg.get("price_changes", []):
                asset_id = change.get("asset_id")
                price = change.get("price")
                size = change.get("size")
                side = change.get("side")
                
                if asset_id and price and size and side:
                    market.order_book.apply_price_change(asset_id, price, size, side)
                    
            # Write book_update record
            record = UnifiedRecord(
                record_type="book_update",
                recv_ts_ns=recv_ts_ns,
                server_ts_ms=server_ts_ms,
                update_type="price_change",
                market_id=market_id,
                raw=msg,
            )
            data_file.write_record(record)
            
        elif event_type == "last_trade_price":
            # Trade executed
            asset_id = msg.get("asset_id")
            price = msg.get("price")
            size = msg.get("size")
            side = msg.get("side")
            fee_bps = msg.get("fee_rate_bps")
            tx_hash = msg.get("transaction_hash")
            
            if asset_id and price:
                market.order_book.update_last_trade_price(asset_id, price)
                
            # Write trade record
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
                raw=msg,
            )
            data_file.write_record(record)
            
        elif event_type == "best_bid_ask":
            # Best bid/ask update
            record = UnifiedRecord(
                record_type="book_update",
                recv_ts_ns=recv_ts_ns,
                server_ts_ms=server_ts_ms,
                update_type="best_bid_ask",
                market_id=market_id,
                raw=msg,
            )
            data_file.write_record(record)
            
    def _clob_on_error(self, ws, error):
        print(f"[CLOB WS] Error: {error}")
        
    def _clob_on_close(self, ws, status_code, msg):
        print(f"[CLOB WS] Closed: {status_code} {msg}")
        
    def _run_clob_ws(self):
        """Run CLOB WebSocket with reconnection."""
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
                
    # -------------------------------------------------------------------------
    # RTDS WEBSOCKET (Chainlink Prices)
    # -------------------------------------------------------------------------
    
    def _rtds_on_open(self, ws):
        """Handle RTDS WebSocket open."""
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
        print(f"[RTDS WS] Subscribed to Chainlink prices for {self.assets}")
        
        # Ping thread
        def ping_loop():
            while not self._stop.is_set():
                try:
                    ws.send("PING")
                except:
                    break
                time.sleep(PING_INTERVAL_RTDS)
                
        threading.Thread(target=ping_loop, daemon=True).start()
        
    def _rtds_on_message(self, ws, message: str):
        """Handle RTDS WebSocket message."""
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
        
        # Determine asset
        asset = None
        for a in self.assets:
            if a.lower() in symbol:
                asset = a
                break
                
        if not asset or not data_points:
            return
            
        # Get latest price
        latest = data_points[-1] if data_points else None
        if latest:
            price = str(latest.get("value"))
            ts_ms = latest.get("timestamp")
            
            with self._prices_lock:
                self.crypto_prices[asset.lower()] = (price, ts_ms)
                
            # Write price tick record if we have an active market
            with self._markets_lock:
                market = self.markets.get(asset)
                
            if market:
                current_ts = recv_ts_ns / 1e9
                data_file = self.file_manager.get_file(asset, current_ts)
                
                record = UnifiedRecord(
                    record_type="price_tick",
                    recv_ts_ns=recv_ts_ns,
                    server_ts_ms=ts_ms,
                    crypto_price_usd=price,
                    crypto_price_ts_ms=ts_ms,
                )
                data_file.write_record(record)
                
    def _rtds_on_error(self, ws, error):
        print(f"[RTDS WS] Error: {error}")
        
    def _rtds_on_close(self, ws, status_code, msg):
        print(f"[RTDS WS] Closed: {status_code} {msg}")
        
    def _run_rtds_ws(self):
        """Run RTDS WebSocket with reconnection."""
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
                
    # -------------------------------------------------------------------------
    # MARKET REFRESH LOOP
    # -------------------------------------------------------------------------
    
    def _market_refresh_loop(self):
        """Periodically refresh markets and handle window transitions."""
        while not self._stop.is_set():
            try:
                # Close old window files
                self.file_manager.close_old_windows(now_sec())
                
                # Refresh markets
                self.update_markets()
                
                # Reconnect WebSockets if needed (to subscribe to new tokens)
                # This is handled by the websocket loop naturally
                
            except Exception as e:
                print(f"[Market Refresh] Error: {e}")
                
            time.sleep(30)  # Check every 30 seconds
            
    # -------------------------------------------------------------------------
    # MAIN RUN
    # -------------------------------------------------------------------------
    
    def start(self):
        """Start the collector."""
        print("=" * 60)
        print("UNIFIED POLYMARKET DATA COLLECTOR")
        print(f"Assets: {self.assets}")
        print(f"Data root: {DATA_ROOT.absolute()}")
        print("=" * 60)
        
        # Initial market discovery
        self.update_markets()
        
        if not self.markets:
            print("[Collector] WARNING: No active markets found. Will retry...")
        
        # Start threads
        threads = [
            ("CLOB WS", self._run_clob_ws),
            ("RTDS WS", self._run_rtds_ws),
            ("Snapshot", self._snapshot_loop),
            ("REST Checkpoint", self._rest_checkpoint_loop),
            ("Market Refresh", self._market_refresh_loop),
        ]
        
        for name, target in threads:
            t = threading.Thread(target=target, name=name, daemon=True)
            t.start()
            self._threads.append(t)
            print(f"[Collector] Started thread: {name}")
            
        print("\n[Collector] All threads started. Press Ctrl+C to stop.\n")
        
    def stop(self):
        """Stop the collector."""
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
        """Run until interrupted."""
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
    # Handle SIGTERM gracefully
    collector = UnifiedCollector(assets=("btc", "eth"))
    
    def signal_handler(sig, frame):
        collector.stop()
        sys.exit(0)
        
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    collector.run_forever()


if __name__ == "__main__":
    main()


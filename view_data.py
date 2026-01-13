#!/usr/bin/env python3
"""
Quick Data Viewer for Polymarket Collected Data

Usage:
    python view_data.py <file.jsonl>                  # View file summary
    python view_data.py <file.jsonl> --snapshots      # View all snapshots
    python view_data.py <file.jsonl> --trades         # View all trades
    python view_data.py <file.jsonl> --record N       # View specific record
    python view_data.py latest                        # View latest file
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from decimal import Decimal


def find_latest_file() -> Path:
    """Find the most recently modified data file."""
    data_dir = Path("data")
    files = list(data_dir.rglob("window_*.jsonl"))
    if not files:
        print("No data files found")
        sys.exit(1)
    return max(files, key=lambda f: f.stat().st_mtime)


def load_records(file_path: Path):
    """Load all records from file."""
    records = []
    with open(file_path) as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))
    return records


def format_timestamp(ts_ns: int) -> str:
    """Format nanosecond timestamp to human readable."""
    dt = datetime.fromtimestamp(ts_ns / 1e9, tz=timezone.utc)
    return dt.strftime("%H:%M:%S.%f")[:-3]


def print_summary(records, file_path):
    """Print file summary."""
    print(f"\n{'='*60}")
    print(f"📁 FILE: {file_path}")
    print(f"{'='*60}")
    
    # Get metadata
    meta = next((r for r in records if r.get("record_type") == "market_meta"), None)
    if meta:
        print(f"\n📋 MARKET:")
        print(f"   Asset: {meta.get('asset')}")
        print(f"   Question: {meta.get('question', 'N/A')}")
        print(f"   Window Index: {meta.get('window_index')}/96")
        print(f"   Token YES: {meta.get('token_id_yes', 'N/A')[:20]}...")
        print(f"   Token NO: {meta.get('token_id_no', 'N/A')[:20]}...")
    
    # Count records
    by_type = {}
    for r in records:
        rtype = r.get("record_type", "unknown")
        by_type[rtype] = by_type.get(rtype, 0) + 1
    
    print(f"\n📊 RECORDS ({len(records)} total):")
    for rtype, count in sorted(by_type.items()):
        print(f"   {rtype}: {count}")
    
    # Time range
    timestamps = [r.get("recv_ts_ns", 0) for r in records if r.get("recv_ts_ns")]
    if timestamps:
        first_ts = min(timestamps)
        last_ts = max(timestamps)
        duration = (last_ts - first_ts) / 1e9
        print(f"\n⏱️  TIME RANGE:")
        print(f"   Start: {format_timestamp(first_ts)}")
        print(f"   End: {format_timestamp(last_ts)}")
        print(f"   Duration: {duration:.1f} seconds")
    
    # Latest snapshot
    snapshots = [r for r in records if r.get("record_type") == "snapshot"]
    if snapshots:
        latest = snapshots[-1]
        print(f"\n📸 LATEST SNAPSHOT:")
        print(f"   Elapsed: {latest.get('elapsed_sec', 0):.1f}s / Remaining: {latest.get('remaining_sec', 0):.1f}s")
        
        if latest.get("crypto_price_usd"):
            print(f"   Crypto Price: ${float(latest['crypto_price_usd']):,.2f}")
        
        if latest.get("yes_best_bid") and latest.get("yes_best_ask"):
            print(f"   YES: bid={latest['yes_best_bid']} / ask={latest['yes_best_ask']}")
        if latest.get("no_best_bid") and latest.get("no_best_ask"):
            print(f"   NO:  bid={latest['no_best_bid']} / ask={latest['no_best_ask']}")
        
        metrics = latest.get("metrics", {})
        if metrics.get("pair_cost_at_ask"):
            print(f"   Pair Cost @ Ask: {metrics['pair_cost_at_ask']}")
        if metrics.get("arbitrage_spread"):
            arb = float(metrics['arbitrage_spread']) * 100
            print(f"   Arbitrage Spread: {arb:.2f}%")
    
    # Trade summary
    trades = [r for r in records if r.get("record_type") == "trade"]
    if trades:
        print(f"\n💹 TRADES ({len(trades)} total):")
        # Show last 5
        for t in trades[-5:]:
            side = t.get("trade_side", "?")
            price = t.get("trade_price", "?")
            size = t.get("trade_size", "?")
            print(f"   {format_timestamp(t['recv_ts_ns'])} {side} {size} @ {price}")
    
    print(f"\n{'='*60}\n")


def print_snapshots(records):
    """Print all snapshots."""
    snapshots = [r for r in records if r.get("record_type") == "snapshot"]
    print(f"\n📸 SNAPSHOTS ({len(snapshots)} total):")
    print("-" * 80)
    print(f"{'Time':<12} {'Elapsed':>8} {'Crypto$':>12} {'YES Bid':>8} {'YES Ask':>8} {'NO Bid':>8} {'NO Ask':>8}")
    print("-" * 80)
    
    for s in snapshots:
        time = format_timestamp(s.get("recv_ts_ns", 0))
        elapsed = f"{s.get('elapsed_sec', 0):.1f}s"
        crypto = f"${float(s.get('crypto_price_usd', 0)):,.0f}" if s.get("crypto_price_usd") else "N/A"
        yes_bid = s.get("yes_best_bid", "N/A")
        yes_ask = s.get("yes_best_ask", "N/A")
        no_bid = s.get("no_best_bid", "N/A")
        no_ask = s.get("no_best_ask", "N/A")
        
        print(f"{time:<12} {elapsed:>8} {crypto:>12} {yes_bid:>8} {yes_ask:>8} {no_bid:>8} {no_ask:>8}")


def print_trades(records):
    """Print all trades."""
    trades = [r for r in records if r.get("record_type") == "trade"]
    print(f"\n💹 TRADES ({len(trades)} total):")
    print("-" * 80)
    print(f"{'Time':<12} {'Side':<6} {'Price':>8} {'Size':>12} {'Fee BPS':>8} {'Asset':<10}")
    print("-" * 80)
    
    for t in trades:
        time = format_timestamp(t.get("recv_ts_ns", 0))
        side = t.get("trade_side", "?")
        price = t.get("trade_price", "?")
        size = t.get("trade_size", "?")
        fee = t.get("trade_fee_bps", "?")
        asset_id = t.get("trade_asset_id", "?")[:10] + "..."
        
        print(f"{time:<12} {side:<6} {price:>8} {size:>12} {fee:>8} {asset_id:<10}")


def print_record(records, index):
    """Print a specific record."""
    if index < 0 or index >= len(records):
        print(f"Invalid record index. Valid range: 0-{len(records)-1}")
        return
    
    print(json.dumps(records[index], indent=2))


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    
    file_arg = sys.argv[1]
    
    if file_arg == "latest":
        file_path = find_latest_file()
        print(f"Found latest: {file_path}")
    else:
        file_path = Path(file_arg)
        if not file_path.exists():
            print(f"File not found: {file_path}")
            sys.exit(1)
    
    records = load_records(file_path)
    
    # Parse options
    if "--snapshots" in sys.argv:
        print_snapshots(records)
    elif "--trades" in sys.argv:
        print_trades(records)
    elif "--record" in sys.argv:
        idx = sys.argv.index("--record")
        if idx + 1 < len(sys.argv):
            try:
                n = int(sys.argv[idx + 1])
                print_record(records, n)
            except ValueError:
                print("Invalid record number")
        else:
            print("Please provide record number")
    else:
        print_summary(records, file_path)


if __name__ == "__main__":
    main()


#!/usr/bin/env python3
"""
Utility to read compressed (.jsonl.gz) data files.

Usage:
    python read_compressed.py <file.jsonl.gz>
    python read_compressed.py <file.jsonl.gz> --count
    python read_compressed.py <file.jsonl.gz> --head 10
    python read_compressed.py <file.jsonl.gz> --snapshots
    python read_compressed.py <file.jsonl.gz> --trades
"""

import gzip
import json
import sys
from pathlib import Path
from datetime import datetime, timezone


def read_records(file_path: Path):
    """Read records from file (supports both .jsonl and .jsonl.gz)."""
    records = []
    
    if file_path.suffix == ".gz":
        with gzip.open(file_path, "rt", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    records.append(json.loads(line))
    else:
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    records.append(json.loads(line))
    
    return records


def print_summary(records, file_path):
    """Print file summary."""
    by_type = {}
    for r in records:
        rtype = r.get("record_type", "unknown")
        by_type[rtype] = by_type.get(rtype, 0) + 1
    
    # Get file size
    size_bytes = file_path.stat().st_size
    if size_bytes < 1024:
        size_str = f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        size_str = f"{size_bytes / 1024:.1f} KB"
    else:
        size_str = f"{size_bytes / 1024 / 1024:.2f} MB"
    
    print(f"\n{'='*60}")
    print(f"📁 FILE: {file_path}")
    print(f"📦 SIZE: {size_str}")
    print(f"📊 RECORDS: {len(records)}")
    print(f"{'='*60}")
    
    print("\nRecords by Type:")
    for rtype, count in sorted(by_type.items()):
        print(f"  {rtype}: {count}")
    
    # Get timing
    timestamps = [r.get("recv_ts_ns", 0) for r in records if r.get("recv_ts_ns")]
    if timestamps:
        duration = (max(timestamps) - min(timestamps)) / 1e9
        print(f"\nDuration: {duration:.1f} seconds")
    
    print()


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    
    file_path = Path(sys.argv[1])
    if not file_path.exists():
        print(f"File not found: {file_path}")
        sys.exit(1)
    
    records = read_records(file_path)
    
    if "--count" in sys.argv:
        print(len(records))
    elif "--head" in sys.argv:
        idx = sys.argv.index("--head")
        n = int(sys.argv[idx + 1]) if idx + 1 < len(sys.argv) else 10
        for r in records[:n]:
            print(json.dumps(r, indent=2))
    elif "--snapshots" in sys.argv:
        snapshots = [r for r in records if r.get("record_type") == "snapshot"]
        for s in snapshots:
            print(json.dumps(s))
    elif "--trades" in sys.argv:
        trades = [r for r in records if r.get("record_type") == "trade"]
        for t in trades:
            print(json.dumps(t))
    else:
        print_summary(records, file_path)


if __name__ == "__main__":
    main()


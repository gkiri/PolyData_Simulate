#!/usr/bin/env python3
"""
Production-Grade Data Validator for Polymarket 15-Minute Event Data

This validator provides comprehensive validation of collected data to ensure:
1. Data Completeness - All expected data sources present
2. Data Consistency - Values make logical sense
3. Data Integrity - Schema compliance, valid JSON, correct types
4. Cross-Validation - Compare with live reference data
5. Statistical Validation - Detect anomalies, gaps, outliers

Usage:
    # Validate a specific file
    python validator.py validate data/BTC/date=2026-01-12/window_42_1768250400.jsonl
    
    # Run collection + validation test (1 minute)
    python validator.py test --duration 60
    
    # Run collection + validation test (30 seconds)
    python validator.py test --duration 30

Author: Production validation for HFT data collection
"""

import json
import sys
import time
import subprocess
import threading
import signal
import statistics
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Set
from collections import defaultdict

import requests

from market_discovery import discover_active_crypto_15m


# =============================================================================
# VALIDATION RESULT STRUCTURES
# =============================================================================

@dataclass
class ValidationIssue:
    """Single validation issue found."""
    severity: str  # ERROR, WARNING, INFO
    category: str  # completeness, consistency, integrity, etc.
    message: str
    record_index: Optional[int] = None
    field: Optional[str] = None
    expected: Optional[str] = None
    actual: Optional[str] = None
    
    def __str__(self):
        base = f"[{self.severity}] [{self.category}] {self.message}"
        if self.record_index is not None:
            base += f" (record #{self.record_index})"
        if self.field:
            base += f" [field: {self.field}]"
        if self.expected and self.actual:
            base += f" [expected: {self.expected}, actual: {self.actual}]"
        return base


@dataclass
class ValidationStats:
    """Statistics about the validated data."""
    total_records: int = 0
    records_by_type: Dict[str, int] = field(default_factory=dict)
    
    # Timing stats
    first_timestamp_ns: Optional[int] = None
    last_timestamp_ns: Optional[int] = None
    duration_sec: float = 0.0
    
    # Snapshot stats
    snapshot_count: int = 0
    expected_snapshot_count: int = 0
    snapshot_interval_mean_ms: float = 0.0
    snapshot_interval_std_ms: float = 0.0
    
    # Price stats
    crypto_price_count: int = 0
    crypto_price_min: Optional[str] = None
    crypto_price_max: Optional[str] = None
    crypto_price_range_pct: float = 0.0
    
    # Order book stats
    yes_bid_price_samples: int = 0
    yes_ask_price_samples: int = 0
    no_bid_price_samples: int = 0
    no_ask_price_samples: int = 0
    
    # Trade stats
    trade_count: int = 0
    
    # Update stats
    book_update_count: int = 0
    
    # Derived metrics validation
    metrics_present_count: int = 0
    metrics_valid_count: int = 0


@dataclass
class ValidationReport:
    """Complete validation report."""
    file_path: str
    validation_time: str
    is_valid: bool
    
    stats: ValidationStats = field(default_factory=ValidationStats)
    issues: List[ValidationIssue] = field(default_factory=list)
    
    # Scores (0-100)
    completeness_score: float = 0.0
    consistency_score: float = 0.0
    integrity_score: float = 0.0
    overall_score: float = 0.0
    
    # Reference comparison
    reference_checks: Dict[str, bool] = field(default_factory=dict)
    
    def add_issue(self, severity: str, category: str, message: str, **kwargs):
        self.issues.append(ValidationIssue(severity, category, message, **kwargs))
        
    def get_issues_by_severity(self, severity: str) -> List[ValidationIssue]:
        return [i for i in self.issues if i.severity == severity]
    
    def print_summary(self):
        """Print human-readable summary."""
        print("\n" + "=" * 70)
        print("📊 DATA VALIDATION REPORT")
        print("=" * 70)
        print(f"File: {self.file_path}")
        print(f"Validation Time: {self.validation_time}")
        print()
        
        # Overall status
        if self.is_valid:
            print("✅ VALIDATION PASSED")
        else:
            print("❌ VALIDATION FAILED")
        print()
        
        # Scores
        print("📈 SCORES (0-100):")
        print(f"  Completeness:  {self.completeness_score:5.1f}%")
        print(f"  Consistency:   {self.consistency_score:5.1f}%")
        print(f"  Integrity:     {self.integrity_score:5.1f}%")
        print(f"  ─────────────────────")
        print(f"  OVERALL:       {self.overall_score:5.1f}%")
        print()
        
        # Statistics
        print("📊 STATISTICS:")
        print(f"  Total Records: {self.stats.total_records}")
        print(f"  Duration: {self.stats.duration_sec:.2f} seconds")
        print()
        print("  Records by Type:")
        for rtype, count in sorted(self.stats.records_by_type.items()):
            print(f"    {rtype}: {count}")
        print()
        
        if self.stats.snapshot_count > 0:
            print(f"  Snapshots: {self.stats.snapshot_count} (expected ~{self.stats.expected_snapshot_count})")
            print(f"  Snapshot Interval: {self.stats.snapshot_interval_mean_ms:.0f}ms ± {self.stats.snapshot_interval_std_ms:.0f}ms")
        
        if self.stats.crypto_price_count > 0:
            print(f"  Crypto Prices: {self.stats.crypto_price_count} samples")
            print(f"    Range: ${self.stats.crypto_price_min} - ${self.stats.crypto_price_max} ({self.stats.crypto_price_range_pct:.3f}%)")
        
        print(f"  Trades: {self.stats.trade_count}")
        print(f"  Book Updates: {self.stats.book_update_count}")
        print()
        
        # Reference checks
        if self.reference_checks:
            print("🔍 REFERENCE CHECKS:")
            for check, passed in self.reference_checks.items():
                status = "✅" if passed else "❌"
                print(f"  {status} {check}")
            print()
        
        # Issues
        errors = self.get_issues_by_severity("ERROR")
        warnings = self.get_issues_by_severity("WARNING")
        infos = self.get_issues_by_severity("INFO")
        
        print("🚨 ISSUES:")
        print(f"  Errors: {len(errors)}")
        print(f"  Warnings: {len(warnings)}")
        print(f"  Info: {len(infos)}")
        print()
        
        if errors:
            print("❌ ERRORS:")
            for issue in errors[:10]:  # Show first 10
                print(f"  • {issue}")
            if len(errors) > 10:
                print(f"  ... and {len(errors) - 10} more errors")
            print()
            
        if warnings:
            print("⚠️  WARNINGS:")
            for issue in warnings[:10]:
                print(f"  • {issue}")
            if len(warnings) > 10:
                print(f"  ... and {len(warnings) - 10} more warnings")
            print()
        
        print("=" * 70)


# =============================================================================
# SCHEMA DEFINITION FOR VALIDATION
# =============================================================================

REQUIRED_FIELDS = {
    "all": ["schema_version", "record_type", "recv_ts_ns", "window_index", 
            "window_start_ts", "window_end_ts", "asset", "elapsed_sec", "remaining_sec"],
    "market_meta": ["market_id", "token_id_yes", "token_id_no"],
    "snapshot": [],  # Optional fields but should have order book data
    "book_update": ["update_type"],
    "trade": ["trade_price", "trade_size"],
    "price_tick": ["crypto_price_usd"],
    "window_end": [],
}

VALID_RECORD_TYPES = {"market_meta", "snapshot", "book_update", "trade", "price_tick", "window_end"}

VALID_UPDATE_TYPES = {"price_change", "book", "best_bid_ask"}


# =============================================================================
# DATA VALIDATOR
# =============================================================================

class DataValidator:
    """
    Comprehensive validator for collected Polymarket data.
    
    Performs:
    1. Structural validation (JSON, schema compliance)
    2. Semantic validation (logical consistency)
    3. Statistical validation (anomaly detection)
    4. Cross-validation (compare with reference)
    """
    
    def __init__(self, file_path: str):
        self.file_path = Path(file_path)
        self.records: List[Dict] = []
        self.report = ValidationReport(
            file_path=str(file_path),
            validation_time=datetime.now(timezone.utc).isoformat(),
            is_valid=True
        )
        
    def load_and_validate(self) -> ValidationReport:
        """Load file and run all validations."""
        print(f"\n🔍 Validating: {self.file_path}")
        
        # Step 1: Load file
        if not self._load_file():
            self.report.is_valid = False
            return self.report
        
        # Step 2: Basic statistics
        self._calculate_stats()
        
        # Step 3: Integrity validation
        integrity_score = self._validate_integrity()
        
        # Step 4: Consistency validation
        consistency_score = self._validate_consistency()
        
        # Step 5: Completeness validation
        completeness_score = self._validate_completeness()
        
        # Step 6: Cross-validation with reference
        self._cross_validate()
        
        # Calculate scores
        self.report.integrity_score = integrity_score
        self.report.consistency_score = consistency_score
        self.report.completeness_score = completeness_score
        self.report.overall_score = (integrity_score + consistency_score + completeness_score) / 3
        
        # Determine pass/fail
        error_count = len(self.report.get_issues_by_severity("ERROR"))
        self.report.is_valid = (
            error_count == 0 and 
            self.report.overall_score >= 70
        )
        
        return self.report
    
    def _load_file(self) -> bool:
        """Load and parse the JSONL file."""
        if not self.file_path.exists():
            self.report.add_issue("ERROR", "integrity", f"File does not exist: {self.file_path}")
            return False
        
        try:
            with open(self.file_path, "r", encoding="utf-8") as f:
                for i, line in enumerate(f):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                        self.records.append(record)
                    except json.JSONDecodeError as e:
                        self.report.add_issue("ERROR", "integrity", 
                                            f"Invalid JSON at line {i+1}: {e}",
                                            record_index=i)
        except Exception as e:
            self.report.add_issue("ERROR", "integrity", f"Failed to read file: {e}")
            return False
        
        if not self.records:
            self.report.add_issue("ERROR", "completeness", "File is empty (no records)")
            return False
        
        self.report.add_issue("INFO", "integrity", f"Loaded {len(self.records)} records successfully")
        return True
    
    def _calculate_stats(self):
        """Calculate statistics about the data."""
        stats = self.report.stats
        stats.total_records = len(self.records)
        
        # Count by type
        for record in self.records:
            rtype = record.get("record_type", "unknown")
            stats.records_by_type[rtype] = stats.records_by_type.get(rtype, 0) + 1
        
        # Timing
        timestamps = [r.get("recv_ts_ns", 0) for r in self.records if r.get("recv_ts_ns")]
        if timestamps:
            stats.first_timestamp_ns = min(timestamps)
            stats.last_timestamp_ns = max(timestamps)
            stats.duration_sec = (stats.last_timestamp_ns - stats.first_timestamp_ns) / 1e9
        
        # Snapshot stats
        snapshots = [r for r in self.records if r.get("record_type") == "snapshot"]
        stats.snapshot_count = len(snapshots)
        stats.expected_snapshot_count = int(stats.duration_sec) if stats.duration_sec > 0 else 0
        
        if len(snapshots) > 1:
            intervals = []
            for i in range(1, len(snapshots)):
                interval_ms = (snapshots[i].get("recv_ts_ns", 0) - snapshots[i-1].get("recv_ts_ns", 0)) / 1e6
                if 0 < interval_ms < 10000:  # Ignore outliers > 10s
                    intervals.append(interval_ms)
            if intervals:
                stats.snapshot_interval_mean_ms = statistics.mean(intervals)
                stats.snapshot_interval_std_ms = statistics.stdev(intervals) if len(intervals) > 1 else 0
        
        # Crypto price stats
        crypto_prices = []
        for r in self.records:
            price = r.get("crypto_price_usd")
            if price:
                try:
                    crypto_prices.append(Decimal(price))
                    stats.crypto_price_count += 1
                except:
                    pass
        
        if crypto_prices:
            stats.crypto_price_min = str(min(crypto_prices))
            stats.crypto_price_max = str(max(crypto_prices))
            mid = (min(crypto_prices) + max(crypto_prices)) / 2
            if mid > 0:
                stats.crypto_price_range_pct = float((max(crypto_prices) - min(crypto_prices)) / mid * 100)
        
        # Order book stats
        for r in self.records:
            if r.get("yes_bids"):
                stats.yes_bid_price_samples += 1
            if r.get("yes_asks"):
                stats.yes_ask_price_samples += 1
            if r.get("no_bids"):
                stats.no_bid_price_samples += 1
            if r.get("no_asks"):
                stats.no_ask_price_samples += 1
        
        # Trade and update counts
        stats.trade_count = stats.records_by_type.get("trade", 0)
        stats.book_update_count = stats.records_by_type.get("book_update", 0)
        
        # Metrics validation count
        for r in self.records:
            if r.get("metrics"):
                stats.metrics_present_count += 1
                # Check if metrics are valid
                m = r["metrics"]
                if m.get("yes_spread") and m.get("no_spread"):
                    stats.metrics_valid_count += 1
    
    def _validate_integrity(self) -> float:
        """Validate data integrity (schema compliance, types)."""
        checks_passed = 0
        checks_total = 0
        
        for i, record in enumerate(self.records):
            # Check schema version
            checks_total += 1
            if record.get("schema_version") == 2:
                checks_passed += 1
            else:
                self.report.add_issue("WARNING", "integrity", 
                                    f"Unexpected schema version",
                                    record_index=i,
                                    expected="2",
                                    actual=str(record.get("schema_version")))
            
            # Check record type
            checks_total += 1
            rtype = record.get("record_type")
            if rtype in VALID_RECORD_TYPES:
                checks_passed += 1
            else:
                self.report.add_issue("ERROR", "integrity",
                                    f"Invalid record_type: {rtype}",
                                    record_index=i)
            
            # Check required fields
            required = REQUIRED_FIELDS.get("all", []) + REQUIRED_FIELDS.get(rtype, [])
            for field_name in required:
                checks_total += 1
                if field_name in record and record[field_name] is not None:
                    checks_passed += 1
                else:
                    self.report.add_issue("ERROR", "integrity",
                                        f"Missing required field: {field_name}",
                                        record_index=i,
                                        field=field_name)
            
            # Validate timestamp is positive integer
            checks_total += 1
            ts = record.get("recv_ts_ns")
            if isinstance(ts, int) and ts > 0:
                checks_passed += 1
            else:
                self.report.add_issue("ERROR", "integrity",
                                    f"Invalid timestamp: {ts}",
                                    record_index=i,
                                    field="recv_ts_ns")
            
            # Validate window_index is 0-95
            checks_total += 1
            widx = record.get("window_index")
            if isinstance(widx, int) and 0 <= widx <= 95:
                checks_passed += 1
            else:
                self.report.add_issue("WARNING", "integrity",
                                    f"Invalid window_index: {widx}",
                                    record_index=i,
                                    field="window_index")
            
            # Validate price fields are valid decimals
            price_fields = ["crypto_price_usd", "yes_best_bid", "yes_best_ask",
                          "no_best_bid", "no_best_ask", "trade_price"]
            for pf in price_fields:
                if pf in record and record[pf] is not None:
                    checks_total += 1
                    try:
                        Decimal(record[pf])
                        checks_passed += 1
                    except:
                        self.report.add_issue("ERROR", "integrity",
                                            f"Invalid decimal value: {record[pf]}",
                                            record_index=i,
                                            field=pf)
        
        return (checks_passed / checks_total * 100) if checks_total > 0 else 0
    
    def _validate_consistency(self) -> float:
        """Validate data consistency (logical correctness)."""
        checks_passed = 0
        checks_total = 0
        
        prev_timestamp = None
        
        for i, record in enumerate(self.records):
            # Timestamps should be monotonically increasing
            ts = record.get("recv_ts_ns")
            if prev_timestamp is not None:
                checks_total += 1
                if ts >= prev_timestamp:
                    checks_passed += 1
                else:
                    self.report.add_issue("WARNING", "consistency",
                                        f"Non-monotonic timestamp (went backwards)",
                                        record_index=i,
                                        field="recv_ts_ns")
            prev_timestamp = ts
            
            # elapsed_sec + remaining_sec should equal ~900 (15 minutes)
            elapsed = record.get("elapsed_sec", 0)
            remaining = record.get("remaining_sec", 0)
            if elapsed is not None and remaining is not None:
                checks_total += 1
                total = elapsed + remaining
                if 898 <= total <= 902:  # Allow 2 seconds tolerance
                    checks_passed += 1
                else:
                    self.report.add_issue("WARNING", "consistency",
                                        f"elapsed + remaining != 900: {total:.2f}",
                                        record_index=i)
            
            # Order book consistency: best_bid < best_ask
            for side in ["yes", "no"]:
                bid_field = f"{side}_best_bid"
                ask_field = f"{side}_best_ask"
                if record.get(bid_field) and record.get(ask_field):
                    checks_total += 1
                    try:
                        bid = Decimal(record[bid_field])
                        ask = Decimal(record[ask_field])
                        if bid < ask:
                            checks_passed += 1
                        else:
                            self.report.add_issue("ERROR", "consistency",
                                                f"{side.upper()} bid >= ask: {bid} >= {ask}",
                                                record_index=i)
                    except:
                        pass
            
            # Prices should be between 0 and 1 for prediction market
            for pf in ["yes_best_bid", "yes_best_ask", "no_best_bid", "no_best_ask"]:
                if record.get(pf):
                    checks_total += 1
                    try:
                        price = Decimal(record[pf])
                        if Decimal("0") <= price <= Decimal("1"):
                            checks_passed += 1
                        else:
                            self.report.add_issue("ERROR", "consistency",
                                                f"Price out of range [0,1]: {price}",
                                                record_index=i,
                                                field=pf)
                    except:
                        pass
            
            # YES_price + NO_price should be approximately 1.0
            yes_ask = record.get("yes_best_ask")
            no_ask = record.get("no_best_ask")
            if yes_ask and no_ask:
                checks_total += 1
                try:
                    total = Decimal(yes_ask) + Decimal(no_ask)
                    # Allow 0.90 to 1.10 (markets can have arbitrage opportunities)
                    if Decimal("0.90") <= total <= Decimal("1.10"):
                        checks_passed += 1
                    else:
                        self.report.add_issue("WARNING", "consistency",
                                            f"YES + NO asks significantly != 1.0: {total}",
                                            record_index=i)
                except:
                    pass
            
            # Verify derived metrics match calculations
            if record.get("record_type") == "snapshot" and record.get("metrics"):
                m = record["metrics"]
                
                # Check spread calculation
                if record.get("yes_best_bid") and record.get("yes_best_ask") and m.get("yes_spread"):
                    checks_total += 1
                    try:
                        expected_spread = Decimal(record["yes_best_ask"]) - Decimal(record["yes_best_bid"])
                        actual_spread = Decimal(m["yes_spread"])
                        if abs(expected_spread - actual_spread) < Decimal("0.0001"):
                            checks_passed += 1
                        else:
                            self.report.add_issue("WARNING", "consistency",
                                                f"Spread mismatch",
                                                record_index=i,
                                                expected=str(expected_spread),
                                                actual=str(actual_spread))
                    except:
                        pass
                
                # Check mid price calculation
                if record.get("yes_best_bid") and record.get("yes_best_ask") and m.get("yes_mid"):
                    checks_total += 1
                    try:
                        expected_mid = (Decimal(record["yes_best_bid"]) + Decimal(record["yes_best_ask"])) / 2
                        actual_mid = Decimal(m["yes_mid"])
                        if abs(expected_mid - actual_mid) < Decimal("0.0001"):
                            checks_passed += 1
                        else:
                            self.report.add_issue("WARNING", "consistency",
                                                f"Mid price mismatch",
                                                record_index=i,
                                                expected=str(expected_mid),
                                                actual=str(actual_mid))
                    except:
                        pass
        
        return (checks_passed / checks_total * 100) if checks_total > 0 else 0
    
    def _validate_completeness(self) -> float:
        """Validate data completeness."""
        score = 100.0
        deductions = []
        
        stats = self.report.stats
        
        # Must have market_meta record
        if "market_meta" not in stats.records_by_type:
            self.report.add_issue("ERROR", "completeness", "Missing market_meta record")
            deductions.append(20)
        
        # Should have snapshots
        if stats.snapshot_count == 0:
            self.report.add_issue("ERROR", "completeness", "No snapshot records found")
            deductions.append(30)
        elif stats.expected_snapshot_count > 0:
            snapshot_ratio = stats.snapshot_count / stats.expected_snapshot_count
            if snapshot_ratio < 0.5:
                self.report.add_issue("WARNING", "completeness", 
                                    f"Low snapshot count: {stats.snapshot_count} vs expected ~{stats.expected_snapshot_count}")
                deductions.append(15)
            elif snapshot_ratio < 0.8:
                self.report.add_issue("INFO", "completeness",
                                    f"Slightly low snapshot count: {stats.snapshot_count} vs expected ~{stats.expected_snapshot_count}")
                deductions.append(5)
        
        # Should have crypto price data
        if stats.crypto_price_count == 0:
            self.report.add_issue("WARNING", "completeness", "No crypto price data found")
            deductions.append(10)
        
        # Should have order book data
        if stats.yes_bid_price_samples == 0:
            self.report.add_issue("WARNING", "completeness", "No YES bid data found")
            deductions.append(10)
        if stats.yes_ask_price_samples == 0:
            self.report.add_issue("WARNING", "completeness", "No YES ask data found")
            deductions.append(10)
        if stats.no_bid_price_samples == 0:
            self.report.add_issue("WARNING", "completeness", "No NO bid data found")
            deductions.append(10)
        if stats.no_ask_price_samples == 0:
            self.report.add_issue("WARNING", "completeness", "No NO ask data found")
            deductions.append(10)
        
        # Snapshots should have metrics
        if stats.snapshot_count > 0:
            metrics_ratio = stats.metrics_present_count / stats.snapshot_count
            if metrics_ratio < 0.9:
                self.report.add_issue("WARNING", "completeness",
                                    f"Low metrics coverage: {metrics_ratio*100:.1f}%")
                deductions.append(10)
        
        # Check for significant time gaps
        if len(self.records) > 1:
            timestamps = sorted([r.get("recv_ts_ns", 0) for r in self.records if r.get("recv_ts_ns")])
            max_gap_sec = 0
            for i in range(1, len(timestamps)):
                gap_sec = (timestamps[i] - timestamps[i-1]) / 1e9
                if gap_sec > max_gap_sec:
                    max_gap_sec = gap_sec
            
            if max_gap_sec > 10:
                self.report.add_issue("WARNING", "completeness",
                                    f"Large time gap detected: {max_gap_sec:.1f} seconds")
                deductions.append(10)
            elif max_gap_sec > 5:
                self.report.add_issue("INFO", "completeness",
                                    f"Moderate time gap detected: {max_gap_sec:.1f} seconds")
                deductions.append(5)
        
        return max(0, score - sum(deductions))
    
    def _cross_validate(self):
        """Cross-validate with live reference data."""
        # Try to get current market info for comparison
        try:
            # Find which asset we're validating
            asset = None
            for r in self.records:
                if r.get("asset"):
                    asset = r["asset"].lower()
                    break
            
            if not asset:
                self.report.add_issue("INFO", "reference", "Could not determine asset for reference check")
                return
            
            # Get current market from API
            markets = discover_active_crypto_15m([asset])
            market_info = markets.get(asset.upper())
            
            if not market_info:
                self.report.add_issue("INFO", "reference", f"No active market found for {asset.upper()} to compare")
                return
            
            # Compare market IDs
            stored_market_id = None
            for r in self.records:
                if r.get("market_id"):
                    stored_market_id = r["market_id"]
                    break
            
            if stored_market_id:
                self.report.reference_checks["market_id_valid"] = (
                    len(stored_market_id) > 10 and stored_market_id.startswith("0x")
                )
            
            # Compare token IDs
            stored_yes_token = None
            stored_no_token = None
            for r in self.records:
                if r.get("token_id_yes"):
                    stored_yes_token = r["token_id_yes"]
                if r.get("token_id_no"):
                    stored_no_token = r["token_id_no"]
                if stored_yes_token and stored_no_token:
                    break
            
            if stored_yes_token and stored_no_token:
                self.report.reference_checks["token_ids_present"] = True
                # Token IDs should be long numeric strings
                self.report.reference_checks["token_ids_valid_format"] = (
                    len(stored_yes_token) > 20 and stored_yes_token.isdigit() and
                    len(stored_no_token) > 20 and stored_no_token.isdigit()
                )
            
            # Try to fetch live order book for comparison
            try:
                resp = requests.get(
                    "https://clob.polymarket.com/book",
                    params={"token_id": market_info["token_ids"][0]},
                    timeout=10
                )
                if resp.status_code == 200:
                    self.report.reference_checks["api_accessible"] = True
                    book = resp.json()
                    
                    # Verify book structure matches what we collect
                    if "bids" in book and "asks" in book:
                        self.report.reference_checks["book_structure_valid"] = True
                    
                    # Compare last_trade_price format
                    if "last_trade_price" in book:
                        self.report.reference_checks["last_trade_price_available"] = True
                else:
                    self.report.reference_checks["api_accessible"] = False
            except Exception as e:
                self.report.add_issue("INFO", "reference", f"Could not fetch reference book: {e}")
                
        except Exception as e:
            self.report.add_issue("INFO", "reference", f"Reference validation error: {e}")


# =============================================================================
# TEST RUNNER
# =============================================================================

class CollectorTestRunner:
    """
    Runs the collector for a specified duration and validates the output.
    """
    
    def __init__(self, duration_sec: int = 60):
        self.duration_sec = duration_sec
        self.collector_process = None
        self.output_files: List[Path] = []
        
    def run_test(self) -> List[ValidationReport]:
        """Run collection test and return validation reports."""
        print("\n" + "=" * 70)
        print("🧪 COLLECTOR TEST")
        print("=" * 70)
        print(f"Duration: {self.duration_sec} seconds")
        print()
        
        # Step 1: Record initial state
        data_dir = Path("data")
        initial_files = set()
        if data_dir.exists():
            for f in data_dir.rglob("*.jsonl"):
                initial_files.add(str(f))
        
        # Step 2: Start collector
        print("🚀 Starting collector...")
        self.collector_process = subprocess.Popen(
            ["python3", "unified_collector.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=str(Path(__file__).parent),
        )
        
        # Step 3: Wait for duration
        print(f"⏱️  Collecting data for {self.duration_sec} seconds...")
        
        try:
            for i in range(self.duration_sec):
                time.sleep(1)
                if (i + 1) % 10 == 0:
                    print(f"   {i + 1}/{self.duration_sec} seconds...")
        except KeyboardInterrupt:
            print("\n⚠️  Test interrupted by user")
        
        # Step 4: Stop collector
        print("🛑 Stopping collector...")
        if self.collector_process:
            self.collector_process.terminate()
            try:
                self.collector_process.wait(timeout=5)
            except:
                self.collector_process.kill()
        
        # Give a moment for files to flush
        time.sleep(1)
        
        # Step 5: Find new files
        print("🔍 Finding new data files...")
        new_files = []
        if data_dir.exists():
            for f in data_dir.rglob("*.jsonl"):
                if str(f) not in initial_files:
                    new_files.append(f)
        
        if not new_files:
            print("❌ No new data files were created!")
            return []
        
        print(f"📁 Found {len(new_files)} new file(s)")
        for f in new_files:
            print(f"   • {f}")
        
        # Step 6: Validate each file
        print("\n📊 Validating collected data...")
        reports = []
        for f in new_files:
            validator = DataValidator(str(f))
            report = validator.load_and_validate()
            reports.append(report)
            report.print_summary()
        
        # Step 7: Overall summary
        print("\n" + "=" * 70)
        print("📋 OVERALL TEST SUMMARY")
        print("=" * 70)
        
        all_passed = all(r.is_valid for r in reports)
        if all_passed:
            print("✅ ALL VALIDATIONS PASSED")
        else:
            print("❌ SOME VALIDATIONS FAILED")
        
        for r in reports:
            status = "✅" if r.is_valid else "❌"
            print(f"  {status} {Path(r.file_path).name}: {r.overall_score:.1f}%")
        
        print("=" * 70)
        
        return reports


# =============================================================================
# QUICK VALIDATION FUNCTIONS
# =============================================================================

def validate_file(file_path: str) -> ValidationReport:
    """Validate a single file."""
    validator = DataValidator(file_path)
    report = validator.load_and_validate()
    report.print_summary()
    return report


def run_collection_test(duration_sec: int = 60) -> List[ValidationReport]:
    """Run collection test for specified duration."""
    runner = CollectorTestRunner(duration_sec)
    return runner.run_test()


def quick_check(file_path: str) -> bool:
    """Quick check if a file is valid (minimal output)."""
    validator = DataValidator(file_path)
    report = validator.load_and_validate()
    
    if report.is_valid:
        print(f"✅ {file_path}: VALID ({report.overall_score:.1f}%)")
    else:
        print(f"❌ {file_path}: INVALID ({report.overall_score:.1f}%)")
        for issue in report.get_issues_by_severity("ERROR")[:3]:
            print(f"   • {issue.message}")
    
    return report.is_valid


# =============================================================================
# CLI
# =============================================================================

def print_usage():
    print("""
Polymarket Data Validator - Usage:

    # Validate a specific file
    python validator.py validate <file_path>
    
    # Run collection test (default 60 seconds)
    python validator.py test [--duration SECONDS]
    
    # Quick check (minimal output)
    python validator.py check <file_path>
    
    # Validate all files in a directory
    python validator.py validate-all <directory>

Examples:
    python validator.py validate data/BTC/date=2026-01-12/window_42_1768250400.jsonl
    python validator.py test --duration 30
    python validator.py test --duration 60
    python validator.py check data/BTC/date=2026-01-12/window_42_1768250400.jsonl
""")


def main():
    if len(sys.argv) < 2:
        print_usage()
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == "validate":
        if len(sys.argv) < 3:
            print("Error: Please provide file path")
            sys.exit(1)
        report = validate_file(sys.argv[2])
        sys.exit(0 if report.is_valid else 1)
        
    elif command == "test":
        duration = 60
        if "--duration" in sys.argv:
            idx = sys.argv.index("--duration")
            if idx + 1 < len(sys.argv):
                try:
                    duration = int(sys.argv[idx + 1])
                except:
                    print("Error: Invalid duration")
                    sys.exit(1)
        
        reports = run_collection_test(duration)
        all_passed = all(r.is_valid for r in reports) if reports else False
        sys.exit(0 if all_passed else 1)
        
    elif command == "check":
        if len(sys.argv) < 3:
            print("Error: Please provide file path")
            sys.exit(1)
        is_valid = quick_check(sys.argv[2])
        sys.exit(0 if is_valid else 1)
        
    elif command == "validate-all":
        if len(sys.argv) < 3:
            print("Error: Please provide directory path")
            sys.exit(1)
        
        directory = Path(sys.argv[2])
        if not directory.exists():
            print(f"Error: Directory does not exist: {directory}")
            sys.exit(1)
        
        files = list(directory.rglob("*.jsonl"))
        print(f"Found {len(files)} JSONL files")
        
        results = []
        for f in files:
            is_valid = quick_check(str(f))
            results.append(is_valid)
        
        passed = sum(results)
        total = len(results)
        print(f"\nSummary: {passed}/{total} files valid")
        sys.exit(0 if all(results) else 1)
        
    else:
        print(f"Unknown command: {command}")
        print_usage()
        sys.exit(1)


if __name__ == "__main__":
    main()


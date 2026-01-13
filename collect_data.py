import json
import os
import time
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, List

import requests
from websocket import WebSocketApp

from market_discovery import collect_asset_ids, discover_active_crypto_15m


# -----------------------------
# Config
# -----------------------------
CLOB_WS_BASE = "wss://ws-subscriptions-clob.polymarket.com"
CLOB_WS_CHANNEL = "market"  # "market" or "user"
RTDS_WS_URL = "wss://ws-live-data.polymarket.com"

CLOB_REST_BASE = "https://clob.polymarket.com"

DATA_ROOT = Path("data")

PING_INTERVAL_SEC_CLOB = 10   # docs sample uses 10s for CLOB WS ping 
PING_INTERVAL_SEC_RTDS = 5    # RTDS docs suggest ping every ~5s ideally 

# How often to checkpoint REST /book while market is active (optional but recommended)
BOOK_CHECKPOINT_INTERVAL_SEC = 60

# -----------------------------
# Utilities
# -----------------------------
def now_ns() -> int:
    return time.time_ns()

def utc_date_str_from_ns(ts_ns: int) -> str:
    # Use UTC date based on wall time at ingestion (good enough for partitioning)
    return time.strftime("%Y-%m-%d", time.gmtime(ts_ns / 1e9))

def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

def write_jsonl_line(path: Path, obj: Dict[str, Any]) -> None:
    ensure_parent(path)
    # Line-buffered append. Flush per line for crash resilience.
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, separators=(",", ":"), ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())


# -----------------------------
# CLOB REST checkpointing
# -----------------------------
def fetch_book_snapshot(token_id: str) -> Dict[str, Any]:
    # GET /book?token_id=<id>  (token_id is required per docs) 
    resp = requests.get(f"{CLOB_REST_BASE}/book", params={"token_id": token_id}, timeout=10)
    resp.raise_for_status()
    return resp.json()


# -----------------------------
# Routing helpers
# -----------------------------
def extract_market(msg: Dict[str, Any]) -> Optional[str]:
    return msg.get("market")

def extract_asset_id(msg: Dict[str, Any]) -> Optional[str]:
    # Many message types include a direct asset_id (book, tick_size_change, last_trade_price)
    if "asset_id" in msg:
        return msg.get("asset_id")

    # price_change has array of price_changes with asset_id
    # We'll return None here and handle fan-out separately.
    return None


def clob_event_paths(recv_ts_ns: int, msg: Dict[str, Any]) -> List[Path]:
    """
    Returns one or more output paths for this message.
    For price_change messages that touch multiple assets, we fan out into each asset folder.
    """
    date = utc_date_str_from_ns(recv_ts_ns)
    market = extract_market(msg) or "unknown_market"
    event_type = msg.get("event_type", "unknown_event")

    base = DATA_ROOT / "raw" / "clob_ws" / f"date={date}" / f"market={market}"

    if event_type == "price_change" and isinstance(msg.get("price_changes"), list):
        paths = []
        for ch in msg["price_changes"]:
            aid = ch.get("asset_id", "unknown_asset")
            paths.append(base / f"asset={aid}" / "events.jsonl")
        # If empty, fall back
        return paths or [base / "asset=unknown_asset" / "events.jsonl"]

    aid = extract_asset_id(msg) or "unknown_asset"
    return [base / f"asset={aid}" / "events.jsonl"]


def rtds_event_path(recv_ts_ns: int, topic: str) -> Path:
    date = utc_date_str_from_ns(recv_ts_ns)
    return DATA_ROOT / "raw" / "rtds" / f"date={date}" / f"{topic}.jsonl"


# -----------------------------
# CLOB WebSocket Client
# -----------------------------
@dataclass
class CLOBCollector:
    asset_ids: List[str]
    custom_feature_enabled: bool = True

    def __post_init__(self):
        self.url = f"{CLOB_WS_BASE}/ws/{CLOB_WS_CHANNEL}"  # from docs sample: url + "/ws/" + channel_type 
        self.ws: Optional[WebSocketApp] = None
        self._stop = threading.Event()

    def on_open(self, ws: WebSocketApp):
        sub = {
            "assets_ids": self.asset_ids,
            "type": "market",
            "custom_feature_enabled": bool(self.custom_feature_enabled),
        }
        ws.send(json.dumps(sub))
        # Start ping thread
        threading.Thread(target=self._ping_loop, args=(ws,), daemon=True).start()
        # Start REST checkpoint thread (optional)
        threading.Thread(target=self._checkpoint_loop, daemon=True).start()

    def _ping_loop(self, ws: WebSocketApp):
        while not self._stop.is_set():
            try:
                ws.send("PING")
            except Exception:
                return
            time.sleep(PING_INTERVAL_SEC_CLOB)

    def _checkpoint_loop(self):
        """
        Optional: periodically call REST /book for each asset_id as a checkpoint.
        Saves to checkpoints_book.jsonl under each market folder.
        """
        while not self._stop.is_set():
            for token_id in list(self.asset_ids):
                try:
                    snap = fetch_book_snapshot(token_id)
                    recv_ts_ns = now_ns()
                    market = snap.get("market", "unknown_market")
                    date = utc_date_str_from_ns(recv_ts_ns)
                    out = DATA_ROOT / "raw" / "clob_ws" / f"date={date}" / f"market={market}" / "checkpoints_book.jsonl"
                    rec = {
                        "schema_version": 1,
                        "source": "clob_rest_book",
                        "recv_ts_ns": recv_ts_ns,
                        "token_id": token_id,
                        "raw": snap,
                    }
                    write_jsonl_line(out, rec)
                except Exception as e:
                    # Don't crash the collector on checkpoint errors
                    err_out = DATA_ROOT / "raw" / "clob_ws" / "errors.jsonl"
                    write_jsonl_line(err_out, {
                        "schema_version": 1,
                        "source": "clob_rest_book",
                        "recv_ts_ns": now_ns(),
                        "token_id": token_id,
                        "error": repr(e),
                    })
            time.sleep(BOOK_CHECKPOINT_INTERVAL_SEC)

    def on_message(self, ws: WebSocketApp, message: str):
        recv_ts_ns = now_ns()
        try:
            msg = json.loads(message)
        except Exception:
            # non-JSON messages (rare) -> log raw
            out = DATA_ROOT / "raw" / "clob_ws" / "non_json_messages.jsonl"
            write_jsonl_line(out, {
                "schema_version": 1,
                "source": "clob_ws_market",
                "recv_ts_ns": recv_ts_ns,
                "raw_text": message,
            })
            return

        # Write once per involved asset (price_change can include multiple assets)
        for path in clob_event_paths(recv_ts_ns, msg):
            rec = {
                "schema_version": 1,
                "source": "clob_ws_market",
                "recv_ts_ns": recv_ts_ns,
                "event_type": msg.get("event_type"),
                "market": msg.get("market"),
                "server_ts_ms": safe_int_ms(msg.get("timestamp")),
                "raw": msg,
            }
            write_jsonl_line(path, rec)

    def on_error(self, ws: WebSocketApp, error: Exception):
        out = DATA_ROOT / "raw" / "clob_ws" / "errors.jsonl"
        write_jsonl_line(out, {
            "schema_version": 1,
            "source": "clob_ws_market",
            "recv_ts_ns": now_ns(),
            "error": repr(error),
        })

    def on_close(self, ws: WebSocketApp, status_code: int, msg: str):
        out = DATA_ROOT / "raw" / "clob_ws" / "lifecycle.jsonl"
        write_jsonl_line(out, {
            "schema_version": 1,
            "source": "clob_ws_market",
            "recv_ts_ns": now_ns(),
            "event": "close",
            "status_code": status_code,
            "message": msg,
        })

    def run_forever(self):
        self.ws = WebSocketApp(
            self.url,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )

        # Simple reconnect loop
        backoff = 1
        while not self._stop.is_set():
            try:
                self.ws.run_forever()
            except Exception as e:
                self.on_error(self.ws, e)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)

    def stop(self):
        self._stop.set()
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass


def safe_int_ms(x: Any) -> Optional[int]:
    if x is None:
        return None
    try:
        # timestamps often arrive as strings in docs examples 
        return int(x)
    except Exception:
        return None


# -----------------------------
# RTDS Client (Chainlink BTC/USD)
# -----------------------------
@dataclass
class RTDSCollector:
    chainlink_symbol: str = "btc/usd"
    also_binance: bool = False  # optionally subscribe to btcusdt too

    def __post_init__(self):
        self.ws: Optional[WebSocketApp] = None
        self._stop = threading.Event()

    def on_open(self, ws: WebSocketApp):
        # Chainlink subscription message based on RTDS Crypto Prices docs 
        subs = [{
            "topic": "crypto_prices_chainlink",
            "type": "*",
            "filters": json.dumps({"symbol": self.chainlink_symbol}),
        }]

        if self.also_binance:
            subs.append({
                "topic": "crypto_prices",
                "type": "update",
                "filters": "btcusdt",
            })

        ws.send(json.dumps({
            "action": "subscribe",
            "subscriptions": subs,
        }))

        threading.Thread(target=self._ping_loop, args=(ws,), daemon=True).start()

    def _ping_loop(self, ws: WebSocketApp):
        while not self._stop.is_set():
            try:
                ws.send("PING")
            except Exception:
                return
            time.sleep(PING_INTERVAL_SEC_RTDS)

    def on_message(self, ws: WebSocketApp, message: str):
        recv_ts_ns = now_ns()
        try:
            msg = json.loads(message)
        except Exception:
            out = DATA_ROOT / "raw" / "rtds" / "non_json_messages.jsonl"
            write_jsonl_line(out, {
                "schema_version": 1,
                "source": "rtds",
                "recv_ts_ns": recv_ts_ns,
                "raw_text": message,
            })
            return

        topic = msg.get("topic", "unknown_topic")
        out_path = rtds_event_path(recv_ts_ns, topic)
        rec = {
            "schema_version": 1,
            "source": "rtds",
            "recv_ts_ns": recv_ts_ns,
            "topic": topic,
            "type": msg.get("type"),
            "server_ts_ms": msg.get("timestamp"),
            "raw": msg,
        }
        write_jsonl_line(out_path, rec)

    def on_error(self, ws: WebSocketApp, error: Exception):
        out = DATA_ROOT / "raw" / "rtds" / "errors.jsonl"
        write_jsonl_line(out, {
            "schema_version": 1,
            "source": "rtds",
            "recv_ts_ns": now_ns(),
            "error": repr(error),
        })

    def on_close(self, ws: WebSocketApp, status_code: int, msg: str):
        out = DATA_ROOT / "raw" / "rtds" / "lifecycle.jsonl"
        write_jsonl_line(out, {
            "schema_version": 1,
            "source": "rtds",
            "recv_ts_ns": now_ns(),
            "event": "close",
            "status_code": status_code,
            "message": msg,
        })

    def run_forever(self):
        self.ws = WebSocketApp(
            RTDS_WS_URL,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )

        backoff = 1
        while not self._stop.is_set():
            try:
                self.ws.run_forever()
            except Exception as e:
                self.on_error(self.ws, e)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)

    def stop(self):
        self._stop.set()
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass


# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    # Discover current BTC/ETH 15-minute markets dynamically via Gamma slug lookup
    discovered_markets = discover_active_crypto_15m(("btc", "eth"))
    ASSET_IDS = collect_asset_ids(discovered_markets)

    if not ASSET_IDS:
        raise SystemExit("No active BTC/ETH 15-minute markets found via Gamma API.")

    for symbol, info in discovered_markets.items():
        if info:
            print(
                f"[{symbol}] slug={info['slug']} market_id={info['market_id']} "
                f"tokens={info['token_ids'][:2]} end={info['end_time']} "
                f"minutes_remaining={info['minutes_remaining']:.2f}"
            )

    clob = CLOBCollector(asset_ids=ASSET_IDS, custom_feature_enabled=True)
    rtds = RTDSCollector(chainlink_symbol="btc/usd", also_binance=False)

    t1 = threading.Thread(target=clob.run_forever, daemon=True)
    t2 = threading.Thread(target=rtds.run_forever, daemon=True)
    t1.start()
    t2.start()

    print("Collectors running. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping...")
        clob.stop()
        rtds.stop()

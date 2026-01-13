import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

# Gamma hosts the Polymarket market discovery endpoints
GAMMA_API_BASE = "https://gamma-api.polymarket.com"

# 15-minute crypto windows use a fixed cadence
FIFTEEN_MINUTES_SECONDS = 900

# Ignore markets that are effectively expired
MIN_MINUTES_REMAINING = 0.5


def _parse_tokens(raw: Any) -> List[str]:
    """
    Normalize clobTokenIds which may arrive as JSON string or list.
    """
    if isinstance(raw, list):
        return [str(t) for t in raw if t is not None]

    if isinstance(raw, str):
        try:
            loaded = json.loads(raw)
            if isinstance(loaded, list):
                return [str(t) for t in loaded if t is not None]
        except json.JSONDecodeError:
            return [t.strip() for t in raw.split(",") if t.strip()]

    return []


def _parse_prices(raw: Any) -> Tuple[Optional[float], Optional[float]]:
    """
    Extract YES/NO prices when present. Used only for informational logging.
    """
    if isinstance(raw, list) and len(raw) >= 2:
        try:
            return float(raw[0]), float(raw[1])
        except (TypeError, ValueError):
            return None, None

    if isinstance(raw, str):
        try:
            loaded = json.loads(raw)
            if isinstance(loaded, list) and len(loaded) >= 2:
                return float(loaded[0]), float(loaded[1])
        except (json.JSONDecodeError, TypeError, ValueError):
            return None, None

    return None, None


def _parse_end_time(market: Dict[str, Any]) -> Optional[datetime]:
    """
    Handle the various end-date keys the API may return.
    """
    end_date_str = (
        market.get("endDate")
        or market.get("end_date")
        or market.get("endDateIso")
        or market.get("end_date_iso")
    )

    if not end_date_str:
        return None

    # Normalize Zulu suffix for datetime.fromisoformat
    if isinstance(end_date_str, str) and end_date_str.endswith("Z"):
        end_date_str = end_date_str.replace("Z", "+00:00")

    try:
        dt = datetime.fromisoformat(end_date_str)
    except (TypeError, ValueError):
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt


def _window_start(now: Optional[int], window_seconds: int) -> int:
    ts = now if now is not None else int(time.time())
    return (ts // window_seconds) * window_seconds


def fetch_crypto_15m_market(asset: str, *, now: Optional[int] = None) -> Optional[Dict[str, Any]]:
    """
    Discover the current active 15-minute up/down market for the given asset.

    The Gamma /events endpoint does NOT surface these markets, so we construct
    the slug directly: {asset}-updown-15m-{window_start_ts}. If the current
    window has just rolled, we also try the next window.
    """
    asset = asset.lower()
    base_slug = f"{asset}-updown-15m"
    current_window = _window_start(now, FIFTEEN_MINUTES_SECONDS)

    for window_start in (current_window, current_window + FIFTEEN_MINUTES_SECONDS):
        slug = f"{base_slug}-{window_start}"
        try:
            resp = requests.get(
                f"{GAMMA_API_BASE}/events/slug/{slug}",
                timeout=10,
            )
        except requests.RequestException:
            continue

        if resp.status_code != 200:
            continue

        event = resp.json()
        for market in event.get("markets", []):
            end_time = _parse_end_time(market)
            if not end_time:
                continue

            minutes_remaining = (end_time - datetime.now(timezone.utc)).total_seconds() / 60.0
            if minutes_remaining < MIN_MINUTES_REMAINING:
                continue

            tokens = _parse_tokens(
                market.get("clobTokenIds") or market.get("clob_token_ids") or []
            )
            if len(tokens) < 2:
                continue

            yes_price, no_price = _parse_prices(
                market.get("outcomePrices") or market.get("outcome_prices") or []
            )

            is_active = market.get("active", True) and not market.get("closed", False)

            return {
                "slug": slug,
                "market_id": str(market.get("id", "")),
                "condition_id": market.get("conditionId") or market.get("condition_id", ""),
                "question": market.get("question") or market.get("title", ""),
                "token_ids": tokens,
                "minutes_remaining": minutes_remaining,
                "end_time": end_time.isoformat(),
                "yes_price": yes_price,
                "no_price": no_price,
                "is_active": is_active,
            }

    return None


def discover_active_crypto_15m(assets: Iterable[str]) -> Dict[str, Optional[Dict[str, Any]]]:
    """
    Discover active 15-minute markets for the provided asset symbols.
    """
    results: Dict[str, Optional[Dict[str, Any]]] = {}
    for asset in assets:
        results[asset.upper()] = fetch_crypto_15m_market(asset)
    return results


def collect_asset_ids(markets: Dict[str, Optional[Dict[str, Any]]]) -> List[str]:
    """
    Flatten token ids from discovered markets for websocket subscription.
    """
    asset_ids: List[str] = []
    for info in markets.values():
        if info and info.get("token_ids"):
            asset_ids.extend(info["token_ids"][:2])
    return asset_ids


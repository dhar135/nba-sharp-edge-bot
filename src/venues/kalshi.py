"""Kalshi read-only market data client (no trading).

Docs: https://docs.kalshi.com — verify base URL/params/fees before relying
on this in production. API surface last verified 2026-07-06 against the
live endpoint.

Field-shape note (2026-07-06): the market-listing endpoint now returns
quote fields as DOLLAR-denominated strings rather than integer cents.
Confirmed live response fields include "yes_bid_dollars" (e.g. "0.1810"),
"yes_ask_dollars" (e.g. "0.2030"), and "last_price_dollars", alongside a
"response_price_units": "usd_cent" marker. The older integer-cent keys
"yes_bid"/"yes_ask" are absent (or None) in current responses. This module
converts the dollar strings to cents internally so that normalize_market's
returned dict keeps the same shape (yes_bid/yes_ask as floats in CENTS)
that Task 16 depends on.

Series/ticker prefixes observed live on 2026-07-06 (NBA is in the
offseason, so no NBA player-prop series were open; recorded prefixes are
whatever active series appeared in a limit=5 sample, not NBA-specific):
  - KXMVE-... (miscellaneous/misc event markets)
  - KXHIGHNY-... (NYC weather high-temperature markets)
No NBA series were open at verification time — this is expected for
early July (NBA offseason) and is not a bug in this client.
"""
import math
import os
import requests
from utils.utils import logger

KALSHI_BASE = os.getenv("KALSHI_API_BASE",
                        "https://api.elections.kalshi.com/trade-api/v2")


def get_markets(series_ticker=None, status="open", limit=200):
    """Public market-data endpoint (no auth required for reads)."""
    params = {"status": status, "limit": limit}
    if series_ticker:
        params["series_ticker"] = series_ticker
    try:
        resp = requests.get(f"{KALSHI_BASE}/markets", params=params, timeout=15)
        if resp.status_code != 200:
            logger.warning(f"[!] Kalshi markets HTTP {resp.status_code}")
            return []
        return resp.json().get("markets", [])
    except Exception as e:
        logger.warning(f"[!] Kalshi request failed: {e}")
        return []


def _dollars_to_cents(val):
    """Convert a dollar-denominated string/number field (e.g. "0.5500") to
    cents as a float. Returns None if val is missing or not parseable."""
    try:
        return round(float(val) * 100.0, 1)
    except (TypeError, ValueError):
        return None


def normalize_market(m):
    yes_bid = _dollars_to_cents(m.get("yes_bid_dollars"))
    yes_ask = _dollars_to_cents(m.get("yes_ask_dollars"))
    prob = None
    if yes_bid is not None and yes_ask is not None and (yes_bid or yes_ask):
        prob = round((yes_bid + yes_ask) / 2.0, 1)  # cents == implied %
    return {"ticker": m.get("ticker", ""), "title": m.get("title", ""),
            "market_prob": prob, "yes_bid": yes_bid, "yes_ask": yes_ask}


def taker_fee_cents(price_cents, contracts=1):
    """Kalshi general fee: ceil(0.07 * C * P * (1-P)) per side, P in dollars."""
    p = price_cents / 100.0
    return math.ceil(0.07 * contracts * p * (1 - p) * 100)

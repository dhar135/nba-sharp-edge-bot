import json
import os
from venues.kalshi import normalize_market, taker_fee_cents

FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "kalshi_markets.json")


def test_normalize_uses_bid_ask_mid():
    m = json.load(open(FIXTURE))["markets"][0]
    norm = normalize_market(m)
    assert norm["ticker"].startswith("KXNBAPTS")
    assert norm["market_prob"] == 57.0  # (55+59)/2


def test_taker_fee_at_50_cents_is_2_cents():
    # ceil(0.07 * 1 * 0.50 * 0.50 * 100) = 2 cents. Verify factor vs current
    # Kalshi fee schedule at execution time.
    assert taker_fee_cents(50) == 2


def test_no_quotes_returns_none_prob():
    assert normalize_market({"ticker": "X", "title": "t"})["market_prob"] is None

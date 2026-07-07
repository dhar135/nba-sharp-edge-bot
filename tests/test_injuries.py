import json
import os
from services.injuries import parse_injuries

FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "espn_injuries.json")


def test_out_and_doubtful_blocked_day_to_day_not():
    payload = json.load(open(FIXTURE))
    blocked = parse_injuries(payload)
    assert "star player" in blocked
    assert "hurt guy" in blocked
    assert "healthy enough guy" not in blocked


def test_malformed_payload_returns_empty():
    assert parse_injuries({}) == set()
    assert parse_injuries({"injuries": [{"nope": 1}]}) == set()

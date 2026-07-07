# src/services/injuries.py
"""ESPN public injuries feed — blocks Out/Doubtful players before projection.
Payload shape verified against the live endpoint on implementation day."""
import requests
from utils.utils import logger

ESPN_INJURIES_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/injuries"
BLOCKING_STATUSES = {"out", "doubtful"}


def parse_injuries(payload):
    blocked = set()
    for team in payload.get("injuries", []) or []:
        for inj in team.get("injuries", []) or []:
            status = str(inj.get("status", "")).lower()
            name = (inj.get("athlete") or {}).get("displayName", "")
            if status in BLOCKING_STATUSES and name:
                blocked.add(name.lower().strip())
    return blocked


def fetch_injury_blocklist():
    try:
        resp = requests.get(ESPN_INJURIES_URL, timeout=10)
        if resp.status_code != 200:
            logger.warning(f"[!] Injury feed HTTP {resp.status_code} — continuing without it.")
            return set()
        blocked = parse_injuries(resp.json())
        logger.info(f"[+] Injury feed: {len(blocked)} players Out/Doubtful.")
        return blocked
    except Exception as e:
        logger.warning(f"[!] Injury feed failed ({e}) — continuing without it.")
        return set()

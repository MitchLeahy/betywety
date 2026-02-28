"""Polymarket REST API client (Gamma API)."""

import requests

BASE_URL = "https://gamma-api.polymarket.com"


def fetch_events(
    active: bool = True,
    closed: bool = False,
    tag_id: int | None = None,
    limit: int = 100,
) -> list[dict]:
    """
    Fetch events from Polymarket Gamma API (paginated).

    Args:
        active: only active events (default: True)
        closed: include closed events (default: False)
        tag_id: optional tag ID to filter (e.g. sports)
        limit: page size (default: 100)

    Returns:
        List of event dicts (each may contain nested 'markets')
    """
    url = f"{BASE_URL}/events"
    all_events = []
    offset = 0

    while True:
        params = {
            "active": str(active).lower(),
            "closed": str(closed).lower(),
            "limit": limit,
            "offset": offset,
        }
        if tag_id is not None:
            params["tag_id"] = tag_id

        r = requests.get(url, params=params)
        r.raise_for_status()
        events = r.json()

        if not events:
            break

        all_events.extend(events)
        if len(events) < limit:
            break
        offset += limit

    return all_events


def fetch_markets(
    active: bool = True,
    closed: bool = False,
    limit: int = 100,
) -> list[dict]:
    """
    Fetch markets from Polymarket Gamma API (paginated).

    Args:
        active: only active markets (default: True)
        closed: include closed markets (default: False)
        limit: page size (default: 100)

    Returns:
        List of market dicts
    """
    url = f"{BASE_URL}/markets"
    all_markets = []
    offset = 0

    while True:
        params = {
            "active": str(active).lower(),
            "closed": str(closed).lower(),
            "limit": limit,
            "offset": offset,
        }

        r = requests.get(url, params=params)
        r.raise_for_status()
        markets = r.json()

        if not markets:
            break

        all_markets.extend(markets)
        if len(markets) < limit:
            break
        offset += limit

    return all_markets


def extract_markets_from_events(events: list[dict]) -> list[dict]:
    """
    Extract nested market dicts from event responses.

    Args:
        events: list of event dicts from fetch_events()

    Returns:
        Flat list of market dicts with event_id added
    """
    markets = []
    for event in events:
        event_id = event.get("id")
        for market in event.get("markets", []):
            market["_event_id"] = event_id
            markets.append(market)
    return markets

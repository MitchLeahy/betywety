"""Kalshi REST API client."""

import requests

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"


def fetch_events(series_ticker: str, status: str = "open", limit: int = 200) -> list[dict]:
    """
    Fetch all open events for a series (paginated).

    Args:
        series_ticker: e.g. KXNCAAMBGAME for Men's College Basketball
        status: event status filter (default: open)
        limit: page size (default: 200)

    Returns:
        List of event dicts from the API
    """
    url = f"{BASE_URL}/events"
    all_events = []
    cursor = None

    while True:
        params = {
            "status": status,
            "series_ticker": series_ticker,
            "limit": limit,
        }
        if cursor:
            params["cursor"] = cursor

        r = requests.get(url, params=params)
        r.raise_for_status()
        data = r.json()

        all_events.extend(data["events"])
        cursor = data.get("cursor")
        if not cursor:
            break

    return all_events

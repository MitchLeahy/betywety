"""
Utilities for matching betting markets across different sportsbooks.
"""

from thefuzz import fuzz, process
from typing import List, Dict, Optional
from datetime import datetime, timedelta


def normalize_event_name(name: str) -> str:
    """
    Normalize event name for matching.
    
    Args:
        name: Raw event name
    
    Returns:
        Normalized event name
    """
    # Convert to lowercase
    name = name.lower()
    
    # Remove common prefixes/suffixes
    prefixes = ["nfl", "nba", "mlb", "nhl", "college", "cfb", "cbb"]
    for prefix in prefixes:
        if name.startswith(prefix + " "):
            name = name[len(prefix) + 1 :]
    
    # Remove special characters but keep spaces
    name = "".join(c if c.isalnum() or c.isspace() else " " for c in name)
    
    # Remove extra spaces
    name = " ".join(name.split())
    
    return name.strip()


def extract_teams_from_name(name: str) -> List[str]:
    """
    Extract team names from event name.
    
    Args:
        name: Event name (e.g., "Lakers vs Warriors")
    
    Returns:
        List of team names
    """
    # Common separators
    separators = [" vs ", " v ", " @ ", " at ", " - "]
    
    for sep in separators:
        if sep in name:
            parts = name.split(sep)
            return [normalize_event_name(p) for p in parts]
    
    return [normalize_event_name(name)]


def match_events(
    event_a: Dict, event_b: Dict, threshold: int = 85
) -> bool:
    """
    Determine if two events are the same.
    
    Args:
        event_a: First event dict with 'name', 'sport', 'start_time'
        event_b: Second event dict with 'name', 'sport', 'start_time'
        threshold: Similarity threshold (0-100)
    
    Returns:
        True if events match
    """
    # Must be same sport
    if event_a.get("sport") != event_b.get("sport"):
        return False
    
    # Check time proximity (within 2 hours)
    if "start_time" in event_a and "start_time" in event_b:
        try:
            time_a = datetime.fromisoformat(event_a["start_time"])
            time_b = datetime.fromisoformat(event_b["start_time"])
            time_diff = abs((time_a - time_b).total_seconds())
            if time_diff > 7200:  # 2 hours
                return False
        except (ValueError, TypeError):
            pass
    
    # Check name similarity
    name_a = normalize_event_name(event_a.get("name", ""))
    name_b = normalize_event_name(event_b.get("name", ""))
    
    # Try exact match first
    if name_a == name_b:
        return True
    
    # Try fuzzy match
    similarity = fuzz.ratio(name_a, name_b)
    if similarity >= threshold:
        return True
    
    # Try matching team names
    teams_a = extract_teams_from_name(event_a.get("name", ""))
    teams_b = extract_teams_from_name(event_b.get("name", ""))
    
    if len(teams_a) == 2 and len(teams_b) == 2:
        # Check if teams match (order independent)
        matches = 0
        for team_a in teams_a:
            for team_b in teams_b:
                if fuzz.ratio(team_a, team_b) >= threshold:
                    matches += 1
                    break
        
        if matches >= 2:
            return True
    
    return False


def find_matching_markets(
    market_a: Dict, markets: List[Dict], threshold: int = 85
) -> Optional[Dict]:
    """
    Find a matching market from a list.
    
    Args:
        market_a: Market to match
        markets: List of markets to search
        threshold: Similarity threshold
    
    Returns:
        Matching market or None
    """
    event_a = {
        "name": market_a.get("event_name", ""),
        "sport": market_a.get("sport", ""),
        "start_time": market_a.get("start_time"),
    }
    
    for market_b in markets:
        event_b = {
            "name": market_b.get("event_name", ""),
            "sport": market_b.get("sport", ""),
            "start_time": market_b.get("start_time"),
        }
        
        if match_events(event_a, event_b, threshold):
            # Also check if market type matches
            if market_a.get("market_type") == market_b.get("market_type"):
                return market_b
    
    return None

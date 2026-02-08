"""Transformer for The Odds API data to silver layer format."""

from typing import Dict, Any, List, Optional
from datetime import datetime
import logging
from dateutil import parser as date_parser

from src.utils.odds_converter import (
    american_to_decimal,
    american_to_implied_probability,
    decimal_to_american,
    decimal_to_implied_probability
)

logger = logging.getLogger(__name__)


class OddsApiTransformer:
    """Transform The Odds API raw data to normalized betting lines."""
    
    @staticmethod
    def transform(raw_response: Dict[str, Any], bronze_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Transform The Odds API response to normalized betting lines.
        
        Args:
            raw_response: Raw API response from bronze layer
            bronze_id: ID of the bronze record
        
        Returns:
            List of normalized betting line dictionaries
        """
        lines = []
        
        try:
            # Extract data from response
            data = raw_response.get("data", [])
            
            if not data:
                logger.warning("No data found in Odds API response")
                return lines
            
            timestamp = datetime.utcnow()
            
            for game in data:
                try:
                    # Extract game information
                    event_name = game.get("sport_title", "") or f"{game.get('home_team', '')} vs {game.get('away_team', '')}"
                    sport = game.get("sport_key", "").replace("_", "").lower()
                    
                    # Parse start time
                    event_start_time = None
                    if game.get("commence_time"):
                        try:
                            event_start_time = date_parser.parse(game["commence_time"])
                        except Exception:
                            pass
                    
                    # Process each bookmaker
                    bookmakers = game.get("bookmakers", [])
                    for bookmaker in bookmakers:
                        source = bookmaker.get("key", "unknown")
                        markets = bookmaker.get("markets", [])
                        
                        for market in markets:
                            market_type = market.get("key", "").lower()
                            outcomes = market.get("outcomes", [])
                            
                            for outcome in outcomes:
                                line = OddsApiTransformer._create_line(
                                    event_name=event_name,
                                    sport=sport,
                                    market_type=market_type,
                                    outcome=outcome,
                                    source=source,
                                    timestamp=timestamp,
                                    bronze_id=bronze_id,
                                    event_start_time=event_start_time
                                )
                                if line:
                                    lines.append(line)
                                    
                except Exception as e:
                    logger.error(f"Error transforming Odds API game: {e}", exc_info=True)
                    continue
            
            logger.info(f"Transformed {len(lines)} lines from Odds API data")
            
        except Exception as e:
            logger.error(f"Error transforming Odds API response: {e}", exc_info=True)
        
        return lines
    
    @staticmethod
    def _create_line(
        event_name: str,
        sport: str,
        market_type: str,
        outcome: Dict[str, Any],
        source: str,
        timestamp: datetime,
        bronze_id: Optional[str],
        event_start_time: Optional[datetime]
    ) -> Optional[Dict[str, Any]]:
        """
        Create a normalized betting line from Odds API outcome.
        
        Args:
            event_name: Event name
            sport: Sport type
            market_type: Market type (h2h, spreads, totals)
            outcome: Outcome data from API
            source: Bookmaker name
            timestamp: Timestamp
            bronze_id: Bronze record ID
            event_start_time: Event start time
        
        Returns:
            Normalized line dictionary or None
        """
        try:
            outcome_name = outcome.get("name", "")
            odds = outcome.get("price")
            
            if odds is None:
                return None
            
            # Convert to American odds if needed
            # The Odds API returns American odds by default
            american_odds = float(odds)
            decimal_odds = american_to_decimal(american_odds)
            implied_prob = american_to_implied_probability(american_odds)
            
            # Extract line value for spreads/totals
            line_value = None
            if market_type in ["spreads", "totals"]:
                line_value = outcome.get("point")
            
            return {
                "timestamp": timestamp,
                "source": source,
                "event_name": event_name,
                "sport": sport,
                "market_type": market_type,
                "market_description": f"{market_type} - {outcome_name}",
                "outcome_name": outcome_name,
                "outcome_odds": american_odds,
                "outcome_odds_decimal": decimal_odds,
                "line_value": float(line_value) if line_value is not None else None,
                "implied_probability": implied_prob,
                "is_active": True,  # Assume active if returned by API
                "event_start_time": event_start_time,
                "bronze_id": bronze_id
            }
        except Exception as e:
            logger.error(f"Error creating Odds API line: {e}", exc_info=True)
            return None

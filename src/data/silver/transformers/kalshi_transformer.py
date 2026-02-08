"""Transformer for Kalshi API data to silver layer format."""

from typing import Dict, Any, List, Optional
from datetime import datetime
import logging

from src.utils.odds_converter import (
    american_to_decimal,
    american_to_implied_probability,
    decimal_to_american,
    decimal_to_implied_probability
)

logger = logging.getLogger(__name__)


class KalshiTransformer:
    """Transform Kalshi raw data to normalized betting lines."""
    
    @staticmethod
    def transform(raw_response: Dict[str, Any], bronze_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Transform Kalshi API response to normalized betting lines.
        
        Args:
            raw_response: Raw API response from bronze layer
            bronze_id: ID of the bronze record
        
        Returns:
            List of normalized betting line dictionaries
        """
        lines = []
        
        try:
            # Extract data from response
            data = raw_response.get("data", {})
            markets = data.get("markets", [])
            
            if not markets:
                logger.warning("No markets found in Kalshi response")
                return lines
            
            timestamp = datetime.utcnow()
            
            for market in markets:
                try:
                    # Extract market information
                    event_name = market.get("event_ticker", "") or market.get("title", "")
                    sport = market.get("category", "").lower()
                    market_type = market.get("subtitle", "").lower()
                    
                    # Kalshi markets are typically Yes/No questions
                    outcomes = market.get("yes_bid", 0), market.get("no_bid", 0)
                    
                    # Convert Kalshi prices (0-100) to odds
                    # Kalshi uses price format where price = probability * 100
                    # We need to convert to odds
                    
                    yes_price = market.get("yes_bid", 0) / 100.0 if market.get("yes_bid") else None
                    no_price = market.get("no_bid", 0) / 100.0 if market.get("no_bid") else None
                    
                    # Create lines for Yes and No outcomes
                    if yes_price and 0 < yes_price < 1:
                        yes_line = KalshiTransformer._create_line(
                            event_name=event_name,
                            sport=sport,
                            market_type=market_type or "yes_no",
                            outcome_name="Yes",
                            price=yes_price,
                            timestamp=timestamp,
                            bronze_id=bronze_id,
                            market_data=market
                        )
                        if yes_line:
                            lines.append(yes_line)
                    
                    if no_price and 0 < no_price < 1:
                        no_line = KalshiTransformer._create_line(
                            event_name=event_name,
                            sport=sport,
                            market_type=market_type or "yes_no",
                            outcome_name="No",
                            price=no_price,
                            timestamp=timestamp,
                            bronze_id=bronze_id,
                            market_data=market
                        )
                        if no_line:
                            lines.append(no_line)
                            
                except Exception as e:
                    logger.error(f"Error transforming Kalshi market: {e}", exc_info=True)
                    continue
            
            logger.info(f"Transformed {len(lines)} lines from Kalshi data")
            
        except Exception as e:
            logger.error(f"Error transforming Kalshi response: {e}", exc_info=True)
        
        return lines
    
    @staticmethod
    def _create_line(
        event_name: str,
        sport: str,
        market_type: str,
        outcome_name: str,
        price: float,
        timestamp: datetime,
        bronze_id: Optional[str],
        market_data: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Create a normalized betting line from Kalshi price.
        
        Args:
            event_name: Event name
            sport: Sport type
            market_type: Market type
            outcome_name: Outcome name (Yes/No)
            price: Kalshi price (0-1 probability)
            timestamp: Timestamp
            bronze_id: Bronze record ID
            market_data: Full market data
        
        Returns:
            Normalized line dictionary or None
        """
        try:
            # Convert price to decimal odds
            decimal_odds = 1.0 / price if price > 0 else None
            if not decimal_odds or decimal_odds < 1.01:
                return None
            
            # Convert to American odds
            american_odds = decimal_to_american(decimal_odds)
            
            # Calculate implied probability
            implied_prob = decimal_to_implied_probability(decimal_odds)
            
            return {
                "timestamp": timestamp,
                "source": "kalshi",
                "event_name": event_name,
                "sport": sport,
                "market_type": market_type,
                "market_description": market_data.get("title", ""),
                "outcome_name": outcome_name,
                "outcome_odds": float(american_odds),
                "outcome_odds_decimal": float(decimal_odds),
                "line_value": None,  # Kalshi doesn't use line values
                "implied_probability": float(implied_prob),
                "is_active": market_data.get("status") == "open",
                "event_start_time": None,  # Extract if available
                "bronze_id": bronze_id
            }
        except Exception as e:
            logger.error(f"Error creating Kalshi line: {e}", exc_info=True)
            return None

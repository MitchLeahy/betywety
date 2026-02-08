"""Data quality validators for silver layer."""

from typing import Dict, Any, Optional, List
import logging

logger = logging.getLogger(__name__)


class DataValidator:
    """Validate betting line data quality."""
    
    @staticmethod
    def validate_odds(odds: Optional[float], odds_format: str = "american") -> bool:
        """
        Validate odds are in reasonable range.
        
        Args:
            odds: Odds value
            odds_format: 'american' or 'decimal'
        
        Returns:
            True if valid, False otherwise
        """
        if odds is None:
            return False
        
        if odds_format == "american":
            # American odds: typically -10000 to +10000
            return -10000 <= odds <= 10000
        elif odds_format == "decimal":
            # Decimal odds: typically 1.01 to 1000
            return 1.01 <= odds <= 1000
        else:
            return False
    
    @staticmethod
    def validate_probability(prob: Optional[float]) -> bool:
        """
        Validate probability is between 0 and 1.
        
        Args:
            prob: Probability value
        
        Returns:
            True if valid, False otherwise
        """
        if prob is None:
            return False
        return 0.0 <= prob <= 1.0
    
    @staticmethod
    def validate_line_value(line_value: Optional[float], market_type: str) -> bool:
        """
        Validate line value for spreads/totals.
        
        Args:
            line_value: Line value (e.g., -3.5, 225.5)
            market_type: Type of market
        
        Returns:
            True if valid or not required
        """
        if market_type in ["spread", "total"]:
            if line_value is None:
                return False
            # Reasonable range for spreads: -100 to +100
            # Reasonable range for totals: 0 to 500
            if market_type == "spread":
                return -100 <= line_value <= 100
            else:  # total
                return 0 <= line_value <= 500
        return True  # Not required for other market types
    
    @staticmethod
    def validate_betting_line(line_data: Dict[str, Any]) -> tuple[bool, List[str]]:
        """
        Validate a complete betting line.
        
        Args:
            line_data: Dictionary with betting line data
        
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        # Required fields
        required_fields = ["source", "event_name", "outcome_name"]
        for field in required_fields:
            if not line_data.get(field):
                errors.append(f"Missing required field: {field}")
        
        # Validate odds
        odds = line_data.get("outcome_odds")
        if not DataValidator.validate_odds(odds, "american"):
            errors.append(f"Invalid American odds: {odds}")
        
        odds_decimal = line_data.get("outcome_odds_decimal")
        if odds_decimal and not DataValidator.validate_odds(odds_decimal, "decimal"):
            errors.append(f"Invalid decimal odds: {odds_decimal}")
        
        # Validate probability
        prob = line_data.get("implied_probability")
        if prob and not DataValidator.validate_probability(prob):
            errors.append(f"Invalid probability: {prob}")
        
        # Validate line value
        market_type = line_data.get("market_type")
        line_value = line_data.get("line_value")
        if not DataValidator.validate_line_value(line_value, market_type or ""):
            errors.append(f"Invalid line value for {market_type}: {line_value}")
        
        return len(errors) == 0, errors

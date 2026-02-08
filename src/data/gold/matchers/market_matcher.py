"""Market matching across different sources."""

from typing import List, Dict, Any, Optional, Tuple
from sqlalchemy.orm import Session
import logging

from src.models.silver import BettingLine
from src.models.gold import MatchedMarket
from src.utils.market_matcher import match_events

logger = logging.getLogger(__name__)


class MarketMatcher:
    """Match betting markets across different sources."""
    
    def __init__(self, confidence_threshold: float = 0.85):
        """
        Initialize market matcher.
        
        Args:
            confidence_threshold: Minimum confidence score for matching (0-1)
        """
        self.confidence_threshold = confidence_threshold
    
    def find_matches(
        self,
        kalshi_lines: List[BettingLine],
        other_lines: List[BettingLine],
        db_session: Session
    ) -> List[MatchedMarket]:
        """
        Find matching markets between Kalshi and other sources.
        
        Args:
            kalshi_lines: List of Kalshi betting lines
            other_lines: List of betting lines from other sources
            db_session: Database session
        
        Returns:
            List of matched market records
        """
        matches = []
        
        for kalshi_line in kalshi_lines:
            for other_line in other_lines:
                # Check if events match
                event_a = {
                    "name": kalshi_line.event_name,
                    "sport": kalshi_line.sport,
                    "start_time": kalshi_line.event_start_time.isoformat() if kalshi_line.event_start_time else None
                }
                event_b = {
                    "name": other_line.event_name,
                    "sport": other_line.sport,
                    "start_time": other_line.event_start_time.isoformat() if other_line.event_start_time else None
                }
                
                if match_events(event_a, event_b, threshold=int(self.confidence_threshold * 100)):
                    # Check if market types match
                    if kalshi_line.market_type == other_line.market_type:
                        # Calculate confidence score
                        confidence = self._calculate_confidence(kalshi_line, other_line)
                        
                        if confidence >= self.confidence_threshold:
                            # Create matched market
                            matched_market = MatchedMarket(
                                event_name=kalshi_line.event_name,
                                sport=kalshi_line.sport,
                                market_type=kalshi_line.market_type,
                                kalshi_line_id=kalshi_line.id,
                                other_line_id=other_line.id,
                                other_source=other_line.source,
                                confidence_score=confidence
                            )
                            db_session.add(matched_market)
                            matches.append(matched_market)
        
        try:
            db_session.commit()
            logger.info(f"Created {len(matches)} matched markets")
        except Exception as e:
            logger.error(f"Error committing matched markets: {e}", exc_info=True)
            db_session.rollback()
            raise
        
        return matches
    
    def _calculate_confidence(
        self,
        line_a: BettingLine,
        line_b: BettingLine
    ) -> float:
        """
        Calculate confidence score for a match.
        
        Args:
            line_a: First betting line
            line_b: Second betting line
        
        Returns:
            Confidence score (0-1)
        """
        # Base confidence from event matching
        confidence = 0.7
        
        # Boost if market types match exactly
        if line_a.market_type == line_b.market_type:
            confidence += 0.2
        
        # Boost if outcomes are similar (for same market type)
        if line_a.outcome_name.lower() == line_b.outcome_name.lower():
            confidence += 0.1
        
        return min(confidence, 1.0)

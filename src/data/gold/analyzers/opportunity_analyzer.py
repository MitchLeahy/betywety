"""Analyze betting opportunities and arbitrage."""

from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session
import logging
from datetime import datetime, timedelta

from src.models.silver import BettingLine
from src.models.gold import MatchedMarket, Opportunity, ArbitrageOpportunity
from src.analyzers.arbitrage_detector import detect_arbitrage, find_all_arbitrages
from src.utils.odds_converter import (
    american_to_implied_probability,
    calculate_expected_value
)

logger = logging.getLogger(__name__)


class OpportunityAnalyzer:
    """Analyze matched markets for opportunities and arbitrage."""
    
    def __init__(
        self,
        min_ev_threshold: float = 5.0,
        min_arbitrage_threshold: float = 1.0
    ):
        """
        Initialize opportunity analyzer.
        
        Args:
            min_ev_threshold: Minimum expected value percentage
            min_arbitrage_threshold: Minimum arbitrage profit percentage
        """
        self.min_ev_threshold = min_ev_threshold
        self.min_arbitrage_threshold = min_arbitrage_threshold
    
    def analyze_matched_markets(
        self,
        matched_markets: List[MatchedMarket],
        db_session: Session
    ) -> tuple[List[Opportunity], List[ArbitrageOpportunity]]:
        """
        Analyze matched markets for opportunities and arbitrage.
        
        Args:
            matched_markets: List of matched markets
            db_session: Database session
        
        Returns:
            Tuple of (opportunities, arbitrage_opportunities)
        """
        opportunities = []
        arbitrage_opportunities = []
        
        for matched_market in matched_markets:
            try:
                # Get the actual line records
                kalshi_line = db_session.query(BettingLine).filter(
                    BettingLine.id == matched_market.kalshi_line_id
                ).first()
                other_line = db_session.query(BettingLine).filter(
                    BettingLine.id == matched_market.other_line_id
                ).first()
                
                if not kalshi_line or not other_line:
                    continue
                
                # Check for arbitrage first
                arb = self._check_arbitrage(kalshi_line, other_line, matched_market)
                if arb:
                    arbitrage_opportunities.append(arb)
                
                # Check for opportunistic bets
                opp = self._check_opportunity(kalshi_line, other_line, matched_market)
                if opp:
                    opportunities.append(opp)
                    
            except Exception as e:
                logger.error(
                    f"Error analyzing matched market {matched_market.id}: {e}",
                    exc_info=True
                )
                continue
        
        # Store in database
        try:
            for opp in opportunities:
                db_session.add(opp)
            for arb in arbitrage_opportunities:
                db_session.add(arb)
            db_session.commit()
            logger.info(
                f"Created {len(opportunities)} opportunities and "
                f"{len(arbitrage_opportunities)} arbitrage opportunities"
            )
        except Exception as e:
            logger.error(f"Error storing opportunities: {e}", exc_info=True)
            db_session.rollback()
            raise
        
        return opportunities, arbitrage_opportunities
    
    def _check_arbitrage(
        self,
        kalshi_line: BettingLine,
        other_line: BettingLine,
        matched_market: MatchedMarket
    ) -> Optional[ArbitrageOpportunity]:
        """Check for arbitrage opportunity."""
        try:
            # Create market dicts for arbitrage detector
            kalshi_market = {
                "outcomes": [
                    {"name": kalshi_line.outcome_name, "odds": float(kalshi_line.outcome_odds)},
                    {"name": "opposite", "odds": -float(kalshi_line.outcome_odds)}  # Simplified
                ],
                "id": str(kalshi_line.id),
                "event_name": kalshi_line.event_name,
                "timestamp": kalshi_line.timestamp
            }
            
            other_market = {
                "outcomes": [
                    {"name": other_line.outcome_name, "odds": float(other_line.outcome_odds)},
                    {"name": "opposite", "odds": -float(other_line.outcome_odds)}  # Simplified
                ],
                "source": other_line.source,
                "id": str(other_line.id)
            }
            
            arb_result = detect_arbitrage(
                kalshi_market,
                other_market,
                min_profit_threshold=self.min_arbitrage_threshold
            )
            
            if arb_result:
                return ArbitrageOpportunity(
                    matched_market_id=matched_market.id,
                    kalshi_side=arb_result["kalshi_side"],
                    other_side=arb_result["other_side"],
                    profit_percentage=arb_result["profit_percentage"],
                    optimal_bet_sizing=arb_result.get("optimal_bet_sizing", {}),
                    expires_at=datetime.utcnow() + timedelta(hours=1)  # Default expiry
                )
        except Exception as e:
            logger.error(f"Error checking arbitrage: {e}", exc_info=True)
        
        return None
    
    def _check_opportunity(
        self,
        kalshi_line: BettingLine,
        other_line: BettingLine,
        matched_market: MatchedMarket
    ) -> Optional[Opportunity]:
        """Check for opportunistic bet (better value on Kalshi)."""
        try:
            # Compare implied probabilities
            kalshi_prob = float(kalshi_line.implied_probability) if kalshi_line.implied_probability else None
            other_prob = float(other_line.implied_probability) if other_line.implied_probability else None
            
            if not kalshi_prob or not other_prob:
                return None
            
            # Calculate expected value if betting on Kalshi
            # EV = (kalshi_prob * other_payout) - (1 - kalshi_prob) * stake
            # Simplified: compare probabilities
            if kalshi_prob < other_prob:  # Better odds on Kalshi
                # Calculate EV
                decimal_odds_kalshi = float(kalshi_line.outcome_odds_decimal) if kalshi_line.outcome_odds_decimal else None
                if decimal_odds_kalshi:
                    ev = calculate_expected_value(kalshi_prob, decimal_odds_kalshi, 100.0)
                    
                    if ev >= self.min_ev_threshold:
                        return Opportunity(
                            matched_market_id=matched_market.id,
                            type="opportunistic",
                            kalshi_side={
                                "outcome": kalshi_line.outcome_name,
                                "odds": float(kalshi_line.outcome_odds),
                                "implied_prob": kalshi_prob
                            },
                            other_side={
                                "source": other_line.source,
                                "outcome": other_line.outcome_name,
                                "odds": float(other_line.outcome_odds),
                                "implied_prob": other_prob
                            },
                            expected_value=ev,
                            expires_at=datetime.utcnow() + timedelta(hours=1)  # Default expiry
                        )
        except Exception as e:
            logger.error(f"Error checking opportunity: {e}", exc_info=True)
        
        return None

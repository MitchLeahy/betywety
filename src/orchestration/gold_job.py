"""Gold layer aggregation job."""

import logging
from typing import List
from sqlalchemy.orm import Session

from src.models.silver import BettingLine
from src.models.gold import MatchedMarket
from src.models.database import SessionLocal
from src.data.gold.matchers.market_matcher import MarketMatcher
from src.data.gold.analyzers.opportunity_analyzer import OpportunityAnalyzer
from src.data.gold.storage import GoldStorage

logger = logging.getLogger(__name__)


class GoldAggregationJob:
    """Job to match markets and detect opportunities/arbitrage."""
    
    def __init__(
        self,
        min_ev_threshold: float = 5.0,
        min_arbitrage_threshold: float = 1.0
    ):
        """
        Initialize gold aggregation job.
        
        Args:
            min_ev_threshold: Minimum expected value percentage
            min_arbitrage_threshold: Minimum arbitrage profit percentage
        """
        self.matcher = MarketMatcher()
        self.analyzer = OpportunityAnalyzer(
            min_ev_threshold=min_ev_threshold,
            min_arbitrage_threshold=min_arbitrage_threshold
        )
        self.storage = GoldStorage()
    
    def run(self, db_session: Session = None) -> dict:
        """
        Run the gold aggregation job.
        
        Args:
            db_session: Database session (creates new if not provided)
        
        Returns:
            Dictionary with counts of matches, opportunities, and arbitrage
        """
        logger.info("Starting gold aggregation job")
        
        close_session = False
        if db_session is None:
            db_session = SessionLocal()
            close_session = True
        
        try:
            # Get recent active lines
            kalshi_lines = db_session.query(BettingLine).filter(
                BettingLine.source == "kalshi",
                BettingLine.is_active == True
            ).all()
            
            other_lines = db_session.query(BettingLine).filter(
                BettingLine.source != "kalshi",
                BettingLine.is_active == True
            ).all()
            
            if not kalshi_lines or not other_lines:
                logger.info("Insufficient lines for matching")
                return {"matches": 0, "opportunities": 0, "arbitrage": 0}
            
            logger.info(
                f"Matching {len(kalshi_lines)} Kalshi lines against "
                f"{len(other_lines)} other lines"
            )
            
            # Match markets
            matched_markets = self.matcher.find_matches(
                kalshi_lines=kalshi_lines,
                other_lines=other_lines,
                db_session=db_session
            )
            
            if not matched_markets:
                logger.info("No markets matched")
                return {"matches": 0, "opportunities": 0, "arbitrage": 0}
            
            # Analyze for opportunities and arbitrage
            opportunities, arbitrage_opportunities = self.analyzer.analyze_matched_markets(
                matched_markets=matched_markets,
                db_session=db_session
            )
            
            # Store to Parquet
            self.storage.store_opportunities(opportunities)
            self.storage.store_arbitrage(arbitrage_opportunities)
            
            result = {
                "matches": len(matched_markets),
                "opportunities": len(opportunities),
                "arbitrage": len(arbitrage_opportunities)
            }
            
            logger.info(
                f"Gold aggregation job completed: {result['matches']} matches, "
                f"{result['opportunities']} opportunities, "
                f"{result['arbitrage']} arbitrage opportunities"
            )
            
            return result
            
        finally:
            if close_session:
                db_session.close()

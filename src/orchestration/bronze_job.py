"""Bronze layer ingestion job."""

import logging
from typing import List, Optional
from datetime import datetime

from src.data.bronze.collectors.kalshi_collector import KalshiCollector
from src.data.bronze.collectors.odds_api_collector import OddsApiCollector
from src.models.bronze import RawLine

logger = logging.getLogger(__name__)


class BronzeIngestionJob:
    """Job to collect raw data from all sources and store in bronze layer."""
    
    def __init__(self):
        """Initialize the ingestion job."""
        self.collectors = []
        self._setup_collectors()
    
    def _setup_collectors(self):
        """Set up all collectors."""
        try:
            kalshi_collector = KalshiCollector()
            self.collectors.append(("kalshi", kalshi_collector))
        except Exception as e:
            logger.warning(f"Failed to initialize Kalshi collector: {e}")
        
        try:
            odds_collector = OddsApiCollector()
            self.collectors.append(("odds_api", odds_collector))
        except Exception as e:
            logger.warning(f"Failed to initialize Odds API collector: {e}")
    
    def run(self, **kwargs) -> List[Optional[RawLine]]:
        """
        Run the bronze ingestion job.
        
        Args:
            **kwargs: Parameters to pass to collectors
        
        Returns:
            List of RawLine instances (one per collector)
        """
        logger.info("Starting bronze ingestion job")
        results = []
        
        for name, collector in self.collectors:
            try:
                logger.info(f"Collecting data from {name}")
                result = collector.collect(**kwargs.get(name, {}))
                results.append(result)
                
                if result:
                    logger.info(
                        f"Successfully collected from {name}: {result.id}"
                    )
                else:
                    logger.warning(f"No data collected from {name}")
                    
            except Exception as e:
                logger.error(
                    f"Error collecting from {name}: {e}",
                    exc_info=True
                )
                results.append(None)
        
        logger.info(f"Bronze ingestion job completed. Collected {sum(1 for r in results if r)} results.")
        return results

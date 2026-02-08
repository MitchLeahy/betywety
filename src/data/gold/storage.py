"""Storage utilities for gold layer - write to PostgreSQL and Parquet."""

import os
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional
import pandas as pd

from src.models.gold import Opportunity, ArbitrageOpportunity, MatchedMarket

logger = logging.getLogger(__name__)


class GoldStorage:
    """Handle storage of gold layer data to Parquet files."""
    
    def __init__(self, data_dir: Optional[str] = None):
        """
        Initialize gold storage.
        
        Args:
            data_dir: Base directory for Parquet files (default: ./data/gold)
        """
        self.data_dir = Path(data_dir or os.getenv("DATA_DIR", "./data/gold"))
        self.data_dir.mkdir(parents=True, exist_ok=True)
    
    def store_opportunities(self, opportunities: List[Opportunity]):
        """Store opportunities to Parquet file."""
        if not opportunities:
            return
        
        self._store_to_parquet(
            opportunities,
            "opportunities",
            lambda opp: {
                "id": str(opp.id),
                "matched_market_id": str(opp.matched_market_id),
                "type": opp.type,
                "kalshi_side": str(opp.kalshi_side),
                "other_side": str(opp.other_side),
                "expected_value": float(opp.expected_value) if opp.expected_value else None,
                "detected_at": opp.detected_at,
                "expires_at": opp.expires_at,
                "is_active": opp.is_active
            }
        )
    
    def store_arbitrage(self, arbitrage: List[ArbitrageOpportunity]):
        """Store arbitrage opportunities to Parquet file."""
        if not arbitrage:
            return
        
        self._store_to_parquet(
            arbitrage,
            "arbitrage_opportunities",
            lambda arb: {
                "id": str(arb.id),
                "matched_market_id": str(arb.matched_market_id),
                "kalshi_side": str(arb.kalshi_side),
                "other_side": str(arb.other_side),
                "profit_percentage": float(arb.profit_percentage),
                "optimal_bet_sizing": str(arb.optimal_bet_sizing),
                "detected_at": arb.detected_at,
                "expires_at": arb.expires_at,
                "is_active": arb.is_active
            }
        )
    
    def _store_to_parquet(self, records: List, table_name: str, transform_func):
        """Store records to Parquet file partitioned by date."""
        if not records:
            return
        
        # Group by date
        records_by_date = {}
        for record in records:
            date_key = record.detected_at.date()
            if date_key not in records_by_date:
                records_by_date[date_key] = []
            records_by_date[date_key].append(record)
        
        # Write each date partition
        for date, date_records in records_by_date.items():
            partition_path = (
                self.data_dir / table_name /
                str(date.year) / f"{date.month:02d}" / f"{date.day:02d}"
            )
            partition_path.mkdir(parents=True, exist_ok=True)
            
            # Create DataFrame
            df = pd.DataFrame([transform_func(record) for record in date_records])
            
            # Append to or create Parquet file
            parquet_file = partition_path / "data.parquet"
            
            if parquet_file.exists():
                existing_df = pd.read_parquet(parquet_file)
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                combined_df.to_parquet(parquet_file, index=False, engine="pyarrow")
            else:
                df.to_parquet(parquet_file, index=False, engine="pyarrow")

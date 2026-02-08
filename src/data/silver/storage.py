"""Storage utilities for silver layer - write to PostgreSQL and Parquet."""

import os
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional
import pandas as pd
from sqlalchemy.orm import Session

from src.models.silver import BettingLine
from src.models.database import SessionLocal
from src.data.silver.validators import DataValidator

logger = logging.getLogger(__name__)


class SilverStorage:
    """Handle storage of cleaned data to both PostgreSQL and Parquet files."""
    
    def __init__(self, data_dir: Optional[str] = None):
        """
        Initialize silver storage.
        
        Args:
            data_dir: Base directory for Parquet files (default: ./data/silver)
        """
        self.data_dir = Path(data_dir or os.getenv("DATA_DIR", "./data/silver"))
        self.data_dir.mkdir(parents=True, exist_ok=True)
    
    def store_betting_lines(
        self,
        lines: List[Dict[str, Any]],
        db_session: Optional[Session] = None
    ) -> List[BettingLine]:
        """
        Store betting lines to both PostgreSQL and Parquet.
        
        Args:
            lines: List of normalized betting line dictionaries
            db_session: Database session (creates new if not provided)
        
        Returns:
            List of BettingLine model instances
        """
        if not lines:
            return []
        
        # Validate and filter lines
        valid_lines = []
        for line in lines:
            is_valid, errors = DataValidator.validate_betting_line(line)
            if is_valid:
                valid_lines.append(line)
            else:
                logger.warning(f"Invalid line skipped: {errors}")
        
        if not valid_lines:
            logger.warning("No valid lines to store")
            return []
        
        # Store in PostgreSQL
        close_session = False
        if db_session is None:
            db_session = SessionLocal()
            close_session = True
        
        stored_lines = []
        try:
            for line_data in valid_lines:
                betting_line = BettingLine(**line_data)
                db_session.add(betting_line)
                stored_lines.append(betting_line)
            
            db_session.commit()
            
            # Refresh to get IDs
            for line in stored_lines:
                db_session.refresh(line)
            
            # Store in Parquet file
            self._store_to_parquet(stored_lines)
            
            logger.info(f"Stored {len(stored_lines)} betting lines to silver layer")
            
        except Exception as e:
            logger.error(f"Error storing betting lines: {e}", exc_info=True)
            db_session.rollback()
            raise
        finally:
            if close_session:
                db_session.close()
        
        return stored_lines
    
    def _store_to_parquet(self, betting_lines: List[BettingLine]):
        """Store betting lines to Parquet file partitioned by date."""
        if not betting_lines:
            return
        
        # Group by date for partitioning
        lines_by_date = {}
        for line in betting_lines:
            date_key = line.timestamp.date()
            if date_key not in lines_by_date:
                lines_by_date[date_key] = []
            lines_by_date[date_key].append(line)
        
        # Write each date partition
        for date, lines in lines_by_date.items():
            partition_path = (
                self.data_dir / "betting_lines" /
                str(date.year) / f"{date.month:02d}" / f"{date.day:02d}"
            )
            partition_path.mkdir(parents=True, exist_ok=True)
            
            # Create DataFrame
            df = pd.DataFrame([{
                "id": str(line.id),
                "timestamp": line.timestamp,
                "source": line.source,
                "event_name": line.event_name,
                "sport": line.sport,
                "market_type": line.market_type,
                "market_description": line.market_description,
                "outcome_name": line.outcome_name,
                "outcome_odds": float(line.outcome_odds) if line.outcome_odds else None,
                "outcome_odds_decimal": float(line.outcome_odds_decimal) if line.outcome_odds_decimal else None,
                "line_value": float(line.line_value) if line.line_value else None,
                "implied_probability": float(line.implied_probability) if line.implied_probability else None,
                "is_active": line.is_active,
                "event_start_time": line.event_start_time,
                "bronze_id": str(line.bronze_id) if line.bronze_id else None,
                "created_at": line.created_at
            } for line in lines])
            
            # Append to or create Parquet file
            parquet_file = partition_path / "data.parquet"
            
            if parquet_file.exists():
                existing_df = pd.read_parquet(parquet_file)
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                combined_df.to_parquet(parquet_file, index=False, engine="pyarrow")
            else:
                df.to_parquet(parquet_file, index=False, engine="pyarrow")

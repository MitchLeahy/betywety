"""Storage utilities for bronze layer - write to PostgreSQL and Parquet."""

import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
import json
import pandas as pd
from sqlalchemy.orm import Session

from src.models.bronze import RawLine
from src.models.database import SessionLocal


class BronzeStorage:
    """Handle storage of raw data to both PostgreSQL and Parquet files."""
    
    def __init__(self, data_dir: Optional[str] = None):
        """
        Initialize bronze storage.
        
        Args:
            data_dir: Base directory for Parquet files (default: ./data/bronze)
        """
        self.data_dir = Path(data_dir or os.getenv("DATA_DIR", "./data/bronze"))
        self.data_dir.mkdir(parents=True, exist_ok=True)
    
    def store_raw_response(
        self,
        source: str,
        raw_response: Dict[str, Any],
        api_endpoint: Optional[str] = None,
        response_status: Optional[int] = None,
        timestamp: Optional[datetime] = None,
        db_session: Optional[Session] = None
    ) -> RawLine:
        """
        Store raw API response to both PostgreSQL and Parquet.
        
        Args:
            source: Data source name (e.g., 'kalshi', 'odds_api')
            raw_response: Raw API response as dict
            api_endpoint: API endpoint that was called
            response_status: HTTP status code
            timestamp: Timestamp of the response (default: now)
            db_session: Database session (creates new if not provided)
        
        Returns:
            RawLine model instance
        """
        if timestamp is None:
            timestamp = datetime.utcnow()
        
        # Store in PostgreSQL
        close_session = False
        if db_session is None:
            db_session = SessionLocal()
            close_session = True
        
        try:
            raw_line = RawLine(
                source=source,
                timestamp=timestamp,
                api_endpoint=api_endpoint,
                raw_response=raw_response,
                response_status=response_status
            )
            db_session.add(raw_line)
            db_session.commit()
            db_session.refresh(raw_line)
            
            # Store in Parquet file
            self._store_to_parquet(raw_line)
            
            return raw_line
        finally:
            if close_session:
                db_session.close()
    
    def _store_to_parquet(self, raw_line: RawLine):
        """Store raw line to Parquet file partitioned by date/hour."""
        # Create partition path: bronze/raw_lines/{source}/{year}/{month}/{day}/{hour}/
        partition_path = (
            self.data_dir / "raw_lines" / raw_line.source /
            str(raw_line.timestamp.year) / 
            f"{raw_line.timestamp.month:02d}" /
            f"{raw_line.timestamp.day:02d}" /
            f"{raw_line.timestamp.hour:02d}"
        )
        partition_path.mkdir(parents=True, exist_ok=True)
        
        # Create DataFrame from raw line
        df = pd.DataFrame([{
            "id": str(raw_line.id),
            "source": raw_line.source,
            "timestamp": raw_line.timestamp,
            "api_endpoint": raw_line.api_endpoint,
            "raw_response": json.dumps(raw_line.raw_response),
            "response_status": raw_line.response_status,
            "created_at": raw_line.created_at
        }])
        
        # Append to Parquet file (or create new)
        parquet_file = partition_path / f"data_{raw_line.timestamp.hour:02d}.parquet"
        
        if parquet_file.exists():
            # Append to existing file
            existing_df = pd.read_parquet(parquet_file)
            combined_df = pd.concat([existing_df, df], ignore_index=True)
            combined_df.to_parquet(parquet_file, index=False, engine="pyarrow")
        else:
            # Create new file
            df.to_parquet(parquet_file, index=False, engine="pyarrow")

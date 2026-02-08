"""Silver layer transformation job."""

import logging
from typing import Optional
from sqlalchemy.orm import Session

from src.models.bronze import RawLine
from src.models.database import SessionLocal, get_db
from src.data.silver.transformers.kalshi_transformer import KalshiTransformer
from src.data.silver.transformers.odds_api_transformer import OddsApiTransformer
from src.data.silver.storage import SilverStorage

logger = logging.getLogger(__name__)


class SilverTransformationJob:
    """Job to transform bronze data to silver layer."""
    
    def __init__(self):
        """Initialize the transformation job."""
        self.storage = SilverStorage()
        self.transformers = {
            "kalshi": KalshiTransformer(),
            "odds_api": OddsApiTransformer(),
        }
    
    def run(self, limit: int = 100, db_session: Optional[Session] = None) -> int:
        """
        Run the silver transformation job.
        
        Args:
            limit: Maximum number of bronze records to process
            db_session: Database session (creates new if not provided)
        
        Returns:
            Number of lines transformed and stored
        """
        logger.info("Starting silver transformation job")
        
        close_session = False
        if db_session is None:
            db_session = SessionLocal()
            close_session = True
        
        try:
            # Get unprocessed bronze records (those without corresponding silver records)
            # For simplicity, process recent records
            from sqlalchemy import text
            
            # Get recent bronze records
            bronze_records = db_session.query(RawLine).order_by(
                RawLine.timestamp.desc()
            ).limit(limit).all()
            
            if not bronze_records:
                logger.info("No bronze records to process")
                return 0
            
            total_lines = 0
            
            for bronze_record in bronze_records:
                try:
                    # Get transformer for source
                    transformer = self.transformers.get(bronze_record.source)
                    if not transformer:
                        logger.warning(
                            f"No transformer found for source: {bronze_record.source}"
                        )
                        continue
                    
                    # Transform raw response
                    raw_response = bronze_record.raw_response
                    if not raw_response or "error" in raw_response:
                        logger.debug(f"Skipping error record: {bronze_record.id}")
                        continue
                    
                    lines = transformer.transform(
                        raw_response=raw_response,
                        bronze_id=str(bronze_record.id)
                    )
                    
                    if lines:
                        # Store in silver layer
                        stored_lines = self.storage.store_betting_lines(
                            lines=lines,
                            db_session=db_session
                        )
                        total_lines += len(stored_lines)
                        logger.debug(
                            f"Transformed {len(stored_lines)} lines from "
                            f"bronze record {bronze_record.id}"
                        )
                    
                except Exception as e:
                    logger.error(
                        f"Error transforming bronze record {bronze_record.id}: {e}",
                        exc_info=True
                    )
                    continue
            
            logger.info(
                f"Silver transformation job completed. "
                f"Transformed {total_lines} lines from {len(bronze_records)} bronze records."
            )
            return total_lines
            
        finally:
            if close_session:
                db_session.close()

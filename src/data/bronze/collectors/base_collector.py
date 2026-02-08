"""Base collector class for all data sources."""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from datetime import datetime
import logging

from src.data.bronze.storage import BronzeStorage

logger = logging.getLogger(__name__)


class BaseCollector(ABC):
    """Abstract base class for all data collectors."""
    
    def __init__(self, source_name: str, storage: Optional[BronzeStorage] = None):
        """
        Initialize collector.
        
        Args:
            source_name: Name of the data source (e.g., 'kalshi', 'odds_api')
            storage: BronzeStorage instance (creates new if not provided)
        """
        self.source_name = source_name
        self.storage = storage or BronzeStorage()
        self.logger = logging.getLogger(f"{__name__}.{source_name}")
    
    @abstractmethod
    def fetch_data(self, **kwargs) -> Dict[str, Any]:
        """
        Fetch data from the API.
        
        Args:
            **kwargs: Source-specific parameters
        
        Returns:
            Raw API response as dictionary
        """
        pass
    
    def collect(self, **kwargs) -> Optional[Any]:
        """
        Collect data and store in bronze layer.
        
        Args:
            **kwargs: Parameters to pass to fetch_data
        
        Returns:
            RawLine model instance or None if collection failed
        """
        try:
            self.logger.info(f"Starting data collection from {self.source_name}")
            
            # Fetch data
            raw_response = self.fetch_data(**kwargs)
            
            if not raw_response:
                self.logger.warning(f"No data received from {self.source_name}")
                return None
            
            # Store in bronze layer
            raw_line = self.storage.store_raw_response(
                source=self.source_name,
                raw_response=raw_response,
                api_endpoint=self._get_api_endpoint(**kwargs),
                response_status=200  # Assume success if we got data
            )
            
            self.logger.info(
                f"Successfully stored {self.source_name} data: {raw_line.id}"
            )
            return raw_line
            
        except Exception as e:
            self.logger.error(
                f"Error collecting data from {self.source_name}: {e}",
                exc_info=True
            )
            # Store error response
            try:
                error_response = {
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "timestamp": datetime.utcnow().isoformat()
                }
                self.storage.store_raw_response(
                    source=self.source_name,
                    raw_response=error_response,
                    response_status=500
                )
            except Exception as storage_error:
                self.logger.error(
                    f"Failed to store error response: {storage_error}",
                    exc_info=True
                )
            return None
    
    def _get_api_endpoint(self, **kwargs) -> Optional[str]:
        """
        Get the API endpoint that was called.
        
        Override in subclasses to provide endpoint information.
        """
        return None

"""Kalshi API collector for bronze layer."""

import os
import httpx
from typing import Dict, Any, Optional
import logging

from src.data.bronze.collectors.base_collector import BaseCollector

logger = logging.getLogger(__name__)


class KalshiCollector(BaseCollector):
    """Collector for Kalshi API data."""
    
    def __init__(self, api_key: Optional[str] = None, api_secret: Optional[str] = None):
        """
        Initialize Kalshi collector.
        
        Args:
            api_key: Kalshi API key (from env if not provided)
            api_secret: Kalshi API secret (from env if not provided)
        """
        super().__init__(source_name="kalshi")
        self.api_key = api_key or os.getenv("KALSHI_API_KEY")
        self.api_secret = api_secret or os.getenv("KALSHI_API_SECRET")
        self.base_url = "https://api.kalshi.com/trade-api/v2"
        
        if not self.api_key or not self.api_secret:
            logger.warning(
                "Kalshi API credentials not found. Set KALSHI_API_KEY and "
                "KALSHI_API_SECRET environment variables."
            )
    
    def fetch_data(self, **kwargs) -> Dict[str, Any]:
        """
        Fetch markets from Kalshi API.
        
        Args:
            **kwargs: Optional parameters:
                - exchange_id: Exchange ID to filter
                - limit: Number of results (default: 100)
                - cursor: Pagination cursor
        
        Returns:
            Raw API response
        """
        if not self.api_key or not self.api_secret:
            raise ValueError("Kalshi API credentials not configured")
        
        # Build request
        endpoint = "/markets"
        url = f"{self.base_url}{endpoint}"
        
        params = {
            "limit": kwargs.get("limit", 100),
        }
        
        if "exchange_id" in kwargs:
            params["exchange_id"] = kwargs["exchange_id"]
        if "cursor" in kwargs:
            params["cursor"] = kwargs["cursor"]
        
        # Make request with authentication
        # Note: Kalshi uses API key authentication
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        try:
            with httpx.Client(timeout=30.0) as client:
                response = client.get(url, headers=headers, params=params)
                response.raise_for_status()
                
                return {
                    "endpoint": endpoint,
                    "params": params,
                    "status_code": response.status_code,
                    "data": response.json(),
                    "headers": dict(response.headers)
                }
        except httpx.HTTPError as e:
            logger.error(f"HTTP error fetching Kalshi data: {e}")
            return {
                "endpoint": endpoint,
                "params": params,
                "error": str(e),
                "error_type": "HTTPError"
            }
        except Exception as e:
            logger.error(f"Error fetching Kalshi data: {e}")
            return {
                "endpoint": endpoint,
                "params": params,
                "error": str(e),
                "error_type": type(e).__name__
            }
    
    def _get_api_endpoint(self, **kwargs) -> str:
        """Get the API endpoint."""
        return f"{self.base_url}/markets"

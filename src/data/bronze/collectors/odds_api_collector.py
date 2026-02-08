"""The Odds API collector for bronze layer."""

import os
import httpx
from typing import Dict, Any, Optional
import logging

from src.data.bronze.collectors.base_collector import BaseCollector

logger = logging.getLogger(__name__)


class OddsApiCollector(BaseCollector):
    """Collector for The Odds API data."""
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize The Odds API collector.
        
        Args:
            api_key: The Odds API key (from env if not provided)
        """
        super().__init__(source_name="odds_api")
        self.api_key = api_key or os.getenv("THE_ODDS_API_KEY")
        self.base_url = "https://api.the-odds-api.com/v4"
        
        if not self.api_key:
            logger.warning(
                "The Odds API key not found. Set THE_ODDS_API_KEY "
                "environment variable."
            )
    
    def fetch_data(
        self,
        sport: str = "americanfootball_nfl",
        regions: str = "us",
        markets: str = "h2h,spreads,totals",
        **kwargs
    ) -> Dict[str, Any]:
        """
        Fetch odds from The Odds API.
        
        Args:
            sport: Sport key (default: 'americanfootball_nfl')
            regions: Comma-separated regions (default: 'us')
            markets: Comma-separated markets (default: 'h2h,spreads,totals')
            **kwargs: Additional parameters:
                - oddsFormat: 'american' or 'decimal' (default: 'american')
                - dateFormat: 'iso' or 'unix' (default: 'iso')
        
        Returns:
            Raw API response
        """
        if not self.api_key:
            raise ValueError("The Odds API key not configured")
        
        # Build request
        endpoint = f"/sports/{sport}/odds"
        url = f"{self.base_url}{endpoint}"
        
        params = {
            "apiKey": self.api_key,
            "regions": regions,
            "markets": markets,
            "oddsFormat": kwargs.get("oddsFormat", "american"),
            "dateFormat": kwargs.get("dateFormat", "iso"),
        }
        
        # Add any additional params
        for key in ["bookmakers"]:
            if key in kwargs:
                params[key] = kwargs[key]
        
        try:
            with httpx.Client(timeout=30.0) as client:
                response = client.get(url, params=params)
                response.raise_for_status()
                
                return {
                    "endpoint": endpoint,
                    "params": {k: v for k, v in params.items() if k != "apiKey"},  # Don't store API key
                    "status_code": response.status_code,
                    "data": response.json(),
                    "headers": dict(response.headers)
                }
        except httpx.HTTPError as e:
            logger.error(f"HTTP error fetching Odds API data: {e}")
            return {
                "endpoint": endpoint,
                "params": params,
                "error": str(e),
                "error_type": "HTTPError"
            }
        except Exception as e:
            logger.error(f"Error fetching Odds API data: {e}")
            return {
                "endpoint": endpoint,
                "params": params,
                "error": str(e),
                "error_type": type(e).__name__
            }
    
    def _get_api_endpoint(self, **kwargs) -> str:
        """Get the API endpoint."""
        sport = kwargs.get("sport", "americanfootball_nfl")
        return f"{self.base_url}/sports/{sport}/odds"

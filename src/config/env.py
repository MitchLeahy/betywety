"""Environment variable loading and configuration."""

import os
from pathlib import Path
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)

# Find .env file in project root
_project_root = Path(__file__).parent.parent.parent
_env_path = _project_root / ".env"

# Load .env file if it exists
_env_loaded = False
if _env_path.exists():
    try:
        load_dotenv(_env_path, override=False)
        _env_loaded = True
        logger.debug(f"Loaded .env file from {_env_path}")
    except Exception as e:
        logger.warning(f"Could not load .env file from {_env_path}: {e}")
        # Try loading from current directory as fallback
        try:
            load_dotenv(override=False)
            _env_loaded = True
        except Exception as e2:
            logger.warning(f"Could not load .env file from current directory: {e2}")
else:
    # Try loading from current directory as fallback
    try:
        load_dotenv(override=False)
        _env_loaded = True
    except Exception as e:
        logger.debug(f"Could not load .env file: {e}")


def get_env(key: str, default: str = None) -> str:
    """Get environment variable with .env file support."""
    return os.getenv(key, default)

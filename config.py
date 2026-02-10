"""Central configuration — single source of truth for all configurable values.

Reads from environment variables with sensible defaults.
The only thing that changes between local and cloud is DATA_PATH.
"""

import os

# Storage path — ./data locally, gs://bucket-name in cloud
DATA_PATH = os.environ.get("DATA_PATH", "./data")

# Location
LOCALITY = os.environ.get("LOCALITY", "Toronto")
LATITUDE = float(os.environ.get("LATITUDE", "43.65"))
LONGITUDE = float(os.environ.get("LONGITUDE", "-79.38"))
TIMEZONE = os.environ.get("TIMEZONE", "America/Toronto")

# Historical data range
HISTORICAL_START = os.environ.get("HISTORICAL_START", "2023-01-01")
HISTORICAL_END = os.environ.get("HISTORICAL_END", "2026-02-10")

# API cache expiry in seconds
CACHE_EXPIRY = int(os.environ.get("CACHE_EXPIRY", "3600"))

"""Load transformed data to storage (Parquet files).

Writes to DATA_PATH which can be:
- A local path like ./data (for development)
- A GCS path like gs://bucket-name (for cloud deployment)

pandas.to_parquet() handles both transparently when gcsfs is installed.
"""

import os

import pandas as pd


def save(
    daily_df: pd.DataFrame,
    hourly_df: pd.DataFrame,
    data_path: str,
    sun_df: pd.DataFrame | None = None,
    aqi_df: pd.DataFrame | None = None,
) -> None:
    """Write all DataFrames to Parquet files.

    Creates directory structure if needed (local paths only).
    GCS paths (gs://) are handled by gcsfs automatically.
    sun_df and aqi_df are optional — pipeline doesn't fail if they're None.
    """
    daily_path = f"{data_path}/daily/weather.parquet"
    hourly_path = f"{data_path}/hourly/weather.parquet"

    # Create local directories if needed (no-op for gs:// paths)
    if not data_path.startswith("gs://"):
        os.makedirs(f"{data_path}/daily", exist_ok=True)
        os.makedirs(f"{data_path}/hourly", exist_ok=True)

    daily_df.to_parquet(daily_path, index=False)
    hourly_df.to_parquet(hourly_path, index=False)

    # Supplementary data — best effort
    if sun_df is not None:
        sun_path = f"{data_path}/sun/times.parquet"
        if not data_path.startswith("gs://"):
            os.makedirs(f"{data_path}/sun", exist_ok=True)
        sun_df.to_parquet(sun_path, index=False)

    if aqi_df is not None:
        aqi_path = f"{data_path}/aqi/quality.parquet"
        if not data_path.startswith("gs://"):
            os.makedirs(f"{data_path}/aqi", exist_ok=True)
        aqi_df.to_parquet(aqi_path, index=False)

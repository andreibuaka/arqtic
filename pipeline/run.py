"""Pipeline orchestrator — the entrypoint.

Runs: extract → validate → transform → load
Each stage is wrapped in error handling. If any stage fails,
the pipeline logs the error and exits with code 1.
Bad data never reaches storage silently.

Usage: python -m pipeline.run
   or: make run-pipeline
"""

import sys
import time

from config import (
    DATA_PATH,
    HISTORICAL_END,
    HISTORICAL_START,
    LATITUDE,
    LOCALITY,
    LONGITUDE,
    TIMEZONE,
)
from pipeline.extract import (
    extract_air_quality,
    extract_forecast,
    extract_historical,
    extract_sun_times,
)
from pipeline.load import save
from pipeline.quality import DailyWeatherSchema, HourlyWeatherSchema, validate
from pipeline.transform import transform


def main():
    start = time.time()
    print(f"{'=' * 60}")
    print(f"Arqtic Weather Pipeline — {LOCALITY}")
    print(f"{'=' * 60}")
    print(f"Location: {LATITUDE}, {LONGITUDE} ({TIMEZONE})")
    print(f"Historical range: {HISTORICAL_START} → {HISTORICAL_END}")
    print(f"Output: {DATA_PATH}")
    print()

    # --- Extract ---
    print("[1/4] Extracting data from Open-Meteo...")
    try:
        daily_df = extract_historical(LATITUDE, LONGITUDE, HISTORICAL_START, HISTORICAL_END, TIMEZONE)
        hourly_df = extract_forecast(LATITUDE, LONGITUDE, TIMEZONE)
    except RuntimeError as e:
        print(f"  EXTRACTION FAILED: {e}")
        sys.exit(1)

    print(f"  Historical daily: {len(daily_df):,} rows")
    print(f"  Hourly forecast:  {len(hourly_df):,} rows")

    # Supplementary data — best effort, pipeline doesn't fail if these error
    sun_df = None
    aqi_df = None

    try:
        sun_df = extract_sun_times(LATITUDE, LONGITUDE, TIMEZONE)
        print(f"  Sun times:        {len(sun_df)} days")
    except Exception as e:
        print(f"  Sun times:        SKIPPED ({e})")

    try:
        aqi_df = extract_air_quality(LATITUDE, LONGITUDE, TIMEZONE)
        print(f"  Air quality:      {len(aqi_df)} hours")
    except Exception as e:
        print(f"  Air quality:      SKIPPED ({e})")

    print()

    # --- Validate ---
    print("[2/4] Validating data quality (Pandera)...")
    try:
        daily_df = validate(daily_df, DailyWeatherSchema)
        hourly_df = validate(hourly_df, HourlyWeatherSchema)
    except Exception as e:
        print(f"  VALIDATION FAILED: {e}")
        sys.exit(1)

    print("  Daily schema:  PASSED ✓")
    print("  Hourly schema: PASSED ✓")
    print()

    # --- Transform ---
    print("[3/4] Transforming data...")
    daily_df, hourly_df = transform(daily_df, hourly_df)

    new_daily_cols = [
        c
        for c in daily_df.columns
        if c
        not in {
            "date",
            "temperature_2m_max",
            "temperature_2m_min",
            "apparent_temperature_max",
            "apparent_temperature_min",
            "precipitation_sum",
            "wind_speed_10m_max",
            "relative_humidity_2m_mean",
            "weather_code",
            "daylight_duration",
        }
    ]
    print(f"  Added {len(new_daily_cols)} columns to daily: {new_daily_cols}")
    print()

    # --- Load ---
    print(f"[4/4] Saving to {DATA_PATH}...")
    save(daily_df, hourly_df, DATA_PATH, sun_df=sun_df, aqi_df=aqi_df)

    elapsed = time.time() - start
    print()
    print(f"{'=' * 60}")
    print(f"Pipeline complete in {elapsed:.1f}s")
    print(f"  {len(daily_df):,} daily rows → {DATA_PATH}/daily/weather.parquet")
    print(f"  {len(hourly_df):,} hourly rows → {DATA_PATH}/hourly/weather.parquet")
    if sun_df is not None:
        print(f"  {len(sun_df)} sun time rows → {DATA_PATH}/sun/times.parquet")
    if aqi_df is not None:
        print(f"  {len(aqi_df)} air quality rows → {DATA_PATH}/aqi/quality.parquet")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()

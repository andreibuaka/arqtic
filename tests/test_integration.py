"""Integration test — full pipeline E2E + dashboard data loading.

This test runs the actual pipeline against the Open-Meteo API,
writes to a temporary directory, then verifies the dashboard's
data loading functions can read everything back.

Marked with @pytest.mark.integration so it can be run separately
from fast unit tests: pytest -m integration
"""

import os
import tempfile

import duckdb
import pytest

from pipeline.extract import (
    extract_air_quality,
    extract_forecast,
    extract_historical,
    extract_sun_times,
)
from pipeline.load import save
from pipeline.quality import DailyWeatherSchema, HourlyWeatherSchema, validate
from pipeline.transform import transform


@pytest.mark.integration
class TestPipelineE2E:
    """Run the full pipeline against a real API and verify outputs."""

    @pytest.fixture(autouse=True)
    def setup_temp_dir(self):
        """Create a temporary data directory for each test run."""
        with tempfile.TemporaryDirectory() as tmpdir:
            self.data_path = tmpdir
            self.lat = 43.65
            self.lon = -79.38
            self.tz = "America/Toronto"
            yield

    def test_full_pipeline_produces_valid_output(self):
        """E2E: extract → validate → transform → load → read back."""
        # --- Extract ---
        daily_df = extract_historical(self.lat, self.lon, "2025-01-01", "2025-01-31", self.tz)
        hourly_df = extract_forecast(self.lat, self.lon, self.tz)

        assert len(daily_df) > 0, "Daily extraction returned no rows"
        assert len(hourly_df) > 0, "Hourly extraction returned no rows"

        # --- Validate ---
        daily_df = validate(daily_df, DailyWeatherSchema)
        hourly_df = validate(hourly_df, HourlyWeatherSchema)

        # --- Transform ---
        daily_df, hourly_df = transform(daily_df, hourly_df)

        # Verify transform added expected columns
        assert "comfort_label" in daily_df.columns
        assert "condition_icon" in daily_df.columns
        assert "is_anomaly" in daily_df.columns
        assert "vs_historical_avg" in daily_df.columns
        assert "wind_label" in hourly_df.columns
        assert "visibility_label" in hourly_df.columns
        assert "comfort_label" in hourly_df.columns

        # --- Supplementary data (best-effort) ---
        sun_df = extract_sun_times(self.lat, self.lon, self.tz)
        aqi_df = extract_air_quality(self.lat, self.lon, self.tz)

        assert len(sun_df) > 0, "Sun times extraction returned no rows"
        assert "sunrise" in sun_df.columns
        assert "sunset" in sun_df.columns

        assert len(aqi_df) > 0, "AQI extraction returned no rows"
        assert "us_aqi" in aqi_df.columns

        # --- Load ---
        save(daily_df, hourly_df, self.data_path, sun_df=sun_df, aqi_df=aqi_df)

        # --- Verify files exist ---
        assert os.path.exists(f"{self.data_path}/daily/weather.parquet")
        assert os.path.exists(f"{self.data_path}/hourly/weather.parquet")
        assert os.path.exists(f"{self.data_path}/sun/times.parquet")
        assert os.path.exists(f"{self.data_path}/aqi/quality.parquet")

        # --- Verify DuckDB can read them (same as dashboard) ---
        daily_back = duckdb.query(f"SELECT * FROM read_parquet('{self.data_path}/daily/weather.parquet')").df()
        hourly_back = duckdb.query(f"SELECT * FROM read_parquet('{self.data_path}/hourly/weather.parquet')").df()
        sun_back = duckdb.query(f"SELECT * FROM read_parquet('{self.data_path}/sun/times.parquet')").df()
        aqi_back = duckdb.query(f"SELECT * FROM read_parquet('{self.data_path}/aqi/quality.parquet')").df()

        assert len(daily_back) == len(daily_df)
        assert len(hourly_back) == len(hourly_df)
        assert len(sun_back) == len(sun_df)
        assert len(aqi_back) == len(aqi_df)

        # --- Verify dashboard-critical columns survive round-trip ---
        assert "comfort_label" in daily_back.columns
        assert "condition_icon" in daily_back.columns
        assert "wind_label" in hourly_back.columns
        assert "visibility_label" in hourly_back.columns

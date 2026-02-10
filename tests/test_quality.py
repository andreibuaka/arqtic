"""Tests for pipeline/quality.py — Pandera schema validation."""

import pandas as pd
import pytest

from pipeline.quality import DailyWeatherSchema, HourlyWeatherSchema, validate


def _make_valid_daily(overrides=None):
    """Create a valid daily DataFrame for testing."""
    data = {
        "date": pd.Timestamp("2025-06-15"),
        "temperature_2m_max": 25.0,
        "temperature_2m_min": 15.0,
        "apparent_temperature_max": 27.0,
        "apparent_temperature_min": 13.0,
        "precipitation_sum": 2.5,
        "wind_speed_10m_max": 15.0,
        "relative_humidity_2m_mean": 65.0,
        "weather_code": 3.0,
        "daylight_duration": 52000.0,
    }
    if overrides:
        data.update(overrides)
    return pd.DataFrame([data])


def _make_valid_hourly(overrides=None):
    """Create a valid hourly DataFrame for testing."""
    data = {
        "timestamp": pd.Timestamp("2025-06-15 12:00"),
        "temperature_2m": 22.0,
        "apparent_temperature": 24.0,
        "relative_humidity_2m": 55.0,
        "wind_speed_10m": 8.0,
        "wind_gusts_10m": 20.0,
        "precipitation_probability": 10.0,
        "precipitation": 0.5,
        "uv_index": 5.0,
        "weather_code": 2.0,
        "visibility": 10000.0,
    }
    if overrides:
        data.update(overrides)
    return pd.DataFrame([data])


class TestDailySchema:
    def test_valid_data_passes(self):
        df = _make_valid_daily()
        result = validate(df, DailyWeatherSchema)
        assert len(result) == 1

    def test_extreme_temperature_fails(self):
        df = _make_valid_daily({"temperature_2m_max": 999.0})
        with pytest.raises(Exception):
            validate(df, DailyWeatherSchema)

    def test_negative_wind_fails(self):
        df = _make_valid_daily({"wind_speed_10m_max": -5.0})
        with pytest.raises(Exception):
            validate(df, DailyWeatherSchema)

    def test_humidity_over_100_fails(self):
        df = _make_valid_daily({"relative_humidity_2m_mean": 110.0})
        with pytest.raises(Exception):
            validate(df, DailyWeatherSchema)

    def test_negative_precipitation_fails(self):
        df = _make_valid_daily({"precipitation_sum": -1.0})
        with pytest.raises(Exception):
            validate(df, DailyWeatherSchema)


class TestHourlySchema:
    def test_valid_data_passes(self):
        df = _make_valid_hourly()
        result = validate(df, HourlyWeatherSchema)
        assert len(result) == 1

    def test_extreme_temperature_fails(self):
        df = _make_valid_hourly({"temperature_2m": 999.0})
        with pytest.raises(Exception):
            validate(df, HourlyWeatherSchema)

    def test_precip_probability_over_100_fails(self):
        df = _make_valid_hourly({"precipitation_probability": 150.0})
        with pytest.raises(Exception):
            validate(df, HourlyWeatherSchema)

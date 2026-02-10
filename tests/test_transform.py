"""Tests for pipeline/transform.py — computed columns and enrichment."""

import pandas as pd

from pipeline.transform import (
    COMFORT_TRANSLATIONS,
    _get_stress_category,
    add_daylight_hours,
    add_historical_comparison,
    add_thermal_comfort,
    add_weather_conditions,
    flag_anomalies,
    synthesize_visibility_label,
    synthesize_wind_label,
)


class TestWMOCodes:
    def test_known_code_maps_correctly(self):
        df = pd.DataFrame({"weather_code": [73.0]})
        result = add_weather_conditions(df)
        assert result["condition_text"].iloc[0] == "Moderate snow"
        assert result["condition_icon"].iloc[0] == "❄️"

    def test_unknown_code_returns_default(self):
        df = pd.DataFrame({"weather_code": [999.0]})
        result = add_weather_conditions(df)
        assert result["condition_text"].iloc[0] == "Unknown"
        assert result["condition_icon"].iloc[0] == "❓"

    def test_clear_sky(self):
        df = pd.DataFrame({"weather_code": [0.0]})
        result = add_weather_conditions(df)
        assert result["condition_text"].iloc[0] == "Clear sky"
        assert result["condition_icon"].iloc[0] == "☀️"


class TestThermalComfort:
    def test_cold_stress(self):
        assert _get_stress_category(-5.0) == "Moderate cold stress"

    def test_no_thermal_stress(self):
        assert _get_stress_category(20.0) == "No thermal stress"

    def test_heat_stress(self):
        assert _get_stress_category(35.0) == "Strong heat stress"

    def test_extreme_cold(self):
        assert _get_stress_category(-45.0) == "Extreme cold stress"

    def test_comfort_label_mapping(self):
        df = pd.DataFrame({"apparent_temperature": [-1.3]})
        result = add_thermal_comfort(df, "apparent_temperature")
        assert result["stress_category"].iloc[0] == "Moderate cold stress"
        assert result["comfort_label"].iloc[0] == "Cold"
        assert result["comfort_advice"].iloc[0] == "Wear layers and a warm jacket."

    def test_all_categories_have_translations(self):
        test_temps = [-50, -35, -20, -5, 5, 15, 30, 35, 42, 50]
        for temp in test_temps:
            cat = _get_stress_category(temp)
            assert cat in COMFORT_TRANSLATIONS, f"No translation for {cat}"


class TestAnomalyDetection:
    def test_extreme_value_flagged(self):
        # Normal temps around 10, then one spike at 40
        temps = [10.0] * 40 + [40.0] + [10.0] * 10
        df = pd.DataFrame(
            {
                "date": pd.date_range("2025-01-01", periods=len(temps)),
                "temperature_2m_max": temps,
                "wind_speed_10m_max": [5.0] * len(temps),
                "relative_humidity_2m_mean": [60.0] * len(temps),
            }
        )
        result = flag_anomalies(df)
        assert result["is_anomaly"].iloc[40], "The spike should be flagged"

    def test_stable_data_no_anomalies(self):
        df = pd.DataFrame(
            {
                "date": pd.date_range("2025-01-01", periods=60),
                "temperature_2m_max": [10.0] * 60,
                "wind_speed_10m_max": [5.0] * 60,
                "relative_humidity_2m_mean": [60.0] * 60,
            }
        )
        result = flag_anomalies(df)
        assert not result["is_anomaly"].any()


class TestHistoricalComparison:
    def test_delta_is_computed(self):
        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=365 + 10),
                "temperature_2m_max": list(range(375)),
            }
        )
        result = add_historical_comparison(df)
        assert "vs_historical_avg" in result.columns
        # Values should be finite
        assert result["vs_historical_avg"].notna().any()


class TestDaylightHours:
    def test_seconds_to_hours(self):
        df = pd.DataFrame({"daylight_duration": [36000.0]})
        result = add_daylight_hours(df)
        assert result["daylight_hours"].iloc[0] == 10.0

    def test_missing_column_is_safe(self):
        df = pd.DataFrame({"other_col": [1.0]})
        result = add_daylight_hours(df)
        assert "daylight_hours" not in result.columns


class TestWindLabel:
    def test_calm_wind_returns_none(self):
        df = pd.DataFrame({"wind_speed_10m": [10.0], "wind_gusts_10m": [20.0]})
        result = synthesize_wind_label(df)
        assert result["wind_label"].iloc[0] is None

    def test_windy_returns_label(self):
        df = pd.DataFrame({"wind_speed_10m": [25.0], "wind_gusts_10m": [30.0]})
        result = synthesize_wind_label(df)
        assert result["wind_label"].iloc[0] == "Windy — secure loose items"

    def test_dangerous_wind_returns_label(self):
        df = pd.DataFrame({"wind_speed_10m": [50.0], "wind_gusts_10m": [70.0]})
        result = synthesize_wind_label(df)
        assert result["wind_label"].iloc[0] == "Dangerous wind — limit time outside"


class TestVisibilityLabel:
    def test_clear_visibility_returns_none(self):
        df = pd.DataFrame({"visibility": [10000.0]})
        result = synthesize_visibility_label(df)
        assert result["visibility_label"].iloc[0] is None

    def test_reduced_visibility_returns_label(self):
        df = pd.DataFrame({"visibility": [3000.0]})
        result = synthesize_visibility_label(df)
        assert result["visibility_label"].iloc[0] == "Reduced visibility"

    def test_fog_returns_label(self):
        df = pd.DataFrame({"visibility": [500.0]})
        result = synthesize_visibility_label(df)
        assert result["visibility_label"].iloc[0] == "Low visibility — fog"

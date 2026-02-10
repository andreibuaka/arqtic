"""Data transformations — where raw weather data becomes useful to humans.

Each function adds computed columns to the DataFrames:
- Thermal comfort labels with actionable advice
- WMO weather descriptions and icons
- Wind and visibility synthesis labels
- Anomaly detection (z-score)
- Historical comparison deltas
- Daylight hours
"""

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# WMO Weather Interpretation Codes → human description + emoji icon
# https://open-meteo.com/en/docs — "WMO Weather interpretation codes"
# ---------------------------------------------------------------------------
WMO_CODES: dict[int, tuple[str, str]] = {
    0: ("Clear sky", "☀️"),
    1: ("Mainly clear", "🌤️"),
    2: ("Partly cloudy", "⛅"),
    3: ("Overcast", "☁️"),
    45: ("Fog", "🌫️"),
    48: ("Depositing rime fog", "🌫️"),
    51: ("Light drizzle", "🌦️"),
    53: ("Moderate drizzle", "🌦️"),
    55: ("Dense drizzle", "🌧️"),
    56: ("Light freezing drizzle", "🌧️"),
    57: ("Dense freezing drizzle", "🌧️"),
    61: ("Slight rain", "🌧️"),
    63: ("Moderate rain", "🌧️"),
    65: ("Heavy rain", "🌧️"),
    66: ("Light freezing rain", "🌧️"),
    67: ("Heavy freezing rain", "🌧️"),
    71: ("Slight snow", "🌨️"),
    73: ("Moderate snow", "❄️"),
    75: ("Heavy snow", "❄️"),
    77: ("Snow grains", "❄️"),
    80: ("Slight rain showers", "🌦️"),
    81: ("Moderate rain showers", "🌧️"),
    82: ("Violent rain showers", "🌧️"),
    85: ("Slight snow showers", "🌨️"),
    86: ("Heavy snow showers", "❄️"),
    95: ("Thunderstorm", "⛈️"),
    96: ("Thunderstorm with slight hail", "⛈️"),
    99: ("Thunderstorm with heavy hail", "⛈️"),
}

# ---------------------------------------------------------------------------
# Thermal comfort: apparent temperature → UTCI stress category → everyday label
# Thresholds from the Universal Thermal Climate Index (COST Action 730)
# Applied to apparent_temperature as a practical proxy.
# ---------------------------------------------------------------------------

# Scientific category → (everyday label, action advice, color indicator)
COMFORT_TRANSLATIONS: dict[str, tuple[str, str, str]] = {
    "Extreme heat stress": ("Dangerously hot", "Stay indoors. Hydrate constantly.", "🔴"),
    "Very strong heat stress": ("Very hot", "Limit time outside. Lots of water.", "🟠"),
    "Strong heat stress": ("Hot", "Light clothes, sunscreen, water.", "🟠"),
    "Moderate heat stress": ("Warm", "Comfortable with light clothing.", "🟡"),
    "No thermal stress": ("Perfect", "Enjoy the outdoors!", "🟢"),
    "Slight cold stress": ("Cool", "Grab a light jacket.", "🔵"),
    "Moderate cold stress": ("Cold", "Wear layers and a warm jacket.", "🔵"),
    "Strong cold stress": ("Very cold", "Bundle up. Limit time outside.", "🟣"),
    "Very strong cold stress": ("Bitter cold", "Heavy winter gear essential.", "🟣"),
    "Extreme cold stress": ("Dangerously cold", "Stay indoors if possible.", "🔴"),
}


def _get_stress_category(apparent_temp_c: float) -> str:
    """Map apparent temperature to UTCI-equivalent thermal stress category."""
    if apparent_temp_c > 46:
        return "Extreme heat stress"
    elif apparent_temp_c > 38:
        return "Very strong heat stress"
    elif apparent_temp_c > 32:
        return "Strong heat stress"
    elif apparent_temp_c > 26:
        return "Moderate heat stress"
    elif apparent_temp_c > 9:
        return "No thermal stress"
    elif apparent_temp_c > 0:
        return "Slight cold stress"
    elif apparent_temp_c > -13:
        return "Moderate cold stress"
    elif apparent_temp_c > -27:
        return "Strong cold stress"
    elif apparent_temp_c > -40:
        return "Very strong cold stress"
    else:
        return "Extreme cold stress"


def add_weather_conditions(df: pd.DataFrame) -> pd.DataFrame:
    """Add human-readable weather condition text and icon from WMO codes."""
    default = ("Unknown", "❓")
    df["condition_text"] = df["weather_code"].apply(lambda c: WMO_CODES.get(int(c), default)[0])
    df["condition_icon"] = df["weather_code"].apply(lambda c: WMO_CODES.get(int(c), default)[1])
    return df


def add_thermal_comfort(df: pd.DataFrame, temp_col: str) -> pd.DataFrame:
    """Add thermal stress category and everyday comfort labels.

    Args:
        df: DataFrame with an apparent temperature column.
        temp_col: Name of the apparent temperature column to use.
    """
    df["stress_category"] = df[temp_col].apply(_get_stress_category)
    df["comfort_label"] = df["stress_category"].map(lambda s: COMFORT_TRANSLATIONS[s][0])
    df["comfort_advice"] = df["stress_category"].map(lambda s: COMFORT_TRANSLATIONS[s][1])
    df["comfort_color"] = df["stress_category"].map(lambda s: COMFORT_TRANSLATIONS[s][2])
    return df


def flag_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    """Flag days where weather values exceed 2σ from the 30-day rolling mean.

    Checks temperature_2m_max, wind_speed_10m_max, and relative_humidity_2m_mean.
    """
    df = df.copy()
    df["is_anomaly"] = False

    for col in ["temperature_2m_max", "wind_speed_10m_max", "relative_humidity_2m_mean"]:
        if col not in df.columns:
            continue
        rolling_mean = df[col].rolling(window=30, min_periods=7, center=True).mean()
        rolling_std = df[col].rolling(window=30, min_periods=7, center=True).std()
        is_outlier = (df[col] - rolling_mean).abs() > 2 * rolling_std
        df["is_anomaly"] = df["is_anomaly"] | is_outlier.fillna(False)

    return df


def add_historical_comparison(df: pd.DataFrame) -> pd.DataFrame:
    """Add delta vs. the multi-year day-of-year average temperature.

    Produces: 'vs_historical_avg' = today's temp minus average for this calendar date.
    Positive means warmer than usual, negative means colder.
    """
    df = df.copy()
    df["day_of_year"] = df["date"].dt.dayofyear
    doy_mean = df.groupby("day_of_year")["temperature_2m_max"].transform("mean")
    df["vs_historical_avg"] = df["temperature_2m_max"] - doy_mean
    df.drop(columns=["day_of_year"], inplace=True)
    return df


def add_daylight_hours(df: pd.DataFrame) -> pd.DataFrame:
    """Convert daylight_duration (seconds) to daylight_hours for clean display."""
    if "daylight_duration" in df.columns:
        df["daylight_hours"] = df["daylight_duration"] / 3600
    return df


def synthesize_wind_label(df: pd.DataFrame) -> pd.DataFrame:
    """Add human-readable wind label based on speed and gust thresholds.

    Returns None (no label) when wind is calm — silence means safe.
    Only surfaces a label when wind is noteworthy.
    """
    if "wind_speed_10m" not in df.columns or "wind_gusts_10m" not in df.columns:
        df["wind_label"] = None
        return df

    conditions = [
        (df["wind_speed_10m"] > 40) | (df["wind_gusts_10m"] > 60),
        (df["wind_speed_10m"] > 20) | (df["wind_gusts_10m"] > 40),
    ]
    choices = [
        "Dangerous wind — limit time outside",
        "Windy — secure loose items",
    ]
    df["wind_label"] = np.select(conditions, choices, default=None)
    # np.select returns '0' for default None, fix it
    df["wind_label"] = df["wind_label"].replace("0", None)
    return df


def synthesize_visibility_label(df: pd.DataFrame) -> pd.DataFrame:
    """Add human-readable visibility label for fog/haze conditions.

    Returns None when visibility is fine — only alerts when it matters.
    """
    if "visibility" not in df.columns:
        df["visibility_label"] = None
        return df

    conditions = [
        df["visibility"] < 1000,
        df["visibility"] < 5000,
    ]
    choices = [
        "Low visibility — fog",
        "Reduced visibility",
    ]
    df["visibility_label"] = np.select(conditions, choices, default=None)
    df["visibility_label"] = df["visibility_label"].replace("0", None)
    return df


def transform(daily_df: pd.DataFrame, hourly_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Apply all transformations to daily and hourly DataFrames.

    Returns both enriched DataFrames.
    """
    # --- Daily transformations ---
    daily_df = add_weather_conditions(daily_df)
    daily_df = add_thermal_comfort(daily_df, "apparent_temperature_max")
    daily_df = flag_anomalies(daily_df)
    daily_df = add_historical_comparison(daily_df)
    daily_df = add_daylight_hours(daily_df)

    # --- Hourly transformations ---
    hourly_df = add_weather_conditions(hourly_df)
    hourly_df = add_thermal_comfort(hourly_df, "apparent_temperature")
    hourly_df = synthesize_wind_label(hourly_df)
    hourly_df = synthesize_visibility_label(hourly_df)

    return daily_df, hourly_df

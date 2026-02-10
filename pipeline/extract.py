"""Data extraction from Open-Meteo API.

Fetches historical daily weather data, hourly forecast data,
sunrise/sunset times, and air quality data.
Uses FlatBuffers SDK for zero-copy deserialization, requests-cache
to avoid hammering the API, and retry-requests for resilience.
"""

import openmeteo_requests
import pandas as pd
import requests
import requests_cache
from retry_requests import retry

from config import CACHE_EXPIRY

# Shared client: cached + retry with exponential backoff
_cache_session = requests_cache.CachedSession(".cache", expire_after=CACHE_EXPIRY)
_retry_session = retry(_cache_session, retries=5, backoff_factor=0.2)
_client = openmeteo_requests.Client(session=_retry_session)

# Fields we request from each endpoint
# NOTE: sunrise/sunset removed — archive API returns 0 for string fields,
# and the SDK can't parse them. We fetch these separately via extract_sun_times().
DAILY_FIELDS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "apparent_temperature_max",
    "apparent_temperature_min",
    "precipitation_sum",
    "wind_speed_10m_max",
    "relative_humidity_2m_mean",
    "weather_code",
    "daylight_duration",
]

HOURLY_FIELDS = [
    "temperature_2m",
    "apparent_temperature",
    "relative_humidity_2m",
    "wind_speed_10m",
    "wind_gusts_10m",
    "precipitation_probability",
    "precipitation",
    "uv_index",
    "weather_code",
    "visibility",
    "is_day",
]


def extract_historical(
    latitude: float,
    longitude: float,
    start_date: str,
    end_date: str,
    timezone: str,
) -> pd.DataFrame:
    """Fetch historical daily weather data from Open-Meteo archive API.

    Returns a DataFrame with one row per day, covering the date range.
    Typically ~1,096 rows for 3 years.
    """
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "daily": DAILY_FIELDS,
        "timezone": timezone,
    }

    try:
        responses = _client.weather_api("https://archive-api.open-meteo.com/v1/archive", params=params)
    except Exception as e:
        raise RuntimeError(f"Failed to fetch historical data from Open-Meteo: {e}") from e

    response = responses[0]
    daily = response.Daily()

    # Build date index from SDK timestamps
    dates = pd.date_range(
        start=pd.to_datetime(daily.Time(), unit="s", utc=True),
        end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=daily.Interval()),
        inclusive="left",
    )

    # Extract each variable as numpy array (zero-copy from FlatBuffers)
    data = {"date": dates}
    for i, field in enumerate(DAILY_FIELDS):
        data[field] = daily.Variables(i).ValuesAsNumpy()

    df = pd.DataFrame(data)
    df["date"] = df["date"].dt.tz_localize(None)  # Remove UTC for simplicity
    return df


def extract_forecast(
    latitude: float,
    longitude: float,
    timezone: str,
) -> pd.DataFrame:
    """Fetch hourly forecast data (past 7 days + next 7 days) from Open-Meteo.

    Returns a DataFrame with one row per hour, typically ~336 rows.
    """
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": HOURLY_FIELDS,
        "timezone": timezone,
        "past_days": 7,
        "forecast_days": 7,
    }

    try:
        responses = _client.weather_api("https://api.open-meteo.com/v1/forecast", params=params)
    except Exception as e:
        raise RuntimeError(f"Failed to fetch forecast data from Open-Meteo: {e}") from e

    response = responses[0]
    hourly = response.Hourly()

    timestamps = pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left",
    )

    data = {"timestamp": timestamps}
    for i, field in enumerate(HOURLY_FIELDS):
        data[field] = hourly.Variables(i).ValuesAsNumpy()

    df = pd.DataFrame(data)
    df["timestamp"] = df["timestamp"].dt.tz_localize(None)
    return df


def extract_sun_times(
    latitude: float,
    longitude: float,
    timezone: str,
) -> pd.DataFrame:
    """Fetch sunrise/sunset times via direct HTTP (bypasses SDK string bug).

    The Open-Meteo SDK uses FlatBuffers which can't deserialize the ISO8601
    string values for sunrise/sunset. Direct JSON parsing works fine.

    Returns a small DataFrame with columns: date, sunrise, sunset.
    """
    resp = requests.get(
        "https://api.open-meteo.com/v1/forecast",
        params={
            "latitude": latitude,
            "longitude": longitude,
            "daily": "sunrise,sunset",
            "timezone": timezone,
            "forecast_days": 7,
        },
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()["daily"]

    return pd.DataFrame(
        {
            "date": pd.to_datetime(data["time"]),
            "sunrise": pd.to_datetime(data["sunrise"]),
            "sunset": pd.to_datetime(data["sunset"]),
        }
    )


def extract_air_quality(
    latitude: float,
    longitude: float,
    timezone: str,
) -> pd.DataFrame:
    """Fetch air quality data from Open-Meteo's Air Quality API.

    Same provider, free, no API key. Returns US AQI and PM2.5 hourly data.
    """
    resp = requests.get(
        "https://air-quality-api.open-meteo.com/v1/air-quality",
        params={
            "latitude": latitude,
            "longitude": longitude,
            "hourly": "us_aqi,pm2_5",
            "timezone": timezone,
            "forecast_days": 2,
        },
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()["hourly"]

    return pd.DataFrame(
        {
            "timestamp": pd.to_datetime(data["time"]),
            "us_aqi": data["us_aqi"],
            "pm2_5": data["pm2_5"],
        }
    )

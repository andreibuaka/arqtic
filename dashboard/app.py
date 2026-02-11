"""Arqtic Weather Dashboard — Main Application.

An interactive Streamlit dashboard that reads weather data from Parquet files
via DuckDB and provides 4 tabs: current conditions, trends, forecast, and
data quality monitoring. Supports live city search via Open-Meteo geocoding.
"""

import sys
from pathlib import Path

import duckdb
import pandas as pd
import requests
import streamlit as st

# Add project root to path so config is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import DATA_PATH, LATITUDE, LOCALITY, LONGITUDE, TIMEZONE
from dashboard.components.current import render_current
from dashboard.components.forecast_tab import render_forecast
from dashboard.components.quality import render_quality
from dashboard.components.trends import render_trends

st.set_page_config(page_title="Arqtic Weather", page_icon="🌤️", layout="wide")

# ---------------------------------------------------------------------------
# Quick-pick cities (preset coordinates — no geocoding call needed)
# ---------------------------------------------------------------------------
PRESET_CITIES = {
    "Toronto": {"lat": 43.65, "lon": -79.38, "tz": "America/Toronto", "country": "Canada"},
    "London": {"lat": 51.51, "lon": -0.13, "tz": "Europe/London", "country": "United Kingdom"},
    "Tokyo": {"lat": 35.68, "lon": 139.69, "tz": "Asia/Tokyo", "country": "Japan"},
    "New York": {"lat": 40.71, "lon": -74.01, "tz": "America/New_York", "country": "United States"},
    "Sydney": {"lat": -33.87, "lon": 151.21, "tz": "Australia/Sydney", "country": "Australia"},
    "Dubai": {"lat": 25.28, "lon": 55.30, "tz": "Asia/Dubai", "country": "UAE"},
}


# ---------------------------------------------------------------------------
# Geocoding (cached 24 hours — city coordinates never change)
# ---------------------------------------------------------------------------
@st.cache_data(ttl=86400)
def geocode(query: str) -> list[dict]:
    """Search for a city using Open-Meteo's geocoding API. Handles typos."""
    try:
        resp = requests.get(
            "https://geocoding-api.open-meteo.com/v1/search",
            params={"name": query, "count": 5},
            timeout=5,
        )
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])
        return [
            {
                "name": r["name"],
                "country": r.get("country", ""),
                "admin1": r.get("admin1", ""),
                "lat": r["latitude"],
                "lon": r["longitude"],
                "tz": r.get("timezone", "UTC"),
            }
            for r in results
        ]
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Live data loading (for searched cities)
# ---------------------------------------------------------------------------
@st.cache_data(ttl=600)
def load_live_weather(lat: float, lon: float, tz: str):
    """Fetch weather data from Open-Meteo for any city. Returns (hourly_df, sun_df, aqi_df)."""
    from pipeline.extract import extract_air_quality, extract_forecast, extract_sun_times
    from pipeline.quality import HourlyWeatherSchema, validate
    from pipeline.transform import (
        add_thermal_comfort,
        add_weather_conditions,
        synthesize_visibility_label,
        synthesize_wind_label,
    )

    # Fetch forecast data (the one required call)
    hourly_df = extract_forecast(lat, lon, tz)
    hourly_df = validate(hourly_df, HourlyWeatherSchema)

    # Apply transforms
    hourly_df = add_weather_conditions(hourly_df)
    hourly_df = add_thermal_comfort(hourly_df, "apparent_temperature")
    hourly_df = synthesize_wind_label(hourly_df)
    hourly_df = synthesize_visibility_label(hourly_df)

    # Build a daily summary from hourly data for the "Right Now" tab
    hourly_df["date"] = hourly_df["timestamp"].dt.normalize()
    daily_df = (
        hourly_df.groupby("date")
        .agg(
            temperature_2m_max=("temperature_2m", "max"),
            temperature_2m_min=("temperature_2m", "min"),
            apparent_temperature_max=("apparent_temperature", "max"),
            apparent_temperature_min=("apparent_temperature", "min"),
            wind_speed_10m_max=("wind_speed_10m", "max"),
            relative_humidity_2m_mean=("relative_humidity_2m", "mean"),
            weather_code=("weather_code", lambda x: x.mode().iloc[0] if len(x) > 0 else 0),
            precipitation_sum=("precipitation", "sum"),
        )
        .reset_index()
    )

    # Add daily transforms
    daily_df = add_weather_conditions(daily_df)
    daily_df = add_thermal_comfort(daily_df, "apparent_temperature_max")

    # Best-effort: sun times
    sun_df = None
    try:
        sun_df = extract_sun_times(lat, lon, tz)
    except Exception:
        pass

    # Best-effort: air quality
    aqi_df = None
    try:
        aqi_df = extract_air_quality(lat, lon, tz)
    except Exception:
        pass

    return daily_df, hourly_df, sun_df, aqi_df


# ---------------------------------------------------------------------------
# Parquet data loading (for default city)
# Uses pandas for GCS paths (gcsfs handles auth), DuckDB for local files.
# ---------------------------------------------------------------------------
def _read_parquet(path: str) -> pd.DataFrame:
    """Read a Parquet file from local disk or GCS."""
    if path.startswith("gs://"):
        return pd.read_parquet(path)
    return duckdb.query(f"SELECT * FROM read_parquet('{path}')").df()


@st.cache_data(ttl=600)
def load_daily():
    return _read_parquet(f"{DATA_PATH}/daily/weather.parquet")


@st.cache_data(ttl=600)
def load_hourly():
    return _read_parquet(f"{DATA_PATH}/hourly/weather.parquet")


@st.cache_data(ttl=600)
def load_sun():
    return _read_parquet(f"{DATA_PATH}/sun/times.parquet")


@st.cache_data(ttl=600)
def load_aqi():
    return _read_parquet(f"{DATA_PATH}/aqi/quality.parquet")


# ---------------------------------------------------------------------------
# Sidebar — Search and Location
# ---------------------------------------------------------------------------
st.sidebar.title("Arqtic Weather")

# City search
search_query = st.sidebar.text_input("Search any city...", key="city_search")

# Quick-pick chips
st.sidebar.markdown("**Popular:**")
chip_cols = st.sidebar.columns(len(PRESET_CITIES))
selected_preset = None
for i, (city_name, city_data) in enumerate(PRESET_CITIES.items()):
    if chip_cols[i].button(city_name, key=f"chip_{city_name}", use_container_width=True):
        selected_preset = city_name

# ---------------------------------------------------------------------------
# Resolve which city to show
# ---------------------------------------------------------------------------
# State management
if "active_city" not in st.session_state:
    st.session_state.active_city = None

# Preset chip clicked
if selected_preset:
    city = PRESET_CITIES[selected_preset]
    st.session_state.active_city = {
        "name": selected_preset,
        "country": city["country"],
        "lat": city["lat"],
        "lon": city["lon"],
        "tz": city["tz"],
    }

# Search query submitted
if search_query and not selected_preset:
    results = geocode(search_query)
    if len(results) == 0:
        st.sidebar.warning(f"No cities found for '{search_query}' — try a different spelling.")
    elif len(results) == 1:
        # Auto-select single result
        r = results[0]
        st.session_state.active_city = r
    else:
        # Disambiguate
        options = [f"{r['name']}, {r['admin1']}, {r['country']} ({r['lat']:.2f}, {r['lon']:.2f})" for r in results]
        choice = st.sidebar.selectbox("Pick a city:", options)
        idx = options.index(choice)
        st.session_state.active_city = results[idx]

# Determine if we're in live mode or default mode
active = st.session_state.active_city
is_default = active is None or (abs(active["lat"] - LATITUDE) < 0.01 and abs(active["lon"] - LONGITUDE) < 0.01)

# ---------------------------------------------------------------------------
# Load data (Parquet for default, live API for searched)
# ---------------------------------------------------------------------------
if is_default:
    # Default mode: load from stored Parquet files
    active_name = LOCALITY
    active_country = ""
    active_lat = LATITUDE
    active_lon = LONGITUDE
    active_tz = TIMEZONE

    try:
        daily_df = load_daily()
        hourly_df = load_hourly()
    except Exception:
        st.error("No weather data found. Run the pipeline first: `make run-pipeline`")
        st.stop()

    try:
        sun_df = load_sun()
    except Exception:
        sun_df = None

    try:
        aqi_df = load_aqi()
    except Exception:
        aqi_df = None

    has_historical = True
else:
    # Live mode: fetch from Open-Meteo API
    active_name = active["name"]
    active_country = active.get("country", "")
    active_lat = active["lat"]
    active_lon = active["lon"]
    active_tz = active.get("tz", "UTC")

    with st.spinner(f"Loading weather for {active_name}..."):
        try:
            daily_df, hourly_df, sun_df, aqi_df = load_live_weather(active_lat, active_lon, active_tz)
        except Exception as e:
            st.error(f"Couldn't load weather for {active_name}: {e}")
            st.stop()

    has_historical = False

# ---------------------------------------------------------------------------
# Sidebar — location info
# ---------------------------------------------------------------------------
location_label = f"{active_name}, {active_country}" if active_country else active_name
st.sidebar.markdown(f"**Current:** {location_label}")
lat_dir = "N" if active_lat >= 0 else "S"
lon_dir = "E" if active_lon >= 0 else "W"
st.sidebar.caption(f"{abs(active_lat):.2f}°{lat_dir}, {abs(active_lon):.2f}°{lon_dir} · {active_tz}")

# Show map for non-default cities
if not is_default:
    st.map(pd.DataFrame({"lat": [active_lat], "lon": [active_lon]}), zoom=8)

# ---------------------------------------------------------------------------
# Sidebar — Trends controls (only show when we have historical data)
# ---------------------------------------------------------------------------
if has_historical:
    min_date = daily_df["date"].min().date()
    max_date = daily_df["date"].max().date()
    default_start = max(min_date, max_date - __import__("datetime").timedelta(days=90))

    date_range = st.sidebar.date_input(
        "Date range",
        value=(default_start, max_date),
        min_value=min_date,
        max_value=max_date,
    )
else:
    # For live data, use what we have
    min_date = daily_df["date"].min().date()
    max_date = daily_df["date"].max().date()
    date_range = (min_date, max_date)

# Metric selector for Trends tab
metric_options = {
    "Temperature (High)": "temperature_2m_max",
    "Temperature (Low)": "temperature_2m_min",
    "Feels Like (High)": "apparent_temperature_max",
    "Humidity": "relative_humidity_2m_mean",
    "Wind Speed": "wind_speed_10m_max",
    "Precipitation": "precipitation_sum",
}
if "daylight_hours" in daily_df.columns:
    metric_options["Daylight Hours"] = "daylight_hours"

selected_metric_label = st.sidebar.selectbox("Metric", list(metric_options.keys()))
selected_metric = metric_options[selected_metric_label]

# Granularity for Trends tab
granularity = st.sidebar.radio("Granularity", ["Daily", "Weekly", "Monthly"])

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------
tab1, tab2, tab3, tab4 = st.tabs(["☀️ Right Now", "📈 Trends", "🔮 Forecast", "✅ Data Quality"])

with tab1:
    render_current(daily_df, hourly_df, sun_df=sun_df, aqi_df=aqi_df)

with tab2:
    render_trends(daily_df, selected_metric, selected_metric_label, granularity, date_range)

with tab3:
    render_forecast(daily_df, has_historical=has_historical)

with tab4:
    render_quality(daily_df, DATA_PATH, is_live=not is_default)

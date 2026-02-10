"""Arqtic Weather Dashboard — Main Application.

An interactive Streamlit dashboard that reads weather data from Parquet files
via DuckDB and provides 4 tabs: current conditions, trends, forecast, and
data quality monitoring.
"""

import sys
from pathlib import Path

import duckdb
import streamlit as st

# Add project root to path so config is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import DATA_PATH, LOCALITY
from dashboard.components.current import render_current
from dashboard.components.forecast_tab import render_forecast
from dashboard.components.quality import render_quality
from dashboard.components.trends import render_trends

st.set_page_config(page_title="Arqtic Weather", page_icon="🌤️", layout="wide")


# ---------------------------------------------------------------------------
# Data loading (cached 10 minutes)
# ---------------------------------------------------------------------------
@st.cache_data(ttl=600)
def load_daily():
    path = f"{DATA_PATH}/daily/weather.parquet"
    return duckdb.query(f"SELECT * FROM read_parquet('{path}')").df()


@st.cache_data(ttl=600)
def load_hourly():
    path = f"{DATA_PATH}/hourly/weather.parquet"
    return duckdb.query(f"SELECT * FROM read_parquet('{path}')").df()


@st.cache_data(ttl=600)
def load_sun():
    path = f"{DATA_PATH}/sun/times.parquet"
    return duckdb.query(f"SELECT * FROM read_parquet('{path}')").df()


@st.cache_data(ttl=600)
def load_aqi():
    path = f"{DATA_PATH}/aqi/quality.parquet"
    return duckdb.query(f"SELECT * FROM read_parquet('{path}')").df()


# ---------------------------------------------------------------------------
# Check data exists
# ---------------------------------------------------------------------------
try:
    daily_df = load_daily()
    hourly_df = load_hourly()
except Exception:
    st.error("No weather data found. Run the pipeline first: `make run-pipeline`")
    st.stop()

# Supplementary data — best effort (dashboard still works without these)
try:
    sun_df = load_sun()
except Exception:
    sun_df = None

try:
    aqi_df = load_aqi()
except Exception:
    aqi_df = None

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
st.sidebar.title("Arqtic Weather")
st.sidebar.markdown(f"📍 **{LOCALITY}**")

# Date range filter for Trends tab
min_date = daily_df["date"].min().date()
max_date = daily_df["date"].max().date()
default_start = max(min_date, max_date - __import__("datetime").timedelta(days=90))

date_range = st.sidebar.date_input(
    "Date range",
    value=(default_start, max_date),
    min_value=min_date,
    max_value=max_date,
)

# Metric selector for Trends tab
metric_options = {
    "Temperature (High)": "temperature_2m_max",
    "Temperature (Low)": "temperature_2m_min",
    "Feels Like (High)": "apparent_temperature_max",
    "Humidity": "relative_humidity_2m_mean",
    "Wind Speed": "wind_speed_10m_max",
    "Precipitation": "precipitation_sum",
    "Daylight Hours": "daylight_hours",
}
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
    render_forecast(daily_df)

with tab4:
    render_quality(daily_df, DATA_PATH)

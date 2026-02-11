"""Tab 3: Forecast — Prophet 30-day prediction with uncertainty bands.

Overlays the Open-Meteo physics-based 16-day forecast for comparison.
Wrapped in @st.fragment so Prophet training doesn't rerun when users
interact with other tabs or sidebar filters.
"""

import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st

from config import LATITUDE, LONGITUDE, TIMEZONE
from pipeline.transform import COMFORT_TRANSLATIONS, _get_stress_category


@st.cache_data(ttl=3600)
def _fetch_api_forecast() -> pd.DataFrame | None:
    """Fetch the Open-Meteo 16-day physics-based daily forecast."""
    try:
        resp = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": LATITUDE,
                "longitude": LONGITUDE,
                "daily": "temperature_2m_max",
                "timezone": TIMEZONE,
                "forecast_days": 16,
            },
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()["daily"]
        df = pd.DataFrame({"date": pd.to_datetime(data["time"]), "temp": data["temperature_2m_max"]})
        return df.dropna(subset=["temp"])
    except Exception:
        return None


@st.fragment
def render_forecast(daily_df: pd.DataFrame, has_historical: bool = True):
    """Render the Forecast tab with Prophet predictions."""
    if not has_historical:
        st.markdown("#### Forecast")
        st.info(
            "This tab runs a 30-day Prophet forecast using historical training data. "
            "Prophet is available for the default city which has years of stored data.\n\n"
            "Current conditions and the short-term outlook are in the **Right Now** and **Trends** tabs."
        )
        return

    from forecast.predict import make_forecast

    st.markdown("#### 30-Day Temperature Forecast")
    st.caption("Powered by Prophet — auto-detected yearly seasonality with uncertainty bands.")
    st.caption("Cross-validated accuracy: ~4°C average error over 30-day horizons (7 test windows).")

    # Cache the forecast computation
    @st.cache_data(ttl=3600)
    def _cached_forecast(_df_hash, metric_col, periods):
        return make_forecast(daily_df, metric_col=metric_col, periods=periods)

    # Use a hash of the dataframe length + last date as cache key
    df_hash = f"{len(daily_df)}_{daily_df['date'].iloc[-1]}"

    try:
        forecast = _cached_forecast(df_hash, "temperature_2m_max", 30)
    except Exception as e:
        st.error(f"Forecast failed: {e}")
        return

    # Split into historical and future
    last_date = daily_df["date"].max()
    future = forecast[forecast["ds"] > last_date]

    # Build chart
    fig = go.Figure()

    # Historical actual data
    fig.add_trace(
        go.Scatter(
            x=daily_df["date"].tail(90),
            y=daily_df["temperature_2m_max"].tail(90),
            mode="lines",
            name="Actual",
            line=dict(color="#1f77b4", width=2),
        )
    )

    # Forecast line
    fig.add_trace(
        go.Scatter(
            x=future["ds"],
            y=future["yhat"],
            mode="lines",
            name="Prophet (30-day)",
            line=dict(color="#ff7f0e", width=2, dash="dash"),
        )
    )

    # Uncertainty bands
    fig.add_trace(
        go.Scatter(
            x=pd.concat([future["ds"], future["ds"][::-1]]),
            y=pd.concat([future["yhat_upper"], future["yhat_lower"][::-1]]),
            fill="toself",
            fillcolor="rgba(255,127,14,0.15)",
            line=dict(color="rgba(255,127,14,0)"),
            name="Uncertainty",
            hoverinfo="skip",
        )
    )

    # Overlay the API's physics-based forecast
    api_forecast = _fetch_api_forecast()
    if api_forecast is not None and len(api_forecast) > 0:
        fig.add_trace(
            go.Scatter(
                x=api_forecast["date"],
                y=api_forecast["temp"],
                mode="lines",
                name="Weather Model (16-day)",
                line=dict(color="#2ca02c", width=2),
            )
        )

    fig.update_layout(
        yaxis_title="Temperature (°C)",
        height=450,
        margin=dict(t=30, b=30),
        hovermode="x unified",
    )

    st.plotly_chart(fig, width="stretch")

    st.caption(
        "Green = physics-based weather model (accurate 1-2 weeks). "
        "Orange dashed = Prophet statistical forecast (extends to 30 days)."
    )

    # Next 7 days summary
    st.markdown("#### Next 7 Days Prediction")
    next_7 = future.head(7)
    if len(next_7) > 0:
        cols = st.columns(len(next_7))
        for i, (_, row) in enumerate(next_7.iterrows()):
            day_name = row["ds"].strftime("%a")
            predicted_temp = row["yhat"]
            stress = _get_stress_category(predicted_temp)
            label = COMFORT_TRANSLATIONS[stress][0]
            color = COMFORT_TRANSLATIONS[stress][2]
            with cols[i]:
                st.metric(
                    label=f"{day_name}",
                    value=f"{predicted_temp:.0f}°C",
                    delta=f"{label} {color}",
                    delta_color="off",
                    border=True,
                )

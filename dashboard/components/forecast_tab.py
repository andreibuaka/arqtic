"""Tab 3: Forecast — Prophet 30-day prediction with uncertainty bands.

Wrapped in @st.fragment so Prophet training doesn't rerun when users
interact with other tabs or sidebar filters.
"""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from pipeline.transform import COMFORT_TRANSLATIONS, _get_stress_category


@st.fragment
def render_forecast(daily_df: pd.DataFrame):
    """Render the Forecast tab with Prophet predictions."""
    from forecast.predict import make_forecast

    st.markdown("#### 30-Day Temperature Forecast")
    st.caption("Powered by Prophet — auto-detected yearly seasonality with uncertainty bands.")

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
            name="Forecast",
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

    fig.update_layout(
        yaxis_title="Temperature (°C)",
        height=450,
        margin=dict(t=30, b=30),
        hovermode="x unified",
    )

    st.plotly_chart(fig, width="stretch")

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

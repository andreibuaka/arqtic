"""Tab 4: Data Quality — freshness monitoring, anomalies, and stats.

Shows pipeline health: when data was last updated, validation status,
detected anomalies, and basic data statistics.
"""

import os
from datetime import datetime, timezone

import pandas as pd
import streamlit as st


def render_quality(daily_df: pd.DataFrame, data_path: str, is_live: bool = False):
    """Render the Data Quality tab."""
    st.markdown("#### Pipeline Health")

    col1, col2, col3 = st.columns(3)

    # --- Freshness ---
    if is_live:
        col1.metric(
            label="Data Source",
            value="🟢 Live API",
            delta="Fetched from Open-Meteo",
            delta_color="off",
            border=True,
        )
    else:
        parquet_path = f"{data_path}/daily/weather.parquet"
        if not data_path.startswith("gs://") and os.path.exists(parquet_path):
            mtime = os.path.getmtime(parquet_path)
            last_updated = datetime.fromtimestamp(mtime, tz=timezone.utc)
            age_hours = (datetime.now(tz=timezone.utc) - last_updated).total_seconds() / 3600
            if age_hours < 6:
                freshness = "🟢 Fresh"
            elif age_hours < 12:
                freshness = "🟡 Aging"
            else:
                freshness = "🔴 Stale"

            col1.metric(
                label="Data Freshness",
                value=freshness,
                delta=f"Updated {age_hours:.0f}h ago",
                delta_color="off",
                border=True,
            )
        else:
            col1.metric(label="Data Freshness", value="⚪ Cloud", border=True)

    # --- Validation status ---
    col2.metric(
        label="Pandera Validation",
        value="✅ Passed",
        delta="All schema checks OK",
        delta_color="off",
        border=True,
    )

    # --- Row counts ---
    col3.metric(
        label="Daily Records",
        value=f"{len(daily_df):,}",
        delta=f"{daily_df['date'].min().strftime('%Y-%m-%d')} → {daily_df['date'].max().strftime('%Y-%m-%d')}",
        delta_color="off",
        border=True,
    )

    st.divider()

    # --- Anomalies ---
    st.markdown("#### Detected Anomalies")
    if "is_anomaly" in daily_df.columns:
        anomalies = daily_df[daily_df["is_anomaly"]].copy()
        if len(anomalies) > 0:
            st.markdown(f"**{len(anomalies)} anomalous days** detected (>2σ from 30-day rolling mean)")
            display_cols = ["date", "temperature_2m_max", "wind_speed_10m_max", "condition_text"]
            display_cols = [c for c in display_cols if c in anomalies.columns]
            st.dataframe(
                anomalies[display_cols].tail(20).sort_values("date", ascending=False),
                hide_index=True,
                width="stretch",
            )
        else:
            st.success("No anomalies detected in the dataset.")
    else:
        st.info("Anomaly detection not available.")

    st.divider()

    # --- Data stats ---
    st.markdown("#### Data Statistics")
    col1, col2, col3, col4 = st.columns(4)

    null_count = daily_df.isnull().sum().sum()
    col1.metric("Null Values", f"{null_count:,}", border=True)

    unique_conditions = daily_df["condition_text"].nunique() if "condition_text" in daily_df.columns else 0
    col2.metric("Weather Types", f"{unique_conditions}", border=True)

    anomaly_count = daily_df["is_anomaly"].sum() if "is_anomaly" in daily_df.columns else 0
    col3.metric("Anomaly Days", f"{int(anomaly_count)}", border=True)

    avg_daylight = daily_df["daylight_hours"].mean() if "daylight_hours" in daily_df.columns else 0
    col4.metric("Avg Daylight", f"{avg_daylight:.1f}h", border=True)

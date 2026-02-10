"""Tab 2: Trends — interactive time-series with drill-down.

Uses Plotly with range selector buttons, range slider, and zoom/pan
for the "proper trend visualization and drill-down" the assessment requires.
"""

import duckdb
import pandas as pd
import plotly.graph_objects as go
import streamlit as st


def render_trends(
    daily_df: pd.DataFrame,
    metric_col: str,
    metric_label: str,
    granularity: str,
    date_range: tuple,
):
    """Render the Trends tab with interactive Plotly chart."""
    # Apply date filter
    if len(date_range) == 2:
        start, end = pd.Timestamp(date_range[0]), pd.Timestamp(date_range[1])
        mask = (daily_df["date"] >= start) & (daily_df["date"] <= end)
        filtered = daily_df[mask].copy()
    else:
        filtered = daily_df.copy()

    if len(filtered) == 0:
        st.warning("No data in selected date range.")
        return

    # Aggregate by granularity using DuckDB
    if granularity == "Weekly":
        agg = duckdb.query(f"""
            SELECT date_trunc('week', date) as period,
                   AVG("{metric_col}") as value,
                   COUNT(*) as days
            FROM filtered
            GROUP BY 1
            ORDER BY 1
        """).df()
    elif granularity == "Monthly":
        agg = duckdb.query(f"""
            SELECT date_trunc('month', date) as period,
                   AVG("{metric_col}") as value,
                   COUNT(*) as days
            FROM filtered
            GROUP BY 1
            ORDER BY 1
        """).df()
    else:  # Daily
        agg = filtered[["date", metric_col]].rename(columns={"date": "period", metric_col: "value"})

    # Build Plotly figure
    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=agg["period"],
            y=agg["value"],
            mode="lines",
            name=metric_label,
            line=dict(color="#1f77b4", width=2),
            hovertemplate="%{x|%Y-%m-%d}<br>%{y:.1f}<extra></extra>",
        )
    )

    # Anomaly markers (daily only)
    if granularity == "Daily" and "is_anomaly" in filtered.columns:
        anomalies = filtered[filtered["is_anomaly"]]
        if len(anomalies) > 0:
            fig.add_trace(
                go.Scatter(
                    x=anomalies["date"],
                    y=anomalies[metric_col],
                    mode="markers",
                    name="Anomaly",
                    marker=dict(color="red", size=8, symbol="circle"),
                    hovertemplate="⚠️ Anomaly<br>%{x|%Y-%m-%d}<br>%{y:.1f}<extra></extra>",
                )
            )

    # Range selector buttons for drill-down
    fig.update_xaxes(
        rangeselector=dict(
            buttons=[
                dict(count=7, label="7d", step="day", stepmode="backward"),
                dict(count=14, label="14d", step="day", stepmode="backward"),
                dict(count=1, label="1m", step="month", stepmode="backward"),
                dict(count=3, label="3m", step="month", stepmode="backward"),
                dict(count=6, label="6m", step="month", stepmode="backward"),
                dict(count=1, label="1y", step="year", stepmode="backward"),
                dict(step="all", label="All"),
            ]
        ),
        rangeslider=dict(visible=True),
    )

    fig.update_layout(
        title=f"{metric_label} — {granularity}",
        yaxis_title=metric_label,
        height=500,
        margin=dict(t=60, b=40),
        hovermode="x unified",
    )

    st.plotly_chart(fig, width="stretch")

    # Summary stats
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Mean", f"{agg['value'].mean():.1f}")
    col2.metric("Min", f"{agg['value'].min():.1f}")
    col3.metric("Max", f"{agg['value'].max():.1f}")
    col4.metric("Std Dev", f"{agg['value'].std():.1f}")

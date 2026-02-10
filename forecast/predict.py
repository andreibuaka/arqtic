"""Prophet forecasting module.

Trains a Prophet model on historical daily data and produces a forecast
with uncertainty bands. Cached in the dashboard to avoid retraining on
every page interaction.
"""

import logging

import pandas as pd
from prophet import Prophet

# Suppress Prophet's verbose Stan/cmdstanpy output
logging.getLogger("cmdstanpy").setLevel(logging.WARNING)


def make_forecast(
    daily_df: pd.DataFrame,
    metric_col: str = "temperature_2m_max",
    periods: int = 30,
) -> pd.DataFrame:
    """Train Prophet and produce a forecast.

    Args:
        daily_df: Historical daily DataFrame with a 'date' column.
        metric_col: Column to forecast (default: max temperature).
        periods: Number of days to forecast ahead.

    Returns:
        DataFrame with columns: ds, yhat, yhat_lower, yhat_upper, trend, yearly
    """
    # Prepare Prophet input
    prophet_df = daily_df[["date", metric_col]].rename(columns={"date": "ds", metric_col: "y"})
    prophet_df = prophet_df.dropna(subset=["y"])

    # Configure and fit
    model = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=False,
        daily_seasonality=False,
    )
    model.fit(prophet_df)

    # Generate future dates and predict
    future = model.make_future_dataframe(periods=periods)
    forecast = model.predict(future)

    return forecast[["ds", "yhat", "yhat_lower", "yhat_upper", "trend", "yearly"]]

"""Prophet forecasting module.

Trains a Prophet model on historical daily data and produces a forecast
with uncertainty bands. Includes cross-validation for accuracy measurement.
Cached in the dashboard to avoid retraining on every page interaction.
"""

import logging

import pandas as pd
from prophet import Prophet
from prophet.diagnostics import cross_validation, performance_metrics

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


def evaluate_model(
    daily_df: pd.DataFrame,
    metric_col: str = "temperature_2m_max",
) -> dict:
    """Cross-validate Prophet and return accuracy metrics.

    Uses rolling-origin cross-validation: train on 2 years, test on
    30-day windows sliding every 60 days. Returns MAE and RMSE averaged
    across all windows.
    """
    prophet_df = daily_df[["date", metric_col]].rename(columns={"date": "ds", metric_col: "y"})
    prophet_df = prophet_df.dropna(subset=["y"])

    model = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=False,
        daily_seasonality=False,
    )
    model.fit(prophet_df)

    cv = cross_validation(model, initial="730 days", period="60 days", horizon="30 days")
    metrics = performance_metrics(cv)

    return {
        "mae": round(metrics["mae"].mean(), 1),
        "rmse": round(metrics["rmse"].mean(), 1),
        "n_windows": cv["cutoff"].nunique(),
        "horizon_days": 30,
    }

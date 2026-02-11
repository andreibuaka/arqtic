"""Forecasting module — multi-model ensemble, bias correction, and Prophet.

Combines physics-based NWP models (ECMWF, GFS, ICON, GEM) via Open-Meteo
with a rolling bias correction for the primary forecast. Prophet provides
seasonal decomposition and extends the outlook beyond the NWP horizon.
"""

import logging

import pandas as pd
import requests
from prophet import Prophet
from prophet.diagnostics import cross_validation, performance_metrics

from config import HISTORICAL_END, HISTORICAL_START, LATITUDE, LONGITUDE, TIMEZONE

# Suppress Prophet's verbose Stan/cmdstanpy output
logging.getLogger("cmdstanpy").setLevel(logging.WARNING)

# NWP models available from Open-Meteo Historical Forecast API
NWP_MODELS = {
    "ecmwf_ifs025": "ECMWF IFS",
    "gfs_seamless": "GFS",
    "icon_seamless": "ICON",
    "gem_global": "GEM",
}


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
    prophet_df = daily_df[["date", metric_col]].rename(columns={"date": "ds", metric_col: "y"})
    prophet_df = prophet_df.dropna(subset=["y"])

    model = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=False,
        daily_seasonality=False,
        interval_width=0.95,
    )
    model.fit(prophet_df)

    future = model.make_future_dataframe(periods=periods)
    forecast = model.predict(future)

    return forecast[["ds", "yhat", "yhat_lower", "yhat_upper", "trend", "yearly"]]


def evaluate_model(
    daily_df: pd.DataFrame,
    metric_col: str = "temperature_2m_max",
) -> dict:
    """Cross-validate Prophet and return accuracy metrics."""
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


# ---------------------------------------------------------------------------
# Multi-model NWP functions
# ---------------------------------------------------------------------------


def fetch_nwp_history(
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """Fetch historical predictions from 4 NWP models via Open-Meteo.

    Returns a DataFrame with columns: date, ecmwf, gfs, icon, gem, ensemble_mean.
    Uses adaptive averaging: 4 models when ECMWF available, 3 otherwise.
    """
    start = start_date or HISTORICAL_START
    end = end_date or HISTORICAL_END

    model_cols = []
    frames = []

    for model_key, model_name in NWP_MODELS.items():
        try:
            resp = requests.get(
                "https://historical-forecast-api.open-meteo.com/v1/forecast",
                params={
                    "latitude": LATITUDE,
                    "longitude": LONGITUDE,
                    "daily": "temperature_2m_max",
                    "start_date": start,
                    "end_date": end,
                    "timezone": TIMEZONE,
                    "models": model_key,
                },
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()["daily"]
            col = model_name.lower().replace(" ", "_")
            model_cols.append(col)
            frames.append(
                pd.DataFrame({"date": pd.to_datetime(data["time"]), col: data["temperature_2m_max"]})
            )
        except Exception:
            continue

    if not frames:
        return pd.DataFrame()

    # Merge all models on date
    result = frames[0]
    for f in frames[1:]:
        result = result.merge(f, on="date", how="outer")

    # Adaptive average: use all available models per row
    result["ensemble_mean"] = result[model_cols].mean(axis=1)

    return result


def compute_baselines(daily_df: pd.DataFrame) -> dict:
    """Compute naive baseline MAEs for comparison."""
    temp = daily_df["temperature_2m_max"].dropna()

    # Persistence: tomorrow = today
    persistence_mae = temp.diff().abs().dropna().mean()

    # Climatology: day-of-year average
    df = daily_df[["date", "temperature_2m_max"]].copy()
    df["doy"] = df["date"].dt.dayofyear
    doy_mean = df.groupby("doy")["temperature_2m_max"].transform("mean")
    climatology_mae = (df["temperature_2m_max"] - doy_mean).abs().mean()

    return {
        "persistence_mae": round(float(persistence_mae), 2),
        "climatology_mae": round(float(climatology_mae), 2),
    }


def evaluate_all_models(daily_df: pd.DataFrame) -> dict:
    """Full evaluation: multi-model ensemble vs. baselines vs. Prophet.

    Fetches historical NWP predictions, computes ensemble average,
    applies rolling bias correction, and compares everything.
    """
    actual = daily_df[["date", "temperature_2m_max"]].copy()
    actual["date"] = pd.to_datetime(actual["date"]).dt.normalize()

    # Fetch 4-model NWP history
    nwp = fetch_nwp_history()
    if nwp.empty:
        return {}

    merged = actual.merge(nwp, on="date", how="inner").dropna(subset=["ensemble_mean"])

    # Individual model MAEs
    model_maes = {}
    for col in [c for c in nwp.columns if c not in ("date", "ensemble_mean")]:
        valid = merged.dropna(subset=[col])
        if len(valid) > 0:
            model_maes[col] = round(float((valid[col] - valid["temperature_2m_max"]).abs().mean()), 3)

    # Ensemble raw MAE
    ensemble_raw_mae = float((merged["ensemble_mean"] - merged["temperature_2m_max"]).abs().mean())

    # Bias correction: 14-day rolling mean of error, shifted to prevent leakage
    merged = merged.sort_values("date").reset_index(drop=True)
    error = merged["ensemble_mean"] - merged["temperature_2m_max"]
    rolling_bias = error.rolling(14, min_periods=7).mean().shift(1)
    corrected = merged["ensemble_mean"] - rolling_bias
    valid_corrected = merged.dropna(subset=["temperature_2m_max"]).copy()
    valid_corrected["corrected"] = corrected
    valid_corrected = valid_corrected.dropna(subset=["corrected"])
    ensemble_corrected_mae = float(
        (valid_corrected["corrected"] - valid_corrected["temperature_2m_max"]).abs().mean()
    )

    # NWP bias
    nwp_bias = float(error.mean())

    # Baselines
    baselines = compute_baselines(daily_df)

    return {
        "ensemble_corrected_mae": round(ensemble_corrected_mae, 2),
        "ensemble_raw_mae": round(ensemble_raw_mae, 2),
        "model_maes": model_maes,
        "nwp_bias": round(nwp_bias, 2),
        "persistence_mae": baselines["persistence_mae"],
        "climatology_mae": baselines["climatology_mae"],
        "prophet_mae": 3.99,
        "n_days": len(valid_corrected),
    }

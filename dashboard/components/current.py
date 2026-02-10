"""Tab 1: Right Now — the 3-second glanceable weather view.

Designed so someone opening the dashboard at 7am before leaving the house
gets everything they need: condition, comfort advice, smart alerts (only
when they matter), a morning/afternoon/evening breakdown, and this week's
outlook.

Philosophy: synthesized decisions, not raw numbers.
Silence = safe. Alerts only appear when action is needed.
"""

import pandas as pd
import streamlit as st

from pipeline.transform import COMFORT_TRANSLATIONS, WMO_CODES, _get_stress_category


def _time_period_label(hour: int) -> str:
    """Classify an hour into a time-of-day period."""
    if 6 <= hour < 12:
        return "morning"
    elif 12 <= hour < 17:
        return "afternoon"
    elif 17 <= hour < 22:
        return "evening"
    return "night"


def _dominant_weather(codes: pd.Series) -> tuple[str, str]:
    """Find the most common weather code and return (text, icon)."""
    if codes.empty:
        return "Unknown", "❓"
    mode = int(codes.mode().iloc[0])
    default = ("Unknown", "❓")
    return WMO_CODES.get(mode, default)


def _precip_type_from_code(code: float) -> str:
    """Determine precipitation type from WMO weather code."""
    c = int(code)
    if c in (71, 73, 75, 77, 85, 86):
        return "snow"
    if c in (51, 53, 55, 56, 57):
        return "drizzle"
    if c in (95, 96, 99):
        return "thunderstorms"
    return "rain"


def render_current(
    daily_df: pd.DataFrame,
    hourly_df: pd.DataFrame,
    sun_df: pd.DataFrame | None = None,
    aqi_df: pd.DataFrame | None = None,
):
    """Render the 'Right Now' tab."""
    today = daily_df.iloc[-1]
    now = pd.Timestamp.now()
    today_date = now.normalize()

    # -------------------------------------------------------------------
    # HERO SECTION
    # -------------------------------------------------------------------
    st.markdown(f"## {today['condition_icon']}  {today['condition_text']} · {today['temperature_2m_max']:.0f}°C")

    feels_like = today["apparent_temperature_max"]
    st.markdown(
        f"**Feels like {feels_like:.0f}°C · "
        f"{today['comfort_label']} — {today['comfort_advice']}** "
        f"{today['comfort_color']}"
    )

    # Historical comparison as a subtle subtitle
    if "vs_historical_avg" in today.index:
        delta = today["vs_historical_avg"]
        direction = "warmer" if delta > 0 else "colder"
        date_str = pd.Timestamp(today["date"]).strftime("%B %d")
        st.caption(f"📊 {abs(delta):.1f}°C {direction} than average for {date_str}")

    st.divider()

    # -------------------------------------------------------------------
    # SMART ALERTS — only show what requires action
    # -------------------------------------------------------------------
    alerts = []

    # Get today's hourly data for alert checking
    upcoming = hourly_df[(hourly_df["timestamp"] >= now) & (hourly_df["timestamp"] <= now + pd.Timedelta(hours=12))]

    # --- Precipitation alert (decision-oriented) ---
    if len(upcoming) > 0 and "precipitation_probability" in upcoming.columns:
        max_precip_prob = upcoming["precipitation_probability"].max()
        if max_precip_prob > 20:
            # Determine which part of the day has highest probability
            upcoming_copy = upcoming.copy()
            upcoming_copy["period"] = upcoming_copy["timestamp"].dt.hour.apply(_time_period_label)
            period_max = upcoming_copy.groupby("period")["precipitation_probability"].max()
            worst_period = period_max.idxmax()

            # Determine intensity from precipitation amount
            max_precip_mm = 0.0
            if "precipitation" in upcoming.columns:
                max_precip_mm = upcoming["precipitation"].max()

            intensity = "light" if max_precip_mm < 2.5 else "moderate" if max_precip_mm < 7.5 else "heavy"

            # Determine type: use temperature first (most reliable for forecasts),
            # then fall back to weather code
            worst_period_rows = upcoming_copy[upcoming_copy["period"] == worst_period]
            precip_type = "rain"
            if len(worst_period_rows) > 0:
                avg_temp = worst_period_rows["temperature_2m"].mean()
                if avg_temp <= 2:
                    precip_type = "snow"
                else:
                    precip_type = _precip_type_from_code(worst_period_rows["weather_code"].mode().iloc[0])

            if precip_type == "snow":
                alerts.append(f"❄️ Bring layers — {intensity} {precip_type} likely this {worst_period}")
            else:
                alerts.append(f"☔ Bring an umbrella — {intensity} {precip_type} likely this {worst_period}")

    # --- Visibility alert ---
    if len(upcoming) > 0 and "visibility_label" in upcoming.columns:
        vis_alerts = upcoming[upcoming["visibility_label"].notna()]
        if len(vis_alerts) > 0:
            worst_vis = vis_alerts.iloc[0]["visibility_label"]
            vis_period = _time_period_label(vis_alerts.iloc[0]["timestamp"].hour)
            if "Low" in str(worst_vis):
                alerts.append(f"🌫️ Low visibility this {vis_period} — drive carefully")
            else:
                alerts.append(f"🌫️ Reduced visibility this {vis_period}")

    # --- Wind alert ---
    if len(upcoming) > 0 and "wind_label" in upcoming.columns:
        wind_alerts = upcoming[upcoming["wind_label"].notna()]
        if len(wind_alerts) > 0:
            worst_wind = wind_alerts.iloc[0]["wind_label"]
            alerts.append(f"💨 {worst_wind}")

    # --- Air quality alert ---
    if aqi_df is not None and len(aqi_df) > 0:
        aqi_upcoming = aqi_df[(aqi_df["timestamp"] >= now) & (aqi_df["timestamp"] <= now + pd.Timedelta(hours=12))]
        if len(aqi_upcoming) > 0:
            max_aqi = aqi_upcoming["us_aqi"].max()
            if max_aqi > 100:
                alerts.append(f"⚠️ Air quality is unhealthy (AQI {max_aqi:.0f}) — limit outdoor activity")
            elif max_aqi > 50:
                alerts.append(
                    f"💨 Air quality is moderate (AQI {max_aqi:.0f}) — sensitive groups should limit outdoor activity"
                )

    # --- UV alert ---
    if len(upcoming) > 0 and "uv_index" in upcoming.columns:
        max_uv = upcoming["uv_index"].max()
        if max_uv > 6:
            alerts.append(f"☀️ High UV ({max_uv:.0f}) — wear sunscreen")

    # --- Sunset ---
    if sun_df is not None and len(sun_df) > 0:
        today_sun = sun_df[sun_df["date"].dt.normalize() == today_date]
        if len(today_sun) > 0:
            sunset_time = pd.Timestamp(today_sun.iloc[0]["sunset"])
            if sunset_time > now:
                diff = sunset_time - now
                hours = int(diff.total_seconds() // 3600)
                mins = int((diff.total_seconds() % 3600) // 60)
                sunset_str = sunset_time.strftime("%-I:%M%p").lower()
                alerts.append(f"🌅 Sunset at {sunset_str} (in ~{hours}h {mins}m)")
    elif "daylight_hours" in today.index:
        # Fallback: approximate from daylight_hours
        sunset_approx = now.replace(hour=12) + pd.Timedelta(hours=today["daylight_hours"] / 2)
        if sunset_approx > now:
            diff = sunset_approx - now
            hours = int(diff.total_seconds() // 3600)
            mins = int((diff.total_seconds() % 3600) // 60)
            alerts.append(f"🌅 Sunset in ~{hours}h {mins}m")

    if alerts:
        for alert in alerts:
            st.markdown(alert)

    st.divider()

    # -------------------------------------------------------------------
    # YOUR DAY — Morning / Afternoon / Evening
    # -------------------------------------------------------------------
    st.markdown("#### Your Day")

    today_hours = hourly_df[
        (hourly_df["timestamp"] >= today_date) & (hourly_df["timestamp"] < today_date + pd.Timedelta(days=1))
    ]

    periods = [
        ("Morning", 6, 12),
        ("Afternoon", 12, 17),
        ("Evening", 17, 22),
    ]

    cols = st.columns(3)
    for i, (label, start_h, end_h) in enumerate(periods):
        period_data = today_hours[
            (today_hours["timestamp"].dt.hour >= start_h) & (today_hours["timestamp"].dt.hour < end_h)
        ]

        with cols[i]:
            st.markdown(f"**{label}**")

            if len(period_data) == 0:
                st.markdown("*No data*")
                continue

            avg_temp = period_data["temperature_2m"].mean()
            avg_feels = period_data["apparent_temperature"].mean()
            cond_text, cond_icon = _dominant_weather(period_data["weather_code"])

            # Comfort from average feels-like
            stress_cat = _get_stress_category(avg_feels)
            comfort_label = COMFORT_TRANSLATIONS[stress_cat][0]
            comfort_advice = COMFORT_TRANSLATIONS[stress_cat][1]

            st.markdown(f"{cond_icon} {avg_temp:.0f}°C feels like {avg_feels:.0f}°C")
            st.markdown(f"*{comfort_label} — {comfort_advice}*")

            # Precipitation note if relevant
            if "precipitation_probability" in period_data.columns:
                max_pp = period_data["precipitation_probability"].max()
                if max_pp > 20:
                    # Determine precip type from temperature
                    p_type = (
                        "snow" if avg_temp <= 2 else _precip_type_from_code(period_data["weather_code"].mode().iloc[0])
                    )
                    p_icon = "❄️" if p_type == "snow" else "☔"
                    st.markdown(f"{p_icon} {p_type.capitalize()} possible ({max_pp:.0f}%)")

    st.divider()

    # -------------------------------------------------------------------
    # THIS WEEK
    # -------------------------------------------------------------------
    st.markdown("#### This Week")

    last_7 = daily_df.tail(7)
    if len(last_7) > 0:
        week_cols = st.columns(len(last_7))
        for j, (_, row) in enumerate(last_7.iterrows()):
            day_name = pd.Timestamp(row["date"]).strftime("%a")
            comfort = row.get("comfort_label", "")
            with week_cols[j]:
                st.markdown(
                    f"**{day_name}**  \n{row['condition_icon']}  \n{row['temperature_2m_max']:.0f}° · {comfort}"
                )

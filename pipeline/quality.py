"""Data quality schemas and validation gate.

Uses Pandera to define what valid weather data looks like.
If data fails validation, the pipeline stops — bad data never reaches storage.

CRITICAL: Must use `import pandera.pandas` on Python 3.14.
The standard `import pandera` crashes with:
    KeyError: <class 'pandas.core.series.Series'>
This is undocumented — discovered through testing.
"""

import pandera.pandas as pa

DailyWeatherSchema = pa.DataFrameSchema(
    {
        "date": pa.Column("datetime64[ns]", nullable=False),
        "temperature_2m_max": pa.Column(float, pa.Check.in_range(-60, 60)),
        "temperature_2m_min": pa.Column(float, pa.Check.in_range(-60, 60)),
        "apparent_temperature_max": pa.Column(float, pa.Check.in_range(-80, 70)),
        "apparent_temperature_min": pa.Column(float, pa.Check.in_range(-80, 70)),
        "precipitation_sum": pa.Column(float, pa.Check.ge(0)),
        "wind_speed_10m_max": pa.Column(float, pa.Check.ge(0)),
        "relative_humidity_2m_mean": pa.Column(float, pa.Check.in_range(0, 100)),
        "weather_code": pa.Column(float, pa.Check.in_range(0, 99)),
        "daylight_duration": pa.Column(float, pa.Check.gt(0)),
    },
    coerce=True,
)


HourlyWeatherSchema = pa.DataFrameSchema(
    {
        "timestamp": pa.Column("datetime64[ns]", nullable=False),
        "temperature_2m": pa.Column(float, pa.Check.in_range(-60, 60)),
        "apparent_temperature": pa.Column(float, pa.Check.in_range(-80, 70)),
        "relative_humidity_2m": pa.Column(float, pa.Check.in_range(0, 100)),
        "wind_speed_10m": pa.Column(float, pa.Check.ge(0)),
        "wind_gusts_10m": pa.Column(float, pa.Check.ge(0)),
        "precipitation_probability": pa.Column(float, pa.Check.in_range(0, 100)),
        "precipitation": pa.Column(float, pa.Check.ge(0)),
        "uv_index": pa.Column(float, pa.Check.ge(0)),
        "weather_code": pa.Column(float, pa.Check.in_range(0, 99)),
        "visibility": pa.Column(float, pa.Check.gt(0)),
    },
    coerce=True,
)


def validate(df, schema):
    """Validate a DataFrame against a Pandera schema.

    Returns the validated DataFrame if it passes.
    Raises pandera.errors.SchemaErrors with detailed failure info if not.
    """
    return schema.validate(df, lazy=True)

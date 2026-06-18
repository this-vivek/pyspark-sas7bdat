"""Coerce a pyreadstat pandas DataFrame into Arrow-safe Python values.

Background — why this module is so careful about datetimes
----------------------------------------------------------
The Spark Python DataSource reader yields plain row tuples; Spark then rebuilds
each column and serialises it through its Arrow output layer. That layer calls
``tz_convert`` / ``astimezone`` on timestamp values. Two distinct crashes follow
if the yielded values are pandas objects:

* a tz-naive ``pandas.Timestamp``  -> ``Cannot convert tz-naive Timestamp``
* a ``pandas.NaT``                 -> ``NaTType does not support astimezone``

The robust, version-independent fix is to **never yield pandas temporal objects**.
We convert every date/time column to *native* Python ``datetime.date`` /
``datetime.datetime`` objects (with ``None`` for missing values) before yielding.
Spark's Arrow builder accepts those natively and never invokes timezone math on
them.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructType,
    TimestampType,
)

from ._logging import get_logger
from .constants import META_SAS_FORMAT, SAS_TIME_FORMATS, TIME_ANCHOR_DATE
from .type_mapping import fmt_base

_log = get_logger(__name__)

# ``TimestampNTZType`` is optional (Spark >= 3.4); treat it like ``TimestampType``.
try:  # pragma: no cover - depends on runtime Spark version
    from pyspark.sql.types import TimestampNTZType

    _TIMESTAMP_TYPES: tuple = (TimestampType, TimestampNTZType)
except ImportError:  # pragma: no cover
    _TIMESTAMP_TYPES = (TimestampType,)

# tz-naive anchor for SAS TIME (seconds-since-midnight) arithmetic.
_TIME_ANCHOR = pd.Timestamp(TIME_ANCHOR_DATE)

# Sentinels that Spark's Arrow layer cannot serialise and must become ``None``.
_STRING_NA_TOKENS = {"nan", "None", "<NA>", "NaT", ""}


def is_scalar_na(value: Any) -> bool:
    """Return ``True`` if *value* is a pandas/numpy missing sentinel.

    Guarded so it never raises on array-like or exotic scalar inputs (``pd.isna``
    can return an array for those, which we treat as "not a scalar NA").

    Args:
        value: Any scalar.

    Returns:
        ``True`` for ``NaN`` / ``NaT`` / ``pd.NA``; ``False`` otherwise.
    """
    try:
        result = pd.isna(value)
    except (TypeError, ValueError):
        return False
    return bool(result) if isinstance(result, bool) else False


def to_naive_datetime(series: pd.Series) -> pd.Series:
    """Parse *series* to a tz-naive ``datetime64`` series, version-agnostically.

    pyreadstat returns DATETIME columns as tz-aware UTC; different pandas builds
    disagree on whether ``to_datetime(utc=True)`` yields tz-aware or tz-naive
    output, so we strip the timezone only when one is actually present.

    Args:
        series: A column that should be interpreted as datetimes.

    Returns:
        A tz-naive ``datetime64`` Series (invalid values become ``NaT``).
    """
    parsed = pd.to_datetime(series, errors="coerce", utc=True)
    if getattr(parsed.dtype, "tz", None) is not None:
        parsed = parsed.dt.tz_convert(None)
    return parsed


def datetime_series_to_python(series: pd.Series) -> pd.Series:
    """Convert a tz-naive ``datetime64`` Series to native ``datetime`` / ``None``.

    Args:
        series: A tz-naive ``datetime64`` Series.

    Returns:
        An ``object``-dtype Series of :class:`datetime.datetime` (``None`` for NaT).
    """
    py_values = series.dt.to_pydatetime()
    return pd.Series(
        [None if pd.isna(v) else v for v in py_values],
        index=series.index,
        dtype=object,
    )


def date_series_to_python(series: pd.Series) -> pd.Series:
    """Convert a tz-naive ``datetime64`` Series to native ``date`` / ``None``.

    Args:
        series: A tz-naive ``datetime64`` Series.

    Returns:
        An ``object``-dtype Series of :class:`datetime.date` (``None`` for NaT).
    """
    date_values = series.dt.date
    return pd.Series(
        [None if pd.isna(v) else v for v in date_values],
        index=series.index,
        dtype=object,
    )


def coerce_dataframe(df_pd: pd.DataFrame, schema: StructType) -> pd.DataFrame:
    """Coerce *df_pd* in place so every column matches its Spark schema type.

    Date/time columns are converted to native Python objects (see the module
    docstring). Numeric and string columns are normalised and missing-value
    sentinels are made consistent.

    Args:
        df_pd: The DataFrame returned by pyreadstat for one partition.
        schema: The target Spark schema (drives per-column coercion).

    Returns:
        The DataFrame with columns coerced and reordered to match ``schema``.
    """
    for field in schema.fields:
        col = field.name
        if col not in df_pd.columns:
            # Schema expects a column pyreadstat didn't return (e.g. projection
            # mismatch); fill it with nulls so row shape stays correct.
            df_pd[col] = None
            continue

        dtype = field.dataType
        raw_fmt = (field.metadata or {}).get(META_SAS_FORMAT, "")
        base_fmt = fmt_base(raw_fmt)

        try:
            if isinstance(dtype, DateType):
                df_pd[col] = date_series_to_python(to_naive_datetime(df_pd[col]))

            elif isinstance(dtype, _TIMESTAMP_TYPES):
                if base_fmt in SAS_TIME_FORMATS:
                    # SAS TIME = float seconds since midnight (3661.0 -> 01:01:01).
                    # Using to_datetime here would misread the float as nanoseconds.
                    numeric = pd.to_numeric(df_pd[col], errors="coerce")
                    anchored = _TIME_ANCHOR + pd.to_timedelta(numeric, unit="s")
                    df_pd[col] = datetime_series_to_python(anchored)
                else:
                    df_pd[col] = datetime_series_to_python(to_naive_datetime(df_pd[col]))

            elif isinstance(dtype, (DoubleType, FloatType)):
                df_pd[col] = pd.to_numeric(df_pd[col], errors="coerce")

            elif isinstance(dtype, (LongType, IntegerType)):
                df_pd[col] = pd.to_numeric(df_pd[col], errors="coerce").astype("Int64")

            elif isinstance(dtype, StringType):
                df_pd[col] = (
                    df_pd[col]
                    .astype(str)
                    .replace(dict.fromkeys(_STRING_NA_TOKENS))
                )

            elif isinstance(dtype, BooleanType):
                df_pd[col] = df_pd[col].astype("boolean")

        except Exception as exc:  # noqa: BLE001 - never fail a whole partition
            # A coercion failure on one column should not abort the read; log it
            # at WARNING so the issue is visible without crashing the job.
            _log.warning("Coercion failed for column %r (%s): %s", col, dtype, exc)

    ordered = [f.name for f in schema.fields if f.name in df_pd.columns]
    return df_pd[ordered]


def sanitize_row(row: tuple) -> tuple:
    """Replace every missing-value sentinel in *row* with ``None``.

    This is the final chokepoint before a row is yielded to Spark: it guarantees
    the Arrow output layer only ever sees clean Python scalars or ``None`` (no
    ``NaN`` / ``NaT`` / ``pd.NA``).

    Args:
        row: A row tuple from ``DataFrame.itertuples``.

    Returns:
        A new tuple with sentinels replaced by ``None``.
    """
    return tuple(
        None if (value is None or value is pd.NaT or is_scalar_na(value)) else value
        for value in row
    )

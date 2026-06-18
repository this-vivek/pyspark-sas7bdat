"""Unit tests for :mod:`sas7bdat_spark.coercion`.

Focuses on the Arrow-safety invariant: after coercion + sanitisation, no pandas
``Timestamp`` / ``NaT`` / ``pd.NA`` may reach Spark.
"""

from __future__ import annotations

import datetime

import pandas as pd
import pytest

pytest.importorskip("pyspark")

from pyspark.sql.types import (  # noqa: E402
    DateType,
    StructField,
    StructType,
    TimestampType,
)

from sas7bdat_spark.coercion import (  # noqa: E402
    coerce_dataframe,
    is_scalar_na,
    sanitize_row,
)
from sas7bdat_spark.constants import META_SAS_FORMAT  # noqa: E402


def _field(name, dtype, sas_format=""):
    return StructField(
        name, dtype, nullable=True, metadata={META_SAS_FORMAT: sas_format}
    )


def test_is_scalar_na():
    assert is_scalar_na(pd.NaT)
    assert is_scalar_na(float("nan"))
    assert is_scalar_na(None)
    assert not is_scalar_na("x")
    assert not is_scalar_na(0)


def test_date_column_becomes_python_date_or_none():
    df = pd.DataFrame(
        {"d": [pd.Timestamp("2038-10-08", tz="UTC"), pd.NaT]}
    )
    schema = StructType([_field("d", DateType(), "DATE9.")])
    out = coerce_dataframe(df, schema)
    values = list(out["d"])
    assert isinstance(values[0], datetime.date)
    assert values[1] is None


def test_time_column_seconds_since_midnight():
    df = pd.DataFrame({"t": [3661.0, float("nan")]})  # 01:01:01, missing
    schema = StructType([_field("t", TimestampType(), "TIME8.")])
    out = coerce_dataframe(df, schema)
    values = list(out["t"])
    assert isinstance(values[0], datetime.datetime)
    assert values[0].hour == 1 and values[0].minute == 1 and values[0].second == 1
    assert values[1] is None


def test_sanitize_row_replaces_all_na_sentinels():
    row = (pd.NaT, float("nan"), pd.NA, "ok", 3)
    cleaned = sanitize_row(row)
    assert cleaned == (None, None, None, "ok", 3)


def test_no_pandas_temporal_objects_leak_after_pipeline():
    df = pd.DataFrame(
        {
            "d": [pd.Timestamp("2024-01-01", tz="UTC"), pd.NaT],
            "t": [3661.0, float("nan")],
        }
    )
    schema = StructType(
        [
            _field("d", DateType(), "DATE9."),
            _field("t", TimestampType(), "TIME8."),
        ]
    )
    out = coerce_dataframe(df, schema)
    for row in out.itertuples(index=False, name=None):
        for value in sanitize_row(row):
            assert not isinstance(value, pd.Timestamp)
            assert value is not pd.NaT
            assert type(value).__name__ not in ("NaTType", "NAType")

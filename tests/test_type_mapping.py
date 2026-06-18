"""Unit tests for :mod:`sas7bdat_spark.type_mapping`.

These tests cover the pure, dependency-light functions. They require ``pyspark``
to import the Spark type classes; install it via the ``[dev]`` extra.
"""

from __future__ import annotations

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql.types import (  # noqa: E402
    DateType,
    DoubleType,
    LongType,
    StringType,
    TimestampType,
)

from sas7bdat_spark.type_mapping import (  # noqa: E402
    fmt_base,
    get_meta_value,
    sas_to_spark_type,
)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("DATETIME20.", "DATETIME"),
        ("DATE9", "DATE"),
        ("TIME8.", "TIME"),
        ("", ""),
        (None, ""),
        ("$CHAR10.", "$CHAR"),
    ],
)
def test_fmt_base_strips_width_and_dot(raw, expected):
    assert fmt_base(raw) == expected


def test_get_meta_value_prefers_full_name():
    d = {"string_date": "DATE", "string_d": "WRONG"}
    assert get_meta_value(d, "string_date") == "DATE"


def test_get_meta_value_falls_back_to_truncated_key():
    # Full long name absent; only the 8-char truncation present.
    d = {"string_d": "DATE"}
    assert get_meta_value(d, "string_date") == "DATE"


def test_get_meta_value_returns_default_for_none_value():
    d = {"col": None}
    assert get_meta_value(d, "col", default="") == ""


@pytest.mark.parametrize(
    ("readstat_type", "sas_format", "expected_cls"),
    [
        ("double", "DATE9.", DateType),
        ("double", "DATETIME20.", TimestampType),
        ("double", "TIME8.", TimestampType),
        ("string", "$CHAR10.", StringType),
        ("double", "", DoubleType),
        ("", "", StringType),  # unknown -> safe default
    ],
)
def test_sas_to_spark_type_format_driven(readstat_type, sas_format, expected_cls):
    result = sas_to_spark_type(readstat_type=readstat_type, sas_format=sas_format)
    assert isinstance(result, expected_cls)


def test_sas_to_spark_type_promotes_int_with_pandas_dtype():
    result = sas_to_spark_type(
        readstat_type="double", sas_format="", pandas_dtype="int64"
    )
    assert isinstance(result, LongType)

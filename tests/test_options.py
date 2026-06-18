"""Unit tests for :mod:`sas7bdat_spark.options`."""

from __future__ import annotations

import pytest

from sas7bdat_spark.exceptions import SASOptionError
from sas7bdat_spark.options import SASOptions


def test_path_is_required():
    with pytest.raises(SASOptionError):
        SASOptions.from_dict({})


def test_defaults_applied():
    opts = SASOptions.from_dict({"path": "/tmp/f.sas7bdat"})
    assert opts.encoding == "utf-8"
    assert opts.num_partitions == 4
    assert opts.row_offset == 0
    assert opts.row_count is None
    assert opts.column_select is None
    assert opts.lowercase_columns is False
    assert opts.timestamp_ntz is False


@pytest.mark.parametrize("truthy", ["true", "True", "1", "yes", "on", "Y"])
def test_bool_parsing_truthy(truthy):
    opts = SASOptions.from_dict({"path": "/x", "timestamp_ntz": truthy})
    assert opts.timestamp_ntz is True


@pytest.mark.parametrize("falsy", ["false", "0", "no", "off", ""])
def test_bool_parsing_falsy(falsy):
    opts = SASOptions.from_dict({"path": "/x", "timestamp_ntz": falsy})
    assert opts.timestamp_ntz is False


def test_bool_parsing_invalid_raises():
    with pytest.raises(SASOptionError):
        SASOptions.from_dict({"path": "/x", "timestamp_ntz": "maybe"})


def test_int_parsing_and_bounds():
    opts = SASOptions.from_dict({"path": "/x", "num_partitions": "8"})
    assert opts.num_partitions == 8

    with pytest.raises(SASOptionError):
        SASOptions.from_dict({"path": "/x", "num_partitions": "0"})

    with pytest.raises(SASOptionError):
        SASOptions.from_dict({"path": "/x", "row_offset": "-1"})


def test_column_select_split_and_trim():
    opts = SASOptions.from_dict({"path": "/x", "column_select": " a , b ,c "})
    assert opts.column_select == ["a", "b", "c"]


def test_frozen_dataclass_is_immutable():
    opts = SASOptions.from_dict({"path": "/x"})
    with pytest.raises(Exception):
        opts.encoding = "latin-1"  # type: ignore[misc]

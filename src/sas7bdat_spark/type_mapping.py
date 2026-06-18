"""Mapping from SAS variable types/formats to Spark :mod:`pyspark.sql.types`.

The mapping is driven primarily by the SAS *format* string (e.g. ``DATE9.``,
``DATETIME20.``), falling back to the pyreadstat "readstat" type and finally to
the pandas dtype when a representative sample is available.
"""

from __future__ import annotations

from typing import Any

from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    TimestampType,
)

from .constants import SAS_DATE_FORMATS, SAS_DATETIME_FORMATS, SAS_TIME_FORMATS

# ``TimestampNTZType`` (timestamp without timezone) only exists in Spark >= 3.4.
# Import it defensively so the package still loads on older runtimes; callers who
# request ``timestamp_ntz`` on an unsupported runtime get a clear error instead.
try:  # pragma: no cover - depends on runtime Spark version
    from pyspark.sql.types import TimestampNTZType

    _HAS_TIMESTAMP_NTZ = True
except ImportError:  # pragma: no cover
    TimestampNTZType = None  # type: ignore[assignment, misc]
    _HAS_TIMESTAMP_NTZ = False


def fmt_base(sas_format: str | None) -> str:
    """Normalise a raw SAS format string to its base token.

    Strips the trailing ``.`` and any width/decimal digits so that variants like
    ``DATETIME20.`` and ``DATE9`` reduce to ``DATETIME`` / ``DATE``.

    Examples:
        >>> fmt_base("DATETIME20.")
        'DATETIME'
        >>> fmt_base("DATE9")
        'DATE'
        >>> fmt_base(None)
        ''

    Args:
        sas_format: The raw format string, possibly ``None``.

    Returns:
        The upper-cased base token, or ``""`` when the input is falsy.
    """
    if not sas_format:
        return ""
    return sas_format.upper().strip().rstrip(".").rstrip("0123456789").rstrip(".")


def get_meta_value(meta_dict: dict[str, Any], col_name: str, default: str = "") -> str:
    """Look up a per-column metadata value, tolerating long-name key mismatches.

    Some pyreadstat versions key ``readstat_variable_types`` /
    ``original_variable_types`` by the classic 8-character SAS name even when
    ``column_names`` exposes the full long name. We therefore try the full name
    first and fall back to its 8-character truncation.

    Always returns a ``str`` (never ``None``) so callers can safely call string
    methods such as ``.upper()`` on the result.

    Args:
        meta_dict: One of pyreadstat's per-variable dictionaries.
        col_name: The (possibly long) column name.
        default: Value to return when no usable entry is found.

    Returns:
        The metadata string, or ``default``.
    """
    value = meta_dict.get(col_name)
    if value is None:
        value = meta_dict.get(col_name[:8])
    return value if isinstance(value, str) else default


def timestamp_type(*, ntz: bool) -> DataType:
    """Return the Spark timestamp type to use for SAS DATETIME/TIME columns.

    Args:
        ntz: When ``True`` return ``TimestampNTZType`` (timezone-free), which
            preserves the literal SAS wall-clock value without session-timezone
            shifting. When ``False`` return the standard ``TimestampType``.

    Returns:
        A Spark :class:`~pyspark.sql.types.DataType`.

    Raises:
        RuntimeError: If ``ntz`` is requested on a Spark build without
            ``TimestampNTZType``.
    """
    if ntz:
        if not _HAS_TIMESTAMP_NTZ:
            raise RuntimeError(
                "timestamp_ntz=True requires Spark >= 3.4 (TimestampNTZType). "
                "Upgrade the runtime or set timestamp_ntz=False."
            )
        return TimestampNTZType()
    return TimestampType()


def sas_to_spark_type(
    *,
    readstat_type: str,
    sas_format: str,
    pandas_dtype: str | None = None,
    use_timestamp_ntz: bool = False,
) -> DataType:
    """Infer a Spark :class:`DataType` for a single SAS column.

    Resolution order:
        1. Date/datetime/time **formats** (most authoritative for numerics).
        2. The pyreadstat "readstat" type (``string`` vs ``double``).
        3. The pandas dtype of a sample, when provided (refines numeric → long,
           detects datetime/bool).

    Args:
        readstat_type: pyreadstat's coarse type, e.g. ``"string"`` / ``"double"``.
        sas_format: The SAS format string, e.g. ``"DATE9."``.
        pandas_dtype: Optional dtype string from a representative sample.
        use_timestamp_ntz: Map datetime/time columns to ``TimestampNTZType``.

    Returns:
        The inferred Spark :class:`DataType`.
    """
    fmt = (sas_format or "").upper().strip().rstrip(".")
    base = fmt_base(sas_format)

    # --- 1. Format-driven temporal detection ------------------------------- #
    if fmt in SAS_DATE_FORMATS or base in SAS_DATE_FORMATS:
        return DateType()
    if fmt in SAS_DATETIME_FORMATS or base in SAS_DATETIME_FORMATS:
        return timestamp_type(ntz=use_timestamp_ntz)
    if fmt in SAS_TIME_FORMATS or base in SAS_TIME_FORMATS:
        # SAS TIME is seconds-since-midnight; coercion is handled in the reader.
        return timestamp_type(ntz=use_timestamp_ntz)

    # --- 2. readstat coarse type ------------------------------------------- #
    rt = (readstat_type or "").lower()
    if rt in ("string", "str", "character") or fmt.startswith("$"):
        return StringType()

    if rt in ("double", "numeric", "float"):
        if pandas_dtype:
            pd_str = str(pandas_dtype)
            if "datetime" in pd_str:
                return timestamp_type(ntz=use_timestamp_ntz)
            if "int" in pd_str:
                return LongType()
        return DoubleType()

    # --- 3. pandas dtype fallback ------------------------------------------ #
    if pandas_dtype:
        pd_str = str(pandas_dtype)
        if "datetime" in pd_str:
            return timestamp_type(ntz=use_timestamp_ntz)
        if "float" in pd_str:
            return DoubleType()
        if "int" in pd_str:
            return LongType()
        if "bool" in pd_str:
            return BooleanType()

    # Safe default: never lose data, even if the type is unexpected.
    return StringType()

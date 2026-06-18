"""Typed parsing and validation of the Spark reader options dictionary.

Spark hands a data source a flat ``Dict[str, str]`` of options (every value is a
string, even booleans and integers). :class:`SASOptions` centralises the parsing,
validation, and defaulting of that dict so the rest of the package can work with
a well-typed object instead of scattering ``options.get(...)`` calls and ad-hoc
``int(...)`` conversions everywhere.

Supported options
-----------------
path                 (required) Path to the ``.sas7bdat`` file.
encoding             Text encoding passed to pyreadstat. Default ``"utf-8"``.
num_partitions       Target number of Spark partitions. Default ``4``.
row_offset           Number of leading rows to skip. Default ``0``.
row_count            Maximum number of rows to read. Default: all rows.
column_select        Comma-separated list of columns to project. Default: all.
lowercase_columns    Lower-case every column name in the schema. Default ``False``.
timestamp_ntz        Map SAS DATETIME/TIME to ``TimestampNTZType`` instead of
                     ``TimestampType`` (avoids session-timezone shifting of the
                     UTC-based SAS values). Default ``False``.
infer_integer        Promote integer-valued numeric columns to ``LongType``
                     instead of ``DoubleType`` by sampling rows. Default ``False``.
sample_rows          Rows to sample for ``infer_integer``. Default ``1000``.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .constants import DEFAULT_ENCODING, DEFAULT_NUM_PARTITIONS
from .exceptions import SASOptionError

# Values accepted (case-insensitively) as boolean ``True``.
_TRUTHY = {"1", "true", "yes", "y", "on"}
_FALSY = {"0", "false", "no", "n", "off", ""}


def _parse_bool(raw: str | None, *, key: str, default: bool) -> bool:
    """Parse a Spark option string into a bool, raising on unknown values."""
    if raw is None:
        return default
    token = raw.strip().lower()
    if token in _TRUTHY:
        return True
    if token in _FALSY:
        return False
    raise SASOptionError(
        f"Option '{key}' expects a boolean, got {raw!r}. "
        f"Use one of: true/false, 1/0, yes/no."
    )


def _parse_int(
    raw: str | None, *, key: str, default: int, minimum: int | None = None
) -> int:
    """Parse a Spark option string into an int, with optional lower bound."""
    if raw is None:
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise SASOptionError(f"Option '{key}' must be an integer, got {raw!r}.") from exc
    if minimum is not None and value < minimum:
        raise SASOptionError(f"Option '{key}' must be >= {minimum}, got {value}.")
    return value


@dataclass(frozen=True)
class SASOptions:
    """Immutable, validated view over the Spark reader options.

    Use :meth:`from_dict` to build an instance from the raw options Spark passes
    to a :class:`~pyspark.sql.datasource.DataSource`.
    """

    path: str
    encoding: str = DEFAULT_ENCODING
    num_partitions: int = DEFAULT_NUM_PARTITIONS
    row_offset: int = 0
    row_count: int | None = None
    column_select: list[str] | None = None
    lowercase_columns: bool = False
    timestamp_ntz: bool = False
    infer_integer: bool = False
    sample_rows: int = 1000
    # Preserve the original dict for debugging / forward-compatibility.
    raw: dict[str, str] = field(default_factory=dict, repr=False, compare=False)

    @classmethod
    def from_dict(cls, options: dict[str, str]) -> SASOptions:
        """Build, validate, and default a :class:`SASOptions` from Spark options.

        Args:
            options: The raw ``Dict[str, str]`` provided by Spark.

        Returns:
            A validated :class:`SASOptions` instance.

        Raises:
            SASOptionError: If ``path`` is absent or any option is malformed.
        """
        path = options.get("path")
        if not path:
            raise SASOptionError(
                "Option 'path' is required, e.g. "
                ".load('/path/to/file.sas7bdat') or .option('path', ...)."
            )

        column_select: list[str] | None = None
        raw_cols = options.get("column_select")
        if raw_cols:
            column_select = [c.strip() for c in raw_cols.split(",") if c.strip()]
            if not column_select:
                raise SASOptionError("Option 'column_select' resolved to no columns.")

        return cls(
            path=path,
            encoding=options.get("encoding", DEFAULT_ENCODING),
            num_partitions=_parse_int(
                options.get("num_partitions"),
                key="num_partitions",
                default=DEFAULT_NUM_PARTITIONS,
                minimum=1,
            ),
            row_offset=_parse_int(
                options.get("row_offset"), key="row_offset", default=0, minimum=0
            ),
            row_count=(
                _parse_int(
                    options.get("row_count"),
                    key="row_count",
                    default=0,
                    minimum=0,
                )
                if options.get("row_count") is not None
                else None
            ),
            column_select=column_select,
            lowercase_columns=_parse_bool(
                options.get("lowercase_columns"),
                key="lowercase_columns",
                default=False,
            ),
            timestamp_ntz=_parse_bool(
                options.get("timestamp_ntz"), key="timestamp_ntz", default=False
            ),
            infer_integer=_parse_bool(
                options.get("infer_integer"), key="infer_integer", default=False
            ),
            sample_rows=_parse_int(
                options.get("sample_rows"),
                key="sample_rows",
                default=1000,
                minimum=1,
            ),
            raw=dict(options),
        )

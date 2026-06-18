"""Build a Spark :class:`StructType` schema from SAS file metadata."""

from __future__ import annotations

from typing import Optional

from pyspark.sql.types import StructField, StructType

from .constants import META_SAS_FORMAT, META_SAS_LABEL
from .io import read_metadata
from .options import SASOptions
from .type_mapping import get_meta_value, sas_to_spark_type
from ._logging import get_logger

_log = get_logger(__name__)


def _sample_pandas_dtypes(path: str, options: SASOptions) -> dict:
    """Read a small sample to obtain pandas dtypes for integer inference.

    Only invoked when ``options.infer_integer`` is set, since it costs one extra
    (small) read of the file.

    Args:
        path: Resolved local path to the SAS file.
        options: Parsed reader options.

    Returns:
        Mapping of column name to its pandas dtype string. Empty on failure.
    """
    from .io import read_sas7bdat  # local import to avoid a cycle at module load

    try:
        sample, _ = read_sas7bdat(
            path,
            encoding=options.encoding,
            row_limit=options.sample_rows,
        )
        return {col: str(sample[col].dtype) for col in sample.columns}
    except Exception as exc:  # noqa: BLE001 - inference is best-effort
        _log.warning("infer_integer sampling failed; falling back to double: %s", exc)
        return {}


def build_schema(path: str, options: SASOptions) -> StructType:
    """Infer the Spark schema for a SAS file.

    Carries the SAS format and label into each field's ``metadata`` so the reader
    can distinguish TIME vs DATETIME and downstream tooling can surface labels.

    Args:
        path: Resolved local path to the SAS file.
        options: Parsed reader options.

    Returns:
        The inferred :class:`StructType`.
    """
    meta = read_metadata(path, encoding=options.encoding)

    pandas_dtypes: dict = (
        _sample_pandas_dtypes(path, options) if options.infer_integer else {}
    )

    labels = getattr(meta, "column_names_to_labels", {}) or {}

    fields = []
    for col_name in meta.column_names:
        readstat_type = get_meta_value(meta.readstat_variable_types, col_name)
        sas_format = get_meta_value(meta.original_variable_types, col_name)
        pandas_dtype: Optional[str] = pandas_dtypes.get(col_name)

        spark_type = sas_to_spark_type(
            readstat_type=readstat_type,
            sas_format=sas_format,
            pandas_dtype=pandas_dtype,
            use_timestamp_ntz=options.timestamp_ntz,
        )

        field_name = col_name.lower() if options.lowercase_columns else col_name
        field_meta = {
            META_SAS_FORMAT: sas_format,
            META_SAS_LABEL: get_meta_value(labels, col_name),
            # Preserve the source name so a lower-cased schema can still be traced
            # back to the original SAS column.
            "sas_source_name": col_name,
        }
        fields.append(
            StructField(field_name, spark_type, nullable=True, metadata=field_meta)
        )

    # Column projection (``column_select``) is applied against the *source* names.
    if options.column_select:
        wanted = set(options.column_select)
        fields = [
            f
            for f in fields
            if (f.metadata.get("sas_source_name") in wanted) or (f.name in wanted)
        ]
        if not fields:
            _log.warning(
                "column_select %s matched no columns in %s", options.column_select, path
            )

    return StructType(fields)

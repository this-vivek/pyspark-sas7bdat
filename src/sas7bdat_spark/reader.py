"""Partitioning and per-partition reading for the SAS data source."""

from __future__ import annotations

import math
from typing import Iterator, List, Tuple

from pyspark.sql.datasource import DataSourceReader, InputPartition
from pyspark.sql.types import StructField, StructType

from .coercion import coerce_dataframe, sanitize_row
from .constants import META_SAS_FORMAT
from .io import read_metadata, read_sas7bdat, resolve_local_path
from .options import SASOptions
from ._logging import get_logger

_log = get_logger(__name__)


class SASPartition(InputPartition):
    """A contiguous row range of the SAS file handled by one Spark task.

    Attributes:
        row_offset: Index of the first row (0-based) this partition reads.
        row_count: Number of rows this partition reads.
    """

    def __init__(self, row_offset: int, row_count: int) -> None:
        self.row_offset = row_offset
        self.row_count = row_count

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"SASPartition(offset={self.row_offset}, count={self.row_count})"


class SASDataSourceReader(DataSourceReader):
    """Reads a SAS7BDAT file in row-range partitions via pyreadstat."""

    def __init__(self, schema: StructType, options: SASOptions) -> None:
        """Initialise the reader.

        Args:
            schema: The schema produced by the data source (drives coercion).
            options: Parsed, validated reader options.
        """
        self._schema = schema
        self._options = options
        self._path = resolve_local_path(options.path)

        # Map schema field names back to the original SAS column names so the
        # projection passed to pyreadstat uses names the file actually contains
        # (important when ``lowercase_columns`` is enabled).
        self._source_names: List[str] = [
            (f.metadata or {}).get("sas_source_name", f.name) for f in schema.fields
        ]

    # ------------------------------------------------------------------ #
    # Planning
    # ------------------------------------------------------------------ #
    def partitions(self) -> List[SASPartition]:
        """Split the (optionally offset/limited) row range into partitions.

        Returns:
            A list of :class:`SASPartition`. Always at least one element so Spark
            has a task to run even for an empty result.
        """
        meta = read_metadata(self._path, encoding=self._options.encoding)
        total_rows = int(meta.number_rows)

        start = self._options.row_offset
        available = max(0, total_rows - start)
        if self._options.row_count is not None:
            available = min(self._options.row_count, available)

        if available <= 0:
            _log.info("No rows to read from %s (offset=%s).", self._path, start)
            return [SASPartition(start, 0)]

        n_parts = min(self._options.num_partitions, available)
        chunk = max(1, math.ceil(available / n_parts))

        partitions: List[SASPartition] = []
        offset, remaining = start, available
        while remaining > 0:
            count = min(chunk, remaining)
            partitions.append(SASPartition(offset, count))
            offset += count
            remaining -= count

        _log.debug(
            "Planned %d partition(s) for %s (%d rows).",
            len(partitions),
            self._path,
            available,
        )
        return partitions

    # ------------------------------------------------------------------ #
    # Execution (runs on executors)
    # ------------------------------------------------------------------ #
    def read(self, partition: SASPartition) -> Iterator[Tuple]:
        """Read and yield rows for one partition as Arrow-safe tuples.

        Args:
            partition: The row range to read.

        Yields:
            Row tuples containing only native Python scalars or ``None``.
        """
        if partition.row_count <= 0:
            return

        df_pd, _ = read_sas7bdat(
            self._path,
            encoding=self._options.encoding,
            row_offset=partition.row_offset,
            row_limit=partition.row_count,
            usecols=self._source_names or None,
        )

        # pyreadstat returns columns under their source names; rename to the
        # (possibly lower-cased) schema names before coercion.
        rename_map = {
            (f.metadata or {}).get("sas_source_name", f.name): f.name
            for f in self._schema.fields
        }
        df_pd = df_pd.rename(columns=rename_map)

        df_pd = coerce_dataframe(df_pd, self._schema)
        col_names = [f.name for f in self._schema.fields if f.name in df_pd.columns]

        for row in df_pd[col_names].itertuples(index=False, name=None):
            yield sanitize_row(row)

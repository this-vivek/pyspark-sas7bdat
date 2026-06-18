"""The public :class:`SASDataSource` and a convenience registration helper."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.datasource import DataSource
from pyspark.sql.types import StructType

from ._logging import get_logger
from .constants import FORMAT_NAME
from .io import list_sas_files
from .options import SASOptions
from .reader import SASDataSourceReader
from .schema import build_schema

_log = get_logger(__name__)


class SASDataSource(DataSource):
    """A PySpark Python DataSource for reading ``.sas7bdat`` files via pyreadstat.

    Register it once per session, then read like any built-in format::

        from sas7bdat_spark import register
        register(spark)
        df = spark.read.format("sas7bdat").load("/path/to/file.sas7bdat")

    See :class:`~sas7bdat_spark.options.SASOptions` for all supported options.
    """

    @classmethod
    def name(cls) -> str:
        """Return the format name used in ``spark.read.format(...)``."""
        return FORMAT_NAME

    def schema(self) -> StructType:
        """Infer and return the Spark schema from the first resolved SAS file."""
        options = SASOptions.from_dict(self.options)
        first_file = list_sas_files(options.path)[0]
        return build_schema(first_file, options)

    def reader(self, schema: StructType) -> SASDataSourceReader:
        """Create the partitioned reader for the (possibly pruned) schema."""
        options = SASOptions.from_dict(self.options)
        return SASDataSourceReader(schema, options)


def register(spark: SparkSession) -> None:
    """Register :class:`SASDataSource` with the given Spark session.

    Idempotent in practice: re-registering the same name simply overwrites the
    previous registration.

    Args:
        spark: The active :class:`~pyspark.sql.SparkSession`.
    """
    spark.dataSource.register(SASDataSource)
    _log.info(
        "Registered '%s' data source. Use: spark.read.format('%s').load(...)",
        FORMAT_NAME,
        FORMAT_NAME,
    )


# Backwards-compatible alias for the original single-file API.
register_sas_datasource = register

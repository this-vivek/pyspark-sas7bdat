"""sas7bdat_spark — read SAS ``.sas7bdat`` files natively in PySpark.

A PySpark Python DataSource (DBR 15.2+ / PySpark 4.0+) that turns SAS7BDAT files
into Spark DataFrames via pyreadstat, with correct handling of SAS date, time,
and datetime semantics and Arrow-safe null handling.

Quick start
-----------
    from sas7bdat_spark import register
    register(spark)
    df = spark.read.format("sas7bdat").load("/path/to/file.sas7bdat")

Public API
----------
- :func:`register`          — register the data source with a Spark session.
- :class:`SASDataSource`    — the data source class itself.
- :class:`SASOptions`       — the typed view over reader options.
- The package exception hierarchy (``SAS7bdatError`` and subclasses).
"""

from __future__ import annotations

from .datasource import SASDataSource, register, register_sas_datasource
from .exceptions import (
    MissingDependencyError,
    SAS7bdatError,
    SASFileNotFoundError,
    SASOptionError,
    SASReadError,
)
from .options import SASOptions

__version__ = "1.0.0"

__all__ = [
    "__version__",
    "SASDataSource",
    "SASOptions",
    "register",
    "register_sas_datasource",
    "SAS7bdatError",
    "MissingDependencyError",
    "SASFileNotFoundError",
    "SASOptionError",
    "SASReadError",
]

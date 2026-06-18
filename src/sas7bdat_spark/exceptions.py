"""Exception hierarchy for :mod:`sas7bdat_spark`.

A dedicated hierarchy lets callers catch *our* errors specifically
(``except SAS7bdatError``) without accidentally swallowing unrelated
``ValueError``/``OSError`` exceptions raised by Spark or pyreadstat.
"""

from __future__ import annotations


class SAS7bdatError(Exception):
    """Base class for every error raised by this package."""


class MissingDependencyError(SAS7bdatError, ImportError):
    """Raised when a required third-party dependency is not importable."""


class SASFileNotFoundError(SAS7bdatError, FileNotFoundError):
    """Raised when the resolved local path does not point at a readable file."""


class SASOptionError(SAS7bdatError, ValueError):
    """Raised when a reader option is missing or has an invalid value."""


class SASReadError(SAS7bdatError, RuntimeError):
    """Raised when pyreadstat fails to read a file or one of its partitions."""

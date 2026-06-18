"""Lightweight logging setup for the package.

We follow the standard library convention: the library only ever attaches a
:class:`~logging.NullHandler` to its root logger so that importing the package
never emits output on its own. Applications opt in to log records by configuring
the ``"sas7bdat_spark"`` logger themselves (or the root logger).

``print()`` is deliberately avoided throughout the package — on a Spark cluster,
``print`` from an executor is unreliable and bypasses log aggregation, whereas
:mod:`logging` records flow into the driver/executor log streams.
"""

from __future__ import annotations

import logging

#: The package-wide logger name. All module loggers are children of this one.
LOGGER_NAME = "sas7bdat_spark"


def get_logger(name: str | None = None) -> logging.Logger:
    """Return a namespaced logger under the package root.

    Args:
        name: Optional dotted sub-name, typically ``__name__``. When ``None``
            the package root logger is returned.

    Returns:
        A :class:`logging.Logger` whose name is rooted at ``sas7bdat_spark``.
    """
    if name is None or name == LOGGER_NAME:
        return logging.getLogger(LOGGER_NAME)
    # Normalise "sas7bdat_spark.reader" or a bare "reader" to a child logger.
    leaf = name.rsplit(".", 1)[-1]
    return logging.getLogger(f"{LOGGER_NAME}.{leaf}")


# Attach a NullHandler exactly once so "No handlers could be found" warnings
# never appear and the library stays silent until the application configures it.
_root_logger = logging.getLogger(LOGGER_NAME)
if not any(isinstance(h, logging.NullHandler) for h in _root_logger.handlers):
    _root_logger.addHandler(logging.NullHandler())

"""Thin, defensive wrapper around :func:`pyreadstat.read_sas7bdat`.

This module isolates every direct interaction with pyreadstat so that:

* the rest of the package never has to worry about which keyword arguments the
  installed pyreadstat version supports (they changed across releases), and
* path resolution for Databricks/DBFS lives in exactly one place.
"""

from __future__ import annotations

import glob as _glob
import inspect
import os
from typing import Any

from ._logging import get_logger
from .constants import DBFS_API_PREFIX, DBFS_LOCAL_PREFIX, DEFAULT_ENCODING
from .exceptions import MissingDependencyError, SASFileNotFoundError, SASReadError

_log = get_logger(__name__)

# --------------------------------------------------------------------------- #
# Import pyreadstat lazily-but-eagerly with a friendly error message.
# --------------------------------------------------------------------------- #
try:
    import pandas as pd
    import pyreadstat
except ImportError as exc:  # pragma: no cover - environment dependent
    raise MissingDependencyError(
        "sas7bdat_spark requires 'pyreadstat' and 'pandas'. "
        "Install them with: pip install pyreadstat pandas"
    ) from exc


# --------------------------------------------------------------------------- #
# Detect which keyword arguments the installed pyreadstat supports.
# Older releases lack row_offset/row_limit; we fall back to manual slicing.
# --------------------------------------------------------------------------- #
_SIGNATURE_PARAMS = set(inspect.signature(pyreadstat.read_sas7bdat).parameters)
SUPPORTS_ROW_LIMIT: bool = "row_limit" in _SIGNATURE_PARAMS
SUPPORTS_ROW_OFFSET: bool = "row_offset" in _SIGNATURE_PARAMS

_log.debug(
    "pyreadstat %s detected (row_limit=%s, row_offset=%s)",
    getattr(pyreadstat, "__version__", "unknown"),
    SUPPORTS_ROW_LIMIT,
    SUPPORTS_ROW_OFFSET,
)


def resolve_local_path(path: str) -> str:
    """Translate a Spark/DBFS path into a local FUSE path pyreadstat can open.

    pyreadstat reads from the local filesystem, so ``dbfs:`` URIs and ``/mnt/``
    mount points must be rewritten to their ``/dbfs`` FUSE equivalents.

    Args:
        path: The path as provided in the reader options.

    Returns:
        A local filesystem path.
    """
    if path.startswith(DBFS_API_PREFIX):
        return DBFS_LOCAL_PREFIX + path[len(DBFS_API_PREFIX):]
    if path.startswith("/mnt/"):
        return DBFS_LOCAL_PREFIX + path
    return path


def list_sas_files(path: str) -> list[str]:
    """Return resolved local paths for all .sas7bdat files at *path*.

    Accepts three forms:
    - A single ``.sas7bdat`` file path.
    - A directory — all ``*.sas7bdat`` files inside it are returned (sorted).
    - A glob pattern (contains ``*``, ``?``, or ``[``) — matched files are returned.

    Args:
        path: The path as provided in the reader options (DBFS URIs are resolved).

    Returns:
        A non-empty list of resolved local filesystem paths.

    Raises:
        SASFileNotFoundError: If no matching files are found.
    """
    local = resolve_local_path(path)

    if os.path.isdir(local):
        files = sorted(
            os.path.join(local, f)
            for f in os.listdir(local)
            if f.lower().endswith(".sas7bdat")
        )
        if not files:
            raise SASFileNotFoundError(
                f"No .sas7bdat files found in directory {local!r}."
            )
        return files

    if any(c in local for c in ("*", "?", "[")):
        files = sorted(_glob.glob(local))
        if not files:
            raise SASFileNotFoundError(
                f"No .sas7bdat files matched glob pattern {local!r}."
            )
        return files

    # Single file — existence check deferred to read_metadata for a clear error.
    return [local]


def read_sas7bdat(
    path: str,
    *,
    encoding: str = DEFAULT_ENCODING,
    metadataonly: bool = False,
    row_offset: int = 0,
    row_limit: int | None = None,
    usecols: list[str] | None = None,
) -> tuple[pd.DataFrame, Any]:
    """Read a SAS7BDAT file (or just its metadata) into a pandas DataFrame.

    Only forwards keyword arguments that the installed pyreadstat actually
    supports; when ``row_offset``/``row_limit`` are unsupported it slices the
    resulting DataFrame manually so behaviour is identical across versions.

    Note:
        pyreadstat's parameter is named ``row_limit`` (max rows to read), *not*
        ``row_count``; mixing them up silently reads the whole file.

    Args:
        path: Local filesystem path (already resolved via :func:`resolve_local_path`).
        encoding: Text encoding for character columns.
        metadataonly: When ``True``, only the metadata object is populated and the
            returned DataFrame is empty (fast schema inference).
        row_offset: Number of leading rows to skip.
        row_limit: Maximum number of rows to read (``None`` = all).
        usecols: Optional column projection.

    Returns:
        ``(dataframe, metadata)`` where ``metadata`` is pyreadstat's metadata object.

    Raises:
        SASReadError: If pyreadstat raises while reading the file.
    """
    kwargs: dict[str, Any] = {"encoding": encoding, "metadataonly": metadataonly}

    if not metadataonly:
        if SUPPORTS_ROW_OFFSET and row_offset > 0:
            kwargs["row_offset"] = row_offset
        if SUPPORTS_ROW_LIMIT and row_limit is not None:
            kwargs["row_limit"] = row_limit
        if usecols:
            kwargs["usecols"] = usecols

    try:
        df_pd, meta = pyreadstat.read_sas7bdat(path, **kwargs)
    except Exception as exc:  # noqa: BLE001 - re-wrap any pyreadstat failure
        raise SASReadError(f"pyreadstat failed to read {path!r}: {exc}") from exc

    # Manual slicing fallback for older pyreadstat without row_offset/row_limit.
    if not metadataonly:
        if not SUPPORTS_ROW_OFFSET and row_offset > 0:
            df_pd = df_pd.iloc[row_offset:]
        if not SUPPORTS_ROW_LIMIT and row_limit is not None:
            df_pd = df_pd.iloc[:row_limit]
        df_pd = df_pd.reset_index(drop=True)

    return df_pd, meta


def read_metadata(path: str, *, encoding: str = DEFAULT_ENCODING) -> Any:
    """Return only the pyreadstat metadata object for a file.

    Validates that the file exists first so the caller gets a clear
    :class:`SASFileNotFoundError` instead of an opaque pyreadstat error.

    Args:
        path: Local filesystem path.
        encoding: Text encoding for character columns.

    Returns:
        pyreadstat's metadata object (``number_rows``, ``column_names``, ...).

    Raises:
        SASFileNotFoundError: If ``path`` is not an existing file.
    """
    if not os.path.isfile(path):
        raise SASFileNotFoundError(
            f"SAS file not found at resolved path {path!r}. "
            f"Check the 'path' option (and DBFS mount, if applicable)."
        )
    _, meta = read_sas7bdat(path, encoding=encoding, metadataonly=True)
    return meta

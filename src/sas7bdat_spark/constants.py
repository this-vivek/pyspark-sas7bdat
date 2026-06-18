"""Static constants shared across the :mod:`sas7bdat_spark` package.

Keeping these in one place avoids the "magic string" anti-pattern and makes the
SAS format vocabulary easy to extend in a single, reviewable location.
"""

from __future__ import annotations

from typing import Final, FrozenSet

# --------------------------------------------------------------------------- #
# Data source identity
# --------------------------------------------------------------------------- #
#: The short name registered with Spark, i.e. ``spark.read.format(FORMAT_NAME)``.
FORMAT_NAME: Final[str] = "sas7bdat"

# --------------------------------------------------------------------------- #
# Defaults
# --------------------------------------------------------------------------- #
DEFAULT_ENCODING: Final[str] = "utf-8"
DEFAULT_NUM_PARTITIONS: Final[int] = 4

# --------------------------------------------------------------------------- #
# Databricks / DBFS path handling
# --------------------------------------------------------------------------- #
DBFS_LOCAL_PREFIX: Final[str] = "/dbfs"
DBFS_API_PREFIX: Final[str] = "dbfs:"

# --------------------------------------------------------------------------- #
# StructField.metadata keys
# --------------------------------------------------------------------------- #
#: Carries the raw SAS format string through the schema so the reader can tell
#: a ``TIME`` column apart from a ``DATETIME`` column without re-opening the file.
META_SAS_FORMAT: Final[str] = "sas_format"

#: Carries the SAS variable label (free-text description) for downstream tooling.
META_SAS_LABEL: Final[str] = "sas_label"

# --------------------------------------------------------------------------- #
# SAS format vocabularies
# --------------------------------------------------------------------------- #
# SAS encodes the *intended* rendering of a numeric column in its format string
# (e.g. ``DATE9.``, ``DATETIME20.``). We map those families onto Spark types.
# Width/decimal suffixes are stripped before matching (see ``type_mapping.fmt_base``),
# so only the base token needs to be listed here.

SAS_DATE_FORMATS: Final[FrozenSet[str]] = frozenset(
    {
        "DATE",
        "DDMMYY",
        "MMDDYY",
        "YYMMDD",
        "YYMMDDN",
        "MMDDYYN",
        "DATE9",
        "DATE7",
        "JULIAN",
        "MONYY",
        "WEEKDATE",
        "DTDATE",
        "EURDFDD",
        "EURDFDE",
    }
)

SAS_DATETIME_FORMATS: Final[FrozenSet[str]] = frozenset(
    {
        "DATETIME",
        "DATETIME18",
        "DATETIME20",
        "DATETIME22",
        "DATEAMPM",
        "DTMONYY",
        "MDYAMPM",
        "E8601DT",
        "B8601DT",
    }
)

# Kept separate from DATETIME because TIME values are *seconds since midnight*
# and need a different coercion path (timedelta), not an epoch interpretation.
SAS_TIME_FORMATS: Final[FrozenSet[str]] = frozenset(
    {
        "TIME",
        "TOD",
        "HHMM",
        "HOUR",
        "MMSS",
        "E8601TM",
        "B8601TM",
    }
)

#: Anchor date for SAS TIME columns. The date component is arbitrary and is only
#: meaningful when the column is materialised as a full ``TimestampType``; callers
#: who care solely about the time-of-day can extract it in SQL.
TIME_ANCHOR_DATE: Final[str] = "1970-01-01"

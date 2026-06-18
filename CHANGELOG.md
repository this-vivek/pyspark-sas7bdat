# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/) and the project adheres to
semantic versioning.

## [1.0.0] - 2026-06-18

First packaged release, refactored from a single-file Databricks notebook script
into an installable, tested Python package.

### Added
- `src/`-layout package `sas7bdat_spark` with separated modules
  (constants, exceptions, options, io, type_mapping, coercion, schema, reader,
  datasource, logging).
- Typed, validated `SASOptions` dataclass for reader options.
- New options: `lowercase_columns`, `timestamp_ntz`, `infer_integer`,
  `sample_rows`.
- SAS variable **labels** and original column names preserved in field metadata.
- Custom exception hierarchy (`SAS7bdatError` and friends).
- Standard-library logging (NullHandler) instead of `print`.
- Unit tests, ruff/mypy config, GitHub Actions CI, packaging metadata.

### Fixed (carried over from the notebook iterations)
- SAS `TIME` (seconds-since-midnight) coercion.
- tz-aware vs tz-naive timestamp handling across pandas versions.
- `NaTType does not support astimezone` in Spark's Arrow output layer by
  emitting native Python date/datetime objects.
- `pd.NA` from nullable `Int64` columns sanitised to `None`.
- Long (>8 char) SAS column-name metadata lookups.
- Character-missing `""` mapped to SQL `NULL`.

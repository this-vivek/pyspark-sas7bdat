# pyspark-sas7bdat

Read SAS **`.sas7bdat`** files natively in PySpark through a custom
[Python DataSource](https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html),
backed by [pyreadstat](https://github.com/Roche/pyreadstat). Built and tested
for **Databricks (DBR 15.2+) / PySpark 4.0+** on **Python 3.10+**.

```python
from sas7bdat_spark import register

register(spark)

# Single file
df = spark.read.format("sas7bdat").load("/path/to/file.sas7bdat")

# Entire directory — all .sas7bdat files processed in parallel
df = spark.read.format("sas7bdat").load("/path/to/sas_files/")

# Glob pattern
df = spark.read.format("sas7bdat").load("/path/to/sas_files/*.sas7bdat")

df.show()
```

No JVM library, no intermediate CSV/Parquet export — files are read directly
into a Spark DataFrame with correct SAS date/time/datetime semantics.

---

## Why this exists

SAS stores dates, times, and datetimes as numbers with a *format* attached, and
exposes long (>8 char) column names and labels. Naively bridging pyreadstat →
pandas → Spark/Arrow trips over several sharp edges:

| Pitfall | Symptom | How this library handles it |
| --- | --- | --- |
| `TIME` = seconds since midnight | `01:01:01` read as `…000003661` ns | Coerced via `to_timedelta`, anchored to a reference date |
| pyreadstat returns tz-aware UTC | `Cannot convert tz-naive Timestamp` | Timezone normalised defensively across pandas versions |
| `NaT` in Arrow output layer | `NaTType does not support astimezone` | Date/time columns emitted as **native Python** objects |
| Nullable `Int64` → `pd.NA` | Arrow serialisation failure | Every NA sentinel sanitised to `None` at the yield boundary |
| Long names keyed by 8-char truncation | columns silently typed as string | Metadata lookup falls back to the truncated key |
| Character missing `""` | empty string instead of `NULL` | Mapped to SQL `NULL` |

---

## Installation

```bash
pip install sas7bdat-spark        # plus pyreadstat, pandas
# on a local machine you also want Spark:
pip install "sas7bdat-spark[spark]"
```

On Databricks:

```python
%pip install pyreadstat sas7bdat-spark
dbutils.library.restartPython()
```

**Requirements:** Python 3.10+, PySpark 4.0+ / Databricks DBR 15.2+.

---

## Multi-file parallel reads

Pass a directory or glob pattern to `.load()` and every matching `.sas7bdat`
file is read in parallel across Spark executors. Schema is inferred from the
first file — all files must share the same schema.

```python
register(spark)

# All files in a directory
df = (
    spark.read.format("sas7bdat")
    .option("num_partitions", "4")   # partitions per file
    .load("/mnt/data/sas_exports/")
)

# Glob — only files matching a pattern
df = spark.read.format("sas7bdat").load("/mnt/data/sas_exports/claims_*.sas7bdat")
```

**How the task count works:**

```
10 files  ×  num_partitions=4  →  40 Spark tasks
```

Each file is independently split into `num_partitions` row-range chunks.
Spark schedules all tasks across available executors, so both cross-file and
within-file parallelism are fully utilised.

---

## Options

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `path` | str | — (required) | Path to a `.sas7bdat` file, a directory of `.sas7bdat` files, or a glob pattern. `dbfs:` and `/mnt/` URIs are resolved automatically. |
| `encoding` | str | `utf-8` | Text encoding for character columns. |
| `num_partitions` | int | `4` | Spark partitions **per file** (row-range chunks). |
| `row_offset` | int | `0` | Leading rows to skip (applied per file). |
| `row_count` | int | all | Maximum rows to read (applied per file). |
| `column_select` | csv | all | Project a subset of columns early. |
| `lowercase_columns` | bool | `false` | Lower-case all schema column names. |
| `timestamp_ntz` | bool | `false` | Use `TimestampNTZType` (no session-tz shift) for SAS datetimes. |
| `infer_integer` | bool | `false` | Sample rows to promote integer-valued columns to `LongType`. |
| `sample_rows` | int | `1000` | Rows sampled when `infer_integer` is enabled. |

```python
df = (
    spark.read.format("sas7bdat")
    .option("num_partitions", "8")
    .option("column_select", "id,event_dt,amount")
    .option("timestamp_ntz", "true")
    .load("/mnt/data/sas_exports/")
)
```

SAS variable labels are preserved on each field's metadata under `sas_label`,
and the original SAS column name under `sas_source_name`.

---

## Project layout

```
src/sas7bdat_spark/
├── __init__.py        # public API: register(), SASDataSource, SASOptions, errors
├── constants.py       # format vocabularies, defaults, metadata keys
├── exceptions.py      # SAS7bdatError hierarchy
├── options.py         # typed, validated SASOptions dataclass
├── io.py              # pyreadstat wrapper, DBFS path resolution, multi-file glob
├── type_mapping.py    # SAS type/format -> Spark DataType
├── coercion.py        # Arrow-safe pandas -> Python value coercion
├── schema.py          # StructType inference (labels, projection)
├── reader.py          # SASPartition (file + row range) + SASDataSourceReader
├── datasource.py      # SASDataSource + register()
└── _logging.py        # NullHandler-based library logging
```

---

## Development

```bash
pip install -e ".[dev]"
pytest            # run tests
ruff check .      # lint
mypy src          # type-check
```

CI runs the full matrix against Python 3.10, 3.11, and 3.12.

---

## Notes & caveats

- **Python version.** Requires Python 3.10+. Python 3.9 is not supported because
  the upstream `pyreadstat` library requires Python 3.10+ as of its recent releases.
- **Timezones.** SAS datetimes are UTC-based. With the default `TimestampType`,
  Spark interprets the materialised wall-clock value in the session timezone.
  Set `spark.sql.session.timeZone` to `UTC`, or use `timestamp_ntz=true`, to
  avoid shifting.
- **pyreadstat reads locally.** Files must be on a local/FUSE path
  (`/dbfs/...`); `dbfs:` and `/mnt/` URIs are translated automatically.
- **Shared schema.** For multi-file reads, schema is inferred from the first
  file (alphabetically). All files in the set must have identical column names
  and types — mismatched schemas will cause read failures at the executor.
- **Partitioning** is by row range; every partition re-opens the file with a
  `row_offset`/`row_limit` window. `row_offset` and `row_count` options apply
  independently to each file.

## License

MIT — see [LICENSE](LICENSE).

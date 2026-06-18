"""Example: reading SAS7BDAT files on Databricks with sas7bdat_spark.

Run the cells in a Databricks notebook (DBR 15.2+ / PySpark 4.0+). The cluster
must have ``pyreadstat`` installed:

    %pip install pyreadstat sas7bdat-spark
    dbutils.library.restartPython()
"""

from pyspark.sql import SparkSession

from sas7bdat_spark import register

spark = SparkSession.builder.getOrCreate()

# 1. Register the data source once per session.
register(spark)

PATH = "dbfs:/FileStore/sas/dates_longname_char.sas7bdat"

# 2. Basic read — schema is inferred from the SAS metadata.
df = spark.read.format("sas7bdat").load(PATH)
df.printSchema()
df.show(truncate=False)

# 3. Tuning options.
df_tuned = (
    spark.read.format("sas7bdat")
    .option("num_partitions", "8")        # parallelism
    .option("encoding", "latin1")         # non-UTF-8 files
    .option("column_select", "dates,times")  # project columns early
    .option("row_count", "100000")        # cap rows
    .option("lowercase_columns", "true")  # normalise names
    .option("timestamp_ntz", "true")      # keep SAS UTC wall-clock, no tz shift
    .load(PATH)
)
df_tuned.show()

# 4. SAS variable labels are preserved in field metadata.
for field in df.schema.fields:
    label = field.metadata.get("sas_label", "")
    print(f"{field.name:20s} {str(field.dataType):16s} {label}")

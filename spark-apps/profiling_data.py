"""
 Profile the raw clearing data.
Outputs:
  - column_summary (one row per column)
  - top_values    (top 10 values per categorical column)
Both written to HDFS as Parquet.
"""
from datetime import date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType
from schema import clearing_schema


INGESTION_DATE = date.today().isoformat()
RAW_PATH=f"hdfs://hdfs-namenode:9000/data/clearing/raw/ingestion_date={INGESTION_DATE}"
PROFILE_BASE="hdfs://hdfs-namenode:9000/data/clearing/profiles"

COLUMN_SUMMARY_PATH=f"{PROFILE_BASE}/ingestion_date={INGESTION_DATE}/coumn_summary"
TOP_VALUES_PATH = f"{PROFILE_BASE}/ingestion_date={INGESTION_DATE}/top_values"


def profile_columns(df:DataFrame) -> DataFrame:
    """
    Return one row per column with total, null count, null %, distinct count,
    min and max (as strings for compatibility).
    """

    total = df.count()
    rows = []

    for field in df.schema.fields:
        col = field.name
        dtype = field.dataType


        agg = df.agg(
            F.count(F.lit(1)).alias("total"),
            F.sum(F.when(F.col(col).isNull(), 1).otherwise(0)).alias("null_count"),
            F.count_distinct(F.col(col)).alias("distinct_count")
        ).collect()[0]


        null_count = agg["null_count"] or 0
        distinct_count = agg["distinct_count"] or 0

        try:
            max_min = df.agg(
                F.max(F.col(col).cast("string")).alias("min_value"),
                F.min(F.col(col).cast("string")).alias("max_value")
            )

            min_value = max_min["min_value"]
            max_value = max_min["max_value"]

        except Exception:
            min_value, max_value = None, None

        rows.append((
            col,
            str(dtype),
            total,
            int(null_count),
            round((null_count/total) * 100, 4) if total else 0.0,
            int(distinct_count),
            min_value,
            max_value
        ))

        schema = "column_name string, data_type string, total_count long, " \
             "null_count long, null_percent double, distinct_count long, " \
             "min_value string, max_value string"

        return df.sparkSession.createDataFrame(rows, schema=schema)
    
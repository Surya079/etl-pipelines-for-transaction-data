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
from schema.clearing_schema import CLEARING_SCHEMA

INGESTION_DATE = date.today().isoformat()
RAW_PATH = (
    f"hdfs://hdfs-namenode:9000/data/clearing/raw/ingestion_date={INGESTION_DATE}"
)
PROFILE_BASE = "hdfs://hdfs-namenode:9000/data/clearing/profiles"

COLUMN_SUMMARY_PATH = f"{PROFILE_BASE}/ingestion_date={INGESTION_DATE}/coumn_summary"
TOP_VALUES_PATH = f"{PROFILE_BASE}/ingestion_date={INGESTION_DATE}/top_values"


def profile_columns(df: DataFrame) -> DataFrame:
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
            F.count_distinct(F.col(col)).alias("distinct_count"),
        ).collect()[0]

        null_count = agg["null_count"] or 0
        distinct_count = agg["distinct_count"] or 0

        try:
            max_min = df.agg(
                F.max(F.col(col).cast("string")).alias("min_value"),
                F.min(F.col(col).cast("string")).alias("max_value"),
            ).collect()[0]

            min_value = max_min["min_value"]
            max_value = max_min["max_value"]

        except Exception:
            min_value, max_value = None, None

        rows.append(
            (
                col,
                str(dtype),
                total,
                int(null_count),
                round((null_count / total) * 100, 4) if total else 0.0,
                int(distinct_count),
                min_value,
                max_value,
            )
        )

    schema = (
        "column_name string, data_type string, total_count long, "
        "null_count long, null_percent double, distinct_count long, "
        "min_value string, max_value string"
    )

    return df.sparkSession.createDataFrame(rows, schema=schema)


def top_values(
    df: DataFrame,
    summary_df: DataFrame,
    max_distinct: int = 1000,
    max_cols: int = 20,
    top_n: int = 10,
) -> DataFrame:
    """
    For low-cardinality categorical columns, compute top N values with counts.
    Columns with distinct_count >= max_distinct are skipped (they are identifiers,
    not categories).
    """
    # Get categorical columns
    cat_cols = [
        f.name
        for f in df.schema.fields
        if isinstance(f.dataType, (StringType, BooleanType))
    ]

    # Filter to low-cardinality columns using the profile we already computed
    low_card_rows = (
        summary_df.filter(F.col("column_name").isin(cat_cols))
        .filter(F.col("distinct_count") < max_distinct)
        .select("column_name")
        .collect()
    )
    low_card_cols = [r["column_name"] for r in low_card_rows][:max_cols]

    if not low_card_cols:
        # Return an empty DataFrame with the right schema
        schema = "column_name string, value string, count long"
        return df.sparkSession.createDataFrame([], schema=schema)

    frames = []
    for col in low_card_cols:
        top = (
            df.groupBy(F.col(col).cast("string").alias("value"))
            .count()
            .orderBy(F.desc("count"))
            .limit(top_n)
            .withColumn("column_name", F.lit(col))
            .select("column_name", "value", "count")
        )
        frames.append(top)

    result = frames[0]
    for f in frames[1:]:
        result = result.unionByName(f)

    return result


def main():

    spark = (
        SparkSession.builder.appName("profile-raw-clearing")
        .config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:9000")
        .getOrCreate()
    )

    print(f"Reading raw data: {RAW_PATH}")

    df = spark.read.option("header", True).schema(CLEARING_SCHEMA).csv(RAW_PATH)

    print(f"Row count: {df.count():,}")

    print("Profiling columns...")
    summary = profile_columns(df)
    summary.show(50, truncate=False)
    summary.write.mode("overwrite").parquet(COLUMN_SUMMARY_PATH)
    print(f"✅ Column summary written to {COLUMN_SUMMARY_PATH}")

    print("Computing top values for categorical columns...")
    tops = top_values(df, summary_df=summary)
    tops.write.mode("overwrite").parquet(TOP_VALUES_PATH)
    print(f"✅ Top values written to {TOP_VALUES_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()

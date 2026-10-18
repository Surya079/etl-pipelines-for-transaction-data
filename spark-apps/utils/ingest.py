from __future__ import annotations
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def read_csv_with_schema(spark: SparkSession, path: str, schema) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(schema)
        .csv(path)
    )


def split_good_and_rejected(
    df: DataFrame, mandatory_cols: list[str]
) -> tuple[DataFrame, DataFrame]:
    """
    Split a DataFrame into (good, rejected) without losing rows.
    A row is rejected if Spark couldn't parse it OR a mandatory column is null.
    """
    if "_corrupt_record" not in df.columns:
        df = df.withColumn("_corrupt_record", F.lit(None).cast("string"))

    reason = F.when(F.col("_corrupt_record").isNotNull(), F.lit("corrupt_record"))
    for col in mandatory_cols:
        reason = reason.when(F.col(col).isNull(), F.lit(f"null_{col}"))
    reason = reason.otherwise(F.lit(None).cast("string"))

    marked = df.withColumn("_reject_reason", reason)

    good = marked.filter(F.col("_reject_reason").isNull()).drop(
        "_corrupt_record", "_reject_reason"
    )
    rejected = marked.filter(F.col("_reject_reason").isNotNull()).drop(
        "_corrupt_record"
    )
    return good, rejected

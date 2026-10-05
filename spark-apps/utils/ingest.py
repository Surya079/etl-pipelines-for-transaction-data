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


def split_good_and_rejected(df: DataFrame) -> tuple[DataFrame, DataFrame]:

    mandatory = ["clearing_file_id", "transaction_id", "clearing_date"]

    good = df.filter(F.col("_corrupt_record").isNull())

    for col in mandatory:
        good = good.filter(F.col(col).isNotNull())

    rejected = df.subtract(good)

    return good.drop("_corrupt_record"), rejected

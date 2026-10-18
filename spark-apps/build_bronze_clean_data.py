"""
 Bronze cleaning layer.
Reads raw CSV, applies semantic cleaning rules, splits into clean + rejected.
"""

from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from schema.clearing_schema import CLEARING_SCHEMA
from utils.clean import (
    clean_strings,
    normalize_case,
    cast_amounts,
    add_validation_flags,
    add_dq_flags,
    deduplicate,
)

INGESTION_DATE = date.today().isoformat()
RAW_PATH = (
    f"hdfs://hdfs-namenode:9000/data/clearing/raw/ingestion_date={INGESTION_DATE}"
)
BRONZE_BASE = "hdfs://hdfs-namenode:9000/data/clearing/bronze"
REJECTED_BASE = "hdfs://hdfs-namenode:9000/data/clearing/bronze_rejected"


BRONZE_PATH = f"{BRONZE_BASE}/ingestion_date={INGESTION_DATE}"
REJECTED_PATH = f"{REJECTED_BASE}/ingestion_date={INGESTION_DATE}"


def main():
    spark = (
        SparkSession.builder.appName("Bronze-Cleaning")
        .config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:9000")
        .getOrCreate()
    )

    print(f"Reading raw: {RAW_PATH}")
    df = (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(CLEARING_SCHEMA)
        .csv(RAW_PATH)
    )
    raw_count = df.count()
    print(f"Raw rows: {raw_count:,}")

    # 1-3. String cleanup
    df = clean_strings(df)
    # 4. Case normalization
    df = normalize_case(df)
    # 5-9. Type casting
    df = cast_amounts(df)
    # 10. Validation (adds _reject_reason)
    df = add_validation_flags(df)
    # 12. Info flags
    df = add_dq_flags(df)

    # Split BEFORE dedup so we don't lose rejected rows
    good = df.filter(F.col("_reject_reason").isNull()).drop("_reject_reason")
    rejected = df.filter(F.col("_reject_reason").isNotNull())

    # 11. Dedup only the good side
    good_before_dedup = good.count()
    good = deduplicate(good)
    good_after_dedup = good.count()
    dup_removed = good_before_dedup - good_after_dedup

    rejected_count = rejected.count()

    print(f"Good (pre-dedup):  {good_before_dedup:,}")
    print(f"Duplicates removed: {dup_removed:,}")
    print(f"Good (post-dedup):  {good_after_dedup:,}")
    print(f"Rejected:           {rejected_count:,}")
    print(
        f"Check: good+rejected+dups = {good_after_dedup + rejected_count + dup_removed:,}"
    )

    assert (
        good_after_dedup + rejected_count + dup_removed == raw_count
    ), "Count mismatch!"

    print(f"Writing bronze to {BRONZE_PATH}")
    (good.coalesce(4).write.mode("overwrite").parquet(BRONZE_PATH))

    if rejected_count > 0:
        print(f"Writing rejected to {REJECTED_PATH}")
        (rejected.coalesce(4).write.mode("overwrite").parquet(REJECTED_PATH))
        print("Reject reasons:")
        rejected.groupBy("_reject_reason").count().orderBy(F.desc("count")).show(
            truncate=False
        )

    print("✅data cleansing complete.")
    spark.stop()


if __name__ == "__main__":
    main()

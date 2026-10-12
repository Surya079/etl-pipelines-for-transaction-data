"""
Build the bronze clean layer from raw clearing data.
- Standardize strings (trim, uppercase codes)
- Deduplicate on transaction_id (keep latest file_process_date)
- Partition by clearing_date
"""

from datetime import date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from schema.clearing_schema import CLEARING_SCHEMA

INGESTION_DATE = date.today().isoformat()
RAW_PATH = (
    f"hdfs://hdfs-namenode:9000/data/clearing/raw/ingestion_date={INGESTION_DATE}"
)
BRONZE_PATH = "hdfs://hdfs-namenode:9000/data/clearing/bronze"

UPPERCASE_COLS = [
    "currency",
    "cardholder_billing_currency",
    "settlement_currency",
    "card_network",
    "card_type",
    "mcc",
    "record_status",
    "function_code",
    "processing_code",
    "cvv_result",
    "avs_result",
    "terminal_environment",
    "issuer_country",
    "merchant_country",
    "transaction_type",
    "pos_entry_mode",
]

TRIM_ONLY_COLS = [
    "merchant_name",
    "merchant_dba_name",
    "merchant_city",
    "merchant_state",
    "merchant_postal_code",
    "mcc_description",
    "acquirer_bank",
    "card_number_masked",
    "issuer_bin",
    "authorization_code",
    "rrn",
    "trace_number",
    "terminal_id",
    "chargeback_reason_code",
    "file_name",
]


def standardize(df: DataFrame) -> DataFrame:
    for col in UPPERCASE_COLS:
        if col in df.columns:
            df = df.withColumn(col, F.upper(F.trim(F.col(col))))

    for col in TRIM_ONLY_COLS:
        if col in df.columns:
            df = df.withColumn(col, F.trim(F.col(col)))
    return df


def deduplicate(df: DataFrame) -> DataFrame:
    w = Window.partitionBy("transaction_id").orderBy(
        F.col("file_process_date").desc_nulls_last(),
        F.col("clearing_date").desc(),
    )

    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def main():

    spark = (
        SparkSession.builder.appName("build-bronze-level-clearing-data")
        .config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:9000")
        .getOrCreate()
    )

    print(f"Reading raw: {RAW_PATH}")

    raw = spark.read.option("header", True).schema(CLEARING_SCHEMA).csv(RAW_PATH)

    raw_count = raw.count()
    print(f"Raw rows: {raw_count:,}")

    print("Standardizing strings...")
    clean = standardize(raw)

    print("Deduplicating on transaction_id...")

    dedup = deduplicate(clean)
    dedup_count = dedup.count()

    print(f"After dedup: {dedup_count:,}")
    print(f"Duplicates removed: {raw_count - dedup_count:,}")

    print("Writing bronze partitioned by clearing_date...")

    (dedup.write.mode("overwrite").partitionBy("clearing_date").parquet(BRONZE_PATH))
    print(f"✅ Bronze written to {BRONZE_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()

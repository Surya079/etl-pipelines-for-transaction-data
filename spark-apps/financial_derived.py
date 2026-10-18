from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

INGESTION_DATE = date.today().isoformat()
BRONZE_PATH = (
    f"hdfs://hdfs-namenode:9000/data/clearing/bronze/ingestion_date={INGESTION_DATE}"
)
SILVER_BASE = "hdfs://hdfs-namenode:9000/data/clearing/silver/financials"
SILVER_PATH = f"{SILVER_BASE}/ingestion_date={INGESTION_DATE}"


def add_financial_derived(df):
    """
    Add financial derived columns. Each column has a clear business purpose.
    """

    # 1. Excepted net amount

    expected_net = (
        F.col("gross_amount")
        - F.coalesce(F.col("interchange_fee"), F.lit(0))
        - F.coalesce(F.col("network_fee"), F.lit(0))
        - F.coalesce(F.col("acquirer_fee"), F.lit(0))
    ).cast(DecimalType(18, 2))

    df = df.withColumn("expected_net_amount", expected_net)

    # 2.  fee variance

    df = df.withColumn(
        "fee_variance",
        F.col("net_settlement_amount")
        - F.col("expected_net_amount").cast(DecimalType(18, 2)),
    )

    # 3. fee variance flag
    df = df.withColumn("fee_variance_flag", F.abs(F.col("fee_variance")) > F.lit(0.01))

    # 4. total fees
    df = df.withColumn(
        "total_fees",
        (
            F.coalesce(F.col("interchange_fee"), F.lit(0))
            + F.coalesce(F.col("network_fee"), F.lit(0))
            + F.coalesce(F.col("acquirer_fee"), F.lit(0))
        ).cast(DecimalType(18, 2)),
    )

    # 5. fee ratio
    df = df.withColumn(
        "fee_ratio",
        F.when(
            F.col("gross_amount") > 0,
            (F.col("total_fees") / F.col("gross_amount")).cast(DecimalType(8, 4)),
        ).otherwise(F.lit(None).cast(DecimalType(8, 4))),
    )

    # 6. Merchant payout ratio
    df = df.withColumn(
        "merchant_payout_ratio",
        F.when(
            F.col("gross_amount") > 0,
            (F.col("net_settlement_amount") / F.col("gross_amount")).cast(
                DecimalType(8, 4)
            ),
        ).otherwise(F.lit(None).cast(DecimalType(8, 4))),
    )

    # 7. is domestic
    df = df.withColumn(
        "is_domestic", F.col("merchant_country") == F.col("issuer_country")
    )

    # 8. is charge back
    df = df.withColumn(
        "is_chargeback",
        F.upper(F.col("record_status")) == F.lit("CHARGEBACK"),
    )

    # 9. is refund
    df = df.withColumn(
        "is_refund",
        F.upper(F.col("transaction_type")) == F.lit("REFUND"),
    )

    # 10. transaction month
    df = df.withColumn(
        "transaction_month",
        F.date_format(F.col("transaction_datetime"), "yyyy-MM"),
    )
    # 11. transaction_day_of_week (1=Sunday .. 7=Saturday in Spark)
    df = df.withColumn(
        "transaction_day_of_week",
        F.dayofweek(F.col("transaction_datetime")),
    )

    # 12. transaction_hour_utc
    df = df.withColumn(
        "transaction_hour_utc",
        F.hour(F.col("transaction_datetime")),
    )

    return df


def main():
    spark = (
        SparkSession.builder.appName("Financial-derived")
        .config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:9000")
        .getOrCreate()
    )

    print(f"Reading bronze: {BRONZE_PATH}")
    df = spark.read.parquet(BRONZE_PATH)
    raw_cnt = df.count()
    print(f"Bronze rows: {raw_cnt:,}")

    print("Adding derived columns...")
    df = add_financial_derived(df)
    df.cache()

    # ------- Sanity checks -------
    print("\n--- Sanity checks ---")

    # 1. No row should have fee_variance > 0.01 (bronze already validated)
    var_count = df.filter(F.col("fee_variance_flag")).count()
    print(f"Rows with fee_variance > 0.01: {var_count:,}")

    # 2. fee_ratio range
    fr = df.agg(
        F.min("fee_ratio").alias("min_fr"),
        F.max("fee_ratio").alias("max_fr"),
        F.avg("fee_ratio").alias("avg_fr"),
    ).collect()[0]
    print(f"fee_ratio: min={fr['min_fr']}, max={fr['max_fr']}, avg={fr['avg_fr']}")

    # 3. payout ratio range
    pr = df.agg(
        F.min("merchant_payout_ratio").alias("min_pr"),
        F.max("merchant_payout_ratio").alias("max_pr"),
        F.avg("merchant_payout_ratio").alias("avg_pr"),
    ).collect()[0]
    print(f"payout_ratio: min={pr['min_pr']}, max={pr['max_pr']}, avg={pr['avg_pr']}")

    # 4. Domestic vs cross-border
    print("\nDomestic vs cross-border:")
    df.groupBy("is_domestic").count().show()

    # 5. Refund vs purchase
    print("Refund vs purchase:")
    df.groupBy("is_refund").count().show()

    # 6. Chargeback count
    print(f"Chargebacks: {df.filter('is_chargeback').count():,}")

    # 7. Sum of derived fields equals sum of raw fields
    sums = df.agg(
        F.sum("gross_amount").alias("gross"),
        F.sum("total_fees").alias("fees"),
        F.sum("net_settlement_amount").alias("net"),
        F.sum("expected_net_amount").alias("expected_net"),
    ).collect()[0]
    print(f"\nTotals:")
    print(f"  gross_amount         = {sums['gross']}")
    print(f"  total_fees           = {sums['fees']}")
    print(f"  net_settlement_amount= {sums['net']}")
    print(f"  expected_net_amount  = {sums['expected_net']}")
    print(f"  gross - fees         = {sums['gross'] - sums['fees']}")
    print(f"  (should match net within rounding tolerance)")

    print(f"\nWriting silver: {SILVER_PATH}")
    df.coalesce(4).write.mode("overwrite").parquet(SILVER_PATH)
    print("✅ Compute financial data reconciliation complete.")
    spark.stop()


if __name__ == "__main__":
    main()

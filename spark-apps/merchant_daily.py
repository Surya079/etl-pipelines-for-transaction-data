"""
Silver merchant daily fact table.
Aggregates financials by merchant_id + transaction_date.
"""

from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

INGESTION_DATE = date.today().isoformat()
SILVER_PATH = "hdfs://hdfs-namenode:9000/data/clearing/silver/financials/ingestion_date=2026-10-18"
OUTPUT_BASE = "hdfs://hdfs-namenode:9000/data/clearing/silver/merchant_daily"

OUTPUT_PATH = f"{OUTPUT_BASE}/ingestion_date=2026-10-18"


def build_merchant_daily(df):
    """
    Aggregate silver financials into one row per (merchant_id, transaction_date).
    """

    # Derive a transaction_date from posting_date or transaction_datetime
    df = df.withColumn(
        "transaction_date",
        F.to_date(F.col("posting_date")),
    )

    # Pre-compute flags used in aggregation
    df = df.withColumn(
        "is_refund_flag",
        F.when(F.col("is_refund"), 1).otherwise(0),
    )

    df = df.withColumn(
        "is_chargeback_flag",
        F.when(F.col("is_chargeback"), 1).otherwise(0),
    )

    df = df.withColumn(
        "is_domestic_flag",
        F.when(F.col("is_domestic"), 1).otherwise(0),
    )

    # Aggregate
    agg = df.groupBy("merchant_id", "merchant_name", "transaction_date").agg(
        # Counts
        F.count("*").alias("txn_count"),
        F.sum(1 - F.col("is_refund_flag")).alias("purchase_count"),
        F.sum("is_refund_flag").alias("refund_count"),
        F.sum("is_chargeback_flag").alias("chargeback_count"),
        # Amounts
        F.sum("gross_amount").cast(DecimalType(18, 2)).alias("gross_amount"),
        F.sum(F.when(F.col("is_refund_flag") == 0, F.col("gross_amount")).otherwise(0))
        .cast(DecimalType(18, 2))
        .alias("purchase_gross"),
        F.sum(F.when(F.col("is_refund_flag") == 1, F.col("gross_amount")).otherwise(0))
        .cast(DecimalType(18, 2))
        .alias("refund_gross"),
        # Fees
        F.sum("total_fees").cast(DecimalType(18, 2)).alias("total_fees"),
        F.sum("interchange_fee").cast(DecimalType(18, 2)).alias("interchange_fees"),
        F.sum("network_fee").cast(DecimalType(18, 2)).alias("network_fees"),
        F.sum("acquirer_fee").cast(DecimalType(18, 2)).alias("acquirer_fees"),
        # Net settlement
        F.sum("net_settlement_amount")
        .cast(DecimalType(18, 2))
        .alias("net_settlement_amount"),
        F.sum("chargeback_amount").cast(DecimalType(18, 2)).alias("chargeback_amount"),
        # Averages
        F.avg("fee_ratio").cast(DecimalType(8, 4)).alias("fee_ratio_avg"),
        # Domestic / cross-border
        F.sum("is_domestic_flag").alias("domestic_txn_count"),
        F.sum(1 - F.col("is_domestic_flag")).alias("cross_border_txn_count"),
    )

    # Derived metrics
    agg = agg.withColumn(
        "avg_ticket",
        F.when(
            F.col("txn_count") > 0,
            (F.col("gross_amount") / F.col("txn_count")).cast(DecimalType(18, 2)),
        ).otherwise(F.lit(None).cast(DecimalType(18, 2))),
    )
    agg = agg.withColumn(
        "chargeback_rate",
        F.when(
            F.col("txn_count") > 0,
            (F.col("chargeback_count") / F.col("txn_count")).cast(DecimalType(8, 4)),
        ).otherwise(F.lit(None).cast(DecimalType(8, 4))),
    )
    agg = agg.withColumn(
        "refund_rate",
        F.when(
            F.col("txn_count") > 0,
            (F.col("refund_count") / F.col("txn_count")).cast(DecimalType(8, 4)),
        ).otherwise(F.lit(None).cast(DecimalType(8, 4))),
    )

    return agg


def main():
    spark = (
        SparkSession.builder.appName("Merchant-Daily")
        .config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:9000")
        .getOrCreate()
    )

    print(f"Reading silver: {SILVER_PATH}")
    silver = spark.read.parquet(SILVER_PATH)
    silver_cnt = silver.count()
    print(f"Silver rows: {silver_cnt:,}")

    # Source totals for reconciliation
    source_totals = silver.agg(
        F.sum("gross_amount").alias("src_gross"),
        F.sum("net_settlement_amount").alias("src_net"),
        F.sum("total_fees").alias("src_fees"),
        F.count("*").alias("src_cnt"),
    ).collect()[0]

    print(f"Source gross: {source_totals['src_gross']}")
    print(f"Source net:   {source_totals['src_net']}")
    print(f"Source fees:  {source_totals['src_fees']}")
    print(f"Source count: {source_totals['src_cnt']:,}")

    print("\nBuilding merchant-daily aggregates...")
    merchant_daily = build_merchant_daily(silver)
    merchant_daily.cache()
    fact_cnt = merchant_daily.count()
    print(f"Fact rows: {fact_cnt:,}")

    # Reconciliation: aggregated totals must equal source totals
    fact_totals = merchant_daily.agg(
        F.sum("gross_amount").alias("agg_gross"),
        F.sum("net_settlement_amount").alias("agg_net"),
        F.sum("total_fees").alias("agg_fees"),
        F.sum("txn_count").alias("agg_cnt"),
    ).collect()[0]

    print(f"\nReconciliation:")
    print(
        f"  gross: source={source_totals['src_gross']} fact={fact_totals['agg_gross']}"
    )
    print(f"  net:   source={source_totals['src_net']} fact={fact_totals['agg_net']}")
    print(f"  fees:  source={source_totals['src_fees']} fact={fact_totals['agg_fees']}")
    print(f"  count: source={source_totals['src_cnt']} fact={fact_totals['agg_cnt']}")

    assert source_totals["src_cnt"] == fact_totals["agg_cnt"], "Count mismatch!"
    assert (
        abs(float(source_totals["src_gross"]) - float(fact_totals["agg_gross"])) < 0.01
    ), "Gross mismatch!"
    assert (
        abs(float(source_totals["src_net"]) - float(fact_totals["agg_net"])) < 0.01
    ), "Net mismatch!"
    print("✅ Reconciliation passed.")

    print("\nSample rows:")
    merchant_daily.orderBy("merchant_id", "transaction_date").show(10, truncate=False)

    print(f"\nWriting to {OUTPUT_PATH}")
    merchant_daily.coalesce(2).write.mode("overwrite").parquet(OUTPUT_PATH)
    print("✅ Merchant Daily job completed")
    spark.stop()


if __name__ == "__main__":
    main()

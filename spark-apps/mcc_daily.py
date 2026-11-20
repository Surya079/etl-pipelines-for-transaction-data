"""
🎯 Goal
    Aggregate clearing financials by Merchant Category Code (MCC). One row per (mcc, merchant_country, transaction_date) containing:

    Volume and value

    Average ticket size

    Refund and chargeback rates

    Top merchant within each MCC (via window function)

    Concentration metric — how much of the MCC's volume is dominated by a single merchant


    Silver MCC daily fact table.
    Aggregates financials by mcc + merchant_country + transaction_date.
    Uses window functions to identify the top merchant within each MCC group.

"""

from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window

INGESTION_DATE = date.today().isoformat()
SILVER_PATH = "hdfs://hdfs-namenode:9000/data/clearing/silver/financials/ingestion_date=2026-10-18"
OUTPUT_BASE = "hdfs://hdfs-namenode:9000/data/clearing/silver/mcc_daily"

OUTPUT_PATH = f"{OUTPUT_BASE}/ingestion_date=2026-10-18"


def compute_top_merchant(df):
    """
    Compute the top merchant per (mcc, country, date) by gross amount.
    Returns a DataFrame with columns:
      mcc, merchant_country, transaction_date, top_merchant_name, top_merchant_gross
    Uses a window function to rank merchants within each group.
    """

    # Step 1: merchant-level gross within each MCC group
    merchant_gross = df.groupBy(
        "mcc", "merchant_country", "transaction_date", "merchant_id", "merchant_name"
    ).agg(F.sum("gross_amount").cast(DecimalType(18, 2)).alias("merchant_gross"))

    # Step 2: rank by gross descending within each MCC group
    rank_window = Window.partitionBy(
        "mcc", "merchant_country", "transaction_date"
    ).orderBy(F.desc("merchant_gross"))

    ranked = merchant_gross.withColumn("rnk", F.row_number().over(rank_window))

    # Step 3: keep only the top-ranked merchant per group
    top = ranked.filter(F.col("rnk") == 1).select(
        "mcc",
        "merchant_country",
        "transaction_date",
        F.col("merchant_name").alias("top_merchant_name"),
        F.col("merchant_gross").alias("top_merchant_gross"),
    )
    return top


def build_mcc_daily(df):
    """
    Aggregate into (mcc, mcc_description, merchant_country, transaction_date).
    Joins with top_merchant to compute concentration.
    """

    # Prepare date + flags
    df = df.withColumn("transaction_date", F.to_date(F.col("posting_date")))
    df = df.withColumn("is_refund_i", F.when(F.col("is_refund"), 1).otherwise(0))
    df = df.withColumn(
        "is_chargeback_i", F.when(F.col("is_chargeback"), 1).otherwise(0)
    )
    df = df.withColumn("is_domestic_i", F.when(F.col("is_domestic"), 1).otherwise(0))

    # Step A: main MCC aggregate
    mcc_agg = df.groupBy(
        "mcc", "mcc_description", "merchant_country", "transaction_date"
    ).agg(
        # Volume
        F.count("*").alias("txn_count"),
        F.countDistinct("merchant_id").alias("unique_merchants"),
        # Value
        F.sum("gross_amount").cast(DecimalType(18, 2)).alias("gross_amount"),
        F.sum(F.when(F.col("is_refund_i") == 0, F.col("gross_amount")).otherwise(0))
        .cast(DecimalType(18, 2))
        .alias("purchase_gross"),
        F.sum(F.when(F.col("is_refund_i") == 1, F.col("gross_amount")).otherwise(0))
        .cast(DecimalType(18, 2))
        .alias("refund_gross"),
        # Fees
        F.sum("total_fees").cast(DecimalType(18, 2)).alias("total_fees"),
        F.sum("net_settlement_amount")
        .cast(DecimalType(18, 2))
        .alias("net_settlement_amount"),
        # Disputes & refunds
        F.sum("is_chargeback_i").alias("chargeback_count"),
        F.sum("chargeback_amount").cast(DecimalType(18, 2)).alias("chargeback_amount"),
        F.sum("is_refund_i").alias("refund_count"),
        # Geography
        F.sum("is_domestic_i").alias("domestic_txn_count"),
        F.sum(1 - F.col("is_domestic_i")).alias("cross_border_txn_count"),
    )

    # Derived metrics on the aggregate
    mcc_agg = mcc_agg.withColumn(
        "avg_ticket",
        F.when(
            F.col("txn_count") > 0,
            (F.col("gross_amount") / F.col("txn_count")).cast(DecimalType(18, 2)),
        ).otherwise(F.lit(None).cast(DecimalType(18, 2))),
    )
    mcc_agg = mcc_agg.withColumn(
        "refund_rate",
        F.when(
            F.col("txn_count") > 0,
            (F.col("refund_count") / F.col("txn_count")).cast(DecimalType(8, 4)),
        ).otherwise(F.lit(None).cast(DecimalType(8, 4))),
    )
    mcc_agg = mcc_agg.withColumn(
        "chargeback_rate",
        F.when(
            F.col("txn_count") > 0,
            (F.col("chargeback_count") / F.col("txn_count")).cast(DecimalType(8, 4)),
        ).otherwise(F.lit(None).cast(DecimalType(8, 4))),
    )
    mcc_agg = mcc_agg.withColumn(
        "cross_border_ratio",
        F.when(
            F.col("txn_count") > 0,
            (F.col("cross_border_txn_count") / F.col("txn_count")).cast(
                DecimalType(8, 4)
            ),
        ).otherwise(F.lit(None).cast(DecimalType(8, 4))),
    )

    # Step B: top merchant per group
    top = compute_top_merchant(df)

    # Step C: join
    mcc_agg = mcc_agg.join(
        top,
        on=["mcc", "merchant_country", "transaction_date"],
        how="left",
    )

    # Step D: concentration metric
    mcc_agg = mcc_agg.withColumn(
        "top_merchant_share",
        F.when(
            F.col("gross_amount") > 0,
            (F.col("top_merchant_gross") / F.col("gross_amount")).cast(
                DecimalType(8, 4)
            ),
        ).otherwise(F.lit(None).cast(DecimalType(8, 4))),
    )

    return mcc_agg


def main():
    spark = (
        SparkSession.builder.appName("MCC-Daily")
        .config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:9000")
        .getOrCreate()
    )

    print(f"Reading silver: {SILVER_PATH}")
    silver = spark.read.parquet(SILVER_PATH)
    silver_cnt = silver.count()
    print(f"Silver rows: {silver_cnt:,}")

    # Source totals for reconciliation
    src = silver.agg(
        F.sum("gross_amount").alias("src_gross"),
        F.count("*").alias("src_cnt"),
    ).collect()[0]
    print(f"Source gross: {src['src_gross']}")
    print(f"Source count: {src['src_cnt']:,}")

    print("\nBuilding MCC-daily aggregates...")
    mcc_daily = build_mcc_daily(silver)
    mcc_daily.cache()
    fact_cnt = mcc_daily.count()
    print(f"Fact rows: {fact_cnt:,}")

    fact = mcc_daily.agg(
        F.sum("gross_amount").alias("agg_gross"),
        F.sum("txn_count").alias("agg_cnt"),
    ).collect()[0]

    print(f"\nReconciliation:")
    print(f"  gross: source={src['src_gross']} fact={fact['agg_gross']}")
    print(f"  count: source={src['src_cnt']} fact={fact['agg_cnt']}")

    assert src["src_cnt"] == fact["agg_cnt"], "Count mismatch!"
    assert (
        abs(float(src["src_gross"]) - float(fact["agg_gross"])) < 0.01
    ), "Gross mismatch!"
    print("✅ Reconciliation passed.")

    # Sanity checks
    print("\nTop 10 MCCs by gross amount:")
    mcc_daily.groupBy("mcc", "mcc_description").agg(
        F.sum("gross_amount").alias("total_gross"),
        F.sum("txn_count").alias("total_txns"),
        F.avg("avg_ticket").alias("overall_avg_ticket"),
        F.avg("chargeback_rate").alias("avg_cb_rate"),
    ).orderBy(F.desc("total_gross")).show(10, truncate=False)

    print("\nMost concentrated MCCs (highest top_merchant_share):")
    mcc_daily.groupBy("mcc", "mcc_description").agg(
        F.max("top_merchant_share").alias("max_concentration"),
        F.first("top_merchant_name").alias("example_top_merchant"),
    ).orderBy(F.desc("max_concentration")).show(10, truncate=False)

    print("\nSample rows:")
    mcc_daily.orderBy("mcc", "transaction_date").show(10, truncate=False)

    print(f"\nWriting to {OUTPUT_PATH}")
    mcc_daily.coalesce(2).write.mode("overwrite").parquet(OUTPUT_PATH)
    print(
        "✅ Aggregates financials by mcc + merchant_country + transaction_date complete."
    )
    spark.stop()


if __name__ == "__main__":
    main()

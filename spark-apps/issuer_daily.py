"""
Silver issuer daily fact table.
Aggregates financials by issuer_bin + issuer_country + transaction_date.


🎯 Goal
Aggregate silver financials from the issuer's perspective. One row per (issuer_bin, issuer_country, transaction_date) containing:

Transaction volume and value

Interchange earned (issuer revenue)

Chargeback rate (issuer risk)

Refund rate (issuer liability)

Domestic vs cross-border exposure


"""

from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

INGESTION_DATE = date.today().isoformat()
SILVER_PATH = "hdfs://hdfs-namenode:9000/data/clearing/silver/financials/ingestion_date=2026-10-18"
OUTPUT_BASE = "hdfs://hdfs-namenode:9000/data/clearing/silver/issuer_daily"

OUTPUT_PATH = f"{OUTPUT_BASE}/ingestion_date=2026-10-18"


def build_issuer_daily(df):
    df = df.withColumn("transaction_date", F.to_date(F.col("posting_date")))
    df = df.withColumn("is_refund_i", F.when(F.col("is_refund"), 1).otherwise(0))
    df = df.withColumn("is_chargeback_i", F.when(F.col("is_chargeback"), 1).otherwise(0))
    df = df.withColumn("is_domestic_i", F.when(F.col("is_domestic"), 1).otherwise(0))

    agg = df.groupBy("issuer_bin", "issuer_country", "transaction_date").agg(
        # Volume
        F.count("*").alias("txn_count"),
        F.countDistinct("card_number_masked").alias("unique_cards"),

        # Value
        F.sum("gross_amount").cast(DecimalType(18, 2)).alias("gross_amount"),

        # Issuer revenue
        F.sum("interchange_fee").cast(DecimalType(18, 2)).alias("interchange_earned"),
        F.sum("network_fee").cast(DecimalType(18, 2)).alias("network_fees_paid"),

        # Disputes
        F.sum("is_chargeback_i").alias("chargeback_count"),
        F.sum("chargeback_amount").cast(DecimalType(18, 2)).alias("chargeback_amount"),

        # Refunds
        F.sum("is_refund_i").alias("refund_count"),
        F.sum(
            F.when(F.col("is_refund_i") == 1, F.col("gross_amount")).otherwise(0)
        ).cast(DecimalType(18, 2)).alias("refund_amount"),

        # Geography
        F.sum("is_domestic_i").alias("domestic_txn_count"),
        F.sum(1 - F.col("is_domestic_i")).alias("cross_border_txn_count"),
    )

    # Derived
    agg = agg.withColumn(
        "net_issuer_revenue",
        (F.col("interchange_earned") - F.col("network_fees_paid"))
        .cast(DecimalType(18, 2)),
    )
    
    agg = agg.withColumn(
        "refund_rate",
        F.when(F.col("txn_count") > 0,
               (F.col("refund_count") / F.col("txn_count")).cast(DecimalType(8, 4))
        ).otherwise(F.lit(None).cast(DecimalType(8, 4))),
    )
    agg = agg.withColumn(
        "cross_border_ratio",
        F.when(F.col("txn_count") > 0,
               (F.col("cross_border_txn_count") / F.col("txn_count")).cast(DecimalType(8, 4))
        ).otherwise(F.lit(None).cast(DecimalType(8, 4))),
    )
    agg = agg.withColumn(
        "avg_ticket",
        F.when(F.col("txn_count") > 0,
               (F.col("gross_amount") / F.col("txn_count")).cast(DecimalType(18, 2))
        ).otherwise(F.lit(None).cast(DecimalType(18, 2))),
    )

    return agg

def main():
    spark = (
        SparkSession.builder.appName("Issuer-Daily")
        .config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:9000")
        .getOrCreate()
    )

    print(f"Reading silver: {SILVER_PATH}")
    silver = spark.read.parquet(SILVER_PATH)
    silver_cnt = silver.count()
    print(f"Silver rows: {silver_cnt:,}")

    src = silver.agg(
        F.sum("interchange_fee").alias("src_interchange"),
        F.sum("network_fee").alias("src_network"),
        F.count("*").alias("src_cnt"),
    ).collect()[0]
    print(f"Source interchange: {src['src_interchange']}")
    print(f"Source network_fee: {src['src_network']}")
    print(f"Source count:       {src['src_cnt']:,}")

    print("\nBuilding issuer-daily aggregates...")
    issuer_daily = build_issuer_daily(silver)
    issuer_daily.cache()
    fact_cnt = issuer_daily.count()
    print(f"Fact rows: {fact_cnt:,}")

    fact = issuer_daily.agg(
        F.sum("interchange_earned").alias("agg_interchange"),
        F.sum("network_fees_paid").alias("agg_network"),
        F.sum("txn_count").alias("agg_cnt"),
    ).collect()[0]

    print(f"\nReconciliation:")
    print(f"  interchange: source={src['src_interchange']} fact={fact['agg_interchange']}")
    print(f"  network:     source={src['src_network']} fact={fact['agg_network']}")
    print(f"  count:       source={src['src_cnt']} fact={fact['agg_cnt']}")

    assert src["src_cnt"] == fact["agg_cnt"], "Count mismatch!"
    assert abs(float(src["src_interchange"]) - float(fact["agg_interchange"])) < 0.01, "Interchange mismatch!"
    print("✅ Reconciliation passed.")
     
    print("\nTop 10 issuer BINs by interchange earned:")
    issuer_daily.groupBy("issuer_bin", "issuer_country").agg(
        F.sum("interchange_earned").alias("total_interchange"),
        F.sum("txn_count").alias("total_txns"),
    ).orderBy(F.desc("total_interchange")).show(10, truncate=False)

    print("\nSample rows:")
    issuer_daily.orderBy("issuer_bin", "transaction_date").show(10, truncate=False)

    print(f"\nWriting to {OUTPUT_PATH}")
    issuer_daily.coalesce(2).write.mode("overwrite").parquet(OUTPUT_PATH)
    print("✅ Silver issuer daily fact table aggregation complete.")
    spark.stop()

if __name__ == "__main__":
    main()

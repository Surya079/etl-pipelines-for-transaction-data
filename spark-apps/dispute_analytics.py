"""
🎯 Goal
    Split chargebacks and refunds into dedicated fact tables for dispute reporting, and build an enriched merchant-dispute summary that answers:

    Which merchants have the highest chargeback rate?

    What reason codes dominate disputes?

    How much refund exposure does each merchant carry?

    What's the net dispute exposure (chargebacks + refunds) per merchant per day?
"""

"""
Dispute analytics — chargeback and refund fact tables.
Produces 4 outputs:
  - dispute_detail           (detail grain)
  - chargeback_daily         (fact grain)
  - refund_daily             (fact grain)
  - merchant_dispute_summary (enriched merchant_daily)
"""

from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

INGESTION_DATE = date.today().isoformat()

SILVER_PATH = "hdfs://hdfs-namenode:9000/data/clearing/silver/financials/ingestion_date=2026-10-18"
MERCHANT_DAILY_PATH = "hdfs://hdfs-namenode:9000/data/clearing/silver/merchant_daily/ingestion_date=2026-10-18"

OUTPUT_BASE = "hdfs://hdfs-namenode:9000/data/clearing/silver/disputes"

DETAIL_PATH = f"{OUTPUT_BASE}/ingestion_date=2026-10-18/dispute_detail"
CHARGEBACK_DAILY_PATH = f"{OUTPUT_BASE}/ingestion_date=2026-10-18/chargeback_daily"
REFUND_DAILY_PATH = f"{OUTPUT_BASE}/ingestion_date=2026-10-18/refund_daily"
SUMMARY_PATH = f"{OUTPUT_BASE}/ingestion_date=2026-10-18/merchant_dispute_summary"


# -----------------------------------------------------------
# 1) Dispute detail — one row per dispute event
# -----------------------------------------------------------
def build_dispute_detail(silver):
    return (
        silver.filter(F.col("is_chargeback") | F.col("is_refund"))
        .withColumn(
            "dispute_type",
            F.when(F.col("is_chargeback"), F.lit("CHARGEBACK"))
            .when(F.col("is_refund"), F.lit("REFUND"))
            .otherwise(F.lit("OTHER")),
        )
        .select(
            "transaction_id",
            "merchant_id",
            "merchant_name",
            "mcc",
            "mcc_description",
            "issuer_bin",
            "card_network",
            "card_type",
            "transaction_datetime",
            "posting_date",
            "gross_amount",
            "chargeback_amount",
            "chargeback_reason_code",
            "record_status",
            "dispute_type",
            "is_chargeback",
            "is_refund",
        )
    )


# -----------------------------------------------------------
# 2) Chargeback daily — merchant × date × reason
# -----------------------------------------------------------
def build_chargeback_daily(silver):
    cb = (
        silver.filter(F.col("is_chargeback"))
        .withColumn("transaction_date", F.to_date(F.col("posting_date")))
        .withColumn(
            "chargeback_reason_code",
            F.when(
                F.col("chargeback_reason_code").isNull()
                | (F.trim(F.col("chargeback_reason_code")) == ""),
                F.lit("UNKNOWN"),
            ).otherwise(F.col("chargeback_reason_code")),
        )
    )
    return cb.groupBy(
        "merchant_id",
        "merchant_name",
        "mcc",
        "transaction_date",
        "chargeback_reason_code",
    ).agg(
        F.count("*").alias("chargeback_count"),
        F.sum("chargeback_amount").cast(DecimalType(18, 2)).alias("chargeback_amount"),
        F.avg("chargeback_amount")
        .cast(DecimalType(18, 2))
        .alias("avg_chargeback_amount"),
        F.countDistinct("card_number_masked").alias("unique_cards"),
        F.sum("gross_amount").cast(DecimalType(18, 2)).alias("original_gross_amount"),
    )


# -----------------------------------------------------------
# 3) Refund daily — merchant × date
# -----------------------------------------------------------
def build_refund_daily(silver):
    rf = silver.filter(F.col("is_refund")).withColumn(
        "transaction_date", F.to_date(F.col("posting_date"))
    )
    return rf.groupBy("merchant_id", "merchant_name", "mcc", "transaction_date").agg(
        F.count("*").alias("refund_count"),
        F.sum("gross_amount").cast(DecimalType(18, 2)).alias("refund_amount"),
        F.avg("gross_amount").cast(DecimalType(18, 2)).alias("avg_refund_amount"),
        F.countDistinct("card_number_masked").alias("unique_cards"),
    )


# -----------------------------------------------------------
# 4) Merchant dispute summary — merchant_daily enriched with disputes
# -----------------------------------------------------------
def build_merchant_dispute_summary(merchant_daily, chargeback_daily, refund_daily):
    # Re-aggregate the reason-level chargeback table down to merchant × date
    cb_agg = chargeback_daily.groupBy("merchant_id", "transaction_date").agg(
        F.sum("chargeback_count").alias("total_chargebacks"),
        F.sum("chargeback_amount")
        .cast(DecimalType(18, 2))
        .alias("total_chargeback_amount"),
    )
    rf_agg = refund_daily.groupBy("merchant_id", "transaction_date").agg(
        F.sum("refund_count").alias("total_refunds"),
        F.sum("refund_amount").cast(DecimalType(18, 2)).alias("total_refund_amount"),
    )

    base = merchant_daily.groupBy("merchant_id", "transaction_date").agg(
        F.first("merchant_name", ignorenulls=True).alias("merchant_name"),
        F.sum("txn_count").alias("txn_count"),
        F.sum("gross_amount").cast(DecimalType(18, 2)).alias("gross_amount"),
        F.sum("purchase_count").alias("purchase_count"),
        F.sum("purchase_gross").cast(DecimalType(18, 2)).alias("purchase_gross"),
    )

    # Left join so merchants with zero disputes still appear
    result = (
        base.join(cb_agg, on=["merchant_id", "transaction_date"], how="left")
        .join(rf_agg, on=["merchant_id", "transaction_date"], how="left")
        .fillna(
            0,
            subset=[
                "total_chargebacks",
                "total_chargeback_amount",
                "total_refunds",
                "total_refund_amount",
            ],
        )
    )
    # Rates (guarded against divide-by-zero)
    result = result.withColumn(
        "chargeback_rate",
        F.when(
            F.col("txn_count") > 0,
            (F.col("total_chargebacks") / F.col("txn_count")).cast(DecimalType(8, 4)),
        ).otherwise(F.lit(None).cast(DecimalType(8, 4))),
    )
    result = result.withColumn(
        "refund_rate",
        F.when(
            F.col("txn_count") > 0,
            (F.col("total_refunds") / F.col("txn_count")).cast(DecimalType(8, 4)),
        ).otherwise(F.lit(None).cast(DecimalType(8, 4))),
    )
    result = result.withColumn(
        "net_dispute_exposure",
        (F.col("total_chargeback_amount") + F.col("total_refund_amount")).cast(
            DecimalType(18, 2)
        ),
    )
    return result


# -----------------------------------------------------------
# 5) Reason-code summary — printed only, for validation
# -----------------------------------------------------------
def print_reason_summary(chargeback_daily):
    print("\nChargeback reason-code summary:")
    (
        chargeback_daily.groupBy("chargeback_reason_code")
        .agg(
            F.sum("chargeback_count").alias("total_count"),
            F.sum("chargeback_amount").cast(DecimalType(18, 2)).alias("total_amount"),
            F.avg("avg_chargeback_amount").cast(DecimalType(18, 2)).alias("avg_amount"),
        )
        .orderBy(F.desc("total_amount"))
        .show(truncate=False)
    )


def main():
    spark = (
        SparkSession.builder.appName("Dispute-Analytics")
        .config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:9000")
        .getOrCreate()
    )

    print(f"Reading silver: {SILVER_PATH}")
    silver = spark.read.parquet(SILVER_PATH)
    print(f"Silver rows: {silver.count():,}")

    # Source-of-truth totals for reconciliation
    src = silver.agg(
        F.sum(
            F.when(F.col("is_chargeback"), F.col("chargeback_amount")).otherwise(0)
        ).alias("src_cb_amount"),
        F.sum(F.when(F.col("is_chargeback"), 1).otherwise(0)).alias("src_cb_count"),
        F.sum(F.when(F.col("is_refund"), F.col("gross_amount")).otherwise(0)).alias(
            "src_rf_amount"
        ),
        F.sum(F.when(F.col("is_refund"), 1).otherwise(0)).alias("src_rf_count"),
    ).collect()[0]
    print(
        f"Source chargebacks: {src['src_cb_count']:,} rows / {src['src_cb_amount']} amount"
    )
    print(
        f"Source refunds:     {src['src_rf_count']:,} rows / {src['src_rf_amount']} amount"
    )
    # Build the four outputs
    print("\n[1/4] Building dispute_detail ...")
    detail = build_dispute_detail(silver)
    print(f"    detail rows: {detail.count():,}")

    print("[2/4] Building chargeback_daily ...")
    chargeback_daily = build_chargeback_daily(silver).cache()
    print(f"    chargeback_daily rows: {chargeback_daily.count():,}")

    print("[3/4] Building refund_daily ...")
    refund_daily = build_refund_daily(silver).cache()
    print(f"    refund_daily rows: {refund_daily.count():,}")

    print("[4/4] Building merchant_dispute_summary ...")
    merchant_daily = spark.read.parquet(MERCHANT_DAILY_PATH)
    summary = build_merchant_dispute_summary(
        merchant_daily, chargeback_daily, refund_daily
    ).cache()
    print(f"    summary rows: {summary.count():,}")

    # ---- Reconciliation ----
    print("\nReconciliation:")
    cb_check = chargeback_daily.agg(
        F.sum("chargeback_count").alias("cb_count"),
        F.sum("chargeback_amount").alias("cb_amount"),
    ).collect()[0]
    rf_check = refund_daily.agg(
        F.sum("refund_count").alias("rf_count"),
        F.sum("refund_amount").alias("rf_amount"),
    ).collect()[0]

    print(
        f"  chargeback_count:  source={src['src_cb_count']:,}  fact={cb_check['cb_count']:,}"
    )
    print(
        f"  chargeback_amount: source={src['src_cb_amount']}  fact={cb_check['cb_amount']}"
    )
    print(
        f"  refund_count:      source={src['src_rf_count']:,}  fact={rf_check['rf_count']:,}"
    )
    print(
        f"  refund_amount:     source={src['src_rf_amount']}  fact={rf_check['rf_amount']}"
    )

    assert int(src["src_cb_count"]) == int(
        cb_check["cb_count"]
    ), "Chargeback count mismatch"
    assert int(src["src_rf_count"]) == int(
        rf_check["rf_count"]
    ), "Refund count mismatch"
    assert (
        abs(float(src["src_cb_amount"]) - float(cb_check["cb_amount"])) < 0.01
    ), "Chargeback amount mismatch"
    assert (
        abs(float(src["src_rf_amount"]) - float(rf_check["rf_amount"])) < 0.01
    ), "Refund amount mismatch"
    print("  ✅ Reconciliation passed.")

    # ---- Reason-code summary ----
    print_reason_summary(chargeback_daily)

    # ---- Top merchants by dispute exposure ----
    print("\nTop 10 merchants by net_dispute_exposure:")
    (
        summary.groupBy("merchant_id", "merchant_name")
        .agg(
            F.sum("net_dispute_exposure").cast(DecimalType(18, 2)).alias("exposure"),
            F.sum("total_chargebacks").alias("chargebacks"),
            F.sum("total_refunds").alias("refunds"),
        )
        .orderBy(F.desc("exposure"))
        .show(10, truncate=False)
    )

    # ---- Write outputs ----
    print(f"\nWriting dispute_detail       -> {DETAIL_PATH}")
    detail.coalesce(2).write.mode("overwrite").parquet(DETAIL_PATH)

    print(f"Writing chargeback_daily     -> {CHARGEBACK_DAILY_PATH}")
    chargeback_daily.coalesce(2).write.mode("overwrite").parquet(CHARGEBACK_DAILY_PATH)

    print(f"Writing refund_daily         -> {REFUND_DAILY_PATH}")
    refund_daily.coalesce(2).write.mode("overwrite").parquet(REFUND_DAILY_PATH)

    print(f"Writing merchant_dispute_summary -> {SUMMARY_PATH}")
    summary.coalesce(2).write.mode("overwrite").parquet(SUMMARY_PATH)

    print(
        "\n✅ Split chargebacks and refunds into dedicated fact tables for dispute reporting, and build an enriched merchant-dispute summary complete."
    )
    spark.stop()


if __name__ == "__main__":
    main()

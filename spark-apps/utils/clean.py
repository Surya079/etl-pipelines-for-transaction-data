"""
Reusable bronze-cleaning transformations.
Each function is pure: DataFrame in, DataFrame out.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

NULL_LIKE = ["NULL", "None", "N/A", "null", "n/a", "-", ""]

ENUM_VALUES = {
    "card_network": ["VISA", "MASTERCARD", "AMERICAN EXPRESS", "DISCOVER"],
    "card_type": ["CREDIT", "DEBIT", "PREPAID"],
    "transaction_type": ["PURCHASE", "REFUND", "ATM WITHDRAWAL", "BALANCE INQUIRY"],
    "pos_entry_mode": [
        "CHIP",
        "MAGNETIC STRIPE",
        "CONTACTLESS",
        "MANUAL KEYED",
        "E-COMMERCE",
    ],
    "record_status": ["POSTED", "CHARGEBACK", "REVERSED"],
    "terminal_environment": ["ATTENDED", "UNATTENDED", "E-COMMERCE"],
    "function_code": ["200", "201", "400"],
}

VALID_CURRENCIES = {"USD", "EUR", "GBP", "INR", "CAD", "AUD", "JPY", "SGD"}

VALID_MCCS = {
    "5812",
    "5942",
    "5310",
    "5411",
    "5541",
    "4511",
    "5732",
    "5651",
    "5814",
    "5815",
    "5311",
}


def strip_control_chars(col: str) -> F.Column:
    """Remove tabs, newlines, quotes, pipes, backslashes, and zero-width chars."""
    cleaned = F.regexp_replace(F.col(col), r"[\t\n\r\"'\\|\u200b\ufeff]", "")
    # remove common mojibake sequences
    cleaned = F.regexp_replace(cleaned, r"(Ã©|Ã±|â€™|â€œ|Ã‚|Ã )", "")
    return cleaned


def clean_strings(df: DataFrame) -> DataFrame:
    """Apply null-like normalization, trimming, and control-char stripping to all string cols."""
    for field in df.schema.fields:
        if field.dataType.typeName() != "string":
            continue
        col = field.name
        df = df.withColumn(col, strip_control_chars(col))
        df = df.withColumn(col, F.trim(F.col(col)))
        df = df.withColumn(
            col,
            F.when(F.col(col).isin(NULL_LIKE), F.lit(None)).otherwise(F.col(col)),
        )
    return df


def normalize_case(df: DataFrame) -> DataFrame:
    """Upper-case code fields, Title-case human-readable fields."""
    upper_cols = [
        "card_network",
        "card_type",
        "transaction_type",
        "pos_entry_mode",
        "record_status",
        "currency",
        "cardholder_billing_currency",
        "settlement_currency",
        "mcc",
        "function_code",
        "terminal_environment",
        "merchant_country",
        "issuer_country",
    ]
    for col in upper_cols:
        if col in df.columns:
            df = df.withColumn(col, F.upper(F.col(col)))

    title_cols = [
        "merchant_name",
        "merchant_dba_name",
        "merchant_city",
        "merchant_state",
        "acquirer_bank",
        "mcc_description",
    ]
    for col in title_cols:
        if col in df.columns:
            df = df.withColumn(col, F.initcap(F.col(col)))

    return df


def cast_amounts(df: DataFrame) -> DataFrame:
    """Cast money columns to Decimal(18,2) and rates to Decimal(8,4)."""
    money_cols = [
        "gross_amount",
        "cardholder_billing_amount",
        "interchange_fee",
        "network_fee",
        "acquirer_fee",
        "net_settlement_amount",
        "chargeback_amount",
    ]
    for col in money_cols:
        if col in df.columns:
            df = df.withColumn(col, F.col(col).cast(DecimalType(18, 2)))

    rate_cols = ["merchant_discount_rate"]
    for col in rate_cols:
        if col in df.columns:
            df = df.withColumn(col, F.col(col).cast(DecimalType(8, 4)))

    if "currency_conversion_rate" in df.columns:
        df = df.withColumn(
            "currency_conversion_rate",
            F.col("currency_conversion_rate").cast(DecimalType(12, 6)),
        )

    return df


def add_validation_flags(df: DataFrame) -> DataFrame:
    """
    Add `_reject_reason` column.
    Each validation rule creates a separate column, then coalesce picks the first non-null.
    This avoids a massive nested CaseWhen expression that OOMs the driver.
    """
    reason_cols = []

    # Enum checks
    for col, allowed in ENUM_VALUES.items():
        if col in df.columns:
            cname = f"_rr_{col}"
            df = df.withColumn(
                cname,
                F.when(
                    F.col(col).isNotNull() & ~F.col(col).isin(allowed),
                    F.lit(f"invalid_{col}"),
                ),
            )
            reason_cols.append(cname)

    # Currency
    for col in ["currency", "cardholder_billing_currency", "settlement_currency"]:
        if col in df.columns:
            cname = f"_rr_{col}"
            df = df.withColumn(
                cname,
                F.when(
                    F.col(col).isNotNull() & ~F.col(col).isin(list(VALID_CURRENCIES)),
                    F.lit(f"invalid_{col}"),
                ),
            )
            reason_cols.append(cname)

    # MCC
    if "mcc" in df.columns:
        df = df.withColumn(
            "_rr_mcc_format",
            F.when(
                F.col("mcc").isNotNull() & (F.length("mcc") != 4),
                F.lit("invalid_mcc_format"),
            ),
        )
        df = df.withColumn(
            "_rr_mcc_value",
            F.when(
                F.col("mcc").isNotNull()
                & (F.length("mcc") == 4)
                & ~F.col("mcc").isin(list(VALID_MCCS)),
                F.lit("invalid_mcc_value"),
            ),
        )
        reason_cols.extend(["_rr_mcc_format", "_rr_mcc_value"])

    # BIN
    if "issuer_bin" in df.columns:
        df = df.withColumn(
            "_rr_bin",
            F.when(
                F.col("issuer_bin").isNotNull() & (F.length("issuer_bin") != 6),
                F.lit("invalid_issuer_bin"),
            ),
        )
        reason_cols.append("_rr_bin")

    # Mandatory not-null
    for col in ["clearing_file_id", "transaction_id", "clearing_date"]:
        if col in df.columns:
            cname = f"_rr_null_{col}"
            df = df.withColumn(cname, F.when(F.col(col).isNull(), F.lit(f"null_{col}")))
            reason_cols.append(cname)

    # Fee equation
    if all(
        c in df.columns
        for c in [
            "gross_amount",
            "interchange_fee",
            "network_fee",
            "acquirer_fee",
            "net_settlement_amount",
        ]
    ):
        expected = (
            F.col("gross_amount")
            - F.coalesce(F.col("interchange_fee"), F.lit(0))
            - F.coalesce(F.col("network_fee"), F.lit(0))
            - F.coalesce(F.col("acquirer_fee"), F.lit(0))
        )
        diff = F.abs(expected - F.col("net_settlement_amount"))
        df = df.withColumn(
            "_rr_fee",
            F.when(
                F.col("net_settlement_amount").isNotNull()
                & F.col("gross_amount").isNotNull()
                & (diff > 0.01),
                F.lit("fee_mismatch"),
            ),
        )
        reason_cols.append("_rr_fee")

    # Coalesce first non-null
    df = df.withColumn("_reject_reason", F.coalesce(*[F.col(c) for c in reason_cols]))

    # Drop helper columns
    df = df.drop(*reason_cols)
    return df


def add_dq_flags(df: DataFrame) -> DataFrame:
    """Add non-rejecting quality flags (informational only)."""
    flags = F.array()
    if "gross_amount" in df.columns:
        flags = F.when(
            F.col("gross_amount") < 0,
            F.array_union(flags, F.array(F.lit("negative_amount"))),
        ).otherwise(flags)
    if "transaction_datetime" in df.columns:
        flags = F.when(
            F.col("transaction_datetime") > F.current_timestamp(),
            F.array_union(flags, F.array(F.lit("future_date"))),
        ).otherwise(flags)
    return df.withColumn("data_quality_flags", flags)


def deduplicate(df: DataFrame) -> DataFrame:
    """Keep first row per transaction_id."""
    if "transaction_id" in df.columns:
        return df.dropDuplicates(["transaction_id"])
    return df

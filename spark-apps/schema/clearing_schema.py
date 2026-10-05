from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    IntegerType,
    LongType,
    DecimalType,
    BooleanType,
    TimestampType,
    DateType,
)

CLEARING_SCHEMA = StructType(
    [
        # File / batch identifiers
        StructField("clearing_file_id", StringType(), False),
        StructField("clearing_cycle", IntegerType(), False),
        StructField("clearing_date", DateType(), False),
        StructField("settlement_date", DateType(), False),
        StructField("function_code", StringType(), False),
        StructField("processing_code", StringType(), True),
        # Transaction identifiers
        StructField("transaction_id", StringType(), False),
        StructField("trace_number", StringType(), True),
        StructField("rrn", StringType(), True),
        StructField("authorization_code", StringType(), True),
        # Card
        StructField("card_number_masked", StringType(), True),
        StructField("card_network", StringType(), True),
        StructField("card_type", StringType(), True),
        StructField("issuer_bin", StringType(), True),
        StructField("issuer_country", StringType(), True),
        # Merchant
        StructField("merchant_id", StringType(), True),
        StructField("merchant_name", StringType(), True),
        StructField("merchant_dba_name", StringType(), True),
        StructField("merchant_city", StringType(), True),
        StructField("merchant_state", StringType(), True),
        StructField("merchant_country", StringType(), True),
        StructField("merchant_postal_code", StringType(), True),
        StructField("mcc", StringType(), True),
        StructField("mcc_description", StringType(), True),
        # Acquirer / network
        StructField("acquirer_bank", StringType(), True),
        StructField("acquirer_ica", StringType(), True),
        # Timestamps
        StructField("transaction_datetime", TimestampType(), True),
        StructField("local_transaction_datetime", TimestampType(), True),
        StructField("posting_date", DateType(), True),
        # Transaction metadata
        StructField("transaction_type", StringType(), True),
        StructField("pos_entry_mode", StringType(), True),
        StructField("card_present", BooleanType(), True),
        StructField("terminal_id", StringType(), True),
        # Amounts (money — always Decimal, never Float)
        StructField("gross_amount", DecimalType(18, 2), True),
        StructField("currency", StringType(), True),
        StructField("cardholder_billing_amount", DecimalType(18, 2), True),
        StructField("cardholder_billing_currency", StringType(), True),
        StructField("interchange_fee", DecimalType(18, 2), True),
        StructField("network_fee", DecimalType(18, 2), True),
        StructField("acquirer_fee", DecimalType(18, 2), True),
        StructField("merchant_discount_rate", DecimalType(8, 4), True),
        StructField("net_settlement_amount", DecimalType(18, 2), True),
        StructField("settlement_currency", StringType(), True),
        StructField("currency_conversion_rate", DecimalType(12, 6), True),
        # Security / terminal
        StructField("cvv_result", StringType(), True),
        StructField("avs_result", StringType(), True),
        StructField("pin_verified", BooleanType(), True),
        StructField("terminal_environment", StringType(), True),
        # Dispute
        StructField("chargeback_flag", BooleanType(), True),
        StructField("chargeback_reason_code", StringType(), True),
        StructField("chargeback_amount", DecimalType(18, 2), True),
        # File metadata
        StructField("file_name", StringType(), True),
        StructField("file_process_date", TimestampType(), True),
        StructField("record_status", StringType(), True),
        StructField("_corrupt_record", StringType(), True),
    ]
)

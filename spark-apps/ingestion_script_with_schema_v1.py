from datetime import date
from pyspark.sql import SparkSession

from schema.clearing_schema import CLEARING_SCHEMA
from utils.ingest import read_csv_with_schema, split_good_and_rejected

LOCAL_CSV_PATH = "file:///opt/spark-apps/data/clearing_transaction_data.csv"
HDFS_RAW_BASE = "hdfs://hdfs-namenode:9000/data/clearing/raw"
HDFS_REJECTED_BASE = "hdfs://hdfs-namenode:9000/data/clearing/rejected"
INGESTION_DATE = date.today().isoformat()

HDFS_RAW_TARGET = f"{HDFS_RAW_BASE}/ingestion_date={INGESTION_DATE}"
HDFS_REJECTED_TARGET = f"{HDFS_REJECTED_BASE}/ingestion_date={INGESTION_DATE}"


def main():

    spark = (
        SparkSession.builder.appName("Ingest-With-Schema")
        .config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:9000")
        .getOrCreate()
    )

    print(f"Reading CSV with strict schema: {LOCAL_CSV_PATH}")

    df = read_csv_with_schema(spark, LOCAL_CSV_PATH, CLEARING_SCHEMA)

    total = df.count()

    print(f"Total rows read: {total:,}")

    good, rejected = split_good_and_rejected(df)

    good_count = good.count()
    rejected_count = rejected.count()

    print(f"Good rows:     {good_count:,}")
    print(f"Rejected rows: {rejected_count:,}")

    assert good_count + rejected_count == total, "Row count mismatch!"

    print("Writing good rows to raw zone...")

    (good.write.mode("overwrite").option("header", True).csv(HDFS_RAW_TARGET))

    if rejected_count > 0:
        print(f"Writing {rejected_count:,} rejected rows to quarantine...")
        rejected.write.mode("overwrite").option("header", True).csv(
            HDFS_REJECTED_TARGET
        )

    print("Ingestion completed")
    spark.stop()


if __name__ == "__main__":
    main()

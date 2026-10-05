import os
from pyspark.sql import SparkSession
from datetime import datetime

# Config

LOCAL_CSV_PATH = "/opt/spark-apps/data/clearing_transaction_data.csv"
HDFS_RAW_BASE = "hdfs://hdfs-namenode:9000/data/clearing/raw"
INGESTION_DATE = datetime.isoformat()
HDFS_TARGET = f"{HDFS_RAW_BASE}/ingestion_date={INGESTION_DATE}"


def main():

    spark = (
        SparkSession.builder.appName("ingest-raw-data")
        .config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:9000")
        .getOrCreate()
    )

    print(f"Reading local CSV: {LOCAL_CSV_PATH}")

    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(LOCAL_CSV_PATH)
    )

    row_count = df.count()
    print(f"Rows read: {row_count:,}")
    print("Schema:")
    df.printSchema()

    print(f"Writing to HDFS: {HDFS_TARGET}")

    df.write.mode("overwrite").option("header", True).csv(HDFS_TARGET)

    print("✅ Ingestion complete.")

    spark.stop()


if __name__ == "__main__":
    main()

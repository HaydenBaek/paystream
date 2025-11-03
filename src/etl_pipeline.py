import psycopg2
from psycopg2.extras import execute_batch
from config.db_config import DB_CONFIG
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lag, unix_timestamp
from pyspark.sql.window import Window
import pandas as pd
import os
import time
from datetime import datetime

start = time.time()

def main():
    print("Starting ETL pipeline...")

    spark = SparkSession.builder.appName("PayStreamETL").getOrCreate()
    base_dir = os.path.dirname(__file__)
    input_path = os.path.join(base_dir, "..", "data", "transactions.csv")
    output_path = os.path.join(base_dir, "..", "data", "transactions_cleaned.csv")

    print("Loading raw transactions...")
    df = spark.read.option("header", "true").csv(input_path, inferSchema=True)

    df_cleaned = df.filter(
        (col("amount").isNotNull()) &
        (col("timestamp").isNotNull()) &
        (col("merchant").isNotNull()) &
        (col("city").isNotNull())
    )

    df_cleaned = df_cleaned.filter(col("amount") > 0)
    df_cleaned = df_cleaned.withColumn("high_value_flag", when(col("amount") > 250, True).otherwise(False))

    window = Window.partitionBy("city").orderBy("timestamp")
    df_cleaned = df_cleaned.withColumn("prev_time", lag("timestamp").over(window))
    df_cleaned = df_cleaned.withColumn(
        "time_diff",
        unix_timestamp(col("timestamp")) - unix_timestamp(col("prev_time"))
    )
    df_cleaned = df_cleaned.withColumn(
        "fraud_flag",
        when((col("amount") > 400) | (col("time_diff") < 60), True).otherwise(False)
    )

    print("Preview of cleaned data:")
    df_cleaned.select(
        "transaction_id", "timestamp", "merchant", "amount", "city", "fraud_flag", "high_value_flag"
    ).show(5, truncate=False)

    print("Writing cleaned transactions to CSV...")
    df_cleaned.write.mode("overwrite").option("header", "true").csv(output_path)
    print(f"Saved cleaned transactions to {output_path}")

    print("Writing cleaned transactions to PostgreSQL...")

    pandas_df = df_cleaned.toPandas()

    def safe_value(v):
        if pd.isna(v):
            return None
        if isinstance(v, pd.Timestamp):
            return v.to_pydatetime()
        if isinstance(v, (float, int, bool, str, datetime)):
            return v
        return str(v)

    records = [
        (
            safe_value(row["transaction_id"]),
            safe_value(row["timestamp"]),
            safe_value(row["merchant"]),
            safe_value(row["amount"]),
            safe_value(row["city"]),
            safe_value(row["fraud_flag"]),
            safe_value(row["high_value_flag"])
        )
        for _, row in pandas_df.iterrows()
    ]

    print("Sample record for debugging:")
    print(records[0])
    print("Value types:", [type(x) for x in records[0]])

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS transactions (
        transaction_id VARCHAR(64) PRIMARY KEY,
        timestamp TIMESTAMP,
        merchant VARCHAR(255),
        amount DOUBLE PRECISION,
        city VARCHAR(255),
        fraud_flag BOOLEAN,
        high_value_flag BOOLEAN
    )
    """)

    insert_query = """
        INSERT INTO transactions (transaction_id, timestamp, merchant, amount, city, fraud_flag, high_value_flag)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (transaction_id) DO NOTHING
    """

    execute_batch(cur, insert_query, records, page_size=500)

    conn.commit()
    cur.close()
    conn.close()

    print("Saved cleaned transactions to PostgreSQL (batch insert).")

if __name__ == "__main__":
    main()

end = time.time()
print(f"ETL completed in {end - start:.2f} seconds")

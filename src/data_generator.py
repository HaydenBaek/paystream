
from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, TimestampType
from datetime import datetime
import random
import os

fake = Faker()
spark = SparkSession.builder.appName("PayStreamDataGenerator").getOrCreate()

schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("merchant", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("city", StringType(), True),
    StructField("fraud_flag", BooleanType(), True)
])

cities = [
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
    "Toronto", "Vancouver", "Calgary", "Seattle", "San Francisco",
    "Boston", "Miami", "Dallas", "Denver", "Atlanta",
    "Ottawa", "Montreal", "Edmonton", "Las Vegas", "Portland"
]

def generate_transaction():
    amount = round(random.uniform(5, 500), 2)
    if amount > 400 and random.random() < 0.2:
        fraud_flag = True
    else:
        fraud_flag = random.random() < 0.03
    return {
        "transaction_id": fake.uuid4(),
        "timestamp": datetime.now(),
        "merchant": fake.company(),
        "amount": amount,
        "city": random.choice(cities),
        "fraud_flag": fraud_flag
    }

def main():
    print("Generating fake transactions...")
    data = [generate_transaction() for _ in range(5000)]
    df = spark.createDataFrame(data, schema=schema)
    df.show(5, truncate=False)
    output_dir = os.path.join(os.path.dirname(__file__), "..", "data")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "transactions.csv")
    df.write.mode("overwrite").option("header", "true").csv(output_path)
    print(f"Saved 5000 transactions to {output_path}")

if __name__ == "__main__":
    main()

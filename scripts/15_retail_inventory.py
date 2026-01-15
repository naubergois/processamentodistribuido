import time
import json
import random
import multiprocessing
import os
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import sqlalchemy

# --- Configurations ---
KAFKA_TOPIC = "pos-sales"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
DB_PATH = "data/retail.db"
PARQUET_PATH = "data/retail_inventory.parquet"

# --- Producer ---
def run_producer():
    print("Starting POS Producer...")
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    products = [f'PROD-{i:03d}' for i in range(1, 21)]
    
    try:
        while True:
            # Simulate Checkout
            cart_size = random.randint(1, 5)
            for _ in range(cart_size):
                data = {
                    'store_id': 'STORE-001',
                    'product_id': random.choice(products),
                    'quantity': random.randint(1, 3),
                    'timestamp': time.time()
                }
                producer.send(KAFKA_TOPIC, value=data)
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        producer.close()

# --- Processor ---
def process_batch(df, epoch_id):
    pdf = df.toPandas()
    if not pdf.empty:
        engine = sqlalchemy.create_engine(f'sqlite:///{DB_PATH}')
        
        # In a real app, we would READ current inventory then DECREMENT.
        # Here we just log the cumulative sales (sold_count) per batch.
        # pdf contains: product_id, sum(quantity)
        
        pdf.to_sql('sales_log', engine, if_exists='append', index=False)
        
        # Check high velocity items
        hot_items = pdf[pdf['sold_quantity'] > 10]
        if not hot_items.empty:
            print(f"RESTOCK ALERT: Items selling fast: {hot_items['product_id'].tolist()}")

def run_consumer():
    spark = SparkSession.builder \
        .appName("InventoryMonitor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("store_id", StringType()),
        StructField("product_id", StringType()),
        StructField("quantity", IntegerType()),
        StructField("timestamp", FloatType())
    ])

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .load()

    sales = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # Aggregation (Sales per product in this trigger)
    sales_counts = sales.groupBy("product_id").agg(_sum("quantity").alias("sold_quantity"))

    # Output 1: Update Mode to Console/DB
    query_db = sales_counts.writeStream \
        .outputMode("update") \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "logs/checkpoint_retail_db") \
        .start()
        
    # Output 2: Raw Archive
    query_parquet = sales.writeStream \
        .format("parquet") \
        .option("path", PARQUET_PATH) \
        .option("checkpointLocation", "logs/checkpoint_retail_raw") \
        .outputMode("append") \
        .start()

    query_db.awaitTermination()
    query_parquet.awaitTermination()

if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
    os.makedirs("logs", exist_ok=True)
    
    p = multiprocessing.Process(target=run_producer)
    p.start()
    
    try:
        run_consumer()
    except KeyboardInterrupt:
        pass
    finally:
        p.terminate()

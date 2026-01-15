import time
import json
import random
import multiprocessing
import os
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, when
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import sqlalchemy

# --- Configurations ---
KAFKA_TOPIC = "ride-requests"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
DB_PATH = "data/ridesharing.db"
PARQUET_PATH = "data/ride_pricing.parquet"

# --- Producer ---
def run_producer():
    print("Starting Ride Producer...")
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    regions = ['Downtown', 'Airport', 'Suburb', 'Beach']
    
    try:
        while True:
            # 50% Airport
            region = random.choices(regions, weights=[20, 50, 20, 10])[0]
            data = {
                'region': region,
                'user_id': random.randint(1000, 9999),
                'timestamp': time.time()
            }
            producer.send(KAFKA_TOPIC, value=data)
            time.sleep(0.05) # High throughput
    except KeyboardInterrupt:
        pass
    finally:
        producer.close()

# --- Processor ---
def process_batch(df, epoch_id):
    pdf = df.toPandas()
    if not pdf.empty:
        engine = sqlalchemy.create_engine(f'sqlite:///{DB_PATH}')
        
        # Flatten window
        pdf['window_end'] = pdf['window'].apply(lambda x: x['end'])
        pdf_save = pdf.drop(columns=['window'])
        
        pdf_save.to_sql('dynamic_pricing', engine, if_exists='append', index=False)
        
        # Log Surge
        surge = pdf_save[pdf_save['multiplier'] > 1.0]
        if not surge.empty:
            print("--- SURGE PRICING ACTIVE ---")
            print(surge[['region', 'multiplier', 'count']])

def run_consumer():
    spark = SparkSession.builder \
        .appName("RideSurgePricing") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("region", StringType()),
        StructField("timestamp", FloatType())
    ])

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .load()

    requests = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    
    # Needs timestamp type
    requests = requests.withColumn("timestamp", col("timestamp").cast("timestamp"))

    # Aggregation (10s window)
    demand = requests \
        .withWatermark("timestamp", "10 seconds") \
        .groupBy(
            window(col("timestamp"), "10 seconds"),
            col("region")
        ) \
        .count()

    # Pricing Logic
    pricing = demand.withColumn("multiplier", 
        when(col("count") > 50, 2.0)
        .when(col("count") > 20, 1.5)
        .otherwise(1.0)
    )

    query = pricing.writeStream \
        .outputMode("update") \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "logs/checkpoint_rides") \
        .start()
        
    # Raw Archive
    query_raw = requests.writeStream \
        .format("parquet") \
        .option("path", PARQUET_PATH) \
        .option("checkpointLocation", "logs/checkpoint_rides_raw") \
        .outputMode("append") \
        .start()

    query.awaitTermination()
    query_raw.awaitTermination()

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

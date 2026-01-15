import time
import json
import random
import threading
import multiprocessing
import os
from datetime import datetime
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# --- Configurations ---
KAFKA_TOPIC = "flight-telemetry"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
DB_PATH = "data/aviation.db"
PARQUET_PATH = "data/aviation_telemetry.parquet"

# --- Producer (Simulator) ---
def run_producer():
    print("Starting Producer...")
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    flights = ['AA-101', 'BA-202', 'DA-303', 'KL-404', 'AF-505']
    
    try:
        while True:
            for flight in flights:
                data = {
                    'timestamp': datetime.now().isoformat(),
                    'flight_id': flight,
                    'altitude': random.randint(25000, 42000),
                    'speed': random.randint(700, 950),
                    'engine_temp': random.randint(400, 950),
                    'fuel_level': random.randint(10, 100)
                }
                producer.send(KAFKA_TOPIC, value=data)
            time.sleep(1) 
    except KeyboardInterrupt:
        pass
    finally:
        producer.close()

# --- Consumer & Processor (Spark) ---
def process_batch(df, epoch_id):
    # Persist to SQLite (for dashboards/alerts)
    # Using pandas for small batch writing to SQLite is often simpler than JDBC for quick demos
    pdf = df.toPandas()
    if not pdf.empty:
        import sqlalchemy
        engine = sqlalchemy.create_engine(f'sqlite:///{DB_PATH}')
        pdf.to_sql('telemetry_live', engine, if_exists='append', index=False)
        print(f"Batch {epoch_id}: Processed {len(pdf)} records. Saved to SQLite.")
        
        # Check for critical alerts
        critical = pdf[pdf['engine_temp'] > 900]
        if not critical.empty:
            print(f"CRITICAL ALERT: High Engine Temp detected for {critical['flight_id'].unique()}")

def run_consumer():
    print("Starting Spark Consumer...")
    spark = SparkSession.builder \
        .appName("AviationTelemetry") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("timestamp", TimestampType()),
        StructField("flight_id", StringType()),
        StructField("altitude", IntegerType()),
        StructField("speed", IntegerType()),
        StructField("engine_temp", IntegerType()),
        StructField("fuel_level", IntegerType())
    ])

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # Enrichment: Add 'Status' column
    enriched_df = parsed_df.withColumn("status", 
        when(col("engine_temp") > 900, "CRITICAL")
        .when(col("fuel_level") < 20, "LOW_FUEL")
        .otherwise("NORMAL")
    )

    # Output 1: Write raw data to Parquet (Historical Storage)
    # Note: Append mode allows adding new files
    query_parquet = enriched_df.writeStream \
        .format("parquet") \
        .option("path", PARQUET_PATH) \
        .option("checkpointLocation", "logs/checkpoint_aviation_parquet") \
        .outputMode("append") \
        .start()

    # Output 2: Process batch (Write to SQLite + Alerts)
    query_sqlite = enriched_df.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "logs/checkpoint_aviation_sqlite") \
        .start()

    query_parquet.awaitTermination()
    query_sqlite.awaitTermination()

if __name__ == "__main__":
    # Ensure directories exist
    os.makedirs("data", exist_ok=True)
    os.makedirs("logs", exist_ok=True)
    
    # Run Producer in background process
    p = multiprocessing.Process(target=run_producer)
    p.start()
    
    try:
        # Run Consumer in main thread
        run_consumer()
    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        p.terminate()

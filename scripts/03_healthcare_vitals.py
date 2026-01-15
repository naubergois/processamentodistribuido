import time
import json
import random
import threading
import multiprocessing
import os
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, window
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, TimestampType
import sqlalchemy

# --- Configurations ---
KAFKA_TOPIC = "vital-signs"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
DB_PATH = "data/healthcare.db"
PARQUET_PATH = "data/healthcare_vitals.parquet"

# --- Producer ---
def run_producer():
    print("Starting Medical Sensor Producer...")
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    patient_ids = [101, 102, 103, 104, 105]
    
    try:
        while True:
            for pid in patient_ids:
                data = {
                    'patient_id': pid,
                    'timestamp': time.time(),
                    'heart_rate': random.randint(50, 150),
                    'systolic_bp': random.randint(90, 180),
                    'diastolic_bp': random.randint(50, 120),
                    'spo2': random.randint(80, 100)
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
        # SQLite storage
        engine = sqlalchemy.create_engine(f'sqlite:///{DB_PATH}')
        pdf.to_sql('vitals_log', engine, if_exists='append', index=False)
        
        # Check alerts logic in Python (often easier for complex custom alerting than SQL)
        critical_patients = pdf[pdf['status'] == 'CRITICAL']
        if not critical_patients.empty:
            print(f"ALERT: Critical status for patients: {critical_patients['patient_id'].tolist()}")

def run_consumer():
    spark = SparkSession.builder \
        .appName("HealthcareMonitor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("patient_id", IntegerType()),
        StructField("timestamp", FloatType()),
        StructField("heart_rate", IntegerType()),
        StructField("systolic_bp", IntegerType()),
        StructField("diastolic_bp", IntegerType()),
        StructField("spo2", IntegerType())
    ])

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .load()

    vitals = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    
    # Enrichment: classify status
    enriched = vitals.withColumn("status", 
        when((col("spo2") < 90) | (col("heart_rate") > 130), "CRITICAL")
        .when((col("systolic_bp") > 160), "WARNING")
        .otherwise("NORMAL")
    )

    # Output 1: Parquet Archive
    query_parquet = enriched.writeStream \
        .format("parquet") \
        .option("path", PARQUET_PATH) \
        .option("checkpointLocation", "logs/checkpoint_health_parquet") \
        .outputMode("append") \
        .start()

    # Output 2: Database & Alerts
    query_db = enriched.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "logs/checkpoint_health_db") \
        .start()

    query_parquet.awaitTermination()
    query_db.awaitTermination()

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

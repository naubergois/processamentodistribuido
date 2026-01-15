import time
import json
import random
import multiprocessing
import os
from datetime import datetime
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import sqlalchemy

# --- Configurations ---
KAFKA_TOPIC = "network-logs"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
DB_PATH = "data/security.db"
PARQUET_PATH = "data/network_logs.parquet"

# --- Producer ---
def run_producer():
    print("Starting IDS Producer...")
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    ips = [f'192.168.1.{i}' for i in range(1, 101)]
    attacker_ip = '10.0.0.666'
    
    try:
        while True:
            # Normal Traffic
            data = {'source_ip': random.choice(ips), 'port': 80, 'timestamp': datetime.now().isoformat()}
            producer.send(KAFKA_TOPIC, value=data)
            
            # Attack Traffic (Flood)
            if random.random() < 0.2:
                for _ in range(20):
                    data = {'source_ip': attacker_ip, 'port': 80, 'timestamp': datetime.now().isoformat()}
                    producer.send(KAFKA_TOPIC, value=data)
            time.sleep(0.05)
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
        if 'window' in pdf.columns:
            pdf['window_start'] = pdf['window'].apply(lambda x: x['start'])
            pdf.drop(columns=['window'], inplace=True)
            
        pdf.to_sql('ids_alerts', engine, if_exists='append', index=False)
        
        # Log Alert
        print(f"IDS ALERT: Detected suspicious activity from {pdf['source_ip'].unique()}")

def run_consumer():
    spark = SparkSession.builder \
        .appName("IntrusionDetection") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("source_ip", StringType()),
        StructField("port", StringType()),
        StructField("timestamp", TimestampType())
    ])

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .load()

    logs = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # Aggregation
    traffic_stats = logs \
        .withWatermark("timestamp", "10 seconds") \
        .groupBy(
            window(col("timestamp"), "5 seconds"),
            col("source_ip")
        ) \
        .count()

    # Filter High Rate
    alerts = traffic_stats.filter(col("count") > 50)

    query = alerts.writeStream \
        .outputMode("update") \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "logs/checkpoint_ids") \
        .start()
        
    # Raw log archive
    query_raw = logs.writeStream \
        .format("parquet") \
        .option("path", PARQUET_PATH) \
        .option("checkpointLocation", "logs/checkpoint_ids_raw") \
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

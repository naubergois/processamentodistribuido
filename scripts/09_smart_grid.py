import time
import json
import random
import multiprocessing
import os
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import sqlalchemy

# --- Configurations ---
KAFKA_TOPIC = "energy-grid"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
DB_PATH = "data/energy.db"
PARQUET_PATH = "data/energy_grid.parquet"

# --- Producer ---
def run_producer():
    print("Starting Energy Producer...")
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    transformers = [f'TR-{i}' for i in range(1, 21)]
    
    try:
        while True:
            for tr in transformers:
                # 2% chance of anomaly
                if random.random() < 0.02:
                    voltage = random.choice([random.randint(85, 109), random.randint(241, 265)])
                else:
                    voltage = random.randint(110, 240)
                    
                data = {'transformer_id': tr, 'voltage': voltage, 'load': random.randint(50, 90), 'timestamp': time.time()}
                producer.send(KAFKA_TOPIC, value=data)
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        producer.close()

# --- Processor ---
def process_batch(df, epoch_id):
    pdf = df.toPandas()
    if not pdf.empty:
        # SQLite
        engine = sqlalchemy.create_engine(f'sqlite:///{DB_PATH}')
        pdf.to_sql('grid_measurements', engine, if_exists='append', index=False)
        
        # Alerts
        anomalies = pdf[pdf['status'] == 'ANOMALY']
        if not anomalies.empty:
            print(f"GRID ALERT: Voltage anomalies at {anomalies['transformer_id'].tolist()}")

def run_consumer():
    spark = SparkSession.builder \
        .appName("SmartGrid") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("transformer_id", StringType()),
        StructField("voltage", IntegerType()),
        StructField("load", IntegerType()),
        StructField("timestamp", FloatType())
    ])

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .load()

    grid = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    
    # Enrichment
    enriched = grid.withColumn("status", 
        when((col("voltage") > 240) | (col("voltage") < 110), "ANOMALY")
        .otherwise("NORMAL")
    )

    # Output 1: SQLite/Alerts
    query_db = enriched.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "logs/checkpoint_energy_db") \
        .start()
        
    # Output 2: Parquet Archive
    query_parquet = enriched.writeStream \
        .format("parquet") \
        .option("path", PARQUET_PATH) \
        .option("checkpointLocation", "logs/checkpoint_energy_parquet") \
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

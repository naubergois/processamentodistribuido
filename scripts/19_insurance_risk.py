import time
import json
import random
import multiprocessing
import os
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
import sqlalchemy

# --- Configurations ---
KAFKA_TOPIC = "insurance-claims"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
DB_PATH = "data/insurance.db"
PARQUET_PATH = "data/claim_risk.parquet"

# --- Producer ---
def run_producer():
    print("Starting Claims Producer...")
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    try:
        while True:
            data = {
                'claim_id': random.randint(100000, 999999),
                'policy_age': random.randint(0, 15),
                'amount': random.randint(500, 80000),
                'timestamp': time.time()
            }
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
        engine = sqlalchemy.create_engine(f'sqlite:///{DB_PATH}')
        pdf.to_sql('claims_processed', engine, if_exists='append', index=False)
        
        # Risk Alert
        high_risk = pdf[pdf['risk'] == 'HIGH']
        if not high_risk.empty:
            print("--- HIGH RISK CLAIM DETECTED ---")
            print(high_risk)

def run_consumer():
    spark = SparkSession.builder \
        .appName("InsuranceRisk") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("claim_id", IntegerType()),
        StructField("policy_age", IntegerType()),
        StructField("amount", IntegerType()),
        StructField("timestamp", FloatType())
    ])

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .load()

    claims = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # Risk Logic
    enriched = claims.withColumn("risk", 
        when((col("amount") > 50000) & (col("policy_age") < 1), "HIGH")
        .when(col("amount") > 70000, "MEDIUM")
        .otherwise("LOW")
    )

    query = enriched.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "logs/checkpoint_insurance") \
        .start()
        
    query_raw = claims.writeStream \
        .format("parquet") \
        .option("path", PARQUET_PATH) \
        .option("checkpointLocation", "logs/checkpoint_insurance_raw") \
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

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
KAFKA_TOPIC = "player-scores"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
DB_PATH = "data/gaming.db"
PARQUET_PATH = "data/gaming_scores.parquet"

# --- Producer ---
def run_producer():
    print("Starting Game Event Producer...")
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    players = [f'Player_{i}' for i in range(1, 21)]
    
    try:
        while True:
            data = {
                'player_id': random.choice(players),
                'score': random.choice([10, 50, 100, 500]),
                'timestamp': time.time()
            }
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
        # Sort for leaderboard
        leaderboard = pdf.sort_values(by='total_score', ascending=False).head(5)
        
        # Save snapshot
        leaderboard.to_sql(f'leaderboard_snapshot', engine, if_exists='replace', index=False)
        
        print(f"--- LEADERBOARD UPDATE (Batch {epoch_id}) ---")
        print(leaderboard)

def run_consumer():
    spark = SparkSession.builder \
        .appName("GamingLeaderboard") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("player_id", StringType()),
        StructField("score", IntegerType()),
        StructField("timestamp", FloatType())
    ])

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .load()

    scores = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # Cumulative Sum (Stateful) -> OutputMode "Complete"
    leaderboard = scores.groupBy("player_id").agg(_sum("score").alias("total_score"))

    # Output 1: Console/DB (Complete Mode)
    query_db = leaderboard.writeStream \
        .outputMode("complete") \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "logs/checkpoint_gaming_db") \
        .start()
        
    # Output 2: Raw Archive (Append Mode)
    # Note: We can't archive the 'leaderboard' aggregate in append mode easily without watermark.
    # So we archive the raw stream.
    query_parquet = scores.writeStream \
        .format("parquet") \
        .option("path", PARQUET_PATH) \
        .option("checkpointLocation", "logs/checkpoint_gaming_raw") \
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

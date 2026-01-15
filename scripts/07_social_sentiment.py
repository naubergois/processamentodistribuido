import time
import json
import random
import multiprocessing
import os
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from textblob import TextBlob
import sqlalchemy

# --- Configurations ---
KAFKA_TOPIC = "social-tweets"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
DB_PATH = "data/social.db"
PARQUET_PATH = "data/social_sentiment.parquet"

# --- Producer ---
def run_producer():
    print("Starting Social Media Producer...")
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    subjects = ["Product X", "Service Y", "Brand Z"]
    comments = [
        "I love this!", "Amazing experience", "Best purchase ever",
        "Terrible service", "I hate waiting", "Worst experience",
        "It is okay", "Not bad", "Could be better"
    ]
    
    try:
        while True:
            text = f"{random.choice(comments)} - {random.choice(subjects)}"
            data = {'text': text, 'timestamp': time.time()}
            producer.send(KAFKA_TOPIC, value=data)
            time.sleep(0.3)
    except KeyboardInterrupt:
        pass
    finally:
        producer.close()

# --- Processor ---
def analyze_sentiment(text):
    try:
        analysis = TextBlob(text)
        if analysis.sentiment.polarity > 0.1: return "POSITIVE"
        if analysis.sentiment.polarity < -0.1: return "NEGATIVE"
        return "NEUTRAL"
    except:
        return "NEUTRAL"

def process_batch(df, epoch_id):
    pdf = df.toPandas()
    if not pdf.empty:
        engine = sqlalchemy.create_engine(f'sqlite:///{DB_PATH}')
        pdf.to_sql('tweets_sentiment', engine, if_exists='append', index=False)
        
        # Monitor Negative Spikes
        negatives = pdf[pdf['sentiment'] == 'NEGATIVE']
        if len(negatives) > 5:
            print(f"ALERT: High volume of negative sentiment detected! ({len(negatives)} tweets)")

def run_consumer():
    spark = SparkSession.builder \
        .appName("SocialSentiment") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("text", StringType()),
        StructField("timestamp", FloatType())
    ])

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .load()

    tweets = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # UDF
    sentiment_udf = udf(analyze_sentiment, StringType())
    results = tweets.withColumn("sentiment", sentiment_udf(col("text")))

    # Output 1: SQLite with Alerts
    query_db = results.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "logs/checkpoint_social_db") \
        .start()
        
    # Output 2: Parquet Archive
    query_parquet = results.writeStream \
        .format("parquet") \
        .option("path", PARQUET_PATH) \
        .option("checkpointLocation", "logs/checkpoint_social_parquet") \
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

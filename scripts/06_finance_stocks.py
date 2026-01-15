import time
import json
import random
import multiprocessing
import os
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
import sqlalchemy

KAFKA_TOPIC = "stock-prices"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
DB_PATH = "data/finance.db"
PARQUET_PATH = "data/finance_stocks.parquet"

def run_producer():
    print("Starting Stock Producer...")
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    stocks = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']
    
    try:
        while True:
            for stock in stocks:
                price = random.uniform(100, 200)
                # Volatility
                if random.random() < 0.1:
                    price = price * 1.1 # Jump
                
                data = {
                    'symbol': stock,
                    'price': price,
                    'timestamp': time.time()
                }
                producer.send(KAFKA_TOPIC, value=data)
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        producer.close()

def process_batch(df, epoch_id):
    pdf = df.toPandas()
    if not pdf.empty:
        engine = sqlalchemy.create_engine(f'sqlite:///{DB_PATH}')
        # Flatten the window struct for SQL
        pdf['window_start'] = pdf['window'].apply(lambda x: x['start'])
        pdf['window_end'] = pdf['window'].apply(lambda x: x['end'])
        pdf_save = pdf.drop(columns=['window'])
        
        pdf_save.to_sql('stock_averages', engine, if_exists='append', index=False)
        print(f"Batch {epoch_id}: Saved {len(pdf)} window aggregations.")

def run_consumer():
    spark = SparkSession.builder \
        .appName("StockAnalysis") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("symbol", StringType()),
        StructField("price", FloatType()),
        StructField("timestamp", FloatType())
    ])

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .load()

    stocks = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    
    # Convert timestamp float to timestamp type for windowing
    stocks_ts = stocks.withColumn("timestamp", col("timestamp").cast("timestamp"))

    # Windowed Aggregation (10s window, sliding 5s)
    windowed_avg = stocks_ts \
        .groupBy(
            window(col("timestamp"), "10 seconds", "5 seconds"),
            col("symbol")
        ) \
        .agg(avg("price").alias("average_price"))

    # Output 1: Console for debug
    # Output 2: SQLite for persistence (Aggregation results)
    query = windowed_avg.writeStream \
        .outputMode("update") \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "logs/checkpoint_finance") \
        .start()
        
    # Output 3: Raw Archive
    # Note: Can't write 'update' mode to parquet directly usually, so we archive raw data separately if needed.
    # We will archive the raw stream.
    query_raw = stocks_ts.writeStream \
        .format("parquet") \
        .option("path", PARQUET_PATH) \
        .option("checkpointLocation", "logs/checkpoint_finance_raw") \
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

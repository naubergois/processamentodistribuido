import time
import json
import random
import multiprocessing
import os
import shutil
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
import sqlalchemy

# --- Configurations ---
KAFKA_TOPIC = "sales-transactions"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
DB_PATH = "data/sales.db"
PARQUET_PATH = "data/sales_fraud.parquet"

# --- Producer ---
def run_producer():
    print("Starting Sales Producer...")
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    try:
        while True:
            # Simulate mixing normal and Fraud
            is_fraud = random.random() > 0.95
            
            if is_fraud:
                # High amount or far distance
                amount = random.uniform(2000, 10000)
                distance = random.uniform(100, 1000)
            else:
                amount = random.uniform(10, 500)
                distance = random.uniform(0, 20)
                
            data = {
                'transaction_id': f"TXN-{random.randint(100000, 999999)}",
                'amount': amount,
                'distance_km': distance,
                'timestamp': time.time()
            }
            producer.send(KAFKA_TOPIC, value=data)
            time.sleep(0.2)
    except KeyboardInterrupt:
        pass
    finally:
        producer.close()

# --- Processor ---
def run_consumer():
    spark = SparkSession.builder \
        .appName("SalesFraud") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # 1. Train Model (Offline Phase simulation)
    print("Training initial K-Means model...")
    training_data = []
    for _ in range(500):
        # Normal data
        training_data.append((random.uniform(10, 500), random.uniform(0, 20)))
    
    # Create DF
    train_df = spark.createDataFrame(training_data, ["amount", "distance_km"])
    assembler = VectorAssembler(inputCols=["amount", "distance_km"], outputCol="features")
    train_vec = assembler.transform(train_df)
    
    kmeans = KMeans(k=2, seed=1)
    model = kmeans.fit(train_vec)
    print("Model trained. Cluster Centers:", model.clusterCenters())

    # 2. Streaming Inference
    schema = StructType([
        StructField("transaction_id", StringType()),
        StructField("amount", DoubleType()),
        StructField("distance_km", DoubleType()),
        StructField("timestamp", DoubleType())
    ])

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .load()

    txns = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # Feature Engineering for Stream
    # Note: VectorAssembler isn't a stateful transformer, so it works in streaming directly
    # We need to assemble features for the model
    vec_assembler_stream = VectorAssembler(inputCols=["amount", "distance_km"], outputCol="features")
    
    # We can't use 'transform' of the pipeline model directly easily on streaming DF in some versions without saving/loading
    # BUT KMeansModel supports transform on streaming dataframes.
    
    # Apply transformation
    txns_vec = vec_assembler_stream.transform(txns)
    predictions = model.transform(txns_vec)

    # 3. Sink
    def process_batch(batch_df, epoch_id):
        # Rename prediction to cluster
        final_df = batch_df.select("transaction_id", "amount", "distance_km", "prediction")
        
        pdf = final_df.toPandas()
        if not pdf.empty:
            # Simple heuristic: Assume cluster 1 is outlier if it has higher average values (simplified)
            # Or just save raw cluster
            
            engine = sqlalchemy.create_engine(f'sqlite:///{DB_PATH}')
            pdf.to_sql('fraud_predictions', engine, if_exists='append', index=False)
            
            # Simple Console Alert
            # In a real scenario we'd know which cluster index corresponds to 'fraud' based on centers analysis
            # Let's assume prediction=1 is the 'smaller' cluster (outlier) usually if k=2 and data is imbalanced, 
            # but k-means indices are random. Let's just print.
            print(f"Batch processed. {len(pdf)} records.")

    query = predictions.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "logs/checkpoint_sales") \
        .start()

    query.awaitTermination()

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

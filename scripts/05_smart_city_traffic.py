import time
import random
import multiprocessing
import os
import shutil
import pandas as pd
import numpy as np
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster
import sqlalchemy

# --- Configurations ---
DATA_INGEST_DIR = "data/traffic_ingest"
DB_PATH = "data/smart_city.db"
PARQUET_PATH = "data/traffic_optimization.parquet"

# --- Producer ---
def run_producer():
    print("Starting Traffic Sensor Producer...")
    if os.path.exists(DATA_INGEST_DIR):
        shutil.rmtree(DATA_INGEST_DIR)
    os.makedirs(DATA_INGEST_DIR, exist_ok=True)
    
    intersections = [f'INT-{i:03d}' for i in range(50)]
    file_count = 0
    
    try:
        while True:
            # Generate 1 minute of data for all intersections
            df = pd.DataFrame({
                'intersection_id': np.random.choice(intersections, 500),
                'timestamp': pd.Timestamp.now(),
                'vehicle_count': np.random.poisson(20, 500),
                'avg_speed': np.random.normal(40, 10, 500)
            })
            
            # Simulate congestion
            df.loc[df['vehicle_count'] > 30, 'avg_speed'] = df['avg_speed'] * 0.4
            
            filename = f"{DATA_INGEST_DIR}/batch_{file_count}.csv"
            df.to_csv(filename, index=False)
            print(f"Produced {filename}")
            
            file_count += 1
            if file_count > 50: 
                try:
                    os.remove(f"{DATA_INGEST_DIR}/batch_{file_count-50}.csv")
                except: pass
            
            time.sleep(3)
    except KeyboardInterrupt:
        pass

# --- Consumer ---
def run_consumer():
    print("Starting Dask Consumer...")
    cluster = LocalCluster(n_workers=2, threads_per_worker=1)
    client = Client(cluster)
    
    processed_files = set()
    engine = sqlalchemy.create_engine(f'sqlite:///{DB_PATH}')
    
    def optimize_signal(row):
        # Business Logic
        if row['vehicle_count'] > 35: return "EXTEND_GREEN_30S"
        if row['vehicle_count'] > 25: return "EXTEND_GREEN_15S"
        return "NORMAL_CYCLE"
    
    try:
        while True:
            all_files = set([f for f in os.listdir(DATA_INGEST_DIR) if f.endswith(".csv")])
            new_files = list(all_files - processed_files)
            
            if new_files:
                file_paths = [os.path.join(DATA_INGEST_DIR, f) for f in new_files]
                ddf = dd.read_csv(file_paths)
                
                # Aggregations
                stats = ddf.groupby('intersection_id').agg({
                    'vehicle_count': 'mean',
                    'avg_speed': 'mean'
                })
                
                # Compute
                result = stats.compute()
                
                # Apply optimization logic (Pandas level after compute is strictly easier/faster for small result sets)
                result['action'] = result.apply(optimize_signal, axis=1)
                
                print("--- Traffic Optimization Actions ---")
                print(result.head())
                
                # Ensure types for Parquet
                for col_name in ddf.select_dtypes(include=['object', 'string']).columns:
                    ddf[col_name] = ddf[col_name].astype("string[pyarrow]")

                # Persist
                result.to_sql('traffic_signals', engine, if_exists='append')
                ddf.to_parquet(PARQUET_PATH, engine='pyarrow', append=True, ignore_divisions=True)
                
                processed_files.update(new_files)
            
            time.sleep(5)
    except KeyboardInterrupt:
        pass
    finally:
        client.close()
        cluster.close()

if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
    
    p = multiprocessing.Process(target=run_producer)
    p.start()
    
    # Wait for data
    time.sleep(3)
    
    try:
        run_consumer()
    except KeyboardInterrupt:
        pass
    finally:
        p.terminate()

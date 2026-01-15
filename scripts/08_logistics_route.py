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
DATA_INGEST_DIR = "data/logistics_ingest"
DB_PATH = "data/logistics.db"
PARQUET_PATH = "data/logistics_routes.parquet"

# --- Producer ---
def run_producer():
    print("Starting Logistics Producer...")
    if os.path.exists(DATA_INGEST_DIR):
        shutil.rmtree(DATA_INGEST_DIR)
    os.makedirs(DATA_INGEST_DIR, exist_ok=True)
    
    file_count = 0
    try:
        while True:
            df = pd.DataFrame({
                'route_id': np.random.choice(['R1', 'R2', 'R3', 'R4', 'R5'], 200),
                'truck_id': np.random.randint(100, 200, 200),
                'timestamp': pd.Timestamp.now(),
                'distance_km': np.random.normal(50, 10, 200),
                'duration_min': np.random.normal(60, 15, 200),
                'fuel_consumed_l': np.random.normal(8, 2, 200)
            })
            
            # Inefficiency Logic (Route 3 is slow)
            mask = df['route_id'] == 'R3'
            df.loc[mask, 'duration_min'] += 20
            
            filename = f"{DATA_INGEST_DIR}/log_{file_count}.csv"
            df.to_csv(filename, index=False)
            print(f"Produced {filename}")
            
            file_count += 1
            if file_count > 50: os.remove(f"{DATA_INGEST_DIR}/log_{file_count-50}.csv")
            
            time.sleep(4)
    except KeyboardInterrupt:
        pass

# --- Consumer ---
def run_consumer():
    print("Starting Dask Consumer...")
    cluster = LocalCluster(n_workers=2, threads_per_worker=1)
    client = Client(cluster)
    
    processed_files = set()
    engine = sqlalchemy.create_engine(f'sqlite:///{DB_PATH}')
    
    try:
        while True:
            all_files = set([f for f in os.listdir(DATA_INGEST_DIR) if f.endswith(".csv")])
            new_files = list(all_files - processed_files)
            
            if new_files:
                file_paths = [os.path.join(DATA_INGEST_DIR, f) for f in new_files]
                ddf = dd.read_csv(file_paths)
                
                # Metric: Liters per 100km
                ddf['fuel_efficiency'] = (ddf['fuel_consumed_l'] / ddf['distance_km']) * 100
                
                # Aggregations
                stats = ddf.groupby('route_id')[['duration_min', 'fuel_efficiency']].mean()
                
                result = stats.compute()
                print("--- Route Performance ---")
                print(result)
                
                # Save to DB
                result.to_sql('route_stats', engine, if_exists='append')
                
                # Ensure types for Parquet
                for col_name in ddf.select_dtypes(include=['object', 'string']).columns:
                    ddf[col_name] = ddf[col_name].astype("string[pyarrow]")

                # Save Archive
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
    
    time.sleep(2)
    
    try:
        run_consumer()
    except KeyboardInterrupt:
        pass
    finally:
        p.terminate()

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
DATA_INGEST_DIR = "data/agri_ingest"
DB_PATH = "data/agriculture.db"
PARQUET_PATH = "data/smart_irrigation.parquet"

# --- Producer ---
def run_producer():
    print("Starting Agri Sensor Producer...")
    if os.path.exists(DATA_INGEST_DIR):
        shutil.rmtree(DATA_INGEST_DIR)
    os.makedirs(DATA_INGEST_DIR, exist_ok=True)
    
    file_count = 0
    try:
        while True:
            # 10 Zones
            df = pd.DataFrame({
                'zone_id': np.random.choice([f'ZONE-{i}' for i in range(1, 11)], 500),
                'sensor_id': np.random.choice([f'SENS-{i}' for i in range(100)], 500),
                'timestamp': pd.Timestamp.now(),
                'moisture_percent': np.random.uniform(10, 80, 500),
                'temperature_c': np.random.uniform(20, 35, 500)
            })
            
            # Dry Zone 3
            mask = df['zone_id'] == 'ZONE-3'
            df.loc[mask, 'moisture_percent'] *= 0.4
            
            filename = f"{DATA_INGEST_DIR}/readings_{file_count}.csv"
            df.to_csv(filename, index=False)
            print(f"Produced {filename}")
            
            file_count += 1
            if file_count > 50: os.remove(f"{DATA_INGEST_DIR}/readings_{file_count-50}.csv")
            
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
    
    try:
        while True:
            all_files = set([f for f in os.listdir(DATA_INGEST_DIR) if f.endswith(".csv")])
            new_files = list(all_files - processed_files)
            
            if new_files:
                file_paths = [os.path.join(DATA_INGEST_DIR, f) for f in new_files]
                ddf = dd.read_csv(file_paths)
                
                # Logic: Avg Moisture by Zone
                stats = ddf.groupby('zone_id')[['moisture_percent']].mean()
                
                result = stats.compute()
                
                # Identify Irrigation Needs (< 25%)
                needs_water = result[result['moisture_percent'] < 25]
                
                if not needs_water.empty:
                    print("--- IRRIGATION ACTIVATED ---")
                    print(needs_water)
                    needs_water.to_sql('irrigation_log', engine, if_exists='append')
                
                # Ensure types for Parquet
                for col_name in ddf.select_dtypes(include=['object', 'string']).columns:
                    ddf[col_name] = ddf[col_name].astype("string[pyarrow]")

                # Archive
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

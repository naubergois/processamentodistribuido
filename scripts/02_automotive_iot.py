import time
import json
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
DATA_INGEST_DIR = "data/automotive_ingest"
DB_PATH = "data/automotive.db"
PARQUET_PATH = "data/automotive_iot.parquet"

# --- Producer (File Generator) ---
# Dask works great with files, so we simulate streaming by generating small CSVs continuously
def run_producer():
    print("Starting Producer (CSV Generator)...")
    if os.path.exists(DATA_INGEST_DIR):
        shutil.rmtree(DATA_INGEST_DIR)
    os.makedirs(DATA_INGEST_DIR, exist_ok=True)
    
    file_count = 0
    try:
        while True:
            # Generate 500 records
            num_records = 500
            df = pd.DataFrame({
                'vehicle_id': np.random.randint(100, 200, num_records),
                'speed_kmh': np.random.normal(80, 25, num_records),
                'engine_rpm': np.random.normal(3000, 1000, num_records),
                'oil_temp': np.random.normal(90, 15, num_records),
                'timestamp': pd.Timestamp.now() # All same timestamp for batch
            })
            
            # Anomaly injection
            df.loc[df['speed_kmh'] > 140, 'driving_style'] = 'AGGRESSIVE'
            df.loc[df['speed_kmh'] <= 140, 'driving_style'] = 'NORMAL'
            
            filename = f"{DATA_INGEST_DIR}/batch_{file_count}.csv"
            df.to_csv(filename, index=False)
            print(f"Produced {filename}")
            
            file_count += 1
            if file_count > 100: # Cleanup old files to prevent disk fill in demo
                try:
                    os.remove(f"{DATA_INGEST_DIR}/batch_{file_count-100}.csv")
                except: pass
                
            time.sleep(2)
    except KeyboardInterrupt:
        pass

# --- Consumer (Dask Processor) ---
def run_consumer():
    print("Starting Dask Consumer...")
    # Setup Local Cluster
    cluster = LocalCluster(n_workers=2, threads_per_worker=1)
    client = Client(cluster)
    print(f"Dask Dashboard: {client.dashboard_link}")
    
    # In a real streaming scenario with Dask, usually 'dask-stream' or custom polling is used.
    # Here we will poll the directory every 5 seconds.
    
    processed_files = set()
    engine = sqlalchemy.create_engine(f'sqlite:///{DB_PATH}')
    
    try:
        while True:
            # Find new files
            all_files = set([f for f in os.listdir(DATA_INGEST_DIR) if f.endswith(".csv")])
            new_files = list(all_files - processed_files)
            
            if new_files:
                print(f"Processing {len(new_files)} new files...")
                file_paths = [os.path.join(DATA_INGEST_DIR, f) for f in new_files]
                
                # Read into Dask
                ddf = dd.read_csv(file_paths)
                
                # Enrichment: Calculate Efficiency (Mock)
                ddf['efficiency'] = ddf['speed_kmh'] / (ddf['engine_rpm'] / 1000)
                
                # Compute Aggregations
                stats = ddf.groupby('driving_style')[['speed_kmh', 'efficiency']].mean()
                
                # Persist Results (Compute & Save)
                result_df = stats.compute()
                print("--- Batch Statistics ---")
                print(result_df)
                
                # Save summary to SQLite
                result_df.to_sql('driving_stats_summary', engine, if_exists='append')
                
                # Ensure types for Parquet
                meta = ddf._meta.copy()
                for col_name in meta.select_dtypes(include=['object', 'string']).columns:
                    ddf[col_name] = ddf[col_name].astype("string[pyarrow]")

                ddf.to_parquet(PARQUET_PATH, engine='pyarrow', append=True, ignore_divisions=True)
                
                # Mark as processed
                processed_files.update(new_files)
            
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("Stopping Dask Consumer...")
    finally:
        client.close()
        cluster.close()

if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
    
    # Run Producer
    p = multiprocessing.Process(target=run_producer)
    p.start()
    
    # Give producer time to make some files
    time.sleep(3)
    
    try:
        run_consumer()
    except KeyboardInterrupt:
        pass
    finally:
        p.terminate()

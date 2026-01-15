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
DATA_INGEST_DIR = "data/manufacturing_ingest"
DB_PATH = "data/manufacturing.db"
PARQUET_PATH = "data/failures_predictive.parquet"

# --- Producer ---
def run_producer():
    print("Starting Factory Producer...")
    if os.path.exists(DATA_INGEST_DIR):
        shutil.rmtree(DATA_INGEST_DIR)
    os.makedirs(DATA_INGEST_DIR, exist_ok=True)
    
    file_count = 0
    machines = [f'MAC-{i}' for i in range(101, 151)]
    
    try:
        while True:
            # 200 records per batch
            df = pd.DataFrame({
                'machine_id': np.random.choice(machines, 200),
                'timestamp': pd.Timestamp.now(),
                'vibration': np.random.normal(50, 5, 200),
                'temperature': np.random.normal(70, 10, 200)
            })
            
            # Failure simulation for MAC-120
            mask = df['machine_id'] == 'MAC-120'
            df.loc[mask, 'vibration'] += 30
            df.loc[mask, 'temperature'] += 25
            
            filename = f"{DATA_INGEST_DIR}/log_{file_count}.csv"
            df.to_csv(filename, index=False)
            print(f"Produced {filename}")
            
            file_count += 1
            if file_count > 50: os.remove(f"{DATA_INGEST_DIR}/log_{file_count-50}.csv")
            
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
                
                # Health Score Logic
                ddf['health_score'] = 100 - (df['vibration'] * 0.5 + df['temperature'] * 0.3)
                
                # Aggregations
                stats = ddf.groupby('machine_id')[['vibration', 'temperature', 'health_score']].mean()
                
                result = stats.compute()
                
                # Identify worst machines
                alerts = result[result['health_score'] < 40]
                if not alerts.empty:
                    print("--- MAINTENANCE ALERTS ---")
                    print(alerts)
                
                # Save
                result.to_sql('machine_status', engine, if_exists='append')

                # Ensure types for Parquet
                for col_name in ddf.select_dtypes(include=['object', 'string']).columns:
                    ddf[col_name] = ddf[col_name].astype("string[pyarrow]")

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

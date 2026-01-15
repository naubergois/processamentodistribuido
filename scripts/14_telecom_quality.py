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
DATA_INGEST_DIR = "data/telecom_ingest"
DB_PATH = "data/telecom.db"
PARQUET_PATH = "data/network_quality.parquet"

# --- Producer ---
def run_producer():
    print("Starting Telecom CDR Producer...")
    if os.path.exists(DATA_INGEST_DIR):
        shutil.rmtree(DATA_INGEST_DIR)
    os.makedirs(DATA_INGEST_DIR, exist_ok=True)
    
    file_count = 0
    towers = [f'TOWER-{i:03d}' for i in range(50)]
    
    try:
        while True:
            df = pd.DataFrame({
                'call_id': np.random.randint(100000, 999999, 1000),
                'tower_id': np.random.choice(towers, 1000),
                'timestamp': pd.Timestamp.now(),
                'duration_sec': np.random.exponential(120, 1000),
                'status': np.random.choice(['SUCCESS', 'DROP', 'FAIL'], 1000, p=[0.9, 0.05, 0.05])
            })
            
            # Degraded Tower 42
            mask = df['tower_id'] == 'TOWER-042'
            df.loc[mask, 'status'] = np.random.choice(['SUCCESS', 'DROP'], mask.sum(), p=[0.6, 0.4])
            
            filename = f"{DATA_INGEST_DIR}/cdr_{file_count}.csv"
            df.to_csv(filename, index=False)
            print(f"Produced {filename}")
            
            file_count += 1
            if file_count > 50: os.remove(f"{DATA_INGEST_DIR}/cdr_{file_count-50}.csv")
            
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
                
                # Custom Aggregation logic using Pandas syntax via Dask
                stats = ddf.groupby('tower_id').agg(
                    total_calls=('call_id', 'count'),
                    drops=('status', lambda x: (x == 'DROP').sum())
                )
                
                result = stats.compute()
                result['drop_rate'] = result['drops'] / result['total_calls']
                
                # Detect Bad Towers
                bad_towers = result[result['drop_rate'] > 0.10]
                
                if not bad_towers.empty:
                    print("--- NETWORK QUALITY ALERT ---")
                    print(bad_towers.sort_values('drop_rate', ascending=False).head())
                    bad_towers.to_sql('bad_towers_log', engine, if_exists='append')
                
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

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
DATA_INGEST_DIR = "data/weather_ingest"
DB_PATH = "data/weather.db"
PARQUET_PATH = "data/weather_alerts.parquet"

# --- Producer ---
def run_producer():
    print("Starting Weather Producer...")
    if os.path.exists(DATA_INGEST_DIR):
        shutil.rmtree(DATA_INGEST_DIR)
    os.makedirs(DATA_INGEST_DIR, exist_ok=True)
    
    file_count = 0
    stations = [f'STATION-{i:02d}' for i in range(20)]
    
    try:
        while True:
            df = pd.DataFrame({
                'station_id': np.random.choice(stations, 500),
                'timestamp': pd.Timestamp.now(),
                'temp_c': np.random.normal(25, 5, 500),
                'humidity': np.random.uniform(30, 90, 500)
            })
            
            # Heatwave Simulation (Station 05)
            mask = df['station_id'] == 'STATION-05'
            df.loc[mask, 'temp_c'] += 15
            
            filename = f"{DATA_INGEST_DIR}/weather_{file_count}.csv"
            df.to_csv(filename, index=False)
            print(f"Produced {filename}")
            
            file_count += 1
            if file_count > 50: os.remove(f"{DATA_INGEST_DIR}/weather_{file_count-50}.csv")
            
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
                
                # Logic: Detect Heatwave (> 35C)
                stats = ddf.groupby('station_id').agg({'temp_c': 'max'})
                
                result = stats.compute()
                alerts = result[result['temp_c'] > 35]
                
                if not alerts.empty:
                    print("--- HEATWAVE ALERTS ---")
                    print(alerts)
                    alerts.to_sql('weather_alerts', engine, if_exists='append')
                
                # Persistence
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

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
DATA_INGEST_DIR = "data/hotel_ingest"
DB_PATH = "data/hospitality.db"
PARQUET_PATH = "data/hotel_bookings.parquet"

# --- Producer ---
def run_producer():
    print("Starting Hotel Producer...")
    if os.path.exists(DATA_INGEST_DIR):
        shutil.rmtree(DATA_INGEST_DIR)
    os.makedirs(DATA_INGEST_DIR, exist_ok=True)
    
    file_count = 0
    hotels = [f'HOTEL-{city}' for city in ['NYC', 'LA', 'MIA', 'CHI', 'SF']]
    
    try:
        while True:
            # 500 bookings
            df = pd.DataFrame({
                'hotel_id': np.random.choice(hotels, 500),
                'booking_id': range(file_count*500, (file_count+1)*500),
                'check_in': pd.date_range(start='2024-01-01', periods=500, freq='h'),
                'nights': np.random.randint(1, 14, 500),
                'price': np.random.uniform(100, 800, 500),
                'status': np.random.choice(['CONFIRMED', 'CANCELLED'], 500, p=[0.9, 0.1])
            })
            
            filename = f"{DATA_INGEST_DIR}/bookings_{file_count}.csv"
            df.to_csv(filename, index=False)
            print(f"Produced {filename}")
            
            file_count += 1
            if file_count > 50: os.remove(f"{DATA_INGEST_DIR}/bookings_{file_count-50}.csv")
            
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
                
                # Filter Valid
                valid = ddf[ddf['status'] == 'CONFIRMED']
                
                # Revenue
                revenue = valid.groupby('hotel_id').agg(
                    total_revenue=('price', 'sum'),
                    total_nights=('nights', 'sum')
                )
                
                result = revenue.compute()
                print("--- REVENUE DASHBOARD ---")
                print(result)
                
                result.to_sql('revenue_snapshot', engine, if_exists='append')

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

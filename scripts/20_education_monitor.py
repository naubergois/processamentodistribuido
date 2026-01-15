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
DATA_INGEST_DIR = "data/education_ingest"
DB_PATH = "data/education.db"
PARQUET_PATH = "data/exam_monitor.parquet"

# --- Producer ---
def run_producer():
    print("Starting Exam Producer...")
    if os.path.exists(DATA_INGEST_DIR):
        shutil.rmtree(DATA_INGEST_DIR)
    os.makedirs(DATA_INGEST_DIR, exist_ok=True)
    
    file_count = 0
    students = [f'STUDENT-{i}' for i in range(1001, 1051)]
    
    try:
        while True:
            df = pd.DataFrame({
                'student_id': np.random.choice(students, 500),
                'exam_id': 'EXAM-FINAL-2024',
                'event': np.random.choice(['ANSWER', 'TAB_SWITCH', 'IDLE'], 500, p=[0.8, 0.1, 0.1]),
                'duration': np.random.exponential(30, 500)
            })
            
            # Cheater simulation
            mask = df['student_id'] == 'STUDENT-1042'
            df.loc[mask, 'event'] = 'TAB_SWITCH'
            
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
                
                # Logic: Suspicious if many tab switches
                stats = ddf.groupby('student_id')['event'].apply(
                    lambda x: (x == 'TAB_SWITCH').sum(), 
                    meta=('x', 'int')
                )
                
                result = stats.compute().reset_index()
                result.columns = ['student_id', 'switches']
                
                # Flag
                flagged = result[result['switches'] > 5]
                if not flagged.empty:
                    print("--- CHEATING ALERTS ---")
                    print(flagged)
                    flagged.to_sql('flagged_students', engine, if_exists='append')
                
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

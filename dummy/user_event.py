import os
from datetime import datetime, timedelta
import s3fs # pip install s3fs pandas pyarrow
import pandas as pd
import numpy as np
from trino.dbapi import connect
from dotenv import load_dotenv


load_dotenv()
# --- CONFIG ---
TARGET_DATE = "2023-01-01" # Start here, then increment
BUCKET_NAME = "landing"
PREFIX = "daily_activity"
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = os.getenv("S3_ACCESS_KEY_ID")
MINIO_SECRET_KEY = os.getenv("S3_SECRET_ACCESS_KEY")


def bootstrap_trino_infrastructure():
    conn = connect(host="localhost", port=8080, user="admin")
    cur = conn.cursor()
    
    cur.execute("""
        CREATE SCHEMA IF NOT EXISTS hive.raw_zone 
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS hive.raw_zone.daily_activity (
            user_id VARCHAR,
            timestamp TIMESTAMP,
            activity VARCHAR,
            event_date VARCHAR
        )
        WITH (
            format = 'PARQUET',
            partitioned_by = ARRAY['event_date'],
            external_location = 's3a://landing/daily_activity/'
        )
    """)
    print("Trino infrastructure verified.")

# --- FUNCTIONS ---

def run_pipeline(date_str):
    print(f"--- Starting Pipeline for {date_str} ---")
    
    num_users = 100
    user_ids = [f"user_{np.random.randint(1, 500)}" for _ in range(num_users)]
    activities = ["login", "purchase", "view_product", "add_to_cart", "checkout"]
    
    data = []
    for uid in user_ids:
        for _ in range(np.random.randint(1, 3)):
            random_seconds = np.random.randint(0, 86400)
            # The timestamp is locked to the partition date
            ts = datetime.strptime(date_str, "%Y-%m-%d") + timedelta(seconds=random_seconds)
            data.append({
                "user_id": uid,
                "timestamp": ts,
                "activity": np.random.choice(activities)
            })

    df = pd.DataFrame(data)
    print(df.count())
    
    # Write to MinIO using Hive-style partitioning
    fs = s3fs.S3FileSystem(key=MINIO_ACCESS_KEY, secret=MINIO_SECRET_KEY, 
                           client_kwargs={'endpoint_url': MINIO_ENDPOINT})
    
    path = f"{BUCKET_NAME}/{PREFIX}/event_date={date_str}/user_event.parquet"
    with fs.open(path, 'wb') as f:
        df.to_parquet(f, index=False)
    print(f"1. Data landed in MinIO: event_date={date_str}")


# --- THE "NEXT RUN" LOOP ---
if __name__ == "__main__":
    start_date = datetime.strptime(TARGET_DATE, "%Y-%m-%d")
    # Let's simulate 5 days in a row
    for i in range(5):
        current_run_date = (start_date + timedelta(days=i)).strftime("%Y-%m-%d")
        run_pipeline(current_run_date)
    
    bootstrap_trino_infrastructure()
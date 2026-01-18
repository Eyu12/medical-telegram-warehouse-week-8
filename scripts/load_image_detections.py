import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

CSV_PATH = "data/enriched/image_detections.csv"

# SQL for inserting data
SQL_INSERT = """
INSERT INTO raw.image_detections (
    message_id,
    channel_username,
    message_date,
    image_path,
    detected_object,
    confidence,
    detected_at
)
VALUES (
    %(message_id)s,
    %(channel_username)s,
    %(message_date)s,
    %(image_path)s,
    %(detected_object)s,
    %(confidence)s,
    %(detected_at)s
);
"""

# SQL to create schema and table if they don't exist
SQL_CREATE = """
-- Create schema if it does not exist
CREATE SCHEMA IF NOT EXISTS raw;

-- Create table if it does not exist
CREATE TABLE IF NOT EXISTS raw.image_detections (
    message_id TEXT,
    channel_username TEXT,
    message_date DATE,
    image_path TEXT,
    detected_object TEXT,
    confidence NUMERIC,
    detected_at TIMESTAMP
);
"""

def main():
    # Load CSV
    df = pd.read_csv(CSV_PATH)

    # Ensure detected_at is datetime
    if not pd.api.types.is_datetime64_any_dtype(df["detected_at"]):
        df["detected_at"] = pd.to_datetime(df["detected_at"], errors="coerce")

    # Drop rows with invalid detected_at
    df = df.dropna(subset=["detected_at"])

    # Add message_date column based on detected_at
    df["message_date"] = df["detected_at"].dt.date

    # Convert DataFrame to list of dictionaries
    records = df.to_dict(orient="records")

    # Connect to PostgreSQL
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            # Create schema and table if they don't exist
            cur.execute(SQL_CREATE)
            conn.commit()

            # Insert data in batches
            execute_batch(cur, SQL_INSERT, records, page_size=500)
            conn.commit()

    print(f"{len(records)} image detections loaded into PostgreSQL.")

if __name__ == "__main__":
    main()

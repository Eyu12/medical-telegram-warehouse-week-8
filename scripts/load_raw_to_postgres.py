import os
import json
import csv
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Folder containing CSV and JSON files
DATA_PATH = "data/raw/telegram_messages/2026-01-16"

# PostgreSQL connection configuration
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "port": os.getenv("POSTGRES_PORT", 5432)
}

# SQL insert statement
SQL = """
INSERT INTO raw.telegram_messages (
    message_id,
    channel_id,
    channel_username,
    channel_title,
    message_date,
    message_text,
    views,
    forwards,
    replies,
    media_type,
    media_file,
    message_url,
    scraped_at
) VALUES (
    %(message_id)s,
    %(channel_id)s,
    %(channel_username)s,
    %(channel_title)s,
    %(message_date)s,
    %(message_text)s,
    %(views)s,
    %(forwards)s,
    %(replies)s,
    %(media_type)s,
    %(media_file)s,
    %(message_url)s,
    %(scraped_at)s
)
ON CONFLICT (message_id) DO NOTHING;
"""

def normalize_record(record):
    """Normalize keys and ensure all required fields exist"""
    if not isinstance(record, dict):
        return {}

    # Rename keys from JSON/CSV to match DB columns
    mapping = {
        "text": "message_text",
        "file_path": "media_file",
        "date": "message_date",
        "url": "message_url"
    }
    for k, v in mapping.items():
        if k in record:
            record[v] = record.pop(k)

    # Ensure all required keys exist
    required_keys = [
        "message_id",
        "channel_id",
        "channel_username",
        "channel_title",
        "message_date",
        "message_text",
        "views",
        "forwards",
        "replies",
        "media_type",
        "media_file",
        "message_url",
        "scraped_at"
    ]
    for key in required_keys:
        if key not in record or record[key] is None:
            record[key] = None

    # Convert numeric fields
    for int_field in ["views", "forwards", "replies", "channel_id", "message_id"]:
        if record[int_field] is not None:
            try:
                record[int_field] = int(record[int_field])
            except (ValueError, TypeError):
                record[int_field] = 0
        else:
            record[int_field] = 0

    # Ensure timestamp fields are None if empty string
    for ts_field in ["message_date", "scraped_at"]:
        if record[ts_field] == "":
            record[ts_field] = None

    return record

def load_json(file_path):
    """Load records from a JSON file"""
    with open(file_path, encoding="utf-8") as f:
        data = json.load(f)
        records = []

        # Wrap single JSON object in a list
        if isinstance(data, dict):
            data = [data]

        for r in data:
            if isinstance(r, dict):
                records.append(normalize_record(r))
            else:
                continue
    return records

def load_csv(file_path):
    """Load records from a CSV file"""
    records = []
    with open(file_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            records.append(normalize_record(row))
    return records

def load_records():
    """Load all records from CSV and JSON in DATA_PATH"""
    records = []
    for root, _, files in os.walk(DATA_PATH):
        for f in files:
            path = os.path.join(root, f)
            if f.endswith(".json"):
                records.extend(load_json(path))
            elif f.endswith(".csv"):
                records.extend(load_csv(path))
    return records

def main():
    print("Loading records into PostgreSQL...")
    records = load_records()
    if not records:
        print("No records found. Check your DATA_PATH and file formats.")
        return

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    execute_batch(cur, SQL, records, page_size=1000)

    conn.commit()
    cur.close()
    conn.close()

    print(f"Loaded {len(records)} messages into raw.telegram_messages")

if __name__ == "__main__":
    main()

# api/main.py

from fastapi import FastAPI, HTTPException
from api.database import SessionLocal
import pandas as pd

app = FastAPI(title="Telegram Analytics API")


# -----------------------------
# Endpoint 1: Top Products
# -----------------------------
@app.get("/api/reports/top-products")
def top_products(limit: int = 10):
    session = SessionLocal()
    try:
        df = pd.read_sql("SELECT detected_class FROM marts.fct_image_detections", session.bind)
        top = df['detected_class'].value_counts().head(limit).reset_index()
        top.columns = ['detected_class', 'count']
        return top.to_dict(orient="records")
    finally:
        session.close()


# -----------------------------
# Endpoint 2: Channel Activity
# -----------------------------
@app.get("/api/channels/{channel_name}/activity")
def channel_activity(channel_name: str):
    session = SessionLocal()
    try:
        df = pd.read_sql(
            f"""
            SELECT date_key, COUNT(*) AS posts
            FROM fct_messages
            WHERE channel_key = '{channel_name}'
            GROUP BY date_key
            ORDER BY date_key
            """,
            session.bind
        )
        return df.to_dict(orient="records")
    finally:
        session.close()


# -----------------------------
# Endpoint 3: Message Search
# -----------------------------
@app.get("/api/search/messages")
def search_messages(query: str, limit: int = 20):
    session = SessionLocal()
    try:
        df = pd.read_sql(
            f"""
            SELECT message_id, content
            FROM fct_messages
            WHERE content ILIKE '%{query}%'
            LIMIT {limit}
            """,
            session.bind
        )
        return df.to_dict(orient="records")
    finally:
        session.close()


# -----------------------------
# Endpoint 4: Visual Content Stats
# -----------------------------
@app.get("/api/reports/visual-content")
def visual_content_stats():
    session = SessionLocal()
    try:
        df = pd.read_sql(
            """
            SELECT channel_key, COUNT(*) AS image_count
            FROM marts.fct_image_detections
            GROUP BY channel_key
            """,
            session.bind
        )
        return df.to_dict(orient="records")
    finally:
        session.close()

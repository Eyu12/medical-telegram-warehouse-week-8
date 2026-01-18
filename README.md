# Task 1: Data Scraping and Collection

## Overview
This module implements a robust data scraping pipeline that extracts messages and images from Telegram channels related to Ethiopian medical businesses. The scraped data is stored in a raw data lake structure for further processing.

## Features
- **Multi-channel scraping**: Scrapes data from multiple Telegram channels simultaneously
- **Image downloading**: Automatically downloads images from messages
- **Data extraction**: Extracts message metadata, text, views, forwards, etc.
- **Product detection**: Identifies medical product mentions using regex patterns
- **Price extraction**: Extracts price information from messages
- **Contact detection**: Identifies phone numbers in messages
- **Structured storage**: Saves data in organized JSON files with partitioned directories
- **Database loading**: Loads scraped data into PostgreSQL for further processing
- **Comprehensive logging**: Detailed logging for monitoring and debugging

## Prerequisites

### 1. Telegram API Access
1. Visit https://my.telegram.org/apps
2. Log in with your Telegram account
3. Create a new application
4. Note down your `api_id` and `api_hash`

# Task 2 - Data Modeling and Transformation (Transform)

## Project Overview
This task focuses on transforming raw, messy Telegram data into a **clean, structured data warehouse** using **DBT** and dimensional modeling. The goal is to create a reliable and trusted data product for analytics.

The workflow includes:

- Loading raw JSON and CSV data into PostgreSQL (`raw.telegram_messages` table).
- Cleaning and standardizing the data in **DBT staging models**.
- Building a **star schema** with dimension and fact tables.
- Adding calculated fields and tests to ensure data quality.

## PostgreSQL Raw Table

**Table:** `raw.telegram_messages`

| Column Name        | Type      | Description                      |
|-------------------|-----------|----------------------------------|
| message_id         | BIGINT    | Unique message identifier        |
| channel_id         | BIGINT    | Telegram channel ID              |
| channel_username   | TEXT      | Channel username                 |
| channel_title      | TEXT      | Channel title                    |
| message_date       | TIMESTAMP | Message timestamp                |
| message_text       | TEXT      | Content of the message           |
| views              | INT       | Number of views                  |
| forwards           | INT       | Number of forwards               |
| replies            | INT       | Number of replies                |
| media_type         | TEXT      | Type of media (photo, video…)   |
| media_file         | TEXT      | Path to media file               |
| message_url        | TEXT      | URL of the message               |
| scraped_at         | TIMESTAMP | Timestamp when message was scraped|

---

## DBT Staging Model

**Model:** `stg_telegram_messages`

- Cleans and standardizes raw data.
- Adds calculated fields:
  - `message_length` — number of characters in the message.
  - `has_image` — boolean flag indicating presence of media.
- Filters out messages with empty text.
- Includes DBT tests for:
  - Primary key (`message_id`) uniqueness and not null.
  - Critical columns not null (`channel_id`, `message_text`, `message_date`).

---

## Star Schema (Marts)

### Dimension Tables

1. **dim_channels**
   - Surrogate key: `channel_key`
   - Aggregated metrics: `total_posts`, `avg_views`
   - First and last post dates per channel

2. **dim_dates**
   - Surrogate key: `date_key`
   - Includes day, week, month, quarter, year, and weekend flag for time-based analysis

### Fact Table

**fct_messages**
- One row per message
- Includes metrics: `view_count`, `forward_count`, `message_length`, `has_image`
- Joins to `dim_channels` and `dim_dates` using surrogate keys

# Task 3 - Data Enrichment with Object Detection (YOLO)

**Objective:**  
Use computer vision to analyze images and integrate the findings into your data warehouse.

## Instructions

1. **Set Up YOLO Environment**
    pip install ultralytics
- Use YOLOv8 nano (`yolov8n.pt`) for efficiency.

2. **Implement Object Detection Script**
- Create `src/yolo_detect.py` to:
  - Scan images from Task 1
  - Run YOLOv8 detection on each image
  - Record detected objects and confidence scores
  - Save results to CSV

3. **Create Classification Scheme**
- `promotional`: person + product  
- `product_display`: bottle/container, no person  
- `lifestyle`: person, no product  
- `other`: neither detected

4. **Integrate with Data Warehouse**
- Create `models/marts/fct_image_detections.sql` dbt model
  - Load YOLO results into PostgreSQL
  - Join with `fct_messages` on `message_id`
  - Include columns: `message_id`, `channel_key`, `date_key`, `detected_class`, `confidence_score`, `image_category`

5. **Analyze Results**
- Questions to answer in report:
  - Do "promotional" posts get more views than "product_display" posts?
  - Which channels use more visual content?
  - Limitations of pre-trained models for domain-specific tasks

## Deliverables
- Object detection script (`src/yolo_detect.py`)
- Detection results CSV
- `fct_image_detections` dbt model
- Analysis report

# Task 4 - Build an Analytical API

**Objective:**  
Expose your data warehouse through a REST API for business analytics.

## Instructions

1. **Set Up FastAPI Project**
pip install fastapi uvicorn
- Create API structure in `api/`
- Use SQLAlchemy for DB connections

2. **Implement Analytical Endpoints**
- **Top Products**
GET /api/reports/top-products?limit=10
- **Channel Activity**
GET /api/channels/{channel_name}/activity
- **Message Search**
GET /api/search/messages?query=paracetamol&limit=20
- **Visual Content Stats**
GET /api/reports/visual-content

3. **Add Data Validation**
- Use Pydantic models (`api/schemas.py`)
- Include error handling and HTTP status codes

4. **Document the API**
- FastAPI generates OpenAPI docs automatically
- Access at `/docs`
- Add descriptions for endpoints & parameters

## Deliverables
- FastAPI application (`api/main.py`)
- 4+ analytical endpoints
- Pydantic schemas
- Screenshots of API docs and example responses


# Task 5 - Pipeline Orchestration

**Objective:**  
Automate your data pipeline using Dagster.

## Instructions

1. **Install Dagster**
pip install dagster dagster-webserver

2. **Define Pipeline as Dagster Job**
- Convert scripts into Dagster `ops`:
  - `scrape_telegram_data`
  - `load_raw_to_postgres`
  - `run_dbt_transformations`
  - `run_yolo_enrichment`

3. **Create the Job Graph**
- Define dependencies between ops
- Ensure proper execution order

4. **Launch and Test**
dagster dev -f pipeline.py
- Access UI: [http://localhost:3000](http://localhost:3000)
- Run manually
- Monitor logs & execution

5. **Add Scheduling**
- Configure daily pipeline runs
- Set up alerts for failures

## Deliverables
- Dagster pipeline definition (`pipeline.py`)
- Screenshots of successful runs in UI

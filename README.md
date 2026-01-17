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
# Task 1: Telegram Data Scraping and Collection

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





FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements-task1.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements-task1.txt

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p /app/data/raw/images \
    /app/data/raw/telegram_messages \
    /app/data/raw/archive \
    /app/logs

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "src/scraper.py"]
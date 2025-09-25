# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies for psycopg2
RUN apt-get update && apt-get install -y \
    build-essential \ 
    libpq-dev \ 
    && rm -rf /var/lib/apt/lists/*

# Copy dependency list
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy bot source code
COPY filecloudsupabaseX.py .

# Expose the healthcheck port
EXPOSE 8000

# Run the bot
CMD ["python", "filecloudsupabaseX.py"]

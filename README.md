# Delta Viewer

A minimal SRE tool for browsing S3/MinIO storage and viewing data files with Delta table support. Built with FastAPI for high performance.

## Quick Start

1. Copy `.env.example` to `.env` and configure your S3/MinIO settings
2. Install dependencies: `pip install -r requirements.txt`
3. Run: `python delta_viewer.py`
4. Open browser to `http://localhost:5000`
5. View API docs at `http://localhost:5000/docs`

## Features

- 📁 Browse S3/MinIO folders like a file system
- 📊 View CSV, Parquet, and Avro files
- 🔺 View folders as Delta tables
- 🔍 Preview first 1000 rows (configurable)
- 🏥 Health check endpoint at `/health`
- 🚀 FastAPI with async performance
- 📚 Automatic API documentation

## Configuration

Set these environment variables in `.env`:

```
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
S3_BUCKET_NAME=your-bucket
S3_ENDPOINT_URL=http://localhost:9000  # For MinIO
AWS_REGION=us-east-1
MAX_PREVIEW_ROWS=1000
```

## Running

```bash
# Direct run
python delta_viewer.py

# With uvicorn for development
uvicorn delta_viewer:app --host 0.0.0.0 --port 5000 --reload
```
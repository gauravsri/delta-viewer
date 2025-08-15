# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Delta Viewer is a minimal SRE tool for browsing S3/MinIO storage and viewing data files (CSV, Parquet, Avro) with Delta table support. The entire application is contained in a single Python file for simplicity.

## Development Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run the application
python delta_viewer.py

# Run with uvicorn directly
uvicorn delta_viewer:app --host 0.0.0.0 --port 5000 --reload

# Test health endpoint
curl http://localhost:5000/health

# View API docs (FastAPI automatic documentation)
# Open http://localhost:5000/docs in browser
```

## Configuration

Create `.env` file from `.env.example` with your S3/MinIO credentials:
- `AWS_ACCESS_KEY_ID` - S3/MinIO access key
- `AWS_SECRET_ACCESS_KEY` - S3/MinIO secret key  
- `S3_BUCKET_NAME` - Target bucket name
- `S3_ENDPOINT_URL` - MinIO endpoint (optional for AWS)
- `MAX_PREVIEW_ROWS` - Row limit for file previews (default: 1000)

## Architecture

Single-file FastAPI application with these components:

- **S3Handler**: Manages S3/MinIO connections and object listing
- **FileViewers**: Handles CSV, Parquet, Avro, and Delta table reading
- **FastAPI Routes**: Async web interface for browsing and viewing files
- **Simple Template Renderer**: Custom template system with inline HTML
- **FastAPI Features**: Automatic API documentation, async support, better performance

## Key Features

- File system-like browsing of S3/MinIO buckets
- Preview data files with automatic format detection
- Delta table viewing with metadata display
- Responsive web interface with breadcrumb navigation
- Health check endpoint for monitoring
- Automatic FastAPI documentation at `/docs`
- Async performance improvements

## File Structure

```
delta-viewer/
├── delta_viewer.py      # Main application (single file)
├── requirements.txt     # Python dependencies
├── .env.example        # Configuration template
├── README.md           # User documentation
└── CLAUDE.md           # This file
```

## Error Handling

The application includes comprehensive error handling for:
- S3/MinIO connection failures
- Invalid file formats
- Missing configuration
- Delta table reading errors

All errors are displayed in the web interface with helpful messages.
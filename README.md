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
- 📊 View CSV, Parquet, Avro, JSON, and XML files
- 📝 View text files and code with syntax detection
- 🔍 View any file type as raw content (text or hex dump)
- 🔺 View folders as Delta tables
- ➕ Create new folders
- 🗑️ Delete files and folders (with confirmation)
- 🔍 Preview first 1000 rows (configurable)
- 🏥 Health check endpoint at `/health`
- 🚀 FastAPI with async performance
- 📚 Automatic API documentation

## Configuration

### AWS S3 Configuration
Set these environment variables in `.env`:

```bash
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
S3_BUCKET_NAME=your-bucket-name
AWS_REGION=us-east-1
MAX_PREVIEW_ROWS=1000
# Leave S3_ENDPOINT_URL empty for AWS S3
```

### MinIO Configuration
For MinIO (local S3-compatible storage):

```bash
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
S3_BUCKET_NAME=your-bucket-name
S3_ENDPOINT_URL=http://localhost:9000
AWS_REGION=us-east-1
MAX_PREVIEW_ROWS=1000
```

### MinIO Setup
To run MinIO locally:

```bash
# Using Docker
docker run -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"

# Access MinIO Console at http://localhost:9001
# API endpoint: http://localhost:9000
```

## Running

```bash
# Direct run
python delta_viewer.py

# With uvicorn for development
uvicorn delta_viewer:app --host 0.0.0.0 --port 5000 --reload
```

## Usage Examples

1. **Browse folders**: Navigate through S3/MinIO buckets like a file system
2. **View data files**: Click on CSV, Parquet, Avro, JSON, or XML files to preview data
3. **JSON handling**: 
   - JSON arrays of objects → displayed as tables
   - JSON objects → displayed as key-value pairs
   - Other JSON → displayed as formatted text
4. **XML handling**:
   - Structured XML (repeated elements) → displayed as tables
   - Mixed/simple XML → displayed as formatted text
   - Attributes prefixed with `@`, text content as `_text`
5. **Text files**: View logs, code, configuration files with automatic content type detection
6. **Unknown files**: Any file can be viewed as raw text or binary hex dump
7. **Delta tables**: Use "View as Delta Table" button to read Delta Lake tables
8. **File management**:
   - Create folders: Click "📁 Create Folder" button
   - Delete files/folders: Click 🗑️ button (with confirmation dialog)
9. **Health check**: Visit `/health` to verify S3/MinIO connectivity

### File Type Icons
- 📊 Data files (CSV, Parquet, Avro, JSON, XML)
- 📝 Text files (TXT, LOG, MD, code files)
- 🔍 Other viewable files

### Management Controls
- **Create Folder**: Button in the management bar to create new folders
- **Delete Items**: 🗑️ buttons next to each file/folder for deletion
- **Confirmations**: All delete operations require confirmation
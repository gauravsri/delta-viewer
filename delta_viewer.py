#!/usr/bin/env python3
"""
Delta Viewer - S3/MinIO File Browser and Delta Table Viewer
A minimal SRE tool for browsing cloud storage and viewing data files.
"""

import os
import json
import logging
from typing import Dict, List, Any
from urllib.parse import quote
from io import BytesIO

import boto3
import pandas as pd
import pyarrow.parquet as pq
from deltalake import DeltaTable
import fastavro
from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from dotenv import load_dotenv
import uvicorn

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
class Config:
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID', '')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', '')
    AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
    S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL')
    S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME', '')
    MAX_PREVIEW_ROWS = int(os.getenv('MAX_PREVIEW_ROWS', '1000'))

# S3 Client Handler
class S3Handler:
    def __init__(self):
        self.session = boto3.Session(
            aws_access_key_id=Config.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=Config.AWS_SECRET_ACCESS_KEY,
            region_name=Config.AWS_REGION
        )
        
        self.s3_client = self.session.client(
            's3',
            endpoint_url=Config.S3_ENDPOINT_URL
        )
        
        self.bucket_name = Config.S3_BUCKET_NAME

    def list_objects(self, prefix: str = '') -> List[Dict[str, Any]]:
        """List objects in S3 bucket with given prefix."""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
                Delimiter='/'
            )
            
            folders = []
            files = []
            
            # Add folders
            if 'CommonPrefixes' in response:
                for prefix_info in response['CommonPrefixes']:
                    folder_name = prefix_info['Prefix'].rstrip('/')
                    folders.append({
                        'name': folder_name.split('/')[-1],
                        'path': prefix_info['Prefix'],
                        'type': 'folder'
                    })
            
            # Add files
            if 'Contents' in response:
                for obj in response['Contents']:
                    if not obj['Key'].endswith('/'):
                        files.append({
                            'name': obj['Key'].split('/')[-1],
                            'path': obj['Key'],
                            'size': obj['Size'],
                            'modified': obj['LastModified'].isoformat(),
                            'type': 'file'
                        })
            
            return folders + files
            
        except Exception as e:
            logger.error(f"Error listing objects: {e}")
            return []

    def get_object_content(self, key: str) -> bytes:
        """Get object content from S3."""
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            return response['Body'].read()
        except Exception as e:
            logger.error(f"Error getting object {key}: {e}")
            raise

# File Viewers
class FileViewers:
    @staticmethod
    def view_csv(content: bytes, max_rows: int = 1000) -> Dict[str, Any]:
        """View CSV file content."""
        try:
            df = pd.read_csv(BytesIO(content))
            if len(df) > max_rows:
                df = df.head(max_rows)
                truncated = True
            else:
                truncated = False
                
            return {
                'type': 'table',
                'data': df.to_dict('records'),
                'columns': df.columns.tolist(),
                'total_rows': len(df),
                'truncated': truncated,
                'max_rows': max_rows
            }
        except Exception as e:
            return {'type': 'error', 'message': f"Error reading CSV: {e}"}

    @staticmethod
    def view_parquet(content: bytes, max_rows: int = 1000) -> Dict[str, Any]:
        """View Parquet file content."""
        try:
            table = pq.read_table(BytesIO(content))
            df = table.to_pandas()
            
            if len(df) > max_rows:
                df = df.head(max_rows)
                truncated = True
            else:
                truncated = False
                
            return {
                'type': 'table',
                'data': df.to_dict('records'),
                'columns': df.columns.tolist(),
                'total_rows': len(df),
                'truncated': truncated,
                'max_rows': max_rows,
                'schema': str(table.schema)
            }
        except Exception as e:
            return {'type': 'error', 'message': f"Error reading Parquet: {e}"}

    @staticmethod
    def view_avro(content: bytes, max_rows: int = 1000) -> Dict[str, Any]:
        """View Avro file content."""
        try:
            records = []
            schema = None
            
            with BytesIO(content) as bio:
                reader = fastavro.reader(bio)
                schema = reader.writer_schema
                
                for i, record in enumerate(reader):
                    if i >= max_rows:
                        break
                    records.append(record)
            
            truncated = len(records) == max_rows
            columns = list(records[0].keys()) if records else []
            
            return {
                'type': 'table',
                'data': records,
                'columns': columns,
                'total_rows': len(records),
                'truncated': truncated,
                'max_rows': max_rows,
                'schema': json.dumps(schema, indent=2)
            }
        except Exception as e:
            return {'type': 'error', 'message': f"Error reading Avro: {e}"}

    @staticmethod
    def view_delta_table(s3_path: str, max_rows: int = 1000) -> Dict[str, Any]:
        """View Delta table content."""
        try:
            # Configure S3 for Delta Lake
            storage_options = {
                'AWS_ACCESS_KEY_ID': Config.AWS_ACCESS_KEY_ID,
                'AWS_SECRET_ACCESS_KEY': Config.AWS_SECRET_ACCESS_KEY,
                'AWS_REGION': Config.AWS_REGION,
            }
            
            if Config.S3_ENDPOINT_URL:
                storage_options['AWS_ENDPOINT_URL'] = Config.S3_ENDPOINT_URL
                storage_options['AWS_ALLOW_HTTP'] = 'true'
            
            full_path = f"s3://{Config.S3_BUCKET_NAME}/{s3_path.rstrip('/')}"
            dt = DeltaTable(full_path, storage_options=storage_options)
            
            df = dt.to_pandas()
            
            if len(df) > max_rows:
                df = df.head(max_rows)
                truncated = True
            else:
                truncated = False
            
            # Get table metadata
            metadata = {
                'version': dt.version(),
                'files': len(dt.files()),
                'schema': str(dt.schema())
            }
            
            return {
                'type': 'delta_table',
                'data': df.to_dict('records'),
                'columns': df.columns.tolist(),
                'total_rows': len(df),
                'truncated': truncated,
                'max_rows': max_rows,
                'metadata': metadata
            }
        except Exception as e:
            return {'type': 'error', 'message': f"Error reading Delta table: {e}"}

# FastAPI Application
app = FastAPI(
    title="Delta Viewer",
    description="S3/MinIO File Browser and Delta Table Viewer",
    version="1.0.0"
)

# Create a simple template renderer
class SimpleTemplates:
    def render(self, template: str, **kwargs) -> str:
        """Simple template rendering using string formatting."""
        # Convert lists to template-friendly format
        if 'items' in kwargs:
            items_html = ""
            for item in kwargs['items']:
                icon = "📁" if item['type'] == 'folder' else "📄"
                if item['type'] == 'folder':
                    items_html += f'''
                    <li class="file-item">
                        <span class="file-icon">{icon}</span>
                        <div class="file-name">
                            <a href="/?path={quote(item['path'])}">{item['name']}/</a>
                        </div>
                    </li>'''
                else:
                    data_icon = "📊" if item['name'].endswith(('.csv', '.parquet', '.avro')) else ""
                    size_kb = item['size'] / 1024
                    items_html += f'''
                    <li class="file-item">
                        <span class="file-icon">{icon}</span>
                        <div class="file-name">
                            <a href="/view?file={quote(item['path'])}">{item['name']}</a>
                            {f'<span style="margin-left: 10px; color: #059669;">{data_icon}</span>' if data_icon else ''}
                        </div>
                        <div class="file-meta">
                            {size_kb:.2f} KB | {item['modified'][:10]}
                        </div>
                    </li>'''
            kwargs['items_html'] = items_html
        
        # Handle breadcrumbs
        if 'breadcrumbs' in kwargs:
            breadcrumb_html = '<a href="/">🏠 Home</a>'
            for crumb in kwargs['breadcrumbs']:
                breadcrumb_html += f' / <a href="{crumb["url"]}">{crumb["name"]}</a>'
            kwargs['breadcrumb_html'] = breadcrumb_html
        
        # Handle file content tables
        if 'file_content' in kwargs and kwargs['file_content'].get('type') in ['table', 'delta_table']:
            content = kwargs['file_content']
            table_html = "<table><thead><tr>"
            for col in content['columns']:
                table_html += f"<th>{col}</th>"
            table_html += "</tr></thead><tbody>"
            
            for row in content['data']:
                table_html += "<tr>"
                for col in content['columns']:
                    value = row.get(col, '')
                    table_html += f"<td>{value if value is not None else ''}</td>"
                table_html += "</tr>"
            table_html += "</tbody></table>"
            kwargs['table_html'] = table_html
            
            if content.get('truncated'):
                kwargs['info_message'] = f"Showing first {content['max_rows']} rows of {content['total_rows']} total rows."
        
        return template.format(**kwargs)

templates = SimpleTemplates()
s3_handler = S3Handler()
file_viewers = FileViewers()

# HTML Template
HTML_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>Delta Viewer - S3/MinIO Browser</title>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }}
        .container {{ max-width: 1200px; margin: 0 auto; background: white; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
        .header {{ background: #2563eb; color: white; padding: 20px; border-radius: 8px 8px 0 0; }}
        .header h1 {{ margin: 0; }}
        .breadcrumb {{ padding: 15px 20px; background: #f8fafc; border-bottom: 1px solid #e2e8f0; }}
        .breadcrumb a {{ color: #2563eb; text-decoration: none; margin-right: 5px; }}
        .breadcrumb a:hover {{ text-decoration: underline; }}
        .content {{ padding: 20px; }}
        .file-list {{ list-style: none; padding: 0; margin: 0; }}
        .file-item {{ padding: 12px; border-bottom: 1px solid #e2e8f0; display: flex; align-items: center; }}
        .file-item:hover {{ background: #f8fafc; }}
        .file-icon {{ margin-right: 10px; font-size: 18px; }}
        .file-name {{ flex: 1; }}
        .file-name a {{ color: #1e293b; text-decoration: none; }}
        .file-name a:hover {{ color: #2563eb; }}
        .file-meta {{ color: #64748b; font-size: 12px; }}
        .table-container {{ overflow-x: auto; margin-top: 20px; }}
        table {{ width: 100%; border-collapse: collapse; }}
        th, td {{ padding: 8px 12px; text-align: left; border-bottom: 1px solid #e2e8f0; }}
        th {{ background: #f8fafc; font-weight: 600; }}
        .error {{ color: #dc2626; background: #fef2f2; padding: 15px; border-radius: 6px; border: 1px solid #fecaca; }}
        .info {{ color: #1d4ed8; background: #eff6ff; padding: 15px; border-radius: 6px; border: 1px solid #bfdbfe; margin-bottom: 20px; }}
        .delta-btn {{ background: #059669; color: white; padding: 8px 16px; border: none; border-radius: 6px; cursor: pointer; margin-left: 10px; }}
        .delta-btn:hover {{ background: #047857; }}
        .metadata {{ background: #f8fafc; padding: 15px; border-radius: 6px; margin-top: 20px; }}
        .metadata pre {{ margin: 0; font-size: 12px; overflow-x: auto; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🗂️ Delta Viewer</h1>
            <p>S3/MinIO File Browser and Delta Table Viewer</p>
        </div>
        
        <div class="breadcrumb">
            {breadcrumb_html}
            {delta_button}
        </div>
        
        <div class="content">
            {error_html}
            {info_html}
            {content_html}
        </div>
    </div>
    
    <script>
        function viewDeltaTable(path) {{
            window.location.href = '/delta?path=' + encodeURIComponent(path);
        }}
    </script>
</body>
</html>
'''

@app.get("/", response_class=HTMLResponse)
async def index(path: str = Query("")):
    """Main browser interface."""
    try:
        items = s3_handler.list_objects(path)
        
        # Build breadcrumbs
        breadcrumbs = []
        if path:
            parts = path.strip('/').split('/')
            current = ''
            for part in parts:
                if part:
                    current += part + '/'
                    breadcrumbs.append({'name': part, 'url': f'/?path={current}'})
        
        # Prepare template variables
        template_vars = {
            'items': items,
            'breadcrumbs': breadcrumbs,
            'breadcrumb_html': '<a href="/">🏠 Home</a>',
            'delta_button': '',
            'error_html': '',
            'info_html': '',
            'content_html': ''
        }
        
        # Build breadcrumb HTML
        if breadcrumbs:
            for crumb in breadcrumbs:
                template_vars['breadcrumb_html'] += f' / <a href="{crumb["url"]}">{crumb["name"]}</a>'
        
        # Add delta button if we have a path
        if path:
            template_vars['delta_button'] = f'<button class="delta-btn" onclick="viewDeltaTable(\'{path}\')">View as Delta Table</button>'
        
        # Build content HTML
        if items:
            content_html = '<ul class="file-list">'
            for item in items:
                icon = "📁" if item['type'] == 'folder' else "📄"
                if item['type'] == 'folder':
                    content_html += f'''
                    <li class="file-item">
                        <span class="file-icon">{icon}</span>
                        <div class="file-name">
                            <a href="/?path={quote(item['path'])}">{item['name']}/</a>
                        </div>
                    </li>'''
                else:
                    data_icon = "📊" if item['name'].endswith(('.csv', '.parquet', '.avro')) else ""
                    size_kb = item['size'] / 1024
                    content_html += f'''
                    <li class="file-item">
                        <span class="file-icon">{icon}</span>
                        <div class="file-name">
                            <a href="/view?file={quote(item['path'])}">{item['name']}</a>
                            {f'<span style="margin-left: 10px; color: #059669;">{data_icon}</span>' if data_icon else ''}
                        </div>
                        <div class="file-meta">
                            {size_kb:.2f} KB | {item['modified'][:10]}
                        </div>
                    </li>'''
            content_html += '</ul>'
            template_vars['content_html'] = content_html
        
        return templates.render(HTML_TEMPLATE, **template_vars)
    except Exception as e:
        logger.error(f"Error in index: {e}")
        template_vars = {
            'breadcrumb_html': '<a href="/">🏠 Home</a>',
            'delta_button': '',
            'error_html': f'<div class="error">{str(e)}</div>',
            'info_html': '',
            'content_html': ''
        }
        return templates.render(HTML_TEMPLATE, **template_vars)

@app.get("/view", response_class=HTMLResponse)
async def view_file(file: str = Query("")):
    """View individual file content."""
    try:
        if not file:
            raise HTTPException(status_code=400, detail="No file specified")
        
        content = s3_handler.get_object_content(file)
        
        # Determine file type and viewer
        if file.lower().endswith('.csv'):
            file_content = file_viewers.view_csv(content, Config.MAX_PREVIEW_ROWS)
        elif file.lower().endswith('.parquet'):
            file_content = file_viewers.view_parquet(content, Config.MAX_PREVIEW_ROWS)
        elif file.lower().endswith('.avro'):
            file_content = file_viewers.view_avro(content, Config.MAX_PREVIEW_ROWS)
        else:
            file_content = {'type': 'error', 'message': 'Unsupported file type'}
        
        # Build breadcrumbs for file view
        breadcrumbs = []
        parts = file.split('/')
        current = ''
        for part in parts[:-1]:
            if part:
                current += part + '/'
                breadcrumbs.append({'name': part, 'url': f'/?path={current}'})
        
        # Prepare template variables
        template_vars = {
            'breadcrumb_html': '<a href="/">🏠 Home</a>',
            'delta_button': '',
            'error_html': '',
            'info_html': '',
            'content_html': ''
        }
        
        # Build breadcrumb HTML
        if breadcrumbs:
            for crumb in breadcrumbs:
                template_vars['breadcrumb_html'] += f' / <a href="{crumb["url"]}">{crumb["name"]}</a>'
        
        if parts:
            template_vars['breadcrumb_html'] += f' / {parts[-1]}'
        
        # Handle file content
        if file_content['type'] == 'error':
            template_vars['error_html'] = f'<div class="error">{file_content["message"]}</div>'
        elif file_content['type'] in ['table', 'delta_table']:
            if file_content.get('truncated'):
                template_vars['info_html'] = f'<div class="info">Showing first {file_content["max_rows"]} rows of {file_content["total_rows"]} total rows.</div>'
            
            # Build table HTML
            table_html = '<div class="table-container"><table><thead><tr>'
            for col in file_content['columns']:
                table_html += f'<th>{col}</th>'
            table_html += '</tr></thead><tbody>'
            
            for row in file_content['data']:
                table_html += '<tr>'
                for col in file_content['columns']:
                    value = row.get(col, '')
                    table_html += f'<td>{value if value is not None else ""}</td>'
                table_html += '</tr>'
            table_html += '</tbody></table></div>'
            
            # Add metadata if present
            if file_content.get('schema') or file_content.get('metadata'):
                table_html += '<div class="metadata">'
                if file_content.get('metadata'):
                    table_html += f'<h3>Delta Table Metadata</h3><pre>{json.dumps(file_content["metadata"], indent=2)}</pre>'
                if file_content.get('schema'):
                    table_html += f'<h3>Schema</h3><pre>{file_content["schema"]}</pre>'
                table_html += '</div>'
            
            template_vars['content_html'] = table_html
        
        return templates.render(HTML_TEMPLATE, **template_vars)
    except Exception as e:
        logger.error(f"Error viewing file: {e}")
        template_vars = {
            'breadcrumb_html': '<a href="/">🏠 Home</a>',
            'delta_button': '',
            'error_html': f'<div class="error">{str(e)}</div>',
            'info_html': '',
            'content_html': ''
        }
        return templates.render(HTML_TEMPLATE, **template_vars)

@app.get("/delta", response_class=HTMLResponse)
async def view_delta(path: str = Query("")):
    """View folder as Delta table."""
    try:
        if not path:
            raise HTTPException(status_code=400, detail="No path specified")
        
        file_content = file_viewers.view_delta_table(path, Config.MAX_PREVIEW_ROWS)
        
        # Build breadcrumbs
        breadcrumbs = []
        if path:
            parts = path.strip('/').split('/')
            current = ''
            for part in parts:
                if part:
                    current += part + '/'
                    breadcrumbs.append({'name': part, 'url': f'/?path={current}'})
        
        # Prepare template variables
        template_vars = {
            'breadcrumb_html': '<a href="/">🏠 Home</a>',
            'delta_button': '',
            'error_html': '',
            'info_html': '',
            'content_html': ''
        }
        
        # Build breadcrumb HTML
        if breadcrumbs:
            for crumb in breadcrumbs:
                template_vars['breadcrumb_html'] += f' / <a href="{crumb["url"]}">{crumb["name"]}</a>'
        
        # Handle file content
        if file_content['type'] == 'error':
            template_vars['error_html'] = f'<div class="error">{file_content["message"]}</div>'
        elif file_content['type'] == 'delta_table':
            if file_content.get('truncated'):
                template_vars['info_html'] = f'<div class="info">Showing first {file_content["max_rows"]} rows of {file_content["total_rows"]} total rows.</div>'
            
            # Build table HTML
            table_html = '<div class="table-container"><table><thead><tr>'
            for col in file_content['columns']:
                table_html += f'<th>{col}</th>'
            table_html += '</tr></thead><tbody>'
            
            for row in file_content['data']:
                table_html += '<tr>'
                for col in file_content['columns']:
                    value = row.get(col, '')
                    table_html += f'<td>{value if value is not None else ""}</td>'
                table_html += '</tr>'
            table_html += '</tbody></table></div>'
            
            # Add metadata
            if file_content.get('metadata'):
                table_html += f'<div class="metadata"><h3>Delta Table Metadata</h3><pre>{json.dumps(file_content["metadata"], indent=2)}</pre></div>'
            
            template_vars['content_html'] = table_html
        
        return templates.render(HTML_TEMPLATE, **template_vars)
    except Exception as e:
        logger.error(f"Error viewing Delta table: {e}")
        template_vars = {
            'breadcrumb_html': '<a href="/">🏠 Home</a>',
            'delta_button': '',
            'error_html': f'<div class="error">{str(e)}</div>',
            'info_html': '',
            'content_html': ''
        }
        return templates.render(HTML_TEMPLATE, **template_vars)

@app.get("/health")
async def health_check():
    """Health check endpoint for monitoring."""
    try:
        # Test S3 connection
        s3_handler.s3_client.head_bucket(Bucket=Config.S3_BUCKET_NAME)
        return JSONResponse({'status': 'healthy', 'bucket': Config.S3_BUCKET_NAME})
    except Exception as e:
        return JSONResponse({'status': 'unhealthy', 'error': str(e)}, status_code=500)

if __name__ == '__main__':
    try:
        # Validate configuration
        if not all([Config.AWS_ACCESS_KEY_ID, Config.AWS_SECRET_ACCESS_KEY, Config.S3_BUCKET_NAME]):
            logger.error("Missing required configuration. Check your .env file.")
            exit(1)
        
        logger.info(f"Starting Delta Viewer on bucket: {Config.S3_BUCKET_NAME}")
        uvicorn.run(app, host="0.0.0.0", port=5000, log_level="info")
    except Exception as e:
        logger.error(f"Failed to start application: {e}")
        exit(1)
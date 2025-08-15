#!/usr/bin/env python3
"""
Delta Viewer - S3/MinIO File Browser and Delta Table Viewer
A minimal SRE tool for browsing cloud storage and viewing data files.
"""

import os
import json
import logging
import html
import xml.etree.ElementTree as ET
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
    def view_json(content: bytes, max_rows: int = 1000) -> Dict[str, Any]:
        """View JSON file content."""
        try:
            content_str = content.decode('utf-8')
            data = json.loads(content_str)
            
            # Handle different JSON structures
            if isinstance(data, list):
                # JSON array - treat as table if objects have consistent structure
                if data and isinstance(data[0], dict):
                    # Get all unique columns from all objects
                    all_columns = set()
                    for item in data:
                        if isinstance(item, dict):
                            all_columns.update(item.keys())
                    
                    columns = sorted(list(all_columns))
                    
                    # Limit rows
                    display_data = data[:max_rows] if len(data) > max_rows else data
                    truncated = len(data) > max_rows
                    
                    # Ensure all rows have all columns
                    normalized_data = []
                    for item in display_data:
                        if isinstance(item, dict):
                            normalized_row = {col: item.get(col, '') for col in columns}
                            normalized_data.append(normalized_row)
                    
                    return {
                        'type': 'table',
                        'data': normalized_data,
                        'columns': columns,
                        'total_rows': len(display_data),
                        'truncated': truncated,
                        'max_rows': max_rows,
                        'raw_json': json.dumps(data[:100] if len(data) > 100 else data, indent=2)  # Show first 100 items as raw
                    }
                else:
                    # JSON array of primitives or mixed types
                    return {
                        'type': 'json_raw',
                        'content': json.dumps(data, indent=2),
                        'size': len(data) if isinstance(data, list) else 1
                    }
            elif isinstance(data, dict):
                # Single JSON object - show as key-value pairs
                items = list(data.items())
                display_items = items[:max_rows] if len(items) > max_rows else items
                truncated = len(items) > max_rows
                
                table_data = [{'key': k, 'value': json.dumps(v) if isinstance(v, (dict, list)) else str(v)} 
                             for k, v in display_items]
                
                return {
                    'type': 'table',
                    'data': table_data,
                    'columns': ['key', 'value'],
                    'total_rows': len(display_items),
                    'truncated': truncated,
                    'max_rows': max_rows,
                    'raw_json': json.dumps(data, indent=2)
                }
            else:
                # Primitive JSON value
                return {
                    'type': 'json_raw',
                    'content': json.dumps(data, indent=2),
                    'size': 1
                }
                
        except json.JSONDecodeError as e:
            return {'type': 'error', 'message': f"Invalid JSON format: {e}"}
        except UnicodeDecodeError as e:
            return {'type': 'error', 'message': f"Unable to decode file as UTF-8: {e}"}
        except Exception as e:
            return {'type': 'error', 'message': f"Error reading JSON: {e}"}

    @staticmethod
    def view_xml(content: bytes, max_rows: int = 1000) -> Dict[str, Any]:
        """View XML file content."""
        try:
            content_str = content.decode('utf-8')
            root = ET.fromstring(content_str)
            
            # Try to detect if XML is structured data (like a list of records)
            children = list(root)
            
            if children and len(children) > 0:
                # Check if all children have the same tag (indicating a list of similar records)
                first_child_tag = children[0].tag
                if all(child.tag == first_child_tag for child in children[:10]):  # Check first 10 for performance
                    # This looks like structured data - convert to table
                    records = []
                    all_columns = set()
                    
                    # Collect all possible columns from first few records
                    sample_size = min(50, len(children))
                    for child in children[:sample_size]:
                        record = {}
                        # Handle attributes
                        for attr, value in child.attrib.items():
                            attr_key = f"@{attr}"
                            record[attr_key] = value
                            all_columns.add(attr_key)
                        
                        # Handle child elements and text content
                        if child.text and child.text.strip():
                            record['_text'] = child.text.strip()
                            all_columns.add('_text')
                        
                        for sub_elem in child:
                            if sub_elem.text and sub_elem.text.strip():
                                record[sub_elem.tag] = sub_elem.text.strip()
                                all_columns.add(sub_elem.tag)
                            # Include attributes of sub-elements
                            for attr, value in sub_elem.attrib.items():
                                attr_key = f"{sub_elem.tag}@{attr}"
                                record[attr_key] = value
                                all_columns.add(attr_key)
                    
                    columns = sorted(list(all_columns))
                    
                    # Process records up to max_rows
                    display_children = children[:max_rows] if len(children) > max_rows else children
                    truncated = len(children) > max_rows
                    
                    for child in display_children:
                        record = {col: '' for col in columns}  # Initialize with empty values
                        
                        # Handle attributes
                        for attr, value in child.attrib.items():
                            attr_key = f"@{attr}"
                            if attr_key in record:
                                record[attr_key] = value
                        
                        # Handle text content
                        if child.text and child.text.strip():
                            if '_text' in record:
                                record['_text'] = child.text.strip()
                        
                        # Handle child elements
                        for sub_elem in child:
                            if sub_elem.text and sub_elem.text.strip():
                                if sub_elem.tag in record:
                                    record[sub_elem.tag] = sub_elem.text.strip()
                            # Include attributes of sub-elements
                            for attr, value in sub_elem.attrib.items():
                                attr_key = f"{sub_elem.tag}@{attr}"
                                if attr_key in record:
                                    record[attr_key] = value
                        
                        records.append(record)
                    
                    return {
                        'type': 'table',
                        'data': records,
                        'columns': columns,
                        'total_rows': len(records),
                        'truncated': truncated,
                        'max_rows': max_rows,
                        'xml_info': {
                            'root_tag': root.tag,
                            'total_elements': len(children),
                            'element_type': first_child_tag
                        }
                    }
                else:
                    # Mixed structure - show as formatted XML
                    return {
                        'type': 'xml_raw',
                        'content': content_str,
                        'xml_info': {
                            'root_tag': root.tag,
                            'child_count': len(children),
                            'structure': 'mixed'
                        }
                    }
            else:
                # Simple XML or single element - show formatted
                return {
                    'type': 'xml_raw',
                    'content': content_str,
                    'xml_info': {
                        'root_tag': root.tag,
                        'has_text': bool(root.text and root.text.strip()),
                        'structure': 'simple'
                    }
                }
                
        except ET.ParseError as e:
            return {'type': 'error', 'message': f"Invalid XML format: {e}"}
        except UnicodeDecodeError as e:
            return {'type': 'error', 'message': f"Unable to decode file as UTF-8: {e}"}
        except Exception as e:
            return {'type': 'error', 'message': f"Error reading XML: {e}"}

    @staticmethod
    def view_raw(content: bytes, file_path: str, max_bytes: int = 50000) -> Dict[str, Any]:
        """View raw file content for unknown file types."""
        try:
            file_size = len(content)
            file_name = file_path.split('/')[-1]
            file_ext = file_name.split('.')[-1].lower() if '.' in file_name else 'unknown'
            
            # Truncate content if too large
            display_content = content[:max_bytes] if len(content) > max_bytes else content
            truncated = len(content) > max_bytes
            
            # Try to decode as text first
            try:
                text_content = display_content.decode('utf-8')
                content_type = 'text'
                # Check for common text file patterns
                if any(keyword in text_content.lower() for keyword in ['<html', '<?xml', 'def ', 'class ', 'import ', 'function']):
                    if '<html' in text_content.lower():
                        content_type = 'html'
                    elif '<?xml' in text_content.lower():
                        content_type = 'xml'
                    elif any(keyword in text_content for keyword in ['def ', 'import ', 'class ']):
                        content_type = 'code'
                    else:
                        content_type = 'text'
                
                return {
                    'type': 'raw_text',
                    'content': text_content,
                    'file_info': {
                        'name': file_name,
                        'extension': file_ext,
                        'size_bytes': file_size,
                        'content_type': content_type,
                        'truncated': truncated,
                        'max_bytes': max_bytes
                    }
                }
            except UnicodeDecodeError:
                # Binary content - show hex dump
                hex_lines = []
                for i in range(0, min(len(display_content), 1024), 16):  # Show first 1KB as hex
                    chunk = display_content[i:i+16]
                    hex_part = ' '.join(f'{b:02x}' for b in chunk)
                    ascii_part = ''.join(chr(b) if 32 <= b <= 126 else '.' for b in chunk)
                    hex_lines.append(f'{i:08x}  {hex_part:<48} |{ascii_part}|')
                
                return {
                    'type': 'raw_binary',
                    'content': '\n'.join(hex_lines),
                    'file_info': {
                        'name': file_name,
                        'extension': file_ext,
                        'size_bytes': file_size,
                        'content_type': 'binary',
                        'truncated': truncated,
                        'max_bytes': max_bytes,
                        'hex_preview_bytes': min(len(display_content), 1024)
                    }
                }
                
        except Exception as e:
            return {'type': 'error', 'message': f"Error reading file: {e}"}

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
                    # Show different icons for different file types
                    if item['name'].endswith(('.csv', '.parquet', '.avro', '.json', '.xml')):
                        data_icon = "📊"  # Data file icon
                    elif item['name'].endswith(('.txt', '.log', '.md', '.py', '.js', '.html', '.css', '.sql', '.yaml', '.yml')):
                        data_icon = "📝"  # Text file icon
                    else:
                        data_icon = "🔍"  # Unknown/viewable file icon
                    
                    size_kb = item['size'] / 1024
                    content_html += f'''
                    <li class="file-item">
                        <span class="file-icon">{icon}</span>
                        <div class="file-name">
                            <a href="/view?file={quote(item['path'])}">{item['name']}</a>
                            <span style="margin-left: 10px; color: #059669;">{data_icon}</span>
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
        elif file.lower().endswith('.json'):
            file_content = file_viewers.view_json(content, Config.MAX_PREVIEW_ROWS)
        elif file.lower().endswith('.xml'):
            file_content = file_viewers.view_xml(content, Config.MAX_PREVIEW_ROWS)
        else:
            # For unknown file types, show raw content
            file_content = file_viewers.view_raw(content, file)
        
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
        elif file_content['type'] == 'json_raw':
            # Show raw JSON content
            escaped_content = html.escape(file_content["content"])
            template_vars['content_html'] = f'''
            <div class="info">JSON content ({file_content.get("size", 1)} items)</div>
            <div class="metadata">
                <h3>JSON Content</h3>
                <pre>{escaped_content}</pre>
            </div>'''
        elif file_content['type'] == 'xml_raw':
            # Show raw XML content
            xml_info = file_content.get('xml_info', {})
            info_text = f"XML document (root: {xml_info.get('root_tag', 'unknown')})"
            if 'child_count' in xml_info:
                info_text += f", {xml_info['child_count']} children"
            if 'structure' in xml_info:
                info_text += f", {xml_info['structure']} structure"
            
            escaped_content = html.escape(file_content["content"])
            template_vars['content_html'] = f'''
            <div class="info">{info_text}</div>
            <div class="metadata">
                <h3>XML Content</h3>
                <pre>{escaped_content}</pre>
            </div>'''
        elif file_content['type'] == 'raw_text':
            # Show raw text content
            file_info = file_content.get('file_info', {})
            info_text = f"Text file ({file_info.get('content_type', 'text')})"
            if file_info.get('size_bytes'):
                size_kb = file_info['size_bytes'] / 1024
                info_text += f" - {size_kb:.1f} KB"
            if file_info.get('truncated'):
                info_text += f" (showing first {file_info.get('max_bytes', 50000)} bytes)"
            
            escaped_content = html.escape(file_content["content"])
            template_vars['content_html'] = f'''
            <div class="info">{info_text}</div>
            <div class="metadata">
                <h3>File Content</h3>
                <pre style="white-space: pre-wrap; word-wrap: break-word;">{escaped_content}</pre>
            </div>'''
        elif file_content['type'] == 'raw_binary':
            # Show binary content as hex dump
            file_info = file_content.get('file_info', {})
            size_kb = file_info.get('size_bytes', 0) / 1024
            info_text = f"Binary file ({file_info.get('extension', 'unknown')} format) - {size_kb:.1f} KB"
            if file_info.get('truncated'):
                info_text += f" (showing hex dump of first {file_info.get('hex_preview_bytes', 1024)} bytes)"
            
            template_vars['content_html'] = f'''
            <div class="info">{info_text}</div>
            <div class="metadata">
                <h3>Hex Dump</h3>
                <pre style="font-family: 'Courier New', monospace; font-size: 12px;">{file_content["content"]}</pre>
            </div>'''
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
                    escaped_value = html.escape(str(value) if value is not None else "")
                    table_html += f'<td>{escaped_value}</td>'
                table_html += '</tr>'
            table_html += '</tbody></table></div>'
            
            # Add metadata if present
            if file_content.get('schema') or file_content.get('metadata') or file_content.get('raw_json') or file_content.get('xml_info'):
                table_html += '<div class="metadata">'
                if file_content.get('metadata'):
                    table_html += f'<h3>Delta Table Metadata</h3><pre>{json.dumps(file_content["metadata"], indent=2)}</pre>'
                if file_content.get('schema'):
                    escaped_schema = html.escape(file_content["schema"])
                    table_html += f'<h3>Schema</h3><pre>{escaped_schema}</pre>'
                if file_content.get('raw_json'):
                    escaped_json = html.escape(file_content["raw_json"])
                    table_html += f'<h3>Raw JSON (sample)</h3><pre>{escaped_json}</pre>'
                if file_content.get('xml_info'):
                    escaped_xml_info = html.escape(json.dumps(file_content["xml_info"], indent=2))
                    table_html += f'<h3>XML Structure Info</h3><pre>{escaped_xml_info}</pre>'
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
                    escaped_value = html.escape(str(value) if value is not None else "")
                    table_html += f'<td>{escaped_value}</td>'
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
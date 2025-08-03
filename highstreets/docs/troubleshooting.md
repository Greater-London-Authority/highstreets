# Troubleshooting Guide

This guide covers common issues, solutions, and debugging techniques for the Highstreets package. Use this guide to quickly diagnose and resolve problems.

## Table of Contents

- [Quick Diagnostics](#quick-diagnostics)
- [Installation Issues](#installation-issues)
- [Database Connection Issues](#database-connection-issues)
- [API Authentication Issues](#api-authentication-issues)
- [Data Processing Issues](#data-processing-issues)
- [Performance Issues](#performance-issues)
- [Memory Issues](#memory-issues)
- [File System Issues](#file-system-issues)
- [Sub-licensing Issues](#sub-licensing-issues)
- [Logging and Debugging](#logging-and-debugging)
- [Getting Help](#getting-help)

## Quick Diagnostics

### System Health Check

Run this script to quickly check the health of your Highstreets installation:

```python
"""
Quick system health check for Highstreets package
"""
import os
import sys
import logging
from datetime import datetime

def run_health_check():
    """Run comprehensive health check"""
    
    results = {
        'timestamp': datetime.now(),
        'python_version': sys.version,
        'checks': {}
    }
    
    print("🔍 Running Highstreets Health Check...\n")
    
    # 1. Check Python version
    print("1. Python Version Check")
    if sys.version_info >= (3, 8):
        print(f"   ✓ Python {sys.version_info.major}.{sys.version_info.minor} (compatible)")
        results['checks']['python'] = True
    else:
        print(f"   ✗ Python {sys.version_info.major}.{sys.version_info.minor} (requires 3.8+)")
        results['checks']['python'] = False
    
    # 2. Check package imports
    print("\n2. Package Import Check")
    import_tests = {
        'highstreets.data_source_sink.dataloader': 'DataLoader',
        'highstreets.data_source_sink.datawriter': 'DataWriter',
        'highstreets.data_transformation.hextransform': 'HexTransform',
        'highstreets.api.clientbase': 'APIClient',
        'highstreets.core.sublicense_manager': 'SublicenseManager'
    }
    
    for module, class_name in import_tests.items():
        try:
            mod = __import__(module, fromlist=[class_name])
            getattr(mod, class_name)
            print(f"   ✓ {module}")
            results['checks'][f'import_{module}'] = True
        except Exception as e:
            print(f"   ✗ {module}: {e}")
            results['checks'][f'import_{module}'] = False
    
    # 3. Check environment variables
    print("\n3. Environment Variables Check")
    required_vars = [
        'PG_HOST', 'PG_DATABASE', 'PG_USER', 'PG_PASSWORD',
        'CONSUMER_KEY', 'CONSUMER_SECRET', 'BASE_DIR'
    ]
    
    for var in required_vars:
        if os.getenv(var):
            print(f"   ✓ {var}")
            results['checks'][f'env_{var}'] = True
        else:
            print(f"   ✗ {var} (not set)")
            results['checks'][f'env_{var}'] = False
    
    # 4. Check database connection
    print("\n4. Database Connection Check")
    try:
        from highstreets.data_source_sink.dataloader import DataLoader
        loader = DataLoader()
        # Try a simple query
        test_query = "SELECT 1 as test"
        with loader.engine.connect() as conn:
            result = conn.execute(test_query)
            print("   ✓ Database connection successful")
            results['checks']['database'] = True
    except Exception as e:
        print(f"   ✗ Database connection failed: {e}")
        results['checks']['database'] = False
    
    # 5. Check API connection
    print("\n5. API Connection Check")
    try:
        from highstreets.api.clientbase import APIClient
        client = APIClient()
        token = client.get_access_token()
        if token:
            print("   ✓ API authentication successful")
            results['checks']['api'] = True
        else:
            print("   ✗ API authentication failed: No token received")
            results['checks']['api'] = False
    except Exception as e:
        print(f"   ✗ API authentication failed: {e}")
        results['checks']['api'] = False
    
    # 6. Check file system access
    print("\n6. File System Access Check")
    base_dir = os.getenv('BASE_DIR')
    if base_dir:
        try:
            if os.path.exists(base_dir):
                test_file = os.path.join(base_dir, 'test_write.tmp')
                with open(test_file, 'w') as f:
                    f.write('test')
                os.remove(test_file)
                print(f"   ✓ File system access: {base_dir}")
                results['checks']['filesystem'] = True
            else:
                print(f"   ✗ Base directory does not exist: {base_dir}")
                results['checks']['filesystem'] = False
        except Exception as e:
            print(f"   ✗ File system access failed: {e}")
            results['checks']['filesystem'] = False
    else:
        print("   ⚠ BASE_DIR not set, skipping file system check")
        results['checks']['filesystem'] = None
    
    # Summary
    print("\n" + "="*50)
    print("HEALTH CHECK SUMMARY")
    print("="*50)
    
    passed = sum(1 for v in results['checks'].values() if v is True)
    failed = sum(1 for v in results['checks'].values() if v is False)
    skipped = sum(1 for v in results['checks'].values() if v is None)
    total = len(results['checks'])
    
    print(f"Checks passed: {passed}/{total}")
    print(f"Checks failed: {failed}/{total}")
    print(f"Checks skipped: {skipped}/{total}")
    
    if failed == 0:
        print("\n🎉 All checks passed! Your system is ready.")
    else:
        print(f"\n⚠️  {failed} checks failed. Please review the issues above.")
    
    return results

if __name__ == "__main__":
    health_results = run_health_check()
```

## Installation Issues

### Common Installation Problems

#### 1. Poetry Installation Issues

**Problem**: Poetry command not found or installation fails

**Solutions**:
```bash
# Reinstall Poetry
curl -sSL https://install.python-poetry.org | python3 -

# Add Poetry to PATH (Linux/macOS)
export PATH="$HOME/.local/bin:$PATH"

# Windows PowerShell
$env:PATH += ";$HOME\.local\bin"

# Verify Poetry installation
poetry --version
```

#### 2. Dependency Conflicts

**Problem**: Package dependency conflicts during installation

**Solutions**:
```bash
# Clear Poetry cache
poetry cache clear pypi --all

# Remove virtual environment and reinstall
poetry env remove python
poetry install

# Install with verbose output for debugging
poetry install -vvv

# Use specific Python version
poetry env use python3.9
poetry install
```

#### 3. System Dependencies Missing

**Problem**: Missing system-level dependencies (Ubuntu/Debian)

**Solutions**:
```bash
# Install essential build tools
sudo apt update
sudo apt install build-essential python3-dev

# Install PostgreSQL development libraries
sudo apt install libpq-dev

# Install GDAL for geographic processing
sudo apt install gdal-bin libgdal-dev

# Install additional Python libraries
sudo apt install python3-pip python3-venv
```

**Problem**: Missing system dependencies (macOS)

**Solutions**:
```bash
# Install Homebrew if not installed
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install dependencies
brew install postgresql
brew install gdal
brew install python@3.9

# Update PATH
echo 'export PATH="/opt/homebrew/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

#### 4. Permission Issues

**Problem**: Permission denied during installation

**Solutions**:
```bash
# Use user installation
pip install --user -e .

# Fix ownership of Python directories
sudo chown -R $(whoami) ~/.local

# Use virtual environment
python -m venv venv
source venv/bin/activate  # Linux/macOS
# or
venv\Scripts\activate  # Windows

pip install -e .
```

## Database Connection Issues

### PostgreSQL Connection Problems

#### 1. Connection Refused

**Problem**: `psycopg2.OperationalError: could not connect to server`

**Diagnosis**:
```python
import psycopg2
import os

def diagnose_db_connection():
    """Diagnose database connection issues"""
    
    # Check environment variables
    required_vars = ['PG_HOST', 'PG_PORT', 'PG_DATABASE', 'PG_USER', 'PG_PASSWORD']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"Missing environment variables: {missing_vars}")
        return
    
    # Test connection with detailed error information
    try:
        conn = psycopg2.connect(
            host=os.getenv('PG_HOST'),
            port=os.getenv('PG_PORT', 5432),
            database=os.getenv('PG_DATABASE'),
            user=os.getenv('PG_USER'),
            password=os.getenv('PG_PASSWORD'),
            connect_timeout=10
        )
        print("✓ Database connection successful")
        conn.close()
        
    except psycopg2.OperationalError as e:
        error_msg = str(e)
        
        if "could not connect to server" in error_msg:
            print("✗ Server connection failed")
            print("  Check: Server is running, host/port are correct")
            
        elif "authentication failed" in error_msg:
            print("✗ Authentication failed")
            print("  Check: Username and password are correct")
            
        elif "database" in error_msg and "does not exist" in error_msg:
            print("✗ Database does not exist")
            print("  Check: Database name is correct and database exists")
            
        else:
            print(f"✗ Connection error: {error_msg}")

diagnose_db_connection()
```

**Solutions**:
```bash
# Check if PostgreSQL is running
sudo systemctl status postgresql  # Linux
brew services list | grep postgres  # macOS

# Start PostgreSQL service
sudo systemctl start postgresql  # Linux
brew services start postgresql  # macOS

# Check if port is accessible
telnet localhost 5432
# or
nc -zv localhost 5432

# Test connection with psql
psql -h localhost -p 5432 -U your_user -d your_database
```

#### 2. Authentication Failed

**Problem**: Password authentication failed

**Solutions**:
```bash
# Reset PostgreSQL password
sudo -u postgres psql
ALTER USER your_username WITH PASSWORD 'new_password';
\q

# Check pg_hba.conf authentication method
sudo nano /etc/postgresql/*/main/pg_hba.conf

# Ensure line exists for your connection:
# local   all   your_user   md5
# host    all   your_user   127.0.0.1/32   md5

# Restart PostgreSQL after changes
sudo systemctl restart postgresql
```

#### 3. Database Does Not Exist

**Problem**: Database specified in configuration doesn't exist

**Solutions**:
```bash
# Connect as postgres user
sudo -u postgres psql

# Create database
CREATE DATABASE highstreets;

# Create user with permissions
CREATE USER highstreets_user WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE highstreets TO highstreets_user;

# Enable PostGIS if needed
\c highstreets
CREATE EXTENSION postgis;
\q
```

## API Authentication Issues

### BT API Connection Problems

#### 1. Invalid Credentials

**Problem**: 401 Unauthorized or 403 Forbidden responses

**Diagnosis**:
```python
def diagnose_api_auth():
    """Diagnose API authentication issues"""
    
    import os
    import base64
    import requests
    
    # Check environment variables
    consumer_key = os.getenv('CONSUMER_KEY')
    consumer_secret = os.getenv('CONSUMER_SECRET')
    
    if not consumer_key or not consumer_secret:
        print("✗ Missing API credentials")
        print("  Set CONSUMER_KEY and CONSUMER_SECRET environment variables")
        return
    
    # Test authentication
    try:
        # Encode credentials
        auth_header = base64.b64encode(
            f"{consumer_key}:{consumer_secret}".encode()
        ).decode()
        
        headers = {"Authorization": f"Basic {auth_header}"}
        params = {"grant_type": "client_credentials"}
        
        response = requests.post(
            "https://api.business.bt.com/oauth/accesstoken",
            headers=headers,
            params=params,
            timeout=30
        )
        
        if response.status_code == 200:
            token = response.json().get("accessToken")
            if token:
                print(f"✓ API authentication successful")
                print(f"  Token preview: {token[:20]}...")
            else:
                print("✗ No token in response")
                print(f"  Response: {response.text}")
        else:
            print(f"✗ API authentication failed: {response.status_code}")
            print(f"  Response: {response.text}")
            
    except requests.exceptions.RequestException as e:
        print(f"✗ Request failed: {e}")

diagnose_api_auth()
```

**Solutions**:
```bash
# Verify credentials in .env file
cat .env | grep -E "(CONSUMER_KEY|CONSUMER_SECRET)"

# Test credentials manually
curl -X POST "https://api.business.bt.com/oauth/accesstoken" \
  -H "Authorization: Basic $(echo -n 'key:secret' | base64)" \
  -d "grant_type=client_credentials"

# Check for special characters in credentials
# Ensure no extra spaces or newlines in .env file
```

#### 2. Network Connectivity Issues

**Problem**: Connection timeouts or network errors

**Solutions**:
```bash
# Test basic connectivity
ping api.business.bt.com

# Test HTTPS connectivity
curl -I https://api.business.bt.com

# Check firewall/proxy settings
echo $https_proxy
echo $HTTPS_PROXY

# Test with proxy if needed
export https_proxy=http://proxy.company.com:8080
```

#### 3. Rate Limiting

**Problem**: 429 Too Many Requests

**Solutions**:
```python
def handle_rate_limiting():
    """Handle API rate limiting"""
    
    import time
    import requests
    from datetime import datetime
    
    def api_request_with_retry(url, headers, params, max_retries=3):
        """Make API request with exponential backoff"""
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=headers, params=params)
                
                if response.status_code == 200:
                    return response.json()
                    
                elif response.status_code == 429:
                    # Rate limited
                    retry_after = int(response.headers.get('Retry-After', 60))
                    print(f"Rate limited. Waiting {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue
                    
                else:
                    print(f"Request failed: {response.status_code}")
                    return None
                    
            except Exception as e:
                wait_time = 2 ** attempt
                print(f"Attempt {attempt + 1} failed: {e}")
                print(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
        
        return None
```

## Data Processing Issues

### Data Transformation Problems

#### 1. Schema Validation Errors

**Problem**: Data doesn't match expected schema

**Diagnosis**:
```python
def diagnose_schema_issues(data):
    """Diagnose data schema issues"""
    
    import pandas as pd
    from highstreets.data.schema import validate_bt_hex_raw
    
    try:
        # Validate against schema
        validated_data = validate_bt_hex_raw(data)
        print("✓ Schema validation passed")
        return validated_data
        
    except Exception as e:
        print(f"✗ Schema validation failed: {e}")
        
        # Analyze data structure
        if isinstance(data, pd.DataFrame):
            print("\nData Analysis:")
            print(f"  Shape: {data.shape}")
            print(f"  Columns: {list(data.columns)}")
            print(f"  Data types:")
            for col, dtype in data.dtypes.items():
                print(f"    {col}: {dtype}")
            
            # Check for missing values
            missing = data.isnull().sum()
            if missing.any():
                print(f"  Missing values:")
                for col, count in missing[missing > 0].items():
                    print(f"    {col}: {count}")
        
        return None
```

**Solutions**:
```python
def fix_common_schema_issues(data):
    """Fix common data schema issues"""
    
    import pandas as pd
    
    # Make a copy to avoid modifying original
    fixed_data = data.copy()
    
    # 1. Fix date columns
    if 'count_date' in fixed_data.columns:
        fixed_data['count_date'] = pd.to_datetime(fixed_data['count_date'])
    
    # 2. Fix numeric columns
    numeric_columns = ['resident', 'visitor', 'worker', 'loyalty_percentage', 'dwell_time']
    for col in numeric_columns:
        if col in fixed_data.columns:
            fixed_data[col] = pd.to_numeric(fixed_data[col], errors='coerce')
    
    # 3. Fix categorical columns
    if 'hours' in fixed_data.columns:
        valid_hours = ["00-03", "03-06", "06-09", "09-12", "12-15", "15-18", "18-21", "21-00"]
        fixed_data = fixed_data[fixed_data['hours'].isin(valid_hours)]
    
    # 4. Remove null values in required columns
    required_columns = ['hex_id', 'count_date', 'hours']
    for col in required_columns:
        if col in fixed_data.columns:
            fixed_data = fixed_data.dropna(subset=[col])
    
    return fixed_data
```

#### 2. Missing Data Issues

**Problem**: API returns empty results or null values

**Solutions**:
```python
def handle_missing_data():
    """Handle missing data in API responses"""
    
    from highstreets.data_source_sink.dataloader import DataLoader
    from datetime import datetime, timedelta
    
    loader = DataLoader()
    
    # Check data availability for recent dates
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=7)
    
    date_range = pd.date_range(start_date, end_date)
    
    print("Checking data availability:")
    for date in date_range:
        date_str = date.strftime('%Y-%m-%d')
        try:
            data = loader.get_hex_data(date_str, date_str)
            if data:
                print(f"  {date_str}: ✓ {len(data)} records")
            else:
                print(f"  {date_str}: ✗ No data")
        except Exception as e:
            print(f"  {date_str}: ✗ Error: {e}")
```

## Performance Issues

### Slow Query Performance

#### 1. Database Query Optimization

**Problem**: Queries taking too long

**Diagnosis**:
```sql
-- Check slow queries
SELECT query, calls, mean_time, total_time
FROM pg_stat_statements
WHERE mean_time > 1000
ORDER BY mean_time DESC;

-- Analyze specific query
EXPLAIN ANALYZE SELECT * FROM bt_footfall_tfl_hex_3hourly 
WHERE count_date BETWEEN '2023-01-01' AND '2023-12-31';
```

**Solutions**:
```sql
-- Create indexes for common queries
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_bt_hex_date 
ON bt_footfall_tfl_hex_3hourly (count_date);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_bt_hex_hex_id 
ON bt_footfall_tfl_hex_3hourly (hex_id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_bt_hex_composite 
ON bt_footfall_tfl_hex_3hourly (hex_id, count_date, hours);

-- Update table statistics
ANALYZE bt_footfall_tfl_hex_3hourly;
```

#### 2. Memory Usage Optimization

**Problem**: High memory usage during processing

**Solutions**:
```python
def optimize_memory_usage():
    """Optimize memory usage for large datasets"""
    
    import pandas as pd
    import gc
    
    def process_in_chunks(data_loader, table_name, chunk_size=10000):
        """Process large tables in chunks"""
        
        # Get total row count
        total_rows = pd.read_sql(
            f"SELECT COUNT(*) as count FROM {table_name}", 
            data_loader.engine
        )['count'].iloc[0]
        
        print(f"Processing {total_rows} rows in chunks of {chunk_size}")
        
        for offset in range(0, total_rows, chunk_size):
            # Load chunk
            chunk_query = f"""
            SELECT * FROM {table_name} 
            ORDER BY count_date 
            LIMIT {chunk_size} OFFSET {offset}
            """
            
            chunk = pd.read_sql(chunk_query, data_loader.engine)
            
            # Process chunk
            yield chunk
            
            # Force garbage collection
            del chunk
            gc.collect()
    
    def optimize_dataframe_memory(df):
        """Optimize DataFrame memory usage"""
        
        # Downcast numeric types
        for col in df.select_dtypes(include=['int64']).columns:
            df[col] = pd.to_numeric(df[col], downcast='integer')
        
        for col in df.select_dtypes(include=['float64']).columns:
            df[col] = pd.to_numeric(df[col], downcast='float')
        
        # Convert strings to categories where appropriate
        for col in df.select_dtypes(include=['object']).columns:
            if df[col].nunique() / len(df) < 0.5:
                df[col] = df[col].astype('category')
        
        return df
```

## Memory Issues

### Out of Memory Errors

#### 1. Large Dataset Processing

**Problem**: `MemoryError` when processing large datasets

**Solutions**:
```python
def handle_large_datasets():
    """Handle large datasets without memory issues"""
    
    import pandas as pd
    from highstreets.data_source_sink.dataloader import DataLoader
    
    def chunked_processing_example():
        """Example of chunked processing"""
        
        loader = DataLoader()
        
        # Process data in date chunks
        start_date = '2023-01-01'
        end_date = '2023-12-31'
        
        date_range = pd.date_range(start_date, end_date, freq='W')  # Weekly chunks
        
        for i in range(len(date_range) - 1):
            chunk_start = date_range[i].strftime('%Y-%m-%d')
            chunk_end = date_range[i + 1].strftime('%Y-%m-%d')
            
            print(f"Processing {chunk_start} to {chunk_end}")
            
            # Process chunk
            try:
                chunk_data = loader.get_hex_data(chunk_start, chunk_end)
                # Process and save chunk
                # ... processing logic here ...
                
                # Clear memory
                del chunk_data
                
            except MemoryError:
                print(f"Memory error processing {chunk_start}-{chunk_end}")
                print("Try reducing chunk size")
                break
```

#### 2. Memory Monitoring

**Problem**: Need to monitor memory usage

**Solutions**:
```python
def monitor_memory_usage():
    """Monitor memory usage during processing"""
    
    import psutil
    import os
    
    def get_memory_usage():
        """Get current memory usage"""
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        
        return {
            'rss': memory_info.rss / 1024 / 1024,  # MB
            'vms': memory_info.vms / 1024 / 1024,  # MB
            'percent': process.memory_percent()
        }
    
    def memory_monitor_decorator(func):
        """Decorator to monitor memory usage of functions"""
        
        def wrapper(*args, **kwargs):
            print(f"Starting {func.__name__}")
            start_memory = get_memory_usage()
            print(f"  Initial memory: {start_memory['rss']:.1f} MB")
            
            result = func(*args, **kwargs)
            
            end_memory = get_memory_usage()
            print(f"  Final memory: {end_memory['rss']:.1f} MB")
            print(f"  Memory change: {end_memory['rss'] - start_memory['rss']:.1f} MB")
            
            return result
        
        return wrapper
    
    return memory_monitor_decorator
```

## File System Issues

### Permission Problems

#### 1. File Access Denied

**Problem**: Permission denied when reading/writing files

**Solutions**:
```bash
# Check file permissions
ls -la /path/to/your/data/directory

# Fix permissions
chmod 755 /path/to/your/data/directory
chmod 644 /path/to/your/data/files/*

# Change ownership if needed
sudo chown -R $(whoami):$(whoami) /path/to/your/data

# Set umask for new files
umask 022
```

#### 2. Disk Space Issues

**Problem**: No space left on device

**Solutions**:
```bash
# Check disk usage
df -h

# Check directory sizes
du -sh /path/to/data/*

# Find large files
find /path/to/data -type f -size +1G -ls

# Clean up temporary files
rm -rf /tmp/highstreets*
```

## Sub-licensing Issues

### Configuration Problems

#### 1. YAML Syntax Errors

**Problem**: Invalid YAML configuration

**Diagnosis**:
```python
def validate_yaml_config():
    """Validate YAML configuration file"""
    
    import yaml
    import os
    
    config_path = 'highstreets/core/settings/sublicenses.yaml'
    
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
            print("✓ YAML syntax is valid")
            
            # Check required sections
            required_sections = ['config', 'sublicenses']
            for section in required_sections:
                if section in config:
                    print(f"✓ {section} section found")
                else:
                    print(f"✗ {section} section missing")
            
            return config
            
    except yaml.YAMLError as e:
        print(f"✗ YAML syntax error: {e}")
        return None
    except FileNotFoundError:
        print(f"✗ Configuration file not found: {config_path}")
        return None

validate_yaml_config()
```

#### 2. Query Template Issues

**Problem**: SQL query templates failing

**Solutions**:
```python
def debug_query_templates():
    """Debug SQL query template issues"""
    
    from highstreets.core.sublicense_manager import SublicenseManager
    
    manager = SublicenseManager()
    
    # Test query template formatting
    sublicense_name = "colliers-hsds"
    config = manager.sublicenses.get(sublicense_name)
    
    if config:
        for template_name, template_query in config.get('query_templates', {}).items():
            try:
                # Try to format the query
                formatted_query = manager._build_query_from_template(
                    template_query, config
                )
                print(f"✓ {template_name} template is valid")
                
            except Exception as e:
                print(f"✗ {template_name} template error: {e}")
                print(f"  Template: {template_query}")
```

## Logging and Debugging

### Enable Debug Logging

```python
def enable_debug_logging():
    """Enable detailed debug logging"""
    
    import logging
    import os
    
    # Set debug level
    os.environ['LOG_LEVEL'] = 'DEBUG'
    
    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('highstreets_debug.log'),
            logging.StreamHandler()
        ]
    )
    
    # Enable SQL query logging
    os.environ['ECHO_SQL'] = 'true'
    
    print("Debug logging enabled")
    print("Check 'highstreets_debug.log' for detailed logs")

enable_debug_logging()
```

### Common Debug Techniques

```python
def debug_data_flow():
    """Debug data flow through the system"""
    
    import pandas as pd
    from highstreets.data_source_sink.dataloader import DataLoader
    
    loader = DataLoader()
    
    # Debug: Check data at each step
    try:
        # Step 1: Raw API data
        raw_data = loader.get_hex_data("2023-12-01", "2023-12-01")
        print(f"Raw data: {len(raw_data)} records")
        
        if raw_data:
            print(f"Sample record: {raw_data[0]}")
        
        # Step 2: DataFrame conversion
        if raw_data:
            df = pd.DataFrame(raw_data)
            print(f"DataFrame shape: {df.shape}")
            print(f"Columns: {list(df.columns)}")
            print(f"Data types: {df.dtypes.to_dict()}")
        
        # Step 3: Check for anomalies
        if not df.empty:
            for col in df.columns:
                if df[col].dtype in ['object']:
                    unique_vals = df[col].nunique()
                    print(f"{col}: {unique_vals} unique values")
                    if unique_vals < 20:
                        print(f"  Values: {df[col].unique()}")
                elif df[col].dtype in ['int64', 'float64']:
                    print(f"{col}: min={df[col].min()}, max={df[col].max()}")
    
    except Exception as e:
        print(f"Debug error: {e}")
        import traceback
        traceback.print_exc()

debug_data_flow()
```

## Getting Help

### Create Support Package

When reporting issues, create a support package with diagnostic information:

```python
def create_support_package():
    """Create support package for issue reporting"""
    
    import json
    import sys
    import os
    from datetime import datetime
    
    support_info = {
        'timestamp': datetime.now().isoformat(),
        'python_version': sys.version,
        'platform': sys.platform,
        'environment_variables': {
            key: '***' if 'password' in key.lower() or 'secret' in key.lower() else value
            for key, value in os.environ.items()
            if key.startswith(('PG_', 'CONSUMER_', 'BASE_', 'LOG_'))
        },
        'package_versions': {},
        'recent_errors': []
    }
    
    # Get package versions
    try:
        import pkg_resources
        for package in ['pandas', 'geopandas', 'sqlalchemy', 'psycopg2', 'requests']:
            try:
                version = pkg_resources.get_distribution(package).version
                support_info['package_versions'][package] = version
            except:
                support_info['package_versions'][package] = 'not found'
    except:
        pass
    
    # Run health check
    try:
        health_results = run_health_check()
        support_info['health_check'] = health_results
    except Exception as e:
        support_info['health_check_error'] = str(e)
    
    # Save support package
    with open('highstreets_support_package.json', 'w') as f:
        json.dump(support_info, f, indent=2, default=str)
    
    print("Support package created: highstreets_support_package.json")
    print("Please include this file when reporting issues")

create_support_package()
```

### Contact Information

- **GitHub Issues**: [Create an issue](https://github.com/Greater-London-Authority/highstreets/issues)
- **Email Support**: anupam.bose@london.gov.uk
- **Documentation**: [Package Documentation](https://github.com/Greater-London-Authority/highstreets/wiki)

### Before Reporting Issues

1. **Run the health check** to identify obvious problems
2. **Check the logs** for detailed error messages
3. **Search existing issues** on GitHub
4. **Create a support package** with diagnostic information
5. **Include sample code** that reproduces the issue

### Issue Report Template

```markdown
## Issue Description
Brief description of the problem

## Environment
- OS: [e.g., Ubuntu 20.04, macOS 12.0, Windows 10]
- Python version: [e.g., 3.9.7]
- Package version: [e.g., 1.0.0]

## Steps to Reproduce
1. Step one
2. Step two
3. Step three

## Expected Behavior
What you expected to happen

## Actual Behavior
What actually happened

## Error Messages
```
Include full error messages and stack traces
```

## Additional Context
Any other relevant information

## Support Package
Attach the support package file generated by create_support_package()
``` 
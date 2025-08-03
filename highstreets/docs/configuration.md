# Configuration Guide

This guide covers all configuration options for the Highstreets package, focusing on the actual configuration structure used in the codebase.

## Table of Contents

- [Overview](#overview)
- [Environment Variables](#environment-variables)
- [Configuration Structure](#configuration-structure)
- [Database Configuration](#database-configuration)
- [API Configuration](#api-configuration)
- [File System Configuration](#file-system-configuration)
- [AWS Configuration](#aws-configuration)
- [Environment-Specific Settings](#environment-specific-settings)

## Overview

The Highstreets package uses environment variables loaded through python-dotenv. Configuration is loaded from:

1. **Environment variables** (highest priority)
2. **`.env` file** in project root
3. **Default values** in `highstreets/config.py`

## Environment Variables

The following environment variables are used by the package:

### Core Configuration

```env
# Project Configuration
PROJECT_FILE=path/to/project/file
PROJECT_ROOT=/path/to/project/root
```

### Database Configuration

```env
# PostgreSQL Database Configuration
PG11_DATABASE=your_database_name
PG11_USER=your_username
PG11_PASSWORD=your_password
PG11_HOST=localhost
PG11_PORT=5432
```

Note: The package uses `PG11_*` variable names for historical reasons but works with PostgreSQL 12+.

### API Configuration

```env
# BT API Authentication
CONSUMER_KEY=your_bt_consumer_key
CONSUMER_SECRET=your_bt_consumer_secret
```

### File System Configuration

```env
# Base directory for data storage
# Can be local path or S3 URL
BASE_DIR=s3://hsds-data/
# Or for local development:
# BASE_DIR=/path/to/local/data/
```

### Mastercard Configuration

```env
# Year-over-year file configuration
YOY_FILE=path/to/yoy/file
```

## Configuration Structure

The package configuration is centralized in `highstreets/config.py`:

### Database Configuration

```python
from highstreets import config

# Database connection parameters
database_config = {
    'database': config.PG11_DATABASE,
    'user': config.PG11_USER,
    'password': config.PG11_PASSWORD,
    'host': config.PG11_HOST,
    'port': config.PG11_PORT
}
```

### File Paths

```python
# Base directory (S3 or local)
base_dir = config.BASE_DIR  # e.g., "s3://hsds-data/"

# S3 bucket name
s3_bucket = config.S3_BUCKET  # "hsds-data"

# Specific data directories
sp_dir = config.SP_DIR  # Spending Pulse directory
sp_filepath = config.SP_FILEPATH_PROCESSED  # Processed SP file
```

### API Endpoints

```python
# Consumer Price Index API
cpi_api = config.CPI_API_ENDPOINT  # ONS API endpoint
cpi_categories = config.CPI_CATEGORIES  # CPI categories configuration
```

### Data File Paths

The package defines several standardized paths for different data types:

```python
# Mastercard Adjustment Factor paths (multiple fallback locations)
adj_paths = [
    config.MCARD_ADJ_PATH,   # Primary S3 location
    config.MCARD_ADJ_PATH1,  # Fallback location 1
    config.MCARD_ADJ_PATH2,  # Fallback location 2
    config.MCARD_ADJ_PATH3   # Fallback location 3
]

# Spending Pulse directories
sp_dir = config.SP_DIR  # Raw data directory
adjustment_factor_dir = config.ADJUSTMENT_FACTOR_DIR
inner_outer_quad_dir = config.INNER_OUTER_QUAD_DIR
```

## Database Configuration

### Connection Setup

The DataLoader class automatically creates database connections using environment variables:

```python
from highstreets.data_source_sink.dataloader import DataLoader

# DataLoader automatically uses environment variables:
# PG11_HOST, PG11_DATABASE, PG11_USER, PG11_PASSWORD, PG11_PORT
loader = DataLoader()
```

### Alternative Environment Variable Names

For compatibility, you can also use these variable names:

```env
# Alternative naming (mapped internally)
PG_DATABASE=your_database_name
PG_USER=your_username
PG_PASSWORD=your_password
PG_HOST=localhost
PG_PORT=5432
```

## API Configuration

### BT API Configuration

```env
# Required for BT API access
CONSUMER_KEY=your_consumer_key
CONSUMER_SECRET=your_consumer_secret
```

The API client automatically handles OAuth authentication using these credentials.

### API Endpoints

The package includes predefined API endpoints accessible through the configuration:

```python
from highstreets import config

# API endpoints are defined in config
# Access through DataLoader which reads these configurations
loader = DataLoader()
# Endpoints are automatically configured:
# - BT Hex API
# - BT MSOA API
# - BT LSOA API
# - BT Outage APIs
# - BT Daily Aggregated Shapes
```

## File System Configuration

### S3 Configuration

For S3 storage (recommended for production):

```env
BASE_DIR=s3://your-bucket-name/
S3_BUCKET=your-bucket-name

# AWS credentials (if not using IAM roles)
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_DEFAULT_REGION=eu-west-2
```

### Local File System

For local development:

```env
BASE_DIR=/path/to/local/data/
# Example: BASE_DIR=/mnt/data/highstreets/
```

### Data Directory Structure

The package expects the following directory structure under `BASE_DIR`:

```
BASE_DIR/
├── bt/
│   ├── raw/
│   └── processed/
├── mastercard/
│   ├── spendingpulse/
│   │   ├── received/
│   │   └── mcard_adjustment_factor.csv
│   └── mrli_3hourly/
└── reference_data/
    └── mcard_adjustment_factor.csv
```

## AWS Configuration

### S3 Integration

The package integrates with S3 using fsspec and s3fs:

```python
import fsspec

# S3 file system access
fs = fsspec.filesystem('s3')

# Reading from S3
data = pd.read_csv(f's3://bucket-name/path/to/file.csv')
```

### IAM Permissions

Required S3 permissions for the package:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::your-bucket-name",
                "arn:aws:s3:::your-bucket-name/*"
            ]
        }
    ]
}
```

## Environment-Specific Settings

### Development Environment

```env
# Development settings
BASE_DIR=/path/to/local/data/
PG11_HOST=localhost
PG11_DATABASE=highstreets_dev

# Use local database
# Smaller datasets for testing
```

### Production Environment

```env
# Production settings
BASE_DIR=s3://hsds-data/
PG11_HOST=your-prod-db-host
PG11_DATABASE=highstreets_prod

# Use S3 for data storage
# Full datasets
```

### AWS Batch Environment

For AWS Batch jobs, environment variables are typically passed through job definitions:

```env
# Common AWS Batch variables
START_DATE=2023-01-01
END_DATE=2023-01-31

# Database connection
PG11_HOST=your-rds-endpoint
PG11_DATABASE=highstreets

# S3 configuration
BASE_DIR=s3://hsds-data/
```

## Configuration Best Practices

### Security

1. **Never commit secrets** to version control
2. **Use environment variables** for sensitive data
3. **Use IAM roles** instead of access keys where possible
4. **Rotate credentials** regularly

### File Paths

1. **Use S3 for production** data storage
2. **Use absolute paths** for local development
3. **Maintain consistent directory structure**
4. **Use fallback paths** for critical files

### Database

1. **Use connection pooling** in production
2. **Set appropriate timeouts**
3. **Use read replicas** for analytics workloads
4. **Monitor connection usage**

## Configuration Validation

### Environment Check

```python
import os
from highstreets import config

# Check required environment variables
required_vars = [
    'PG11_HOST', 'PG11_DATABASE', 'PG11_USER', 'PG11_PASSWORD',
    'CONSUMER_KEY', 'CONSUMER_SECRET', 'BASE_DIR'
]

missing_vars = [var for var in required_vars if not os.getenv(var)]
if missing_vars:
    print(f"Missing environment variables: {missing_vars}")
else:
    print("All required environment variables are set")
```

### Database Connection Test

```python
from highstreets.data_source_sink.dataloader import DataLoader

try:
    loader = DataLoader()
    print("✓ Database configuration is valid")
except Exception as e:
    print(f"✗ Database configuration error: {e}")
```

### API Configuration Test

```python
from highstreets.api.clientbase import APIClient

try:
    client = APIClient()
    token = client.get_access_token()
    if token:
        print("✓ API configuration is valid")
    else:
        print("✗ API authentication failed")
except Exception as e:
    print(f"✗ API configuration error: {e}")
```

## Troubleshooting Configuration

### Common Issues

1. **Missing environment variables**: Check `.env` file exists and is readable
2. **Database connection failures**: Verify host, port, credentials
3. **API authentication failures**: Check consumer key and secret
4. **File path issues**: Ensure directories exist and are accessible
5. **S3 access issues**: Verify AWS credentials and permissions

### Debug Configuration

```python
# Print current configuration (sensitive data redacted)
from highstreets import config
import os

print("Configuration Summary:")
print(f"BASE_DIR: {config.BASE_DIR}")
print(f"Database Host: {config.PG11_HOST}")
print(f"Database Name: {config.PG11_DATABASE}")
print(f"S3 Bucket: {config.S3_BUCKET}")
print(f"Consumer Key: {'***' if os.getenv('CONSUMER_KEY') else 'Not Set'}")
```

For additional support, refer to the [Troubleshooting Guide](troubleshooting.md) or contact the development team. 
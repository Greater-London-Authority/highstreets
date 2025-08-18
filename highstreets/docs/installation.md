# Installation Guide

This guide provides detailed instructions for installing and setting up the Highstreets package for London footfall and transaction data processing.

## Table of Contents

- [System Requirements](#system-requirements)
- [Development Installation](#development-installation)
- [Configuration](#configuration)
- [Database Setup](#database-setup)
- [Verification](#verification)
- [Troubleshooting](#troubleshooting)

## System Requirements

### Minimum Requirements
- **Python**: 3.8 to 3.10 (3.11+ not currently supported)
- **RAM**: 8GB (16GB recommended for large datasets)
- **Storage**: 50GB free space for data processing
- **Network**: Stable internet connection for API access

### Database Requirements
- **PostgreSQL**: 12.0 or higher
- **Extensions**: PostGIS (for geographic data)
- **Disk Space**: 100GB+ for full London dataset

### External Dependencies
- **Git**: For version control
- **Poetry**: For dependency management (recommended)

## Development Installation

### Step 1: Clone Repository

```bash
# Clone the repository
git clone https://github.com/Greater-London-Authority/highstreets.git
cd highstreets
```

### Step 2: Set Up Python Environment

#### Option A: Using Poetry (Recommended)

```bash
# Install Poetry if not already installed
curl -sSL https://install.python-poetry.org | python3 -

# Install dependencies
poetry install

# Activate virtual environment
poetry shell
```

#### Option B: Using pip and venv

```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate

# Install dependencies
pip install -e .
```

### Step 3: Install Optional GLA Package

If you have access to the GLA private repository:

```bash
# Set GITHUB_TOKEN environment variable first
export GITHUB_TOKEN=your_github_token

# glapy will be automatically installed when you import highstreets
poetry run python -c "import highstreets"
```

## Configuration

### Environment Variables

Create a `.env` file in the project root:

```bash
# Copy example if available
cp .env.example .env
```

Edit the `.env` file with your configurations:

```env
# Database Configuration
PG_DATABASE=your_database_name
PG_USER=your_username
PG_PASSWORD=your_password
PG_HOST=localhost
PG_PORT=5432

# BT API Configuration
CONSUMER_KEY=your_bt_consumer_key
CONSUMER_SECRET=your_bt_consumer_secret

# Project Configuration
PROJECT_ROOT=/path/to/your/project
BASE_DIR=s3://your-s3-bucket/  # or local path like /data/highstreets/

# AWS Configuration (if using S3)
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_DEFAULT_REGION=eu-west-2

# Optional: GitHub token for private dependencies
GITHUB_TOKEN=your_github_token
```

### Configuration Structure

The package uses the following configuration pattern:

```python
from highstreets import config

# Base directory for data storage
base_dir = config.BASE_DIR  # e.g., "s3://hsds-data/" or "/mnt/data/"

# Database connection parameters
db_config = {
    'host': config.PG11_HOST,
    'database': config.PG11_DATABASE,
    'user': config.PG11_USER,
    'password': config.PG11_PASSWORD,
    'port': config.PG11_PORT
}

# API endpoints
bt_hex_endpoint = config.BT_HEX_API_ENDPOINT
```

## Database Setup

### PostgreSQL Installation

#### On Ubuntu/Debian:
```bash
sudo apt update
sudo apt install postgresql postgresql-contrib postgis
```

#### On macOS:
```bash
brew install postgresql postgis
brew services start postgresql
```

#### On Windows:
Download and install from [PostgreSQL official website](https://www.postgresql.org/download/windows/)

### Database Creation

```bash
# Connect to PostgreSQL
sudo -u postgres psql

# Create database and user
CREATE DATABASE your_database_name;
CREATE USER your_username WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE your_database_name TO your_username;

# Enable PostGIS extension
\c your_database_name
CREATE EXTENSION postgis;
\q
```

### Database Schema

The package expects the following schema structure:

- **Schema**: `gisapdata` (default)
- **Tables**: Various BT footfall and Mastercard data tables
- **Lookup Tables**: Geographic boundary mappings

## Verification

### Test Installation

```python
# Test basic imports
import highstreets
from highstreets.data_source_sink.dataloader import DataLoader
from highstreets.data_source_sink.datawriter import DataWriter
from highstreets.data_transformation.hextransform import HexTransform

print("✓ All modules imported successfully")
```

### Test Database Connection

```python
from highstreets.data_source_sink.dataloader import DataLoader

try:
    loader = DataLoader()
    print("✓ Database connection successful")
except Exception as e:
    print(f"✗ Database connection failed: {e}")
```

### Test API Connection

```python
from highstreets.api.clientbase import APIClient

try:
    client = APIClient()
    token = client.get_access_token()
    if token:
        print("✓ API authentication successful")
    else:
        print("✗ No token received")
except Exception as e:
    print(f"✗ API authentication failed: {e}")
```

## Troubleshooting

### Common Issues

#### 1. Poetry Installation Issues

```bash
# Clear Poetry cache
poetry cache clear pypi --all

# Reset virtual environment
poetry env remove python3.8  # or your Python version
poetry install
```

#### 2. Python Version Compatibility

```bash
# Check Python version
python --version

# The package requires Python 3.8-3.10
# If you have Python 3.11+, use a version manager like pyenv
pyenv install 3.10.12
pyenv local 3.10.12
```

#### 3. Database Connection Issues

```bash
# Check PostgreSQL service
sudo systemctl status postgresql

# Test connection manually
psql -h localhost -U your_user -d your_database -c "SELECT version();"
```

#### 4. Missing Geographic Dependencies

```bash
# Install system dependencies (Ubuntu/Debian)
sudo apt install build-essential python3-dev libpq-dev gdal-bin

# On macOS
brew install gdal postgresql
```

#### 5. GLA Package Installation

```bash
# Manually install glapy if automatic installation fails
poetry add git+https://github.com/Greater-London-Authority/glapy@feature/lds-update-data
```

### Environment-Specific Notes

#### Development Environment
- Use local PostgreSQL database
- Set `BASE_DIR` to local path for development
- Enable debug logging if needed

#### AWS Environment
- Configure S3 access for `BASE_DIR`
- Set appropriate IAM roles and permissions
- Use environment variables for sensitive data

#### Docker Environment (for AWS Batch)
- Ensure all environment variables are properly passed
- Mount appropriate volumes for local development
- Use multi-stage builds for optimized images

## Next Steps

After successful installation:

1. **Read the [User Guide](user_guide.md)** for common workflows
2. **Check [Configuration Guide](configuration.md)** for detailed settings
3. **Review [API Reference](api_reference.md)** for technical details
4. **See [Examples](examples.md)** for practical use cases
5. **Understand [AWS Pipeline](aws_pipeline.md)** for production deployment

## Support

For additional support:
- **Issues**: [GitHub Issues](https://github.com/Greater-London-Authority/highstreets/issues)
- **Contact**: anupam.bose@london.gov.uk
- **Documentation**: Project README and docs/ directory 
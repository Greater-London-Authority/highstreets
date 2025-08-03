# API Reference

Complete API reference for the Highstreets package, documenting the actual classes and methods present in the codebase.

## Table of Contents

- [API Client](#api-client)
- [Core Management](#core-management)
- [Data Loading](#data-loading)
- [Data Writing](#data-writing)
- [Data Transformation](#data-transformation)
- [AWS Pipeline Modules](#aws-pipeline-modules)
- [Configuration](#configuration)

## API Client

### highstreets.api.clientbase

#### APIClient

Main class for handling BT API authentication and data requests.

```python
class APIClient:
    """Handles authentication and requests to BT API."""
    
    token_endpoint = "https://api.business.bt.com/oauth/accesstoken"
```

**Constructor:**
```python
def __init__(self)
```
Automatically obtains access token and sets up CPI configuration.

**Static Methods:**

##### get_access_token()
```python
@staticmethod
def get_access_token() -> str
```
Obtain OAuth access token using CONSUMER_KEY and CONSUMER_SECRET environment variables.

**Returns:**
- `str`: Access token for API authentication

**Raises:**
- `APIClientException`: If authentication fails

**Instance Methods:**

##### fetch_cpi()
```python
def fetch_cpi() -> pd.DataFrame
```
Fetch Consumer Price Index data from ONS API.

**Returns:**
- `pd.DataFrame`: CPI data with columns: `yr`, `month`, `Aggregate`, `cpi_index`

##### get_data_request()
```python
def get_data_request(self, endpoint: str, headers: Dict = None, params: Dict = None) -> List[Dict]
```
Make paginated API request to specified endpoint.

**Parameters:**
- `endpoint` (str): API endpoint URL
- `headers` (dict, optional): Additional request headers
- `params` (dict, optional): Request parameters

**Returns:**
- `List[Dict]`: Complete paginated response data

#### APIClientException
```python
class APIClientException(Exception):
    """Exception raised for API client errors."""
```

## Core Management

### highstreets.core.sublicense_manager

#### SublicenseManager

Main class for managing data sharing agreements with external partners.

```python
class SublicenseManager:
    """Enhanced sublicense manager with database-driven processing and YAML configuration support."""
```

**Constructor:**
```python
def __init__(self, config_path: Optional[str] = None)
```

**Parameters:**
- `config_path` (str, optional): Path to sublicense YAML configuration file

**Key Methods:**

##### process_sublicense_complete()
```python
def process_sublicense_complete(
    self, 
    sublicense_name: str,
    save_files: bool = True,
    upload_to_datastore: bool = True
) -> Dict[str, Any]
```
Process all data sources for a specific sublicense.

##### process_all_sublicenses()
```python
def process_all_sublicenses(
    self,
    include_inactive: bool = False,
    save_files: bool = True,
    upload_to_datastore: bool = True
) -> Dict[str, Any]
```
Process all active sublicenses.

##### validate_configuration()
```python
def validate_configuration() -> Dict[str, Any]
```
Validate YAML sublicense configuration.

### highstreets.core.sql_manager

#### SQLManager

SQL query management and execution.

```python
class SQLManager:
    """Manages SQL queries with performance monitoring."""
```

**Constructor:**
```python
def __init__(self, engine: Optional[Engine] = None)
```

## Data Loading

### highstreets.data_source_sink.dataloader

#### DataLoader

Central component for fetching data from various sources.

```python
class DataLoader:
    """Central component for fetching and managing data from multiple sources."""
```

**Constructor:**
```python
def __init__(self)
```
Automatically sets up database connections and API endpoints from environment variables.

**Key Methods:**

##### get_hex_data()
```python
def get_hex_data(self, date_from: str, date_to: str) -> List[Dict]
```
Fetch hex grid footfall data from BT API.

##### get_lsoa_data()
```python
def get_lsoa_data(self, date_from: str, date_to: str) -> List[Dict]
```
Fetch LSOA level footfall data from BT API.

##### get_msoa_data()
```python
def get_msoa_data(self, date_from: str, date_to: str) -> List[Dict]
```
Fetch MSOA level footfall data from BT API.

##### get_full_data()
```python
def get_full_data(self, table_name: str) -> pd.DataFrame
```
Retrieve complete data from a PostgreSQL table.

#### Exceptions

```python
class DataLoaderException(Exception):
    """Exception raised for data loading errors."""

class SchemaMismatchError(Exception):
    """Exception raised for schema validation errors."""

class DateRangeError(Exception):
    """Exception raised for invalid date ranges."""
```

## Data Writing

### highstreets.data_source_sink.datawriter

#### DataWriter

Handles data storage and export operations.

```python
class DataWriter:
    """Handles data storage, organization, and dissemination across various formats."""
```

**Constructor:**
```python
def __init__(self)
```

**Key Methods:**

##### append_data_to_postgres()
```python
def append_data_to_postgres(
    self,
    data: pd.DataFrame,
    table_name: str,
    date_column: str = "count_date"
) -> None
```
Append data to PostgreSQL table with automatic deduplication.

##### truncate_and_load_to_postgres()
```python
def truncate_and_load_to_postgres(
    self,
    dataframe: pd.DataFrame,
    table_name: str,
    schema: str = "gisapdata",
    index: bool = False
) -> None
```
Truncate table and load new data.

##### upload_data_to_lds()
```python
def upload_data_to_lds(
    self,
    slug: str,
    resource_title: str,
    df: pd.DataFrame = None,
    file_path: str = None,
    custom_date_column: str = "count_date"
) -> None
```
Upload data to London Datastore (requires glapy package).

## Data Transformation

### highstreets.data_transformation.hextransform

#### HexTransform

Transforms hex grid data into standardized formats and geographic aggregations.

```python
class HexTransform(DataLoader):
    """Transform class for performing data transformation on hex grid data."""
```

**Constructor:**
```python
def __init__(self)
```
Inherits from DataLoader for direct API access.

**Key Methods:**

##### transform_data()
```python
def transform_data(self, data: Union[List, pd.DataFrame]) -> pd.DataFrame
```
Transform raw hex data from BT API into standardized format.

**Parameters:**
- `data` (List or pd.DataFrame): Raw hex data from API

**Returns:**
- `pd.DataFrame`: Transformed data with standardized columns

**Expected Input Columns:**
- `poi_id`: Hex grid identifier
- `date`: Date of measurement
- `total_volume`: Total footfall volume
- `worker_population_percentage`: Worker percentage
- `resident_population_percentage`: Resident percentage

##### fetch_and_transform_hex_data()
```python
def fetch_and_transform_hex_data(
    self,
    transform_layer: str,
    table_name: str,
    truncate: bool = False,
    load_to_db: bool = True
) -> None
```
Fetch hex data and transform to geographic boundaries using SQL templates.

### Other Transformation Classes

#### McardTransform
```python
class McardTransform:
    """Processes and transforms Mastercard transaction data."""
```

#### DailyTransform
```python
class DailyTransform:
    """Handles daily aggregation processing."""
```

#### LsoaTransform
```python
class LsoaTransform:
    """Handles LSOA data transformation."""
```

#### MsoaTransform
```python
class MsoaTransform:
    """Handles MSOA data transformation."""
```

## AWS Pipeline Modules

The package includes several AWS pipeline modules designed for AWS Batch execution:

### highstreets.aws_pipeline.hex_e2e

End-to-end hex data processing pipeline.

**Environment Variables:**
- `START_DATE`: Processing start date (YYYY-MM-DD)
- `END_DATE`: Processing end date (YYYY-MM-DD)

**Process:**
1. Fetch hex data from BT API
2. Transform data using HexTransform
3. Save to PostgreSQL table `bt_footfall_tfl_hex_3hourly`

### highstreets.aws_pipeline.daily_agg

Daily aggregation processing pipeline.

### highstreets.aws_pipeline.mcard_3hourly

Mastercard 3-hourly data processing pipeline.

### Other Pipeline Modules

- `bt_lookups.py`: BT lookup table processing
- `bt_outage.py`: BT outage data processing
- `lsoa_e2e.py`: LSOA end-to-end processing
- `msoa_e2e.py`: MSOA end-to-end processing
- `mcard_weekly.py`: Mastercard weekly processing
- `mcard_weekly_intl.py`: Mastercard international weekly processing

## Configuration

### highstreets.config

Configuration module providing access to environment variables and settings.

**Key Configuration Variables:**

```python
# Database Configuration
PG11_DATABASE: str    # Database name
PG11_USER: str       # Database user
PG11_PASSWORD: str   # Database password
PG11_HOST: str       # Database host
PG11_PORT: str       # Database port

# File Paths
BASE_DIR: str        # Base directory (S3 or local)
S3_BUCKET: str       # S3 bucket name

# API Configuration
CPI_API_ENDPOINT: str     # ONS CPI API endpoint
CPI_CATEGORIES: List      # CPI categories to fetch

# Mastercard Configuration
SP_DIR: str                    # Spending Pulse directory
MCARD_ADJ_PATH: str           # Adjustment factor file path
ADJUSTMENT_FACTOR_DIR: str     # Adjustment factor directory
```

**Usage:**
```python
from highstreets import config

base_dir = config.BASE_DIR
database = config.PG11_DATABASE
```

## Usage Examples

### Basic Data Processing
```python
from highstreets.data_source_sink.dataloader import DataLoader
from highstreets.data_transformation.hextransform import HexTransform
from highstreets.data_source_sink.datawriter import DataWriter

# Initialize components
loader = DataLoader()
transformer = HexTransform()
writer = DataWriter()

# Fetch and process data
data = loader.get_hex_data("2023-01-01", "2023-01-31")
transformed = transformer.transform_data(data)
writer.append_data_to_postgres(transformed, "bt_footfall_tfl_hex_3hourly")
```

### Sublicense Processing
```python
from highstreets.core.sublicense_manager import SublicenseManager

manager = SublicenseManager()
results = manager.process_sublicense_complete("partner-name")
```

### API Authentication
```python
from highstreets.api.clientbase import APIClient

client = APIClient()
token = client.get_access_token()
# Token is automatically used for subsequent requests
```

## Error Handling

All modules include comprehensive error handling:

- **APIClientException**: API communication errors
- **DataLoaderException**: Data loading errors
- **SchemaMismatchError**: Data validation errors
- **DateRangeError**: Invalid date range errors

**Example:**
```python
try:
    data = loader.get_hex_data("2023-01-01", "2023-01-31")
except DataLoaderException as e:
    print(f"Data loading failed: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")
```

## Performance Considerations

- **Database Connections**: Automatic connection pooling and management
- **API Rate Limiting**: Built-in pagination and request handling
- **Memory Management**: Streaming processing for large datasets
- **S3 Integration**: Efficient file operations using fsspec

For detailed usage examples, see the [User Guide](user_guide.md) and [Examples](examples.md). 
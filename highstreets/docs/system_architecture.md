# System Architecture

This document provides a comprehensive overview of the Highstreets package architecture, including system design principles, data flow, component interactions, and technical implementation details.

## Table of Contents

- [Overview](#overview)
- [System Architecture](#system-architecture)
- [Component Architecture](#component-architecture)
- [Data Architecture](#data-architecture)
- [Processing Pipelines](#processing-pipelines)
- [AWS Infrastructure](#aws-infrastructure)
- [Security Architecture](#security-architecture)
- [Design Patterns](#design-patterns)

## Overview

The Highstreets package is designed as a comprehensive data processing platform for London's footfall and transaction data. It follows a modular, layered architecture that separates concerns and enables scalable, maintainable data processing workflows.

### Core Principles

1. **Modularity**: Components are loosely coupled and highly cohesive
2. **Scalability**: Designed to handle London-wide datasets efficiently
3. **Reliability**: Robust error handling and data validation
4. **Extensibility**: Easy to add new data sources and processing logic
5. **Performance**: Optimized for large-scale data processing
6. **Security**: Secure handling of sensitive data and credentials

## System Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        External Systems                         │
├─────────────────┬───────────────────┬───────────────────────────┤
│   BT API        │   Mastercard      │   London Datastore        │
│   (Footfall)    │   (Transactions)  │   (Distribution)          │
└─────────────────┴───────────────────┴───────────────────────────┘
          │                 │                        ▲
          │                 │                        │
          ▼                 ▼                        │
┌─────────────────────────────────────────────────────────────────┐
│                     API & Integration Layer                     │
├─────────────────┬───────────────────────────────────────────────┤
│   APIClient     │          DataLoader                          │
└─────────────────┴───────────────────────────────────────────────┘
          │                                              ▲
          ▼                                              │
┌─────────────────────────────────────────────────────────────────┐
│                    Data Processing Layer                        │
├─────────────────┬───────────────────┬───────────────────────────┤
│  HexTransform   │   McardTransform  │   DailyTransform          │
│  LsoaTransform  │   MsoaTransform   │   Data Validation         │
└─────────────────┴───────────────────┴───────────────────────────┘
          │                 │                        │
          ▼                 ▼                        ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Data Storage Layer                          │
├─────────────────┬───────────────────┬───────────────────────────┤
│   PostgreSQL    │      S3 Storage   │     DataWriter            │
│   (Primary)     │    (Archive)      │     (Output)              │
└─────────────────┴───────────────────┴───────────────────────────┘
          │                                              ▲
          ▼                                              │
┌─────────────────────────────────────────────────────────────────┐
│                   Business Logic Layer                          │
├─────────────────┬───────────────────┬───────────────────────────┤
│  Sublicense     │   SQL Manager     │    AWS Pipeline           │
│  Manager        │                   │    Orchestration          │
└─────────────────┴───────────────────┴───────────────────────────┘
```

## Component Architecture

### Core Components

```
highstreets/
├── api/                    # External API clients
│   └── clientbase.py      # BT API client with OAuth
├── core/                  # Core business logic
│   ├── processors/        # Sublicense processing
│   ├── settings/          # YAML configurations
│   ├── sql_manager.py     # SQL operations
│   └── sublicense_manager.py  # Partner management
├── data_source_sink/      # Data I/O operations
│   ├── dataloader.py      # Data loading from APIs/DB
│   ├── datawriter.py      # Data writing to DB/files
│   └── lookup_manager.py  # Geographic lookups
├── data_transformation/   # Data processing
│   ├── hextransform.py    # Hex grid processing
│   ├── mcard_transform.py # Mastercard processing
│   ├── dailytransform.py  # Daily aggregations
│   ├── lsoatransform.py   # LSOA processing
│   └── msoatransform.py   # MSOA processing
├── aws_pipeline/          # AWS Batch pipelines
│   ├── hex_e2e.py         # End-to-end hex processing
│   ├── daily_agg.py       # Daily processing
│   ├── mcard_3hourly.py   # Mastercard 3-hourly
│   ├── mcard_weekly.py    # Mastercard weekly
│   ├── bt_lookups.py      # BT lookup processing
│   ├── lsoa_e2e.py        # LSOA pipeline
│   └── msoa_e2e.py        # MSOA pipeline
└── config.py              # Configuration management
```

### Component Relationships

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   APIClient     │───▶│   DataLoader    │───▶│  Transform      │
│  (OAuth & API)  │    │(Data Ingestion) │    │  Classes        │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Environment    │    │   Database      │    │   DataWriter    │
│  Configuration  │    │   Connections   │    │  (Storage)      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │                       │
                                ▼                       ▼
                       ┌─────────────────┐    ┌─────────────────┐
                       │   SQLManager    │    │ SublicenseManager│
                       │                 │    │                 │
                       └─────────────────┘    └─────────────────┘
```

## Data Architecture

### Data Model

#### Core Data Entities

1. **Hex Grid Data**
   - Primary geographic unit (350m/400m grid)
   - Temporal: 3-hourly intervals
   - Metrics: Residents, visitors, workers, loyalty, dwell time

2. **Geographic Boundaries**
   - High Streets
   - Town Centres
   - Business Improvement Districts (BIDs)
   - Bespoke Areas
   - Administrative Boundaries (LSOA, MSOA)

3. **Transaction Data**
   - Mastercard spending data
   - Temporal: 3-hourly and weekly aggregations
   - Metrics: Transaction amounts, counts, visitor types

4. **Lookup Tables**
   - Spatial relationships between hex grids and boundaries
   - Geographic metadata and classifications

### Database Schema

```sql
-- Core footfall data
bt_footfall_tfl_hex_3hourly (
    hex_id VARCHAR(50),
    count_date DATE,
    hours VARCHAR(10),
    resident INTEGER,
    visitor INTEGER,
    worker INTEGER,
    loyalty_percentage FLOAT,
    dwell_time FLOAT,
    PRIMARY KEY (hex_id, count_date, hours)
);

-- Geographic aggregations
econ_busyness_bt_highstreets_3hourly_counts (
    highstreet_id INTEGER,
    highstreet_name VARCHAR(255),
    count_date DATE,
    hours VARCHAR(10),
    resident INTEGER,
    visitor INTEGER,
    worker INTEGER,
    ave_loyalty_percentage FLOAT,
    ave_dwell_time FLOAT
);

-- Lookup tables
econ_busyness_hex_highstreet_lookup (
    hex_id VARCHAR(50),
    highstreet_id INTEGER,
    highstreet_name VARCHAR(255)
);
```

### Data Flow

#### Ingestion Flow
```
BT API → APIClient → DataLoader → Raw Data → Validation → HexTransform → PostgreSQL
```

#### Processing Flow
```
Raw Data → Schema Validation → Business Logic → Geographic Aggregation → Output
```

#### Distribution Flow
```
Processed Data → SublicenseManager → CSV Export → London Datastore Upload
```

## Processing Pipelines

### BT Footfall Pipeline

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  BT API     │───▶│ APIClient   │───▶│HexTransform │───▶│ DataWriter  │
│ (OAuth)     │    │(Pagination) │    │(Processing) │    │ (Storage)   │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
                            │                   │                 │
                            ▼                   ▼                 ▼
                   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
                   │Data         │    │SQL-based    │    │Quality      │
                   │Validation   │    │Aggregation  │    │Assurance    │
                   └─────────────┘    └─────────────┘    └─────────────┘
```

### Mastercard Pipeline

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│File Input   │───▶│ DataLoader  │───▶│McardTransform│───▶│Inflation    │
│(S3/Local)   │    │(File Proc.) │    │(Processing) │    │Adjustment   │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
                            │                   │                 │
                            ▼                   ▼                 ▼
                   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
                   │File         │    │Weekly/3hr   │    │Output       │
                   │Validation   │    │Aggregation  │    │Generation   │
                   └─────────────┘    └─────────────┘    └─────────────┘
```

### Sub-licensing Pipeline

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│YAML Config  │───▶│SQL Template │───▶│Database     │───▶│File Export  │
│(Partners)   │    │Engine       │    │Execution    │    │& Upload     │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
                            │                   │                 │
                            ▼                   ▼                 ▼
                   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
                   │Filter       │    │Data         │    │London       │
                   │Generation   │    │Formatting   │    │Datastore    │
                   └─────────────┘    └─────────────┘    └─────────────┘
```

## AWS Infrastructure

### Architecture Overview

The package is designed for AWS cloud deployment using:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Step Functions │───▶│   AWS Batch     │───▶│   RDS/S3        │
│  (Orchestration)│    │   (Compute)     │    │   (Storage)     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  CloudWatch     │    │  Docker Images  │    │  Data Tables    │
│  (Scheduling)   │    │  (ECR)          │    │  (PostgreSQL)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Pipeline Execution

Each AWS pipeline module (`hex_e2e.py`, `daily_agg.py`, etc.) is designed to:
1. Read environment variables for configuration
2. Execute specific data processing tasks
3. Output results to PostgreSQL or S3
4. Handle errors and logging appropriately

## Security Architecture

### Authentication & Authorization

1. **API Authentication**
   - OAuth 2.0 for BT API using APIClient
   - Environment-based credential management
   - Automatic token refresh

2. **Database Security**
   - PostgreSQL with connection pooling
   - Environment-based credentials
   - SSL/TLS connections

3. **File System Security**
   - S3 integration with IAM roles
   - Environment-based access keys
   - Secure file operations using fsspec

### Data Security

1. **Data at Rest**
   - PostgreSQL encryption
   - S3 server-side encryption
   - Secure credential storage

2. **Data in Transit**
   - HTTPS for API communications
   - TLS for database connections
   - Secure S3 transfers

3. **Data Privacy**
   - Aggregated data only
   - Geographic anonymization
   - GDPR compliance measures

## Design Patterns

### Repository Pattern

```python
class DataLoader:
    """Central repository for data access"""
    def get_hex_data(self, start, end): pass
    def get_lsoa_data(self, start, end): pass
    def get_full_data(self, table): pass
```

### Factory Pattern

```python
class TransformFactory:
    """Factory for creating appropriate transformers"""
    @staticmethod
    def create_transformer(data_type):
        if data_type == 'hex':
            return HexTransform()
        elif data_type == 'mastercard':
            return McardTransform()
        # ... other transformers
```

### Template Method Pattern

```python
class DataPipeline:
    """Template for AWS pipeline execution"""
    def process(self):
        self.load_config()
        self.fetch_data()
        self.transform_data()
        self.save_data()
    
    # Abstract methods implemented by subclasses
    def fetch_data(self): pass
    def transform_data(self): pass
```

### Strategy Pattern

```python
class SublicenseProcessor:
    """Different processing strategies for different partners"""
    def process_data_source(self, strategy):
        return strategy.execute_query()
```

## Configuration Management

### Environment-Based Configuration

```python
# config.py pattern
import os
from dotenv import load_dotenv

load_dotenv()

# Database configuration
PG11_HOST = os.getenv("PG11_HOST")
PG11_DATABASE = os.getenv("PG11_DATABASE")

# File paths
BASE_DIR = os.getenv("BASE_DIR", "s3://hsds-data/")

# API configuration
CPI_API_ENDPOINT = "https://api.beta.ons.gov.uk/v1/datasets/cpih01"
```

### YAML Configuration

```yaml
# Sublicense configuration pattern
config:
  base_schema: "gisapdata"
  data_sources:
    bt_footfall:
      aggregated_tables:
        bid: "econ_busyness_bt_bids_3hourly_counts"

sublicenses:
  partner-name:
    status: "active"
    data_sources: ["bt_footfall"]
    filters:
      bid_ids: [21, 77]
```

## Performance Considerations

### Database Optimization

1. **Connection Pooling**: SQLAlchemy connection management
2. **Query Optimization**: SQL-based filtering and aggregation
3. **Indexing**: Strategic database indexes
4. **Batch Processing**: Chunked data processing

### Memory Management

1. **Streaming Processing**: Large dataset handling
2. **Garbage Collection**: Proper resource cleanup
3. **Data Types**: Efficient pandas data types
4. **Chunked Operations**: Memory-efficient processing

### S3 Integration

1. **fsspec Usage**: Efficient file operations
2. **Parallel Uploads**: Concurrent file operations
3. **Compression**: Data compression for storage
4. **Caching**: Strategic caching of frequently accessed data

This architecture provides a solid foundation for scalable, maintainable, and secure data processing while supporting both local development and cloud deployment scenarios. 
# London High Streets Data Processing Package
  
  [![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
  [![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
  [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A comprehensive Python toolkit for processing, analyzing, and managing footfall and transaction data across London's high streets, town centers, and business improvement districts (BIDs). This package supports evidence-based decision-making for urban planning and economic development.

## 🚀 Quick Start

```bash
# Clone the repository
git clone https://github.com/Greater-London-Authority/highstreets
cd highstreets

# Install with Poetry
poetry install
poetry shell

# Or install with pip
pip install -e .
```

## 📋 Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Quick Start Guide](#quick-start-guide)
- [Documentation](#documentation)
- [Architecture](#architecture)
- [Contributing](#contributing)
- [License](#license)
- [Support](#support)

## ✨ Features

### 🔄 **Data Integration & Processing**
- **BT Footfall Data**: Process sensor data from 350m/400m hex grids
- **Mastercard Transactions**: Handle spending and transaction volume data
- **Multi-Geographic Support**: High streets, town centers, BIDs, LSOAs, MSOAs
- **Temporal Aggregations**: 3-hourly, daily, weekly, and yearly summaries

### 📊 **Analytics & Insights**
- **Footfall Analytics**: Visitor counts, demographics, and behavior patterns
- **Economic Metrics**: Transaction volumes, spending patterns, and growth rates
- **Performance Indicators**: Year-over-year comparisons and trend analysis
- **Demographic Segmentation**: Residents, workers, visitors, and international tourists

### 🗺️ **Geographic Intelligence**
- **Spatial Processing**: Hex grid management and boundary integration
- **GIS Compatibility**: Integration with shapefiles and geographic databases
- **Multi-Level Aggregation**: From hex grids to administrative boundaries
- **Lookup Management**: Automated spatial relationship mapping

### 🔍 **Data Quality & Validation**
- **Schema Enforcement**: Automated data validation and type checking
- **Outage Detection**: Monitor and track data quality issues
- **Missing Data Handling**: Intelligent gap filling and interpolation
- **Performance Monitoring**: Track processing speeds and data integrity

### 🚀 **Enterprise Features**
- **Sub-licensing System**: Automated data sharing with partners
- **AWS Integration**: S3 storage and cloud processing
- **Database Management**: PostgreSQL integration with change tracking
- **API Integration**: External data source connectivity

## 🛠 Installation

### Prerequisites

- **Python 3.8+**
- **PostgreSQL 12+**
- **Poetry** (recommended) or pip
- **Git**

### Development Setup

1. **Clone the repository**:
   ```bash
   git clone https://github.com/Greater-London-Authority/highstreets
   cd highstreets
   ```

2. **Install dependencies**:
   ```bash
   # Using Poetry (recommended)
   poetry install
   poetry shell
   
   # Or using pip
   pip install -e .
   ```

3. **Configure environment**:
   ```bash
   cp .env.example .env
   # Edit .env with your configurations
   ```

4. **Set up database**:
   ```bash
   # Create database and run migrations
   python -m highstreets.scripts.setup_database
   ```

### Production Installation

```bash
pip install highstreets
```

## 🏃‍♂️ Quick Start Guide

### Basic Usage

```python
from highstreets.data_source_sink.dataloader import DataLoader
from highstreets.data_source_sink.datawriter import DataWriter
from highstreets.data_transformation.hextransform import HexTransform

# Initialize components
loader = DataLoader()
writer = DataWriter()
transformer = HexTransform()

# Fetch and process BT footfall data
data = loader.get_hex_data("2023-01-01", "2023-01-31")
transformed_data = transformer.transform_data(data)

# Save to database
writer.append_data_to_postgres(transformed_data, "bt_footfall_tfl_hex_3hourly")
```

### Processing Sublicense Data

```python
from highstreets.core.sublicense_manager import SublicenseManager

# Process all active sublicense agreements
manager = SublicenseManager()
results = manager.process_all_sublicenses()

print(f"Processed {results['summary']['total_sublicenses']} sublicenses")
print(f"Created {results['summary']['total_files']} files")
```

### Configuration Management

```python
from highstreets import config

# Access configuration
print(f"Base directory: {config.BASE_DIR}")
print(f"Database schema: {config.DB_SCHEMA}")
print(f"Available data sources: {config.DATA_SOURCES}")
```

## 📚 Documentation

### User Documentation
- **[Installation Guide](docs/installation.md)** - Detailed setup instructions
- **[Configuration Guide](docs/configuration.md)** - Environment and settings
- **[User Guide](docs/user_guide.md)** - Common tasks and workflows
- **[API Reference](docs/api_reference.md)** - Complete API documentation
- **[Examples](docs/examples.md)** - Code examples and tutorials

### Technical Documentation
- **[Architecture Overview](docs/architecture.md)** - System design and components
- **[Data Pipeline](docs/pipeline.md)** - Data flow and processing
- **[Database Schema](docs/database.md)** - Database structure and relationships
- **[Performance Guide](docs/performance.md)** - Optimization and monitoring

### Specialized Guides
- **[BT Data Processing](docs/bt_e2e_dataflow.md)** - BT footfall data pipeline
- **[Sublicensing System](docs/sublicensing.md)** - Partner data sharing
- **[Geographic Processing](docs/geographic.md)** - Spatial data handling
- **[Data Quality](docs/data_quality.md)** - Validation and monitoring

### Operations
- **[Deployment Guide](docs/deployment.md)** - Production deployment
- **[Monitoring Guide](docs/monitoring.md)** - System monitoring
- **[Troubleshooting](docs/troubleshooting.md)** - Common issues and solutions
- **[Contributing](docs/contributing.md)** - Development guidelines

## 🏗 Architecture

### System Components

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │    │   Processing    │    │   Storage &     │
│                 │    │   Pipeline      │    │   Distribution  │
├─────────────────┤    ├─────────────────┤    ├─────────────────┤
│ • BT API        │───▶│ • Data Loaders  │───▶│ • PostgreSQL    │
│ • Mastercard    │    │ • Transformers  │    │ • S3 Storage    │
│ • Geographic    │    │ • Validators    │    │ • London        │
│   Boundaries    │    │ • Aggregators   │    │   Datastore     │
│ • External APIs │    │ • Quality Checks│    │ • CSV Exports   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Key Modules

- **`api/`** - External API clients and authentication
- **`core/`** - Core business logic and processors  
- **`data_source_sink/`** - Data loading and writing operations
- **`data_transformation/`** - Data processing and transformation
- **`aws_pipeline/`** - Cloud processing pipelines
- **`sql/`** - Database queries and schemas

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](docs/contributing.md) for details.

### Development Workflow

1. **Fork and clone** the repository
2. **Create a feature branch**: `git checkout -b feature-name`
3. **Install pre-commit hooks**: `pre-commit install`
4. **Make changes** and add tests
5. **Run tests**: `pytest`
6. **Submit a pull request**

### Code Standards

- **Code Style**: Black formatting
- **Documentation**: Comprehensive docstrings
- **Testing**: Pytest with >80% coverage
- **Type Hints**: Required for new code

## 📈 Performance

- **Processing Speed**: >1M records/minute on standard hardware
- **Memory Usage**: Optimized for streaming large datasets
- **Database Performance**: Indexed queries and connection pooling
- **Scalability**: Designed for multi-year, London-wide datasets

## 🐛 Issues and Support

- **Bug Reports**: [GitHub Issues](https://github.com/Greater-London-Authority/highstreets/issues)
- **Feature Requests**: [GitHub Discussions](https://github.com/Greater-London-Authority/highstreets/discussions)
- **Documentation**: [GitHub Wiki](https://github.com/Greater-London-Authority/highstreets/wiki)

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 📞 Contact

**Project Maintainer**: Anupam Bose - anupam.bose@london.gov.uk

**Greater London Authority**  
City Data, City Intelligence Unit  
City Hall, London E16 1ZE

---

<div align="center">
  <strong>Built with ❤️ by the Greater London Authority</strong>
</div>

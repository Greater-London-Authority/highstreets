# Contributing Guide

Welcome to the Highstreets project! We appreciate your interest in contributing. This guide will help you understand our development process, coding standards, and how to contribute effectively.

## Table of Contents

- [Getting Started](#getting-started)
- [Development Environment](#development-environment)
- [Code Standards](#code-standards)
- [Contributing Workflow](#contributing-workflow)
- [Testing Guidelines](#testing-guidelines)
- [Documentation Guidelines](#documentation-guidelines)
- [Pull Request Process](#pull-request-process)
- [Release Process](#release-process)
- [Community Guidelines](#community-guidelines)

## Getting Started

### Prerequisites

Before contributing, ensure you have:

- **Python 3.8+** installed
- **Git** installed and configured
- **Poetry** for dependency management
- **PostgreSQL** for local testing
- **Basic knowledge** of the domain (footfall data, geographic processing)

### Initial Setup

1. **Fork the repository** on GitHub
2. **Clone your fork** locally:
   ```bash
   git clone https://github.com/your-username/highstreets.git
   cd highstreets
   ```

3. **Add upstream remote**:
   ```bash
   git remote add upstream https://github.com/Greater-London-Authority/highstreets.git
   ```

4. **Install dependencies**:
   ```bash
   poetry install --with dev,test
   ```

5. **Set up pre-commit hooks**:
   ```bash
   poetry run pre-commit install
   ```

6. **Configure environment**:
   ```bash
   cp .env.example .env.dev
   # Edit .env.dev with your local settings
   ```

## Development Environment

### Local Database Setup

Set up a local PostgreSQL database for development:

```bash
# Create development database
createdb highstreets_dev

# Run migrations (if available)
python scripts/setup_database.py

# Load sample data (if available)
python scripts/load_sample_data.py
```

### Environment Configuration

Create a development environment file:

```bash
# .env.dev
ENV=development
DEBUG=true
LOG_LEVEL=DEBUG

# Database
PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=highstreets_dev
PG_USER=your_dev_user
PG_PASSWORD=your_dev_password

# API credentials (use test/sandbox if available)
CONSUMER_KEY=your_test_key
CONSUMER_SECRET=your_test_secret

# File paths
BASE_DIR=./data/dev
```

### IDE Setup

#### VS Code Configuration

Create `.vscode/settings.json`:

```json
{
    "python.defaultInterpreterPath": "./.venv/bin/python",
    "python.linting.enabled": true,
    "python.linting.pylintEnabled": true,
    "python.linting.mypyEnabled": true,
    "python.formatting.provider": "black",
    "python.formatting.blackArgs": ["--line-length=88"],
    "python.sortImports.args": ["--profile=black"],
    "[python]": {
        "editor.formatOnSave": true,
        "editor.codeActionsOnSave": {
            "source.organizeImports": true
        }
    }
}
```

#### PyCharm Configuration

1. **Set interpreter**: File → Settings → Project → Python Interpreter
2. **Enable Black formatter**: File → Settings → Tools → External Tools
3. **Configure pytest**: Run → Edit Configurations → Add pytest configuration

## Code Standards

### Style Guidelines

We follow [PEP 8](https://pep8.org/) with some modifications:

- **Line length**: 88 characters (Black default)
- **Indentation**: 4 spaces
- **Quotes**: Double quotes for strings, single quotes for short/simple strings
- **Imports**: Organized with isort (black profile)

### Code Formatting

Use **Black** and **flake8** for code formatting:

```bash
# Format all code
poetry run black .

# Check formatting
poetry run black --check .
```

### Import Organization

Use **isort** for import organization:

```bash
# Sort imports
poetry run isort .

# Check import sorting
poetry run isort --check-only .
```

### Type Hints

Use type hints for all new code:

```python
from typing import List, Dict, Optional, Union
import pandas as pd

def process_data(
    data: List[Dict[str, Union[str, int, float]]],
    start_date: str,
    end_date: Optional[str] = None
) -> pd.DataFrame:
    """
    Process raw data into DataFrame.
    
    Args:
        data: Raw data from API
        start_date: Processing start date in YYYY-MM-DD format
        end_date: Processing end date (optional)
    
    Returns:
        Processed DataFrame
        
    Raises:
        ValueError: If date format is invalid
    """
    # Implementation here
    pass
```

### Documentation Standards

#### Docstring Format

Use **Google-style docstrings**:

```python
def complex_function(param1: str, param2: int, param3: bool = False) -> Dict[str, Any]:
    """
    Brief description of what the function does.
    
    Longer description if needed, explaining the purpose,
    algorithm, or important details about the function.
    
    Args:
        param1: Description of param1
        param2: Description of param2  
        param3: Description of param3 (default: False)
    
    Returns:
        Dictionary containing processed results with keys:
        - 'status': Processing status ('success' or 'error')
        - 'data': Processed data or None if error
        - 'message': Status message
    
    Raises:
        ValueError: If param1 is empty or invalid
        ProcessingError: If data processing fails
        
    Example:
        >>> result = complex_function("test", 42, True)
        >>> print(result['status'])
        success
        
    Note:
        This function modifies global state and should be used carefully
        in multi-threaded environments.
    """
    pass
```

#### Code Comments

- Use comments to explain **why**, not **what**
- Add comments for complex algorithms or business logic
- Keep comments up-to-date with code changes

```python
# Good: Explains why
# Use exponential backoff to handle API rate limiting
for attempt in range(max_retries):
    time.sleep(2 ** attempt)
    
# Bad: Explains what (obvious from code)
# Increment counter by 1
counter += 1
```

### Error Handling

Use consistent error handling patterns:

```python
import logging
from typing import Optional

logger = logging.getLogger(__name__)

class DataProcessingError(Exception):
    """Raised when data processing fails."""
    pass

def process_data_with_error_handling(data: List[Dict]) -> Optional[pd.DataFrame]:
    """
    Process data with comprehensive error handling.
    
    Args:
        data: Raw data to process
        
    Returns:
        Processed DataFrame or None if processing fails
    """
    try:
        # Validate input
        if not data:
            raise ValueError("Input data is empty")
            
        # Process data
        df = pd.DataFrame(data)
        
        # Validate output
        if df.empty:
            logger.warning("Processing resulted in empty DataFrame")
            return None
            
        logger.info(f"Successfully processed {len(df)} records")
        return df
        
    except ValueError as e:
        logger.error(f"Input validation failed: {e}")
        raise DataProcessingError(f"Invalid input data: {e}") from e
        
    except Exception as e:
        logger.error(f"Unexpected error during processing: {e}")
        raise DataProcessingError(f"Processing failed: {e}") from e
```

## Contributing Workflow

### Branch Strategy

We use **Git Flow** with the following branches:

- `main`: Production-ready code
- `develop`: Integration branch for features
- `feature/*`: New features
- `bugfix/*`: Bug fixes
- `hotfix/*`: Critical production fixes
- `release/*`: Release preparation

### Creating a Feature

1. **Sync with upstream**:
   ```bash
   git checkout develop
   git pull upstream develop
   ```

2. **Create feature branch**:
   ```bash
   git checkout -b feature/your-feature-name
   ```

3. **Make changes** following code standards

4. **Run tests** locally:
   ```bash
   poetry run pytest
   poetry run black --check .
   poetry run isort --check-only .
   poetry run mypy .
   ```

5. **Commit changes**:
   ```bash
   git add .
   git commit -m "feat: add new data processing feature
   
   - Implement hex grid aggregation
   - Add validation for geographic boundaries
   - Update documentation
   
   Closes #123"
   ```

6. **Push to your fork**:
   ```bash
   git push origin feature/your-feature-name
   ```

### Commit Message Format

Use **Conventional Commits** format:

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

**Types**:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `test`: Adding or updating tests
- `chore`: Maintenance tasks

**Examples**:
```bash
feat(api): add BT API rate limiting support

Add exponential backoff and retry logic to handle API rate limits.
Includes configuration options for retry attempts and delay intervals.

Closes #456

fix(transform): handle null values in hex data

Previously, null values in visitor counts caused transformation failures.
Now null values are properly handled and logged as warnings.

Fixes #789

docs: update installation guide for Windows users

Add specific instructions for Windows PowerShell and common
troubleshooting steps for dependency installation.
```

## Testing Guidelines

### Test Structure

```
tests/
├── unit/                   # Unit tests
│   ├── test_dataloader.py
│   ├── test_transforms.py
│   └── test_api_client.py
├── integration/            # Integration tests
│   ├── test_database.py
│   └── test_api_integration.py
├── end_to_end/            # End-to-end tests
│   └── test_full_pipeline.py
├── fixtures/              # Test data
│   ├── sample_data.json
│   └── test_config.yaml
└── conftest.py            # Pytest configuration
```

### Writing Tests

#### Unit Tests

```python
import pytest
import pandas as pd
from unittest.mock import Mock, patch
from highstreets.data_transformation.hextransform import HexTransform

class TestHexTransform:
    """Test cases for HexTransform class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.transformer = HexTransform()
        self.sample_data = [
            {
                "hex_id": "test_001",
                "date": "2023-01-01",
                "time_indicator": "09-12",
                "total_volume": 1000,
                "worker_pop_percent": 30.0,
                "resident_pop_percent": 40.0,
                "loyalty_percentage": 65.0,
                "dwell_time": 45.5
            }
        ]
    
    def test_transform_data_valid_input(self):
        """Test transformation with valid input data."""
        result = self.transformer.transform_data(self.sample_data)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert "hex_id" in result.columns
        assert "count_date" in result.columns
        assert result["hex_id"].iloc[0] == "test_001"
    
    def test_transform_data_empty_input(self):
        """Test transformation with empty input."""
        with pytest.raises(ValueError, match="Input data is empty"):
            self.transformer.transform_data([])
    
    @patch('highstreets.data_transformation.hextransform.pd.DataFrame')
    def test_transform_data_processing_error(self, mock_dataframe):
        """Test handling of processing errors."""
        mock_dataframe.side_effect = Exception("Processing error")
        
        with pytest.raises(Exception):
            self.transformer.transform_data(self.sample_data)
```

#### Integration Tests

```python
import pytest
from highstreets.data_source_sink.dataloader import DataLoader

class TestDatabaseIntegration:
    """Integration tests for database operations."""
    
    @pytest.fixture
    def loader(self):
        """Create DataLoader instance for testing."""
        return DataLoader()
    
    def test_database_connection(self, loader):
        """Test database connection works."""
        # This test requires a test database
        with loader.engine.connect() as conn:
            result = conn.execute("SELECT 1 as test")
            assert result.fetchone()[0] == 1
    
    @pytest.mark.slow
    def test_full_data_load(self, loader):
        """Test loading data from test table."""
        # This test may be slow, mark as such
        data = loader.get_full_data('test_table')
        assert data is not None
```

### Test Configuration

#### conftest.py

```python
import pytest
import os
import tempfile
from sqlalchemy import create_engine
from highstreets.data_source_sink.dataloader import DataLoader

@pytest.fixture(scope="session")
def test_database():
    """Create test database for integration tests."""
    # Use in-memory SQLite for fast tests
    engine = create_engine("sqlite:///:memory:")
    
    # Create test tables
    with engine.connect() as conn:
        conn.execute("""
            CREATE TABLE test_table (
                id INTEGER PRIMARY KEY,
                name TEXT,
                value REAL
            )
        """)
        
        # Insert test data
        conn.execute("""
            INSERT INTO test_table (name, value) 
            VALUES ('test1', 1.0), ('test2', 2.0)
        """)
    
    yield engine
    engine.dispose()

@pytest.fixture
def temp_directory():
    """Create temporary directory for file tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir

@pytest.fixture
def sample_api_response():
    """Sample API response for testing."""
    return [
        {
            "hex_id": "test_001",
            "date": "2023-01-01",
            "time_indicator": "09-12",
            "total_volume": 1000,
            "worker_pop_percent": 30.0,
            "resident_pop_percent": 40.0
        }
    ]

# Markers for different test types
def pytest_configure(config):
    """Configure pytest markers."""
    config.addinivalue_line("markers", "slow: marks tests as slow")
    config.addinivalue_line("markers", "integration: marks tests as integration tests")
    config.addinivalue_line("markers", "api: marks tests that require API access")
```

### Running Tests

```bash
# Run all tests
poetry run pytest

# Run with coverage
poetry run pytest --cov=highstreets --cov-report=html

# Run specific test categories
poetry run pytest tests/unit/
poetry run pytest -m "not slow"
poetry run pytest -k "test_transform"

# Run with verbose output
poetry run pytest -v

# Run in parallel (if pytest-xdist installed)
poetry run pytest -n auto
```

## Documentation Guidelines

### Documentation Types

1. **API Documentation**: Auto-generated from docstrings
2. **User Guides**: Step-by-step instructions
3. **Tutorials**: Learning-oriented content
4. **Architecture**: System design and technical details

### Writing Documentation

#### Markdown Style

- Use **clear headings** (H1 for main sections, H2 for subsections)
- Add **table of contents** for long documents
- Use **code blocks** with language specification
- Include **examples** where appropriate
- Add **cross-references** to related documentation

#### Code Examples

Always include complete, runnable examples:

```python
# Good: Complete example
from highstreets.data_source_sink.dataloader import DataLoader

def example_usage():
    """Example of loading data."""
    loader = DataLoader()
    data = loader.get_hex_data("2023-01-01", "2023-01-31")
    print(f"Loaded {len(data)} records")

if __name__ == "__main__":
    example_usage()
```

```python
# Bad: Incomplete example
loader.get_hex_data("2023-01-01", "2023-01-31")
```

### Building Documentation

If using automated documentation generation:

```bash
# Generate API documentation
poetry run sphinx-build docs/ docs/_build/

# Serve documentation locally
poetry run python -m http.server 8000 --directory docs/_build/
```

## Pull Request Process

### Before Submitting

1. **Ensure tests pass**:
   ```bash
   poetry run pytest
   poetry run black --check .
   poetry run isort --check-only .
   poetry run mypy .
   ```

2. **Update documentation** if needed

3. **Add changelog entry** (if applicable)

4. **Rebase on latest develop**:
   ```bash
   git checkout develop
   git pull upstream develop
   git checkout feature/your-feature
   git rebase develop
   ```

### Pull Request Template

Use this template for pull requests:

```markdown
## Description
Brief description of changes

## Type of Change
- [ ] Bug fix (non-breaking change which fixes an issue)
- [ ] New feature (non-breaking change which adds functionality)
- [ ] Breaking change (fix or feature that would cause existing functionality to not work as expected)
- [ ] Documentation update

## Testing
- [ ] Unit tests pass
- [ ] Integration tests pass
- [ ] Manual testing completed

## Documentation
- [ ] Code is documented
- [ ] User documentation updated
- [ ] API documentation updated

## Checklist
- [ ] Code follows style guidelines
- [ ] Self-review completed
- [ ] Tests added/updated
- [ ] Documentation updated
- [ ] No merge conflicts

## Related Issues
Closes #123
Related to #456

## Screenshots (if applicable)

## Additional Notes
Any additional information that reviewers should know
```

### Review Process

1. **Automated checks** must pass (CI/CD)
2. **Code review** by maintainers
3. **Address feedback** and update PR
4. **Final approval** by project maintainer
5. **Merge** to develop branch

## Release Process

### Version Numbering

We use **Semantic Versioning** (SemVer):

- `MAJOR.MINOR.PATCH` (e.g., `1.2.3`)
- **MAJOR**: Breaking changes
- **MINOR**: New features (backward compatible)
- **PATCH**: Bug fixes (backward compatible)

### Creating Releases

1. **Create release branch**:
   ```bash
   git checkout develop
   git pull upstream develop
   git checkout -b release/v1.2.0
   ```

2. **Update version numbers** in:
   - `pyproject.toml`
   - `highstreets/__init__.py`
   - Documentation configs

3. **Update CHANGELOG.md**

4. **Run full test suite**

5. **Create release PR** to main

6. **After merge**, create release tag:
   ```bash
   git checkout main
   git pull upstream main
   git tag -a v1.2.0 -m "Release version 1.2.0"
   git push upstream v1.2.0
   ```

### Changelog Format

```markdown
# Changelog

## [1.2.0] - 2023-12-01

### Added
- New BT API rate limiting support
- Hex grid aggregation improvements
- Advanced error handling

### Changed
- Updated data validation logic
- Improved performance for large datasets

### Fixed
- Fixed null value handling in transformations
- Resolved memory leak in batch processing

### Deprecated
- Old API client methods (use new APIClient class)

### Removed
- Deprecated helper functions

### Security
- Updated dependencies with security patches
```

## Community Guidelines

### Code of Conduct

We follow the [Contributor Covenant Code of Conduct](https://www.contributor-covenant.org/). Please read and follow it.

### Communication

- **Be respectful** and professional
- **Be constructive** in feedback
- **Be patient** with new contributors
- **Ask questions** if something is unclear

### Getting Help

- **Documentation**: Check existing documentation first
- **Issues**: Search existing issues before creating new ones
- **Discussions**: Use GitHub Discussions for questions
- **Email**: Contact maintainers for sensitive issues

### Recognition

Contributors are recognized in:

- **CONTRIBUTORS.md** file
- **Release notes** for significant contributions
- **GitHub contributors** page

Thank you for contributing to the Highstreets project! 🎉 
# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- CI/CD: GitHub Actions workflows for linting/testing (`test.yml`) and ECR deployment (`deploy.yml`)
- `.env.example` with all required environment variables
- `safe_append_data()` method in `DataWriter` for idempotent date-range writes
- Great Expectations validation for Mastercard output (`mcard_output_validation.py`)
- Great Expectations validation for BT output (`bt_output_validation.py`)
- BT auto-scheduler Lambda (`lambda/bt_scheduler.py`) with PostgreSQL freshness check
- Data freshness monitoring Lambda (`lambda/data_freshness_check.py`)
- SQL index definitions for Mastercard performance (`create_mcard_indexes.sql`)
- SQL UNIQUE constraints for BT tables (`create_bt_constraints.sql`)
- Master lookup generation: quad-to-all and hex-to-all unified lookup tables
- `.cursor/rules/highstreets.mdc` project context file

### Changed
- `mcard_weekly.py`: parallelised 8 aggregation queries, cached shared lookups
- `append_chunk()`: now uses `method='multi', chunksize=5000` for faster writes
- `append_data_without_check()`: now uses `method='multi', chunksize=5000`
- Docker: multi-stage build (builder + runtime), smaller production image
- `pyproject.toml`: Python version narrowed to `>=3.10,<3.12`; dev-only deps moved to dev group
- All pipeline scripts wrapped in `if __name__ == "__main__":` guards
- `bt_outage.py`: added `START_DATE`/`END_DATE` validation
- `mcard_adjust_weekly()`: accepts optional cached lookups to avoid redundant DB reads

### Removed
- `highstreets/highstreets.py` (empty placeholder)
- `highstreets/data/bt_read_raw.py` (dead code referencing non-existent functions)
- `highstreets/core/processors/sublicense_processor.py` (superseded by `SublicenseManager`)

## [0.1.0] - 2024-01-01

### Added
- AWS Batch pipeline with Step Functions orchestration
- BT footfall pipeline (hex, MSOA, LSOA, daily aggregate, outage, catchment)
- Mastercard pipeline (weekly, international, 3-hourly)
- LDC premises ETL with Great Expectations validation
- Sublicense management system
- SQL-based transform architecture via `SQLManager`
- London Datastore integration via `datapress`

## [0.0.1] - 2022-04-05

### Added
- Initial project setup with Poetry
- Core data loading and transformation modules

[Unreleased]: https://github.com/Greater-London-Authority/highstreets/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/Greater-London-Authority/highstreets/compare/v0.0.1...v0.1.0
[0.0.1]: https://github.com/Greater-London-Authority/highstreets/releases/tag/v0.0.1

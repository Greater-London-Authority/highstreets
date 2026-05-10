---
title: "LDC Premises Data Source"
space: "CDU"
parent: "Highstreets Data Platform - Overview"
type: "page"
labels: ["ldc", "premises", "data-source", "snowflake", "etl"]
---

# LDC Premises Data Source

## Overview

The LDC (Local Data Company) premises dataset provides a complete record of commercial business occupancy across the UK from 2014 to present. It tracks every retail, leisure, and service premises -- when businesses open, close, change name, or relocate -- creating a longitudinal history of the commercial landscape.

## Data Provider

- **Source**: Local Data Company (LDC), part of the Data Analytics Group
- **Database**: Snowflake (LDC-hosted, accessed via `snowflake-sqlalchemy`)
- **Coverage**: All UK commercial premises (London subset used by HSDS)
- **Update Frequency**: Monthly pipeline run (Snowflake data refreshed continuously by LDC surveyors)

## Data Structure

### Core Identifiers

| Identifier | Scope | Persistence | Example |
|------------|-------|-------------|---------|
| `premises_id` | Physical address | Stable unless property splits/merges | `50314472` |
| `tenant_id` | Specific business occupancy at a premises | One per business-at-location instance | `14078166` |
| `date_create` | Start date of the tenancy | Immutable once set | `2023-03-31` |

### Record Lifecycle

Each row represents a **tenancy** -- a specific business occupying a specific premises during a specific period. New rows are created when:

1. A business opens at a location (new `tenant_id` + `premises_id`)
2. A business closes and a new one opens at the same location
3. A premises becomes vacant
4. A concession (store-within-store) opens inside an existing business
5. A property splits into multiple units or merges

When a business closes, the existing record is updated with a `date_close` and a new record is created for whatever replaces it.

### Data Sources (Three Eras)

| Source | Tag | Period | Rows | Description |
|--------|-----|--------|------|-------------|
| Historic Excel | `historic` | 2014-2024 | ~734K | Static 10-year time series file from LDC |
| Snowflake DB | `live` | Rolling ~12 months | ~895K | Current live view -- all open businesses plus ~12 months of closures |
| Archived Backups | `archived` | Gap-fill | ~36K | Snapshots from previous pipeline runs to fill the gap period |

### The Gap Problem (Solved)

LDC's Snowflake database is a **rolling window** -- businesses that closed more than ~12 months ago silently disappear. The historic Excel only covers up to November 2024. Between these two sources, a gap existed for businesses that closed between ~Nov 2024 and early 2025.

**Our solution**: The pipeline accumulates all data in PostgreSQL. Once a record is ingested, it is never lost. Monthly Snowflake pulls add new records and update existing ones via hash-based upsert, but never delete. The S3 Parquet archive provides an additional immutable audit trail.

## Pipeline Architecture

### Storage Tiers

| Tier | Storage | Columns | Purpose | Query Method |
|------|---------|---------|---------|-------------|
| **Hot** | PostgreSQL `ldc_premises_raw` | 60 | Working dataset for transformations | SQL via application |
| **Hot** | PostgreSQL `ldc_premises_clean` | 38 | Analyst-facing output | SQL via application |
| **Cold** | S3 Parquet (snapshots) | 162 | Monthly Snowflake archives, exactly as received | Athena via Glue |
| **Cold** | S3 Parquet (baseline) | 195 | One-time initial load archive | Athena via Glue |
| **Cold** | S3 Parquet (clean) | 38 | Monthly clean output archives | Athena via Glue |

### Monthly Pipeline Flow (12 Steps)

```
Step 1:  Snowflake Fetch ─────────────────── ~895K rows, 162 columns
Step 2:  Schema Validation (Suite 1: STOP) ─ Critical columns, PK integrity
Step 3:  S3 Archive (raw snapshot) ───────── s3://hsds-data/ldc/snowflake_snapshots/year=YYYY/month=MM/
Step 4:  Select 60 working columns + hash ── Row-level hash for change detection
Step 5:  Upsert to PostgreSQL raw ────────── INSERT ON CONFLICT with hash comparison
Step 6:  Raw Quality Validation (Suite 2: WARN)
Step 7:  Read full accumulated raw from PG ── All historical data (~1.68M+ rows)
Step 8:  Transform (7-step pipeline) ──────── basic_formatting → fix_premises_dates → ... → choose_columns
Step 9:  Business Logic Validation (Suite 3: WARN)
Step 10: Clean Output Validation (Suite 4: STOP)
Step 11: Truncate + reload clean table ───── Full refresh of analyst-facing table
Step 12: S3 Archive (clean) ──────────────── s3://hsds-data/ldc/clean_archives/year=YYYY/month=MM/
```

### Upsert Strategy

The pipeline uses **hash-based change detection** to efficiently handle ~895K incoming rows against ~1.68M+ accumulated rows:

1. Compute `row_hash` via `pd.util.hash_pandas_object` (truncated to 32-bit hex) over all working columns (excluding metadata)
2. Load to UNLOGGED staging table (TRUNCATE + append)
3. `INSERT INTO raw SELECT FROM staging ON CONFLICT (tenant_id, premises_id, date_create) DO UPDATE SET ... WHERE raw.row_hash IS DISTINCT FROM EXCLUDED.row_hash`

This ensures:
- New records (new PK) → INSERT
- Changed records (same PK, different hash) → UPDATE with `ingested_at = NOW()`
- Unchanged records → no write, no I/O

### Transformation Pipeline

The transformation logic is preserved from the original DS team's `hsds_datahub/src/etl/premises.py`. It produces 38 analyst-facing columns from the 57-column raw data:

| Step | Function | Effect |
|------|----------|--------|
| 1 | `basic_formatting` | Datetime coercion, NaN handling, deduplication on `(tenant_id, premises_id, date_create)` with source priority |
| 2 | `fix_premises_dates` | Correct nonsensical date ranges (close < open), fill gaps in premises history |
| 3 | `_forward_fill_uprn` | Forward-fill UPRN by premises_id |
| 4 | `fetch_latest_checks` | Compute `latest_record_check` and `latest_premises_check` |
| 5 | `fill_vacant_use` | Set classification, category, and subcategory to `'Vacant'` for all vacant properties |
| 6 | `amend_floorspace` | Replace 0 floorspace with NULL |
| 7 | `choose_columns` | Select final 38 columns |

## AWS Infrastructure

### Compute

| Component | Service | Configuration |
|-----------|---------|---------------|
| Pipeline execution | AWS Batch | Single job, Docker container from ECR |
| Orchestration | Step Functions | Single-step state machine |
| Entrypoint | `ldc_premises_e2e.py` | `["poetry", "run", "python", "highstreets/aws_pipeline/ldc_premises_e2e.py"]` |

### Storage

| Component | Path | Format |
|-----------|------|--------|
| Snowflake snapshots | `s3://hsds-data/ldc/snowflake_snapshots/year=YYYY/month=MM/` | Parquet (Hive-partitioned) |
| Initial load baseline | `s3://hsds-data/ldc/initial_load/` | Parquet |
| Clean archives | `s3://hsds-data/ldc/clean_archives/year=YYYY/month=MM/` | Parquet |

### Analytics (Athena)

| Glue Database | Crawler | Athena Table | Schedule |
|---------------|---------|-------------|----------|
| `ldc_premises` | `ldc-baseline-crawler` | `baseline_*` | One-time |
| `ldc_premises` | `ldc-snapshots-crawler` | `snapshots_*` | Monthly |
| `ldc_premises` | `ldc-clean-crawler` | `clean_*` | Monthly |

## Data Quality (Great Expectations)

Four validation suites gate the pipeline at critical points:

| Suite | Stage | On Fail | Key Checks |
|-------|-------|---------|------------|
| 1: Source Schema | After Snowflake fetch | **STOP** | Critical columns exist (`tenant_id`, `premises_id`, `date_create`, `timestamp_update`, `latitude`, `longitude`, `tenant`, `source`), PK not null, PK unique (mostly=1.0) |
| 2: Raw Quality | After upsert | **WARN** | Nulls, UK geographic bounds, source values, date sanity, hash integrity |
| 3: Business Logic | After transform | **WARN** | Tenant status consistency, date ordering, PK near-uniqueness |
| 4: Clean Output | Before publish | **STOP** | Exact 38 columns, PK near-uniqueness, all critical not-null, geographic bounds, tenant status values |

**STOP** = pipeline halts, no data published. **WARN** = data ingested but issues logged for review.

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Raw table PK | Composite `(tenant_id, premises_id, date_create)` | Natural key from LDC, enables upsert |
| Clean table PK | `BIGSERIAL` surrogate | Rare SWS duplicates on composite key after `fix_premises_dates` |
| Dedup key | `(tenant_id, premises_id, date_create)` with source priority | Stable identifier (not mutable business name). See [Dedup Analysis](05.1-ldc-dedup-analysis.md) |
| Cold storage | S3 Parquet with Hive partitioning | Athena-queryable, compressed, schema-on-read for 195-column archives |
| Hot storage | 57 columns in PG (not 195) | Only columns needed for transform + clean output |
| Clean reload | Truncate + full reload (not incremental) | Transform logic has cross-row dependencies that can change existing clean rows |
| S3 archival | One file per month, overwrite | Prevents duplicate files from multiple runs within a month |

## Related Information

- **[LDC Dedup Analysis](05.1-ldc-dedup-analysis.md)**: Detailed analysis of the deduplication logic change
- **[LDC Data Flow Diagram](05.2-ldc-data-flow-diagram.md)**: Visual pipeline diagrams (AS-IS and TO-BE)
- **[LDC Data Dictionary](05.3-ldc-data-dictionary.md)**: Column-level schema reference for all tables
- **[Database Schema](04-database-schema.md)**: Platform-wide database architecture
- **[Data Sources](02-data-sources.md)**: Overview of all HSDS data sources

---

**LDC Data Summary**:
- **Coverage**: All UK commercial premises, 2014-present (~1.68M+ accumulated records)
- **Update Frequency**: Monthly Snowflake pull with hash-based upsert
- **Quality Assurance**: 4 Great Expectations validation suites (2 STOP gates, 2 WARN gates)
- **Dual Storage**: PostgreSQL for operational queries, S3 Parquet for archival and Athena analytics
- **Gap-Proof**: Accumulated architecture ensures no business records are ever lost

**Next Steps**: Review [LDC Dedup Analysis](05.1-ldc-dedup-analysis.md) for the data quality investigation

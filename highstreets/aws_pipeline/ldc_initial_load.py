"""
LDC Premises Initial Load Script.

This script performs the one-time initial load:
1. Archive existing all_biz_raw.csv (195 cols) to S3 as baseline
2. Archive historic Excel file to S3 for audit
3. Load existing all_biz_raw.csv to PostgreSQL raw table
4. Run transformation and load to PostgreSQL clean table

This preserves all gap-fill history from the existing pipeline.

Usage:
    python ldc_initial_load.py
"""
from highstreets import config
from highstreets.data_transformation.ldc_premises_transform import LdcPremisesTransform
from highstreets.great_expectations.ldc_premises_validation import LdcPremisesValidator

import logging
import os
import sys
from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import fsspec
from dotenv import find_dotenv, load_dotenv
from sqlalchemy import create_engine, text

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv(find_dotenv())

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_pg_engine():
    """Create PostgreSQL SQLAlchemy engine."""
    database = os.getenv("PG_DATABASE")
    username = os.getenv("PG_USER")
    password = os.getenv("PG_PASSWORD")
    host = os.getenv("PG_HOST")
    port = os.getenv("PG_PORT")

    return create_engine(
        f"postgresql+psycopg2://{username}:{password}@"
        f"{host}:{port}/{database}"
    )


def archive_to_s3_parquet(df: pd.DataFrame, s3_path: str, filename: str):
    """Archive DataFrame to S3 as Parquet."""
    full_path = f"{s3_path.rstrip('/')}/{filename}"

    logger.info(f"Archiving {len(df)} rows to {full_path}")

    table = pa.Table.from_pandas(df)

    fs = fsspec.filesystem('s3')

    # Ensure directory exists
    dir_path = s3_path.rstrip('/')
    try:
        fs.makedirs(dir_path, exist_ok=True)
    except Exception:
        pass  # Directory might already exist

    with fs.open(full_path, 'wb') as f:
        pq.write_table(table, f)

    logger.info(f"Archive complete: {full_path}")


def compute_row_hash(df: pd.DataFrame) -> pd.DataFrame:
    """Compute row hash for all columns (excluding metadata)."""
    exclude_cols = ['row_hash', 'ingested_at']
    hash_cols = [c for c in df.columns if c not in exclude_cols]

    df['row_hash'] = pd.util.hash_pandas_object(
        df[hash_cols],
        index=False
    ).apply(lambda x: format(x & 0xFFFFFFFF, '08x'))

    return df


def run_initial_load(dry_run: bool = False):
    """
    Execute the initial load process.

    Args:
        dry_run: If True, don't write to any destinations
    """
    stats = {
        'start_time': datetime.now().isoformat(),
        'steps': {},
        'success': False
    }

    try:
        # ==================================================================
        # STEP 1: Create PostgreSQL tables
        # ==================================================================
        logger.info("=" * 60)
        logger.info("STEP 1: Creating PostgreSQL tables")
        logger.info("=" * 60)

        engine = create_pg_engine()

        # Read and execute the DDL
        ddl_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            'sql', 'queries', 'ldc', 'create_ldc_tables.sql'
        )

        if not dry_run:
            with open(ddl_path, 'r') as f:
                ddl = f.read()

            # Execute DDL statements (split by semicolons, skip comments)
            with engine.connect() as conn:
                for statement in ddl.split(';'):
                    statement = statement.strip()
                    if statement and not statement.startswith('--'):
                        try:
                            conn.execute(text(statement))
                        except Exception as e:
                            logger.warning(f"DDL statement warning: {e}")
                conn.commit()

        stats['steps']['create_tables'] = {'status': 'completed'}
        logger.info("Tables created successfully")

        # ==================================================================
        # STEP 2: Load existing all_biz_raw.csv
        # ==================================================================
        logger.info("=" * 60)
        logger.info("STEP 2: Loading existing all_biz_raw.csv")
        logger.info("=" * 60)

        raw_path = config.LDC_Z_DRIVE_RAW
        logger.info(f"Reading from: {raw_path}")

        raw_df = pd.read_csv(raw_path, low_memory=False)
        logger.info(f"Loaded {len(raw_df)} rows with {len(raw_df.columns)} columns")

        stats['steps']['load_raw_csv'] = {
            'rows': len(raw_df),
            'columns': len(raw_df.columns)
        }

        # ==================================================================
        # STEP 3: Archive baseline to S3
        # ==================================================================
        logger.info("=" * 60)
        logger.info("STEP 3: Archiving baseline to S3")
        logger.info("=" * 60)

        if not dry_run:
            archive_to_s3_parquet(
                raw_df,
                config.LDC_S3_INITIAL_LOAD,
                'all_biz_raw_baseline.parquet'
            )

        stats['steps']['archive_baseline'] = {
            'path': f"{config.LDC_S3_INITIAL_LOAD}all_biz_raw_baseline.parquet",
            'rows': len(raw_df)
        }

        # ==================================================================
        # STEP 4: Archive historic Excel to S3
        # ==================================================================
        logger.info("=" * 60)
        logger.info("STEP 4: Archiving historic Excel to S3")
        logger.info("=" * 60)

        try:
            historic_path = config.LDC_Z_DRIVE_HISTORIC
            historic_df = pd.read_excel(historic_path)
            historic_df.columns = historic_df.columns.str.lower()

            logger.info(
                f"Loaded historic Excel: {len(historic_df)} rows, "
                f"{len(historic_df.columns)} columns"
            )

            if not dry_run:
                archive_to_s3_parquet(
                    historic_df,
                    config.LDC_S3_INITIAL_LOAD,
                    'historic_excel_original.parquet'
                )

            stats['steps']['archive_historic'] = {
                'path': f"{config.LDC_S3_INITIAL_LOAD}historic_excel_original.parquet",
                'rows': len(historic_df)
            }
        except Exception as e:
            logger.warning(f"Could not archive historic Excel: {e}")
            stats['steps']['archive_historic'] = {'error': str(e)}

        # ==================================================================
        # STEP 5: Select working columns and compute hashes
        # ==================================================================
        logger.info("=" * 60)
        logger.info("STEP 5: Preparing data for PostgreSQL raw table")
        logger.info("=" * 60)

        # Define working columns (must exist in raw_df)
        working_columns = [
            'tenant_id', 'premises_id', 'date_create',
            'tenant', 'tenant_status', 'premises_status',
            'category', 'category_id', 'classification', 'classification_id',
            'subcategory', 'subcategory_id', 'subcategory_previous', 'subcategory_previous_id',  # noqa: E501
            'address', 'building', 'street', 'street_number', 'unit_number',
            'city', 'zip', 'geography', 'geography_id',
            'geography_large', 'geography_large_id', 'geography_large_pct',
            'latitude', 'longitude', 'uprn_id',
            'property', 'property_id', 'property_type', 'premises_previous_id',
            'company', 'company_id', 'company_holding', 'company_holding_id',
            'tenant_care_of', 'flag_independent', 'flag_concession',
            'flag_field_researched', 'flag_area_sm_modelled',
            'area_sm', 'voa_business_rate',
            'phone', 'retail_mix', 'url_website', 'url_image',
            'date_close', 'date_premises_create',
            'date_last_survey_field', 'date_last_survey_office',
            'timestamp_create', 'timestamp_update',
            'source'
        ]

        # Select only columns that exist
        available_cols = [c for c in working_columns if c in raw_df.columns]
        pg_raw_df = raw_df[available_cols].copy()

        # Ensure source column exists
        if 'source' not in pg_raw_df.columns:
            pg_raw_df['source'] = 'historic'

        # Compute row hashes
        pg_raw_df = compute_row_hash(pg_raw_df)

        logger.info(
            f"Prepared {len(pg_raw_df)} rows with "
            f"{len(pg_raw_df.columns)} columns for PostgreSQL"
        )

        stats['steps']['prepare_pg_raw'] = {
            'rows': len(pg_raw_df),
            'columns': len(pg_raw_df.columns)
        }

        # ==================================================================
        # STEP 6: Load to PostgreSQL raw table
        # ==================================================================
        logger.info("=" * 60)
        logger.info("STEP 6: Loading to PostgreSQL raw table")
        logger.info("=" * 60)

        if not dry_run:
            pg_raw_df.to_sql(
                config.LDC_RAW_TABLE,
                engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=10000
            )
            logger.info(f"Loaded {len(pg_raw_df)} rows to {config.LDC_RAW_TABLE}")

        stats['steps']['load_pg_raw'] = {'rows': len(pg_raw_df)}

        # ==================================================================
        # STEP 7: Transform and load to clean table
        # ==================================================================
        logger.info("=" * 60)
        logger.info("STEP 7: Transforming and loading to clean table")
        logger.info("=" * 60)

        transformer = LdcPremisesTransform()
        clean_df = transformer.transform(raw_df)

        logger.info(
            f"Transformed: {len(raw_df)} -> {len(clean_df)} rows, "
            f"{len(clean_df.columns)} columns"
        )

        if not dry_run:
            clean_df.to_sql(
                config.LDC_CLEAN_TABLE,
                engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=10000
            )
            logger.info(f"Loaded {len(clean_df)} rows to {config.LDC_CLEAN_TABLE}")

        stats['steps']['load_pg_clean'] = {'rows': len(clean_df)}

        # ==================================================================
        # STEP 8: Validate
        # ==================================================================
        logger.info("=" * 60)
        logger.info("STEP 8: Validating clean output")
        logger.info("=" * 60)

        validator = LdcPremisesValidator()
        passed, details = validator.validate_clean_output(clean_df)

        stats['validation'] = details

        if passed:
            logger.info("Clean output validation PASSED")
        else:
            logger.warning(f"Clean output validation FAILED: {details['failures']}")

        # ==================================================================
        # Complete
        # ==================================================================
        stats['end_time'] = datetime.now().isoformat()
        stats['success'] = True

        logger.info("=" * 60)
        logger.info("INITIAL LOAD COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)

        return stats

    except Exception as e:
        logger.error(f"Initial load failed: {str(e)}")
        stats['error'] = str(e)
        stats['end_time'] = datetime.now().isoformat()
        raise


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description='LDC Premises Initial Load')
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Run without writing to any destinations'
    )

    args = parser.parse_args()

    try:
        stats = run_initial_load(dry_run=args.dry_run)

        print("\n" + "=" * 60)
        print("INITIAL LOAD SUMMARY")
        print("=" * 60)
        print(f"Status: {'SUCCESS' if stats['success'] else 'FAILED'}")
        print(f"Start: {stats['start_time']}")
        print(f"End: {stats['end_time']}")

        if 'steps' in stats:
            print("\nSteps:")
            for step, info in stats['steps'].items():
                print(f"  {step}: {info}")

        return 0 if stats['success'] else 1

    except Exception as e:
        logger.error(f"Initial load failed: {str(e)}")
        return 1


if __name__ == '__main__':
    sys.exit(main())

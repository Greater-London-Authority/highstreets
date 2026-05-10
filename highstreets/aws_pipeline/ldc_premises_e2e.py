"""
LDC Premises ETL End-to-End Pipeline.

This script orchestrates the complete monthly LDC premises data pipeline:
1.  Fetch data from Snowflake
2.  Validate source schema (Suite 1 - STOP on fail)
3.  Archive full raw snapshot to S3 Parquet
4.  Select working columns, compute row hashes, upsert to PostgreSQL raw
5.  Validate raw data quality (Suite 2 - WARN on fail)
6.  Read accumulated raw from PostgreSQL, transform
7.  Validate business logic (Suite 3 - WARN on fail)
8.  Validate clean output (Suite 4 - STOP on fail)
9.  Load to PostgreSQL clean table (truncate + reload)
10. Archive clean data to S3 Parquet

Usage:
    python -m highstreets.aws_pipeline.ldc_premises_e2e
    python -m highstreets.aws_pipeline.ldc_premises_e2e --dry-run
    python -m highstreets.aws_pipeline.ldc_premises_e2e --no-validation
"""
from highstreets import config
from highstreets.data_source_sink.snowflake_loader import SnowflakeLoader
from highstreets.data_source_sink.datawriter import DataWriter
from highstreets.data_transformation.ldc_premises_transform import LdcPremisesTransform
from highstreets.great_expectations.ldc_premises_validation import LdcPremisesValidator

import logging
import os
import sys
from datetime import datetime

import pandas as pd
from dotenv import find_dotenv, load_dotenv
from sqlalchemy import create_engine, text

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv(find_dotenv())

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

PG_SCHEMA = 'gisapdata'


class LdcPremisesETLException(Exception):
    """Exception raised for LDC ETL pipeline errors."""
    pass


class LdcPremisesETL:
    """
    End-to-end ETL pipeline for LDC premises data.

    Orchestrates data flow from Snowflake through transformations
    to PostgreSQL and S3 outputs.
    """

    def __init__(
        self,
        run_validations: bool = True,
        dry_run: bool = False
    ):
        self.run_validations = run_validations
        self.dry_run = dry_run

        self.snowflake_loader = None
        self.data_writer = DataWriter()
        self.transformer = LdcPremisesTransform()
        self.validator = LdcPremisesValidator(log_to_cloudwatch=True)

        self.pg_engine = self._create_pg_engine()

        self.run_timestamp = datetime.now()
        self.run_year = self.run_timestamp.year
        self.run_month = self.run_timestamp.month

        logger.info(
            "LdcPremisesETL initialized - "
            f"run_validations={run_validations}, dry_run={dry_run}"
        )

    def _create_pg_engine(self):
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

    def compute_row_hash(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute row hash for ALL columns (excluding metadata).

        Uses pandas hash for efficiency, converts to hex string for storage.
        This ensures ANY change in ANY column is detected during upsert.
        """
        exclude_cols = ['row_hash', 'ingested_at']
        hash_cols = [c for c in df.columns if c not in exclude_cols]

        logger.info(f"Computing row hash over {len(hash_cols)} columns")

        df['row_hash'] = pd.util.hash_pandas_object(
            df[hash_cols],
            index=False
        ).apply(lambda x: format(x & 0xFFFFFFFF, '08x'))

        return df

    def upsert_to_postgres(
        self,
        df: pd.DataFrame,
        table_name: str
    ) -> int:
        """
        Upsert data to PostgreSQL using the DDL-defined UNLOGGED staging table.

        Truncates the existing staging table, loads incoming data via append,
        then runs INSERT ON CONFLICT with hash-based change detection.

        Returns:
            Number of rows affected (inserted + updated)
        """
        staging_table = config.LDC_STAGING_TABLE
        qualified_table = f"{PG_SCHEMA}.{table_name}"
        qualified_staging = f"{PG_SCHEMA}.{staging_table}"

        logger.info(f"Starting upsert of {len(df):,} rows to {qualified_table}")

        with self.pg_engine.connect() as conn:
            conn.execute(text(f"TRUNCATE TABLE {qualified_staging}"))
            conn.commit()

        df.to_sql(
            staging_table,
            self.pg_engine,
            schema=PG_SCHEMA,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=10000
        )
        logger.info(f"Loaded {len(df):,} rows to staging table")

        col_list = ', '.join(df.columns)
        pk_cols = ['tenant_id', 'premises_id', 'date_create']
        update_cols = [c for c in df.columns if c not in pk_cols]
        update_set = ', '.join(f"{c} = EXCLUDED.{c}" for c in update_cols)

        upsert_sql = f"""
        INSERT INTO {qualified_table} ({col_list})
        SELECT {col_list} FROM {qualified_staging}
        ON CONFLICT (tenant_id, premises_id, date_create)
        DO UPDATE SET
            {update_set},
            ingested_at = NOW()
        WHERE {qualified_table}.row_hash IS DISTINCT FROM EXCLUDED.row_hash
        """

        with self.pg_engine.connect() as conn:
            result = conn.execute(text(upsert_sql))
            conn.commit()
            affected_rows = result.rowcount

        logger.info(f"Upsert complete: {affected_rows:,} rows affected")
        return affected_rows

    def archive_to_s3_parquet(
        self,
        df: pd.DataFrame,
        s3_path: str,
        partition_by_month: bool = True
    ):
        """
        Archive DataFrame to S3 as Parquet.

        Uses "one file per month, overwrite on each run" strategy
        to avoid duplicate files from multiple runs within the same month.
        """
        import pyarrow as pa
        import pyarrow.parquet as pq
        import fsspec

        if partition_by_month:
            full_path = (
                f"{s3_path.rstrip('/')}/year={self.run_year}/"
                f"month={self.run_month:02d}/current.parquet"
            )
        else:
            full_path = f"{s3_path.rstrip('/')}/current.parquet"

        logger.info(f"Archiving {len(df):,} rows to {full_path}")

        table = pa.Table.from_pandas(df)
        fs = fsspec.filesystem('s3')
        with fs.open(full_path, 'wb') as f:
            pq.write_table(table, f)

        logger.info(f"Archive complete: {full_path}")

    def run(self) -> dict:
        """
        Execute the full ETL pipeline.

        Returns:
            Dictionary with pipeline statistics and status
        """
        stats = {
            'start_time': self.run_timestamp.isoformat(),
            'steps': {},
            'validations': {},
            'success': False
        }

        try:
            # ==============================================================
            # STEP 1: Fetch data from Snowflake
            # ==============================================================
            logger.info("=" * 60)
            logger.info("STEP 1: Fetching data from Snowflake")
            logger.info("=" * 60)

            self.snowflake_loader = SnowflakeLoader()
            source_df = self.snowflake_loader.load_full_data()

            stats['steps']['snowflake_fetch'] = {
                'rows': len(source_df),
                'columns': len(source_df.columns)
            }
            logger.info(
                f"Fetched {len(source_df):,} rows with "
                f"{len(source_df.columns)} columns"
            )

            # ==============================================================
            # STEP 2: Validate source schema (STOP on fail)
            # ==============================================================
            if self.run_validations:
                logger.info("=" * 60)
                logger.info("STEP 2: Validating source schema")
                logger.info("=" * 60)

                passed, details = self.validator.validate_source_schema(
                    source_df
                )
                stats['validations']['source_schema'] = details

                if not passed:
                    raise LdcPremisesETLException(
                        f"Source schema validation failed: "
                        f"{details['failures']}"
                    )

            # ==============================================================
            # STEP 3: Archive full raw snapshot to S3
            # ==============================================================
            logger.info("=" * 60)
            logger.info("STEP 3: Archiving raw snapshot to S3")
            logger.info("=" * 60)

            if not self.dry_run:
                self.archive_to_s3_parquet(
                    source_df,
                    config.LDC_S3_SNOWFLAKE_SNAPSHOTS,
                    partition_by_month=True
                )

            stats['steps']['s3_raw_archive'] = {
                'path': config.LDC_S3_SNOWFLAKE_SNAPSHOTS,
                'rows': len(source_df)
            }

            # ==============================================================
            # STEP 4: Select working columns and compute hashes
            # ==============================================================
            logger.info("=" * 60)
            logger.info("STEP 4: Preparing data for PostgreSQL")
            logger.info("=" * 60)

            working_cols = self.snowflake_loader.WORKING_COLUMNS
            available_cols = [c for c in working_cols if c in source_df.columns]
            raw_df = source_df[available_cols].copy()
            raw_df['source'] = 'live'

            raw_df = self.compute_row_hash(raw_df)

            stats['steps']['prepare_raw'] = {
                'rows': len(raw_df),
                'columns': len(raw_df.columns)
            }

            # ==============================================================
            # STEP 5: Upsert to PostgreSQL raw table
            # ==============================================================
            logger.info("=" * 60)
            logger.info("STEP 5: Upserting to PostgreSQL raw table")
            logger.info("=" * 60)

            if not self.dry_run:
                affected_rows = self.upsert_to_postgres(
                    raw_df, config.LDC_RAW_TABLE
                )
                stats['steps']['upsert_raw'] = {
                    'affected_rows': affected_rows
                }

            # ==============================================================
            # STEP 6: Validate raw data quality (WARN on fail)
            # ==============================================================
            if self.run_validations:
                logger.info("=" * 60)
                logger.info("STEP 6: Validating raw data quality")
                logger.info("=" * 60)

                passed, details = self.validator.validate_raw_quality(raw_df)
                stats['validations']['raw_quality'] = details

            # ==============================================================
            # STEP 7: Load accumulated raw and transform
            # ==============================================================
            logger.info("=" * 60)
            logger.info("STEP 7: Loading accumulated raw from PostgreSQL")
            logger.info("=" * 60)

            qualified_raw = f"{PG_SCHEMA}.{config.LDC_RAW_TABLE}"
            with self.pg_engine.connect() as conn:
                accumulated_raw = pd.read_sql(
                    text(f"SELECT * FROM {qualified_raw}"), conn
                )

            stats['steps']['load_accumulated'] = {
                'rows': len(accumulated_raw)
            }
            logger.info(
                f"Loaded {len(accumulated_raw):,} accumulated raw rows"
            )

            # ==============================================================
            # STEP 8: Transform data
            # ==============================================================
            logger.info("=" * 60)
            logger.info("STEP 8: Transforming data")
            logger.info("=" * 60)

            clean_df = self.transformer.transform(accumulated_raw)

            stats['steps']['transform'] = {
                'rows_in': len(accumulated_raw),
                'rows_out': len(clean_df),
                'columns_out': len(clean_df.columns)
            }
            logger.info(
                f"Transformed: {len(accumulated_raw):,} -> "
                f"{len(clean_df):,} rows"
            )

            # ==============================================================
            # STEP 9: Validate business logic (WARN on fail)
            # ==============================================================
            if self.run_validations:
                logger.info("=" * 60)
                logger.info("STEP 9: Validating business logic")
                logger.info("=" * 60)

                passed, details = self.validator.validate_business_logic(
                    clean_df
                )
                stats['validations']['business_logic'] = details

            # ==============================================================
            # STEP 10: Validate clean output (STOP on fail)
            # ==============================================================
            if self.run_validations:
                logger.info("=" * 60)
                logger.info("STEP 10: Validating clean output")
                logger.info("=" * 60)

                passed, details = self.validator.validate_clean_output(
                    clean_df
                )
                stats['validations']['clean_output'] = details

                if not passed:
                    raise LdcPremisesETLException(
                        f"Clean output validation failed: "
                        f"{details['failures']}"
                    )

            # ==============================================================
            # STEP 11: Load to PostgreSQL clean table
            # ==============================================================
            logger.info("=" * 60)
            logger.info("STEP 11: Loading to PostgreSQL clean table")
            logger.info("=" * 60)

            if not self.dry_run:
                self.data_writer.truncate_and_load_to_postgres(
                    clean_df,
                    config.LDC_CLEAN_TABLE,
                    schema=PG_SCHEMA
                )
                stats['steps']['load_clean'] = {'rows': len(clean_df)}

            # ==============================================================
            # STEP 12: Archive clean data to S3
            # ==============================================================
            logger.info("=" * 60)
            logger.info("STEP 12: Archiving clean data to S3")
            logger.info("=" * 60)

            if not self.dry_run:
                self.archive_to_s3_parquet(
                    clean_df,
                    config.LDC_S3_CLEAN_ARCHIVES,
                    partition_by_month=True
                )

            stats['steps']['s3_clean_archive'] = {
                'path': config.LDC_S3_CLEAN_ARCHIVES,
                'rows': len(clean_df)
            }

            # ==============================================================
            # Complete
            # ==============================================================
            stats['end_time'] = datetime.now().isoformat()
            stats['success'] = True

            logger.info("=" * 60)
            logger.info("PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("=" * 60)

            return stats

        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            stats['error'] = str(e)
            stats['end_time'] = datetime.now().isoformat()
            raise

        finally:
            if self.snowflake_loader:
                self.snowflake_loader.close()


def main():
    """Main entry point for the ETL pipeline."""
    import argparse

    parser = argparse.ArgumentParser(description='LDC Premises ETL Pipeline')
    parser.add_argument(
        '--no-validation',
        action='store_true',
        help='Skip validation suites'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Run without writing to any destinations'
    )

    args = parser.parse_args()

    try:
        etl = LdcPremisesETL(
            run_validations=not args.no_validation,
            dry_run=args.dry_run
        )

        stats = etl.run()

        print("\n" + "=" * 60)
        print("PIPELINE SUMMARY")
        print("=" * 60)
        print(f"Status: {'SUCCESS' if stats['success'] else 'FAILED'}")
        print(f"Start: {stats['start_time']}")
        print(f"End: {stats['end_time']}")

        if 'steps' in stats:
            print("\nSteps:")
            for step, info in stats['steps'].items():
                print(f"  {step}: {info}")

        if 'validations' in stats:
            print("\nValidations:")
            for suite, details in stats['validations'].items():
                failures = len(details.get('failures', []))
                warnings = len(details.get('warnings', []))
                print(f"  {suite}: {failures} failures, {warnings} warnings")

        return 0 if stats['success'] else 1

    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        return 1


if __name__ == '__main__':
    sys.exit(main())

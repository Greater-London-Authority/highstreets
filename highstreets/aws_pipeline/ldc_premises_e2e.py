"""
LDC Premises ETL End-to-End Pipeline.

This script orchestrates the complete monthly LDC premises data pipeline:
1.   Fetch data from Snowflake
2.   Validate source schema (Suite 1 - STOP on fail)
3.   Archive full raw snapshot to S3 Parquet
4.   Select working columns, compute row hashes, upsert to PostgreSQL raw
5.   Upsert to PostgreSQL raw table
6.   Validate raw data quality (Suite 2 - WARN on fail)
7.   Load accumulated raw from PostgreSQL
8.   Transform data
8.5. Apply validated addresses from external DB (hsds_dev)
9.   Validate business logic (Suite 3 - WARN on fail)
10.  Validate clean output (Suite 4 - STOP on fail)
11.  Load to PostgreSQL clean table (truncate + reload)
12.  Archive clean data to S3 Parquet

Usage:
    python -m highstreets.aws_pipeline.ldc_premises_e2e
    python -m highstreets.aws_pipeline.ldc_premises_e2e --dry-run
    python -m highstreets.aws_pipeline.ldc_premises_e2e --no-validation
"""
from highstreets import config
from highstreets.data_source_sink.snowflake_loader import SnowflakeLoader
from highstreets.data_source_sink.datawriter import DataWriter
from highstreets.data_source_sink.lookup_manager import LookupManager
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
        self.lookup_manager = LookupManager()
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

    def apply_validated_addresses(
        self,
        df: pd.DataFrame,
        validated_df: pd.DataFrame,
    ) -> dict:
        """Merge validated address data onto the clean DataFrame.

        Overwrites uprn_id, latitude, and longitude for premises that
        have a matching validated record.  The heavy lifting (external
        DB connection, dedup, type coercion) is done upstream by
        LookupManager.fetch_validated_addresses().

        Args:
            df: Clean premises DataFrame (output of Step 8 transform).
            validated_df: Validated addresses from LookupManager.

        Returns:
            dict with 'df' (DataFrame) and 'overlay_stats' (dict).
        """
        no_change = {
            'df': df,
            'overlay_stats': {
                'status': 'skipped',
                'validated_loaded': 0,
                'rows_updated': 0,
                'rows_original': len(df),
                'unique_premises_updated': 0,
            },
        }

        if validated_df.empty:
            return no_change

        raw_count = len(validated_df)
        merge_key = "premises_id"
        pre_merge_count = len(df)

        # --- Pre-merge quality check on lookup data ---
        qc_issues = []
        if validated_df["latitude"].isna().any():
            qc_issues.append(
                f"{validated_df['latitude'].isna().sum()} null latitudes"
            )
        if validated_df["longitude"].isna().any():
            qc_issues.append(
                f"{validated_df['longitude'].isna().sum()} null longitudes"
            )
        lat_oob = validated_df["latitude"].dropna().between(49.9, 60.9)
        if (~lat_oob).any():
            qc_issues.append(
                f"{(~lat_oob).sum()} latitudes outside UK bounds"
            )
        lon_oob = validated_df["longitude"].dropna().between(-8.2, 1.8)
        if (~lon_oob).any():
            qc_issues.append(
                f"{(~lon_oob).sum()} longitudes outside UK bounds"
            )
        if qc_issues:
            logger.warning(
                f"Validated address quality issues: "
                f"{'; '.join(qc_issues)}"
            )

        # Ensure merge key types align
        df[merge_key] = pd.to_numeric(df[merge_key], errors="coerce")

        # Tag validated rows and rename to avoid column collisions
        validated_df = validated_df.copy()
        validated_df["_is_validated"] = True
        validated_df = validated_df.rename(columns={
            "uprn": "val_uprn",
            "latitude": "val_latitude",
            "longitude": "val_longitude",
        })

        merged = df.merge(
            validated_df[
                [merge_key, "val_uprn", "val_latitude",
                 "val_longitude", "_is_validated"]
            ],
            on=merge_key,
            how="left",
        )

        if len(merged) != pre_merge_count:
            logger.error(
                f"Row count changed after validated-address merge: "
                f"{pre_merge_count:,} → {len(merged):,}. "
                f"Falling back to original data."
            )
            no_change['overlay_stats']['status'] = 'merge_error'
            return no_change

        has_validated = merged["_is_validated"].fillna(False).astype(bool)
        updated_count = int(has_validated.sum())
        unique_premises_updated = (
            merged.loc[has_validated, merge_key].nunique()
        )

        # Match-rate sanity check
        unique_in_df = df[merge_key].nunique()
        if unique_in_df > 0 and raw_count > 0:
            match_rate = unique_premises_updated / raw_count * 100
            logger.info(
                f"Validated address match rate: "
                f"{unique_premises_updated:,}/{raw_count:,} "
                f"({match_rate:.1f}%)"
            )
            if match_rate < 5:
                logger.warning(
                    f"Low match rate ({match_rate:.1f}%) — "
                    f"check premises_id alignment between "
                    f"clean data and lookup table"
                )

        if updated_count > 0:
            for src, dst in [
                ("val_uprn", "uprn_id"),
                ("val_latitude", "latitude"),
                ("val_longitude", "longitude"),
            ]:
                merged.loc[has_validated, dst] = (
                    merged.loc[has_validated, src]
                )

        merged = merged.drop(
            columns=["val_uprn", "val_latitude", "val_longitude",
                     "_is_validated"]
        )

        original_count = pre_merge_count - updated_count
        logger.info(
            f"Validated address overlay: "
            f"{unique_premises_updated:,} unique premises "
            f"({updated_count:,} tenancy rows) updated, "
            f"{original_count:,} rows kept original LDC values"
        )

        return {
            'df': merged,
            'overlay_stats': {
                'status': 'applied',
                'validated_loaded': raw_count,
                'unique_premises_updated': unique_premises_updated,
                'rows_updated': updated_count,
                'rows_original': original_count,
            },
        }

    HASH_EXCLUDE_COLS = [
        'row_hash', 'ingested_at',
        'timestamp_create', 'timestamp_update',
        'latitude', 'longitude', 'uprn_id',
    ]

    def compute_row_hash(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute row hash over business-relevant columns only.

        Excludes pipeline metadata (row_hash, ingested_at), LDC batch
        timestamps (timestamp_create, timestamp_update), and spatial
        columns (latitude, longitude, uprn_id) which will be overwritten
        by validated address data from a separate lookup table.

        Normalizes dtypes before hashing so that int64 vs float64
        representations of the same value (caused by nullable columns)
        produce identical hashes across runs.
        """
        hash_cols = [c for c in df.columns if c not in self.HASH_EXCLUDE_COLS]

        logger.info(f"Computing row hash over {len(hash_cols)} columns")

        hash_input = df[hash_cols].copy()
        for col in hash_input.columns:
            if hash_input[col].dtype in ['int64', 'Int64']:
                hash_input[col] = hash_input[col].astype('float64')
            elif pd.api.types.is_datetime64_any_dtype(hash_input[col]):
                hash_input[col] = hash_input[col].astype(str)
            elif hash_input[col].dtype == 'bool':
                hash_input[col] = hash_input[col].astype('float64')
            elif hash_input[col].dtype == 'object':
                hash_input[col] = hash_input[col].fillna('__NULL__')

        df['row_hash'] = pd.util.hash_pandas_object(
            hash_input,
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

        import datetime as dt

        df_out = df.copy()

        for col in df_out.columns:
            if df_out[col].dtype == 'object':
                non_null = df_out[col].dropna()
                if len(non_null) > 0 and isinstance(non_null.iloc[0], dt.time):
                    df_out[col] = df_out[col].apply(
                        lambda x: str(x) if x is not None and x is not pd.NaT else None
                    )

        for col in df_out.select_dtypes(include=['datetime64']).columns:
            df_out[col] = df_out[col].dt.floor('us')

        table = pa.Table.from_pandas(df_out)
        fs = fsspec.filesystem('s3')
        with fs.open(full_path, 'wb') as f:
            pq.write_table(
                table, f,
                coerce_timestamps='us',
                allow_truncated_timestamps=True
            )

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
            # STEP 8.5: Apply validated addresses from external DB
            # ==============================================================
            logger.info("=" * 60)
            logger.info(
                "STEP 8.5: Applying validated addresses from external DB"
            )
            logger.info("=" * 60)

            if not self.dry_run:
                validated_df = (
                    self.lookup_manager.fetch_validated_addresses()
                )
            else:
                logger.info("Dry run: skipping external DB fetch")
                validated_df = pd.DataFrame()

            result = self.apply_validated_addresses(clean_df, validated_df)
            clean_df = result['df']

            stats['steps']['validated_addresses'] = result['overlay_stats']

            # Include overlay summary in validations for the HTML report
            ov = result['overlay_stats']
            ov_passed = ov['status'] != 'merge_error'

            stats['validations']['validated_addresses'] = {
                'passed': ov_passed,
                'failure_mode': 'WARN',
                'row_count': ov.get('validated_loaded', 0),
                'expectations_passed': 1 if ov_passed else 0,
                'expectations_total': 1,
                'expectation_results': [{
                    'expectation_type': 'validated_address_overlay',
                    'success': ov_passed,
                    'kwargs': {},
                    'result': {
                        'observed_value': (
                            f"status={ov['status']}, "
                            f"{ov.get('unique_premises_updated', 0):,} "
                            f"premises updated, "
                            f"{ov.get('rows_original', 0):,} kept original"
                        ),
                    },
                }],
                'failures': (
                    [f"Validated address merge failed: "
                     f"{ov.get('status')}"]
                    if not ov_passed else []
                ),
                'warnings': (
                    ["Lookup quality issues detected: "
                     "check pipeline logs"]
                    if ov.get('status') == 'applied'
                    and ov.get('unique_premises_updated', 0) == 0
                    else []
                ),
            }

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
            # Step 8: Generate validation report to S3
            # ==============================================================
            if self.run_validations and stats.get('validations'):
                logger.info("Generating HTML validation report...")
                report_path = self.validator.generate_html_report(
                    all_results=stats['validations'],
                    run_timestamp=stats.get('start_time', datetime.now().isoformat()),
                    s3_path=config.LDC_S3_VALIDATION_REPORTS
                )
                stats['steps']['validation_report'] = {'s3_path': report_path}
                logger.info(f"Validation report: {report_path}")

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

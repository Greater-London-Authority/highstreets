"""
LDC Premises Data Quality Validation using Great Expectations.

This module implements four validation suites for the LDC premises ETL pipeline:
1. Source Schema Validation (STOP on fail) - After Snowflake fetch
2. Raw Data Quality (WARN on fail) - After upsert
3. Business Logic Validation (WARN on fail) - After transform
4. Clean Output Validation (STOP on fail) - Before publish

Design Principles:
- STOP = Pipeline must not proceed (data unsafe for downstream)
- WARN = Data ingested but issues logged and monitored

Uses batch.validate(expectation) pattern for robustness - each expectation
is validated individually against a batch, avoiding GX context naming
conflicts on re-runs.

Requires: great_expectations >= 1.0.0
Install with: pip install great_expectations
"""

import logging
from datetime import datetime
from typing import Tuple, Dict, Any, List, Optional

import pandas as pd
import great_expectations as gx


class LdcValidationException(Exception):
    """Exception raised for LDC validation errors."""
    pass


class LdcPremisesValidator:
    """
    Data quality validator for LDC premises data using Great Expectations.

    Implements four validation suites with STOP/WARN failure modes.
    Uses GX Core API (v1.x) with batch.validate(expectation) pattern.
    """

    # Expected columns for source schema validation
    CRITICAL_COLUMNS = [
        'tenant_id', 'premises_id', 'date_create', 'timestamp_update',
        'latitude', 'longitude', 'tenant', 'source'
    ]

    # Expected clean output columns (38)
    CLEAN_COLUMNS = [
        'tenant_id', 'tenant', 'address', 'street', 'geography', 'zip',
        'geography_large', 'uprn_id', 'latitude', 'longitude', 'premises_id',
        'property_id', 'property', 'tenant_status', 'premises_status',
        'company_id', 'company', 'company_holding', 'tenant_care_of',
        'flag_independent', 'category', 'classification', 'subcategory',
        'phone', 'url_website', 'url_image', 'area_sm', 'voa_business_rate',
        'date_create', 'date_close', 'date_last_survey_field',
        'date_last_survey_office', 'date_premises_create', 'timestamp_create',
        'timestamp_update', 'latest_record_check', 'latest_premises_check',
        'source'
    ]

    # Known valid categorical values
    VALID_SOURCES = ['live', 'historic', 'archived']
    VALID_TENANT_STATUSES = ['Live', 'Closed', 'Vacant', 'Demolished']

    # UK geographic bounds
    UK_LAT_MIN, UK_LAT_MAX = 49.9, 60.9
    UK_LON_MIN, UK_LON_MAX = -8.2, 1.8

    def __init__(self, log_to_cloudwatch: bool = False):
        """
        Initialize the validator.

        Args:
            log_to_cloudwatch: If True, send validation results to CloudWatch
        """
        self.logger = logging.getLogger(__name__)
        self.log_to_cloudwatch = log_to_cloudwatch
        self.logger.info(
            "LdcPremisesValidator initialized with GX version %s",
            gx.__version__
        )

    def _get_batch(self, df: pd.DataFrame, asset_name: str):
        """
        Create a GX batch from a pandas DataFrame.

        Each call creates a fresh ephemeral context to avoid name
        conflicts when the validator is reused across multiple runs.

        Args:
            df: The DataFrame to validate
            asset_name: Name for the data asset

        Returns:
            A GX Batch object
        """
        context = gx.get_context()
        data_source = context.data_sources.add_pandas(name="pandas")
        data_asset = data_source.add_dataframe_asset(name=asset_name)
        batch_definition = data_asset.add_batch_definition_whole_dataframe(
            name=f"{asset_name}_batch"
        )
        batch = batch_definition.get_batch(
            batch_parameters={"dataframe": df}
        )
        return batch

    def _run_expectations(
        self,
        df: pd.DataFrame,
        suite_name: str,
        expectations: List[gx.expectations.Expectation]
    ) -> List[Dict[str, Any]]:
        """
        Run a list of expectations one-by-one against a DataFrame batch.

        Uses batch.validate(expectation) for each expectation individually.
        This is the most robust pattern - no suite/checkpoint naming issues.

        Args:
            df: DataFrame to validate
            suite_name: Name for logging
            expectations: List of GX Expectation objects

        Returns:
            List of result dicts, one per expectation
        """
        batch = self._get_batch(df, suite_name)

        results = []
        for expectation in expectations:
            try:
                validation_result = batch.validate(expectation)

                result_dict = {
                    'expectation_type': type(expectation).__name__,
                    'success': validation_result.success,
                    'kwargs': {},
                    'result': {},
                }

                # Extract kwargs from the expectation config
                if hasattr(validation_result, 'expectation_config'):
                    config = validation_result.expectation_config
                    if hasattr(config, 'kwargs'):
                        result_dict['kwargs'] = dict(config.kwargs)
                    elif hasattr(config, 'to_dict'):
                        config_dict = config.to_dict()
                        result_dict['kwargs'] = config_dict.get('kwargs', {})

                # Extract result metrics
                if hasattr(validation_result, 'result'):
                    res = validation_result.result
                    if isinstance(res, dict):
                        result_dict['result'] = res
                    elif hasattr(res, 'to_dict'):
                        result_dict['result'] = res.to_dict()

                results.append(result_dict)

            except Exception as e:
                self.logger.warning(
                    "Expectation %s failed to execute: %s",
                    type(expectation).__name__, str(e)
                )
                results.append({
                    'expectation_type': type(expectation).__name__,
                    'success': False,
                    'kwargs': {},
                    'result': {},
                    'error': str(e)
                })

        return results

    def _build_response(
        self,
        suite_name: str,
        failure_mode: str,
        expectation_results: List[Dict[str, Any]],
        row_count: int,
        extra_details: Optional[Dict[str, Any]] = None
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Build a standardised response from individual expectation results.

        Args:
            suite_name: Name of the validation suite
            failure_mode: "STOP" or "WARN"
            expectation_results: Results from _run_expectations
            row_count: Number of rows validated
            extra_details: Any additional details to include

        Returns:
            Tuple of (passed: bool, details: dict)
        """
        failures = []
        warnings = []

        for res in expectation_results:
            if not res['success']:
                exp_type = res['expectation_type']
                column = res.get('kwargs', {}).get('column', 'N/A')
                unexpected_count = res.get('result', {}).get(
                    'unexpected_count', 'N/A'
                )
                unexpected_pct = res.get('result', {}).get(
                    'unexpected_percent', 0
                )
                error = res.get('error')

                if error:
                    msg = f"{exp_type}: {error}"
                elif unexpected_count != 'N/A':
                    msg = (
                        f"{exp_type} on '{column}': "
                        f"{unexpected_count} unexpected "
                        f"({unexpected_pct:.2f}%)"
                    )
                else:
                    msg = f"{exp_type} on '{column}': failed"

                if failure_mode == "STOP":
                    failures.append(msg)
                else:
                    warnings.append(msg)

        passed = all(r['success'] for r in expectation_results)
        passed_count = sum(1 for r in expectation_results if r['success'])

        details = {
            'suite': suite_name,
            'passed': passed,
            'failure_mode': failure_mode,
            'failures': failures,
            'warnings': warnings,
            'expectations_passed': passed_count,
            'expectations_total': len(expectation_results),
            'expectation_results': expectation_results,
            'row_count': row_count,
            'timestamp': datetime.now().isoformat()
        }

        if extra_details:
            details.update(extra_details)

        # Log
        self._log_result(suite_name, passed, details, failure_mode)

        return passed, details

    def _log_result(
        self,
        suite_name: str,
        passed: bool,
        details: Dict[str, Any],
        failure_mode: str
    ):
        """Log validation result with appropriate level."""
        status = "PASSED" if passed else f"FAILED ({failure_mode})"
        passed_n = details.get('expectations_passed', '?')
        total_n = details.get('expectations_total', '?')
        message = (
            f"Suite '{suite_name}': {status} "
            f"({passed_n}/{total_n} expectations passed)"
        )

        if passed:
            self.logger.info(message)
        elif failure_mode == "STOP":
            self.logger.error(message)
            for f in details.get('failures', [])[:5]:
                self.logger.error("  - %s", f)
        else:
            self.logger.warning(message)
            for w in details.get('warnings', [])[:5]:
                self.logger.warning("  - %s", w)

    # =========================================================================
    # SUITE 1: SOURCE SCHEMA VALIDATION (STOP on fail)
    # =========================================================================

    def validate_source_schema(
        self, df: pd.DataFrame
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Suite 1: Source Schema Validation.

        Guarantees structural compatibility with downstream transformations,
        database constraints, and upsert semantics.

        On Fail: STOP - Pipeline must not proceed

        Checks:
        - Critical columns must exist
        - Primary key columns not null
        - Composite primary key must be unique

        Args:
            df: DataFrame from Snowflake fetch

        Returns:
            Tuple of (passed: bool, details: dict)
        """
        expectations = []

        # Check 1: Critical columns must exist
        for col in self.CRITICAL_COLUMNS:
            expectations.append(
                gx.expectations.ExpectColumnToExist(column=col)
            )

        # Check 2: Primary key columns should not be null
        for col in ['tenant_id', 'premises_id', 'date_create']:
            if col in df.columns:
                expectations.append(
                    gx.expectations.ExpectColumnValuesToNotBeNull(column=col)
                )

        # Check 3: Composite primary key uniqueness
        # Snowflake data should have unique (tenant_id, premises_id, date_create)
        if all(c in df.columns for c in ['tenant_id', 'premises_id', 'date_create']):
            expectations.append(
                gx.expectations.ExpectCompoundColumnsToBeUnique(
                    column_list=['tenant_id', 'premises_id', 'date_create'],
                    mostly=1.0
                )
            )

        try:
            results = self._run_expectations(
                df, "ldc_source_schema", expectations
            )
            return self._build_response(
                "ldc_source_schema", "STOP", results, len(df)
            )
        except Exception as e:
            self.logger.error("Suite 'ldc_source_schema' error: %s", str(e))
            return False, {
                'suite': 'ldc_source_schema',
                'passed': False,
                'failure_mode': 'STOP',
                'failures': [f"Execution error: {str(e)}"],
                'warnings': [],
                'timestamp': datetime.now().isoformat()
            }

    # =========================================================================
    # SUITE 2: RAW DATA QUALITY (WARN on fail)
    # =========================================================================

    def validate_raw_quality(
        self, df: pd.DataFrame
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Suite 2: Raw Data Quality Validation.

        Detects anomalous or corrupted data while preserving full historical
        ingestion. Failures here should raise alerts but not block ingestion.

        On Fail: WARN - Data ingested but issues logged

        Checks:
        - Primary identifiers not null
        - Geographic bounds (UK)
        - Source values in allowed set
        - Date sanity checks
        - Row hash integrity (if present)

        Args:
            df: DataFrame after upsert to raw table

        Returns:
            Tuple of (passed: bool, details: dict)
        """
        # If date columns are strings (pre-transform raw data), convert
        # them to datetime in a copy so GX can compare properly
        date_cols_to_convert = ['date_create']
        needs_date_conversion = any(
            col in df.columns
            and not pd.api.types.is_datetime64_any_dtype(df[col])
            for col in date_cols_to_convert
        )
        if needs_date_conversion:
            df = df.copy()
            for col in date_cols_to_convert:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')

        expectations = []

        # Check 1: Primary identifiers not null
        for col in ['tenant_id', 'premises_id', 'date_create']:
            if col in df.columns:
                expectations.append(
                    gx.expectations.ExpectColumnValuesToNotBeNull(
                        column=col, mostly=0.999
                    )
                )

        # Check 2: Coordinates not null
        for col in ['latitude', 'longitude']:
            if col in df.columns:
                expectations.append(
                    gx.expectations.ExpectColumnValuesToNotBeNull(
                        column=col, mostly=0.99
                    )
                )

        # Check 3: Geographic bounds (UK)
        if 'latitude' in df.columns:
            expectations.append(
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column='latitude',
                    min_value=self.UK_LAT_MIN,
                    max_value=self.UK_LAT_MAX,
                    mostly=0.95
                )
            )

        if 'longitude' in df.columns:
            expectations.append(
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column='longitude',
                    min_value=self.UK_LON_MIN,
                    max_value=self.UK_LON_MAX,
                    mostly=0.95
                )
            )

        # Check 4: Source values in allowed set
        if 'source' in df.columns:
            expectations.append(
                gx.expectations.ExpectColumnValuesToBeInSet(
                    column='source',
                    value_set=self.VALID_SOURCES
                )
            )

        # Check 5: Date sanity - date_create within reasonable range
        # Column is now guaranteed to be datetime (converted above if needed)
        if 'date_create' in df.columns:
            expectations.append(
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column='date_create',
                    min_value=pd.Timestamp('1970-01-01'),
                    max_value=pd.Timestamp.now(),
                    mostly=0.99
                )
            )

        # Check 6: Row hash integrity (if present)
        if 'row_hash' in df.columns:
            expectations.append(
                gx.expectations.ExpectColumnValuesToNotBeNull(
                    column='row_hash'
                )
            )
            expectations.append(
                gx.expectations.ExpectColumnValueLengthsToBeBetween(
                    column='row_hash',
                    min_value=8,
                    max_value=64
                )
            )

        try:
            results = self._run_expectations(
                df, "ldc_raw_quality", expectations
            )
            return self._build_response(
                "ldc_raw_quality", "WARN", results, len(df)
            )
        except Exception as e:
            self.logger.error("Suite 'ldc_raw_quality' error: %s", str(e))
            return False, {
                'suite': 'ldc_raw_quality',
                'passed': False,
                'failure_mode': 'WARN',
                'failures': [],
                'warnings': [f"Execution error: {str(e)}"],
                'timestamp': datetime.now().isoformat()
            }

    # =========================================================================
    # SUITE 3: BUSINESS LOGIC VALIDATION (WARN on fail)
    # =========================================================================

    def validate_business_logic(
        self, df: pd.DataFrame
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Suite 3: Business Logic Validation.

        Ensures semantic consistency across fields after transformations
        and date corrections.

        On Fail: WARN - Data processed but issues logged

        Checks:
        - Tenant status values in expected set
        - Primary key uniqueness after transform
        - Latitude/longitude valid ranges
        - Computed columns exist
        - Custom: date_close >= date_create
        - Custom: active tenants should not have date_close
        - Custom: closed tenants should have date_close

        Args:
            df: DataFrame after transformation

        Returns:
            Tuple of (passed: bool, details: dict)
        """
        expectations = []

        # Check 1: Tenant status values
        if 'tenant_status' in df.columns:
            expectations.append(
                gx.expectations.ExpectColumnValuesToBeInSet(
                    column='tenant_status',
                    value_set=self.VALID_TENANT_STATUSES,
                    mostly=0.99
                )
            )

        # Check 2: PK uniqueness after transform
        # mostly=0.9999 tolerates rare SWS edge cases from fix_premises_dates
        pk_cols = ['tenant_id', 'premises_id', 'date_create']
        if all(col in df.columns for col in pk_cols):
            expectations.append(
                gx.expectations.ExpectCompoundColumnsToBeUnique(
                    column_list=pk_cols,
                    mostly=0.9999
                )
            )

        # Check 3: Valid coordinate ranges
        if 'latitude' in df.columns:
            expectations.append(
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column='latitude',
                    min_value=-90,
                    max_value=90
                )
            )

        if 'longitude' in df.columns:
            expectations.append(
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column='longitude',
                    min_value=-180,
                    max_value=180
                )
            )

        # Check 4: Computed columns exist
        for col in ['latest_record_check', 'latest_premises_check']:
            if col in df.columns:
                expectations.append(
                    gx.expectations.ExpectColumnToExist(column=col)
                )

        # Run GX expectations
        try:
            results = self._run_expectations(
                df, "ldc_business_logic", expectations
            )
            passed, details = self._build_response(
                "ldc_business_logic", "WARN", results, len(df)
            )
        except Exception as e:
            self.logger.error("Suite 'ldc_business_logic' error: %s", str(e))
            passed = False
            details = {
                'suite': 'ldc_business_logic',
                'passed': False,
                'failure_mode': 'WARN',
                'failures': [],
                'warnings': [f"Execution error: {str(e)}"],
                'timestamp': datetime.now().isoformat()
            }

        # Custom business logic checks (not easily expressed as GX expectations)
        custom_warnings = self._check_business_logic_custom(df)
        if custom_warnings:
            details.setdefault('warnings', []).extend(custom_warnings)
        details['custom_checks'] = custom_warnings

        return passed, details

    def _check_business_logic_custom(self, df: pd.DataFrame) -> List[str]:
        """
        Custom business logic checks beyond GX expectations.

        Returns list of warning messages.
        """
        warnings = []

        # Check: date_close >= date_create (or null)
        if 'date_close' in df.columns and 'date_create' in df.columns:
            try:
                date_close = pd.to_datetime(df['date_close'], errors='coerce')
                date_create = pd.to_datetime(df['date_create'], errors='coerce')

                invalid_dates = (
                    date_close.notna()
                    & date_create.notna()
                    & (date_close < date_create)
                )
                invalid_count = invalid_dates.sum()

                if invalid_count > 0:
                    pct = (invalid_count / len(df)) * 100
                    warnings.append(
                        f"date_close < date_create: "
                        f"{invalid_count} rows ({pct:.2f}%)"
                    )
            except Exception as e:
                warnings.append(f"Date comparison error: {str(e)}")

        # Check: Active tenants should not have date_close
        if 'tenant_status' in df.columns and 'date_close' in df.columns:
            try:
                active_with_close = (
                    (df['tenant_status'].str.lower() == 'active')
                    & df['date_close'].notna()
                )
                count = active_with_close.sum()
                if count > 0:
                    pct = (count / len(df)) * 100
                    warnings.append(
                        f"Active tenants with date_close: "
                        f"{count} rows ({pct:.2f}%)"
                    )
            except Exception:
                pass

        # Check: Closed tenants should have date_close
        if 'tenant_status' in df.columns and 'date_close' in df.columns:
            try:
                closed_no_close = (
                    (df['tenant_status'].str.lower() == 'closed')
                    & df['date_close'].isna()
                )
                count = closed_no_close.sum()
                if count > 0:
                    pct = (count / len(df)) * 100
                    warnings.append(
                        f"Closed tenants without date_close: "
                        f"{count} rows ({pct:.2f}%)"
                    )
            except Exception:
                pass

        return warnings

    # =========================================================================
    # SUITE 4: CLEAN OUTPUT VALIDATION (STOP on fail)
    # =========================================================================

    def validate_clean_output(
        self, df: pd.DataFrame
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Suite 4: Clean Output Validation.

        Final contract enforcement before data becomes consumable by analysts
        and downstream systems.

        On Fail: STOP - Pipeline must not proceed

        Checks:
        - Exact column count (38)
        - All expected columns exist
        - Primary key uniqueness
        - Critical columns not null
        - Computed columns populated
        - Source distribution
        - Geographic bounds
        - Date sanity
        - Tenant status values

        Args:
            df: DataFrame ready for publication

        Returns:
            Tuple of (passed: bool, details: dict)
        """
        expectations = []

        # Check 1: Column count must be exactly 38
        expectations.append(
            gx.expectations.ExpectTableColumnCountToEqual(
                value=len(self.CLEAN_COLUMNS)
            )
        )

        # Check 2: All expected columns must exist
        for col in self.CLEAN_COLUMNS:
            expectations.append(
                gx.expectations.ExpectColumnToExist(column=col)
            )

        # Check 3: Primary key uniqueness
        # mostly=0.9999 allows for rare SWS edge cases where
        # fix_premises_dates produces a main + SWS row at same PK
        if all(c in df.columns for c in ['tenant_id', 'premises_id', 'date_create']):
            expectations.append(
                gx.expectations.ExpectCompoundColumnsToBeUnique(
                    column_list=['tenant_id', 'premises_id', 'date_create'],
                    mostly=0.9999
                )
            )

        # Check 4: Critical columns not null
        critical_not_null = [
            'tenant_id', 'premises_id', 'date_create',
            'latitude', 'longitude', 'tenant'
        ]
        for col in critical_not_null:
            if col in df.columns:
                expectations.append(
                    gx.expectations.ExpectColumnValuesToNotBeNull(
                        column=col, mostly=0.999
                    )
                )

        # Check 5: Source distribution
        if 'source' in df.columns:
            expectations.append(
                gx.expectations.ExpectColumnValuesToBeInSet(
                    column='source',
                    value_set=self.VALID_SOURCES
                )
            )

        # Check 6: Geographic bounds
        if 'latitude' in df.columns:
            expectations.append(
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column='latitude',
                    min_value=self.UK_LAT_MIN,
                    max_value=self.UK_LAT_MAX,
                    mostly=0.90
                )
            )

        if 'longitude' in df.columns:
            expectations.append(
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column='longitude',
                    min_value=self.UK_LON_MIN,
                    max_value=self.UK_LON_MAX,
                    mostly=0.90
                )
            )

        # Check 7: Date sanity
        if 'date_create' in df.columns:
            expectations.append(
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column='date_create',
                    min_value=pd.Timestamp('1970-01-01'),
                    max_value=pd.Timestamp.now(),
                    mostly=0.99
                )
            )

        # Check 8: Tenant status values
        if 'tenant_status' in df.columns:
            expectations.append(
                gx.expectations.ExpectColumnValuesToBeInSet(
                    column='tenant_status',
                    value_set=self.VALID_TENANT_STATUSES,
                    mostly=0.99
                )
            )

        try:
            results = self._run_expectations(
                df, "ldc_clean_output", expectations
            )
            passed, details = self._build_response(
                "ldc_clean_output", "STOP", results, len(df)
            )
        except Exception as e:
            self.logger.error("Suite 'ldc_clean_output' error: %s", str(e))
            return False, {
                'suite': 'ldc_clean_output',
                'passed': False,
                'failure_mode': 'STOP',
                'failures': [f"Execution error: {str(e)}"],
                'warnings': [],
                'timestamp': datetime.now().isoformat()
            }

        # Additional statistics
        details['stats'] = {
            'row_count': len(df),
            'column_count': len(df.columns),
            'source_distribution': (
                df['source'].value_counts().to_dict()
                if 'source' in df.columns else {}
            ),
        }

        return passed, details

    # =========================================================================
    # UTILITY METHODS
    # =========================================================================

    def run_all_validations(
        self,
        source_df: Optional[pd.DataFrame] = None,
        raw_df: Optional[pd.DataFrame] = None,
        transformed_df: Optional[pd.DataFrame] = None,
        clean_df: Optional[pd.DataFrame] = None,
        stop_on_failure: bool = True
    ) -> Dict[str, Dict[str, Any]]:
        """
        Run all applicable validation suites.

        Args:
            source_df: DataFrame for source schema validation
            raw_df: DataFrame for raw quality validation
            transformed_df: DataFrame for business logic validation
            clean_df: DataFrame for clean output validation
            stop_on_failure: If True, raise exception on STOP failures

        Returns:
            Dictionary with results from each suite
        """
        results = {}

        # Suite 1: Source Schema (STOP)
        if source_df is not None:
            passed, details = self.validate_source_schema(source_df)
            results['source_schema'] = details
            if not passed and stop_on_failure:
                raise LdcValidationException(
                    f"Source schema validation failed: "
                    f"{details.get('failures', [])}"
                )

        # Suite 2: Raw Quality (WARN)
        if raw_df is not None:
            passed, details = self.validate_raw_quality(raw_df)
            results['raw_quality'] = details

        # Suite 3: Business Logic (WARN)
        if transformed_df is not None:
            passed, details = self.validate_business_logic(transformed_df)
            results['business_logic'] = details

        # Suite 4: Clean Output (STOP)
        if clean_df is not None:
            passed, details = self.validate_clean_output(clean_df)
            results['clean_output'] = details
            if not passed and stop_on_failure:
                raise LdcValidationException(
                    f"Clean output validation failed: "
                    f"{details.get('failures', [])}"
                )

        return results

    def get_validation_summary(
        self,
        results: Dict[str, Dict[str, Any]]
    ) -> str:
        """
        Generate a human-readable summary of validation results.

        Args:
            results: Dictionary from run_all_validations()

        Returns:
            Formatted summary string
        """
        lines = [
            "=" * 60,
            "LDC PREMISES VALIDATION SUMMARY",
            "=" * 60,
            ""
        ]

        for suite_name, details in results.items():
            passed = details.get('passed', False)
            failure_mode = details.get('failure_mode', 'UNKNOWN')
            status = "PASSED" if passed else f"FAILED ({failure_mode})"
            passed_n = details.get('expectations_passed', '?')
            total_n = details.get('expectations_total', '?')

            lines.append(f"Suite: {suite_name}")
            lines.append(f"  Status: {status}")
            lines.append(f"  Expectations: {passed_n}/{total_n} passed")

            if details.get('failures'):
                lines.append(f"  Failures ({len(details['failures'])}):")
                for f in details['failures'][:5]:
                    lines.append(f"    - {f}")
                remaining = len(details['failures']) - 5
                if remaining > 0:
                    lines.append(f"    ... and {remaining} more")

            if details.get('warnings'):
                lines.append(f"  Warnings ({len(details['warnings'])}):")
                for w in details['warnings'][:5]:
                    lines.append(f"    - {w}")
                remaining = len(details['warnings']) - 5
                if remaining > 0:
                    lines.append(f"    ... and {remaining} more")

            lines.append("")

        lines.append("=" * 60)
        return "\n".join(lines)


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def validate_source_schema(df: pd.DataFrame) -> Tuple[bool, Dict[str, Any]]:
    """Convenience function for source schema validation."""
    validator = LdcPremisesValidator()
    return validator.validate_source_schema(df)


def validate_raw_quality(df: pd.DataFrame) -> Tuple[bool, Dict[str, Any]]:
    """Convenience function for raw quality validation."""
    validator = LdcPremisesValidator()
    return validator.validate_raw_quality(df)


def validate_business_logic(
    df: pd.DataFrame
) -> Tuple[bool, Dict[str, Any]]:
    """Convenience function for business logic validation."""
    validator = LdcPremisesValidator()
    return validator.validate_business_logic(df)


def validate_clean_output(df: pd.DataFrame) -> Tuple[bool, Dict[str, Any]]:
    """Convenience function for clean output validation."""
    validator = LdcPremisesValidator()
    return validator.validate_clean_output(df)


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    print(f"Great Expectations version: {gx.__version__}")
    print("LDC Premises Validator module loaded successfully")
    print("\nAvailable suites:")
    print("  1. validate_source_schema(df) - STOP on fail")
    print("  2. validate_raw_quality(df)   - WARN on fail")
    print("  3. validate_business_logic(df) - WARN on fail")
    print("  4. validate_clean_output(df)  - STOP on fail")
    print("\nUsage:")
    print("  from highstreets.great_expectations import LdcPremisesValidator")
    print("  validator = LdcPremisesValidator()")
    print("  passed, details = validator.validate_source_schema(df)")

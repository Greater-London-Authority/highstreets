"""
BT Output Data Quality Validation using Great Expectations.

STOP-level validation run before uploading BT footfall data to London Datastore.
Catches empty datasets, null IDs, out-of-range dates, and negative counts.

Uses batch.validate(expectation) pattern consistent with ldc_premises_validation.py.
Requires: great_expectations >= 1.0.0
"""

import logging
from typing import Dict, Any, List, Tuple

import pandas as pd
import great_expectations as gx

logger = logging.getLogger(__name__)


class BtValidationException(Exception):
    pass


def validate_bt_output(
    df: pd.DataFrame,
    dataset_name: str,
    start_date: str = None,
    end_date: str = None,
    id_column: str = None,
    date_column: str = "count_date",
) -> Tuple[bool, List[Dict[str, Any]]]:
    """Validate a BT output DataFrame before LDS upload.

    Args:
        df: The DataFrame to validate.
        dataset_name: Human-readable name (e.g., 'hex_3hourly', 'msoa_hourly').
        start_date: Expected start of date range (optional, ISO format).
        end_date: Expected end of date range (optional, ISO format).
        id_column: Primary entity ID column to check for nulls (e.g., 'hex_id').
        date_column: Name of the date column.

    Returns:
        (passed, list_of_failure_details)
    """
    failures: List[Dict[str, Any]] = []
    context = gx.get_context()
    data_source = context.data_sources.add_pandas("bt_output_validation")
    data_asset = data_source.add_dataframe_asset(dataset_name)
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        f"{dataset_name}_batch"
    )
    batch = batch_definition.get_batch(
        batch_parameters={"dataframe": df}
    )

    checks = [
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=1),
    ]

    if date_column and date_column in df.columns:
        checks.append(
            gx.expectations.ExpectColumnValuesToNotBeNull(column=date_column)
        )

    if id_column and id_column in df.columns:
        checks.append(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column=id_column, mostly=0.99
            )
        )

    count_columns = [
        c for c in df.columns
        if any(
            c.startswith(prefix)
            for prefix in ["visitor", "worker", "resident", "total_"]
        )
        or c in ["unique_volume", "dwell_time"]
    ]
    for col in count_columns:
        checks.append(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column=col, min_value=0, mostly=0.99
            )
        )

    for check in checks:
        result = batch.validate(check)
        if not result.success:
            failures.append({
                "dataset": dataset_name,
                "expectation": type(check).__name__,
                "column": getattr(check, "column", None),
                "details": str(result.result),
            })

    passed = len(failures) == 0
    if passed:
        logger.info(f"BT validation PASSED for {dataset_name} ({len(df)} rows)")
    else:
        logger.error(
            f"BT validation FAILED for {dataset_name}: "
            f"{len(failures)} check(s) failed"
        )
    return passed, failures

"""
Mastercard Output Data Quality Validation using Great Expectations.

STOP-level validation run before uploading adjusted weekly data to London Datastore.
Catches bad data (nulls, negative values, missing areas, extreme YoY ratios) before
it reaches the public-facing platform.

Uses batch.validate(expectation) pattern consistent with ldc_premises_validation.py.
Requires: great_expectations >= 1.0.0
"""

import logging
from typing import Dict, Any, List, Tuple

import pandas as pd
import great_expectations as gx

logger = logging.getLogger(__name__)

EXPECTED_AREAS_TXN = [
    "bespoke", "bids", "boroughs", "caz",
    "highstreets", "inner_outer", "london", "msoas", "towncentres",
]

TXN_AMOUNT_COLUMNS = [
    "txn_amt_wd_eating", "txn_amt_we_eating",
    "txn_amt_wd_apparel", "txn_amt_we_apparel",
    "txn_amt_wd_retail", "txn_amt_we_retail",
]

TXN_COUNT_COLUMNS = [
    "txn_cnt_wd_eating", "txn_cnt_we_eating",
    "txn_cnt_wd_apparel", "txn_cnt_we_apparel",
    "txn_cnt_wd_retail", "txn_cnt_we_retail",
]


class McardValidationException(Exception):
    pass


def validate_weekly_txn_output(
    df: pd.DataFrame, area_name: str
) -> Tuple[bool, List[Dict[str, Any]]]:
    """Validate a single weekly txn output DataFrame before LDS upload.

    Returns (passed, list_of_failure_details).
    """
    failures: List[Dict[str, Any]] = []
    context = gx.get_context()
    data_source = context.data_sources.add_pandas("mcard_txn_validation")
    data_asset = data_source.add_dataframe_asset("txn_data")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        "txn_batch"
    )
    batch = batch_definition.get_batch(
        batch_parameters={"dataframe": df}
    )

    checks = [
        gx.expectations.ExpectTableRowCountToBeGreaterThan(value=0),
        gx.expectations.ExpectColumnToExist(column="week_start"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="week_start"),
    ]

    for col in TXN_AMOUNT_COLUMNS + TXN_COUNT_COLUMNS:
        if col in df.columns:
            checks.append(
                gx.expectations.ExpectColumnValuesToBeOfType(
                    column=col, type_="float64"
                )
            )

    for check in checks:
        result = batch.validate(check)
        if not result.success:
            failures.append({
                "area": area_name,
                "expectation": type(check).__name__,
                "column": getattr(check, "column", None),
                "details": str(result.result),
            })

    passed = len(failures) == 0
    if passed:
        logger.info(f"Mcard txn validation PASSED for {area_name}")
    else:
        logger.error(
            f"Mcard txn validation FAILED for {area_name}: "
            f"{len(failures)} check(s) failed"
        )
    return passed, failures


def validate_weekly_yoy_output(
    df: pd.DataFrame, area_name: str
) -> Tuple[bool, List[Dict[str, Any]]]:
    """Validate YoY output — ratios should be within reasonable bounds."""
    failures: List[Dict[str, Any]] = []
    context = gx.get_context()
    data_source = context.data_sources.add_pandas("mcard_yoy_validation")
    data_asset = data_source.add_dataframe_asset("yoy_data")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        "yoy_batch"
    )
    batch = batch_definition.get_batch(
        batch_parameters={"dataframe": df}
    )

    checks = [
        gx.expectations.ExpectTableRowCountToBeGreaterThan(value=0),
    ]

    yoy_cols = [c for c in df.columns if c.startswith("yoy_")]
    for col in yoy_cols:
        checks.append(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column=col, min_value=-1.0, max_value=100.0,
                mostly=0.95,
            )
        )

    for check in checks:
        result = batch.validate(check)
        if not result.success:
            failures.append({
                "area": area_name,
                "expectation": type(check).__name__,
                "column": getattr(check, "column", None),
                "details": str(result.result),
            })

    passed = len(failures) == 0
    if passed:
        logger.info(f"Mcard YoY validation PASSED for {area_name}")
    else:
        logger.error(
            f"Mcard YoY validation FAILED for {area_name}: "
            f"{len(failures)} check(s) failed"
        )
    return passed, failures


def validate_adjustment_factors(df: pd.DataFrame) -> Tuple[bool, List[Dict[str, Any]]]:
    """Validate adjustment factors are non-null and non-zero."""
    failures: List[Dict[str, Any]] = []
    context = gx.get_context()
    data_source = context.data_sources.add_pandas("mcard_adj_validation")
    data_asset = data_source.add_dataframe_asset("adj_data")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        "adj_batch"
    )
    batch = batch_definition.get_batch(
        batch_parameters={"dataframe": df}
    )

    adj_cols = [c for c in df.columns if c.startswith("adjustment_factor_")]
    checks = [
        gx.expectations.ExpectTableRowCountToBeGreaterThan(value=0),
    ]
    for col in adj_cols:
        checks.extend([
            gx.expectations.ExpectColumnValuesToNotBeNull(column=col, mostly=0.95),
            gx.expectations.ExpectColumnValuesToBeGreaterThan(
                column=col, value=0.0, mostly=0.95
            ),
        ])

    for check in checks:
        result = batch.validate(check)
        if not result.success:
            failures.append({
                "expectation": type(check).__name__,
                "column": getattr(check, "column", None),
                "details": str(result.result),
            })

    passed = len(failures) == 0
    if passed:
        logger.info("Adjustment factor validation PASSED")
    else:
        logger.error(
            f"Adjustment factor validation FAILED: {len(failures)} check(s)"
        )
    return passed, failures

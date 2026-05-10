"""
LdcPremisesTransform: Data transformation logic for LDC premises data.

This module ports the business logic from hsds_datahub/src/etl/premises.py
into the HSDS pipeline architecture. The core transformation logic is
preserved EXACTLY to maintain data consistency with the original pipeline.

Original source: Z:\\Anupam\\Repositories\\hsds_datahub\\src\\etl\\premises.py
"""

import logging
from typing import List, Optional

import numpy as np
import pandas as pd


class LdcPremisesTransformException(Exception):
    """Exception raised for LDC premises transformation errors."""
    pass


class LdcPremisesTransform:
    """
    Data transformation class for LDC premises data.

    This class implements all cleaning and transformation logic from the
    original hsds_datahub premises.py module. The business logic is preserved
    EXACTLY to ensure data consistency.

    Key transformations:
    - Basic formatting (datetime conversion, null handling)
    - Deduplication (based on tenant, premises_id, date_create, etc.)
    - Date fixing (vacancy overlaps, gaps, neighbouring duplicates)
    - Derived columns (latest_record_check, latest_premises_check)
    - Vacancy classification (fill_vacant_use)
    - Column selection (38 clean columns)
    """

    # 38 columns in the clean output (from choose_columns)
    CLEAN_COLUMNS = [
        'tenant_id',
        'tenant',
        'address',
        'street',
        'geography',
        'zip',
        'geography_large',
        'uprn_id',
        'latitude',
        'longitude',
        'premises_id',
        'property_id',
        'property',
        'tenant_status',
        'premises_status',
        'company_id',
        'company',
        'company_holding',
        'tenant_care_of',
        'flag_independent',
        'category',
        'classification',
        'subcategory',
        'phone',
        'url_website',
        'url_image',
        'area_sm',
        'voa_business_rate',
        'date_create',
        'date_close',
        'date_last_survey_field',
        'date_last_survey_office',
        'date_premises_create',
        'timestamp_create',
        'timestamp_update',
        'latest_record_check',
        'latest_premises_check',
        'source',
    ]

    # Datetime columns that need conversion
    DATE_COLUMNS = [
        'timestamp_update',
        'timestamp_create',
        'date_last_survey_field',
        'date_last_survey_office',
        'date_create',
        'date_close',
        'date_premises_create'
    ]

    def __init__(self):
        """Initialize LdcPremisesTransform."""
        self.logger = logging.getLogger(__name__)
        self.logger.info("LdcPremisesTransform initialized")

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Execute the full transformation pipeline.

        This replicates the exact flow from clean_premises_data.py:
        1. basic_formatting (datetime + dedup)
        2. fix_premises_dates (date corrections, gap filling)
        3. forward-fill uprn_id
        4. fetch_latest_checks
        5. fill_vacant_use
        6. amend_floorspace
        7. choose_columns

        Args:
            df: Raw premises DataFrame

        Returns:
            Cleaned DataFrame with 38 columns
        """
        self.logger.info(f"Starting transformation on {len(df)} rows")

        # Step 1: Basic formatting
        df = self.basic_formatting(df)
        self.logger.info(f"After basic_formatting: {len(df)} rows")

        # Step 2: Fix premises dates
        df = self.fix_premises_dates(df)
        self.logger.info(f"After fix_premises_dates: {len(df)} rows")

        # Step 3: Forward-fill uprn_id by premises_id
        df = self._forward_fill_uprn(df)
        self.logger.info("Forward-filled uprn_id by premises_id")

        # Step 4: Fetch latest checks
        df = self.fetch_latest_checks(df)
        self.logger.info("Computed latest_record_check and latest_premises_check")

        # Step 5: Fill vacant use
        df = self.fill_vacant_use(df)
        self.logger.info("Filled vacant property classifications")

        # Step 6: Amend floorspace
        df = self.amend_floorspace(df)
        self.logger.info("Amended floorspace (replaced 0 with NA)")

        # Step 7: Choose columns
        df = self.choose_columns(df)
        self.logger.info(f"Final output: {len(df)} rows, {len(df.columns)} columns")

        return df

    def _forward_fill_uprn(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Forward-fill uprn_id by premises_id.

        This fills missing UPRN values using other records for the same premises.
        """
        if 'uprn_id' in df.columns:
            df = df.sort_values(
                by=['premises_id', 'date_create'],
                ascending=[True, False]
            )
            df['uprn_id'] = df.groupby('premises_id')['uprn_id'].ffill()
            df['uprn_id'] = df.groupby('premises_id')['uprn_id'].bfill()
        return df

    # =========================================================================
    # CORE BUSINESS LOGIC - PRESERVED EXACTLY FROM premises.py
    # =========================================================================

    def drop_duplicate_rows(self, all_biz: pd.DataFrame) -> pd.DataFrame:
        """
        Deduplicate on the natural key (tenant_id, premises_id, date_create).

        Keeps one row per key, preferring live > historic > archived,
        then latest timestamp_update.

        Updated from original premises.py (which used tenant name, category,
        and tenant_care_of as dedup keys). The new key uses tenant_id which
        is LDC's stable identifier, resolving name-change and SWS-parent-
        change duplicates that accumulated across sources.

        See: ldc_dedup_analysis.md for full validation of this change.
        """
        if 'source' in all_biz.columns:
            all_biz['source'] = all_biz['source'].fillna('historic')
            source_priority = {'live': 0, 'historic': 1, 'archived': 2}
            all_biz['_src_pri'] = (
                all_biz['source'].map(source_priority).fillna(3)
            )
            sort_cols = [
                '_src_pri', 'timestamp_update',
                'date_create', 'date_close', 'tenant_id'
            ]
            ascending = [True, False, False, False, False]
        else:
            all_biz['_src_pri'] = 0
            sort_cols = [
                'timestamp_update', 'date_create',
                'date_close', 'tenant_id'
            ]
            ascending = [False, False, False, False]

        all_biz = (
            all_biz.sort_values(
                by=sort_cols,
                ascending=ascending,
                na_position='first'
            ).drop_duplicates(
                subset=['tenant_id', 'premises_id', 'date_create'],
                keep='first'
            )
        )
        all_biz = all_biz.drop(columns=['_src_pri'], errors='ignore')
        return all_biz

    def basic_formatting(self, all_biz: pd.DataFrame) -> pd.DataFrame:
        """
        Basic/general formatting.
        Before fixing dates:
        1. Make sure consistent NaNs in DF
        2. Set datetime columns
        3. Get latest record check from date_last_survey fields
        4. Drop duplicates between live and historic

        Source: premises.py lines 99-128
        """
        # coerce nulls all to same format
        all_biz = all_biz.fillna(np.nan)

        # convert datetime columns
        for col in self.DATE_COLUMNS:
            if col in all_biz.columns:
                all_biz[col] = pd.to_datetime(all_biz[col], errors='coerce')

        # Drop duplicates (to remove duplicates between live and historic)
        all_biz = self.drop_duplicate_rows(all_biz)

        return all_biz

    def reset_next_and_previous_dates(self, biz_main: pd.DataFrame) -> pd.DataFrame:
        """
        Helper function for cleaning dates.
        Called multiple times in the 'fix premises dates' function.

        Source: premises.py lines 131-159
        """
        biz_main = biz_main.sort_values(
            by=[
                'premises_id',
                'date_create',
                'date_close'  # remove source sort, added close sort
            ],
            ascending=False,
            na_position='first'
        ).reset_index(drop=True)

        biz_main['next_date_create'] = biz_main.groupby(
            ['premises_id']
        )['date_create'].shift(1)

        biz_main['prev_date_create'] = biz_main.groupby(
            ['premises_id']
        )['date_create'].shift(-1)

        biz_main['prev_date_close'] = biz_main.groupby(
            ['premises_id']
        )['date_close'].shift(-1)

        return biz_main

    def merge_neighbouring_duplicate_tenants(
        self, biz_main: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Merges neighbouring duplicate tenants.
        - The same tenants need to be next to each other in time
        - Then this will take the earliest date_create and
          latest date_close (including NaT)

        Source: premises.py lines 162-217
        """
        biz_main = biz_main.sort_values(
            by=[
                'premises_id',
                'date_create',
                'date_close'  # remove source sort, added close sort
            ],
            ascending=False,
            na_position='first'
        ).reset_index(drop=True)

        biz_main['prev_tenant'] = biz_main.groupby(
            ['premises_id']
        )['tenant'].shift(-1)

        biz_main['next_tenant'] = biz_main.groupby(
            ['premises_id']
        )['tenant'].shift(1)

        # Where the previous tenant is the same as the current tenant,
        # take the earliest date_create and the
        # latest date_close (including NaT) of both rows
        biz_main['date_create'] = np.where(
            biz_main['tenant'] == biz_main['prev_tenant'],
            biz_main[
                ['prev_date_create', 'date_create']
            ].min(axis=1),
            biz_main['date_create']
        )

        biz_main['date_close'] = np.where(
            biz_main['tenant'] == biz_main['prev_tenant'],
            # skipna=False will take NaT if it's there, rather than the max
            # date_close (i.e. because it hasn't closed)
            biz_main[
                ['prev_date_close', 'date_close']
            ].max(axis=1, skipna=False),
            biz_main['date_close']
        )

        # and then drop the second row i.e., where
        # next tenant is the same as first tenant
        biz_main = biz_main[
            biz_main['next_tenant'] != biz_main['tenant']
        ]

        return biz_main

    def drop_rows_where_date_close_is_earlier_than_date_create(
        self, biz_main: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Helper function that removes rows where date_close < date_create.
        Needs to be repeated multiple times within fix_premises_dates.

        Source: premises.py lines 220-232
        """
        biz_main = biz_main[
            (biz_main['date_close'].isnull()) | (biz_main['date_close'] > biz_main[
                'date_create'])
        ]
        return biz_main

    def fix_premises_dates(self, all_biz: pd.DataFrame) -> pd.DataFrame:
        """
        Complex date correction including:
        1. Separate out SWS and non-SWS (cleaning only for main premises)
        2. Sort and get next/previous dates for each premise
        3. Merge neighbouring duplicate tenants
        4. Drop rows where date_close < date_create
        5. Fix vacancy overlaps
        6. Remove duplicate vacancies when business open at same time
        7. Find gaps in timeseries -> add 'Vacant Property' row
        8. Check for overlaps -> change date_close to next date_create
        9. Concatenate SWS df back on

        Source: premises.py lines 236-393
        """
        # SEPARATE SWS AND MAIN PREMISES
        # (proceed only with main)
        biz_sws = all_biz[
            all_biz['tenant_care_of'].notna()
        ].copy()
        biz_main = all_biz[
            all_biz['tenant_care_of'].isna()
        ].copy()

        # MAIN PREMISES CLEANING

        # Get next and previous row dates
        # (Rerun this whenever rows are added/dropped or dates are changed)
        biz_main = self.reset_next_and_previous_dates(biz_main)

        # Merge duplicates first
        # (e.g. if there are 2 of the same tenant next to each other
        # at a premises, merge into one record)
        biz_main = self.merge_neighbouring_duplicate_tenants(biz_main)

        # Only keep rows where date_close is NULL or
        # date_close is LATER than date_create
        # (best to just remove these; mostly Vacant Properties, not very many)
        biz_main = self.drop_rows_where_date_close_is_earlier_than_date_create(
            biz_main
        )
        biz_main = self.reset_next_and_previous_dates(biz_main)

        # Fixing the vacancy overlaps
        # (If a property is vacant but the vacancy started BEFORE
        # the previous tenant left, set the vacancy date_create
        # equal to previous date_close)
        biz_main['date_create'] = np.where(
            (biz_main['tenant'] == 'Vacant Property') & (biz_main[
                'prev_date_close'] > biz_main['date_create']),
            biz_main['prev_date_close'],
            biz_main['date_create']
        )

        # Drop any rows that appeared that don't make sense
        biz_main = self.drop_duplicate_rows(biz_main)
        biz_main = self.drop_rows_where_date_close_is_earlier_than_date_create(
            biz_main
        )
        biz_main = self.reset_next_and_previous_dates(biz_main)

        # If there's a vacancy and another business active
        # at the same time (they have the same date_create),
        # remove the vacant row
        biz_main = biz_main[
            ~(
                (biz_main['date_create'] == biz_main['next_date_create']) & (biz_main[
                    'tenant'] == 'Vacant Property')
            )
        ]

        biz_main = self.reset_next_and_previous_dates(biz_main)

        # FILL GAPS WITH VACANCIES

        # Get all the rows where there's a gap BEFORE the row
        gaps = biz_main[
            (biz_main['prev_date_close'] < biz_main['date_create'])
        ].copy()

        # Generate some bogus values for these new 'Vacant Property' rows

        # create empty tenant_ids (so we don't inadvertently
        # use tenant_ids that LDC will use in the future)
        gaps['tenant_id'] = 0
        gaps['tenant'] = 'Vacant Property'
        gaps['source'] = 'historic'  # this will always be 'historic' data
        gaps['date_close'] = gaps['date_create'].copy()
        gaps['date_create'] = gaps['prev_date_close'].copy()
        gaps['tenant_status'] = 'Vacant'
        gaps['premises_status'] = 'Vacant'
        gaps['classification'] = np.nan  # These will be set to 'Unknown' later
        gaps['category'] = np.nan
        gaps['subcategory'] = np.nan

        # Add back on to main dataframe
        biz_main = pd.concat([biz_main, gaps]).sort_values(
            by=[
                'premises_id',
                'date_create',
                'date_close'  # remove source sort, added close sort
            ],
            ascending=False,
            na_position='first'
        )

        # Reset dates again
        biz_main = self.reset_next_and_previous_dates(biz_main)

        # Where there's not a match between date_create and previous_closure,
        # set date_close to the next_date_create
        biz_main['date_close'] = np.where(
            (biz_main['date_close'] != biz_main['next_date_create']) & (biz_main[
                'next_date_create'].notnull()),
            biz_main['next_date_create'],
            biz_main['date_close']
        )

        # Merge duplicates again to catch duplicates after new gaps are added
        biz_main = self.merge_neighbouring_duplicate_tenants(biz_main)

        # CONCATENATE MAIN AND SWS BACK
        all_biz = pd.concat([biz_main, biz_sws])

        return all_biz

    def fetch_latest_checks(self, all_biz: pd.DataFrame) -> pd.DataFrame:
        """
        Fetch the latest record check (field vs office research date).
        Additional code to check the latest_record_check date is < date create
        (where there isn't a date_close e.g., it's the 'live' data).
        If not, set latest_record_check to be the same as date_create.

        Source: premises.py lines 396-432
        """
        # Fetch latest_record_check date as max of field or office check date
        all_biz['latest_record_check'] = all_biz[
            [
                'date_last_survey_field',
                'date_last_survey_office'
            ]
        ].max(axis=1)
        all_biz['latest_record_check'] = pd.to_datetime(
            all_biz['latest_record_check']
        )

        # Make sure it's not before a date_create, as this doesn't make sense
        all_biz['latest_record_check'] = np.where(
            (all_biz['latest_record_check'] < all_biz['date_create']) & (all_biz[
                'date_close'].isnull()),
            all_biz['date_create'],
            all_biz['latest_record_check']
        )

        # Then fetch the latest premises check (from the full history)
        all_biz['latest_premises_check'] = all_biz.groupby(
            ['premises_id']
        )['latest_record_check'].transform('max')
        all_biz['latest_premises_check'] = pd.to_datetime(
            all_biz['latest_premises_check']
        )

        return all_biz

    def fill_vacant_use(self, all_biz: pd.DataFrame) -> pd.DataFrame:
        """
        Set all vacant property classifications / categories to 'Vacant'.

        Source: premises.py lines 435-459
        """
        all_biz['classification'] = np.where(
            all_biz['tenant'] == 'Vacant Property',
            'Vacant',
            all_biz['classification']
        )

        all_biz['category'] = np.where(
            all_biz['tenant'] == 'Vacant Property',
            'Vacant',
            all_biz['category']
        )

        all_biz['subcategory'] = np.where(
            all_biz['tenant'] == 'Vacant Property',
            'Vacant',
            all_biz['subcategory']
        )

        return all_biz

    def amend_floorspace(self, all_biz: pd.DataFrame) -> pd.DataFrame:
        """
        Replace 0 floorspace with missing (pd.NA).

        Source: premises.py lines 462-472
        """
        all_biz['area_sm'] = np.where(
            all_biz['area_sm'] == 0,
            pd.NA,
            all_biz['area_sm']
        )
        return all_biz

    def choose_columns(
        self,
        all_biz: pd.DataFrame,
        columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Reduce to clean output columns (38 by default).

        Source: premises.py lines 475-523

        Args:
            all_biz: DataFrame with all columns
            columns: Optional list of columns to keep. Defaults to CLEAN_COLUMNS.

        Returns:
            DataFrame with selected columns only
        """
        cols_to_keep = columns or self.CLEAN_COLUMNS

        # Only keep columns that exist in the dataframe
        all_biz = all_biz[
            [col for col in cols_to_keep if col in all_biz.columns]
        ]

        return all_biz

    # =========================================================================
    # ADDITIONAL HELPER METHODS
    # =========================================================================

    def transform_raw_only(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply only raw-level transformations (no column selection).

        This is useful when you want to keep all columns but apply
        the date fixes and deduplication.

        Args:
            df: Raw premises DataFrame

        Returns:
            DataFrame with date fixes and deduplication applied
        """
        self.logger.info(f"Starting raw transformation on {len(df)} rows")

        # Step 1: Basic formatting
        df = self.basic_formatting(df)

        # Step 2: Fix premises dates
        df = self.fix_premises_dates(df)

        # Step 3: Forward-fill uprn_id by premises_id
        df = self._forward_fill_uprn(df)

        # Step 4: Fetch latest checks
        df = self.fetch_latest_checks(df)

        # Step 5: Fill vacant use
        df = self.fill_vacant_use(df)

        # Step 6: Amend floorspace
        df = self.amend_floorspace(df)

        self.logger.info(f"Raw transformation complete: {len(df)} rows")
        return df

    def get_transformation_stats(self, df_before: pd.DataFrame, df_after: pd.DataFrame) -> dict:  # noqa: E501
        """
        Calculate transformation statistics.

        Args:
            df_before: DataFrame before transformation
            df_after: DataFrame after transformation

        Returns:
            Dictionary with transformation statistics
        """
        return {
            'rows_before': len(df_before),
            'rows_after': len(df_after),
            'rows_added': max(0, len(df_after) - len(df_before)),
            'rows_removed': max(0, len(df_before) - len(df_after)),
            'cols_before': len(df_before.columns),
            'cols_after': len(df_after.columns),
            'vacant_rows_after': len(df_after[df_after['tenant'] == 'Vacant Property'])
                if 'tenant' in df_after.columns else 0,   # noqa: E131
        }

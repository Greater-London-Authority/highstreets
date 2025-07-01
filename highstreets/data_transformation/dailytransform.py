import logging
import pandas as pd
import numpy as np

from highstreets.data_source_sink.dataloader import DataLoader


class DailyTransform(DataLoader):
    def __init__(self):
        super().__init__()  # Initialize parent class to get database connection
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(logging.StreamHandler())
        self._lookup_cache = None  # Cache for lookup table

    def _get_poi_lookup_table(self):
        """
        Fetch and cache the POI lookup table for uid to id mapping.

        Returns:
            pandas.DataFrame: Lookup table with uid, id, layer, name columns
        """
        if self._lookup_cache is not None:
            return self._lookup_cache

        try:
            self.logger.info("Fetching POI lookup table...")
            self._lookup_cache = self.get_full_data(
                "econ_busyness_bt_uid_jan25_lookup"
            )

            if self._lookup_cache is None or self._lookup_cache.empty:
                raise ValueError(
                    "Lookup table is empty or could not be retrieved"
                )

            # Validate required columns
            required_lookup_cols = ['uid', 'id', 'layer', 'name']
            missing_cols = [
                col for col in required_lookup_cols
                if col not in self._lookup_cache.columns
            ]
            if missing_cols:
                raise ValueError(
                    f"Missing required columns in lookup table: "
                    f"{', '.join(missing_cols)}"
                )

            self.logger.info(
                f"Successfully loaded lookup table with "
                f"{len(self._lookup_cache)} records"
            )
            return self._lookup_cache

        except Exception as e:
            self.logger.error(f"Failed to fetch POI lookup table: {str(e)}")
            raise ValueError(
                f"Failed to fetch POI lookup table: {str(e)}"
            ) from None

    def raw_bt_daily_preprocess_data(self, data):
        self.logger.info("Starting data transformation...")
        # Validate input data
        # Convert JSON data to DataFrame
        try:
            df = pd.DataFrame(data)
        except ValueError:
            self.logger.error("Invalid JSON data format. Cannot convert to DataFrame.")
            raise ValueError(
                "Invalid JSON data format. Cannot convert to DataFrame."
            ) from None
        # Validate input data columns
        required_columns = [
            "poi_id",
            "poi_name",
            "poi_type",
            "date",
            "time_indicator",
            "total_unique_volume",
            "total_unique_intl_only_visitors",
            "total_unique_domestic_visitors",
            "total_unique_workers",
            "total_unique_residents",
            "avg_dwell_time"
        ]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            self.logger.error(f"Missing required columns: {', '.join(missing_columns)}")
            raise ValueError(
                f"Missing required columns: {', '.join(missing_columns)}"
            ) from None

        # Copy data to avoid modifying the original DataFrame
        transformed_data = df.copy()

        # Handle the new poi_uid and poi_id logic
        self.logger.info("Processing POI ID lookup...")

        # Rename the original poi_id from API to poi_uid
        transformed_data = transformed_data.rename(columns={"poi_id": "poi_uid"})

        # Get lookup table and perform join
        try:
            lookup_table = self._get_poi_lookup_table()

            # Perform left join to get the mapped poi_id
            transformed_data = transformed_data.merge(
                lookup_table[['uid', 'id']],
                left_on='poi_uid',
                right_on='uid',
                how='left'
            )

            # Rename the joined 'id' column to 'poi_id'
            transformed_data = transformed_data.rename(columns={"id": "poi_id"})

            # Drop the redundant 'uid' column from the join
            transformed_data = transformed_data.drop(columns=['uid'])

            # Check for unmapped records
            unmapped_count = transformed_data['poi_id'].isna().sum()
            if unmapped_count > 0:
                self.logger.warning(
                    f"Found {unmapped_count} records with unmapped poi_uid values"
                )
                # Log some examples for debugging
                unmapped_uids = transformed_data[
                    transformed_data['poi_id'].isna()
                ]['poi_uid'].unique()[:5]
                self.logger.warning(
                    f"Examples of unmapped poi_uid values: {list(unmapped_uids)}"
                )

        except Exception as e:
            self.logger.error(f"Failed to perform POI lookup: {str(e)}")
            raise ValueError(f"Failed to perform POI lookup: {str(e)}") from None

        # Continue with existing transformations
        transformed_data["date"] = pd.to_datetime(transformed_data["date"])
        transformed_data = transformed_data.rename(
            columns={"date": "count_date"}
        )

        # Select specific columns - now including poi_uid
        transformed_data = transformed_data[
            [
                "poi_id",
                "poi_uid",
                "poi_name",
                "poi_type",
                "count_date",
                "time_indicator",
                "total_unique_volume",
                "total_unique_intl_only_visitors",
                "total_unique_domestic_visitors",
                "total_unique_workers",
                "total_unique_residents",
                "avg_dwell_time",
            ]
        ]

        # Data type conversions
        transformed_data.replace("IDE", np.nan, inplace=True)
        transformed_data['total_unique_domestic_visitors'] = transformed_data[
            'total_unique_domestic_visitors'].astype('Int64')
        transformed_data['total_unique_intl_only_visitors'] = transformed_data[
            'total_unique_intl_only_visitors'].astype('Int64')
        transformed_data['total_unique_residents'] = transformed_data[
            'total_unique_residents'].astype('Int64')
        transformed_data['total_unique_volume'] = transformed_data[
            'total_unique_volume'].astype('Int64')
        transformed_data['total_unique_workers'] = transformed_data[
            'total_unique_workers'].astype('Int64')
        transformed_data['avg_dwell_time'] = transformed_data[
            'avg_dwell_time'].astype('Int64')

        # Handle data types for ID columns
        transformed_data['poi_uid'] = transformed_data['poi_uid'].astype(str)
        # poi_id from lookup might be int or string - convert to string for consistency
        transformed_data['poi_id'] = transformed_data['poi_id'].astype(str)

        self.logger.info("Data pre-processing completed.")
        return transformed_data

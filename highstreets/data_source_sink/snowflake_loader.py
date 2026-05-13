"""
SnowflakeLoader: Data loader for LDC premises data from Snowflake.

This module provides functionality to connect to the LDC Snowflake database,
fetch premises data, and prepare it for ingestion into the HSDS pipeline.
"""

import logging
import os
from typing import Optional, List

import pandas as pd
from dotenv import find_dotenv, load_dotenv
from sqlalchemy import create_engine, text
from snowflake.sqlalchemy import URL

load_dotenv(find_dotenv())


class SnowflakeLoaderException(Exception):
    """Exception raised for Snowflake loader errors."""
    pass


class SnowflakeLoader:
    """
    Data loader for LDC premises data from Snowflake.

    Connects to the LDC Snowflake database and provides methods to fetch
    premises data for the HSDS pipeline.

    Attributes:
        engine: SQLAlchemy engine for Snowflake connection
        logger: Logger instance for this class
        schema: Snowflake schema name
        table: Snowflake table/view name
    """

    # Default Snowflake connection parameters
    DEFAULT_SCHEMA = "SCH_GREEN_STREET"
    DEFAULT_TABLE = "VW_GS_RETAIL_UK_TENANT_V2"

    # Columns to select for PostgreSQL raw table (57 columns)
    WORKING_COLUMNS = [
        # Primary key columns
        'tenant_id', 'premises_id', 'date_create',

        # Core fields
        'tenant', 'tenant_status', 'premises_status',
        'category', 'category_id', 'classification', 'classification_id',
        'subcategory', 'subcategory_id', 'subcategory_previous',
        'subcategory_previous_id',

        # Location
        'address', 'street', 'city', 'zip', 'building', 'street_number', 'unit_number',
        'geography', 'geography_id', 'geography_large', 'geography_large_id',
        'latitude', 'longitude', 'uprn_id',

        # Property
        'property', 'property_id', 'premises_previous_id', 'property_type',

        # Company
        'company', 'company_id', 'company_holding', 'company_holding_id',
        'tenant_care_of', 'flag_independent', 'flag_concession', 'flag_field_researched',
        'flag_area_sm_modelled',

        # Metrics
        'area_sm', 'voa_business_rate', 'geography_large_pct',

        # Contact
        'phone', 'retail_mix', 'url_website', 'url_image',

        # Dates
        'date_close', 'date_premises_create',
        'date_last_survey_field', 'date_last_survey_office',
        'timestamp_create', 'timestamp_update',
    ]

    def __init__(
        self,
        schema: Optional[str] = None,
        table: Optional[str] = None
    ):
        """
        Initialize SnowflakeLoader with connection parameters.

        Args:
            schema: Snowflake schema name. Defaults to SCH_GREEN_STREET.
            table: Snowflake table/view name. Defaults to VW_GS_RETAIL_UK_TENANT_V2.
        """
        self.logger = logging.getLogger(__name__)
        self.schema = schema or self.DEFAULT_SCHEMA
        self.table = table or self.DEFAULT_TABLE
        self.engine = self._create_engine()
        self.logger.info(
            f"SnowflakeLoader initialized for {self.schema}.{self.table}"
        )

    def _create_engine(self):
        """
        Create SQLAlchemy engine for Snowflake connection.

        Returns:
            SQLAlchemy engine instance

        Raises:
            SnowflakeLoaderException: If required environment variables are missing
        """
        try:
            user = os.getenv("GS_USER")
            password = os.getenv("GS_PASSWORD")
            account_identifier = os.getenv("GS_ACCT_ID")
            database_name = os.getenv("GS_DATABASE")
            schema = os.getenv("GS_SCHEMA")
            warehouse = os.getenv("GS_WAREHOUSE")
            role = os.getenv("GS_ROLE")

            # Validate required credentials
            required_vars = {
                "GS_USER": user,
                "GS_PASSWORD": password,
                "GS_ACCT_ID": account_identifier,
                "GS_DATABASE": database_name,
                "GS_WAREHOUSE": warehouse,
            }

            missing = [k for k, v in required_vars.items() if not v]
            if missing:
                raise SnowflakeLoaderException(
                    f"Missing required environment variables: {', '.join(missing)}"
                )

            url = URL(
                user=user,
                password=password,
                account=account_identifier,
                database=database_name,
                schema=schema,
                warehouse=warehouse,
                role=role
            )
            engine = create_engine(url, connect_args={'insecure_mode': True})
            self.logger.info("Snowflake engine created successfully")
            return engine

        except Exception as e:
            self.logger.error(f"Failed to create Snowflake engine: {str(e)}")
            raise SnowflakeLoaderException(
                f"Failed to create Snowflake engine: {str(e)}"
            ) from e

    def load_full_data(self, additional_query: Optional[str] = None) -> pd.DataFrame:
        """
        Load all data from Snowflake table/view.

        Args:
            additional_query: Optional SQL clause to append (e.g., "WHERE ...")

        Returns:
            DataFrame containing all premises data

        Raises:
            SnowflakeLoaderException: If data fetch fails
        """
        try:
            if additional_query:
                query = f"SELECT * FROM {self.schema}.{self.table} {additional_query}"
            else:
                query = f"SELECT * FROM {self.schema}.{self.table}"

            self.logger.info(f"Executing query: {query[:100]}...")

            with self.engine.connect() as connection:
                df = pd.read_sql(text(query), connection)

            # Lowercase column names for consistency
            df.columns = df.columns.str.lower()

            # Add source column
            df['source'] = 'live'

            self.logger.info(
                f"Loaded {len(df)} rows with {len(df.columns)} columns from Snowflake"
            )
            return df

        except Exception as e:
            self.logger.error(f"Failed to load data from Snowflake: {str(e)}")
            raise SnowflakeLoaderException(
                f"Failed to load data from Snowflake: {str(e)}"
            ) from e

    def load_working_columns(
        self,
        columns: Optional[List[str]] = None,
        additional_query: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Load only the working columns needed for PostgreSQL raw table.

        Args:
            columns: List of column names to select. Defaults to WORKING_COLUMNS.
            additional_query: Optional SQL clause to append (e.g., "WHERE ...")

        Returns:
            DataFrame containing selected columns

        Raises:
            SnowflakeLoaderException: If data fetch fails
        """
        cols = columns or self.WORKING_COLUMNS

        try:
            # Build column list, handling case sensitivity
            col_str = ", ".join(cols)

            if additional_query:
                query = (f"SELECT {col_str} FROM {self.schema}.{self.table} "
                         f"{additional_query}")
            else:
                query = f"SELECT {col_str} FROM {self.schema}.{self.table}"

            self.logger.info(f"Executing query for {len(cols)} columns...")

            with self.engine.connect() as connection:
                df = pd.read_sql(text(query), connection)

            # Lowercase column names for consistency
            df.columns = df.columns.str.lower()

            # Add source column
            df['source'] = 'live'

            self.logger.info(
                f"Loaded {len(df)} rows with {len(df.columns)} columns from Snowflake"
            )
            return df

        except Exception as e:
            self.logger.error(f"Failed to load working columns: {str(e)}")
            raise SnowflakeLoaderException(
                f"Failed to load working columns: {str(e)}"
            ) from e

    def get_row_count(self) -> int:
        """
        Get the total row count from Snowflake table.

        Returns:
            Number of rows in the table
        """
        try:
            query = f"SELECT COUNT(*) as cnt FROM {self.schema}.{self.table}"

            with self.engine.connect() as connection:
                result = connection.execute(text(query)).fetchone()

            count = result[0] if result else 0
            self.logger.info(f"Snowflake table has {count} rows")
            return count

        except Exception as e:
            self.logger.error(f"Failed to get row count: {str(e)}")
            raise SnowflakeLoaderException(
                f"Failed to get row count: {str(e)}"
            ) from e

    def get_column_names(self) -> List[str]:
        """
        Get the list of column names from Snowflake table.

        Returns:
            List of column names (lowercase)
        """
        try:
            query = f"SELECT * FROM {self.schema}.{self.table} LIMIT 1"

            with self.engine.connect() as connection:
                df = pd.read_sql(text(query), connection)

            columns = [col.lower() for col in df.columns.tolist()]
            self.logger.info(f"Snowflake table has {len(columns)} columns")
            return columns

        except Exception as e:
            self.logger.error(f"Failed to get column names: {str(e)}")
            raise SnowflakeLoaderException(
                f"Failed to get column names: {str(e)}"
            ) from e

    def validate_schema(self, expected_columns: List[str]) -> dict:
        """
        Validate that expected columns exist in Snowflake table.

        Args:
            expected_columns: List of expected column names

        Returns:
            Dict with 'valid' (bool), 'missing' (list), 'extra' (list)
        """
        try:
            actual_columns = set(self.get_column_names())
            expected_set = set(col.lower() for col in expected_columns)

            missing = expected_set - actual_columns
            extra = actual_columns - expected_set

            result = {
                'valid': len(missing) == 0,
                'missing': list(missing),
                'extra': list(extra),
                'actual_count': len(actual_columns),
                'expected_count': len(expected_set)
            }

            if missing:
                self.logger.warning(f"Missing columns: {missing}")

            return result

        except Exception as e:
            self.logger.error(f"Failed to validate schema: {str(e)}")
            raise SnowflakeLoaderException(
                f"Failed to validate schema: {str(e)}"
            ) from e

    def close(self):
        """Close the Snowflake connection."""
        if self.engine:
            self.engine.dispose()
            self.logger.info("Snowflake connection closed")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False

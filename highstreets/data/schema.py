"""
Defines schema models for BT footfall data across different boundary types.

This module provides pandera schema validation for BT footfall data that flows
through the highstreets data pipeline. It includes schemas for:

1. Raw data coming from the BT API (hex grid format)
2. Transformed data stored in the database
3. Aggregated data by different boundary types (highstreets, town centres, BIDs,
   bespoke areas)
4. Lookup mappings between hex grids and various boundary types

Each schema enforces data types, nullable fields, and valid value ranges to ensure
data quality throughout the pipeline. The schemas follow a hierarchy with a base
schema that defines common validation methods.
"""

import pandas as pd
import pandera as pa
from pandera.typing import Series


# Base schema that can be extended by other schemas
class BaseSchema(pa.SchemaModel):
    """
    Base schema providing common validation methods and configuration.

    This class serves as the foundation for all other schemas in the module,
    providing inheritance and extension capabilities.
    """

    @classmethod
    def add_columns(cls, **kwargs):
        """Add columns to schema dynamically."""
        for name, field in kwargs.items():
            cls.Config.columns[name] = field
        return cls


# BT Hex Raw Data Schema (from API)
class BTHexRawSchema(BaseSchema):
    """
    Schema for validating raw BT hex grid footfall data from API.

    This schema validates the structure and content of data received directly
    from the BT footfall API before any transformations. It ensures that all
    required fields are present and correctly formatted.
    """

    hex_id: Series[str] = pa.Field(
        description="Unique identifier for the TfL hex grid cell"
    )
    count_date: Series[pd.Timestamp] = pa.Field(
        description="Date when footfall was measured (YYYY-MM-DD)"
    )
    time_indicator: Series[str] = pa.Field(
        description="3-hour time period in format HH-HH (e.g., '09-12')",
        isin=["00-03", "03-06", "06-09", "09-12", "12-15",
              "15-18", "18-21", "21-00"]
    )
    resident: Series[int] = pa.Field(
        description=("Estimated count of residents within the"
                     "hex grid during the time period"),
        nullable=True,
        coerce=True
    )
    visitor: Series[int] = pa.Field(
        description=("Estimated count of visitors (non-residents, non-workers)"
                     "within the hex grid"),
        nullable=True,
        coerce=True
    )
    worker: Series[int] = pa.Field(
        description=("Estimated count of workers within the"
                     "hex grid during the time period"),
        nullable=True,
        coerce=True
    )
    loyalty_percentage: Series[float] = pa.Field(
        description=("Percentage of visitors who have made repeat visits"
                     "to the location"),
        nullable=True,
        coerce=True
    )
    dwell_time: Series[float] = pa.Field(
        description=("Average time (in minutes) that people spend in the"
                     "hex grid area"),
        nullable=True,
        coerce=True
    )


# Transformed BT Hex Schema (after database storage)
class BTHexTransformedSchema(BaseSchema):
    """
    Schema for validating transformed BT hex grid data.

    After initial processing of raw API data, this schema validates the
    structure of the transformed data before storage in the database. It ensures
    consistent naming conventions and data types across the pipeline.
    """

    hex_id: Series[str] = pa.Field(
        description="Unique identifier for the TfL hex grid cell"
    )
    count_date: Series[pd.Timestamp] = pa.Field(
        description="Date when footfall was measured (YYYY-MM-DD)"
    )
    hours: Series[str] = pa.Field(
        description="3-hour time period in format HH-HH (e.g., '09-12')",
        isin=["00-03", "03-06", "06-09", "09-12", "12-15",
              "15-18", "18-21", "21-00"]
    )
    resident: Series[int] = pa.Field(
        description=("Estimated count of residents within the"
                     "hex grid during the time period"),
        nullable=True,
        coerce=True
    )
    visitor: Series[int] = pa.Field(
        description=("Estimated count of visitors (non-residents, non-workers)"
                     "within the hex grid"),
        nullable=True,
        coerce=True
    )
    worker: Series[int] = pa.Field(
        description=("Estimated count of workers within the"
                     "hex grid during the time period"),
        nullable=True,
        coerce=True
    )
    loyalty_percentage: Series[float] = pa.Field(
        description=("Percentage of visitors who have made repeat visits"
                     "to the location"),
        nullable=True,
        coerce=True
    )
    dwell_time: Series[float] = pa.Field(
        description=("Average time (in minutes) that people spend in the"
                     "hex grid area"),
        nullable=True,
        coerce=True
    )

    class Config:
        coerce = True
        strict = False


# Highstreet-specific BT data schema
class BTHighstreetSchema(BaseSchema):
    """
    Schema for BT data aggregated by highstreet boundaries.

    This schema validates footfall data that has been aggregated from hex grids
    to highstreet boundaries. Used for the 'econ_busyness_bt_highstreets_3hourly_counts'
    table and related data exports.
    """

    highstreet_id: Series[int] = pa.Field(
        description="Unique identifier for the highstreet boundary"
    )
    highstreet_name: Series[str] = pa.Field(
        description="Official name of the highstreet"
    )
    count_date: Series[pd.Timestamp] = pa.Field(
        description="Date when footfall was measured (YYYY-MM-DD)"
    )
    hours: Series[str] = pa.Field(
        description="3-hour time period in format HH-HH (e.g., '09-12')"
    )
    x: Series[float] = pa.Field(
        description="X coordinate of the highstreet centroid (EPSG:27700)",
        nullable=True
    )
    y: Series[float] = pa.Field(
        description="Y coordinate of the highstreet centroid (EPSG:27700)",
        nullable=True
    )
    borough: Series[str] = pa.Field(
        description="London borough containing the highstreet",
        nullable=True
    )
    resident: Series[int] = pa.Field(
        description="Aggregated count of residents within the highstreet boundary",
        nullable=True
    )
    visitor: Series[int] = pa.Field(
        description="Aggregated count of visitors within the highstreet boundary",
        nullable=True
    )
    worker: Series[int] = pa.Field(
        description="Aggregated count of workers within the highstreet boundary",
        nullable=True
    )
    ave_loyalty_percentage: Series[float] = pa.Field(
        description="Average loyalty percentage across all hex cells in the highstreet",
        nullable=True
    )
    ave_dwell_time: Series[float] = pa.Field(
        description=("Average dwell time across all hex cells in the"
                     "highstreet (minutes)"),
        nullable=True
    )

    class Config:
        coerce = True
        strict = False


# Town Centre-specific BT data schema
class BTTownCentreSchema(BaseSchema):
    """
    Schema for BT data aggregated by town centre boundaries.

    Validates footfall data aggregated from hex grids to town centre boundaries.
    Used for the 'econ_busyness_bt_towncentres_3hourly_counts' table and exports.
    """

    tc_id: Series[int] = pa.Field(
        description="Unique identifier for the town centre boundary"
    )
    tc_name: Series[str] = pa.Field(
        description="Official name of the town centre"
    )
    count_date: Series[pd.Timestamp] = pa.Field(
        description="Date when footfall was measured (YYYY-MM-DD)"
    )
    hours: Series[str] = pa.Field(
        description="3-hour time period in format HH-HH (e.g., '09-12')"
    )
    x: Series[float] = pa.Field(
        description="X coordinate of the town centre centroid (EPSG:27700)",
        nullable=True
    )
    y: Series[float] = pa.Field(
        description="Y coordinate of the town centre centroid (EPSG:27700)",
        nullable=True
    )
    borough: Series[str] = pa.Field(
        description="London borough containing the town centre",
        nullable=True
    )
    resident: Series[int] = pa.Field(
        description="Aggregated count of residents within the town centre boundary",
        nullable=True
    )
    visitor: Series[int] = pa.Field(
        description="Aggregated count of visitors within the town centre boundary",
        nullable=True
    )
    worker: Series[int] = pa.Field(
        description="Aggregated count of workers within the town centre boundary",
        nullable=True
    )
    ave_loyalty_percentage: Series[float] = pa.Field(
        description=("Average loyalty percentage across all hex cells in the"
                     "town centre"),
        nullable=True
    )
    ave_dwell_time: Series[float] = pa.Field(
        description=("Average dwell time across all hex cells in the"
                     "town centre (minutes)"),
        nullable=True
    )


# BID-specific BT data schema
class BTBidSchema(BaseSchema):
    """
    Schema for BT data aggregated by Business Improvement District (BID) boundaries.

    Validates footfall data aggregated from hex grids to BID boundaries.
    Used for the 'econ_busyness_bt_bids_3hourly_counts' table and exports.
    """

    bid_id: Series[int] = pa.Field(
        description="Unique identifier for the Business Improvement District boundary"
    )
    bid_name: Series[str] = pa.Field(
        description="Official name of the Business Improvement District"
    )
    count_date: Series[pd.Timestamp] = pa.Field(
        description="Date when footfall was measured (YYYY-MM-DD)"
    )
    hours: Series[str] = pa.Field(
        description="3-hour time period in format HH-HH (e.g., '09-12')"
    )
    resident: Series[int] = pa.Field(
        description="Aggregated count of residents within the BID boundary",
        nullable=True
    )
    visitor: Series[int] = pa.Field(
        description="Aggregated count of visitors within the BID boundary",
        nullable=True
    )
    worker: Series[int] = pa.Field(
        description="Aggregated count of workers within the BID boundary",
        nullable=True
    )
    ave_loyalty_percentage: Series[float] = pa.Field(
        description="Average loyalty percentage across all hex cells in the BID",
        nullable=True
    )
    ave_dwell_time: Series[float] = pa.Field(
        description="Average dwell time across all hex cells in the BID (minutes)",
        nullable=True
    )


# Bespoke area-specific BT data schema
class BTBespokeSchema(BaseSchema):
    """
    Schema for BT data aggregated by bespoke area boundaries.

    Validates footfall data aggregated from hex grids to bespoke area boundaries.
    Used for the 'econ_busyness_bt_bespokes_3hourly_counts' table and exports.
    These are custom-defined areas not matching standard boundaries.
    """

    bespoke_area_id: Series[int] = pa.Field(
        description="Unique identifier for the bespoke area boundary"
    )
    name: Series[str] = pa.Field(
        description=("Name of the bespoke area (can be a project name or"
                     "area description)")
    )
    count_date: Series[pd.Timestamp] = pa.Field(
        description="Date when footfall was measured (YYYY-MM-DD)"
    )
    hours: Series[str] = pa.Field(
        description="3-hour time period in format HH-HH (e.g., '09-12')"
    )
    resident: Series[int] = pa.Field(
        description="Aggregated count of residents within the bespoke area boundary",
        nullable=True
    )
    visitor: Series[int] = pa.Field(
        description="Aggregated count of visitors within the bespoke area boundary",
        nullable=True
    )
    worker: Series[int] = pa.Field(
        description="Aggregated count of workers within the bespoke area boundary",
        nullable=True
    )
    ave_loyalty_percentage: Series[float] = pa.Field(
        description=("Average loyalty percentage across all hex cells in the"
                     "bespoke area"),
        nullable=True
    )
    ave_dwell_time: Series[float] = pa.Field(
        description=("Average dwell time across all hex cells in the"
                     "bespoke area (minutes)"),
        nullable=True
    )


# Combined BT data schema (for all layers)
class BTCombinedSchema(BaseSchema):
    """
    Schema for combined BT data from all boundary types.

    Used for the unified 'econ_busyness_bt_3hourly_counts' table that combines
    data from highstreets, town centres, BIDs, and bespoke areas into a single
    dataset with a layer identifier.
    """

    count_date: Series[pd.Timestamp] = pa.Field(
        description="Date when footfall was measured (YYYY-MM-DD)"
    )
    hours: Series[str] = pa.Field(
        description="3-hour time period in format HH-HH (e.g., '09-12')"
    )
    id: Series[int] = pa.Field(
        description=("ID of the boundary (highstreet_id, tc_id, bid_id, or"
                     "bespoke_area_id)")
    )
    name: Series[str] = pa.Field(
        description=("Name of the boundary (corresponds to the boundary type)")
    )
    layer: Series[str] = pa.Field(
        description="Type of boundary layer this record belongs to",
        isin=["highstreets", "towncentres", "bids", "bespoke"]
    )
    resident: Series[int] = pa.Field(
        description="Aggregated count of residents within the boundary",
        nullable=True
    )
    visitor: Series[int] = pa.Field(
        description="Aggregated count of visitors within the boundary",
        nullable=True
    )
    worker: Series[int] = pa.Field(
        description="Aggregated count of workers within the boundary",
        nullable=True
    )
    ave_loyalty_percentage: Series[float] = pa.Field(
        description="Average loyalty percentage across all hex cells in the boundary",
        nullable=True
    )
    ave_dwell_time: Series[float] = pa.Field(
        description="Average dwell time across all hex cells in the boundary (minutes)",
        nullable=True
    )


# Lookup schemas for different boundary types
class HexHighstreetLookupSchema(BaseSchema):
    """
    Schema for hex grid to highstreet boundary lookup table.

    This defines the relationship between hex grid cells and highstreet boundaries,
    allowing aggregation of hex data to highstreet level.
    Used for the 'econ_busyness_hex_highstreet_lookup' table.
    """

    hex_id: Series[str] = pa.Field(
        description="Unique identifier for the TfL hex grid cell"
    )
    highstreet_id: Series[int] = pa.Field(
        description=("Unique identifier for the highstreet boundary that contains"
                     "this hex")
    )
    highstreet_name: Series[str] = pa.Field(
        description="Official name of the highstreet"
    )


class HexTownCentreLookupSchema(BaseSchema):
    """
    Schema for hex grid to town centre boundary lookup table.

    This defines the relationship between hex grid cells and town centre boundaries,
    allowing aggregation of hex data to town centre level.
    Used for the 'econ_busyness_hex_towncentre_lookup' table.
    """

    hex_id: Series[str] = pa.Field(
        description="Unique identifier for the TfL hex grid cell"
    )
    tc_id: Series[int] = pa.Field(
        description=("Unique identifier for the town centre boundary that contains"
                     "this hex")
    )
    tc_name: Series[str] = pa.Field(
        description="Official name of the town centre"
    )


class HexBidLookupSchema(BaseSchema):
    """
    Schema for hex grid to BID boundary lookup table.

    This defines the relationship between hex grid cells and BID boundaries,
    allowing aggregation of hex data to BID level.
    Used for the 'econ_busyness_hex_bid_lookup' table.
    """

    hex_id: Series[str] = pa.Field(
        description="Unique identifier for the TfL hex grid cell"
    )
    bid_id: Series[int] = pa.Field(
        description="Unique identifier for the BID boundary that contains this hex"
    )
    bid_name: Series[str] = pa.Field(
        description="Official name of the Business Improvement District"
    )


class HexBespokeLookupSchema(BaseSchema):
    """
    Schema for hex grid to bespoke area boundary lookup table.

    This defines the relationship between hex grid cells and bespoke area boundaries,
    allowing aggregation of hex data to bespoke area level.
    Used for the 'econ_busyness_hex_bespoke_lookup' table.
    """

    hex_id: Series[str] = pa.Field(
        description="Unique identifier for the TfL hex grid cell"
    )
    bespoke_area_id: Series[int] = pa.Field(
        description="Unique identifier for the bespoke area that contains this hex"
    )
    name: Series[str] = pa.Field(
        description=("Name of the bespoke area (can be a project name or"
                     "area description)")
    )


# Function to validate raw BT hex data
def validate_bt_hex_raw(data: pd.DataFrame) -> pd.DataFrame:
    """
    Validate raw BT hex data from API against the schema.

    Args:
        data: DataFrame containing raw BT hex data directly from API

    Returns:
        Validated DataFrame if valid

    Raises:
        pa.errors.SchemaError: If data doesn't match the expected schema
    """
    try:
        return BTHexRawSchema.validate(data)
    except pa.errors.SchemaError as e:
        print(f"Validation error in raw BT hex data: {e}")
        # You may want to handle specific validation errors
        raise


# Function to validate transformed BT hex data
def validate_bt_hex_transformed(data: pd.DataFrame) -> pd.DataFrame:
    """
    Validate transformed BT hex data against the schema.

    Args:
        data: DataFrame containing transformed BT hex data

    Returns:
        Validated DataFrame if valid

    Raises:
        pa.errors.SchemaError: If data doesn't match the expected schema
    """
    try:
        return BTHexTransformedSchema.validate(data)
    except pa.errors.SchemaError as e:
        print(f"Validation error in transformed BT hex data: {e}")
        raise

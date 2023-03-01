import numpy as np
import pandas as pd
import pandera as pa

days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
times_of_day = ["Morning", "Noon", "Evening", "Night"]

# ================= define schemas for different file types =================
# define a base schema for all files
base = pa.DataFrameSchema(
    columns={
        "file_date": pa.Column(pa.DateTime, required=True, coerce=True, nullable=False),
        "file_name": pa.Column(pa.String, required=True, coerce=False, nullable=False),
        "scaled_volume": pa.Column(
            pa.Float,
            required=True,
            coerce=True,
            nullable=True,
            checks=pa.Check.greater_than(0),
        ),
        "loyalty_percentage": pa.Column(
            pa.Float,
            required=True,
            coerce=True,
            nullable=True,
            checks=pa.Check.greater_than(0),
        ),
        "dwell_time": pa.Column(
            pa.Float,
            required=True,
            coerce=True,
            nullable=True,
            checks=pa.Check.greater_than(0),
        ),
    },
)

# add columns to the schema for the hex daily files
hex_daily = base.add_columns(
    {
        "hex_grid_id": pa.Column(pa.Int, required=True, coerce=False, nullable=False),
        "time_indicator": pa.Column(
            pa.Int,
            required=True,
            coerce=False,
            nullable=False,
            checks=pa.Check.in_range(0, 23),
        ),
        "date": pa.Column(pa.DateTime, required=True, coerce=True, nullable=False),
    }
)

# add columns to the schema for the hex monthly files
hex_monthly = base.add_columns(
    {
        "hex_grid_id": pa.Column(pa.Int, required=True, coerce=False, nullable=False),
        "month": pa.Column(pa.DateTime, required=True, coerce=False, nullable=False),
        "day_name": pa.Column(
            pa.Category,
            required=True,
            coerce=False,
            nullable=False,
            checks=pa.Check.isin(days),
        ),
        "time_indicator": pa.Column(
            pa.Int,
            required=True,
            coerce=False,
            nullable=False,
            checks=pa.Check.in_range(0, 23),
        ),
    }
)

oa_daily = base.add_columns(
    {
        "date": pa.Column(pa.DateTime, required=True, coerce=True, nullable=False),
        "time_indicator": pa.Column(
            pa.Category,
            required=True,
            coerce=False,
            nullable=False,
            checks=pa.Check.isin(times_of_day),
        ),
        "worker_population_percentage": pa.Column(
            pa.Float,
            required=True,
            coerce=True,
            nullable=True,
            checks=pa.Check.greater_than(0),
        ),
        "resident_population_percentage": pa.Column(
            pa.Float,
            required=True,
            coerce=True,
            nullable=True,
            checks=pa.Check.greater_than(0),
        ),
    }
)

oa_monthly = base.add_columns(
    {
        "month": pa.Column(pa.DateTime, required=True, coerce=True, nullable=False),
        "day_name": pa.Column(
            pa.Category,
            required=True,
            coerce=False,
            nullable=False,
            checks=pa.Check.isin(days),
        ),
        "time_indicator": pa.Column(
            pa.Category,
            required=True,
            coerce=False,
            nullable=False,
            checks=pa.Check.isin(times_of_day),
        ),
        "worker_population_percentage": pa.Column(
            pa.Float,
            required=True,
            coerce=True,
            nullable=True,
            checks=pa.Check.greater_than(0),
        ),
        "resident_population_percentage": pa.Column(
            pa.Float,
            required=True,
            coerce=True,
            nullable=True,
            checks=pa.Check.greater_than(0),
        ),
    }
)

lsoa_daily = oa_daily.add_columns(
    {
        "lsoa_id": pa.Column(
            pa.String, required=True, coerce=False, nullable=False, regex=True
        ),
    }
)
msoa_daily = oa_daily.add_columns(
    {
        "msoa_id": pa.Column(
            pa.String, required=True, coerce=False, nullable=False, regex=True
        ),
    }
)

lsoa_monthly = oa_monthly.add_columns(
    {
        "lsoa_id": pa.Column(
            pa.String, required=True, coerce=False, nullable=False, regex=True
        ),
    }
)
msoa_monthly = oa_monthly.add_columns(
    {
        "msoa_id": pa.Column(
            pa.String, required=True, coerce=False, nullable=False, regex=True
        ),
    }
)


# ================ define cleaning functions for different file types ===============
# these functions mainly coerce data types and add a file_date column
# as well as replacing certain strings with NaNs
# before validating the dataframe against the relevant schema


def clean_base(df, file_date, file_name):
    # replace 'IDE' with NaN and coerce to floats
    df["loyalty_percentage"] = (
        df["loyalty_percentage"].replace("IDE", np.nan).astype(float)
    )
    df["scaled_volume"] = df["scaled_volume"].replace("IDE", np.nan).astype(float)
    df["dwell_time"] = df["dwell_time"].replace("IDE", np.nan).astype(float)

    # add date of file from which the data was extracted
    df["file_date"] = file_date

    # add file_name to the dataframe
    df["file_name"] = file_name

    return df


def clean_hex_daily(df, file_date, file_name):
    df = clean_base(df, file_date, file_name)

    # convert hex_grid_id to int
    df["hex_grid_id"] = df["hex_grid_id"].astype(int)
    df["time_indicator"] = df["time_indicator"].str[:2].astype(int)

    # validate the dataframe against the schema
    hex_daily.validate(df)

    return df


def clean_hex_monthly(df, file_date, file_name):
    df = clean_base(df, file_date, file_name)

    # convert data types
    df["month"] = pd.to_datetime(df["month"])
    df["day_name"] = df["day_name"].astype("category")
    df["hex_grid_id"] = df["hex_grid_id"].astype(int)
    df["time_indicator"] = df["time_indicator"].str[:2].astype(int)

    # validate the dataframe against the schema
    hex_monthly.validate(df)

    return df


def clean_lsoa_daily(df, file_date, file_name):
    df = clean_base(df, file_date, file_name)

    # convert data types
    df["date"] = pd.to_datetime(df["date"])
    df["time_indicator"] = df["time_indicator"].astype("category")
    df["worker_population_percentage"] = (
        df["worker_population_percentage"].replace("IDE", np.nan).astype(float)
    )
    df["resident_population_percentage"] = (
        df["resident_population_percentage"].replace("IDE", np.nan).astype(float)
    )

    # validate the dataframe against the schema
    lsoa_daily.validate(df)

    return df


def clean_lsoa_monthly(df, file_date, file_name):
    df = clean_base(df, file_date, file_name)

    # convert data types
    df["month"] = pd.to_datetime(df["month"])
    df["day_name"] = df["day_name"].astype("category")
    df["time_indicator"] = df["time_indicator"].astype("category")
    df["worker_population_percentage"] = (
        df["worker_population_percentage"].replace("IDE", np.nan).astype(float)
    )
    df["resident_population_percentage"] = (
        df["resident_population_percentage"].replace("IDE", np.nan).astype(float)
    )

    # validate the dataframe against the schema
    lsoa_monthly.validate(df)

    return df


def clean_msoa_daily(df, file_date, file_name):
    df = clean_base(df, file_date, file_name)

    # convert data types
    df["date"] = pd.to_datetime(df["date"])
    df["time_indicator"] = df["time_indicator"].astype("category")
    df["worker_population_percentage"] = (
        df["worker_population_percentage"].replace("IDE", np.nan).astype(float)
    )
    df["resident_population_percentage"] = (
        df["resident_population_percentage"].replace("IDE", np.nan).astype(float)
    )

    # validate the dataframe against the schema
    msoa_daily.validate(df)

    return df


def clean_msoa_monthly(df, file_date, file_name):
    df = clean_base(df, file_date, file_name)

    # convert data types
    df["month"] = pd.to_datetime(df["month"])
    df["day_name"] = df["day_name"].astype("category")
    df["time_indicator"] = df["time_indicator"].astype("category")
    df["worker_population_percentage"] = (
        df["worker_population_percentage"].replace("IDE", np.nan).astype(float)
    )
    df["resident_population_percentage"] = (
        df["resident_population_percentage"].replace("IDE", np.nan).astype(float)
    )

    # validate the dataframe against the schema
    msoa_monthly.validate(df)

    return df

# standard library
import os

import pandas as pd
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

DATA_PATH = os.environ.get("DATA_PATH")
YOY_FILE = os.environ.get("YOY_FILE")
PROFILE_FILE = os.environ.get("PROFILE_FILE")
TC_LOOKUP = os.environ.get("TC_LOOKUP")
PROJECT_ROOT = os.environ.get("PROJECT_ROOT")


def main():
    """Runs data processing scripts to turn raw data from (../raw) into
    cleaned data ready to be analyzed (saved in ../processed).
    """
    print(DATA_PATH)
    print("year over year file: ", YOY_FILE)
    print("profile file: ", PROFILE_FILE)


def avg_retail_wd_we(df, spend_col_prefix=""):
    """Averages retail spending between weekend and weekday

    :param df: pandas dataframe produced by loading processed yoy
    Mastercard data
    :type df: pandas dataframe
    :param spend_col_prefix: prefix to the spend columns ('yoy_' for yoy
    spend files), defaults to ''
    :type spend_col_prefix: str, optional
    :return: copy of df with a new column for the averaged spend
    :rtype: pandas dataframe
    """

    spend_categories = ["retail"]
    time_periods = ["we", "wd"]
    aggregation_groups = ["txn_amt", "txn_cnt"]

    agg_cols = [
        [
            spend_col_prefix + grp + "_" + tp + "_" + cat
            for cat in spend_categories
            for tp in time_periods
        ]
        for grp in aggregation_groups
    ]

    df_minimal = df.copy()[["week_start", "highstreet_id", "highstreet_name"]]
    for (i, col) in enumerate(aggregation_groups):
        df_minimal.loc[:, col] = df[agg_cols[i]].agg("mean", 1)

    df_minimal = df_minimal.rename(columns={"week_start": "period_start"})
    df_minimal = df_minimal.set_index("period_start", drop=False)

    return df_minimal


def stack_retail_we_wd(df, spend_col_prefix=""):
    """Stacks weekend and weekday retail spending on top of each other

    :param df: pandas dataframe produced by loading processed yoy
    Mastercard data
    :type df: pandas dataframe
    :param spend_col_prefix: prefix to the spend columns ('yoy_' for yoy
    spend files), defaults to ''
    :type spend_col_prefix: str, optional
    :return: copy of df with a new column for the averaged spend
    :rtype: pandas dataframe
    """

    time_periods = ["we", "wd"]
    aggregation_groups = ["txn_amt", "txn_cnt"]

    main_cols = ["week_start", "highstreet_id", "highstreet_name"]

    dfs_to_stack = []
    for tp in time_periods:
        cols_to_stack = [
            spend_col_prefix + grp + "_" + tp + "_retail" for grp in aggregation_groups
        ]
        df_temp = df[main_cols + cols_to_stack]
        df_temp = df_temp.rename(columns={"week_start": "period_start"})
        df_temp = df_temp.rename(
            columns=dict(zip(cols_to_stack, aggregation_groups, strict=True))
        )
        if "we" in tp:
            tvec = df_temp["period_start"] + pd.DateOffset(days=5)
            df_temp.loc[:, "we_wd"] = "we"
        else:
            tvec = df_temp["period_start"]
            df_temp.loc[:, "we_wd"] = "wd"

        df_temp.loc[:, "period_start"] = tvec
        dfs_to_stack.append(df_temp)

    df_minimal = pd.concat(dfs_to_stack)
    df_minimal = df_minimal.set_index("period_start", drop=False)
    df_minimal.sort_index(inplace=True)

    return df_minimal


def extract_data_array(hsd_long_format, dates, column):
    """Extract hsd spend data array (with shape N_highstreets x N_weeks)
        and time vector (with shape N_weeks x 1) for modelling

    :param hsd_wide_format: Mastercard data loaded in long format
    :type hsd_wide_format: pandas dataframe
    :param dates: Date range to extract data for
    :type dates: (str, str)
    :param column: column of the dataframe to extract into a
    numpy array
    :type column: str
    :return: Returns an array in wide format of just the specified
    column against time, along with a time vector and a list of
    highstreet ids
    :rtype: (numpy array, pandas series, list[int])
    """
    hsd_wide_format = hsd_long_format.pivot(
        index=["period_start"],
        columns=["highstreet_id", "highstreet_name"],
        values=[column],
    )

    # interpolate over missing values
    hsd_wide_format = hsd_wide_format.interpolate()

    # Drop any remaining missing vals
    hsd_wide_format = hsd_wide_format.dropna(how="any", axis="columns")

    hsd_wide_range = hsd_wide_format.loc[dates[0] : dates[1]]

    """
    capping yoy spend to no greater than 5, as per Amanda's decision
    in her analysis as part of the effort to reproduce her results
    """
    hsd_wide_range = hsd_wide_range.clip(upper=5)

    return hsd_wide_range


if __name__ == "__main__":

    print("Running make_dataset as main!")

    main()

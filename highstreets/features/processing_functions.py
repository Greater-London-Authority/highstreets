import pandas as pd
from sklearn.linear_model import LinearRegression


# create function to take averages of every x months
# and see which hs fall in which proportion x months after that
def create_labels(hs, label_month, comparison_start, comparison_end):
    """Labels high streets at risk, depending on whether
    spending is in which quartile of average of x months prior"""
    hs_latest = hs.loc[(hs["month_year"] == label_month)]
    hs_latest = hs_latest.groupby(["highstreet_name"]).mean()
    # turn comp start and comp end variables into dates and find sequence btween
    comparison_start = pd.to_datetime(comparison_start)
    comparison_end = pd.to_datetime(comparison_end)
    date_range = pd.date_range(comparison_start, comparison_end, freq="m").to_period(
        "M"
    )
    hs_compare = hs.loc[(hs["month_year"]).isin(date_range)]
    hs_compare = hs_compare.groupby(["highstreet_name"]).mean()

    # merge dataframe
    merged_df = pd.merge(hs_latest, hs_compare, on="highstreet_name")
    # condtional labelling, if latest value not at least at average level, label at risk
    merged_df["proportion"] = (
        merged_df["txn_amt_wd_retail_x"] / merged_df["txn_amt_wd_retail_y"]
    )
    merged_df["labels"] = pd.qcut(
        merged_df["txn_amt_wd_retail_x"] / merged_df["txn_amt_wd_retail_y"],
        4,
        labels=[0, 1, 2, 3],
    )
    merged_df = merged_df.reset_index()
    # subset relevant columns
    merged_df = merged_df[["labels"]]
    return merged_df


# loop and select data for relevant months
def create_mean_sd_o2(highstreet_df, predictor_days):
    df = []
    for days in predictor_days:
        df.append(highstreet_df.loc[(highstreet_df["count_date"] == days)])
    df = pd.concat(df)

    # group to create mean and std across all months
    mean = df.groupby(["highstreet_name"]).mean().reset_index()
    # rename and merge so less possibility of mistakes
    mean = mean.rename({"h13": "mean_o2"}, axis=1)
    std = df.groupby(["highstreet_name"]).std().reset_index()
    std = std.rename({"h13": "std_o2"}, axis=1)
    # merge and select columns
    features = pd.merge(mean, std, on="highstreet_name")[
        ["highstreet_name", "mean_o2", "std_o2"]
    ]
    return features

    # loop and select data for relevant months


def create_mean_sd_mcard(highstreet_df, predictor_months):
    df = []
    for months in predictor_months:
        df.append(highstreet_df.loc[(highstreet_df["month_year"] == months)])
    df = pd.concat(df)

    # group to create mean and std across all months
    mean = df.groupby(["highstreet_name"]).mean().reset_index()
    # rename and merge so less possibility of mistakes
    mean = mean.rename({"txn_amt_wd_retail": "mean"}, axis=1)
    std = df.groupby(["highstreet_name"]).std().reset_index()
    std = std.rename({"txn_amt_wd_retail": "std"}, axis=1)
    # merge and select columns
    features = pd.merge(mean, std, on="highstreet_name")[
        ["highstreet_name", "mean", "std"]
    ]
    return features


def create_gradient(hs, months):
    """Use linear regression to calculate gradient of each high street
    over the specified range of months"""
    df = []
    # create df of 3 months
    for month in months:
        df.append(hs.loc[(hs["month_year"] == month)])
    df = pd.concat(df)
    # craete ordinal time data
    df["date_ordinal"] = pd.to_datetime(df["week_start"]).apply(
        lambda date: date.toordinal()
    )

    # create list of df grouped by highstreet name, ordered
    df_grad = []
    names = []
    for highstreet, subdf in df.groupby(["highstreet_name"], sort=True):

        X = subdf["date_ordinal"].values.reshape(-1, 1)
        y = subdf["txn_amt_wd_retail"].values.reshape(-1, 1)
        linear_regressor = LinearRegression()
        # perform linear regression
        model = linear_regressor.fit(X, y)
        # make predictions
        gradient = model.coef_
        df_grad.append(gradient)
        names.append(highstreet)

    # flatten and create dataframe
    flat_list = [item for sublist in df_grad for item in sublist]
    flat = [item for sublist in flat_list for item in sublist]

    # create df
    gradients = pd.DataFrame(
        list(zip(names, flat, strict=True)), columns=["highstreet_name", "gradient"]
    )
    return gradients


def create_gradient_o2(hs, months):
    """Use linear regression to calculate gradient of each high street
    over the specified range of months"""
    df = []
    # create df of 3 months
    for month in months:
        df.append(hs.loc[(hs["count_date"] == month)])
    df = pd.concat(df)
    # craete ordinal time data
    df["date_ordinal"] = pd.to_datetime(df["count_date"]).apply(
        lambda date: date.toordinal()
    )

    # create list of df grouped by highstreet name, ordered
    df_grad = []
    names = []
    for highstreet, subdf in df.groupby(["highstreet_name"], sort=True):

        X = subdf["date_ordinal"].values.reshape(-1, 1)
        y = subdf["h13"].values.reshape(-1, 1)
        linear_regressor = LinearRegression()
        # perform linear regression
        model = linear_regressor.fit(X, y)
        # make predictions
        gradient = model.coef_
        df_grad.append(gradient)
        names.append(highstreet)

    # flatten and create dataframe
    flat_list = [item for sublist in df_grad for item in sublist]
    flat = [item for sublist in flat_list for item in sublist]

    # create df
    gradients = pd.DataFrame(
        list(zip(names, flat, strict=True)), columns=["highstreet_name", "gradient"]
    )
    return gradients

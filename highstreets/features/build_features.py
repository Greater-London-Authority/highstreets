import os
from itertools import product

import numpy as np
import pandas as pd
from dotenv import find_dotenv, load_dotenv
from scipy import stats as spstat
from sklearn.linear_model import HuberRegressor, LinearRegression
from sklearn.multioutput import MultiOutputRegressor

load_dotenv(find_dotenv())

YOY_FILE = os.environ.get("YOY_FILE")
O2_CLUSTERS = os.environ.get("O2_CLUSTERS")


def clean_hs_profiles(stats):
    """Cleans high street profile data

    :param stats: A dataframe in the format returned by
    append_profile_features
    :type stats: Pandas DataFrame
    """

    # These are the categories assigned by clustering algorithms previously
    # run.
    stats = stats.drop(
        columns=["pattern_2020", "pattern_2021", "all_pandemic", "RECODE"],
    )

    # stats = stats.dropna(axis=0, how="any")

    stats["pct offices"] = (
        stats["total_number_of_offices"] / stats["total_number_of_addresses"]
    )

    stats["pct JSA 2021"] = (
        stats["total number of job seekers allownace 2021"] / stats["TotalPopulation"]
    )

    stats["pct HW"] = (
        stats["total estimated number of home workers"] / stats["TotalPopulation"]
    )

    stats["pct work age"] = stats["WorkAgePopulation"] / stats["TotalPopulation"]

    stats["pct employees"] = stats["Employees"] / stats["TotalPopulation"]

    drop_cols = [
        "total_number_of_residential_addresses",
        "total_number_of_commercial_addresses",
        "total_number_of_offices",
        "total number of job seekers allownace 2021",
        "total estimated number of home workers",
        "WorkAgePopulation",
        "Employees",
        "Sum_y2021_10wd",
        "Cnt_highstreet",
        "pc_chng",
        "LOAC2011_Group_Name (Rank 2)",
        "LOAC2011_Group_Name (Rank 3)",
        "PTAL2015 (Rank 2)",
        "PTAL2015 (Rank 3)",
        "loac2011 percentage of overlap % (Rank 1)",
        "loac2011 percentage of overlap % (Rank 2)",
        "loac2011 percentage of overlap % (Rank 3)",
        "ptal2015 percentage of overlap %(Rank 1)",
        "ptal2015 percentage of overlap% (Rank 2)",
        "ptal2015 percentage of overlap%(Rank 3)",
    ]

    stats = stats.rename(
        columns={
            "percentage of commercial addresses (%)": "pct commercial addresses",
            "precentage of residential addresses (%)": "pct residential addresses",
            "LOAC2011_Group_Name (Rank 1)": "loac rank 1",
            "PTAL2015 (Rank 1)": "ptal",
            "Sum_y2019_07wd": "2019 scale",
            "TotalPopulation": "Pop",
            "total_number_of_addresses": "num_addresses",
        }
    )

    # log transform skewed features
    skewed_features = [
        "pct commercial addresses",
        "Pop",
        "2019 scale",
        "pct offices",
        "pct HW",
        "pct employees",
    ]
    stats[["log_" + x for x in skewed_features]] = np.log(stats[skewed_features])

    drop_cols = drop_cols + skewed_features
    stats = stats.drop(columns=drop_cols)

    return stats


def add_split_group_vals(data, n_grp=4, split_cols=("mean 2020", "slope 2020")):

    # sort by the first column
    data = data.sort_values(by=[split_cols[0]])

    # then split by the sorted column
    # and simultaneously sort by the second within each split
    splits1 = [x.sort_values(by=[split_cols[1]]) for x in np.array_split(data, n_grp)]

    # split by the second column
    splits_both = [np.array_split(y, n_grp) for y in splits1]

    for (grp, (i, j)) in enumerate(product(range(n_grp), range(n_grp))):

        data.loc[splits_both[i][j].index, "group_" + split_cols[0]] = splits_both[i][j][
            split_cols[0]
        ].mean()

        data.loc[splits_both[i][j].index, "group_" + split_cols[1]] = splits_both[i][j][
            split_cols[1]
        ].mean()

        data.loc[
            splits_both[i][j].index,
            "group_num",
        ] = grp

    return data


def append_profile_features(hsp, data, reg_model):

    hsd_yoy = pd.read_csv(YOY_FILE, parse_dates=["week_start"])
    hs_o2_clusters = pd.read_csv(O2_CLUSTERS)

    means_2020 = (
        data.loc["2020-03-14":"2020-11-01", :]
        .mean()
        .unstack(level=0)
        .droplevel("highstreet_name")
        .rename(columns={"txn_amt": "mean 2020"})
    )

    slopes_2020 = reg_model["2020"].coef_
    slopes_2021 = reg_model["2021"].coef_

    means_2021 = (
        data.loc["2021-03-14":"2021-11-01", :]
        .mean()
        .unstack(level=0)
        .droplevel("highstreet_name")
        .rename(columns={"txn_amt": "mean 2021"})
    )

    hit_percent_2020 = (
        data.loc["2020-03-24":"2020-06-15"].mean()
        / data.loc["2020-01-04":"2020-03-24"].mean()
    )

    hit_percent_2021 = (
        data.loc["2021-01-05":"2021-03-12"].mean()
        / data.loc["2020-08-01":"2020-11-05"].mean()
    )

    stats = means_2020.join(means_2021)

    stats = stats.join(
        hit_percent_2020.rename("hit percent 2020").droplevel([0, "highstreet_name"])
    )

    stats = stats.join(
        hit_percent_2021.rename("hit percent 2021").droplevel([0, "highstreet_name"])
    )

    spend_by_sector = (
        hsd_yoy.groupby("highstreet_id").sum().filter(regex=(".*txn_amt.*")).dropna()
    )

    spend_by_sector["total"] = spend_by_sector.sum(axis=1)
    spend_by_sector["total_we"] = spend_by_sector.filter(regex=(".*we.*")).sum(axis=1)
    spend_by_sector["total_wd"] = spend_by_sector.filter(regex=(".*wd.*")).sum(axis=1)

    for sector in ["eating", "retail", "apparel"]:
        spend_by_sector[f"percent_{sector}"] = (
            spend_by_sector[f"yoy_txn_amt_wd_{sector}"]
            + spend_by_sector[f"yoy_txn_amt_we_{sector}"]
        ) / spend_by_sector["total"]

    for period in ["we", "wd"]:
        spend_by_sector[f"percent_{period}"] = (
            spend_by_sector[f"total_{period}"]
        ) / spend_by_sector["total"]

    spend_by_sector = spend_by_sector[
        [
            "percent_eating",
            "percent_apparel",
            "percent_retail",
            "percent_we",
            "percent_wd",
        ]
    ]

    stats = stats.join(spend_by_sector)

    stats["slope 2020"] = [x[0] for x in slopes_2020]
    stats["slope 2021"] = [x[0] for x in slopes_2021]

    hs_o2_clusters.set_index("highstreet_id")
    stats = stats.join(
        hs_o2_clusters[["cluster_hourly", "cluster_daily", "cluster_size"]],
        on="highstreet_id",
    )

    return stats.join(hsp.set_index("highstreet_id"), how="left")


def get_fit_lines(start_date, tvec, array_in, robust=False):
    t0 = pd.to_datetime(start_date)
    days_since_reopen = (tvec - t0).days.values

    X = days_since_reopen.reshape(-1, 1)
    y = np.transpose(array_in)
    if robust:
        reg = MultiOutputRegressor(HuberRegressor(epsilon=1.05)).fit(X, y)
    else:
        reg = LinearRegression().fit(X, y)

    return reg, reg.predict(X)


def group_highstreets(
    data,
    group_cols,
    n_grp,
    col_names=(
        "highstreet_id",
        "highstreet_name",
        "mrli_yoy_mean_2020_recovery",
        "mrli_fit_slope_2020_recovery",
        "overall_group",
        "group_1",
        "group_2",
    ),
    low_pct=10,
    high_pct=90,
):

    data_array = np.transpose(data.to_numpy())
    highstreet_ids = data.columns.get_level_values(1).to_numpy()[:, np.newaxis]

    hs_id_name_lookup = dict(
        zip(
            data.columns.get_level_values(1),
            data.columns.get_level_values(2),
            strict=True,
        )
    )

    # prepend columns by which highstreets will be sorted and grouped for plotting
    array_w_sorting_cols = np.concatenate(
        (group_cols[0], group_cols[1], data_array, highstreet_ids), axis=1
    )

    # sort the array by the first column
    array_sorted_by_col_one = array_w_sorting_cols[array_w_sorting_cols[:, 0].argsort()]

    # split the indices into groups by the first column
    array_split_by_col_one = np.array_split(array_sorted_by_col_one, n_grp)

    # loop through groups sorting each by second column and splitting by second column
    highstreets_with_groups = []
    group_num = 1
    for i, group in enumerate(array_split_by_col_one):
        group_sorted_by_col_two = group[group[:, 1].argsort()]
        group_split_by_col_two = np.array_split(group_sorted_by_col_two, n_grp)
        for j, subgroup in enumerate(group_split_by_col_two):
            subgroup = subgroup[subgroup[:, 1].argsort()]
            hs_ids = subgroup[:, -1]
            hs_names = [hs_id_name_lookup.get(key) for key in hs_ids]
            for k, hs_name in enumerate(hs_names):
                highstreets_with_groups.append(
                    [
                        hs_ids[k],  # HS id
                        hs_name,  # HS name
                        subgroup[k, 0],  # quantity 1 grp avg
                        subgroup[k, 1],  # quantity 2 grp avg
                        group_num,  # group number
                        i + 1,  # row
                        j + 1,
                    ]  # column
                )

            group_num += 1

    highstreets_by_group = pd.DataFrame(
        highstreets_with_groups,
        columns=col_names,
    ).astype({"highstreet_id": "int64"})

    return highstreets_by_group


def hist2d_highstreets(
    data,
    n_grp=4,
    group_cols=("mean 2020", "slope 2020"),
    low_pct=10,
    high_pct=90,
    rcg_names=("row", "column", "group"),
):

    bin_one = np.linspace(
        np.percentile(data[group_cols[0]], low_pct),
        np.percentile(data[group_cols[0]], high_pct),
        n_grp + 1,
    )
    bin_two = np.linspace(
        np.percentile(data[group_cols[1]], low_pct),
        np.percentile(data[group_cols[1]], high_pct),
        n_grp + 1,
    )

    ret = spstat.binned_statistic_2d(
        data[group_cols[0]].squeeze(),
        data[group_cols[1]].squeeze(),
        None,
        "count",
        bins=[bin_one, bin_two],
        expand_binnumbers=True,
    )

    data[rcg_names] = None

    for i in np.unique(ret.binnumber[0, :]):
        data.loc[ret.binnumber[0, :] == i, rcg_names[0]] = i
    for j in np.unique(ret.binnumber[1, :]):
        data.loc[ret.binnumber[1, :] == j, rcg_names[1]] = j

    data[rcg_names[2]] = data[rcg_names[0]] * (n_grp + 2) + data[rcg_names[1]]

    idx_split = np.array_split(range(data.shape[0]), n_grp)

    for gc in group_cols:
        for rg in rcg_names[:-1]:
            data = data.sort_values(by=gc, axis=0)
            data[rg + "_even"] = None
            for j, idx in enumerate(idx_split):
                data[rg + "_even"].iloc[idx] = j

    data[rcg_names[2] + "_even"] = (
        data[rcg_names[0] + "_even"] * n_grp + data[rcg_names[1] + "_even"]
    )

    return data


# def extract_extra_features(hsd_yoy):

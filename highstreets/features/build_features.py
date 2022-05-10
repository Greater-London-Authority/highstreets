from itertools import product

import numpy as np


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

    # log transform skewed features
    skewed_features = [
        "total_number_of_addresses",
    ]
    stats[["log_" + x for x in skewed_features]] = np.log(stats[skewed_features])

    drop_cols = drop_cols + skewed_features
    stats = stats.drop(columns=drop_cols)

    stats = stats.rename(
        columns={
            "percentage of commercial addresses (%)": "pct commercial addresses",
            "precentage of residential addresses (%)": "pct residential addresses",
            "LOAC2011_Group_Name (Rank 1)": "loac rank 1",
            "PTAL2015 (Rank 1)": "ptal",
            "Sum_y2019_07wd": "2019 scale",
            "TotalPopulation": "Pop",
            "log_total_number_of_addresses": "log_num_addresses",
        }
    )

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

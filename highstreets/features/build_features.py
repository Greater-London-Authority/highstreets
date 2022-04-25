import numpy as np


def clean_hs_profiles(stats):
    """Cleans high street profile data

    :param stats: A dataframe in the format returned by
    append_profile_features
    :type stats: Pandas DataFrame
    """
    stats = stats.drop(
        columns=["pattern_2020", "pattern_2021", "all_pandemic", "RECODE"],
    )

    stats = stats.dropna(axis=0, how="any")

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
    ]

    # log transform skewed features
    skewed_features = [
        "total_number_of_addresses",
    ]
    stats[[x for x in skewed_features]] = np.log(stats[skewed_features])

    drop_cols = drop_cols + skewed_features

    stats = stats.drop(columns=drop_cols)

    return stats

import os

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from dotenv import find_dotenv, load_dotenv
from matplotlib.backends.backend_pdf import PdfPages
from scipy import stats as spstat

load_dotenv(find_dotenv())

YOY_FILE = os.environ.get("YOY_FILE")
PROFILE_FILE = os.environ.get("PROFILE_FILE")
PROJECT_ROOT = os.environ.get("PROJECT_ROOT")


def plot_all_profiles_full(data, fit_lines):
    """Plots each highstreet's yoy spending along with
    line segments fit to the 2020 and 2021 recovery periods

    :param data: A dict with keys '2020','2021','full',
    where each value is a pandas dataframe with one
    column per highstreet, each covering the corresponding
    period (2020, 2021, full period)
    :type data: Dict of three pandas dataframes
    :param fit_lines: A dict with keys '2020','2021','full', containing
    numpy arrays of size T_period x N_highstreets, each column being the
    values of a line fit to the corresponding period's data for each
    highstreet.
    :type fit_lines: Dict of numpy arrays
    """
    nrows = 6
    ncols = 3
    colors = sns.color_palette()

    nb_dates = pd.to_datetime(
        [
            "2020-03-24",
            "2020-06-15",
            "2020-09-22",
            "2020-11-05",
            "2020-12-02",
            "2021-01-05",
            "2021-04-12",
        ]
    )

    num_hs = len(data["2020"].columns)
    print("Number of highstreets: ", num_hs)
    plots_per_page = ncols * nrows

    fig, axes = plt.subplots(nrows, ncols, figsize=(12, 15), sharey=False)
    axes_flat = axes.reshape(-1)
    sns.set(font_scale=0.75)
    current_plot = 1
    page = 1
    print("page: ", page)

    with PdfPages(
        PROJECT_ROOT + "/reports/figures/hs_profiles_w_linear_fit.pdf"
    ) as pdf:

        for hs in range(num_hs):

            if current_plot > plots_per_page:
                # if we've reached the end of the page, print, and close the figure
                pdf.savefig()
                plt.close(fig)
                page = page + 1
                print("page: ", page)

                # make a new figure and set of axes
                fig, axes = plt.subplots(nrows, ncols, figsize=(12, 15), sharey=False)
                axes_flat = axes.reshape(-1)

                current_plot = 1

            ax = axes_flat[current_plot - 1]
            row = (current_plot - 1) // ncols

            # plot full time series along with fits
            ax.plot(data["full"].index, data["full"].iloc[:, hs], color=colors[0])
            ax.plot(data["2020"].index, fit_lines["2020"][:, hs], color=colors[1])
            ax.plot(data["2021"].index, fit_lines["2021"][:, hs], color=colors[1])

            if row == nrows - 1:
                ax.tick_params(axis="x", labelrotation=30)
            else:
                ax.set_xticklabels([])

            # extract Highstreet name and ID for axis title
            current_hs = data["full"].iloc[:, hs].name[2]
            hs_id = data["full"].iloc[:, hs].name[1]

            ax.set_title(current_hs + ", id: " + str(hs_id))
            yl = ax.get_ylim()
            ax.plot([nb_dates, nb_dates], yl, "--k", linewidth=1)

            ax.grid(b=True, which="major", color="white", linewidth=0.8)
            ax.get_xaxis().set_minor_locator(mpl.ticker.AutoMinorLocator(3))
            ax.grid(b=True, which="minor", color="white", linewidth=0.1)

            current_plot = current_plot + 1


def plot_highstreets_grouped(
    plot_array,
    plot_tvec,
    sort_cols,
    nb_dates,
    filename,
    xlim=("2020-01-01", "2021-12-31"),
    figure_title="Highstreet profiles grouped",
    n_grp=4,
    equal_hs_per_bin=True,
    low_pct=5,
    high_pct=90,
):
    """_summary_

    :param plot_array: _description_
    :type plot_array: _type_
    :param plot_tvec: _description_
    :type plot_tvec: _type_
    :param sort_cols: _description_
    :type sort_cols: _type_
    :param nb_dates: _description_
    :type nb_dates: _type_
    :param filename: _description_
    :type filename: _type_
    :param xlim: _description_, defaults to ('2020-01-01','2020-12-31')
    :type xlim: tuple, optional
    """

    sns.set_theme(style="darkgrid")

    fig, axes = plt.subplots(
        n_grp,
        n_grp,
        figsize=(14, 14),
        sharey=True,
        sharex=True,
    )

    xticks = pd.to_datetime(
        [
            "2020-02",
            "2020-04",
            "2020-06",
            "2020-08",
            "2020-10",
            "2020-12",
            "2021-02",
            "2021-04",
            "2021-06",
            "2021-08",
        ]
    )
    xticklabels = [
        "Feb 20",
        "Apr 20",
        "Jun 20",
        "Aug 20",
        "Oct 20",
        "Dec 20",
        "Feb 21",
        "Apr 21",
        "Jun 21",
        "Aug 21",
    ]

    # prepend columns by which highstreets will be sorted and grouped for plotting
    array_w_sorting_cols = np.concatenate(
        (sort_cols[0], sort_cols[1], plot_array), axis=1
    )

    # sort the array by the first column
    array_sorted_by_col_one = array_w_sorting_cols[array_w_sorting_cols[:, 0].argsort()]

    # split the indices into groups by the first column
    array_split_by_col_one = np.array_split(array_sorted_by_col_one, n_grp)

    # Here we are either splitting highstreets into bins
    # with roughly equal numbers of highstreets per bin
    # (if equal_hs_per_bin is True),
    # or else we are splitting the range of the two sort cols
    # into equally spaced bins
    if equal_hs_per_bin:
        # loop through groups sorting each by second column
        # and splitting by second column
        group_number = 1
        for i, group in enumerate(array_split_by_col_one):
            group_sorted_by_col_two = group[group[:, 1].argsort()]
            group_split_by_col_two = np.array_split(group_sorted_by_col_two, n_grp)
            for j, subgroup in enumerate(group_split_by_col_two):
                subgroup = subgroup[subgroup[:, 1].argsort()]
                axes[i][j].plot(
                    plot_tvec, np.transpose(subgroup[:, 2:]), "0.7", linewidth=1
                )
                axes[i][j].plot((plot_tvec[0], plot_tvec[-1]), (1, 1), "0.0")
                axes[i][j].plot(plot_tvec, subgroup[:, 2:].mean(0), "b", linewidth=2)
                axes[i][j].set_ylim([0, 4])
                axes[i][j].plot([nb_dates, nb_dates], [0, 5], "--k", linewidth=0.5)
                axes[i][j].set_xticks(xticks)
                axes[i][j].set_xticklabels(xticklabels, rotation=45)
                axes[i][j].set_xlim(pd.to_datetime(xlim))
                axes[i][j].set_title(group_number)
                group_number += 1

    else:
        # define bin ends for sorting columns

        plot_tvec = np.transpose(plot_tvec)

        # create bins spanning the percentiles from low_pct to high_pct
        # of the range of each variable of interest
        bin_one = np.linspace(
            np.percentile(sort_cols[0], low_pct),
            np.percentile(sort_cols[0], high_pct),
            n_grp + 1,
        )
        bin_two = np.linspace(
            np.percentile(sort_cols[1], low_pct),
            np.percentile(sort_cols[1], high_pct),
            n_grp + 1,
        )

        # create 2d histogram returning an object which includes
        # the bin numbers for each highstreet
        ret = spstat.binned_statistic_2d(
            sort_cols[0].squeeze(),
            sort_cols[1].squeeze(),
            None,
            "count",
            bins=[bin_one, bin_two],
            expand_binnumbers=True,
        )

        group_number = 1
        for i in range(1, n_grp + 1):
            for j in range(1, n_grp + 1):
                plot_subgroup = np.transpose(
                    array_w_sorting_cols[
                        np.logical_and(
                            ret.binnumber[0, :] == i, ret.binnumber[1, :] == j
                        ),
                        2:,
                    ]
                )

                group_means = np.mean(
                    array_w_sorting_cols[
                        np.logical_and(
                            ret.binnumber[0, :] == i, ret.binnumber[1, :] == j
                        ),
                        0:2,
                    ],
                    axis=0,
                )

                axes[i - 1][j - 1].plot(
                    plot_tvec,
                    plot_subgroup,
                    "0.7",
                    linewidth=1,
                )
                axes[i - 1][j - 1].plot((plot_tvec[0], plot_tvec[-1]), (1, 1), "0.0")
                axes[i - 1][j - 1].plot(
                    plot_tvec, plot_subgroup.mean(1), "b", linewidth=2
                )
                axes[i - 1][j - 1].set_ylim([0, 4])
                axes[i - 1][j - 1].plot(
                    [nb_dates, nb_dates], [0, 5], "--k", linewidth=0.5
                )
                axes[i - 1][j - 1].set_xticks(xticks)
                axes[i - 1][j - 1].set_xticklabels(xticklabels, rotation=45)
                axes[i - 1][j - 1].set_xlim(pd.to_datetime(xlim))
                axes[i - 1][j - 1].set_title(
                    f"Group: {group_number: .0f},"
                    f"#: {ret.statistic[i-1][j-1]},"
                    f"({group_means[0]: .2f},{group_means[1]: .2f})"
                )
                group_number += 1

    axes[0][0].set_ylabel("MRLI relative to 2019")
    fig.suptitle(figure_title, fontsize=16, y=0.91)

    plt.savefig(PROJECT_ROOT + "/reports/figures/" + filename)

    if equal_hs_per_bin:
        return None
    else:
        return ret

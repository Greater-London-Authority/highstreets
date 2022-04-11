from matplotlib.backends.backend_pdf import PdfPages
import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv, find_dotenv
import os
import matplotlib as mpl
import numpy as np

load_dotenv(find_dotenv())

YOY_FILE  = os.environ.get("YOY_FILE")
PROFILE_FILE = os.environ.get("PROFILE_FILE")
PROJECT_ROOT = os.environ.get("PROJECT_ROOT")

def plot_all_profiles_full(data,fit_lines):

    nrows = 6
    ncols = 3
    colors = sns.color_palette()

    nb_dates = pd.to_datetime(['2020-03-24','2020-06-15','2020-09-22','2020-11-05','2020-12-02','2021-01-05','2021-04-12'])

    num_hs = len(data['2020'].columns)
    print('Number of highstreets: ', num_hs)
    plots_per_page = ncols*nrows

    fig, axes = plt.subplots(nrows, ncols,figsize=(12,15), sharey=False)
    axes_flat = axes.reshape(-1)
    sns.set(font_scale = 0.75)
    current_plot = 1
    page = 1
    print("page: ",page)

    with PdfPages(PROJECT_ROOT + '/reports/figures/hs_profiles_w_linear_fit.pdf') as pdf:
            
        for hs in range(num_hs):

            if current_plot > plots_per_page:
                # if we've reached the end of the page, print, and close the figure
                pdf.savefig()
                plt.close(fig)
                page = page + 1
                print("page: ",page)

                # make a new figure and set of axes
                fig, axes = plt.subplots(nrows, ncols,figsize=(12,15), sharey=False)
                axes_flat = axes.reshape(-1)

                current_plot = 1

            ax = axes_flat[current_plot-1]
            row = (current_plot-1) // ncols

            # plot full time series along with fits
            ax.plot(data['full'].index,data['full'].iloc[:,hs],color=colors[0])
            ax.plot(data['2020'].index,fit_lines['2020'][:,hs],color=colors[1])
            ax.plot(data['2021'].index,fit_lines['2021'][:,hs],color=colors[1])

            if row == nrows - 1: 
                ax.tick_params(axis='x', labelrotation = 30)
            else:
                ax.set_xticklabels([])
                
            # extract Highstreet name and ID for axis title
            current_hs = data['full'].iloc[:,hs].name[2]
            hs_id = data['full'].iloc[:,hs].name[1]

            ax.set_title(current_hs + ", id: " + str(hs_id))
            yl = ax.get_ylim()
            ax.plot([nb_dates, nb_dates],yl,'--k', linewidth=1)

            ax.grid(b=True, which='major', color='white', linewidth=0.8)
            ax.get_xaxis().set_minor_locator(mpl.ticker.AutoMinorLocator(3))
            ax.grid(b=True, which='minor', color='white', linewidth=0.1)

            current_plot = current_plot + 1



def plot_highstreets_grouped(plot_array, plot_tvec, sort_cols, nb_dates, filename):
    n_grp = 4

    _, axes = plt.subplots(n_grp, n_grp,figsize=(14,14), sharey=True, sharex=True)

    array_w_sorting_cols = np.concatenate((sort_cols[0][:,None],sort_cols[1],plot_array), axis=1)

    # sort the array by the first sorting column
    array_sorted_by_col_one = array_w_sorting_cols[array_w_sorting_cols[:,0].argsort()]

    # split the indices into groups by mean
    array_split_by_col_one = np.array_split(array_sorted_by_col_one,n_grp)

    hs_per_group = int(np.ceil(plot_array.shape[0] / (n_grp*n_grp)))
    colors = sns.color_palette("BuGn", n_colors = hs_per_group)

    # loop through groups sorting each by slope and splitting by slope
    axn=[]
    for i, group in enumerate(array_split_by_col_one):
        group_sorted_by_col_two = group[group[:,1].argsort()]
        group_split_by_col_two = np.array_split(group_sorted_by_col_two, n_grp)
        for j, subgroup in enumerate(group_split_by_col_two):
            subgroup = subgroup[subgroup[:,1].argsort()]
            #axes[i][j].set_prop_cycle(color=colors)
            axes[i][j].plot(plot_tvec, np.transpose(subgroup[:,2:]), '0.7',linewidth=1)
            axes[i][j].plot((plot_tvec[0],plot_tvec[-1]),(1,1),'0.0')
            axes[i][j].plot(plot_tvec, subgroup[:,2:].mean(0),'b', linewidth=2)
            axes[i][j].set_ylim([0,4])
            axes[i][j].plot([nb_dates, nb_dates],[0, 5],'--k', linewidth=0.5)
            axes[i][j].set_xticks(pd.to_datetime(['2020-02','2020-04','2020-06','2020-08','2020-10','2020-12']))
            axes[i][j].set_xticklabels(['Feb 20','Apr 20','Jun 20','Aug 20','Oct 20','Dec 20'], rotation=45)  
        axes[i][0].set_ylabel('MRLI relative to 2019')

    plt.savefig(PROJECT_ROOT + '/reports/figures/' + filename)
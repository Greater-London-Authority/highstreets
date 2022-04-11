from matplotlib.backends.backend_pdf import PdfPages
import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv, find_dotenv
import os
import matplotlib as mpl

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

![Banner](<banner.png>)
<!-- See https://github.com/rmariuzzo/github-banner -->

<div align="center">
<h1> London High Streets </h1>
<h4> Analysis and modeling of London high street profiles </h4>
</div>

<!-- <a href="#top">""</a> -->

---

Author: Conor Dempsey & Tabby Duenger



<p align="center">
  <a href="#how-to-use">How To Use</a> •
  <a href="#roadmap">Roadmap</a> •
  <a href="#contribute">Contribute</a> •
  <a href="#contact">Contact</a>
</p>

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

---

## How To Use
[(Back to top)](#how-to-use)

To clone and run you'll need [Git](https://git-scm.com) and [poetry](https://python-poetry.org/docs/master/#installing-with-the-official-installerl) installed.

```bash
# Clone this repository (you'll need your GlA github login - your username and a personal access token)
$ git clone https://github.com/Greater-London-Authority/highstreets

# Go into the repository
$ cd highstreets

# Install dependencies and create the environment for the project
$ poetry install

# Activate the virtual environment
$ poetry shell
```

A good place to start is notebooks/exploratory which contains Jupyter notebooks that demonstrate the analyses that have been done so far. Note: if you are editing the package files and you want these changes to be automatically registered in any Jupyter notebooks then put following command at the top of your notebook:

```
%load_ext autoreload
%autoreload 2
```

There are currently two data files needed to run these analyses. The paths to these files should be specified in a .env file in the project's root directory and can be obtained from the shared drives (contact Conor for more info).

* yoy_highstreets.csv
* highstreet_profiles_updated.xlsx


## Contribute
[(Back to top)](#how-to-use)

Contact Conor Dempsey to be added to the repo as a contributor.

If you are contributing to the repo please use pre-commit using the pre-commit-config.yaml included here.

To install pre-commit run:
```bash
pip install pre-commit
```

Then to set up the git hooks specified in the pre-commit-config.yaml file navigate to the repo and run:
```bash
pre-commit install
```

Now when you commit code various linters and other pre-commit checks will be run against your staged changes. All of these tests have to pass sucessfully before the commit will be accepted.


<!-- ROADMAP -->
## Roadmap
[(Back to top)](#how-to-use)

- [ ] Sense check the lack of correlation between mean/slope and size of highstreet - compare to data Paul shared.
- [ ] Try classification approaches where the labels are mean/slope groups.
- [ ] Run MoE models - using a hand-picked gating structure and then using a full MoE setup. Start with mixture of linear models.
- [ ] Depending on linear MoE results try other more nonlinear approaches - a small NN maybe?
- [ ] Look at other data that might be included if slope/mean grouping seems difficult to predict. O2 footfall data for example.
- [ ] Look at models that are less ad-hoc, in the sense that the fit parameters are not treated as a separate set of parameters to be fit and then treated as regression targets.
- [ ] Quantify/visualise/describe relationship between 2020 and 2021 parameters
- [ ] Visualise change of parameters from 2020 to 2021. Cluster highstreets based on the direction and magnitude of this change?
- [ ] Compare the results of our clustering with Amanda's, at a granular level, to see if the results are reasonably well aligned.
- [ ] Add pipeline to produce yoy data from raw data
- [x] Make ordered profile plots for 2020, 2021, full period, sorted by mean and fit slope
- [x] Add scripts to produce figures of all HSs w fits
- [x] Compare results of different clustering approaches (k-means on full time series, k-means on fit parameters, k-means with DTW, hierarchical clustering)
- [x] Sample from hierarchical regression models to see what features, if any, can predict differences in recovery profile.
- [x] Consider different methods for dealing with missing data

## Credit

A very useful primer on Bayesian hierarchical linear regressions can be found [here](https://docs.pymc.io/en/v3/pymc-examples/examples/case_studies/multilevel_modeling.html).

## Contact
[(Back to top)](#v)

Conor Dempsey - conor.dempsey@london.gov.uk

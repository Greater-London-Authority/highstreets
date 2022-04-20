![Banner](<banner.png>)
<!-- See https://github.com/rmariuzzo/github-banner -->

<div align="center">
<h1> London High Streets </h1>
<h4> Analysis and modeling of London high street profiles </h4>
</div>

---

Author: Conor Dempsey



<p align="center">
  <a href="#demo-preview">Preview</a> •
  <a href="#how-to-use">How To Use</a> •
  <a href="#roadmap">Roadmap</a> •
  <a href="#contribute">Contribute</a> •
  <a href="#contact">Contact</a>
</p>

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

---

## How To Use
[(Back to top)](#demo-preview)

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

A good place to start is notebooks/reports which contains Jupyter notebooks that demonstrate the analyses that have been done so far. Note: if you are editing the package files and you want these changes to be automatically registered in any Jupyter notebooks then put following command at the top of your notebook:

```
%load_ext autoreload
%autoreload 2
```

There are currently two data files needed to run these analyses. These should be placed in the data/raw subdirectory and can be obtained from the shared drives (contact Conor for more info).

* yoy_highstreets.csv
* highstreet_profiles_updated.xlsx


# Contribute
[(Back to top)](#demo-preview)

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
[(Back to top)](#demo-preview)

- [x] Make ordered profile plots for 2020, 2021, full period, sorted by mean and fit slope
- [ ] Quantify/visualisse/describe relationship between 2020 and 2021 parameters
- [x] Add scripts to produce figures of all HSs w fits
- [ ] Add script comparing results of different clustering approaches (k-means on full time series, k-means on fit parameters, k-means with DTW, hierarchical clustering)
- [ ] Produce figure showing progressive cluster breakdowns using hierarchical clustering
- [ ] Begin building regression models to see what features can predict differences in recovery profile.
- [ ] Produce analyses to understand sources of higher noise in some highstreets and not others.
- [ ] Add pipeline to produce yoy data from raw data
- [ ] Consider and document methods for dealing with missing data


# Contact
[(Back to top)](#demo-preview)

Conor Dempsey - conor.dempsey@london.gov.uk

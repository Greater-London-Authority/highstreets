<div align="center">
<h1> High Streets Research Notes </h1>
</div>
Author: Conor Dempsey

---

## The problem framing

Currently (as of April 2022) we are broadly approaching the High Streets mastercard data in the following two ways:

1. We are using unsupervised methods to try to discover structure in the dynamics of expenditure across different highstreets in London.
2. We are attempting to build supervised models that can predict the dynamics of expenditure in a given high street from other data about that high street.

## Assumptions / limitations

- Currently we are only looking at overall retail spending.
- Currently we are not dealing efficiently with missing data and outliers. We are capping data at a cutoff value to limit the influence of large fluctuations. We are discarding high streets with missing values.
- We have not carefully examined noise sources and different levels of noise nor taken these into account in models.


## Approaches tried:

### Unsupervised:

1. K-Means clustering directly on high street recovery profiles.
2. K-Means clustering using the DTW distance metric.
3. Fit a line to the recovery in 2020 or 2021 and perform k-means on the parameters fit to each high street.

#### Results so far:

1. For a given period: K-means returns clusters which largely sort the highstreets along a linear combination of the mean and slope.
2. K-Means and K-means with DTW return similar results
3. K-Means on fit line parameters produces different clustering (it clusters more based solely on the slope). However, it is worth noting that the clusters produced by k-means on the full data are cleanly separable in slope/mean space and so information available in slope/mean space is sufficient to reproduce the clustering produced using the full data.
4. In slope-mean space the clusters produced by k-means on the full data appear as linear divisions within a continuous density.

### Supervised:

1. Fit a line to the 2020 recovery period. Then, treat the parameters of the fit lines as targets for a regression using other data (which we call the high street profile).

Models we have used to try to predict these fit parameters from high street profiles:
1. Linear regression (regularised with ridge)
2. Huber regression to try to deal with outlying target values.
3. Support Vector Regression with polynomial and RBF kernels.

#### Results so far:

1. There are clear correlations between some of the high street profile features and also between fit parameters. For example, the means in 2020 and 2021 are highly correlated, as are the fit slopes.
2. Mean 2020 is weakly predictable from high street profiles.
3. Slope 2020 has so far been very difficult to predict.


#### Next steps:

- Try other regularisation schemes.
- High streets show a lot of variation by mean and slope of recovery. Focusing on 2020 data first, we can try grouping high streets by mean and best fit slope, and then treat these groups labels as targets for  classification.
- Further, we can check whether conditioning on one parameter (crudely by just partitioning the data by one parameter) allows the other parameter to be predicted. It would be good to understand the structure of such a model - is it equivalent to some probabilistic model?
- As a more general version of the above: we can try fitting mixture models. Linear mixed models being the starting point.
- Decision tree regressors.
- Ensemble methods.

### Bayesian

1. We have also tried using [NumPyro](https://num.pyro.ai/en/latest/index.html) to construct fully Bayesian models of high street profiles. So far we have a notebook where NumPyro is used to specify a simple linear model of each high streets 2020 recovery (linear in time that is). This model uses partial pooling where the means and intercepts for each high street are constrained to be draws from an overall group distribution. NumPyro allows us to make samples from the posterior distribution of such a model using MCMC methods.

#### Results so far:

1. The models work in the sense that the posterior means are reasonable 'fits' to the real data, and we can view posterior distributions for the population mean fit parameters.
2. There are some potential issues with the posterior geometry which should be explored further.

#### Next steps:

- Sample from a piecewise linear model of the full period.
- Construct a model where each high street has fit parameters given by a linear function of their high street profile features. The parameters of this linear function would be drawn from a group distribution.
- Think about ways we can rigorously test for latent group structure. Can we do a formal model comparison of different latent variable models?

## Other modeling approaches to try:


## Other notes:

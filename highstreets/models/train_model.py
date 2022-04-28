import numpy as np
import seaborn as sns
from matplotlib import pyplot as plt
from sklearn.inspection import permutation_importance
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import GridSearchCV
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


def run_experiment(model, X_train, X_test, y_train, y_test):
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    return {
        "model": model,
        "R2": r2_score(y_test, y_pred),
        "MAE": mean_absolute_error(y_test, y_pred),
        "RMSE": np.sqrt(mean_squared_error(y_test, y_pred)),
    }


def evaluate(model, X, y):
    y_pred = model.predict(X)
    return {
        "R2": r2_score(y, y_pred),
        "MAE": mean_absolute_error(y, y_pred),
        "MSE": np.sqrt(mean_squared_error(y, y_pred)),
    }


def run_experiment_w_cv(
    model,
    tuned_params,
    X_train,
    X_test,
    y_train,
    y_test,
    scoring="r2",
    verbose=0,
):

    model = Pipeline(
        [
            ("scaler", StandardScaler()),
            ("model", model),
        ]
    )

    model_cv = GridSearchCV(model, tuned_params, scoring=scoring, verbose=verbose)

    results_pred = run_experiment(model_cv, X_train, X_test, y_train, y_test)

    best_model = results_pred["model"].best_estimator_

    fig, axes = plt.subplots(2, 1, figsize=(15, 10))

    ind = y_train.values.argsort()
    sns.scatterplot(x=range(y_train.shape[0]), y=y_train.iloc[ind], ax=axes[0])
    sns.scatterplot(
        x=range(y_train.shape[0]), y=best_model.predict(X_train)[ind], ax=axes[0]
    )

    ind = y_test.values.argsort()
    sns.scatterplot(x=range(y_test.shape[0]), y=y_test.iloc[ind], ax=axes[1])
    sns.scatterplot(
        x=range(y_test.shape[0]), y=best_model.predict(X_test)[ind], ax=axes[1]
    )

    # axes[0].set_ylim((0.1,1.9))
    # axes[1].set_ylim((0.1,1.9))

    _, ax = plt.subplots(1, 1, figsize=(6, 4))
    sns.scatterplot(x=y_test, y=best_model.predict(X_test))

    r = permutation_importance(best_model, X_test, y_test, n_repeats=30, random_state=0)

    for i in r.importances_mean.argsort()[::-1]:
        if r.importances_mean[i] - 2 * r.importances_std[i] > 0:
            print(
                f"{X_train.columns[i]:<8}: "
                f"{r.importances_mean[i]:.3f}"
                f" +/- {r.importances_std[i]:.3f}"
            )

    print("Best model params: ", best_model.get_params())
    print("Best score: ", results_pred["R2"])

    return best_model, fig

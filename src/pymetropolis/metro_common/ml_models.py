from __future__ import annotations

from typing import TYPE_CHECKING

from loguru import logger

from pymetropolis.metro_common import MetropyError

if TYPE_CHECKING:
    import numpy as np
    import polars as pl


def compute_lasso(
    endog_variable: pl.Series, exog_variables: pl.DataFrame
) -> tuple[np.ndarray, np.ndarray, float, dict[str, float]]:
    import numpy as np
    from sklearn.linear_model import LassoCV
    from sklearn.metrics import root_mean_squared_error

    logger.info("Fitting a LASSO model...")
    Y = endog_variable.to_numpy()
    X = exog_variables.to_numpy()
    if X.ndim != 2:
        raise MetropyError(f"Endog variables should be 2-dimensional:\n{X}")
    if Y.ndim != 1:
        raise MetropyError(f"Exog variables should be 1-dimensional:\n{Y}")
    if X.shape[0] != Y.shape[0]:
        raise MetropyError(
            f"Mismatched number of observations: endog: {Y.shape[0]} / exog: {X.shape[0]}"
        )
    logger.debug(f"Number of observations: {X.shape[0]}")
    logger.debug(f"Number of variables: {X.shape[1]}")
    lassocv = LassoCV(fit_intercept=False, max_iter=10_000)
    lassocv.fit(X, Y)
    logger.debug(f"Value of the penalization factor: {lassocv.alpha_}")
    Y_hat = lassocv.predict(X)
    residuals = Y - Y_hat
    rmse = root_mean_squared_error(Y, Y_hat)
    logger.debug(f"RMSE: {rmse}")
    corr = np.corrcoef(Y, Y_hat)[0][1]
    logger.debug(f"Correlation: {corr:.8%}")
    coefs = lassocv.coef_
    coef_lasso = {var: coef for var, coef in zip(exog_variables.columns, coefs)}
    return (Y_hat, residuals, rmse, coef_lasso)

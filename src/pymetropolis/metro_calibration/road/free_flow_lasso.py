from __future__ import annotations

import itertools
import re
from typing import TYPE_CHECKING

from loguru import logger

from pymetropolis.metro_calibration.road.files import (
    RoadEdgesPenaltyCoefficientsFile,
    RoadEdgesVariablesFile,
    TomTomRoutesFile,
    TomTomRoutesMatchedFile,
)
from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_common.ml_models import compute_lasso
from pymetropolis.metro_pipeline import Step
from pymetropolis.metro_pipeline.parameters import ListParameter
from pymetropolis.metro_pipeline.types import List, String

if TYPE_CHECKING:
    import polars as pl


def compute_regression_variables(
    variables: pl.DataFrame,
    routes: pl.DataFrame,
    additive_variables: list[str],
    additive_interaction_variables: list[list[str]],
    multiplicative_variables: list[str],
    multiplicative_interaction_variables: list[list[str]],
):
    import polars as pl

    logger.info("Computing congested time by edge characteristics...")
    df = routes.select(
        "tomtom_id",
        "path",
        cst=pl.col("path").list.len(),
        tt_cst=pl.col("path")
        .list.eval(
            pl.element().replace_strict(variables["edge_id"], variables["base_free_flow_tt"])
        )
        .list.sum(),
    )
    # Check that all variables are available.
    all_vars = (
        set(additive_variables)
        | set(multiplicative_variables)
        | set(itertools.chain.from_iterable(additive_interaction_variables))
        | set(itertools.chain.from_iterable(multiplicative_interaction_variables))
    )
    has_error = False
    for var in all_vars:
        if not any(map(lambda c: c.startswith(var), variables.columns)):
            logger.error(f"Variable not found: {var}")
            has_error = True
    if has_error:
        raise MetropyError("Some defined variables could not be found.")
    logger.debug("Additive variables...")
    for var in additive_variables:
        logger.debug(f"\t{var}...")
        for col in filter(lambda c: c.startswith(var), variables.columns):
            df = df.with_columns(
                pl.col("path")
                .list.eval(pl.element().replace_strict(variables["edge_id"], variables[col]))
                .list.sum()
                .alias(col)
            )
    logger.debug("Additive interaction variables...")
    for var1, var2 in additive_interaction_variables:
        logger.debug(f"\t{var1} x {var2}...")
        for col1 in filter(lambda c: c.startswith(var1), variables.columns):
            for col2 in filter(lambda c: c.startswith(var2), variables.columns):
                df = df.with_columns(
                    pl.col("path")
                    .list.eval(
                        pl.element().replace_strict(variables["edge_id"], variables[col1])
                        * pl.element().replace_strict(variables["edge_id"], variables[col2])
                    )
                    .list.sum()
                    .alias(f"{col1}_x_{col2}")
                )
    logger.debug("Multiplicative variables...")
    for var in multiplicative_variables:
        logger.debug(f"\t{var}...")
        for col in filter(lambda c: c.startswith(var), variables.columns):
            df = df.with_columns(
                pl.col("path")
                .list.eval(
                    pl.element().replace_strict(variables["edge_id"], variables[col])
                    * pl.element().replace_strict(
                        variables["edge_id"], variables["base_free_flow_tt"]
                    )
                )
                .list.sum()
                .alias(f"tt_{col}")
            )
    logger.debug("Multiplicative interaction variables...")
    for var1, var2 in multiplicative_interaction_variables:
        logger.debug(f"\t{var1} x {var2}...")
        for col1 in filter(lambda c: c.startswith(var1), variables.columns):
            for col2 in filter(lambda c: c.startswith(var2), variables.columns):
                df = df.with_columns(
                    pl.col("path")
                    .list.eval(
                        pl.element().replace_strict(variables["edge_id"], variables[col1])
                        * pl.element().replace_strict(variables["edge_id"], variables[col2])
                        * pl.element().replace_strict(
                            variables["edge_id"], variables["base_free_flow_tt"]
                        )
                    )
                    .list.sum()
                    .alias(f"tt_{col1}_x_{col2}")
                )
    df = df.drop("tomtom_id", "path")
    return df


def coefs_to_df(coefs: dict[str, float]) -> pl.DataFrame:
    import polars as pl

    data = list()
    pattern = re.compile(r"^(tt_)?(.+?)(?:_x_(.+))?$")
    for name, value in coefs.items():
        m = pattern.match(name)
        if m is None:
            raise MetropyError(f"Unexpected coefficient name: {name}")
        ptype = "multiplicative" if m.group(1) is not None else "additive"
        var1 = m.group(2)
        var2 = m.group(3)
        data.append((ptype, var1, var2, value))
    df = pl.DataFrame(data, schema=["type", "variable1", "variable2", "penalty"], orient="row")
    return df


class FreeFlowLassoStep(Step):
    """Estimates free-flow penalty coefficients from a Lasso regression on TomTom routes."""

    additive_variables = ListParameter(
        "calibration.free_flow_travel_time.additive_variables",
        inner=String(),
        default=[],
        description="Name of variables to be used as additive penalties in the Lasso regression.",
        note=(
            "A constant is automatically included. "
            "The listed variables must be available in `RoadEdgesVariablesFile`."
        ),
    )
    additive_interaction_variables = ListParameter(
        "calibration.free_flow_travel_time.additive_interaction_variables",
        inner=List(inner=String(), length=2),
        default=[],
        description=(
            "Name of variable pairs whose interaction is used as additive penalties in the Lasso "
            "regression."
        ),
        note="The listed variables must be available in `RoadEdgesVariablesFile`.",
    )
    multiplicative_variables = ListParameter(
        "calibration.free_flow_travel_time.multiplicative_variables",
        inner=String(),
        default=[],
        description=(
            "Name of variables to be used as multiplicative penalties in the Lasso regression."
        ),
        note=(
            "A constant is automatically included. "
            "The listed variables must be available in `RoadEdgesVariablesFile`."
        ),
    )
    multiplicative_interaction_variables = ListParameter(
        "calibration.free_flow_travel_time.multiplicative_interaction_variables",
        inner=List(inner=String(), length=2),
        default=[],
        description=(
            "Name of variable pairs whose interaction is used as multiplicative penalties in the "
            "Lasso regression."
        ),
        note="The listed variables must be available in `RoadEdgesVariablesFile`.",
    )

    input_files = {
        "variables": RoadEdgesVariablesFile,
        "routes": TomTomRoutesFile,
        "matched_routes": TomTomRoutesMatchedFile,
    }
    output_files = {"coefs": RoadEdgesPenaltyCoefficientsFile}

    mandatory_keys = [
        "calibration.free_flow_calibration.additive_variables",
        "calibration.free_flow_calibration.additive_interaction_variables",
        "calibration.free_flow_calibration.multiplicative_variables",
        "calibration.free_flow_calibration.multiplicative_interaction_variables",
        "calibration.free_flow_calibration.coef_filename",
    ]

    def run(self):
        import polars as pl

        assert self.additive_variables is not None
        assert self.additive_interaction_variables is not None
        assert self.multiplicative_variables is not None
        assert self.multiplicative_interaction_variables is not None

        variables = self.input["variables"].read()
        routes = self.input["routes"].read()
        matched_routes = self.input["matched_routes"].read()

        exog_variables = compute_regression_variables(
            variables,
            matched_routes,
            self.additive_variables,
            self.additive_interaction_variables,
            self.multiplicative_variables,
            self.multiplicative_interaction_variables,
        )

        endog_variable = pl.Series(
            routes.loc[routes["tomtom_id"].isin(matched_routes["tomtom_id"]), "tt_no_traffic"]
        ).dt.total_seconds(fractional=True)

        (_Y_hat, _residuals, _rmse, coefs) = compute_lasso(endog_variable, exog_variables)

        df = coefs_to_df(coefs)
        breakpoint()
        self.output["coefs"].write(df)

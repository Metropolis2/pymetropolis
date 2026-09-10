from __future__ import annotations

import json
from typing import TYPE_CHECKING

from loguru import logger

from pymetropolis.metro_calibration.survey.files import (
    SurveyedToursFile,
    SurveyedToursTravelTimesFile,
)
from pymetropolis.metro_calibration.survey.modes import ModeClassifierConfigStep
from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_common.utils import pl_duration_to_seconds
from pymetropolis.metro_pipeline.parameters import ListParameter, StringParameter
from pymetropolis.metro_pipeline.types import String

from .files import SurveyModeChoiceParametersFile, SurveyModeChoiceStatsFile

if TYPE_CHECKING:
    from pathlib import Path

    import pandas as pd


# Variables treated as alternative-varying, read from `{variable}_{mode}` columns.
ALTERNATIVE_VARYING_VARIABLES = {"travel_time"}


def generic_variable_mode(mode: str) -> str:
    """Maps `car_*` modes to just `car`."""
    return "car" if "car" in mode else mode


class SurveyEconometricModeChoiceStep(ModeClassifierConfigStep):
    """Estimates a Multinomial Logit model of tour-level mode choice from the surveyed tours,
    using [biogeme](https://biogeme.epfl.ch/).

    Tours are filtered and cleaned the same way as for `ModeClassifierConfigStep` (restricted to
    `mode_classifier.modes`, car-driver modes optionally grouped, tours outside the survey
    perimeter or with an undefined mode dropped).
    """

    variables = ListParameter(
        "mode_choice_estimation.variables",
        inner=String(),
        description="Explanatory variables of the mode-choice model.",
        note=(
            "Tour-, person- or household-level variables (constant across alternatives) "
            "must exist as a column of `SurveyedToursFile`; one coefficient is estimated for "
            "each non-reference mode. "
            'Alternative-varying variables (e.g., "travel_time") must exist as one '
            "`{variable}_{mode}` column per mode; one coefficient is estimated for each mode."
        ),
    )
    reference_mode = StringParameter(
        "mode_choice_estimation.reference_mode",
        description=(
            "Mode used as the reference alternative (its constant and case-variable "
            "coefficients are fixed to 0)."
        ),
        note="Defaults to the first mode in `mode_classifier.modes`.",
    )

    input_files = {"tours": SurveyedToursFile, "tours_tt": SurveyedToursTravelTimesFile}
    output_files = {
        "parameters": SurveyModeChoiceParametersFile,
        "stats": SurveyModeChoiceStatsFile,
    }

    def is_defined(self):
        return self.modes is not None and len(self.modes) >= 2

    def run(self):
        import polars as pl
        import polars.selectors as cs

        assert self.modes is not None

        tours = self.filter_survey_tours(self.input["tours"].read())
        tours_tt = self.input["tours_tt"].read()
        tours = tours.join(tours_tt, on="tour_id", how="left")

        modes = list(tours["tour_mode"].unique())

        if self.reference_mode is not None:
            reference_mode = self.reference_mode
            if reference_mode not in modes:
                raise MetropyError(
                    f"`mode_choice_estimation.reference_mode` (`{reference_mode}`) must be one of "
                    f"`mode_classifier.modes` (`{modes}`)."
                )
        else:
            reference_mode = self.modes[0]
        if self.group_car_driver_modes and reference_mode.startswith("car_driver_"):
            reference_mode = "car_driver"

        variables = self.variables or []
        generic_variables = [v for v in variables if v in ALTERNATIVE_VARYING_VARIABLES]
        case_variables = [v for v in variables if v not in ALTERNATIVE_VARYING_VARIABLES]

        generic_columns = {
            variable: {mode: f"{variable}_{generic_variable_mode(mode)}" for mode in modes}
            for variable in generic_variables
        }
        missing_generic = sorted(
            {
                col
                for cols in generic_columns.values()
                for col in cols.values()
                if col not in tours.columns
            }
        )
        if missing_generic:
            raise MetropyError(
                "Missing columns for the alternative-varying `mode_choice_estimation.variables`: "
                f"{', '.join(missing_generic)}. Each such variable must exist as one "
                "`{variable}_{mode}` column for every mode in `mode_classifier.modes`."
            )
        missing_case = [v for v in case_variables if v not in tours.columns]
        if missing_case:
            raise MetropyError(
                "Missing columns for `mode_choice_estimation.variables`: "
                f"{', '.join(missing_case)}."
            )

        # Convert Duration columns (e.g. `travel_time_*`) to plain floats (in hours) so they can
        # be used as numeric variables in the model.
        tours = tours.with_columns(pl_duration_to_seconds(cs.by_dtype(pl.Duration)) / 3600)

        # Drop tours with a missing survey weight or a missing case variable: both enter the
        # utility of every alternative, so a null value would invalidate the whole observation.
        required_cols = ["weight", *case_variables]
        n0 = len(tours)
        tours = tours.drop_nulls(required_cols)
        n1 = len(tours)
        if n1 < n0:
            logger.warning(
                f"Dropping {n0 - n1:,} observations ({(n0 - n1) / n0:.2%}) with a missing "
                "survey weight or case variable."
            )

        mode_ids = {mode: i for i, mode in enumerate(modes)}
        tours = tours.with_columns(
            alt_id=pl.col("tour_mode").replace_strict(mode_ids, return_dtype=pl.Int64)
        )

        # An alternative is available for a given tour if all its generic variables are defined
        # (e.g., a travel time by car could be missing because of a routing failure).
        tours = tours.with_columns(
            **{
                f"avail_{mode}": pl.all_horizontal(
                    *(pl.col(generic_columns[v][mode]).is_not_null() for v in generic_variables)
                ).cast(pl.Int8)
                if generic_variables
                else pl.lit(1, dtype=pl.Int8)
                for mode in modes
            }
        )
        # Fill missing generic variables with 0: irrelevant since the alternative is then marked
        # unavailable, but required so that the utility expression always evaluates to a finite
        # number.
        tours = tours.with_columns(
            **{
                col: pl.col(col).fill_null(0.0)
                for cols in generic_columns.values()
                for col in cols.values()
            }
        )

        # Drop tours whose chosen mode is unavailable.
        tours = tours.with_columns(
            chosen_avail=pl.concat_list([pl.col(f"avail_{mode}") for mode in modes]).list.get(
                "alt_id"
            )
        )
        n0 = len(tours)
        tours = tours.filter(pl.col("chosen_avail") == 1)
        n1 = len(tours)
        if n1 < n0:
            logger.warning(
                f"Dropping {n0 - n1:,} observations ({(n0 - n1) / n0:.2%}) whose chosen mode is "
                "unavailable (missing generic variable)."
            )

        columns = [
            "alt_id",
            "weight",
            *(f"avail_{mode}" for mode in modes),
            *case_variables,
            *sorted({col for cols in generic_columns.values() for col in cols.values()}),
        ]
        df = tours.select(
            *(pl.col(c).cast(pl.Float64) if c not in ("alt_id",) else pl.col(c) for c in columns)
        ).to_pandas()

        params, stats = estimate_mnl(
            df,
            mode_ids,
            reference_mode,
            generic_columns,
            case_variables,
            out_dir=self.output["parameters"].complete_path.parent,
            model_name=type(self).__name__,
        )

        self.output["parameters"].write(json.dumps(params, indent=2, sort_keys=True))
        self.output["stats"].write(json.dumps(stats, indent=2, sort_keys=True))


def estimate_mnl(
    df: pd.DataFrame,
    mode_ids: dict[str, int],
    reference_mode: str,
    generic_columns: dict[str, dict[str, str]],
    case_variables: list[str],
    out_dir: Path,
    model_name: str,
):
    """Estimates a Multinomial Logit model of `alt_id` from `df` with biogeme, and returns
    `(parameters, stats)`, two plain, JSON-serializable dicts.

    `generic_columns` maps each generic variable to its `{mode: column_name}` mapping (see
    `generic_variable_mode`).
    """
    from biogeme import models
    from biogeme.biogeme import BIOGEME
    from biogeme.database import Database
    from biogeme.expressions import Beta, Expression, Variable
    from biogeme.results_processing import get_pandas_estimated_parameters

    out_dir.mkdir(parents=True, exist_ok=True)

    database = Database(model_name, df)

    utilities: dict[int, Expression | float] = {}
    for mode, alt_id in mode_ids.items():
        terms = [
            Beta(f"B_{variable}_{mode}", 0.0, None, None, 0)
            * Variable(generic_columns[variable][mode])
            for variable in generic_columns
        ]
        if mode != reference_mode:
            terms.append(Beta(f"ASC_{mode}", 0.0, None, None, 0))
            terms.extend(
                Beta(f"B_{variable}_{mode}", 0.0, None, None, 0) * Variable(variable)
                for variable in case_variables
            )
        utilities[alt_id] = sum(terms, start=0.0)

    availabilities: dict[int, Expression | float] = {
        alt_id: Variable(f"avail_{mode}") for mode, alt_id in mode_ids.items()
    }

    logprob = models.loglogit(utilities, availabilities, Variable("alt_id"))
    formulas = {"loglike": logprob, "weight": Variable("weight")}

    biogeme = BIOGEME(
        database,  # ty: ignore[too-many-positional-arguments]
        formulas,
        parameters=str(out_dir / f"{model_name}.toml"),  # ty: ignore[unknown-argument]
        generate_html=False,  # ty: ignore[unknown-argument]
        generate_yaml=False,  # ty: ignore[unknown-argument]
        generate_netcdf=False,  # ty: ignore[unknown-argument]
        save_iterations=False,  # ty: ignore[unknown-argument]
    )
    biogeme.model_name = model_name  # ty: ignore[unresolved-attribute]

    results = biogeme.estimate()  # ty: ignore[unresolved-attribute]

    parameters_df = next(iter(get_pandas_estimated_parameters(results).values()))
    parameters = {
        row["Name"]: {
            "value": row["Value"],
            "robust_std_err": row["Robust std err."],
            "robust_t_stat": row["Robust t-stat."],
            "robust_p_value": row["Robust p-value"],
        }
        for row in parameters_df.to_dict(orient="records")
    }

    stats = results.get_general_statistics()

    return parameters, stats

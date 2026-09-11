from __future__ import annotations

import json
from typing import TYPE_CHECKING

from loguru import logger

from pymetropolis.metro_calibration.econometrics.files import SurveyModeChoiceResultsFile
from pymetropolis.metro_calibration.survey.files import (
    SurveyedToursFile,
    SurveyedToursTravelTimesFile,
)
from pymetropolis.metro_calibration.survey.modes import ModeClassifierConfigStep
from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_common.utils import pl_duration_to_seconds
from pymetropolis.metro_pipeline.parameters import ListParameter, StringParameter
from pymetropolis.metro_pipeline.types import List, String

if TYPE_CHECKING:
    from pathlib import Path

    import pandas as pd


# Name of the variable holding the travel time of each alternative.
TRAVEL_TIME_VARIABLE = "travel_time"

# Variables treated as alternative-varying, read from `{variable}_{mode}` columns.
ALTERNATIVE_VARYING_VARIABLES = {TRAVEL_TIME_VARIABLE}

# Column of `SurveyedToursFile` indicating whether the person holds a driving license.
DRIVING_LICENSE_COLUMN = "has_driving_license"

# Column of `SurveyedToursFile` indicating the number of cars owned.
CAR_OWNERSHIP_COLUMN = "nb_cars"


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
        default=[],
        description="Explanatory variables of the mode-choice model.",
        note=(
            "Tour-, person- or household-level variables (constant across alternatives) "
            "must exist as a column of `SurveyedToursFile`; one coefficient is estimated for "
            "each non-reference mode. "
            'Alternative-varying variables (e.g., "travel_time") must exist as one '
            "`{variable}_{mode}` column per mode; one coefficient is estimated for each mode."
        ),
    )
    interaction_variables = ListParameter(
        "mode_choice_estimation.interaction_variables",
        inner=List(inner=String(), length=2),
        default=[],
        description="Pairs of variables whose interaction is included as an explanatory variable.",
        note=(
            "Each pair may mix an alternative-varying variable (e.g., `travel_time`) with a "
            "case variable (e.g., `woman`) — one coefficient is then estimated for each mode — "
            "or combine two case variables — one coefficient is then estimated for each "
            "non-reference mode, like a regular case variable. The variables do not need to also "
            "appear in `mode_choice_estimation.variables`."
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
    output_files = {"results": SurveyModeChoiceResultsFile}

    def is_defined(self):
        return self.modes is not None and len(self.modes) >= 2

    def run(self):
        import polars as pl
        import polars.selectors as cs

        assert self.modes is not None
        assert self.variables is not None
        assert self.interaction_variables is not None

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

        variables = self.variables
        interactions = self.interaction_variables
        generic_variables = [v for v in variables if v in ALTERNATIVE_VARYING_VARIABLES]
        case_variables = [v for v in variables if v not in ALTERNATIVE_VARYING_VARIABLES]
        interaction_generic_variables = {
            v for pair in interactions for v in pair if v in ALTERNATIVE_VARYING_VARIABLES
        }
        all_generic_variables = sorted({*generic_variables, *interaction_generic_variables})

        generic_columns = {
            variable: {mode: f"{variable}_{generic_variable_mode(mode)}" for mode in modes}
            for variable in all_generic_variables
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
                "Missing columns for the alternative-varying variables: "
                f"{', '.join(missing_generic)}. Each such variable must exist as one "
                "`{variable}_{mode}` column for every mode."
            )

        # A case variable used only inside an interaction (e.g., `woman` if only
        # `["travel_time", "woman"]` is requested) still needs its own column resolved.
        interaction_case_columns = sorted(
            {
                v
                for pair in interactions
                for v in pair
                if v not in ALTERNATIVE_VARYING_VARIABLES and v not in case_variables
            }
        )
        missing_case = [
            v for v in [*case_variables, *interaction_case_columns] if v not in tours.columns
        ]
        if missing_case:
            raise MetropyError(f"Missing columns for variables: {', '.join(missing_case)}.")

        # Convert Duration columns (e.g. `travel_time_*`) to plain floats (in hours) so they can
        # be used as numeric variables in the model.
        tours = tours.with_columns(pl_duration_to_seconds(cs.by_dtype(pl.Duration)) / 3600)

        # Drop tours with a missing survey weight or a missing case variable: both enter the
        # utility of every alternative, so a null value would invalidate the whole observation.
        required_cols = ["weight", *case_variables, *interaction_case_columns]
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

        # Car-driver modes are restricted to holders of a driving license.
        has_license_data = DRIVING_LICENSE_COLUMN in tours.columns
        if not has_license_data and any(mode.startswith("car_driver") for mode in modes):
            logger.warning(
                f"`{DRIVING_LICENSE_COLUMN}` is not available in `SurveyedToursFile`: car-driver "
                "modes will not be restricted to driving-license holders."
            )
        # Car modes are restricted to car owners.
        has_car_ownership_data = CAR_OWNERSHIP_COLUMN in tours.columns
        if not has_car_ownership_data and any(mode.startswith("car") for mode in modes):
            logger.warning(
                f"`{CAR_OWNERSHIP_COLUMN}` is not available in `SurveyedToursFile`: car modes will "
                "not be restricted to car owners."
            )
        avail_exprs = {}
        for mode in modes:
            conditions = [pl.col(generic_columns[v][mode]).is_not_null() for v in generic_columns]
            if has_license_data and mode.startswith("car_driver"):
                conditions.append(pl.col(DRIVING_LICENSE_COLUMN).fill_null(False))
            if has_car_ownership_data and mode.startswith("car"):
                conditions.append(pl.col(CAR_OWNERSHIP_COLUMN).ge(1).fill_null(False))
            avail_exprs[f"avail_{mode}"] = (
                pl.all_horizontal(*conditions) if conditions else pl.lit(True)
            ).cast(pl.Int8)
        tours = tours.with_columns(**avail_exprs)
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
                "unavailable."
            )

        columns = [
            "alt_id",
            "weight",
            *(f"avail_{mode}" for mode in modes),
            *case_variables,
            *interaction_case_columns,
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
            generic_variables,
            case_variables,
            interactions,
            out_dir=self.output["results"].complete_path.parent,
            model_name=type(self).__name__,
        )

        # The specification is stored next to the estimated values so that a downstream step can
        # interpret the parameter names (which variables are alternative-varying, which pairs are
        # interacted, which mode is the reference) without duplicating the config keys.
        specification = {
            "modes": sorted(modes),
            "reference_mode": reference_mode,
            "variables": case_variables + generic_variables,
            "interaction_variables": interactions,
        }
        results = {"specification": specification, "stats": stats, "parameters": params}

        self.output["results"].write(json.dumps(results, indent=2, sort_keys=True))


def estimate_mnl(
    df: pd.DataFrame,
    mode_ids: dict[str, int],
    reference_mode: str,
    generic_columns: dict[str, dict[str, str]],
    generic_variables: list[str],
    case_variables: list[str],
    interactions: list[list[str]],
    out_dir: Path,
    model_name: str,
):
    """Estimates a Multinomial Logit model of `alt_id` from `df` with biogeme, and returns
    `(parameters, stats)`, two plain, JSON-serializable dicts.

    `generic_columns` maps every alternative-varying variable (whether listed in
    `generic_variables` or only used inside `interactions`) to its `{mode: column_name}` mapping
    (see `generic_variable_mode`); `generic_variables` and `case_variables` are the variables for
    which a standalone term is added; `interactions` are pairs of variables (either) whose
    product is added as an extra term, per-mode (if either variable is alternative-varying, i.e.,
    a key of `generic_columns`) or per-non-reference-mode (otherwise), mirroring the treatment of
    a plain generic/case variable.
    """
    from biogeme import models
    from biogeme.biogeme import BIOGEME
    from biogeme.database import Database
    from biogeme.expressions import Beta, Expression, Variable
    from biogeme.results_processing import get_pandas_estimated_parameters

    out_dir.mkdir(parents=True, exist_ok=True)

    database = Database(model_name, df)

    def col_for(variable: str, mode: str) -> str:
        return generic_columns[variable][mode] if variable in generic_columns else variable

    utilities: dict[int, Expression | float] = {}
    for mode, alt_id in mode_ids.items():
        terms = [
            Beta(f"B_{variable}_{mode}", 0.0, None, None, 0)
            * Variable(generic_columns[variable][mode])
            for variable in generic_variables
        ]
        terms.extend(
            Beta(f"B_{v1}_x_{v2}_{mode}", 0.0, None, None, 0)
            * Variable(col_for(v1, mode))
            * Variable(col_for(v2, mode))
            for v1, v2 in interactions
            if v1 in generic_columns or v2 in generic_columns
        )
        if mode != reference_mode:
            terms.append(Beta(f"ASC_{mode}", 0.0, None, None, 0))
            terms.extend(
                Beta(f"B_{variable}_{mode}", 0.0, None, None, 0) * Variable(variable)
                for variable in case_variables
            )
            terms.extend(
                Beta(f"B_{v1}_x_{v2}_{mode}", 0.0, None, None, 0) * Variable(v1) * Variable(v2)
                for v1, v2 in interactions
                if v1 not in generic_columns and v2 not in generic_columns
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

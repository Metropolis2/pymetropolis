from __future__ import annotations

from typing import TYPE_CHECKING

from loguru import logger

from pymetropolis.metro_calibration.econometrics.files import SurveyModeChoiceResultsFile
from pymetropolis.metro_calibration.survey.files import (
    SurveyedToursFile,
    SurveyedToursTravelTimesFile,
)
from pymetropolis.metro_calibration.survey.modes import filter_survey_tours
from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_common.utils import pl_duration_to_seconds
from pymetropolis.metro_environment.fuel import StepWithFuel
from pymetropolis.metro_pipeline.parameters import (
    BoolParameter,
    DictParameter,
    FloatParameter,
    ListParameter,
    StringParameter,
)
from pymetropolis.metro_pipeline.types import List, String
from pymetropolis.metro_simulation.common import StepWithRidesharingCount
from pymetropolis.modes import (
    CarMode,
    MetaMode,
    StepWithModeAvailability,
    StepWithModes,
    mode_availability_conditions,
    mode_from_str,
    warn_missing_availability_columns,
)

if TYPE_CHECKING:
    from collections.abc import Iterable
    from pathlib import Path

    import pandas as pd
    import polars as pl


# Name of the variable holding the travel time of each alternative.
TRAVEL_TIME_VARIABLE = "travel_time"

# Name of the variable holding the monetary cost of each alternative (in €).
MONETARY_COST_VARIABLE = "monetary_cost"

# Variables treated as alternative-varying, read from `{variable}_{mode}` columns.
ALTERNATIVE_VARYING_VARIABLES = {TRAVEL_TIME_VARIABLE, MONETARY_COST_VARIABLE}

# Alternative-varying variables with a single coefficient shared by all modes (instead of one
# coefficient per mode).
GENERIC_COEFFICIENT_VARIABLES = {MONETARY_COST_VARIABLE}


def generic_variable_mode(mode: MetaMode) -> str:
    """Return the mode as a string, mapping all car-based modes to "car"."""
    return "car" if mode.is_car_based() else repr(mode)


def monetary_cost_exprs(
    modes: Iterable[MetaMode],
    fuel_factor: float,
    fuel_price: float,
    ridesharing_passenger_count: float,
) -> dict[str, pl.Expr]:
    """Returns, for each mode, an expression of the monetary cost of the tour (in €), as a
    `monetary_cost_{mode}` column.

    The monetary cost currently only includes the fuel cost of car modes, computed from the
    `distance_car` column (in meters), the fuel consumption factor (liters per km) and the fuel
    price (€ per liter), and weighted by the share of fuel paid by the traveler of each car mode.
    It is zero for all the other modes.
    """
    import polars as pl

    exprs = {}
    for mode in modes:
        name = f"{MONETARY_COST_VARIABLE}_{mode!r}"
        if isinstance(mode, CarMode):
            fuel_share = mode.get_fuel_share(ridesharing_passenger_count)
            exprs[name] = pl.col("distance_car") / 1000 * fuel_factor * fuel_price * fuel_share
        else:
            exprs[name] = pl.lit(0.0, dtype=pl.Float64)
    return exprs


def add_derived_variables(df: pl.DataFrame, derived: dict[str, str]) -> pl.DataFrame:
    """Returns `df` with one extra column for each derived variable of `derived`, which maps the
    variable names to (polars) SQL expressions of the existing columns of `df`.
    """
    import polars as pl

    for name, expr in derived.items():
        if name in ALTERNATIVE_VARYING_VARIABLES:
            raise MetropyError(
                f"Derived variable `{name}` cannot use the name of an alternative-varying variable."
            )
        if name in df.columns:
            raise MetropyError(
                f"Derived variable `{name}` cannot use the name of an existing column."
            )
    try:
        return df.with_columns(**{name: pl.sql_expr(expr) for name, expr in derived.items()})
    except Exception as e:
        # The polars error does not say which expression failed, so each one is evaluated in turn
        # to report the faulty one.
        for name, expr in derived.items():
            try:
                df.select(pl.sql_expr(expr))
            except Exception as e2:
                raise MetropyError(
                    f"Cannot evaluate derived variable `{name}` (`{expr}`): {e2}"
                ) from e2
        raise MetropyError(f"Cannot evaluate derived variables: {e}") from e


class SurveyEconometricModeChoiceStep(
    StepWithModes, StepWithModeAvailability, StepWithFuel, StepWithRidesharingCount
):
    """Estimates a Multinomial Logit model of tour-level mode choice from the surveyed tours,
    using [biogeme](https://biogeme.epfl.ch/).

    The choice set of each tour is restricted to the available modes, as defined by the
    `mode_availability` parameters (the same rules are used in the simulation).
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
            "`{variable}_{mode}` column per mode; one coefficient is estimated for each mode. "
            'The "monetary_cost" variable is the monetary cost of each alternative, in €; it '
            "currently only includes the fuel cost of car modes (computed from the car route "
            "distance and the `fuel` parameters, which must be defined) and a single "
            "coefficient is estimated for all modes."
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
    derived_variables = DictParameter(
        "mode_choice_estimation.derived_variables",
        inner=String(),
        default={},
        description=(
            "Variables computed from the columns of the tours, which can then be used in "
            "`mode_choice_estimation.variables` and `mode_choice_estimation.interaction_variables`."
        ),
        note=(
            "Table mapping each variable name to a SQL expression (polars dialect) of the "
            "tour-, person- or household-level columns of the tours. "
        ),
        example='`{ age_18_29 = "age BETWEEN 18 AND 29", age_65_plus = "age >= 65" }`',
    )
    reference_mode = StringParameter(
        "mode_choice_estimation.reference_mode",
        description=(
            "Mode used as the reference alternative (its constant and case-variable "
            "coefficients are fixed to 0)."
        ),
        note="Defaults to the first mode in `mode_classifier.modes`.",
    )
    reference_vot = FloatParameter(
        "mode_choice_estimation.reference_value_of_time",
        lower_bound=0.0,
        description=(
            "Value of time of the `mode_choice_estimation.reference_value_of_time_mode` mode "
            "(€/h), imposed in the estimation to identify the marginal utility of money."
        ),
        note=(
            "The `travel_time` coefficient of the reference mode is constrained to be equal to "
            "this value times the `monetary_cost` coefficient (the marginal utility of money), "
            "which is then used to convert the utilities to euros. "
            "This works whether or not `monetary_cost` is part of the "
            "`mode_choice_estimation.variables` (if it is not, the constraint is only a "
            "reparametrization of the model). "
            "If `travel_time` is interacted with case variables, the value of time applies to the "
            "base category (e.g., men for a `woman` x `travel_time` interaction)."
        ),
    )
    reference_vot_mode = StringParameter(
        "mode_choice_estimation.reference_value_of_time_mode",
        description=(
            "Mode whose value of time is set to `mode_choice_estimation.reference_value_of_time`."
        ),
    )
    shared_car_driver_vot = BoolParameter(
        "mode_choice_estimation.shared_car_driver_value_of_time",
        default=False,
        description=(
            "If `true`, car-driver modes share the same `travel_time` coefficients "
            "(including the interactions of `travel_time` with case variables)."
        ),
    )

    input_files = {"tours": SurveyedToursFile, "tours_tt": SurveyedToursTravelTimesFile}
    output_files = {"results": SurveyModeChoiceResultsFile}

    def is_defined(self):
        return self.has_mode_choice()

    def run(self):
        import polars as pl
        import polars.selectors as cs

        assert self.modes is not None
        assert self.variables is not None
        assert self.interaction_variables is not None
        assert self.derived_variables is not None
        assert self.ridesharing_passenger_count is not None

        tours = filter_survey_tours(self.modes, self.input["tours"].read())
        tours_tt = self.input["tours_tt"].read()
        tours = tours.join(tours_tt, on="tour_id", how="left")

        # Only the derived variables actually used by the model are computed, so that an unused
        # definition does not need to be valid.
        used_variables = {
            *self.variables,
            *(v for pair in self.interaction_variables for v in pair),
        }
        derived_variables = {
            name: expr for name, expr in self.derived_variables.items() if name in used_variables
        }
        tours = add_derived_variables(tours, derived_variables)

        modes = [mode_from_str(m) for m in tours["tour_mode"].unique()]

        if self.reference_mode is not None:
            reference_mode = mode_from_str(self.reference_mode)
            if reference_mode not in modes:
                raise MetropyError(
                    f"`mode_choice_estimation.reference_mode` (`{reference_mode}`) must be one of "
                    f"`{modes}`."
                )
        else:
            reference_mode = self.modes[0]

        variables = self.variables
        interactions = self.interaction_variables
        generic_variables = [v for v in variables if v in ALTERNATIVE_VARYING_VARIABLES]
        case_variables = [v for v in variables if v not in ALTERNATIVE_VARYING_VARIABLES]
        interaction_generic_variables = {
            v for pair in interactions for v in pair if v in ALTERNATIVE_VARYING_VARIABLES
        }
        all_generic_variables = sorted({*generic_variables, *interaction_generic_variables})

        reference_vot = check_reference_vot(
            self.reference_vot, self.reference_vot_mode, generic_variables, modes
        )
        vot_aliases = check_shared_car_driver_vot(
            bool(self.shared_car_driver_vot), modes, all_generic_variables, reference_vot
        )

        if any(v in GENERIC_COEFFICIENT_VARIABLES for pair in interactions for v in pair):
            raise MetropyError(
                f"The `{MONETARY_COST_VARIABLE}` variable cannot be used in "
                "`mode_choice_estimation.interaction_variables`."
            )
        if MONETARY_COST_VARIABLE in generic_variables:
            if self.fuel_factor is None or self.fuel_price is None:
                raise MetropyError(
                    f"The `{MONETARY_COST_VARIABLE}` variable requires the "
                    "`fuel.consumption_factor` and `fuel.price` parameters."
                )
            if any(mode.is_car_based() for mode in modes) and "distance_car" not in tours.columns:
                raise MetropyError(
                    f"The `{MONETARY_COST_VARIABLE}` variable requires the `distance_car` column "
                    "of `SurveyedToursTravelTimesFile`."
                )
            tours = tours.with_columns(
                **monetary_cost_exprs(
                    modes, self.fuel_factor, self.fuel_price, self.ridesharing_passenger_count
                )
            )

        generic_columns = {
            variable: {
                repr(mode): f"{variable}_{mode!r}"
                if variable == MONETARY_COST_VARIABLE
                else f"{variable}_{generic_variable_mode(mode)}"
                for mode in modes
            }
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

        mode_ids = {repr(mode): i for i, mode in enumerate(modes)}
        tours = tours.with_columns(
            alt_id=pl.col("tour_mode").replace_strict(mode_ids, return_dtype=pl.Int64)
        )

        # A mode is available if all its alternative-varying variables are defined and if the
        # mode-availability rules are satisfied.
        rules = self.availability_rules()
        warn_missing_availability_columns(modes, rules, tours.columns)
        avail_exprs = {}
        for mode in modes:
            conditions = [
                pl.col(generic_columns[v][repr(mode)]).is_not_null() for v in generic_columns
            ]
            conditions.extend(mode_availability_conditions(mode, rules, tours.columns))
            avail_exprs[f"avail_{mode!r}"] = (
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
            chosen_avail=pl.concat_list([pl.col(f"avail_{mode!r}") for mode in modes]).list.get(
                "alt_id"
            )
        )
        n0 = len(tours)
        tours = tours.filter(chosen_avail=1)
        n1 = len(tours)
        if n1 < n0:
            logger.warning(
                f"Dropping {n0 - n1:,} observations ({(n0 - n1) / n0:.2%}) whose chosen mode is "
                "unavailable."
            )

        # Normalize the survey weights so that they sum to the number of observations: the
        # log-likelihood (and thus the standard errors) then reflects the actual sample size
        # rather than the size of the population represented by the survey.
        tours = tours.with_columns(weight=pl.col("weight") / pl.col("weight").mean())

        columns = [
            "alt_id",
            "weight",
            *(f"avail_{mode!r}" for mode in modes),
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
            repr(reference_mode),
            generic_columns,
            generic_variables,
            case_variables,
            interactions,
            reference_vot,
            vot_aliases,
            out_dir=self.output["results"].complete_path.parent,
            model_name=type(self).__name__,
        )

        # The specification is stored next to the estimated values so that a downstream step can
        # interpret the parameter names (which variables are alternative-varying, which pairs are
        # interacted, which mode is the reference) without duplicating the config keys.
        specification = {
            "modes": sorted(map(repr, modes)),
            "reference_mode": repr(reference_mode),
            "variables": case_variables + generic_variables,
            "interaction_variables": interactions,
            "derived_variables": derived_variables,
            "reference_value_of_time": None
            if reference_vot is None
            else {"mode": reference_vot[0], "value": reference_vot[1]},
            "shared_car_driver_value_of_time": bool(vot_aliases),
        }
        results = {"specification": specification, "stats": stats, "parameters": params}

        self.output["results"].write(results)


def check_reference_vot(
    reference_vot: float | None,
    reference_vot_mode: str | None,
    generic_variables: list[str],
    modes: list[MetaMode],
) -> tuple[str, float] | None:
    """Validates the reference value-of-time parameters and returns them as a
    `(mode, value of time)` tuple, or `None` if they are not defined.
    """
    if reference_vot is None and reference_vot_mode is None:
        return None
    if reference_vot is None or reference_vot_mode is None:
        raise MetropyError(
            "`mode_choice_estimation.reference_value_of_time` and "
            "`mode_choice_estimation.reference_value_of_time_mode` must be defined together."
        )
    if TRAVEL_TIME_VARIABLE not in generic_variables:
        raise MetropyError(
            "`mode_choice_estimation.reference_value_of_time` requires the "
            f"`{TRAVEL_TIME_VARIABLE}` variable in `mode_choice_estimation.variables`."
        )
    mode = mode_from_str(reference_vot_mode)
    if mode not in modes:
        hint = (
            " (car modes might be grouped as `car_driver`)"
            if mode.is_car_based() and any(m.is_car_based() for m in modes)
            else ""
        )
        raise MetropyError(
            f"`mode_choice_estimation.reference_value_of_time_mode` (`{mode!r}`) must be one of "
            f"the estimated modes (`{', '.join(map(repr, modes))}`){hint}."
        )
    return repr(mode), reference_vot


def check_shared_car_driver_vot(
    enabled: bool,
    modes: list[MetaMode],
    all_generic_variables: list[str],
    reference_vot: tuple[str, float] | None,
) -> dict[str, str]:
    """Validates the `shared_car_driver_value_of_time` parameter and returns the
    `{mode: source mode}` mapping of the modes whose `travel_time` coefficients are those of
    another mode (empty if the parameter is disabled).

    The car-driver modes (car-based modes where the traveler is the driver) all share the
    coefficients of a single source mode: the reference value-of-time mode if it is a car-driver
    mode (so that the constraint applies to all of them), or the first one in alphabetical order
    otherwise.
    """
    if not enabled:
        return {}
    driver_modes = sorted(repr(m) for m in modes if isinstance(m, CarMode) and m.is_driver())
    if len(driver_modes) < 2:
        raise MetropyError(
            "`mode_choice_estimation.shared_car_driver_value_of_time` requires at least two "
            "car-driver modes (e.g., `car_driver` and `car_driver_with_passengers`) among the "
            f"estimated modes (`{', '.join(map(repr, modes))}`); car modes might be grouped as "
            "`car_driver`."
        )
    if TRAVEL_TIME_VARIABLE not in all_generic_variables:
        raise MetropyError(
            "`mode_choice_estimation.shared_car_driver_value_of_time` requires the "
            f"`{TRAVEL_TIME_VARIABLE}` variable in `mode_choice_estimation.variables` or "
            "`mode_choice_estimation.interaction_variables`."
        )
    if reference_vot is not None and reference_vot[0] in driver_modes:
        source = reference_vot[0]
    else:
        source = driver_modes[0]
    return {mode: source for mode in driver_modes if mode != source}


def estimate_mnl(
    df: pd.DataFrame,
    mode_ids: dict[str, int],
    reference_mode: str,
    generic_columns: dict[str, dict[str, str]],
    generic_variables: list[str],
    case_variables: list[str],
    interactions: list[list[str]],
    reference_vot: tuple[str, float] | None,
    vot_aliases: dict[str, str] | None = None,
    *,
    out_dir: Path,
    model_name: str,
):
    """Estimates a Multinomial Logit model of `alt_id` from `df` with biogeme, and returns
    `(parameters, stats)`, two plain, JSON-serializable dicts.

    `generic_columns` maps every alternative-varying variable (whether listed in
    `generic_variables` or only used inside `interactions`) to its `{mode: column_name}` mapping
    (see `generic_variable_mode`); `generic_variables` and `case_variables` are the variables for
    which a standalone term is added (one coefficient per mode, except for the variables in
    `GENERIC_COEFFICIENT_VARIABLES` which have a single coefficient shared by all modes);
    `interactions` are pairs of variables (either) whose
    product is added as an extra term, per-mode (if either variable is alternative-varying, i.e.,
    a key of `generic_columns`) or per-non-reference-mode (otherwise), mirroring the treatment of
    a plain generic/case variable.

    If `reference_vot` is a `(mode, value of time)` tuple, the `travel_time` coefficient of that
    mode is not estimated but constrained to be equal to the value of time (€/h) times the
    `monetary_cost` coefficient (estimated even if `monetary_cost` is not in `generic_variables`).
    This constrained coefficient is added to the returned parameters, with a `derived` flag.

    `vot_aliases` maps modes to another mode whose `travel_time` coefficients (standalone and
    interacted) they share. The shared coefficients are estimated (or constrained by
    `reference_vot`) under the name of the source mode and copied to the name of the aliased mode
    in the returned parameters, with a `derived` flag and a `shared_with` key.
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

    # Marginal utility of money, shared by all the terms in which it appears.
    b_money = Beta(f"B_{MONETARY_COST_VARIABLE}", 0.0, None, None, 0)

    vot_aliases = vot_aliases or {}

    def is_travel_time_term(v1: str, v2: str) -> bool:
        return TRAVEL_TIME_VARIABLE in (v1, v2)

    def generic_coefficient(variable: str, mode: str) -> Expression:
        if variable == MONETARY_COST_VARIABLE:
            return b_money
        if variable == TRAVEL_TIME_VARIABLE:
            mode = vot_aliases.get(mode, mode)
        if (
            variable == TRAVEL_TIME_VARIABLE
            and reference_vot is not None
            and mode == reference_vot[0]
        ):
            return reference_vot[1] * b_money
        return Beta(f"B_{variable}_{mode}", 0.0, None, None, 0)

    utilities: dict[int, Expression | float] = {}
    for mode, alt_id in mode_ids.items():
        terms = [
            generic_coefficient(variable, mode) * Variable(generic_columns[variable][mode])
            for variable in generic_variables
        ]
        terms.extend(
            Beta(
                f"B_{v1}_x_{v2}_"
                f"{vot_aliases.get(mode, mode) if is_travel_time_term(v1, v2) else mode}",
                0.0,
                None,
                None,
                0,
            )
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

    if reference_vot is not None:
        mode, vot = reference_vot
        money = parameters[f"B_{MONETARY_COST_VARIABLE}"]
        parameters[f"B_{TRAVEL_TIME_VARIABLE}_{mode}"] = {
            "value": vot * money["value"],
            "robust_std_err": vot * money["robust_std_err"],
            "robust_t_stat": money["robust_t_stat"],
            "robust_p_value": money["robust_p_value"],
            "derived": True,
        }

    shared_names = [f"{v1}_x_{v2}" for v1, v2 in interactions if is_travel_time_term(v1, v2)]
    if TRAVEL_TIME_VARIABLE in generic_variables:
        shared_names.append(TRAVEL_TIME_VARIABLE)
    for mode, source in vot_aliases.items():
        for name in shared_names:
            parameters[f"B_{name}_{mode}"] = {
                **parameters[f"B_{name}_{source}"],
                "derived": True,
                "shared_with": source,
            }

    stats = results.get_general_statistics()

    return parameters, stats

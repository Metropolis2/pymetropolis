from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from loguru import logger

from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_demand.modes import MODE_PREFERENCES_FILES
from pymetropolis.metro_demand.population.files import ToursFile
from pymetropolis.metro_pipeline import PopulationStep
from pymetropolis.metro_pipeline.parameters import BoolParameter, FloatParameter, StringParameter
from pymetropolis.modes import StepWithModes

from .files import SurveyModeChoiceResultsFile
from .mode_choice import ALTERNATIVE_VARYING_VARIABLES, TRAVEL_TIME_VARIABLE

if TYPE_CHECKING:
    import polars as pl

# Prefix of the mode names that can fall back to the coefficients estimated for `car_driver` when
# `mode_classifier.group_car_driver_modes` grouped all car-driver modes together.
CAR_MODE_PREFIX = "car"

# Mode whose coefficients are used by a car mode that was not estimated on its own.
GROUPED_CAR_MODE = "car_driver"


def resolve_estimated_mode(mode: str, spec: dict[str, Any]) -> str:
    """Returns the name of the alternative of the estimated model to be used for `mode`.

    A mode estimated on its own uses its own coefficients. A car mode that was not estimated on its
    own (e.g., because `mode_classifier.group_car_driver_modes` is `true`) falls back to the
    coefficients of the grouped `car_driver` alternative.
    """
    modes = spec["modes"]
    if mode in modes:
        return mode
    if mode.startswith(CAR_MODE_PREFIX) and GROUPED_CAR_MODE in modes:
        logger.info(
            f"Mode `{mode}` was not estimated in the mode-choice model: using the coefficients of "
            f"`{GROUPED_CAR_MODE}`."
        )
        return GROUPED_CAR_MODE
    raise MetropyError(
        f"Mode `{mode}` is not an alternative of the estimated mode-choice model "
        f"(`{', '.join(modes)}`). Either add it to `mode_classifier.modes` or generate its "
        "preferences from another source."
    )


def _is_generic(variable: str) -> bool:
    return variable in ALTERNATIVE_VARYING_VARIABLES


def _variable_expr(variable: str):
    """Returns the value of `variable` as a float, with the null values replaced by zeros."""
    import polars as pl

    return pl.col(variable).cast(pl.Float64).fill_null(0.0)


def _constant_terms(spec: dict[str, Any], mode: str) -> list[tuple[str, list[str]]]:
    """Returns the `(coefficient name, interacted variables)` pairs entering the constant of
    `mode`.
    """
    terms: list[tuple[str, list[str]]] = [(f"ASC_{mode}", [])]
    terms.extend((f"B_{v}_{mode}", [v]) for v in spec["variables"] if not _is_generic(v))
    terms.extend(
        (f"B_{v1}_x_{v2}_{mode}", [v1, v2])
        for v1, v2 in spec["interaction_variables"]
        if not _is_generic(v1) and not _is_generic(v2)
    )
    return terms


def _value_of_time_terms(spec: dict[str, Any], mode: str) -> list[tuple[str, list[str]]]:
    """Returns the `(coefficient name, interacted variables)` pairs entering the value of time of
    `mode`.
    """
    terms: list[tuple[str, list[str]]] = []
    if TRAVEL_TIME_VARIABLE in spec["variables"]:
        terms.append((f"B_{TRAVEL_TIME_VARIABLE}_{mode}", []))
    for v1, v2 in spec["interaction_variables"]:
        if v1 != TRAVEL_TIME_VARIABLE and v2 != TRAVEL_TIME_VARIABLE:
            continue
        if v1 == v2:
            # Travel time x travel time interaction.
            raise MetropyError(
                "Pymetropolis cannot handle quadratic utility functions of travel time."
            )
        name = f"B_{v1}_x_{v2}_{mode}"
        other = v2 if v1 == TRAVEL_TIME_VARIABLE else v1
        if _is_generic(other):
            logger.warning(
                f"Ignoring the `{name}` coefficient: `{other}` is an alternative-varying "
                "variable, which cannot be represented in the tour-level preferences."
            )
            continue
        terms.append((name, [other]))
    return terms


def _utility_expr(terms: list[tuple[str, list[str]]], params: dict[str, dict[str, float]]):
    """Returns the penalty (in utils) represented by `terms`, i.e., minus the sum of the terms'
    utilities.
    """
    import polars as pl

    expr = pl.lit(0.0, dtype=pl.Float64)
    for name, variables in terms:
        if name not in params:
            # The coefficient is not estimated for this mode (e.g., the constant and the case
            # variables of the reference mode), so the term is zero.
            continue
        term = pl.lit(params[name]["value"], dtype=pl.Float64)
        for variable in variables:
            term = term * _variable_expr(variable)
        expr = expr + term
    return -expr


def get_utility_scale(vot: pl.Series, reference_value_of_time: float, mode: str) -> float:
    """Returns the marginal utility of money (utils per euro) implied by
    `reference_value_of_time` (€/h) being the *average* value of time of `mode` in the population.

    `vot` is the value of time of each tour for that mode, in (minus) utils per hour, as returned
    by `compute_mode_preferences`.
    """
    mean_vot: float = vot.mean()  # ty: ignore[invalid-assignment]
    if mean_vot <= 0.0:
        raise MetropyError(
            f"The average value of time of mode `{mode}` in the population is not positive "
            f"({mean_vot:.4f} utils/h), so it cannot be used to convert the utilities to euros."
        )
    return mean_vot / reference_value_of_time


def compute_mode_preferences(
    tours: pl.DataFrame, spec: dict[str, Any], params: dict[str, dict[str, float]], mode: str
) -> pl.DataFrame:
    """Computes the constant and the value of time of `mode`, for each tour of `tours`, from the
    coefficients of the estimated mode-choice model.

    `spec` and `params` are the two parts of `SurveyModeChoiceResultsFile` and `mode` is the
    simulated mode whose preferences are generated.

    The returned DataFrame has a `tour_id` column, a `{mode}_cst` column (in euros, per tour) and a
    `{mode}_vot` column (in euros per hour). Both are penalties, i.e., they are the *opposite* of
    the estimated utilities.
    """
    estimated_mode = resolve_estimated_mode(mode, spec)
    cst_terms = _constant_terms(spec, estimated_mode)
    vot_terms = _value_of_time_terms(spec, estimated_mode)

    # Only the variables of the coefficients that are actually estimated for this mode are needed.
    variables = sorted({v for name, vs in cst_terms + vot_terms if name in params for v in vs})
    missing = [v for v in variables if v not in tours.columns]
    if missing:
        raise MetropyError(
            "Variables of the estimated mode-choice model are missing from the tours in ToursFile: "
            f"{', '.join(missing)}."
        )
    for variable in variables:
        nb_nulls = tours[variable].null_count()
        if nb_nulls:
            logger.warning(
                f"Variable `{variable}` is null for {nb_nulls:,} tours "
                f"({nb_nulls / len(tours):.2%}): the corresponding terms of the `{mode}` "
                "preferences are set to zero."
            )

    return tours.select(
        "tour_id",
        _utility_expr(cst_terms, params).alias(f"{mode}_cst"),
        _utility_expr(vot_terms, params).alias(f"{mode}_vot"),
    )


class ModePreferencesFromEconometricsStep(StepWithModes, PopulationStep):
    """Generates the preference parameters for each mode and each tour from the coefficients of the
    econometric mode-choice model.

    For each mode, the following parameters are generated:

    - constant: penalty of traveling with the mode, *per tour*, from the alternative-specific
      constant and the case variables of the estimated model
    - value of time / alpha: penalty per hour spent traveling with the mode, from the `travel_time`
      coefficient of the estimated model

    To enable this Step, set the
    [`from_econometrics`](parameters.md#mode_preferencesfrom_econometrics) parameter to `true`.

    The coefficients are estimated in utils, whereas the preference parameters are expressed in
    euros.
    They are converted using the
    [`reference_value_of_time`](parameters.md#mode_preferencesreference_value_of_time)
    parameter: all coefficients are divided by the marginal utility of money implied by the
    requested value of time for the
    [`reference_value_of_time_mode`](parameters.md#mode_preferencesreference_value_of_time_mode)
    mode.
    """

    from_econometrics = BoolParameter(
        "mode_preferences.from_econometrics",
        default=False,
        description=(
            "If `true`, the preference parameters of the simulated modes are computed from the "
            "coefficients of the econometric mode-choice model."
        ),
    )
    reference_vot = FloatParameter(
        "mode_preferences.reference_value_of_time",
        lower_bound=0.0,
        description=(
            "Value of time of the `mode_preferences.reference_value_of_time_mode` mode (€/h), used "
            "to convert the utilities of the econometric mode-choice model to euros."
        ),
        note="This is the *average* value of time in the population for this mode.",
    )
    reference_vot_mode = StringParameter(
        "mode_preferences.reference_value_of_time_mode",
        description=(
            "Mode whose value of time is set to `mode_preferences.reference_value_of_time`."
        ),
    )

    input_files = {"tours": ToursFile, "results": SurveyModeChoiceResultsFile}
    output_files = dict(MODE_PREFERENCES_FILES)

    def is_defined(self):
        return bool(self.from_econometrics) and self.has_trip_mode()

    def run(self):
        import polars as pl

        assert self.modes is not None

        tours: pl.DataFrame = self.input["tours"].read()
        results = json.loads(self.input["results"].read())
        spec = results["specification"]
        params = results["parameters"]

        modes = [mode for mode in self.modes if mode in MODE_PREFERENCES_FILES]
        mode_prefs = dict()
        for mode in modes:
            mode_prefs[mode] = compute_mode_preferences(tours, spec, params, mode)

        if self.reference_vot is not None and self.reference_vot_mode is not None:
            ref_mode = self.reference_vot_mode
            if ref_mode not in mode_prefs:
                raise MetropyError(
                    f"`mode_preferences.reference_value_of_time_mode` (`{ref_mode}`) must be one "
                    f"of the simulated trip-based modes (`{', '.join(modes)}`)."
                )
            # The scale is set so that the average VOT in the population (among all tours) for
            # the reference mode is `mode_preferences.reference_value_of_time`.
            # At this point, the VOT is in (minus) utils.
            scale = get_utility_scale(
                mode_prefs[ref_mode][f"{ref_mode}_vot"], self.reference_vot, ref_mode
            )
            logger.debug(
                f"Marginal utility of money implied by the estimated model: {scale:.4g} /€."
            )
        else:
            scale = 1.0
            logger.warning(
                "`mode_preferences.reference_value_of_time` or "
                "`mode_preferences.reference_value_of_time_mode` are not defined: "
                "cannot convert utilities of the econometric mode-choice model to euros. "
                "All preference parameters will be interpreted as utils."
            )

        for mode in modes:
            df = mode_prefs[mode].with_columns(pl.col(f"{mode}_cst", f"{mode}_vot") / scale)
            self.output[mode].write(df)

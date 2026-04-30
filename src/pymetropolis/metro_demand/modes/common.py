import inspect

import polars as pl
from loguru import logger

from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_demand.population.files import PersonsFile
from pymetropolis.metro_pipeline import Step
from pymetropolis.metro_pipeline.parameters import PathParameter
from pymetropolis.random import FloatDistributionParameter, RandomStep, generate_values


def pref_constant_parameter(mode: str):
    return FloatDistributionParameter(
        f"modes.{mode}.constant",
        default=0.0,
        description=f"Constant penalty for each {mode} trip (€).",
    )


def pref_value_of_time_parameter(mode: str):
    return FloatDistributionParameter(
        f"modes.{mode}.alpha", default=0.0, description=f"Value of time by {mode} (€/h)."
    )


def cst_preferences_step_docstring(mode: str):
    doc = f"""Generates the preference parameters of traveling by {mode}, for each trip, from
    exogenous values.

    The following parameters are generated:

    - constant: penalty of traveling by {mode}, *per trip*
    - value of time / alpha: penalty per hour spent traveling by {mode}

    The values can be constant over trips or sampled from a specific distribution.
    """
    return inspect.cleandoc(doc)


class PreferencesStep(RandomStep):
    """Abstract Step to generate the preference parameters of traveling from exogenous values."""

    constant = 0.0
    value_of_time = 0.0
    input_files = {"persons": PersonsFile}

    def is_defined(self):
        return self.constant != 0.0 or self.value_of_time != 0.0

    def get_preferences(self, mode: str, persons: pl.DataFrame):
        rng = self.get_rng()
        df = persons.select(
            "person_id",
            generate_values(self.constant, len(persons), rng).alias(f"{mode}_cst"),
            generate_values(self.value_of_time, len(persons), rng).alias(f"{mode}_vot"),
        )
        return df


def pref_file_parameter(mode: str):
    return PathParameter(
        f"modes.{mode}.preferences_file",
        check_file_exists=True,
        description=(
            "Path to a Parquet or CSV file with the constant and alpha values for different "
            "population segments."
        ),
        note=(
            "Possible columns: `mode`, `constant`, `alpha`, `value_of_time`, any persons' "
            "characteristics column from [`PersonsFile`](files.md#personsfile)."
        ),
    )


def preferences_step_docstring(mode: str):
    doc = f"""Generates the preference parameters of traveling by {mode}, for each trip, from
    constant values over population segments.

    The following parameters are generated:

    - constant: penalty of traveling by {mode}, *per trip*
    - value of time / alpha: penalty per hour spent traveling by {mode}

    The [`modes.{mode}.preferences_file`](parameters.md#modes{mode}preferences_file) parameter
    must point to a Parquet or CSV file with the constant and/or alpha value for the population
    segments.
    The file can have the following columns:

    - `constant`: constant penalty for each {mode} trip (default is 0 when omitted)
    - `alpha` or `value_of_time`: penalty per hour spent traveling by {mode} (default is 0 when
      omitted)
    - `mode`: if present, only rows with `mode = "{mode}"` are used
    - any column representing persons' characteristics from [`PersonsFile`](files.md#personsfile)

    For example, to set different preferences for men and women, you can use the following CSV file:

    ```csv
    mode,woman,constant,value_of_time
    {mode},true,-2,15
    {mode},false,-1,12
    ```
    """
    return inspect.cleandoc(doc)


class ModePreferencesFromPopulationStep(Step):
    """Abstract Step to generate the preference parameters for a given mode from constant values
    over population segments.
    """

    pref_file = None
    input_files = {"persons": PersonsFile}

    def is_defined(self):
        return self.pref_file is not None

    def get_person_preferences(self, persons: pl.DataFrame, pref: pl.DataFrame, mode: str):
        # Filter by mode if the column is present.
        if "mode" in pref.columns:
            pref = pref.filter(mode=mode)
        if pref.is_empty():
            raise MetropyError(f'No preference values for mode "{mode}" in `{self.pref_file}`.')
        # Check that the value of time column is present at most once.
        alpha_columns = {"alpha", "value_of_time"}
        if sum(col in pref.columns for col in alpha_columns) > 1:
            raise MetropyError(
                f"File `{self.pref_file}` has multiple columns for the value of time."
            )
        # Rename constant and value of time columns to the correct output names.
        pref = pref.rename(
            {"constant": f"{mode}_cst", "alpha": f"{mode}_vot", "value_of_time": f"{mode}_vot"},
            strict=False,
        )
        # Find the common person characteristics columns.
        characs_columns = set(pref.columns) & set(persons.columns)
        # Send a warning for unused columns in the input file.
        unused_columns = (
            set(pref.columns).difference(characs_columns).difference({"constant"} | alpha_columns)
        )
        if unused_columns:
            for col in unused_columns:
                logger.warning(f"Column `{col}` is ignored (not a valid person characteristic).")
            pref = pref.drop(list(unused_columns))
        # Raise an error if there is no valid column to match persons.
        if not characs_columns:
            raise MetropyError(
                f"No valid persons' characteristics column in file `{self.pref_file}`"
            )
        # Cast input columns to the expected dtype.
        for col in characs_columns:
            dtype = persons.schema[col]
            try:
                pref = pref.with_columns(pl.col(col).cast(dtype))
            except Exception:
                raise MetropyError(
                    f"Cannot cast column {col} to {dtype} in file `{self.pref_file}`"
                )
        df = persons.select("person_id", *characs_columns).join(
            pref, on=list(characs_columns), how="left"
        )
        return df

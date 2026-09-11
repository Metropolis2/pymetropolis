from __future__ import annotations

import inspect
from typing import TYPE_CHECKING

from loguru import logger

from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_common.io import read_dataframe
from pymetropolis.metro_demand.population.files import ToursFile
from pymetropolis.metro_pipeline import PopulationStep
from pymetropolis.metro_pipeline.parameters import PathParameter
from pymetropolis.random import FloatDistributionParameter, RandomStep, generate_values

if TYPE_CHECKING:
    import polars as pl


def pref_constant_parameter(mode: str):
    return FloatDistributionParameter(
        f"modes.{mode}.constant",
        default=0.0,
        description=f"Constant penalty for each {mode} tour (€).",
    )


def pref_value_of_time_parameter(mode: str):
    return FloatDistributionParameter(
        f"modes.{mode}.alpha", default=0.0, description=f"Value of time by {mode} (€/h)."
    )


def cst_preferences_step_docstring(mode: str):
    doc = f"""Generates the preference parameters of traveling by {mode}, for each tour, from
    exogenous values.

    The following parameters are generated:

    - constant: penalty of traveling by {mode}, *per tour*
    - value of time / alpha: penalty per hour spent traveling by {mode}

    The values can be constant over tours or sampled from a specific distribution.
    """
    return inspect.cleandoc(doc)


class PreferencesStep(RandomStep, PopulationStep):
    """Abstract Step to generate the preference parameters of traveling from exogenous values."""

    _mode: ClassVar[str | None] = None

    constant = 0.0
    value_of_time = 0.0
    input_files = {"tours": ToursFile}

    def is_defined(self):
        return self.constant != 0.0 or (self.value_of_time != 0.0 and self._mode is not None)

    def run(self):
        tours: pl.DataFrame = self.input["tours"].read()
        df = self.get_preferences(tours)
        self.output["preferences"].write(df)

    def get_preferences(self, tours: pl.DataFrame):
        assert self._mode is not None
        rng = self.get_rng(str(self))
        df = tours.select(
            "tour_id",
            generate_values(self.constant, len(tours), rng).alias(f"{self._mode}_cst"),
            generate_values(self.value_of_time, len(tours), rng).alias(f"{self._mode}_vot"),
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
            "Possible columns: `mode`, `constant`, `alpha`, `value_of_time`, any tour, person or "
            "household characteristics column from [`ToursFile`](files.md#toursfile)."
        ),
    )


def preferences_step_docstring(mode: str):
    doc = f"""Generates the preference parameters of traveling by {mode}, for each tour, from
    constant values over population segments.

    The following parameters are generated:

    - constant: penalty of traveling by {mode}, *per tour*
    - value of time / alpha: penalty per hour spent traveling by {mode}

    The [`modes.{mode}.preferences_file`](parameters.md#modes{mode}preferences_file) parameter
    must point to a Parquet or CSV file with the constant and/or alpha value for the population
    segments.
    The file can have the following columns:

    - `constant`: constant penalty for each {mode} tour (default is 0 when omitted)
    - `alpha` or `value_of_time`: penalty per hour spent traveling by {mode} (default is 0 when
      omitted)
    - `mode`: if present, only rows with `mode = "{mode}"` are used
    - any column representing tour, person or household characteristics from
      [`ToursFile`](files.md#toursfile)

    For example, to set different preferences for men and women, you can use the following CSV file:

    ```csv
    mode,woman,constant,value_of_time
    {mode},true,-2,15
    {mode},false,-1,12
    ```
    """
    return inspect.cleandoc(doc)


class ModePreferencesFromPopulationStep(PopulationStep):
    """Abstract Step to generate the preference parameters for a given mode from constant values
    over population segments.
    """

    _mode: str | None = None

    pref_file: PathParameter | None = None
    input_files = {"tours": ToursFile}

    def is_defined(self):
        return self.pref_file is not None

    def run(self):
        assert self.pref_file is not None
        tours: pl.DataFrame = self.input["tours"].read()
        pref = read_dataframe(self.pref_file)
        df = self.get_tour_preferences(tours, pref)
        self.output["preferences"].write(df)

    def get_tour_preferences(self, tours: pl.DataFrame, pref: pl.DataFrame):
        assert self._mode is not None
        # Filter by mode if the column is present.
        if "mode" in pref.columns:
            pref = pref.filter(mode=self._mode).drop("mode")
        if pref.is_empty():
            raise MetropyError(
                f'No preference values for mode "{self._mode}" in `{self.pref_file}`.'
            )
        # Check that the value of time column is present at most once.
        alpha_columns = {"alpha", "value_of_time"}
        if sum(col in pref.columns for col in alpha_columns) > 1:
            raise MetropyError(
                f"File `{self.pref_file}` has multiple columns for the value of time."
            )
        # Rename constant and value of time columns to the correct output names.
        pref = pref.rename(
            {
                "constant": f"{self._mode}_cst",
                "alpha": f"{self._mode}_vot",
                "value_of_time": f"{self._mode}_vot",
            },
            strict=False,
        )
        # Find the common tour characteristics columns.
        characs_columns = set(pref.columns) & set(tours.columns)
        # Send a warning for unused columns in the input file.
        unused_columns = (
            set(pref.columns)
            .difference(characs_columns)
            .difference({f"{self._mode}_cst", f"{self._mode}_vot"})
        )
        if unused_columns:
            for col in unused_columns:
                logger.warning(f"Column `{col}` is ignored (not a valid tour characteristic).")
            pref = pref.drop(list(unused_columns))
        # Raise an error if there is no valid column to match tours.
        if not characs_columns:
            raise MetropyError(f"No valid tours' characteristics column in file `{self.pref_file}`")
        # Cast input columns to the expected dtype.
        for col in characs_columns:
            dtype = tours.schema[col]
            try:
                pref = pref.cast({col: dtype})
            except Exception:
                raise MetropyError(
                    f"Cannot cast column {col} to {dtype} in file `{self.pref_file}`"
                )
        df = (
            tours.select("tour_id", *characs_columns)
            .join(pref, on=list(characs_columns), how="left")
            .drop(characs_columns)
        )
        return df

from loguru import logger

from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_common.io import read_dataframe
from pymetropolis.metro_demand.modes.files import ModeChoiceMuFile
from pymetropolis.metro_demand.population.files import ToursFile
from pymetropolis.metro_pipeline import PopulationStep
from pymetropolis.metro_pipeline.parameters import PathParameter
from pymetropolis.random import FloatDistributionParameter, RandomStep, generate_values


class ModeChoiceMuStep(RandomStep, PopulationStep):
    """Generates the mode choice error scales from exogenous values."""

    mode_choice_mu = FloatDistributionParameter(
        "mode_choice.mu", default=1.0, description="Error scale for Logit mode-choice models (€)."
    )

    input_files = {"tours": ToursFile}
    output_files = {"mu": ModeChoiceMuFile}
    priority = 0

    def run(self):
        assert self.mode_choice_mu is not None
        tours = self.input["tours"].read()
        rng = self.get_rng(str(self))
        df = tours.select(
            "tour_id", mode_choice_mu=generate_values(self.mode_choice_mu, len(tours), rng)
        )
        self.output["mu"].write(df)


class ModeChoiceMuFromPopulationStep(PopulationStep):
    """Generates the mode choice error scales from constant values over population segments."""

    mu_file = PathParameter(
        "mode_choice.mu_file",
        check_file_exists=True,
        description=(
            "Path to a Parquet or CSV file with the mu values for different population segments."
        ),
        note=(
            "Possible columns: `mu`, any tour, person or household characteristics column from "
            "[`ToursFile`](files.md#toursfile)."
        ),
    )

    input_files = {"tours": ToursFile}
    output_files = {"mu": ModeChoiceMuFile}
    priority = 0

    def is_defined(self):
        return self.mu_file is not None

    def run(self):
        assert self.mu_file is not None
        tours = self.input["tours"].read()
        mus = read_dataframe(self.mu_file)
        if mus.is_empty():
            raise MetropyError(f"No values in file `{self.mu_file}`.")
        # Check that the mu column is present.
        if "mu" not in mus.columns:
            raise MetropyError(f"File `{self.mu_file}` has no `mu` column.")
        # Rename mu column to the correct output name.
        mus = mus.rename({"mu": "mode_choice_mu"})
        # Find the common tour characteristics columns.
        characs_columns = set(mus.columns) & set(tours.columns)
        # Send a warning for unused columns in the input file.
        unused_columns = set(mus.columns).difference(characs_columns).difference({"mode_choice_mu"})
        if unused_columns:
            for col in unused_columns:
                logger.warning(f"Column `{col}` is ignored (not a valid tour characteristic).")
            mus = mus.drop(list(unused_columns))
        # Raise an error if there is no valid column to match tours.
        if not characs_columns:
            raise MetropyError(f"No valid tours' characteristics column in file `{self.mu_file}`")
        # Cast input columns to the expected dtype.
        for col in characs_columns:
            dtype = tours.schema[col]
            try:
                mus = mus.cast({col: dtype})
            except Exception:
                raise MetropyError(f"Cannot cast column {col} to {dtype} in file `{self.mu_file}`")
        df = (
            tours.select("tour_id", *characs_columns)
            .join(mus, on=list(characs_columns), how="left")
            .drop(characs_columns)
        )
        self.output["mu"].write(df)

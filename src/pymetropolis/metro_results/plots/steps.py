import matplotlib.pyplot as plt
import polars as pl

from pymetropolis.metro_common.utils import time_to_seconds_since_midnight_pl
from pymetropolis.metro_pipeline import Step
from pymetropolis.metro_results.demand import TripResultsFile

from .files import TripDepartureTimeDistributionPlotFile


class TripDepartureTimeDistributionStep(Step):
    """Generates a histogram of departure-time distribution at the trip level."""

    output_files = {"trip_departure_time_distribution_plot": TripDepartureTimeDistributionPlotFile}

    def required_files(self):
        return {"trip_results": TripResultsFile}

    def run(self):
        df = self.input["trip_results"].read()
        fig, ax = plt.subplots()
        # TODO. Make this a proper Time xaxis (not just hours)
        ax.hist(
            df.select(
                time_to_seconds_since_midnight_pl(pl.col("departure_time")) / 3600
            ).to_series(),
            bins=120,
            density=True,
            alpha=0.9,
            histtype="step",
        )
        ax.set_xlabel("Departure time")
        ax.set_ylabel("Density")
        ax.set_ylim(bottom=0)
        ax.grid()
        fig.tight_layout()
        self.output["trip_departure_time_distribution_plot"].write(fig)

import matplotlib.pyplot as plt
import polars as pl

from pymetropolis.metro_common.errors import error_context
from pymetropolis.metro_common.utils import time_to_seconds_since_midnight_pl
from pymetropolis.metro_pipeline import Config, Step
from pymetropolis.metro_pipeline.file import MetroPlotFile
from pymetropolis.metro_simulation.results import TRIP_RESULTS_FILE

TRIP_DEPARTURE_TIME_DISTRIBUTION_PLOT_FILE = MetroPlotFile(
    "departure_time_distribution_plot",
    path="results/graphs/departure_time_distribution.pdf",
    description="TODO",
)


@error_context(msg="Cannot plot trip departure-time distribution")
def plot_trip_departure_time_distribution(config: Config):
    df = TRIP_RESULTS_FILE.read(config)
    fig, ax = plt.subplots()
    # TODO. Make this a proper Time xaxis (not just hours)
    ax.hist(
        df.select(time_to_seconds_since_midnight_pl(pl.col("departure_time")) / 3600).to_series(),
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
    TRIP_DEPARTURE_TIME_DISTRIBUTION_PLOT_FILE.save(fig, config)
    return True


TRIP_DEPARTURE_TIME_DISTRIBUTION_STEP = Step(
    "trip-departure-time-distribution",
    plot_trip_departure_time_distribution,
    required_files=[TRIP_RESULTS_FILE],
    output_files=[TRIP_DEPARTURE_TIME_DISTRIBUTION_PLOT_FILE],
)

CONFIG_SCHEMA = []
STEPS = [TRIP_DEPARTURE_TIME_DISTRIBUTION_STEP]

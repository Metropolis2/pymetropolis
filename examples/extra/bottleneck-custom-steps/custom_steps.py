import sys
from datetime import timedelta
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import polars as pl
from matplotlib.ticker import FuncFormatter
from shapely.geometry import LineString

from pymetropolis.metro_common.utils import (
    pl_duration_to_seconds,
    seconds_since_midnight_to_time_string,
)
from pymetropolis.metro_demand.population.draws import UniformDrawsStep
from pymetropolis.metro_network.road_network.files import (
    RoadEdgesCapacitiesFile,
    RoadEdgesCleanFile,
)
from pymetropolis.metro_pipeline import PopulationStep, Step
from pymetropolis.metro_pipeline.file import MetroPlotFile, PopulationFile
from pymetropolis.metro_pipeline.parameters import (
    BoolParameter,
    DurationParameter,
    FloatParameter,
    ListParameter,
    TimeParameter,
)
from pymetropolis.metro_pipeline.types import Time
from pymetropolis.metro_results.demand import TripResultsFile

# `analytical_solution.py` sits next to this file rather than in an importable package, so this
# directory is added to `sys.path` to import it directly.
sys.path.insert(0, str(Path(__file__).parent))
import analytical_solution

# Arbitrary length of the bottleneck edge, in meters. Only the ratio between `length` and
# `speed_limit` matters (it must be equal to `free_flow_travel_time`), so the actual value of
# `length` is not important.
EDGE_LENGTH = 1_000.0


class BottleneckNetworkStep(Step):
    """Generates a road network made of a single directed edge, going from node "origin" to node
    "destination", to be used as a bottleneck.
    """

    free_flow_travel_time = FloatParameter(
        "bottleneck_network.free_flow_travel_time",
        description="Free-flow travel time of the edge, in seconds.",
    )
    capacity = FloatParameter(
        "bottleneck_network.capacity",
        description="Bottleneck capacity of the edge, in PCE per hour.",
    )
    output_files = {"edges": RoadEdgesCleanFile, "capacities": RoadEdgesCapacitiesFile}

    def is_defined(self) -> bool:
        return self.free_flow_travel_time is not None and self.capacity is not None

    def run(self):
        assert self.free_flow_travel_time is not None
        assert self.capacity is not None

        speed_limit = EDGE_LENGTH / self.free_flow_travel_time * 3.6

        edges_df = pl.DataFrame(
            {
                "edge_id": ["edge"],
                "source": ["origin"],
                "target": ["destination"],
                "edge_type": ["Bottleneck"],
                "length": [EDGE_LENGTH],
                "speed_limit": [speed_limit],
                "default_speed_limit": [False],
                "lanes": [1.0],
                "hov_lanes": [0.0],
                "default_lanes": [False],
                "oneway": [True],
                "toll": [False],
                "roundabout": [False],
                "give_way": [False],
                "stop": [False],
                "traffic_signals": [False],
                "source_in_degree": [0],
                "source_out_degree": [1],
                "target_in_degree": [1],
                "target_out_degree": [0],
            },
            schema_overrides={
                "source_in_degree": pl.UInt8,
                "source_out_degree": pl.UInt8,
                "target_in_degree": pl.UInt8,
                "target_out_degree": pl.UInt8,
            },
        )
        edges = gpd.GeoDataFrame(
            edges_df.to_pandas(), geometry=[LineString([[0, 0], [EDGE_LENGTH, 0]])]
        )
        self.output["edges"].write(edges)

        capacities = pl.DataFrame(
            {"edge_id": ["edge"], "capacity": [self.capacity]},
            schema={"edge_id": pl.String, "capacity": pl.Float64},
        )
        self.output["capacities"].write(capacities)


class UniformDrawsStep(UniformDrawsStep):
    """Overrides the built-in `UniformDrawsStep` to optionally draw the inverse-transform-sampling
    epsilons with systematic sampling instead of independent uniform draws.

    Systematic sampling spreads the draws evenly over `[0, 1)` (`i / n` for `i` in
    `0, ..., n - 1`), which reduces sampling variance compared to independent uniform draws.
    """

    systematic_sampling = BoolParameter(
        "uniform_draws.systematic_sampling",
        default=True,
        description=(
            "Whether to draw the epsilons with systematic sampling instead of independent uniform "
            "draws."
        ),
    )

    # Note that input / output files don't need to be specified since they are inherited from the
    # original UniformDrawsStep.
    # They can still be accessed with `self.input["trips"]` and `self.output["uniform_draws"]`.

    def run(self):
        if not self.systematic_sampling:
            # Systematic sampling is not enabled, running the original function instead.
            super().run()
            return

        trips: pl.DataFrame = self.input["trips"].read()
        tour_ids = trips["tour_id"].unique().sort()
        nb_tours = len(tour_ids)
        draws = np.arange(nb_tours) / nb_tours
        df = pl.DataFrame({"tour_id": tour_ids, "mode_u": draws, "departure_time_u": draws})
        self.output["uniform_draws"].write(df)


class DepartureRateComparisonPlotFile(MetroPlotFile, PopulationFile):
    path = "results/graphs/{population}/trips/departure_rate_comparison.png"
    description = (
        "Comparison of the simulated departure rate from origin, at the trip level, against the "
        "analytical equilibrium departure rate of the stochastic bottleneck model."
    )


class DepartureRateComparisonStep(PopulationStep):
    """Generates a graph comparing the simulated departure rate from origin, at the trip level,
    against the analytical equilibrium departure rate of the stochastic bottleneck model (de
    Palma, Ben-Akiva, Lefevre, and Litinas, 1983), for the `car_driver` mode used by the bottleneck
    examples.
    """

    free_flow_travel_time = FloatParameter("bottleneck_network.free_flow_travel_time")
    capacity = FloatParameter("bottleneck_network.capacity")
    # Parameters below are all original parameters from Pymetropolis that we need to repeat here
    # to collect their values in the function.
    period = ListParameter("simulation.period", inner=Time(), length=2)
    mu = FloatParameter("departure_time_choice.mu", default=1.0)
    alpha = FloatParameter("modes.car_driver.alpha", default=0.0)
    beta = FloatParameter("departure_time.linear_schedule.beta", default=0.0)
    gamma = FloatParameter("departure_time.linear_schedule.gamma", default=0.0)
    delta = DurationParameter("departure_time.linear_schedule.delta", default=timedelta(0.0))
    tstar = TimeParameter("departure_time.linear_schedule.tstar")

    input_files = {"trip_results": TripResultsFile}
    output_files = {"plot": DepartureRateComparisonPlotFile}

    def is_defined(self) -> bool:
        return (
            self.free_flow_travel_time is not None
            and self.capacity is not None
            and self.tstar is not None
        )

    def run(self):
        assert self.free_flow_travel_time is not None
        assert self.capacity is not None
        assert self.period is not None
        assert self.mu is not None
        assert self.alpha is not None
        assert self.beta is not None
        assert self.gamma is not None
        assert self.delta is not None
        assert self.tstar is not None

        df: pl.DataFrame = self.input["trip_results"].read()
        t0 = self.period[0].seconds()
        t1 = self.period[1].seconds()
        params = {
            "n": df.height,
            "mu": self.mu,
            "bottleneck_flow": self.capacity / 3600.0,
            "alpha": self.alpha / 3600.0,
            "beta": self.beta / 3600.0,
            "gamma": self.gamma / 3600.0,
            "tstar": self.tstar.seconds(),
            "delta": self.delta.total_seconds(),
            "period": [t0, t1],
            "tt0": self.free_flow_travel_time,
        }
        times, denominator = analytical_solution.equilibrium(params)

        ts = np.linspace(t0, t1, 300)
        theoretical_rate = np.fromiter(
            (analytical_solution.dep_rate(t, denominator, times, params) for t in ts),
            dtype=np.float64,
        )

        departure_times = df.select(pl_duration_to_seconds("departure_time")).to_series().to_numpy()
        nb_bins = 60
        bin_edges = np.linspace(t0, t1, nb_bins + 1)
        counts, _ = np.histogram(departure_times, bins=bin_edges)
        bin_width = (t1 - t0) / nb_bins
        simulated_rate = counts / bin_width
        bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2

        fig, ax = plt.subplots()
        ax.step(bin_centers, simulated_rate, where="mid", alpha=0.9, label="Simulated")
        ax.plot(ts, theoretical_rate, alpha=0.9, label="Analytical")
        ax.set_xlabel("Departure time")
        ax.set_ylabel("Rate of departures from origin (trips/s)")
        ax.set_xlim(t0, t1)
        ax.set_ylim(bottom=0)
        ax.xaxis.set_major_formatter(
            FuncFormatter(lambda x, pos: seconds_since_midnight_to_time_string(x))
        )
        ax.legend()
        ax.grid()
        fig.tight_layout()
        self.output["plot"].write(fig)

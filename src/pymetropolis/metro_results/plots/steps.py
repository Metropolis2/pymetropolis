import matplotlib.pyplot as plt
import polars as pl
from matplotlib.ticker import FuncFormatter, PercentFormatter

from pymetropolis.metro_common.utils import (
    seconds_since_midnight_to_time_string,
    seconds_to_duration_string,
    time_to_seconds_since_midnight_pl,
)
from pymetropolis.metro_network.road_network import EdgesFreeFlowTravelTimeFile
from pymetropolis.metro_pipeline import Step
from pymetropolis.metro_results.aggregate import IterationResultsFile
from pymetropolis.metro_results.demand import TripResultsFile
from pymetropolis.metro_simulation.run import (
    MetroExpectedTravelTimeFunctionsFile,
    MetroSimulatedTravelTimeFunctionsFile,
)

from .files import (
    ExpectedRoadNetworkCongestionFunctionPlotFile,
    ExpectedRoadTravelTimesConvergencePlotFile,
    MeanSurplusConvergencePlotFile,
    RoadTripsShareConvergencePlotFile,
    RouteLengthDiffConvergencePlotFile,
    SimulatedRoadTravelTimesConvergencePlotFile,
    SimulationRoadNetworkCongestionFunctionPlotFile,
    TourDepartureTimeConvergencePlotFile,
    TripDepartureTimeDistributionPlotFile,
    TripModeSharesPlotFile,
)

# Define a color palette from Okabe and Ito.
ORANGE = "#E69F00"
LIGHTBLUE = "#56B4E9"
GREEN = "#009E73"
YELLOW = "#F0E442"
BLUE = "#0072B2"
RED = "#D55E00"
PINK = "#CC79A7"
BLACK = "#000000"
COLORS = [ORANGE, LIGHTBLUE, GREEN, BLUE, RED, PINK, YELLOW, BLACK]

PURPLE = "#9932CC"
TEAL = "#008080"


class ConvergencePlotStep(Step):
    """Generates various graphs to analyze the convergence of the simulation."""

    input_files = {"iteration_results": IterationResultsFile}
    output_files = {
        "tour_departure_time": TourDepartureTimeConvergencePlotFile,
        "simulated_travel_time": SimulatedRoadTravelTimesConvergencePlotFile,
        "expected_travel_time": ExpectedRoadTravelTimesConvergencePlotFile,
        "route_length_diff": RouteLengthDiffConvergencePlotFile,
        "surplus": MeanSurplusConvergencePlotFile,
        "road_trips_share": RoadTripsShareConvergencePlotFile,
    }

    def run(self):
        # TODO. What if the values are all null? (e.g., no trip simulation).
        # TODO. Add configuration for log vs linear scale (and labels?).
        df = self.input["iteration_results"].read()
        # Remove first iteration (no value).
        df = df[1:]
        xs = df["iteration"]
        # Plot graphs for the duration variables.
        for col, ofile, label in (
            ("rmse_tour_departure_time", "tour_departure_time", "Departure time RMSE"),
            (
                "rmse_simulated_road_travel_times",
                "simulated_travel_time",
                "Simulated edge-level travel times RMSE",
            ),
            (
                "rmse_expected_road_travel_times",
                "expected_travel_time",
                "Expected edge-level travel times RMSE",
            ),
        ):
            fig, ax = plt.subplots()
            ys = df[col].dt.total_nanoseconds() / 1e9
            ax.semilogy(xs, ys, alpha=0.9)
            ax.set_xlabel("Iteration")
            ax.set_ylabel(f"{label} (log scale)")
            ax.set_xlim(xs.min(), xs.max())
            # ax.set_ylim(bottom=0)
            ax.yaxis.set_major_formatter(
                FuncFormatter(lambda x, pos: seconds_to_duration_string(x))
            )
            ax.grid()
            fig.tight_layout()
            self.output[ofile].write(fig)
        # Plot graphs for the float variables.
        df = df.with_columns(
            road_trips_share=pl.col("nb_road_trips")
            / (pl.col("nb_road_trips") + pl.col("nb_non_road_trips"))
        )
        for col, ofile, label, bottom_to_zero, percent_format in (
            (
                "mean_road_trip_length_diff",
                "route_length_diff",
                "Route length difference (m)",
                True,
                False,
            ),
            ("mean_surplus", "surplus", "Mean surplus (€)", False, False),
            ("road_trips_share", "road_trips_share", "Share of road trips", False, True),
        ):
            fig, ax = plt.subplots()
            ys = df[col]
            ax.plot(xs, ys, alpha=0.9)
            ax.set_xlabel("Iteration")
            ax.set_ylabel(label)
            ax.set_xlim(xs.min(), xs.max())
            if bottom_to_zero:
                ax.set_ylim(bottom=0)
            if percent_format:
                ax.yaxis.set_major_formatter(PercentFormatter(xmax=1))
            ax.grid()
            fig.tight_layout()
            self.output[ofile].write(fig)


class TripDepartureTimeDistributionStep(Step):
    """Generates a histogram of departure-time distribution at the trip level."""

    input_files = {"trip_results": TripResultsFile}
    output_files = {"trip_departure_time_distribution_plot": TripDepartureTimeDistributionPlotFile}

    def run(self):
        # TODO. Add configuration for number of bins (and maybe axis labels?)
        df = self.input["trip_results"].read()
        fig, ax = plt.subplots()
        values = df.select(time_to_seconds_since_midnight_pl(pl.col("departure_time"))).to_series()
        ax.hist(
            values,
            bins=60,
            density=True,
            alpha=0.9,
            histtype="step",
        )
        ax.set_xlabel("Departure time")
        ax.set_ylabel("Density")
        ax.set_xlim(values.min(), values.max())
        ax.set_ylim(bottom=0)
        ax.xaxis.set_major_formatter(
            FuncFormatter(lambda x, pos: seconds_since_midnight_to_time_string(x))
        )
        ax.grid()
        fig.tight_layout()
        self.output["trip_departure_time_distribution_plot"].write(fig)


class RoadNetworkCongestionFunctionPlotsStep(Step):
    """Generates plots of expected and simulated congestion function over the entire road network."""

    input_files = {
        "edges_fftt": EdgesFreeFlowTravelTimeFile,
        "sim_ttfs": MetroSimulatedTravelTimeFunctionsFile,
        "exp_ttfs": MetroExpectedTravelTimeFunctionsFile,
    }
    output_files = {
        "sim_plot": SimulationRoadNetworkCongestionFunctionPlotFile,
        "exp_plot": ExpectedRoadNetworkCongestionFunctionPlotFile,
    }

    def run(self):
        edges_fftt = self.input["edges_fftt"].read()
        tot_fftt = edges_fftt["free_flow_travel_time"].sum().total_seconds()
        for x, label in (("sim", "Simulated"), ("exp", "Expected")):
            df = self.input[f"{x}_ttfs"].read()
            fig, ax = plt.subplots()
            # TODO. Which vehicle type to select?
            # For now, we take the fastest.
            df = (
                df.group_by("departure_time", "edge_id")
                .agg(tt=pl.col("travel_time").min())
                .group_by("departure_time")
                .agg(pl.col("tt").sum())
                .with_columns(cong=pl.col("tt") / tot_fftt - 1)
                .sort("departure_time")
            )
            ax.plot(df["departure_time"], df["cong"], alpha=0.9)
            ax.set_xlabel("Departure time")
            ax.set_ylabel(f"{label} road-network congestion")
            ax.set_xlim(df["departure_time"].min(), df["departure_time"].max())
            ax.set_ylim(bottom=0)
            ax.xaxis.set_major_formatter(
                FuncFormatter(lambda x, pos: seconds_since_midnight_to_time_string(x))
            )
            ax.yaxis.set_major_formatter(PercentFormatter(xmax=1))
            ax.grid()
            fig.tight_layout()
            self.output[f"{x}_plot"].write(fig)


class TripModeSharesStep(Step):
    """Generates a plot of mode shares at the trip level."""

    input_files = {"trip_results": TripResultsFile}
    output_files = {"plot": TripModeSharesPlotFile}

    def run(self):
        df = self.input["trip_results"].read()
        shares = df["mode"].value_counts(normalize=True, sort=True)
        fig, ax = plt.subplots()
        bars = ax.barh(
            y=shares["mode"],
            width=shares["proportion"],
            height=0.9,
            align="center",
            color=COLORS,
            zorder=1,
        )
        ax.bar_label(bars, fmt="{:.0%}", padding=5, zorder=3)
        ax.xaxis.set_major_formatter(PercentFormatter(xmax=1, decimals=0))
        ax.set_xlim(left=0)
        ax.tick_params(axis="y", which="both", length=0)
        ax.set_xlabel("Share")
        ax.grid(which="major", axis="x", zorder=2)
        fig.tight_layout(pad=0.5)
        self.output["plot"].write(fig)

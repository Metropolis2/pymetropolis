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
    SimulatedRoadTravelTimesConvergencePlotFile,
    SimulationRoadNetworkCongestionFunctionPlotFile,
    TourDepartureTimeConvergencePlotFile,
    TripDepartureTimeDistributionPlotFile,
)


class ConvergencePlotStep(Step):
    """Generates various graphs to analyze the convergence of the simulation."""

    input_files = {"iteration_results": IterationResultsFile}
    output_files = {
        "tour_departure_time": TourDepartureTimeConvergencePlotFile,
        "simulated_travel_time": SimulatedRoadTravelTimesConvergencePlotFile,
        "expected_travel_time": ExpectedRoadTravelTimesConvergencePlotFile,
    }

    def run(self):
        # TODO. What if the values are all null? (e.g., no trip simulation).
        # TODO. Add configuration for log vs linear scale (and labels?).
        df = self.input["iteration_results"].read()
        # Remove first iteration (no value).
        df = df[1:]
        xs = df["iteration"]
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
            df = (
                df.group_by("departure_time")
                .agg(pl.col("travel_time").sum())
                .with_columns(cong=pl.col("travel_time") / tot_fftt - 1)
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

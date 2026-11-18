from __future__ import annotations

from loguru import logger

from pymetropolis.metro_calibration.road.files import TargetRoutesFile
from pymetropolis.metro_calibration.road.utils import sum_edge_values_along_path
from pymetropolis.metro_common.plots import PlotStep, plot_travel_time_comparison
from pymetropolis.metro_network.road_network.files import RoadEdgesFreeFlowTravelTimeFile

from .files import FreeFlowTravelTimeComparisonPlotFile


class FreeFlowTravelTimeComparisonStep(PlotStep):
    """Compares observed and Metropolis-simulated free-flow travel times of the target routes."""

    input_files = {"edges_fftt": RoadEdgesFreeFlowTravelTimeFile, "routes": TargetRoutesFile}
    output_files = {"comparison_plot": FreeFlowTravelTimeComparisonPlotFile}

    def run(self):
        import polars as pl

        routes = self.input["routes"].scan()
        if "target_free_flow_tt" not in routes.collect_schema().names():
            logger.warning('No "target_free_flow_tt" in the target routes, skipping step.')
            return

        edges_fftt = self.input["edges_fftt"].read()

        edges_fftt = edges_fftt.with_columns(
            free_flow_travel_time=pl.col("free_flow_travel_time").dt.total_seconds(fractional=True)
        )

        df = routes.select(
            metropolis_tt=sum_edge_values_along_path(
                pl.col("path"), edges_fftt["edge_id"], edges_fftt["free_flow_travel_time"]
            ),
            target_tt=pl.col("target_free_flow_tt").dt.total_seconds(fractional=True),
        ).collect()

        observed = df["target_tt"].to_numpy()
        predicted = df["metropolis_tt"].to_numpy()
        rmse = float(((observed - predicted) ** 2).mean() ** 0.5)

        fig = plot_travel_time_comparison(
            observed,
            predicted,
            rmse,
            xlabel="Observed free-flow travel time",
            ylabel="Metropolis free-flow travel time",
        )
        self.output["comparison_plot"].write(fig)

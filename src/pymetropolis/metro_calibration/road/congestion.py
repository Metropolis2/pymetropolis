from __future__ import annotations

import json
import subprocess
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

from loguru import logger

from pymetropolis.common import ThreadedStep
from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_network.road_network.files import RoadEdgesCleanFile
from pymetropolis.metro_pipeline import Step
from pymetropolis.metro_simulation.parameters.step import StepWithPeriod
from pymetropolis.metro_simulation.run.exec import AbstractRunSimulationStep
from pymetropolis.metro_simulation.run.files import MetroExAnteSimulatedTravelTimeFunctionsFile

from .files import (
    CongestionTimeComparisonPlotFile,
    RoadEdgesPenaltiesFile,
    TomTomCongestionTimesFile,
    TomTomRoutesFile,
    TomTomRoutesMatchedFile,
)
from .plots import plot_travel_time_comparison

if TYPE_CHECKING:
    import polars as pl

# Vehicle type assigned to every replayed TomTom route: TomTom probes are single, unoccupied
# vehicles, so they are modeled as solo car drivers.
# In particular, this means that they cannot take HOV lanes.
CAR_VEHICLE_ID = "car_driver_alone"


def prepare_trips(
    routes: pl.DataFrame, matched_routes: pl.DataFrame, edges: pl.DataFrame, period: list
) -> pl.DataFrame:
    """Builds one row per map-matched TomTom route, with its departure time (in seconds since
    midnight) and its origin / destination node (derived from the first / last edge of the
    matched path, so they are guaranteed to be valid road-network nodes).
    """
    import polars as pl

    routes = routes.filter(pl.col("departure_time").dt.weekday() <= 5).with_columns(
        dt=(
            pl.col("departure_time").dt.hour().cast(pl.Float64) * 3600
            + pl.col("departure_time").dt.minute().cast(pl.Float64) * 60
            + pl.col("departure_time").dt.second().cast(pl.Float64)
        )
    )
    t0, t1 = period[0].seconds(), period[1].seconds()
    routes = routes.filter(pl.col("dt") >= t0, pl.col("dt") <= t1)

    trips = matched_routes.join(routes.select("tomtom_id", "dt"), on="tomtom_id", how="inner")
    trips = trips.with_columns(path=pl.col("path").cast(pl.List(pl.String)))
    trips = trips.with_columns(
        origin=pl.col("path").list.first().replace_strict(edges["edge_id"], edges["source"]),
        destination=pl.col("path").list.last().replace_strict(edges["edge_id"], edges["target"]),
    )
    return trips.select("tomtom_id", "dt", "origin", "destination", "path")


def prepare_edges(edges: pl.DataFrame, penalties: pl.DataFrame) -> pl.DataFrame:
    import polars as pl

    # Note. Parallel edges can be left as they are, since there will be no routing in the
    # simulation.
    df = (
        edges.select(
            pl.col("edge_id").cast(pl.String),
            "source",
            "target",
            "length",
            "lanes",
            speed=pl.col("speed_limit") / 3.6,
            overtaking=pl.lit(True),
        )
        .join(
            penalties.select(pl.col("edge_id").cast(pl.String), "constant", "speed_multiplier"),
            on="edge_id",
            how="left",
        )
        .rename({"constant": "constant_travel_time"})
        .with_columns(speed=pl.col("speed") * pl.col("speed_multiplier").fill_null(1.0))
        .drop("speed_multiplier")
        .sort("source", "target")
    )
    return df


def write_congestion_inputs(
    tmp_dir: Path,
    trips: pl.DataFrame,
    edges: pl.DataFrame,
    period: list,
    road_network_conditions_path: Path,
):
    """Writes the agents / alternatives / trips input files and the `parameters.json` file for the
    congestion-replay simulation into `tmp_dir`.
    """
    import polars as pl

    agents = trips.select(agent_id="tomtom_id")
    alts = trips.select(
        agent_id="tomtom_id",
        alt_id=pl.lit(1),
        **{"dt_choice.type": pl.lit("Constant"), "dt_choice.departure_time": pl.col("dt")},
    )
    demand_trips = trips.select(
        agent_id="tomtom_id",
        alt_id=pl.lit(1),
        trip_id="tomtom_id",
        **{
            "class.type": pl.lit("Road"),
            "class.origin": pl.col("origin"),
            "class.destination": pl.col("destination"),
            "class.vehicle": pl.lit(CAR_VEHICLE_ID),
            "class.route": pl.col("path"),
        },
    )
    # headway is mandatory but not used with `only_computed_decisions`
    vehicles = pl.DataFrame([{"vehicle_id": CAR_VEHICLE_ID, "headway": 1.0}])

    agents.write_parquet(tmp_dir / "agents.parquet")
    alts.write_parquet(tmp_dir / "alts.parquet")
    demand_trips.write_parquet(tmp_dir / "trips.parquet")
    edges.write_parquet(tmp_dir / "edges.parquet")
    vehicles.write_parquet(tmp_dir / "vehicle_types.parquet")

    t0, t1 = period
    params = {
        "input_files": {
            "agents": "agents.parquet",
            "alternatives": "alts.parquet",
            "trips": "trips.parquet",
            "edges": "edges.parquet",
            "vehicle_types": "vehicle_types.parquet",
            "road_network_conditions": str(road_network_conditions_path),
        },
        "output_directory": "output",
        "period": [t0.seconds(), t1.seconds()],
        "learning_model": {"type": "Exponential", "value": 0.0},
        "only_compute_decisions": True,
        "saving_format": "Parquet",
        "nb_threads": 0,
        "road_network": {"recording_interval": 900.0, "spillback": False},
    }
    with open(tmp_dir / "parameters.json", "w") as f:
        json.dump(params, f)


def run_congestion_simulation(exec_path: Path, tmp_dir: Path):
    res = subprocess.run([exec_path, tmp_dir / "parameters.json"], check=False)
    if res.returncode:
        raise MetropyError("Metropolis-Core congestion simulation failed.")
    if not (tmp_dir / "output" / "route_results.parquet").is_file():
        raise MetropyError("Output file not written: `route_results.parquet`")


def compute_congestion_times(route_results: pl.DataFrame, edges: pl.DataFrame) -> pl.DataFrame:
    """Aggregates Metropolis-Core's edge-level route results into one row per route, with the
    total simulated travel time and the congested time (travel time minus free-flow time).
    """
    import polars as pl

    edges_fftt = edges.select(
        "edge_id",
        ff_time=pl.col("length") / pl.col("speed") + pl.col("constant_travel_time").fill_null(0.0),
    )
    df = (
        route_results.join(edges_fftt, on="edge_id")
        .with_columns(travel_time=pl.col("exit_time") - pl.col("entry_time"))
        .group_by("trip_id")
        .agg(travel_time=pl.col("travel_time").sum(), ff_time=pl.col("ff_time").sum())
        .select(
            tomtom_id="trip_id",
            travel_time=pl.duration(seconds="travel_time"),
            congested_time=pl.duration(seconds=pl.col("travel_time") - pl.col("ff_time")),
        )
    )
    return df


class CongestionSimulationStep(ThreadedStep, AbstractRunSimulationStep, StepWithPeriod):
    """Replays map-matched TomTom routes through Metropolis-Core to get simulated travel times.

    Builds a Metropolis-Core simulation whose demand is exactly the map-matched TomTom routes, with
    each trip's route and departure time fixed (`class.route`, `dt_choice.type = "Constant"`) and
    `only_compute_decisions` enabled, so that travel times are computed for the fixed routes without
    any route, mode or departure-time choice. The road-network congestion state is fixed to the
    ex-ante simulation's last-iteration travel-time functions.
    """

    input_files = {
        "routes": TomTomRoutesFile,
        "matched_routes": TomTomRoutesMatchedFile,
        "edges": RoadEdgesCleanFile,
        "penalties": RoadEdgesPenaltiesFile,
        "road_network_conditions": MetroExAnteSimulatedTravelTimeFunctionsFile,
    }
    output_files = {"congestion_times": TomTomCongestionTimesFile}

    def is_defined(self) -> bool:
        return self.exec_path is not None and self.period is not None

    def run(self):
        import polars as pl

        assert self.exec_path is not None
        assert self.period is not None

        routes_gdf = self.input["routes"].read()
        routes = pl.from_pandas(routes_gdf.loc[:, ["tomtom_id", "departure_time"]])
        matched_routes = self.input["matched_routes"].read()
        edges = self.input["edges"].read_as_df()  # ty: ignore[unresolved-attribute]
        penalties = self.input["penalties"].read()

        trips = prepare_trips(routes, matched_routes, edges, self.period)
        # The `edges.parquet` from the actual simulation cannot be reused here since the simulation
        # needs to have all edges (primary and secondary).
        edges = prepare_edges(edges, penalties)

        with tempfile.TemporaryDirectory() as tmp_directory:
            tmp_dir = Path(tmp_directory)
            write_congestion_inputs(
                tmp_dir,
                trips,
                edges,
                self.period,
                self.input["road_network_conditions"].get_path(absolute=True),
            )
            logger.debug("Running congestion simulation")
            run_congestion_simulation(self.exec_path, tmp_dir)
            route_results = pl.read_parquet(tmp_dir / "output" / "route_results.parquet")

        logger.debug("Computing congestion times")
        congestion_times = compute_congestion_times(route_results, edges)
        self.output["congestion_times"].write(congestion_times)


class CongestionTimeComparisonStep(Step):
    """Compares TomTom-observed and Metropolis-simulated congested times, by OD."""

    input_files = {"routes": TomTomRoutesFile, "congestion_times": TomTomCongestionTimesFile}
    output_files = {"comparison_plot": CongestionTimeComparisonPlotFile}

    def run(self):
        import polars as pl

        routes = self.input["routes"].read()
        congestion_times = self.input["congestion_times"].scan()

        observed_tt = pl.from_pandas(
            routes.loc[:, ["tomtom_id", "tt_no_traffic", "tt_traffic"]]
        ).with_columns(
            tomtom_congested_time=(pl.col("tt_traffic") - pl.col("tt_no_traffic")).dt.total_seconds(
                fractional=True
            )
        )

        simulated_tt = congestion_times.select(
            "tomtom_id",
            metropolis_congested_time=pl.col("congested_time").dt.total_seconds(fractional=True),
        ).collect()

        df = simulated_tt.join(observed_tt, on="tomtom_id", how="inner")
        observed = df["tomtom_congested_time"].to_numpy()
        predicted = df["metropolis_congested_time"].to_numpy()
        rmse = float(((observed - predicted) ** 2).mean() ** 0.5)

        fig = plot_travel_time_comparison(
            observed,
            predicted,
            rmse,
            xlabel="TomTom congested time",
            ylabel="Metropolis congested time",
        )
        self.output["comparison_plot"].write(fig)

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from loguru import logger

from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_common.io import read_dataframe
from pymetropolis.metro_demand.population.files import TripsDestinationsFile, TripsOriginsFile
from pymetropolis.metro_demand.zones.file import (
    ZonesLevel1File,
    ZonesLevel2File,
    ZonesLevel3File,
    ZonesLevel4File,
    ZonesLevel5File,
)
from pymetropolis.metro_network.road_network.files import RoadEdgesCleanFile, RoadEdgesUrbanFlagFile
from pymetropolis.metro_pipeline import PopulationStep
from pymetropolis.metro_pipeline.parameters import (
    BoolParameter,
    CustomParameter,
    FloatParameter,
    IntParameter,
    PathParameter,
    StringParameter,
)
from pymetropolis.metro_pipeline.steps import InputFile
from pymetropolis.random import IntDistributionParameter, RandomStep, generate_int_values

if TYPE_CHECKING:
    import geopandas as gpd
    import numpy as np
    import polars as pl


def is_valid_map(value: dict) -> bool:
    """Returns True if the given value is a valid map key->numeric value."""
    return all(isinstance(v, int | float) for v in value.values())


def weight_validator(value: Any) -> int | float | dict:
    """Validator function for edge weights in the OD matrix disaggregation.

    Returns the value if it is a valid parameter input, otherwise raises an error.

    Three cases:
        - Numeric or None: constant weights
        - Map edge_type -> numeric value: weights by edge type
        - Nested map urban/rural -> edge_type -> numeric value: weights by edge type x urban.
    """
    if isinstance(value, int | float) or value is None:
        # Case 1. constant penalty.
        # Weights are normalized so we can just return 1.
        return 1
    elif isinstance(value, dict):
        keys = set(value.keys())
        if keys == {"urban", "rural"}:
            # Case 2. nested map urban/rural -> edge_type -> penalty
            for k in keys:
                if not is_valid_map(value[k]):
                    raise MetropyError(
                        f"Invalid {k} weights (map edge_type->weight expected): `{value[k]}`"
                    )
            return value
        else:
            # Case 3. map edge_type -> penalty
            if is_valid_map(value):
                return value
            else:
                raise MetropyError(f"Invalid weights (map edge_type->penalty expected): `{value}`")
    else:
        raise MetropyError(f"Invalid weights (number or dictionary expected): `{value}`")


def apply_regex(df: pl.DataFrame, regex: str) -> pl.DataFrame:
    """Filters origin / destination ids to the given regular expression."""
    import polars as pl

    return df.filter(
        pl.col("origin").cast(pl.String).str.contains(regex),
        pl.col("destination").cast(pl.String).str.contains(regex),
    )


def get_node_geometries(edges: gpd.GeoDataFrame) -> pl.DataFrame:
    import geopandas as gpd
    import pandas as pd
    import polars as pl
    from shapely.geometry import Point

    source_nodes = edges[["source", "geometry"]].rename(columns={"source": "node"})
    source_nodes["geometry"] = source_nodes["geometry"].apply(lambda g: Point(g.coords[0]))
    target_nodes = edges[["target", "geometry"]].rename(columns={"target": "node"})
    target_nodes["geometry"] = target_nodes["geometry"].apply(lambda g: Point(g.coords[-1]))
    nodes = pd.concat([source_nodes, target_nodes], ignore_index=True).drop_duplicates(
        subset="node"
    )
    node_points = pl.DataFrame(
        {"node": nodes["node"], "wkb": gpd.GeoSeries(nodes["geometry"]).to_wkb()}
    )
    return node_points


def generate_trips_from_od_matrix(df: pl.DataFrame, rng: np.random.Generator):
    """Generates a DataFrame of trips (trip_id, origin, destination) from a DataFrame representing
    an origin-destination matrix (origin, destination, size).
    """
    import numpy as np
    import polars as pl

    if df["size"].dtype.is_float():
        decimals = df["size"] % 1.0
        if (decimals.is_close(0.0, abs_tol=1e-9).not_()).any():
            # Some values for `size` are not integers: we randomly draw the previous or next integer
            # for each value, with probability equal to the decimal part.
            # The draws are such that, on aggregate, the total number of trips is equal to the sum
            # of `size`.
            nb_extras = decimals.sum()
            nb_extras = int(nb_extras) + rng.binomial(1, nb_extras % 1.0)  # ty: ignore[unsupported-operator]
            extras = rng.choice(
                len(df), size=nb_extras, replace=False, p=decimals.to_numpy() / decimals.sum()
            )
            df = df.with_columns(
                index=pl.arange(pl.len()), int_size=pl.col("size").cast(pl.UInt32)
            ).with_columns(
                size=pl.when(pl.col("index").is_in(extras))
                .then(pl.col("size") + 1)
                .otherwise("size")
            )
    trips = pl.DataFrame(
        {
            "origin": np.repeat(df["origin"], df["size"]),
            "destination": np.repeat(df["destination"], df["size"]),
        }
    )
    # We just cast trip_id to String here because they might be forced-converted to String later and
    # that create many problems.
    trips = trips.with_columns(trip_id=pl.arange(1, pl.len() + 1, dtype=pl.UInt64).cast(pl.String))
    return trips


def edges_in_zones(edges: gpd.GeoDataFrame, zones: gpd.GeoDataFrame):
    import geopandas as gpd

    logger.debug("Matching edges with zones...")
    n0 = len(edges)
    gdf = edges.sjoin(zones, predicate="intersects", how="inner").reset_index(drop=True)
    n1 = gdf["edge_id"].nunique()
    if n1 < n0:
        logger.debug(f"{n0 - n1:,} edges are not part of any zone")
    # Clip each edge's geometry to the polygon of the zone it is matched with.
    zone_geoms = zones.set_index("zone_id").geometry
    matched_geoms = gpd.GeoSeries(zone_geoms.loc[gdf["zone_id"]].values, crs=zones.crs)
    gdf["geometry"] = gdf.geometry.intersection(matched_geoms)
    gdf["length_in_zone"] = gdf.geometry.length
    return gdf


class AbstractODMatrixStep(PopulationStep, RandomStep):
    zone_level = IntParameter(
        "od_matrix.zone_level",
        description="Zone level (1-5) used by the origin-destination matrix.",
        note=(
            "When set to NULL (default), one zone is defined for each node of the road network. "
            "It is discouraged to do so with a large network (> 10k nodes). "
            "Similarly, it is recommended to use a zone level with fewer than 10k nodes."
        ),
        lower_bound=1,
        upper_bound=5,
    )
    use_centroid = BoolParameter(
        "od_matrix.use_centroid",
        default=False,
        description=(
            "If true, origin / destination coordinates are set to the centroid of the zone."
        ),
        note=(
            "Ignored when `zone_level` is NULL, in which case origin / destination coordinates "
            "are always the road nodes' coordinates. "
        ),
    )
    # TODO. It might be useful to add a bool parameter to toggle between road and pedestrian network
    # to be used when automatically creating zones.
    max_edges_per_zone = IntParameter(
        "od_matrix.max_edges_per_zone",
        description=(
            "Maximum number of edges that can be used as origin / destination in each zone."
        ),
        note=(
            "Only used when origins / destinations are drawn along road edges "
            "(i.e., `zone_level` is set and `use_centroid` is false). "
            "Default is to allow an unlimited number of edges per zone."
        ),
        lower_bound=1,
    )
    weights = CustomParameter(
        "od_matrix.edge_weights",
        validator=weight_validator,
        validator_description=(
            "a number (constant weight for all edges), a table with edge types as keys and weights"
            ' as values, or a table with "urban" and "rural" as keys and `edge_type->value` tables'
            " as values (see example)"
        ),
        description="Probability weights of edges when drawing origins / destinations.",
        note=(
            "Only used when origins / destinations are drawn along road edges "
            "(i.e., `zone_level` is set and `use_centroid` is false). "
            "The probability that an edge is used as origin / destination for a trip is "
            "proportional to `weight * length_in_zone` where `length_in_zone` is the length of the "
            "edge within the current zone. "
            "Edges with larger weights are more likely to be selected as origin / destination. "
            "Actual origins / destinations are then set to a random point drawn along the selected "
            "edge. "
            "Edges with 0 weight cannot be selected as origin / destination, unless all edges in "
            "the zone have zero-weight, in which case all edges in the zone are equally likely to "
            "be selected. "
            "Default is to use a weight of 1 for all edges."
        ),
        example="""
```toml
[od_matrix.edge_weights]
[od_matrix.edge_weights.urban]
motorway = 4
road = 2
[od_matrix.edge_weights.rural]
motorway = 4
road = 3
```
        """,
    )
    zone_regex = StringParameter(
        "od_matrix.zone_regex",
        description=(
            "Regular expression specifying the zones to be selected as possible "
            "origin / destination."
        ),
        note=(
            "If not specified, any zone can be an origin / destination. "
            "If `zone_level` is NULL, the regex applies to the road nodes instead."
        ),
    )

    input_files = {
        "zones1": InputFile(
            ZonesLevel1File, when=lambda step: step.zone_level == 1, when_doc="`zone_level = 1`"
        ),
        "zones2": InputFile(
            ZonesLevel2File, when=lambda step: step.zone_level == 2, when_doc="`zone_level = 2`"
        ),
        "zones3": InputFile(
            ZonesLevel3File, when=lambda step: step.zone_level == 3, when_doc="`zone_level = 3`"
        ),
        "zones4": InputFile(
            ZonesLevel4File, when=lambda step: step.zone_level == 4, when_doc="`zone_level = 4`"
        ),
        "zones5": InputFile(
            ZonesLevel5File, when=lambda step: step.zone_level == 5, when_doc="`zone_level = 5`"
        ),
        "edges": InputFile(
            RoadEdgesCleanFile,
            when=lambda inst: inst.edges_required(),
            when_doc="`zone_level` is NULL or `use_centroid` is false",
        ),
        "urban_flags": InputFile(
            RoadEdgesUrbanFlagFile,
            when=lambda inst: inst.urban_flag_required(),
            when_doc="if weights rely on the urban flag",
        ),
    }
    output_files = {"origins": TripsOriginsFile, "destinations": TripsDestinationsFile}

    def edges_required(self) -> bool:
        return self.zone_level is None or not self.use_centroid

    def urban_flag_required(self) -> bool:
        return (
            self.zone_level is not None
            and not self.use_centroid
            and isinstance(self.weights, dict)
            and "urban" in self.weights
        )

    def run(self):
        rng = self.get_rng(str(self))

        if self.edges_required():
            edges: gpd.GeoDataFrame = self.input["edges"].read()
            edges = edges.loc[:, ["edge_id", "source", "target", "edge_type", "geometry"]]
        else:
            edges = None

        if self.zone_level is None:
            zones = None
        else:
            zones = self.input[f"zones{self.zone_level}"].read()
            if self.restrict_to_area() and "within_area" in zones.columns:
                zones = zones.filter("within_area")

        ods = self.get_od_pairs(edges, zones)
        if self.zone_regex is not None:
            ods = apply_regex(ods, self.zone_regex)
        ods = self.od_pairs_with_size(ods)
        trips = generate_trips_from_od_matrix(ods, rng)

        if zones is not None:
            # Filter out unused zones.
            used_zones = set(trips["origin"]) | set(trips["destination"])
            zones = zones.loc[zones["zone_id"].isin(used_zones)]

        if self.zone_level is None:
            assert edges is not None
            origins_gdf, destinations_gdf = self.coords_from_road_nodes(trips, edges)
        elif self.use_centroid:
            assert zones is not None
            origins_gdf, destinations_gdf = self.coords_from_centroids(trips, zones)
        else:
            assert zones is not None
            assert edges is not None
            origins_gdf, destinations_gdf = self.coords_from_disaggregate(trips, edges, zones, rng)
        origins_gdf.sort_values("trip_id", inplace=True)
        destinations_gdf.sort_values("trip_id", inplace=True)
        self.output["origins"].write(origins_gdf)
        self.output["destinations"].write(destinations_gdf)

    def restrict_to_area(self) -> bool:
        """Returns `True` if only zones within the simulation area must be used.

        Usually, when OD counts are automatically defined (e.g., ODMatrixEachStep,
        GravityODMatrixStep) we want to generate trips only for OD pairs within the simulation area.
        When the OD counts are user-defined (e.g., CustomODMatrixStep), we allow the user to use
        zones outside the simulation area.
        """
        return True

    def distance_required(self) -> bool:
        """Returns `True` if a `distance` column is required in the DataFrame of OD pairs, in order
        to compute OD pairs with counts.
        """
        return False

    def get_od_pairs(
        self, edges: gpd.GeoDataFrame | None, zones: gpd.GeoDataFrame | None
    ) -> pl.DataFrame:
        """Returns a DataFrame with all OD pairs."""
        import geopandas as gpd
        import numpy as np
        import polars as pl

        if self.zone_level is None:
            assert edges is not None, "edges must be given when `zone_level` is None"
            origins = pl.Series(edges["source"]).unique().sort().to_numpy()
            destinations = pl.Series(edges["target"]).unique().sort().to_numpy()
        else:
            assert zones is not None, "zones must be given when `zone_level` is not None"
            origins = zones["zone_id"].to_numpy()
            destinations = origins
        df = pl.DataFrame(
            {
                "origin": np.repeat(origins, len(destinations)),
                "destination": np.tile(destinations, len(origins)),
            }
        )
        if self.distance_required():
            if self.zone_level is None:
                assert edges is not None
                node_points = get_node_geometries(edges)
                origin_nodes = gpd.GeoSeries.from_wkb(
                    df.join(node_points, left_on="origin", right_on="node")["wkb"], crs=edges.crs
                )
                destination_nodes = gpd.GeoSeries.from_wkb(
                    df.join(node_points, left_on="destination", right_on="node")["wkb"],
                    crs=edges.crs,
                )
                df = df.with_columns(distance=pl.Series(origin_nodes.distance(destination_nodes)))
            else:
                assert zones is not None
                zone_centroids = zones.set_index("zone_id").centroid
                origin_centroids = zone_centroids.loc[df["origin"]]
                destination_centroids = zone_centroids.loc[df["destination"]]
                df = df.with_columns(
                    distance=pl.Series(origin_centroids.distance(destination_centroids))
                )
        return df

    def od_pairs_with_size(self, df: pl.DataFrame) -> pl.DataFrame:
        """Reads a DataFrame with one row for each OD pair and columns `origin` and `destination`
        and adds a `size` column to it, with the number of trips to generate for the OD pair.
        """
        raise NotImplementedError

    def coords_from_road_nodes(
        self, trips: pl.DataFrame, edges: gpd.GeoDataFrame
    ) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
        """Returns origin / destination coordinates when origin / destination ids in `trips`match
        nodes from `edges`.
        """
        import geopandas as gpd

        # origin / destination correspond to road nodes -> origins / destinations are inferred
        # from the nodes' coordinates.
        node_points = get_node_geometries(edges)
        origins_df = trips.join(node_points, left_on="origin", right_on="node", how="left")
        destinations_df = trips.join(
            node_points, left_on="destination", right_on="node", how="left"
        )
        origins_gdf = gpd.GeoDataFrame(
            {"trip_id": origins_df["trip_id"]},
            geometry=gpd.GeoSeries.from_wkb(origins_df["wkb"], crs=edges.crs),
        )
        destinations_gdf = gpd.GeoDataFrame(
            {"trip_id": destinations_df["trip_id"]},
            geometry=gpd.GeoSeries.from_wkb(destinations_df["wkb"], crs=edges.crs),
        )
        return origins_gdf, destinations_gdf

    def coords_from_centroids(
        self, trips: pl.DataFrame, zones: gpd.GeoDataFrame
    ) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
        """Returns origin / destination coordinates from the centroids of the zones."""
        import geopandas as gpd
        import polars as pl

        centroids = pl.DataFrame({"zone_id": zones["zone_id"], "wkb": zones.centroid.to_wkb()})
        origins_df = trips.join(centroids, left_on="origin", right_on="zone_id", how="left")
        destinations_df = trips.join(
            centroids, left_on="destination", right_on="zone_id", how="left"
        )
        origins_gdf = gpd.GeoDataFrame(
            {"trip_id": origins_df["trip_id"]},
            geometry=gpd.GeoSeries.from_wkb(origins_df["wkb"], crs=zones.crs),
        )
        destinations_gdf = gpd.GeoDataFrame(
            {"trip_id": destinations_df["trip_id"]},
            geometry=gpd.GeoSeries.from_wkb(destinations_df["wkb"], crs=zones.crs),
        )
        return origins_gdf, destinations_gdf

    def coords_from_disaggregate(
        self,
        trips: pl.DataFrame,
        edges: gpd.GeoDataFrame,
        zones: gpd.GeoDataFrame,
        rng: np.random.Generator,
    ) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
        """Returns origin / destination coordinates by disaggregating the OD matrix to random points
        along the edges within each zone.

        Weights can be used to define the probabilities of edges to be drawn, based on their type or
        urban flag.
        """
        import geopandas as gpd
        import polars as pl
        import shapely

        gdf = edges_in_zones(edges, zones)
        df = pl.from_pandas(
            gdf.loc[:, ["edge_id", "length_in_zone", "edge_type", "zone_id"]]
        ).with_columns(wkb=pl.from_pandas(gdf["geometry"].to_wkb()))
        df = (
            self.set_weights(df)
            .with_columns(prob=pl.col("length_in_zone") * pl.col("weight"))
            # Set probability to 1 when all weights are zero in a zone.
            .with_columns(
                prob=pl.when(pl.col("prob").sum().over("zone_id").eq(0.0))
                .then(1.0)
                .otherwise("prob")
            )
            # Normalize probabilities.
            .with_columns(prob=pl.col("prob") / pl.col("prob").sum().over("zone_id"))
        )
        zone_edges = (
            df.select("zone_id", "edge_id", "prob", "wkb")
            .filter(pl.col("prob") > 0.0)
            .partition_by(["zone_id"], as_dict=True)
        )
        m = self.max_edges_per_zone or len(trips) + 1

        def draw_points(group_col: str) -> tuple[np.ndarray, np.ndarray]:
            """For each zone, draws one random point along a random edge of the zone (with
            probabilities given by `zone_edges`) for each trip whose `group_col` (`"origin"` or
            `"destination"`) is that zone. Returns the trip ids and the corresponding points, in
            an arbitrary order.
            """
            import numpy as np

            groups = trips.partition_by(group_col, as_dict=True, include_key=False)
            trip_ids = list()
            points = list()
            for (zone_id,), zone_trips in groups.items():
                count = len(zone_trips)
                try:
                    zone_df = zone_edges[(zone_id,)]
                except KeyError:
                    raise MetropyError(f"There is no edge in zone `{zone_id}`.")
                if len(zone_df) > m and count > m:
                    # Randomly select `m` candidate edges to be selected.
                    idx = rng.choice(
                        len(zone_df),
                        p=zone_df["prob"] / zone_df["prob"].sum(),
                        size=m,
                        replace=False,
                    )
                    zone_df = zone_df[idx]
                idx = rng.choice(
                    len(zone_df), p=zone_df["prob"] / zone_df["prob"].sum(), size=count
                )
                lines = shapely.from_wkb(zone_df["wkb"][idx].to_numpy())
                distances = rng.uniform(size=count) * shapely.length(lines)
                points.append(shapely.line_interpolate_point(lines, distances))
                trip_ids.append(zone_trips["trip_id"].to_numpy())
            return np.concatenate(trip_ids), np.concatenate(points)

        logger.debug("Drawing origins...")
        origin_trip_ids, origin_points = draw_points("origin")
        logger.debug("Drawing destinations...")
        destination_trip_ids, destination_points = draw_points("destination")
        origins_gdf = gpd.GeoDataFrame(
            {"trip_id": origin_trip_ids}, geometry=gpd.GeoSeries(origin_points, crs=edges.crs)
        )
        destinations_gdf = gpd.GeoDataFrame(
            {"trip_id": destination_trip_ids},
            geometry=gpd.GeoSeries(destination_points, crs=edges.crs),
        )
        return origins_gdf, destinations_gdf

    def set_weights(self, edges: pl.DataFrame) -> pl.DataFrame:
        import polars as pl

        if self.weights == 1:
            return edges.with_columns(weight=1)
        assert isinstance(self.weights, dict)
        if "urban" in self.weights and "rural" in self.weights:
            urban_flags = self.input["urban_flags"].read()
            edges = edges.join(urban_flags, on="edge_id", how="left")
            edges = edges.with_columns(
                weight=pl.when("urban")
                .then(pl.col("edge_type").replace_strict(self.weights["urban"], default=0.0))
                .otherwise(pl.col("edge_type").replace_strict(self.weights["rural"], default=0.0))
                .clip(lower_bound=0.0)
            )
        else:
            edges = edges.with_columns(
                weight=pl.col("edge_type")
                .replace_strict(self.weights, default=0.0)
                .clip(lower_bound=0.0)
            )
        return edges


class ODMatrixEachStep(AbstractODMatrixStep):
    """Generates car driver origin-destination pairs by generating a fixed number of trips for each
    eligible origin-destination pair.

    When `zone_level` is not set, eligible origins are all road nodes with at least one outgoing
    edge and eligible destinations are all road nodes with at least one incoming edge. If the road
    network is not strongly connected, there is no guarantee that all the origin-destination pairs
    generated are feasible (i.e., there is a path from origin to destination).

    Origins and destinations are drawn from road-network nodes by default. If `zone_level` is set
    (between 1 and 5), zones of the corresponding zone level are used as origins / destinations
    instead.

    When zones are used, trip coordinates are set to the zone's centroid if `use_centroid` is
    true, or, by default, to a random point drawn along a random edge of the road network
    intersecting the zone. In the latter case, edges (and points along them) are drawn with a
    probability proportional to their length within the zone, weighted by `edge_weights`;
    `max_edges_per_zone` can be used to cap the number of candidate edges considered in each zone,
    for performance reasons.

    Only zones within the simulation area are used as origin / destination.
    The `zone_regex` parameter can be used to restrict the zones (or, when `zone_level` is not set,
    the road nodes) that are eligible as origin / destination.
    """

    each = IntDistributionParameter(
        "od_matrix.trips_per_pair",
        description="Number of trips to generate for each origin-destination pair.",
    )

    def is_defined(self) -> bool:
        return self.each is not None

    def od_pairs_with_size(self, df: pl.DataFrame) -> pl.DataFrame:
        df = df.with_columns(size=generate_int_values(self.each, len(df), self.get_rng(str(self))))
        return df


class GravityODMatrixStep(AbstractODMatrixStep):
    r"""Generates car driver origin-destination pairs by generating trips from a gravity model.

    The total number of trips generated from each origin is fixed (parameter `trips_per_origin`).
    Then, the number of trips generated from origin \\(i\\) to destination \\(j\\) is proportional
    to:

    \\[ e^{-\\lambda \\cdot d} \\]

    where \\(\\lambda\\) is the decay rate (parameter `exponential_decay`) and \\(d\\) is the
    Euclidean distance between \\(i\\) and \\(j\\) (in kilometers).

    When zones are used (see below), \\(d\\) is always computed between the zones' centroids, even
    when `use_centroid` is false and trip coordinates are actually disaggregated to points along
    road edges.

    Origins and destinations are drawn from road-network nodes by default. If `zone_level` is set
    (between 1 and 5), zones of the corresponding zone level are used as origins / destinations
    instead.

    When zones are used, trip coordinates are set to the zone's centroid if `use_centroid` is
    true, or, by default, to a random point drawn along a random edge of the road network
    intersecting the zone. In the latter case, edges (and points along them) are drawn with a
    probability proportional to their length within the zone, weighted by `edge_weights`;
    `max_edges_per_zone` can be used to cap the number of candidate edges considered in each zone,
    for performance reasons.

    By default, only zones within the simulation area are used as origin / destination.
    The `zone_regex` parameter can be used to restrict the zones (or, when `zone_level` is not set,
    the road nodes) that are eligible as origin / destination.
    """

    exponential_decay = FloatParameter(
        "od_matrix.gravity.exponential_decay",
        description=(
            "Exponential decay rate of flows as a function of distance (rate per kilometers)."
        ),
    )
    trips_per_origin = IntDistributionParameter(
        "od_matrix.gravity.trips_per_origin",
        description="Number of trips to be generated from each origin.",
    )

    def is_defined(self) -> bool:
        return self.exponential_decay is not None and self.trips_per_origin is not None

    def distance_required(self) -> bool:
        # Distance column is required to compute the gravity decay.
        return True

    def od_pairs_with_size(self, df: pl.DataFrame) -> pl.DataFrame:
        import polars as pl

        decay = self.exponential_decay
        rng = self.get_rng(str(self))
        df = df.filter(pl.col("origin") != pl.col("destination"))
        df = (
            df.with_columns(rate=(-pl.lit(decay) * pl.col("distance")).exp())
            .with_columns(normalized_rate=pl.col("rate") / pl.col("rate").sum().over("origin"))
            .with_columns(trips_per_origin=generate_int_values(self.trips_per_origin, len(df), rng))
            .with_columns(size=pl.col("normalized_rate") * pl.col("trips_per_origin"))
        )
        return df


class CustomODMatrixStep(AbstractODMatrixStep):
    """Generates car driver origin-destination pairs from the provided origin-destination matrix.

    The origin-destination matrix is provided in a CSV or Parquet file with the following columns:

    - `origin`: id of the origin zone / node,
    - `destination`: id of the destination zone / node,
    - `size`: number of trips to be generated from origin to destination.

    The `size` variable can be integers or floats. For float values, stochastic rounding is used
    to convert the value to an integer: e.g., 3.3 is converted to 3 with probability 70% and to 4
    with probability 30%.

    Origins and destinations are drawn from road-network nodes by default. If `zone_level` is set
    (between 1 and 5), zones of the corresponding zone level are used as origins / destinations
    instead.

    When zones are used, trip coordinates are set to the zone's centroid if `use_centroid` is
    true, or, by default, to a random point drawn along a random edge of the road network
    intersecting the zone. In the latter case, edges (and points along them) are drawn with a
    probability proportional to their length within the zone, weighted by `edge_weights`;
    `max_edges_per_zone` can be used to cap the number of candidate edges considered in each zone,
    for performance reasons.

    The origin-destination matrix can reference zones outside of the area.
    """

    file = PathParameter(
        "od_matrix.file",
        check_file_exists=True,
        description="Path to the CSV or Parquet file containing the origin-destination matrix.",
        note=(
            "Required columns are: `origin` (id of origin zone / node), `destination` (id of "
            "destination zone / node), and `size` (int or float, number of trips)."
        ),
    )

    def is_defined(self) -> bool:
        return self.file is not None

    def restrict_to_area(self) -> bool:
        # Allow users to define trips with zones outside of the simulation area.
        return False

    def od_pairs_with_size(self, df: pl.DataFrame) -> pl.DataFrame:
        assert self.file is not None

        matrix = read_dataframe(self.file, columns=["origin", "destination", "size"])
        missing_origins = matrix.join(df, on="origin", how="anti")["origin"].unique().to_list()
        if missing_origins:
            raise MetropyError(
                "The following ids are used as origins but are not valid origin zones / nodes: "
                f"{missing_origins}"
            )
        missing_destinations = (
            matrix.join(df, on="destination", how="anti")["destination"].unique().to_list()
        )
        if missing_destinations:
            raise MetropyError(
                "The following ids are used as destinations but are not valid destination "
                f"zones / nodes: {missing_destinations}"
            )
        return matrix

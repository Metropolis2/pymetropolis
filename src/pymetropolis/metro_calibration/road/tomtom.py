from __future__ import annotations

import asyncio
import time
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, NoReturn

from loguru import logger

from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_network.road_network.files import RoadEdgesCleanFile
from pymetropolis.metro_pipeline.parameters import (
    DateParameter,
    FloatParameter,
    IntParameter,
    ListParameter,
    StringParameter,
    TimeParameter,
)
from pymetropolis.metro_pipeline.types import String
from pymetropolis.metro_spatial import GeoStep
from pymetropolis.random import RandomStep

from .files import TomTomRoutesFile

if TYPE_CHECKING:
    from collections.abc import Generator

    import aiohttp
    import geopandas as gpd
    import numpy as np
    import pyproj
    from tqdm import tqdm

BASE_URL = "https://api.tomtom.com/routing/1/calculateRoute/"
PARAMS = {"computeTravelTimeFor": "all", "traffic": "true"}
MAX_CONSECUTIVE_ERRORS = 8
# Number of points drawn at once for each route, when drawing points by rejection sampling.
REJECTION_BATCH_SIZE = 64
# Number of rejection-sampling rounds before listing the candidates exactly.
MAX_REJECTION_ROUNDS = 8


def generate_random_nodes(
    edges: gpd.GeoDataFrame,
    rng: np.random.Generator,
    nb_routes: int,
    nb_waypoints: int,
    excluded_edge_types: list[str] = [],
    min_distance: float | None = None,
    max_distance: float | None = None,
    crs: pyproj.CRS | str | None = None,
) -> tuple[np.ndarray, np.ndarray]:
    """Draws random routes (origin, waypoints, destination) from the source nodes of the edges.

    When `min_distance` and / or `max_distance` are set, each point (except the origin) is drawn
    among the nodes whose Euclidean distance to the previous point is within these bounds.
    Distances are computed in `crs` (the CRS of `edges` if `None`), which must be projected.

    Returns the drawn node ids and their (latitude, longitude) coordinates.
    """
    import geopandas as gpd
    import shapely

    if min_distance is not None and max_distance is not None and min_distance > max_distance:
        raise MetropyError(
            f"The minimum distance ({min_distance}) is larger than the maximum distance "
            f"({max_distance})."
        )
    logger.debug("Generating random origin-destination pairs...")
    # Remove excluded edge types and keep one edge per source node.
    mask = ~edges["edge_type"].isin(excluded_edge_types)
    edges = edges.loc[mask].drop_duplicates(subset="source")
    node_ids = edges["source"].to_numpy()
    node_points = gpd.GeoSeries(shapely.get_point(edges.geometry.values, 0), crs=edges.crs)
    # Number of nodes to draw for each route (origin + destination + waypoints).
    nb_nodes = nb_waypoints + 2
    if min_distance is None and max_distance is None:
        idx = rng.integers(0, len(node_ids), size=(nb_routes, nb_nodes))
    else:
        if crs is not None:
            node_points = node_points.to_crs(crs)
        xy = shapely.get_coordinates(node_points.values)
        idx = draw_indices_in_window(
            xy, node_ids, rng, nb_routes, nb_nodes, min_distance, max_distance
        )
    # Only the drawn nodes are converted to longitude / latitude.
    selected_points = gpd.GeoSeries(node_points.values[idx.ravel()], crs=node_points.crs)
    lng_lat = shapely.get_coordinates(selected_points.to_crs("EPSG:4326").values)
    # Switch latitude and longitude.
    coordinates = lng_lat.reshape(nb_routes, nb_nodes, 2)[:, :, ::-1]
    return node_ids[idx], coordinates


def draw_indices_in_window(
    xy: np.ndarray,
    node_ids: np.ndarray,
    rng: np.random.Generator,
    nb_routes: int,
    nb_nodes: int,
    min_distance: float | None,
    max_distance: float | None,
) -> np.ndarray:
    """Draws the indices (in `xy`) of `nb_nodes` successive points for each route, such that the
    Euclidean distance between two consecutive points is between `min_distance` and
    `max_distance` (at least one of them must be set).

    Each point is drawn uniformly among the points satisfying the constraints.

    The points are first drawn by rejection sampling (points are drawn uniformly among all the
    points until one satisfies the constraints), which is fast when the distance window contains
    a non-negligible share of the points. For the (few) routes where no point is accepted after
    `MAX_REJECTION_ROUNDS` rounds, the candidates are listed exactly with a KD-tree.
    """
    import numpy as np
    from scipy.spatial import KDTree
    from tqdm import tqdm

    assert min_distance is not None or max_distance is not None

    def in_window(dists: np.ndarray) -> np.ndarray:
        ok = np.ones(dists.shape, dtype=bool)
        if min_distance is not None:
            ok &= dists >= min_distance
        if max_distance is not None:
            ok &= dists <= max_distance
        return ok

    all_indices = np.arange(len(xy))
    idx = np.empty((nb_routes, nb_nodes), dtype=np.int64)
    idx[:, 0] = rng.integers(0, len(xy), size=nb_routes)
    nb_exact = 0
    for k in tqdm(range(1, nb_nodes), total=nb_nodes - 1, desc="Drawing nodes", smoothing=0.05):
        # Routes for which the k-th point has not been drawn yet.
        pending = np.arange(nb_routes)
        for _ in range(MAX_REJECTION_ROUNDS):
            draws = rng.integers(0, len(xy), size=(len(pending), REJECTION_BATCH_SIZE))
            prev_xy = xy[idx[pending, k - 1]]
            ok = in_window(np.linalg.norm(xy[draws] - prev_xy[:, None, :], axis=2))
            # The first accepted point of each route is uniformly drawn among the candidates.
            found = ok.any(axis=1)
            first = ok.argmax(axis=1)
            idx[pending[found], k] = draws[found, first[found]]
            pending = pending[~found]
            if len(pending) == 0:
                break
        if len(pending) == 0:
            continue
        # List the candidates exactly for the remaining routes.
        nb_exact += len(pending)
        tree = KDTree(xy)
        prev = idx[pending, k - 1]
        # Points within `max_distance` of the previous point (the candidates).
        outer = (
            tree.query_ball_point(xy[prev], r=max_distance) if max_distance is not None else None
        )
        # Points within `min_distance` of the previous point (excluded from the candidates).
        inner = (
            tree.query_ball_point(xy[prev], r=min_distance) if min_distance is not None else None
        )
        for j, i in enumerate(pending):
            candidates = outer[j] if outer is not None else all_indices
            if inner is not None:
                candidates = np.setdiff1d(candidates, inner[j], assume_unique=True)
            if len(candidates) == 0:
                raise_empty_window(node_ids[prev[j]], min_distance, max_distance)
            idx[i, k] = candidates[rng.integers(len(candidates))]
    if nb_exact > 0:
        logger.debug(f"Candidates were listed exactly for {nb_exact} drawn points")
    distances = np.linalg.norm(xy[idx[:, 1:]] - xy[idx[:, :-1]], axis=2)
    logger.debug(
        f"Distance between consecutive points: min = {distances.min():.0f}m, "
        f"mean = {distances.mean():.0f}m, max = {distances.max():.0f}m"
    )
    return idx


def raise_empty_window(
    node_id: object, min_distance: float | None, max_distance: float | None
) -> NoReturn:
    bounds = []
    if min_distance is not None:
        bounds.append(f"at least {min_distance} meters")
    if max_distance is not None:
        bounds.append(f"at most {max_distance} meters")
    raise MetropyError(
        f"There is no node at a distance of {' and '.join(bounds)} from node {node_id}. "
        "The distance window should be enlarged (decrease `tomtom_requests.min_distance` "
        "and / or increase `tomtom_requests.max_distance`)."
    )


def batch_iter(arr: np.ndarray, batch_size: int) -> Generator[np.ndarray]:
    """Iterates a numpy array in batches."""
    n = len(arr)
    for start in range(0, n, batch_size):
        yield arr[start : start + batch_size]


async def get_tomtom_request(url: str, session: aiohttp.ClientSession, params: dict):
    try:
        async with session.get(url, params=params) as response:
            if response.status == 200:
                data = await response.json()
                return data
            else:
                logger.error(
                    f"Failed request. Status = {response.status}. Reason = {response.reason}."
                )
                text = await response.text()
                logger.debug(text)
    except Exception as e:
        logger.error(e)
        pass


async def process_batch(
    api_key: str,
    nodes: np.ndarray,
    coordinates: np.ndarray,
    params: dict,
    rng: np.random.Generator,
    date: date,
    departure_time: timedelta | None,
    pbar: tqdm,
) -> tuple[gpd.GeoDataFrame, float, float]:
    import aiohttp
    import geopandas as gpd
    from shapely.geometry import LineString

    batch_results = []
    successive_errors = 0
    processing_time = 0.0
    api_time = 0.0
    async with aiohttp.ClientSession() as session:
        for node_ids, points in zip(nodes, coordinates):
            waypoint_coords = ":".join([f"{lat},{lon}" for lat, lon in points])
            url = f"{BASE_URL}{waypoint_coords}/json?key={api_key}"
            # Set request departure time.
            # A random departure time is drawn for each request when it is not specified.
            if departure_time is None:
                request_departure_time = timedelta(seconds=int(rng.integers(0, 24 * 60 * 60)))
            else:
                request_departure_time = departure_time
            td = datetime.combine(date, datetime.min.time()) + request_departure_time
            # The params dict is shared by the batches running concurrently so it is copied
            # rather than modified in place.
            request_params = {**params, "departAt": td.strftime("%Y-%m-%dT%H:%M:%S")}
            t0 = time.perf_counter()
            data = await get_tomtom_request(url, session, request_params)
            t1 = time.perf_counter()
            api_time += t1 - t0
            if data is None:
                successive_errors += 1
                if successive_errors >= MAX_CONSECUTIVE_ERRORS:
                    raise MetropyError(
                        f"Aborting due to {MAX_CONSECUTIVE_ERRORS} consecutive errors."
                    )
            if data and "routes" in data:
                successive_errors = 0
                assert len(data["routes"]) == 1
                route = data["routes"][0]
                assert len(route["legs"]) == len(node_ids) - 1
                for i, leg in enumerate(route["legs"]):
                    geom = LineString([[p["longitude"], p["latitude"]] for p in leg["points"]])
                    leg_departure_time = datetime.fromisoformat(leg["summary"]["departureTime"])
                    res = {
                        "source": node_ids[i],
                        "target": node_ids[i + 1],
                        "length": float(leg["summary"]["lengthInMeters"]),
                        "departure_time": leg_departure_time,
                        "tt_no_traffic": timedelta(
                            seconds=leg["summary"]["noTrafficTravelTimeInSeconds"]
                        ),
                        "tt_traffic": timedelta(
                            seconds=leg["summary"]["historicTrafficTravelTimeInSeconds"]
                        ),
                        "geometry": geom,
                    }
                    batch_results.append(res)
            processing_time += time.perf_counter() - t1
            pbar.update(1)
    gdf = gpd.GeoDataFrame(batch_results, crs="EPSG:4326")
    return gdf, api_time, processing_time


async def get_tomtom_data(
    api_key: str,
    nodes: np.ndarray,
    coordinates: np.ndarray,
    rng: np.random.Generator,
    date: date,
    departure_time: timedelta | None,
    nb_batches: int = 1,
) -> gpd.GeoDataFrame:
    import asyncio

    import geopandas as gpd
    import numpy as np
    import pandas as pd
    from tqdm import tqdm

    logger.debug("Processing batches...")
    nb_routes = nodes.shape[0]
    batch_size = int(np.ceil(nb_routes / nb_batches))
    params = PARAMS
    with tqdm(total=nb_routes, desc="Running API requests", smoothing=0.01) as pbar:
        results = await asyncio.gather(
            *(
                process_batch(
                    api_key=api_key,
                    nodes=batch_nodes,
                    coordinates=batch_coordinates,
                    params=params,
                    rng=rng,
                    date=date,
                    departure_time=departure_time,
                    pbar=pbar,
                )
                for i, (batch_nodes, batch_coordinates) in enumerate(
                    zip(batch_iter(nodes, batch_size), batch_iter(coordinates, batch_size))
                )
            )
        )
    gdfs, api_times, processing_times = zip(*results)
    total_api = sum(api_times)
    total_processing = sum(processing_times)
    logger.debug(
        f"Summed API time: {total_api:.1f}s | Summed processing time: {total_processing:.1f}s"
    )
    gdf = gpd.GeoDataFrame(pd.concat(gdfs), crs="EPSG:4326")
    gdf["tomtom_id"] = np.arange(len(gdf))
    return gdf


class TomTomRequestsStep(RandomStep, GeoStep):
    """Retrieves historical travel time for some origin-destination pairs from TomTom API."""

    date = DateParameter(
        "tomtom_requests.date",
        description="Date to be used for the requests.",
        note=(
            "It is recommended to set a date a few months in the future, on a weekday, so that "
            "TomTom does not rely of real time data and roadworks."
        ),
    )
    departure_time = TimeParameter(
        "tomtom_requests.departure_time",
        description="Departure time to be used for the requests.",
        note=(
            "This parameter only controls the departure time from origin, not from the "
            "intermediate stops. "
            "When not specified, a random departure time is chosen for each request."
        ),
    )
    nb_routes = IntParameter(
        "tomtom_requests.nb_routes",
        description="Number of routes to request.",
        example=2000,
        note="Free TomTom API is limiting the daily number of requests to 2500.",
    )
    nb_waypoints = IntParameter(
        "tomtom_requests.nb_waypoints",
        description="Number of waypoints (intermediate stops) to use for each request.",
        default=0,
        upper_bound=148,
        note=(
            "The number of OD pairs per route is equal to the number of waypoints plus 1, "
            "so that the total number of OD pairs is equal to `nb_routes * (nb_waypoints + 1)`. "
            "TomTom API is limiting the number of waypoints to 148."
        ),
    )
    excluded_edge_types = ListParameter(
        "tomtom_requests.excluded_edge_types",
        inner=String(),
        default=[],
        description=(
            "List of edge types that are excluded from the network when selecting "
            "origin-destination pairs."
        ),
        example='`["motorway", "motorway_link", "trunk", "trunk_link"]`',
        note=(
            "It is recommended to exclude major highways so that minor roads are more likely to be"
            "observed (major highways will be part of most fastest paths anyway)."
        ),
    )
    min_distance = FloatParameter(
        "tomtom_requests.min_distance",
        lower_bound=0.0,
        description=(
            "Minimum Euclidean distance between two consecutive points of a request, in meters."
        ),
        example=1000,
        note=(
            "The constraint applies to each origin-destination pair of a request (origin to first "
            "waypoint, between waypoints, last waypoint to destination). "
            "Distances are computed in the projected CRS. "
            "An error is raised if there is no node satisfying the distance constraints from a "
            "drawn node."
        ),
    )
    max_distance = FloatParameter(
        "tomtom_requests.max_distance",
        lower_bound=0.0,
        description=(
            "Maximum Euclidean distance between two consecutive points of a request, in meters."
        ),
        example=10000,
        note=(
            "The constraint applies to each origin-destination pair of a request (origin to first "
            "waypoint, between waypoints, last waypoint to destination). "
            "Distances are computed in the projected CRS. "
            "An error is raised if there is no node satisfying the distance constraints from a "
            "drawn node."
        ),
    )
    nb_batches = IntParameter(
        "tomtom_requests.nb_batches",
        description="Number of batches to be computed in parallel.",
        default=1,
        note=(
            "Ideally, the value should be the number of threads that you want to use. "
            "If the value is too large, TomTom API might complain about exceeding the number of "
            "requests per second."
        ),
    )
    api_key = StringParameter(
        "tomtom_requests.api_key",
        description="Key for the TomTom API.",
        note=(
            "You need to generate your own API key on the `my.tomtom.com` portal. "
            'You can use the `"secret:tomtom_api_key"` syntax to keep the key secret when sharing '
            "your configuration TOML file."
        ),
    )

    input_files = {"edges": RoadEdgesCleanFile}
    output_files = {"results": TomTomRoutesFile}
    priority = 0

    def is_defined(self):
        return self.date is not None and self.nb_routes is not None and self.api_key is not None

    def run(self):
        assert self.date is not None
        assert self.nb_routes is not None
        assert self.api_key is not None
        assert self.nb_waypoints is not None
        assert self.nb_batches is not None
        assert self.excluded_edge_types is not None

        edges: gpd.GeoDataFrame = self.input["edges"].read()

        nodes, coordinates = generate_random_nodes(
            edges=edges,
            rng=self.get_rng(str(self)),
            nb_routes=self.nb_routes,
            nb_waypoints=self.nb_waypoints,
            excluded_edge_types=self.excluded_edge_types,
            min_distance=self.min_distance,
            max_distance=self.max_distance,
            crs=self.crs,
        )

        if self.departure_time is None:
            departure_time = None
        else:
            departure_time = timedelta(seconds=self.departure_time.seconds())

        gdf = asyncio.run(
            get_tomtom_data(
                api_key=self.api_key,
                nodes=nodes,
                coordinates=coordinates,
                rng=self.get_rng(str(self)),
                departure_time=departure_time,
                date=self.date,
                nb_batches=self.nb_batches,
            )
        )

        gdf = gdf.to_crs(self.crs)

        self.output["results"].write(gdf)

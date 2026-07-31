from __future__ import annotations

import asyncio
import time
from collections.abc import Generator
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING

from loguru import logger

from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_network.road_network.files import RoadEdgesCleanFile
from pymetropolis.metro_pipeline.parameters import (
    DateParameter,
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
    import aiohttp
    import geopandas as gpd
    import numpy as np
    from tqdm import tqdm

BASE_URL = "https://api.tomtom.com/routing/1/calculateRoute/"
PARAMS = {"computeTravelTimeFor": "all", "traffic": "true"}
MAX_CONSECUTIVE_ERRORS = 8


def generate_random_nodes(
    edges: gpd.GeoDataFrame,
    rng: np.random.Generator,
    nb_routes: int,
    nb_waypoints: int,
    excluded_edge_types: list[str] = [],
) -> tuple[np.ndarray, np.ndarray]:
    import geopandas as gpd
    import numpy as np

    logger.debug("Generating random origin-destination pairs...")
    # Remove excluded edge types.
    mask = ~edges["edge_type"].isin(excluded_edge_types)
    edges = gpd.GeoDataFrame(edges.loc[mask].copy())
    all_nodes = list(edges["source"])
    # Number of nodes to draw for each route (origin + destination + waypoints).
    nb_nodes = nb_waypoints + 2
    selected_nodes = rng.choice(all_nodes, size=(nb_routes, nb_nodes))
    # Dictionary node_id -> lng, lat.
    source_to_xy = (
        edges.set_index("source")["geometry"]
        .to_crs("EPSG:4326")
        .apply(lambda geom: geom.coords[0])
        .to_dict()
    )
    coordinates = np.array([source_to_xy[node] for node in selected_nodes.flatten()])
    coordinates = coordinates.reshape(nb_routes, nb_nodes, 2)
    # Switch latitude and longitude.
    coordinates = coordinates[:, :, ::-1]
    return selected_nodes, coordinates


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
            if departure_time is None:
                departure_time = timedelta(seconds=int(rng.integers(0, 24 * 60 * 60)))
            td = datetime.combine(date, datetime.min.time()) + departure_time
            params["departAt"] = td.strftime("%Y-%m-%dT%H:%M:%S")
            t0 = time.perf_counter()
            data = await get_tomtom_request(url, session, params)
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
            rng=self.get_rng(),
            nb_routes=self.nb_routes,
            nb_waypoints=self.nb_waypoints,
            excluded_edge_types=self.excluded_edge_types,
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
                rng=self.get_rng(),
                departure_time=departure_time,
                date=self.date,
                nb_batches=self.nb_batches,
            )
        )

        gdf = gdf.to_crs(self.crs)

        self.output["results"].write(gdf)

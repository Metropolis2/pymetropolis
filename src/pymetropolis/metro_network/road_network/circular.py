from math import cos, pi, sin

import geopandas as gpd
import numpy as np
from shapely.geometry import LineString

from pymetropolis.metro_common.errors import MetropyError, error_context
from pymetropolis.metro_pipeline import Config, ConfigTable, ConfigValue, Step

from .files import RAW_EDGES_FILE

NB_RADIALS = ConfigValue(
    "circular_network.nb_radials",
    key="nb_radials",
    default=8,
    expected_type=int,
    description="Number of radial axis.",
)

NB_RINGS = ConfigValue(
    "circular_network.nb_rings",
    key="nb_rings",
    expected_type=int,
    description="Number of rings.",
)

RADIUS = ConfigValue(
    "circular_network.radius",
    key="radius",
    default=4000.0,
    expected_type=float | list[float],
    description="Radius of each ring, in meters.",
    note="If a scalar, the distance between each ring. If a list, the (cumulative) distance of each ring to the center",
)

RESOLUTION = ConfigValue(
    "circular_network.resolution",
    key="resolution",
    default=8,
    expected_type=int,
    description="The number of points in the geometry of the ring roads.",
)

CIRCULAR_NETWORK_TABLE = ConfigTable(
    "circular_network",
    "circular_network",
    items=[NB_RADIALS, NB_RINGS, RADIUS, RESOLUTION],
    description="Import a road network from an arbitrary list of edges.",
)


@error_context(msg="Cannot generate circular network")
def generate_circular_network(config: Config) -> bool:
    nb_radials = config[NB_RADIALS]
    nb_rings = config[NB_RINGS]
    resolution = config[RESOLUTION]
    radius = config[RADIUS]
    if isinstance(radius, list):
        if len(radius) != nb_rings:
            raise MetropyError("The number of `radius` values must be equal to the number of rings")
        center_dist = [0.0] + radius
    else:
        assert isinstance(radius, int | float)
        center_dist = [i * float(radius) for i in range(nb_rings + 1)]
    if nb_radials == 2:
        directions = ["East", "West"]
    elif nb_radials == 4:
        directions = ["East", "North", "West", "South"]
    elif nb_radials == 8:
        directions = [
            "E",
            "NE",
            "N",
            "NW",
            "W",
            "SW",
            "S",
            "SE",
        ]
    else:
        if nb_radials <= 0:
            raise MetropyError("The radial number must be at least 1")
        directions = [f"A{i}" for i in range(1, nb_radials + 1)]
    edges = list()
    # Add radial edges.
    for ring in range(1, nb_rings + 1):
        for i, dir in enumerate(directions):
            n1 = f"{dir} {ring}"
            if ring == 1:
                n2 = "CBD"
            else:
                n2 = f"{dir} {ring - 1}"
            length = center_dist[ring] - center_dist[ring - 1]
            angle = 2 * pi * i / nb_radials
            x1 = center_dist[ring] * cos(angle) / 1000
            y1 = center_dist[ring] * sin(angle) / 1000
            x2 = center_dist[ring - 1] * cos(angle) / 1000
            y2 = center_dist[ring - 1] * sin(angle) / 1000
            edges.append(
                {
                    "edge_id": f"In {ring} - {dir}",
                    "source": n1,
                    "target": n2,
                    "length": length,
                    "road_type": f"Radial {ring}",
                    "geometry": LineString([[x1, y1], [x2, y2]]),
                }
            )
            edges.append(
                {
                    "edge_id": f"Out {ring} - {dir}",
                    "source": n2,
                    "target": n1,
                    "length": length,
                    "road_type": f"Radial {ring}",
                    "geometry": LineString([[x2, y2], [x1, y1]]),
                }
            )
    # Add ring edges.
    for ring in range(1, nb_rings + 1):
        for i in range(nb_radials):
            j = (i + 1) % nb_radials
            dir1 = directions[i]
            dir2 = directions[j]
            n1 = f"{dir1} {ring}"
            n2 = f"{dir2} {ring}"
            length = 2 * pi * center_dist[ring] / nb_radials
            angles = np.linspace(2 * pi * i / nb_radials, 2 * pi * (i + 1) / nb_radials, resolution)
            xs = center_dist[ring] * np.cos(angles) / 1000
            ys = center_dist[ring] * np.sin(angles) / 1000
            points = list(zip(xs, ys))
            edges.append(
                {
                    "edge_id": f"{dir1}-{dir2} {ring}",
                    "source": n1,
                    "target": n2,
                    "length": length,
                    "road_type": f"Ring {ring}",
                    "geometry": LineString(points),
                }
            )
            edges.append(
                {
                    "edge_id": f"{dir2}-{dir1} {ring}",
                    "source": n2,
                    "target": n1,
                    "length": length,
                    "road_type": f"Ring {ring}",
                    "geometry": LineString(points[::-1]),
                }
            )
    gdf = gpd.GeoDataFrame(edges)
    RAW_EDGES_FILE.save(gdf, config)
    return True


CIRCULAR_NETWORK_STEP = Step(
    "circular-network",
    generate_circular_network,
    output_files=[RAW_EDGES_FILE],
    config_values=[NB_RADIALS, NB_RINGS, RADIUS, RESOLUTION],
)

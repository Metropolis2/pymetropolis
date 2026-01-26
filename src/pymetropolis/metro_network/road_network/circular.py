from math import cos, pi, sin
from typing import Any

import geopandas as gpd
import numpy as np
from shapely.geometry import LineString
from typeguard import TypeCheckError, check_type

from pymetropolis.metro_common.errors import MetropyError
from pymetropolis.metro_pipeline import Step
from pymetropolis.metro_pipeline.parameters import (
    BoolParameter,
    CustomParameter,
    FloatParameter,
    IntParameter,
)

from .files import RawEdgesFile


def generate_circular_network(
    nb_radials: int,
    nb_rings: int,
    resolution: int,
    radius: float | list[float],
    with_ramps: bool,
    entry_ramps_length: float,
    exit_ramps_length: float,
    ring_inter_ramp_length: float,
    radial_inter_ramp_length: float,
) -> gpd.GeoDataFrame:
    if isinstance(radius, list):
        if len(radius) != nb_rings:
            raise MetropyError("The number of `radius` values must be equal to the number of rings")
        center_dist = [0.0] + radius
        min_radius = np.min(np.diff(center_dist))
    else:
        assert isinstance(radius, int | float)
        center_dist = [i * float(radius) for i in range(nb_rings + 1)]
        min_radius = radius
    if nb_radials == 2:
        directions = ["E", "W"]
    elif nb_radials == 4:
        directions = ["E", "N", "W", "S"]
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
        if nb_radials <= 1:
            raise MetropyError("The radial number must be at least 2")
        directions = [f"D{i}" for i in range(1, nb_radials + 1)]
    if with_ramps:
        min_ring_length = 2 * pi * center_dist[1] / nb_radials
        if ring_inter_ramp_length > min_ring_length:
            raise MetropyError(
                f"`ring_inter_ramp_length` cannot be greater than the smallest ring road length ({min_ring_length})"
            )
        if radial_inter_ramp_length > min_radius:
            raise MetropyError(
                f"`radial_inter_ramp_length` cannot be greater than the smallest inter-ring distance ({min_radius:.0f})"
            )
    else:
        in_ramp_length = 0.0
        out_ramp_length = 0.0
        ring_inter_ramp_length = 0.0
        radial_inter_ramp_length = 0.0
    edges = list()

    # Add radial edges.
    for ring in range(1, nb_rings + 1):
        for i, dir in enumerate(directions):
            nout = f"{dir}-{ring}"
            if with_ramps:
                nout_inner = f"{nout}i"
                nout_outer = f"{nout}o"
            else:
                nout_inner, nout_outer = nout, nout
            if ring == 1:
                nin_outer = "CBD"
            elif with_ramps:
                nin_outer = f"{dir}-{ring - 1}o"
            else:
                nin_outer = f"{dir}-{ring - 1}"
            # The radial ring starts at the radius of the previous ring (+ half the inter-ramp
            # length if the previous ring is not the CBD).
            start = center_dist[ring - 1]
            if ring > 1:
                start += radial_inter_ramp_length / 2.0
            # The radial ring ends at the radius of the current ring - half the inter-ramp length.
            end = center_dist[ring] - radial_inter_ramp_length / 2.0
            length = end - start
            assert length > 0.0
            angle = 2 * pi * i / nb_radials
            x1 = start * cos(angle) / 1000
            y1 = start * sin(angle) / 1000
            x2 = end * cos(angle) / 1000
            y2 = end * sin(angle) / 1000
            edges.append(
                {
                    "edge_id": f"In{ring}-{dir}",
                    "source": nout_inner,
                    "target": nin_outer,
                    "length": length,
                    "road_type": f"Radial {ring}",
                    "geometry": LineString([[x2, y2], [x1, y1]]),
                }
            )
            edges.append(
                {
                    "edge_id": f"Out{ring}-{dir}",
                    "source": nin_outer,
                    "target": nout_inner,
                    "length": length,
                    "road_type": f"Radial {ring}",
                    "geometry": LineString([[x1, y1], [x2, y2]]),
                }
            )
            if with_ramps:
                # Add radial ramp segment.
                x1 = end * cos(angle) / 1000
                y1 = end * sin(angle) / 1000
                x2 = (end + radial_inter_ramp_length) * cos(angle) / 1000
                y2 = (end + radial_inter_ramp_length) * sin(angle) / 1000
                edges.append(
                    {
                        "edge_id": f"InTunnel{ring}-{dir}",
                        "source": nout_outer,
                        "target": nout_inner,
                        "length": radial_inter_ramp_length,
                        "road_type": f"RadialTunnel {ring}",
                        "geometry": LineString([[x2, y2], [x1, y1]]),
                    }
                )
                edges.append(
                    {
                        "edge_id": f"OutTunnel{ring}-{dir}",
                        "source": nout_inner,
                        "target": nout_outer,
                        "length": radial_inter_ramp_length,
                        "road_type": f"RadialTunnel {ring}",
                        "geometry": LineString([[x1, y1], [x2, y2]]),
                    }
                )

    # Add ring edges.
    for ring in range(1, nb_rings + 1):
        for i in range(nb_radials):
            j = (i + 1) % nb_radials
            dir_right = directions[i]
            dir_left = directions[j]
            n_right = f"{dir_right}-{ring}"
            if with_ramps:
                n_right_right = f"{n_right}r"
                n_right_left = f"{n_right}l"
            else:
                n_right_right, n_right_left = n_right, n_right
            n_left = f"{dir_left}-{ring}"
            if with_ramps:
                n_left_right = f"{n_left}r"
            else:
                n_left_right = n_left
            inter_ramp_degree = ring_inter_ramp_length / center_dist[ring]
            start_angle = 2 * pi * i / nb_radials + inter_ramp_degree / 2.0
            end_angle = 2 * pi * (i + 1) / nb_radials - inter_ramp_degree / 2.0
            length = 2 * pi * center_dist[ring] / nb_radials - ring_inter_ramp_length
            angles = np.linspace(start_angle, end_angle, resolution)
            xs = center_dist[ring] * np.cos(angles) / 1000
            ys = center_dist[ring] * np.sin(angles) / 1000
            points = list(zip(xs, ys))
            edges.append(
                {
                    "edge_id": f"{dir_right}-{dir_left}-{ring}",
                    "source": n_right_left,
                    "target": n_left_right,
                    "length": length,
                    "road_type": f"Ring {ring}",
                    "geometry": LineString(points),
                }
            )
            edges.append(
                {
                    "edge_id": f"{dir_left}-{dir_right}-{ring}",
                    "source": n_left_right,
                    "target": n_right_left,
                    "length": length,
                    "road_type": f"Ring {ring}",
                    "geometry": LineString(points[::-1]),
                }
            )
            if with_ramps:
                # Add ring ramp segments.
                angles = np.linspace(start_angle - inter_ramp_degree, start_angle, resolution)
                xs = center_dist[ring] * np.cos(angles) / 1000
                ys = center_dist[ring] * np.sin(angles) / 1000
                points = list(zip(xs, ys))
                edges.append(
                    {
                        "edge_id": f"LeftBridge{ring}-{dir_right}",
                        "source": n_right_right,
                        "target": n_right_left,
                        "length": ring_inter_ramp_length,
                        "road_type": f"RingBridge {ring}",
                        "geometry": LineString(points),
                    }
                )
                edges.append(
                    {
                        "edge_id": f"RightBridge{ring}-{dir_right}",
                        "source": n_right_left,
                        "target": n_right_right,
                        "length": ring_inter_ramp_length,
                        "road_type": f"RingBridge {ring}",
                        "geometry": LineString(points[::-1]),
                    }
                )

    if with_ramps:
        # Add ramps.
        for ring in range(1, nb_rings + 1):
            for i, dir in enumerate(directions):
                n = f"{dir}-{ring}"
                n_inner = f"{n}i"
                n_outer = f"{n}o"
                n_right = f"{n}r"
                n_left = f"{n}l"
                angle = 2 * pi * i / nb_radials
                inter_ramp_degree = ring_inter_ramp_length / center_dist[ring]
                # x,y for inner node.
                d_inner = center_dist[ring] - radial_inter_ramp_length / 2.0
                x_inner = d_inner * cos(angle) / 1000
                y_inner = d_inner * sin(angle) / 1000
                # x,y for outer node.
                d_outer = center_dist[ring] + radial_inter_ramp_length / 2.0
                x_outer = d_outer * cos(angle) / 1000
                y_outer = d_outer * sin(angle) / 1000
                # x,y for right node.
                angle_right = angle - inter_ramp_degree / 2.0
                x_right = center_dist[ring] * cos(angle_right) / 1000
                y_right = center_dist[ring] * sin(angle_right) / 1000
                # x,y for left node.
                angle_left = angle + inter_ramp_degree / 2.0
                x_left = center_dist[ring] * cos(angle_left) / 1000
                y_left = center_dist[ring] * sin(angle_left) / 1000
                # Inner -> Right.
                edges.append(
                    {
                        "edge_id": f"EntryRight{ring}-{dir}",
                        "source": n_inner,
                        "target": n_right,
                        "length": in_ramp_length,
                        "road_type": f"EntryRamp {ring}",
                        "geometry": LineString([[x_inner, y_inner], [x_right, y_right]]),
                    }
                )
                # Right -> Outer.
                edges.append(
                    {
                        "edge_id": f"ExitRight{ring}-{dir}",
                        "source": n_right,
                        "target": n_outer,
                        "length": out_ramp_length,
                        "road_type": f"ExitRamp {ring}",
                        "geometry": LineString([[x_right, y_right], [x_outer, y_outer]]),
                    }
                )
                # Outer -> Left.
                edges.append(
                    {
                        "edge_id": f"EntryLeft{ring}-{dir}",
                        "source": n_outer,
                        "target": n_left,
                        "length": in_ramp_length,
                        "road_type": f"EntryRamp {ring}",
                        "geometry": LineString([[x_outer, y_outer], [x_left, y_left]]),
                    }
                )
                # Left -> Inner.
                edges.append(
                    {
                        "edge_id": f"ExitLeft{ring}-{dir}",
                        "source": n_left,
                        "target": n_inner,
                        "length": out_ramp_length,
                        "road_type": f"ExitRamp {ring}",
                        "geometry": LineString([[x_left, y_left], [x_inner, y_inner]]),
                    }
                )

    gdf = gpd.GeoDataFrame(edges)
    return gdf


def validate_radius(value: Any) -> float | list[float]:
    try:
        return check_type(value, float | list[float])
    except TypeCheckError:
        raise MetropyError(f"Not a numeric or list of numeric: `{value}`")


class CircularNetworkStep(Step):
    nb_radials = IntParameter("circular_network.nb_radials", description="Number of radial axis.")
    nb_rings = IntParameter("circular_network.nb_rings", description="Number of rings.")
    radius = CustomParameter(
        "circular_network.radius",
        validator=validate_radius,
        description="Radius of each ring, in meters.",
        note="If a scalar, the distance between each ring. If a list, the (cumulative) distance of each ring to the center",
    )
    resolution = IntParameter(
        "circular_network.resolution",
        default=16,
        description="The number of points in the geometry of the ring roads.",
    )
    with_ramps = BoolParameter(
        "circular_network.with_ramps",
        default=False,
        description="Whether entry / exit ramps to the ring roads should be added.",
    )
    entry_ramps_length = FloatParameter(
        "circular_network.entry_ramps_length",
        default=0.0,
        description="Length of entry ramps, in meters.",
    )
    exit_ramps_length = FloatParameter(
        "circular_network.exit_ramps_length",
        default=0.0,
        description="Length of exit ramps, in meters.",
    )
    ring_inter_ramp_length = FloatParameter(
        "circular_network.ring_inter_ramp_length",
        default=0.0,
        description="Length of the ring road segments (bridges) between the left and right ramps, in meters.",
    )
    radial_inter_ramp_length = FloatParameter(
        "circular_network.radial_inter_ramp_length",
        default=0.0,
        description="Length of the radial road segments (tunnels) between the clockwise and counter-clockwise ramps, in meters.",
    )
    output_files = {"raw_edges": RawEdgesFile}

    def is_defined(self) -> bool:
        return self.nb_radials is not None and self.nb_rings is not None and self.radius is not None

    def run(self):
        gdf = generate_circular_network(
            nb_radials=self.nb_radials,
            nb_rings=self.nb_rings,
            radius=self.radius,
            resolution=self.resolution,
            with_ramps=self.with_ramps,
            entry_ramps_length=self.entry_ramps_length,
            exit_ramps_length=self.exit_ramps_length,
            ring_inter_ramp_length=self.ring_inter_ramp_length,
            radial_inter_ramp_length=self.radial_inter_ramp_length,
        )
        self.output["raw_edges"].write(gdf)

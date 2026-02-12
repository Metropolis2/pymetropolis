import geopandas as gpd
from shapely.geometry import LineString

from pymetropolis.metro_common.errors import MetropyError
from pymetropolis.metro_pipeline import Step
from pymetropolis.metro_pipeline.parameters import (
    BoolParameter,
    FloatParameter,
    IntParameter,
)

from .files import RawEdgesFile


def generate_grid_network(
    nb_rows: int,
    nb_columns: int,
    length: float,
    left_to_right: bool = False,
    right_to_left: bool = False,
    bottom_to_top: bool = False,
    top_to_bottom: bool = False,
) -> gpd.GeoDataFrame:
    if nb_rows <= 0:
        raise MetropyError("Grid network must have at least 1 row.")
    if nb_columns <= 0:
        raise MetropyError("Grid network must have at least 1 column.")
    if nb_columns == 1 and nb_rows == 1:
        raise MetropyError("Grid network cannot have only 1 row and 1 column.")

    if nb_columns > 1 and not left_to_right and not right_to_left:
        raise MetropyError("At least one of `left_to_right` and `right_to_left` must be `true`.")
    if nb_rows > 1 and not bottom_to_top and not top_to_bottom:
        raise MetropyError("At least one of `bottom_to_top` and `top_to_bottom` must be `true`.")

    edges = list()
    # Add horizontal edges.
    for y in range(nb_rows):
        for x in range(nb_columns - 1):
            # For row y, add edge from column x to column x + 1.
            source = f"Node_{x}_{y}"
            target = f"Node_{x + 1}_{y}"
            if left_to_right:
                edges.append(
                    {
                        "edge_id": f"Row_{y}:{x}to{x + 1}",
                        "source": source,
                        "target": target,
                        "length": length,
                        "road_type": "LeftToRight",
                        "geometry": LineString([[x, y], [x + 1, y]]),
                    }
                )
            if right_to_left:
                edges.append(
                    {
                        "edge_id": f"Row_{y}:{x + 1}to{x}",
                        "source": target,
                        "target": source,
                        "length": length,
                        "road_type": "RightToLeft",
                        "geometry": LineString([[x + 1, y], [x, y]]),
                    }
                )

    # Add vertical edges.
    for x in range(nb_columns):
        for y in range(nb_rows - 1):
            # For column x, add edge from row y to row y + 1.
            source = f"Node_{x}_{y}"
            target = f"Node_{x}_{y + 1}"
            if bottom_to_top:
                edges.append(
                    {
                        "edge_id": f"Col_{x}:{y}to{y + 1}",
                        "source": source,
                        "target": target,
                        "length": length,
                        "road_type": "BottomToTop",
                        "geometry": LineString([[x, y], [x, y + 1]]),
                    }
                )
            if top_to_bottom:
                edges.append(
                    {
                        "edge_id": f"Col_{x}:{y + 1}to{y}",
                        "source": target,
                        "target": source,
                        "length": length,
                        "road_type": "TopToBottom",
                        "geometry": LineString([[x, y + 1], [x, y]]),
                    }
                )

    gdf = gpd.GeoDataFrame(edges)
    return gdf


class GridNetworkStep(Step):
    """Generates a toy road network from a grid.

    The network is defined by a number of rows and columns.
    The first node is located at the bottom left, at coordinates (0, 0).
    Then, the node on the i-th column and j-th row has coordinates (i - 1, j - 1).
    The total number of nodes is `nb_columns * nb_rows`.

    By default, edges are bidirectional, connecting all adjacent nodes.
    The total number of edges is thus
    `2 * nb_columns * (nb_rows - 1) + 2 * nb_rows * (nb_columns - 1)`.

    The `length` parameter control the length of all edges.

    With the parameters `left_to_right`, `right_to_left`, `bottom_to_top`, and `top_to_bottom` (by
    default all equal to `true`), it is possible to disable the creation of edges in some direction.
    For example, if `right_to_left` is set to `false`, all horizontal edges will be unidirectional
    (towards increasing `x` coordinate).

    Be careful, if the edges are not all bidirectional, the grid network is not strongly connected
    which can cause issue later on in the simulation.

    A network of successive roads can be created by setting `nb_rows` to 1, `nb_columns` to the
    desired number of roads + 1, and `right_to_left` to `false`.

    Node ids are set to `"Node_{x}_{y}"`, where `x` is the index of the column and `y` is the index
    of the row (starting at 0).

    For row edges, ids are set to `"Row_{y}:{x1}to{x2}"` for row `y`, when connecting column `x1` to
    column `x2`.
    For column edges, ids are set to `"Col_{x}:{y1}to{y2}"` for column `x`, when connecting row `x1`
    to row `x2`.

    Road types are set to the edge direction: `"LeftToRight"`, `"RightToLeft"`, `"TopToBottom"`, or
    `"BottomToTop"`.
    """

    nb_rows = IntParameter(
        "grid_network.nb_rows", description="Number of rows (i.e., number of nodes on each column)."
    )
    nb_columns = IntParameter(
        "grid_network.nb_columns",
        description="Number of columns (i.e., number of nodes on each row).",
    )
    length = FloatParameter("grid_network.length", description="Length of an edge, in meters.")
    left_to_right = BoolParameter(
        "grid_network.left_to_right",
        default=True,
        description="Whether edges going from left to right should be generated.",
    )
    right_to_left = BoolParameter(
        "grid_network.right_to_left",
        default=True,
        description="Whether edges going from right to left should be generated.",
    )
    bottom_to_top = BoolParameter(
        "grid_network.bottom_to_top",
        default=True,
        description="Whether edges going from bottom to top should be generated.",
    )
    top_to_bottom = BoolParameter(
        "grid_network.top_to_bottom",
        default=True,
        description="Whether edges going from top to bottom should be generated.",
    )
    output_files = {"raw_edges": RawEdgesFile}

    def is_defined(self) -> bool:
        return self.nb_rows is not None and self.nb_columns is not None and self.length is not None

    def run(self):
        gdf = generate_grid_network(
            nb_rows=self.nb_rows,
            nb_columns=self.nb_columns,
            length=self.length,
            left_to_right=self.left_to_right,
            right_to_left=self.right_to_left,
            bottom_to_top=self.bottom_to_top,
            top_to_bottom=self.top_to_bottom,
        )
        self.output["raw_edges"].write(gdf)

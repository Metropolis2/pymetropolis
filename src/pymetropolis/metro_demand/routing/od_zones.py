from __future__ import annotations

from typing import TYPE_CHECKING

from loguru import logger

from pymetropolis.metro_demand.routing.od_pairs import StepWithRoadForbiddenTypes
from pymetropolis.metro_demand.zones.file import (
    ZonesLevel1File,
    ZonesLevel2File,
    ZonesLevel3File,
    ZonesLevel4File,
    ZonesLevel5File,
)
from pymetropolis.metro_network.road_network.files import RoadEdgesCleanFile
from pymetropolis.metro_pipeline.parameters import FloatParameter, IntParameter
from pymetropolis.metro_spatial import GeoStep

from .files import (
    ZonesLevel1RoadNodeFile,
    ZonesLevel2RoadNodeFile,
    ZonesLevel3RoadNodeFile,
    ZonesLevel4RoadNodeFile,
    ZonesLevel5RoadNodeFile,
)

if TYPE_CHECKING:
    import geopandas as gpd
    import numpy as np
    import polars as pl


class ZonesBaseModel(GeoStep, StepWithRoadForbiddenTypes):
    threshold = FloatParameter(
        "zones.weiszfeld.threshold",
        default=1e-6,
        description=(
            "Convergence threshold for the Weiszfeld algorithm used to approximate each zone's "
            "geometric median road node."
        ),
        note=(
            "The algorithm stops once the candidate point moves by less than this distance "
            "(in the network's CRS units) between two iterations."
        ),
    )
    max_iter = IntParameter(
        "zones.weiszfeld.max_iter",
        default=100,
        description=(
            "Maximum number of iterations for the Weiszfeld algorithm used to approximate each "
            "zone's geometric median road node."
        ),
    )

    priority = 0

    def weiszfeldOptimumNode(self, nodes: np.ndarray) -> np.ndarray:
        """Find the approximate of the geometric median of nodes."""
        import numpy as np

        assert self.max_iter is not None
        assert self.threshold is not None

        node_ = nodes.mean(axis=0)
        for _ in range(self.max_iter):
            distances = np.sqrt(np.sum((nodes - node_) ** 2, axis=1))
            if np.all(distances != 0):
                weights = 1 / distances
                new_node_ = np.sum(weights[:, None] * nodes, axis=0) / np.sum(weights)
            else:
                new_node_ = nodes[distances == 0][0, :]

            if np.sqrt(np.sum((node_ - new_node_) ** 2)) < self.threshold:
                return new_node_

            node_ = new_node_
        return node_

    def find_origin_destination_node(
        self, zones: gpd.GeoDataFrame, edges: gpd.GeoDataFrame
    ) -> pl.DataFrame:
        import geopandas as gpd
        import pandas as pd
        import polars as pl
        from scipy.spatial import KDTree
        from shapely.geometry import Point

        logger.debug("Listing candidate road-network nodes.")
        source_nodes = edges[["source", "geometry"]].rename(columns={"source": "node"})
        source_nodes["geometry"] = source_nodes["geometry"].apply(lambda g: Point(g.coords[0]))
        target_nodes = edges[["target", "geometry"]].rename(columns={"target": "node"})
        target_nodes["geometry"] = target_nodes["geometry"].apply(lambda g: Point(g.coords[-1]))
        nodes = gpd.GeoDataFrame(
            pd.concat([source_nodes, target_nodes], ignore_index=True), crs=edges.crs
        ).drop_duplicates(subset="node")

        logger.debug("Assigning each road-network node to its zone.")
        nodes = nodes.sjoin(zones, how="inner", predicate="within").drop(columns=["index_right"])

        logger.debug("Finding the optimum_node node in each zone.")
        nodes["x"] = nodes.geometry.x
        nodes["y"] = nodes.geometry.y
        df = pl.from_pandas(nodes.loc[:, ["node", "zone_id", "x", "y"]])
        optimum_nodes: dict = {}
        for (zone_id,), zone_df in df.partition_by(
            "zone_id", as_dict=True, include_key=False
        ).items():
            nodes_ = zone_df.select("x", "y").to_numpy()
            tree = KDTree(nodes_)
            o_node = self.weiszfeldOptimumNode(nodes_)
            _, optimum_node_idx = tree.query(o_node)
            optimum_nodes[zone_id] = zone_df["node"][int(optimum_node_idx)]
        return pl.DataFrame(
            {"zone_id": list(optimum_nodes.keys()), "road_node": list(optimum_nodes.values())}
        ).with_columns(pl.col("road_node").cast(pl.UInt64))

    def run(self):
        zones = self.input["zone"].read()
        zones = zones.to_crs(self.crs)
        edges = self.input["edges"].read()
        edges = edges.loc[
            ~edges["edge_type"].isin(self.forbidden_types),
            ["edge_id", "geometry", "source", "target"],
        ]
        nodes = self.find_origin_destination_node(zones, edges)
        self.output["zone_road_node"].write(nodes)


class ZonesLevel1RoadNodesStep(ZonesBaseModel):
    input_files = {"zone": ZonesLevel1File, "edges": RoadEdgesCleanFile}
    output_files = {"zone_road_node": ZonesLevel1RoadNodeFile}


class ZonesLevel2RoadNodesStep(ZonesBaseModel):
    input_files = {"zone": ZonesLevel2File, "edges": RoadEdgesCleanFile}
    output_files = {"zone_road_node": ZonesLevel2RoadNodeFile}


class ZonesLevel3RoadNodesStep(ZonesBaseModel):
    input_files = {"zone": ZonesLevel3File, "edges": RoadEdgesCleanFile}
    output_files = {"zone_road_node": ZonesLevel3RoadNodeFile}


class ZonesLevel4RoadNodesStep(ZonesBaseModel):
    input_files = {"zone": ZonesLevel4File, "edges": RoadEdgesCleanFile}
    output_files = {"zone_road_node": ZonesLevel4RoadNodeFile}


class ZonesLevel5RoadNodesStep(ZonesBaseModel):
    input_files = {"zone": ZonesLevel5File, "edges": RoadEdgesCleanFile}
    output_files = {"zone_road_node": ZonesLevel5RoadNodeFile}

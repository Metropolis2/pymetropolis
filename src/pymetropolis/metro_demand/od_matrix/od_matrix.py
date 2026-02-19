import geopandas as gpd
import polars as pl
from shapely.geometry import LineString, Point

from pymetropolis.metro_demand.modes.car import CarODsFile
from pymetropolis.metro_demand.od_matrix.file import RoadODMatrixFile
from pymetropolis.metro_network.road_network.files import CleanEdgesFile
from pymetropolis.metro_pipeline import Step


class RoadODMatrixStep(Step):
    """Construct an origin-destination matrix (at the road-network node level) from the trips'
    origins and destinations.
    """

    input_files = {"edges": CleanEdgesFile, "car_ods": CarODsFile}
    output_files = {"od_matrix": RoadODMatrixFile}

    def run(self):
        ods = self.input["car_ods"].read()
        od_matrix = ods.group_by("origin_node_id", "destination_node_id").len()
        edges_gdf = self.input["edges"].read()
        source_points = dict()
        target_points = dict()
        for _, row in edges_gdf.iterrows():
            source_points[row["source"]] = Point(row["geometry"].coords[0]).wkb
            target_points[row["target"]] = Point(row["geometry"].coords[-1]).wkb
        od_matrix = od_matrix.with_columns(
            source_point=pl.col("origin_node_id").replace_strict(source_points),
            target_point=pl.col("destination_node_id").replace_strict(target_points),
        )
        linestrings = [
            LineString([s, t])
            for s, t in zip(
                gpd.GeoSeries.from_wkb(od_matrix["source_point"]),
                gpd.GeoSeries.from_wkb(od_matrix["target_point"]),
            )
        ]
        gdf = gpd.GeoDataFrame(
            od_matrix.select("origin_node_id", "destination_node_id", size="len").to_pandas(),
            geometry=linestrings,
        )
        self.output["od_matrix"].write(gdf)

from osmium.osm import Way

from pymetropolis.metro_network.osm import OpenStreetMapNetworkImport
from pymetropolis.metro_network.pedestrian_network.files import PedestrianEdgesRawFile
from pymetropolis.metro_pipeline.parameters import ListParameter
from pymetropolis.metro_pipeline.steps import InputFile
from pymetropolis.metro_pipeline.types import String
from pymetropolis.metro_spatial import GeoStep, OSMStep
from pymetropolis.metro_spatial.simulation_area.file import SimulationAreaFile


class OSMPedestrianNetworkImport(OpenStreetMapNetworkImport):
    def extra_way_filter(self, way: Way) -> bool:
        """Returns True if the candidate way should be imported."""
        has_access = "access" not in way.tags or way.tags["access"] != "private"
        return has_access and not way.tags.get("area") == "yes"


class OpenStreetMapPedestrianImportStep(GeoStep, OSMStep):
    """Imports a pedestrian network from OpenStreetMap data.

    Edges of the pedestrian network are read from the OpenStreetMap ways with tag
    [highway:*](https://wiki.openstreetmap.org/wiki/Key:highway).

    The [`osm_pedestrian_import.highways`](parameters.md#osm_pedestrian_importhighways) parameter is
    used to define the `highway` values which are part of the pedestrian network. For example,
    values `"footway"` and `"path"` should usually be included, while value `"motorway"` should be
    excluded as motorways are generally not allowed for pedestrians.

    In addition to the `highways` parameter, this step requires both the
    [`osm_file`](parameters.md#osm_file) and [`crs`](parameters.md#crs) parameters to be set.

    For example, to import the pedestrian network of Paris, you can use:

    ```toml
    osm_file = "path/to/paris.osm.pbf"
    crs = "epsg:2154"

    [osm_pedesrian_import]
    highways = [
        "primary",
        "primary_link",
        "secondary",
        "secondary_link",
        "tertiary",
        "tertiary_link",
        "living_street",
        "unclassified",
        "residential",
        "road",
        "service",
        "track",
        "footway",
        "path",
        "pedestrian",
    ]
    ```

    An OpenStreetMap way is selected as valid pedestrian-network edge if it satisfies all the
    conditions below:

    - Tag `highway` matches one of the value given in
      [`highways`](parameters.md#osm_pedestrian_importhighways) parameter.
    - The way has no tag `access` or tag `access` is not `"private"`.
    - The way's geometry is a valid LineString.
    - The way intersects with the simulation area (if the
      [SimulationAreaFile](files.md#simulationareafile)) exists).

    Edges attributes are defined as follows:

    - `edge_id`: OSM id of the way, with "r" appended if the edge is going backward, with "-[idx]"
      appended if the edge is split in multiple segments (with `idx` the segment index).
    - `source`: OSM id of the source node.
    - `target`: OSM id of the target node.
    - `original_id`: OSM id of the way (note that values are generally not unique).
    - `length`: computed as geometric operation on the ways' LineString, after conversion to the
      simulation CRS.
    - `road_type`: `highway` tag value.
    - `name`: `name` tag value if any, otherwise `addr:street` tag value if any, otherwise `ref` tag
      value if any.
    """

    highways = ListParameter(
        "osm_pedestrian_import.highways",
        inner=String(),
        min_length=1,
        description=(
            "List of `highway=*` OpenStreetMap tags to be considered as valid pedetrian ways."
        ),
        example='`["track", "footway", "path", "pedestrian"]`',
        note=(
            "A list of highway tags with description is available on the "
            "[OpenStreetMap wiki](https://wiki.openstreetmap.org/wiki/Key:highway)."
        ),
    )

    input_files = {"simulation_area": InputFile(SimulationAreaFile, optional=True)}
    output_files = {"raw_edges": PedestrianEdgesRawFile}

    def is_defined(self) -> bool:
        return self.crs is not None and self.osm_file is not None and self.highways is not None

    def run(self):
        importer = OSMPedestrianNetworkImport(
            osm_file=self.osm_file,
            highway_tags=self.highways,
            crs=self.crs,
            filter_polygon=self.input["simulation_area"].get_area_opt(),  # ty: ignore[unresolved-attribute]
        )
        edges = importer.run()
        self.output["raw_edges"].write(edges)

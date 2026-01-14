from metro_common.errors import error_context
from metro_common.io import read_geodataframe
from metro_pipeline import Config, ConfigTable, InputFile, Step

from .files import RAW_EDGES_FILE

EDGES_FILE = InputFile(
    "custom_road_import.edges_file",
    key="edges_file",
    description="Path to the geospatial file containing the edges definition.",
    example='`"data/my_edges.geojson"`',
)

CUSTOM_ROAD_IMPORT_TABLE = ConfigTable(
    "custom_road_import",
    "custom_road_import",
    items=[EDGES_FILE],
    description="Import a road network from an arbitrary list of edges.",
)


@error_context(msg="Cannot import road network from custom edges")
def import_network(config: Config) -> bool:
    """Main function to import a road network from custom nodes / edges files."""
    input_filename = config[EDGES_FILE]
    edges = read_geodataframe(input_filename)
    RAW_EDGES_FILE.save(edges, config)
    return True


CUSTOM_ROAD_IMPORT = Step(
    "custom-road-import",
    import_network,
    output_files=[RAW_EDGES_FILE],
    config_values=[EDGES_FILE],
)

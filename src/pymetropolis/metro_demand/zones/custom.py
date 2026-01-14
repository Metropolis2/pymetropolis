from pymetropolis.metro_common.errors import error_context
from pymetropolis.metro_common.io import read_geodataframe
from pymetropolis.metro_pipeline import Config, InputFile, Step

from .file import ZONES_FILE

CUSTOM_ZONES_FILE = InputFile(
    "zones.custom_zones",
    key="custom_zones",
    description="Path to the geospatial file containing the zones definition.",
    example='`"data/my_zones.geojson"`',
)


@error_context(msg="Cannot import custom zones from geospatial file")
def import_zones(config: Config) -> bool:
    """Main function to import custom zones from geospatial file."""
    input_filename = config[CUSTOM_ZONES_FILE]
    zones = read_geodataframe(input_filename)
    ZONES_FILE.save(zones, config)
    return True


CUSTOM_ZONES_IMPORT = Step(
    "custom-zones-import",
    import_zones,
    output_files=[ZONES_FILE],
    config_values=[CUSTOM_ZONES_FILE],
)

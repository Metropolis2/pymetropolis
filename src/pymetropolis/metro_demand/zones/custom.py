from pymetropolis.metro_common.io import read_geodataframe
from pymetropolis.metro_pipeline.parameters import PathParameter
from pymetropolis.metro_pipeline.steps import Step

from .file import ZonesFile


class CustomZonesStep(Step):
    """Reads zones from a geospatial file.

    The input file can have these three columns:

    - `geometry`: Polygon or MultiPolygon of the zones
    - `zone_id`: Identifier of the zone (integer or sting)
    - `name`: Name of the zone (string)

    The two first columns are mandatory.
    """

    custom_zones_file = PathParameter(
        "zones.custom_zones",
        check_file_exists=True,
        description="Path to the geospatial file containing the zones definition.",
        example='`"data/my_zones.geojson"`',
    )
    output_files = {"zones": ZonesFile}

    def is_defined(self) -> bool:
        return self.custom_zones_file is not None

    def run(self):
        input_filename = self.custom_zones_file
        zones = read_geodataframe(input_filename)
        self.output["zones"].write(zones)

from pymetropolis.metro_common.io import read_geodataframe
from pymetropolis.metro_pipeline.parameters import PathParameter
from pymetropolis.metro_pipeline.steps import MetroStep

from .file import ZonesFile


class CustomZonesStep(MetroStep):
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

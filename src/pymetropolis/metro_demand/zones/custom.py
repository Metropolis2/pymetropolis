from pymetropolis.metro_common.io import read_geodataframe
from pymetropolis.metro_pipeline.parameters import PathParameter
from pymetropolis.metro_pipeline.steps import Step

from .file import (
    ZonesLevel1File,
    ZonesLevel2File,
    ZonesLevel3File,
    ZonesLevel4File,
    ZonesLevel5File,
)


class CustomZonesLevel1Step(Step):
    """Reads level-1 zones from a geospatial file.

    The input file can have these three columns:

    - `geometry`: Polygon or MultiPolygon of the zones
    - `zone_id`: Identifier of the zone (integer or sting)
    - `name`: Name of the zone (string)

    The two first columns are mandatory.
    """

    file = PathParameter(
        "zones.custom_files.level1",
        check_file_exists=True,
        description="Path to the geospatial file containing the zones definition for level 1.",
        example='`"data/my_zones.geojson"`',
    )
    output_files = {"zones": ZonesLevel1File}

    def is_defined(self) -> bool:
        return self.file is not None

    def run(self):
        zones = read_geodataframe(self.file)
        self.output["zones"].write(zones)


class CustomZonesLevel2Step(Step):
    """Reads level-2 zones from a geospatial file.

    The input file can have these three columns:

    - `geometry`: Polygon or MultiPolygon of the zones
    - `zone_id`: Identifier of the zone (integer or sting)
    - `name`: Name of the zone (string)

    The two first columns are mandatory.
    """

    file = PathParameter(
        "zones.custom_files.level2",
        check_file_exists=True,
        description="Path to the geospatial file containing the zones definition for level 2.",
        example='`"data/my_zones.geojson"`',
    )
    output_files = {"zones": ZonesLevel2File}

    def is_defined(self) -> bool:
        return self.file is not None

    def run(self):
        zones = read_geodataframe(self.file)
        self.output["zones"].write(zones)


class CustomZonesLevel3Step(Step):
    """Reads level-3 zones from a geospatial file.

    The input file can have these three columns:

    - `geometry`: Polygon or MultiPolygon of the zones
    - `zone_id`: Identifier of the zone (integer or sting)
    - `name`: Name of the zone (string)

    The two first columns are mandatory.
    """

    file = PathParameter(
        "zones.custom_files.level3",
        check_file_exists=True,
        description="Path to the geospatial file containing the zones definition for level 3.",
        example='`"data/my_zones.geojson"`',
    )
    output_files = {"zones": ZonesLevel3File}

    def is_defined(self) -> bool:
        return self.file is not None

    def run(self):
        zones = read_geodataframe(self.file)
        self.output["zones"].write(zones)


class CustomZonesLevel4Step(Step):
    """Reads level-4 zones from a geospatial file.

    The input file can have these three columns:

    - `geometry`: Polygon or MultiPolygon of the zones
    - `zone_id`: Identifier of the zone (integer or sting)
    - `name`: Name of the zone (string)

    The two first columns are mandatory.
    """

    file = PathParameter(
        "zones.custom_files.level4",
        check_file_exists=True,
        description="Path to the geospatial file containing the zones definition for level 4.",
        example='`"data/my_zones.geojson"`',
    )
    output_files = {"zones": ZonesLevel4File}

    def is_defined(self) -> bool:
        return self.file is not None

    def run(self):
        zones = read_geodataframe(self.file)
        self.output["zones"].write(zones)


class CustomZonesLevel5Step(Step):
    """Reads level-5 zones from a geospatial file.

    The input file can have these three columns:

    - `geometry`: Polygon or MultiPolygon of the zones
    - `zone_id`: Identifier of the zone (integer or sting)
    - `name`: Name of the zone (string)

    The two first columns are mandatory.
    """

    file = PathParameter(
        "zones.custom_files.level5",
        check_file_exists=True,
        description="Path to the geospatial file containing the zones definition for level 5.",
        example='`"data/my_zones.geojson"`',
    )
    output_files = {"zones": ZonesLevel5File}

    def is_defined(self) -> bool:
        return self.file is not None

    def run(self):
        zones = read_geodataframe(self.file)
        self.output["zones"].write(zones)

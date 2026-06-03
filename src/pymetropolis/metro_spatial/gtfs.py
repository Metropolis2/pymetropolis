from pymetropolis.metro_pipeline.parameters import ListParameter
from pymetropolis.metro_pipeline.steps import Step
from pymetropolis.metro_pipeline.types import PathType


class GTFSStep(Step):
    gtfs_files = ListParameter(
        "gtfs_files",
        inner=PathType(check_file_exists=True),
        description="List of GTFS files that form the public-transit network.",
        example='`["data/gtfs/madrid-gtfs.zip"]`',
    )

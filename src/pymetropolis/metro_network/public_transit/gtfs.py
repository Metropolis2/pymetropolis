from pymetropolis.metro_pipeline.parameters import DateParameter, ListParameter
from pymetropolis.metro_pipeline.steps import Step
from pymetropolis.metro_pipeline.types import PathType


class GTFSStep(Step):
    gtfs_files = ListParameter(
        "gtfs.files",
        inner=PathType(check_file_exists=True),
        description="List of GTFS files that form the public-transit network.",
        example='`["data/gtfs/madrid-gtfs.zip"]`',
    )
    gtfs_date = DateParameter(
        "gtfs.date",
        description="Date to be considered for the public-transit network.",
        note="Ensure that the GTFS file(s) have active services for this date.",
    )

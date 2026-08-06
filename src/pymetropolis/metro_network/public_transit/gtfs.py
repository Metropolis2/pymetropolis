from pymetropolis.metro_pipeline.parameters import DateParameter, ListParameter
from pymetropolis.metro_pipeline.steps import Step
from pymetropolis.metro_pipeline.types import PathType
from pymetropolis.metro_spatial import GeoStep

from .files import PublicTransitRoutesFile, PublicTransitStopsFile


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


class ReadPublicTransitNetworkStep(GTFSStep, GeoStep):
    """Reads the input GTFS file(s) and extracts all stops and routes.

    If [`gtfs.date`](parameters.md#gtfsdate) is specified, only stops and routes with at least one
    event on the given date are extracted.
    """

    output_files = {"stops": PublicTransitStopsFile, "routes": PublicTransitRoutesFile}

    def is_defined(self):
        return self.gtfs_files is not None

    def run(self):
        assert self.gtfs_files is not None

        # PFE: Read GTFS file(s), extract stops and routes with their characteristics.
        # If `gtfs_date` is specified restrict to stops / routes used on the given date.
        # Create Point geometries for stop locations from lng, lat. Transform them to the
        # simulation's crs (`self.crs`).
        pass

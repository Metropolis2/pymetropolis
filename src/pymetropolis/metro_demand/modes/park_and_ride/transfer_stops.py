from pymetropolis.metro_demand.population.files import TripsFile, TripsOriginsFile
from pymetropolis.metro_network.public_transit.files import (
    PublicTransitRoutesFile,
    PublicTransitStopsFile,
)
from pymetropolis.metro_pipeline import Step
from pymetropolis.metro_pipeline.parameters import ListParameter
from pymetropolis.metro_pipeline.types import Int, String

from .files import ParkAndRideStopsFile


class ParkAndRideFacilitiesFromNearestStopStep(Step):
    """Generates park-and-ride facilities location for each tour based on nearest stop location."""

    route_types = ListParameter(
        "modes.park_and_ride.allowed_route_types",
        inner=Int(),
        description=(
            "List of route types to be considered as valid transport mode for park-and-ride "
            "facilities selection."
        ),
        note=(
            "Route types follow the GTFS specification. "
            "If not specified, all route types are considered as valid."
        ),
    )
    agencies = ListParameter(
        "modes.park_and_ride.allowed_agencies",
        inner=String(),
        description=(
            "List of agency ids to be considered as valid agencies for park-and-ride facilities "
            "selection."
        ),
        note="If not specified, all agencies are considered as valid.",
    )
    input_files = {
        "trips": TripsFile,
        "origins": TripsOriginsFile,
        "stops": PublicTransitStopsFile,
        "routes": PublicTransitRoutesFile,
    }
    output_files = {"facilities": ParkAndRideStopsFile}
    primary = 0

    def run(self):
        # PFR. Compute the nearest PT stops (considering the optional route types / agency ids
        # criteria) for the first origin of each tour.
        # Origins and stops locations are already in the same projected CRS, so distance computation
        # can be done directly.
        # This should just be a `sjoin_nearest` basically I think.
        #
        # Note that for tours where the first origin is not the same as the last destination, then
        # we get nearest stop to first origin, which might not be the same as nearest stop to last
        # destination. In any case, it does not really make sense to allow P+R for these trips. For
        # Eqasim, I think there is no such trips given the way tours are defined (to be checked).
        # Nevertheless, we should check that they are properly handled (maybe handle them normally
        # but discard them when writing input).
        #
        # Also, we should check what happen for single-trip tour (probably we want to allow P+R,
        # with just a single car trip followed by a PT trip). This means that when computing things
        # for both the first trip and last trip of each tour in the next steps (e.g., when computing
        # PT time), the "last trip" should only be defined if there are at least two trips in the
        # tour, i.e., a single-trip tour has a "first trip" but no "last trip".
        pass

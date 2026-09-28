from __future__ import annotations

from typing import TYPE_CHECKING

from loguru import logger

from pymetropolis.metro_demand.population.files import TripsFile, TripsOriginsFile
from pymetropolis.metro_network.public_transit.files import (
    PublicTransitRoutesFile,
    PublicTransitStopsFile,
)
from pymetropolis.metro_pipeline import PopulationStep
from pymetropolis.metro_pipeline.parameters import ListParameter
from pymetropolis.metro_pipeline.types import Int, String
from pymetropolis.modes import StepWithModes

from .files import ParkAndRideStopsFile

if TYPE_CHECKING:
    import polars as pl


def park_and_ride_eligible_trips(trips: pl.DataFrame) -> pl.DataFrame:
    """Returns the first trip of the tours eligible to park-and-ride, with their number of trips.

    A tour is eligible if it starts from home and either ends at home or has a single trip.
    """
    import polars as pl

    tours = (
        trips.sort("tour_id", "trip_index")
        .group_by("tour_id", maintain_order=True)
        .agg(
            first_trip_id=pl.col("trip_id").first(),
            last_trip_id=pl.col("trip_id").last(),
            nb_trips=pl.len(),
            first_purpose=pl.col("origin_purpose_group").first(),
            last_purpose=pl.col("destination_purpose_group").last(),
        )
    )
    return tours.filter(
        pl.col("first_purpose") == "home",
        (pl.col("last_purpose") == "home") | (pl.col("nb_trips") == 1),
    ).select("tour_id", "first_trip_id", "last_trip_id", "nb_trips")


def park_and_ride_car_trips(trips: pl.DataFrame, tour_ids: pl.Series) -> pl.DataFrame:
    """Returns the trips with a car part when traveling by park-and-ride, for the given tours.

    The car part is at the beginning of the first trip of the tour (`is_outbound = True`, from the
    origin to the P+R facility) and at the end of the last trip of the tour (`is_outbound = False`,
    from the P+R facility to the destination).
    Single-trip tours only have an outbound car part.
    """
    import polars as pl

    trips = trips.filter(pl.col("tour_id").is_in(tour_ids.implode())).sort("tour_id", "trip_index")
    is_first = pl.col("trip_id") == pl.col("trip_id").first().over("tour_id")
    is_last = (pl.col("trip_id") == pl.col("trip_id").last().over("tour_id")) & ~is_first
    return trips.filter(is_first | is_last).select("trip_id", "tour_id", is_outbound=is_first)


class ParkAndRideFacilitiesFromNearestStopStep(StepWithModes, PopulationStep):
    """Generates park-and-ride facilities location for each tour based on nearest stop location.

    Only the tours that start from home and end at home (or that have a single trip) are eligible
    to park-and-ride.
    For each eligible tour, the P+R facility is the public-transit stop nearest to the origin of
    the first trip of the tour, among the stops served by at least one valid route.
    Valid routes can be restricted by route type
    ([`modes.park_and_ride.allowed_route_types`](parameters.md#modespark_and_rideallowed_route_types))
    and by agency
    ([`modes.park_and_ride.allowed_agencies`](parameters.md#modespark_and_rideallowed_agencies)).

    For example, to only consider train stations as P+R facilities:

    ```toml
    [modes.park_and_ride]
    allowed_route_types = [2]
    ```
    """

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
    priority = 0

    def is_defined(self) -> bool:
        return self.has_mode("park_and_ride")

    def run(self):
        import polars as pl

        routes: pl.DataFrame = self.input["routes"].read()
        if self.route_types is not None:
            routes = routes.filter(pl.col("route_type").is_in(self.route_types))
        if self.agencies is not None:
            routes = routes.filter(pl.col("agency_id").cast(pl.String).is_in(self.agencies))
        stops = self.input["stops"].read()
        valid_route_ids = set(routes["route_id"])
        is_valid = stops["route_ids"].apply(lambda ids: any(r in valid_route_ids for r in ids))
        stops = stops.loc[is_valid, ["stop_id", "geometry"]]
        logger.info(f"Number of valid P+R stops: {len(stops):,}")
        if stops.empty:
            logger.warning("No valid stop for park-and-ride facilities")

        tours = park_and_ride_eligible_trips(self.input["trips"].read())
        logger.info(f"Number of tours eligible to P+R: {len(tours):,}")
        origins = self.input["origins"].read()
        origins = origins.loc[
            origins["trip_id"].isin(tours["first_trip_id"]), ["trip_id", "geometry"]
        ]
        facilities = origins.sjoin_nearest(stops, how="inner")
        # Duplicate indices occur when there are two stops at the same distance.
        facilities = facilities.drop_duplicates(subset=["trip_id"])
        # Use the location of the stop (not of the origin) as geometry.
        facilities["geometry"] = stops.loc[facilities["index_right"], "geometry"].values
        facilities = facilities.merge(
            tours.select("tour_id", trip_id="first_trip_id").to_pandas(), on="trip_id"
        )
        facilities = facilities.loc[:, ["tour_id", "stop_id", "geometry"]].rename(
            columns={"stop_id": "park_and_ride_stop_id"}
        )
        self.output["facilities"].write(facilities)

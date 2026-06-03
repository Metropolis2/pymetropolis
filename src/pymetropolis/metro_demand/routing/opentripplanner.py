from __future__ import annotations

import math
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import humanize
import requests
from loguru import logger
from requests.exceptions import RequestException

from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_common.time import MetroTime
from pymetropolis.metro_demand.departure_time.files import TstarsFile
from pymetropolis.metro_demand.population.files import (
    TripsDestinationsFile,
    TripsFile,
    TripsOriginsFile,
)
from pymetropolis.metro_demand.routing.files import TripsPublicTransitItinerariesFile
from pymetropolis.metro_pipeline import Step
from pymetropolis.metro_pipeline.parameters import (
    DateParameter,
    EnumParameter,
    FloatParameter,
    IntParameter,
    StringParameter,
    TimeParameter,
)
from pymetropolis.metro_pipeline.steps import InputFile

if TYPE_CHECKING:
    import polars as pl

MAX_TRIES = 3

HEADERS = {"Content-Type": "application/json", "OTPTimeout": "10000"}

QUERY = """
query plan(
    $originLat: CoordinateValue!,
    $originLng: CoordinateValue!,
    $destinationLat: CoordinateValue!,
    $destinationLng: CoordinateValue!,
    $datetime: OffsetDateTime!,
    $walkSpeed: Speed!,
    $walkReluctance: Reluctance!,
    $waitReluctance: Reluctance!,
    $busReluctance: Reluctance!,
    $tramReluctance: Reluctance!,
    $subwayReluctance: Reluctance!,
    $railReluctance: Reluctance!,
    $transferCost: Cost!,
) {
    planConnection(
        origin: {
          location:  { coordinate: { latitude: $originLat, longitude: $originLng } }
        }
        destination: {
          location:  { coordinate: { latitude: $destinationLat, longitude: $destinationLng } }
        }
        dateTime: { __DATETIME_FIELD__: $datetime }
        modes: {
          transitOnly: true
          transit: {
            transit: [
            { mode: BUS,        cost: { reluctance: $busReluctance } },
            { mode: COACH,      cost: { reluctance: $busReluctance } }
            { mode: TROLLEYBUS, cost: { reluctance: $busReluctance } }
            { mode: TRAM,       cost: { reluctance: $tramReluctance } }
            { mode: CABLE_CAR,  cost: { reluctance: $tramReluctance }  }
            { mode: FUNICULAR,  cost: { reluctance: $tramReluctance } }
            { mode: FERRY,      cost: { reluctance: $tramReluctance } }
            { mode: SUBWAY,     cost: { reluctance: $subwayReluctance } }
            { mode: MONORAIL,   cost: { reluctance: $subwayReluctance } }
            { mode: RAIL,       cost: { reluctance: $railReluctance } }
            ]
          }
        }
        preferences: {
          street: {
            walk: { speed: $walkSpeed, reluctance: $walkReluctance, boardCost: 0, safetyFactor: 0 }
          }
          transit: {
            board: { waitReluctance: $waitReluctance }
            transfer: { slack: "PT1M", cost: $transferCost }
          }
        }
    ) {
      edges {
        node {
          duration
          generalizedCost
          waitingTime
          legs {
            mode
            duration
            route { gtfsId }
            from { stop { gtfsId } }
            to { stop { gtfsId } }
          }
        }
      }
    }
}
"""

QUERY_ARRIVAL = QUERY.replace("__DATETIME_FIELD__", "latestArrival")
QUERY_DEPARTURE = QUERY.replace("__DATETIME_FIELD__", "earliestDeparture")

_thread_local = threading.local()


def get_session() -> requests.Session:
    if not hasattr(_thread_local, "session"):
        session = requests.Session()
        session.headers.update(HEADERS)
        _thread_local.session = session
    return _thread_local.session


def run_queries(
    trips: pl.DataFrame,
    api_url: str,
    parameters: dict,
    batch_size: int | None = None,
    nb_threads: int | None = None,
) -> pl.DataFrame:
    import polars as pl

    batch_size = batch_size or len(trips)
    batch_size = max(batch_size, 1)
    nb_batches = math.ceil(len(trips) / batch_size)
    if nb_batches == 1:
        return run_queries_batch(trips, api_url, parameters, nb_threads)
    with tempfile.TemporaryDirectory() as tmp_dir:
        for i in range(nb_batches):
            df = run_queries_batch(
                trips[i * batch_size : (i + 1) * batch_size], api_url, parameters, nb_threads
            )
            df.write_parquet(Path(tmp_dir) / Path(f"otp_results_{i}.parquet"))
            del df
        df = pl.concat(
            (
                pl.scan_parquet(Path(tmp_dir) / Path(f"otp_results_{i}.parquet"))
                for i in range(nb_batches)
            ),
            how="vertical",
        ).collect()
    return df  # ty: ignore[invalid-return-type]


def run_queries_batch(
    trips: pl.DataFrame, api_url: str, parameters: dict, nb_threads: int | None = None
) -> pl.DataFrame:
    import polars as pl
    from tqdm import tqdm

    logger.debug("Running new batch")
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=nb_threads) as executor:
        futures = [
            executor.submit(get_least_cost_itinerary, row, api_url, parameters)
            for row in trips.iter_rows(named=True)
        ]
        results = []
        for future in tqdm(
            as_completed(futures), total=len(futures), desc="Processing batch", smoothing=0.01
        ):
            results.append(future.result())
    df = pl.from_records(
        results,
        orient="row",
        schema=[
            ("trip_id", trips.schema["trip_id"]),
            ("travel_time", pl.Float64),
            ("generalized_time", pl.Float64),
            ("waiting_time", pl.Float64),
            (
                "legs",
                pl.List(
                    pl.Struct(
                        [
                            pl.Field("mode", pl.String),
                            pl.Field("travel_time", pl.Float64),
                            pl.Field("route_id", pl.String),
                            pl.Field("from_stop_id", pl.String),
                            pl.Field("to_stop_id", pl.String),
                        ]
                    )
                ),
            ),
            ("query_time", pl.Float64),
        ],
    )
    tot_query_time = timedelta(seconds=float(df["query_time"].sum()))
    tot_time = timedelta(seconds=time.time() - t0)
    logger.debug(f"Total query time: {humanize.precisedelta(tot_query_time)}")
    logger.debug(f"Total time: {humanize.precisedelta(tot_time)}")
    df = df.with_columns(
        travel_time=pl.duration(seconds=pl.col("travel_time")),
        generalized_time=pl.duration(seconds=pl.col("generalized_time")),
        waiting_time=pl.duration(seconds=pl.col("waiting_time")),
        legs=pl.col("legs").list.eval(
            pl.element().struct.with_fields(
                travel_time=pl.duration(seconds=pl.element().struct.field("travel_time"))
            )
        ),
    )
    return df


def get_least_cost_itinerary(row: dict, api_url: str, parameters: dict, nb_tries: int = 0):
    variables = {
        **parameters,
        "datetime": f"{row['date']}T{row['time']}+00:00",
        "originLat": row["origin_lat"],
        "originLng": row["origin_lng"],
        "destinationLat": row["destination_lat"],
        "destinationLng": row["destination_lng"],
    }
    if row["arrive_by"]:
        query = QUERY_ARRIVAL
    else:
        query = QUERY_DEPARTURE
    session = get_session()
    try:
        t0 = time.time()
        req = session.post(api_url, headers=HEADERS, json={"query": query, "variables": variables})
        query_time = time.time() - t0
        req.raise_for_status()
        data = req.json()
    except (RequestException, ValueError) as e:
        if nb_tries > MAX_TRIES:
            raise MetropyError(f"OpenTripPlanner request failed: {e}")
        # Retry.
        return get_least_cost_itinerary(row, api_url, parameters, nb_tries + 1)
    itineraries = data.get("data", dict()).get("planConnection", dict()).get("edges", None)
    if itineraries is None:
        raise MetropyError(f"Invalid OpenTripPlanner result data:\n{data}")
    # Find the itinerary with the least cost.
    edge = min(itineraries, key=lambda it: it["node"]["generalizedCost"], default=None)
    if edge is None:
        # No itinerary found.
        return (row["trip_id"], None, None, None, None, None)
    it = edge["node"]
    legs = [clean_leg(leg) for leg in it["legs"]]
    return (
        row["trip_id"],
        it["duration"],
        it["generalizedCost"],
        it["waitingTime"],
        legs,
        query_time,
    )


def clean_leg(leg: dict):
    if leg["route"] is not None:
        route_id = leg["route"]["gtfsId"]
        from_stop = leg["from"]["stop"]["gtfsId"]
        to_stop = leg["to"]["stop"]["gtfsId"]
    else:
        route_id = None
        from_stop = None
        to_stop = None
    result = {
        "mode": leg["mode"],
        "travel_time": leg["duration"],
        "route_id": route_id,
        "from_stop_id": from_stop,
        "to_stop_id": to_stop,
    }
    return result


def clean_trips_time(
    trips: pl.DataFrame, tstars: pl.DataFrame | None, time_type: str, time: MetroTime | None
):
    """Add `seconds` and `arrive_by` column to trips."""
    import polars as pl

    if time_type in ("departure", "arrival"):
        col_name = f"{time_type}_time"
        if col_name not in trips.columns:
            trips = trips.with_columns(pl.lit(None, dtype=pl.Duration).alias(col_name))
        trips = trips.select(
            "trip_id", seconds=pl.col(col_name).dt.total_seconds(), arrive_by=time_type == "arrival"
        )
    elif time_type == "tstar":
        if tstars is None:
            raise MetropyError('tstars should be given when `time_type` is `"tstar"`')
        trips = (
            trips.select("trip_id")
            .join(
                tstars.select("trip_id", seconds=pl.col("tstar").dt.total_seconds()),
                on="trip_id",
                how="left",
            )
            .with_columns(arrive_by=True)
        )
    else:
        trips = trips.select(
            "trip_id", seconds=pl.lit(None, dtype=pl.Int64), arrive_by=time_type == "custom_arrival"
        )
    if trips["seconds"].is_null().any():
        if time is None:
            c = trips["seconds"].null_count()
            raise MetropyError(
                f"Departure / arrival time is undefined for {c:,} trips but the "
                "`opentripplanner.time` parameter is not set."
            )
        if "custom" not in time_type:
            s = trips["seconds"].null_count() / len(trips)
            logger.warning(
                f"{s:.1%} trips have NULL values for departure / arrival time, using default "
                "value for them"
            )
        # Fill null values for time column with the given time parameter.
        trips = trips.with_columns(pl.col("seconds").fill_null(round(time.seconds())))
    return trips


class TripsOpenTripPlannerStep(Step):
    """Computes the trips' travel time and generalized time by public transit with OpenTripPlanner.

    This step requires having access to an OpenTripPlanner API server.
    You can run one on your machine by following the
    [OpenTripPlanner documentation](https://docs.opentripplanner.org/en/latest/Basic-Tutorial).
    This step has been tested with OpenTripPlanner v2.9.0.

    If you want a simpler (but less flexible) way to compute public-transit travel times, check out
    [TripsPublicTransitTravelTimeFromR5Step](steps.md#tripspublictransittraveltimefromr5step).

    The OpenTripPlanner server must be running when you execute Pymetropolis and accessible from the
    URL [`opentripplanner.url`](parameters.md#opentripplannerurl) (`"http://0.0.0.0:8080"` by
    default).

    The [`date`](parameters.md#opentripplannerdate) parameter controls the date used in the
    public-transit timetables, for all queries.
    You need to ensure that the GTFS file(s) have running services at this date.

    The departure / arrival time of the trips is control by the
    [`time_type`](parameters.md#opentripplannertime_type) and
    [`time`](parameters.md#opentripplannertime) parameters.
    Five different cases are possible:

    - `time_type = "departure"`: trips are departing at their ex-ante departure time, read from
      [TripsFile](files.md#tripsfile) (with `time` as a fallback value)
    - `time_type = "arrival"`: trips are arriving at their ex-ante arrival time, read from
      [TripsFile](files.md#tripsfile) (with `time` as a fallback value)
    - `time_type = "tstar"`: trips are arriving at their desired arrival time, read from
      [TstarsFile](files.md#tstarsfile) (with `time` as a fallback value)
    - `time_type = "custom_departure"`: trips are all departing at time `time`
    - `time_type = "custom_arrival"`: trips are all arriving at time `time`

    Note that the departure / arrival time represents a time window, controllable by
    OpenTripPlanner: for example, a trips with departure time `08:00:00` might actually depart at
    `08:04:00` to match the schedule of a bus.

    The itinerary selected by OpenTripPlanner is the one that minimizes the "generalized time",
    which is equal to the travel time with different weights applied to different modes.

    You can control how the generalized time function is defined through
    several parameters:

    - [`walking_speed`](parameters.md#opentripplannerwalking_speed): walking speed on the pedestrian
      network, in km/h (default is 4 km/h).
    - [`transfer_cost`](parameters.md#opentripplannertransfer_cost): penalty for transfers, in
      seconds equivalent (default is 5 minutes).
    - [`multipliers.walk`](parameters.md#opentripplannermultipliers.walk): multiplier for the value
      of time walking (default is 2).
    - [`multipliers.wait`](parameters.md#opentripplannermultipliers.wait): multiplier for the value
      of time waiting (default is 1.1).
    - [`multipliers.bus`](parameters.md#opentripplannermultipliers.bus): multiplier for the value of
      time in a bus (default is 1.2).
    - [`multipliers.tram`](parameters.md#opentripplannermultipliers.tram): multiplier for the value
      of time in a tramway (default is 1).
    - [`multipliers.subway`](parameters.md#opentripplannermultipliers.subway): multiplier for the
      value of time in a subway (default is 1).
    - [`multipliers.rail`](parameters.md#opentripplannermultipliers.rail): multiplier for the value
      of time for rail transport (default is 1).

    When running this step, the public-transit itineraries of all trips are hold in memory.
    If you are running out of memory, you can try to use the
    [`batch_size`](parameters.md#opentripplannerbatch_size) parameter to reduce RAM consumption.
    Reducing the batch size should reduce memory consumption, at the cost of an increase in running
    time.
    By default, all trips are run in a single batch.

    Example of configuration for this step:

    ```toml
    [opentripplanner]
    url = "http://0.0.0.0:8080"
    batch_size = 50000
    date = 2026-05-28
    time_type = "tstar"
    walking_speed = 4.0
    transfer_cost = 300
    [multipliers]
    walk = 2
    wait = 1.1
    bus = 1.2
    tram = 1
    subway = 1
    rail = 1
    """

    otp_url = StringParameter(
        "opentripplanner.url",
        default="http://0.0.0.0:8080",
        description="URL from which the OpenTripPlanner API can be accessed.",
    )
    batch_size = IntParameter(
        "opentripplanner.batch_size",
        description="How many trips should be processed in each batch.",
        note=(
            "Default is to process all trips in a single batch. "
            "Use a lower value if you are running out of memory."
        ),
    )
    date = DateParameter(
        "opentripplanner.date",
        description="Date to be used for the requests.",
        note="Ensure that the GTFS file read by OpenTripPlanner has active services for this date.",
    )
    time_type = EnumParameter(
        "opentripplanner.time_type",
        values=["departure", "arrival", "tstar", "custom_departure", "custom_arrival"],
        description="How the departure / arrival time of the requests is defined.",
        note=(
            "If `\"departure\"`, trips' departure times are read from the trips' ex-ante departure "
            "times. "
            "If `\"arrival\"`, trips' arrival times are read from the trips' ex-ante arrival "
            "times. "
            "If `\"tstar\"`, trips' arrival times are read from the trips' desired arrival times. "
            'If `"custom_departure"`, the departure times are equal to the value of '
            "`opentripplanner.time` for all trips. "
            'If `"custom_arrival"`, the arrival times are equal to the value of '
            "`opentripplanner.time` for all trips. "
        ),
    )
    time = TimeParameter(
        "opentripplanner.time",
        description="Departure / arrival time of the requests.",
        note=(
            'If `time_type` is `"custom_departure"`, this is the departure time used for all '
            "requests. "
            'If `time_type` is `"custom_arrival"`, this is the arrival time used for all '
            "requests. "
            "Otherwise, the value is only used as a default for missing departure / arrival time."
        ),
    )
    walking_speed = FloatParameter(
        "opentripplanner.walking_speed",
        default=4.0,
        description="Walking speed for public-transit trips, in km/h.",
    )
    walking_reluctance = FloatParameter(
        "opentripplanner.multipliers.walk",
        default=2.0,
        description="Multiplier for the value of time walking.",
    )
    waiting_reluctance = FloatParameter(
        "opentripplanner.multipliers.wait",
        default=1.1,
        description="Multiplier for the value of time waiting.",
    )
    bus_reluctance = FloatParameter(
        "opentripplanner.multipliers.bus",
        default=1.2,
        description="Multiplier for the value of time in a bus.",
    )
    tram_reluctance = FloatParameter(
        "opentripplanner.multipliers.tram",
        default=1.0,
        description="Multiplier for the value of time in a tramway.",
    )
    subway_reluctance = FloatParameter(
        "opentripplanner.multipliers.subway",
        default=1.0,
        description="Multiplier for the value of time in a subway.",
    )
    rail_reluctance = FloatParameter(
        "opentripplanner.multipliers.rail",
        default=1.0,
        description="Multiplier for the value of time for rail transport.",
    )
    transfer_cost = IntParameter(
        "opentripplanner.transfer_cost",
        default=300,
        description="Penalty for transfers, in seconds equivalent.",
    )
    input_files = {
        "trips": TripsFile,
        "origins": TripsOriginsFile,
        "destinations": TripsDestinationsFile,
        "tstars": InputFile(
            TstarsFile,
            when=lambda inst: inst.time_type == "tstar",
            when_doc='`time_type` is `"tstar"`',
        ),
    }
    output_files = {"costs": TripsPublicTransitItinerariesFile}

    def is_defined(self):
        return self.time_type is not None

    def run(self):
        import polars as pl

        trips = self.input["trips"].read()
        # Note that tstars are read even when not required. This could be optimized although the
        # impact is probably very small.
        trips = clean_trips_time(
            trips, self.input["tstars"].read_if_exists(), self.time_type, self.time
        )
        # Convert time column to a HH:MM:SS string.
        trips = trips.with_columns(
            time=pl.time(
                hour=pl.col("seconds") // 3600 % 24,
                minute=pl.col("seconds") // 60 % 60,
                second=pl.col("seconds") % 60,
            ).dt.strftime("%H:%M:%S")
        ).drop("seconds")
        # Add date.
        trips = trips.with_columns(date=self.date)

        # Read origin / destination longitude and latitude.
        origins = self.input["origins"].read()
        origins.to_crs("EPSG:4326", inplace=True)
        origins_df = pl.DataFrame(
            {
                "trip_id": origins["trip_id"],
                "origin_lng": origins.geometry.x,
                "origin_lat": origins.geometry.y,
            }
        )
        trips = trips.join(origins_df, on="trip_id")
        destinations = self.input["destinations"].read()
        destinations.to_crs("EPSG:4326", inplace=True)
        destinations_df = pl.DataFrame(
            {
                "trip_id": destinations["trip_id"],
                "destination_lng": destinations.geometry.x,
                "destination_lat": destinations.geometry.y,
            }
        )
        trips = trips.join(destinations_df, on="trip_id")

        parameters = {
            "walkSpeed": self.walking_speed / 3.6,
            "walkReluctance": self.walking_reluctance,
            "waitReluctance": self.waiting_reluctance,
            "busReluctance": self.bus_reluctance,
            "tramReluctance": self.tram_reluctance,
            "subwayReluctance": self.subway_reluctance,
            "railReluctance": self.rail_reluctance,
            "transferCost": self.transfer_cost,
        }

        # TODO: Add nb_threads parameter.
        df = run_queries(trips, self.otp_url, parameters, self.batch_size)
        self.output["costs"].write(df)

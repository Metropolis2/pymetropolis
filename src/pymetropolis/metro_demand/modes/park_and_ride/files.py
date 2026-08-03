from pymetropolis.metro_pipeline.file import Column, MetroDataType, MetroGeoDataFrameFile


class ParkAndRideStopsFile(MetroGeoDataFrameFile):
    # Note PFE: I set this to a GeoDataFrame (i.e. with the stop Point locations) so that transfer
    # stops can be easily visualized.
    path = "demand/population/modes/park_and_ride/transfer_stops.parquet"
    description = "Location of the P+R facility for each tour."
    schema = [
        Column(
            "tour_id",
            MetroDataType.ID,
            description="Identifier of the tour",
            unique=True,
            nullable=False,
        ),
        Column(
            "park_and_ride_stop_id",
            MetroDataType.ID,
            description="Identifier of the public-transit stop where the car is parked.",
            nullable=True,
        ),
    ]

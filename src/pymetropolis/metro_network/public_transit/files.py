from pymetropolis.metro_pipeline.file import (
    Column,
    MetroDataFrameFile,
    MetroDataType,
    MetroGeoDataFrameFile,
)


class PublicTransitStopsFile(MetroGeoDataFrameFile):
    path = "network/public_transit/stops.geo.parquet"
    description = "Characteristics of the public-transit stops."
    schema = [
        Column(
            "stop_id",
            MetroDataType.ID,
            description="Identifier of the stop.",
            unique=True,
            nullable=False,
        ),
        Column("name", MetroDataType.STRING, description="Name of the stop.", nullable=True),
        Column(
            "location_type",
            MetroDataType.STRING,
            description=(
                "Location type of the stop (platform, station, entrance, generic, boarding_area)."
            ),
            nullable=True,
        ),
        Column(
            "parent_station",
            MetroDataType.ID,
            description="Identifier of the stop corresponding to the parent station of this stop.",
            nullable=True,
        ),
        Column(
            "route_ids",
            MetroDataType.LIST_OF_IDS,
            description="Identifiers of the routes that serve this stop.",
            nullable=False,
        ),
    ]


class PublicTransitRoutesFile(MetroDataFrameFile):
    path = "network/public_transit/routes.parquet"
    description = "Characteristics of the public-transit routes."
    schema = [
        Column(
            "route_id",
            MetroDataType.ID,
            description="Identifier of the route.",
            unique=True,
            nullable=False,
        ),
        Column("name", MetroDataType.STRING, description="Name of the route.", nullable=True),
        Column(
            "agency_id",
            MetroDataType.ID,
            description="Identifier of the agency managing this route.",
            nullable=True,
        ),
        Column(
            "route_type",
            MetroDataType.UINT,
            description=(
                "Type of transportation for the route. Values follow the GTFS specification."
            ),
            nullable=False,
        ),
        Column(
            "color",
            MetroDataType.STRING,
            description="Color that should be used to represent the route (hex code).",
            nullable=True,
        ),
    ]

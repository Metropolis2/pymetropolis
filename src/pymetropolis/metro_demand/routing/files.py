from pymetropolis.metro_pipeline.file import Column, MetroDataFrameFile, MetroDataType


class TripsPedestrianNodesFile(MetroDataFrameFile):
    path = "demand/population/trips_pedestrian_nodes.parquet"
    description = "Origin and destination nodes on the pedestrian network for each trip."
    schema = [
        Column(
            "trip_id",
            MetroDataType.ID,
            description="Identifier of the trip.",
            unique=True,
            nullable=False,
        ),
        Column(
            "origin_pedestrian_node",
            MetroDataType.ID,
            description="Identifier of the origin node on the pedestrian network.",
            nullable=True,
        ),
        Column(
            "origin_pedestrian_node_dist",
            MetroDataType.FLOAT,
            description=(
                "Distance between the trip's origin and the corresponding pedestrian node, "
                "in meters."
            ),
            nullable=True,
            optional=True,
        ),
        Column(
            "origin_pedestrian_node_dist_on_edge",
            MetroDataType.FLOAT,
            description=(
                "Distance between the trip's origin and the corresponding pedestrian node, "
                "projected on the closest edge, in meters."
            ),
            nullable=True,
            optional=True,
        ),
        Column(
            "origin_pedestrian_edge",
            MetroDataType.ID,
            description="Identifier of the pedestrian edge closest to the trip's origin.",
            nullable=True,
            optional=True,
        ),
        Column(
            "origin_pedestrian_edge_dist",
            MetroDataType.FLOAT,
            description=(
                "Distance between the trip's origin and the closest pedestrian edge, in meters."
            ),
            nullable=True,
            optional=True,
        ),
        Column(
            "destination_pedestrian_node",
            MetroDataType.ID,
            description="Identifier of the destination node on the pedestrian network.",
            nullable=True,
        ),
        Column(
            "destination_pedestrian_node_dist",
            MetroDataType.FLOAT,
            description=(
                "Distance between the trip's destination and the corresponding pedestrian node, "
                "in meters."
            ),
            nullable=True,
            optional=True,
        ),
        Column(
            "destination_pedestrian_node_dist_on_edge",
            MetroDataType.FLOAT,
            description=(
                "Distance between the trip's destination and the corresponding pedestrian node, "
                "projected on the closest edge, in meters."
            ),
            nullable=True,
            optional=True,
        ),
        Column(
            "destination_pedestrian_edge",
            MetroDataType.ID,
            description="Identifier of the pedestrian edge closest to the trip's destination.",
            nullable=True,
            optional=True,
        ),
        Column(
            "destination_pedestrian_edge_dist",
            MetroDataType.FLOAT,
            description=(
                "Distance between the trip's destination and the closest pedestrian edge, "
                "in meters."
            ),
            nullable=True,
            optional=True,
        ),
    ]

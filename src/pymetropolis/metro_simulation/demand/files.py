from pymetropolis.metro_pipeline.file import Column, MetroDataFrameFile, MetroDataType


class MetroAgentsFile(MetroDataFrameFile):
    path = "run/input/agents.parquet"
    description = "TODO"
    schema = [
        Column(
            "agent_id",
            MetroDataType.ID,
            description="Identifier of the agent",
            unique=True,
            nullable=False,
        ),
        Column(
            "alt_choice.type",
            MetroDataType.STRING,
            description="Type of choice model for the alternative choice",
            nullable=True,
            optional=True,
        ),
        Column(
            "alt_choice.u",
            MetroDataType.FLOAT,
            description="Uniform draw to simulate the chosen alternative",
            nullable=True,
            optional=True,
        ),
        Column(
            "alt_choice.mu",
            MetroDataType.FLOAT,
            description="Variance of the stochastic terms in the utility function",
            nullable=True,
            optional=True,
        ),
    ]


class MetroAlternativesFile(MetroDataFrameFile):
    path = "run/input/alts.parquet"
    description = "TODO"
    schema = [
        Column(
            "agent_id",
            MetroDataType.ID,
            description="Identifier of the agent",
            nullable=False,
        ),
        Column(
            "alt_id",
            MetroDataType.ID,
            description="Identifier of the alternative",
            nullable=False,
        ),
        Column(
            "origin_delay",
            MetroDataType.FLOAT,
            description="Extra delay between the chosen departure time and the actual first trip start, in seconds",
            nullable=True,
            optional=True,
        ),
        Column(
            "dt_choice.type",
            MetroDataType.STRING,
            description="Whether departure time is exogenous, random discrete, or random continuous",
            nullable=True,
        ),
        Column(
            "dt_choice.departure_time",
            MetroDataType.FLOAT,
            description="Departure time, when exogenous, in seconds after midnight",
            nullable=True,
            optional=True,
        ),
        Column(
            "dt_choice.period",
            MetroDataType.LIST_OF_FLOATS,
            description="Time window in which the departure time is chosen",
            nullable=True,
            optional=True,
        ),
        Column(
            "dt_choice.interval",
            MetroDataType.FLOAT,
            description="Time between two intervals of departure time, in seconds",
            nullable=True,
            optional=True,
        ),
        Column(
            "dt_choice.offset",
            MetroDataType.FLOAT,
            description="Offset time added to the selected departure time, in seconds",
            nullable=True,
            optional=True,
        ),
        Column(
            "dt_choice.model.type",
            MetroDataType.STRING,
            description="Type of choice model when departure time is randomly chosen",
            nullable=True,
            optional=True,
        ),
        Column(
            "dt_choice.model.u",
            MetroDataType.FLOAT,
            description="Uniform draw to simulate the chosen departure time",
            nullable=True,
            optional=True,
        ),
        Column(
            "dt_choice.model.mu",
            MetroDataType.FLOAT,
            description="Variance of the stochastic terms in the utility function",
            nullable=True,
            optional=True,
        ),
        Column(
            "dt_choice.model.constants",
            MetroDataType.LIST_OF_FLOATS,
            description="Constant value added to the utility of each departure-time interval",
            nullable=True,
            optional=True,
        ),
        Column(
            "constant_utility",
            MetroDataType.FLOAT,
            description="Constant utility added to the alternative",
            nullable=True,
            optional=True,
        ),
    ]


class MetroTripsFile(MetroDataFrameFile):
    path = "run/input/trips.parquet"
    description = "TODO"
    schema = [
        Column(
            "agent_id",
            MetroDataType.ID,
            description="Identifier of the agent",
            nullable=False,
        ),
        Column(
            "alt_id",
            MetroDataType.ID,
            description="Identifier of the alternative",
            nullable=False,
        ),
        Column(
            "trip_id",
            MetroDataType.ID,
            description="Identifier of the trip",
            nullable=False,
        ),
        Column(
            "class.type",
            MetroDataType.STRING,
            description="Type of trip",
            nullable=False,
        ),
        Column(
            "class.origin",
            MetroDataType.ID,
            description="Identifier of the origin node of the trip",
            nullable=True,
            optional=True,
        ),
        Column(
            "class.destination",
            MetroDataType.ID,
            description="Identifier of the destination node of the trip",
            nullable=True,
            optional=True,
        ),
        Column(
            "class.vehicle",
            MetroDataType.ID,
            description="Identifier of the vehicle type of the trip",
            nullable=True,
            optional=True,
        ),
        Column(
            "class.route",
            MetroDataType.LIST_OF_IDS,
            description="List of edge identifiers representing the route itinerary to be followed",
            nullable=True,
            optional=True,
        ),
        Column(
            "class.travel_time",
            MetroDataType.FLOAT,
            description="Exogenous travel time of the trip, in seconds",
            nullable=True,
            optional=True,
        ),
        Column(
            "stopping_time",
            MetroDataType.FLOAT,
            description="Time spent at the end of the trip, before the next trip starts, in seconds",
            nullable=True,
            optional=True,
        ),
        Column(
            "constant_utility",
            MetroDataType.FLOAT,
            description="Constant utility added to the trip utility",
            nullable=True,
            optional=True,
        ),
        Column(
            "alpha",
            MetroDataType.FLOAT,
            description="Value of time (€/s)",
            nullable=True,
            optional=True,
        ),
        Column(
            "schedule_utility.type",
            MetroDataType.STRING,
            description="Type of function used for the schedule delay at trip's destination",
            nullable=True,
            optional=True,
        ),
        Column(
            "schedule_utility.tstar",
            MetroDataType.FLOAT,
            description="Desired arrival time at destination, in seconds after midnight",
            nullable=True,
            optional=True,
        ),
        Column(
            "schedule_utility.beta",
            MetroDataType.FLOAT,
            description="Penalty for arriving earlier than the desired arrival time (€/s)",
            nullable=True,
            optional=True,
        ),
        Column(
            "schedule_utility.gamma",
            MetroDataType.FLOAT,
            description="Penalty for arriving later than the desired arrival time (€/s)",
            nullable=True,
            optional=True,
        ),
        Column(
            "schedule_utility.delta",
            MetroDataType.FLOAT,
            description="Length of the desired arrival-time window, in seconds",
            nullable=True,
            optional=True,
        ),
    ]

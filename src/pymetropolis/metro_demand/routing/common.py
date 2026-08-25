from pymetropolis.metro_pipeline import Step
from pymetropolis.metro_pipeline.parameters import ListParameter
from pymetropolis.metro_pipeline.types import String


class StepWithRoadForbiddenTypes(Step):
    forbidden_types = ListParameter(
        "road_network.forbidden_types",
        inner=String(),
        default=[],
        description=(
            "List of road edges' types that *cannot* be used as origin / destination edge."
        ),
        example='`["motorway", "motorway_link", "trunk", "trunk_link"]`',
    )

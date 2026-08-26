from .files import (
    ZoneODLevel1CongestedTravelTimesFile,
    ZoneODLevel1FreeFlowTravelTimesFile,
    ZoneODLevel2CongestedTravelTimesFile,
    ZoneODLevel2FreeFlowTravelTimesFile,
    ZoneODLevel3CongestedTravelTimesFile,
    ZoneODLevel3FreeFlowTravelTimesFile,
    ZoneODLevel4CongestedTravelTimesFile,
    ZoneODLevel4FreeFlowTravelTimesFile,
    ZoneODLevel5CongestedTravelTimesFile,
    ZoneODLevel5FreeFlowTravelTimesFile,
)
from .od_zones import ZonesODCongestedTravelTimesStep, ZonesODFreeFlowTravelTimesStep

TRAVEL_TIMES_FILES = [
    ZoneODLevel1CongestedTravelTimesFile,
    ZoneODLevel2CongestedTravelTimesFile,
    ZoneODLevel3CongestedTravelTimesFile,
    ZoneODLevel4CongestedTravelTimesFile,
    ZoneODLevel5CongestedTravelTimesFile,
    ZoneODLevel1FreeFlowTravelTimesFile,
    ZoneODLevel2FreeFlowTravelTimesFile,
    ZoneODLevel3FreeFlowTravelTimesFile,
    ZoneODLevel4FreeFlowTravelTimesFile,
    ZoneODLevel5FreeFlowTravelTimesFile,
]

TRAVEL_TIMES_STEPS = [ZonesODFreeFlowTravelTimesStep, ZonesODCongestedTravelTimesStep]

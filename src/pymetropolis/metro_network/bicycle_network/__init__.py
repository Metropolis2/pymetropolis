from .costs import ExogenousBicycleEdgeCostsStep
from .files import BicycleEdgesCleanFile, BicycleEdgesCostsFile, BicycleEdgesRawFile
from .osm import OpenStreetMapBicycleImportStep
from .postprocess import PostprocessBicycleNetworkStep

BICYCLE_NETWORK_FILES = [BicycleEdgesRawFile, BicycleEdgesCleanFile, BicycleEdgesCostsFile]

BICYCLE_NETWORK_STEPS = [
    OpenStreetMapBicycleImportStep,
    PostprocessBicycleNetworkStep,
    ExogenousBicycleEdgeCostsStep,
]

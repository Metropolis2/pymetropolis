from .edges import WriteMetroEdgesStep
from .files import MetroEdgesFile, MetroVehicleTypesFile
from .vehicle_types import WriteMetroVehicleTypesStep

SUPPLY_FILES = [MetroEdgesFile, MetroVehicleTypesFile]

SUPPLY_STEPS = [WriteMetroEdgesStep, WriteMetroVehicleTypesStep]

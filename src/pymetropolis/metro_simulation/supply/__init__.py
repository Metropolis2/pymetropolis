from .edges import WriteMetroEdgesStep
from .files import MetroEdgesFile, MetroExAnteVehicleTypesFile, MetroVehicleTypesFile
from .vehicle_types import WriteExAnteMetroVehicleTypesStep, WriteMetroVehicleTypesStep

SUPPLY_FILES = [MetroEdgesFile, MetroVehicleTypesFile, MetroExAnteVehicleTypesFile]

SUPPLY_STEPS = [WriteMetroEdgesStep, WriteMetroVehicleTypesStep, WriteExAnteMetroVehicleTypesStep]

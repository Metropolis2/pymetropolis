from .each import ODMatrixEachStep
from .file import TripZonesFile
from .gravity import GravityODMatrixStep

OD_MATRIX_FILES = [TripZonesFile]
OD_MATRIX_STEPS = [ODMatrixEachStep, GravityODMatrixStep]

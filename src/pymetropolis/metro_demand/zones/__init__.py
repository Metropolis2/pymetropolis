from .custom import (
    CustomZonesLevel1Step,
    CustomZonesLevel2Step,
    CustomZonesLevel3Step,
    CustomZonesLevel4Step,
    CustomZonesLevel5Step,
)
from .file import (
    ZonesLevel1File,
    ZonesLevel2File,
    ZonesLevel3File,
    ZonesLevel4File,
    ZonesLevel5File,
)
from .france import FrenchZonesStep

ZONES_FILES = [ZonesLevel1File, ZonesLevel2File, ZonesLevel3File, ZonesLevel4File, ZonesLevel5File]
ZONES_STEPS = [
    CustomZonesLevel1Step,
    CustomZonesLevel2Step,
    CustomZonesLevel3Step,
    CustomZonesLevel4Step,
    CustomZonesLevel5Step,
    FrenchZonesStep,
]

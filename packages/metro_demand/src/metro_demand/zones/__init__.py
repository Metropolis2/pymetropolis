from metro_pipeline import ConfigTable

from .custom import CUSTOM_ZONES_FILE, CUSTOM_ZONES_IMPORT
from .file import ZONES_FILE as ZONES_FILE

ZONES_TABLE = ConfigTable(
    "zones",
    "zones",
    items=[CUSTOM_ZONES_FILE],
    description="Import zones.",
)

ZONES_CONFIG_SCHEMA = [ZONES_TABLE]

ZONES_STEPS = [CUSTOM_ZONES_IMPORT]

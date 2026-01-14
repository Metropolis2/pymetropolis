from metro_common.types import Path
from metro_pipeline import ConfigValue

OSM_FILE = ConfigValue(
    "osm_file",
    "osm_file",
    expected_type=Path(extensions=[".osm", ".pbf"]),
    description=(
        "Path to the OpenStreetMap file (`.osm` or `.osm.pbf`) with data for the simulation area."
    ),
    example='`"data/osm/france-250101.osm.pbf"`',
    note=(
        "You can download extract of OpenStreetMap data for any region in the world through the "
        "Geofabrik website. You can also download data directly from the OSM website, using the "
        "“Export” button, although it is limited to small areas."
    ),
)

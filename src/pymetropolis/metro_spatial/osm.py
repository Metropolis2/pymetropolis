from pymetropolis.metro_pipeline.parameters import PathParameter
from pymetropolis.metro_pipeline.steps import MetroStep


class OSMMetroStep(MetroStep):
    osm_file = PathParameter(
        "osm_file",
        check_file_exists=True,
        extensions=[".pdf", ".osm"],
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

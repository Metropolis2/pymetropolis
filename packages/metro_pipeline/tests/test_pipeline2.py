import os

from metro_pipeline import BASE_SCHEMA, Config, ConfigTable, ConfigValue, InputFile, MetroFile, Step

# === Config ===
CRS = ConfigValue("crs", "crs")
OSM_FILE = InputFile("osm_file", "osm_file")
CUSTOM_NETWORK_FILE = InputFile("network_file", "network_file")
BBOX = ConfigValue("simulation_area.bbox", "bbox", default="1")

SIMULATION_AREA_TABLE = ConfigTable("simulation_area", "simulation_area", [BBOX])
CUSTOM_NETWORK_TABLE = ConfigTable("custom_network", "custom_network", [CUSTOM_NETWORK_FILE])

CONFIG_SCHEMA = BASE_SCHEMA + [CRS, OSM_FILE, SIMULATION_AREA_TABLE]

config = Config.from_toml("config.toml", CONFIG_SCHEMA)

# === Files ===

SIMULATION_AREA_FILE = MetroFile("simulation_area", "simulation_area.geo.parquet")
RAW_EDGES_FILE = MetroFile(
    "road_network.raw_edges", os.path.join("road_network", "raw_edges.geo.parquet")
)
URBAN_AREAS_FILE = MetroFile("misc.urban_areas", os.path.join("misc", "urban_areas.geo.parquet"))
NETWORK_FILE = MetroFile("simulation.network", os.path.join("simulation", "network.parquet"))
TRIPS_FILE = MetroFile("simulation.trips", os.path.join("simulation", "trips.parquet"))

# === Steps ===

SIMULATION_AREA = Step(
    "simulation-area", output_files=[SIMULATION_AREA_FILE], config_values=[CRS, BBOX]
)
OSM_ROAD_IMPORT = Step(
    "osm-road-import",
    optional_files=[SIMULATION_AREA_FILE],
    output_files=[RAW_EDGES_FILE, URBAN_AREAS_FILE],
    config_values=[CRS, OSM_FILE],
)
CUSTOM_NETWORK_IMPORT = Step(
    "custom-network-import",
    output_files=[RAW_EDGES_FILE],
    config_values=[CUSTOM_NETWORK_FILE],
)
TRIPS_IMPORT = Step("trips-import", output_files=[TRIPS_FILE])
POST_PROCESS = Step("post-process", required_files=[RAW_EDGES_FILE], output_files=[NETWORK_FILE])
SIMULATION_RUN = Step("simulation-run", required_files=[NETWORK_FILE, TRIPS_FILE])

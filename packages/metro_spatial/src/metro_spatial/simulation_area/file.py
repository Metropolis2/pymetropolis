from metro_pipeline.file import MetroGeoDataFrameFile

SIMULATION_AREA_FILE = MetroGeoDataFrameFile(
    "simulation_area",
    path="simulation_area.geo.parquet",
    description="Single-feature file with the geometry of the simulation area.",
    max_rows=1,
)

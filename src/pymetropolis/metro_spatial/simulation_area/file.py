from pymetropolis.metro_pipeline.file import MetroGeoDataFrameFile


class SimulationAreaFile(MetroGeoDataFrameFile):
    path = "simulation_area.geo.parquet"
    description = "Single-feature file with the geometry of the simulation area."
    max_rows = 1

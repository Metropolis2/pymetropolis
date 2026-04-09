from pymetropolis.metro_pipeline.file import MetroGeoDataFrameFile


class UrbanAreasFile(MetroGeoDataFrameFile):
    path = "areas/urban_areas.geo.parquet"
    description = (
        "Single-feature file with a Polygon or MultiPolygon geometry representing the areas "
        "classified as urban."
    )
    max_rows = 1

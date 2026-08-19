from __future__ import annotations

from typing import TYPE_CHECKING

from mobisurvstd.resources.insee_data import load_insee_data

from pymetropolis.metro_pipeline import Step

from .common import DENSITY_CATS, FNC_AREA_CAT_CATS, FNC_AREA_TYPE_CATS, URBAN_TYPE_CATS
from .files import (
    HouseholdsHomesUrbanTypeFile,
    HouseholdsZonesFile,
    TripsUrbanTypeFile,
    TripsZonesFile,
)

if TYPE_CHECKING:
    import polars as pl


def get_insee_data() -> pl.DataFrame:
    import polars as pl

    insee_data: pl.DataFrame = load_insee_data()
    max_year = max(
        col.removeprefix("insee_density_")
        for col in insee_data.columns
        if col.startswith("insee_density_")
    )
    insee_data = insee_data.select(
        "insee",
        density=pl.col(f"insee_density_{max_year}").cast(pl.String).cast(pl.Enum(DENSITY_CATS)),
        urban_type=pl.col(f"insee_urban_type_{max_year}").cast(pl.Enum(URBAN_TYPE_CATS)),
        functional_area_type=pl.col(f"insee_aav_type_{max_year}")
        .cast(pl.String)
        .cast(pl.Enum(FNC_AREA_TYPE_CATS)),
        functional_area_category=pl.col(f"aav_category_{max_year}")
        .cast(pl.String)
        .cast(pl.Enum(FNC_AREA_CAT_CATS)),
    )
    return insee_data


class FrenchHouseholdsUrbanTypeStep(Step):
    """Add urban type attributes to households' home, based on INSEE municipalities attributes, for
    France only.
    """

    input_files = {"home_zones": HouseholdsZonesFile}
    output_files = {"home_urban_types": HouseholdsHomesUrbanTypeFile}

    def run(self):
        import polars as pl

        zones = self.input["home_zones"].read()

        insee_data = get_insee_data()

        # Level 3 should be INSEE id if zones are from the French system.
        df = (
            zones.select("household_id", "home_zone3")
            .join(
                insee_data.select(
                    pl.all().exclude("insee").name.prefix("home_"), home_zone3="insee"
                ),
                on="home_zone3",
                how="left",
            )
            .drop("home_zone3")
        )

        self.output["home_urban_types"].write(df)


class FrenchTripsUrbanTypeStep(Step):
    """Add urban type attributes to trips' origin / destination, based on INSEE municipalities
    attributes, for France only.
    """

    input_files = {"trip_zones": TripsZonesFile}
    output_files = {"trip_urban_types": TripsUrbanTypeFile}

    def run(self):
        import polars as pl

        zones = self.input["trip_zones"].read()

        insee_data = get_insee_data()

        # Level 3 should be INSEE id if zones are from the French system.
        columns = ["trip_id", "origin_zone3", "destination_zone3"]
        df = (
            zones.select(columns)
            .join(
                insee_data.select(
                    pl.all().exclude("insee").name.prefix("origin_"), origin_zone3="insee"
                ),
                on="origin_zone3",
                how="left",
            )
            .join(
                insee_data.select(
                    pl.all().exclude("insee").name.prefix("destination_"), destination_zone3="insee"
                ),
                on="destination_zone3",
                how="left",
            )
            .drop("origin_zone3", "destination_zone3")
        )

        self.output["trip_urban_types"].write(df)

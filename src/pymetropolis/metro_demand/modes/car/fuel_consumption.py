import polars as pl

from pymetropolis.metro_pipeline import Step
from pymetropolis.metro_pipeline.parameters import FloatParameter

from .files import CarFreeFlowDistancesFile, CarFuelFile


class CarFuelStep(Step):
    """Generates the fuel consumption and price of each car trips by applying a constant emission
    factor to the free-flow fastest-path length, combined with a fuel price.
    """

    fuel_factor = FloatParameter(
        "fuel.consumption_factor", description="Fuel consumption, in liters per km."
    )
    fuel_price = FloatParameter("fuel.price", description="Price of fuel, in € per liter.")
    input_files = {"distances": CarFreeFlowDistancesFile}
    output_files = {"fuel_consumption": CarFuelFile}

    def is_defined(self) -> bool:
        return self.fuel_factor is not None

    def run(self):
        df: pl.DataFrame = self.input["distances"].read()
        df = df.select("trip_id", fuel_consumption=self.fuel_factor * pl.col("distance") / 1000.0)
        # If `fuel_price` is not defined (None), `fuel_cost` will be all null values.
        df = df.with_columns(fuel_cost=pl.col("fuel_consumption") * self.fuel_price)
        self.output["fuel_consumption"].write(df)

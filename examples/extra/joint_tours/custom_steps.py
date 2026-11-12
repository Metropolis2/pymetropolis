import numpy as np
import polars as pl
from loguru import logger

from pymetropolis.metro_calibration.econometrics.files import SurveyModeChoiceResultsFile
from pymetropolis.metro_calibration.econometrics.mode_choice import (
    MONETARY_COST_VARIABLE,
    add_derived_variables,
    generic_variable_mode,
    monetary_cost_exprs,
)
from pymetropolis.metro_calibration.econometrics.preferences import (
    compute_mode_preferences,
    get_monetary_utility_scale,
)
from pymetropolis.metro_calibration.survey.mobisurvstd import MobiSurvStdImportStep
from pymetropolis.metro_common import MetropyError
from pymetropolis.metro_common.plots import PlotStep
from pymetropolis.metro_demand.modes.files import BicycleTravelTimesFile, WalkingTravelTimesFile
from pymetropolis.metro_demand.population.files import (
    ExAnteDepartureTimesFile,
    JointToursFile,
    ToursFile,
    TripsFile,
)
from pymetropolis.metro_demand.routing.files import (
    TripsPublicTransitItinerariesFile,
    TripsRoadNodesFile,
)
from pymetropolis.metro_demand.routing.routing_cli import (
    RoutingCLIStep,
    prepare_edges,
    trip_routing,
)
from pymetropolis.metro_environment.fuel import StepWithFuel
from pymetropolis.metro_network.road_network.files import (
    RoadEdgesCleanFile,
    RoadEdgesFreeFlowTravelTimeFile,
)
from pymetropolis.metro_pipeline import PopulationStep
from pymetropolis.metro_pipeline.file import (
    Column,
    MetroDataFrameFile,
    MetroDataType,
    MetroPlotFile,
    PopulationFile,
)
from pymetropolis.metro_pipeline.parameters import BoolParameter
from pymetropolis.metro_pipeline.steps import InputFile
from pymetropolis.metro_simulation.common import StepWithFuelSharing
from pymetropolis.metro_simulation.run.files import MetroExAnteSimulatedTravelTimeFunctionsFile
from pymetropolis.modes import (
    JOINT_TOUR_COLUMN,
    CarDriver,
    CarDriverAloneVehicle,
    CarDriverWithPassengers,
    CarPassenger,
    StepWithModeAvailability,
    mode_availability_expr,
    mode_from_str,
    warn_missing_availability_columns,
)

RS_SUBSIDY_LEVELS = np.arange(0, 4, 0.05)
KM_COST = 0.5


class MobiSurvStdImportStep(MobiSurvStdImportStep):
    """Overrides the built-in `MobiSurvStdImportStep` to optionally discard the surveyed
    households with at least one member younger than 5 years old (they are not surveyed).
    """

    exclude_young_children = BoolParameter(
        "survey.mobisurvstd.exclude_young_children", default=True
    )

    def run(self):
        # Read and standardize the survey with the original Step.
        super().run()
        if not self.exclude_young_children:
            return

        persons: pl.DataFrame = self.output["persons"].read()
        excluded_ids = persons.filter(pl.col("age") < 5)["household_id"].unique()
        for name in ("households", "persons", "trips", "legs", "cars", "motorcycles"):
            df = self.output[name].read()
            self.output[name].write(df.filter(pl.col("household_id").is_in(excluded_ids).not_()))


class ToursCongestedCarTravelTimesFile(MetroDataFrameFile, PopulationFile):
    path = "demand/{population}/tours/congested_car_travel_times.parquet"
    description = "Travel time and route distance by car of each tour, under ex-ante congestion."
    schema = [
        Column(
            "tour_id",
            MetroDataType.ID,
            description="Identifier of the tour.",
            unique=True,
            nullable=False,
        ),
        Column(
            "travel_time_car",
            MetroDataType.DURATION,
            description=(
                "Total travel time by car of the tour's trips, under the ex-ante congested "
                "conditions."
            ),
            nullable=True,
        ),
        Column(
            "distance_car",
            MetroDataType.FLOAT,
            description="Total length of the tour's car routes, in meters.",
            nullable=True,
        ),
    ]


class ToursCongestedCarTravelTimesStep(RoutingCLIStep, PopulationStep):
    """Computes the congested car travel time of every tour of the synthetic population.

    Each trip is routed from its origin to its destination road node, departing at its ex-ante
    departure time, against the simulated edge travel-time functions of the ex-ante simulation
    (free-flow travel times are used for the edges that are not simulated). The tour's values are
    the sums over its trips.

    The values are computed for all tours, whatever their ex-ante mode, so they approximate, but
    do not equal, the simulated travel times of the ex-ante trip results.
    """

    input_files = {
        "trips": TripsFile,
        "od_pairs": TripsRoadNodesFile,
        "departure_times": ExAnteDepartureTimesFile,
        "edges": RoadEdgesCleanFile,
        "edges_fftt": RoadEdgesFreeFlowTravelTimeFile,
        "congestion_conditions": MetroExAnteSimulatedTravelTimeFunctionsFile,
    }
    output_files = {"tt": ToursCongestedCarTravelTimesFile}

    def run(self):
        assert self.exec_path is not None

        edges = prepare_edges(self.input["edges"].read(), self.input["edges_fftt"].read())
        edges_ttfs = (
            self.input["congestion_conditions"]
            .read()
            .filter(vehicle_id=repr(CarDriverAloneVehicle))
        )

        queries = (
            self.input["od_pairs"]
            .read()
            .select(
                "trip_id", origin_node="origin_road_node", destination_node="destination_road_node"
            )
            .join(
                self.input["departure_times"].read().select("trip_id", "departure_time"),
                on="trip_id",
                how="inner",
            )
            .drop_nulls()
        )
        results = trip_routing(
            queries, edges, self.exec_path, with_routes=True, network_conditions=edges_ttfs
        )
        trip_tts = queries.join(results, on="trip_id", how="inner").select(
            "trip_id",
            travel_time=pl.col("value")
            - pl.col("departure_time").dt.total_seconds(fractional=True),
            distance=pl.col("route")
            .list.eval(pl.element().replace_strict(edges["edge_id"], edges["length"]))
            .list.sum()
            .cast(pl.Float64),
        )
        n = len(queries) - len(trip_tts)
        if n:
            logger.warning(f"{n} trips could not be routed")

        # A tour's values are null as soon as one of its trips has no route.
        tours = (
            self.input["trips"]
            .read()
            .select("trip_id", "tour_id")
            .join(trip_tts, on="trip_id", how="left")
            .group_by("tour_id")
            .agg(
                travel_time_car=pl.when(pl.col("travel_time").null_count() == 0).then(
                    pl.col("travel_time").sum()
                ),
                distance_car=pl.when(pl.col("distance").null_count() == 0).then(
                    pl.col("distance").sum()
                ),
            )
            .with_columns(travel_time_car=pl.duration(seconds="travel_time_car"))
            .sort("tour_id")
        )
        self.output["tt"].write(tours)


class RidesharingSubsidyResultsFile(MetroDataFrameFile, PopulationFile):
    path = "demand/{population}/tours/ridesharing_subsidy_results.parquet"
    description = (
        "Aggregate results of the econometric mode-choice model applied to the tours, for each "
        "ridesharing subsidy level."
    )
    schema = [
        Column(
            "subsidy_level",
            MetroDataType.FLOAT,
            description="Ridesharing subsidy, per tour (€).",
            unique=True,
            nullable=False,
        ),
        Column(
            "vehicle_kilometers",
            MetroDataType.FLOAT,
            description=(
                "Expected car vehicle-kilometers: sum over tours of the car distance (km) times "
                "the probability of car_driver or car_driver_with_passengers."
            ),
            nullable=False,
        ),
        Column(
            "car_driver_with_passengers_share",
            MetroDataType.FLOAT,
            description="Sum over tours of the probability of car_driver_with_passengers.",
            nullable=False,
        ),
        Column(
            "car_passenger_share",
            MetroDataType.FLOAT,
            description="Sum over tours of the probability of car_passenger.",
            nullable=False,
        ),
        Column(
            "policy_cost",
            MetroDataType.FLOAT,
            description=(
                "Sum over tours of the subsidy times the probability of a ridesharing mode (€)."
            ),
            nullable=False,
        ),
        Column(
            "surplus",
            MetroDataType.FLOAT,
            description="Sum over tours of the logsum (€), including the subsidy.",
            nullable=False,
        ),
        Column(
            "vkm_gains",
            MetroDataType.FLOAT,
            description=(
                "Reduction in vehicle-kilometers relative to the first subsidy level, valued at "
                "`KM_COST` (€)."
            ),
            nullable=False,
        ),
        Column(
            "surplus_gains",
            MetroDataType.FLOAT,
            description="Increase in surplus relative to the first subsidy level (€).",
            nullable=False,
        ),
        Column(
            "global_gains",
            MetroDataType.FLOAT,
            description="Vehicle-kilometer gains plus surplus gains minus policy cost (€).",
            nullable=False,
        ),
    ]


def _tour_sum(trips: pl.DataFrame, values: pl.DataFrame, column: str, name: str) -> pl.DataFrame:
    """Returns the sum of the trip-level duration `column` over each tour, in hours, as column
    `name` (null if any trip of the tour has a missing value).
    """
    return (
        trips.join(values.select("trip_id", column), on="trip_id", how="left")
        .group_by("tour_id")
        .agg(
            pl.when(pl.col(column).null_count() == 0)
            .then(pl.col(column).dt.total_seconds(fractional=True).sum() / 3600)
            .alias(name)
        )
    )


class ToursModeUtilitiesStep(
    StepWithModeAvailability, StepWithFuel, StepWithFuelSharing, PopulationStep
):
    """Computes aggregate results of the econometric mode-choice model applied to the tours of the
    synthetic population, for each ridesharing subsidy level of `RS_SUBSIDY_LEVELS`.

    The systematic utility of each mode is computed for each tour from the coefficients of the
    estimated model and converted to euros with the estimated marginal utility of money. Only the
    modes available to the tour (`mode_availability` rules and defined travel time) are included.
    The subsidy increases the utility of car_driver_with_passengers and car_passenger by the
    subsidy level (€). The logit probabilities and the logsum (in €) of each tour are then
    aggregated into the vehicle-kilometers, the ridesharing mode shares (sums of probabilities),
    the policy cost and the surplus.

    Car travel times and distances are the ex-ante congested ones (see
    `ToursCongestedCarTravelTimesStep`). Walking and bicycle travel times are those of the
    simulation, so they include the snap distances if `modes.*.distance.with_snap` is `true`,
    unlike the surveyed travel times used in the estimation. Public-transit travel times are the
    generalized times of the itineraries, like in the estimation.
    """

    input_files = {
        "tours": ToursFile,
        "joint_tours": InputFile(JointToursFile, optional=True),
        "trips": TripsFile,
        "results": SurveyModeChoiceResultsFile,
        "car_tt": ToursCongestedCarTravelTimesFile,
        "walking_tt": InputFile(WalkingTravelTimesFile, optional=True),
        "bicycle_tt": InputFile(BicycleTravelTimesFile, optional=True),
        "pt_tt": InputFile(TripsPublicTransitItinerariesFile, optional=True),
    }
    output_files = {"results": RidesharingSubsidyResultsFile}

    def run(self):
        results = self.input["results"].read()
        spec = results["specification"]
        params = results["parameters"]
        modes = [mode_from_str(m) for m in spec["modes"]]

        tours: pl.DataFrame = self.input["tours"].read()
        joint_tours = self.input["joint_tours"].read_if_exists()
        if joint_tours is not None:
            tours = tours.join(joint_tours, on="tour_id", how="left")
        else:
            tours = tours.with_columns(pl.lit(False).alias(JOINT_TOUR_COLUMN))
        tours = add_derived_variables(tours, spec.get("derived_variables", {}))

        # Tour-level travel times (in hours) and car distance.
        trips = self.input["trips"].read().select("trip_id", "tour_id")
        car_tt = (
            self.input["car_tt"]
            .read()
            .select(
                "tour_id",
                "distance_car",
                travel_time_car=pl.col("travel_time_car").dt.total_seconds(fractional=True) / 3600,
            )
        )
        tours = tours.join(car_tt, on="tour_id", how="left")
        for key, column, name in (
            ("walking_tt", "walking_travel_time", "travel_time_walking"),
            ("bicycle_tt", "bicycle_travel_time", "travel_time_bicycle"),
            ("pt_tt", "generalized_time", "travel_time_public_transit"),
        ):
            values = self.input[key].read_if_exists()
            if values is None:
                tours = tours.with_columns(pl.lit(None, dtype=pl.Float64).alias(name))
            else:
                tours = tours.join(_tour_sum(trips, values, column, name), on="tour_id", how="left")

        with_cost = MONETARY_COST_VARIABLE in spec["variables"]
        if with_cost:
            if self.fuel_factor is None or self.fuel_price is None:
                raise MetropyError(
                    f"The `{MONETARY_COST_VARIABLE}` variable requires the "
                    "`fuel.consumption_factor` and `fuel.price` parameters."
                )
            tours = tours.with_columns(
                **monetary_cost_exprs(modes, self.fuel_factor, self.fuel_price, self.fuel_share)
            )

        scale = get_monetary_utility_scale(params)
        if scale is None:
            raise MetropyError(
                "The marginal utility of money is not estimated: cannot express the utilities in "
                "euros."
            )
        b_money = params[f"B_{MONETARY_COST_VARIABLE}"]["value"]

        rules = self.availability_rules()
        warn_missing_availability_columns(modes, rules, tours.columns)
        utilities = []
        for mode in modes:
            tt_col = f"travel_time_{generic_variable_mode(mode)}"
            prefs = compute_mode_preferences(tours, spec, params, mode)
            df = tours.join(prefs, on="tour_id", how="left").filter(
                mode_availability_expr(mode, rules, tours.columns), pl.col(tt_col).is_not_null()
            )
            utility = -pl.col(f"{mode!r}_cst") - pl.col(f"{mode!r}_vot") * pl.col(tt_col)
            if with_cost:
                utility = utility + b_money * pl.col(f"{MONETARY_COST_VARIABLE}_{mode!r}")
            utilities.append(
                df.select(
                    "tour_id",
                    mode=pl.lit(repr(mode)),
                    utility=utility / scale,
                    # Vehicle-kilometers are only counted for the car-driver modes.
                    vkm=pl.col("distance_car") / 1000
                    if mode in (CarDriver, CarDriverWithPassengers)
                    else pl.lit(0.0),
                )
            )
        utilities_df = pl.concat(utilities).sort("tour_id", "mode")

        is_rs_mode = pl.col("mode").is_in([repr(CarDriverWithPassengers), repr(CarPassenger)])
        rows = []
        for level in map(float, RS_SUBSIDY_LEVELS):
            # The probabilities use the utilities in utils (scale = marginal utility of money).
            v = (pl.col("utility") + pl.when(is_rs_mode).then(level).otherwise(0.0)) * scale
            df = utilities_df.with_columns(v_max=v.max().over("tour_id")).with_columns(
                e=(v - pl.col("v_max")).exp()
            )
            df = df.with_columns(
                probability=pl.col("e") / pl.col("e").sum().over("tour_id"),
                # Logsum of the tour, in euros (repeated on each row of the tour).
                logsum=(pl.col("v_max") + pl.col("e").sum().over("tour_id").log()) / scale,
            )
            row = df.select(
                vehicle_kilometers=(pl.col("vkm") * pl.col("probability")).sum(),
                car_driver_with_passengers_share=pl.col("probability")
                .filter(mode=repr(CarDriverWithPassengers))
                .sum(),
                car_passenger_share=pl.col("probability").filter(mode=repr(CarPassenger)).sum(),
                policy_cost=level * pl.col("probability").filter(is_rs_mode).sum(),
                # The logsum is counted once per tour.
                surplus=pl.col("logsum").filter(pl.col("tour_id").is_first_distinct()).sum(),
            ).row(0, named=True)
            rows.append({"subsidy_level": level, **row})

        # Gains are relative to the first (lowest) subsidy level.
        results = (
            pl.DataFrame(rows)
            .sort("subsidy_level")
            .with_columns(
                vkm_gains=(pl.col("vehicle_kilometers").first() - pl.col("vehicle_kilometers"))
                * KM_COST,
                surplus_gains=pl.col("surplus") - pl.col("surplus").first(),
            )
            .with_columns(
                global_gains=pl.col("vkm_gains") + pl.col("surplus_gains") - pl.col("policy_cost")
            )
        )
        self.output["results"].write(results)


class RidesharingSubsidyGainsPlotFile(MetroPlotFile, PopulationFile):
    path = "results/graphs/{population}/ridesharing_subsidy_gains.{format}"
    description = "Gains of the ridesharing subsidy, as a function of the subsidy level."


class RidesharingSubsidyGainsPlotStep(PlotStep, PopulationStep):
    """Plots the vehicle-kilometer gains, the net surplus gains (surplus gains minus policy cost)
    and the global gains of the ridesharing subsidy against the subsidy level.
    """

    input_files = {"results": RidesharingSubsidyResultsFile}
    output_files = {"plot": RidesharingSubsidyGainsPlotFile}

    def run(self):
        import matplotlib.pyplot as plt

        df = self.input["results"].read().sort("subsidy_level")
        x = df["subsidy_level"].to_numpy()
        series = {
            "Global gains": df["global_gains"],
            "Vehicle-kilometer gains": df["vkm_gains"],
            "Surplus gains minus policy cost": df["surplus_gains"] - df["policy_cost"],
        }
        fig, ax = plt.subplots()
        ax.axhline(0.0, color="black", linewidth=0.8)
        for label, values in series.items():
            ax.plot(x, values.to_numpy() / 1000, linewidth=2, label=self.text(label))
        # Highlight the subsidy level maximizing the global gains.
        best = df.row(df["global_gains"].arg_max(), named=True)
        ax.plot(best["subsidy_level"], best["global_gains"] / 1000, "o", color="C0", markersize=8)
        ax.annotate(
            self.text(f"{best['subsidy_level']:.2f} €"),
            (best["subsidy_level"], best["global_gains"] / 1000),
            textcoords="offset points",
            xytext=(6, -6),
            ha="left",
            va="top",
        )
        ax.set_xlabel(self.text("Ridesharing subsidy (€ per tour)"))
        ax.set_ylabel(self.text("Gains relative to no subsidy (thousand €)"))
        ax.set_xlim(x[0], x[-1])
        ax.legend()
        ax.grid(alpha=0.3)
        fig.tight_layout()
        self.output["plot"].write(fig)

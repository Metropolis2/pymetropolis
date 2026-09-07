# Changelog

## [Unreleased]

New steps:

- `ExternalJointToursClassifierStep`
- `ExternalModeClassifierStep`
- `EstimateModeClassifierStep`
- `ClassifyToursModeStep`
- `FreeFlowTravelTimeComparisonStep`
- `ImportTripCoordinatesStep`
- `AbstractODMatrixStep`
- `AbstractFrenchZonesStep`
- `ModeClassifierConfigStep`
- `CompareToursModeSharesStep`
- `PrepareExAnteMetroAgentsStep`
- `WriteExAnteMetroAgentsStep`
- `PrepareExAnteMetroAlternativesStep`
- `WriteExAnteMetroAlternativesStep`
- `PrepareExAnteMetroTripsStep`
- `WriteExAnteMetroTripsStep`
- `WriteExAnteMetroParametersStep`
- `RunExAnteSimulationStep`
- `CongestionSimulationStep`
- `CongestionTimeComparisonStep`
- `SurveyedPedestrianODNodesFromCoordinatesStep`
- `SurveyedTripsPedestrianDistanceStep`

New files:

- `ModeEstimatorFile`
- `ToursModeFile`
- `FreeFlowTravelTimeComparisonPlotFile`
- `ToursModeShareComparisonFile`
- `ToursModeShareTourCountPlotFile`
- `ToursModeShareDistancePlotFile`
- `MetroExAnteAgentsPopulationFile`
- `MetroExAnteAgentsFile`
- `MetroExAnteAlternativesPopulationFile`
- `MetroExAnteAlternativesFile`
- `MetroExAnteTripsPopulationFile`
- `MetroExAnteTripsFile`
- `MetroExAnteParametersFile`
- `MetroExAnteAgentResultsFile`
- `MetroExAnteTripResultsFile`
- `MetroExAnteRouteResultsFile`
- `MetroExAnteSimulatedTravelTimeFunctionsFile`
- `MetroExAnteExpectedTravelTimeFunctionsFile`
- `MetroExAnteNextExpectedTravelTimeFunctionsFile`
- `TomTomCongestionTimesFile`
- `CongestionTimeComparisonPlotFile`
- `SurveyedTripsPedestrianNodesFile`
- `SurveyedTripsPedestrianDistancesFile`

New parameters:

- `road_network.capacity_multipliers.traffic_signal`
- `road_network.capacity_multipliers.roundabout`

Other changes:

- Steps `ODMatrixEachStep`, `GravityODMatrixStep`, and `CustomODMatrixStep` now inherits from
  `AbtractODMatrixStep` and can thus define an OD matrix not only from pairs of road nodes, but also
  from zone pairs, with the actual origin and destination either set to the zone' centroid or drawn
  randomly along the edges within the zone. These steps received new parameters and had some
  existing parameters renamed. They now generate origin / destination coordinates instead of origin
  / destination road nodes.

Removed steps:

- `GenericPopulationStep` (should no longer be needed with the changes to OD matrix steps)

## [0.12.0] – 2026-08-25

New steps:

- `MobiSurvStdImportStep`
- `CleanSurveyToursStep`
- `EstimateJointToursClassifierStep`
- `CreateToursStep`
- `ClassifyJointToursStep`
- `FrenchZonesStep`
- `CustomZonesLevel1Step`
- `CustomZonesLevel2Step`
- `CustomZonesLevel3Step`
- `CustomZonesLevel4Step`
- `CustomZonesLevel5Step`
- `HouseholdHomesZonesStep`
- `TripsZonesStep`
- `FrenchHouseholdsUrbanTypeStep`
- `FrenchTripsUrbanTypeStep`
- `ZonesLevel1RoadNodesStep`
- `ZonesLevel2RoadNodesStep`
- `ZonesLevel3RoadNodesStep`
- `ZonesLevel4RoadNodesStep`
- `ZonesLevel5RoadNodesStep`
- `ZonesODFreeFlowTravelTimesStep`
- `ZonesODCongestedTravelTimesStep`

New files:

- `SurveyedHouseholdsFile`
- `SurveyedPersonsFile`
- `SurveyedTripsFile`
- `SurveyedLegsFile`
- `SurveyedCarsFile`
- `SurveyedMotorcyclesFile`
- `SurveyedSpecialLocationsFile`
- `SurveyedDetailedZonesFile`
- `SurveyedDrawZonesFile`
- `SurveyedToursFile`
- `JointTourEstimatorFile`
- `ToursFile`
- `JointToursFile`
- `ZonesLevel1File`
- `ZonesLevel2File`
- `ZonesLevel3File`
- `ZonesLevel4File`
- `ZonesLevel5File`
- `HouseholdHomesUrbanTypeFile`
- `TripsUrbanTypeFile`
- `ZonesLevel1RoadNodeFile`
- `ZonesLevel2RoadNodeFile`
- `ZonesLevel3RoadNodeFile`
- `ZonesLevel4RoadNodeFile`
- `ZonesLevel5RoadNodeFile`
- `ZoneODLevel1FreeFlowTravelTimesFile`
- `ZoneODLevel2FreeFlowTravelTimesFile`
- `ZoneODLevel3FreeFlowTravelTimesFile`
- `ZoneODLevel4FreeFlowTravelTimesFile`
- `ZoneODLevel5FreeFlowTravelTimesFile`
- `ZoneODLevel1CongestedTravelTimesFile`
- `ZoneODLevel2CongestedTravelTimesFile`
- `ZoneODLevel3CongestedTravelTimesFile`
- `ZoneODLevel4CongestedTravelTimesFile`
- `ZoneODLevel5CongestedTravelTimesFile`

New parameters:

- `gtfs.date`
- `main_population`
- `extra_populations`
- `custom_steps`
- `road_network.forbiden_types`
- `zones.weiszfeld.threshold`
- `zones.weiszfeld.max_iter`
- `od_matrix_travel_times.zones_levels`
- `od_matrix_travel_times.time_window`

New features:

- New `MetroFile` type for Machine Learning estimators saved with joblib: `MetroMLEstimatorFile`
- New `PopulationFile` abstract class for population-specific MetroFiles
- New `PopulationStep` abstract class for population-specific steps
- Parameters can be `shared` when they are not necessarily specific to a population (e.g.,
  `random_seed`)
- Users can define their own `Step` subclasses in Python files listed under `custom_steps` (paths
  relative to the main config file); they are loaded and made available to the pipeline like any
  built-in Step. A custom Step whose name matches an existing one (e.g. `EqasimImportStep`)
  overrides it
- Environment variables can be automatically loaded from .env files

Other changes:

- Relative `main_directory`, `secrets_file`, and every `PathParameter`/`ExecPathParameter` value
  (e.g. `exec_path`, `routing_exec_path`, `osm_file`, `eqasim_output`, ...) are now resolved against
  the directory of the main config file, instead of the current working directory (consistent with
  `extra_populations` and `custom_steps`)
- Put `"unemployed"` and `"homemaker"` professional activity modalities in the `"other"` modality
- Rename `"shop"` purpose to `"shopping"`
- The random number generator (`RandomStep.get_rng()`) now use a different seed for each step, so
  that reproducibility does not depend on run order
- Examples configurations are now all located within the `examples/` directory
- `exec_path` and `routing_exec_path` now default to `"env:METROPOLIS_EXEC_PATH"` and
  `"env:METROPOLIS_ROUTING_EXEC_PATH"`, respectively
- Step status with `--dry-run` use more distinct colors and a legend has been added

Removed steps:

- `CustomZonesStep` (use one of the 5 `CustomZonesLevelXStep` instead)
- `FrenchHouseholdHomesZonesStep`
- `FrenchTripsZonesStep`

Removed files:

- `ZonesFile` (use one of the 5 `ZonesLevelXFile` instead)

Removed parameters:

- `opentripplanner.date`
- `r5.date`

## [0.11.0] – 2026-07-31

**Deleting your main directory to start from scratch is strongly recommended when updating.**

New steps:

- `ActivitiesLocationsFromTripsLocationsStep`
- `ActivityResultsStep`
- `EdgePenaltiesFromCoefficientsStep`
- `TomTomRequestsStep`
- `MapMatchingStep`
- `FreeFlowLassoStep`
- `FreeFlowPenaltyCoefficientsFromFileStep`

New files:

- `ActivitiesLocationsFile`
- `ActivityResultsFile`
- `TomTomRoutesFile`
- `TomTomRoutesMatchedFile`
- `RoadEdgesPenaltyCoefficientsFile`

New columns:

- `speed_multiplier` in `RoadEdgesPenaltiesFile`
- `path` and `path_length` in `NonPrimaryCarTripsFile`
- `base_free_flow_tt` in `RoadEdgesVariablesFile`

New parameters:

- `secrets_file`
- `osm_bicycle_import.reindex`
- `osm_pedestrian_import.reindex`
- `osm_road_import.reindex`
- `road_network.speed_multiplier`
- `road_network.min_effective_speed`
- `road_network.max_effective_speed`

New features:

- Configuration parameters can be set to `"secret:key"` to read them from the secrets file
- Configuration parameters can be set to `"env:VAR"` to read them from environment variables

Removed parameters:

- `road_network.reindex`

Other changes:

- Guess the separator when reading a CSV datafile
- Use dummy variables for categorical columns in `RoadEdgesVariablesFile`
- Include non-primary road trips to `RouteResultsFile`
- Move some MetroFiles from `network/road_network/` to `calibration/road`
- Renamed / moved many MetroFiles

Fixes:

- Discard public-transit trips when no itineraries were found
- Fix pipeline sequence when using `--step` on a non-primary step that would not be run otherwise
- Properly handle non-primary road trips in `TripResultsFile`
- Fix an error when running `TripsPedestrianDistancesStep` with `output_path` set to `false`
- Fix `RoadNetworkCongestionFunctionPlotStep` with non-primary edges
- Fix a crash when reading simulation area from OSM tags
- Fix multiple crashes when edge ids are automatically converted to string type

## [0.10.0] – 2026-06-15

New steps:

- `RouteResultsStep`
- `AggregateResultsStep`

New files:

- `MetroRouteResultsFile`
- `RouteResultsFile`
- `AggregateOutputFile`

New columns:

- `access_length` and `egress_length` in `PrimaryCarTripsAccessEgressFile`
- `vehicle_id` in `TripResultsFile`

Other changes:

- Add `.scan()` method for `MetroDataFrameFile`
- Rename column `length` to `route_length` in `TripResultsFile`
- `TripResultsStep` now properly account for access / egress parts of road trips
- The hash of executable files is no longer compared when checking if a step needs to re-run

## [0.9.0] – 2026-06-11

New steps:

- `GTFSStep`
- `TripsOpenTripPlannerStep`
- `TripsPublicTransitTravelTimeFromR5Step`
- `RoadEdgesVariablesStep`

New files:

- `TripsPublicTransitItinerariesFile`
- `RoadEdgesVariablesFile`

New parameters:

- `simulation_ratio`
- `nb_threads`

New features:

- New parameter type `DateParameter`
- Utility of public-transit trips is computed from the generalized time (mode-weighted travel time)

Removed files:

- `PublicTransitTravelTimesFile`

Other changes:

- Make `professional_activity`, `education_level`, and `detailed_education_level` optional in the
  Eqasim output
- Allow simulation areas to be MultiPolygon
- Switch to official METROPOLIS2 colors

Fixes:

- Set the car constant to a *negative* utility
- Add `click` dependency

## [0.8.0] – 2026-05-11

New mode: `bicycle`

New steps:

- `PedestrianODNodesFromCoordinatesStep`
- `RoadODNodesFromCoordinatesStep`
- `TripsPedestrianDistancesStep`
- `TripsCarFreeFlowTravelTimesStep`
- `RoadNetworkPrimaryEdgesStep`
- `CarAccessEgressStep`
- `BicyclePreferencesStep`
- `BicyclePreferencesFromPopulationStep`
- `WalkingPreferencesFromPopulationStep`
- `PublicTransitPreferencesFromPopulationStep`
- `CarDriverPreferencesFromPopulationStep`
- `CarDriverWithPassengersPreferencesFromPopulationStep`
- `CarPassengerPreferencesFromPopulationStep`
- `CarRidesharingPreferencesFromPopulationStep`
- `LinearScheduleFromPurposeStep`
- `BicycleTravelTimesFromDistanceStep`
- `WalkingTravelTimesFromDistanceStep`
- `TstarFromArrivalTimeStep`
- `PopulationFromTripCoordinatesStep`

New files:

- `TripsPedestrianNodesFile`
- `TripsPedestrianDistancesFile`
- `TripsRoadNodesFile`
- `TripsCarFreeFlowTravelTimesFile`
- `RoadEdgesPrimaryFlagFile`
- `PrimaryCarTripsAccessEgressFile`
- `NonPrimaryCarTripsFile`
- `BicyclePreferencesFile`
- `BicycleTravelTimesFile`
- `WalkingTravelTimesFile`

New features:

- `--step` command line argument to force a Step to be run
- `--step-by-step` command line argument to ask for confirmation before running next step
- Print how long the execution took after each Step
- Time parameters (e.g., `simulation.period`) can be specified as string
- In `WriteMetroEdgesStep`, dummy edges are automatically added when required to prevent parallel
  edges

Breaking changes:

- Renamed `road_type` to `edge_type`
- Removed `nb_road_trips` and `nb_virtual_trips` from `IterationResultsFile` (they are incompatible
  with the primary / secondary road split)
- All Time columns have switched to Duration (allows time after midnight)

Removed steps:

- `CarFreeFlowDistancesStep` (superseded by `TripsCarFreeFlowTravelTimesStep`)
- `CarShortestDistancesStep`

Removed files:

- `CarODsFile` (replaced by `TripsRoadNodesFile`)
- `CarFreeFlowDistancesFile` (superseded by `TripsCarFreeFlowTravelTimesFile`)
- `CarShortestDistancesFile`
- `RoadTripsShareConvergencePlotFile`

Other changes:

- Completely rewrote the pipeline to handle Steps' conflicts
- Lazily import most packages to speed up CLI startup time

## [0.7.0] – 2026-04-21

New steps:

- `OpenStreetMapRoadImportStep`
- `OpenStreetMapPedestrianImportStep`
- `UrbanEdgesStep`
- `OpenStreetMapUrbanAreasStep`
- `PostprocessPedestrianNetworkStep`
- `EqasimImportStep`
- `TripDistancesStep`
- `FrenchHouseholdsHomesZonesStep`
- `FrenchTripsZonesStep`
- `WalkingPreferencesStep`

New files:

- `PedestrianEdgesRawFile`
- `PedestrianEdgesCleanFile`
- `UrbanAreasFile`
- `UrbanEdgesFile`
- `HouseholdsHomesFile`
- `HouseholdsZonesFile`
- `CarsFile`
- `TripsOriginsFile`
- `TripsDestinationsFile`
- `TripsZonesFile`
- `WalkingPreferencesFile`

Breaking changes:

- Renamed `nb_lanes` to `lanes`
- Updated path for some demand files

[unreleased]: https://github.com/Metropolis2/pymetropolis/compare/0.12.0...HEAD
[0.12.0]: https://github.com/Metropolis2/pymetropolis/releases/tag/0.12.0
[0.11.0]: https://github.com/Metropolis2/pymetropolis/releases/tag/0.11.0
[0.10.0]: https://github.com/Metropolis2/pymetropolis/releases/tag/0.10.0
[0.9.0]: https://github.com/Metropolis2/pymetropolis/releases/tag/0.9.0
[0.8.0]: https://github.com/Metropolis2/pymetropolis/releases/tag/0.8.0
[0.7.0]: https://github.com/Metropolis2/pymetropolis/releases/tag/0.7.0

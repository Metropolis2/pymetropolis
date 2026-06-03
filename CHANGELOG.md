# Changelog

## [Unreleased]

New steps:

- `GTFSStep`
- `TripsOpenTripPlannerStep`
- `TripsPublicTransitTravelTimeFromR5Step`

New files:

- `TripsPublicTransitItinerariesFile`

New features:

- New parameter type `DateParameter`

Other changes:

- Make `professional_activity`, `education_level`, and `detailed_education_level` optional in the
  Eqasim output
- Allow simulation areas to be MultiPolygon

Fixes:

- Set the car constant to a *negative* utility

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

[unreleased]: https://github.com/Metropolis2/pymetropolis/compare/0.7.0...HEAD
[0.7.0]: https://github.com/Metropolis2/pymetropolis/releases/tag/0.7.0

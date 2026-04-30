# Changelog

## [Unreleased]

New steps:

- `PedestrianODNodesFromCoordinatesStep`
- `RoadODNodesFromCoordinatesStep`
- `TripsPedestrianDistancesStep`
- `TripsCarFreeFlowTravelTimesStep`
- `RoadNetworkPrimaryEdgesStep`
- `CarAccessEgressStep`
- `BicyclePreferencesStep`

New files:

- `TripsPedestrianNodesFile`
- `TripsPedestrianDistancesFile`
- `TripsRoadNodesFile`
- `TripsCarFreeFlowTravelTimesFile`
- `RoadEdgesPrimaryFlagFile`
- `TripsCarAccessEgressFile`
- `BicyclePreferencesFile`

New features:

- `--step` command line argument to force a Step to be run

Breaking changes:

- Renamed `road_type` to `edge_type`

Removed steps:

- `CarFreeFlowDistancesStep` (superseded by `TripsCarFreeFlowTravelTimesStep`)
- `CarShortestDistancesStep`

Removed files:

- `CarODsFile` (replaced by `TripsRoadNodesFile`)
- `CarFreeFlowDistancesFile` (superseded by `TripsCarFreeFlowTravelTimesFile`)
- `CarShortestDistancesFile`

Other changes:

- Completely rewrote the pipeline to handle Steps' conflicts

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

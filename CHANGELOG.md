# Changelog

## [Unreleased]

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

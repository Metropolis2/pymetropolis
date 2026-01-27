from pymetropolis.metro_pipeline.file import MetroPlotFile


class TripDepartureTimeDistributionPlotFile(MetroPlotFile):
    path = "results/graphs/trip_departure_time_distribution.pdf"
    description = "Histogram of departure time distribution, over trips."

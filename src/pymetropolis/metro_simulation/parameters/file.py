from pymetropolis.metro_pipeline.file import MetroTxtFile


class MetroParametersFile(MetroTxtFile):
    path = "run/parameters.json"
    description = "JSON file with the parameters for the Metropolis-Core simulation."

from pymetropolis.metro_pipeline.file import MetroJsonFile


class MetroParametersFile(MetroJsonFile):
    path = "run/parameters.json"
    description = "JSON file with the parameters for the Metropolis-Core simulation."


class MetroExAnteParametersFile(MetroJsonFile):
    path = "run/ex_ante_parameters.json"
    description = "JSON file with the parameters for the ex-ante simulation."

from pymetropolis.metro_pipeline.file import (
    Column,
    MetroDataFrameFile,
    MetroDataType,
    MetroJsonFile,
)


class SurveyModeChoiceResultsFile(MetroJsonFile):
    path = "calibration/econometrics/mode_choice_results.json"
    description = (
        "Results of the survey mode choice econometric model. "
        "The JSON file has four keys: `specification` (the modes, the reference mode, the "
        "variables and the definitions of the derived variables of the estimated model), `stats` "
        "(summary statistics), `parameters` (the estimated value, standard error, t-statistic "
        "and p-value of each coefficient, by coefficient name) and `mode_shares` (the observed "
        "and predicted shares of each mode, weighted by the survey weights)."
    )


class SurveyModeChoiceProbabilitiesFile(MetroDataFrameFile):
    path = "calibration/econometrics/mode_choice_probabilities.parquet"
    description = (
        "Probability of each available mode for each surveyed tour, predicted by the survey mode "
        "choice econometric model. "
        "Only the tours used in the estimation are included, with one row per available mode."
    )
    schema = [
        Column("tour_id", MetroDataType.ID, description="Identifier of the tour.", nullable=False),
        Column(
            "mode", MetroDataType.STRING, description="Mode of the alternative.", nullable=False
        ),
        Column(
            "probability",
            MetroDataType.FLOAT,
            description="Predicted probability of choosing the mode.",
            nullable=False,
        ),
        Column(
            "chosen",
            MetroDataType.BOOL,
            description="Whether the mode is the one chosen in the survey.",
            nullable=False,
        ),
    ]

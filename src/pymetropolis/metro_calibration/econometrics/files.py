from pymetropolis.metro_pipeline.file import MetroTxtFile


class SurveyModeChoiceResultsFile(MetroTxtFile):
    path = "calibration/econometrics/mode_choice_results.json"
    description = (
        "Results of the survey mode choice econometric model. "
        "The JSON file has three keys: `specification` (the modes, the reference mode and the "
        "variables of the estimated model), `stats` (summary statistics) and `parameters` (the "
        "estimated value, standard error, t-statistic and p-value of each coefficient, by "
        "coefficient name)."
    )

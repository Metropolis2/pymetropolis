from pymetropolis.metro_pipeline.file import MetroTxtFile


class SurveyModeChoiceParametersFile(MetroTxtFile):
    path = "calibration/econometrics/mode_choice_parameters.json"
    description = "Estimated parameters of the survey mode choice econometric model."


class SurveyModeChoiceStatsFile(MetroTxtFile):
    path = "calibration/econometrics/mode_choice_stats.json"
    description = "Summary statistics of the survey mode choice econometric model."

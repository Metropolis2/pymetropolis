from .files import (
    JointTourEstimatorFile,
    SurveyedCarsFile,
    SurveyedDetailedZonesFile,
    SurveyedDrawZonesFile,
    SurveyedHouseholdsFile,
    SurveyedLegsFile,
    SurveyedMotorcyclesFile,
    SurveyedPersonsFile,
    SurveyedSpecialLocationsFile,
    SurveyedToursFile,
    SurveyedTripsFile,
)
from .joint_travel import ClassifyJointToursStep, EstimateJointToursClassifierStep
from .mobisurvstd import MobiSurvStdImportStep
from .tours import CleanSurveyToursStep

SURVEY_FILES = [
    SurveyedHouseholdsFile,
    SurveyedPersonsFile,
    SurveyedTripsFile,
    SurveyedLegsFile,
    SurveyedCarsFile,
    SurveyedMotorcyclesFile,
    SurveyedSpecialLocationsFile,
    SurveyedDetailedZonesFile,
    SurveyedDrawZonesFile,
    SurveyedToursFile,
    JointTourEstimatorFile,
]

SURVEY_STEPS = [
    MobiSurvStdImportStep,
    CleanSurveyToursStep,
    EstimateJointToursClassifierStep,
    ClassifyJointToursStep,
]

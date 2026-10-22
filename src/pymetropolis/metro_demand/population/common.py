# List of activity purposes in the survey data.
# Note. For now these are MobiSurvStd purposes. Ideally it should be configurable.
PURPOSES = ["home", "work", "education", "shopping", "task", "leisure", "escort", "other"]

# lowest category -> lowest density
DENSITY_CATS = "7", "6", "5", "4", "3", "2", "1"
URBAN_TYPE_CATS = "outside_urban_unit", "suburb", "isolated_city", "central_city"
FNC_AREA_TYPE_CATS = "30", "20", "13", "12", "11"
FNC_AREA_CAT_CATS = "5", "4", "3", "2", "1"

# Human-friendly names of the tour-level variables (the columns of `ToursFile`), used to label the
# variables in plots. Variables missing from the dictionary are shown with their column name.
TOUR_VARIABLE_LABELS = {
    # Tour-level variables.
    "nb_trips": "Nb. trips",
    "nb_activities": "Nb. activities",
    "first_purpose": "First purpose",
    "last_purpose": "Last purpose",
    "first_departure_time": "First departure time",
    "last_arrival_time": "Last arrival time",
    "first_activity_start": "First activity start",
    "last_activity_end": "Last activity end",
    "trip_weekday": "Weekday",
    "outside_perimeter": "Outside perimeter",
    "total_tour_duration": "Total tour duration",
    "total_activity_duration": "Total activity duration",
    "total_travel_time": "Total travel time",
    "total_distance": "Total distance",
    **{f"has_{purpose}_purpose": f"Has {purpose} purpose" for purpose in PURPOSES},
    "lowest_density": "Lowest density",
    "highest_density": "Highest density",
    "lowest_urban_type": "Lowest urban type",
    "highest_urban_type": "Highest urban type",
    "lowest_functional_area_type": "Lowest functional area type",
    "highest_functional_area_type": "Highest functional area type",
    "lowest_functional_area_category": "Lowest functional area category",
    "highest_functional_area_category": "Highest functional area category",
    # Household-level variables.
    "home_density": "Home density",
    "home_urban_type": "Home urban type",
    "home_functional_area_type": "Home functional area type",
    "home_functional_area_category": "Home functional area category",
    "nb_cars": "Nb. cars",
    "nb_motorcycles": "Nb. motorcycles",
    "nb_bicycles": "Nb. bicycles",
    "nb_persons": "Nb. persons",
    "nb_majors": "Nb. adults",
    "nb_minors": "Nb. minors",
    "nb_women": "Nb. women",
    "nb_major_women": "Nb. adult women",
    "nb_men": "Nb. men",
    "nb_major_men": "Nb. adult men",
    "nb_driving_licenses": "Nb. driving licenses",
    "household_type": "Household type",
    "minor_ratio": "Share of minors",
    "car_ratio": "Cars per person",
    "driving_license_ratio": "Cars per license holder",
    # Person-level variables.
    "woman": "Woman",
    "age": "Age",
    "oldest_age_diff": "Age gap to oldest member",
    "education_level": "Education level",
    "professional_activity": "Professional activity",
    "socioprofessional_class": "Socioprofessional class",
    "has_driving_license": "Has driving license",
    "has_public_transit_subscription": "Has PT subscription",
    "nb_tours": "Nb. tours",
}

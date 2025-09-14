# Base URLs
FBREF_BASE_URL = 'https://fbref.com'

# User Agent
USER_AGENT = 'Ryan/5.0'

# HTML Class Names
STATS_TABLE_CLASS = 'stats_table'
TEAM_LOGO_CLASS = 'teamlogo'
SCORE_CLASS = 'score'
SCORE_AGGREGATE_CLASS = 'score_aggr'
SCORE_PENALTY_CLASS = 'score_pen'
DATAPOINT_CLASS = 'datapoint'
SCOREBOX_META_CLASS = 'scorebox_meta'
EVENT_HEADER_CLASS = 'event_header'
EVENT_A_CLASS = 'event a'
EVENT_B_CLASS = 'event b'
EVENT_ICON_CLASS = 'event_icon'
YELLOW_CARD_CLASS = 'yellow_card'
RED_CARD_CLASS = 'red_card'
LINEUP_CLASS = 'lineup'
VENUETIME_CLASS = 'venuetime'

# Event Type Mappings
EVENT_TYPE_MAPPING = {
    'substitute_in': 'substitute',
    'penalty_shootout_goal': 'penalty goal',
    'penalty_shootout_miss': 'penalty miss',
}

# Match Event Headers
MATCH_EVENT_HEADERS = {
    'Kick Off': '1st half',
    'Half Time': '2nd half',
    'Full Time': 'Extra time',
    'Penalty Shootout': 'Penalty'
}

# Officials Role Mapping
OFFICIALS_ROLE_MAPPING = {
    "AR1": "assistant_referee_1",
    "AR2": "assistant_referee_2",
    "4th": "fourth_official",
    "Referee": "main_referee",
    "VAR": "video_assistant_referee",
}

SEARCH_STATUS_NOT_EXISTS = 1
SEARCH_STATUS_CONFUSE = 2
SEARCH_STATUS_SUCCESS = 3

COUNTRY_CODE_MAPPING =  {
    "ARG": "Argentina",
    "AUS": "Australia",
    "AUT": "Austria",
    "BEL": "Belgium",
    "BOL": "Bolivia",
    "BRA": "Brazil",
    "BUL": "Bulgaria",
    "CAN": "Canada",
    "CHI": "Chile",
    "CHN": "China",
    "COL": "Colombia",
    "CRO": "Croatia",
    "CZE": "Czech Republic",
    "DEN": "Denmark",
    "ECU": "Ecuador",
    "ENG": "England",
    "ESP": "Spain",
    "FIN": "Finland",
    "FRA": "France",
    "GER": "Germany",
    "GRE": "Greece",
    "HUN": "Hungary",
    "IND": "India",
    "IRN": "Iran",
    "ITA": "Italy",
    "JPN": "Japan",
    "KOR": "South Korea",
    "KSA": "Saudi Arabia",
    "MEX": "Mexico",
    "NED": "Netherlands",
    "NOR": "Norway",
    "PAR": "Paraguay",
    "PER": "Peru",
    "POL": "Poland",
    "POR": "Portugal",
    "ROU": "Romania",
    "RSA": "South Africa",
    "RUS": "Russia",
    "SCO": "Scotland",
    "SRB": "Serbia",
    "SUI": "Switzerland",
    "SWE": "Sweden",
    "TUR": "Turkey",
    "UKR": "Ukraine",
    "URU": "Uruguay",
    "USA": "United States",
    "VEN": "Venezuela"
}

COMPETITION_CATEGORIES = [
    'Club International Cups', 'National Team Competitions', 
    'Big 5 European Leagues', 
    'Domestic Leagues - 1st Tier', 'Domestic Leagues - 2nd Tier', 'Domestic Leagues - 3rd Tier and Lower',
    'National Team Qualification', 'Domestic Cups', 'Domestic Youth Leagues'
]
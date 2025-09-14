import re
import yaml
import pandas as pd
import urllib.parse

from bs4.element import Tag
from typing import List, Any, Dict

def indent_print(msg: str, indent_level: int = 0, end: str = '\n') -> None:
    """
    Prints a message with a specified indentation level.
    """
    indent = '\t' * indent_level
    if msg.startswith('\n'):
        msg = '\n' + indent + msg.lstrip('\n')
    print(f'{indent}{msg}', end=end)

def normalize_string_for_url(name : str) -> str:
    """
    Normalizes a string for use in a URL by replacing spaces with hyphens and quoting.
    """
    return urllib.parse.quote(name.replace(' ', '-')) # Türkiye

def extract_hrefs(html_element: Tag, pattern: str) -> List[str]:
    """
    Extracts href attributes from <a> tags within a specific HTML element (Tag object)
    that match a given regex pattern.
    """
    hrefs: List[str] = []
    regex = re.compile(pattern)

    for a in html_element.find_all('a', href=True):
        href = a['href']
        if regex.search(href):
            hrefs.append(href)

    return hrefs

def load_config(config_path: str) -> dict[str, Any]:
    """
    Loads configuration from a YAML file.
    """
    try:
        with open(config_path, 'r', encoding='utf-8') as file:
            config = yaml.safe_load(file)
        return config
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file not found at: {config_path}")
    except yaml.YAMLError as e:
        raise ValueError(f"Error parsing YAML configuration: {e}")
    
def report_country_stats(enable_countries : pd.DataFrame) -> None:
    """
    Prints statistics for country filtering.
    """
    grouped = enable_countries.groupby('Governing Body')['Country'].apply(list)

    for governing_body, countries in grouped.items():
        indent_print(f"\n[{governing_body}], add {len(countries)} teams", indent_level=1)
        if countries:
            indent_print("- Include: " + ", ".join(countries), indent_level=2)

def report_club_stats(country: str, total_clubs: int, filtered_clubs: List[str]) -> None:
    """
    Prints statistics for club filtering.
    """
    indent_print(f'- {country}: add {len(filtered_clubs)} clubs in total {total_clubs} clubs', indent_level=2)
    if filtered_clubs:
        indent_print('+ Include: ' + ', '.join(filtered_clubs) + '\n', indent_level=3)

def report_competition_stats(enable_competitions: Dict[str, Any]) -> None:
    """
    Prints statistics for competition filtering, grouped by type (Domestic, Club International, National)
    and then by Country or Governing Body.
    """

    indent_print(f'\nTotal competitions processed: {len(enable_competitions)}', indent_level=1)

    domestic_comps = enable_competitions[
        enable_competitions['Category'].str.contains('Domestic')
    ]

    if not domestic_comps.empty:
        indent_print('\n[DOMESTIC]', indent_level=1)
        for country, group in domestic_comps.groupby("Country"):
            comps = ", ".join(group["Competition Name"].tolist())
            indent_print(f"- Add ({comps}) from {country}", indent_level=2)

    club_international_comps = enable_competitions[
        enable_competitions['Category'] == 'Club International Cups'
    ].copy()

    if not club_international_comps.empty:
        indent_print('\n[CLUB INTERNATIONAL]', indent_level=1)
        for gov, group in club_international_comps.groupby("Governing Body"): 
            comps = ", ".join(group["Competition Name"].tolist())
            indent_print(f"- Add ({comps}) from {gov}", indent_level=2)

    national_team_comps = enable_competitions[
        enable_competitions['Category'].str.contains('National')
    ].copy()

    if not national_team_comps.empty:
        indent_print('\n[NATIONAL]', indent_level=1)
        for gov, group in national_team_comps.groupby("Governing Body"): 
            comps = ", ".join(group["Competition Name"].tolist())
            indent_print(f"- Add ({comps}) from {gov}", indent_level=2)

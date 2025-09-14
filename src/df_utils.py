import re
import pandas as pd
from bs4.element import Tag
from typing import Dict, Any

from src.utils import extract_hrefs 

def clean_table(table: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans a pandas DataFrame by removing duplicate headers and entirely null rows.
    Assumes duplicate headers are rows where all values match the column names.
    """
    if table.columns.nlevels == 2:
        cols = table.columns.get_level_values(1).astype(str).unique()
    else:
        cols = table.columns

    table = table[~table.astype(str).isin(cols).all(axis=1)]
    table = table.dropna(how='all')

    return table    

def process_fixture(table: pd.DataFrame) -> pd.DataFrame:
    """
    Processes a fixture DataFrame to extract home/away scores and penalties from a 'Score' column.
    """
    pattern = r'(?:\((\d*)\)\s*)?(\d+)\s*-\s*(\d+)(?:\s*\((\d*)\))?'
    table['Score'] = table['Score'].fillna('').str.replace(r'[–—−]', '-', regex=True).str.strip()
    matches = table['Score'].str.extract(pattern)

    table['Home Score'] = pd.to_numeric(matches[1], errors='coerce')
    table['Away Score'] = pd.to_numeric(matches[2], errors='coerce')

    table['Home Penalty'] = pd.to_numeric(matches[0], errors='coerce')
    table['Away Penalty'] = pd.to_numeric(matches[3], errors='coerce')

    table = table.rename(columns={'xG': 'Home xG', 'xG.1': 'Away xG'})
    return table.drop(columns='Score', errors='ignore')

def add_match_code(
        table: pd.DataFrame, 
        html_tag: Tag # Đổi từ BeautifulSoup sang Tag
    ) -> pd.DataFrame:
    """Adds 'Match Code' and updates 'Notes' column in a DataFrame based on HTML links."""

    pattern = r'^/en/matches/([a-z0-9]+)/(.+)$'
    hrefs = extract_hrefs(html_tag, pattern)
    match_codes = [href.split('/')[3] for href in hrefs]

    # Match_codes are duplicated for some reason 
    # match_codes = [match_codes[i] for i in range(0, len(match_codes), 2)] 
    match_codes = list(dict.fromkeys(match_codes))

    column_name = None
    if 'Match Report' in table.columns:
        column_name = 'Match Report'
    elif 'Final' in table.columns:
        column_name = 'Final'

    if column_name is not None:
        table['Match Code'] = None  
        mask = table[column_name] == 'Match Report'
        table.loc[mask, 'Match Code'] = match_codes[:mask.sum()]
        table = table.drop(columns=column_name)

    if match_codes:
        first_match_idx = table.index[table['Match Code'].notna()][0]

        mask_above = table.index < first_match_idx
        table.loc[mask_above & table['Match Code'].isna(), 'Match Code'] = "Not yet played"

        mask_below = table.index > first_match_idx
        table.loc[mask_below & table['Match Code'].isna(), 'Match Code'] = "No data available"
    else:
        table['Match Code'] = "Not yet played"

    return table

def filter_countries(countries_df: pd.DataFrame, country_filter_config: Dict[str, Any]) -> pd.DataFrame:
    """Filters country DataFrame based on provided configuration."""
    filtered_df = countries_df.copy()

    if country_filter_config.get('governing'):
        filtered_df = filtered_df[filtered_df['Governing Body'].isin(country_filter_config['governing'])]
    if country_filter_config.get('country'):
        filtered_df = filtered_df[filtered_df['Country'].isin(country_filter_config['country'])]
    
    filtered_df = filtered_df[~filtered_df['National Code'].isna()]
    return filtered_df

def filter_clubs(clubs_df: pd.DataFrame, club_filter_config: Dict[str, Any]) -> pd.DataFrame:
    """Filters club DataFrame based on provided configuration."""
    filtered_df = clubs_df.copy()
    filtered_df = filtered_df[filtered_df['Gender'] == 'M']

    if club_filter_config.get('club'):
        filtered_df = filtered_df[filtered_df['Club'].isin(club_filter_config['club'])]
        
    return filtered_df


def filter_competitions(competitions_df: pd.DataFrame, comp_filter_config: Dict[str, Any]) -> pd.DataFrame:
    """Filters competition DataFrame based on provided configuration."""

    allowed_governing_bodies = comp_filter_config.get('governing', [])
    allowed_domestic_categories = comp_filter_config.get('domestic', [])
    allowed_national_comp_names = comp_filter_config.get('national', [])
    allowed_domestic_countries = comp_filter_config.get('country', [])
    
    competitions_df = competitions_df[competitions_df['Gender'] == 'M'].copy()

    domestic_competitions = pd.DataFrame()
    if allowed_domestic_categories and allowed_domestic_countries:
        domestic_competitions = competitions_df[
            competitions_df['Category'].isin(allowed_domestic_categories)
            & competitions_df['Country'].isin(allowed_domestic_countries) 
        ].copy()


    club_international_competitions = pd.DataFrame() 
    if allowed_governing_bodies:
        club_international_competitions = competitions_df[
            competitions_df['Governing Body'].isin(allowed_governing_bodies) 
            & (competitions_df['Category'] == 'Club International Cups')
        ].copy()
    

    national_competitions = pd.DataFrame() # Khởi tạo DataFrame rỗng
    if allowed_national_comp_names:
        national_competitions = competitions_df[
            competitions_df['Competition Name'].isin(allowed_national_comp_names)
        ].copy()


    filtered_df = pd.concat([domestic_competitions, national_competitions, club_international_competitions], ignore_index=True)
    filtered_df = filtered_df.drop_duplicates(subset=['Competition Index']) 

    return filtered_df
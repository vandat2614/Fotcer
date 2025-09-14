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

    # table = table.rename(columns={'xG': 'Home xG', 'xG.1': 'Away xG'})
    return table.drop(columns=['Score', 'xG', 'xG.1'], errors='ignore')

def add_match_code(table: pd.DataFrame,  html_tag: Tag) -> pd.DataFrame:
    """Adds 'Match Code' column in a DataFrame based on HTML links."""

    pattern = r'^/en/matches/([a-z0-9]+)/(.+)$'
    hrefs = extract_hrefs(html_tag, pattern)
    match_codes = [href.split('/')[3] for href in hrefs]

    # Match_codes are duplicated for some reason 
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

    table['Match Code'] = table['Match Code'].fillna('No data available')

    return table

def match_info_to_df(match_info: dict, match_code: str) -> pd.DataFrame:
    """Convert match_info dict to a DataFrame with specific columns."""

    date = match_info.get('datetime', {}).get('date')
    time = match_info.get('datetime', {}).get('time')
    
    home_team = match_info.get('teams', {}).get('home', {}).get('name')
    away_team = match_info.get('teams', {}).get('away', {}).get('name')
    
    attendance = match_info.get('attendance')
    
    venue_dict = match_info.get('venue', {})
    venue = venue_dict.get('stadium')
    
    referee = match_info.get('officials', {}).get('main_referee', None)
    notes = None
    
    home_score = match_info.get('scores', {}).get('home')
    away_score = match_info.get('scores', {}).get('away')
    
    home_penalty = match_info.get('penalties', {}).get('home') if 'penalties' in match_info else None
    away_penalty = match_info.get('penalties', {}).get('away') if 'penalties' in match_info else None
    
    row = {
        "Date": date,
        "Time": time,
        "Home": home_team,
        "Away": away_team,
        "Attendance": attendance,
        "Venue": venue,
        "Referee": referee,
        "Notes": notes,
        "Match Code": match_code,
        "Home Score": home_score,
        "Away Score": away_score,
        "Home Penalty": home_penalty,
        "Away Penalty": away_penalty
    }
    
    return pd.DataFrame([row])

def split_champion_column(table: pd.DataFrame) -> pd.DataFrame:
    """Splits the 'Champion' column into two columns 'Champion' and 'Point'"""
    split_data = table['Champion'].str.rsplit('-', n=1, expand=True)

    table['Champion'] = split_data[0].str.strip()
    table['Points'] = pd.to_numeric(split_data[1].str.strip(), errors='coerce')

    table[['Champion', 'Points']] = table[['Champion', 'Points']].fillna("Season not finished yet")
    return table
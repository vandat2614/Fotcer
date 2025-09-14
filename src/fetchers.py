import re
import time
import pandas as pd
import urllib.request

from io import StringIO
from bs4 import BeautifulSoup
from bs4.element import Tag
from functools import lru_cache
from typing import Tuple, List, Dict, Any, Optional

from src.utils import normalize_string_for_url, extract_hrefs
from src.df_utils import clean_table, process_fixture, add_match_code
from src.constants import FBREF_BASE_URL, USER_AGENT, STATS_TABLE_CLASS, COUNTRY_CODE_MAPPING
from .parsers import get_match_events, get_match_lineups, get_match_stats, get_match_info

@lru_cache(maxsize=None)
def _fetch(url: str) -> Tuple[List[pd.DataFrame], BeautifulSoup, List[Tag]]: # Thay đổi kiểu trả về để bao gồm tables_html (List[Tag])
    """
    Fetches HTML from a given URL, cleans it, and parses it into pandas DataFrames
    and a BeautifulSoup object. Also returns raw HTML tables as Tag objects.
    Includes error handling and a delay.
    """
    try:
        req = urllib.request.Request(url, headers={'User-Agent': USER_AGENT})
        with urllib.request.urlopen(req) as response:
            html_bytes = response.read()

        html_str = html_bytes.decode('utf-8', errors='ignore')
        html_str = re.sub(r'<!--|-->', '', html_str)

        tables = pd.read_html(StringIO(html_str), attrs={'class': STATS_TABLE_CLASS})
        soup = BeautifulSoup(html_str, 'lxml')
        tables_html_tags = soup.find_all('table', {'class': STATS_TABLE_CLASS}) # Tên rõ ràng hơn
        
        time.sleep(5) 
        
        return tables, soup, tables_html_tags # Trả về cả soup và tables_html_tags

    except urllib.error.URLError as e:
        # print(f"URL Error for {url}: {e.reason}")
        raise
    except pd.errors.ParserError as e:
        # print(f"Pandas HTML parsing error for {url}: {e}")
        raise
    except Exception as e:
        # print(f"An unexpected error occurred while fetching {url}: {e}")
        raise

def fetch_country() -> pd.DataFrame:
    """Fetches country data from FBref, including country codes and national team codes."""
    url = f'{FBREF_BASE_URL}/en/countries/'
    tables, soup, tables_html_tags = _fetch(url) # Nhận cả tables_html_tags
    table = clean_table(tables[0]) # tables[0] là DataFrame

    # Giả định bạn muốn extract_hrefs từ bảng đầu tiên
    html_table_tag = tables_html_tags[0] 
    
    # Extract country codes từ bảng đầu tiên
    country_hrefs = extract_hrefs(html_table_tag, pattern='^/en/country/[A-Z]+/[A-Za-z-]+$')
    country_codes = [href.split('/')[3] for href in country_hrefs]

    table['Country Code'] = country_codes

    national_team_href_pattern = r'^/en/squads/([a-z0-9]+)/history/([A-Za-z0-9\-]+)-Stats-and-History$'
    national_team_hrefs = extract_hrefs(html_table_tag, national_team_href_pattern) # Sử dụng html_table_tag
    national_codes = [href.split('/')[3] for href in national_team_hrefs]

    index = 0
    for idx, row in table.iterrows():
        if pd.isna(row['National Teams']):
            continue
        count = len(row['National Teams'].split('/'))
        table.at[idx, 'National Code'] = national_codes[index]
        index += count

    return table[['Country', 'Country Code', '# Clubs', 'Governing Body', 'National Code']]


def fetch_club(country_name : str, country_code : str) -> pd.DataFrame:
    """Fetches club data for a specific country from FBref."""
    url = f'{FBREF_BASE_URL}/en/country/clubs/{country_code}/{normalize_string_for_url(country_name)}-Football-Clubs'
    tables, soup, tables_html_tags = _fetch(url) # Nhận cả tables_html_tags

    table = clean_table(tables[0])
    table.rename(columns={'Squad' : 'Club'}, inplace=True)

    club_href_pattern = r'^/en/squads/([a-z0-9]+)/history/([A-Za-z0-9\-]+)-Stats-and-History$'
    club_hrefs = extract_hrefs(tables_html_tags[0], club_href_pattern) # Sử dụng tables_html_tags[0]
    club_codes = [href.split('/')[3] for href in club_hrefs]
    
    table['Club Code'] = club_codes
    table.loc[:,'Country'] = country_name

    return table[['Country', 'Club', 'Club Code', 'From', 'To', 'Gender']]

def fetch_h2h(first_team_name: str, first_team_code: str, second_team_name: str, second_team_code: str) -> pd.DataFrame:
    """Fetches head-to-head match history between two teams from FBref."""
    url = (
        f'{FBREF_BASE_URL}/en/stathead/matchup/teams/{first_team_code}/{second_team_code}/'
        f'{normalize_string_for_url(first_team_name)}-vs-{normalize_string_for_url(second_team_name)}-History'
    )
    tables, soup, tables_html_tags = _fetch(url) # Nhận cả tables_html_tags
    
    table = clean_table(tables[0])
    # add_match_code cần một Tag, nên truyền tables_html_tags[0] vào đây
    table = add_match_code(table, tables_html_tags[0]) 
    table = process_fixture(table)

    return table

def fetch_match_detail(match_code: str) -> Dict[str, Dict[str, Any]]:
    """
    Fetches detailed information for a specific match from FBref,
    including lineups, match info, events, and stats.
    """
    url = f"{FBREF_BASE_URL}/en/matches/{match_code}"

    # Use _fetch for consistency and caching
    # For match_detail, we primarily need the full soup object for parsing
    _, soup, _ = _fetch(url) # Bỏ qua tables và tables_html_tags nếu không cần

    lineups = get_match_lineups(soup)
    match_info = get_match_info(soup)

    home_team: str
    away_team: str
    if len(lineups) == 2:
        home_team, away_team = list(lineups.keys())
    elif 'teams' in match_info and 'home' in match_info['teams'] and 'away' in match_info['teams']:
        home_team = match_info['teams']['home']['team_name']
        away_team = match_info['teams']['away']['team_name']
    else:
        raise ValueError(f"Could not determine home and away teams for match {match_code}")

    match_events = get_match_events(soup, home_team, away_team)
    stats = get_match_stats(soup)

    return {
        'info' : match_info,
        'lineup' : lineups,
        'events' : match_events,
        'stats' : stats
    }

def fetch_competitions() -> pd.DataFrame:
    """
    Fetches raw competition data from FBref, categorizes, and cleans it.
    """
    url = f'{FBREF_BASE_URL}/en/comps/'
    tables, soup, tables_html_tags = _fetch(url) 

    categories = [
        'Club International Cups', 'National Team Competitions', 
        'Big 5 European Leagues', 
        'Domestic Leagues - 1st Tier', 'Domestic Leagues - 2nd Tier', 'Domestic Leagues - 3rd Tier and Lower',
        'National Team Qualification', 'Domestic Cups', 'Domestic Youth Leagues'
    ]

    clean_tables = []
    for i in range(len(tables)):
        if i == 2: continue 
        
        hrefs = extract_hrefs(tables_html_tags[i], pattern=r'^/en/comps/[A-Za-z0-9]+/history/.+$')
        comp_indicies = [href.split('/')[3] for href in hrefs]

        table = clean_table(tables[i])
        table['Category'] = categories[i] 
        table['Competition Index'] = comp_indicies
        
        table['Format'] = 'League' if (3 <= i <= 5 or i == 8) else 'Cup'
        columns = ['Competition', 'Gender', 'First Season', 'Last Season', 'Category', 'Competition Index', 'Format']
        
        columns = ['Competition Name', 'Gender', 'First Season', 'Last Season', 'Category', 'Competition Index', 'Format']
        if 'Governing Body' in table.columns: columns.append('Governing Body')
        if 'Country' in table.columns: 
            columns.append('Country')
            table['Country'] = table['Country'].str.split(' ').str[1]
            table["Country"] = table["Country"].map(COUNTRY_CODE_MAPPING).fillna(table["Country"])

        clean_tables.append(table[columns])
    
    return pd.concat(clean_tables, ignore_index=True)

def split_champion_column(table: pd.DataFrame) -> pd.DataFrame:
    """Splits the 'Champion' column into two columns 'Champion' and 'Point'
    """
    split_data = table['Champion'].str.rsplit('-', n=1, expand=True)

    table['Champion'] = split_data[0].str.strip()
    table['Points'] = pd.to_numeric(split_data[1].str.strip(), errors='coerce')

    table[['Champion', 'Points']] = table[['Champion', 'Points']].fillna("Season not finished yet")
    return table

def fetch_history(comp_index: str, category: str) -> pd.DataFrame: # Kiểu trả về là DataFrame
    """Fetches the historical data for a specific competition."""

    url = f"{FBREF_BASE_URL}/en/comps/{comp_index}/history"
    tables, soup, tables_html_tags = _fetch(url) 
    
    table = clean_table(tables[0])

    if '# Squads' not in table.columns: # super cup
        table = add_match_code(table, tables_html_tags[0])

    if "Domestic Leagues" in category:
        table = split_champion_column(table)
    else:
        if category == 'National Team Competitions':
            table['Champion'] = table['Champion'].str.split(' ', n=1).str[1]
            table['Runner-Up'] = table['Runner-Up'].str.split(' ', n=1).str[1]
        table[['Champion', 'Runner-Up']] = table[['Champion', 'Runner-Up']].fillna("Season not finished yet")

    
    drop_columns = []
    if 'Top Scorer' in table.columns: drop_columns.append('Top Scorer')
    if 'Final' in table.columns: drop_columns.append('Final')
    table = table.drop(columns=drop_columns)

    if 'Year' in table.columns:
        table = table.rename(columns={'Year' : 'Season'})
    table['Season'] = table['Season'].astype(str)


    return table

def fetch_fixture(comp_name : str = None, comp_index : str = None, season : str = None, match_code : str = None):
    if match_code:
        url = f"{FBREF_BASE_URL}/en/matches/{match_code}"
        tables, soup, tables_html_tags = _fetch(url) 

        match_info = get_match_info(soup)
        return match_info_to_df(match_info, match_code)

    url = f'{FBREF_BASE_URL}/en/comps/{comp_index}/{season}/schedule/{season}-{normalize_string_for_url(comp_name)}-Scores-and-Fixtures'
    
    try:
        tables, soup, tables_html_tags = _fetch(url) 
    except:
        return None
    
    table = clean_table(tables[0])
    table = add_match_code(table, tables_html_tags[0])

    return process_fixture(table)

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

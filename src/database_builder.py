import pandas as pd
from typing import Any, Dict, Generator, Tuple, List, Literal

from src.utils import indent_print, load_config, report_country_stats, report_club_stats, report_competition_stats
from src.df_utils import filter_countries, filter_clubs, filter_competitions
from src.fetchers import fetch_country, fetch_club, fetch_competitions, fetch_history, fetch_fixture
from src.database_manager import DatabaseManager 

import warnings
warnings.filterwarnings('ignore')

def _get_and_process_countries(config: Dict[str, Any] = {}) -> Tuple[pd.DataFrame, Dict[str, Dict[str, Any]]]:
    """
    Fetches raw country data, applies filters, and computes statistics.
    Returns filtered DataFrame and a dictionary of statistics.
    """
    raw_countries = fetch_country()
    
    total_stats: Dict[str, Dict[str, Any]] = {} 
    for gov in raw_countries['Governing Body'].unique():
        df_gov = raw_countries[raw_countries['Governing Body'] == gov]
        total_stats[gov] = {'total': len(df_gov)}

    filtered_countries = filter_countries(raw_countries, config)
    for gov in total_stats.keys():
        current_filtered_countries = filtered_countries[
            filtered_countries['Governing Body'] == gov
        ]
        total_stats[gov]['filtered_count'] = current_filtered_countries['Country'].nunique()
        total_stats[gov]['filtered_names'] = current_filtered_countries['Country'].to_list()

    return filtered_countries.reset_index(drop=True), total_stats

def _get_and_process_clubs(club_filter_config: Dict[str, Any]) -> Generator[Dict[str, Any], None, None]:
    """
    Fetches raw country data, applies *club_filter_config's country/governing filters* to decide
    which countries to fetch clubs for. Then fetches club data, applies remaining club filters,
    and computes statistics for clubs.
    """
    all_raw_countries = fetch_country()

    countries_to_fetch_clubs = filter_countries(all_raw_countries, club_filter_config)
    countries_to_fetch_clubs = countries_to_fetch_clubs.sort_values(
        by=['Governing Body', 'Country'], ascending=True
    ).reset_index(drop=True)
    
    for _, row in countries_to_fetch_clubs.iterrows():
        country_name = row['Country']
        country_code = row['Country Code']
        total_clubs_in_country = row['# Clubs'] # Số lượng club thô từ fbref

        current_country_stats = {
            'country': country_name, 
            'governing': row['Governing Body'], 
            'total': total_clubs_in_country, # Vẫn là tổng số clubs ban đầu của quốc gia đó
            'filtered_names': []
        }

        if total_clubs_in_country == 0:
            yield {'data': pd.DataFrame(), 'stats': current_country_stats}
            continue

        # indent_print(f'  Fetching clubs for {country_name}...', indent_level=2)
        clubs_raw_df = fetch_club(country_name, country_code)
        
        filtered_clubs_df = filter_clubs(clubs_raw_df, club_filter_config)

        current_country_stats['filtered_names'] = filtered_clubs_df['Club'].to_list()
        yield {'data': filtered_clubs_df, 'stats': current_country_stats}

def _get_and_process_competitions(comp_filter_config: Dict[str, Any]) -> pd.DataFrame: # THAY ĐỔI KIỂU TRẢ VỀ
    """
    Fetches raw competition data, applies filters. Then returns the filtered DataFrame.
    """
    raw_competitions = fetch_competitions()
    filtered_competitions = filter_competitions(raw_competitions, comp_filter_config)

    return filtered_competitions.reset_index(drop=True)

def build_database(config_path: str, db_name: str = 'fotcer', saved_path: str = None, overwrite_db: bool = False) -> None:
    """Orchestrates the process of fetching, filtering, and storing football data into a SQLite database."""
    
    indent_print('=== STARTING DATABASE BUILD ===\n', indent_level=0)

    config = load_config(config_path)
    update_config = config['update']

    db_manager = DatabaseManager(db_name=db_name, saved_path=saved_path)
    db_manager.initialize_database(overwrite=overwrite_db)

    if update_config['country']:
        indent_print('\n=== UPDATING NATIONAL TEAMS ===', indent_level=0)
        country_filter = config.get('country', {}) 
        filtered_countries, country_stats = _get_and_process_countries(country_filter)
        
        db_manager.write_dataframe(filtered_countries, table_name='Country', if_exists='replace')
        for governing_body, stats in country_stats.items():
            report_country_stats(governing_body, stats['total'], stats['filtered_names'])
        
        indent_print(f'\nTotal countries processed: {len(filtered_countries)}', indent_level=1)

    if update_config['club']:
        indent_print('\n=== UPDATING CLUBS ===', indent_level=0)
    
        club_filter = config.get('club', {})
        current_gov_for_clubs = None

        for country_output in _get_and_process_clubs(club_filter): 
            clubs_df = country_output['data']
            stats = country_output['stats']
            
            governing_body = stats['governing']
            country_name = stats['country']
            
            if current_gov_for_clubs != governing_body:
                current_gov_for_clubs = governing_body
                indent_print(f'\n[{current_gov_for_clubs}]', indent_level=1)
            
            report_club_stats(country_name, stats['total'], stats['filtered_names'])

            if not clubs_df.empty:
                if db_manager.is_table_existing('Club'):
                    existing_countries = db_manager.read_table('Club')['Country'].unique().tolist() 
                    if country_name in existing_countries:
                        db_manager.delete_records('Club', {'Country': country_name})
                        indent_print(f'+ Deleted existing club records for {country_name}\n', indent_level=3)
                
                db_manager.write_dataframe(clubs_df, table_name='Club', if_exists='append')
            else:
                indent_print(f'+ No clubs found for {country_name} after filtering. Skipping database write.', indent_level=3)
   
    if update_config['competition']:
        indent_print('\n=== UPDATING COMPETITIONS ===', indent_level=0)
    
        comp_filter = config.get('competition', {})
        filtered_competitions = _get_and_process_competitions(comp_filter)

        db_manager.write_dataframe(filtered_competitions, table_name='Competition', if_exists='replace')
        report_competition_stats(filtered_competitions, comp_filter)


    if update_config['history']:
        indent_print('\n=== UPDATING COMPETITION HISTORY ===', indent_level=0)

        if not db_manager.is_table_existing('Competition'):
            raise ValueError("You must build the Competition table before updating history.")
        
        competitions = db_manager.read_table('Competition')
        for idx, row in competitions.iterrows():
            comp_name = row['Competition Name']
            comp_index = row['Competition Index']
            
            history_df = fetch_history(comp_index)
            seasons = ', '.join(history_df['Season'].tolist())

            indent_print(f'\n[{comp_name}] - Avail seasons: {seasons}', indent_level=1)

            table_name = f'{comp_name} History'
            db_manager.write_dataframe(history_df, table_name=table_name, if_exists='replace')

    if update_config['fixture']:
        indent_print('\n=== UPDATING COMPETITION FIXTURES ===', indent_level=0)

        if not db_manager.is_table_existing('Competition'):
            raise ValueError("You must build the Competition table before updating history.")

        competitions = db_manager.read_table('Competition')
        for _, row in competitions.iterrows():
            comp_name = row['Competition Name']
            comp_index = row['Competition Index']

            indent_print(f'[{comp_name}]', indent_level=0)

            if not db_manager.is_table_existing(f'{comp_name} History'):
                raise ValueError("You must build the History table before updating fixture.")
        
            comp_history = db_manager.read_table(f'{comp_name} History')
            avail_seasons = comp_history['Season'].tolist()

            table_name = f'{comp_name} Fixture'
            if db_manager.is_table_existing(table_name):
                exists_fixtures = db_manager.read_table(table_name)
                exists_seasons = exists_fixtures['Season'].tolist()
            else:
                exists_fixtures = None
                exists_seasons = []

            update_seasons = [ss for ss in avail_seasons if ss not in exists_seasons]
            if avail_seasons[0] not in update_seasons:
                update_seasons.insert(0, avail_seasons[0])

            for i, season in enumerate(update_seasons):
                indent_print(f'Season {season}', indent_level=1)
        
                fixture = fetch_fixture(comp_name, comp_index, season)
                if fixture is None:
                    indent_print(f'- No data found', indent_level=2)
                    continue

                fixture['Season'] = season
                if 'Round' in fixture.columns:
                    mask = fixture['Round'].str.contains('Relegation')
                    fixture = fixture[~mask] 
                    fixture = fixture.drop(columns=['Round'])
                
                if not db_manager.is_table_existing(table_name):
                    db_manager.write_dataframe(fixture, table_name, if_exists='replace')
                else:
                    if season == avail_seasons[0]: # latest season
                        db_manager.delete_records(table_name, conditions={'Season' : season})
                        indent_print(f'- Delete old records', indent_level=2)

                    db_manager.add_records(table_name, fixture)
                indent_print(f'- Add {len(fixture)} matches\n', indent_level=2)


    indent_print('\n=== DATABASE BUILD COMPLETE ===\n', indent_level=0)
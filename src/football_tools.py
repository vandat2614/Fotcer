# src/football_tools.py (Đã cập nhật - Hàm độc lập với @tool)

import pandas as pd
from rapidfuzz import process, fuzz
from typing import Dict, Any, Union, Tuple, List, Optional

# Cần các import này VÌ SỬ DỤNG @tool VÀ PYDANTIC TRỰC TIẾP Ở ĐÂY
from langchain_core.tools import tool 
from pydantic import BaseModel, Field

from src.constants import SEARCH_STATUS_NOT_EXISTS, SEARCH_STATUS_CONFUSE, SEARCH_STATUS_SUCCESS
from src.fetchers import fetch_h2h, fetch_match_detail
from src.database_manager import DatabaseManager # Vẫn cần DatabaseManager để sử dụng


# ==============================================================================
# Pydantic Models cho Input Schema của Tools (Đặt ở đây)
# ==============================================================================
class Head2HeadInput(BaseModel):
    """Input for get_h2h tool."""
    first_team: str = Field(..., description="Name of the first team")
    second_team: str = Field(..., description="Name of the second team")

class MatchDetailInput(BaseModel):
    """Input for get_match_detail tool."""
    first_team: str = Field(..., description="Name of the first team")
    second_team: str = Field(..., description="Name of the second team")
    date: str = Field(..., description="Date of the match in YYYY-MM-DD format (e.g., 'YYYY-MM-DD')")


# ==============================================================================
# Global / Module-level variable để lưu trữ dữ liệu đội (cần khởi tạo)
# ==============================================================================
_ALL_TEAM_DATA: Optional[List[Dict[str, str]]] = None
_ALL_TEAM_DISPLAY_NAMES: Optional[List[str]] = None
_DB_MANAGER: Optional[DatabaseManager] = None

def _initialize_team_data_and_db_manager(db_manager: DatabaseManager) -> None:
    """Initializes global team data and db_manager for the module."""
    global _ALL_TEAM_DATA, _ALL_TEAM_DISPLAY_NAMES, _DB_MANAGER
    if _ALL_TEAM_DATA is None or _DB_MANAGER is None:
        _DB_MANAGER = db_manager
        try:
            club_data = _DB_MANAGER.get_all_club_names_with_codes()
            country_data = _DB_MANAGER.get_all_country_names_with_codes()
            _ALL_TEAM_DATA = club_data + country_data
            _ALL_TEAM_DISPLAY_NAMES = [team['name'] for team in _ALL_TEAM_DATA]
        except Exception as e:
            print(f"Warning: Could not load all team names for fuzzy matching: {e}")
            print("Fuzzy matching for team names might be limited or unavailable. Ensure the database is built.")
            _ALL_TEAM_DATA = []
            _ALL_TEAM_DISPLAY_NAMES = []


def _search_team_internal(team_name: str) -> Dict[str, Any]:
    """
    Internal function to search for a team (club or national) in the database.
    Assumes _ALL_TEAM_DATA and _DB_MANAGER have been initialized.
    """
    if _ALL_TEAM_DATA is None:
        return {'status': SEARCH_STATUS_NOT_EXISTS, 'message': "Team data not initialized. Call _initialize_team_data_and_db_manager first."}

    # --- Bước 1: Tìm kiếm khớp chính xác (Case-insensitive) ---
    exact_matches = [
        team for team in _ALL_TEAM_DATA 
        if team['name'].lower() == team_name.lower()
    ]

    if len(exact_matches) == 1:
        return {'status': SEARCH_STATUS_SUCCESS, 'code': exact_matches[0]['code'], 'name': exact_matches[0]['name']}
    
    elif len(exact_matches) > 1:
         found = ', '.join([m['name'] for m in exact_matches])
         return {
             'status': SEARCH_STATUS_CONFUSE,
             'message': f'The name \'{team_name}\' is not specific enough. We found multiple exact matches: {found}. Please try again with a more specific name.'
         }

    # --- Bước 2: Fuzzy Matching ---
    if not _ALL_TEAM_DISPLAY_NAMES:
        return {
            'status': SEARCH_STATUS_NOT_EXISTS,
            'message': f"Team '{team_name}' was not found and fuzzy matching data is unavailable."
        }

    FUZZY_MATCH_THRESHOLD = 90 
    
    matches = process.extract(team_name, _ALL_TEAM_DISPLAY_NAMES, limit=5, scorer=fuzz.WRatio)
    
    highly_similar_matches = [(match_name, score) for match_name, score, _ in matches if score >= FUZZY_MATCH_THRESHOLD]

    if len(highly_similar_matches) == 1:
        chosen_name = highly_similar_matches[0][0]
        chosen_team_data = next(
            (team for team in _ALL_TEAM_DATA if team['name'] == chosen_name),
            None
        )
        if chosen_team_data:
            return {'status': SEARCH_STATUS_SUCCESS, 'code': chosen_team_data['code'], 'name': chosen_team_data['name']}
        
    elif len(highly_similar_matches) > 1:
        found = ', '.join([m[0] for m in highly_similar_matches])
        return {
            'status': SEARCH_STATUS_CONFUSE,
            'message': f'The name \'{team_name}\' is not specific enough. We found several highly similar matches: {found}. Please try again with the correct club name.'
        }
    
    # --- Bước 3: Gợi ý ---
    recommends_list = [match[0] for match in matches if match[1] > 70 and match[0] not in [m[0] for m in highly_similar_matches]]
    recommends = ', '.join(recommends_list)

    message_suffix = f" Perhaps you meant: {recommends}?" if recommends else ""
    return {
        'status': SEARCH_STATUS_NOT_EXISTS,
        'message': f"Team '{team_name}' was not found.{message_suffix}"
    }


def _check_teams_exist_and_get_codes(first_team_input: str, second_team_input: str) -> Dict[str, Any]:
    """Checks if both teams exist and returns their codes and exact names if successful."""
    res1 = _search_team_internal(first_team_input)
    res2 = _search_team_internal(second_team_input)

    message_parts = []
    if res1['status'] != SEARCH_STATUS_SUCCESS: message_parts.append(res1['message'])
    if res2['status'] != SEARCH_STATUS_SUCCESS: message_parts.append(res2['message'])

    if message_parts:
        return {'status': SEARCH_STATUS_NOT_EXISTS, 'message': '\n'.join(message_parts)}
    
    return {
        'status': SEARCH_STATUS_SUCCESS, 
        'team_codes': (res1['code'], res2['code']),
        'team_names': (res1['name'], res2['name'])
    }


def _search_match_internal(first_team_input: str, second_team_input: str, date: str) -> Dict[str, Any]:
    """Searches for a specific match on a given date between two teams."""
    if _DB_MANAGER is None:
        return {'status': SEARCH_STATUS_NOT_EXISTS, 'message': "Database manager not initialized."}

    check_result = _check_teams_exist_and_get_codes(first_team_input, second_team_input)
    if check_result['status'] != SEARCH_STATUS_SUCCESS:
        return check_result

    actual_first_team, actual_second_team = check_result['team_names']
    first_code, second_code = check_result['team_codes']

    h2h_data = fetch_h2h(actual_first_team, first_code, actual_second_team, second_code)

    if isinstance(h2h_data, str):
        return {'status' : SEARCH_STATUS_NOT_EXISTS, 'message' : h2h_data} 

    match_on_date = h2h_data[h2h_data['Date'] == date]
    if match_on_date.empty:
        match_dates = ', '.join(h2h_data['Date'].unique().tolist())
        msg = f"No match found: {actual_first_team} vs {actual_second_team} on {date}. Available dates: {match_dates}."
        return {'status' : SEARCH_STATUS_NOT_EXISTS, 'message' : msg}
    
    match_row = match_on_date.iloc[0]
    return {
        'status' : SEARCH_STATUS_SUCCESS, 
        'match_code' : match_row['Match Code'], 
        'competition' : match_row['Comp']
    }

# ==========================================================================
# Public Langchain Tools (Hàm độc lập với @tool)
# ==========================================================================
# Các hàm này sẽ trực tiếp là Langchain Tool.
# Chúng cần nhận db_manager như một tham số nếu chúng cần nó.
# Tuy nhiên, vì _initialize_team_data_and_db_manager đã thiết lập biến global,
# các hàm này có thể gọi các helper mà không cần truyền db_manager trực tiếp.

@tool(args_schema=Head2HeadInput) 
def get_h2h(first_team: str, second_team: str) -> Union[str, pd.DataFrame]:
    """
    Historical head-to-head matches between two football teams.
    Returns a DataFrame of match history or an error message if teams are not found or ambiguous.
    """
    check_result = _check_teams_exist_and_get_codes(first_team, second_team)
    if check_result['status'] != SEARCH_STATUS_SUCCESS:
        return check_result['message']
    
    first_code, second_code = check_result['team_codes']
    actual_first_team, actual_second_team = check_result['team_names']
    
    return fetch_h2h(actual_first_team, first_code, actual_second_team, second_code)

@tool(args_schema=MatchDetailInput) 
def get_match_detail(first_team: str, second_team: str, date: str) -> Union[str, Dict[str, Any]]:
    """
    Get detailed match information for a given match date between two teams.
    Date should be in YYYY-MM-DD format.
    Returns a dictionary of match details or an error message.
    """
    search_result = _search_match_internal(first_team, second_team, date)    
    if search_result['status'] != SEARCH_STATUS_SUCCESS:
        return search_result['message']
    
    match_code = search_result['match_code']
    match_detail_data = fetch_match_detail(match_code=match_code)
    
    match_detail_data['competition'] = search_result['competition']

    return match_detail_data
# # main.py (Đã cập nhật - Khởi tạo _football_tools và sử dụng trực tiếp các Tool)

# import yaml
# from typing import Any, Dict, List, Union

# # KHÔNG CẦN CÁC IMPORT NÀY NỮA
# # from pydantic import BaseModel, Field
# # from langchain.tools import Tool 

# from src.database_manager import DatabaseManager
# # Import trực tiếp các hàm Tool và Pydantic Models từ football_tools
# from src.football_tools import get_h2h, get_match_detail, Head2HeadInput, MatchDetailInput, _initialize_team_data_and_db_manager
# from src.utils import load_config 

# # ==============================================================================
# # 1. Pydantic Models đã được chuyển vào football_tools.py
# # ==============================================================================

# # ==============================================================================
# # 2. Khởi tạo Database và Service Logic
# # ==============================================================================
# def initialize_application_resources(config_path: str = 'config.yaml') -> Dict[str, Any]:
#     app_config = load_config(config_path) 
    
#     db_name = app_config.get('database', {}).get('name', 'fotcer')
#     saved_path = app_config.get('database', {}).get('path', None)

#     db_manager = DatabaseManager(db_name=db_name, saved_path=saved_path)
    
#     # KHỞI TẠO DỮ LIỆU ĐỘI VÀ DB_MANAGER TRONG MODULE football_tools
#     _initialize_team_data_and_db_manager(db_manager)

#     # ==========================================================================
#     # 3. Tập hợp Tools cho Agent (Sử dụng trực tiếp các hàm đã @tool-ed)
#     # ==========================================================================
#     tools_list = [get_h2h, get_match_detail] # List các Langchain Tool trực tiếp

#     return {
#         "db_manager": db_manager,
#         # Không cần football_tools_instance nữa nếu không có class
#         # "football_logic_service": None, # Hoặc xóa hoàn toàn dòng này
#         "tools_list": tools_list, 
#         "tool_input_schemas": [Head2HeadInput, MatchDetailInput]
#     }


# app_resources = initialize_application_resources('config.yaml')
# print(app_resources['tools_list'][1].invoke({'first_team' : 'Real Madrid FC', 'second_team' : 'FC Barcelona', 'date' : '2024-10-26'}))


from src.fetchers import fetch_competitions, fetch_history, fetch_fixture
from src.df_utils import filter_competitions
from src.utils import report_competition_stats
from src.database_builder import build_database



# comps = fetch_competitions()
# comps.to_csv('1.csv')

# from src.database_manager import DatabaseManager

# db = DatabaseManager()
# print(db.read_table('Premier League Fixture')['Season'].value_counts())
# print(db.get_table_names())
# db.read_table('EFL Cup History').to_csv('hehe.csv')
# db.read_table('Competition').to_csv('comps.csv')

build_database('config.yaml')

# config = {
#     'country' : ['Spain', 'Portugal', 'Netherlands', 'Italy', 'Germany', 'France', 'England', 'Turkey'],
#     'governing' : ['UEFA', 'FIFA'],
#     'domestic' : ['Domestic Cups', 'Domestic Leagues - 1st Tier'],
#     'national' : ['FIFA World Cup', 'UEFA Nations League', 'UEFA European Football Championship']
# }

# r = filter_competitions(comps, config)
# r.to_csv('2.csv', index=Fsalse)

# import pandas as pd
# r = pd.read_csv('track.csv')
# report_competition_stats(r, config)
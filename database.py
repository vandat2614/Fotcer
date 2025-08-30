import os
import time
from typing import List, Dict
import ast
import urllib.request
import re

import pandas as pd
from sqlalchemy import create_engine
from langchain_community.utilities import SQLDatabase
from langchain_community.tools.sql_database.tool import QuerySQLDatabaseTool

class Database:
    def __init__(self, competitions_info: List[Dict]):
        self.competitions_info = competitions_info

    def load(self, path: str):
        if not os.path.exists(path):
            raise FileNotFoundError(f"Database file not found: {path}")
        self._load(path)

    def _fetch(self, base_url):
        req = urllib.request.Request(base_url, headers={"User-Agent": "Ryan/5.0"})
        with urllib.request.urlopen(req) as response:
            html = response.read()

        html = html.decode("utf-8", errors="ignore")
        html = re.sub(r'<!--|-->', '', str(html))
        
        try:
            tables = pd.read_html(html, attrs={'class': 'stats_table'})
        except Exception as e:
            return None

        clean_tables = []
        for df in tables:
            df = df.dropna(how='all')

            if df.columns.nlevels == 2:
                cols = df.columns.get_level_values(1).astype(str).unique()
            else: cols = df.columns
            
            df = df[~df.astype(str).isin(cols).all(axis=1)]
            clean_tables.append(df)

        return clean_tables

    def _fetch_schedule(self, league_index, season, league_name):
        base_url = f'https://fbref.com/en/comps/{league_index}/{season}/schedule/{season}-{league_name}-Scores-and-Fixtures'
        return self._fetch(base_url)

    def _fetch_history(self, league_index, league_name):
        base_url = f'https://fbref.com/en/comps/{league_index}/history/{league_name}-Seasons'
        return self._fetch(base_url)

    def _clean_table(self, table, domestic, format):
        
        drop_columns = ['Match Report', 'Score']


        pattern = r"(?:\((\d*)\)\s*)?(\d+)\s*-\s*(\d+)(?:\s*\((\d*)\))?"
        table["Score"] = table["Score"].str.replace(r"[–—−]", "-", regex=True).str.strip()

        matches = table["Score"].str.extract(pattern)

        table["Home Score"]   = pd.to_numeric(matches[1], errors="coerce")
        table["Away Score"]   = pd.to_numeric(matches[2], errors="coerce")

        if format == 'cup':
            table["Home Penalty"] = pd.to_numeric(matches[0], errors="coerce")
            table["Away Penalty"] = pd.to_numeric(matches[3], errors="coerce")
        # elif 'Round' in table.columns:
        #     table = table[~table['Round'].str.contains('Relegation', case=False, na=False)]
        #     drop_columns.append('Round')

        if not domestic:
            table["Home Nation"] = table["Home"].str.rsplit(' ', n=1).str[1]
            table["Away Nation"] = table["Away"].str.split(' ', n=1).str[0]

            table["Home"] = table["Home"].str.rsplit(' ', n=1).str[0]
            table["Away"] = table["Away"].str.split(' ', n=1).str[1]

        table = table.drop(columns=drop_columns)
        table = table.rename(columns={'xG': 'Home xG', 'xG.1': 'Away xG'})

        return table
    
    def _load(self, path):
        self.db_path = path
        self.engine = create_engine(f"sqlite:///{path}")
        self.db = SQLDatabase(engine=self.engine)

        self.dialect = self.db.dialect
        self.get_usable_table_names = self.db.get_usable_table_names
        self.get_table_info = self.db.get_table_info

    def update(self, db_name=None, save_db_folder=None, save_data_folder=None):
        if not hasattr(self, "engine"):
            if db_name == None:
                raise RuntimeError("Database not loaded. Call load() first or provide save path for new database")

            if save_db_folder is None:
                self.db_path = f"{db_name}.db"
            else:
                os.makedirs(save_db_folder, exist_ok=True)
                self.db_path = os.path.join(save_db_folder, f"{db_name}.db")            
            self._load(self.db_path)


        for info in self.competitions_info:
            league_index = info["index"]
            league_name = info["name"]
            domestic = info['domestic']
            format = info['format']
            table_name = league_name.replace("-", "")

            history = self._fetch_history(league_index, league_name)[0]
            history.to_sql(name=table_name + "History",con=self.engine,index=False,if_exists="replace")

            if save_data_folder is not None:
                base_path = os.path.join(save_data_folder, table_name)
                saved_path = os.path.join(base_path, "history.csv")
                os.makedirs(os.path.dirname(saved_path), exist_ok=True)
                history.to_csv(saved_path, index=False)

            if "Season" in history.columns:
                seasons = [str(season) for season in history["Season"]]
            else:
                seasons = [str(season) for season in history["Year"]]

            latest_season = max(seasons)

            print(f"Update {league_name}")

            for i, season in enumerate(seasons[::-1]):

                table_exists = table_name in self.db.get_usable_table_names()

                if table_exists:
                    query = f"SELECT COUNT(*) FROM {table_name} WHERE Season='{season}'"
                    result = self.db.run(query)
                    parsed = ast.literal_eval(result)
                    count = parsed[0][0]
                else: count = 0

                need_fetch = False
                if count == 0:
                    need_fetch = True
                    print(f"\tAdd season {season}")
                elif season == latest_season and table_exists:
                    need_fetch = True
                    print(f"\tRefreshing latest season {season}")

                    delete_query = f"DELETE FROM {table_name} WHERE Season='{season}'"
                    self.db.run(delete_query)

                if need_fetch:
                    table = self._fetch_schedule(league_index, season, league_name)
                    if table is None:
                        print('\t\tNo table found')
                        continue

                    if domestic and len(table) > 1:
                        table = table[1]
                    else: table = table[0]

                    if save_data_folder is not None:
                        saved_path = os.path.join(save_data_folder, table_name, season.replace("-", "_"), "schedule.csv")
                        os.makedirs(os.path.dirname(saved_path), exist_ok=True)
                        table.to_csv(saved_path, index=False)

                    table = self._clean_table(table, domestic, format)
                    table["Season"] = season

                    if save_data_folder is not None:
                        saved_path = os.path.join(save_data_folder+'_clean', table_name, season.replace("-", "_"), "schedule.csv")
                        os.makedirs(os.path.dirname(saved_path), exist_ok=True)
                        table.to_csv(saved_path, index=False)

                    try:
                        table.to_sql(name=table_name, con=self.engine, index=False, if_exists="append")
                    except Exception as e:
                        print(f"\t\tSome errors, fallback to concat + replace")
                        old_df = pd.read_sql(f"SELECT * FROM {table_name}", con=self.engine)

                        merged = pd.concat([old_df, table], ignore_index=True)
                        merged.to_sql(name=table_name, con=self.engine, index=False, if_exists="replace")
                    
                    if i < len(seasons) - 1:
                        time.sleep(5)

        self._load(self.db_path)

    def check_all_tables(self):
        """Kiểm tra toàn bộ dữ liệu trong database, in ra số bảng, số hàng và một vài dòng đầu"""
        tables = self.db.get_usable_table_names()
        results = {}
        for table in tables:
            try:
                df = pd.read_sql_table(table, self.engine)
                results[table] = {
                    "rows": len(df),
                    "cols" : df.columns.tolist()
                }
            except Exception as e:
                results[table] = {"error": str(e)}
        return results
    
    def run(self, query):

        if not hasattr(self, "engine"):
            raise RuntimeError("Database not loaded. Call load() first")

        execute_query_tool = QuerySQLDatabaseTool(db=self.db)
        result = execute_query_tool.invoke(query)

        return result
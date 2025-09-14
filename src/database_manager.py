import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Inspector
from typing import Dict, Any, List, Optional
from .constants import SEARCH_STATUS_CONFUSE, SEARCH_STATUS_NOT_EXISTS, SEARCH_STATUS_SUCCESS
from rapidfuzz import process, fuzz
from typing import Union, List, Tuple, Dict

class DatabaseManager:
    def __init__(self, db_name: str = 'fotcer', saved_path: str = None):
        self.db_name = db_name
        self.db_path = self._get_db_path(saved_path)
        self.engine = create_engine(f'sqlite:///{self.db_path}')
        
        self.dialect = 'sqlite'
        self.initialize_team_data()

    def _get_db_path(self, saved_path: str = None) -> str:
        if saved_path:
            return os.path.join(saved_path, f'{self.db_name}.db')
        return f'{self.db_name}.db'

    def initialize_database(self, overwrite: bool = True) -> None:
        """Initializes the database, deleting existing one if overwrite is True."""
        if overwrite and os.path.exists(self.db_path):
            os.remove(self.db_path)
            print(f'Deleted existing database: {self.db_path}')
        print(f'Database path: {self.db_path}')

    def execute_query(self, query_str: str, as_list: bool = False) -> Union[pd.DataFrame, List[Tuple], Dict[str, str]]:
        """Executes a SQL query and returns the result."""
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(query_str, con=conn)

            if as_list:
                return list(df.itertuples(index=False, name=None))
            return df

        except Exception as e:
            return e

    def write_dataframe(self, df: pd.DataFrame, table_name: str, if_exists: str = 'append', **kwargs) -> None:
        """Writes a DataFrame to a specified table in the database."""
        with self.engine.connect() as conn:
            df.to_sql(name=table_name, con=conn, index=False, if_exists=if_exists, **kwargs)
            conn.commit()

    def add_records(self, table_name: str, table: pd.DataFrame, subset: Optional[List[str]] = None) -> None:
        """
        Appends new records from a DataFrame into the specified table.
        Removes duplicates based on subset of columns (or all columns if None).
        """
        if table.empty:
            raise ValueError("The provided DataFrame is empty. Nothing to add.")

        if self.is_table_existing(table_name):
            existing_df = self.read_table(table_name)
            
            combined = pd.concat([existing_df, table], ignore_index=True)
            combined = combined.drop_duplicates(subset=subset)

            with self.engine.connect() as conn:
                combined.to_sql(
                    name=table_name,
                    con=conn,
                    index=False,
                    if_exists="replace"
                )
                conn.commit()
        else:
            self.write_dataframe(table, table_name, if_exists='replace')

    def delete_records(self, table_name: str, conditions: Dict[str, Any]) -> None:
        if not conditions:
            raise ValueError("Conditions must be provided for deletion.")

        where_clauses = []
        params = {}

        for i, (col, val) in enumerate(conditions.items()):
            param_name = f"param_{i}"  
            where_clauses.append(f'"{col}" = :{param_name}')
            params[param_name] = val

        query = f'DELETE FROM "{table_name}" WHERE {" AND ".join(where_clauses)}'
        self.execute_query(query)        

    def is_table_existing(self, table_name: str) -> bool:
        """Checks if a table exists in the database."""
        inspector = Inspector.from_engine(self.engine) 
        return table_name in inspector.get_table_names()

    def read_table(self, table_name: str) -> pd.DataFrame:
        """Reads a table from the database into a pandas DataFrame."""
        if not self.is_table_existing(table_name):
            return pd.DataFrame() 
        return self.execute_query(f'SELECT * FROM "{table_name}"')

    def get_inspector(self) -> Inspector:
        """Return the SQLAlchemy Inspector for introspection."""
        return Inspector.from_engine(self.engine)

    def get_table_info(self, sample_rows: int = 3) -> str:
        """Return schema and sample data for all tables in the database."""
        insp = self.get_inspector()
        tables = insp.get_table_names()

        if not tables:
            return "-- No tables found in the database."

        results = []
        for table_name in tables:
            columns = insp.get_columns(table_name)

            create_sql = f'CREATE TABLE "{table_name}" (\n'
            col_defs = []
            for col in columns:
                col_name = col["name"]
                col_type = str(col["type"])
                col_defs.append(f'\t"{col_name}" {col_type}')
            create_sql += ", \n".join(col_defs) + "\n)\n"

            sample_str = ""
            df = self.read_table(table_name).head(sample_rows)

            if not df.empty:
                sample_str += f"\n/*\n{len(df)} rows from {table_name} table:\n"
                sample_str += "\t".join(df.columns) + "\n"
                for _, row in df.iterrows():
                    sample_str += "\t".join(str(val) for val in row.tolist()) + "\n"
                sample_str += "*/\n"

            results.append(create_sql + sample_str)

        return "\n".join(results)

    def get_table_names(self) -> List[str]:
        """Returns a list of all table names in the database."""
        inspector = Inspector.from_engine(self.engine)
        return inspector.get_table_names()

    def delete_table(self, table_name: str) -> None:
        """Deletes an entire table from the database."""
        if not self.is_table_existing(table_name):
            raise ValueError(f"Table '{table_name}' does not exist.")

        query = f'DROP TABLE "{table_name}"'
        with self.engine.connect() as conn:
            conn.execute(text(query))
            conn.commit()

    def initialize_team_data(self) -> None:
        """Load team data (clubs + countries) if tables exist, else init empty."""
        if not (self.is_table_existing("Club") and self.is_table_existing("Country")):
            self._all_team_data = pd.DataFrame(columns=["code", "name"])
            return

        clubs = self.get_all_club_names_with_codes()
        countries = self.get_all_country_names_with_codes()

        if isinstance(clubs, Exception):
            clubs = pd.DataFrame(columns=["code", "name"])
        if isinstance(countries, Exception):
            countries = pd.DataFrame(columns=["code", "name"])

        self._all_team_data = pd.concat([clubs, countries], ignore_index=True)

    def search_team(self, team_name: str, fuzzy_threshold: int = 90) -> Dict[str, Any]:
        """
        Search for a team (club or national) in cached database data.
        Requires initialize_team_data() to be called first.
        """

        if self._all_team_data.empty:
            return {
                'status': SEARCH_STATUS_NOT_EXISTS,
                'message': "Team data not initialized. Call initialize_team_data() first."
            }

        # --- Step 1: Exact match (case-insensitive)
        exact_matches = self._all_team_data[
            self._all_team_data['name'].str.lower() == team_name.lower()
        ]

        if len(exact_matches) == 1:
            row = exact_matches.iloc[0]
            return {
                'status': SEARCH_STATUS_SUCCESS,
                'code': row['code'],
            }

        # --- Step 2: Fuzzy matching (top 5 recommendations)
        matches = process.extract(
            team_name.lower(),
            self._all_team_data['name'].str.lower(),
            limit=5,
            scorer=fuzz.WRatio
        )

        candidates = [
            self._all_team_data.iloc[idx]['name']
            for name, score, idx in matches if score >= fuzzy_threshold
        ]

        if not candidates:
            return {
                'status': SEARCH_STATUS_NOT_EXISTS,
                'message': f"No similar team found for '{team_name}'."
            }

        found = ', '.join(candidates)
        return {
            'status': SEARCH_STATUS_CONFUSE,
            'message': f"No exact match for '{team_name}'. Did you mean one of these?",
            'message': f"The name '{team_name}' is not specific enough. Did you mean one of these: {found}"
        }

    def get_all_club_names_with_codes(self) -> List[Dict[str, str]]:
        """Fetches all club names and their codes from the database."""
        
        query = 'SELECT "Club Code" AS code, "Club" AS name FROM Club'
        return self.execute_query(query)

    def get_all_country_names_with_codes(self) -> List[Dict[str, str]]:
        """Fetches all country names (national teams) and their codes from the database."""

        query = 'SELECT "National Code" AS code, "Country" AS name FROM Country'
        return self.execute_query(query)
        

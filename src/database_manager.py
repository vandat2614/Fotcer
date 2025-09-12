import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Inspector
from typing import Dict, Any, List

class DatabaseManager:
    def __init__(self, db_name: str = 'fotcer', saved_path: str = None):
        self.db_name = db_name
        self.db_path = self._get_db_path(saved_path)
        self.engine = create_engine(f'sqlite:///{self.db_path}')

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

    def write_dataframe(self, df: pd.DataFrame, table_name: str, if_exists: str = 'append', **kwargs) -> None:
        """Writes a DataFrame to a specified table in the database."""
        with self.engine.connect() as conn:
            df.to_sql(name=table_name, con=conn, index=False, if_exists=if_exists, **kwargs)
            conn.commit() # Commit transaction if using begin/commit pattern

    def delete_records(self, table_name: str, conditions: Dict[str, Any]) -> None:
        """Deletes records from a table based on conditions."""
        if not conditions:
            raise ValueError("Conditions must be provided for deletion.")
        
        # Tạo WHERE clause
        where_clauses = [f"{col} = :{col}" for col in conditions.keys()]
        query = f'DELETE FROM "{table_name}" WHERE {" AND ".join(where_clauses)}'
        
        with self.engine.connect() as conn:
            conn.execute(text(query), conditions)  # phải dùng text()
            conn.commit()

    def add_records(self, table_name: str, table: pd.DataFrame) -> None:
        """Appends new records from a DataFrame into the specified table."""
        if table.empty:
            raise ValueError("The provided DataFrame is empty. Nothing to add.")

        with self.engine.connect() as conn:
            table.to_sql(
                name=table_name,
                con=conn,
                index=False,
                if_exists="append"  # đảm bảo append
            )
            conn.commit()


    def read_table(self, table_name: str) -> pd.DataFrame:
        """Reads a table from the database into a pandas DataFrame."""
        if not self.is_table_existing(table_name):
            return pd.DataFrame() # Return empty DataFrame if table does not exist

        with self.engine.connect() as conn:
            return pd.read_sql(f'SELECT * FROM "{table_name}"', con=conn)
        
    def is_table_existing(self, table_name: str) -> bool:
        """Checks if a table exists in the database."""
        inspector = Inspector.from_engine(self.engine) # Correct way to get Inspector
        return table_name in inspector.get_table_names()
        
    def execute_query(self, query_str: str) -> pd.DataFrame:
        """Executes a SQL query and returns the result as a pandas DataFrame."""
        with self.engine.connect() as conn:
            return pd.read_sql(query_str, con=conn)
        
    def get_all_club_names_with_codes(self) -> List[Dict[str, str]]:
        """Fetches all club names and their codes from the database."""
        query = 'SELECT "Club Code" AS code, "Club" AS name FROM Club'
        with self.engine.connect() as conn:
            return pd.read_sql(query, conn).to_dict(orient='records')

    def get_all_country_names_with_codes(self) -> List[Dict[str, str]]:
        """Fetches all country names (national teams) and their codes from the database."""
        query = 'SELECT "National Code" AS code, "Country" AS name FROM Country'
        with self.engine.connect() as conn:
            return pd.read_sql(query, conn).to_dict(orient='records')
        
    def get_table_names(self) -> List[str]:
        """Returns a list of all table names in the database."""
        inspector = Inspector.from_engine(self.engine)
        return inspector.get_table_names()
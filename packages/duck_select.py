# main.py
import logging
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent))
import duckdb
import pandas as pd
from pathlib import Path
from config import duckdb_db_path



def execute_sql_query(sql_query: str) -> pd.DataFrame:
    logging.info(Path.cwd() / duckdb_db_path)

    con: duckdb.DuckDBPyConnection = None

    try:
        # Connect to the DuckDB database
        con = duckdb.connect(database=Path.cwd() / duckdb_db_path, read_only=False)

        # Execute the provided SQL query and fetch the results into a DataFrame
        result_df = con.execute(sql_query).fetchdf()
        return result_df

    except duckdb.Error as e:
        print(f"Error executing DuckDB query: {str(e)}")
        return pd.DataFrame()

    except Exception as e:
        print(f"Unexpected error occurred: {str(e)}")
        return pd.DataFrame()

    finally:
        if con is not None:
            try:
                con.close()
            except Exception as e:
                print(f"Error closing database connection: {str(e)}")

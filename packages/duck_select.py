# main.py
import logging
import sys
from pathlib import Path

from packages.db_connection import DuckDBConnection

sys.path.append(str(Path(__file__).parent))
import duckdb
import pandas as pd
from config import duckdb_db_path, ldf

logger = logging.getLogger(__name__)


def execute_sql_query(sql_query: str, parameters: list = None) -> pd.DataFrame:
    df = pd.DataFrame()
    try:
        with DuckDBConnection(Path.cwd() / duckdb_db_path) as conn:
            df = conn.execute(sql_query, parameters).fetchdf()
    except duckdb.Error as e:
        logger.error(f"Error executing DuckDB query: {str(e)}")
        return df
    except Exception as e:
        logger.error(f"Unexpected error occurred: {str(e)}")
        return df
    logger.info(f"Query executed successfully: {sql_query}")
    return df

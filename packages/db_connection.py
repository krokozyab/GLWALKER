# packages/db_connection.py

import duckdb
from pathlib import Path
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class DuckDBConnection:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.connection: Optional[duckdb.DuckDBPyConnection] = None

    def __enter__(self) -> duckdb.DuckDBPyConnection:
        try:
            self.connection = duckdb.connect(database=self.db_path, read_only=False)
            logger.info(f"Successfully connected to database at {self.db_path}")
            return self.connection
        except Exception as e:
            logger.error(f"Failed to connect to database: {str(e)}")
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            try:
                self.connection.close()
                logger.info("Database connection closed successfully")
            except Exception as e:
                logger.error(f"Error closing database connection: {str(e)}")

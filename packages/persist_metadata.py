import json
import logging
from pathlib import Path
from typing import List

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
import duckdb
import re

# Configure logging to output to console with level INFO
logging.basicConfig(level=logging.INFO)


def load_lg_list_to_dataframe(file_path: str) -> pd.DataFrame:
    """
    Loads the LG list from a JSON-like file into a pandas DataFrame.

    Parameters:
    - file_path (str): The path to the lg_list.json file.

    Returns:
    - pd.DataFrame: DataFrame containing the ledger segments.
    """
    try:
        # Step 1: Read the file content
        with open(file_path, 'r', encoding='utf-8') as file:
            content: str = file.read()

        # Step 2: Extract the JSON array using regex
        # This regex finds content within the first pair of parentheses
        match: re.Match = re.search(r'LEDGERS_LIST\s*\(\s*(\[\s*{.*}\s*])\s*\)', content, re.DOTALL)
        if not match:
            raise ValueError("The file format is incorrect or the JSON array is not found.")

        json_array_str: str = match.group(1)

        # Step 3: Parse the JSON data
        data: list = json.loads(json_array_str)

        # Step 4: Load into pandas DataFrame
        df: pd.DataFrame = pd.DataFrame(data)

        return df

    except FileNotFoundError:
        logging.error(f"Error: The file '{file_path}' was not found.")
    except json.JSONDecodeError as jde:
        logging.error(f"Error decoding JSON: {jde}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")


def construct_api_url(base_url: str, endpoint: str) -> str:
    """
    Constructs the full API URL by combining the base URL and endpoint.

    Parameters:
    - base_url (str): The base URL of the API.
    - endpoint (str): The specific API endpoint path.

    Returns:
    - str: The full API URL.
    """
    if not base_url.endswith('/'):
        base_url += '/'
    if endpoint.startswith('/'):
        endpoint = endpoint[1:]
    url = base_url + endpoint
    return url


def fetch_api_data(url: str, username: str, password: str, params=None, verify_ssl=True) -> list:
    """
    Fetches data from the specified API URL using Basic Authentication.

    Parameters:
    - url (str): The full API URL.
    - username (str): Username for Basic Authentication.
    - password (str): Password for Basic Authentication.
    - params (dict): Query parameters as key-value pairs.
    - verify_ssl (bool): Whether to verify SSL certificates.

    Returns:
    - dict: Parsed JSON response from the API.

    Raises:
    - requests.exceptions.RequestException: For network-related errors.
    - ValueError: If JSON decoding fails.
    - KeyError: If expected keys are missing in the response.
    """
    all_items: List[dict] = []
    offset: int = 0

    # Initialize params dictionary if None
    if params is None:
        params: dict = {}

    while True:
        # Update offset in parameters
        params['offset'] = offset
        params['limit'] = 500

        try:
            response: requests.Response = requests.get(
                url,
                auth=HTTPBasicAuth(username, password),
                params=params,
                # verify=verify_ssl
            )
            response.raise_for_status()

            data: dict = response.json()

            # Extract items from current response
            if 'items' in data:
                all_items.extend(data['items'])

            # Check if there are more items to fetch
            if not data.get('hasMore', False):
                break

            # Increment offset for next request
            offset += 500

        except requests.exceptions.RequestException as e:
            logging.error(f"Error making request: {e}")
            raise
        except ValueError as e:
            logging.error(f"Error parsing JSON response: {e}")
            raise
        except KeyError as e:
            logging.error(f"Missing expected key in response: {e}")
            raise

    return all_items


def save_dataframe_to_duckdb(df: pd.DataFrame, db_path: str, table_name: str = 'ledgers', if_exists: str = 'replace'):
    """
    Saves the pandas DataFrame to a DuckDB table.

    Parameters:
    - df (pd.DataFrame): The DataFrame to save.
    - db_path (str): Path to the DuckDB database file.
    - table_name (str): Name of the table to create or replace.
    - if_exists (str): Behavior when the table exists ('fail', 'replace', 'append').
    """
    try:
        # Connect to DuckDB (creates the database file if it doesn't exist)
        con: duckdb.DuckDBPyConnection = duckdb.connect(database=Path.cwd() / db_path, read_only=False)

        # Register the DataFrame as a DuckDB relation named 'temp_df'
        con.register('temp_df', df)

        if if_exists == 'replace':
            # This will drop the table if it exists and create a new one
            create_table_query: str = f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT * FROM temp_df
            """
            con.execute(create_table_query)
        elif if_exists == 'append':
            # Append data to the existing table
            append_table_query: str = f"""
                INSERT INTO {table_name}
                SELECT * FROM temp_df
            """
            con.execute(append_table_query)
        elif if_exists == 'fail':
            # Attempt to create the table and fail if it exists
            try:
                create_table_query: str = f"""
                    CREATE TABLE {table_name} AS
                    SELECT * FROM temp_df
                """
                con.execute(create_table_query)
            except duckdb.CatalogException:
                raise ValueError(f"Table '{table_name}' already exists.")
        else:
            raise ValueError("if_exists parameter must be one of 'fail', 'replace', or 'append'.")

        logging.info(f"Data successfully written to DuckDB table '{table_name}' in database '{db_path}'.")

        # Unregister the temporary DataFrame to clean up
        con.unregister('temp_df')

        # Close the connection
        con.close()

    except Exception as e:
        logging.error(f"Failed to write DataFrame to DuckDB: {e}")


def query_duckdb(db_path, query):
    """
    Executes a SQL query on the specified DuckDB database.

    Parameters:
    - db_path (str): Path to the DuckDB database file.
    - query (str): The SQL query to execute.

    Returns:
    - pd.DataFrame: The result of the SQL query.
    """
    try:
        con = duckdb.connect(database=db_path, read_only=True)
        result = con.execute(query).fetchdf()
        con.close()
        return result
    except Exception as e:
        print(f"Failed to execute query on DuckDB: {e}")
        return None

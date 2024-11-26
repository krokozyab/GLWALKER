import logging

import pandas as pd

from packages.endpoints import segments_endpoint, segments_query_params, ledgers_endpoint, ledgers_query_params, \
    ledgers_table, periods_endpoint, periods_query_params, currencies_endpoint, currencies_query_params
from packages.persist_metadata import construct_api_url, fetch_api_data, save_dataframe_to_duckdb


def load_metadata(df: pd.DataFrame, base_api_url: str, username: str, password: str, duckdb_db_path: str):
    """"
    Load the metadata from the API into DuckDB
    """
    # Create a new np array with unique values
    flex_segments: np.ndarray = df['SEGMENT_NAME'].unique()
    for segment in flex_segments:
        # Construct the full API URL
        segment_api_url: str = construct_api_url(base_api_url, segments_endpoint) + segment + '/child/values'
        # Fetch the ledger data
        segment_list: list[str] = fetch_api_data(segment_api_url, username, password,
                                                 segments_query_params)  # , verify_ssl=verify_ssl)
        if segment_list:
            # Load the items into a pandas DataFrame
            segment_df: pd.DataFrame = pd.DataFrame(segment_list)
            # Write the DataFrame to DuckDB
            save_dataframe_to_duckdb(segment_df, duckdb_db_path, table_name=segment, if_exists='replace')

    # Construct the full API URL ledgers
    ledgers_api_url: str = construct_api_url(base_api_url, ledgers_endpoint)

    # Fetch the ledger data
    ledgers_list: list[str] = fetch_api_data(ledgers_api_url, username, password,
                                             ledgers_query_params)  # , verify_ssl=verify_ssl)

    if ledgers_list:
        # Load the items into a pandas DataFrame
        df: pd.DataFrame = pd.DataFrame(ledgers_list)
        # Write the DataFrame to DuckDB
        save_dataframe_to_duckdb(df, duckdb_db_path, table_name=ledgers_table, if_exists='replace')
        logging.info('Ledgers loaded into DuckDB')
    periods_api_url: str = construct_api_url(base_api_url, periods_endpoint)
    periods_list: list[str] = fetch_api_data(periods_api_url, username, password, periods_query_params)
    if periods_list:
        df = pd.DataFrame(periods_list)
        save_dataframe_to_duckdb(df, duckdb_db_path, table_name='accounting_periods', if_exists='replace')
        logging.info('Accounting periods loaded into DuckDB')
    currencies_api_url: str = construct_api_url(base_api_url, currencies_endpoint)
    currencies_list: list[str] = fetch_api_data(currencies_api_url, username, password, currencies_query_params)
    if currencies_list:
        df: pd.DataFrame = pd.DataFrame(currencies_list)
        save_dataframe_to_duckdb(df, duckdb_db_path, table_name='currencies', if_exists='replace')
        logging.info('Currencies loaded into DuckDB')
    logging.info('Metadata loaded into DuckDB')
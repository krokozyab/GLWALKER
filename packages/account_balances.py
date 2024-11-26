import pandas as pd

from packages.endpoints import balances_query_params, periods_query_params, periods_endpoint, balances_endpoint
from packages.load_env_vars import load_environment_variables, get_env_variable
from packages.persist_metadata import fetch_api_data, construct_api_url, save_dataframe_to_duckdb


def construct_params(p_combination: str, p_accounting_period: str, p_currency: str, p_ledger_name: str, p_mode: str,
                     p_currency_type: str) -> dict:
    """
    Constructs the parameters dictionary for the API request.

    Parameters:
    - combination (str): The account combination.
    - accounting_period (str): The accounting period.
    - currency (str): The currency.
    - ledger_name (str): The ledger name.
    - mode (str): The mode.
    - currency_type (str): The currency type.

    Returns:
    - dict: The constructed parameters' dictionary.
    """

    # Step 1: Parse the 'finder' string
    finder_str = balances_query_params['finder']
    finder_parts = finder_str.split(';', 1)  # Split only at the first semicolon

    # Extract finder name and parameters string
    finder_name = finder_parts[0]  # 'AccountBalanceFinder'
    finder_params_str = finder_parts[1] if len(finder_parts) > 1 else ''

    # Step 2: Convert parameters into a dictionary
    finder_params_list = finder_params_str.split(',')

    finder_params = {}
    for param in finder_params_list:
        key_value = param.split('=', 1)
        if len(key_value) == 2:
            key, value = key_value
            finder_params[key] = value
        else:
            finder_params[key_value[0]] = ''

    # Step 3: Update the 'accountingPeriod' parameter
    finder_params['accountCombination'] = p_combination
    finder_params['accountingPeriod'] = p_accounting_period
    finder_params['currency'] = p_currency
    finder_params['ledgerName'] = p_ledger_name
    finder_params['mode'] = p_mode
    finder_params['currencyType'] = p_currency_type

    # Step 4: Reassemble the 'finder' string
    finder_params_str_updated = ','.join(f"{key}={value}" for key, value in finder_params.items())
    finder_str_updated = f"{finder_name};{finder_params_str_updated}"

    # Step 5: Update the original dictionary
    balances_query_params['finder'] = finder_str_updated
    return balances_query_params


""""
# test
print(construct_params('101.%.%.%.%.33.%.%', 'Jun-23', 'EUR', 'US Primary Ledger', 'Detail', 'Total'))

load_environment_variables()

# Retrieve variables from environment
base_api_url = get_env_variable('BASE_API_URL')
username = get_env_variable('ORACLE_FUSION_USERNAME')
password = get_env_variable('ORACLE_FUSION_PASSWORD')
verify_ssl = get_env_variable('VERIFY_SSL', required=False)
duckdb_db_path = get_env_variable('DUCKDB_DB_PATH', required=False) or 'ledgers.duckdb'

balances_api_url: str = construct_api_url(base_api_url, balances_endpoint)
balances_list: list = fetch_api_data(balances_api_url, username, password, construct_params('239.%.%.%.%.%.%.%', 'Jun-24', 'USD', 'US Primary Ledger', 'Detail', 'Total'))
if balances_list:
    df = pd.DataFrame(balances_list)
    save_dataframe_to_duckdb(df, duckdb_db_path, table_name='balances_test', if_exists='replace')
"""
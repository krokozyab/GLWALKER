import pandas as pd

from packages.load_env_vars import get_env_variable, load_environment_variables
from packages.persist_metadata import load_lg_list_to_dataframe

# Loads environment variables from a .env file
load_environment_variables()

base_api_url: str = get_env_variable('BASE_API_URL')
username: str = get_env_variable('ORACLE_FUSION_USERNAME')
password: str = get_env_variable('ORACLE_FUSION_PASSWORD')
verify_ssl = get_env_variable('VERIFY_SSL', required=False)
duckdb_db_path: str = get_env_variable('DUCKDB_DB_PATH', required=False) or 'ledgers.duckdb'

# Load json with ledgers definitions
l_file_path: str = 'lg_list.json'  # Replace with your file path if different
ldf: pd.DataFrame = load_lg_list_to_dataframe(l_file_path)
ldf: pd.DataFrame = ldf.sort_values(by=['ledger_id', 'SEGMENT_NUMBER'], inplace=False)

import itertools
import logging
from itertools import chain

import pandas as pd
import duckdb
from pathlib import Path

from packages.account_balances import construct_params
from packages.config import base_api_url, username, password, duckdb_db_path
from packages.endpoints import balances_endpoint
from packages.persist_metadata import construct_api_url, fetch_api_data


def generate_combinations(values: list, ids: list, ledger_id: int, xldf: pd.DataFrame) -> list:
    """
    :param xldf:
    :param ledger_id:
    :param values:
    :param ids:
    :return:
    """
    predefined_order: list = xldf[xldf['ledger_id'] == ledger_id]["VALUE_SET_NAME"].tolist()
    # Define the predefined order of indices
    # Create a mapping from index to values
    index_to_values: dict = {}
    for dropdown_id, value in zip(ids, values):
        index = dropdown_id['index']
        if value is None or not value:  # Checks if value is None or if value is an empty list
            index_to_values[index] = ['%']
        else:
            index_to_values[index] = value  # value is already a list
    # Arrange values in the predefined order
    positions_values = []
    for index in predefined_order:
        values_list = index_to_values.get(index, ['%'])  # Default to '%' if index not found
        positions_values.append(values_list)
    # Generate all combinations
    combinations = list(itertools.product(*positions_values))
    # Format combinations into strings
    combinations_strings = ['.'.join(comb) for comb in combinations]
    return combinations_strings

def get_periods_list(p_ledger_id: str, p_period_from: str, p_period_to: str, p_df_ledgers: pd.DataFrame) -> list:
    # Fetch ledger accountedperiodtype based on selected ID
    # v_accountedperiodtype: str = df_ledgers[df_ledgers['LedgerId'] == p_ledger_id].iloc[0]['AccountedPeriodType']
    v_accountedperiodtype: str = p_df_ledgers[p_df_ledgers['LedgerId'] == p_ledger_id].iloc[0]['AccountedPeriodType']

    # load ledgers and currencies from db
    v_con: duckdb.DuckDBPyConnection = duckdb.connect(database=Path.cwd() / duckdb_db_path, read_only=False)
    try:
        query = """
                SELECT PeriodNameId 
                FROM accounting_periods
                WHERE StartDate >= (SELECT StartDate FROM accounting_periods WHERE PeriodNameId=?)
                AND EndDate <= (SELECT EndDate FROM accounting_periods WHERE PeriodNameId=?)
                AND PeriodType = ?
                ORDER BY StartDate
            """
        v_periods: list = v_con.execute(query, [p_period_from, p_period_to, v_accountedperiodtype]).fetchnumpy()[
            'PeriodNameId'].tolist()
        return v_periods
    finally:
        v_con.close()

def prepare_df(p_df_ledgers, p_ledger_id, p_values, p_ids, p_ldf, p_period_from, p_period_to, p_balance_type,
               p_from_currency, p_currency, p_flex_mode) -> pd.DataFrame:
    all_balances = []
    # Fetch ledger name based on selected ID
    xdf_ledgers = pd.DataFrame(p_df_ledgers)
    ledger_name: str = xdf_ledgers[xdf_ledgers['LedgerId'] == p_ledger_id].iloc[0]['Name']
    # Generate ac flex combinations
    combinations_strings = generate_combinations(p_values, p_ids, p_ledger_id, pd.DataFrame(p_ldf))
    logging.info(combinations_strings)
    # Fetch periods list
    periods_list: list = get_periods_list(p_ledger_id, p_period_from, p_period_to, xdf_ledgers)
    if p_balance_type == 'From':
        balance_type = f'From {p_from_currency}'
    else:
        balance_type = p_balance_type
    logging.info(balance_type)
    for period in periods_list:
        for combination in combinations_strings:
            balances_api_url: str = construct_api_url(base_api_url, balances_endpoint)
            balances_list: list = fetch_api_data(balances_api_url, username, password,
                                                 construct_params(combination, period, p_currency,
                                                                  ledger_name, p_flex_mode, balance_type))
            all_balances.append(balances_list)
    flattened_balances: list = list(chain.from_iterable(all_balances))
    if all_balances and all_balances != [[]]:
        df = pd.DataFrame(flattened_balances)
        if p_flex_mode == 'Detail':
            # Extract value_set_description ordered by segment_number for the specific ledger_id
            xldf = pd.DataFrame(p_ldf)
            column_names = xldf[xldf['ledger_id'] == p_ledger_id].sort_values(by='SEGMENT_NUMBER')[
                'VALUE_SET_DESCRIPTION'].tolist()
            split_columns = df['DetailAccountCombination'].str.split('.', expand=True)
            # Assign the extracted column names
            split_columns.columns = column_names
            df = pd.concat([df, split_columns], axis=1)
            # Now reorder the columns to put split columns right after DetailAccountCombination
            # Get all column names
            all_columns = df.columns.tolist()
            # Remove the new columns from the list
            for col in column_names:
                all_columns.remove(col)
            # Find the index of DetailAccountCombination
            detail_acc_idx = all_columns.index('DetailAccountCombination')
            # Create new column order
            new_column_order = all_columns[:(detail_acc_idx + 1)] + column_names + all_columns[
                                                                                   (detail_acc_idx + 1):]
            # Reindex the dataframe with the new column order
            df = df[new_column_order]
            df[['PeriodActivity', 'BeginningBalance', 'EndingBalance']] = df[
                ['PeriodActivity', 'BeginningBalance', 'EndingBalance']].apply(pd.to_numeric, errors='coerce')
        return df

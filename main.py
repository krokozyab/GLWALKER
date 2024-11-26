import itertools
from pathlib import Path

import dash_dangerously_set_inner_html
import pandas as pd

from packages.account_balances import construct_params
from packages.endpoints import ledgers_endpoint, ledgers_query_params, ledgers_table, periods_endpoint, \
    periods_query_params, segments_endpoint, segments_query_params, balances_endpoint, currencies_endpoint, \
    currencies_query_params
from packages.load_env_vars import load_environment_variables, get_env_variable
from packages.load_metadata import load_metadata
from packages.persist_metadata import construct_api_url, fetch_api_data, \
    save_dataframe_to_duckdb, load_lg_list_to_dataframe

import numpy as np
import duckdb

import dash
from dash import dcc, html, Patch, dash_table
from dash.dependencies import Input, Output, State, ALL
from dash.exceptions import PreventUpdate
import logging
from itertools import chain
import dash_ag_grid as dag
import dash_bootstrap_components as dbc
import pygwalker as pyg

# Configure logging to output to console with level INFO
logging.basicConfig(level=logging.INFO)

load_environment_variables()

# Retrieve variables from environment
base_api_url: str = get_env_variable('BASE_API_URL')
username: str = get_env_variable('ORACLE_FUSION_USERNAME')
password: str = get_env_variable('ORACLE_FUSION_PASSWORD')
verify_ssl = get_env_variable('VERIFY_SSL', required=False)
duckdb_db_path: str = get_env_variable('DUCKDB_DB_PATH', required=False) or 'ledgers.duckdb'

# Load json with ledgers definitions
l_file_path: str = 'lg_list.json'  # Replace with your file path if different
ldf: pd.DataFrame = load_lg_list_to_dataframe(l_file_path)
ldf: pd.DataFrame = ldf.sort_values(by=['ledger_id', 'SEGMENT_NUMBER'], inplace=False)

# todo
load_metadata(ldf, base_api_url, username, password, duckdb_db_path)

# load ledgers and currencies from db
con: duckdb.DuckDBPyConnection = duckdb.connect(database=Path.cwd() / duckdb_db_path, read_only=False)
try:
    df_ledgers = con.execute(
        "SELECT lg.LedgerId, lg.Name, lg.CurrencyCode, lg.accountedperiodtype FROM ledgers lg where lg.LedgerId in (select distinct ledger_id from ldf)").fetchdf()
    df_currencies = con.execute("SELECT CurrencyCode, Name FROM currencies").fetchdf()
finally:
    con.close()

# ------------------------------------------------------
# Initialize the Dash app
dbc_css = "https://cdn.jsdelivr.net/gh/AnnMarieW/dash-bootstrap-templates/dbc.min.css"
app = dash.Dash(external_stylesheets=[dbc.themes.BOOTSTRAP, dbc_css])

app.title = "Ledger Selector"

# Define the layout
app.layout = dbc.Container([

    dbc.Row([dbc.Col(html.H2("Ledger Selection Dashboard"), width=12)]),

    dbc.Row([
        dbc.Col([
            dbc.Label("Ledger:"),
            dcc.Dropdown(
                id='ledger-dropdown',
                options=[
                    {'label': name, 'value': ledger_id}
                    for name, ledger_id in zip(df_ledgers['Name'], df_ledgers['LedgerId'])
                ],
                placeholder="Select a ledger",
                persistence=True,  # Enable persistence for the dropdown value
                persistence_type='memory',  # Store the value in session storage
                value=None  # Default value; can set to a specific ledger_id if desired
            )], width=2),
        dbc.Col([
            dbc.Label("Currency:"),
            dcc.Dropdown(
                id='currency-dropdown',
                options=[
                    {'label': currency_code + ' | ' + name, 'value': currency_code}
                    for name, currency_code in zip(df_currencies['Name'], df_currencies['CurrencyCode'])
                ],
                placeholder="Select a currency",
                persistence=True,  # Enable persistence for the dropdown value
                persistence_type='memory',  # Store the value in session storage
                value=None  # Default value; can set to a specific ledger_id if desired

            )], width=2),

        dbc.Col([
            dbc.Label("Currency type:"),
            dcc.RadioItems(id='balance-type', options=['Total', 'Entered', 'From'], value='Total', persistence=True,
                           persistence_type='memory')], width=2, style={'width': '130px'}),
        dbc.Col([
            dbc.Label("From currency:"),
            dbc.Label(" "),
            dbc.Label(" "),
            dcc.Dropdown(
                id='from-currency-dropdown',
                options=[
                    {'label': currency_code + ' | ' + name, 'value': currency_code}
                    for name, currency_code in zip(df_currencies['Name'], df_currencies['CurrencyCode'])
                ],
                placeholder="Select a from currency",
                persistence=True,  # Enable persistence for the dropdown value
                persistence_type='memory',  # Store the value in session storage
                value=None  # Default value; can set to a specific ledger_id if desired

            )], width=2),

        dbc.Col([
            dbc.Label("Balance type:"),
            dcc.RadioItems(id='flex_mode', options=['Detail', 'Summary'], value='Detail', persistence=True,
                           persistence_type='memory')], width=2, align="start"),
        dbc.Col([
            dbc.Label("Periods range:"),
            html.Div([
                dcc.Dropdown(
                    id='period-from-dropdown',
                    options=[],
                    placeholder="Select a period from",
                    persistence=True,  # Enable persistence for the dropdown value
                    persistence_type='memory',  # Store the value in session storage
                    value=None
                ),
                dcc.Dropdown(
                    id='period-to-dropdown',
                    options=[],
                    placeholder="Select a period to",
                    persistence=True,  # Enable persistence for the dropdown value
                    persistence_type='memory',  # Store the value in session storage
                    value=None
                )], id="periods_container")], width=2, align="start"),
    ]),
    html.Hr(style={'borderTop': '1px solid #ccc', 'margin': '20px 0'}),

    dbc.Row([
        dbc.Col([
            html.Div(id='flex_from_dropdown')
        ], width=4),
        dbc.Col([
            dbc.Button("Table", id="list_flex_btn", n_clicks=0)
        ], width=2),
        dbc.Col([
            dbc.Button("Pyg", id="pyg_flex_btn", n_clicks=0)
        ], width=2)

    ]),

    dbc.Row([
        dbc.Col([
            html.Div(id="data_table_div")
        ], width=12)
    ]),
    dbc.Row([
        dbc.Col([
            html.Div(id="pygwalker_div")
        ], width=12)
    ]),

    # Store component to hold the selected ledger_id in session storage
    dcc.Store(id='ledger-store', storage_type='memory', data=[]),
    # Store component to hold the selected periods in session storage
    dcc.Store(id='periods-store', storage_type='memory', data=[]),
    # Store to hold ldf DataFrame
    dcc.Store(id='ldf-store', data=[]),  # ldf.to_dict('records')
    # Store to hold df_ledgers DataFrame
    dcc.Store(id='df_ledgers-store', data=df_ledgers.to_dict('records')),
    html.Div(id='dummy-div'),  # A div that triggered callback at page load
])


# Callback to load metadata on page load
@app.callback(
    Output('ldf-store', 'data'),
    Input('dummy-div', 'children')  # This triggers the callback upon page load
)
def load_data_on_page_load(_):
    # Load json with ledgers definitions
    v_l_file_path: str = 'lg_list.json'  # Replace with your file path if different
    v_ldf: pd.DataFrame = load_lg_list_to_dataframe(v_l_file_path)
    v_ldf: pd.DataFrame = v_ldf.sort_values(by=['ledger_id', 'SEGMENT_NUMBER'], inplace=False)
    # Convert DataFrame to dictionary to store in dcc.Store
    v_ldf_dict = v_ldf.to_dict('records')
    logging.info(f"Config file: {v_l_file_path}' loaded.")
    logging.info(v_ldf.iloc[0].to_dict())
    # load ledgers and currencies from db
    v_con: duckdb.DuckDBPyConnection = duckdb.connect(database=Path.cwd() / duckdb_db_path, read_only=False)
    try:
        v_df_ledgers = v_con.execute(
            "SELECT lg.LedgerId, lg.Name, lg.CurrencyCode, lg.accountedperiodtype FROM ledgers lg where lg.LedgerId in (select distinct ledger_id from v_ldf)").fetchdf()
        v_df_currencies = v_con.execute("SELECT CurrencyCode, Name FROM currencies").fetchdf()
    finally:
        v_con.close()
    return v_ldf_dict


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


def get_periods_list(p_ledger_id: str, p_perod_fom: str, p_period_to: str) -> list:
    # Fetch ledger accountedperiodtype based on selected ID
    accountedperiodtype: str = df_ledgers[df_ledgers['LedgerId'] == p_ledger_id].iloc[0]['AccountedPeriodType']

    # load ledgers and currencies from db
    con: duckdb.DuckDBPyConnection = duckdb.connect(database=Path.cwd() / duckdb_db_path, read_only=False)
    try:
        query = """
                SELECT PeriodNameId 
                FROM accounting_periods
                WHERE StartDate >= (SELECT StartDate FROM accounting_periods WHERE PeriodNameId=?)
                AND EndDate <= (SELECT EndDate FROM accounting_periods WHERE PeriodNameId=?)
                AND PeriodType = ?
                ORDER BY StartDate
            """
        periods: list = con.execute(query, [p_perod_fom, p_period_to, accountedperiodtype]).fetchnumpy()[
            'PeriodNameId'].tolist()
        return periods
    finally:
        con.close()


@app.callback(
    # Output('flex_params_div', 'children'),
    Output('data_table_div', 'children'),
    Input("list_flex_btn", "n_clicks"),
    State({"type": "flex-dynamic-dropdown", "index": ALL}, "value"),
    State({"type": "flex-dynamic-dropdown", "index": ALL}, "id"),
    State('ledger-dropdown', 'value'),
    State('period-from-dropdown', 'value'),
    State('period-to-dropdown', 'value'),
    State('flex_mode', 'value'),
    State('currency-dropdown', 'value'),
    State('balance-type', 'value'),
    State('from-currency-dropdown', 'value'),
    State('ldf-store', 'data'),
    State('df_ledgers-store', 'data'),
    prevent_initial_call=True
)
def display_flex_values(n_clicks, values, ids, ledger_id, p_period_from, p_period_to, p_flex_mode, p_currency,
                        p_balance_type, p_from_currency, p_ldf, p_df_ledgers):
    """
    Shows datatable on button click
    :param p_ldf:
    :param p_df_ledgers:
    :param p_from_currency:
    :param p_balance_type:
    :param p_currency:
    :param p_flex_mode:
    :param p_period_to:
    :param p_period_from:
    :param ledger_id:
    :param n_clicks:
    :param values:
    :param ids:
    :return:
    """
    if n_clicks is None or n_clicks == 0:
        return "Click the button to list chosen values."

    if not values or not ids:
        return "No values selected."
    all_balances = []
    # Fetch ledger name based on selected ID
    xdf_ledgers = pd.DataFrame(p_df_ledgers)
    ledger_name: str = xdf_ledgers[xdf_ledgers['LedgerId'] == ledger_id].iloc[0]['Name']
    # Generate ac flex combinations
    combinations_strings = generate_combinations(values, ids, ledger_id, pd.DataFrame(p_ldf))
    logging.info(combinations_strings)
    # Fetch periods list
    periods_list: list = get_periods_list(ledger_id, p_period_from, p_period_to)

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

    patched_children = Patch()
    patched_children.clear()  # remove previous selections

    flattened_balances: list = list(chain.from_iterable(all_balances))
    if all_balances and all_balances != [[]]:
        df = pd.DataFrame(flattened_balances)
        if p_flex_mode == 'Detail':
            # First, extract the index values from the ids parameter to use as column names
            # column_names = [item['index'] for item in ids]

            # Extract value_set_description ordered by segment_number for the specific ledger_id
            xldf = pd.DataFrame(p_ldf)
            column_names = xldf[xldf['ledger_id'] == ledger_id].sort_values(by='SEGMENT_NUMBER')[
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
        new_element = html.Div([

            dag.AgGrid(
                id="main-table",
                rowData=df.to_dict("records"),
                columnDefs=[{"field": i} for i in df.columns],
                className="ag-theme-alpine-light"
            ),
            dcc.Loading(
                id="loading-1",
                type="default"
            )

        ])
        patched_children.append(new_element)

    # Initialize a list to hold the Divs
    content = []
    # print(list(zip(ids, values)))
    # Iterate over the ids and values simultaneously
    """for dropdown_id, value in zip(ids, values):
        index = dropdown_id['index']  # Extract the 'index' from the ID dict

        # Handle cases where value might be a list (multi=True)
        if isinstance(value, list):
            value_str = ', '.join([str(v) for v in value if v is not None])
        elif value is not None:
            value_str = str(value)
        else:
            value_str = "No selection"
    """
    # Create a Div for each dropdown's key and value
    # content.append(
    #    html.Div(f"Dropdown {index}: Selected values: {value_str}")
    # )

    # Return a parent Div containing all individual Divs
    # return html.Div(content), patched_children
    return patched_children


@app.callback(
    # Output('flex_params_div', 'children'),
    Output('pygwalker_div', 'children'),
    Input("pyg_flex_btn", "n_clicks"),
    State({"type": "flex-dynamic-dropdown", "index": ALL}, "value"),
    State({"type": "flex-dynamic-dropdown", "index": ALL}, "id"),
    State('ledger-dropdown', 'value'),
    State('period-from-dropdown', 'value'),
    State('period-to-dropdown', 'value'),
    State('flex_mode', 'value'),
    State('currency-dropdown', 'value'),
    State('balance-type', 'value'),
    State('from-currency-dropdown', 'value'),
    State('ldf-store', 'data'),
    State('df_ledgers-store', 'data'),
    prevent_initial_call=True
)
def display_pyg_values(n_clicks, values, ids, ledger_id, p_period_from, p_period_to, p_flex_mode, p_currency,
                       p_balance_type, p_from_currency, p_ldf, p_df_ledgers):
    """
    Shows pygwalker on button click
    :param p_df_ledgers:
    :param p_ldf:
    :param p_from_currency:
    :param p_balance_type:
    :param p_currency:
    :param p_flex_mode:
    :param p_period_to:
    :param p_period_from:
    :param ledger_id:
    :param n_clicks:
    :param values:
    :param ids:
    :return:
    """
    if n_clicks is None or n_clicks == 0:
        return "Click the button to list chosen values."

    if not values or not ids:
        return "No values selected."

    all_balances = []
    # Fetch ledger name based on selected ID
    # ledger_name: str = df_ledgers[df_ledgers['LedgerId'] == ledger_id].iloc[0]['Name']
    # Fetch ledger name based on selected ID
    xdf_ledgers = pd.DataFrame(p_df_ledgers)
    ledger_name: str = xdf_ledgers[xdf_ledgers['LedgerId'] == ledger_id].iloc[0]['Name']

    combinations_strings = generate_combinations(values, ids, ledger_id, pd.DataFrame(p_ldf))
    logging.info(combinations_strings)

    # Fetch periods list
    periods_list: list = get_periods_list(ledger_id, p_period_from, p_period_to)

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

    patched_children = Patch()
    patched_children.clear()  # remove previous selections

    if all_balances and all_balances != [[]]:
        df = pd.DataFrame(flattened_balances)
        if p_flex_mode == 'Detail':
            # First, extract the index values from the ids parameter to use as column names
            # column_names = [item['index'] for item in ids]

            # column_names = ldf[ldf['ledger_id'] == ledger_id].sort_values(by='SEGMENT_NUMBER')['VALUE_SET_DESCRIPTION'].tolist()
            # Extract value_set_description ordered by segment_number for the specific ledger_id
            xldf = pd.DataFrame(p_ldf)
            column_names = xldf[xldf['ledger_id'] == ledger_id].sort_values(by='SEGMENT_NUMBER')[
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
            # df['PeriodActivity'] = pd.to_numeric(df['PeriodActivity'])  # Converts to float

            df[['PeriodActivity', 'BeginningBalance', 'EndingBalance']] = df[
                ['PeriodActivity', 'BeginningBalance', 'EndingBalance']].apply(pd.to_numeric, errors='coerce')

        # html_code = pyg.walk(df,  use_kernel_calc=True, return_html=True).to_html()
        html_code = pyg.walk(df, return_html=True).to_html()

        new_element = html.Div([
            dash_dangerously_set_inner_html.DangerouslySetInnerHTML(html_code)
        ])

        patched_children.append(new_element)

    # Initialize a list to hold the Divs
    content = []
    return patched_children


# Define callback to update output based on ledger selection
@app.callback(
    Output('flex_from_dropdown', 'children'),
    Output('currency-dropdown', 'value'),
    Input('ledger-dropdown', 'value'),
    State('flex_from_dropdown', 'children'),
    State('ldf-store', 'data'),
    prevent_initial_call=True
)
def update_output(selected_ledger_id, flex_from_dropdown, p_ldf):
    global currency_code
    if selected_ledger_id is None:
        return None, None  # "No ledger selected."

    # Fetch ledger details based on selected ID
    # ledger = df_ledgers[df_ledgers['LedgerId'] == selected_ledger_id].iloc[0]

    patched_children = Patch()
    patched_children.clear()  # remove previous selections

    xldf = pd.DataFrame(p_ldf)
    ledger_df = xldf[xldf['ledger_id'] == selected_ledger_id]
    for index, row in ledger_df.iterrows():
        vs_vals = get_flex_values(row['VALUE_SET_NAME'])
        new_element = html.Div([
            dcc.Dropdown(
                options=vs_vals,
                id={
                    'type': 'flex-dynamic-dropdown',
                    'index': row['SEGMENT_NAME']
                },
                placeholder=row['VALUE_SET_DESCRIPTION'],
                multi=True,
                value='%',
                persistence=True,
                persistence_type='memory'
            )
        ])
        patched_children.append(new_element)

        # Fetch ledger details based on selected ID
        currency_code = df_ledgers[df_ledgers['LedgerId'] == selected_ledger_id].iloc[0]['CurrencyCode']
        # currency_code:str = (ledger['CurrencyCode'])
    return patched_children, currency_code


# Define callback to update ledger_id storage and enable currency dropdown
@app.callback(
    Output('ledger-store', 'data'),
    [Input('ledger-dropdown', 'value')]
)
def update_ledger_storage(selected_ledger_id):
    if selected_ledger_id is None:
        raise PreventUpdate
    return selected_ledger_id


@app.callback(
    Output('periods-store', 'data'),
    [Input('ledger-store', 'data')],
    prevent_initial_call=True
)
def get_periods(ledger_store_data):
    """
    Retrieves periods associated with the selected ledger_id and stores them in 'periods-store'.

    Parameters:
    - ledger_store_data: The data stored in 'ledger-store', expected to be a single ledger_id.

    Returns:
    - A list of dictionaries formatted for Dropdown options, e.g., [{'label': 'Period1', 'value': 'Period1'}, ...]
    """
    if not ledger_store_data:
        raise PreventUpdate  # No update if ledger_store_data is empty or None

    # Connect to DuckDB
    try:
        v_con = duckdb.connect(database=Path.cwd() / duckdb_db_path, read_only=False)
    except Exception as e:
        # Handle connection errors
        logging.error(f"Error connecting to DuckDB: {e}' was not found.")
        raise PreventUpdate

    try:
        # Execute the query with parameter substitution to prevent SQL injection
        query = '''
            SELECT ap.periodnameid AS period 
            FROM accounting_periods ap
            JOIN ledgers lg 
                ON (ap.periodsetnameid = lg.periodsetname AND ap.periodtype = lg.accountedperiodtype)
            WHERE lg.ledgerid = ?
            ORDER BY ap.periodyear DESC, ap.periodnumber DESC
        '''
        df_periods = v_con.execute(query, [ledger_store_data]).fetchdf()
    except Exception as e:
        # Handle query execution errors
        logging.error(f"Error executing query: {e}' was not found.")
        v_con.close()
        raise PreventUpdate
    finally:
        v_con.close()

    if df_periods.empty:
        return []  # Return empty list if no periods found

    # Convert the DataFrame to a list of options
    # Assuming 'period' is the column name after aliasing in the query
    options = [{'label': row['period'], 'value': row['period']} for _, row in df_periods.iterrows()]
    return options


@app.callback(
    [Output('period-from-dropdown', 'options'),
     Output('period-to-dropdown', 'options')],
    [Input('periods-store', 'data')]
)
def set_period_dropdown_options(periods_data):
    """
    Updates the 'period-from-dropdown' options based on data from 'periods-store'.

    Parameters:
    - periods_data: A list of dictionaries with 'label' and 'value' keys.

    Returns:
    - A list of dictionaries formatted for Dropdown options.
    """
    if not periods_data:
        return [], []
    return periods_data, periods_data


@app.callback(
    Output('period-to-dropdown', 'value'),
    Input('period-from-dropdown', 'value')
)
def set_period_to_value(period_from):
    """
    Sets the 'period-to-dropdown' value based on the 'period-from-dropdown' value.

    Parameters:
    - period_from: The value selected in the 'period-from-dropdown'.

    Returns:
    - The same value as 'period_from'.
    """
    return period_from


def get_flex_values(flex_table: str) -> list:
    """
    Retrieves flex values from the specified table in DuckDB.

    Parameters:
    - flex_table: The name of the flex table to query.

    Returns:
    - A list of dictionaries formatted for Dropdown options, e.g., [{'label': 'FlexValue1', 'value': 'FlexValue1'}, ...]
    """
    # Connect to DuckDB
    try:
        con = duckdb.connect(database=Path.cwd() / duckdb_db_path, read_only=False)
    except Exception as e:
        # Handle connection errors
        logging.error(f"Error connecting to DuckDB: {e}")
        raise PreventUpdate
    try:
        query = f'SELECT VALUE, DESCRIPTION FROM {flex_table}'
        val_df = con.execute(query).fetchdf()
        con.close()

        if val_df.empty:
            return []  # Return empty list if no flex values found

        # Convert the DataFrame to a list of options
        options = [{'label': row['Value'] + ' ' + str(row['Description']), 'value': row['Value']} for _, row in
                   val_df.iterrows()]
        return options
    except Exception as e:
        logging.error(f"Error retrieving flex values: {e}")
        return []
    finally:
        con.close()


if __name__ == "__main__":
    app.run_server(debug=False)

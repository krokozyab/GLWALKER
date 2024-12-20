from packages.load_metadata import load_metadata
from packages.prepare_df import prepare_df
import pandas as pd
from packages.config import duckdb_db_path, base_api_url, username, password, ldf
from packages.duck_select import execute_sql_query
from packages.persist_metadata import load_lg_list_to_dataframe
import dash
from dash import dcc, html, Patch
from dash.dependencies import Input, Output, State, ALL
from dash.exceptions import PreventUpdate
import dash_dangerously_set_inner_html
import logging
import dash_ag_grid as dag
import dash_bootstrap_components as dbc
import pygwalker as pyg

# Configure logging to output to console with level INFO
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# load ledgers and currencies from db
df_ledgers: pd.DataFrame = pd.DataFrame()
df_currencies = execute_sql_query("SELECT CurrencyCode, Name FROM currencies")

if df_currencies is None or df_currencies.empty:  # naively assume the database is broken or empty kinda migration
    load_metadata(ldf, base_api_url, username, password, duckdb_db_path)
    df_currencies = execute_sql_query("SELECT CurrencyCode, Name FROM currencies")

df_ledgers = execute_sql_query(
    "SELECT lg.LedgerId, lg.Name, lg.CurrencyCode, lg.accountedperiodtype FROM ledgers lg where lg.LedgerId "
    "in (select distinct ledger_id from ldf)")

# Initialize the Dash app
dbc_css = "https://cdn.jsdelivr.net/gh/AnnMarieW/dash-bootstrap-templates/dbc.min.css"
app = dash.Dash(external_stylesheets=[dbc.themes.BOOTSTRAP, dbc_css])

app.title = "Ledger Selector"

# Define the layout
app.layout = dbc.Container([

    dbc.Row([dbc.Col(html.H3("GL Walker"), width=12)]),

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
                value=None,  # Default value; can set to a specific ledger_id if desired
            )], width=2),
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
    ]),
    # html.Hr(style={'borderTop': '1px solid #ccc', 'margin': '20px 0'}),

    dbc.Row([
        dbc.Offcanvas(
            dbc.Col([
                html.Div(id='flex_from_dropdown')
            ], width=4),

            id="offcanvas",
            title="Flex Segments",
            is_open=False,
            placement="start",
            style={"width": "40%"}
        ),
        dbc.ButtonGroup([
            # dbc.Col([
            dbc.Button("Enter accounts range", id="acc_flex_btn", n_clicks=0, color="info", disabled=True),
            # ], width=2),

            # dbc.Col([
            dbc.Button("AG Grid Table", id="list_flex_btn", n_clicks=0, style={"marginLeft": "4px"}, disabled=True),
            # ], width=2),
            # dbc.Col([
            dbc.Button("Pygwalker", id="pyg_flex_btn", n_clicks=0, style={"marginLeft": "4px"}, disabled=True),
            # ], width=2)
            html.Div([
                dbc.Button("Load/Refresh Valuesets", id="load_vsets_btn", n_clicks=0, style={"marginLeft": "4px"},
                           color="info"),
                dbc.Spinner(html.Div(id="loading-valuesets_spin")),
            ])
        ], style={"marginTop": "2px"})
    ]),

    dbc.Row([
        dbc.Col([
            dcc.Loading(
                id="loading-main-table",
                type="default",
                children=html.Div(id="data_table_div", style={"marginTop": "2px"}),
                style={
                    "position": "fixed",
                    "top": "50%",
                    "left": "50%",
                    "transform": "translate(-50%, -50%)",
                    "zIndex": 9999  # Ensures the spinner is always above other elements
                }
            ),
        ], width=12)
    ]),
    dbc.Row([
        dbc.Col([
            dcc.Loading(
                id="loading-pygwalker",
                type="default",
                children=html.Div(id="pygwalker_div", style={"marginTop": "2px"}),
                style={
                    "position": "fixed",
                    "top": "50%",
                    "left": "50%",
                    "transform": "translate(-50%, -50%)",
                    "zIndex": 9999  # Ensures the spinner is always above other elements
                }
            )
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


# Callback to load valuesets in local database
@app.callback(
    Output("loading-valuesets_spin", "children"), [Input("load_vsets_btn", "n_clicks")],
    prevent_initial_call=True
)
def load_valuesets(n_clicks: int):
    if n_clicks:
        load_metadata(ldf, base_api_url, username, password, duckdb_db_path)
        return None


# Callback to open the offcanvas
@app.callback(
    Output("offcanvas", "is_open"),
    Input("acc_flex_btn", "n_clicks"),
    [State("offcanvas", "is_open")],
)
def toggle_offcanvas(n1: int, is_open: bool):
    logger.info(f"acc_flex_btn: {n1}")
    if n1:
        return not is_open
    return is_open


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
    logger.info(f"Config file: {v_l_file_path}' loaded.")
    logger.info(v_ldf.iloc[0].to_dict())
    return v_ldf_dict


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
def display_table(n_clicks: int, p_values, p_ids, p_ledger_id, p_period_from, p_period_to, p_flex_mode, p_currency,
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
    :param p_ledger_id:
    :param n_clicks:
    :param p_values:
    :param p_ids:
    :return:
    """
    if n_clicks is None or n_clicks == 0:
        return "Click the button to list chosen values."

    if not p_values or not p_ids:
        return "No values selected."

    patched_children = Patch()
    patched_children.clear()  # remove previous selections

    df: pd.DataFrame = prepare_df(p_df_ledgers, p_ledger_id, p_values, p_ids, p_ldf, p_period_from, p_period_to,
                                  p_balance_type,
                                  p_from_currency, p_currency, p_flex_mode)
    if df is not None and not df.empty:
        new_element: html.Div = html.Div([

            dag.AgGrid(
                id="main-table",
                rowData=df.to_dict("records"),
                columnDefs=[{"field": i, 'filter': True} for i in df.columns],
                className="ag-theme-alpine",
                columnSize="sizeToFit",
                defaultColDef={"editable": False, "resizable": True, "sortable": True, "filter": True, "minWidth": 100},
                dashGridOptions={"pagination": True, "paginationPageSize": 50, "rowHeight": 30, "autoSizePadding": 10,
                                 "groupIncludeFooter": True, "groupIncludeTotalFooter": True},
                style={"height": "400px", "width": "100%"},
                enableEnterpriseModules=True,  # demo only! remove for switch to free version
                licenseKey='you must buy a license for the AG Grid Enterprise version!',  # demo only! remove for switch to free version
            )

        ])
    else:
        new_element: html.Div = html.Div([
            html.P("No data to display.")
        ])
    patched_children.append(new_element)
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
def display_pygwalker(n_clicks: int, p_values, p_ids, p_ledger_id, p_period_from, p_period_to, p_flex_mode, p_currency,
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
    :param p_ledger_id:
    :param n_clicks:
    :param p_values:
    :param p_ids:
    :return:
    """
    if n_clicks is None or n_clicks == 0:
        return "Click the button to list chosen values."

    if not p_values or not p_ids:
        return "No values selected."

    df: pd.DataFrame = prepare_df(p_df_ledgers, p_ledger_id, p_values, p_ids, p_ldf, p_period_from, p_period_to,
                                  p_balance_type,
                                  p_from_currency, p_currency, p_flex_mode)

    patched_children = Patch()
    patched_children.clear()  # remove previous selections
    if df is not None and not df.empty:
        # html_code = pyg.walk(df,  use_kernel_calc=True, return_html=True).to_html()
        html_code = pyg.walk(df, return_html=True).to_html()

        new_element: html.Div = html.Div([
            dash_dangerously_set_inner_html.DangerouslySetInnerHTML(html_code)
        ])
    else:
        new_element: html.Div = html.Div([
            html.P("No data to display.")
        ])

    patched_children.append(new_element)
    return patched_children


# Define callback to update output based on ledger selection, enable buttons
@app.callback(
    Output('flex_from_dropdown', 'children'),
    Output('currency-dropdown', 'value'),
    Output('acc_flex_btn', 'disabled'),
    Output('list_flex_btn', 'disabled'),
    Output('pyg_flex_btn', 'disabled'),
    Input('ledger-dropdown', 'value'),
    State('flex_from_dropdown', 'children'),
    State('ldf-store', 'data'),
    prevent_initial_call=True
)
def update_output(p_selected_ledger_id, flex_from_dropdown, p_ldf):
    v_currency_code: str = ''
    if p_selected_ledger_id is None:
        return None, None  # "No ledger selected."

    # Fetch ledger details based on selected ID
    # ledger = df_ledgers[df_ledgers['LedgerId'] == selected_ledger_id].iloc[0]

    patched_children = Patch()
    patched_children.clear()  # remove previous selections

    v_ldf = pd.DataFrame(p_ldf)
    ledger_df = v_ldf[v_ldf['ledger_id'] == p_selected_ledger_id]

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
                persistence_type='memory',
                style={"width": "560px"}
            )
        ])
        patched_children.append(new_element)

        v_currency_code: str = df_ledgers[df_ledgers['LedgerId'] == p_selected_ledger_id].iloc[0]['CurrencyCode']
    return patched_children, v_currency_code, False, False, False


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

    query = '''
                SELECT ap.periodnameid AS period 
                FROM accounting_periods ap
                JOIN ledgers lg 
                    ON (ap.periodsetnameid = lg.periodsetname AND ap.periodtype = lg.accountedperiodtype)
                WHERE lg.ledgerid = ?
                ORDER BY ap.periodyear DESC, ap.periodnumber DESC
            '''
    df_periods = execute_sql_query(query, [ledger_store_data])

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
    val_df = execute_sql_query(f'SELECT VALUE, DESCRIPTION FROM {flex_table}')

    if val_df.empty:
        return []  # Return empty list if no flex values found

    # Convert the DataFrame to a list of options
    options = [{'label': row['Value'] + ' ' + str(row['Description']), 'value': row['Value']} for _, row in
               val_df.iterrows()]
    return options


if __name__ == "__main__":
    app.run_server(debug=False)

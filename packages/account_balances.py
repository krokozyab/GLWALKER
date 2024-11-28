from packages.endpoints import balances_query_params


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

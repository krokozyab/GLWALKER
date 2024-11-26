ledgers_endpoint = '/fscmRestApi/resources/11.13.18.05/ledgersLOV'
ledgers_query_params: dict = {
    'onlyData': 'true',
    'q': 'LedgerTypeCode="L"',
    'fields': 'AccountedPeriodType,ChartOfAccountsId,Description,EnableBudgetaryControlFlag,LedgerCategoryCode,'
              'LedgerId,Name,PeriodSetName,CurrencyCode'
}
ledgers_table = 'ledgers'

periods_endpoint = '/fscmRestApi/resources/11.13.18.05/accountingPeriodsLOV'
periods_query_params: dict = {
    'onlyData': 'true'
}

segments_endpoint = '/fscmRestApi/resources/11.13.18.05/valueSets/'
segments_query_params: dict = {
    'onlyData': 'true',
    'fields': 'Value,Description,EnabledFlag,StartDateActive,EndDateActive'
}

balances_endpoint = '/fscmRestApi/resources/11.13.18.05/ledgerBalances'
balances_query_params: dict = {
    'onlyData': 'true',
    'fields': 'AccountGroupName,AccountName,LedgerSetName,LedgerName,Currency,CurrentAccountingPeriod,PeriodName,'
              'CurrentPeriodBalance,BudgetBalance,Scenario,AccountCombination,DetailAccountCombination,'
              'BeginningBalance,PeriodActivity,EndingBalance,AmountType,CurrencyType,ErrorDetail',
    'finder': 'AccountBalanceFinder;accountCombination=101.%.%.%.%.%.%.%,accountingPeriod=Dec-23,currency=USD,'
              'ledgerName=US Primary Ledger,mode=Detail,currencyType=Total'
}

currencies_endpoint = '/fscmRestApi/resources/11.13.18.05/currenciesLOV'
currencies_query_params: dict = {
    'onlyData': 'true',
    'q': 'EnabledFlag=Y',
    'fields': 'CurrencyCode,Name'
}

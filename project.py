# %% Imports
import datetime

import bs4 as bs
import pandas_datareader as web
import requests

# %% Getting the tickers


def get_snp_tickers():
    '''
    Gets the list of S&P500 stocks from wikipedia

    Throws an exception if it can't get the data
    '''

    # We'll be using wikipedia as a main data source
    URL = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    resp = requests.get(URL)

    # If we can't get the page, just throw an exception
    if resp.status_code != 200:
        raise Exception('Couldn\'t access the url.'
                        'fResponse code {resp.status_code}')
    # We've gotten the page by now; start parsing the response
    soup = bs.BeautifulSoup(resp.text, 'html5lib')
    # Find the table containing the tickers
    for table in soup.findAll('table', {'class': 'wikitable sortable'}):
        try:
            # We're searching for a table that has a 'Ticker symbol' as its
            # first column name
            if table.findAll('th')[0].a.contents[0] == 'Ticker symbol':
                res_table = table
        except Exception:
            pass
    # Start extracting the tickers from the table
    res_arr = []
    for row in res_table.findAll('tr')[1:]:
        res_arr.append(row.findAll('td')[0].a.contents[0])
    return res_arr


try:
    tickers = get_snp_tickers()
except Exception as e:
    print(e)

# Verify that the number of tickers is correct
if len(tickers) == 505:
    print('The ticker count is correct')
else:
    print('[ERROR]: The ticker count is incorrect -'
          ' {} instead of 505'.format(len(tickers)))

# %% Getting the data

for ticker in tickers:
    print(ticker)

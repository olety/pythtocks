# %% Imports
import datetime
import importlib
import logging
import os
import pickle
import sys

import bs4 as bs
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pandas_datareader as web
import requests

# Fixing pickle
sys.setrecursionlimit(50000)
# Pandas fancy tables
pd.set_option('display.notebook_repr_html', True)
pd.set_option('max_rows', 10)
# Matplotlib fancy plots
plt.style.use('ggplot')
# Logger setup
importlib.reload(logging)

# FORMAT = "[%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
logging.basicConfig(format='%(levelname)s | line %(lineno)s '
                    '| %(funcName)s | %(message)s',
                    level=logging.INFO, stream=sys.stdout,
                    datefmt='%H:%M:%S')
# Numpy printing setup
np.set_printoptions(threshold=10, linewidth=79, edgeitems=5)

# %% Getting the tickers


def get_snp_tickers():
    '''
    Gets the list of S&P500 stocks from wikipedia

    Throws an exception if it can't get the data
    '''

    # We'll be using wikipedia as a main data source
    URL = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    logging.info('Getting the wikipedia article...')
    resp = requests.get(URL)

    # If we can't get the page, just throw an exception
    logging.info('Checking the response code...')
    if resp.status_code != 200:
        raise Exception('Couldn\'t access the url.'
                        'fResponse code {resp.status_code}')
    # We've gotten the page by now; start parsing the response
    soup = bs.BeautifulSoup(resp.text, 'html5lib')
    # Find the table containing the tickers
    logging.info('Finding the table containing the tickers...')
    for table in soup.findAll('table', {'class': 'wikitable sortable'}):
        try:
            # We're searching for a table that has a 'Ticker symbol' as its
            # first column name
            if table.findAll('th')[0].a.contents[0] == 'Ticker symbol':
                res_table = table
        except Exception:
            pass
    # Start extracting the tickers from the table
    logging.info('Extracting the tickers...')
    res_arr = []
    for row in res_table.findAll('tr')[1:]:
        res_arr.append(row.findAll('td')[0].a.contents[0])

    return res_arr


def save_tickers(folder='data', fname='tickers.csv'):
    '''
    Saves tickers got by get_snp_tickers into a file using Pickle.

    Throws an exception if something goes wrong.
    '''
    logging.info('Getting the tickers...')
    tickers = pd.DataFrame(get_snp_tickers())
    if tickers.shape[0] != 505:
        raise Exception('Bad ticker count - '
                        '{} instead of 505'.format(len(tickers)))
    # Saving the tickers
    new_fname = os.path.join(os.getcwd(), folder, fname)
    logging.info(f'Saving the tickers to {new_fname}')
    tickers.to_csv(new_fname, index=False)
    logging.info(f'Saved the tickers to {new_fname}')


def get_stock_data(folder='data'):
    pass

# %% Getting the data


def get_data(tickers):
    for ticker in tickers:
        web.get_data_yahoo

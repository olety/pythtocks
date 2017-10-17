# %% Imports

import datetime as dt
import importlib
import logging
import os
import sys
import time

import bs4 as bs  # BeautifulSoup, HTML scraping
import matplotlib.pyplot as plt  # Plots q
import numpy as np  # Arrays
import pandas as pd  # DataFrames
import pandas_datareader as web  # Gets stock data from Yahoo
import requests  # HTTP requests
from IPython.display import display  # IPython display
from tqdm import tqdm  # Progress bar

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

# Constants
# We want to get the data from 2000 till 2017
START_DT = dt.datetime(2000, 1, 1)
END_DT = dt.datetime(2017, 1, 1)

# %% Data gathering


def get_snp_tickers():
    '''
    Gets the list of S&P500 stocks from wikipedia

    Throws an exception if it can't get the data
    '''

    # We'll be using wikipedia as a main data source
    URL = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    logging.debug('Getting the wikipedia article...')
    resp = requests.get(URL)

    # If we can't get the page, just throw an exception
    logging.debug('Checking the response code...')
    if resp.status_code != 200:
        raise Exception('Couldn\'t access the url.'
                        'fResponse code {resp.status_code}')
    # We've gotten the page by now; start parsing the response
    soup = bs.BeautifulSoup(resp.text, 'html5lib')
    # Find the table containing the tickers
    logging.debug('Finding the table containing the tickers...')
    for table in soup.findAll('table', {'class': 'wikitable sortable'}):
        try:
            # We're searching for a table that has a 'Ticker symbol' as its
            # first column name
            if table.findAll('th')[0].a.contents[0] == 'Ticker symbol':
                res_table = table
        except Exception:
            pass
    # Start extracting the tickers from the table
    logging.debug('Extracting the tickers...')
    res_arr = []
    for row in res_table.findAll('tr')[1:]:
        # Replacing dots with dashes because otherwise we won't be able to
        # download them from yahoo finance
        res_arr.append(
            row.findAll('td')[0]
            .a.contents[0]
            .replace(',', '-').replace('.', '-')
        )
    return res_arr


def save_tickers(folder='data/tickers', fname='tickers.csv'):
    '''
    Saves tickers got by get_snp_tickers into a file using Pickle.

    Throws an exception if something goes wrong.
    '''
    logging.debug('Getting the tickers...')
    tickers = pd.DataFrame(get_snp_tickers())
    if tickers.shape[0] != 505:
        raise Exception('Bad ticker count - '
                        '{} instead of 505'.format(len(tickers)))
    logging.debug('Gotten tickers')
    tickers.columns = ['Ticker']
    display(tickers)
    # Sorting the tickers alphabetically
    logging.debug('Sorting the tickers...')
    tickers.sort_values(by='Ticker', inplace=True)
    logging.debug('Tickers after sorting')
    display(tickers)
    # Saving the tickers
    fpath = os.path.join(os.getcwd(), folder, fname)
    logging.debug('Creating the destination folder...')
    if not os.path.exists(os.path.dirname(fpath)):
        os.makedirs(os.path.dirname(fpath))
    logging.debug(f'Saving the tickers to {fpath}')

    tickers.to_csv(fpath, index=False)
    logging.info(f'Saved the tickers to {fpath}')

    return tickers


def get_stock_data(start_dt, end_dt, ticker_folder='data/tickers',
                   ticker_fname='tickers.csv', reload_tickers=False,
                   dest_folder='data/stocks', retry_requests=True,
                   max_retries=50, timeout=2):
    '''
    Gets stock data of S&P500 from yahoo and saves it in the {folder}/{tick}.csv

    Throws an exception is anything goes wrong.
    '''
    logging.debug('Obtaining the tickers...')
    if reload_tickers:
        tickers = save_tickers()
    else:
        tickers = pd.read_csv(os.path.join(os.getcwd(),
                                           ticker_folder, ticker_fname))
    logging.debug('Obtained the tickers...')
    display(tickers)
    # We have to check whether the dest folder exists
    dest_path = os.path.join(os.getcwd(), dest_folder)
    if not os.path.exists(dest_path):
        logging.debug('Creating the destination folder')
        os.makedirs(dest_path)

    # Downloading the prices
    logging.debug('Starting processing tickers...')
    for index, ticker in tqdm(tickers.itertuples(), desc='Tickers processed',
                              leave=False, file=sys.stderr, unit='company',
                              total=tickers.shape[0]):
        logging.debug(f'Starting a new outer loop iteration for {ticker}')
        dest_fpath = os.path.join(dest_path, f'{ticker}.csv')
        if not os.path.exists(dest_fpath):
            # Try to download the stock for max_retries tries, waiting
            # for timeout in between tries
            pbar = tqdm(range(max_retries), desc='Number of tries',
                        leave=False, file=sys.stderr, unit='try')
            retries = max_retries
            while retries > 0:
                pbar.update(1)
                retries -= 1
                try:
                    logging.debug(f'Trying to get {ticker} data from yahoo...')
                    df = web.DataReader(ticker, 'yahoo', start_dt, end_dt)
                except Exception:
                    logging.debug('Yahoo has denied our request - '
                                  f'sleeping for {timeout} seconds')
                    time.sleep(timeout)
                logging.debug(f'Successfully got {ticker} data from yahoo. '
                              'Now saving it...')
                df.to_csv(dest_fpath)
                logging.debug(f'Saved the {ticker} data.')
                retries = 0
            pbar.close()

        else:
            logging.debug(f'Not downloading data for {ticker}, '
                          'since we already have it')
    logging.info('Finished processing all tickers!')
    logging.info(f'You can find the results in the folder {dest_path}')


# save_tickers()
get_stock_data(START_DT, END_DT)

# %% Data visualization

# %% Predictive analysis

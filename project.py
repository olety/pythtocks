# %% Imports

import datetime as dt
import importlib
import logging
import os
import sys
import time

import bs4 as bs  # BeautifulSoup, HTML scraping
import matplotlib.pyplot as plt  # Plots
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
        # Replacing dots with dashes because otherwise we won't be able to
        # download them from yahoo finance
        res_arr.append(row.findAll('td')[0].a.contents[0].replace(',', '-'))
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
    fpath = os.path.join(os.getcwd(), folder, fname)
    logging.info('Creating the destination folder...')
    if not os.path.exists(os.path.dirname(fpath)):
        os.makedirs(os.path.dirname(fpath))
    logging.info(f'Saving the tickers to {fpath}')

    tickers.to_csv(fpath, index=False)
    logging.info(f'Saved the tickers to {fpath}')

    return tickers


def get_stock_data(start_dt, end_dt, ticker_folder='data',
                   ticker_fname='tickers.csv', reload_tickers=False,
                   dest_folder='data', retry_requests=True, max_retries=50,
                   timeout=5):
    '''
    Gets stock data of S&P500 from yahoo and saves it in the {folder}/{tick}.csv

    Throws an exception is anything goes wrong.
    '''
    logging.info('Obtaining the tickers...')
    if reload_tickers:
        tickers = save_tickers()
    else:
        tickers = pd.read_csv(os.path.join(os.getcwd(),
                                           ticker_folder, ticker_fname))
    logging.info('Obtained the tickers...')
    display(tickers)
    # We have to check whether the dest folder exists
    dest_path = os.path.join(os.getcwd(), dest_folder)
    if not os.path.exists(dest_path):
        logging.info('Creating the destination folder')
        os.makedirs(dest_path)

    # Downloading the prices
    logging.info('Starting processing tickers...')
    for index, ticker in tqdm(tickers.itertuples(), desc='Tickers processed',
                              leave=False, file=sys.stdout, unit='company',
                              total=tickers.shape[0]):
        dest_fpath = os.path.join(dest_path, f'{ticker}.csv')
        if not os.path.exists(dest_fpath):
            # Try to download the stock for max_retries tries, waiting
            # for timeout in between tries
            for i in tqdm(range(max_retries), desc='Number of tries',
                          leave=False, file=sys.stderr, unit='try'):
                try:
                    df = web.DataReader(ticker, 'yahoo', start_dt, end_dt)
                    df.to_csv(dest_fpath)
                    break
                except Exception:
                    # TODO: Change to debug
                    logging.info('Yahoo has denied our request - '
                                 f'sleeping for {timeout} seconds')
                    time.sleep(timeout)
        else:
            logging.info(f'Not downloading data for {ticker}, '
                         'since we already have it')
    logging.info('Finished processing all tickers!')
    logging.info(f'You can find the results in the folder {dest_path}')


get_stock_data(start_dt=dt.datetime(2000, 1, 1),
               end_dt=dt.datetime(2017, 1, 1))

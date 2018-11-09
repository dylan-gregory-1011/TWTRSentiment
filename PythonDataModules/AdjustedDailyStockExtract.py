##!/usr/bin/env python
"""
Stock API Download: Brings down data from a stock API into the tables.
"""

#Imports
from pathlib import Path
import json
import sqlite3
import requests
import logging
import pandas as pd

__author__ = "Dylan Smith"
__copyright__ = "Copyright (C) 2018 Dylan Smith"
__credits__ = ["Dylan Smith"]

__license__ = "Personal Use"
__version__ = "1.0"
__maintainer__ = "Dylan Smith"
__email__ = "-"
__status__ = "Development"

class AdjustedDailyStockExtract(object):
    #constants
    url = "https://www.alphavantage.co/query?"
    fields = ["day", "open", "high", "low", "close", "adjusted_close", "volume", "dividend_amount","split_coefficient"]
    function = "TIME_SERIES_DAILY_ADJUSTED"
    function_key = "Time Series (Daily)"

    def __init__(self, api_key_dir, connection):
        """
        This object sets up an api call to alphavantage and allows for different symbols to be queried. The
        API rate limit is 5 per minute and 500 per day.

        ::param api_key_dir: The directory where the API Key is stored.
        ::param connection: A connection to the SQLite database where the data is stored.
        """
        logging.info(' Instantiating Stock Download Process')
        self.conn = connection
        self.api_key = self.getAPIKey(api_key_dir)

    def getAPIKey(self, api_key_dir):
        """
        Get the api key for the twitter call

        ::param api_key_dir: the directory where the api key is stored
        return: A String that represents the API Key to access the API
        """
        with open(api_key_dir.joinpath('StocksAPIKey.json'), "r") as file:
            return json.load(file)['APIKey']

    def executeSQLCommand(self, sql_stmnt, data= None):
        """
        The method that executes and commits the sql commands needed

        ::param sql_stmt: the SQL statement to run on the connection
        ::param data(Optional): The data to be loaded into the sql statement
        """
        if data is not None:
            self.conn.executemany(sql_stmnt, data)
        else:
            self.conn.execute(sql_stmnt)
        self.conn.commit()

    def downloadStockData(self, symbol):
        """
        Download the stock data and insert it into the database

        ::param symbol: The symbol for the stock to be downloaded
        """
        #sql commands used in the function below
        create_tbl = "CREATE TABLE %s (day text PRIMARY KEY, %s)" % (symbol, ','.join([f + ' REAL' for f in self.fields[1:]]))
        #check if table exists
        try:
            old_df = pd.read_sql_query("SELECT * FROM %s" % symbol, self.conn)
            outputsize = ''
        except:
            self.executeSQLCommand(create_tbl)
            old_df = pd.DataFrame(columns = self.fields)
            outputsize = "&outputsize=full"

        logging.info('Begin call for stock %s' % symbol)
        full_api_call = self.url + 'function=%s&symbol=%s%s&apikey=%s' % (self.function, symbol, outputsize, self.api_key)
        request = requests.get(full_api_call)
        stock_data = request.json()[self.function_key]
        #begin formatting the data
        new_df = pd.DataFrame.from_dict(stock_data, orient = 'index').reset_index()
        new_df.columns = self.fields
        #add the two dataflows together and drop the duplicates
        new_df = new_df.append(old_df, sort = False)
        new_df.drop_duplicates(keep = 'first', inplace = True)
        #insert the data into the table
        data = tuple(new_df.itertuples(index = False))
        wildcards = ','.join(['?'] * 9)
        insert_sql = "INSERT OR IGNORE INTO %s VALUES (%s)" % (symbol, wildcards)
        self.executeSQLCommand(insert_sql, data)
        logging.info('Data downloaded for %s' % symbol)

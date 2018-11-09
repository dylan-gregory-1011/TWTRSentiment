##!/usr/bin/env python
"""
Financial Analysis Project:  This file downloads the data from SEC 10-Q and 10-K filings and builds a table
that allows for the specific attribute to be analyzed in isolation from the other attributes.
"""

#Imports
from os import listdir
from pathlib import Path
from os.path import join
import logging
import pandas as pd
import zipfile
from io import BytesIO
import numpy as np

__author__ = "Dylan Smith"
__copyright__ = "Copyright (C) 2018 Dylan Smith"
__credits__ = ["Dylan Smith"]

__license__ = "Personal Use"
__version__ = "1.0"
__maintainer__ = "Dylan Smith"
__email__ = "-"
__status__ = "Development"

class SECFilingDataDownload(object):
    def __init__(self, data_source, connection):
        """
        This object is meant to ingest the 10-K and 10-Q data from the SEC filings and format it in a way that
        can be used for analysis.  The output is a table that is formatted in the structure
            (cik, company name, YRQ1, YRQ2, YRQ3....etc)

        ::param data_source: This parameter is the directory where the data filings are stored.  They ingest a
            zipped folder to run properly.  The files are from the url:  https://www.sec.gov/dera/data/financial-statement-data-sets.html
        ::param connection: The connection to a sqlite3 database where the data will be stored.
        """
        logging.info('--- Instantiating SEC Filing Attribute ---')
        self.data_source = data_source
        self.conn = connection
        self.filings_to_analyze = [f for f in listdir(data_source) if zipfile.is_zipfile(join(data_source, f))]

    def splitText(self, text):
        """
        Split the text to allow the data to be processed

        ::param text: the text data to be split
        """
        return text.split('~',1)[0]

    def getQuarterFromDate(self, date):
        """
        ::param date: The date that the Quarterly filing applies to. This uses the date that the data applies to
        """
        dt_str = str(date)
        year, qtr = dt_str[:4], (int(dt_str[4:6]) - 1) // 3 + 1
        return str(year) + 'Q' + str(qtr)

    def extractSECFilingData(self, attribute):
        """
        This function takes the attribute to be investigated and goes through the SEC filings and extracts the
        data to be inserted into the SQL table. This dumps the data into a SQLite database

        ::param attribute: The attributes to be downloaded and analyzed from the SEC filings. This enters
                         the data into a table.
        """
        logging.info(' Extract SEC filing data for %s' % attribute)
        #create a dataframe file to append all of the data on. Delete from the table
        df_tmp = pd.DataFrame(columns = {'cik_name': [],'qtr':[], 'value': [], 'file': []})
        self.conn.execute("DROP TABLE IF EXISTS %s" % attribute)
        self.conn.commit()

        for filing_period in self.filings_to_analyze:
            #read each file and get the numerical data into dataframes
            with zipfile.ZipFile(self.data_source.joinpath(filing_period)) as z:
                iter_data = pd.read_csv(BytesIO(z.read('num.txt')), iterator = True,
                            chunksize = 1000, sep ='\t', encoding = 'latin1')
                df_sub = pd.read_csv(BytesIO(z.read('sub.txt')), sep = '\t')

            #begin formatting the data, filter out non 10K and 10Q data and other bad data
            df_mstr = df_sub[['adsh','cik', 'name', 'form', 'period', 'fy', 'fp']][df_sub['form'].isin(['10-K', '10-Q'])
                                                                                                    & df_sub['fp'].notnull()
                                                                                                    & (df_sub['fp'] != 'Q4')]
            #apply the quarter to the date that the file was submitted for. Prepare the final data
            df_mstr['qtr'] = df_mstr['period'].apply(self.getQuarterFromDate)
            df_data = pd.concat([chunk[['adsh','ddate','value', 'qtrs']][(chunk['tag'] == attribute)
                                                                & (chunk['coreg'].isnull())] for chunk in iter_data])
            df_qtr = pd.merge(df_mstr[['adsh', 'cik', 'name', 'period', 'qtr']], df_data,
                        how = 'left', left_on = ['adsh', 'period'], right_on = ['adsh', 'ddate'])
            #prepare the data to allow the proper records to come through
            df_qtr = df_qtr[df_qtr['ddate'].notnull() & df_qtr['cik'].notnull()]
            df_qtr['cik_name'] = df_qtr['cik'].map(int).map(str) + '|' + df_qtr['name'] + '~' + df_qtr['qtr']
            df_qtr['file'] = filing_period
            #get the minimum quarters reported for each file.  This allows us to get the closes to a quarterly report
            df_min = df_qtr[['adsh', 'ddate', 'qtrs']].groupby(['ddate', 'adsh'], sort = True)['qtrs'].min().reset_index(name = 'qtrs')
            df_min['qtrs_min'] = df_min['qtrs']
            df_min.drop(['qtrs'], axis = 1, inplace = True)
            df_tmp_qtr = pd.merge(df_qtr, df_min, how = 'inner', left_on = ['ddate', 'adsh', 'qtrs'], right_on = ['ddate', 'adsh', 'qtrs_min'])
            #get the value per quarter for each company
            df_tmp_qtr['value'] = df_tmp_qtr['value'] / df_tmp_qtr['qtrs_min']
            df_tmp_qtr.drop(['adsh', 'ddate', 'period', 'cik', 'name', 'qtrs', 'qtrs_min'], axis = 1, inplace = True)
            df_tmp = df_tmp.append(df_tmp_qtr, sort = False)

        logging.info("Preparing to pivot values for final table")
        #get the max filing for each company per quarter
        df_max_files = df_tmp[['cik_name', 'file']].groupby(['cik_name'], sort = True)['file'].max().reset_index(name = 'file')
        df_tmp_final = pd.merge(df_tmp, df_max_files, how = 'inner', on = ['cik_name', 'file'])
        df_tmp_final.drop(['file'], axis = 1, inplace = True)
        df_tmp_final = df_tmp_final.groupby(['cik_name', 'qtr'], sort = True)['value'].max().reset_index(name = 'value')
        df_tmp_final.loc[:, 'cik_name'] = df_tmp_final['cik_name'].apply(self.splitText)

        #apply the pivot properly and write it to a tmp file
        pivot = df_tmp_final.pivot(values = 'value',index = 'cik_name', columns = 'qtr')
        df_final =  pivot.reset_index()
        cik, name = df_final['cik_name'].str.split('|').str
        df_final.drop('cik_name', axis = 1, inplace = True)
        df_final.insert(0, 'name', name)
        df_final.insert(0, 'cik', cik)
        #df_final.to_csv(self.data_source.joinpath('Data.csv'))

        logging.info(' Writing the data to the SQL tables ')
        #create table if not exists
        pd_headers = list(df_final)
        pd_create = ',_'.join([ col + ' BIGINT' for col in pd_headers[2:]])
        self.conn.execute("CREATE TABLE IF NOT EXISTS %s (cik TEXT, name TEXT,_%s)" % (attribute, pd_create))
        self.conn.commit()
        #get dataformatted
        data = tuple(df_final.itertuples(index = False))
        wildcards = ','.join(['?'] * len(pd_headers))
        insert_sql = "INSERT OR IGNORE INTO %s VALUES (%s)" % (attribute, wildcards)
        self.conn.executemany(insert_sql, data)
        self.conn.commit()

##!/usr/bin/env python
"""
NFL Sentiment Analysis credentials controller
"""

#imports
import sys
from os import listdir
from os.path import isdir, join
from pathlib import Path
import logging
import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
logging.basicConfig(stream=sys.stdout, level = logging.INFO)

__author__ = "Dylan Smith"
__copyright__ = "Copyright (C) 2018 Dylan Smith"
__credits__ = ["Dylan Smith"]

__license__ = "Personal Use"
__version__ = "3.0"
__maintainer__ = "Dylan Smith"
__email__ = "-"
__status__ = "Development"

class TwitterSentimentAnalyzer(object):
    def __init__(self, proj_data_dir, proj_analysis_dir, db_connection, days_to_update):
        """ Instantiates an instance of the Twython Cleanser.

            ::param proj_data_dir: The data directory for the project at hand
            ::param proj_analysis_dir: The project folder where the analytical files are housed
            ::param db_connection: the database connection that will drive the sqlite table
            ::param days_to_update: the dates to update the sentiment
        """
        self.load_days = days_to_update
        self.db_con = db_connection
        self.analytics_file = proj_analysis_dir.joinpath('CalculatedSentimentData.csv')
        self.record_counts_file = proj_analysis_dir.joinpath('RecordCounts.csv')
        self.all_groups = [f for f in listdir(proj_data_dir) if isdir(join(proj_data_dir, f))]

    #Basic Methods
    def returnSentimentForTweet(self, tweet):
        """
        ::param tweet: Takes a string input and from the tweet.
        return: The sentiment for the string of each tweet
        """
        return SentimentIntensityAnalyzer().polarity_scores(tweet)['compound']


    def calculateSentimentForTweets(self):
        """
        Iterates through all of the cleansed data files (for each team) and applies the sentiment tool to each tweet and
        re-writes the data back into a cleansed data file.  This is dependent on the previous tweets being loaded into
        the cleanser and the specific dates that will be updated with sentiment values as well as the record counts
        """
        #ingest the total sentiment summation file into a dataframe, pull down the record count file as well
        total_sentiment_DF = pd.read_csv(self.analytics_file,
                                        encoding = 'utf-8',
                                        header = 0)

        total_record_counts_DF = pd.read_csv(self.record_counts_file,
                                             encoding = 'utf-8',
                                             header = 0)

        for group in self.all_groups:
            logging.info("Calculating Sentiment For %s" % group)

            #get the files to be loaded, and remove any potential files that could have duplicate dateata
            if self.load_days is None:
                self.load_days = pd.read_sql_query("""SELECT DISTINCT day FROM %s""" % group, self.db_con)['day'].tolist()

            for date in self.load_days:
                cleansed_data_DF = pd.read_sql_query("""SELECT * FROM %s WHERE day = \"%s\"""" %(group, date), self.db_con)
                cleansed_data_DF.loc[:,'sentiment'] = cleansed_data_DF['full_text'].apply(self.returnSentimentForTweet)
                self.db_con.execute("DELETE FROM %s WHERE day = \"%s\"""" %(group, date))
                self.db_con.commit()

                data = tuple(cleansed_data_DF.itertuples(index = False))
                wildcards = ','.join(['?'] * 7)
                insert_sql = """INSERT OR IGNORE INTO %s VALUES (%s)""" % (group, wildcards)
                self.db_con.executemany(insert_sql, data)
                self.db_con.commit()

                #update the aggregated sentiment file for the dates specified
                for date_to_add in cleansed_data_DF['day'].unique():
                    if date_to_add not in total_record_counts_DF['Date'].tolist():
                        new_record = len(total_record_counts_DF['Date'])
                        total_sentiment_DF.loc[new_record] = [date_to_add] + [0 for group in self.all_groups]
                        total_record_counts_DF.loc[new_record] = [date_to_add] + [0 for group in self.all_groups]

                    #add the records to the data counts
                    total_sentiment_DF.loc[total_sentiment_DF['Date'] == date_to_add, group] = \
                                                    cleansed_data_DF[cleansed_data_DF['day'] == date_to_add]['sentiment'].mean()
                    total_record_counts_DF.loc[total_record_counts_DF['Date'] == date_to_add , group] = \
                                                        cleansed_data_DF[cleansed_data_DF['day'] == date_to_add].shape[0]

        #once all the groups have been iterated through, write the dataframe to file again
        total_sentiment_DF.to_csv(self.analytics_file,
                            mode = 'w',
                            index = False)

        total_record_counts_DF.to_csv(self.record_counts_file,
                            mode = 'w',
                            index = False)

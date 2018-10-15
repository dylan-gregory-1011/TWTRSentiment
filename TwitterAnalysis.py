##!/usr/bin/env python
"""
Twitter Sentiment Analysis Project: Utilizes three distinct objects in order to download tweets
from the Twitter Search API, cleanse the data so that it can be stored properly, and apply a sentiment tool
to the tweets to try to gauge the daily sentiment for each group.


Version 1.0: Initial Scope of the project.  This includes the objects that perform the ETL and analysis
as well as the driver program (This file).

Version 2.0:
    - Changed the path functionality to the python 3 pathlib to ensure that the program runs fine on any
    operating system.  Initially it was just set up for macOS
    - Changed all datasets to be compressed gz files.
    - Updated the reply process to improve performance
    - Changed the Sentiment Tool to be vaderSentiment
    - Calculates the activity for each group, per day

"""

#Imports
import sys
from os.path import basename
from TwitterScraper import TwitterScraper
from TwitterCleanser import TwitterCleanser
from TwitterSentimentAnalyzer import TwitterSentimentAnalyzer
import logging
import pandas as pd
logging.basicConfig(stream=sys.stdout, level = logging.INFO)
from pytz import common_timezones
import csv

__author__ = "Dylan Smith"
__copyright__ = "Copyright (C) 2018 Dylan Smith"
__credits__ = ["Dylan Smith"]

__license__ = "Personal Use"
__version__ = "2.0"
__maintainer__ = "Dylan Smith"
__email__ = "-"
__status__ = "Development"


def updateTwitterDatasets(project_name):
    """
    This function updates the twitter dataset and downloads all relevant Tweets for each group that is part of
    the project.  Twitter API specifications are set in this function so that the parameters outside of the query
    terms are all the same

    ::params - project_name: takes a string input for the project name and accesses the correct folder as well as the
                             inputs the data into the correct data folders
    """
    #set up the query data as well as the keys from the data.  Also set up the query parameters
    proj_query = { "lang": "en",
                    "result_type": "recent",
                    "count": "100",
                    "tweet_mode": "extended"}

    twython_scraper = TwitterScraper(project_name = project_name)
    proj_qrys_file = twython_scraper.returnQueriesToRun()
    #read the query data to drive the twitter API calls
    df_queries = pd.read_csv(proj_qrys_file, index_col= False)
    #iterate over all the teams and update the table after each file has completed
    for index, qry in df_queries.iterrows():
        logging.info("Downloading Tweets for %s" % qry['Name'])
        proj_query['q'] = qry['Query']
        new_max, last_updt = twython_scraper.downloadHistoricalTweets(qry['Name'], proj_query, qry['Max_Record'])
        df_queries.loc[index, 'Max_Record'] = new_max
        df_queries.loc[index, 'Last_Update'] = last_updt
        df_queries.to_csv(proj_qrys_file,
                            mode = 'w',
                            sep =',',
                            index = False,
                            encoding ='utf-8')
        logging.info("Finished Downloading Tweets for %s" % qry['Name'])

def processStoreAndAnalyzeData(load_type):
    """
    This function instantiates a twitter cleanser object as well as a twitter sentiment object.
    It takes a load type and cleanses/applies sentiment tools to that subset of data

    params load_type: Takes a string input and specifies which records should be classified.  It is either
                      a delta load or a full load.
    """
    twitter_cleanser = TwitterCleanser(project_name, load_type)
    twitter_cleanser.uploadTweetsIntoCleanser()
    twitter_cleanser.updateOriginalTweets()
    twitter_cleanser.cleanseRepliedTweets()
    twitter_cleanser.sumRetweets()

    datesToUpdate = twitter_cleanser.getDeltaDatesToUpdate()
    twitter_sentiment = TwitterSentimentAnalyzer(project_name, datesToUpdate)
    twitter_sentiment.calculateSentimentForTweets()


if __name__ == '__main__':
    #Constants
    project_name = 'NFLSentiment'
    updateTwitterDatasets(project_name = project_name)
    processStoreAndAnalyzeData('DELTA')

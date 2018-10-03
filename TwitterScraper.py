##!/usr/bin/env python
"""
Beginner Twitter analysis
:: Sets up a Twython Search API object that returns values based on specifications in a
file
"""

#imports
from twython import Twython, TwythonError
import zlib
import json
import re
from os.path import dirname, abspath, isfile
from os import remove
import pandas as pd
from time import sleep, gmtime
from datetime import datetime
import logging
import zlib
from calendar import timegm


__author__ = "Dylan Smith"
__copyright__ = "Copyright (C) 2018 Dylan Smith"
__credits__ = ["Dylan Smith"]

__license__ = "Personal Use"
__version__ = "1.0"
__maintainer__ = "Dylan Smith"
__email__ = "-"
__status__ = "Development"

#constants
curr_dir = dirname(abspath(__file__))

class TwitterScraper(Twython):
    #initialization
    def __init__(self, project_name):
        """ Instantiates an instance of the Twython Scraper.  Takes one input and sets up
            the correct connection

            ::param output_file_folder: The File Folder that will hold the result sets
        """
        logging.info('------- Instantiating Twython Object -------')
        logging.info('-- Setting Credentials and Search Terms --')
        #Properties
        self.apiCallsLeftInPeriod = 1000
        self.proj_dir = curr_dir + '/' + project_name
        self.proj_name = project_name
        self.client_args = {'headers': {'User-Agent': 'Chrome',"accept-charset": "utf-8", }, }
        self.current_date = datetime.today().strftime('%Y%m%d')
        self.delay_time = 1.5
        #get credentials and instantiate
        self.credentials = self.getTwitterCredentials(self.proj_dir + '/' + self.proj_name + 'keys.json')
        self.twitter = Twython(self.credentials['TWITTER_APP_KEY'],
                            self.credentials['TWITTER_APP_SECRET'],
                            client_args = self.client_args )
        logging.info('-- Twython object instantiated --')

    #Methods
    def getTwitterCredentials(self, credentials_file):
        """
        Takes the input as a json file and returns the dictionary
        that has the Twitter APP_KEY and APP_SECRET as well as the
        search phrase and the max id for the search phrase

        ::param - Credentials File : {TWITTER_APP_KEY: STR, TWITTER_APP_SECRET: STR,
                                      ACCESS_TOKEN: STR, ACCESS_TOKEN_SECRET: STR}
                    A dictionary with the correct keys for the API call.  This includes the
                    full path for the file
        """
        logging.info('-- Getting Credentials and Search Terms --')
        with open(credentials_file, "r") as file:
            return json.load(file)

    def returnQueriesToRun(self):
        """
        Returns the query path for the project in question to allow the user to iterate through queries
        """
        return self.proj_dir + '/' + self.proj_name + 'QueryData.csv'

    def downloadHistoricalTweets(self, output_name, query, max_id):
        """
        Search Twitter going as far back as necessary with the current query status search.  This utilizes the status
        from the previous data pulls to ensure that no data is duplicated and to limit the amount of records that need
        to be pulled

        ::param output_name: Group name to be used to download the data
        ::param query: The query to submit to the Twitter API.  Needs to be in the proper format
        ::param max_id:  The max query from the previous data extract.  This will be used to limit the return
        """
        #update
        self.output_file = self.proj_dir + '/Data/' + output_name + '/RawData/' + output_name + self.current_date + '.csv'
        self.total_records_downloaded = 0
        self.query_updt = query.copy()
        self.query_updt['since_id'] = max_id

        #Make first pass at data and update new max
        self.new_min_id = self.downloadTweetsForQuery(self.query_updt, True)

        while type(self.new_min_id) is int:
            self.query_updt['max_id'] = str(self.new_min_id - 1)
            self.new_min_id = self.downloadTweetsForQuery(self.query_updt, False)

        logging.info('-- %i Tweets on %s for Time Period have Downloaded --' % (self.total_records_downloaded, output_name))
        #return the new max update to update the dataframe
        return self.new_max_id, str(datetime.now())

    def downloadTweetsForQuery(self, query, first):
        """
        This function runs each specific query specified in the Query Data file.  It measures how many api calls
        remain in the period so that the API endpoint is still reachable and it then goes through and calls a query
        based on the specific ID end points.  It works backwards from the most current tweet until the previous endself.
        It also cleans the twitter data to ensure that return types of None are not skipped.

        :: param - query: A string that is a Twython query that will be used for the search API
        :: param - first: A Boolean value that is true for the first search of each query and then false
                        for the subsequent queries.  This allows for the next max value to be set in the meta
                        data folder
        """
        if self.apiCallsLeftInPeriod == 5:
            rate_time_delay = int(self.twitter.get_lastfunction_header('x-rate-limit-reset')) - int(timegm(gmtime()))
            logging.info('-- Rate Limit Approached. Delay for %i Seconds' % rate_time_delay)
            sleep(rate_time_delay)

        sleep(self.delay_time)

        try:
            self.twitter_results = self.twitter.search(**query)
        except TwythonError as e:
            print(e.error_code)
            if int(e.error_code) == 503:
                return self.new_min_id
        #check to see how many calls are available this period
        self.apiCallsLeftInPeriod = self.twitterCallsRemainingDuringPeriod()
        #create the temp dataframe to download the data in
        self.dict_ = { 'id': [],'user': [], 'date': [], 'full_text': [], 'replied_to_id': [], 'retweeted_id': []}

        for self.status in self.twitter_results['statuses']:
            #format the retweeted status as well as the replied to comments
            if 'retweeted_status' not in self.status:
                self.cleaned_tweet = self.cleanTweet(self.status['full_text'])
                self.retweeted_id = ''
            else:
                self.cleaned_tweet = self.cleanTweet(self.status['retweeted_status']['full_text'])
                self.retweeted_id = self.status['retweeted_status']['id']

            #format the dictionary and add the tweets to the format.  clean the tweets
            self.dict_['id'].append(self.xstr(self.status['id']))
            self.dict_['user'].append(self.xstr(self.status['user']['screen_name']))
            self.dict_['date'].append(self.xstr(self.status['created_at']))
            self.dict_['full_text'].append(self.xstr(self.cleaned_tweet))
            self.dict_['replied_to_id'].append(self.xstr(self.status['in_reply_to_status_id_str']))
            self.dict_['retweeted_id'].append(self.xstr(self.retweeted_id))

        self.total_records_downloaded += len(self.twitter_results['statuses'])

        logging.info("-- %i Total Records Downloaded --" % self.total_records_downloaded)
        self.df = pd.DataFrame(self.dict_)
        self.df.to_csv(self.output_file,
                    mode = 'a',
                    sep='\t',
                    index = False,
                    encoding='utf-8',
                    header = False,
                    line_terminator = '\n')

        if first:
            self.new_max_id = max([int(x) for x in self.dict_['id']])

        try:
            return min([int(x) for x in self.dict_['id']])
        except:
            return None

    def xstr(self,s):
        """
        This function returns a blank string if the input is bad and the string if it has a correct input
        ::param s- returns a blank string if it is empty or the string if it is a string
        """
        if s is None:
            return ''
        return s

    def cleanTweet(self, tweet):
        '''
        Utility function to clean the text in a tweet by removing
        links and special characters using regex.
        ::param tweet: Takes a string input and cleans the tweet of links and other characters
        '''
        return ''.join(re.sub("(https?://[A-Za-z0-9./]+)", "", tweet)).replace('\n', ' ').replace('\t', ' ').replace(u'\u2705', ' ')

    def twitterCallsRemainingDuringPeriod(self):
        """
        This function is called after every API search to see how many more requests we have during the time period.
        """
        try:
            return int(self.twitter.get_lastfunction_header('x-rate-limit-remaining'))
        except:
            return self.apiCallsLeftInPeriod

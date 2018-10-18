##!/usr/bin/env python
"""
NFL Sentiment Analysis credentials controller
"""

#imports
import sys
import json
from os import listdir, remove, rename
from os.path import isdir, join, isfile
from pathlib import Path
import logging
import pandas as pd
from datetime import datetime
from textblob import TextBlob
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
logging.basicConfig(stream=sys.stdout, level = logging.INFO)


__author__ = "Dylan Smith"
__copyright__ = "Copyright (C) 2018 Dylan Smith"
__credits__ = ["Dylan Smith"]

__license__ = "Personal Use"
__version__ = "1.0"
__maintainer__ = "Dylan Smith"
__email__ = "-"
__status__ = "Development"

class TwitterSentimentAnalyzer(object):
    #constants
    curr_dir = Path(__file__).resolve().parent
    clean_column_names = ['id', 'user_id','date', 'full_text', 'retweets', 'sentiment']
    analyzer_type = "Vader"
    vader_analyzer = SentimentIntensityAnalyzer()

    def __init__(self, project_name, dates_to_update):
        """ Instantiates an instance of the Twython Cleanser.  Takes one input and sets up
            the correct connection

            ::param project_name: The project that will get twitter data cleansed
            ::param load_type: What type of load will happen.  (FULL, DELTA)
        """
        #declare original properties
        self.project_name = project_name
        self.load_dates = dates_to_update
        self.proj_data_dir = self.curr_dir.joinpath(self.project_name, 'Data')
        self.analytics_file = self.curr_dir.joinpath(self.project_name, 'CalculatedData.csv')
        self.record_counts_file = self.curr_dir.joinpath(self.project_name, 'RecordCounts.csv')
        self.getGroupsToClean()
        self.emoji_replacements = self.getEmojiReplacements()
        self.all_groups.remove('OriginalTweets')

    #Basic Methods
    def getGroupsToClean(self):
        """
        Get the list of groups to be analyzed in this project and return a list so that the
        tweets can be processed.  This uses the current raw file path
        returns - Sets the list of all the groups to be analyzed
        """
        logging.info("Getting List of all teams for sentiment analysis")
        self.all_groups = [f for f in listdir(self.proj_data_dir) if isdir(join(self.proj_data_dir, f))]

    def getEmojiReplacements(self):
        with open(self.curr_dir.joinpath('EmojiDictionary.json'), "r",encoding='utf-8') as file:
            return json.load(file)

    def getFilesInDir(self, directory):
        """
        Get the files in the specified directory.  This is used to ensure that the right files are analyzed
        :: param  Directory: The directory that we desire the files from
        returns - All the files names in the specified directory.
        """
        return [f for f in listdir(directory) if isfile(join(directory, f)) and f.split('.')[1] == 'csv']

    #Sentiment Methods to be used
    def returnSentimentForTweet(self, tweet):
        """
        ::param tweet: Takes a string input and from the tweet.
        return: The sentiment for the string of each tweet
        """
        if self.analyzer_type == 'Vader':
            return self.vader_analyzer.polarity_scores(tweet)['compound']
        else:
            return TextBlob(tweet).sentiment.polarity


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
            cleansed_data_dir = self.proj_data_dir.joinpath(group, 'CleansedData')

            #get the files to be loaded, and remove any potential files that could have duplicate dateata
            if self.load_dates is None:
                cleansed_data_files = self.getFilesInDir(cleansed_data_dir)
            else:
                cleansed_data_files = []
                for date in self.load_dates:
                    cleansed_data_files.append(group + date + '.csv.gz')

            for data_file in cleansed_data_files:
                cleansed_data_DF = pd.read_csv(cleansed_data_dir.joinpath(data_file),
                                                compression = 'gzip',
                                                sep = '\t',
                                                index_col = False,
                                                encoding = 'utf-8',
                                                names = self.clean_column_names).fillna('0')

                cleansed_data_DF.loc[:,'sentiment'] = cleansed_data_DF['full_text'].apply(self.returnSentimentForTweet)
                cleansed_data_DF.to_csv(cleansed_data_dir.joinpath(data_file),
                                            compression = 'gzip',
                                            mode = 'w',
                                            sep = '\t',
                                            index = False,
                                            header = False,
                                            line_terminator = '\n')
                #update the aggregated sentiment file for the dates specified
                date_to_add = data_file[-16:-7]
                if date_to_add not in total_record_counts_DF['Date'].tolist():
                    new_record = len(total_record_counts_DF['Date'])
                    total_sentiment_DF.loc[new_record] = [date_to_add] + [0 for group in self.all_groups] + [0]
                    total_record_counts_DF.loc[new_record] = [date_to_add] + [0 for group in self.all_groups]

                #add the records to the data counts
                total_sentiment_DF.loc[total_sentiment_DF['Date'] == date_to_add, group] = cleansed_data_DF['sentiment'].mean()
                total_record_counts_DF.loc[total_record_counts_DF['Date'] == date_to_add , group] = cleansed_data_DF.shape[0]

        #once all the groups have been iterated through, write the dataframe to file again
        total_sentiment_DF.to_csv(self.analytics_file,
                            mode = 'w',
                            index = False)

        total_record_counts_DF.to_csv(self.record_counts_file,
                            mode = 'w',
                            index = False)

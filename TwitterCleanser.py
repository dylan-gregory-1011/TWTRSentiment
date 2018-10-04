##!/usr/bin/env python
"""
Twitter Cleanser to prepare the tweets for analysis
"""

#imports
import sys
import csv
import re
from os import listdir, remove, rename
from os.path import basename, isdir, join, dirname, abspath, isfile
import logging
import pandas as pd
from pytz import timezone
from datetime import datetime
logging.basicConfig(stream=sys.stdout, level = logging.INFO)


__author__ = "Dylan Smith"
__copyright__ = "Copyright (C) 2018 Dylan Smith"
__credits__ = ["Dylan Smith"]

__license__ = "Personal Use"
__version__ = "1.0"
__maintainer__ = "Dylan Smith"
__email__ = "-"
__status__ = "Development"


class TwitterCleanser(object):
    #constants
    curr_dir = dirname(abspath(__file__))
    clean_column_names = ['id', 'user_id','date', 'full_text', 'retweets', 'sentiment']
    original_dfs, data_files, reply_RT_dfs, delta_dates_updt = [], [] , [], []

    #initialization
    def __init__(self, project_name, load_type):
        """ Instantiates an instance of the Twython Cleanser.  Takes one input and sets up
            the correct connection

            ::param project_name: The project that will get twitter data cleansed
            ::param load_type: What type of load will happen.  (FULL, DELTA)
        """
        #declare original properties
        self.project_name = project_name
        self.load_type = load_type
        self.proj_data_dir = self.curr_dir+ '/' + self.project_name + '/Data/'
        self.orig_tweet_dir = self.proj_data_dir + 'OriginalTweets/'
        self.tmp_file = self.proj_data_dir + 'OriginalTweets/tmp.csv'
        self.analytics_file = self.curr_dir+ '/' + self.project_name + '/' + self.project_name + 'CalculatedData.csv'
        self.current_date = datetime.today().strftime('%Y%m%d')
        self.getGroupsToClean()
        self.all_groups.remove('OriginalTweets')

    #Basic Methods
    def getGroupsToClean(self):
        """
        Get the list of groups to be analyzed in this project and return a list so that the
        tweets can be processed.  This uses the current raw file path
        returns - Sets the list of all the groups to be analyzed
        """
        logging.info("Getting List of all teams")
        self.all_groups = [f for f in listdir(self.proj_data_dir) if isdir(join(self.proj_data_dir, f))]

    def getFilesInDir(self, directory):
        """
        Get the files in the specified directory.  This is used to ensure that the right files are analyzed
        :: param  Directory: The directory that we desire the files from
        returns - All the files names in the specified directory.
        """
        return [f for f in listdir(directory) if isfile(join(directory, f)) and f.split('.')[1] == 'csv']

    def convertToCentralTimeZone(self, dt_tm):
        """
        Convert the date time stamp that is in the Twitter API return from GMT to CST
        :: param dt_tm: The datetime that we want to change to central time zone
        returns - DateTime as a String
        """
        fmt = '%a %b %d %H:%M:%S %z %Y'
        tz = timezone('America/Chicago')
        return datetime.strptime(dt_tm, fmt).astimezone(tz).strftime(fmt)

    def getDateFromDateTime(self, x):
        """
        Get the date from each twitter record as a distinct day.  Reformat Properly
        ::param  x : String of the twitter date
        returns- a String with the date in the format of YYYYMMDD
        """
        return ''.join([x.split(' ')[i] for i in [1,2,5]])

    def getDeltaDatesToUpdate(self):
        """
        Returns an array of the delta dates to update for the sentiment analysis portion
        """
        if self.load_type == 'FULL':
            return None
        else:
            return self.delta_dates_updt

    #DataFrame Objects to write data
    def writeCleansedTwitterData(self, dataframe, output_structure, clean_file, delta):
        """
        ::param dataframe: Takes a dataframe as an object and writes it to a csv file
        ::param file_path: the file to write the dataframe object to
        Take the cleansed data and right it to the correct file.  Ensure that all the fields are formatted
        correctly.  Set as a variable all the dates that need to be updated
        """
        if len(dataframe['date_as_str'].unique()) > len(self.delta_dates_updt):
            self.delta_dates_updt = dataframe['date_as_str'].unique()

        for distinct_date in dataframe['date_as_str'].unique():
            if delta:
                orig_tweet_file = clean_file + distinct_date + '.csv'
                #if there is a file, go through the data and clear out any duplicate records
                if isfile(orig_tweet_file):
                    list_of_ids = dataframe.loc[dataframe['date_as_str'] == distinct_date]['id'].values.tolist()
                    with open(orig_tweet_file, 'r', encoding = 'utf-8') as inp, open(self.tmp_file, 'w', encoding = 'utf-8') as out:
                        #get the files and write the output to a new tmp file
                        writer = csv.writer(out, delimiter = '\t')
                        for row in csv.reader(inp, delimiter = '\t'):
                            #write rows that have distinct values, pass over empty rows
                            try:
                                if row[0] not in list_of_ids:
                                    writer.writerow(row)
                            except:
                                pass
                    #rename the files and ensure the original data is properly situated
                    remove(orig_tweet_file)
                    rename(self.tmp_file, orig_tweet_file)

            dataframe.loc[dataframe['date_as_str'] == distinct_date]\
                            [output_structure].\
                            to_csv(clean_file + distinct_date + '.csv',
                                    mode = 'a',
                                    sep = '\t',
                                    index = False,
                                    header = False,
                                    line_terminator = '\n')

    def uploadTweetsIntoCleanser(self):
        """
        Upload the tweets into the cleanser and insert the original tweets for
        each team in a cleansed datafile.  Takes the type of load and iterates through all of the
        different groups that are a part of the study.
        """
        logging.info("Going through Raw Tweets to Cleanse")
        for group in self.all_groups:
            logging.info("Going through tweets for team %s" % group)
            raw_data = self.proj_data_dir + group + '/RawData/'
            cleansed_data = self.proj_data_dir + group + '/CleansedData/'

            #get the files to be loaded, and remove any potential files that could have duplicate dateata
            if self.load_type == 'FULL':
                raw_data_files = self.getFilesInDir(raw_data)
                for data_file in self.getFilesInDir(cleansed_data):
                    remove(cleansed_data + data_file)
                for data_file in self.getFilesInDir(self.orig_tweet_dir):
                    remove(self.orig_tweet_dir + data_file)
            else:
                raw_data_files = [group + self.current_date + '.csv']

            for data_file in raw_data_files:
                #get original tweets and re-format the date
                data_DF = pd.read_csv(raw_data + data_file,
                                    sep = '\t',
                                    index_col = False,
                                    encoding = 'utf-8',
                                    names = ['id','user_id', 'date', 'full_text', 'replied_to_id', 'retweeted_id'],
                                    converters = {'full_text':lambda x:x.replace('\n','').replace('\r',''),
                                                   'date': lambda x: self.convertToCentralTimeZone(x)},
                                    lineterminator = '\n').fillna('0')
                data_DF.loc[:, 'date_as_str'] = data_DF['date'].apply(self.getDateFromDateTime)

                original_tweet_DF = data_DF.loc[(data_DF['replied_to_id'] == '0') & (data_DF['retweeted_id'] == '0')].copy()
                self.original_dfs.append(original_tweet_DF)
                self.reply_RT_dfs.append(data_DF.loc[(data_DF['replied_to_id'] != '0') | (data_DF['retweeted_id'] != '0')].copy())
                self.data_files.append((cleansed_data, group))

                #if it is an original load, load all of the tweets to the correct file
                original_tweet_DF.loc[:, 'retweets', ] = 0
                original_tweet_DF.loc[:, 'sentiment'] = 0
                self.writeCleansedTwitterData(original_tweet_DF, self.clean_column_names,
                                                cleansed_data + group, False)

    def updateOriginalTweets(self):
        """
        Cleanse the original Tweet data and position in the cleansed tweet data for each group.
        Also store the original tweets in a central location of all the teams
        """
        logging.info("Writing Original Tweets to Central File")
        for distinct_DF in self.original_dfs:
            self.writeCleansedTwitterData(distinct_DF, ['id','user_id', 'date', 'full_text'],
                                    self.orig_tweet_dir + 'OriginalTweetsOn', True)

    def cleanseRepliedTweets(self):
        """
        Cleanse the data for the replied data and iterate over all of the replied data to
        add the full text to each tweet.  Once the replied data has been added, insert the "cleaned"
        data to the dataframe
        """
        original_files = self.getFilesInDir(self.orig_tweet_dir)
        logging.info("Going through Replied Data and retweets")
        for index, clean_data_DF in enumerate(self.reply_RT_dfs):
            replied_data_DF = pd.DataFrame(columns = self.clean_column_names)
            clean_data_DF.loc[:, 'replied_to_id'] = clean_data_DF['replied_to_id'].astype('int')
            for orig_file in original_files:
                original_tweets_DF = pd.read_csv(self.orig_tweet_dir + orig_file,
                                                sep = '\t',
                                                index_col = False,
                                                encoding = 'utf-8',
                                                names = ['id_num','user_id', 'date', 'full_orig_text'])

                original_tweets_DF.loc[:, 'id_num'] = original_tweets_DF['id_num'].astype('int')

                new_DF = pd.merge(clean_data_DF,
                         original_tweets_DF[['id_num','full_orig_text']],
                         how = 'left',
                         left_on = ['replied_to_id'],
                         right_on = ['id_num']).copy().dropna(subset = ['id_num'])
                new_DF.loc[:, 'full_text'] = new_DF['full_orig_text'] + '|| ->' +  new_DF['full_text']

                replied_data_DF = pd.concat([new_DF[['id', 'user_id','date', 'full_text', 'date_as_str']], replied_data_DF], sort = False)

            self.writeCleansedTwitterData(replied_data_DF, self.clean_column_names,
                                        self.data_files[index][0] + self.data_files[index][1],  False)

    def sumRetweets(self):
        """
        Sum the number of retweets in each specific dataframe and add the final counts to the cleaned tweet
        files.  Add all the retweeted id's to the same dataframe for each group and then when a new group is reached,
        add the sum counts to the cleaned tweets
        """
        logging.info("Summing Retweet Counts")
        df_retweet_DF = pd.DataFrame(columns = ['retweeted_id'])
        for index, replied_dataframe in enumerate(self.reply_RT_dfs):
            replied_dataframe.loc[:, 'retweeted_id'] = replied_dataframe['retweeted_id'].fillna('0').astype('int')
            df_retweet_DF = pd.concat([replied_dataframe[['retweeted_id']], df_retweet_DF], sort = False)
            if index + 1 == len(self.reply_RT_dfs) or self.data_files[index][0] != self.data_files[index + 1][0]:
                full_retweets_DF = replied_dataframe[['retweeted_id']].groupby('retweeted_id').size().reset_index(name = 'counts')
                for file in self.getFilesInDir(self.data_files[index][0]):
                    cleaned_tweets = pd.read_csv(self.data_files[index][0] + file,
                                                    sep = '\t',
                                                    index_col = False,
                                                    names = ['id', 'user_id','date', 'full_text', 'retweets', 'sentiment'])


                    new_DF = pd.merge(cleaned_tweets,
                                      full_retweets_DF,
                                      how = 'left',
                                      left_on = ['id'],
                                      right_on = ['retweeted_id'])

                    #new_DF.drop_duplicates(keep= 'first', inplace=True)
                    new_DF.loc[:,'retweets'] = new_DF['counts'].fillna(0) + new_DF['retweets'].fillna(0)
                    new_DF[['id', 'user_id','date', 'full_text', 'retweets', 'sentiment']]\
                            .to_csv(self.data_files[index][0] + file,
                            mode = 'w',
                            sep = '\t',
                            index = False,
                            header = False)
                #cleaned the retweet dataframe and move on to the next team.
                df_retweet_DF.iloc[0:0]

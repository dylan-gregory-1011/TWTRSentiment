##!/usr/bin/env python
"""
Twitter Cleanser to prepare the tweets for analysis
Version 2: Added compression to all of the data as well as updated the reply process to have more
          optimal performance
Version 3: Cleansed Text data into a sqlite database and re-arranged the raw data files into a new structure
"""

#imports
import sys
from os import listdir, remove, rename
from pathlib import Path
from os.path import isdir, join, isfile
import logging
from collections import defaultdict
import pandas as pd
from pytz import timezone
from datetime import datetime, date
logging.basicConfig(stream=sys.stdout, level = logging.INFO)

__author__ = "Dylan Smith"
__copyright__ = "Copyright (C) 2018 Dylan Smith"
__credits__ = ["Dylan Smith"]

__license__ = "Personal Use"
__version__ = "3.0"
__maintainer__ = "Dylan Smith"
__email__ = "-"
__status__ = "Development"

class TwitterCleanser(object):
    #constants
    clean_column_names = ['id', 'user_id','datetime', 'day', 'full_text', 'retweets', 'sentiment']
    original_column_names = ['id','user_id', 'datetime', 'day', 'full_text']
    tmp_rply_column_names = ['id','user_id', 'datetime', 'day', 'full_text', 'replied_to_id', 'retweeted_id']
    original_dfs, delta_dates_updt, reply_RT_dfs = [], [], defaultdict(list)
    original_fields = "id BIGINT PRIMARY KEY, user_id TEXT, datetime TEXT, day TEXT, full_text TEXT"
    table_fields = original_fields + ", retweets INT,  sentiment REAL"
    tmp_rply_fields = original_fields + ", replied_to_id BIGINT, retweeted_id BIGINT"

    #initialization
    def __init__(self, proj_data_dir, db_connection, load_type):
        """ Instantiates an instance of the Twython Cleanser.  Takes one input and sets up
            the correct connection

            ::param proj_data_dir: The directory where the Twitter data is dropped into
            ::param db_connection: a database connection to a sqlite database
            ::param load_type: What type of load will happen.  (FULL, DELTA)
        """
        #declare original properties
        self.load_type = load_type
        self.proj_data_dir = proj_data_dir
        self.connection = db_connection
        self.current_date = datetime.today().strftime('%Y%m%d')
        self.all_groups = [f for f in listdir(self.proj_data_dir) if isdir(join(self.proj_data_dir, f))]
        self.executeSQLCommand("CREATE TABLE IF NOT EXISTS OriginalTweets (%s)" % self.original_fields)
        self.executeSQLCommand("CREATE TABLE IF NOT EXISTS Tmp_Rply (%s)" % self.tmp_rply_fields)
        self.executeSQLCommand("CREATE TABLE IF NOT EXISTS Retweets_To_Add (retweeted_id BIGINT PRIMARY KEY, count INT)")

    #Basic Methods
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

    def getWeekFromDate(self, x):
        """
        Get the week number from the date

        ::param x: A string as a date in the format YYYYMMDD
        returns - a week date for the year.
        """
        month = int(self.calendar.index(x[:3])) + 1
        dt = date(int(x[-4:]), month, int(x[3:5]))
        return 'Week' + str(dt.isocalendar()[1]) + '|' + x[-4:]

    def getDaysToUpdate(self):
        """
        Returns an array of the delta dates to update for the sentiment analysis portion
        """
        if self.load_type == 'FULL':
            return None
        else:
            return self.delta_dates_updt

    def writeCleansedTwitterData(self, dataframe, group, column_struct):
        """
        ::param dataframe: Takes a dataframe as an object and writes it to a csv file
        ::param group: the group that the data is related to.  This identifies the table to write in.
        ::param column_struct: the column structure for the output of the data
        Take the cleansed data and write it to the correct table.
        """
        if group == 'OriginalTweets':
            field_len = 5
        else:
            field_len = 7
        data = tuple(dataframe[column_struct].itertuples(index = False))
        wildcards = ','.join(['?'] * field_len)
        insert_sql = """INSERT OR IGNORE INTO %s VALUES (%s)""" % (group, wildcards)
        self.executeSQLCommand(insert_sql, data)

    def executeSQLCommand(self, sql_stmnt, data= None):
        """
        ::param sql_stmt: the SQL statement to run on the connection
        ::param data(Optional): The data to be loaded into the sql statement
        """
        if data is not None:
            self.connection.executemany(sql_stmnt, data)
        else:
            self.connection.execute(sql_stmnt)
        self.connection.commit()

    def uploadTweetsIntoCleanser(self):
        """
        Upload the tweets into the cleanser and insert the original tweets for
        each team in a cleansed datafile.  Takes the type of load and iterates through all of the
        different groups that are a part of the study.
        """
        logging.info("Going through Raw Tweets to Cleanse")
        for group in self.all_groups:
            logging.info("Going through tweets for team %s" % group)
            self.executeSQLCommand("CREATE TABLE IF NOT EXISTS %s (%s)" % (group, self.table_fields))

            raw_data_dir = self.proj_data_dir.joinpath(group)
            #get the files to be loaded, and remove any potential files that could have duplicate dateata
            if self.load_type == 'FULL':
                raw_data_files = [f for f in listdir(raw_data_dir) if isfile(join(raw_data_dir, f)) and f.split('.')[1] == 'csv']
                self.executeSQLCommand('DELETE FROM %s' % group)
                self.executeSQLCommand('DELETE FROM OriginalTweets')
            else:
                raw_data_files = [group + self.current_date + '.csv.gz']

            for data_file in raw_data_files:
                #get original tweets and re-format the date
                data_DF = pd.read_csv(raw_data_dir.joinpath(data_file),
                                    compression = 'gzip',
                                    sep = '\t',
                                    index_col = False,
                                    encoding = 'utf-8',
                                    names = ['id','user_id', 'datetime', 'full_text', 'replied_to_id', 'retweeted_id'],
                                    converters = {'full_text':lambda x:x.replace('\n','').replace('\r',''),
                                                   'datetime': lambda x: self.convertToCentralTimeZone(x),
                                                   'replied_to_id': lambda x:  str(x) if x else '0',
                                                   'retweeted_id': lambda x: str(x) if x else '0'},
                                    lineterminator = '\n')
                data_DF.loc[:, 'day'] = data_DF['datetime'].apply(self.getDateFromDateTime)

                original_tweet_DF = data_DF.loc[(data_DF['replied_to_id'] == '0') & (data_DF['retweeted_id'] == '0')].copy()
                self.original_dfs.append(original_tweet_DF)
                self.reply_RT_dfs[group].append(data_DF.loc[(data_DF['replied_to_id'] != '0') | (data_DF['retweeted_id'] != '0')].copy())
                #if it is an original load, load all of the tweets to the correct file
                original_tweet_DF.loc[:, 'retweets'] = 0
                original_tweet_DF.loc[:, 'sentiment'] = 0

                self.delta_dates_updt += list(data_DF['day'].unique())
                self.delta_dates_updt = list(set(self.delta_dates_updt))
                self.writeCleansedTwitterData(original_tweet_DF, group, self.clean_column_names)

    def updateOriginalTweets(self):
        """
        Cleanse the original Tweet data and position in the cleansed tweet data for each group.
        Also store the original tweets in a central location of all the teams
        """
        logging.info("Writing Original Tweets to Central File")
        for distinct_DF in self.original_dfs:
            self.writeCleansedTwitterData(distinct_DF, 'OriginalTweets', self.original_column_names)

    def cleanseRepliedTweets(self):
        """
        Cleanse the data for the replied data and iterate over all of the replied data to
        add the full text to each tweet.  Once the replied data has been added, insert the "cleaned"
        data to the dataframe
        """
        logging.info("Going through Replied Data")
        for group in self.all_groups:
            self.executeSQLCommand("DELETE FROM Tmp_Rply")
            for df in self.reply_RT_dfs[group]:
                self.writeCleansedTwitterData(df, 'Tmp_Rply', self.tmp_rply_column_names)

            #insert all the replied tweets to the table
            join_data_sql = """INSERT OR IGNORE INTO %s SELECT a.id, a.user_id, a.datetime,
                a.day, (ifnull(b.full_text, ' ') || ' || -> ' || a.full_text), 0 as retweets, 0 as sentiments
                FROM Tmp_Rply a LEFT OUTER JOIN OriginalTweets b ON (a.replied_to_id = b.id) WHERE a.replied_to_id <> 0""" % group
            self.executeSQLCommand(join_data_sql)
            self.executeSQLCommand("INSERT INTO Retweets_To_Add SELECT retweeted_id, COUNT(retweeted_id) as Count FROM Tmp_Rply GROUP BY retweeted_id")
            print("Summing Retweet values for %s" % group)
            sum_sql = """UPDATE {0} SET Retweets = Retweets + (SELECT Count FROM Retweets_To_Add t1 WHERE {0}.id = t1.retweeted_id) \
            WHERE EXISTS (SELECT * FROM Retweets_To_Add WHERE {0}.id = Retweets_To_Add.retweeted_id);""".format(group)
            self.executeSQLCommand(sum_sql)
            self.executeSQLCommand("DELETE FROM Retweets_To_Add")

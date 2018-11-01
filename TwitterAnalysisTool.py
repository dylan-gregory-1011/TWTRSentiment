##!/usr/bin/env python
"""
Twitter Sentiment Analysis Project: Utilizes three distinct objects in order to download tweets
from the Twitter Search API, cleanse the data so that it can be stored properly, and apply a sentiment tool
to the tweets to try to gauge the daily sentiment for each group.

::param ProjectGroup: The group that the project is to be run in 
::param ProjectName: Sub name of the project.

Version 1.0: Initial Scope of the project.  This includes the objects that perform the ETL and analysis
as well as the driver program (This file).

Version 2.0:
    - Changed the path functionality to the python 3 pathlib to ensure that the program runs fine on any
    operating system.  Initially it was just set up for macOS
    - Changed all datasets to be compressed gz files.
    - Updated the reply process to improve performance
    - Changed the Sentiment Tool to be vaderSentiment
    - Calculates the activity for each group, per day
    - Reformatted the query data to be saved in a json file
    - Created the TwitterAnalysisTool class to drive the daily processing of twitter activity as well
        as wrapped the process in a bash script so that multiple projects can be run at once
Version 3.0:
    -Updated to fit within a structure that allowed multiple projects to be used
    -Inserted cleansed data into an SQLite database.  Also tried to trim the amouht of distinct files
"""
#Imports
import sys
from pathlib import Path
from PythonDataModules.TwitterScraper import TwitterScraper
from PythonDataModules.TwitterCleanser import TwitterCleanser
from PythonDataModules.TwitterSentimentAnalyzer import TwitterSentimentAnalyzer
import logging
logging.basicConfig(stream=sys.stdout, level = logging.INFO)
import sqlite3 as db
import json

__author__ = "Dylan Smith"
__copyright__ = "Copyright (C) 2018 Dylan Smith"
__credits__ = ["Dylan Smith"]

__license__ = "Personal Use"
__version__ = "3.0"
__maintainer__ = "Dylan Smith"
__email__ = "-"
__status__ = "Development"

class TwitterAnalysisTool(object):
    #constants
    curr_dir = Path(__file__).resolve().parent

    #initialization
    def __init__(self, project_area, project_name):
        self.proj_nm = project_name
        self.proj_data_dir = self.curr_dir.joinpath('DataSources','Twitter', project_name)
        self.proj_analysis_dir = self.curr_dir.joinpath(project_area, project_name)
        self.api_key = self.curr_dir.joinpath('APIKeys', project_name + 'TwitterAPIKeys.json')
        self.connection = db.connect(str(self.proj_analysis_dir.joinpath('CleansedData.db')))

    def downloadRecentTwitterActivity(self):
        """
        This function updates the twitter dataset and downloads all relevant Tweets for each group that is part of
        the project.  Twitter API specifications are set in this function so that the parameters outside of the query
        terms are all the same
        """
        twython_scraper = TwitterScraper(proj_data_dir = self.proj_data_dir,
                                        proj_analysis_dir = self.proj_analysis_dir,
                                        api_key_file = self.api_key)
        logging.info('-- Twython object instantiated for %s--' % self.proj_nm)
        qry_file = twython_scraper.returnQueriesToRun()
        with open(qry_file, "r") as file:
             prj_qry_data = json.load(file)

        #set the initial parameters to be used by all queries.  Make sure to copy so as to not edit the initial dict
        grp_qry = prj_qry_data["SearchParameters"].copy()
        #iterate over all the teams and update the table after each file has completed
        for grp_nm, grp_data in prj_qry_data["GroupQueries"].items():
            logging.info("Downloading Tweets for %s" % grp_nm)
            grp_qry["q"] = grp_data["Query"]

            #download tweets and then update the json file that drives the project
            new_max, last_updt = twython_scraper.downloadHistoricalTweets(grp_nm, grp_qry, grp_data['MaxRecord'])
            prj_qry_data["GroupQueries"][grp_nm]['MaxRecord'] = new_max
            prj_qry_data["GroupQueries"][grp_nm]['LastUpdate'] = last_updt

            with open(qry_file, "w") as fp:
                json.dump(prj_qry_data, fp, indent=2)
            logging.info("Finished Downloading Tweets for %s" % grp_nm)

    def processAndStoreData(self, load_type):
        """
        This function instantiates a twitter cleanser object as well as a twitter sentiment object.
        It takes a load type and cleanses/applies sentiment tools to that subset of data

        params load_type: Takes a string input and specifies which records should be classified.  It is either
                          a delta load or a full load.
        """
        self.twitter_cleanser = TwitterCleanser(proj_data_dir = self.proj_data_dir
                                                ,db_connection= self.connection
                                                ,load_type = load_type)
        self.twitter_cleanser.uploadTweetsIntoCleanser()
        self.twitter_cleanser.updateOriginalTweets()
        self.twitter_cleanser.cleanseRepliedTweets()

    def calculateSentiment(self):
        """
        The function that calculates sentiment for the tweets in question.  This 'Delta' vs 'Full' functionality
        is set by the datesToUpdate field that is specified below.  If no dates are specified then the full run
        is calculated
        """
        twitter_sentiment = TwitterSentimentAnalyzer(proj_data_dir = self.proj_data_dir
                                                ,proj_analysis_dir = self.proj_analysis_dir
                                                ,db_connection = self.connection
                                                ,days_to_update = self.twitter_cleanser.getDaysToUpdate())
        twitter_sentiment.calculateSentimentForTweets()

    def createInitialDirectories():
        prj_data_path = Path(__file__).resolve().parent.joinpath(project_area ,project_name, 'Data')
        for grp_nm, grp_data in prj_qry_data["GroupQueries"].items():
            prj_data_path.joinpath(grp_nm, "RawData").mkdir(parents = True)
            prj_data_path.joinpath(grp_nm, "CleansedData").mkdir(parents = True)
        return None

def main(project_area, project_name):
    """
    The driver function for the TwitterAnalysisTool.  This instantiates an object for each project and goes through
    and downloads the recent twitter activity, processes and stores the data, and then calculates the sentiment for the data
    ::param project_area: The project area that is being processed. This corresponds to the folder that will be used
    ::param project_name: The project name that will be used to drive the Analysis
    """
    twitter_analysis = TwitterAnalysisTool(project_area=project_area,
                                            project_name=project_name)
    twitter_analysis.downloadRecentTwitterActivity()
    twitter_analysis.processAndStoreData('FULL')
    twitter_analysis.calculateSentiment()
    print('Download for %s has completed' % project_name)


if __name__ == '__main__':
    #Run the process for the project in question
    #sample statement to run >>python3 TwitterAnalysisTool.py SportsSentiment NFL
    main(sys.argv[1], sys.argv[2])

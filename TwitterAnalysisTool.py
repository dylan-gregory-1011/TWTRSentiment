##!/usr/bin/env python
"""
Twitter Sentiment Analysis Project: Utilizes three distinct objects in order to download tweets
from the Twitter Search API, cleanse the data so that it can be stored properly, and apply a sentiment tool
to the tweets to try to gauge the daily sentiment for each group.

:: param ProjectName: The project name that you want to run.

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

"""

#Imports
import sys
from pathlib import Path
from TwitterScraper import TwitterScraper
from TwitterCleanser import TwitterCleanser
from TwitterSentimentAnalyzer import TwitterSentimentAnalyzer
from multiprocessing import Pool
import logging
logging.basicConfig(stream=sys.stdout, level = logging.INFO)
import json

__author__ = "Dylan Smith"
__copyright__ = "Copyright (C) 2018 Dylan Smith"
__credits__ = ["Dylan Smith"]

__license__ = "Personal Use"
__version__ = "2.0"
__maintainer__ = "Dylan Smith"
__email__ = "-"
__status__ = "Development"

class TwitterAnalysisTool(object):
    #constants
    curr_dir = Path(__file__).resolve().parent

    #initialization
    def __init__(self, project_name):
        self.prj_nm = project_name

    def downloadRecentTwitterActivity(self):
        """
        This function updates the twitter dataset and downloads all relevant Tweets for each group that is part of
        the project.  Twitter API specifications are set in this function so that the parameters outside of the query
        terms are all the same

        ::params - project_name: takes a string input for the project name and accesses the correct folder as well as the
                                 inputs the data into the correct data folders
        """
        self.twython_scraper = TwitterScraper(project_name = self.prj_nm)

        qry_file = self.twython_scraper.returnQueriesToRun()
        with open(qry_file, "r") as file:
             prj_qry_data = json.load(file)

        #set the initial parameters to be used by all queries.  Make sure to copy so as to not edit the initial dict
        grp_qry = prj_qry_data["SearchParameters"].copy()
        #iterate over all the teams and update the table after each file has completed
        for grp_nm, grp_data in prj_qry_data["GroupQueries"].items():
            logging.info("Downloading Tweets for %s" % grp_nm)
            grp_qry["q"] = grp_data["Query"]

            #download tweets and then update the json file that drives the project
            new_max, last_updt = self.twython_scraper.downloadHistoricalTweets(grp_nm, grp_qry, grp_data['MaxRecord'])
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
        self.twitter_cleanser = TwitterCleanser(self.prj_nm, load_type)
        self.twitter_cleanser.uploadTweetsIntoCleanser()
        self.twitter_cleanser.updateOriginalTweets()
        self.twitter_cleanser.cleanseRepliedTweets()
        self.twitter_cleanser.sumRetweets()

    def calculateSentiment(self):
        """
        The function that calculates sentiment for the tweets in question.  This 'Delta' vs 'Full' functionality
        is set by the datesToUpdate field that is specified below.  If no dates are specified then the full run
        is calculated
        """
        self.datesToUpdate = self.twitter_cleanser.getDeltaDatesToUpdate()
        self.twitter_sentiment = TwitterSentimentAnalyzer(self.prj_nm, self.datesToUpdate)
        self.twitter_sentiment.calculateSentimentForTweets()

    def createInitialDirectories():
        prj_data_path = Path(__file__).resolve().parent.joinpath(project_name, 'Data')
        for grp_nm, grp_data in prj_qry_data["GroupQueries"].items():
            prj_data_path.joinpath(grp_nm, "RawData").mkdir(parents = True)
            prj_data_path.joinpath(grp_nm, "CleansedData").mkdir(parents = True)
        return None

def main(project_name):
    """
    The driver function for the TwitterAnalysisTool.  This instantiates an object for each project and goes through
    and downloads the recent twitter activity, processes and stores the data, and then calculates the sentiment for the data
    """
    twitter_analysis = TwitterAnalysisTool(project_name=project_name)
    twitter_analysis.downloadRecentTwitterActivity()
    twitter_analysis.processAndStoreData('DELTA')
    twitter_analysis.calculateSentiment()
    print('Download for %s has completed' % project_name)


if __name__ == '__main__':
    #Run the process for the project in question
    main(sys.argv[1])

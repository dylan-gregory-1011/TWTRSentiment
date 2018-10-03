# TWTRSentiment
Includes the objects that are being used to download twitter data, cleanse the text data and then apply a sentiment tool.
There are three objects that will accomplish different tasks in the process of sentiment analysis using twitter.  
In order to properly run the code, the following structure needs to be followed.

|-TwitterTool.py
|-TwitterCleanser.py
|-TwitterScraper.py
|-TwitterSentimentAnalysis.py
|- ProjectName
|  |- Data 
|    |- Group 1
|      |-RawData
|      |-CleansedData
|    |- Group 2
|      |-RawData
|      |-CleansedData
|    .
|    .
|    .
|    |- Group N
|      |-RawData
|      |-CleansedData
|  ProjectNameCalculatedData.csv
|  ProjectNameKeys.json
|  ProjectNameQueryData.csv

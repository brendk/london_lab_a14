# -*- coding: utf-8 -*-
"""
Created on Fri Jun  4 00:01:03 2021

@author: brend
"""

from iLox import iLox
from iLox.objects.data_prep import DataPrep
from iLox.objects.headlines_match import HeadlinesMatch
from iLox.objects.tweets_match import TweetsMatch
from iLox.objects.clusters import RefEventsClusters
from iLox.objects.scrape_tweets import TweetsScraping


params_file = "iLox_params.json"


if __name__ == "__main__":    
    ilox = iLox(params_file)
    tweets_scraping = TweetsScraping(ilox)
    data_prep = DataPrep(ilox)
    headlines_match = HeadlinesMatch(ilox)
    tweets_match = TweetsMatch(ilox)
    ref_events_clusters = RefEventsClusters(ilox)
    ilox.run(tweets_scraping, 
             data_prep, 
             headlines_match, 
             tweets_match, 
             ref_events_clusters)
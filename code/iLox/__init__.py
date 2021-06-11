# -*- coding: utf-8 -*-
"""
Created on Fri Jun  4 00:39:03 2021

@author: brend
"""

import json
import pandas as pd
import pymongo
from pymongo import MongoClient
from iLox.objects.ilox_logger import iLoxLogger
from iLox.dependencies.bulk_write import bulk_write
from geopy.geocoders import Nominatim


class iLox():
    
    def __init__(self, params_file):
        self.params_file = params_file
        print("Reading params ...")
        self._get_params()
        self.ilox_logger = iLoxLogger(self.logging_params)
        print("Establishing MongoDb connections ...")
        self.mycol_tweets =  MongoClient(
            self.dbs_params["mongoDB_Host"], 
            27017)[self.dbs_params["mongoDB_Db"]][self.dbs_params["mongoDB_Col_Tweets"]]
        self.mycol_headlines =  MongoClient(
            self.dbs_params["mongoDB_Host"], 
            27017)[self.dbs_params["mongoDB_Db"]][self.dbs_params["mongoDB_Col_Headlines"]]
        self.mycol_merged =  MongoClient(
            self.dbs_params["mongoDB_Host"], 
            27017)[self.dbs_params["mongoDB_Db"]][self.dbs_params["mongoDB_Col_Merged"]]
        self.mycol_gpes =  MongoClient(
            self.dbs_params["mongoDB_Host"], 
            27017)[self.dbs_params["mongoDB_Db"]][self.dbs_params["mongoDB_Col_GPEs"]]
        self.mycol_refineries =  MongoClient(
            self.dbs_params["mongoDB_Host"], 
            27017)[self.dbs_params["mongoDB_Db"]][self.dbs_params["mongoDB_Col_Refineries"]]
        print("Connecting to Nominatim ...")
        self.geolocator = Nominatim(user_agent = self.data_prep_params["nominatim_user_agent"])
        
        
    # Get params from json file
    def _get_params(self):
        with open(self.params_file) as f:
            self.all_params = json.load(f)
        self.dbs_params = self.all_params["databases_params"]
        self.logging_params = self.all_params["logging"]
        self.tweets_scraping_params = self.all_params["tweets_scraping"]
        self.data_prep_params = self.all_params["data_preparation"]
        self.ref_match_params = self.all_params["refineries_matching"]
        self.events_match_params = self.all_params["events_matching"]
        self.clustering_params = self.all_params["clustering"]
        
    
    # Store all Tweets/Headlines matched to at least 1 refinery in same database
    def merge_tweets_headlines(self):
        print("Merging Tweets and Headlines ...")
        # Get all Tweets
        all_tweets = list(self.mycol_tweets.find(
            {"ref_match": {"$not": {"$size": 0}}, "events_tags": {"$not": {"$size": 0}}}, 
            {"created_at": 1, "id_str": 1, "full_text": 1, "entities": 1, "user_id_str": 1, 
             "retweet_count": 1, "favorite_count": 1, "reply_count": 1, "quote_count": 1, 
             "favorited": 1, "retweeted": 1, "geo_tags": 1, "owner_tags": 1, "ref_match": 1, 
             "events_tags": 1, "country_match": 1, "_id": 0}))
        # Change keys for consistency with headlines
        convert_keys_tweets = {"created_at": "created_at", "id_str": "id", "full_text": "text", 
                               "entities": "entities", "user_id_str": "source_id", 
                               "retweet_count": "retweet_count", "favorite_count": "favorite_count", 
                               "reply_count": "reply_count", "quote_count": "quote_count", 
                               "favorited": "favorited", "retweeted": "retweeted", 
                               "geo_tags": "geo_tags", "owner_tags": "owner_tags", 
                               "ref_match": "ref_match", "events_tags": "events_tags", 
                               "country_match": "country_match", "_id": "_id"}
        all_tweets = [{convert_keys_tweets[k]: tweet[k] for k in tweet.keys()} 
                      for tweet in all_tweets]
        # Get all headlines
        all_headlines = list(self.mycol_headlines.find(
            {"ref_match": {"$not": {"$size": 0}}, "events_tags": {"$not": {"$size": 0}}}, 
            {"firstCreated": 1, "storyId": 1, "text": 1, "snippet": 1, "sourceName": 1, 
             "geo_tags": 1, "owner_tags": 1, "ref_match": 1, "events_tags": 1, "country_match": 1, 
             "cluster_id": 1, "_id": 0}))
        # Change keys for consistency with Tweets
        convert_keys_headlines = {"firstCreated": "created_at", "storyId": "id", "text": "text", 
                                  "snippet": "snippet", "sourceName": "source_id", 
                                  "geo_tags": "geo_tags", "owner_tags": "owner_tags", 
                                  "ref_match": "ref_match", "events_tags": "events_tags", 
                                  "country_match": "country_match", "cluster_id": "cluster_id", 
                                  "_id": "_id"}
        all_headlines = [{convert_keys_headlines[k]: headline[k] for k in headline.keys()} 
                         for headline in all_headlines]
        # Create dataframe to set missing keys to NaN
        all_items = pd.DataFrame(all_tweets + all_headlines)
        # Change columns order
        all_items = all_items[["id", "created_at", "text", "snippet", "source_id", "retweet_count", 
                               "favorite_count", "reply_count", "quote_count", "favorited", 
                               "retweeted", "entities", "geo_tags", "owner_tags", "ref_match", 
                               "events_tags", "country_match", "cluster_id"]]
        # Drop duplicates
        all_items = all_items.drop_duplicates(subset = ["created_at", "text", "snippet"])
        # Convert back to dictionary
        all_items = all_items.to_dict(orient = "records")
        # Create indexes if not already exist
        existing_idx = self.mycol_merged.index_information()
        if "id" not in existing_idx.keys():
            self.mycol_merged.create_index(
                [("id", pymongo.ASCENDING)], name = "id", unique = True)
        # Insert in collection
        status = bulk_write(self.mycol_merged, all_items)
        del status
        
        
    # Run
    def run(self, tweets_scraping, data_prep, headlines_match, tweets_match, ref_events_clusters):
        self.tweets_scraping = tweets_scraping
        self.data_prep = data_prep
        self.headlines_match = headlines_match
        self.tweets_match = tweets_match
        self.ref_events_clusters = ref_events_clusters
        if self.tweets_scraping_params["run"]:
            self.tweets_scraping._run()
        if self.data_prep_params["run"]:
            self.data_prep._run()
        else:
            self.data_prep._read()
        if self.ref_match_params["tweets_matching"]["run"]:
            self.tweets_match._run()
        if self.ref_match_params["headlines_matching"]["run"]:
            self.headlines_match._run()
        self.merge_tweets_headlines()
        if self.clustering_params["run"]:
            self.ref_events_clusters._run()
        print("\niLox complete")
        
        
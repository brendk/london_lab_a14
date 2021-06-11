# -*- coding: utf-8 -*-
"""
Created on Sat Jun  5 01:25:22 2021

@author: brend
"""

import sys
import os
import re
import spacy
import pymongo
import pandas as pd
from tqdm import tqdm
from datetime import datetime
from iLox.dependencies.get_match import get_match_proba


class TweetsMatch():
    
    def __init__(self, parent):
        attributes = [attr for attr in dir(parent) if not attr.startswith("__")]
        for attribute in attributes:
            setattr(self, attribute, getattr(parent, attribute))
        self._parent = parent
        # Load Spacy models
        self.spacy_models = [spacy.load(model) for model in self.ref_match_params["tweets_matching"]["nlp_models"]]
        if self.ref_match_params["tweets_matching"]["start"] is not None:
            self.tweets_start = self.ref_match_params["tweets_matching"]["start"]
        else:
            self.tweets_start = "1900-01-01"
        if self.events_match_params["start"] is not None:
            self.events_start = self.events_match_params["start"]
        else:
            self.events_start = "1900-01-01"
        
    def _get_parent_attrs(self):
        attributes = [attr for attr in dir(self._parent) if not attr.startswith("_")]
        for attribute in attributes:
            setattr(self, attribute, getattr(self._parent, attribute))
        
    # Perform data preparation process from scratch
    def _run(self):
        # Get parent attributes
        self._get_parent_attrs()
        # Prepare MongoDb col, remove old matchs
        self._prep_col()
        # Match Tweets with refineries names
        self._geotag_tweets_refineries()
        # Match Tweets with cities names
        self._geotag_tweets_cities()
        # Use Spacy to extract other locations (GPEs)
        self._geotag_tweets()
        # Match Tweets with owners names
        self._match_tweets_owners()
        # Get probabilities of match for refineries
        self._match_tweets()
        # Match to country/countries based on ref_match
        self._country_match()
        if self.events_match_params["run"]:
            # Extract event types from Tweets
            self._match_tweets_event_type()
        
    # Prepare MongoDb collection (add geo_tags keys and clean up previous matches)
    def _prep_col(self):
        # Create indexes if not already exist
        existing_idx = self.mycol_tweets.index_information()
        if "full_text" not in existing_idx.keys():
            self.mycol_tweets.create_index(
                [("full_text", pymongo.ASCENDING)], name = "full_text", unique = False)
        if "full_text_text" not in existing_idx.keys():
            self.mycol_tweets.create_index(
                [("full_text", pymongo.TEXT)], 
                name = "full_text_text", unique = False, 
                default_language = "en", language_override = "en")
        # Change date format
        wrong_dates_tweets = list(self.mycol_tweets.find(
            {"created_at": {"$regex": " +"}}, {"created_at": 1}))
        for this_tweet in wrong_dates_tweets:
            self.mycol_tweets.update_one(
                {"_id": this_tweet["_id"]}, 
                {"$set": {"created_at": datetime.strptime(
                    this_tweet["created_at"], "%a %b %d %H:%M:%S %z %Y"
                    ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")}})
        self.mycol_tweets.update_many(
            {"created_at": {"$gte": self.tweets_start}}, 
            {"$set": {"geo_tags": []}})
        self.mycol_tweets.update_many(
            {"created_at": {"$gte": self.tweets_start}}, 
            {"$set": {"owner_tags": []}})
        self.mycol_tweets.update_many(
            {"created_at": {"$gte": self.tweets_start}}, 
            {"$set": {"ref_match": []}})
        self.mycol_tweets.update_many(
            {"created_at": {"$gte": self.tweets_start}}, 
            {"$set": {"country_match": []}})
        self.mycol_tweets.update_many(
            {"created_at": {"$gte": self.tweets_start}}, 
            {"$set": {"events_tags": []}})
            
    # Geotag tweets from refineries names
    def _geotag_tweets_refineries(self):
        for geo_name in tqdm(self.data_prep.geo_names_r, 
                             disable = self.ilox_logger.display_pb(), 
                             desc = "Matching Tweets with refineries names", 
                             leave = True):
            # Get ids of Tweets containing current geoname
            these_ids = [i["_id"] for i in self.mycol_tweets.find(
                {"created_at": {"$gte": self.tweets_start}, 
                 "full_text": {"$regex": "\\b" + geo_name["match"] + "\\b", "$options": "i"}}, 
                {"_id": 1})]
            # Also perform text match using index (accents insensitive)
            these_ids.extend([i["_id"] for i in self.mycol_tweets.find(
                {"created_at": {"$gte": self.tweets_start}, 
                "$text": {"$search": "\"" + geo_name["match"] + "\""}}, 
                {"_id": 1})])
            # Add current geo_name to matching Tweets' geo_tags field
            if len(these_ids) > 0:
                self.mycol_tweets.update_many(
                    {"_id": {"$in": list(set(these_ids))}}, {"$push": {"geo_tags": geo_name}})
            
    # Geotag tweets from cities names
    def _geotag_tweets_cities(self):
        for geo_name in tqdm(self.data_prep.cities_names, 
                             disable = self.ilox_logger.display_pb(), 
                             desc = "Matching Tweets with cities names", 
                             leave = True):
            # Get ids of Tweets containing current geoname
            # Only look for whole word if len of geo_name is less than 8 (e.g. to get RichmondCA)
            if len(geo_name) < 8:
                these_ids = [i["_id"] for i in self.mycol_tweets.find(
                    {"created_at": {"$gte": self.tweets_start}, 
                     "full_text": {"$regex": "\\b" + geo_name["match"] + "\\b", "$options": "i"}}, 
                    {"_id": 1})]
            else:
                these_ids = [i["_id"] for i in self.mycol_tweets.find(
                    {"created_at": {"$gte": self.tweets_start}, 
                     "full_text": {"$regex": geo_name["match"], "$options": "i"}}, 
                    {"_id": 1})]
            # Also perform text match using index (accents insensitive)
            these_ids.extend([i["_id"] for i in self.mycol_tweets.find(
                {"created_at": {"$gte": self.tweets_start}, 
                 "$text": {"$search": "\"" + geo_name["match"] + "\""}}, 
                {"_id": 1})])
            # Add current geo_name to matching Tweets geo_tags field
            if len(these_ids) > 0:
                self.mycol_tweets.update_many(
                    {"_id": {"$in": list(set(these_ids))}}, 
                    {"$push": {"geo_tags": geo_name}})
                
    # Extract GPEs and NORPs using Spacy
    def _extract_spacy(self, text, snippet = None):
        spacy_results = []
        for spacy_model in self.spacy_models:
            [spacy_results.append((str(i), i.label_)) for i in spacy_model(
                text.replace("#", "")).ents if i.label_ in ["GPE", "NORP"]]
            if snippet is not None:
                [spacy_results.append((str(i), i.label_)) for i in spacy_model(
                    snippet.replace("#", "")).ents if i.label_ in ["GPE", "NORP"]]
        # Remove "http" (sometimes matched as GPE)
        spacy_results = [
            tuple((" ".join([re.sub(r'[^\w\s]', '', i3).strip() for i3 in i2.split(" ") 
                             if "http" not in i3]).strip() for i2 in list(i))) for i in spacy_results]
        spacy_results = [i for i in spacy_results if len(i[0]) > 0]
        spacy_results = pd.DataFrame(spacy_results, columns = ["text", "label"])
        # List of texts flagged both as GPEs and NORPs, keep only GPEs
        flag_both = [i for i in spacy_results["text"].unique() if len(
            spacy_results[spacy_results["text"] == i].drop_duplicates()) > 1]
        spacy_results.drop(
            spacy_results[(spacy_results["text"].isin(flag_both)) & 
                          (spacy_results["label"] == "NORP")].index, 
            inplace = True)
        spacy_results["text"] = spacy_results.apply(
            lambda x: self.norps_gpes[x["text"]] if x["text"] in self.norps_gpes.keys() 
            and x["label"] == "NORP" else x["text"], axis = 1)
        return list(set(spacy_results["text"].tolist()))
                
    # Look for location names in Tweets
    def _geotag_tweets(self):
        # Read NORPs to GPEs conversion file if not set already
        if not hasattr(self, "norps_gpes"):
            if os.path.exists(self.ref_match_params["global"]["norps_gpes_file"]):
                self.norps_gpes = pd.read_csv(
                    self.ref_match_params["global"]["norps_gpes_file"], encoding = 'cp1252'
                    ).set_index("NORP")["GPE"].to_dict()
            else:
                print("norps_gpes_file file not found, aborting")
                sys.exit()
        all_tweets = [i for i in self.mycol_tweets.find(
            {"created_at": {"$gte": self.tweets_start}}, 
            {"full_text": 1, "entities.hashtags": 1})]
        for this_tweet in tqdm(all_tweets, 
                               disable = self.ilox_logger.display_pb(), 
                               desc = "Extracting locations using NLP", 
                               leave = True):
            # Use spacy on Tweet's text (replacing hashtags)
            these_locations = [{"id": None, "initial": i, "match": i, "type": "GPE"} 
                               for i in self._extract_spacy(this_tweet["full_text"])]
            # Look into Tweet's hashtags, and try identify those who are locations written in a single word e.g. "WalnutCreek"
            if len(this_tweet["entities"]["hashtags"]) > 0:
                hashtags = [i["text"] for i in this_tweet["entities"]["hashtags"]]
                for hashtag in hashtags:
                    # Must start with capital letter and have > 1 capital letters but not be fully in capital letters
                    if hashtag[0] != hashtag[0].upper() or len(
                            [i for i in hashtag if i == i.upper()]) == 1 or hashtag == hashtag.upper():
                        continue
                    # Create new word from split hashtag on capital letters
                    new_word = " ".join(re.findall('[A-Z][^A-Z]*', hashtag))
                    # Check for GPEs
                    [these_locations.append(
                        {"id": None, "initial": hashtag, "match": i, "type": "GPE"})
                         for i in self._extract_spacy(new_word.upper())]
            if len(these_locations) > 0:
                # Only keep unique locations identified
                these_locations = pd.DataFrame(these_locations).drop_duplicates(
                    subset = ["match"])
                these_locations = these_locations[-these_locations.match.str.startswith("http", na = False)]
                these_locations = these_locations.to_dict(orient = "records")
                for this_location in these_locations:
                    self.mycol_tweets.update_one(
                        {"_id": this_tweet["_id"]}, 
                        {"$push": {"geo_tags": this_location}})
                
    # Match Tweets with owners names
    def _match_tweets_owners(self):
        for owner_name in tqdm(self.data_prep.owners_names, 
                               disable = self.ilox_logger.display_pb(), 
                               desc = "Matching Tweets to owners names", 
                               leave = True):
            # Get ids of Tweets containing current owner_name
            # Only look for whole word if len of owner_name is less than 8 (e.g. to get RichmondCA)
            if len(owner_name["match"]) < 8:
                # For total, search case sensitive for Total and TOTAL
                if owner_name["initial"] == "Total":
                    these_ids = [i["_id"] for i in self.mycol_tweets.find(
                        {"created_at": {"$gte": self.tweets_start}, 
                         "full_text": {"$regex": "\\b" + owner_name["initial"] + "\\b"}}, 
                        {"_id": 1})]
                else:
                    these_ids = [i["_id"] for i in self.mycol_tweets.find(
                        {"created_at": {"$gte": self.tweets_start}, 
                         "full_text": {"$regex": "\\b" + owner_name["match"] + "\\b", "$options": "i"}}, 
                        {"_id": 1})]
            else:
                these_ids = [i["_id"] for i in self.mycol_tweets.find(
                    {"created_at": {"$gte": self.tweets_start}, 
                     "full_text": {"$regex": owner_name["match"], "$options": "i"}}, 
                    {"_id": 1})]
            # Also perform text match using index (accents insensitive) only if owner_name sufficiently 
            # long as cannot do whole word for index search
            if len(owner_name["match"]) > 10:
                these_ids.extend([i["_id"] for i in self.mycol_tweets.find(
                    {"created_at": {"$gte": self.tweets_start}, 
                     "$text": {"$search": "\"" + owner_name["match"] + "\""}}, 
                    {"_id": 1})])
            # Add current owner_name to matching Tweets owner_tags field
            if len(these_ids) > 0:
                self.mycol_tweets.update_many(
                    {"_id": {"$in": list(set(these_ids))}}, 
                    {"$push": {"owner_tags": owner_name}})
        # Clean matches to keep only long name if short name overlap, e.g. Total and 
        # Saudi Aramco Total Refining and Petrochemical Company
        # And also drop duplicates on id
        tweets_match = [i for i in list(self.mycol_tweets.find(
            {"created_at": {"$gte": self.tweets_start}, 
             "owner_tags":{"$not": {"$size": 0}}}, 
            {"owner_tags": 1})) if len(i["owner_tags"]) > 1]
        for tweet in tweets_match:
            this_df = pd.DataFrame(tweet["owner_tags"]).drop_duplicates(subset = ["id", "type"])
            if len(this_df["initial"].unique()) > 1:
                overlaps = [i for i in this_df["match"] if len(
                    [i2 for i2 in this_df["match"] if len(i2) > len(i) and i.lower() in i2.lower()]) > 0]
                this_df.drop(this_df[this_df["match"].isin(overlaps)].index, inplace = True)
            self.mycol_tweets.update_one(
                {"_id": tweet["_id"]}, 
                {"$set": {"owner_tags": this_df.to_dict(orient = "records")}})
            
    def _match_tweets(self):
         # Load refineries_file
         self.refineries_df = pd.read_csv(
             self.refineries_file)[["GeoAssetID", "GeoAssetName", "City", "FromDate", "ToDate"]]
         self.refineries_df["FromDate"] = pd.to_datetime(self.refineries_df["FromDate"])
         # When FromDate is NaT means from infinite
         self.refineries_df["FromDate"] = self.refineries_df["FromDate"].fillna(
             pd.to_datetime("1800-01-01"))
         # When ToDate is NaT means to today
         self.refineries_df["ToDate"] = pd.to_datetime(self.refineries_df["ToDate"])
         self.refineries_df["ToDate"] = self.refineries_df["ToDate"].fillna(
             pd.to_datetime(datetime.utcnow().date()))
         get_match_proba(self.refineries_df, self.mycol_tweets, self.mycol_refineries, 
                         self.mycol_gpes, self.geolocator, self.ilox_logger.display_pb(), 
                         self.tweets_start, "created_at", self.ref_match_params["global"]["nominatim_max_attempts"], 
                         self.ref_match_params["global"]["nominatim_wait_error"], 
                         self.ref_match_params["global"]["nominatim_wait"])
         
    # Match to country/countries based on ref_match
    def _country_match(self):
        tweets_match = list(self.mycol_tweets.find(
            {"created_at": {"$gte": self.tweets_start}, "ref_match": {"$not": {"$size": 0}}}, 
            {"_id": 1, "ref_match": 1}))
        for this_tweet in tqdm(tweets_match, 
                               disable = self.ilox_logger.display_pb(), 
                               desc = "Matching Tweets to countries", 
                               leave = True):
            countries = pd.DataFrame(list(
                self.mycol_refineries.find(
                    {"GeoAssetID": {"$in": [i["GeoAssetID"] for i in this_tweet["ref_match"]]}}, 
                    {"country": 1, "_id": 0})))
            this_match = (countries.country.value_counts()/len(countries)).round(2).to_dict()
            this_match = pd.DataFrame({"p": this_match}).reset_index(
                drop = False).rename(columns = {"index": "country"})
            self.mycol_tweets.update_one(
                {"_id": this_tweet["_id"]}, 
                {"$set": {"country_match": this_match.to_dict(orient = "records")}})
            
    # Extract event types from Tweets
    def _match_tweets_event_type(self):
        # Get ids of tweets containing current event keyword in text
        for event_name in tqdm(self.tweets_scraping_params["events_keywords_text"], 
                               disable = self.ilox_logger.display_pb(), 
                               desc = "Matching Tweets text to event types",
                               leave = True):
            these_tweets = [i["_id"] for i in self.mycol_tweets.find(
                {"created_at": {"$gte": self.events_start}, 
                 "full_text": {"$regex": "\\b" + event_name + "\\b", "$options": "i"}}, 
                {"_id": 1})]
            # Add current geo_name to matching Tweet's events_tags field       
            if len(these_tweets) > 0:
                self.mycol_tweets.update_many(
                    {"_id": {"$in": list(set(these_tweets))}}, 
                    {"$push": {"events_tags": event_name}})
        # Get ids of tweets containing current event keyword in hashtags
        for event_name in tqdm(self.tweets_scraping_params["events_keywords_hashtag"], 
                               disable = self.ilox_logger.display_pb(), 
                               desc = "Matching Tweets hashtags to event types",
                               leave = True):
            these_tweets = [i["_id"] for i in self.mycol_tweets.find(
                {"created_at": {"$gte": self.events_start}, 
                 "entities.hashtags.text": {"$regex": event_name, "$options": "i"}}, 
                {"_id": 1})]
            # Add current geo_name to matching Tweet's events_tags field       
            if len(these_tweets) > 0:
                self.mycol_tweets.update_many(
                    {"_id": {"$in": list(set(these_tweets))}}, 
                    {"$push": {"events_tags": event_name}})
        # Drop duplicate match in events_tags field
        these_tweets = [i for i in self.mycol_tweets.find(
            {"created_at": {"$gte": self.events_start}, 
             "events_tags": {"$not": {"$size": 0}}}, 
            {"events_tags": 1})]
        for this_tweet in tqdm(these_tweets, 
                               disable = True):
            self.mycol_tweets.update_one(
                {"_id": this_tweet["_id"]}, 
                {"$set": {"events_tags": list(set(this_tweet["events_tags"]))}})
            
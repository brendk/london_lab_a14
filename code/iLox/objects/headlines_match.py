# -*- coding: utf-8 -*-
"""
Created on Fri Jun  4 17:32:07 2021

@author: brend
"""

import sys
import os
import spacy
import pymongo
import pandas as pd
from tqdm import tqdm
from datetime import datetime
from iLox.dependencies.get_match import get_match_proba


class HeadlinesMatch():
    
    def __init__(self, parent):
        attributes = [attr for attr in dir(parent) if not attr.startswith("__")]
        for attribute in attributes:
            setattr(self, attribute, getattr(parent, attribute))
        self._parent = parent
        # Load Spacy models
        self.spacy_models = [spacy.load(model) for model in self.ref_match_params["headlines_matching"]["nlp_models"]]
        if self.ref_match_params["headlines_matching"]["start"] is not None:
            self.headlines_start = self.ref_match_params["headlines_matching"]["start"]
        else:
            self.headlines_start = "1900-01-01"
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
        print("Preparing MongoDb collection ...")
        self._prep_col()
        # Match headlines with refineries names
        self._geotag_headlines_refineries()
        # Match headlines with cities names
        self._geotag_headlines_cities()
        # Use Spacy to extract other locations (GPEs)
        self._geotag_headlines()
        # Match headlines with owners names
        self._match_headlines_owners()
        # Get probabilities of match for refineries
        self._match_headlines()
        # Match to country/countries based on ref_match
        self._country_match()
        if self.events_match_params["run"]:
            # Extract event types from headlines
            self._match_headlines_event_type()
        
    # Prepare MongoDb collection (add geo_tags keys and clean up previous matches)
    def _prep_col(self):
        # Create indexes if not already exist
        existing_idx = self.mycol_headlines.index_information()
        if "text" not in existing_idx.keys():
            self.mycol_headlines.create_index(
                [("text", pymongo.ASCENDING)], name = "text", unique = False)
        if "snippet" not in existing_idx.keys():
            self.mycol_headlines.create_index(
                [("snippet", pymongo.ASCENDING)], name = "snippet", unique = False)
        if "text_snippet_text" not in existing_idx.keys():
            self.mycol_headlines.create_index(
                [("text", pymongo.TEXT), ("snippet", pymongo.TEXT)], 
                name = "text_snippet_text", unique = False, 
                default_language = "en", language_override = "en")
        self.mycol_headlines.update_many(
            {"firstCreated": {"$gte": self.headlines_start}}, 
            {"$set": {"geo_tags": []}})
        self.mycol_headlines.update_many(
            {"firstCreated": {"$gte": self.headlines_start}}, 
            {"$set": {"owner_tags": []}})
        self.mycol_headlines.update_many(
            {"firstCreated": {"$gte": self.headlines_start}}, 
            {"$set": {"ref_match": []}})
        self.mycol_headlines.update_many(
            {"firstCreated": {"$gte": self.headlines_start}}, 
            {"$set": {"country_match": []}})
        self.mycol_headlines.update_many(
            {"firstCreated": {"$gte": self.headlines_start}}, 
            {"$set": {"events_tags": []}})
            
    # Geotag headlines from refineries names
    def _geotag_headlines_refineries(self):
        for geo_name in tqdm(self.geo_names_r, 
                             disable = self.ilox_logger.display_pb(), 
                             desc = "Matching headlines with refineries names", 
                             leave = True):
            # Get ids of headlines containing current geoname in text
            these_ids = [i["_id"] for i in self.mycol_headlines.find(
                {"firstCreated": {"$gte": self.headlines_start}, 
                "text": {"$regex": "\\b" + geo_name["match"] + "\\b", "$options": "i"}}, 
                {"_id": 1})]
            # Also get ids of headlines containing current geoname in snippet
            these_ids = [i["_id"] for i in self.mycol_headlines.find(
                {"firstCreated": {"$gte": self.headlines_start}, 
                "snippet": {"$regex": "\\b" + geo_name["match"] + "\\b", "$options": "i"}}, 
                {"_id": 1})]
            # Also perform text match using index (accents insensitive, include text + snippet)
            these_ids.extend([i["_id"] for i in self.mycol_headlines.find(
                {"firstCreated": {"$gte": self.headlines_start}, 
                "$text": {"$search": "\"" + geo_name["match"] + "\""}}, 
                {"_id": 1})])
            # Add current geo_name to matching headline's geo_tags field
            if len(these_ids) > 0:
                self.mycol_headlines.update_many(
                    {"_id": {"$in": list(set(these_ids))}}, {"$push": {"geo_tags": geo_name}})
            
    # Geotag headlines from cities names
    def _geotag_headlines_cities(self):
        for geo_name in tqdm(self.cities_names, 
                             disable = self.ilox_logger.display_pb(), 
                             desc = "Matching headlines with cities names", 
                             leave = True):
            # Get ids of headlines containing current geoname
            # Only look for whole word if len of geo_name is less than 8 (e.g. to get RichmondCA)
            if len(geo_name) < 8:
                these_ids = [i["_id"] for i in self.mycol_headlines.find(
                    {"firstCreated": {"$gte": self.headlines_start}, 
                     "text": {"$regex": "\\b" + geo_name["match"] + "\\b", "$options": "i"}}, 
                    {"_id": 1})]
                these_ids.extend([i["_id"] for i in self.mycol_headlines.find(
                    {"firstCreated": {"$gte": self.headlines_start}, 
                     "snippet": {"$regex": "\\b" + geo_name["match"] + "\\b", "$options": "i"}}, 
                    {"_id": 1})])
            else:
                these_ids = [i["_id"] for i in self.mycol_headlines.find(
                    {"firstCreated": {"$gte": self.headlines_start}, 
                     "text": {"$regex": geo_name["match"], "$options": "i"}}, 
                    {"_id": 1})]
                these_ids.extend([i["_id"] for i in self.mycol_headlines.find(
                    {"firstCreated": {"$gte": self.headlines_start}, 
                     "snippet": {"$regex": geo_name["match"], "$options": "i"}}, 
                    {"_id": 1})])
            # Also perform text match using index (accents insensitive)
            these_ids.extend([i["_id"] for i in self.mycol_headlines.find(
                {"firstCreated": {"$gte": self.headlines_start}, 
                 "$text": {"$search": "\"" + geo_name["match"] + "\""}}, 
                {"_id": 1})])
            # Add current geo_name to matching headline's geo_tags field
            if len(these_ids) > 0:
                self.mycol_headlines.update_many(
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
            
    # Look for location names in headlines
    def _geotag_headlines(self):
        # Read NORPs to GPEs conversion file if not set already
        if not hasattr(self, "norps_gpes"):
            if os.path.exists(self.ref_match_params["global"]["norps_gpes_file"]):
                self.norps_gpes = pd.read_csv(
                    self.ref_match_params["global"]["norps_gpes_file"], encoding = 'cp1252'
                    ).set_index("NORP")["GPE"].to_dict()
            else:
                print("norps_gpes_file file not found, aborting")
                sys.exit()
        all_headlines = [i for i in self.mycol_headlines.find(
            {"firstCreated": {"$gte": self.headlines_start}}, 
            {"text": 1, "snippet": 1})]
        for this_headline in tqdm(all_headlines, 
                                  disable = self.ilox_logger.display_pb(), 
                                  desc = "Extracing locations using NLP", 
                                  leave = True):
            # Use spacy on headline's text and snippet (replacing hashtags)
            these_locations = self._extract_spacy(this_headline["text"], 
                                                  snippet = this_headline["snippet"])
            these_locations = [{"id": None, "initial": i, "match": i, "type": "GPE"} 
                               for i in these_locations]
            for this_location in these_locations:
                self.mycol_headlines.update_one(
                    {"_id": this_headline["_id"]}, 
                    {"$push": {"geo_tags": this_location}})
                
    # Match headlines with owners names
    def _match_headlines_owners(self):
        for owner_name in tqdm(self.owners_names, 
                               disable = self.ilox_logger.display_pb(), 
                               desc = "Matching headlines to owners names", 
                               leave = True):
            # Get ids of headlines containing current owner_name
            # Only look for whole word if len of owner_name is less than 8 (e.g. to get RichmondCA)
            if len(owner_name["match"]) < 8:
                # For total, search case sensitive for Total and TOTAL
                if owner_name["initial"] == "Total":
                    these_ids = [i["_id"] for i in self.mycol_headlines.find(
                        {"firstCreated": {"$gte": self.headlines_start}, 
                         "text": {"$regex": "\\b" + owner_name["initial"] + "\\b"}}, 
                        {"_id": 1})]
                    these_ids.extend([i["_id"] for i in self.mycol_headlines.find(
                        {"firstCreated": {"$gte": self.headlines_start}, 
                         "snippet": {"$regex": "\\b" + owner_name["initial"].upper() + "\\b"}}, 
                        {"_id": 1})])
                else:
                    these_ids = [i["_id"] for i in self.mycol_headlines.find(
                        {"firstCreated": {"$gte": self.headlines_start}, 
                         "text": {"$regex": "\\b" + owner_name["match"] + "\\b", "$options": "i"}}, 
                        {"_id": 1})]
                    these_ids.extend([i["_id"] for i in self.mycol_headlines.find(
                        {"firstCreated": {"$gte": self.headlines_start}, 
                         "snippet": {"$regex": "\\b" + owner_name["match"] + "\\b", "$options": "i"}}, 
                        {"_id": 1})])
            else:
                these_ids = [i["_id"] for i in self.mycol_headlines.find(
                    {"firstCreated": {"$gte": self.headlines_start}, 
                     "text": {"$regex": owner_name["match"], "$options": "i"}}, 
                    {"_id": 1})]
                these_ids.extend([i["_id"] for i in self.mycol_headlines.find(
                    {"firstCreated": {"$gte": self.headlines_start}, 
                     "snippet": {"$regex": owner_name["match"], "$options": "i"}}, 
                    {"_id": 1})])
            # Also perform text match using index (accents insensitive) only if owner_name sufficiently 
            # long as cannot do whole word for index search
            if len(owner_name["match"]) > 10:
                these_ids.extend([i["_id"] for i in self.mycol_headlines.find(
                    {"firstCreated": {"$gte": self.headlines_start}, 
                     "$text": {"$search": "\"" + owner_name["match"] + "\""}}, 
                    {"_id": 1})])
            # Add current owner_name to matching headline's owner_tags field
            if len(these_ids) > 0:
                self.mycol_headlines.update_many(
                    {"_id": {"$in": list(set(these_ids))}}, 
                    {"$push": {"owner_tags": owner_name}})
        # Clean matches to keep only long name if short name overlap, e.g. Total and 
        # Saudi Aramco Total Refining and Petrochemical Company
        # And also drop duplicates on id
        headlines_match = [i for i in list(self.mycol_headlines.find(
            {"firstCreated": {"$gte": self.headlines_start}, 
             "owner_tags":{"$not": {"$size": 0}}}, 
            {"owner_tags": 1})) if len(i["owner_tags"]) > 1]
        for headline in headlines_match:
            this_df = pd.DataFrame(headline["owner_tags"]).drop_duplicates(subset = ["id", "type"])
            if len(this_df["initial"].unique()) > 1:
                overlaps = [i for i in this_df["match"] if len(
                    [i2 for i2 in this_df["match"] if len(i2) > len(i) and i.lower() in i2.lower()]) > 0]
                this_df.drop(this_df[this_df["match"].isin(overlaps)].index, inplace = True)
            self.mycol_headlines.update_one(
                {"_id": headline["_id"]}, 
                {"$set": {"owner_tags": this_df.to_dict(orient = "records")}})
            
    def _match_headlines(self):
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
         get_match_proba(self.refineries_df, self.mycol_headlines, self.mycol_refineries, 
                         self.mycol_gpes, self.geolocator, self.ilox_logger.display_pb(), 
                         self.headlines_start, "firstCreated", self.nominatim_max_attempts, 
                         self.nominatim_wait_error, self.nominatim_wait)
         
    # Match to country/countries based on ref_match
    def _country_match(self):
        headlines_match = list(self.mycol_headlines.find(
            {"firstCreated": {"$gte": self.headlines_start}, "ref_match": {"$not": {"$size": 0}}}, 
            {"_id": 1, "ref_match": 1}))
        for this_headline in tqdm(headlines_match, 
                                  disable = self.ilox_logger.display_pb(), 
                                  desc = "Matching headlines to countries", 
                                  leave = True):
            countries = pd.DataFrame(list(
                self.mycol_refineries.find(
                    {"GeoAssetID": {"$in": [i["GeoAssetID"] for i in this_headline["ref_match"]]}}, 
                    {"country": 1, "_id": 0})))
            this_match = (countries.country.value_counts()/len(countries)).round(2).to_dict()
            this_match = pd.DataFrame({"p": this_match}).reset_index(
                drop = False).rename(columns = {"index": "country"})
            self.mycol_headlines.update_one(
                {"_id": this_headline["_id"]}, 
                {"$set": {"country_match": this_match.to_dict(orient = "records")}})
            
    # Extract event types from headlines
    def _match_headlines_event_type(self):
        # Get ids of headlines containing current event keyword in text or snippet
        for event_name in tqdm(self.tweets_scraping_params["events_keywords_text"], 
                               disable = self.ilox_logger.display_pb(), 
                               desc = "Matching headlines to event types", 
                               leave = True):
            these_headlines = [i["_id"] for i in self.mycol_headlines.find(
                {"firstCreated": {"$gte": self.events_start}, 
                 "text": {"$regex": "\\b" + event_name + "\\b", "$options": "i"}}, 
                {"_id": 1})]
            these_headlines.extend([i["_id"] for i in self.mycol_headlines.find(
                {"firstCreated": {"$gte": self.events_start}, 
                 "snippet": {"$regex": "\\b" + event_name + "\\b", "$options": "i"}}, 
                {"_id": 1})])
            # Add current geo_name to matching Tweet's events_tags field       
            if len(these_headlines) > 0:
                self.mycol_headlines.update_many(
                    {"_id": {"$in": list(set(these_headlines))}}, 
                    {"$push": {"events_tags": event_name}})
        # Drop duplicate match in events_tags field
        these_headlines = [i for i in self.mycol_headlines.find(
            {"firstCreated": {"$gte": self.events_start}, 
             "events_tags": {"$not": {"$size": 0}}}, {"events_tags": 1})]
        for this_headline in tqdm(these_headlines, 
                                  disable = True):
            self.mycol_headlines.update_one(
                {"_id": this_headline["_id"]}, 
                {"$set": {"events_tags": list(set(this_headline["events_tags"]))}})
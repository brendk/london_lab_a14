# -*- coding: utf-8 -*-
"""
Created on Thu Apr 29 19:40:09 2021

@author: brend
"""

import pymongo
import pandas as pd
import numpy as np
import warnings
import re
from tqdm import tqdm
from datetime import datetime
import spacy
nlp = spacy.load("en_core_web_sm")


refineries_file = "data/GeoAssets_Table.csv"
owners_file = "data/GeoAsset_Owner.csv"

mongoDB_Host = "127.0.0.1"
mongoDB_Db = "OilX"
mongoDB_Col = "Tweets_clean"

events_keywords_h = ["fire", "outage", "turnarounds", "tar", "tars", "maintenance", "downtime", "cuts", "runreduction", "run_reduction", 
                     "reduction", "throughput", "explosion", "strike", "problems", "capacity", "capacityreduction", 
                     "capacity_reduction", "expansion", "capacityexpansion", "capacity_expansion", "newrefinery", 
                     "inauguration", "commissioning", "down", "runcuts", "run_cuts", "shutdown", "attack", "blaze", "smoke"]

events_keywords_t = ["fire", "outage", "turnarounds", "tar", "tars", "maintenance", "downtime", "cuts", "run reduction", 
                     "reduction", "throughput", "explosion", "strike", "problems", "capacity", "capacity reduction", 
                     "expansion", "capacity expansion", "inauguration", "commissioning", "down", "run cuts", 
                     "shutdown", "attack", "blaze", "smoke"]



# Create network MongoDB connection
def createNetworkMongo(mongoDB_Host = "localhost", mongoDB_Db = "admin", mongoDB_Col = "", mongoDB_Port = 27017, mongoDB_Auth = False, mongoDB_Username = None, mongoDB_Password = None, noTest = False, retry_count = 10):
    from pymongo import MongoClient
    if mongoDB_Auth:
        while True:
            try:
                myclient = MongoClient(host = mongoDB_Host, port = mongoDB_Port, username = mongoDB_Username, 
                                       password = mongoDB_Password, authSource = mongoDB_Db, authMechanism = 'SCRAM-SHA-256')
                break
            except pymongo.errors.AutoReconnect:
                retry_count -= 1
            if retry_count <= 0: break
    else: 
        myclient = MongoClient(host = mongoDB_Host, port = mongoDB_Port)
    mydb = myclient[mongoDB_Db]
    mycol = mydb[mongoDB_Col]
    if not noTest:
        testConn = mycol.find({})
        try:
            testConn = testConn[0]
        except IndexError:
            warnings.warn("MongoDb connection failed - %s.%s @ %s" % (mongoDB_Db, mongoDB_Col, mongoDB_Host))
    return mycol


# Get list of potential refineries names
def get_geo_names_refineries(refineries_file):
    names = pd.read_csv(refineries_file)[["GeoAssetID", "GeoAssetName"]].drop_duplicates(subset = ["GeoAssetName"])
    geo_names_r_original = names.to_dict(orient = "records")
    geo_names_r = []
    for geo_name in tqdm(geo_names_r_original):
        # Store original name and id
        geo_name_orig = geo_name["GeoAssetName"]
        name_id = geo_name["GeoAssetID"]
        # Remove text between parentheses (and parentheses)
        geo_name = re.sub("[\(].*?[\)]", "", geo_name["GeoAssetName"]).strip()
        # Convert to list of words (lowercase)
        words = geo_name.lower().split(" ")
        # Remove "refin*" and "|" + "(" and ")"
        words = [w.strip().replace("|", "").replace("(", "").replace(")", "") for w in words if not w.startswith("refin")]
        # Recreate sentence
        geo_name = " ".join(words).strip()
        geo_names_r.append({"id": name_id, "initial": geo_name_orig, "match": geo_name, "type": "refname"})
        # If 3 words on less, also add 2 first words merged + all 3 merged (e.g. CarsonLA)
        if len(words) <= 3:
            geo_names_r.append({"id": name_id, "initial": geo_name_orig, "match": "".join(words[:3]), "type": "refname"})
            geo_names_r.append({"id": name_id, "initial": geo_name_orig, "match": "".join(words[:2]), "type": "refname"})
    # Convert geo_names_r to dataframe to drop duplicates
    geo_names_r_df = pd.DataFrame(geo_names_r).drop_duplicates(subset = ["initial", "match"])
    # Keep only not empty names
    geo_names_r_df = geo_names_r_df[geo_names_r_df["match"] != ""]
    # Return as list
    geo_names_r = geo_names_r_df.to_dict(orient = "records")
    return geo_names_r


# Get list of potential owners names
def get_owners_names(owners_file):
    names = pd.read_csv(owners_file)[["GeoAssetID", "CompanyName"]].drop_duplicates()
    names = names.replace("Unknown", np.NaN).dropna().to_dict(orient = "records")
    owners_names = []
    for owner_name in tqdm(names):
        # Store original name and id
        name_orig = owner_name["CompanyName"]
        name_id = owner_name["GeoAssetID"]
        # Remove text between parentheses (and parentheses)
        owner_name_2 = re.sub("[\(].*?[\)]", "", owner_name["CompanyName"]).strip()
        # Convert to list of words (lowercase)
        words = owner_name_2.lower().split(" ")
        # Remove "|" + "(" and ")"
        words = [w.strip().replace("|", "").replace("(", "").replace(")", "") for w in words]
        # Recreate sentence
        owner_name_2 = " ".join(words).strip()
        owners_names.append({"id": name_id, "initial": name_orig, "match": owner_name_2, "type": "ownername"})
        # If 3 words on less, also add 2 first words merged + all 3 merged (e.g. CarsonLA)
        if len(words) <= 3:
            owners_names.append({"id": name_id, "initial": name_orig, "match": "".join(words[:3]), "type": "ownername"})
            owners_names.append({"id": name_id, "initial": name_orig, "match": "".join(words[:2]), "type": "ownername"})
    # Convert geo_names_r to dataframe to drop duplicates
    owners_names_df = pd.DataFrame(owners_names).drop_duplicates(subset = ["id", "match"])
    # Keep only not empty names
    owners_names_df = owners_names_df[owners_names_df["match"] != ""]
    # Return as list
    owners_names = owners_names_df.to_dict(orient = "records")
    return owners_names


# Get list of potential refineries names
def get_geo_names_cities(refineries_file):
    cities = pd.read_csv(refineries_file)[["GeoAssetID", "City"]].drop_duplicates().dropna()
    cities_names_original = cities.to_dict(orient = "records")
    cities_names = []
    for city_name in tqdm(cities_names_original):
        # Store original name and id
        city_name_orig = city_name["City"]
        name_id = city_name["GeoAssetID"]
        # If name contains "-", create new one (keep existing) without "-"
        if "-" in city_name["City"]:
            city_name_2 = " ".join(city_name["City"].replace("-", " ").strip().split())
            words = [w.strip().replace("|", "").replace("(", "").replace(")", "").replace(",", "") for w in city_name_2.split(" ")]
            if len(words) <= 3:
                cities_names.append({"id": name_id, "initial": city_name_orig, "match": "".join(words[:3]), "type": "cityname"})
                cities_names.append({"id": name_id, "initial": city_name_orig, "match": "".join(words[:2]), "type": "cityname"})
            cities_names.append({"id": name_id, "initial": city_name_orig, "match": city_name_2, "type": "cityname"})
        # If name contains ",", create new ones (keep existing) by splitting on ","
        if "," in city_name["City"]:
            for sub_city_name in city_name["City"].split(","):
                sub_city_name_2 = sub_city_name.strip()
                words = [w.strip().replace("|", "").replace("(", "").replace(")", "") for w in sub_city_name_2.split(" ")]
                if len(words) <= 3:
                    cities_names.append({"id": name_id, "initial": city_name_orig, "match": "".join(words[:3]), "type": "cityname"})
                    cities_names.append({"id": name_id, "initial": city_name_orig, "match": "".join(words[:2]), "type": "cityname"})
                cities_names.append({"id": name_id, "initial": city_name_orig, "match": sub_city_name_2, "type": "cityname"})
        words = [w.strip().replace("|", "").replace("(", "").replace(")", "").replace(",", "") for w in city_name["City"].split(" ")]
        if len(words) <= 3:
            cities_names.append({"id": name_id, "initial": city_name_orig, "match": "".join(words[:3]), "type": "cityname"})
            cities_names.append({"id": name_id, "initial": city_name_orig, "match": "".join(words[:2]), "type": "cityname"})
        cities_names.append({"id": name_id, "initial": city_name_orig, "match": city_name["City"], "type": "cityname"})
    # Convert geo_names_r to dataframe to drop duplicates
    cities_names_df = pd.DataFrame(cities_names).drop_duplicates(subset = ["id", "match"])
    # Keep only names with positive length (not empty) and drop duplicates
    cities_names_df = cities_names_df[-cities_names_df["match"].isin(["nan", ""])]
    # Return as list
    cities_names = cities_names_df.to_dict(orient = "records")
    return cities_names


# Look for location names in Tweets
def geotag_tweets(mycol):
    all_tweets = [i for i in mycol.find({})]
    for this_tweet in tqdm(all_tweets):
        these_locations = []
        # Use spacy on Tweet's text (replacing hashtags)
        these_locations.append([{"id": None, "initial": i.text, "match": i.text, "type": "GPE"} 
                                for i in nlp(this_tweet["full_text"].upper().replace("#", "")).ents if i.label_ == "GPE"])
        # Look into Tweet's hashtags, and try identify those who are locations written in a single word e.g. "WalnutCreek"
        if len(this_tweet["entities"]["hashtags"]) > 0:
            hashtags = [i["text"] for i in this_tweet["entities"]["hashtags"]]
            for hashtag in hashtags:
                # Must start with capital letter and have > 1 capital letters but not be fully in capital letters
                if hashtag[0] != hashtag[0].upper() or len([i for i in hashtag if i == i.upper()]) == 1 or hashtag == hashtag.upper():
                    continue
                # Create new word from split hashtag on capital letters
                new_word = " ".join(re.findall('[A-Z][^A-Z]*', hashtag))
                # Check for GPEs
                these_locations.append([{"id": None, "initial": hashtag, "match": i.text, "type": "GPE"} 
                                        for i in nlp(new_word.upper()).ents if i.label_ == "GPE"])
        # Only keep unique locations identified
        these_locations = pd.DataFrame([x for x1 in these_locations for x in x1]).drop_duplicates(
            subset = ["match"]).to_dict(orient = "records")
        for this_location in these_locations:
            mycol.update_one({"_id": this_tweet["_id"]}, {"$push": {"geo_tags": this_location}})
    return


# Geotag Tweets from refineries names
def geotag_tweets_refineries(mycol, geo_names_r):
    for geo_name in tqdm(geo_names_r):
        # Get ids of Tweets containing current geoname
        these_ids = [i["_id"] for i in mycol.find({"full_text": {"$regex": "\\b" + geo_name["match"] + "\\b", "$options": "i"}}, {"_id": 1})]
        # Also perform text match using index (accents insensitive)
        these_ids.extend([i["_id"] for i in mycol.find({"$text": {"$search": "\"" + geo_name["match"] + "\""}}, {"_id": 1})])
        # Add current geo_name to matching Tweet's geo_tags field
        if len(these_ids) > 0:
            mycol.update_many({"_id": {"$in": list(set(these_ids))}}, {"$push": {"geo_tags": geo_name}})
    return
        

# Geotag Tweets from cities names
def geotag_tweets_cities(mycol, cities_names):
    for geo_name in tqdm(cities_names):
        # Get ids of Tweets containing current geoname
        # Only look for whole word if len of geo_name is less than 8 (e.g. to get RichmondCA)
        if len(geo_name) < 8:
            these_ids = [i["_id"] for i in mycol.find({"full_text": {"$regex": "\\b" + geo_name["match"] + "\\b", "$options": "i"}}, {"_id": 1})]
        else:
            these_ids = [i["_id"] for i in mycol.find({"full_text": {"$regex": geo_name["match"], "$options": "i"}}, {"_id": 1})]
        # Also perform text match using index (accents insensitive)
        these_ids.extend([i["_id"] for i in mycol.find({"$text": {"$search": "\"" + geo_name["match"] + "\""}}, {"_id": 1})])
        # Add current geo_name to matching Tweet's geo_tags field
        if len(these_ids) > 0:
            mycol.update_many({"_id": {"$in": list(set(these_ids))}}, {"$push": {"geo_tags": geo_name}})
    return


# Match Tweets with owners names
def match_tweets_owners(mycol, owners_names):
    for owner_name in tqdm(owners_names):
        # Get ids of Tweets containing current owner_name
        # Only look for whole word if len of owner_name is less than 8 (e.g. to get RichmondCA)
        if len(owner_name) < 8:
            these_ids = [i["_id"] for i in mycol.find({"full_text": {"$regex": "\\b" + owner_name["match"] + "\\b", "$options": "i"}}, {"_id": 1})]
        else:
            these_ids = [i["_id"] for i in mycol.find({"full_text": {"$regex": owner_name["match"], "$options": "i"}}, {"_id": 1})]
        # Also perform text match using index (accents insensitive)
        these_ids.extend([i["_id"] for i in mycol.find({"$text": {"$search": "\"" + owner_name["match"] + "\""}}, {"_id": 1})])
        # Add current owner_name to matching Tweet's owner_tags field
        if len(these_ids) > 0:
            mycol.update_many({"_id": {"$in": list(set(these_ids))}}, {"$push": {"owner_tags": owner_name}})
    return


# Prepare MongoDb collection (add geo_tags key)
def prep_col(mycol):
    # Create indexes if not already exist
    existing_idx = mycol.index_information()
    if "full_text" not in existing_idx.keys():
        mycol.create_index([("full_text", pymongo.ASCENDING)], name = "full_text", unique = False)
    if "full_text_text" not in existing_idx.keys():
        mycol.create_index([("full_text", pymongo.TEXT)], name = "full_text_text", unique = False)
    mycol.update_many({}, {"$set": {"geo_tags": []}})
    mycol.update_many({}, {"$set": {"owner_tags": []}})
    mycol.update_many({}, {"$set": {"ref_match": []}})
    mycol.update_many({}, {"$set": {"events_tags": []}})
    
    
# Get probabilities of match for refineries
def get_match_proba(mycol, refineries_file):
    # Load both files
    refineries_df = pd.read_csv(refineries_file)[["GeoAssetID", "GeoAssetName", "City", "FromDate", "ToDate"]]
    refineries_df["FromDate"] = pd.to_datetime(refineries_df["FromDate"])
    # When FromDate is NaT means from infinite
    refineries_df["FromDate"] = refineries_df["FromDate"].fillna(pd.to_datetime("1800-01-01"))
    # When ToDate is NaT means to today
    refineries_df["ToDate"] = pd.to_datetime(refineries_df["ToDate"])
    refineries_df["ToDate"] = refineries_df["ToDate"].fillna(pd.to_datetime(datetime.utcnow().date()))
    # Get all tweets with at least 1 geo_tag
    match_tweets = [i for i in mycol.find({"geo_tags": {"$not": {"$size": 0}}}, 
                                          {"geo_tags": 1, "owner_tags": 1, "created_at": 1})]
    # Loop through each tweet
    for tweet in tqdm(match_tweets):
        tweet_date = pd.to_datetime(datetime.strptime(tweet["created_at"], "%a %b %d %H:%M:%S %z %Y").date())
        geo_tags = pd.DataFrame(tweet["geo_tags"]).drop_duplicates(subset = ["id", "type"])
        geo_tags_types = geo_tags["type"].unique().tolist()
        # Do not use GPE
        geo_tags = geo_tags[geo_tags["type"].isin(["refname", "cityname"])]
        owner_tags = pd.DataFrame(tweet["owner_tags"]).drop_duplicates(subset = ["id", "type"])
        ref_ids = geo_tags[geo_tags["type"] == "refname"]["id"].astype(int).tolist()
        # Get list of refineries with selected ids and active at the time of the tweet
        refineries_match = refineries_df[refineries_df["GeoAssetID"].isin(ref_ids) & (refineries_df["FromDate"] <= tweet_date) & 
                                         (refineries_df["ToDate"] >= tweet_date)]
        # If only 1 match then save to collection with proba = 100% and continue
        if len(refineries_match) == 1:
            match_dict = refineries_match[["GeoAssetID", "GeoAssetName"]].copy()
            match_dict["confidence"] = 1.0
            mycol.update_one({"_id": tweet["_id"]}, {"$set": {"ref_match": match_dict.to_dict(orient = "records")}})
            continue
        # Otherwise look at city - keep only ids that appear twice (must match for refname + city)
        ref_ids.extend(geo_tags[geo_tags["type"] == "cityname"]["id"].astype(int).tolist())
        ref_ids = list(set([r for r in ref_ids if ref_ids.count(r) == len([i for i in geo_tags_types if i in ["refname", "city"]])]))
        # Get list of refineries with selected ids and active at the time of the tweet
        refineries_match = refineries_df[refineries_df["GeoAssetID"].isin(ref_ids) & (refineries_df["FromDate"] <= tweet_date) & 
                                         (refineries_df["ToDate"] >= tweet_date)]
        # If only 1 match then save to collection with proba = 100% and continue
        if len(refineries_match) == 1:
            match_dict = refineries_match[["GeoAssetID", "GeoAssetName"]].copy()
            match_dict["confidence"] = 1.0
            mycol.update_one({"_id": tweet["_id"]}, {"$set": {"ref_match": match_dict.to_dict(orient = "records")}})
            continue
        # Else if no owner information then save all matchs in collection with proba = 1/n matchs
        elif len(refineries_match) > 1 and len(owner_tags) == 0:
            match_dict = refineries_match[["GeoAssetID", "GeoAssetName"]].copy()
            match_dict["confidence"] = 1.0 / len(match_dict)
            mycol.update_one({"_id": tweet["_id"]}, {"$set": {"ref_match": match_dict.to_dict(orient = "records")}})
            continue
        # If no owner information and no match so far, look into GPEs
        elif len(refineries_match) == 0 and len(owner_tags) == 0:
            continue
        # If information on owner
        if len(owner_tags) > 0:
            # Otherwise look at owner - keep only ids that appear once more (must match for refname + city + owner)
            ref_ids.extend(owner_tags[owner_tags["type"] == "ownername"]["id"].astype(int).tolist())
            ref_ids = list(set([r for r in ref_ids if ref_ids.count(r) == len([i for i in geo_tags_types if i in ["refname", "city", "owner"]])]))
            # Get list of refineries with selected ids and active at the time of the tweet
            refineries_match = refineries_df[refineries_df["GeoAssetID"].isin(ref_ids) & (refineries_df["FromDate"] <= tweet_date) & 
                                             (refineries_df["ToDate"] >= tweet_date)]
            # If only 1 match then save to collection with proba = 100% and continue
            if len(refineries_match) == 1:
                match_dict = refineries_match[["GeoAssetID", "GeoAssetName"]].copy()
                match_dict["confidence"] = 1.0
                mycol.update_one({"_id": tweet["_id"]}, {"$set": {"ref_match": match_dict.to_dict(orient = "records")}})
                continue
        # If information on GPE
        if "GPE" in geo_tags_types:
            
        # Otherwise save all matchs in collection with proba = 1/n matchs
        elif len(refineries_match) > 1:
            match_dict = refineries_match[["GeoAssetID", "GeoAssetName"]].copy()
            match_dict["confidence"] = 1.0 / len(match_dict)
            mycol.update_one({"_id": tweet["_id"]}, {"$set": {"ref_match": match_dict.to_dict(orient = "records")}})
            continue
        
    # Get all tweets with owner_tag but no geo_tag
    match_tweets = [i for i in mycol.find({"geo_tags": {"$size": 0}, "owner_tags": {"$not": {"$size": 0}}}, 
                                          {"geo_tags": 1, "owner_tags": 1, "created_at": 1})]
    # Loop through each tweet
    for tweet in tqdm(match_tweets):
        tweet_date = pd.to_datetime(datetime.strptime(tweet["created_at"], "%a %b %d %H:%M:%S %z %Y").date())
        owner_tags = pd.DataFrame(tweet["owner_tags"]).drop_duplicates(subset = ["id", "type"])
        ref_ids = owner_tags[owner_tags["type"] == "ownername"]["id"].astype(int).tolist()
        # Get list of refineries with selected ids and active at the time of the tweet
        refineries_match = refineries_df[refineries_df["GeoAssetID"].isin(ref_ids) & (refineries_df["FromDate"] <= tweet_date) & 
                                         (refineries_df["ToDate"] >= tweet_date)]
        # If only 1 match then save to collection with proba = 100% and continue
        if len(refineries_match) == 1:
            match_dict = refineries_match[["GeoAssetID", "GeoAssetName"]].copy()
            match_dict["confidence"] = 1.0
            mycol.update_one({"_id": tweet["_id"]}, {"$set": {"ref_match": match_dict.to_dict(orient = "records")}})
            continue
        # Otherwise save all matchs in collection with proba = 1/n matchs
        elif len(refineries_match) > 1:
            match_dict = refineries_match[["GeoAssetID", "GeoAssetName"]].copy()
            match_dict["confidence"] = 1.0 / len(match_dict)
            mycol.update_one({"_id": tweet["_id"]}, {"$set": {"ref_match": match_dict.to_dict(orient = "records")}})
            continue
        
        
# Extract event types from tweets
def match_tweets_event_type(mycol, events_keywords_h, events_keywords_t):
    # Get ids of Tweets containing current event keyword in hashtags
    for event_name in tqdm(events_keywords_h):
        these_tweets = [i["_id"] for i in mycol.find({"entities.hashtags.text": {"$regex": event_name, "$options": "i"}}, {"_id": 1})]
        # Add current event_name to matching Tweet's events_tags field       
        if len(these_tweets) > 0:
            mycol.update_many({"_id": {"$in": these_tweets}}, {"$push": {"events_tags": event_name}})
    # Get ids of Tweets containing current event keyword in text
    for event_name in tqdm(events_keywords_t):
        these_tweets = [i["_id"] for i in mycol.find({"full_text": {"$regex": "\\b" + event_name + "\\b", "$options": "i"}}, {"_id": 1})]
        # Add current geo_name to matching Tweet's events_tags field       
        if len(these_tweets) > 0:
            mycol.update_many({"_id": {"$in": these_tweets}}, {"$push": {"events_tags": event_name}})
    # Drop duplicate match in events_tags field
    these_tweets = [i for i in mycol.find({"events_tags": {"$not": {"$size": 0}}}, {"events_tags": 1})]
    for this_tweet in tqdm(these_tweets):
        mycol.update_one({"_id": this_tweet["_id"]}, {"$set": {"events_tags": list(set(this_tweet["events_tags"]))}})
    return
    


def main():
    # Connect to MongoDb collection
    print("Connect to MongoDb collection")
    mycol = createNetworkMongo(mongoDB_Host, mongoDB_Db, mongoDB_Col)
    # Prepare MongoDb collection (add geo_tags key)
    print("Prepare MongoDb collection (add geo_tags key)")
    prep_col(mycol)
    # Get refineries names from merged file 
    print("Get refineries names from merged file")
    geo_names_r = get_geo_names_refineries(refineries_file)
    # Match Tweets with refineries names
    print("Match Tweets with refineries names")
    geotag_tweets_refineries(mycol, geo_names_r)
    # Get cities names from merged file 
    print("Get cities names from merged file")
    cities_names = get_geo_names_cities(refineries_file)
    # Match Tweets with cities names
    print("Match Tweets with cities names")
    geotag_tweets_cities(mycol, cities_names)
    # Use Spacy to extract other locations (GPEs)
    print("Use Spacy to extract other locations (GPEs)")
    geotag_tweets(mycol)
    # Get owners names
    print("Get owners names")
    owners_names = get_owners_names(owners_file)
    # Match Tweets with owners names
    print("Match Tweets with owners names")
    match_tweets_owners(mycol, owners_names)
    # Get probabilities of match for refineries
    print("Get probabilities of match for refineries")
    get_match_proba(mycol, refineries_file)
    # Extract event types from tweets
    print("Extract event types from tweets")
    match_tweets_event_type(mycol, events_keywords_h, events_keywords_t)

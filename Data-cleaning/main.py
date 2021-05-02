# -*- coding: utf-8 -*-
"""
Created on Thu Apr 29 19:40:09 2021

@author: brend
"""

import pymongo
import pandas as pd
import warnings
import re
from tqdm import tqdm
import spacy
nlp = spacy.load("en_core_web_sm")


refineries_file = "refineries_list_merge.xlsx"

mongoDB_Host = "127.0.0.1"
mongoDB_Db = "OilX"
mongoDB_Col = "Tweets_clean"



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
    names = pd.read_excel(refineries_file)[["ID", "Refinery"]].drop_duplicates(subset = ["Refinery"])
    geo_names_r_original = names.to_dict(orient = "records")
    geo_names_r = []
    for geo_name in tqdm(geo_names_r_original):
        # Store original name and id
        geo_name_orig = geo_name["Refinery"]
        name_id = geo_name["ID"]
        # Remove text between parentheses (and parentheses)
        geo_name = re.sub("[\(].*?[\)]", "", geo_name["Refinery"]).strip()
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


# Get list of potential refineries names
def get_geo_names_cities(refineries_file):
    cities = pd.read_excel(refineries_file)[["ID", "City"]].drop_duplicates(subset = ["City"]).dropna()
    cities_names_original = cities.to_dict(orient = "records")
    cities_names = []
    for city_name in tqdm(cities_names_original):
        # Store original name and id
        city_name_orig = city_name["City"]
        name_id = city_name["ID"]
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
    cities_names_df = pd.DataFrame(cities_names).drop_duplicates(subset = ["initial", "match"])
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
                                for i in nlp(this_tweet["full_text"].replace("#", "")).ents if i.label_ == "GPE"])
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
                                        for i in nlp(new_word).ents if i.label_ == "GPE"])
        # Only keep unique locations identified
        these_locations = pd.DataFrame([x for x1 in these_locations for x in x1]).drop_duplicates(
            subset = ["initial", "match"]).to_dict(orient = "records")
        for this_location in these_locations:
            mycol.update_one({"_id": this_tweet["_id"]}, {"$push": {"geo_tags": this_location}})
    return


# Geotag Tweets from refineries names
def geotag_tweets_refineries(mycol, geo_names_r):
    for geo_name in tqdm(geo_names_r):
        # Get ids of Tweets containing current geoname
        these_ids = [i["_id"] for i in mycol.find({"full_text": {"$regex": "\\b" + geo_name["match"] + "\\b", "$options": "i"}}, {"_id": 1})]
        # Add current geo_name to matching Tweet's geo_tags field
        if len(these_ids) > 0:
            mycol.update_many({"_id": {"$in": these_ids}}, {"$push": {"geo_tags": geo_name}})
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
        # Add current geo_name to matching Tweet's geo_tags field
        if len(these_ids) > 0:
            mycol.update_many({"_id": {"$in": these_ids}}, {"$push": {"geo_tags": geo_name}})
    return


# Prepare MongoDb collection (add geo_tags key)
def prep_col(mycol):
    mycol.update_many({}, {"$set": {"geo_tags": []}})


def main():
    # Connect to MongoDb collection
    mycol = createNetworkMongo(mongoDB_Host, mongoDB_Db, mongoDB_Col)
    # Prepare MongoDb collection (add geo_tags key)
    prep_col(mycol)
    # Get refineries names from merged file 
    geo_names_r = get_geo_names_refineries(refineries_file)
    # Match Tweets with refineries names
    geotag_tweets_refineries(mycol, geo_names_r)
    # Get cities names from merged file 
    cities_names = get_geo_names_cities(refineries_file)
    # Match Tweets with cities names
    geotag_tweets_cities(mycol, cities_names)
    # Use Spacy to extract other locations (GPEs)
    geotag_tweets(mycol)
    
